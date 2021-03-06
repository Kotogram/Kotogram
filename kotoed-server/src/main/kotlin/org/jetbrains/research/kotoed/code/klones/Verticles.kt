package org.jetbrains.research.kotoed.code.klones

import com.google.common.collect.Queues
import com.intellij.psi.PsiElement
import com.suhininalex.suffixtree.SuffixTree
import io.vertx.core.json.JsonObject
import kotlinx.coroutines.withContext
import kotlinx.warnings.Warnings.UNUSED_PARAMETER
import kotlinx.warnings.Warnings.USELESS_CAST
import org.antlr.v4.runtime.CharStreams
import org.antlr.v4.runtime.CommonTokenStream
import org.jetbrains.kotlin.psi.KtNamedFunction
import org.jetbrains.kotlin.psi.psiUtil.collectDescendantsOfType
import org.jetbrains.research.kotoed.code.Filename
import org.jetbrains.research.kotoed.data.api.Code
import org.jetbrains.research.kotoed.data.db.ComplexDatabaseQuery
import org.jetbrains.research.kotoed.data.vcs.CloneStatus
import org.jetbrains.research.kotoed.database.enums.SubmissionState
import org.jetbrains.research.kotoed.database.tables.records.CourseRecord
import org.jetbrains.research.kotoed.database.tables.records.ProjectRecord
import org.jetbrains.research.kotoed.database.tables.records.SubmissionResultRecord
import org.jetbrains.research.kotoed.db.condition.lang.formatToQuery
import org.jetbrains.research.kotoed.eventbus.Address
import org.jetbrains.research.kotoed.parsers.HaskellLexer
import org.jetbrains.research.kotoed.util.*
import org.jetbrains.research.kotoed.util.code.getPsi
import org.jetbrains.research.kotoed.util.code.temporaryKotlinEnv
import org.kohsuke.randname.RandomNameGenerator
import ru.spbstu.ktuples.placeholders._0
import ru.spbstu.ktuples.placeholders.bind

sealed class KloneRequest(val priority: Int) : Jsonable, Comparable<KloneRequest> {
    override fun compareTo(other: KloneRequest): Int = priority - other.priority
}

data class ProcessCourseBaseRepo(val course: CourseRecord) : KloneRequest(1)
data class ProcessSubmission(val submissionData: JsonObject) : KloneRequest(2)
data class BuildSubmissionReport(val submissionData: JsonObject) : KloneRequest(3)
data class BuildCourseReport(val course: CourseRecord) : KloneRequest(4)

private val randomnesss = RandomNameGenerator()

@AutoDeployable
class KloneVerticle : AbstractKotoedVerticle(), Loggable {

    private val RESULT_TYPE = "klonecheck"

    enum class Mode {
        COURSE,
        SUBMISSION
    }

    private val ee by lazy { betterSingleThreadContext("kloneVerticle.executor") }

    private val suffixTree = SuffixTree<Token>()
    private val processed = mutableSetOf<Pair<Mode, Int>>()

    private val kloneRequests = Queues.newPriorityBlockingQueue<KloneRequest>()

    suspend fun courseSubmissionData(course: CourseRecord): List<JsonObject> {

        val projQ = ComplexDatabaseQuery("project")
                .find(ProjectRecord().apply {
                    courseId = course.id
                    deleted = false
                })
                .join("denizen", field = "denizen_id")

        val q = ComplexDatabaseQuery("submission")
                .filter(
                        "state in %s".formatToQuery(listOf(SubmissionState.open, SubmissionState.closed))
                )
                .join(projQ, field = "project_id")

        return sendJsonableCollectAsync(Address.DB.query("submission"), q)
    }

    @JsonableEventBusConsumerFor(Address.Code.KloneCheck)
    suspend fun handleCheck(course: CourseRecord) {

        val data = courseSubmissionData(course)

        with(kloneRequests) {
            offer(ProcessCourseBaseRepo(course))
            for (sub in data) offer(ProcessSubmission(sub))
            offer(BuildCourseReport(course))
        }
    }

    override suspend fun start() {
        vertx.setTimer(5000, this::handleRequest)
        super.start()
    }

    fun handleRequest(@Suppress(UNUSED_PARAMETER) timerId: Long) {

        fun handleException(ex: Throwable) {
            log.error("Klones failed!", ex)
            vertx.setTimer(5000, this::handleRequest)
        }

        val left = kloneRequests.size
        if (left % 10 == 1) {
            log.trace("$left klone requests left")
        }

        val req = kloneRequests.poll()
        when (req) {
            is ProcessCourseBaseRepo -> {
                spawn(WithExceptions(::handleException)) {
                    try {
                        if (!handleBase(req.course)) kloneRequests.offer(req)
                    } finally {
                        vertx.setTimer(100, this::handleRequest)
                    }
                }
            }
            is ProcessSubmission -> {
                spawn(WithExceptions(::handleException)) {
                    try {
                        if (!handleSub(req.submissionData)) kloneRequests.offer(req)
                    } finally {
                        vertx.setTimer(100, this::handleRequest)
                    }
                }
            }
            is BuildCourseReport -> {
                spawn(WithExceptions(::handleException)) {
                    try {
                        val data = courseSubmissionData(req.course)
                        if (!handleReport(data)) kloneRequests.offer(req)
                    } finally {
                        vertx.setTimer(100, this::handleRequest)
                    }
                }
            }
            else -> {
                vertx.setTimer(5000, this::handleRequest)
            }
        }
    }

    suspend fun handleBase(crs: CourseRecord): Boolean {

        if (Mode.COURSE to crs.id in processed) return true

        val course = when (crs.state) {
            null -> dbFetchAsync(crs)
            else -> crs
        }

        val files: Code.ListResponse = sendJsonableAsync(
                Address.Api.Course.Code.List,
                Code.Course.ListRequest(course.id)
        )

        return handleFiles(
                Mode.COURSE,
                course.id,
                -1, // Course tokens do not have an owning denizen
                files)
    }

    suspend fun handleSub(sub: JsonObject): Boolean {

        if (Mode.SUBMISSION to sub.getInteger("id") in processed) return true

        val files: Code.ListResponse = sendJsonableAsync(
                Address.Api.Submission.Code.List,
                Code.Submission.ListRequest(sub.getInteger("id"))
        )

        return handleFiles(
                Mode.SUBMISSION,
                sub.getInteger("id"),
                sub.safeNav("project", "denizen", "id") as Int,
                files)
    }

    suspend fun handleFiles(
            mode: Mode,
            id: Int,
            denizenId: Int,
            files: Code.ListResponse): Boolean {

        if (mode to id in processed) return true

        when (files.status) {
            CloneStatus.failed -> {
                log.trace("Repository cloning failed")
                return false
            }
            CloneStatus.pending -> {
                log.trace("Repository not cloned yet")
                return false
            }
            CloneStatus.done -> {
                val allFiles = files.root?.toFileSeq().orEmpty()

                processKtFiles(allFiles.filter { it.endsWith(".kt") }.toList(), mode, id, denizenId)
                processHsFiles(allFiles.filter { it.endsWith(".hs") }.toList(), mode, id, denizenId)

                processed.add(mode to id)

                return true
            }
        }
    }

    // TODO: provide some abstraction over Kotlin/Java/Haskell/XML/etc here
    private suspend fun processKtFiles(allFiles: List<String>, mode: Mode, id: Int, denizenId: Int) {
        if (allFiles.isEmpty()) return //no .kt files

        temporaryKotlinEnv {
            val ktFiles = allFiles
                    .map { filename ->
                        log.trace("filename = $filename")
                        val resp: Code.Submission.ReadResponse = when (mode) {
                            Mode.COURSE ->
                                sendJsonableAsync(Address.Api.Course.Code.Read,
                                        Code.Course.ReadRequest(
                                                courseId = id, path = filename))
                            Mode.SUBMISSION ->
                                sendJsonableAsync(Address.Api.Submission.Code.Read,
                                        Code.Submission.ReadRequest(
                                                submissionId = id, path = filename))
                        }
                        withContext(ee) { getPsi(resp.contents, filename) }
                    }

            withContext(ee) {
                ktFiles.asSequence()
                        .flatMap { file ->
                            file.collectDescendantsOfType<KtNamedFunction>().asSequence()
                        }
                        .filter { method ->
                            method.annotationEntries.all { anno -> "@Test" != anno.text }
                        }
                        .map {
                            @Suppress(USELESS_CAST)
                            it as PsiElement
                        }
                        .map { method ->
                            method to method.dfs { children.asSequence() }
                                    .filter(Token.DefaultFilter)
                                    .map((::makeAnonimizedKtToken)
                                            .bind(_0, mode)
                                            .bind(_0, id)
                                            .bind(_0, denizenId))
                        }
                        .forEach { (_, tokens) ->
                            val lst = tokens.toList()
                            log.trace("lst = ${lst.joinToString(limit = 32)}")
                            val seqId = suffixTree.addSequence(lst)
                            seqId.ignore()
                        }
            }
        }

    }

    private suspend fun processHsFiles(allFiles: List<String>, mode: Mode, id: Int, denizenId: Int) {
        if (allFiles.isEmpty()) return //no .hs files

        loop@ for (filename in allFiles) {

            fun org.antlr.v4.runtime.Token.location() =
                    org.jetbrains.research.kotoed.code.Location(
                            filename = Filename(path = filename), col = charPositionInLine, line = line
                    )

            log.trace("filename = $filename")
            val resp: Code.Submission.ReadResponse = when (mode) {
                Mode.COURSE ->
                    sendJsonableAsync(Address.Api.Course.Code.Read,
                            Code.Course.ReadRequest(
                                    courseId = id, path = filename))
                Mode.SUBMISSION ->
                    sendJsonableAsync(Address.Api.Submission.Code.Read,
                            Code.Submission.ReadRequest(
                                    submissionId = id, path = filename))
            }

            val res = withContext(ee) {
                log.trace("Lexing!")
                CommonTokenStream(HaskellLexer(CharStreams.fromString(resp.contents, filename)))
                        .apply { fill() }
                        .tokens
                        .dropLast(1)
                        .also { log.trace("Lexing finished") }
            }

            log.trace("res = ${res.map { "$it@(${it.location()})" }}")

            if (res.isEmpty()) {
                log.error("Cannot parse source: $filename")
                continue@loop
            }

            val errTok = res.find { it.type == HaskellLexer.ERROR }

            if (errTok != null) {
                log.error("Cannot parse source: $filename: unexpected token: $errTok")
                continue@loop
            }

            res.asSequence()
                    .splitBy { it.type == HaskellLexer.SPACES && it.text.contains(Regex("""\n.*\n""")) }
                    //.windowed(15) { it.flatten() }
                    .forEach { chunk ->
                        log.trace("chunk = $chunk")
                        val lst =
                                withContext(ee) {
                                    chunk
                                            .asSequence()
                                            .filter {
                                                it.type !in setOf(
                                                        HaskellLexer.SPACES,
                                                        HaskellLexer.EOF,
                                                        HaskellLexer.LEFT_CURLY,
                                                        HaskellLexer.RIGHT_CURLY,
                                                        HaskellLexer.LEFT_PAREN,
                                                        HaskellLexer.RIGHT_PAREN,
                                                        HaskellLexer.SEMICOLON
                                                )
                                            }.mapTo(mutableListOf()) {
                                                Token(mode,
                                                        id,
                                                        denizenId,
                                                        HaskellLexer.VOCABULARY.getSymbolicName(it.type),
                                                        it.location(),
                                                        it.location().run { copy(col = col + it.text.length) },
                                                        randomnesss.next()
                                                )
                                            }.apply {
                                                if (isNotEmpty()) {
                                                    add(0, first().copy(text = "\$BEGIN\$"))
                                                    add(last().copy(text = "\$END\$"))
                                                }
                                            }
                                }
                        log.trace("lst = $lst")
                        if (lst.isNotEmpty()) withContext(ee) { suffixTree.addSequence(lst.toList()) }
                    }
        }
    }

    suspend fun handleReport(data: List<JsonObject>): Boolean {

        log.trace("Handling report...")

        val dataBySubmissionId = data.map { it.getInteger("id") to it }.toMap()

        val clones =
                suffixTree.root.dfs {
                    edges
                            .asSequence()
                            .mapNotNull { it.terminal }
                }.filter { node ->
                    node.edges
                            .asSequence()
                            .all { it.begin == it.end && it.begin == it.sequence.size - 1 }
                }.filter { node ->
                    0 == node.parentEdges.lastOrNull()?.begin
                }

        log.trace(clones.joinToString(separator = "\n", limit = 32))

        val filtered = clones
                .map(::CloneClass)
                .filter { cc -> cc.clones.isNotEmpty() }
                .filter { cc -> Mode.COURSE !in cc.clones.map { it.type } }
                .filter { cc -> cc.clones.map { it.submissionId }.toSet().size != 1 }
                .toList()

        filtered.forEachIndexed { i, cloneClass ->
            val builder = StringBuilder()
            val fname = cloneClass.clones
                    .map { clone -> clone.functionName }
                    .distinct()
                    .joinToString()
            builder.appendLine("($fname) Clone class $i:")
            cloneClass.clones.forEach { c ->
                builder.appendLine("${c.submissionId}/${c.functionName}/${c.file.path}:${c.fromLine}:${c.toLine}")
            }
            builder.appendLine()
            log.trace(builder)
        }

        // TODO: Better filtering of the same person clones
        val clonesBySubmission = filtered
                .flatMap { cloneClass ->
                    cloneClass.clones.map { clone -> clone.submissionId to clone.denizenId to cloneClass }
                }
                .groupBy { it.first }
                .mapValues { (key, value) ->
                    value.map { e ->
                        e.second.clones.filter { clone ->
                            key == clone.submissionId to clone.denizenId ||
                                    key.second != clone.denizenId
                        }
                    }.filterNot { it.size <= 1 }
                }
                .filterNot { it.value.isEmpty() }

        data class CloneInfo(
                val submissionId: Int,
                val denizen: Any?,
                val project: Any?,
                val file: Filename,
                val fromLine: Int,
                val toLine: Int,
                val functionName: String
        ) : Jsonable

        clonesBySubmission.asSequence()
                .forEach { (cloneId, cloneClasses) ->
                    dbCreateAsync(SubmissionResultRecord().apply {
                        submissionId = cloneId.first
                        type = RESULT_TYPE
                        body = cloneClasses
                                .sortedBy { with(it.firstOrNull()) { this?.file?.path.orEmpty() + this?.fromLine } }
                                .map { cloneClass ->
                                    cloneClass.map { clone ->
                                        CloneInfo(
                                                submissionId = clone.submissionId,
                                                denizen = dataBySubmissionId[clone.submissionId].safeNav("project", "denizen", "denizen_id"),
                                                project = dataBySubmissionId[clone.submissionId].safeNav("project", "name"),
                                                file = clone.file,
                                                fromLine = clone.fromLine,
                                                toLine = clone.toLine,
                                                functionName = clone.functionName
                                        )
                                    }
                                }
                    })
                }

        return true

    }
}
