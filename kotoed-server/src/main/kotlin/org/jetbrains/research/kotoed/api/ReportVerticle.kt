package org.jetbrains.research.kotoed.api

import io.vertx.core.json.JsonObject
import kotlinx.coroutines.withContext
import org.jetbrains.research.kotoed.data.buildSystem.KotoedRunnerStatus
import org.jetbrains.research.kotoed.data.buildSystem.KotoedRunnerTestMethodRun
import org.jetbrains.research.kotoed.data.buildSystem.KotoedRunnerTestRun
import org.jetbrains.research.kotoed.data.statistics.ReportResponse
import org.jetbrains.research.kotoed.database.Tables
import org.jetbrains.research.kotoed.database.enums.SubmissionState
import org.jetbrains.research.kotoed.database.tables.records.DenizenRecord
import org.jetbrains.research.kotoed.database.tables.records.SubmissionRecord
import org.jetbrains.research.kotoed.database.tables.records.SubmissionResultRecord
import org.jetbrains.research.kotoed.eventbus.Address
import org.jetbrains.research.kotoed.util.*
import org.jetbrains.research.kotoed.util.database.toRecord
import java.time.OffsetDateTime
import java.util.*
import kotlin.math.max

data class ReportRequest(val id: Int, val timestamp: OffsetDateTime?) : Jsonable

@AutoDeployable
class ReportVerticle : AbstractKotoedVerticle() {

    private val reportPool = betterFixedThreadPoolContext(4, "report-pool")

    private val template = "results\\.json".toRegex()
    private val successfulStatus = KotoedRunnerStatus.SUCCESSFUL

    private val ignoredTags = listOf("Example")

    private val List<Int>.grade: Int
        get() = when {
            this.isEmpty() -> 1 // default grade
            this.size == 1 -> this.single()
            else -> throw Exception("Two or more grade tags")
        }

    private val Iterable<String>.onlyNumbers: List<Int>
        get() = this.mapNotNull { it.toIntOrNull() }.distinct()

    companion object {
        val Double.fmt get() = String.format(Locale.ROOT, if (this % 1 == 0.0) "%.0f" else "%.2f", this)
        fun Double?.orZero() = if (this?.isNaN() == false) this else 0.0
    }

    private fun extractLessonsFromTestResults(content: KotoedRunnerTestRun): Map<String, Map<String, List<KotoedRunnerTestMethodRun>>> {
        return content.data
                .groupBy { it.packageName.split('.').first() }
                .mapValues { (_, results) ->
                    results.groupBy { it.methodName }
                }
    }

    private val TAKE_N_HIGHEST_GRADE_TASKS = 2
    private val TAKE_N_HIGHEST_GRADE_LESSONS = 5

    fun calcHighestGradeLessons(lessonGrades: List<Pair<String, Int>>) =
            lessonGrades.sortedByDescending { it.second }.filter { it.second > 0.0 }.take(TAKE_N_HIGHEST_GRADE_LESSONS)

    fun calcHighestGradeTasks(tasks: Map<String, List<KotoedRunnerTestMethodRun>>): List<Pair<String, Int>> {
        val solvableTasks = tasks.filter { it.value.flatMap { it.tags }.intersect(ignoredTags).isEmpty() }
        val successfulTasks = solvableTasks.filterValues { it.flatMap { it.results }.all { it.status == successfulStatus } }
        val taskGrades = successfulTasks.mapValues { it.value.flatMap { it.tags }.onlyNumbers.grade }
        return taskGrades.toList().sortedByDescending { it.second }.take(TAKE_N_HIGHEST_GRADE_TASKS)
    }

    private fun calcScore(sr: SubmissionResultRecord): Double {
        if (template !in sr.type) return 0.0

        val content: KotoedRunnerTestRun = (sr.body as JsonObject).snakeKeys().toJsonable()

        val groupedLessonData = extractLessonsFromTestResults(content)

        val lessonsGrade = groupedLessonData.map { (lesson, tasks) ->
            val highestGrades = calcHighestGradeTasks(tasks)
            lesson to highestGrades.sumOf { it.second }
        }

        val highestGradesLessons = calcHighestGradeLessons(lessonsGrade)
        val totalScore = highestGradesLessons.sumOf { it.second }

        return totalScore.toDouble() // TODO: Change type to Int everywhere
    }

    private fun calcAllScores(sr: SubmissionResultRecord): List<List<String>> {
        if (template !in sr.type) return listOf()

        val scoreDescriptions: MutableMap<String, String> = mutableMapOf()
        val scores: MutableMap<String, Int> = mutableMapOf()

        val content: KotoedRunnerTestRun = (sr.body as JsonObject).snakeKeys().toJsonable()

        val groupedLessonData = extractLessonsFromTestResults(content)

        groupedLessonData.forEach { (lesson, tasks) ->
            val highestGrades = calcHighestGradeTasks(tasks)
            scores[lesson] = highestGrades.sumOf { it.second }
            scoreDescriptions[lesson] = highestGrades.joinToString(separator = " + ") { "${it.second} points for ${it.first}" }
        }

        val highestGradesLessons = calcHighestGradeLessons(scores.toList())
        val totalScore = highestGradesLessons.sumOf { it.second }
        val totalScoreDescription = highestGradesLessons.joinToString(separator = " + ") { it.first }

        val header = listOf(listOf("", "Score", "Description"))
        val lessons = groupedLessonData.keys
        val data = lessons.sorted().mapIndexed { _, lesson ->
            listOf(lesson, scores[lesson]?.toString() ?: "", scoreDescriptions[lesson] ?: "")
        }
        val footer = listOf(listOf("Total", totalScore.toString(), totalScoreDescription))

        return header + data + footer
    }

    private data class SubmissionStatus(val score: Double, val state: SubmissionState, val tags: Set<String>)

    private suspend fun makeReport(request: ReportRequest, subStates: List<String>): Map<String, List<SubmissionStatus>> {
        val date = request.timestamp ?: OffsetDateTime.now()

        return dbFindAsync(DenizenRecord()).map { denizen ->
            async(reportPool) iter@{
                val results = dbQueryAsync(Tables.SUBMISSION_RESULT) {
                    join(Tables.SUBMISSION) {
                        join(Tables.PROJECT) {
                            join(Tables.DENIZEN, field = "denizen_id") {
                                find { id = denizen.id }
                            }
                        }

                        rjoin(Tables.SUBMISSION_TAG, resultField = "tags") {
                            join(Tables.TAG)
                        }
                    }
                    filter("(submission.project.course_id == ${request.id}) and " +
                            "(" + subStates.map { "submission.state == \"$it\"" }.joinToString(" or ") + ") and " +
                            "type == \"results.json\"")
                }.map { it to (it.safeNav("submission") as JsonObject).toRecord<SubmissionRecord>() }
                        .sortedByDescending { (_, record) -> record.datetime }
                        .dropWhile { (_, record) -> record.datetime > date }
                        .groupBy { (_, record) -> record.state }
                        .mapNotNull { (_, list) -> list.firstOrNull() } // Take only the latest submissions of each type

                denizen.denizenId to results.map { (result, record) ->
                    val score = result.toRecord<SubmissionResultRecord>().let(this::calcScore)
                    val state = record.state
                    val tags = result
                            .getJsonObject("submission")
                            ?.getJsonArray("tags")
                            ?.filterIsInstance<JsonObject>()
                            ?.mapNotNullTo(mutableSetOf()) { it.safeNav("tag", "name")?.toString() }
                            ?: setOf<String>()
                    SubmissionStatus(score, state, tags)
                }
            }
        }
                .map { it.await() }
                .toMap()
    }

    private data class Adjustment(val value: Double?, val comment: String? = null) {
        fun toDouble() = value ?: defaultPenalty
        fun isSet() = value != null

        companion object {
            const val defaultPenalty: Double = 0.0

            fun fromSubmissionStatus(submissionStatus: SubmissionStatus): Adjustment = when (submissionStatus.state) {
                // Ignore tags in closed submissions
                // Permanent penalties are handled separately
                SubmissionState.closed -> Adjustment(0.0)
                else -> fromTags(submissionStatus.tags)
            }

            fun fromTags(tags: Set<String>): Adjustment {
                val adjustmentTags = tags.mapNotNull { it.toIntOrNull()?.toDouble() }
                return when {
                    adjustmentTags.size > 1 -> Adjustment(null, "Multiple tags")
                    adjustmentTags.size == 1 -> Adjustment(adjustmentTags.singleOrNull())
                    tags.contains("checked") -> Adjustment(0.0)
                    tags.contains("check me") -> Adjustment(null, "Not checked")
                    else -> Adjustment(null, "No suitable tags found")
                }
            }
        }
    }

    private data class Score(val student: String,
                             val open: Double?, val closed: Double?,
                             val adjustment: Adjustment, val permanentAdjustment: Adjustment,
                             val total: Double = max(
                                     open.orZero() + adjustment.toDouble(),
                                     closed.orZero()
                             ) + permanentAdjustment.toDouble(),
                             val hasBeenChecked: Boolean = adjustment.isSet() || (closed != null && open == null),
                             val totalScoreSource: SubmissionStatus,
                             val comment: String = adjustment.comment ?: "") {
        companion object {
            fun fromRecords(denizen: String,
                            submissions: List<SubmissionStatus>): Score? {
                if (submissions.isEmpty()) {
                    return null // Student have not participated in a course (yet)
                }
                val lastOpen = submissions.firstOrNull { it.state == SubmissionState.open }
                val lastObsolete = submissions.firstOrNull { it.state == SubmissionState.obsolete }
                val lastClosed = submissions.firstOrNull { it.state == SubmissionState.closed }
                val lastCorrect = if (lastObsolete != null && lastOpen == null &&
                        lastObsolete.score > (lastClosed?.score ?: 0.0)) {
                    lastObsolete
                } else {
                    lastOpen
                }
                val permanentAdjs = submissions.asSequence()
                        .filter { it.state == SubmissionState.closed }
                        .map { it.tags }
                        .filter { it.contains("permanent") }
                        .map { Adjustment.fromTags(it) }
                        .filter { it.isSet() }
                val permanentAdj = Adjustment(
                        value = permanentAdjs.sumOf { it.value ?: 0.0 },
                        comment = permanentAdjs.mapNotNull { it.comment }.joinToString()
                )
                val totalScoreSource = sequenceOf(lastCorrect, lastClosed)
                        .filterNotNull()
                        .associateWith { Adjustment.fromSubmissionStatus(it) }
                        .maxByOrNull { (status, adj) -> status.score + adj.toDouble() } ?: return null
                return Score(
                        student = denizen,
                        open = lastCorrect?.score,
                        adjustment = totalScoreSource.value,
                        permanentAdjustment = permanentAdj,
                        closed = lastClosed?.score,
                        totalScoreSource = totalScoreSource.key
                )
            }
        }
    }

    @JsonableEventBusConsumerFor(Address.Api.Course.Report)
    suspend fun handleReport(request: ReportRequest): ReportResponse = withContext(reportPool) {
        val submissions = makeReport(request, listOf("open", "obsolete", "closed"))

        val scores = submissions.mapNotNull { (denizen, submission) ->
            Score.fromRecords(denizen, submission)
        }.sortedWith(compareByDescending<Score> { it.total }.thenBy { it.student })
        val header = listOf(
                listOf("Student", "Score (open)", "Adjustment", "Score (closed)", "Permanent Adj.", "Total", "Source", "Comment")
        )
        val table = scores.map { score ->
            listOf(
                    score.student,
                    score.open.orZero().fmt,
                    score.adjustment.toDouble().fmt,
                    score.closed.orZero().fmt,
                    score.permanentAdjustment.toDouble().fmt,
                    score.total.fmt,
                    score.totalScoreSource.state.literal.capitalize(),
                    score.comment
            )
        }
        val result = header + table

        ReportResponse(result)
    }

    @JsonableEventBusConsumerFor(Address.Api.Submission.Report)
    suspend fun handleSubmission(record: ReportRequest): ReportResponse = withContext(reportPool) {
        val result = dbFindAsync(SubmissionResultRecord().apply { submissionId = record.id }).firstOrNull { template in it.type }

        result ?: return@withContext ReportResponse(listOf())

        ReportResponse(calcAllScores(result))
    }

    @JsonableEventBusConsumerFor(Address.Api.Submission.Result.Report)
    suspend fun handleSubmissionResult(record: ReportRequest): ReportResponse = withContext(reportPool) {
        val result = dbFindAsync(SubmissionResultRecord().apply { id = record.id }).firstOrNull { template in it.type }

        result ?: return@withContext ReportResponse(listOf())

        ReportResponse(calcAllScores(result))
    }

}
