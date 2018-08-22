package org.jetbrains.research.kotoed.code.vcs

import org.jetbrains.research.kotoed.util.Loggable
import org.jetbrains.research.kotoed.util.allLines
import org.jetbrains.research.kotoed.util.futureDone
import org.jetbrains.research.kotoed.util.futureExitCode
import java.io.File
import java.util.concurrent.Future

data class CommandLine(val args: List<String>) : Loggable {
    constructor(vararg vargs: String) : this(vargs.asList())

    data class Output(val rcode: Future<Int>, val cout: Sequence<String>, val cerr: Sequence<String>) {
        fun complete(): Output {
            val memoOut = cout.toList().asSequence()
            val memoErr = cerr.toList().asSequence()
            val exitCode = rcode.get()
            return Output(futureDone(exitCode), memoOut, memoErr)
        }

        override fun toString(): String {
            return "Output(rcode=$rcode, cout=${cout.toList()}, cerr=${cerr.toList()})"
        }

    }

    fun execute(wd: File = File(System.getProperty("user.dir")),
                env: Map<String, String> = mapOf(),
                input: Sequence<String> = sequenceOf()): Output {

        log.info("Running: " + args.joinToString(" "))

        val pb = ProcessBuilder(args).apply {
            environment() += env
        }.directory(wd).start()

        val cin = pb.outputStream.writer()
        for (l in input) cin.append(l).appendln()
        cin.close()

        val cout = pb.inputStream.bufferedReader().allLines()
        val cerr = pb.errorStream.bufferedReader().allLines()

        return Output(pb.futureExitCode, cout, cerr)
    }

}
