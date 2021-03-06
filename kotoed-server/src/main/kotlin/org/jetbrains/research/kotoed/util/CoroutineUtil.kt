@file:Suppress(kotlinx.warnings.Warnings.NOTHING_TO_INLINE, "ObsoleteExperimentalCoroutines")

package org.jetbrains.research.kotoed.util

import com.google.common.util.concurrent.ThreadFactoryBuilder
import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import io.vertx.core.eventbus.Message
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.*
import java.lang.reflect.Method
import java.util.concurrent.Executors
import kotlin.coroutines.*
import kotlin.coroutines.intrinsics.suspendCoroutineUninterceptedOrReturn
import kotlin.coroutines.intrinsics.intercepted
import kotlin.reflect.KFunction

/******************************************************************************/

suspend fun currentCoroutineName() =
        coroutineContext[CoroutineName.Key] ?: CoroutineName(newRequestUUID())

/******************************************************************************/

inline suspend fun vxu(crossinline cb: (Handler<AsyncResult<Void?>>) -> Unit): Void? =
        suspendCoroutine { cont ->
            cb(Handler { res ->
                if (res.succeeded()) cont.resume(res.result())
                else cont.resumeWithException(res.cause())
            })
        }

inline suspend fun <T> vxt(crossinline cb: (Handler<T>) -> Unit): T =
        suspendCoroutine { cont ->
            cb(Handler { res -> cont.resume(res) })
        }

inline suspend fun <T> vxa(crossinline cb: (Handler<AsyncResult<T>>) -> Unit): T =
        suspendCoroutine { cont ->
            cb(Handler { res ->
                if (res.succeeded()) cont.resume(res.result())
                else cont.resumeWithException(res.cause())
            })
        }

/******************************************************************************/

class WithExceptionsContext(val handler: (Throwable) -> Unit) :
        AbstractCoroutineContextElement(CoroutineExceptionHandler.Key),
        CoroutineExceptionHandler,
        Loggable {
    override fun handleException(context: CoroutineContext, exception: Throwable) =
            handler(exception)
}

fun Loggable.LogExceptions() = WithExceptionsContext(
        { log.error("Oops!", it) }
)

fun Loggable.WithExceptions(handler: (Throwable) -> Unit) = WithExceptionsContext(
        { handler(it) }
)

fun <U> Loggable.WithExceptions(handler: Handler<AsyncResult<U>>) = WithExceptionsContext(
        { handleException(handler, it) }
)

fun <U> Loggable.WithExceptions(msg: Message<U>) = WithExceptionsContext(
        { handleException(msg, it) }
)

fun Loggable.WithExceptions(ctx: RoutingContext) = WithExceptionsContext(
        { handleException(ctx, it) }
)

// NOTE: suspendCoroutineOrReturn<> is not recommended by kotlin devs, BUT,
// however, suspendCoroutine<>, the only alternative, does *not* work correctly if suspend fun has no
// suspension points.

suspend inline fun <R> KFunction<R>.callAsync(vararg args: Any?) =
        when {
            isSuspend -> suspendCoroutineUninterceptedOrReturn<R> { call(*args, it.intercepted()) }
            else -> throw Error("$this cannot be called as async")
        }

val Method.isKotlinSuspend
    get() = parameters.lastOrNull()?.type == Continuation::class.java

suspend inline fun Method.invokeAsync(receiver: Any?, vararg args: Any?) =
        when {
            isKotlinSuspend -> suspendCoroutineUninterceptedOrReturn<Any?> { invoke(receiver, *args, it.intercepted()) }
            else -> throw Error("$this cannot be invoked as async")
        }

/******************************************************************************/

class CoroLazy<T>(val generator: suspend () -> T) {
    private var backer: T? = null

    fun isInitialized() = backer != null
    suspend fun get(): T {
        if (!isInitialized()) backer = generator()
        return backer!!
    }
}

fun <T> coroLazy(generator: suspend () -> T) = CoroLazy(generator)

/******************************************************************************/

fun betterFixedThreadPoolContext(nThreads: Int, name: String): ExecutorCoroutineDispatcher =
        Executors.newFixedThreadPool(
                nThreads,
                ThreadFactoryBuilder().setNameFormat("$name-%d").build()
        ).asCoroutineDispatcher()

fun betterSingleThreadContext(name: String): ExecutorCoroutineDispatcher =
        Executors.newSingleThreadExecutor(
                ThreadFactoryBuilder().setNameFormat(name).build()
        ).asCoroutineDispatcher()

/******************************************************************************/

fun RoutingContext.launch(
        context: CoroutineContext = vertx().dispatcher(),
        block: suspend CoroutineScope.() -> Unit) {
    CoroutineScope(context).launch(block = block)
}
