@file:Suppress("UNCHECKED_CAST")

package org.jetbrains.research.kotoed.util

import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.eventbus.*
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import kotlinx.coroutines.experimental.launch
import org.jetbrains.research.kotoed.data.api.VerificationData
import org.jetbrains.research.kotoed.eventbus.Address
import org.jetbrains.research.kotoed.util.database.toJson
import org.jetbrains.research.kotoed.util.database.toRecord
import org.jooq.Record
import org.jooq.Table
import org.jooq.UpdatableRecord
import java.util.*
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.full.declaredMemberFunctions
import kotlin.reflect.full.isSubclassOf
import kotlin.reflect.full.starProjectedType
import kotlin.reflect.jvm.jvmErasure

suspend fun <ReturnType> EventBus.sendAsync(address: String, message: Any): Message<ReturnType> =
        vxa { send(address, message, it) }

@JvmName("sendJsonAsync")
suspend fun EventBus.sendAsync(address: String, message: Any): Message<JsonObject> =
        sendAsync<JsonObject>(address, message)

@JvmName("trySendJsonAsync")
suspend fun EventBus.trySendAsync(address: String, message: Any): Message<JsonObject>? =
        try{ sendAsync<JsonObject>(address, message) }
        catch (ex: ReplyException) { null }

@Deprecated("Forgot to call .toJson()?",
        level = DeprecationLevel.ERROR,
        replaceWith = ReplaceWith("sendAsync(address, message.toJson())"))
@Suppress("UNUSED_PARAMETER")
suspend fun EventBus.sendAsync(address: String, message: Jsonable): Unit = Unit

@Deprecated("Forgot to call .toJson()?",
        level = DeprecationLevel.ERROR,
        replaceWith = ReplaceWith("reply(message.toJson())"))
@Suppress("UNUSED_PARAMETER")
suspend fun <T> Message<T>.reply(message: Jsonable): Unit = Unit

fun EventBus.sendJsonable(address: String, message: Jsonable) =
        send(address, message.toJson())

@Target(AnnotationTarget.FUNCTION)
annotation class EventBusConsumerFor(val address: String)

@Target(AnnotationTarget.FUNCTION)
annotation class JsonableEventBusConsumerFor(val address: String)

private fun getToJsonConverter(type: KType): (value: Any) -> Any {
    val klazz = type.jvmErasure
    return when {
        klazz == JsonObject::class -> {
            { it.expectingIs<JsonObject>() }
        }
        klazz.isSubclassOf(Jsonable::class) -> {
            { it.expectingIs<Jsonable>().toJson() }
        }
        klazz.isSubclassOf(Record::class) -> {
            { it.expectingIs<Record>().toJson() }
        }
        klazz == Unit::class -> {
            { JsonObject() }
        }

    // collections
        klazz == JsonArray::class -> {
            { it }
        }
        klazz.isSubclassOf(Collection::class) -> {
            val elementMapper = getToJsonConverter(type.arguments.first().type!!);
            {
                (it as Collection<*>)
                        .asSequence()
                        .filterNotNull()
                        .map(elementMapper)
                        .toList()
                        .let(::JsonArray)
            }
        }

        else -> throw IllegalArgumentException("Non-jsonable class: $klazz")
    }
}

private fun getFromJsonConverter(type: KType): (value: Any) -> Any {
    val klazz = type.jvmErasure
    return when {
        klazz == JsonObject::class -> {
            { it.expectingIs<JsonObject>() }
        }
        klazz.isSubclassOf(Jsonable::class) -> {
            { fromJson(it.expectingIs<JsonObject>(), klazz) }
        }
        klazz.isSubclassOf(Record::class) -> {
            { it.expectingIs<JsonObject>().toRecord(klazz as KClass<out Record>) }
        }
        klazz == Unit::class -> {
            {}
        }

    // collections
        klazz == JsonArray::class -> {
            { it.expectingIs<JsonArray>() }
        }
        klazz.isSubclassOf(List::class) -> {
            val elementMapper = getFromJsonConverter(type.arguments.first().type!!);
            { it.expectingIs<JsonArray>().map(elementMapper) }
        }
        klazz.isSubclassOf(Set::class) -> {
            val elementMapper = getFromJsonConverter(type.arguments.first().type!!);
            { it.expectingIs<JsonArray>().map(elementMapper).toSet() }
        }

        else -> throw IllegalArgumentException("Non-jsonable class: $klazz")
    }
}

fun AbstractVerticle.registerAllConsumers() {
    val klass = this::class

    val eb = vertx.eventBus()

    for (function in klass.declaredMemberFunctions) {
        for (annotation in function.annotations) {
            when (annotation) {
                is EventBusConsumerFor ->
                    if (function.isSuspend) {
                        eb.consumer<JsonObject>(annotation.address) { msg ->
                            launch(UnconfinedWithExceptions(msg)) {
                                function.callAsync(this@registerAllConsumers, msg)
                            }
                        }
                    } else {
                        eb.consumer<JsonObject>(annotation.address) { msg ->
                            DelegateLoggable(klass.java).withExceptions(msg) {
                                function.call(this, msg)
                            }
                        }
                    }
                is JsonableEventBusConsumerFor -> {
                    // first parameter is the receiver, we need the second one
                    val parameterType = function.parameters[1].type
                    val resultType = function.returnType
                    val toJson = getToJsonConverter(resultType)
                    val fromJson = getFromJsonConverter(parameterType)

                    if (function.isSuspend) {
                        eb.consumer<JsonObject>(annotation.address) { msg ->
                            launch(UnconfinedWithExceptions(msg)) {
                                val argument = fromJson(msg.body())
                                val res = expectNotNull(function.callAsync(this@registerAllConsumers, argument))
                                msg.reply(toJson(res))
                            }
                        }
                    } else {
                        eb.consumer<JsonObject>(annotation.address) { msg ->
                            DelegateLoggable(klass.java).withExceptions(msg) {
                                val argument = fromJson(msg.body())
                                val res = expectNotNull(function.call(this@registerAllConsumers, argument))
                                msg.reply(toJson(res))
                            }
                        }
                    }
                }
            }
        }
    }
}

object DebugInterceptor: Handler<SendContext<*>>, Loggable {
    override fun handle(event: SendContext<*>) {
        val message = event.message()
        log.trace("Message to ${message.address()}[${message.replyAddress() ?: ""}]")
        event.next()
    }
}

open class AbstractKotoedVerticle : AbstractVerticle() {
    override fun start(startFuture: Future<Void>) {
        registerAllConsumers()
        super.start(startFuture)
    }

    @PublishedApi
    @Deprecated("Do not call directly")
    internal suspend fun <Argument : Any, Result : Any> sendJsonableAsync(
            address: String,
            value: Argument,
            argClass: KClass<out Argument>,
            resultClass: KClass<out Result>
    ): Result {
        val toJson = getToJsonConverter(argClass.starProjectedType)
        val fromJson = getFromJsonConverter(resultClass.starProjectedType)
        return vertx.eventBus().sendAsync(address, toJson(value)).body().let(fromJson) as Result
    }

    @PublishedApi
    @Deprecated("Do not call directly")
    internal suspend fun <Argument : Any, Result : Any> sendJsonableCollectAsync(
            address: String,
            value: Argument,
            argClass: KClass<out Argument>,
            resultClass: KClass<out Result>
    ): List<Result> {
        val toJson = getToJsonConverter(argClass.starProjectedType)
        val fromJson = getFromJsonConverter(resultClass.starProjectedType)
        return vertx
                .eventBus()
                .sendAsync<JsonArray>(address, toJson(value))
                .body()
                .asSequence()
                .filterIsInstance<JsonObject>()
                .map(fromJson)
                .map { it as Result }
                .toList()
    }

    // all this debauchery is here due to a kotlin compiler bug:
    // https://youtrack.jetbrains.com/issue/KT-17640
    protected suspend fun <R : UpdatableRecord<R>> dbUpdateAsync(v: R, klass: KClass<out R> = v::class): R =
            sendJsonableAsync(Address.DB.update(v.table.name), v, klass, klass)

    protected suspend fun <R : UpdatableRecord<R>> dbCreateAsync(v: R, klass: KClass<out R> = v::class): R =
            sendJsonableAsync(Address.DB.create(v.table.name), v, klass, klass)

    protected suspend fun <R : UpdatableRecord<R>> dbFetchAsync(v: R, klass: KClass<out R> = v::class): R =
            sendJsonableAsync(Address.DB.read(v.table.name), v, klass, klass)

    protected suspend fun <R : UpdatableRecord<R>> dbFindAsync(v: R, klass: KClass<out R> = v::class): List<R> =
            sendJsonableCollectAsync(Address.DB.find(v.table.name), v, klass, klass)

    protected suspend fun <R : UpdatableRecord<R>> dbProcessAsync(v: R, klass: KClass<out R> = v::class): VerificationData =
            sendJsonableAsync(Address.DB.process(v.table.name), v, klass, VerificationData::class)

    protected suspend fun <R : UpdatableRecord<R>> fetchByIdAsync(instance: Table<R>, id: Int,
                                                                  klass: KClass<out R> = instance.recordType.kotlin): R =
            sendJsonableAsync(Address.DB.read(instance.name), JsonObject("id" to id), JsonObject::class, klass)
}

inline suspend fun <
        reified Result : Any,
        reified Argument : Any
        > AbstractKotoedVerticle.sendJsonableAsync(address: String, value: Argument): Result {
    @Suppress("DEPRECATION")
    return sendJsonableAsync(address, value, Argument::class, Result::class)
}

inline suspend fun <
        reified Result : Any,
        reified Argument : Any
        > AbstractKotoedVerticle.trySendJsonableAsync(address: String, value: Argument): Result? {
    return try { sendJsonableAsync(address, value) }
    catch (ex: ReplyException) {
        if(ex.failureType() == ReplyFailure.NO_HANDLERS) null
        else throw ex
    }
}