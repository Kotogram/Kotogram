package org.jetbrains.research.kotoed.db

import io.vertx.core.Promise
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.jetbrains.research.kotoed.database.Tables
import org.jetbrains.research.kotoed.database.tables.WebSession
import org.jetbrains.research.kotoed.database.tables.records.WebSessionRecord
import org.jetbrains.research.kotoed.util.AutoDeployable
import org.jetbrains.research.kotoed.util.database.into
import org.jetbrains.research.kotoed.util.*

@AutoDeployable
class WebSessionVerticle : CrudDatabaseVerticle<WebSessionRecord>(Tables.WEB_SESSION) {
    private var timerId: Long = -1

    override fun start(startPromise: Promise<Void>) {
        super.start(startPromise)
        timerId = vertx.setPeriodic(1000L * 60 * 20) { handleTick() }
    }

    override fun stop(stopPromise: Promise<Void>?) {
        vertx.cancelTimer(timerId)
        super.stop(stopPromise)
    }

    fun handleTick() {
        with(table as WebSession) {
            spawn {
                db {
                    sqlStateAware {
                        log.trace("WebSessionVerticle::tick")
                        val now = System.currentTimeMillis()
                        deleteFrom(table).where((LAST_ACCESSED + TIMEOUT).gt(now))
                    }
                }
            }
        }
    }

    // For now, this is the only way to guarantee that read-after-write does not behave
    // differently from what vert.x-web expects it to do
    val mutexes = mutableMapOf<String, Mutex>()
    private fun mutex(message: WebSessionRecord) = mutexes.getOrPut(message.id) { Mutex() }

    override suspend fun handleRead(message: WebSessionRecord): WebSessionRecord = mutex(message).withLock {
        super.handleRead(message)
    }

    override suspend fun handleUpdate(message: WebSessionRecord): WebSessionRecord = mutex(message).withLock {
        //log.trace("Update requested in table ${table.name}:\n$message")
        doCreateOrUpdate(message)
    }

    suspend override fun handleCreate(message: WebSessionRecord): WebSessionRecord = mutex(message).withLock {
        //log.trace("Create requested in table ${table.name}:\n$message")
        doCreateOrUpdate(message)
    }

    override suspend fun handleDelete(message: WebSessionRecord): WebSessionRecord = mutex(message).withLock {
        super.handleDelete(message)
    }

    suspend fun doCreateOrUpdate(message: WebSessionRecord): WebSessionRecord {
        val table = WebSession.WEB_SESSION
        return db {
            sqlStateAware {
                withTransaction {
                    val prev = selectFrom(table)
                            .where(table.ID.eq(message.id))
                            .forUpdate()
                            .noWait()
                            .fetch()
                            .into(recordClass)
                            .firstOrNull()
                    processVersions(prev, message)

                    insertInto(table)
                            .set(message)
                            .onConflict(WebSession.WEB_SESSION.ID)
                            .doUpdate()
                            .set(message)
                            .returning()
                            .fetch()
                            .into(recordClass)
                            .first()
                }


            }
        }
    }

    private fun processVersions(
        prev: WebSessionRecord?,
        message: WebSessionRecord
    ) {
        require(prev == null || prev.version == message.version) { "Conflict" }

        // Negative version means we should bump it
        if (message.version < 0) {
            message.version = -message.version + 1
        }
    }

    suspend override fun handleBatchCreate(message: List<WebSessionRecord>): List<WebSessionRecord> =
            throw UnsupportedOperationException()
}
