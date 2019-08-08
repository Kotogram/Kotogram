package org.jetbrains.research.kotoed.web.eventbus.filters

import io.vertx.ext.bridge.BridgeEventType
import io.vertx.ext.web.handler.sockjs.BridgeEvent

class ByType(val type: BridgeEventType) : BridgeEventFilter {
    override suspend fun isAllowed(be: BridgeEvent): Boolean = (be.type() == type).also { logResult(be, it) }

    override fun toString(): String {
        return "ByType(type=$type)"
    }
}
