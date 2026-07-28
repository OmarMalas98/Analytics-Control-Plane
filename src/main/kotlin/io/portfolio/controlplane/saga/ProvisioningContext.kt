package io.portfolio.controlplane.saga

import java.util.concurrent.ConcurrentHashMap

/**
 * The shared scratchpad a pipeline's steps pass values through.
 *
 * <p>Steps deliberately do not hold references to each other. A step reads what it needs from the
 * context and writes what it produced back, which is what lets pipelines be reassembled from the
 * same steps in a different order without touching any of them.
 *
 * <p>It also carries the *record of what happened*, which rollback depends on: a step that created
 * something must leave behind enough for its own rollback to find and remove it.
 */
class ProvisioningContext(
    val subject: String,
    initial: Map<String, Any?> = emptyMap(),
) {

    private val values = ConcurrentHashMap<String, Any>()
    private val trail = mutableListOf<String>()

    init {
        initial.forEach { (key, value) -> if (value != null) values[key] = value }
    }

    operator fun set(key: String, value: Any) {
        values[key] = value
    }

    operator fun get(key: String): Any? = values[key]

    fun string(key: String): String? = values[key] as? String

    fun require(key: String): Any =
        values[key] ?: error("Step precondition not met: '$key' is missing from the context")

    fun requireString(key: String): String =
        string(key) ?: error("Step precondition not met: '$key' is missing from the context")

    fun has(key: String): Boolean = values.containsKey(key)

    fun remove(key: String) {
        values.remove(key)
    }

    /** Appends a human-readable line to the audit trail. */
    fun note(entry: String) {
        synchronized(trail) { trail += entry }
    }

    fun trail(): List<String> = synchronized(trail) { trail.toList() }

    fun snapshot(): Map<String, Any> = values.toMap()
}
