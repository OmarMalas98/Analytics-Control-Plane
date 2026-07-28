package io.portfolio.controlplane.backend

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap

/**
 * A stand-in analytics backend that behaves like the real thing in the ways that matter.
 *
 * <p>Specifically, it enforces the dependency rules — attaching a component template that does not
 * exist is an error, as is attaching to a missing index template. A permissive fake would let a
 * badly ordered pipeline pass here and fail in production, which is the opposite of useful.
 *
 * <p>[failNext] injects a fault so rollback is demonstrable on demand. Being able to *watch* the
 * unwind is worth more than reading an explanation of it.
 */
@Component
class InMemorySearchBackend : SearchBackend {

    private val log = LoggerFactory.getLogger(InMemorySearchBackend::class.java)

    private val pipelines = ConcurrentHashMap<String, String>()
    private val componentTemplates = ConcurrentHashMap<String, String>()
    private val indexTemplates = ConcurrentHashMap<String, String>()
    private val attachments = ConcurrentHashMap<String, MutableSet<String>>()
    private val refreshedPatterns = ConcurrentHashMap.newKeySet<String>()

    /** Operation name that should throw the next time it is called, if any. */
    @Volatile
    private var failOn: String? = null

    fun failNext(operation: String?) {
        failOn = operation
        log.warn("Fault injection armed for '{}'", operation)
    }

    fun reset() {
        pipelines.clear()
        componentTemplates.clear()
        indexTemplates.clear()
        attachments.clear()
        refreshedPatterns.clear()
        failOn = null
    }

    private fun guard(operation: String) {
        if (failOn == operation) {
            failOn = null
            throw IllegalStateException("Injected failure in '$operation'")
        }
    }

    override fun createIngestPipeline(name: String, body: String) {
        guard("createIngestPipeline")
        pipelines[name] = body
        log.info("  + ingest pipeline '{}'", name)
    }

    override fun deleteIngestPipeline(name: String) {
        guard("deleteIngestPipeline")
        pipelines.remove(name)
        log.info("  - ingest pipeline '{}'", name)
    }

    override fun pipelineExists(name: String) = pipelines.containsKey(name)

    override fun createComponentTemplate(name: String, body: String) {
        guard("createComponentTemplate")
        componentTemplates[name] = body
        log.info("  + component template '{}'", name)
    }

    override fun deleteComponentTemplate(name: String) {
        guard("deleteComponentTemplate")
        if (attachments.values.any { name in it }) {
            // Mirrors the real constraint: something still references this, so removing it would
            // leave a dangling reference. A rollback running in the wrong order would hit exactly
            // this, which is why the reverse ordering is not merely tidy.
            throw IllegalStateException("Component template '$name' is still attached to an index template")
        }
        componentTemplates.remove(name)
        log.info("  - component template '{}'", name)
    }

    override fun componentTemplateExists(name: String) = componentTemplates.containsKey(name)

    override fun createIndexTemplate(name: String, body: String) {
        guard("createIndexTemplate")
        indexTemplates[name] = body
        log.info("  + index template '{}'", name)
    }

    override fun deleteIndexTemplate(name: String) {
        guard("deleteIndexTemplate")
        indexTemplates.remove(name)
        attachments.remove(name)
        log.info("  - index template '{}'", name)
    }

    override fun indexTemplateExists(name: String) = indexTemplates.containsKey(name)

    override fun attachComponentTemplate(indexTemplate: String, componentTemplate: String) {
        guard("attachComponentTemplate")
        require(indexTemplates.containsKey(indexTemplate)) {
            "Index template '$indexTemplate' does not exist"
        }
        require(componentTemplates.containsKey(componentTemplate)) {
            "Component template '$componentTemplate' does not exist"
        }
        attachments.computeIfAbsent(indexTemplate) { ConcurrentHashMap.newKeySet() } += componentTemplate
        log.info("  ~ attached '{}' to '{}'", componentTemplate, indexTemplate)
    }

    override fun detachComponentTemplate(indexTemplate: String, componentTemplate: String) {
        guard("detachComponentTemplate")
        attachments[indexTemplate]?.remove(componentTemplate)
        log.info("  ~ detached '{}' from '{}'", componentTemplate, indexTemplate)
    }

    override fun refreshIndexPattern(pattern: String) {
        guard("refreshIndexPattern")
        refreshedPatterns += pattern
        log.info("  ~ refreshed index pattern '{}'", pattern)
    }

    fun isAttached(indexTemplate: String, componentTemplate: String): Boolean =
        attachments[indexTemplate]?.contains(componentTemplate) == true

    fun wasRefreshed(pattern: String) = pattern in refreshedPatterns

    override fun inventory(): Map<String, List<String>> = mapOf(
        "ingestPipelines" to pipelines.keys.sorted(),
        "componentTemplates" to componentTemplates.keys.sorted(),
        "indexTemplates" to indexTemplates.keys.sorted(),
        "attachments" to attachments.entries
            .flatMap { (index, components) -> components.map { "$index ← $it" } }
            .sorted(),
        "refreshedPatterns" to refreshedPatterns.sorted(),
    )

    /** True when nothing at all is provisioned — the assertion a rollback test really wants. */
    fun isEmpty(): Boolean =
        pipelines.isEmpty() && componentTemplates.isEmpty() &&
            indexTemplates.isEmpty() && attachments.isEmpty()
}
