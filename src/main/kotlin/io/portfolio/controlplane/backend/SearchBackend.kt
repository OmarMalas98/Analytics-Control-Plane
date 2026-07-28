package io.portfolio.controlplane.backend

/**
 * The external analytics system this control plane provisions into.
 *
 * <p>Modelled as a port for the usual reason — the engine runs without it — but also because it
 * makes the system's defining property explicit: **every method here is an independent, immediately
 * visible write, and there is no way to group them.** No transaction, no batch, no two-phase
 * commit. That single fact is the reason [io.portfolio.controlplane.saga.StepExecutor] exists.
 *
 * <p>Artifacts are dependent, which is what makes ordering matter: an index template referencing a
 * component template that does not exist is rejected, so creation runs inner-to-outer and removal
 * has to run outer-to-inner — precisely the reverse order a rollback produces for free.
 */
interface SearchBackend {

    fun createIngestPipeline(name: String, body: String)

    fun deleteIngestPipeline(name: String)

    fun pipelineExists(name: String): Boolean

    fun createComponentTemplate(name: String, body: String)

    fun deleteComponentTemplate(name: String)

    fun componentTemplateExists(name: String): Boolean

    fun createIndexTemplate(name: String, body: String)

    fun deleteIndexTemplate(name: String)

    fun indexTemplateExists(name: String): Boolean

    /**
     * Attaches a component template to an index template. Fails if either is missing — the
     * dependency that makes ordering non-negotiable.
     */
    fun attachComponentTemplate(indexTemplate: String, componentTemplate: String)

    fun detachComponentTemplate(indexTemplate: String, componentTemplate: String)

    fun refreshIndexPattern(pattern: String)

    /** Everything currently provisioned, for inspection and for asserting nothing was orphaned. */
    fun inventory(): Map<String, List<String>>
}
