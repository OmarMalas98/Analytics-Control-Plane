package io.portfolio.controlplane.provision

import io.portfolio.controlplane.backend.SearchBackend
import io.portfolio.controlplane.mapping.StreamMapping
import io.portfolio.controlplane.mapping.StreamMappingRepository
import io.portfolio.controlplane.provision.ArtifactNames.Keys
import io.portfolio.controlplane.saga.ProvisioningContext
import io.portfolio.controlplane.saga.Step
import io.portfolio.controlplane.saga.ValidationResult
import io.portfolio.controlplane.templates.TemplateRenderer
import java.util.UUID

/**
 * The concrete provisioning steps.
 *
 * <p>They live together because they are only meaningful as a set — the shape of the whole
 * transaction is easier to see on one screen than spread across six files.
 *
 * <p>Every one follows the same discipline: [Step.validate] refuses to clobber something that
 * already exists, [Step.execute] creates exactly one artifact, [Step.rollbackValidate] checks there
 * is actually something to undo, and [Step.rollback] removes it. That last pair is what makes the
 * unwind safe to run over a partially-executed pipeline.
 */

/** 1. The ingest pipeline every index for this stream will default to. */
class CreateIngestPipeline(
    private val backend: SearchBackend,
    private val renderer: TemplateRenderer,
) : Step {

    override fun name() = "create-ingest-pipeline"

    override fun validate(context: ProvisioningContext): ValidationResult {
        val name = ArtifactNames.pipeline(context.requireString(Keys.STREAM))
        return ValidationResult.successIf(!backend.pipelineExists(name)) {
            "ingest pipeline '$name' already exists"
        }
    }

    override fun execute(context: ProvisioningContext) {
        val stream = context.requireString(Keys.STREAM)
        val name = ArtifactNames.pipeline(stream)
        val body = renderer.render(
            "ingest-pipeline",
            mapOf(
                "stream" to stream,
                "timestampField" to (context.string(Keys.TIMESTAMP_FIELD) ?: "timestamp"),
                "processors" to emptyList<Any>(),
            ),
        )
        backend.createIngestPipeline(name, body)
        context[Keys.PIPELINE] = name
    }

    override fun rollbackValidate(context: ProvisioningContext) =
        ValidationResult.successIf(context.has(Keys.PIPELINE)) { "the pipeline was never created" }

    override fun rollback(context: ProvisioningContext) {
        backend.deleteIngestPipeline(context.requireString(Keys.PIPELINE))
        context.remove(Keys.PIPELINE)
    }
}

/** 2. The field mappings, as a reusable component template. */
class CreateComponentTemplate(
    private val backend: SearchBackend,
    private val renderer: TemplateRenderer,
) : Step {

    override fun name() = "create-component-template"

    override fun validate(context: ProvisioningContext): ValidationResult {
        val name = ArtifactNames.componentTemplate(context.requireString(Keys.STREAM))
        return ValidationResult.successIf(!backend.componentTemplateExists(name)) {
            "component template '$name' already exists"
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun execute(context: ProvisioningContext) {
        val stream = context.requireString(Keys.STREAM)
        val name = ArtifactNames.componentTemplate(stream)
        val body = renderer.render(
            "component-template",
            mapOf(
                "stream" to stream,
                "fields" to (context[Keys.FIELDS] as? List<Map<String, Any>> ?: emptyList()),
            ),
        )
        backend.createComponentTemplate(name, body)
        context[Keys.COMPONENT_TEMPLATE] = name
    }

    override fun rollbackValidate(context: ProvisioningContext) =
        ValidationResult.successIf(context.has(Keys.COMPONENT_TEMPLATE)) {
            "the component template was never created"
        }

    override fun rollback(context: ProvisioningContext) {
        backend.deleteComponentTemplate(context.requireString(Keys.COMPONENT_TEMPLATE))
        context.remove(Keys.COMPONENT_TEMPLATE)
    }
}

/** 3. The index template, which points new indices at the pipeline from step 1. */
class CreateIndexTemplate(
    private val backend: SearchBackend,
    private val renderer: TemplateRenderer,
) : Step {

    override fun name() = "create-index-template"

    override fun validate(context: ProvisioningContext): ValidationResult {
        val stream = context.requireString(Keys.STREAM)
        val name = ArtifactNames.indexTemplate(stream)
        if (backend.indexTemplateExists(name)) {
            return ValidationResult.Failure("index template '$name' already exists")
        }
        // The dependency, checked rather than assumed. Ordering is enforced by the steps, but a
        // pipeline can be reassembled, and a validation that trusts its position in a list is not
        // a validation.
        return ValidationResult.successIf(context.has(Keys.PIPELINE)) {
            "the ingest pipeline must exist before the index template can reference it"
        }
    }

    override fun execute(context: ProvisioningContext) {
        val stream = context.requireString(Keys.STREAM)
        val name = ArtifactNames.indexTemplate(stream)
        val body = renderer.render(
            "index-template",
            mapOf(
                "stream" to stream,
                "indexPattern" to ArtifactNames.indexPattern(stream),
                "pipelineName" to context.requireString(Keys.PIPELINE),
                "priority" to 100,
                "shards" to 1,
                "replicas" to 1,
            ),
        )
        backend.createIndexTemplate(name, body)
        context[Keys.INDEX_TEMPLATE] = name
        context[Keys.INDEX_PATTERN] = ArtifactNames.indexPattern(stream)
    }

    override fun rollbackValidate(context: ProvisioningContext) =
        ValidationResult.successIf(context.has(Keys.INDEX_TEMPLATE)) {
            "the index template was never created"
        }

    override fun rollback(context: ProvisioningContext) {
        backend.deleteIndexTemplate(context.requireString(Keys.INDEX_TEMPLATE))
        context.remove(Keys.INDEX_TEMPLATE)
    }
}

/**
 * 4. Wires the component template into the index template.
 *
 * <p>The step that makes rollback ordering non-negotiable: while this attachment exists, the
 * component template cannot be deleted. Unwinding in reverse detaches before step 2 tries to remove
 * it; unwinding in any other order fails.
 */
class AttachComponentTemplate(private val backend: SearchBackend) : Step {

    override fun name() = "attach-component-template"

    override fun validate(context: ProvisioningContext) =
        ValidationResult.successIf(
            context.has(Keys.INDEX_TEMPLATE) && context.has(Keys.COMPONENT_TEMPLATE),
        ) { "both templates must exist before they can be attached" }

    override fun execute(context: ProvisioningContext) {
        backend.attachComponentTemplate(
            context.requireString(Keys.INDEX_TEMPLATE),
            context.requireString(Keys.COMPONENT_TEMPLATE),
        )
    }

    override fun rollbackValidate(context: ProvisioningContext) =
        ValidationResult.successIf(
            context.has(Keys.INDEX_TEMPLATE) && context.has(Keys.COMPONENT_TEMPLATE),
        ) { "nothing was attached" }

    override fun rollback(context: ProvisioningContext) {
        backend.detachComponentTemplate(
            context.requireString(Keys.INDEX_TEMPLATE),
            context.requireString(Keys.COMPONENT_TEMPLATE),
        )
    }
}

/**
 * 5. Records the mapping in this service's own database.
 *
 * <p>Worth noticing that a local database write sits in the middle of the same pipeline as the
 * remote calls. It is not special: it can fail, and it has to be undone, exactly like the others.
 * Wrapping the local writes in a database transaction and leaving the remote ones outside it would
 * produce the classic hybrid failure — committed locally, absent remotely.
 */
class PersistMapping(private val mappings: StreamMappingRepository) : Step {

    override fun name() = "persist-mapping"

    override fun validate(context: ProvisioningContext): ValidationResult {
        val stream = context.requireString(Keys.STREAM)
        return ValidationResult.successIf(mappings.findByStreamName(stream) == null) {
            "a mapping for '$stream' is already recorded"
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun execute(context: ProvisioningContext) {
        val fields = context[Keys.FIELDS] as? List<Map<String, Any>> ?: emptyList()
        val saved = mappings.save(
            StreamMapping(
                streamName = context.requireString(Keys.STREAM),
                indexPattern = context.requireString(Keys.INDEX_PATTERN),
                pipelineName = context.requireString(Keys.PIPELINE),
                fieldCount = fields.size,
            ),
        )
        context[Keys.MAPPING_ID] = saved.id!!
    }

    override fun rollbackValidate(context: ProvisioningContext) =
        ValidationResult.successIf(context.has(Keys.MAPPING_ID)) { "no mapping was recorded" }

    override fun rollback(context: ProvisioningContext) {
        mappings.deleteById(context.require(Keys.MAPPING_ID) as UUID)
        context.remove(Keys.MAPPING_ID)
    }
}

/**
 * 6. Refreshes the index pattern so the new fields are visible to users.
 *
 * <p>The one step with nothing to undo. Rather than leave [rollback] empty and unexplained, its
 * validation says so — which is what the executor reports, so the audit trail records that this was
 * skipped deliberately rather than missed.
 */
class RefreshIndexPattern(private val backend: SearchBackend) : Step {

    override fun name() = "refresh-index-pattern"

    override fun execute(context: ProvisioningContext) {
        backend.refreshIndexPattern(context.requireString(Keys.INDEX_PATTERN))
    }

    override fun rollbackValidate(context: ProvisioningContext) =
        ValidationResult.Failure("refreshing an index pattern creates nothing to undo")

    override fun rollback(context: ProvisioningContext) {
        // Unreachable: rollbackValidate always declines.
    }
}
