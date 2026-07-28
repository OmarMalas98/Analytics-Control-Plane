package io.portfolio.controlplane.provision

import io.portfolio.controlplane.backend.SearchBackend
import io.portfolio.controlplane.mapping.StreamMappingRepository
import io.portfolio.controlplane.provision.ArtifactNames.Keys
import io.portfolio.controlplane.saga.ProvisioningContext
import io.portfolio.controlplane.saga.Step
import io.portfolio.controlplane.saga.ValidationResult

/**
 * Tear-down steps.
 *
 * <p>Their rollbacks are honest about a hard limit: **deletion is not reversible here.** A deleted
 * artifact cannot be recreated from nothing, because its body is gone. So these steps decline to
 * roll back rather than pretending, which means the executor reports exactly what could not be
 * undone instead of logging a successful rollback that did nothing.
 *
 * <p>The mitigation is ordering: the operations most likely to fail run first, so a failure happens
 * while the least has been destroyed. That is the honest engineering answer to an irreversible
 * operation — you cannot undo it, so you sequence to minimise what is lost.
 */

class DetachComponentTemplateStep(private val backend: SearchBackend) : Step {

    override fun name() = "detach-component-template"

    override fun execute(context: ProvisioningContext) {
        backend.detachComponentTemplate(
            context.requireString(Keys.INDEX_TEMPLATE),
            context.requireString(Keys.COMPONENT_TEMPLATE),
        )
    }

    /** Re-attaching is safe and meaningful, so this one genuinely can be undone. */
    override fun rollbackValidate(context: ProvisioningContext) =
        ValidationResult.successIf(
            backend.indexTemplateExists(context.requireString(Keys.INDEX_TEMPLATE)) &&
                backend.componentTemplateExists(context.requireString(Keys.COMPONENT_TEMPLATE)),
        ) { "one of the templates is already gone, so the attachment cannot be restored" }

    override fun rollback(context: ProvisioningContext) {
        backend.attachComponentTemplate(
            context.requireString(Keys.INDEX_TEMPLATE),
            context.requireString(Keys.COMPONENT_TEMPLATE),
        )
    }
}

class DeleteIndexTemplateStep(private val backend: SearchBackend) : Step {

    override fun name() = "delete-index-template"

    override fun validate(context: ProvisioningContext): ValidationResult {
        val name = context.requireString(Keys.INDEX_TEMPLATE)
        return ValidationResult.successIf(backend.indexTemplateExists(name)) {
            "index template '$name' does not exist"
        }
    }

    override fun execute(context: ProvisioningContext) {
        backend.deleteIndexTemplate(context.requireString(Keys.INDEX_TEMPLATE))
    }

    override fun rollbackValidate(context: ProvisioningContext) =
        ValidationResult.Failure("a deleted index template cannot be restored from here")

    override fun rollback(context: ProvisioningContext) = Unit
}

class DeleteComponentTemplateStep(private val backend: SearchBackend) : Step {

    override fun name() = "delete-component-template"

    override fun validate(context: ProvisioningContext): ValidationResult {
        val name = context.requireString(Keys.COMPONENT_TEMPLATE)
        return ValidationResult.successIf(backend.componentTemplateExists(name)) {
            "component template '$name' does not exist"
        }
    }

    override fun execute(context: ProvisioningContext) {
        backend.deleteComponentTemplate(context.requireString(Keys.COMPONENT_TEMPLATE))
    }

    override fun rollbackValidate(context: ProvisioningContext) =
        ValidationResult.Failure("a deleted component template cannot be restored from here")

    override fun rollback(context: ProvisioningContext) = Unit
}

class DeleteIngestPipelineStep(private val backend: SearchBackend) : Step {

    override fun name() = "delete-ingest-pipeline"

    override fun validate(context: ProvisioningContext): ValidationResult {
        val name = context.requireString(Keys.PIPELINE)
        return ValidationResult.successIf(backend.pipelineExists(name)) {
            "ingest pipeline '$name' does not exist"
        }
    }

    override fun execute(context: ProvisioningContext) {
        backend.deleteIngestPipeline(context.requireString(Keys.PIPELINE))
    }

    override fun rollbackValidate(context: ProvisioningContext) =
        ValidationResult.Failure("a deleted ingest pipeline cannot be restored from here")

    override fun rollback(context: ProvisioningContext) = Unit
}

/**
 * Removes the local record last, on purpose.
 *
 * <p>If tear-down fails midway, the record still points at whatever survived, so the operation can
 * be retried. Deleting it first would strip the only description of what was supposed to exist and
 * leave the leftovers unattributable.
 */
class DeleteMappingStep(private val mappings: StreamMappingRepository) : Step {

    override fun name() = "delete-mapping"

    override fun execute(context: ProvisioningContext) {
        mappings.deleteById(context.require(Keys.MAPPING_ID) as java.util.UUID)
    }

    override fun rollbackValidate(context: ProvisioningContext) =
        ValidationResult.Failure("the mapping record has already been removed")

    override fun rollback(context: ProvisioningContext) = Unit
}
