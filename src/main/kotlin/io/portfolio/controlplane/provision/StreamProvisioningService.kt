package io.portfolio.controlplane.provision

import io.portfolio.controlplane.backend.SearchBackend
import io.portfolio.controlplane.mapping.StreamMappingRepository
import io.portfolio.controlplane.provision.ArtifactNames.Keys
import io.portfolio.controlplane.saga.ProvisioningContext
import io.portfolio.controlplane.saga.StepExecutor
import io.portfolio.controlplane.templates.TemplateRenderer
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

/**
 * Composes the provisioning pipelines from the step catalogue.
 *
 * <p>This is the payoff for making steps independent. Create and tear-down are not two
 * implementations — they are two orderings of one set of reusable pieces, and adding a step to a
 * flow is a line in a list rather than a change to any existing class.
 *
 * <p>Note that tear-down is written as the *reverse* of creation, by hand, for the same reason the
 * rollback runs in reverse: while the component template is attached, it cannot be removed.
 */
@Service
class StreamProvisioningService(
    private val backend: SearchBackend,
    private val renderer: TemplateRenderer,
    private val mappings: StreamMappingRepository,
) {

    private val log = LoggerFactory.getLogger(StreamProvisioningService::class.java)

    /**
     * Provisions a stream end to end. Either every artifact exists afterwards, or none does.
     */
    fun provision(request: ProvisionRequest): StepExecutor.ExecutionReport {
        log.info("Provisioning stream '{}' with {} field(s)", request.stream, request.fields.size)

        val context = ProvisioningContext(
            subject = request.stream,
            initial = mapOf(
                Keys.STREAM to request.stream,
                Keys.FIELDS to request.fields.map { mapOf("name" to it.name, "type" to it.type, "indexed" to it.indexed) },
                Keys.TIMESTAMP_FIELD to request.timestampField,
            ),
        )

        return StepExecutor(
            CreateIngestPipeline(backend, renderer),
            CreateComponentTemplate(backend, renderer),
            CreateIndexTemplate(backend, renderer),
            AttachComponentTemplate(backend),
            PersistMapping(mappings),
            RefreshIndexPattern(backend),
        ).execute(context)
    }

    /**
     * Removes everything [provision] created, outermost first.
     *
     * <p>Modelled as a pipeline too, so a failure part-way through tear-down is itself rolled back
     * rather than leaving a half-dismantled stream.
     */
    fun decommission(stream: String): StepExecutor.ExecutionReport {
        log.info("Decommissioning stream '{}'", stream)

        val context = ProvisioningContext(
            subject = stream,
            initial = mapOf(
                Keys.STREAM to stream,
                Keys.INDEX_TEMPLATE to ArtifactNames.indexTemplate(stream),
                Keys.COMPONENT_TEMPLATE to ArtifactNames.componentTemplate(stream),
                Keys.PIPELINE to ArtifactNames.pipeline(stream),
                Keys.MAPPING_ID to (mappings.findByStreamName(stream)?.id
                    ?: error("No mapping recorded for stream '$stream'")),
            ),
        )

        return StepExecutor(
            DetachComponentTemplateStep(backend),
            DeleteIndexTemplateStep(backend),
            DeleteComponentTemplateStep(backend),
            DeleteIngestPipelineStep(backend),
            DeleteMappingStep(mappings),
        ).execute(context)
    }

    data class ProvisionRequest(
        val stream: String,
        val fields: List<Field> = emptyList(),
        val timestampField: String = "timestamp",
    ) {
        data class Field(val name: String, val type: String = "keyword", val indexed: Boolean = true)
    }
}
