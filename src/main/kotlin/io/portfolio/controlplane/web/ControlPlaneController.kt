package io.portfolio.controlplane.web

import io.portfolio.controlplane.backend.InMemorySearchBackend
import io.portfolio.controlplane.mapping.StreamMappingRepository
import io.portfolio.controlplane.provision.StreamProvisioningService
import io.portfolio.controlplane.report.ReportDefinition
import io.portfolio.controlplane.report.ReportFormat
import io.portfolio.controlplane.report.ReportService
import io.portfolio.controlplane.saga.StepFailedException
import io.portfolio.controlplane.scheduling.ScheduledJobs
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

/**
 * The demo and inspection surface.
 *
 * <p>The endpoint that matters is [failNext]: arming a fault and then provisioning is what makes
 * the rollback visible. Watching the unwind is worth considerably more than reading about it.
 */
@RestController
class ControlPlaneController(
    private val provisioning: StreamProvisioningService,
    private val backend: InMemorySearchBackend,
    private val mappings: StreamMappingRepository,
    private val reports: ReportService,
    private val jobs: ScheduledJobs,
) {

    @GetMapping("/status")
    fun status(): Map<String, Any> = mapOf(
        "provisionedStreams" to mappings.findAll().map { it.streamName }.sorted(),
        "backend" to backend.inventory(),
        "jobs" to jobs.stats(),
    )

    @PostMapping("/streams")
    fun provision(@RequestBody request: StreamProvisioningService.ProvisionRequest): ResponseEntity<Any> =
        try {
            val report = provisioning.provision(request)
            ResponseEntity.ok(
                mapOf(
                    "stream" to report.subject,
                    "outcome" to "provisioned",
                    "steps" to report.timings,
                    "trail" to report.trail,
                    "backend" to backend.inventory(),
                ),
            )
        } catch (failure: StepFailedException) {
            // 409 rather than 500: the request could not be applied, but the system was left
            // consistent by the rollback. That is a different thing from an unexpected error, and
            // a caller can safely retry it once the cause is fixed.
            ResponseEntity.status(409).body(
                mapOf(
                    "stream" to request.stream,
                    "outcome" to "rolled back",
                    "failedStep" to failure.step,
                    "detail" to failure.detail,
                    "rollbackProblems" to failure.suppressed.map { it.message },
                    "backend" to backend.inventory(),
                ),
            )
        }

    @DeleteMapping("/streams/{stream}")
    fun decommission(@PathVariable stream: String): ResponseEntity<Any> =
        try {
            val report = provisioning.decommission(stream)
            ResponseEntity.ok(
                mapOf(
                    "stream" to stream,
                    "outcome" to "decommissioned",
                    "trail" to report.trail,
                    "backend" to backend.inventory(),
                ),
            )
        } catch (failure: StepFailedException) {
            ResponseEntity.status(409).body(
                mapOf("stream" to stream, "failedStep" to failure.step, "detail" to failure.detail),
            )
        }

    /**
     * Arms a fault in the backend so the next matching operation throws.
     *
     * <p>Valid operations: createIngestPipeline, createComponentTemplate, createIndexTemplate,
     * attachComponentTemplate, refreshIndexPattern, deleteIndexTemplate, deleteComponentTemplate,
     * deleteIngestPipeline.
     */
    @PostMapping("/demo/fail-next")
    fun failNext(@RequestParam operation: String): Map<String, Any> {
        backend.failNext(operation)
        return mapOf("armed" to operation)
    }

    @PostMapping("/demo/reset")
    fun reset(): Map<String, Any> {
        backend.reset()
        mappings.deleteAll()
        return mapOf("reset" to true)
    }

    @PostMapping("/reports")
    fun runReport(@RequestBody definition: ReportDefinition): ReportService.ReportOutcome =
        reports.run(definition)

    @GetMapping("/reports/formats")
    fun formats(): List<String> = ReportFormat.entries.map { it.name }
}
