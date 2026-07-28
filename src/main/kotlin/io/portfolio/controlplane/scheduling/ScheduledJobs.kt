package io.portfolio.controlplane.scheduling

import io.portfolio.controlplane.backend.SearchBackend
import io.portfolio.controlplane.mapping.StreamMappingRepository
import io.portfolio.controlplane.report.ReportDefinition
import io.portfolio.controlplane.report.ReportFormat
import io.portfolio.controlplane.report.ReportService
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

/**
 * The recurring work, every piece of it guarded by a lock.
 *
 * <p>A cron expression says *when*, not *how many*. With three replicas, an unguarded
 * `@Scheduled` method runs three times — which for reconciliation means instances fighting over the
 * same artifacts, and for reporting means recipients getting three copies.
 */
@Component
class ScheduledJobs(
    private val locks: JobLockService,
    private val mappings: StreamMappingRepository,
    private val backend: SearchBackend,
    private val reports: ReportService,
) {

    private val log = LoggerFactory.getLogger(ScheduledJobs::class.java)

    private val reconcileRuns = AtomicInteger()
    private val reportRuns = AtomicInteger()

    /**
     * Checks that what the database says exists actually exists in the backend.
     *
     * <p>Drift is not hypothetical: someone deletes an artifact by hand, a rollback fails halfway,
     * a backend is restored from an older snapshot. Detecting it is the point of keeping a local
     * record at all.
     */
    @Scheduled(fixedDelayString = "\${control-plane.jobs.reconcile-interval:PT60S}")
    fun reconcileStreams() {
        locks.runIfLockAcquired("reconcile-streams", Duration.ofMinutes(5)) {
            reconcileRuns.incrementAndGet()
            val drifted = mappings.findAll().filterNot { mapping ->
                backend.pipelineExists(mapping.pipelineName)
            }
            if (drifted.isEmpty()) {
                log.debug("Reconciliation found no drift across {} stream(s)", mappings.count())
            } else {
                log.warn("Drift detected — {} stream(s) missing their ingest pipeline: {}",
                    drifted.size, drifted.map { it.streamName })
            }
        }
    }

    @Scheduled(fixedDelayString = "\${control-plane.jobs.report-interval:PT300S}")
    fun generateScheduledReports() {
        locks.runIfLockAcquired("generate-reports", Duration.ofMinutes(15)) {
            reportRuns.incrementAndGet()
            val outcome = reports.run(
                ReportDefinition(
                    name = "Daily ingest summary",
                    dashboard = "ingest-overview",
                    format = ReportFormat.CSV,
                    recipients = listOf("platform-team@example.test"),
                ),
            )
            log.info("Report '{}' → delivered={} failures={}",
                outcome.report, outcome.delivered, outcome.failures)
        }
    }

    fun stats(): Map<String, Any> = mapOf(
        "instanceId" to locks.instanceId(),
        "reconcileRuns" to reconcileRuns.get(),
        "reportRuns" to reportRuns.get(),
    )
}
