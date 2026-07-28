package io.portfolio.controlplane.report

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import org.springframework.stereotype.Service
import java.time.Instant

/**
 * What to produce, from where, for whom.
 */
data class ReportDefinition(
    val name: String,
    val dashboard: String,
    val format: ReportFormat,
    val recipients: List<String>,
)

enum class ReportFormat { PDF, CSV, HTML }

data class RenderedReport(
    val definition: ReportDefinition,
    val bytes: ByteArray,
    val renderedAt: Instant,
) {
    val sizeBytes: Int get() = bytes.size

    // ByteArray uses identity equality, so a data class holding one needs these written out —
    // otherwise two identical reports compare unequal and the generated hashCode is unstable.
    override fun equals(other: Any?): Boolean =
        this === other || (other is RenderedReport &&
            definition == other.definition && bytes.contentEquals(other.bytes))

    override fun hashCode(): Int = 31 * definition.hashCode() + bytes.contentHashCode()
}

/**
 * Turns a dashboard into bytes.
 *
 * <p>A port because the production implementation drives a headless browser: load the dashboard,
 * wait for every panel to finish rendering, screenshot, assemble a PDF. That needs a browser grid,
 * takes tens of seconds, and fails in its own creative ways. Keeping it behind an interface means
 * the scheduling, locking and delivery around it are testable without any of that.
 */
interface ReportRenderer {
    fun supports(format: ReportFormat): Boolean
    fun render(definition: ReportDefinition): RenderedReport
}

/** Where a finished report goes. */
interface DeliveryChannel {
    fun name(): String
    fun deliver(report: RenderedReport, recipient: String)
}

@Component
class TextReportRenderer : ReportRenderer {

    override fun supports(format: ReportFormat) = format != ReportFormat.PDF

    override fun render(definition: ReportDefinition): RenderedReport {
        val content = buildString {
            appendLine("Report: ${definition.name}")
            appendLine("Dashboard: ${definition.dashboard}")
            appendLine("Generated: ${Instant.now()}")
            appendLine()
            appendLine("metric,value")
            appendLine("ingested_documents,184320")
            appendLine("failed_documents,912")
            appendLine("p95_latency_ms,148")
        }
        return RenderedReport(definition, content.toByteArray(), Instant.now())
    }
}

/**
 * Stands in for the headless-browser renderer.
 *
 * <p>Present rather than omitted so the format-dispatch logic in [ReportService] is real, and so the
 * seam where a browser grid attaches is visible in the code rather than only in the README.
 */
@Component
class PlaceholderPdfRenderer : ReportRenderer {

    override fun supports(format: ReportFormat) = format == ReportFormat.PDF

    override fun render(definition: ReportDefinition): RenderedReport {
        val stub = "%PDF-1.4 stub for '${definition.dashboard}' — a real renderer drives a browser here"
        return RenderedReport(definition, stub.toByteArray(), Instant.now())
    }
}

@Component
class LoggingDeliveryChannel : DeliveryChannel {

    private val log = LoggerFactory.getLogger(LoggingDeliveryChannel::class.java)

    override fun name() = "log"

    override fun deliver(report: RenderedReport, recipient: String) {
        log.info(
            "Delivered '{}' ({} bytes, {}) to {}",
            report.definition.name, report.sizeBytes, report.definition.format, recipient,
        )
    }
}

/**
 * Definition → render → deliver, with each recipient's failure isolated.
 *
 * <p>One unreachable recipient must not cost the others their copy. Reporting is a batch operation
 * and batches fail partially; treating that as normal is the difference between a report that
 * mostly arrives and one that arrives for nobody.
 */
@Service
class ReportService(
    private val renderers: List<ReportRenderer>,
    private val channels: List<DeliveryChannel>,
) {

    private val log = LoggerFactory.getLogger(ReportService::class.java)

    fun run(definition: ReportDefinition): ReportOutcome {
        val renderer = renderers.firstOrNull { it.supports(definition.format) }
            ?: return ReportOutcome(definition.name, false, 0, emptyList(),
                listOf("no renderer supports ${definition.format}"))

        val rendered = renderer.render(definition)
        val failures = mutableListOf<String>()
        val delivered = mutableListOf<String>()

        for (recipient in definition.recipients) {
            for (channel in channels) {
                try {
                    channel.deliver(rendered, recipient)
                    delivered += "$recipient via ${channel.name()}"
                } catch (failure: Exception) {
                    log.error("Delivery of '{}' to {} failed", definition.name, recipient, failure)
                    failures += "$recipient via ${channel.name()}: ${failure.message}"
                }
            }
        }

        return ReportOutcome(definition.name, failures.isEmpty(), rendered.sizeBytes, delivered, failures)
    }

    data class ReportOutcome(
        val report: String,
        val successful: Boolean,
        val sizeBytes: Int,
        val delivered: List<String>,
        val failures: List<String>,
    )
}
