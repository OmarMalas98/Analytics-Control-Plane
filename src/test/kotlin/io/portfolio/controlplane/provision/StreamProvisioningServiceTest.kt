package io.portfolio.controlplane.provision

import io.portfolio.controlplane.backend.InMemorySearchBackend
import io.portfolio.controlplane.mapping.StreamMappingRepository
import io.portfolio.controlplane.saga.StepFailedException
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The real pipeline against the stand-in backend, including a fault injected at every stage.
 *
 * <p>The assertion that carries the weight is the same one every time: after a failure, the backend
 * is completely empty. Not "mostly cleaned up" — empty.
 */
@SpringBootTest(properties = [
    "control-plane.jobs.reconcile-interval=PT1H",
    "control-plane.jobs.report-interval=PT1H",
    // A fixed-delay job still fires once immediately unless an initial delay pushes it out, and
    // reconciliation reads the very mappings these tests are creating and rolling back.
    "control-plane.jobs.initial-delay=PT1H",
])
class StreamProvisioningServiceTest {

    @Autowired
    private lateinit var provisioning: StreamProvisioningService

    @Autowired
    private lateinit var backend: InMemorySearchBackend

    @Autowired
    private lateinit var mappings: StreamMappingRepository

    @BeforeEach
    fun reset() {
        backend.reset()
        mappings.deleteAll()
    }

    @Test
    @DisplayName("a successful run leaves every artifact in place")
    fun `provisions the full artifact set`() {
        provisioning.provision(request("orders"))

        assertTrue(backend.pipelineExists("orders-pipeline"))
        assertTrue(backend.componentTemplateExists("orders-mappings"))
        assertTrue(backend.indexTemplateExists("orders-template"))
        assertTrue(backend.isAttached("orders-template", "orders-mappings"))
        assertTrue(backend.wasRefreshed("orders-*"))
        assertNotNull(mappings.findByStreamName("orders"))
    }

    @Test
    @DisplayName("failing on the last remote call leaves nothing behind")
    fun `rolls back everything when the final step fails`() {
        backend.failNext("refreshIndexPattern")

        assertFailsWith<StepFailedException> { provisioning.provision(request("orders")) }

        assertTrue(backend.isEmpty(), "the backend must be empty, not partly provisioned")
        assertNull(mappings.findByStreamName("orders"), "the local record must go too")
    }

    @Test
    @DisplayName("a failure at any stage leaves nothing behind")
    fun `rolls back from every failure point`() {
        val failurePoints = listOf(
            "createIngestPipeline",
            "createComponentTemplate",
            "createIndexTemplate",
            "attachComponentTemplate",
            "refreshIndexPattern",
        )

        for (operation in failurePoints) {
            reset()
            backend.failNext(operation)

            assertFailsWith<StepFailedException> { provisioning.provision(request("orders")) }

            assertTrue(backend.isEmpty(), "artifacts were orphaned after '$operation' failed")
            assertNull(mappings.findByStreamName("orders"), "a mapping survived '$operation'")
        }
    }

    @Test
    @DisplayName("the failing step is named in the error")
    fun `reports which step failed`() {
        backend.failNext("createIndexTemplate")

        val failure = assertFailsWith<StepFailedException> { provisioning.provision(request("orders")) }

        assertEquals("create-index-template", failure.step)
    }

    @Test
    @DisplayName("rolling back in reverse is what lets the attached component template be removed")
    fun `reverse order satisfies the backend's dependency rules`() {
        // The backend refuses to delete a component template while it is attached. Rolling back in
        // reverse detaches first; any other order would fail here, so a clean backend is proof the
        // ordering held.
        backend.failNext("refreshIndexPattern")

        assertFailsWith<StepFailedException> { provisioning.provision(request("orders")) }

        assertFalse(backend.componentTemplateExists("orders-mappings"))
        assertTrue(backend.isEmpty())
    }

    @Test
    @DisplayName("provisioning the same stream twice is refused before anything is touched")
    fun `refuses to clobber an existing stream`() {
        provisioning.provision(request("orders"))

        val failure = assertFailsWith<StepFailedException> { provisioning.provision(request("orders")) }

        assertEquals("create-ingest-pipeline", failure.step)
        assertTrue(
            backend.pipelineExists("orders-pipeline"),
            "the first stream's artifacts must survive the second attempt's rollback",
        )
        assertNotNull(mappings.findByStreamName("orders"))
    }

    @Test
    @DisplayName("decommissioning removes everything provisioning created")
    fun `decommissions cleanly`() {
        provisioning.provision(request("orders"))

        provisioning.decommission("orders")

        assertTrue(backend.isEmpty())
        assertNull(mappings.findByStreamName("orders"))
    }

    @Test
    @DisplayName("the execution report records the steps that ran")
    fun `reports the trail`() {
        val report = provisioning.provision(request("orders"))

        assertEquals(
            listOf(
                "create-ingest-pipeline",
                "create-component-template",
                "create-index-template",
                "attach-component-template",
                "persist-mapping",
                "refresh-index-pattern",
            ),
            report.timings.map { it.step },
        )
    }

    private fun request(stream: String) = StreamProvisioningService.ProvisionRequest(
        stream = stream,
        fields = listOf(
            StreamProvisioningService.ProvisionRequest.Field("customer_id", "keyword"),
            StreamProvisioningService.ProvisionRequest.Field("amount", "double"),
        ),
    )
}
