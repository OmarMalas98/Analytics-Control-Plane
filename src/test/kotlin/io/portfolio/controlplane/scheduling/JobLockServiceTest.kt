package io.portfolio.controlplane.scheduling

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Exactly-once execution under contention, which is the only property this component has.
 */
@SpringBootTest(properties = [
    "control-plane.jobs.reconcile-interval=PT1H",
    "control-plane.jobs.report-interval=PT1H",
])
class JobLockServiceTest {

    @Autowired
    private lateinit var locks: JobLockService

    @Autowired
    private lateinit var repository: JobLockRepository

    @BeforeEach
    fun reset() {
        repository.deleteAll()
    }

    @Test
    @DisplayName("the work runs when the lock is free")
    fun `runs when uncontended`() {
        var ran = false

        val acquired = locks.runIfLockAcquired("job", Duration.ofMinutes(1)) { ran = true }

        assertTrue(acquired)
        assertTrue(ran)
    }

    @Test
    @DisplayName("the lock is released afterwards so the next run can take it")
    fun `releases after running`() {
        locks.runIfLockAcquired("job", Duration.ofMinutes(1)) { }

        assertTrue(repository.findById("job").isEmpty, "a lock held after completion blocks every later run")
        assertTrue(locks.runIfLockAcquired("job", Duration.ofMinutes(1)) { })
    }

    @Test
    @DisplayName("the lock is released even when the work throws")
    fun `releases after failure`() {
        runCatching {
            locks.runIfLockAcquired("job", Duration.ofMinutes(1)) { throw IllegalStateException("boom") }
        }

        assertTrue(
            repository.findById("job").isEmpty,
            "waiting out a lease after a failure that will just be retried is pure downtime",
        )
    }

    @Test
    @DisplayName("a lock already held elsewhere is not taken")
    fun `does not take a held lock`() {
        repository.save(
            JobLock("job", "another-instance", Instant.now(), Instant.now().plusSeconds(600)),
        )

        var ran = false
        val acquired = locks.runIfLockAcquired("job", Duration.ofMinutes(1)) { ran = true }

        assertFalse(acquired)
        assertFalse(ran)
    }

    @Test
    @DisplayName("an expired lease is taken over, so a crashed holder cannot block a job forever")
    fun `takes over an expired lease`() {
        repository.save(
            JobLock("job", "crashed-instance", Instant.now().minusSeconds(900),
                Instant.now().minusSeconds(300)),
        )

        var ran = false
        val acquired = locks.runIfLockAcquired("job", Duration.ofMinutes(1)) { ran = true }

        assertTrue(acquired, "without lease expiry, a holder that died takes the job with it")
        assertTrue(ran)
    }

    @Test
    @DisplayName("under contention exactly one caller runs the work")
    fun `only one caller wins`() {
        val threads = 8
        val ready = CountDownLatch(threads)
        val go = CountDownLatch(1)
        val runs = AtomicInteger()
        val pool = Executors.newFixedThreadPool(threads)

        repeat(threads) {
            pool.submit {
                ready.countDown()
                go.await()
                // A long lease, and no release inside the block, so all eight genuinely compete
                // for one lock rather than queueing behind each other's releases.
                if (locks.tryAcquire("contended-job", Duration.ofMinutes(10))) {
                    runs.incrementAndGet()
                }
            }
        }

        ready.await(10, TimeUnit.SECONDS)
        go.countDown()
        pool.shutdown()
        pool.awaitTermination(10, TimeUnit.SECONDS)

        assertEquals(1, runs.get(), "a cron expression says when, not how many")
    }
}
