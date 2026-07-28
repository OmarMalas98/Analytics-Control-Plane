package io.portfolio.controlplane.scheduling

import org.slf4j.LoggerFactory
import org.springframework.dao.DataAccessException
import org.springframework.stereotype.Service
import java.net.InetAddress
import java.time.Duration
import java.time.Instant

/**
 * Ensures a scheduled job runs on exactly one instance at a time.
 *
 * <p>Every replica of a control plane has the same `@Scheduled` methods, and most of them must not
 * run concurrently: two instances reconciling the same stream will fight over the same artifacts,
 * and two instances generating the same report will send it twice. **A cron expression is a
 * statement about *when*, not about *how many*.**
 *
 * <p>The arbiter is the database, because it is the only thing every instance shares. Two mechanisms
 * do the work, and both are single statements — the row count returned *is* the answer:
 *
 * <ul>
 *   <li>an <b>insert</b> on the job name, which concurrent callers race on and exactly one wins;</li>
 *   <li>a <b>conditional update</b> that takes over a lease which has lapsed, so an instance that
 *       crashed holding a lock does not take the job down with it.</li>
 * </ul>
 *
 * <p>The tempting shortcut — read the row, decide, then write — is two statements with a gap in the
 * middle, and several callers will pass through that gap believing they hold the lock.
 */
@Service
class JobLockService(private val store: JobLockStore) {

    private val log = LoggerFactory.getLogger(JobLockService::class.java)

    private val instanceId: String =
        "${runCatching { InetAddress.getLocalHost().hostName }.getOrDefault("unknown")}" +
            "-${ProcessHandle.current().pid()}"

    /**
     * Runs [work] if this instance can take the lock; otherwise returns without doing anything.
     *
     * @return true if the work ran here
     */
    fun runIfLockAcquired(jobName: String, lease: Duration, work: () -> Unit): Boolean {
        if (!tryAcquire(jobName, lease)) {
            log.debug("'{}' is held elsewhere; skipping on {}", jobName, instanceId)
            return false
        }
        try {
            log.info("Running '{}' on {}", jobName, instanceId)
            work()
        } finally {
            // Released even if the work threw. The lease would expire eventually, but waiting one
            // out after a failure that will simply be retried is pure downtime.
            release(jobName)
        }
        return true
    }

    fun tryAcquire(jobName: String, lease: Duration): Boolean {
        val now = Instant.now()
        val heldUntil = now.plus(lease)

        // Claim it if nobody has. The catch is deliberately out here, outside the transaction
        // boundary in JobLockStore, so the failed transaction is fully rolled back before the
        // exception reaches this frame.
        val inserted = try {
            store.insertClaim(jobName, instanceId, now, heldUntil)
        } catch (contention: DataAccessException) {
            // The normal outcome on every replica but one. Not an error.
            log.debug("Lost the race for '{}'", jobName)
            0
        }
        if (inserted > 0) {
            return true
        }

        // A row exists. Take it over only if its lease has lapsed — decided inside the statement,
        // so no other instance can slip between the check and the write.
        val takenOver = store.takeOverExpired(jobName, instanceId, now, heldUntil)
        if (takenOver > 0) {
            log.warn("Lease on '{}' had expired; {} has taken it over", jobName, instanceId)
            return true
        }
        return false
    }

    fun release(jobName: String) {
        store.release(jobName, instanceId)
    }

    fun instanceId(): String = instanceId
}
