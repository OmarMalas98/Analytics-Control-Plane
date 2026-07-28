package io.portfolio.controlplane.scheduling

import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Propagation
import org.springframework.transaction.annotation.Transactional
import java.time.Instant

/**
 * Transaction boundaries for the lock statements.
 *
 * <p>Separated from {@link JobLockService} for a specific reason. A losing insert raises a
 * constraint violation, which marks its transaction rollback-only; committing such a transaction
 * throws regardless of whether the exception was caught. Catching it *inside* the transactional
 * method would therefore turn "I did not get the lock" into a thrown exception at commit time.
 *
 * <p>Putting the boundary here and the {@code catch} in the caller means the transaction is fully
 * rolled back before the exception surfaces, so losing the race is an ordinary {@code false}.
 * Spring's proxying is also why this cannot simply be another method on the same class — a
 * self-invocation would not cross a proxy and would get no new transaction at all.
 */
@Component
class JobLockStore(private val locks: JobLockRepository) {

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    fun insertClaim(jobName: String, heldBy: String, now: Instant, heldUntil: Instant): Int =
        locks.insertClaim(jobName, heldBy, now, heldUntil)

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    fun takeOverExpired(jobName: String, heldBy: String, now: Instant, heldUntil: Instant): Int =
        locks.takeOverExpired(jobName, heldBy, now, heldUntil, now)

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    fun release(jobName: String, heldBy: String): Int =
        locks.releaseIfHeldBy(jobName, heldBy)
}
