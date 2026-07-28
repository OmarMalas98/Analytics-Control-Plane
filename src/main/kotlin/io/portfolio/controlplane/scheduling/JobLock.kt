package io.portfolio.controlplane.scheduling

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.Instant

/**
 * A claim on a scheduled job, held in the database.
 *
 * <p>The primary key *is* the job name, which is the whole mechanism: two instances inserting the
 * same name at the same moment means one of them gets a constraint violation, and the database — the
 * only thing both instances agree on — decides the winner. No coordination service, no consensus
 * protocol, no leader election.
 *
 * <p>[heldUntil] is the safety valve. A holder that crashes never releases its lock, so a lease that
 * expires is the difference between "briefly delayed" and "this job never runs again".
 */
@Entity
@Table(name = "job_lock")
class JobLock(

    @Id
    @Column(name = "job_name", length = 128)
    var jobName: String = "",

    @Column(name = "held_by", nullable = false, length = 128)
    var heldBy: String = "",

    @Column(name = "held_at", nullable = false)
    var heldAt: Instant = Instant.now(),

    @Column(name = "held_until", nullable = false)
    var heldUntil: Instant = Instant.now(),
) {

    fun hasExpired(now: Instant): Boolean = heldUntil.isBefore(now)
}
