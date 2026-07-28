package io.portfolio.controlplane.scheduling

import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.transaction.annotation.Transactional
import java.time.Instant

/**
 * Lock operations written as explicit statements rather than entity saves.
 *
 * <p>This is not a stylistic preference — it is the difference between the lock working and not.
 * {@code JpaRepository.save} on an entity with an assigned id performs a merge: a SELECT followed by
 * an INSERT or UPDATE. Those are two statements, and between them any number of other instances can
 * do the same thing, so several callers happily "acquire" the same lock.
 *
 * <p>Each statement below is a single atomic operation whose row count *is* the answer. The database
 * arbitrates, because it is the only participant all the instances share.
 */
interface JobLockRepository : JpaRepository<JobLock, String> {

    /**
     * Inserts a claim. Concurrent callers race on the primary key and exactly one wins; the rest
     * fail on the constraint.
     */
    @Modifying
    @Query(
        value = """
            INSERT INTO job_lock (job_name, held_by, held_at, held_until)
            VALUES (:jobName, :heldBy, :heldAt, :heldUntil)
        """,
        nativeQuery = true,
    )
    fun insertClaim(
        @Param("jobName") jobName: String,
        @Param("heldBy") heldBy: String,
        @Param("heldAt") heldAt: Instant,
        @Param("heldUntil") heldUntil: Instant,
    ): Int

    /**
     * Takes over a lease that has lapsed. The {@code held_until < :now} predicate is inside the
     * statement on purpose: checking expiry in application code first would reintroduce exactly the
     * read-then-write race this is here to avoid.
     *
     * @return 1 if this caller took it over, 0 if it was not expired after all
     */
    @Modifying
    @Query(
        value = """
            UPDATE job_lock
               SET held_by = :heldBy, held_at = :heldAt, held_until = :heldUntil
             WHERE job_name = :jobName
               AND held_until < :now
        """,
        nativeQuery = true,
    )
    fun takeOverExpired(
        @Param("jobName") jobName: String,
        @Param("heldBy") heldBy: String,
        @Param("heldAt") heldAt: Instant,
        @Param("heldUntil") heldUntil: Instant,
        @Param("now") now: Instant,
    ): Int

    /**
     * Releases a lock, but only if this caller still holds it — otherwise an instance whose lease
     * expired and was taken over would release someone else's lock on its way out.
     */
    @Modifying
    @Query(
        value = "DELETE FROM job_lock WHERE job_name = :jobName AND held_by = :heldBy",
        nativeQuery = true,
    )
    fun releaseIfHeldBy(@Param("jobName") jobName: String, @Param("heldBy") heldBy: String): Int

    /**
     * Clears every lock in one statement.
     *
     * <p>Deliberately not {@code deleteAll()}, which loads the rows and then deletes them one by
     * one — the same read-then-write pattern this whole class exists to avoid. If anything releases
     * a lock in the gap between those two operations, the per-row delete finds nothing and Hibernate
     * raises a stale-object failure. A single statement cannot be caught out that way, and deleting
     * zero rows is a perfectly good outcome.
     */
    @Transactional
    @Modifying
    @Query(value = "DELETE FROM job_lock", nativeQuery = true)
    fun releaseAll(): Int
}
