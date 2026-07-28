package io.portfolio.controlplane.crud

import jakarta.persistence.Column
import jakarta.persistence.GeneratedValue
import jakarta.persistence.Id
import jakarta.persistence.MappedSuperclass
import jakarta.persistence.PrePersist
import jakarta.persistence.PreUpdate
import java.time.Instant
import java.util.UUID

/**
 * Identity, versioning and timestamps, once.
 *
 * <p>The version is not decoration. Control-plane resources are edited by several people and by
 * automation at the same time, and an optimistic-locking column turns a silent lost update into a
 * loud conflict. Without it, two concurrent edits to the same mapping produce whichever write
 * landed second and no indication that the other ever existed.
 */
@MappedSuperclass
abstract class BaseEntity {

    @Id
    @GeneratedValue
    var id: UUID? = null

    @jakarta.persistence.Version
    @Column(nullable = false)
    var version: Long = 0

    @Column(nullable = false)
    var createdAt: Instant = Instant.now()

    @Column(nullable = false)
    var updatedAt: Instant = Instant.now()

    @PrePersist
    fun onCreate() {
        val now = Instant.now()
        createdAt = now
        updatedAt = now
    }

    @PreUpdate
    fun onUpdate() {
        updatedAt = Instant.now()
    }
}
