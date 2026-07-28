package io.portfolio.controlplane.crud

import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationEventPublisher
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.transaction.annotation.Transactional
import java.util.UUID

/**
 * Typed CRUD plus change events, inherited rather than rewritten per resource.
 *
 * <p>A control plane accumulates a long tail of managed resource types, and they all want the same
 * five operations with the same event published afterwards. Copying that per type is where the
 * inconsistencies creep in — the third one forgets to publish on delete, and a cache somewhere goes
 * stale in a way nobody can reproduce.
 */
abstract class BaseService<T : BaseEntity>(
    protected val repository: JpaRepository<T, UUID>,
    private val events: ApplicationEventPublisher,
    private val resourceType: String,
) {

    private val log = LoggerFactory.getLogger(javaClass)

    @Transactional(readOnly = true)
    open fun findAll(): List<T> = repository.findAll()

    @Transactional(readOnly = true)
    open fun find(id: UUID): T? = repository.findById(id).orElse(null)

    @Transactional
    open fun create(entity: T): T {
        val saved = repository.save(entity)
        publish(saved, ResourceChangedEvent.Change.CREATED)
        return saved
    }

    @Transactional
    open fun update(entity: T): T {
        val saved = repository.save(entity)
        publish(saved, ResourceChangedEvent.Change.UPDATED)
        return saved
    }

    @Transactional
    open fun delete(id: UUID): Boolean {
        val existing = repository.findById(id).orElse(null) ?: return false
        repository.delete(existing)
        publish(existing, ResourceChangedEvent.Change.DELETED)
        return true
    }

    private fun publish(entity: T, change: ResourceChangedEvent.Change) {
        val id = entity.id ?: return
        log.debug("{} {} {}", resourceType, id, change)
        events.publishEvent(ResourceChangedEvent(resourceType, id, change, entity.version))
    }
}
