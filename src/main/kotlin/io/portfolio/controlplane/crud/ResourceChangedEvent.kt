package io.portfolio.controlplane.crud

import java.util.UUID

/**
 * Published whenever a managed resource changes.
 *
 * <p>A control plane's whole job is to keep an external system in step with its own records, so
 * "something changed" is the most important thing it can say. Publishing it as an event means
 * caches, reconcilers and audit logs subscribe rather than being called — none of them has to be
 * known to the code doing the saving.
 */
data class ResourceChangedEvent(
    val resourceType: String,
    val resourceId: UUID,
    val change: Change,
    val version: Long,
) {
    enum class Change { CREATED, UPDATED, DELETED }
}
