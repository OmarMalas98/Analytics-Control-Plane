package io.portfolio.controlplane.mapping

import org.springframework.data.jpa.repository.JpaRepository
import java.util.UUID

interface StreamMappingRepository : JpaRepository<StreamMapping, UUID> {

    fun findByStreamName(streamName: String): StreamMapping?
}
