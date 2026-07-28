package io.portfolio.controlplane.mapping

import io.portfolio.controlplane.crud.BaseEntity
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

/**
 * This control plane's record of a stream it has provisioned.
 *
 * <p>The point of keeping it is reconciliation. The backend holds the artifacts; this holds what
 * *should* be there. Without a local record, "is this stream correctly provisioned?" can only be
 * answered by inspecting the backend and guessing at intent.
 */
@Entity
@Table(name = "stream_mapping")
class StreamMapping(

    @Column(name = "stream_name", nullable = false, unique = true, length = 128)
    var streamName: String = "",

    @Column(name = "index_pattern", nullable = false, length = 128)
    var indexPattern: String = "",

    @Column(name = "pipeline_name", nullable = false, length = 128)
    var pipelineName: String = "",

    @Column(name = "field_count", nullable = false)
    var fieldCount: Int = 0,

) : BaseEntity()
