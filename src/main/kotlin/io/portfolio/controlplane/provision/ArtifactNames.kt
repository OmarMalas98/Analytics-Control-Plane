package io.portfolio.controlplane.provision

/**
 * Names of every artifact derived from the stream name, in one place.
 *
 * <p>Derived rather than stored, and centralised rather than inlined, because rollback depends on
 * it. A step that has to *find* what it created in order to remove it can only do so if the name is
 * reproducible. Scatter this convention across six step classes and the first one that drifts leaves
 * an artifact nothing can clean up.
 */
object ArtifactNames {

    fun pipeline(stream: String) = "$stream-pipeline"

    fun componentTemplate(stream: String) = "$stream-mappings"

    fun indexTemplate(stream: String) = "$stream-template"

    fun indexPattern(stream: String) = "$stream-*"

    /** Context keys the steps exchange values through. */
    object Keys {
        const val STREAM = "stream"
        const val FIELDS = "fields"
        const val PIPELINE = "pipelineName"
        const val COMPONENT_TEMPLATE = "componentTemplateName"
        const val INDEX_TEMPLATE = "indexTemplateName"
        const val INDEX_PATTERN = "indexPattern"
        const val MAPPING_ID = "mappingId"
        const val TIMESTAMP_FIELD = "timestampField"
    }
}
