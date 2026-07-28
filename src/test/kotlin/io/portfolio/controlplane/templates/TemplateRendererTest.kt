package io.portfolio.controlplane.templates

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Templates produce artifact payloads, so the thing worth asserting is that the output is valid
 * JSON — a template with one stray comma renders happily and is rejected by the backend at the
 * least convenient moment.
 */
class TemplateRendererTest {

    private val renderer = TemplateRenderer()
    private val mapper = ObjectMapper()

    @Test
    @DisplayName("the ingest pipeline renders as valid JSON carrying the stream name")
    fun `renders ingest pipeline`() {
        val rendered = renderer.render(
            "ingest-pipeline",
            mapOf("stream" to "orders", "timestampField" to "created_at", "processors" to emptyList<Any>()),
        )

        val json = mapper.readTree(rendered)
        assertTrue(json.at("/description").asText().contains("orders"))
        assertEquals("created_at", json.at("/processors/1/date/field").asText())
    }

    @Test
    @DisplayName("field definitions become mapping properties")
    fun `renders component template with fields`() {
        val rendered = renderer.render(
            "component-template",
            mapOf(
                "stream" to "orders",
                "fields" to listOf(
                    mapOf("name" to "customer_id", "type" to "keyword", "indexed" to true),
                    mapOf("name" to "amount", "type" to "double", "indexed" to false),
                ),
            ),
        )

        val json = mapper.readTree(rendered)
        assertEquals("keyword", json.at("/template/mappings/properties/customer_id/type").asText())
        assertEquals("double", json.at("/template/mappings/properties/amount/type").asText())
        assertEquals("analytics-control-plane", json.at("/_meta/managedBy").asText())
    }

    @Test
    @DisplayName("no fields still renders valid JSON")
    fun `renders component template with no fields`() {
        val rendered = renderer.render(
            "component-template",
            mapOf("stream" to "orders", "fields" to emptyList<Any>()),
        )

        val json = mapper.readTree(rendered)
        assertTrue(
            json.at("/template/mappings/properties").has("@timestamp"),
            "an empty list must not leave a trailing comma behind",
        )
    }

    @Test
    @DisplayName("the index template points at its pipeline and pattern")
    fun `renders index template`() {
        val rendered = renderer.render(
            "index-template",
            mapOf(
                "stream" to "orders",
                "indexPattern" to "orders-*",
                "pipelineName" to "orders-pipeline",
                "priority" to 100,
                "shards" to 1,
                "replicas" to 1,
            ),
        )

        val json = mapper.readTree(rendered)
        assertEquals("orders-*", json.at("/index_patterns/0").asText())
        assertEquals("orders-pipeline", json.at("/template/settings/index.default_pipeline").asText())
        assertEquals(100, json.at("/priority").asInt())
    }
}
