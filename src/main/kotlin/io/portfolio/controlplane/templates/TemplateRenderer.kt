package io.portfolio.controlplane.templates

import com.github.jknack.handlebars.Handlebars
import com.github.jknack.handlebars.Template
import com.github.jknack.handlebars.io.ClassPathTemplateLoader
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap

/**
 * Renders artifact payloads from templates.
 *
 * <p>The alternative is building these JSON documents in code, and it does not survive contact with
 * reality. Artifact shapes differ per deployment and change with the backend's version; encoding
 * them in Kotlin means a release for what is really a content change, and a pile of string
 * concatenation nobody can review. As templates they are reviewable, diffable, and editable without
 * a build.
 *
 * <p>Templates are compiled once and cached — they are read on every provisioning run and never
 * change between them.
 */
@Component
class TemplateRenderer {

    private val log = LoggerFactory.getLogger(TemplateRenderer::class.java)

    private val handlebars = Handlebars(ClassPathTemplateLoader("/artifact-templates", ".hbs"))

    private val cache = ConcurrentHashMap<String, Template>()

    fun render(templateName: String, model: Map<String, Any?>): String {
        val template = cache.computeIfAbsent(templateName) {
            log.debug("Compiling template '{}'", it)
            handlebars.compile(it)
        }
        return template.apply(model)
    }
}
