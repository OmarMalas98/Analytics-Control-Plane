package io.portfolio.controlplane

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
class ControlPlaneApplication

fun main(args: Array<String>) {
    runApplication<ControlPlaneApplication>(*args)
}
