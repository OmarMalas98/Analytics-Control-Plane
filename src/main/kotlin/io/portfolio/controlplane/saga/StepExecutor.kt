package io.portfolio.controlplane.saga

import org.slf4j.LoggerFactory
import kotlin.time.measureTime

/**
 * Runs an ordered pipeline of steps, and unwinds it in reverse if any of them fails.
 *
 * <p>This is the centre of the project. Provisioning a coherent set of artifacts in an external
 * system means five or six dependent writes — an ingest pipeline, a component template, an index
 * template that references it, a stored mapping, a refreshed pattern. The external system has **no
 * transactions**. Fail on the fifth write and you are left with four orphans that will collide with
 * the next attempt, and someone gets to clean them up by hand.
 *
 * <p>So this is a saga: each step knows how to undo itself, and a failure walks back through
 * everything already executed, in reverse, undoing as it goes.
 *
 * <p>Three details that are easy to get wrong and matter more than they look:
 *
 * <ol>
 *   <li><b>The failed step is rolled back too.</b> A step that threw halfway may already have
 *       created something. Rolling back only the *successful* steps is the single most common way
 *       to leave an orphan behind.</li>
 *   <li><b>Each rollback is independently guarded.</b> One rollback throwing must not abort the
 *       rest — the remaining steps are exactly the ones still needing to be undone. Failures are
 *       collected and reported, never allowed to stop the unwind.</li>
 *   <li><b>The original failure is what propagates.</b> A rollback error is a symptom; the caller
 *       needs the cause. Rollback failures are attached as suppressed exceptions so nothing is
 *       lost.</li>
 * </ol>
 */
class StepExecutor(private vararg val steps: Step) {

    private val log = LoggerFactory.getLogger(StepExecutor::class.java)

    /**
     * Executes every step in order.
     *
     * @throws StepFailedException if any step fails validation or throws; the pipeline is rolled
     *         back first
     */
    fun execute(context: ProvisioningContext): ExecutionReport {
        val executed = mutableListOf<Step>()
        val timings = mutableListOf<StepTiming>()

        for (step in steps) {
            // Recorded *before* running, so a step that throws part-way through is still rolled back.
            executed += step

            try {
                val elapsed = measureTime {
                    when (val validation = step.validate(context)) {
                        is ValidationResult.Success -> {
                            log.info("→ {}", step.name())
                            step.execute(context)
                            context.note("executed: ${step.name()}")
                        }

                        is ValidationResult.Failure -> {
                            throw StepFailedException(step.name(), "validation failed: ${validation.reason}")
                        }
                    }
                }
                timings += StepTiming(step.name(), elapsed.inWholeMilliseconds)
                log.debug("  {} finished in {}", step.name(), elapsed)

            } catch (failure: Exception) {
                log.error("✗ {} failed: {}", step.name(), failure.message)
                val rollbackFailures = unwind(executed, context)
                throw StepFailedException(step.name(), failure.message ?: "no message", failure)
                    .also { thrown -> rollbackFailures.forEach(thrown::addSuppressed) }
            }
        }

        return ExecutionReport(context.subject, timings, context.trail())
    }

    /**
     * Undoes executed steps in reverse order, isolating each one's failure.
     *
     * @return whatever went wrong on the way back out, for the caller to attach to the real cause
     */
    private fun unwind(executed: List<Step>, context: ProvisioningContext): List<Throwable> {
        val order = executed.reversed()
        log.warn("Rolling back {} step(s): {}", order.size, order.joinToString(" → ") { it.name() })

        val failures = mutableListOf<Throwable>()

        for (step in order) {
            when (val validation = step.rollbackValidate(context)) {
                is ValidationResult.Success ->
                    try {
                        step.rollback(context)
                        context.note("rolled back: ${step.name()}")
                        log.info("↩ rolled back {}", step.name())
                    } catch (rollbackFailure: Exception) {
                        // Deliberately swallowed and collected. The steps after this one are the
                        // ones still needing to be undone; stopping here would guarantee orphans.
                        context.note("rollback FAILED: ${step.name()} — ${rollbackFailure.message}")
                        log.error("↩ rollback of {} failed — continuing with the rest", step.name(), rollbackFailure)
                        failures += rollbackFailure
                    }

                is ValidationResult.Failure -> {
                    // Nothing to undo, most often because this step never got far enough to create
                    // anything. Skipping is correct; undoing something that was never done is its
                    // own kind of damage.
                    context.note("rollback skipped: ${step.name()} — ${validation.reason}")
                    log.info("↩ nothing to roll back for {} ({})", step.name(), validation.reason)
                }
            }
        }
        return failures
    }

    data class StepTiming(val step: String, val millis: Long)

    data class ExecutionReport(
        val subject: String,
        val timings: List<StepTiming>,
        val trail: List<String>,
    )
}
