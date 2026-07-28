package io.portfolio.controlplane.saga

import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * The executor's contract, tested with recording steps so the exact order of calls is observable.
 *
 * <p>These are the tests that matter most in the project. The saga's value is entirely in what it
 * does on the unhappy path, and that path is by definition the one nobody exercises by hand.
 */
class StepExecutorTest {

    private val log = mutableListOf<String>()

    @Test
    @DisplayName("a successful pipeline runs every step in order and rolls nothing back")
    fun `runs all steps in order`() {
        val context = ProvisioningContext("subject")

        StepExecutor(step("a"), step("b"), step("c")).execute(context)

        assertEquals(listOf("execute:a", "execute:b", "execute:c"), log)
    }

    @Test
    @DisplayName("a failure rolls the executed steps back in exactly reverse order")
    fun `rolls back in reverse order`() {
        val context = ProvisioningContext("subject")

        assertFailsWith<StepFailedException> {
            StepExecutor(step("a"), step("b"), step("c", failsOnExecute = true), step("d"))
                .execute(context)
        }

        assertEquals(
            listOf(
                "execute:a", "execute:b", "execute:c",
                "rollback:c", "rollback:b", "rollback:a",
            ),
            log,
        )
        assertTrue(log.none { it == "execute:d" }, "steps after the failure must not run")
    }

    @Test
    @DisplayName("the step that failed is rolled back too")
    fun `rolls back the failing step`() {
        val context = ProvisioningContext("subject")

        assertFailsWith<StepFailedException> {
            StepExecutor(step("a"), step("b", failsOnExecute = true)).execute(context)
        }

        assertTrue(
            log.contains("rollback:b"),
            "a step that threw part-way may already have created something; skipping its rollback " +
                "is the most common way to leave an orphan behind",
        )
    }

    @Test
    @DisplayName("a validation failure stops the pipeline before the step executes")
    fun `validation failure short-circuits`() {
        val context = ProvisioningContext("subject")

        val failure = assertFailsWith<StepFailedException> {
            StepExecutor(step("a"), step("b", failsValidation = true), step("c")).execute(context)
        }

        assertEquals("b", failure.step)
        assertTrue(log.none { it == "execute:b" }, "the step must not run when validation fails")
        assertEquals(listOf("execute:a", "rollback:b", "rollback:a"), log)
    }

    @Test
    @DisplayName("one rollback failing does not stop the remaining rollbacks")
    fun `rollback failures are isolated`() {
        val context = ProvisioningContext("subject")

        assertFailsWith<StepFailedException> {
            StepExecutor(
                step("a"),
                step("b", failsOnRollback = true),
                step("c", failsOnExecute = true),
            ).execute(context)
        }

        assertTrue(
            log.contains("rollback:a"),
            "the steps after a failing rollback are exactly the ones still needing to be undone",
        )
    }

    @Test
    @DisplayName("the original failure propagates, with rollback problems attached")
    fun `original cause is preserved`() {
        val context = ProvisioningContext("subject")

        val failure = assertFailsWith<StepFailedException> {
            StepExecutor(
                step("a", failsOnRollback = true),
                step("b", failsOnExecute = true),
            ).execute(context)
        }

        assertEquals("b", failure.step, "the caller needs the cause, not the symptom")
        assertEquals(1, failure.suppressed.size, "the rollback problem must not be lost either")
    }

    @Test
    @DisplayName("a step that declines rollback is skipped and recorded as such")
    fun `declined rollback is skipped`() {
        val context = ProvisioningContext("subject")

        assertFailsWith<StepFailedException> {
            StepExecutor(
                step("a", declinesRollback = true),
                step("b", failsOnExecute = true),
            ).execute(context)
        }

        assertTrue(log.none { it == "rollback:a" })
        assertTrue(
            context.trail().any { it.startsWith("rollback skipped: a") },
            "the trail must record that this was deliberate, not missed",
        )
    }

    @Test
    @DisplayName("the report records each step's duration")
    fun `reports timings`() {
        val report = StepExecutor(step("a"), step("b")).execute(ProvisioningContext("subject"))

        assertEquals(listOf("a", "b"), report.timings.map { it.step })
    }

    private fun step(
        id: String,
        failsOnExecute: Boolean = false,
        failsValidation: Boolean = false,
        failsOnRollback: Boolean = false,
        declinesRollback: Boolean = false,
    ) = object : Step {

        override fun name() = id

        override fun validate(context: ProvisioningContext) =
            if (failsValidation) ValidationResult.Failure("declined by test")
            else ValidationResult.Success

        override fun execute(context: ProvisioningContext) {
            log += "execute:$id"
            if (failsOnExecute) throw IllegalStateException("boom in $id")
        }

        override fun rollbackValidate(context: ProvisioningContext) =
            if (declinesRollback) ValidationResult.Failure("nothing to undo")
            else ValidationResult.Success

        override fun rollback(context: ProvisioningContext) {
            if (failsOnRollback) throw IllegalStateException("rollback of $id failed")
            log += "rollback:$id"
        }
    }
}
