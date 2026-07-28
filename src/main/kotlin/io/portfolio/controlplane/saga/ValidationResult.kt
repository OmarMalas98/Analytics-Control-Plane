package io.portfolio.controlplane.saga

/**
 * Whether a step is safe to run — or, on the way back out, safe to undo.
 *
 * <p>A sealed type rather than a boolean so a failure has to carry a reason. "Validation failed" in
 * a log at 3am, with no indication of which precondition was missing, is barely better than silence.
 */
sealed interface ValidationResult {

    data object Success : ValidationResult

    data class Failure(val reason: String) : ValidationResult

    companion object {
        fun successIf(condition: Boolean, otherwise: () -> String): ValidationResult =
            if (condition) Success else Failure(otherwise())
    }
}
