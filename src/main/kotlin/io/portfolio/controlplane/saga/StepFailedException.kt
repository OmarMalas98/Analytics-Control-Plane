package io.portfolio.controlplane.saga

/**
 * Thrown when a pipeline could not complete. By the time this surfaces the rollback has already run.
 *
 * <p>Any failure encountered *during* the rollback is attached as a suppressed exception, so the
 * caller sees the original cause first — which is what they need to fix — without losing the
 * evidence that cleanup was also imperfect, which is what they need to know about.
 */
class StepFailedException(
    val step: String,
    val detail: String,
    cause: Throwable? = null,
) : RuntimeException("Step '$step' failed: $detail", cause)
