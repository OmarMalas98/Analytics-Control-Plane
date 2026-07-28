package io.portfolio.controlplane.saga

/**
 * One reversible unit of provisioning work.
 *
 * <p>Four methods, in two symmetric pairs. The forward pair is obvious; the backward pair is the
 * one that earns its keep:
 *
 * <ul>
 *   <li>[validate] — is it safe to do this? Checked *before* anything is touched.</li>
 *   <li>[execute] — do it, and record in the context whatever [rollback] will need.</li>
 *   <li>[rollbackValidate] — is it safe, and necessary, to undo this? A step that failed halfway
 *       may have nothing to undo, and undoing something that was never done is its own kind of
 *       damage.</li>
 *   <li>[rollback] — undo it.</li>
 * </ul>
 *
 * <p>Implementations must make [rollback] tolerant of partial execution. It is called for the step
 * that *failed* as well as for the ones that succeeded, because a step that threw halfway may
 * already have created something.
 */
interface Step {

    /** Name used in logs and in the executor's report. Keep it descriptive; it is the audit trail. */
    fun name(): String

    fun validate(context: ProvisioningContext): ValidationResult = ValidationResult.Success

    fun execute(context: ProvisioningContext)

    fun rollbackValidate(context: ProvisioningContext): ValidationResult = ValidationResult.Success

    fun rollback(context: ProvisioningContext)
}
