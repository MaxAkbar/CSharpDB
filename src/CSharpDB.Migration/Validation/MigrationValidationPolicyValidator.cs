namespace CSharpDB.Migration;

internal static class MigrationValidationPolicyValidator
{
    internal const string UnsupportedRejectModeCode = "MIG-VALIDATE-POLICY-REJECT-001";

    internal static void ValidateForExecution(MigrationPlan plan)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(plan.Load);

        if (plan.Load.RejectMode != MigrationRejectMode.FailFast)
        {
            throw new MigrationExecutionPolicyException(
                UnsupportedRejectModeCode,
                $"Migration validation supports only '{MigrationRejectMode.FailFast}' row handling " +
                $"under contract '{MigrationRejectContract.DeterministicFailFastV1}'. " +
                "End-to-end reject-aware source replay and outcome comparison are not enabled.");
        }
    }
}
