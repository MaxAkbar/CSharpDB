namespace CSharpDB.Migration;

public static class MigrationValidationPolicyValidator
{
    public const string UnsupportedRejectModeCode = "MIG-VALIDATE-POLICY-REJECT-001";

    public static void ValidateForExecution(MigrationPlan plan)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(plan.Load);

        if (plan.Load.RejectMode != MigrationRejectMode.FailFast)
        {
            throw new MigrationExecutionPolicyException(
                UnsupportedRejectModeCode,
                $"This strict migration validation entry point supports only '{MigrationRejectMode.FailFast}' row handling " +
                $"under contract '{MigrationRejectContract.DeterministicFailFastV1}'. " +
                "Deterministic rejects require the capability-qualified SDK validation path.");
        }
    }

    internal static void ValidateForExecution(
        MigrationPlan plan,
        IMigrationEvidenceValidationSnapshot sourceSnapshot,
        IMigrationTarget target)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(plan.Load);
        ArgumentNullException.ThrowIfNull(sourceSnapshot);
        ArgumentNullException.ThrowIfNull(target);

        if (plan.Load.RejectMode == MigrationRejectMode.FailFast)
            return;
        if (plan.Load.RejectMode != MigrationRejectMode.DeterministicRejects)
        {
            throw new MigrationExecutionPolicyException(
                UnsupportedRejectModeCode,
                "The migration validation reject policy is unsupported.");
        }
        if (sourceSnapshot is not IMigrationRejectReplayValidationSnapshot)
        {
            ValidateForExecution(plan);
            return;
        }
        if (target is not IMigrationRejectLedgerTarget ||
            target is not IMigrationBatchDigestContractTarget digestTarget)
        {
            ValidateForExecution(plan);
            return;
        }

        string batchDigestFormat;
        try
        {
            batchDigestFormat = digestTarget.BatchDigestFormat;
        }
        catch (Exception error) when (error is not
            (OutOfMemoryException or StackOverflowException or AccessViolationException))
        {
            // Capability getters belong to the provider boundary and may not
            // expose arbitrary adapter messages during policy qualification.
            ValidateForExecution(plan);
            return;
        }
        if (!string.Equals(
                batchDigestFormat,
                MigrationBatchDigest.Format,
                StringComparison.Ordinal))
        {
            ValidateForExecution(plan);
        }
    }
}
