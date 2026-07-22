namespace CSharpDB.Migration;

/// <summary>
/// Versioned row-rejection behavior supported by the staged Phase 2 apply
/// slice. A value failure stops at the first source object, batch, row, and
/// column in canonical execution order. The failing batch is never submitted
/// to the target; already receipted batches remain resumable.
/// </summary>
public static class MigrationRejectContract
{
    public const string DeterministicFailFastV1 = "csharpdb-migration-fail-fast/v1";
}

/// <summary>
/// A stable policy failure raised before an apply target may be mutated.
/// </summary>
public sealed class MigrationExecutionPolicyException : NotSupportedException
{
    public MigrationExecutionPolicyException(string code, string message)
        : base(message)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(code);
        Code = code;
    }

    public string Code { get; }
}

/// <summary>
/// Safe metadata for the first value rejected by deterministic fail-fast.
/// Source payloads, stable keys, and cursor values are deliberately excluded.
/// </summary>
public sealed class MigrationRowRejectedException : Exception
{
    internal MigrationRowRejectedException(
        string code,
        string sourceObjectId,
        string columnObjectId,
        long batchOrdinal,
        long sourceRowOrdinal,
        Exception innerException)
        : base(
            $"{code}: deterministic fail-fast rejected source object '{sourceObjectId}', " +
            $"batch {batchOrdinal}, row {sourceRowOrdinal}, column '{columnObjectId}'.",
            innerException)
    {
        Code = code;
        SourceObjectId = sourceObjectId;
        ColumnObjectId = columnObjectId;
        BatchOrdinal = batchOrdinal;
        SourceRowOrdinal = sourceRowOrdinal;
    }

    public string ContractVersion => MigrationRejectContract.DeterministicFailFastV1;

    public string Code { get; }

    public string SourceObjectId { get; }

    public string ColumnObjectId { get; }

    public long BatchOrdinal { get; }

    public long SourceRowOrdinal { get; }

    /// <summary>
    /// Creates a rejection reported by a migration source adapter without a
    /// free-form message or inner exception. Providers must pass stable rule
    /// and catalog object identifiers; this API bounds their token shape.
    /// </summary>
    public static MigrationRowRejectedException CreateForSource(
        string code,
        string sourceObjectId,
        string columnObjectId,
        long batchOrdinal,
        long sourceRowOrdinal)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(code);
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceObjectId);
        ArgumentException.ThrowIfNullOrWhiteSpace(columnObjectId);
        ArgumentOutOfRangeException.ThrowIfNegative(batchOrdinal);
        ArgumentOutOfRangeException.ThrowIfNegative(sourceRowOrdinal);
        if (!IsBoundedRuleId(code))
            throw new ArgumentException("The rejection code is not a bounded migration rule ID.", nameof(code));
        if (!IsBoundedIdentifier(sourceObjectId))
            throw new ArgumentException("The source object ID is not a bounded identifier.", nameof(sourceObjectId));
        if (!IsBoundedIdentifier(columnObjectId))
            throw new ArgumentException("The column object ID is not a bounded identifier.", nameof(columnObjectId));

        return new MigrationRowRejectedException(
            code,
            sourceObjectId,
            columnObjectId,
            batchOrdinal,
            sourceRowOrdinal,
            new InvalidDataException(
                "The source adapter rejected a value under the deterministic fail-fast contract."));
    }

    private static bool IsBoundedRuleId(string value) =>
        value.Length <= 128 &&
        value.StartsWith("MIG-", StringComparison.Ordinal) &&
        value.All(character =>
            character is >= 'A' and <= 'Z' or
                >= '0' and <= '9' or
                '-');

    private static bool IsBoundedIdentifier(string value) =>
        value.Length <= 512 && value.All(character => !char.IsControl(character));
}

/// <summary>
/// Validates the load policy before a staged target is created or changed.
/// </summary>
public static class MigrationApplyPolicyValidator
{
    public static void ValidateForExecution(MigrationPlan plan)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(plan.Load);

        if (!plan.Load.CreateStagedTarget)
        {
            throw new MigrationExecutionPolicyException(
                "MIG-APPLY-POLICY-STAGED-001",
                "Phase 2 apply supports only a new staged target.");
        }

        if (plan.Load.ResumeMode != MigrationResumeMode.TransactionalReceipts)
        {
            throw new MigrationExecutionPolicyException(
                "MIG-APPLY-POLICY-RESUME-001",
                "Phase 2 apply requires transactional receipts.");
        }

        if (plan.Load.RejectMode != MigrationRejectMode.FailFast)
        {
            throw new MigrationExecutionPolicyException(
                "MIG-APPLY-POLICY-REJECT-001",
                $"Phase 2 apply supports only '{MigrationRejectMode.FailFast}' row handling " +
                $"under contract '{MigrationRejectContract.DeterministicFailFastV1}'. " +
                "Durable skip-and-record rejects are not supported by this receipt schema.");
        }
    }
}
