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

    public const string DeterministicRejectsV1 =
        "csharpdb-migration-deterministic-rejects/v1";

    public const string RejectSetV1 = "csharpdb-migration-reject-set/v1";

    public const int MaximumRuleIdCharacters = 128;

    public const int MaximumObjectIdCharacters = 512;

    public const int MaximumAllowedRuleIds = 4_096;

    public const int MaximumRejectedRowsPerBatch = 65_536;

    public const long MaximumRejectedRowsPerRun = 1_000_000_000L;

    public const int MaximumEvidenceEntriesPerRow = 32;

    public const int MaximumEvidenceNameCharacters = 64;

    public const int MaximumEvidenceValueBytes = 4 * 1024;

    public const int MaximumEvidenceBytesPerRow = 16 * 1024;

    public const long MaximumEvidenceBytesPerBatch = 64L * 1024 * 1024;

    public const int MaximumRawValueBytes = MaximumEvidenceValueBytes;

    public const long MaximumRawValueBytesPerBatch = MaximumEvidenceBytesPerBatch;

    public const long MaximumRawValueBytesPerRun = 1024L * 1024 * 1024 * 1024;

    public const long MaximumArtifactBytes = 1024L * 1024 * 1024 * 1024;

    internal static bool IsBoundedRuleId(string value) =>
        value.Length <= MaximumRuleIdCharacters &&
        value.StartsWith("MIG-", StringComparison.Ordinal) &&
        value.All(character =>
            character is >= 'A' and <= 'Z' or
                >= '0' and <= '9' or
                '-');

    internal static bool IsBoundedIdentifier(string value) =>
        value.Length <= MaximumObjectIdCharacters &&
        value.All(character => !char.IsControl(character));
}

/// <summary>
/// Validates the reject-policy portion of a provider read request without
/// requiring the provider to reconstruct a complete migration plan.
/// </summary>
public static class MigrationRejectReadPolicyValidator
{
    public static void Validate(MigrationReadRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        Validate(
            request.RejectContractVersion,
            request.RejectPolicy,
            request.BatchSize);
    }

    public static void Validate(
        string rejectContractVersion,
        MigrationDeterministicRejectPolicy? rejectPolicy,
        int batchSize)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(rejectContractVersion);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);

        MigrationRejectMode mode = rejectContractVersion switch
        {
            MigrationRejectContract.DeterministicFailFastV1 => MigrationRejectMode.FailFast,
            MigrationRejectContract.DeterministicRejectsV1 => MigrationRejectMode.DeterministicRejects,
            _ => throw new InvalidDataException(
                "Migration read reject contract version is unsupported."),
        };
        MigrationDeterministicRejectPolicyValidator.Validate(new MigrationLoadPolicy
        {
            BatchSize = batchSize,
            RejectMode = mode,
            RejectPolicy = rejectPolicy,
        });
    }
}

internal static class MigrationDeterministicRejectPolicyValidator
{
    internal static void Validate(MigrationLoadPolicy load)
    {
        ArgumentNullException.ThrowIfNull(load);

        switch (load.RejectMode)
        {
            case MigrationRejectMode.FailFast:
                if (load.RejectPolicy is not null)
                {
                    throw new InvalidDataException(
                        "Fail-fast migration plans cannot contain a deterministic reject policy.");
                }

                return;

            case MigrationRejectMode.DeterministicRejects:
                if (load.RejectPolicy is null)
                {
                    throw new InvalidDataException(
                        "Deterministic reject mode requires a plan-bound reject policy.");
                }

                ValidateDeterministic(load.RejectPolicy, load.BatchSize);
                return;

            default:
                throw new InvalidDataException("Migration reject mode is unsupported.");
        }
    }

    private static void ValidateDeterministic(
        MigrationDeterministicRejectPolicy policy,
        int batchSize)
    {
        if (!string.Equals(
                policy.ContractVersion,
                MigrationRejectContract.DeterministicRejectsV1,
                StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                "Deterministic reject policy contract version is unsupported.");
        }

        IReadOnlyList<string> allowedRuleIds = policy.AllowedRuleIds ??
            throw new InvalidDataException("Allowed reject rule ids cannot be null.");
        if (allowedRuleIds.Count == 0 ||
            allowedRuleIds.Count > MigrationRejectContract.MaximumAllowedRuleIds)
        {
            throw new InvalidDataException(
                "Deterministic reject policy must contain a bounded, nonempty rule registry.");
        }

        var uniqueRuleIds = new HashSet<string>(StringComparer.Ordinal);
        foreach (string? ruleId in allowedRuleIds)
        {
            if (ruleId is null ||
                !MigrationRejectContract.IsBoundedRuleId(ruleId) ||
                !uniqueRuleIds.Add(ruleId))
            {
                throw new InvalidDataException(
                    "Deterministic reject policy contains an invalid or duplicate rule id.");
            }
        }

        RequirePositiveBound(
            policy.MaxRejectedRowsPerBatch,
            MigrationRejectContract.MaximumRejectedRowsPerBatch,
            "Maximum rejected rows per batch");
        RequirePositiveBound(
            policy.MaxRejectedRowsPerRun,
            MigrationRejectContract.MaximumRejectedRowsPerRun,
            "Maximum rejected rows per run");
        RequirePositiveBound(
            policy.MaxRawValueBytes,
            MigrationRejectContract.MaximumRawValueBytes,
            "Maximum raw value bytes");
        RequirePositiveBound(
            policy.MaxRawValueBytesPerBatch,
            MigrationRejectContract.MaximumRawValueBytesPerBatch,
            "Maximum raw value bytes per batch");
        RequirePositiveBound(
            policy.MaxRawValueBytesPerRun,
            MigrationRejectContract.MaximumRawValueBytesPerRun,
            "Maximum raw value bytes per run");
        RequirePositiveBound(
            policy.MaxArtifactBytes,
            MigrationRejectContract.MaximumArtifactBytes,
            "Maximum reject artifact bytes");
        if (policy.MaxArtifactBytes < MigrationRejectLedgerCodec.MinimumCanonicalArtifactBytes)
        {
            throw new InvalidDataException(
                "Maximum reject artifact bytes cannot fit the canonical artifact header.");
        }

        if (policy.MaxRejectedRowsPerBatch > batchSize ||
            policy.MaxRejectedRowsPerBatch > policy.MaxRejectedRowsPerRun)
        {
            throw new InvalidDataException(
                "Maximum rejected rows per batch cannot exceed the batch or run limit.");
        }

        if (policy.MaxRawValueBytes > policy.MaxRawValueBytesPerBatch ||
            policy.MaxRawValueBytesPerBatch > policy.MaxRawValueBytesPerRun)
        {
            throw new InvalidDataException(
                "Raw value byte limits must be ordered from value to batch to run.");
        }
    }

    private static void RequirePositiveBound(long value, long ceiling, string description)
    {
        if (value <= 0 || value > ceiling)
            throw new InvalidDataException($"{description} is outside the supported bounds.");
    }
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
        if (!MigrationRejectContract.IsBoundedRuleId(code))
            throw new ArgumentException("The rejection code is not a bounded migration rule ID.", nameof(code));
        if (!MigrationRejectContract.IsBoundedIdentifier(sourceObjectId))
            throw new ArgumentException("The source object ID is not a bounded identifier.", nameof(sourceObjectId));
        if (!MigrationRejectContract.IsBoundedIdentifier(columnObjectId))
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

}

/// <summary>
/// Validates the load policy for a staged target binding. This intentionally
/// permits a valid deterministic reject policy before the execution path is
/// enabled, so target durability can be qualified independently.
/// </summary>
internal static class MigrationStagedTargetPolicyValidator
{
    internal static void ValidateForBinding(MigrationPlan plan)
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

        MigrationDeterministicRejectPolicyValidator.Validate(plan.Load);
        if (plan.Load.RejectMode == MigrationRejectMode.DeterministicRejects &&
            plan.Objects.Any(item => item.Included &&
                !MigrationRejectContract.IsBoundedIdentifier(item.SourceObjectId)))
        {
            throw new InvalidDataException(
                "Deterministic reject plans require bounded included object identifiers.");
        }
    }
}

/// <summary>
/// Validates the load policy before the normal apply execution path may mutate
/// a target.
/// </summary>
public static class MigrationApplyPolicyValidator
{
    public static void ValidateForExecution(MigrationPlan plan)
    {
        MigrationStagedTargetPolicyValidator.ValidateForBinding(plan);

        if (plan.Load.RejectMode != MigrationRejectMode.FailFast)
        {
            throw new MigrationExecutionPolicyException(
                "MIG-APPLY-POLICY-REJECT-001",
                $"This strict migration apply entry point supports only '{MigrationRejectMode.FailFast}' row handling " +
                $"under contract '{MigrationRejectContract.DeterministicFailFastV1}'. " +
                "Deterministic rejects require the capability-qualified SDK apply path.");
        }
    }

    /// <summary>
    /// Capability-qualified policy gate used by the provider-neutral apply
    /// runner. The plan-only overload remains the strict fail-fast boundary;
    /// explicitly opted-in coordinators use this source-and-target gate.
    /// </summary>
    public static void ValidateForExecution(
        MigrationPlan plan,
        IMigrationDataSource source,
        IMigrationTarget target)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(target);
        MigrationStagedTargetPolicyValidator.ValidateForBinding(plan);
        if (plan.Load.RejectMode == MigrationRejectMode.FailFast)
            return;

        if (source is not IMigrationRejectAwareDataSource)
        {
            ValidateForExecution(plan);
            return;
        }

        MigrationRejectSourceCapabilityValidator.ValidateForExecution(plan, source);

        if (target is not IMigrationRejectLedgerTarget)
        {
            throw UnsupportedCapability(
                "MIG-APPLY-POLICY-REJECT-TARGET-001",
                "The migration target does not advertise an authoritative reject ledger.");
        }

        if (target is not IMigrationBatchDigestContractTarget digestTarget ||
            !string.Equals(
                digestTarget.BatchDigestFormat,
                MigrationBatchDigest.Format,
                StringComparison.Ordinal))
        {
            throw UnsupportedCapability(
                "MIG-APPLY-POLICY-REJECT-TARGET-001",
                "Deterministic rejects require the current migration batch digest contract.");
        }
    }

    private static MigrationExecutionPolicyException UnsupportedCapability(
        string code,
        string message) => new(code, message);
}

/// <summary>
/// Provider-neutral source capability gate shared by apply and immutable
/// validation replay. Target qualification remains the coordinator's
/// responsibility.
/// </summary>
public static class MigrationRejectSourceCapabilityValidator
{
    /// <summary>
    /// Verifies that a source advertises the deterministic reject contract and
    /// every rule selected by the plan. This capability-only preflight does not
    /// enumerate source rows or mutate source or target state.
    /// </summary>
    public static void ValidateForExecution(
        MigrationPlan plan,
        IMigrationDataSource source)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(plan.Load);
        ArgumentNullException.ThrowIfNull(source);
        if (plan.Load.RejectMode == MigrationRejectMode.FailFast)
            return;

        MigrationDeterministicRejectPolicy policy = plan.Load.RejectPolicy ??
            throw new InvalidDataException(
                "Deterministic reject mode requires a plan-bound reject policy.");
        if (source is not IMigrationRejectAwareDataSource rejectAwareSource)
        {
            throw UnsupportedCapability(
                "MIG-APPLY-POLICY-REJECT-SOURCE-001",
                "The migration source does not advertise deterministic reject replay.");
        }

        if (!string.Equals(
                rejectAwareSource.RejectContractVersion,
                policy.ContractVersion,
                StringComparison.Ordinal))
        {
            throw UnsupportedCapability(
                "MIG-APPLY-POLICY-REJECT-SOURCE-001",
                "The migration source does not advertise the selected deterministic reject contract.");
        }

        IReadOnlySet<string> supportedRules = rejectAwareSource.SupportedRejectRuleIds ??
            throw UnsupportedCapability(
                "MIG-APPLY-POLICY-REJECT-SOURCE-001",
                "The migration source reject-rule registry is unavailable.");
        foreach (string ruleId in policy.AllowedRuleIds)
        {
            if (!supportedRules.Any(supported =>
                    string.Equals(supported, ruleId, StringComparison.Ordinal)))
            {
                throw UnsupportedCapability(
                    "MIG-APPLY-POLICY-REJECT-RULE-001",
                    "The migration source does not advertise every plan-selected reject rule.");
            }
        }
    }

    private static MigrationExecutionPolicyException UnsupportedCapability(
        string code,
        string message) => new(code, message);
}
