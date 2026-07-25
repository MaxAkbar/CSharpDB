using System.Collections.ObjectModel;

namespace CSharpDB.Migration;

/// <summary>
/// The terminal outcome of isolated EF Core migration scratch analysis.
/// </summary>
public enum EfCoreMigrationScratchAnalysisOutcome
{
    Passed,
    Blocked,
    Failed,
}

/// <summary>
/// The database state against which scratch-chain evidence was collected.
/// </summary>
public enum EfCoreMigrationScratchProofScope
{
    EmptyDatabase,
}

/// <summary>
/// Fixed rule identifiers for EF Core migration scratch analysis.
/// </summary>
public static class EfCoreMigrationScratchAnalysisRules
{
    public const string ScratchPassed =
        "csharpdb.ef.scratch.passed";
    public const string GenerationPreflightBlocked =
        "csharpdb.ef.scratch.generation-preflight-blocked";
    public const string ScratchExecutionFailed =
        "csharpdb.ef.scratch.execution-failed";
    public const string SchemaDifferent =
        "csharpdb.ef.scratch.schema-different";
    public const string RoundTripDifferent =
        "csharpdb.ef.scratch.round-trip-different";
    public const string IdempotenceFailed =
        "csharpdb.ef.scratch.idempotence-failed";
    public const string AnalysisLimit =
        "csharpdb.ef.scratch.analysis-limit";
    public const string ResourceDisposalFailed =
        "csharpdb.ef.scratch.resource-disposal-failed";
}

/// <summary>
/// Sanitized schema and migration-history evidence for one migration prefix.
/// Names, SQL, exception text, and database paths are deliberately absent.
/// </summary>
public sealed record EfCoreMigrationScratchPrefixEvidence
{
    public required int Ordinal { get; init; }

    public required int MigrationOrdinal { get; init; }

    public required MigrationCompatibilityStatus Status { get; init; }

    public required MigrationEvidenceLevel Evidence { get; init; }

    public required string RuleId { get; init; }

    public required string ExpectedSchemaDigest { get; init; }

    public required string ExpectedHistoryDigest { get; init; }

    public string? AppliedSchemaDigest { get; init; }

    public string? AppliedHistoryDigest { get; init; }

    public string? DownSchemaDigest { get; init; }

    public string? DownHistoryDigest { get; init; }

    public string? ReappliedSchemaDigest { get; init; }

    public string? ReappliedHistoryDigest { get; init; }
}

/// <summary>
/// Bounded proof collected while executing a compiled migration chain against
/// isolated empty CSharpDB databases.
/// </summary>
public sealed record EfCoreMigrationScratchChainProof
{
    private IReadOnlyList<EfCoreMigrationScratchPrefixEvidence> prefixes =
        EfCoreMigrationScratchContractCollections
            .Empty<EfCoreMigrationScratchPrefixEvidence>();

    public const string CurrentFormat =
        "csharpdb-ef-scratch-chain/v1";

    public const string EmptyChainAlgorithm =
        "csharpdb-ef-empty-chain/v1";

    public string Format { get; init; } = CurrentFormat;

    public string Algorithm { get; init; } = EmptyChainAlgorithm;

    public EfCoreMigrationScratchProofScope ProofScope { get; init; } =
        EfCoreMigrationScratchProofScope.EmptyDatabase;

    /// <summary>
    /// Empty-database scratch execution cannot prove conversions against
    /// application data. Seeded-clone or source-data preflight is a later tier.
    /// </summary>
    public bool DataPreflightCompleted { get; init; }

    public required EfCoreMigrationScratchAnalysisOutcome Outcome
    {
        get;
        init;
    }

    public int PrefixCount { get; init; }

    public int AppliedPrefixCount { get; init; }

    public int SchemaVerifiedPrefixCount { get; init; }

    public int DownPrefixCount { get; init; }

    public int ReappliedPrefixCount { get; init; }

    public int RoundTripVerifiedPrefixCount { get; init; }

    public int IdempotentApplyCount { get; init; }

    public int ExecutedCommandCount { get; init; }

    public int IdempotentCommandCount { get; init; }

    /// <summary>
    /// A domain-separated digest of commands executed during prefix, down, and
    /// reapply proof. Required exactly when
    /// <see cref="ExecutedCommandCount"/> is non-zero.
    /// </summary>
    public string? ExecutedSqlDigest { get; init; }

    /// <summary>
    /// A domain-separated digest of commands executed while applying
    /// idempotent output. Required exactly when
    /// <see cref="IdempotentCommandCount"/> is non-zero.
    /// </summary>
    public string? IdempotentSqlDigest { get; init; }

    public string? FirstIdempotentSchemaDigest { get; init; }

    public string? FirstIdempotentHistoryDigest { get; init; }

    public string? SecondIdempotentSchemaDigest { get; init; }

    public string? SecondIdempotentHistoryDigest { get; init; }

    public bool ResourcesDisposed { get; init; }

    public IReadOnlyList<EfCoreMigrationScratchPrefixEvidence> Prefixes
    {
        get => prefixes;
        init => prefixes =
            EfCoreMigrationScratchContractCollections.Copy(value);
    }
}

/// <summary>
/// The public envelope for generation preflight plus empty-database
/// scratch-chain evidence. The embedded generation report remains its original
/// v1 contract.
/// </summary>
public sealed record EfCoreMigrationScratchAnalysisReport
{
    private IReadOnlyList<EfCoreMigrationAnalysisDiagnostic> diagnostics =
        EfCoreMigrationScratchContractCollections
            .Empty<EfCoreMigrationAnalysisDiagnostic>();

    public const string CurrentFormat =
        "csharpdb-ef-migration-scratch-analysis/v1";

    public string Format { get; init; } = CurrentFormat;

    public required EfCoreMigrationScratchAnalysisOutcome Outcome
    {
        get;
        init;
    }

    public required MigrationCompatibilityStatus Status { get; init; }

    public required MigrationEvidenceLevel HighestEvidence { get; init; }

    public required string RuleId { get; init; }

    public required EfCoreMigrationAnalysisReport GenerationPreflight
    {
        get;
        init;
    }

    public required EfCoreMigrationScratchChainProof ScratchChain
    {
        get;
        init;
    }

    public IReadOnlyList<EfCoreMigrationAnalysisDiagnostic> Diagnostics
    {
        get => diagnostics;
        init => diagnostics =
            EfCoreMigrationScratchContractCollections.Copy(value);
    }
}

internal static class EfCoreMigrationScratchContractCollections
{
    public static IReadOnlyList<T> Empty<T>() =>
        Array.AsReadOnly(Array.Empty<T>());

    public static IReadOnlyList<T> Copy<T>(IEnumerable<T> items)
    {
        ArgumentNullException.ThrowIfNull(items);
        return new ReadOnlyCollection<T>(items.ToArray());
    }
}
