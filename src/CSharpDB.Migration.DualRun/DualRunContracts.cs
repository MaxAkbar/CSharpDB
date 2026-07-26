using CSharpDB.Migration.Canonicalization;

namespace CSharpDB.Migration.DualRun;

public static class DualRunReportFormats
{
    public const string V1 = "csharpdb-dual-run-report/v1";

    public const string DigestAlgorithm = "sha256";

    public const string CanonicalizationId = CanonicalRowCodec.CanonicalizationId;

    public const string CanonicalizationContractHash = CanonicalRowCodec.ContractHashHex;
}

public enum DualRunOrdering
{
    Ordered,
    Unordered,
}

public enum DualRunReadOnlyEnforcement
{
    StatementValidated,
    ReadOnlyConnection,
    StatementValidatedAndReadOnlyConnection,
}

public enum DualRunValidationStatus
{
    Passed,
    Different,
    Inconclusive,
}

public enum DualRunEndpointStatus
{
    Succeeded,
    Failed,
}

public enum DualRunErrorKind
{
    SafetyRejected,
    TimedOut,
    LimitExceeded,
    InvalidResult,
    ProviderError,
}

public sealed record DualRunLimits
{
    public int MaxRows { get; init; } = 10_000;

    public int MaxColumns { get; init; } = 256;

    public int MaxCellBytes { get; init; } = 1024 * 1024;

    public long MaxTotalCanonicalBytesPerEndpoint { get; init; } = 64L * 1024 * 1024;

    public int MaxMismatchDetails { get; init; } = 100;

    public TimeSpan TimeoutPerEndpoint { get; init; } = TimeSpan.FromSeconds(30);
}

/// <summary>
/// One parameter shared by the source and target query text. Values are never
/// copied into a dual-run report. Only a digest of the complete invocation is
/// retained as evidence.
/// </summary>
public sealed record DualRunParameter
{
    public required string Name { get; init; }

    public required CanonicalType Type { get; init; }

    public object? Value { get; init; }
}

/// <summary>
/// Optional logical result contract. Use it when a source type is intentionally
/// represented by a different physical CSharpDB type (for example, a date
/// represented as canonical text). When omitted, each executor's inferred
/// canonical schema is compared directly.
/// </summary>
public sealed record DualRunColumnContract
{
    public required string Name { get; init; }

    public required CanonicalType Type { get; init; }
}

public sealed record DualRunQueryCase
{
    public required string CaseId { get; init; }

    /// <summary>
    /// Non-secret, content-pinned identity of the coherent source snapshot.
    /// </summary>
    public required string SourceSnapshotIdentity { get; init; }

    /// <summary>
    /// Non-secret, content-pinned identity of the coherent target snapshot.
    /// </summary>
    public required string TargetSnapshotIdentity { get; init; }

    public string CanonicalizationId { get; init; } =
        DualRunReportFormats.CanonicalizationId;

    public string CanonicalizationContractHash { get; init; } =
        DualRunReportFormats.CanonicalizationContractHash;

    public required string SourceSql { get; init; }

    public required string TargetSql { get; init; }

    public DualRunOrdering Ordering { get; init; }

    public IReadOnlyList<DualRunParameter> Parameters { get; init; } = [];

    public IReadOnlyList<DualRunColumnContract> Columns { get; init; } = [];

    public DualRunLimits Limits { get; init; } = new();
}

public sealed record DualRunExecutionRequest
{
    public required string CaseId { get; init; }

    public required string SnapshotIdentity { get; init; }

    public required string CanonicalizationId { get; init; }

    public required string CanonicalizationContractHash { get; init; }

    public required string Sql { get; init; }

    public IReadOnlyList<DualRunParameter> Parameters { get; init; } = [];

    public IReadOnlyList<DualRunColumnContract> Columns { get; init; } = [];

    public required DualRunLimits Limits { get; init; }
}

public sealed record DualRunResultColumn
{
    public required string Name { get; init; }

    public required CanonicalType InferredType { get; init; }
}

/// <summary>
/// A provider execution that has already accepted the statement as read-only.
/// Implementations must stream at most one tabular result and honor the supplied
/// cancellation token.
/// </summary>
public interface IDualRunQueryExecution : IAsyncDisposable
{
    IReadOnlyList<DualRunResultColumn> Columns { get; }

    IAsyncEnumerable<IReadOnlyList<object?>> ReadRowsAsync(
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Provider boundary for a read-only query. Implementations are responsible for
/// enforcing their declared read-only guarantee before sending SQL to a server.
/// The engine rejects executors that do not declare such a guarantee.
/// </summary>
public interface IDualRunQueryExecutor
{
    string ProviderId { get; }

    /// <summary>
    /// Non-secret, content-pinned identity of the coherent snapshot opened by
    /// this executor.
    /// </summary>
    string SnapshotIdentity { get; }

    DualRunReadOnlyEnforcement ReadOnlyEnforcement { get; }

    string ReadOnlyValidatorId { get; }

    ValueTask<IDualRunQueryExecution> ExecuteReadOnlyAsync(
        DualRunExecutionRequest request,
        CancellationToken cancellationToken = default);
}

public sealed record DualRunReadOnlyValidation
{
    public required bool IsReadOnly { get; init; }

    public string RejectionCode { get; init; } = "DUALRUN_READ_ONLY_REQUIRED";
}

/// <summary>
/// Mandatory statement guard used by generic provider executors. Implementations
/// must make a fail-closed decision without executing the SQL.
/// </summary>
public interface IDualRunReadOnlyStatementValidator
{
    string ValidatorId { get; }

    DualRunReadOnlyValidation Validate(string sql);
}

public sealed class DualRunExecutionException : Exception
{
    public DualRunExecutionException(DualRunErrorKind kind, string code)
        : base(code)
    {
        Kind = kind;
        Code = code;
    }

    public DualRunExecutionException(DualRunErrorKind kind, string code, Exception innerException)
        : base(code, innerException)
    {
        Kind = kind;
        Code = code;
    }

    public DualRunErrorKind Kind { get; }

    public string Code { get; }
}

public sealed record DualRunReport
{
    public required string CaseId { get; init; }

    public required string CanonicalizationId { get; init; }

    public required string CanonicalizationContractHash { get; init; }

    public required string InvocationDigest { get; init; }

    public required DualRunOrdering Ordering { get; init; }

    public required DualRunValidationStatus Status { get; init; }

    public required DualRunReportLimits Limits { get; init; }

    public required DualRunEndpointEvidence Source { get; init; }

    public required DualRunEndpointEvidence Target { get; init; }

    public IReadOnlyList<DualRunDifference> Differences { get; init; } = [];
}

public sealed record DualRunReportLimits
{
    public required int MaxRows { get; init; }

    public required int MaxColumns { get; init; }

    public required int MaxCellBytes { get; init; }

    public required long MaxTotalCanonicalBytesPerEndpoint { get; init; }

    public required int MaxMismatchDetails { get; init; }

    public required long TimeoutPerEndpointMilliseconds { get; init; }
}

public sealed record DualRunEndpointEvidence
{
    public required string ProviderId { get; init; }

    public required string SnapshotIdentity { get; init; }

    public required DualRunReadOnlyEnforcement ReadOnlyEnforcement { get; init; }

    public required string ReadOnlyValidatorId { get; init; }

    public required DualRunEndpointStatus Status { get; init; }

    public int? ColumnCount { get; init; }

    public long? RowCount { get; init; }

    public string? SchemaDigest { get; init; }

    public string? ResultDigest { get; init; }

    public DualRunEndpointError? Error { get; init; }
}

public sealed record DualRunEndpointError
{
    public required DualRunErrorKind Kind { get; init; }

    public required string Code { get; init; }
}

public sealed record DualRunDifference
{
    public required string Code { get; init; }

    public string? Endpoint { get; init; }

    public long? RowOrdinal { get; init; }

    public string? RowDigest { get; init; }

    public long? SourceCount { get; init; }

    public long? TargetCount { get; init; }
}

public static class DualRunDifferenceCodes
{
    public const string EndpointFailed = "DUALRUN_ENDPOINT_FAILED";
    public const string SchemaMismatch = "DUALRUN_SCHEMA_MISMATCH";
    public const string RowCountMismatch = "DUALRUN_ROW_COUNT_MISMATCH";
    public const string OrderedRowMismatch = "DUALRUN_ORDERED_ROW_MISMATCH";
    public const string UnorderedRowMultiplicityMismatch = "DUALRUN_UNORDERED_ROW_MULTIPLICITY_MISMATCH";
}

public sealed record DualRunReportEnvelope
{
    public required string Format { get; init; }

    public required string DigestAlgorithm { get; init; }

    public required string Digest { get; init; }

    public required DualRunReport Payload { get; init; }
}
