using CSharpDB.Primitives;

namespace CSharpDB.Migration;

public sealed record MigrationInspectionRequest
{
    public required string TargetCSharpDbVersion { get; init; }

    public bool IncludeProfile { get; init; }

    public int ProfileSampleSize { get; init; } = 1_000;
}

public interface IMigrationSourceInspector
{
    MigrationSourceKind SourceKind { get; }

    ValueTask<MigrationCatalog> InspectAsync(
        MigrationInspectionRequest request,
        CancellationToken cancellationToken = default);
}

public enum MigrationSourceValueKind
{
    Null,
    Boolean,
    SignedInteger,
    UnsignedInteger,
    Decimal,
    FloatingPoint,
    Text,
    Binary,
    Guid,
    Date,
    Time,
    DateTime,
    DateTimeOffset,
    Json,
    Native,
}

/// <summary>
/// A lossless source-side scalar. Text values use a provider-defined invariant
/// representation; binary values use <see cref="BinaryValue"/>.
/// </summary>
public sealed record MigrationSourceValue
{
    public required MigrationSourceValueKind Kind { get; init; }

    public string? CanonicalText { get; init; }

    public ReadOnlyMemory<byte> BinaryValue { get; init; }
}

public sealed record MigrationDataRow
{
    public string? StableKey { get; init; }

    public IReadOnlyList<MigrationSourceValue> Values { get; init; } = [];
}

/// <summary>
/// One bounded, provider-defined piece of deterministic rejection evidence.
/// Names and values are validated by <see cref="MigrationRejectDigest"/> and
/// must be emitted in ordinal name order.
/// </summary>
public sealed record MigrationRejectEvidence
{
    public required string Name { get; init; }

    public string? Value { get; init; }

    public override string ToString() =>
        $"{nameof(MigrationRejectEvidence)} {{ Name = {Name}, Value = <redacted> }}";
}

/// <summary>
/// The first deterministic failure for one attempted source row. Free-form
/// exception messages are deliberately excluded from this durable contract.
/// </summary>
public sealed record MigrationRejectedRow
{
    public long SourceRowOrdinal { get; init; }

    public required string RuleId { get; init; }

    public string? ColumnObjectId { get; init; }

    public IReadOnlyList<MigrationRejectEvidence> Evidence { get; init; } = [];
}

public sealed record MigrationDataBatch
{
    public required string SourceObjectId { get; init; }

    public required string SnapshotIdentity { get; init; }

    public IReadOnlyList<string> ColumnObjectIds { get; init; } = [];

    public long BatchOrdinal { get; init; }

    public string? StartCursor { get; init; }

    public string? NextCursor { get; init; }

    public IReadOnlyList<MigrationDataRow> Rows { get; init; } = [];

    /// <summary>
    /// Recoverable row-local source failures. A source may populate this only
    /// under a reject-aware execution contract; fatal structural failures are
    /// still raised as exceptions.
    /// </summary>
    public IReadOnlyList<MigrationRejectedRow> RejectedRows { get; init; } = [];
}

public sealed record MigrationReadRequest
{
    public required string SourceObjectId { get; init; }

    public IReadOnlyList<string> ColumnObjectIds { get; init; } = [];

    public int BatchSize { get; init; } = 1_000;

    /// <summary>
    /// Maximum combined canonical payload retained for a single batch.
    /// Sources must split earlier when this limit would be exceeded.
    /// </summary>
    public long MaxBatchBytes { get; init; } = 64L * 1024 * 1024;

    /// <summary>Maximum canonical size of one scalar value, including BLOBs.</summary>
    public int MaxValueBytes { get; init; } = 16 * 1024 * 1024;

    public string? ResumeCursor { get; init; }

    public string? SnapshotToken { get; init; }
}

public interface IMigrationDataSource : IAsyncDisposable
{
    MigrationSourceIdentity Source { get; }

    /// <summary>
    /// The actual immutable snapshot, backup, transaction, or watermark bound
    /// to this read session. It must remain stable for the source lifetime.
    /// </summary>
    string SnapshotIdentity { get; }

    IAsyncEnumerable<MigrationDataBatch> ReadAsync(
        MigrationReadRequest request,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Optional stronger binding for sources whose runtime scalar policy depends
/// on the exact inspected catalog. Coordinators reject a different plan
/// catalog before reading.
/// </summary>
public interface IMigrationCatalogBoundDataSource
{
    string CatalogDigest { get; }
}

public sealed record MigrationTypeMappingRequest
{
    public required MigrationCatalogObject SourceObject { get; init; }

    public required MigrationMappingProfile Profile { get; init; }

    public required MigrationProfileCoverage Coverage { get; init; }

    public DbType? CustomTargetType { get; init; }
}

public sealed record MigrationTypeMappingDecision
{
    public required MigrationTypeMapping Mapping { get; init; }

    public MigrationDiagnostic? Diagnostic { get; init; }
}

public interface IDataTypeMappingProvider
{
    string PolicyId { get; }

    int PolicyVersion { get; }

    MigrationTypeMappingDecision Map(MigrationTypeMappingRequest request);
}

public enum MigrationSchemaStage
{
    LoadEssential,
    SecondaryIndexes,
    Constraints,
    Views,
    Triggers,
}

public sealed record MigrationTargetRow
{
    public long SourceRowOrdinal { get; init; }

    public string? StableKey { get; init; }

    public IReadOnlyList<DbValue> Values { get; init; } = [];
}

public sealed record MigrationTargetBatch
{
    public required string PlanDigest { get; init; }

    public required string CatalogDigest { get; init; }

    public required string SourceFingerprint { get; init; }

    public required string SourceSnapshotIdentity { get; init; }

    public required string SourceObjectId { get; init; }

    public IReadOnlyList<string> ColumnObjectIds { get; init; } = [];

    public long BatchOrdinal { get; init; }

    public string? StartCursor { get; init; }

    public string? NextCursor { get; init; }

    public required string BatchDigest { get; init; }

    public string RejectContractVersion { get; init; } =
        MigrationRejectContract.DeterministicFailFastV1;

    public string RejectDigest { get; init; } = string.Empty;

    public IReadOnlyList<MigrationTargetRow> Rows { get; init; } = [];

    public IReadOnlyList<MigrationRejectedRow> RejectedRows { get; init; } = [];
}

public sealed record MigrationBatchReceipt
{
    public required string TargetIdentity { get; init; }

    public required string PlanDigest { get; init; }

    public required string CatalogDigest { get; init; }

    public required string SourceFingerprint { get; init; }

    public required string SourceSnapshotIdentity { get; init; }

    public required string SourceObjectId { get; init; }

    public long BatchOrdinal { get; init; }

    public string? StartCursor { get; init; }

    public string? NextCursor { get; init; }

    public required string BatchDigest { get; init; }

    public string RejectContractVersion { get; init; } =
        MigrationRejectContract.DeterministicFailFastV1;

    public string RejectDigest { get; init; } = string.Empty;

    public long RowCount { get; init; }

    public long RejectedRowCount { get; init; }
}

/// <summary>
/// Optional target capability used to resume receipts written with an older
/// canonical batch-digest format. Targets that do not implement this
/// interface are treated as current-format targets.
/// </summary>
public interface IMigrationBatchDigestContractTarget
{
    string BatchDigestFormat { get; }
}

public interface IMigrationTarget : IAsyncDisposable
{
    string TargetIdentity { get; }

    ValueTask ApplySchemaAsync(
        MigrationPlan plan,
        MigrationCatalog catalog,
        MigrationSchemaStage stage,
        CancellationToken cancellationToken = default);

    ValueTask<MigrationBatchReceipt> WriteBatchAsync(
        MigrationTargetBatch batch,
        CancellationToken cancellationToken = default);

    ValueTask<MigrationBatchReceipt?> ReadReceiptAsync(
        string planDigest,
        string sourceObjectId,
        long batchOrdinal,
        CancellationToken cancellationToken = default);

    IAsyncEnumerable<MigrationBatchReceipt> ReadReceiptsAsync(
        string planDigest,
        string sourceObjectId,
        CancellationToken cancellationToken = default);

    ValueTask<IValidationSnapshot> OpenValidationSnapshotAsync(
        CancellationToken cancellationToken = default);
}

public sealed record MigrationValidationRow
{
    public string? StableKey { get; init; }

    public IReadOnlyList<DbValue> Values { get; init; } = [];
}

public interface IValidationSnapshot : IAsyncDisposable
{
    string SnapshotIdentity { get; }

    ValueTask<long> CountAsync(
        string objectId,
        CancellationToken cancellationToken = default);

    IAsyncEnumerable<MigrationValidationRow> ReadRowsAsync(
        string objectId,
        CancellationToken cancellationToken = default);
}

public enum MigrationValidationLevel
{
    Schema,
    Count,
    Checksum,
    Rows,
    Queries,
}

public enum MigrationValidationStatus
{
    Passed,
    Different,
    Error,
    Skipped,
    Inconclusive,
}

public sealed record MigrationValidationRequest
{
    public required MigrationPlan Plan { get; init; }

    public required MigrationValidationLevel Level { get; init; }
}

public sealed record MigrationValidationResult
{
    public required MigrationValidationStatus Status { get; init; }

    public IReadOnlyList<MigrationDiagnostic> Diagnostics { get; init; } = [];
}

public interface IMigrationValidator
{
    ValueTask<MigrationValidationResult> ValidateAsync(
        MigrationValidationRequest request,
        IValidationSnapshot source,
        IValidationSnapshot target,
        CancellationToken cancellationToken = default);
}
