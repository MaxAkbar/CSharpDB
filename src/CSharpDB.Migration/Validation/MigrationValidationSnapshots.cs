using System.Runtime.CompilerServices;
using CSharpDB.Migration.Validation;
using CSharpDB.Primitives;

namespace CSharpDB.Migration;

/// <summary>
/// Snapshot extension required by the Phase 3 runner. Schema, counts, and rows
/// must all be read from this one snapshot instance.
/// </summary>
public interface IMigrationEvidenceValidationSnapshot : IMigrationSchemaValidationSnapshot
{
    MigrationSnapshotConsistencyStatus ConsistencyStatus { get; }
}

public sealed record MigrationValidationActivationReceipt
{
    public const string ContractVersion = "csharpdb-migration-activation/v1";

    public required string TargetIdentity { get; init; }

    public required string PlanDigest { get; init; }

    public required string CatalogDigest { get; init; }

    public required string SourceSnapshotIdentity { get; init; }

    public required string TargetSnapshotIdentity { get; init; }

    public required MigrationValidationLevel Level { get; init; }

    public required string CanonicalizationVersion { get; init; }

    public required string CanonicalizationContractDigest { get; init; }

    public required string ReportDigest { get; init; }
}

/// <summary>
/// Opaque, runner-issued authority to ask a staged target to activate a
/// published validation report. Public callers can pass permits through but
/// cannot construct one without completing the validation runner's publish
/// boundary.
/// </summary>
public sealed class MigrationValidationActivationPermit
{
    internal MigrationValidationActivationPermit(
        MigrationValidationActivationReceipt receipt,
        string publishedReportPath)
    {
        ArgumentNullException.ThrowIfNull(receipt);
        ArgumentException.ThrowIfNullOrWhiteSpace(publishedReportPath);
        Receipt = receipt;
        PublishedReportPath = Path.GetFullPath(publishedReportPath);
    }

    public MigrationValidationActivationReceipt Receipt { get; }

    public string PublishedReportPath { get; }
}

/// <summary>
/// Narrow capability implemented by staged targets that can atomically persist
/// the validation receipt and transition to their activated lifecycle state.
/// </summary>
public interface IMigrationValidationActivationTarget
{
    ValueTask<MigrationValidationActivationReceipt?> ReadActivationReceiptAsync(
        CancellationToken cancellationToken = default);

    ValueTask ActivateAsync(
        MigrationValidationActivationPermit permit,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Replays a migration data source's immutable bound snapshot through the same
/// planned conversions used during apply, yielding target-shaped rows for
/// canonical logical comparison.
/// </summary>
public sealed class MigrationDataSourceValidationSnapshot : IMigrationEvidenceValidationSnapshot
{
    private readonly MigrationPlan _plan;
    private readonly MigrationCatalog _catalog;
    private readonly IMigrationDataSource _source;
    private readonly IReadOnlyDictionary<string, MigrationPlanObject> _planned;
    private bool _disposed;

    public MigrationDataSourceValidationSnapshot(
        MigrationPlan plan,
        MigrationCatalog catalog,
        IMigrationDataSource source)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentNullException.ThrowIfNull(source);
        MigrationPlanReadinessValidator.ValidateForApply(plan, catalog);
        if (source.Source != plan.Source)
            throw new InvalidDataException("Validation data source identity does not match the bound plan source.");
        if (source is IMigrationCatalogBoundDataSource catalogBoundSource &&
            !string.Equals(catalogBoundSource.CatalogDigest, plan.CatalogDigest, StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                "Validation data source catalog policy does not match the bound plan catalog.");
        }
        if (string.IsNullOrWhiteSpace(source.SnapshotIdentity))
            throw new InvalidDataException("Validation data source snapshot identity is required.");

        _plan = plan;
        _catalog = catalog;
        _source = source;
        _planned = plan.Objects.ToDictionary(item => item.SourceObjectId, StringComparer.Ordinal);
        SnapshotIdentity = source.SnapshotIdentity;
        ConsistencyStatus = plan.Source.Consistency.Kind switch
        {
            MigrationConsistencyKind.Immutable or
            MigrationConsistencyKind.Snapshot or
            MigrationConsistencyKind.Backup or
            MigrationConsistencyKind.Transaction or
            MigrationConsistencyKind.Watermark => MigrationSnapshotConsistencyStatus.Established,
            MigrationConsistencyKind.BestEffort => MigrationSnapshotConsistencyStatus.NotEstablished,
            MigrationConsistencyKind.Unavailable => MigrationSnapshotConsistencyStatus.Unavailable,
            _ => MigrationSnapshotConsistencyStatus.Unavailable,
        };
    }

    public string SnapshotIdentity { get; }

    public MigrationSnapshotConsistencyStatus ConsistencyStatus { get; }

    public ValueTask<MigrationNormalizedSchema> ReadSchemaAsync(
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(MigrationNormalizedSchemaContract.CreateExpected(_plan, _catalog));
    }

    public async ValueTask<long> CountAsync(
        string objectId,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        (string[] columnIds, _) = ResolveObject(objectId);
        long count = 0;
        await foreach (MigrationDataBatch batch in _source.ReadAsync(
                               ReadRequest(objectId, columnIds),
                               cancellationToken)
                           .WithCancellation(cancellationToken)
                           .ConfigureAwait(false))
        {
            ValidateBatch(batch, objectId, columnIds);
            count = checked(count + batch.Rows.Count);
        }
        return count;
    }

    public async IAsyncEnumerable<MigrationValidationRow> ReadRowsAsync(
        string objectId,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        (string[] columnIds, MigrationCatalogObject[] columns) = ResolveObject(objectId);
        MigrationTypeMapping[] mappings = columns
            .Select(column => _planned[column.ObjectId].TypeMappings.Single())
            .ToArray();
        long rowOrdinal = 0;

        await foreach (MigrationDataBatch batch in _source.ReadAsync(
                               ReadRequest(objectId, columnIds),
                               cancellationToken)
                           .WithCancellation(cancellationToken)
                           .ConfigureAwait(false))
        {
            ValidateBatch(batch, objectId, columnIds);
            foreach (MigrationDataRow row in batch.Rows)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (row is null || row.Values is null || row.Values.Count != columns.Length)
                    throw new InvalidDataException($"Validation source row for '{objectId}' has an invalid shape.");

                var converted = new DbValue[columns.Length];
                for (int index = 0; index < converted.Length; index++)
                {
                    converted[index] = MigrationValueConverter.Convert(
                        row.Values[index],
                        columns[index],
                        mappings[index],
                        rowOrdinal);
                }
                yield return new MigrationValidationRow
                {
                    StableKey = row.StableKey,
                    Values = converted,
                };
                rowOrdinal++;
            }
        }
    }

    public ValueTask DisposeAsync()
    {
        _disposed = true;
        return ValueTask.CompletedTask;
    }

    private MigrationReadRequest ReadRequest(string objectId, IReadOnlyList<string> columnIds) => new()
    {
        SourceObjectId = objectId,
        ColumnObjectIds = columnIds,
        BatchSize = _plan.Load.BatchSize,
        MaxBatchBytes = _plan.Load.MaxBatchBytes,
        MaxValueBytes = _plan.Load.MaxValueBytes,
        SnapshotToken = SnapshotIdentity,
    };

    private (string[] ColumnIds, MigrationCatalogObject[] Columns) ResolveObject(string objectId)
    {
        ThrowIfDisposed();
        if (!_planned.TryGetValue(objectId, out MigrationPlanObject? planned) || !planned.Included)
            throw new InvalidDataException($"Validation source object '{objectId}' is not included by the plan.");
        MigrationCatalogObject table = _catalog.Objects.SingleOrDefault(item =>
                string.Equals(item.ObjectId, objectId, StringComparison.Ordinal))
            ?? throw new InvalidDataException($"Validation source object '{objectId}' is absent from the catalog.");
        if (table.Kind is not (MigrationObjectKind.Table or MigrationObjectKind.Collection))
            throw new InvalidDataException($"Validation source object '{objectId}' is not a table or collection.");

        MigrationCatalogObject[] columns = _catalog.Objects
            .Where(item => item.Kind == MigrationObjectKind.Column &&
                string.Equals(item.ParentObjectId, objectId, StringComparison.Ordinal) &&
                _planned.TryGetValue(item.ObjectId, out MigrationPlanObject? value) && value.Included)
            .OrderBy(item => item.ObjectId, StringComparer.Ordinal)
            .ToArray();
        if (columns.Length == 0)
            throw new InvalidDataException($"Validation source object '{objectId}' has no included columns.");
        return (columns.Select(item => item.ObjectId).ToArray(), columns);
    }

    private void ValidateBatch(
        MigrationDataBatch batch,
        string objectId,
        IReadOnlyList<string> columnIds)
    {
        if (batch is null ||
            !string.Equals(batch.SourceObjectId, objectId, StringComparison.Ordinal) ||
            !string.Equals(batch.SnapshotIdentity, SnapshotIdentity, StringComparison.Ordinal) ||
            batch.ColumnObjectIds is null ||
            !batch.ColumnObjectIds.SequenceEqual(columnIds, StringComparer.Ordinal) ||
            batch.Rows is null || batch.Rows.Count == 0 || batch.Rows.Count > _plan.Load.BatchSize)
        {
            throw new InvalidDataException(
                $"Validation source batch for '{objectId}' changed identity, shape, or snapshot.");
        }
    }

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, this);
}
