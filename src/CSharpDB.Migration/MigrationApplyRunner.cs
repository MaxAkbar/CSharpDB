using System.Text;

namespace CSharpDB.Migration;

public enum MigrationApplyStatus
{
    AwaitingValidation,
}

public sealed record MigrationApplyRequest
{
    public required MigrationPlan Plan { get; init; }

    public required MigrationCatalog Catalog { get; init; }

    public required IMigrationDataSource Source { get; init; }

    public required IMigrationTarget Target { get; init; }

    public IDataTypeMappingProvider? MappingPolicy { get; init; }
}

public sealed record MigrationApplyResult
{
    public required MigrationApplyStatus Status { get; init; }

    public required string TargetIdentity { get; init; }

    public required string PlanDigest { get; init; }

    public required string CatalogDigest { get; init; }

    public required string SourceSnapshotIdentity { get; init; }

    public required string RejectContractVersion { get; init; }

    public long BatchesWritten { get; init; }

    public long BatchesSkipped { get; init; }

    public long RowsWritten { get; init; }

    public long RowsSkipped { get; init; }

    public int PeakBufferedRows { get; init; }

    public long PeakBufferedBytes { get; init; }
}

/// <summary>
/// Provider-neutral, bounded streaming coordinator. Targets own the atomic
/// row-plus-receipt transaction; this runner owns deterministic conversion,
/// digest verification, cursor ordering, and schema-stage ordering.
/// </summary>
public sealed class MigrationApplyRunner
{
    private static readonly MigrationSchemaStage[] PostLoadStages =
    [
        MigrationSchemaStage.SecondaryIndexes,
        MigrationSchemaStage.Constraints,
        MigrationSchemaStage.Views,
        MigrationSchemaStage.Triggers,
    ];

    public async ValueTask<MigrationApplyResult> ApplyAsync(
        MigrationApplyRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.Plan);
        ArgumentNullException.ThrowIfNull(request.Catalog);
        ArgumentNullException.ThrowIfNull(request.Source);
        ArgumentNullException.ThrowIfNull(request.Target);

        MigrationPlan plan = request.Plan;
        MigrationCatalog catalog = request.Catalog;
        MigrationPlanReadinessValidator.ValidateForApply(plan, catalog, request.MappingPolicy);
        MigrationApplyPolicyValidator.ValidateForExecution(plan);
        if (request.Source.Source != plan.Source)
            throw new InvalidDataException("Migration data source identity does not match the bound plan source.");
        if (request.Source is IMigrationCatalogBoundDataSource catalogBoundSource &&
            !string.Equals(catalogBoundSource.CatalogDigest, plan.CatalogDigest, StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                "Migration data source catalog policy does not match the bound plan catalog.");
        }
        if (string.IsNullOrWhiteSpace(request.Source.SnapshotIdentity))
            throw new InvalidDataException("Migration data source snapshot identity is required.");
        if (string.IsNullOrWhiteSpace(request.Target.TargetIdentity))
            throw new InvalidDataException("Migration target identity is required.");

        string snapshotIdentity = request.Source.SnapshotIdentity;
        string planDigest = MigrationArtifactSerializer.ComputePlanDigest(plan);
        IReadOnlyDictionary<string, MigrationCatalogObject> catalogObjects = catalog.Objects
            .ToDictionary(item => item.ObjectId, StringComparer.Ordinal);
        IReadOnlyDictionary<string, MigrationPlanObject> planObjects = plan.Objects
            .ToDictionary(item => item.SourceObjectId, StringComparer.Ordinal);

        cancellationToken.ThrowIfCancellationRequested();
        await request.Target.ApplySchemaAsync(
            plan,
            catalog,
            MigrationSchemaStage.LoadEssential,
            cancellationToken).ConfigureAwait(false);

        long batchesWritten = 0;
        long batchesSkipped = 0;
        long rowsWritten = 0;
        long rowsSkipped = 0;
        int peakRows = 0;
        long peakBytes = 0;

        foreach (MigrationCatalogObject sourceObject in catalog.Objects
                     .Where(item => item.Kind is MigrationObjectKind.Table or MigrationObjectKind.Collection)
                     .Where(item => planObjects[item.ObjectId].Included)
                     .OrderBy(item => item.ObjectId, StringComparer.Ordinal))
        {
            cancellationToken.ThrowIfCancellationRequested();
            MigrationCatalogObject[] columns = catalog.Objects
                .Where(item => item.Kind == MigrationObjectKind.Column &&
                    string.Equals(item.ParentObjectId, sourceObject.ObjectId, StringComparison.Ordinal) &&
                    planObjects[item.ObjectId].Included)
                .OrderBy(item => item.ObjectId, StringComparer.Ordinal)
                .ToArray();
            if (columns.Length == 0)
                throw new InvalidDataException($"Included source object '{sourceObject.ObjectId}' has no included columns.");

            string[] columnObjectIds = columns.Select(item => item.ObjectId).ToArray();
            MigrationTypeMapping[] mappings = columns
                .Select(column => planObjects[column.ObjectId].TypeMappings.Single())
                .ToArray();
            var readRequest = new MigrationReadRequest
            {
                SourceObjectId = sourceObject.ObjectId,
                ColumnObjectIds = columnObjectIds,
                BatchSize = plan.Load.BatchSize,
                MaxBatchBytes = plan.Load.MaxBatchBytes,
                MaxValueBytes = plan.Load.MaxValueBytes,
                SnapshotToken = snapshotIdentity,
            };

            long expectedBatchOrdinal = 0;
            long sourceRowOrdinal = 0;
            string? expectedStartCursor = null;
            await foreach (MigrationDataBatch sourceBatch in request.Source
                               .ReadAsync(readRequest, cancellationToken)
                               .WithCancellation(cancellationToken)
                               .ConfigureAwait(false))
            {
                ValidateSourceBatch(
                    sourceBatch,
                    sourceObject.ObjectId,
                    snapshotIdentity,
                    columnObjectIds,
                    expectedBatchOrdinal,
                    expectedStartCursor,
                    plan.Load.BatchSize);

                var targetRows = new MigrationTargetRow[sourceBatch.Rows.Count];
                long bufferedBytes = 0;
                for (int rowIndex = 0; rowIndex < sourceBatch.Rows.Count; rowIndex++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    MigrationDataRow sourceRow = sourceBatch.Rows[rowIndex] ??
                        throw new InvalidDataException(
                            $"Source batch '{sourceObject.ObjectId}'/{expectedBatchOrdinal} contains a null row.");
                    if (sourceRow.Values is null || sourceRow.Values.Count != columns.Length)
                    {
                        throw new InvalidDataException(
                            $"Source row {sourceRowOrdinal} for '{sourceObject.ObjectId}' has {sourceRow.Values?.Count ?? -1} values; expected {columns.Length}.");
                    }

                    var values = new CSharpDB.Primitives.DbValue[columns.Length];
                    for (int columnIndex = 0; columnIndex < columns.Length; columnIndex++)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        CSharpDB.Primitives.DbValue converted;
                        try
                        {
                            converted = MigrationValueConverter.Convert(
                                sourceRow.Values[columnIndex],
                                columns[columnIndex],
                                mappings[columnIndex],
                                sourceRowOrdinal);
                        }
                        catch (MigrationValueException error)
                        {
                            throw new MigrationRowRejectedException(
                                error.Code,
                                sourceObject.ObjectId,
                                columns[columnIndex].ObjectId,
                                sourceBatch.BatchOrdinal,
                                sourceRowOrdinal,
                                error);
                        }
                        int valueBytes = MigrationValueConverter.GetCanonicalByteCount(converted);
                        if (valueBytes > plan.Load.MaxValueBytes)
                        {
                            throw new MigrationRowRejectedException(
                                "MIG-APPLY-VALUE-SIZE-001",
                                sourceObject.ObjectId,
                                columns[columnIndex].ObjectId,
                                sourceBatch.BatchOrdinal,
                                sourceRowOrdinal,
                                new InvalidDataException(
                                    $"Converted value exceeds MaxValueBytes ({plan.Load.MaxValueBytes})."));
                        }

                        bufferedBytes = checked(bufferedBytes + valueBytes);
                        if (bufferedBytes > plan.Load.MaxBatchBytes)
                        {
                            throw new InvalidDataException(
                                $"Source batch '{sourceObject.ObjectId}'/{expectedBatchOrdinal} exceeds MaxBatchBytes ({plan.Load.MaxBatchBytes}).");
                        }
                        values[columnIndex] = converted;
                    }

                    if (sourceRow.StableKey is not null)
                    {
                        bufferedBytes = checked(bufferedBytes + Encoding.UTF8.GetByteCount(sourceRow.StableKey));
                        if (bufferedBytes > plan.Load.MaxBatchBytes)
                        {
                            throw new InvalidDataException(
                                $"Source batch '{sourceObject.ObjectId}'/{expectedBatchOrdinal} exceeds MaxBatchBytes ({plan.Load.MaxBatchBytes}).");
                        }
                    }

                    targetRows[rowIndex] = new MigrationTargetRow
                    {
                        SourceRowOrdinal = sourceRowOrdinal,
                        StableKey = sourceRow.StableKey,
                        Values = values,
                    };
                    sourceRowOrdinal++;
                }

                peakRows = Math.Max(peakRows, targetRows.Length);
                peakBytes = Math.Max(peakBytes, bufferedBytes);
                var targetBatch = new MigrationTargetBatch
                {
                    PlanDigest = planDigest,
                    CatalogDigest = plan.CatalogDigest,
                    SourceFingerprint = plan.Source.Fingerprint,
                    SourceSnapshotIdentity = snapshotIdentity,
                    SourceObjectId = sourceObject.ObjectId,
                    ColumnObjectIds = columnObjectIds,
                    BatchOrdinal = sourceBatch.BatchOrdinal,
                    StartCursor = sourceBatch.StartCursor,
                    NextCursor = sourceBatch.NextCursor,
                    BatchDigest = string.Empty,
                    Rows = targetRows,
                };
                targetBatch = targetBatch with { BatchDigest = MigrationBatchDigest.Compute(targetBatch) };

                MigrationBatchReceipt? existing = await request.Target.ReadReceiptAsync(
                    planDigest,
                    sourceObject.ObjectId,
                    targetBatch.BatchOrdinal,
                    cancellationToken).ConfigureAwait(false);
                if (existing is not null)
                {
                    ValidateReceipt(request.Target.TargetIdentity, targetBatch, existing);
                    batchesSkipped++;
                    rowsSkipped += targetRows.Length;
                }
                else
                {
                    MigrationBatchReceipt written = await request.Target.WriteBatchAsync(
                        targetBatch,
                        cancellationToken).ConfigureAwait(false);
                    ValidateReceipt(request.Target.TargetIdentity, targetBatch, written);
                    batchesWritten++;
                    rowsWritten += targetRows.Length;
                }

                expectedBatchOrdinal++;
                expectedStartCursor = sourceBatch.NextCursor;
            }

            await ValidateReceiptSetAsync(
                request.Target,
                planDigest,
                sourceObject.ObjectId,
                expectedBatchOrdinal,
                cancellationToken).ConfigureAwait(false);
        }

        foreach (MigrationSchemaStage stage in PostLoadStages)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await request.Target.ApplySchemaAsync(plan, catalog, stage, cancellationToken).ConfigureAwait(false);
        }

        return new MigrationApplyResult
        {
            Status = MigrationApplyStatus.AwaitingValidation,
            TargetIdentity = request.Target.TargetIdentity,
            PlanDigest = planDigest,
            CatalogDigest = plan.CatalogDigest,
            SourceSnapshotIdentity = snapshotIdentity,
            RejectContractVersion = MigrationRejectContract.DeterministicFailFastV1,
            BatchesWritten = batchesWritten,
            BatchesSkipped = batchesSkipped,
            RowsWritten = rowsWritten,
            RowsSkipped = rowsSkipped,
            PeakBufferedRows = peakRows,
            PeakBufferedBytes = peakBytes,
        };
    }

    private static void ValidateSourceBatch(
        MigrationDataBatch batch,
        string sourceObjectId,
        string snapshotIdentity,
        IReadOnlyList<string> columnObjectIds,
        long expectedBatchOrdinal,
        string? expectedStartCursor,
        int maximumRows)
    {
        if (batch is null)
            throw new InvalidDataException("Migration source emitted a null batch.");
        if (!string.Equals(batch.SourceObjectId, sourceObjectId, StringComparison.Ordinal))
            throw new InvalidDataException("Migration source batch object identity changed during streaming.");
        if (!string.Equals(batch.SnapshotIdentity, snapshotIdentity, StringComparison.Ordinal))
            throw new InvalidDataException("Migration source snapshot identity changed during streaming.");
        if (batch.ColumnObjectIds is null ||
            !batch.ColumnObjectIds.SequenceEqual(columnObjectIds, StringComparer.Ordinal))
        {
            throw new InvalidDataException("Migration source batch column order does not match the read request.");
        }
        if (batch.BatchOrdinal != expectedBatchOrdinal)
            throw new InvalidDataException($"Migration source batch ordinal {batch.BatchOrdinal} does not match expected ordinal {expectedBatchOrdinal}.");
        if (!string.Equals(batch.StartCursor, expectedStartCursor, StringComparison.Ordinal))
            throw new InvalidDataException("Migration source cursor chain changed during streaming.");
        if (batch.Rows is null || batch.Rows.Count == 0)
            throw new InvalidDataException("Migration source batches must contain at least one row.");
        if (batch.Rows.Count > maximumRows)
            throw new InvalidDataException($"Migration source batch contains {batch.Rows.Count} rows; maximum is {maximumRows}.");
    }

    private static void ValidateReceipt(
        string targetIdentity,
        MigrationTargetBatch batch,
        MigrationBatchReceipt receipt)
    {
        if (!string.Equals(receipt.TargetIdentity, targetIdentity, StringComparison.Ordinal) ||
            !string.Equals(receipt.PlanDigest, batch.PlanDigest, StringComparison.Ordinal) ||
            !string.Equals(receipt.CatalogDigest, batch.CatalogDigest, StringComparison.Ordinal) ||
            !string.Equals(receipt.SourceFingerprint, batch.SourceFingerprint, StringComparison.Ordinal) ||
            !string.Equals(receipt.SourceSnapshotIdentity, batch.SourceSnapshotIdentity, StringComparison.Ordinal) ||
            !string.Equals(receipt.SourceObjectId, batch.SourceObjectId, StringComparison.Ordinal) ||
            receipt.BatchOrdinal != batch.BatchOrdinal ||
            !string.Equals(receipt.StartCursor, batch.StartCursor, StringComparison.Ordinal) ||
            !string.Equals(receipt.NextCursor, batch.NextCursor, StringComparison.Ordinal) ||
            !string.Equals(receipt.BatchDigest, batch.BatchDigest, StringComparison.Ordinal) ||
            receipt.RowCount != batch.Rows.Count ||
            receipt.RejectedRowCount != 0)
        {
            throw new InvalidDataException(
                $"Migration receipt mismatch for '{batch.SourceObjectId}' batch {batch.BatchOrdinal}.");
        }
    }

    private static async ValueTask ValidateReceiptSetAsync(
        IMigrationTarget target,
        string planDigest,
        string sourceObjectId,
        long expectedCount,
        CancellationToken cancellationToken)
    {
        long ordinal = 0;
        await foreach (MigrationBatchReceipt receipt in target
                           .ReadReceiptsAsync(planDigest, sourceObjectId, cancellationToken)
                           .WithCancellation(cancellationToken)
                           .ConfigureAwait(false))
        {
            if (receipt is null ||
                !string.Equals(receipt.TargetIdentity, target.TargetIdentity, StringComparison.Ordinal) ||
                !string.Equals(receipt.PlanDigest, planDigest, StringComparison.Ordinal) ||
                !string.Equals(receipt.SourceObjectId, sourceObjectId, StringComparison.Ordinal) ||
                receipt.BatchOrdinal != ordinal)
            {
                throw new InvalidDataException(
                    $"Migration receipt set for '{sourceObjectId}' is not a contiguous target-owned sequence.");
            }
            ordinal++;
        }

        if (ordinal != expectedCount)
        {
            throw new InvalidDataException(
                $"Migration receipt count for '{sourceObjectId}' is {ordinal}; expected {expectedCount}.");
        }
    }
}
