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

    public long RejectedRowsWritten { get; init; }

    public long RejectedRowsSkipped { get; init; }

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
        MigrationApplyPolicyValidator.ValidateForExecution(
            plan,
            request.Source,
            request.Target);
        if (string.IsNullOrWhiteSpace(request.Target.TargetIdentity))
            throw new InvalidDataException("Migration target identity is required.");
        string batchDigestFormat = request.Target is IMigrationBatchDigestContractTarget digestTarget
            ? digestTarget.BatchDigestFormat
            : MigrationBatchDigest.Format;
        if (batchDigestFormat is not
            (MigrationBatchDigest.LegacyFormat or MigrationBatchDigest.Format))
        {
            throw new InvalidDataException("Migration target batch digest format is unsupported.");
        }

        var replayer = new MigrationOutcomeBatchReplayer(
            plan,
            catalog,
            request.Source,
            batchDigestFormat);
        string snapshotIdentity = replayer.SnapshotIdentity;
        string planDigest = replayer.PlanDigest;
        string rejectContractVersion = replayer.RejectContractVersion;

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
        long rejectedRowsWritten = 0;
        long rejectedRowsSkipped = 0;
        int peakRows = 0;
        long peakBytes = 0;

        foreach (MigrationReplayedOutcomeObject replayObject in
                 replayer.ReplayObjects(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();
            long expectedBatchCount = 0;
            await foreach (MigrationReplayedOutcomeBatch replayedBatch in replayObject.Batches
                               .WithCancellation(cancellationToken)
                               .ConfigureAwait(false))
            {
                MigrationTargetBatch targetBatch = replayedBatch.Batch;
                peakRows = Math.Max(peakRows, replayedBatch.BufferedOutcomeCount);
                peakBytes = Math.Max(peakBytes, replayedBatch.BufferedBytes);

                MigrationBatchReceipt? existing = await request.Target.ReadReceiptAsync(
                    planDigest,
                    replayObject.SourceObjectId,
                    targetBatch.BatchOrdinal,
                    cancellationToken).ConfigureAwait(false);
                if (existing is not null)
                {
                    ValidateReceipt(request.Target.TargetIdentity, targetBatch, existing);
                    batchesSkipped++;
                    rowsSkipped += targetBatch.Rows.Count;
                    rejectedRowsSkipped += targetBatch.RejectedRows.Count;
                }
                else
                {
                    MigrationBatchReceipt written = await request.Target.WriteBatchAsync(
                        targetBatch,
                        cancellationToken).ConfigureAwait(false);
                    ValidateReceipt(request.Target.TargetIdentity, targetBatch, written);
                    batchesWritten++;
                    rowsWritten += targetBatch.Rows.Count;
                    rejectedRowsWritten += targetBatch.RejectedRows.Count;
                }

                expectedBatchCount++;
            }

            await ValidateReceiptSetAsync(
                request.Target,
                planDigest,
                replayObject.SourceObjectId,
                expectedBatchCount,
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
            RejectContractVersion = rejectContractVersion,
            BatchesWritten = batchesWritten,
            BatchesSkipped = batchesSkipped,
            RowsWritten = rowsWritten,
            RowsSkipped = rowsSkipped,
            RejectedRowsWritten = rejectedRowsWritten,
            RejectedRowsSkipped = rejectedRowsSkipped,
            PeakBufferedRows = peakRows,
            PeakBufferedBytes = peakBytes,
        };
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
            !string.Equals(
                receipt.RejectContractVersion,
                batch.RejectContractVersion,
                StringComparison.Ordinal) ||
            !MigrationBatchOutcomeValidator.FixedTimeSha256Equals(
                batch.RejectDigest,
                receipt.RejectDigest) ||
            receipt.RowCount != batch.Rows.Count ||
            receipt.RejectedRowCount != batch.RejectedRows.Count)
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
