using System.Text;

namespace CSharpDB.Migration.Validation;

/// <summary>
/// Compares a source snapshot's deterministic row outcomes with the receipts
/// and authoritative reject ledger captured by one immutable target snapshot.
/// The comparison advances each stream sequentially and retains at most one
/// source batch, receipt, and ledger entry at a time.
/// </summary>
public sealed class MigrationRejectOutcomeComparer
{
    internal const string MismatchMessage =
        "Deterministic migration reject outcomes do not match the authoritative target snapshot.";

    public async ValueTask CompareAsync(
        MigrationPlan plan,
        MigrationCatalog catalog,
        string targetIdentity,
        IMigrationRejectReplayValidationSnapshot sourceSnapshot,
        IMigrationRejectTargetValidationSnapshot targetSnapshot,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentException.ThrowIfNullOrWhiteSpace(targetIdentity);
        ArgumentNullException.ThrowIfNull(sourceSnapshot);
        ArgumentNullException.ThrowIfNull(targetSnapshot);

        try
        {
            await CompareCoreAsync(
                plan,
                catalog,
                targetIdentity,
                sourceSnapshot,
                targetSnapshot,
                cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw new OperationCanceledException(cancellationToken);
        }
        catch (Exception error) when (error is not
            (OutOfMemoryException or StackOverflowException or AccessViolationException))
        {
            // Do not propagate provider messages: a source or target adapter
            // could otherwise place rejected values in an exception string.
            throw Mismatch();
        }
    }

    private static async ValueTask CompareCoreAsync(
        MigrationPlan plan,
        MigrationCatalog catalog,
        string targetIdentity,
        IMigrationRejectReplayValidationSnapshot sourceSnapshot,
        IMigrationRejectTargetValidationSnapshot targetSnapshot,
        CancellationToken cancellationToken)
    {
        MigrationDeterministicRejectPolicy policy = plan.Load.RejectPolicy ??
            throw Mismatch();
        Require(plan.Load.RejectMode == MigrationRejectMode.DeterministicRejects);
        Require(string.Equals(
            policy.ContractVersion,
            MigrationRejectContract.DeterministicRejectsV1,
            StringComparison.Ordinal));

        string planDigest = MigrationArtifactSerializer.ComputePlanDigest(plan);
        Require(string.Equals(
            plan.CatalogDigest,
            MigrationArtifactSerializer.ComputeCatalogDigest(catalog),
            StringComparison.Ordinal));
        string sourceSnapshotIdentity = sourceSnapshot.SnapshotIdentity;
        string targetSnapshotIdentity = targetSnapshot.SnapshotIdentity;
        Require(!string.IsNullOrWhiteSpace(sourceSnapshotIdentity));
        Require(!string.IsNullOrWhiteSpace(targetSnapshotIdentity));

        IReadOnlyDictionary<string, MigrationPlanObject> plannedObjects = plan.Objects
            .ToDictionary(item => item.SourceObjectId, StringComparer.Ordinal);
        ExpectedObject[] expectedObjects = catalog.Objects
            .Where(item => item.Kind is MigrationObjectKind.Table or MigrationObjectKind.Collection)
            .Where(item => plannedObjects.TryGetValue(item.ObjectId, out MigrationPlanObject? planned) &&
                planned.Included)
            .OrderBy(item => item.ObjectId, StringComparer.Ordinal)
            .Select(item => new ExpectedObject(
                item.ObjectId,
                catalog.Objects
                    .Where(candidate =>
                        candidate.Kind == MigrationObjectKind.Column &&
                        string.Equals(
                            candidate.ParentObjectId,
                            item.ObjectId,
                            StringComparison.Ordinal) &&
                        plannedObjects.TryGetValue(
                            candidate.ObjectId,
                            out MigrationPlanObject? plannedColumn) &&
                        plannedColumn.Included)
                    .OrderBy(candidate => candidate.ObjectId, StringComparer.Ordinal)
                    .Select(candidate => candidate.ObjectId)
                    .ToArray()))
            .ToArray();
        var objectOrdinals = expectedObjects
            .Select((item, index) => (item.SourceObjectId, index))
            .ToDictionary(item => item.SourceObjectId, item => item.index, StringComparer.Ordinal);

        long rejectedRowsInRun = 0;
        long rawValueBytesInRun = 0;
        long artifactBytes = MigrationRejectLedgerCodec.GetArtifactHeaderByteCount(planDigest);
        Require(artifactBytes <= policy.MaxArtifactBytes);

        int currentObjectOrdinal = -1;
        long expectedBatchOrdinal = 0;
        long expectedFirstSourceRowOrdinal = 0;
        string? expectedStartCursor = null;
        bool currentObjectTerminated = false;

        await using IAsyncEnumerator<MigrationTargetBatch> source = sourceSnapshot
            .ReplayOutcomeBatchesAsync(cancellationToken)
            .GetAsyncEnumerator(cancellationToken);
        await using IAsyncEnumerator<MigrationBatchReceipt> receipts = targetSnapshot
            .ReadOutcomeReceiptsAsync(planDigest, cancellationToken)
            .GetAsyncEnumerator(cancellationToken);
        await using IAsyncEnumerator<MigrationRejectLedgerEntry> ledger = targetSnapshot
            .ReadRejectLedgerAsync(planDigest, cancellationToken)
            .GetAsyncEnumerator(cancellationToken);

        while (await source.MoveNextAsync().ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            MigrationTargetBatch batch = source.Current ?? throw Mismatch();
            Require(objectOrdinals.TryGetValue(batch.SourceObjectId, out int objectOrdinal));
            Require(objectOrdinal >= currentObjectOrdinal);

            if (objectOrdinal != currentObjectOrdinal)
            {
                Require(currentObjectOrdinal < 0 || currentObjectTerminated);
                currentObjectOrdinal = objectOrdinal;
                expectedBatchOrdinal = 0;
                expectedFirstSourceRowOrdinal = 0;
                expectedStartCursor = null;
                currentObjectTerminated = false;
            }
            else
            {
                Require(!currentObjectTerminated);
            }

            ExpectedObject expectedObject = expectedObjects[currentObjectOrdinal];
            ValidateBatchBinding(
                batch,
                plan,
                planDigest,
                sourceSnapshotIdentity,
                expectedObject,
                expectedBatchOrdinal,
                expectedStartCursor,
                expectedFirstSourceRowOrdinal,
                policy);

            Require(await receipts.MoveNextAsync().ConfigureAwait(false));
            MigrationBatchReceipt receipt = receipts.Current ?? throw Mismatch();
            ValidateReceipt(
                receipt,
                batch,
                targetIdentity);

            long rawValueBytesInBatch = 0;
            foreach (MigrationRejectedRow rejectedRow in batch.RejectedRows)
            {
                cancellationToken.ThrowIfCancellationRequested();
                Require(await ledger.MoveNextAsync().ConfigureAwait(false));
                MigrationRejectLedgerEntry ledgerEntry = ledger.Current ?? throw Mismatch();
                ValidateLedgerEntry(
                    ledgerEntry,
                    planDigest,
                    batch.SourceObjectId,
                    batch.BatchOrdinal,
                    rejectedRow,
                    out int rawValueBytes,
                    out int artifactEntryBytes);

                rawValueBytesInBatch = checked(rawValueBytesInBatch + rawValueBytes);
                rawValueBytesInRun = checked(rawValueBytesInRun + rawValueBytes);
                artifactBytes = checked(artifactBytes + artifactEntryBytes);
                Require(rawValueBytesInBatch <= policy.MaxRawValueBytesPerBatch);
                Require(rawValueBytesInRun <= policy.MaxRawValueBytesPerRun);
                Require(artifactBytes <= policy.MaxArtifactBytes);
            }

            rejectedRowsInRun = checked(rejectedRowsInRun + batch.RejectedRows.Count);
            Require(rejectedRowsInRun <= policy.MaxRejectedRowsPerRun);

            expectedFirstSourceRowOrdinal = checked(
                expectedFirstSourceRowOrdinal +
                batch.Rows.Count +
                batch.RejectedRows.Count);
            expectedBatchOrdinal = checked(expectedBatchOrdinal + 1);
            expectedStartCursor = batch.NextCursor;
            currentObjectTerminated = batch.NextCursor is null;
        }

        Require(currentObjectOrdinal < 0 || currentObjectTerminated);
        Require(!await receipts.MoveNextAsync().ConfigureAwait(false));
        Require(!await ledger.MoveNextAsync().ConfigureAwait(false));
        Require(string.Equals(
            sourceSnapshot.SnapshotIdentity,
            sourceSnapshotIdentity,
            StringComparison.Ordinal));
        Require(string.Equals(
            targetSnapshot.SnapshotIdentity,
            targetSnapshotIdentity,
            StringComparison.Ordinal));
    }

    private static void ValidateBatchBinding(
        MigrationTargetBatch batch,
        MigrationPlan plan,
        string planDigest,
        string sourceSnapshotIdentity,
        ExpectedObject expectedObject,
        long expectedBatchOrdinal,
        string? expectedStartCursor,
        long expectedFirstSourceRowOrdinal,
        MigrationDeterministicRejectPolicy policy)
    {
        Require(string.Equals(batch.PlanDigest, planDigest, StringComparison.Ordinal));
        Require(string.Equals(batch.CatalogDigest, plan.CatalogDigest, StringComparison.Ordinal));
        Require(string.Equals(
            batch.SourceFingerprint,
            plan.Source.Fingerprint,
            StringComparison.Ordinal));
        Require(string.Equals(
            batch.SourceSnapshotIdentity,
            sourceSnapshotIdentity,
            StringComparison.Ordinal));
        Require(string.Equals(
            batch.SourceObjectId,
            expectedObject.SourceObjectId,
            StringComparison.Ordinal));
        IReadOnlyList<string> batchColumns = batch.ColumnObjectIds ?? throw Mismatch();
        Require(batchColumns.SequenceEqual(
            expectedObject.ColumnObjectIds,
            StringComparer.Ordinal));
        Require(batch.BatchOrdinal == expectedBatchOrdinal);
        Require(string.Equals(batch.StartCursor, expectedStartCursor, StringComparison.Ordinal));
        Require(string.Equals(
            batch.RejectContractVersion,
            policy.ContractVersion,
            StringComparison.Ordinal));
        IReadOnlyList<MigrationTargetRow> batchRows = batch.Rows ?? throw Mismatch();
        IReadOnlyList<MigrationRejectedRow> batchRejects =
            batch.RejectedRows ?? throw Mismatch();

        int attemptedRows = checked(batchRows.Count + batchRejects.Count);
        Require(attemptedRows > 0 && attemptedRows <= plan.Load.BatchSize);
        Require(batchRejects.Count <= policy.MaxRejectedRowsPerBatch);
        MigrationBatchOutcomeValidator.Validate(
            batch,
            expectedFirstSourceRowOrdinal,
            plan.Load.BatchSize);

        long rawValueBytesInBatch = 0;
        foreach (MigrationRejectedRow rejectedRow in batchRejects)
        {
            Require(policy.AllowedRuleIds.Contains(rejectedRow.RuleId, StringComparer.Ordinal));
            Require(rejectedRow.ColumnObjectId is null ||
                expectedObject.ColumnObjectIds.Contains(
                    rejectedRow.ColumnObjectId,
                    StringComparer.Ordinal));
            int rawValueBytes = MigrationRejectLedgerCodec.GetRawValueByteCount(rejectedRow);
            Require(rawValueBytes <= policy.MaxRawValueBytes);
            rawValueBytesInBatch = checked(rawValueBytesInBatch + rawValueBytes);
        }
        Require(rawValueBytesInBatch <= policy.MaxRawValueBytesPerBatch);

        foreach (MigrationTargetRow row in batchRows)
        {
            MigrationTargetRow targetRow = row ?? throw Mismatch();
            IReadOnlyList<CSharpDB.Primitives.DbValue> values =
                targetRow.Values ?? throw Mismatch();
            Require(values.Count == expectedObject.ColumnObjectIds.Count);
        }

        long bufferedBytes = 0;
        foreach (MigrationTargetRow row in batchRows)
        {
            foreach (CSharpDB.Primitives.DbValue value in row.Values)
            {
                int valueBytes = MigrationValueConverter.GetCanonicalByteCount(value);
                Require(valueBytes <= plan.Load.MaxValueBytes);
                bufferedBytes = checked(bufferedBytes + valueBytes);
                Require(bufferedBytes <= plan.Load.MaxBatchBytes);
            }
            if (row.StableKey is not null)
            {
                bufferedBytes = checked(
                    bufferedBytes + Encoding.UTF8.GetByteCount(row.StableKey));
                Require(bufferedBytes <= plan.Load.MaxBatchBytes);
            }
        }
        foreach (MigrationRejectedRow rejectedRow in batchRejects)
        {
            bufferedBytes = checked(
                bufferedBytes +
                MigrationRejectLedgerCodec.GetCanonicalEntryByteCount(
                    batch.SourceObjectId,
                    batch.BatchOrdinal,
                    rejectedRow));
            Require(bufferedBytes <= plan.Load.MaxBatchBytes);
        }

        string rejectDigest = MigrationRejectDigest.Compute(batch);
        Require(MigrationBatchOutcomeValidator.FixedTimeSha256Equals(
            rejectDigest,
            batch.RejectDigest));
        string batchDigest = MigrationBatchDigest.Compute(batch, MigrationBatchDigest.Format);
        Require(MigrationBatchOutcomeValidator.FixedTimeSha256Equals(
            batchDigest,
            batch.BatchDigest));
    }

    private static void ValidateReceipt(
        MigrationBatchReceipt receipt,
        MigrationTargetBatch batch,
        string targetIdentity)
    {
        Require(string.Equals(receipt.TargetIdentity, targetIdentity, StringComparison.Ordinal));
        Require(string.Equals(receipt.PlanDigest, batch.PlanDigest, StringComparison.Ordinal));
        Require(string.Equals(receipt.CatalogDigest, batch.CatalogDigest, StringComparison.Ordinal));
        Require(string.Equals(
            receipt.SourceFingerprint,
            batch.SourceFingerprint,
            StringComparison.Ordinal));
        Require(string.Equals(
            receipt.SourceSnapshotIdentity,
            batch.SourceSnapshotIdentity,
            StringComparison.Ordinal));
        Require(string.Equals(
            receipt.SourceObjectId,
            batch.SourceObjectId,
            StringComparison.Ordinal));
        Require(receipt.BatchOrdinal == batch.BatchOrdinal);
        Require(string.Equals(receipt.StartCursor, batch.StartCursor, StringComparison.Ordinal));
        Require(string.Equals(receipt.NextCursor, batch.NextCursor, StringComparison.Ordinal));
        Require(string.Equals(
            receipt.RejectContractVersion,
            batch.RejectContractVersion,
            StringComparison.Ordinal));
        Require(receipt.RowCount == batch.Rows.Count);
        Require(receipt.RejectedRowCount == batch.RejectedRows.Count);
        Require(MigrationBatchOutcomeValidator.FixedTimeSha256Equals(
            batch.RejectDigest,
            receipt.RejectDigest));
        Require(MigrationBatchOutcomeValidator.FixedTimeSha256Equals(
            batch.BatchDigest,
            receipt.BatchDigest));
    }

    private static void ValidateLedgerEntry(
        MigrationRejectLedgerEntry ledgerEntry,
        string planDigest,
        string sourceObjectId,
        long batchOrdinal,
        MigrationRejectedRow expected,
        out int rawValueBytes,
        out int artifactEntryBytes)
    {
        Require(string.Equals(ledgerEntry.PlanDigest, planDigest, StringComparison.Ordinal));
        Require(string.Equals(
            ledgerEntry.SourceObjectId,
            sourceObjectId,
            StringComparison.Ordinal));
        Require(ledgerEntry.BatchOrdinal == batchOrdinal);
        Require(ledgerEntry.RejectedRow is not null);

        string expectedCanonical = MigrationRejectLedgerCodec.SerializeEntry(
            sourceObjectId,
            batchOrdinal,
            expected);
        string actualCanonical = MigrationRejectLedgerCodec.SerializeEntry(
            ledgerEntry.SourceObjectId,
            ledgerEntry.BatchOrdinal,
            ledgerEntry.RejectedRow ?? throw Mismatch());
        Require(string.Equals(expectedCanonical, actualCanonical, StringComparison.Ordinal));

        rawValueBytes = MigrationRejectLedgerCodec.GetRawValueByteCount(expected);
        int canonicalEntryBytes = MigrationRejectLedgerCodec.GetCanonicalEntryByteCount(
            sourceObjectId,
            batchOrdinal,
            expected);
        Require(ledgerEntry.RawValueByteCount == rawValueBytes);
        Require(ledgerEntry.CanonicalEntryByteCount == canonicalEntryBytes);
        artifactEntryBytes = MigrationRejectLedgerCodec.GetCanonicalArtifactEntryByteCount(
            sourceObjectId,
            batchOrdinal,
            expected);
    }

    private static void Require(bool condition)
    {
        if (!condition)
            throw Mismatch();
    }

    private static InvalidDataException Mismatch() => new(MismatchMessage);

    private sealed record ExpectedObject(
        string SourceObjectId,
        IReadOnlyList<string> ColumnObjectIds);
}
