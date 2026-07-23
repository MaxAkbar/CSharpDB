using System.Runtime.CompilerServices;
using System.Text;
using CSharpDB.Primitives;

namespace CSharpDB.Migration;

internal sealed record MigrationReplayedOutcomeBatch
{
    internal required MigrationTargetBatch Batch { get; init; }

    internal required int BufferedOutcomeCount { get; init; }

    internal required long BufferedBytes { get; init; }
}

internal sealed record MigrationReplayedOutcomeObject
{
    internal required string SourceObjectId { get; init; }

    internal required IAsyncEnumerable<MigrationReplayedOutcomeBatch> Batches { get; init; }
}

/// <summary>
/// Reproduces the exact target-shaped outcome stream for an immutable source
/// snapshot. Apply and validation share this path so projection, cursors,
/// source ordinals, conversions, reject evidence, and digests cannot drift.
/// </summary>
internal sealed class MigrationOutcomeBatchReplayer
{
    private readonly MigrationPlan _plan;
    private readonly IMigrationDataSource _source;
    private readonly string _batchDigestFormat;
    private readonly string _planDigest;
    private readonly string _rejectContractVersion;
    private readonly string _snapshotIdentity;
    private readonly ReplayObject[] _objects;
    private readonly IReadOnlyDictionary<string, ReplayObject> _objectsById;

    internal MigrationOutcomeBatchReplayer(
        MigrationPlan plan,
        MigrationCatalog catalog,
        IMigrationDataSource source,
        string batchDigestFormat = MigrationBatchDigest.Format)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentNullException.ThrowIfNull(source);
        ArgumentException.ThrowIfNullOrWhiteSpace(batchDigestFormat);
        if (batchDigestFormat is not
            (MigrationBatchDigest.LegacyFormat or MigrationBatchDigest.Format))
        {
            throw new InvalidDataException("Migration target batch digest format is unsupported.");
        }

        MigrationRejectSourceCapabilityValidator.Validate(plan, source);
        if (source.Source != plan.Source)
            throw new InvalidDataException("Migration data source identity does not match the bound plan source.");
        if (source is IMigrationCatalogBoundDataSource catalogBoundSource &&
            !string.Equals(catalogBoundSource.CatalogDigest, plan.CatalogDigest, StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                "Migration data source catalog policy does not match the bound plan catalog.");
        }
        string snapshotIdentity = source.SnapshotIdentity;
        if (string.IsNullOrWhiteSpace(snapshotIdentity))
            throw new InvalidDataException("Migration data source snapshot identity is required.");

        _rejectContractVersion = plan.Load.RejectMode switch
        {
            MigrationRejectMode.FailFast => MigrationRejectContract.DeterministicFailFastV1,
            MigrationRejectMode.DeterministicRejects =>
                MigrationRejectContract.DeterministicRejectsV1,
            _ => throw new InvalidDataException("Migration plan reject mode is unsupported."),
        };
        MigrationRejectReadPolicyValidator.Validate(
            _rejectContractVersion,
            plan.Load.RejectPolicy,
            plan.Load.BatchSize);
        if (plan.Load.RejectMode == MigrationRejectMode.DeterministicRejects &&
            !string.Equals(batchDigestFormat, MigrationBatchDigest.Format, StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                "Deterministic rejects require the current migration batch digest contract.");
        }

        IReadOnlyDictionary<string, MigrationPlanObject> planned = plan.Objects
            .ToDictionary(item => item.SourceObjectId, StringComparer.Ordinal);
        _objects = catalog.Objects
            .Where(item => item.Kind is MigrationObjectKind.Table or MigrationObjectKind.Collection)
            .Where(item => planned[item.ObjectId].Included)
            .OrderBy(item => item.ObjectId, StringComparer.Ordinal)
            .Select(sourceObject => CreateReplayObject(sourceObject, catalog, planned))
            .ToArray();
        _objectsById = _objects.ToDictionary(item => item.SourceObjectId, StringComparer.Ordinal);
        _plan = plan;
        _source = source;
        _batchDigestFormat = batchDigestFormat;
        _planDigest = MigrationArtifactSerializer.ComputePlanDigest(plan);
        _snapshotIdentity = snapshotIdentity;
    }

    internal string PlanDigest => _planDigest;

    internal string RejectContractVersion => _rejectContractVersion;

    internal string SnapshotIdentity => _snapshotIdentity;

    internal IEnumerable<MigrationReplayedOutcomeObject> ReplayObjects(
        CancellationToken cancellationToken = default)
    {
        foreach (ReplayObject replayObject in _objects)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return new MigrationReplayedOutcomeObject
            {
                SourceObjectId = replayObject.SourceObjectId,
                Batches = ReplayObjectCoreAsync(replayObject, cancellationToken),
            };
        }
    }

    internal IAsyncEnumerable<MigrationReplayedOutcomeBatch> ReplayObjectAsync(
        string sourceObjectId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceObjectId);
        if (!_objectsById.TryGetValue(sourceObjectId, out ReplayObject? replayObject))
        {
            throw new InvalidDataException(
                $"Migration replay source object '{sourceObjectId}' is not included by the plan.");
        }

        return ReplayObjectCoreAsync(replayObject, cancellationToken);
    }

    internal async IAsyncEnumerable<MigrationReplayedOutcomeBatch> ReplayAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (MigrationReplayedOutcomeObject replayObject in ReplayObjects(cancellationToken))
        {
            await foreach (MigrationReplayedOutcomeBatch batch in replayObject.Batches
                               .WithCancellation(cancellationToken)
                               .ConfigureAwait(false))
            {
                yield return batch;
            }
        }
    }

    private async IAsyncEnumerable<MigrationReplayedOutcomeBatch> ReplayObjectCoreAsync(
        ReplayObject replayObject,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var readRequest = new MigrationReadRequest
        {
            SourceObjectId = replayObject.SourceObjectId,
            ColumnObjectIds = replayObject.ColumnObjectIds,
            BatchSize = _plan.Load.BatchSize,
            MaxBatchBytes = _plan.Load.MaxBatchBytes,
            MaxValueBytes = _plan.Load.MaxValueBytes,
            RejectContractVersion = _rejectContractVersion,
            RejectPolicy = _plan.Load.RejectPolicy,
            SnapshotToken = SnapshotIdentity,
        };

        long expectedBatchOrdinal = 0;
        long sourceRowOrdinal = 0;
        string? expectedStartCursor = null;
        await foreach (MigrationDataBatch sourceBatch in _source
                           .ReadAsync(readRequest, cancellationToken)
                           .WithCancellation(cancellationToken)
                           .ConfigureAwait(false))
        {
            long[] acceptedSourceOrdinals = ValidateSourceBatch(
                sourceBatch,
                replayObject.SourceObjectId,
                replayObject.ColumnObjectIds,
                expectedBatchOrdinal,
                expectedStartCursor,
                sourceRowOrdinal);

            var targetRows = new MigrationTargetRow[sourceBatch.Rows.Count];
            long bufferedBytes = 0;
            for (int rowIndex = 0; rowIndex < sourceBatch.Rows.Count; rowIndex++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                MigrationDataRow sourceRow = sourceBatch.Rows[rowIndex] ??
                    throw new InvalidDataException(
                        $"Source batch '{replayObject.SourceObjectId}'/{expectedBatchOrdinal} contains a null row.");
                long acceptedSourceRowOrdinal = acceptedSourceOrdinals[rowIndex];
                if (sourceRow.Values is null ||
                    sourceRow.Values.Count != replayObject.Columns.Length)
                {
                    throw new InvalidDataException(
                        $"Source row {acceptedSourceRowOrdinal} for '{replayObject.SourceObjectId}' has {sourceRow.Values?.Count ?? -1} values; expected {replayObject.Columns.Length}.");
                }

                var values = new DbValue[replayObject.Columns.Length];
                for (int columnIndex = 0; columnIndex < replayObject.Columns.Length; columnIndex++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    DbValue converted;
                    try
                    {
                        converted = MigrationValueConverter.Convert(
                            sourceRow.Values[columnIndex],
                            replayObject.Columns[columnIndex],
                            replayObject.Mappings[columnIndex],
                            acceptedSourceRowOrdinal);
                    }
                    catch (MigrationValueException error)
                    {
                        throw new MigrationRowRejectedException(
                            error.Code,
                            replayObject.SourceObjectId,
                            replayObject.Columns[columnIndex].ObjectId,
                            sourceBatch.BatchOrdinal,
                            acceptedSourceRowOrdinal,
                            error);
                    }

                    int valueBytes = MigrationValueConverter.GetCanonicalByteCount(converted);
                    if (valueBytes > _plan.Load.MaxValueBytes)
                    {
                        throw new MigrationRowRejectedException(
                            "MIG-APPLY-VALUE-SIZE-001",
                            replayObject.SourceObjectId,
                            replayObject.Columns[columnIndex].ObjectId,
                            sourceBatch.BatchOrdinal,
                            acceptedSourceRowOrdinal,
                            new InvalidDataException(
                                $"Converted value exceeds MaxValueBytes ({_plan.Load.MaxValueBytes})."));
                    }

                    bufferedBytes = checked(bufferedBytes + valueBytes);
                    RequireBatchByteLimit(
                        bufferedBytes,
                        replayObject.SourceObjectId,
                        expectedBatchOrdinal);
                    values[columnIndex] = converted;
                }

                if (sourceRow.StableKey is not null)
                {
                    bufferedBytes = checked(
                        bufferedBytes + Encoding.UTF8.GetByteCount(sourceRow.StableKey));
                    RequireBatchByteLimit(
                        bufferedBytes,
                        replayObject.SourceObjectId,
                        expectedBatchOrdinal);
                }

                targetRows[rowIndex] = new MigrationTargetRow
                {
                    SourceRowOrdinal = acceptedSourceRowOrdinal,
                    StableKey = sourceRow.StableKey,
                    Values = values,
                };
            }

            foreach (MigrationRejectedRow rejectedRow in sourceBatch.RejectedRows)
            {
                bufferedBytes = checked(
                    bufferedBytes +
                    MigrationRejectLedgerCodec.GetCanonicalEntryByteCount(
                        replayObject.SourceObjectId,
                        sourceBatch.BatchOrdinal,
                        rejectedRow));
                RequireBatchByteLimit(
                    bufferedBytes,
                    replayObject.SourceObjectId,
                    expectedBatchOrdinal);
            }

            var targetBatch = new MigrationTargetBatch
            {
                PlanDigest = _planDigest,
                CatalogDigest = _plan.CatalogDigest,
                SourceFingerprint = _plan.Source.Fingerprint,
                SourceSnapshotIdentity = SnapshotIdentity,
                SourceObjectId = replayObject.SourceObjectId,
                ColumnObjectIds = replayObject.ColumnObjectIds,
                BatchOrdinal = sourceBatch.BatchOrdinal,
                StartCursor = sourceBatch.StartCursor,
                NextCursor = sourceBatch.NextCursor,
                BatchDigest = string.Empty,
                RejectContractVersion = _rejectContractVersion,
                Rows = targetRows,
                RejectedRows = sourceBatch.RejectedRows,
            };
            targetBatch = targetBatch with
            {
                RejectDigest = MigrationRejectDigest.Compute(targetBatch),
            };
            targetBatch = targetBatch with
            {
                BatchDigest = MigrationBatchDigest.Compute(targetBatch, _batchDigestFormat),
            };

            yield return new MigrationReplayedOutcomeBatch
            {
                Batch = targetBatch,
                BufferedOutcomeCount = checked(targetRows.Length + sourceBatch.RejectedRows.Count),
                BufferedBytes = bufferedBytes,
            };

            sourceRowOrdinal = checked(
                sourceRowOrdinal + sourceBatch.Rows.Count + sourceBatch.RejectedRows.Count);
            expectedBatchOrdinal++;
            expectedStartCursor = sourceBatch.NextCursor;
        }

        if (expectedBatchOrdinal > 0 && expectedStartCursor is not null)
        {
            throw new InvalidDataException(
                $"Migration source cursor chain for '{replayObject.SourceObjectId}' did not terminate.");
        }
    }

    private long[] ValidateSourceBatch(
        MigrationDataBatch batch,
        string sourceObjectId,
        IReadOnlyList<string> columnObjectIds,
        long expectedBatchOrdinal,
        string? expectedStartCursor,
        long expectedFirstSourceRowOrdinal)
    {
        if (batch is null)
            throw new InvalidDataException("Migration source emitted a null batch.");
        if (!string.Equals(batch.SourceObjectId, sourceObjectId, StringComparison.Ordinal))
            throw new InvalidDataException("Migration source batch object identity changed during streaming.");
        if (!string.Equals(batch.SnapshotIdentity, SnapshotIdentity, StringComparison.Ordinal))
            throw new InvalidDataException("Migration source snapshot identity changed during streaming.");
        if (batch.ColumnObjectIds is null ||
            !batch.ColumnObjectIds.SequenceEqual(columnObjectIds, StringComparer.Ordinal))
        {
            throw new InvalidDataException(
                "Migration source batch column order does not match the read request.");
        }
        if (batch.BatchOrdinal != expectedBatchOrdinal)
        {
            throw new InvalidDataException(
                $"Migration source batch ordinal {batch.BatchOrdinal} does not match expected ordinal {expectedBatchOrdinal}.");
        }
        if (!string.Equals(batch.StartCursor, expectedStartCursor, StringComparison.Ordinal))
            throw new InvalidDataException("Migration source cursor chain changed during streaming.");
        if (batch.Rows is null || batch.RejectedRows is null)
            throw new InvalidDataException("Migration source batch outcomes cannot be null.");
        long attemptedRows = checked((long)batch.Rows.Count + batch.RejectedRows.Count);
        if (attemptedRows == 0)
            throw new InvalidDataException("Migration source batches must contain at least one outcome.");
        if (attemptedRows > _plan.Load.BatchSize)
        {
            throw new InvalidDataException(
                $"Migration source batch contains {attemptedRows} outcomes; maximum is {_plan.Load.BatchSize}.");
        }

        switch (_rejectContractVersion)
        {
            case MigrationRejectContract.DeterministicFailFastV1:
                if (batch.RejectedRows.Count != 0)
                {
                    throw new InvalidDataException(
                        "Fail-fast migration sources cannot emit durable rejected-row outcomes.");
                }
                break;

            case MigrationRejectContract.DeterministicRejectsV1:
                ValidateSourceRejects(
                    batch.RejectedRows,
                    columnObjectIds,
                    _plan.Load.RejectPolicy ??
                    throw new InvalidDataException(
                        "Deterministic source replay is missing its plan-bound policy."));
                break;

            default:
                throw new InvalidDataException("Migration source reject contract is unsupported.");
        }

        long expectedEndSourceRowOrdinal = checked(
            expectedFirstSourceRowOrdinal + attemptedRows);
        if (batch.RejectedRows.Count > 0 &&
            (batch.RejectedRows[0].SourceRowOrdinal < expectedFirstSourceRowOrdinal ||
             batch.RejectedRows[^1].SourceRowOrdinal >= expectedEndSourceRowOrdinal))
        {
            throw new InvalidDataException(
                "Migration source rejected-row ordinals are outside the current input interval.");
        }

        var acceptedSourceOrdinals = new long[batch.Rows.Count];
        int acceptedIndex = 0;
        int rejectedIndex = 0;
        for (long ordinal = expectedFirstSourceRowOrdinal;
             ordinal < expectedEndSourceRowOrdinal;
             ordinal++)
        {
            if (rejectedIndex < batch.RejectedRows.Count &&
                batch.RejectedRows[rejectedIndex].SourceRowOrdinal == ordinal)
            {
                rejectedIndex++;
            }
            else if (acceptedIndex < acceptedSourceOrdinals.Length)
            {
                acceptedSourceOrdinals[acceptedIndex++] = ordinal;
            }
            else
            {
                throw new InvalidDataException(
                    "Migration source batch outcomes do not cover one contiguous input interval.");
            }
        }
        if (acceptedIndex != acceptedSourceOrdinals.Length ||
            rejectedIndex != batch.RejectedRows.Count)
        {
            throw new InvalidDataException(
                "Migration source batch outcomes do not cover one contiguous input interval.");
        }
        return acceptedSourceOrdinals;
    }

    private static void ValidateSourceRejects(
        IReadOnlyList<MigrationRejectedRow> rejectedRows,
        IReadOnlyList<string> columnObjectIds,
        MigrationDeterministicRejectPolicy policy)
    {
        MigrationRejectDigest.ValidateRejectedRows(rejectedRows);
        if (rejectedRows.Count > policy.MaxRejectedRowsPerBatch)
        {
            throw new InvalidDataException(
                "Migration source reject count exceeds the plan-bound batch limit.");
        }

        long rawValueBytes = 0;
        foreach (MigrationRejectedRow rejectedRow in rejectedRows)
        {
            if (!policy.AllowedRuleIds.Contains(rejectedRow.RuleId, StringComparer.Ordinal))
            {
                throw new InvalidDataException(
                    "Migration source emitted a reject rule outside the plan-bound registry.");
            }
            if (rejectedRow.ColumnObjectId is not null &&
                !columnObjectIds.Contains(rejectedRow.ColumnObjectId, StringComparer.Ordinal))
            {
                throw new InvalidDataException(
                    "Migration source reject column is outside the requested projection.");
            }

            int rowRawValueBytes = MigrationRejectLedgerCodec.GetRawValueByteCount(rejectedRow);
            if (rowRawValueBytes > policy.MaxRawValueBytes)
            {
                throw new InvalidDataException(
                    "Migration source reject evidence exceeds the plan-bound per-row limit.");
            }
            rawValueBytes = checked(rawValueBytes + rowRawValueBytes);
        }
        if (rawValueBytes > policy.MaxRawValueBytesPerBatch)
        {
            throw new InvalidDataException(
                "Migration source reject evidence exceeds the plan-bound batch limit.");
        }
    }

    private void RequireBatchByteLimit(
        long bufferedBytes,
        string sourceObjectId,
        long batchOrdinal)
    {
        if (bufferedBytes > _plan.Load.MaxBatchBytes)
        {
            throw new InvalidDataException(
                $"Source batch '{sourceObjectId}'/{batchOrdinal} exceeds MaxBatchBytes ({_plan.Load.MaxBatchBytes}).");
        }
    }

    private static ReplayObject CreateReplayObject(
        MigrationCatalogObject sourceObject,
        MigrationCatalog catalog,
        IReadOnlyDictionary<string, MigrationPlanObject> planned)
    {
        MigrationCatalogObject[] columns = catalog.Objects
            .Where(item => item.Kind == MigrationObjectKind.Column &&
                string.Equals(item.ParentObjectId, sourceObject.ObjectId, StringComparison.Ordinal) &&
                planned[item.ObjectId].Included)
            .OrderBy(item => item.ObjectId, StringComparer.Ordinal)
            .ToArray();
        if (columns.Length == 0)
        {
            throw new InvalidDataException(
                $"Included source object '{sourceObject.ObjectId}' has no included columns.");
        }

        return new ReplayObject
        {
            SourceObjectId = sourceObject.ObjectId,
            Columns = columns,
            ColumnObjectIds = columns.Select(item => item.ObjectId).ToArray(),
            Mappings = columns
                .Select(column => planned[column.ObjectId].TypeMappings.Single())
                .ToArray(),
        };
    }

    private sealed record ReplayObject
    {
        internal required string SourceObjectId { get; init; }

        internal required string[] ColumnObjectIds { get; init; }

        internal required MigrationCatalogObject[] Columns { get; init; }

        internal required MigrationTypeMapping[] Mappings { get; init; }
    }
}
