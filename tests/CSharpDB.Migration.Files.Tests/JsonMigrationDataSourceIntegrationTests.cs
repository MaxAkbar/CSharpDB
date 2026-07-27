using System.Text;
using CSharpDB.Migration;
using CSharpDB.Migration.Files.Json;
using CSharpDB.Migration.Validation;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Files.Tests;

public sealed class JsonMigrationDataSourceIntegrationTests
{
    [Theory]
    [InlineData("""[{"id":1},{"id":"bad"},{"id":2}]""", 2, 1)]
    [InlineData("""[{"id":1},{"id":"bad"},{"id":"worse"}]""", 1, 2)]
    public async Task DeterministicApplyReplaysRealJsonOutcomesAgainstLedgerCapableTarget(
        string json,
        int expectedAccepted,
        int expectedRejected)
    {
        CancellationToken cancellationToken =
            TestContext.Current.CancellationToken;
        await using JsonSourceSnapshot sourceSnapshot =
            await JsonSourceSnapshot.CreateAsync(
                new MemoryStream(Utf8Bytes(json)),
                cancellationToken: cancellationToken);
        JsonSourceBinding binding = await JsonSourceBinding.CreateAsync(
            sourceSnapshot,
            cancellationToken: cancellationToken);
        JsonTableSchemaInferenceResult inferred =
            await JsonTableSchemaInferer.InferAsync(
                binding,
                sourceSnapshot,
                maxProfileRecords: 1,
                new JsonTableSchemaInferenceOptions
                {
                    ColumnOverrides =
                    [
                        new JsonTableColumnSchemaOverride
                        {
                            ColumnIndex = 0,
                            ExpectedPropertyName = "id",
                            LogicalType =
                                JsonTableColumnLogicalType.SignedInteger,
                            Nullable = false,
                        },
                    ],
                },
                cancellationToken);
        MigrationCatalog catalog = inferred.CreateCatalog(
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion);
        MigrationPlan failFast = ReadyPlan(catalog, batchSize: 2);
        MigrationPlan plan = failFast with
        {
            Load = failFast.Load with
            {
                RejectMode = MigrationRejectMode.DeterministicRejects,
                RejectPolicy = new MigrationDeterministicRejectPolicy
                {
                    ContractVersion =
                        MigrationRejectContract.DeterministicRejectsV1,
                    AllowedRuleIds =
                    [
                        JsonMigrationDataRules.TypeMismatch,
                    ],
                    MaxRejectedRowsPerBatch = 2,
                    MaxRejectedRowsPerRun = 10,
                    MaxRawValueBytes = 4_096,
                    MaxRawValueBytesPerBatch = 64 * 1_024,
                    MaxRawValueBytesPerRun = 1024 * 1_024,
                    MaxArtifactBytes = 1024 * 1_024,
                },
            },
        };

        await using JsonMigrationDataSource source =
            await JsonMigrationDataSource.CreateAsync(
                inferred,
                sourceSnapshot,
                catalog,
                cancellationToken);
        await using var target = new ReceiptMigrationTarget();
        var request = new MigrationApplyRequest
        {
            Plan = plan,
            Catalog = catalog,
            Source = source,
            Target = target,
        };
        var runner = new MigrationApplyRunner();

        MigrationApplyResult first =
            await runner.ApplyAsync(request, cancellationToken);
        MigrationApplyResult replay =
            await runner.ApplyAsync(request, cancellationToken);

        Assert.Equal(expectedAccepted, first.RowsWritten);
        Assert.Equal(expectedRejected, first.RejectedRowsWritten);
        Assert.Equal(0, first.RowsSkipped);
        Assert.Equal(0, first.RejectedRowsSkipped);
        Assert.Equal(0, replay.RowsWritten);
        Assert.Equal(0, replay.RejectedRowsWritten);
        Assert.Equal(expectedAccepted, replay.RowsSkipped);
        Assert.Equal(expectedRejected, replay.RejectedRowsSkipped);
        Assert.Equal(
            Enumerable.Range(0, expectedAccepted + expectedRejected)
                .Select(value => (long)value),
            target.Batches
                .SelectMany(batch =>
                    batch.Rows
                        .Select(row => row.SourceRowOrdinal)
                        .Concat(batch.RejectedRows.Select(
                            row => row.SourceRowOrdinal)))
                .Order());
        Assert.Equal(
            expectedRejected,
            target.Receipts.Sum(receipt =>
                receipt.RejectedRowCount));
        Assert.Contains(
            target.Batches,
            batch =>
                batch.Rows.Count > 0 &&
                batch.RejectedRows.Count > 0);
        if (expectedRejected == 2)
        {
            Assert.Contains(
                target.Batches,
                batch =>
                    batch.Rows.Count == 0 &&
                    batch.RejectedRows.Count > 0 &&
                    batch.NextCursor is null);
        }

        var ledger = new List<MigrationRejectLedgerEntry>();
        await foreach (MigrationRejectLedgerEntry entry in
                       target.ReadRejectLedgerAsync(
                           first.PlanDigest,
                           cancellationToken))
        {
            ledger.Add(entry);
        }
        Assert.Equal(expectedRejected, ledger.Count);
        Assert.All(
            ledger,
            entry => Assert.Equal(
                JsonMigrationDataRules.TypeMismatch,
                entry.RejectedRow.RuleId));
    }

    [Fact]
    public async Task ApplyWritesJsonBatchesThenSkipsTheSameReceiptsOnReplay()
    {
        const string json =
            """
            [
              {"id":1,"name":"alpha"},
              {"id":2,"name":"beta"},
              {"id":3,"name":"multi\nline"},
              {"id":4,"name":"delta"},
              {"id":5,"name":"epsilon"}
            ]
            """;
        CancellationToken cancellationToken =
            TestContext.Current.CancellationToken;

        await using JsonSourceSnapshot sourceSnapshot =
            await JsonSourceSnapshot.CreateAsync(
                new MemoryStream(Utf8Bytes(json)),
                cancellationToken: cancellationToken);
        JsonSourceBinding binding = await JsonSourceBinding.CreateAsync(
            sourceSnapshot,
            cancellationToken: cancellationToken);
        JsonTableSchemaInferenceResult inferred =
            await JsonTableSchemaInferer.InferAsync(
                binding,
                sourceSnapshot,
                maxProfileRecords: 100,
                cancellationToken: cancellationToken);
        MigrationCatalog catalog = inferred.CreateCatalog(
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion);
        MigrationPlan plan = ReadyPlan(catalog, batchSize: 2);

        await using JsonMigrationDataSource source =
            await JsonMigrationDataSource.CreateAsync(
                inferred,
                sourceSnapshot,
                catalog,
                cancellationToken);
        await using var target = new ReceiptMigrationTarget();
        var request = new MigrationApplyRequest
        {
            Plan = plan,
            Catalog = catalog,
            Source = source,
            Target = target,
        };
        var runner = new MigrationApplyRunner();

        MigrationApplyResult first =
            await runner.ApplyAsync(request, cancellationToken);
        string[] firstDigests = target.Receipts
            .Select(receipt => receipt.BatchDigest)
            .ToArray();
        MigrationApplyResult replay =
            await runner.ApplyAsync(request, cancellationToken);

        Assert.Equal(3, first.BatchesWritten);
        Assert.Equal(0, first.BatchesSkipped);
        Assert.Equal(5, first.RowsWritten);
        Assert.Equal(0, first.RowsSkipped);
        Assert.Equal(0, replay.BatchesWritten);
        Assert.Equal(3, replay.BatchesSkipped);
        Assert.Equal(0, replay.RowsWritten);
        Assert.Equal(5, replay.RowsSkipped);
        Assert.Equal(3, target.WriteCount);
        Assert.Equal(
            firstDigests,
            target.Receipts.Select(receipt =>
                receipt.BatchDigest));
        Assert.All(target.Receipts, receipt =>
        {
            Assert.Equal(first.PlanDigest, receipt.PlanDigest);
            Assert.Equal(first.CatalogDigest, receipt.CatalogDigest);
            Assert.Equal(
                first.SourceSnapshotIdentity,
                receipt.SourceSnapshotIdentity);
        });
    }

    [Fact]
    public async Task ValidationCountThenRowsReplaysTheSameImmutableJsonSnapshot()
    {
        const string json =
            """
            [
              {"id":1,"name":"Alice","amount":1.25},
              {"id":2,"name":"Bob\nB","amount":2.5},
              {"id":3,"name":"","amount":3.75}
            ]
            """;
        CancellationToken cancellationToken =
            TestContext.Current.CancellationToken;

        await using JsonSourceSnapshot sourceSnapshot =
            await JsonSourceSnapshot.CreateAsync(
                new MemoryStream(Utf8Bytes(json)),
                cancellationToken: cancellationToken);
        JsonSourceBinding binding = await JsonSourceBinding.CreateAsync(
            sourceSnapshot,
            cancellationToken: cancellationToken);
        JsonTableSchemaInferenceResult inferred =
            await JsonTableSchemaInferer.InferAsync(
                binding,
                sourceSnapshot,
                maxProfileRecords: 100,
                cancellationToken: cancellationToken);
        MigrationCatalog catalog = inferred.CreateCatalog(
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion);
        MigrationPlan plan = ReadyPlan(catalog, batchSize: 2);

        await using JsonMigrationDataSource source =
            await JsonMigrationDataSource.CreateAsync(
                inferred,
                sourceSnapshot,
                catalog,
                cancellationToken);
        await using var validation =
            new MigrationDataSourceValidationSnapshot(
                plan,
                catalog,
                source);

        long count = await validation.CountAsync(
            JsonMigrationObjectIds.Table,
            cancellationToken);
        var rows = new List<MigrationValidationRow>();
        await foreach (MigrationValidationRow row in
                       validation.ReadRowsAsync(
                           JsonMigrationObjectIds.Table,
                           cancellationToken))
        {
            rows.Add(row);
        }

        Assert.Equal(3, count);
        Assert.Equal(3, rows.Count);
        Assert.Equal(
            [1L, 2L, 3L],
            rows.Select(row => row.Values[0].AsInteger));
        Assert.Equal("Alice", rows[0].Values[1].AsText);
        Assert.Equal("Bob\nB", rows[1].Values[1].AsText);
        Assert.Equal(string.Empty, rows[2].Values[1].AsText);
        Assert.All(
            rows,
            row => Assert.Equal(
                DbType.Integer,
                row.Values[2].Type));
        Assert.Equal(
            [125L, 250L, 375L],
            rows.Select(row => row.Values[2].AsInteger));
    }

    private static MigrationPlan ReadyPlan(
        MigrationCatalog catalog,
        int batchSize)
    {
        MigrationPlan draft =
            new MigrationPlanner().CreatePlan(catalog);
        return draft with
        {
            AcceptedExclusionObjectIds = draft.Objects
                .Where(item => !item.Included)
                .Select(item => item.SourceObjectId)
                .OrderBy(item => item, StringComparer.Ordinal)
                .ToArray(),
            Load = draft.Load with { BatchSize = batchSize },
        };
    }

    private static byte[] Utf8Bytes(string value) =>
        new UTF8Encoding(false, true).GetBytes(value);

    internal sealed class ReceiptMigrationTarget :
        IMigrationTarget,
        IMigrationRejectLedgerTarget,
        IMigrationBatchDigestContractTarget
    {
        private readonly Dictionary<
            (string PlanDigest, string ObjectId, long BatchOrdinal),
            MigrationBatchReceipt> receipts = [];

        private readonly List<MigrationTargetBatch> batches = [];

        public string TargetIdentity =>
            "target:json-receipt-replay";

        public string BatchDigestFormat =>
            MigrationBatchDigest.Format;

        public int WriteCount { get; private set; }

        public IReadOnlyList<MigrationBatchReceipt> Receipts =>
            receipts.Values
                .OrderBy(receipt => receipt.BatchOrdinal)
                .ToArray();

        public IReadOnlyList<MigrationTargetBatch> Batches =>
            batches;

        public ValueTask ApplySchemaAsync(
            MigrationPlan plan,
            MigrationCatalog catalog,
            MigrationSchemaStage stage,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.CompletedTask;
        }

        public ValueTask<MigrationBatchReceipt> WriteBatchAsync(
            MigrationTargetBatch batch,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var receipt = new MigrationBatchReceipt
            {
                TargetIdentity = TargetIdentity,
                PlanDigest = batch.PlanDigest,
                CatalogDigest = batch.CatalogDigest,
                SourceFingerprint = batch.SourceFingerprint,
                SourceSnapshotIdentity =
                    batch.SourceSnapshotIdentity,
                SourceObjectId = batch.SourceObjectId,
                BatchOrdinal = batch.BatchOrdinal,
                StartCursor = batch.StartCursor,
                NextCursor = batch.NextCursor,
                BatchDigest = batch.BatchDigest,
                RejectContractVersion =
                    batch.RejectContractVersion,
                RejectDigest = batch.RejectDigest,
                RowCount = batch.Rows.Count,
                RejectedRowCount = batch.RejectedRows.Count,
            };
            if (!receipts.TryAdd(
                    Key(
                        batch.PlanDigest,
                        batch.SourceObjectId,
                        batch.BatchOrdinal),
                    receipt))
            {
                throw new InvalidOperationException(
                    "The JSON apply attempted a duplicate batch write.");
            }

            batches.Add(batch);
            WriteCount++;
            return ValueTask.FromResult(receipt);
        }

        public ValueTask<MigrationBatchReceipt?> ReadReceiptAsync(
            string planDigest,
            string sourceObjectId,
            long batchOrdinal,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            receipts.TryGetValue(
                Key(planDigest, sourceObjectId, batchOrdinal),
                out MigrationBatchReceipt? receipt);
            return ValueTask.FromResult(receipt);
        }

        public async IAsyncEnumerable<MigrationBatchReceipt>
            ReadReceiptsAsync(
                string planDigest,
                string sourceObjectId,
                [System.Runtime.CompilerServices.EnumeratorCancellation]
                CancellationToken cancellationToken = default)
        {
            MigrationBatchReceipt[] snapshot = receipts.Values
                .Where(receipt =>
                    string.Equals(
                        receipt.PlanDigest,
                        planDigest,
                        StringComparison.Ordinal) &&
                    string.Equals(
                        receipt.SourceObjectId,
                        sourceObjectId,
                        StringComparison.Ordinal))
                .OrderBy(receipt => receipt.BatchOrdinal)
                .ToArray();
            foreach (MigrationBatchReceipt receipt in snapshot)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return receipt;
                await Task.Yield();
            }
        }

        public ValueTask<IValidationSnapshot>
            OpenValidationSnapshotAsync(
                CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public async IAsyncEnumerable<MigrationRejectLedgerEntry>
            ReadRejectLedgerAsync(
                string planDigest,
                [System.Runtime.CompilerServices.EnumeratorCancellation]
                CancellationToken cancellationToken = default)
        {
            foreach (MigrationTargetBatch batch in batches
                         .Where(item => string.Equals(
                             item.PlanDigest,
                             planDigest,
                             StringComparison.Ordinal))
                         .OrderBy(
                             item => item.SourceObjectId,
                             StringComparer.Ordinal)
                         .ThenBy(item => item.BatchOrdinal))
            {
                foreach (MigrationRejectedRow rejectedRow in
                         batch.RejectedRows)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    yield return new MigrationRejectLedgerEntry
                    {
                        PlanDigest = planDigest,
                        SourceObjectId = batch.SourceObjectId,
                        BatchOrdinal = batch.BatchOrdinal,
                        RejectedRow = rejectedRow,
                        RawValueByteCount =
                            MigrationRejectLedgerCodec
                                .GetRawValueByteCount(
                                    rejectedRow),
                        CanonicalEntryByteCount =
                            MigrationRejectLedgerCodec
                                .GetCanonicalEntryByteCount(
                                    batch.SourceObjectId,
                                    batch.BatchOrdinal,
                                    rejectedRow),
                    };
                    await Task.Yield();
                }
            }
        }

        public ValueTask DisposeAsync() =>
            ValueTask.CompletedTask;

        private static (
            string PlanDigest,
            string ObjectId,
            long BatchOrdinal) Key(
                string planDigest,
                string sourceObjectId,
                long batchOrdinal) =>
            (planDigest, sourceObjectId, batchOrdinal);
    }
}
