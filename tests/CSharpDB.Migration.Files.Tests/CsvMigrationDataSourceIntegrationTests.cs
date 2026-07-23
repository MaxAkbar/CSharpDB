using System.Text;
using CSharpDB.Migration;
using CSharpDB.Migration.Files.Csv;
using CSharpDB.Migration.Validation;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Files.Tests;

public sealed class CsvMigrationDataSourceIntegrationTests
{
    [Fact]
    public async Task ApplyWritesCsvBatchesThenSkipsTheSameReceiptsOnReplay()
    {
        const string csv =
            "id,name\n" +
            "1,alpha\n" +
            "2,beta\n" +
            "3,\"multi\nline\"\n" +
            "4,delta\n" +
            "5,epsilon\n";
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using CsvSourceSnapshot sourceSnapshot = await CsvSourceSnapshot.CreateAsync(
            new MemoryStream(new UTF8Encoding(false, true).GetBytes(csv)),
            cancellationToken: cancellationToken);
        CsvFormatInspection inspection = await CsvFormatInspector.InspectAsync(
            sourceSnapshot,
            new CsvReaderOptions(),
            new CsvInspectionOptions { DelimiterCandidates = [","] },
            cancellationToken);
        CsvSourceBinding binding = await CsvSourceBinding.CreateAsync(
            sourceSnapshot,
            inspection,
            cancellationToken: cancellationToken);
        CsvSchemaInferenceResult inferred = await CsvSchemaInferer.InferAsync(
            binding,
            sourceSnapshot,
            maxDataRecords: 100,
            cancellationToken: cancellationToken);
        MigrationCatalog catalog = inferred.CreateCatalog(
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion);
        MigrationPlan plan = ReadyPlan(catalog, batchSize: 2);

        await using CsvMigrationDataSource source = await CsvMigrationDataSource.CreateAsync(
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

        MigrationApplyResult first = await runner.ApplyAsync(request, cancellationToken);
        string[] firstDigests = target.Receipts
            .Select(receipt => receipt.BatchDigest)
            .ToArray();
        MigrationApplyResult replay = await runner.ApplyAsync(request, cancellationToken);

        Assert.Equal(3, first.BatchesWritten);
        Assert.Equal(0, first.BatchesSkipped);
        Assert.Equal(5, first.RowsWritten);
        Assert.Equal(0, first.RowsSkipped);
        Assert.Equal(0, replay.BatchesWritten);
        Assert.Equal(3, replay.BatchesSkipped);
        Assert.Equal(0, replay.RowsWritten);
        Assert.Equal(5, replay.RowsSkipped);
        Assert.Equal(3, target.WriteCount);
        Assert.Equal(firstDigests, target.Receipts.Select(receipt => receipt.BatchDigest));
        Assert.All(target.Receipts, receipt =>
        {
            Assert.Equal(first.PlanDigest, receipt.PlanDigest);
            Assert.Equal(first.CatalogDigest, receipt.CatalogDigest);
            Assert.Equal(first.SourceSnapshotIdentity, receipt.SourceSnapshotIdentity);
        });
    }

    [Fact]
    public async Task ValidationCountThenRowsReplaysTheSameImmutableCsvSnapshot()
    {
        const string csv =
            "id,name,amount\r\n" +
            "1,Alice,1.25\r\n" +
            "2,\"Bob\r\nB\",2.50\r\n" +
            "3,,3.75\r\n";
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using CsvSourceSnapshot sourceSnapshot = await CsvSourceSnapshot.CreateAsync(
            new MemoryStream(new UTF8Encoding(false, true).GetBytes(csv)),
            cancellationToken: cancellationToken);
        CsvFormatInspection inspection = await CsvFormatInspector.InspectAsync(
            sourceSnapshot,
            new CsvReaderOptions(),
            new CsvInspectionOptions { DelimiterCandidates = [","] },
            cancellationToken);
        CsvSourceBinding binding = await CsvSourceBinding.CreateAsync(
            sourceSnapshot,
            inspection,
            cancellationToken: cancellationToken);
        CsvSchemaInferenceResult inferred = await CsvSchemaInferer.InferAsync(
            binding,
            sourceSnapshot,
            maxDataRecords: 100,
            cancellationToken: cancellationToken);
        MigrationCatalog catalog = inferred.CreateCatalog(
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion);
        MigrationPlan plan = ReadyPlan(catalog, batchSize: 2);

        await using CsvMigrationDataSource source = await CsvMigrationDataSource.CreateAsync(
            inferred,
            sourceSnapshot,
            catalog,
            cancellationToken);
        await using var validation = new MigrationDataSourceValidationSnapshot(
            plan,
            catalog,
            source);

        long count = await validation.CountAsync("csv:table:0", cancellationToken);
        var rows = new List<MigrationValidationRow>();
        await foreach (MigrationValidationRow row in validation.ReadRowsAsync(
                           "csv:table:0",
                           cancellationToken))
        {
            rows.Add(row);
        }

        Assert.Equal(3, count);
        Assert.Equal(3, rows.Count);
        Assert.Equal([1L, 2L, 3L], rows.Select(row => row.Values[0].AsInteger));
        Assert.Equal("Alice", rows[0].Values[1].AsText);
        Assert.Equal("Bob\r\nB", rows[1].Values[1].AsText);
        Assert.Equal(string.Empty, rows[2].Values[1].AsText);
        Assert.All(rows, row => Assert.Equal(DbType.Integer, row.Values[2].Type));
        Assert.Equal([125L, 250L, 375L], rows.Select(row => row.Values[2].AsInteger));
    }

    private static MigrationPlan ReadyPlan(MigrationCatalog catalog, int batchSize)
    {
        MigrationPlan draft = new MigrationPlanner().CreatePlan(catalog);
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

    private sealed class ReceiptMigrationTarget : IMigrationTarget
    {
        private readonly Dictionary<(string PlanDigest, string ObjectId, long BatchOrdinal), MigrationBatchReceipt>
            receipts = [];

        public string TargetIdentity => "target:csv-receipt-replay";

        public int WriteCount { get; private set; }

        public IReadOnlyList<MigrationBatchReceipt> Receipts => receipts.Values
            .OrderBy(receipt => receipt.BatchOrdinal)
            .ToArray();

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
                SourceSnapshotIdentity = batch.SourceSnapshotIdentity,
                SourceObjectId = batch.SourceObjectId,
                BatchOrdinal = batch.BatchOrdinal,
                StartCursor = batch.StartCursor,
                NextCursor = batch.NextCursor,
                BatchDigest = batch.BatchDigest,
                RejectContractVersion = batch.RejectContractVersion,
                RejectDigest = batch.RejectDigest,
                RowCount = batch.Rows.Count,
                RejectedRowCount = 0,
            };
            if (!receipts.TryAdd(Key(batch.PlanDigest, batch.SourceObjectId, batch.BatchOrdinal), receipt))
                throw new InvalidOperationException("The CSV apply attempted a duplicate batch write.");
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
            receipts.TryGetValue(Key(planDigest, sourceObjectId, batchOrdinal), out MigrationBatchReceipt? receipt);
            return ValueTask.FromResult(receipt);
        }

        public async IAsyncEnumerable<MigrationBatchReceipt> ReadReceiptsAsync(
            string planDigest,
            string sourceObjectId,
            [System.Runtime.CompilerServices.EnumeratorCancellation]
            CancellationToken cancellationToken = default)
        {
            MigrationBatchReceipt[] snapshot = receipts.Values
                .Where(receipt =>
                    string.Equals(receipt.PlanDigest, planDigest, StringComparison.Ordinal) &&
                    string.Equals(receipt.SourceObjectId, sourceObjectId, StringComparison.Ordinal))
                .OrderBy(receipt => receipt.BatchOrdinal)
                .ToArray();
            foreach (MigrationBatchReceipt receipt in snapshot)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return receipt;
                await Task.Yield();
            }
        }

        public ValueTask<IValidationSnapshot> OpenValidationSnapshotAsync(
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        private static (string PlanDigest, string ObjectId, long BatchOrdinal) Key(
            string planDigest,
            string sourceObjectId,
            long batchOrdinal) => (planDigest, sourceObjectId, batchOrdinal);
    }
}
