using System.Text.Json;
using System.Buffers.Binary;
using CSharpDB.Client;
using CSharpDB.Client.Grpc;
using CSharpDB.Engine;
using CSharpDB.Storage.Diagnostics;
using CSharpDB.Storage.Diagnostics.Internal;

namespace CSharpDB.Tests;

public sealed class StorageSummaryTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Summary_ReadsOnlyPersistedHeader_EvenWithUnreadableWal()
    {
        string path = NewPath();
        try
        {
            await using (var db = await Database.OpenAsync(path, Ct))
                await db.ExecuteAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)", Ct);

            // A summary must not open the WAL or inspect the intentionally invalid extra page.
            await using var wal = new FileStream(path + ".wal", FileMode.Create, FileAccess.ReadWrite, FileShare.None);
            wal.SetLength(16 * 1024 * 1024);
            await using (var file = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
                file.SetLength(file.Length + 4096);
            var report = await DatabaseInspector.InspectAsync(path,
                new() { Mode = DatabaseInspectionMode.Summary, IncludePages = true }, Ct);
            Assert.True(report.IsSummary);
            Assert.True(report.Header.MagicValid);
            Assert.Equal(16 * 1024 * 1024, report.WalFileLengthBytes);
            Assert.Equal(0, report.PageCountScanned);
            Assert.Empty(report.PageTypeHistogram);
            Assert.Null(report.Pages);
            Assert.Null(report.IndexChecks);
            Assert.Contains(report.Issues, issue => issue.Code == "DB_PAGE_COUNT_MISMATCH");
        }
        finally { Cleanup(path); }
    }

    [Fact]
    public async Task CombinedAnalysis_MatchesDetailedScan_AndKeepsOnlyCatalogAndOverflowPayloads()
    {
        string path = NewPath();
        try
        {
            await using var db = await Database.OpenAsync(path, Ct);
            await db.ExecuteAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)", Ct);
            await db.ExecuteAsync($"INSERT INTO items VALUES (1, '{new string('x', 10000)}')", Ct);
            await db.ExecuteAsync("INSERT INTO items VALUES (2, 'small')", Ct);
            await db.ExecuteAsync("CREATE INDEX idx_items_name ON items (name)", Ct);

            var combined = await DatabaseInspector.InspectAsync(path,
                new() { Mode = DatabaseInspectionMode.FullWithIndexes }, Ct);
            var detailed = await DatabaseInspector.InspectAsync(path, new() { IncludePages = true }, Ct);
            var indexes = await IndexInspector.CheckAsync(path, ct: Ct);
            Assert.False(combined.IsSummary);
            Assert.Null(combined.Pages);
            Assert.Equal(detailed.PageCountScanned, combined.PageCountScanned);
            Assert.Equal(detailed.PageTypeHistogram.OrderBy(x => x.Key), combined.PageTypeHistogram.OrderBy(x => x.Key));
            Assert.Equal(detailed.Issues.Select(x => x.Code), combined.Issues.Select(x => x.Code));
            Assert.Equal(JsonSerializer.Serialize(indexes), JsonSerializer.Serialize(combined.IndexChecks));
            var btrees = detailed.Pages!.Where(p => p.PageTypeName is "leaf" or "interior").ToArray();
            Assert.Equal(btrees.Sum(p => (long)p.FreeSpaceBytes), combined.BTreeFreeBytes);
            Assert.Equal(btrees.Count(p => p.FreeSpaceBytes > 0), combined.PagesWithFreeSpace);
            var maintenance = await DatabaseMaintenanceCoordinator.GetMaintenanceReportAsync(path, Ct);
            Assert.Equal(combined.BTreeFreeBytes, maintenance.Fragmentation.BTreeFreeBytes);
            Assert.Equal(combined.PagesWithFreeSpace, maintenance.Fragmentation.PagesWithFreeSpace);
            Assert.Equal(combined.TailFreelistPageCount, maintenance.Fragmentation.TailFreelistPageCount);
            Assert.Equal(combined.WalFileLengthBytes, maintenance.SpaceUsage.WalFileBytes);

            var snapshot = await InspectorEngine.ReadDatabaseSnapshotAsync(path, captureLeafPayload: false, Ct);
            Assert.DoesNotContain(snapshot.Pages.Values.SelectMany(p => p.LeafCells),
                cell => cell.Payload is { Length: > 0 } && cell.Payload.AsSpan().IndexOf("small"u8) >= 0);
            Assert.Contains(snapshot.Pages.Values.SelectMany(p => p.LeafCells), cell => cell.IsOverflowPayload);
            Assert.DoesNotContain(combined.Issues, i => i.Severity == InspectSeverity.Error);
        }
        finally { Cleanup(path); }
    }

    [Fact]
    public async Task CollectionPaths_AreNotCheckedAsPhysicalColumns()
    {
        string path = NewPath();
        try
        {
            await using var db = await Database.OpenAsync(path, Ct);
            var collection = await db.GetCollectionAsync<JsonElement>("events", Ct);
            await collection.PutAsync("one", JsonSerializer.SerializeToElement(new { Header = new { Name = "test" }, Tags = new[] { "one" } }), Ct);
            await collection.EnsureIndexAsync("Header.Name", Ct);
            await collection.EnsureIndexAsync("Tags[]", Ct);
            var report = await IndexInspector.CheckAsync(path, ct: Ct);
            Assert.Equal(2, report.Indexes.Count);
            Assert.All(report.Indexes, item => Assert.True(item.ColumnsExistInTable));
            Assert.DoesNotContain(report.Issues, issue => issue.Code == "INDEX_COLUMN_MISSING");

            // Simulate older catalog metadata, which stored collection indexes as SQL indexes.
            var snapshot = await InspectorEngine.ReadDatabaseSnapshotAsync(path, captureLeafPayload: false, Ct);
            var schemaPages = InspectorEngine.WalkBTree(snapshot.Header.SchemaRootPage, snapshot.Pages,
                snapshot.PhysicalPageCount, [], "schema", Ct);
            var catalogEntry = schemaPages.SelectMany(id => snapshot.Pages[id].LeafCells)
                .Single(cell => cell.Key == InspectorEngine.IndexCatalogSentinel);
            uint indexRoot = BinaryPrimitives.ReadUInt32LittleEndian(catalogEntry.Payload!);
            var indexPages = InspectorEngine.WalkBTree(indexRoot, snapshot.Pages, snapshot.PhysicalPageCount, [], "indexes", Ct);
            foreach (uint id in indexPages)
            {
                var cells = snapshot.Pages[id].LeafCells;
                for (int i = 0; i < cells.Count; i++)
                {
                    byte[] payload = cells[i].Payload!;
                    var schema = SchemaSerializer.DeserializeIndex(payload.AsSpan(4));
                    byte[] legacy = SchemaSerializer.SerializeIndex(new()
                    {
                        IndexName = schema.IndexName, TableName = schema.TableName, Columns = schema.Columns,
                        Kind = CSharpDB.Primitives.IndexKind.Sql,
                    });
                    cells[i] = new() { Key = cells[i].Key, Payload = [.. payload.AsSpan(0, 4), .. legacy] };
                }
            }
            var legacyReport = IndexInspector.CheckSnapshot(snapshot, null, null, Ct);
            Assert.All(legacyReport.Indexes, item => Assert.True(item.ColumnsExistInTable));

            // An ordinary SQL index with a missing physical column must still be reported.
            var page = indexPages.Select(id => snapshot.Pages[id]).First(p => p.LeafCells.Count > 0);
            byte[] firstPayload = page.LeafCells[0].Payload!;
            var original = SchemaSerializer.DeserializeIndex(firstPayload.AsSpan(4));
            byte[] invalidSqlIndex = SchemaSerializer.SerializeIndex(new()
            {
                IndexName = "ordinary_sql_index", TableName = original.TableName, Columns = ["missing_column"],
            });
            page.LeafCells[0] = new() { Key = page.LeafCells[0].Key, Payload = [.. firstPayload.AsSpan(0, 4), .. invalidSqlIndex] };
            Assert.Contains(IndexInspector.CheckSnapshot(snapshot, null, null, Ct).Issues,
                issue => issue.Code == "INDEX_COLUMN_MISSING" && issue.Message.Contains("ordinary_sql_index"));
        }
        finally { Cleanup(path); }
    }

    [Fact]
    public async Task DirectClientAndGrpcMapping_PreserveInspectionModesAndAnalysisFields()
    {
        string path = NewPath();
        try
        {
            await using var client = CSharpDbClient.Create(new() { DataSource = path });
            await client.ExecuteSqlAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT); CREATE INDEX ix ON items (name)", Ct);
            var summary = await client.InspectStorageAsync(DatabaseInspectionMode.Summary, ct: Ct);
            Assert.True(summary.IsSummary);
            Assert.True(GrpcModelMapper.ToModel(GrpcModelMapper.ToMessage(summary)).IsSummary);
            var report = await client.InspectStorageAsync(DatabaseInspectionMode.FullWithIndexes, ct: Ct);
            var roundTrip = GrpcModelMapper.ToModel(GrpcModelMapper.ToMessage(report));
            Assert.Equal(JsonSerializer.Serialize(report), JsonSerializer.Serialize(roundTrip));
            Assert.Contains(roundTrip.IndexChecks!.Indexes, index => index.IndexName == "ix");
            using var cancellation = new CancellationTokenSource();
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.InspectStorageAsync(DatabaseInspectionMode.Summary, ct: cancellation.Token));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.InspectStorageAsync(DatabaseInspectionMode.FullWithIndexes, ct: cancellation.Token));
        }
        finally { Cleanup(path); }
    }

    private static string NewPath() => Path.Combine(Path.GetTempPath(), $"storage-summary-{Guid.NewGuid():N}.db");
    private static void Cleanup(string path)
    {
        File.Delete(path);
        File.Delete(path + ".wal");
    }
}
