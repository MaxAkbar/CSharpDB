using System.Reflection;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Storage.Catalog;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.StorageEngine;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Tests;

public sealed class MemoryMappedSnapshotTests
{
    [Theory(Timeout = 30_000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task BaseFileSnapshot_ContinuesScanAcrossWriterGrowthAndDeferredCheckpoints(bool mapped)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = CreatePath();
        List<DbValue[]> retained = [];
        try
        {
            await using (var db = await Database.OpenAsync(path, CreateOptions(mapped), ct))
            {
                await SeedAsync(db, "items", 64, ct);
                await db.CheckpointAsync(ct);
                uint root = GetTableRoot(db, "items");
                IWriteAheadLog wal = GetWal(db);

                using (var reader = db.CreateReaderSession())
                {
                    Assert.False(GetField<WalSnapshot>(reader, "_snapshot").HasWalFrames);
                    // No ORDER BY: keep a streaming table scan open instead of a materialized sort.
                    await using (var scan = await reader.ExecuteReadAsync("SELECT id, label, payload FROM items", ct))
                    {
                        Assert.True(await scan.MoveNextAsync(ct));
                        retained.Add(scan.Current.ToArray());
                        AssertBasePageRead(reader, root, mapped);

                        await db.BeginTransactionAsync(ct);
                        await UpdateAsync(db, "items", 64, version: 1, ct);
                        await InsertAsync(db, "items", start: 64, count: 96, version: 1, ct);
                        await db.CommitAsync(ct);
                        await db.CheckpointAsync(ct);
                        await db.CheckpointAsync(ct);

                        Assert.True(wal.HasPendingCheckpoint);
                        Assert.False(wal.IsCheckpointCopyComplete);
                        await ReadRemainingAsync(scan, retained, ct);
                        AssertRows(retained, count: 64, version: 0);
                    }

                    // A fresh query on the same session also keeps the original page chain.
                    AssertRows(await ReadAllAsync(reader, "items", ct), count: 64, version: 0);
                }

                await db.CheckpointAsync(ct);
                Assert.False(wal.HasPendingCheckpoint);
                using var latest = db.CreateReaderSession();
                AssertRows(await ReadAllAsync(latest, "items", ct), count: 160, version: 1);
            }

            // Only the DbValue row wrappers were copied; text/blob values must own their data.
            AssertRows(retained, count: 64, version: 0);
        }
        finally { DeleteFiles(path); }
    }

    [Theory(Timeout = 30_000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task WalCoveredSnapshot_PreservesWalRowsAndMappedBasePagesWhileCheckpointCopies(bool mapped)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = CreatePath();
        List<DbValue[]> retained = [];
        DbValue[] retainedBaseRow = [];
        try
        {
            await using (var db = await Database.OpenAsync(path, CreateOptions(mapped), ct))
            {
                await SeedAsync(db, "base_rows", 64, ct);
                await SeedAsync(db, "versioned", 8, ct);
                await db.CheckpointAsync(ct);
                uint baseRoot = GetTableRoot(db, "base_rows");
                uint changedRoot = GetTableRoot(db, "versioned");
                await db.BeginTransactionAsync(ct);
                await UpdateAsync(db, "versioned", 8, version: 1, ct);
                await db.CommitAsync(ct);
                IWriteAheadLog wal = GetWal(db);

                using (var reader = db.CreateReaderSession())
                {
                    WalSnapshot snapshot = GetField<WalSnapshot>(reader, "_snapshot");
                    Assert.True(snapshot.HasWalFrames);
                    Assert.True(snapshot.TryGet(changedRoot, out _));
                    Assert.False(snapshot.TryGet(baseRoot, out _));

                    // Materialize a base-file page before checkpointing even though the changed
                    // table's snapshot versions are in the WAL.
                    await using (var baseScan = await reader.ExecuteReadAsync("SELECT id, label, payload FROM base_rows", ct))
                    {
                        Assert.True(await baseScan.MoveNextAsync(ct));
                        retainedBaseRow = baseScan.Current.ToArray();
                        AssertBasePageRead(reader, baseRoot, mapped);
                    }

                    await using (var scan = await reader.ExecuteReadAsync("SELECT id, label, payload FROM versioned", ct))
                    {
                        Assert.True(await scan.MoveNextAsync(ct));
                        retained.Add(scan.Current.ToArray());
                        await db.BeginTransactionAsync(ct);
                        await UpdateAsync(db, "versioned", 8, version: 2, ct);
                        await db.CommitAsync(ct);
                        await db.CheckpointAsync(ct);
                        await db.CheckpointAsync(ct);

                        Assert.True(wal.HasPendingCheckpoint);
                        Assert.True(wal.IsCheckpointCopyComplete);
                        await ReadRemainingAsync(scan, retained, ct);
                        AssertRows(retained, count: 8, version: 1);
                    }

                    AssertRows(await ReadAllAsync(reader, "base_rows", ct), count: 64, version: 0);
                    AssertRows(await ReadAllAsync(reader, "versioned", ct), count: 8, version: 1);
                }

                await db.CheckpointAsync(ct);
                Assert.False(wal.HasPendingCheckpoint);
                using var latest = db.CreateReaderSession();
                AssertRows(await ReadAllAsync(latest, "versioned", ct), count: 8, version: 2);
            }

            AssertRows(retained, count: 8, version: 1);
            AssertRow(retainedBaseRow, expectedId: 0, version: 0);
        }
        finally { DeleteFiles(path); }
    }

    [Theory(Timeout = 30_000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DisposingOneSnapshot_DoesNotInvalidateAnotherReadersBaseFileScanOrOwnedValues(bool mapped)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = CreatePath();
        DbValue[] retainedFirstRow = [];
        List<DbValue[]> retainedSecondRows = [];
        try
        {
            await using (var db = await Database.OpenAsync(path, CreateOptions(mapped), ct))
            {
                await SeedAsync(db, "items", 64, ct);
                await db.CheckpointAsync(ct);
                uint root = GetTableRoot(db, "items");
                IWriteAheadLog wal = GetWal(db);
                using (var first = db.CreateReaderSession())
                using (var second = db.CreateReaderSession())
                {
                    await using (var firstScan = await first.ExecuteReadAsync("SELECT id, label, payload FROM items", ct))
                    {
                        Assert.True(await firstScan.MoveNextAsync(ct));
                        retainedFirstRow = firstScan.Current.ToArray();
                        AssertBasePageRead(first, root, mapped);
                    }

                    await using (var secondScan = await second.ExecuteReadAsync("SELECT id, label, payload FROM items", ct))
                    {
                        Assert.True(await secondScan.MoveNextAsync(ct));
                        retainedSecondRows.Add(secondScan.Current.ToArray());
                        AssertBasePageRead(second, root, mapped);

                        await db.BeginTransactionAsync(ct);
                        await UpdateAsync(db, "items", 64, version: 1, ct);
                        await InsertAsync(db, "items", start: 64, count: 32, version: 1, ct);
                        await db.CommitAsync(ct);
                        await db.CheckpointAsync(ct);
                        first.Dispose();
                        await db.CheckpointAsync(ct);

                        Assert.True(wal.HasPendingCheckpoint);
                        Assert.False(wal.IsCheckpointCopyComplete);
                        AssertRow(retainedFirstRow, expectedId: 0, version: 0);
                        await ReadRemainingAsync(secondScan, retainedSecondRows, ct);
                        AssertRows(retainedSecondRows, count: 64, version: 0);
                    }
                }

                await db.CheckpointAsync(ct);
                Assert.False(wal.HasPendingCheckpoint);
                using var latest = db.CreateReaderSession();
                AssertRows(await ReadAllAsync(latest, "items", ct), count: 96, version: 1);
            }

            AssertRow(retainedFirstRow, expectedId: 0, version: 0);
            AssertRows(retainedSecondRows, count: 64, version: 0);
        }
        finally { DeleteFiles(path); }
    }

    private static DatabaseOptions CreateOptions(bool mapped) => new()
    {
        StorageEngineOptions = new StorageEngineOptions
        {
            PagerOptions = new PagerOptions
            {
                UseMemoryMappedReads = mapped,
                // Explicit checkpoints control the interleaving; all cache settings stay at defaults.
                CheckpointPolicy = new FrameCountCheckpointPolicy(10_000),
            },
        },
    };

    private static async Task SeedAsync(Database db, string table, int count, CancellationToken ct)
    {
        await ExecuteAsync(db, $"CREATE TABLE {table} (id INTEGER PRIMARY KEY, label TEXT, payload BLOB)", ct);
        await db.BeginTransactionAsync(ct);
        await InsertAsync(db, table, start: 0, count, version: 0, ct);
        await db.CommitAsync(ct);
    }

    private static async Task InsertAsync(Database db, string table, int start, int count, int version, CancellationToken ct)
    {
        for (int id = start; id < start + count; id++)
            await ExecuteAsync(db, $"INSERT INTO {table} VALUES ({id}, '{Text(id, version)}', X'{Convert.ToHexString(Blob(id, version))}')", ct);
    }

    private static async Task UpdateAsync(Database db, string table, int count, int version, CancellationToken ct)
    {
        for (int id = 0; id < count; id++)
            await ExecuteAsync(db, $"UPDATE {table} SET label = '{Text(id, version)}', payload = X'{Convert.ToHexString(Blob(id, version))}' WHERE id = {id}", ct);
    }

    private static async Task ExecuteAsync(Database db, string sql, CancellationToken ct)
    {
        await using var result = await db.ExecuteAsync(sql, ct);
    }

    private static async Task<List<DbValue[]>> ReadAllAsync(Database.ReaderSession reader, string table, CancellationToken ct)
    {
        await using var result = await reader.ExecuteReadAsync($"SELECT id, label, payload FROM {table}", ct);
        List<DbValue[]> rows = [];
        await ReadRemainingAsync(result, rows, ct);
        return rows;
    }

    private static async Task ReadRemainingAsync(QueryResult result, List<DbValue[]> rows, CancellationToken ct)
    {
        while (await result.MoveNextAsync(ct))
            rows.Add(result.Current.ToArray());
    }

    private static void AssertRows(List<DbValue[]> rows, int count, int version)
    {
        Assert.Equal(count, rows.Count);
        DbValue[][] ordered = rows.OrderBy(row => row[0].AsInteger).ToArray();
        for (int id = 0; id < count; id++)
            AssertRow(ordered[id], id, version);
    }

    private static void AssertRow(DbValue[] row, int expectedId, int version)
    {
        Assert.Equal(3, row.Length);
        Assert.Equal((long)expectedId, row[0].AsInteger);
        Assert.Equal(Text(expectedId, version), row[1].AsText);
        Assert.Equal(Blob(expectedId, version), row[2].AsBlob);
    }

    private static void AssertBasePageRead(Database.ReaderSession reader, uint root, bool mapped)
    {
        Pager pager = GetField<Pager>(reader, "_snapshotPager");
        IPageReadProvider provider = GetField<IPageReadProvider>(pager, "_pageReads");
        if (mapped)
            Assert.IsType<MemoryMappedPageReadProvider>(provider);
        else
            Assert.IsType<StorageDevicePageReadProvider>(provider);
        Assert.True(pager.TryGetCachedPageReadBuffer(root, out PageReadBuffer page));
        Assert.Equal(!mapped, page.TryGetOwnedBuffer(out _));
    }

    private static uint GetTableRoot(Database db, string table)
        => GetField<SchemaCatalog>(db, "_catalog").GetTableRootPage(table);

    private static IWriteAheadLog GetWal(Database db)
        => GetField<IWriteAheadLog>(GetField<Pager>(db, "_pager"), "_wal");

    private static T GetField<T>(object instance, string name)
        => Assert.IsAssignableFrom<T>(instance.GetType().GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(instance));

    private static string Text(int id, int version)
        => $"v{version}-row-{id:D3}-" + new string((char)('a' + id % 26), 192);

    private static byte[] Blob(int id, int version)
        => Enumerable.Range(0, 96).Select(index => (byte)((id * 17 + version * 31 + index) & 255)).ToArray();

    private static string CreatePath()
        => Path.Combine(Path.GetTempPath(), $"csharpdb_mapped_snapshot_{Guid.NewGuid():N}.db");

    private static void DeleteFiles(string path)
    {
        foreach (string file in new[] { path, path + ".wal", path + ".lock" })
            if (File.Exists(file)) File.Delete(file);
    }
}
