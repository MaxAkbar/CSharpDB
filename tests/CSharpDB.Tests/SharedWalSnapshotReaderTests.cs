using CSharpDB.Engine;
using CSharpDB.Primitives;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Tests;

public sealed class SharedWalSnapshotReaderTests
{
    [Fact]
    public async Task Checkpoint_TwoReadersFromSameState_FinalizesOnlyAfterLastRetainingReaderReleases()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string databasePath = Path.Combine(Path.GetTempPath(), $"csharpdb_shared_wal_snapshot_{Guid.NewGuid():N}.db");
        string walPath = databasePath + ".wal";
        var options = new DatabaseOptions
        {
            StorageEngineOptions = new StorageEngineOptions
            {
                PagerOptions = new PagerOptions
                {
                    // Explicit checkpoints keep each reader-retention transition deterministic.
                    CheckpointPolicy = new FrameCountCheckpointPolicy(10_000),
                },
            },
        };

        try
        {
            await using (var database = await Database.OpenAsync(databasePath, options, ct))
            {
                await database.ExecuteAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)", ct);
                await database.ExecuteAsync("INSERT INTO items VALUES (1, 10)", ct);
                await database.CheckpointAsync(ct);
                await database.ExecuteAsync("UPDATE items SET value = 11 WHERE id = 1", ct);

                using var first = database.CreateReaderSession();
                using var second = database.CreateReaderSession();
                await using (var firstResult = await first.ExecuteReadAsync("SELECT value FROM items WHERE id = 1", ct))
                {
                    DbValue[] row = Assert.Single(await firstResult.ToListAsync(ct));
                    Assert.Equal(11L, row[0].AsInteger);
                }

                await database.ExecuteAsync("UPDATE items SET value = 12 WHERE id = 1", ct);
                await database.CheckpointAsync(ct);
                Assert.True(new FileInfo(walPath).Length > PageConstants.WalHeaderSize);

                first.Dispose();
                await database.CheckpointAsync(ct);
                Assert.True(new FileInfo(walPath).Length > PageConstants.WalHeaderSize);

                // The second reader has not read this page before: the old value
                // must still be available after the first reader and checkpoint finish.
                await using (var secondResult = await second.ExecuteReadAsync("SELECT value FROM items WHERE id = 1", ct))
                {
                    DbValue[] row = Assert.Single(await secondResult.ToListAsync(ct));
                    Assert.Equal(11L, row[0].AsInteger);
                }

                second.Dispose();
                await database.CheckpointAsync(ct);
                Assert.Equal(PageConstants.WalHeaderSize, new FileInfo(walPath).Length);
            }

            await using var reopened = await Database.OpenAsync(databasePath, options, ct);
            await using var latest = await reopened.ExecuteAsync("SELECT value FROM items WHERE id = 1", ct);
            DbValue[] reopenedRow = Assert.Single(await latest.ToListAsync(ct));
            Assert.Equal(12L, reopenedRow[0].AsInteger);
        }
        finally
        {
            foreach (string path in new[] { databasePath, walPath, databasePath + ".lock" })
            {
                if (File.Exists(path))
                    File.Delete(path);
            }
        }
    }
}
