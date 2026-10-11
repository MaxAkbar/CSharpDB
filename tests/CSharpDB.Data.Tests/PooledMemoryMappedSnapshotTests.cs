using System.Data.Common;
using CSharpDB.Engine;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Data.Tests;

[Collection("ConnectionPoolState")]
public sealed class PooledMemoryMappedSnapshotTests
{
    [Theory(Timeout = 60_000)]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task HeldPreparedReader_WithGrowingWriter_KeepsSnapshotAndReopensDurably(
        bool mapped, bool commit)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(Path.GetTempPath(), $"pooled_mapped_snapshot_{Guid.NewGuid():N}.db");
        var options = new DatabaseOptions
        {
            StorageEngineOptions = new StorageEngineOptions
            {
                PagerOptions = new PagerOptions
                {
                    UseMemoryMappedReads = mapped,
                    MaxCachedPages = 2,
                    CheckpointPolicy = new FrameCountCheckpointPolicy(1),
                },
            },
        };
        string payload = new('x', 256);
        try
        {
            await using (var seed = await Database.OpenAsync(path, ct))
            {
                await seed.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, value INTEGER, payload TEXT)", ct);
                await seed.ExecuteAsync("INSERT INTO t VALUES " + string.Join(',',
                    Enumerable.Range(0, 256).Select(id => $"({id},{id},'{payload}')")), ct);
                await seed.CheckpointAsync(ct);
            }
            long originalLength = new FileInfo(path).Length;
            string connectionString = $"Data Source={path};Pooling=true;Max Pool Size=2";
            await using (var writer = new CSharpDbConnection(connectionString, options))
            await using (var connection = new CSharpDbConnection(connectionString, options))
            {
                await writer.OpenAsync(ct);
                await connection.OpenAsync(ct);
                using var command = connection.CreateCommand();
                command.CommandText = "SELECT id, value, payload FROM t WHERE id >= @lower AND id < @upper ORDER BY id";
                command.Parameters.AddWithValue("lower", 0L);
                command.Parameters.AddWithValue("upper", 1024L);
                command.Prepare();

                await using (var held = await command.ExecuteReaderAsync(ct))
                {
                    // Start the cursor before the writer changes pages, so this reader
                    // retains a live base-file mapping and crosses multiple later leaves.
                    Assert.True(await held.ReadAsync(ct));
                    AssertRow(held, 0, 0, payload);
                    string retainedText = held.GetString(2);
                    await using (var transaction = await writer.BeginTransactionAsync(ct))
                    {
                        await ExecuteAsync(writer, "UPDATE t SET value = value + 1000", ct);
                        await ExecuteAsync(writer, "INSERT INTO t VALUES " + string.Join(',',
                            Enumerable.Range(256, 768).Select(id => $"({id},{id},'{payload}')")), ct);
                        await using (var uncommitted = await command.ExecuteReaderAsync(ct))
                            await AssertRowsAsync(uncommitted, 0, 256, changed: false, payload, ct);
                        if (commit) await transaction.CommitAsync(ct);
                        else await transaction.RollbackAsync(ct);
                    }
                    await AssertRowsAsync(held, 1, 256, changed: false, payload, ct);
                    Assert.Equal(payload, retainedText);
                    await using var current = await command.ExecuteReaderAsync(ct);
                    await AssertRowsAsync(current, 0, commit ? 1024 : 256, commit, payload, ct);
                }

                // Reuse the prepared command after releasing both snapshot readers.
                command.Parameters[0].Value = 250L;
                command.Parameters[1].Value = 270L;
                await using var next = await command.ExecuteReaderAsync(ct);
                await AssertRowsAsync(next, 250, commit ? 270 : 256, commit, payload, ct);
            }

            await CSharpDbConnection.ClearAllPoolsAsync();
            await using var reopened = await Database.OpenAsync(path, options, ct);
            await reopened.CheckpointAsync(ct);
            Assert.Equal(commit, new FileInfo(path).Length > originalLength);
            await using var result = await reopened.ExecuteAsync("SELECT id, value, payload FROM t ORDER BY id", ct);
            var rows = await result.ToListAsync(ct);
            Assert.Equal(commit ? 1024 : 256, rows.Count);
            for (int id = 0; id < rows.Count; id++)
            {
                Assert.Equal(id, rows[id][0].AsInteger);
                Assert.Equal(id + (commit && id < 256 ? 1000 : 0), rows[id][1].AsInteger);
                Assert.Equal(payload, rows[id][2].AsText);
            }
        }
        finally
        {
            await CSharpDbConnection.ClearAllPoolsAsync();
            foreach (string file in new[] { path, path + ".wal", path + ".lock" })
                if (File.Exists(file)) File.Delete(file);
        }
    }

    private static async Task ExecuteAsync(CSharpDbConnection connection, string sql, CancellationToken ct)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task AssertRowsAsync(DbDataReader reader, int first, int end, bool changed, string payload, CancellationToken ct)
    {
        for (int id = first; id < end; id++)
        {
            Assert.True(await reader.ReadAsync(ct));
            AssertRow(reader, id, id + (changed && id < 256 ? 1000 : 0), payload);
        }
        Assert.False(await reader.ReadAsync(ct));
    }

    private static void AssertRow(DbDataReader reader, long id, long value, string payload)
    {
        Assert.Equal(id, reader.GetInt64(0));
        Assert.Equal(value, reader.GetInt64(1));
        Assert.Equal(payload, reader.GetString(2));
    }
}
