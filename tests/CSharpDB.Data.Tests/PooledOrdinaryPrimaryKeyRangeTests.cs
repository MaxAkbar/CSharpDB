using System.Data.Common;
using CSharpDB.Data;

namespace CSharpDB.Data.Tests;

[Collection("ConnectionPoolState")]
public sealed class PooledOrdinaryPrimaryKeyRangeTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ChangingBoundsAndOverlappingReaders_KeepOrdinaryExpressionValues(bool join)
    {
        string path = NewPath();
        try
        {
            await using var connection = await OpenAsync(path);
            await SeedAsync(connection, 512);
            using var command = CreateRangeCommand(connection, join);
            for (int execution = 0; execution < 130; execution++)
            {
                int first = 100 + 4 * (execution % 65);
                SetBounds(command, first, first + 4);
                await using var reader = await command.ExecuteReaderAsync(Ct);
                await AssertRowsAsync(reader, first, 4);
            }

            SetBounds(command, 100, 104);
            await using var firstReader = await command.ExecuteReaderAsync(Ct);
            SetBounds(command, 200, 204);
            await using var secondReader = await command.ExecuteReaderAsync(Ct);
            SetBounds(command, 900, 904);
            await AssertRowsAsync(secondReader, 200, 4);
            await AssertRowsAsync(firstReader, 100, 4);
        }
        finally { await CleanupAsync(path); }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task HeldOrdinaryReader_PreservesSnapshotAcrossWriterCompletionAndLaterDdl(bool commit)
    {
        string path = NewPath();
        try
        {
            await using var writer = await OpenAsync(path);
            await using var connection = await OpenAsync(path);
            await SeedAsync(writer, 256);
            using var command = CreateRangeCommand(connection, join: false, limit: 12);
            SetBounds(command, 250, 262);

            await using (var held = await command.ExecuteReaderAsync(Ct))
            {
                await using (var transaction = await writer.BeginTransactionAsync(Ct))
                {
                    await ExecuteAsync(writer, "UPDATE t SET value = value + 1000 WHERE id >= 250");
                    await ExecuteAsync(writer, "INSERT INTO t VALUES " + string.Join(',',
                        Enumerable.Range(256, 768).Select(id => $"({id},{id * 10})")));
                    await using (var duringWrite = await command.ExecuteReaderAsync(Ct))
                        await AssertRowsAsync(duringWrite, 250, 6);
                    if (commit) await transaction.CommitAsync(Ct);
                    else await transaction.RollbackAsync(Ct);
                }

                await AssertRowsAsync(held, 250, 6);
                await using var current = await command.ExecuteReaderAsync(Ct);
                for (int id = 250; id < (commit ? 262 : 256); id++)
                {
                    Assert.True(await current.ReadAsync(Ct));
                    Assert.Equal(id + 1, current.GetInt64(0));
                    Assert.Equal((id * 10L + (commit && id < 256 ? 1000 : 0)) * 2, current.GetInt64(1));
                }
                Assert.False(await current.ReadAsync(Ct));
            }

            // The pool permits persistent DDL after outstanding readers have been disposed.
            await ExecuteAsync(writer, "DROP TABLE t");
            await ExecuteAsync(writer, "CREATE TABLE t (value BIGINT, id INTEGER PRIMARY KEY)");
            await ExecuteAsync(writer, "INSERT INTO t VALUES (7, 250)");
            await using var replaced = await command.ExecuteReaderAsync(Ct);
            Assert.True(await replaced.ReadAsync(Ct));
            Assert.Equal(251, replaced.GetInt64(0));
            Assert.Equal(14, replaced.GetInt64(1));
            Assert.False(await replaced.ReadAsync(Ct));
        }
        finally { await CleanupAsync(path); }
    }

    private static CSharpDbCommand CreateRangeCommand(CSharpDbConnection connection, bool join, int limit = 4)
    {
        CSharpDbCommand command = connection.CreateCommand();
        command.CommandText = join
            ? $"SELECT l.id + 1 AS next_id, r.value * 2 AS twice_value FROM t l JOIN t r ON l.id = r.id WHERE l.id >= @lower AND l.id < @upper ORDER BY l.id LIMIT {limit}"
            : $"SELECT x.id + 1 AS next_id, x.value * 2 AS twice_value FROM t x WHERE x.id >= @lower AND x.id < @upper ORDER BY x.id LIMIT {limit}";
        command.Parameters.AddWithValue("lower", 0L);
        command.Parameters.AddWithValue("upper", 0L);
        command.Prepare();
        return command;
    }

    private static void SetBounds(CSharpDbCommand command, long lower, long upper)
    {
        command.Parameters[0].Value = lower;
        command.Parameters[1].Value = upper;
    }

    private static async Task AssertRowsAsync(DbDataReader reader, int first, int count)
    {
        Assert.Equal("next_id", reader.GetName(0));
        Assert.Equal("twice_value", reader.GetName(1));
        for (int id = first; id < first + count; id++)
        {
            Assert.True(await reader.ReadAsync(Ct));
            Assert.Equal(id + 1, reader.GetInt64(0));
            Assert.Equal(id * 20L, reader.GetInt64(1));
        }
        Assert.False(await reader.ReadAsync(Ct));
    }

    private static async Task SeedAsync(CSharpDbConnection connection, int count)
    {
        await ExecuteAsync(connection, "CREATE TABLE t (id INTEGER PRIMARY KEY, value BIGINT)");
        await ExecuteAsync(connection, "INSERT INTO t VALUES " + string.Join(',',
            Enumerable.Range(0, count).Select(id => $"({id},{id * 10})")));
    }

    private static async Task<CSharpDbConnection> OpenAsync(string path)
    {
        var connection = new CSharpDbConnection($"Data Source={path};Pooling=true;Max Pool Size=2");
        await connection.OpenAsync(Ct);
        return connection;
    }

    private static async Task ExecuteAsync(CSharpDbConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(Ct);
    }

    private static string NewPath()
        => Path.Combine(Path.GetTempPath(), $"pooled_ordinary_range_{Guid.NewGuid():N}.db");

    private static async Task CleanupAsync(string path)
    {
        await CSharpDbConnection.ClearAllPoolsAsync();
        foreach (string file in new[] { path, path + ".wal", path + ".lock" })
            if (File.Exists(file)) File.Delete(file);
    }
}
