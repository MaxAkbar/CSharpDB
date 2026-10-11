using CSharpDB.Data;
using CSharpDB.Execution;
using System.Reflection;

namespace CSharpDB.Data.Tests;

[Collection("ConnectionPoolState")]
public sealed class PooledPreparedSnapshotRangeTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PreparedRange_DuringGrowingWriterTransaction_UsesCommittedRoots(bool commit)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(Path.GetTempPath(), $"pooled_snapshot_range_{Guid.NewGuid():N}.db");
        try
        {
            await using var writer = new CSharpDbConnection($"Data Source={path};Pooling=true;Max Pool Size=2");
            await using var reader = new CSharpDbConnection(writer.ConnectionString);
            await writer.OpenAsync(ct);
            await reader.OpenAsync(ct);
            await ExecuteAsync(writer, "CREATE TABLE t (id INTEGER PRIMARY KEY, value INTEGER)", ct);
            await ExecuteAsync(writer, "INSERT INTO t VALUES " + string.Join(',', Enumerable.Range(0, 16).Select(i => $"({i},{i * 10})")), ct);

            await using var command = reader.CreateCommand();
            command.CommandText = "SELECT id, value FROM t WHERE id >= @lower AND id < @upper ORDER BY id";
            var lower = new CSharpDbParameter("lower", 0L);
            var upper = new CSharpDbParameter("upper", 1024L);
            command.Parameters.Add(lower);
            command.Parameters.Add(upper);
            command.Prepare();

            await using var transaction = await writer.BeginTransactionAsync(ct);
            await ExecuteAsync(writer, "INSERT INTO t VALUES " + string.Join(',', Enumerable.Range(16, 1008).Select(i => $"({i},{i * 10})")), ct);
            await AssertRangeAsync(command, 0, 16, ct);
            lower.Value = 8L;
            await AssertRangeAsync(command, 8, 8, ct);
            lower.Value = 64L;
            await AssertRangeAsync(command, 64, 0, ct);
            await using (var count = reader.CreateCommand())
            {
                count.CommandText = "SELECT COUNT(*) FROM t";
                Assert.Equal(16L, await count.ExecuteScalarAsync(ct));
            }

            if (commit) await transaction.CommitAsync(ct);
            else await transaction.RollbackAsync(ct);
            lower.Value = 12L;
            upper.Value = 20L;
            await AssertRangeAsync(command, 12, commit ? 8 : 4, ct);
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
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task AssertRangeAsync(CSharpDbCommand command, int first, int count, CancellationToken ct)
    {
        await using var reader = await command.ExecuteReaderAsync(ct);
        var result = Assert.IsType<QueryResult>(typeof(CSharpDbDataReader)
            .GetField("_queryResult", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(reader));
        object? root = typeof(QueryResult)
            .GetProperty("PhysicalRootOperator", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(result);
        Assert.NotNull(root);
        Assert.Equal("PrimaryKeyRangeScanOperator", root.GetType().Name);
        for (int i = first; i < first + count; i++)
        {
            Assert.True(await reader.ReadAsync(ct));
            Assert.Equal(i, reader.GetInt64(0));
            Assert.Equal(i * 10, reader.GetInt64(1));
        }
        Assert.False(await reader.ReadAsync(ct));
    }
}
