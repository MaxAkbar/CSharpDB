using CSharpDB.Data;

namespace CSharpDB.Data.Tests;

public sealed class PreparedPrimaryKeyRangeMutationTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReusedMutation_RebindsBoundsAndPreservesRollback(bool delete)
    {
        await using var connection = new CSharpDbConnection("Data Source=:memory:;Pooling=false");
        await connection.OpenAsync(Ct);
        using var setup = connection.CreateCommand();
        setup.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, value INTEGER)";
        await setup.ExecuteNonQueryAsync(Ct);
        setup.CommandText = "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)";
        await setup.ExecuteNonQueryAsync(Ct);
        using var mutation = connection.CreateCommand();
        mutation.CommandText = (delete ? "DELETE FROM t" : "UPDATE t SET value = value + 1") +
            " WHERE id >= @lower AND id < @upper";
        var lower = mutation.Parameters.AddWithValue("lower", 1L);
        var upper = mutation.Parameters.AddWithValue("upper", 4L);
        mutation.Prepare();
        (object Lower, object Upper, long[] Targets)[] cases =
        [
            (1L, 4L, [1, 2, 3]), (3L, 6L, [3, 4, 5]), (4L, 2L, []),
            (DBNull.Value, 4L, []), (1L, DBNull.Value, []), ((short)2, 4, [2, 3]),
            (1L, 2L, [1]),
        ];
        foreach (var item in cases)
        {
            await using var transaction = await connection.BeginTransactionAsync(Ct);
            lower.Value = item.Lower;
            upper.Value = item.Upper;
            Assert.Equal(item.Targets.Length, await mutation.ExecuteNonQueryAsync(Ct));
            using var query = connection.CreateCommand();
            query.CommandText = "SELECT id, value FROM t ORDER BY id";
            var actual = new List<(long Id, long Value)>();
            await using (var reader = await query.ExecuteReaderAsync(Ct))
                while (await reader.ReadAsync(Ct)) actual.Add((reader.GetInt64(0), reader.GetInt64(1)));
            Assert.Equal(Enumerable.Range(1, 5).Select(i => (long)i).Where(i => !delete || !item.Targets.Contains(i)),
                actual.Select(row => row.Id));
            Assert.All(actual, row => Assert.Equal(row.Id * 10 + (!delete && item.Targets.Contains(row.Id) ? 1 : 0), row.Value));
            await transaction.RollbackAsync(Ct);
        }
    }
}
