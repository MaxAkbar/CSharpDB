using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class TemporalFractionalPrecisionTests
{
    [Fact]
    public async Task PrecisionSevenTemporalValues_RoundTripAndCompareExactly()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_temporal_precision_{Guid.NewGuid():N}.db");

        try
        {
            await using (Database database = await Database.OpenAsync(path, ct))
            {
                await database.ExecuteAsync(
                    "CREATE TABLE temporal_precision (" +
                    "id INTEGER PRIMARY KEY, " +
                    "clock_value TIME(7), " +
                    "stamp_value TIMESTAMP(7), " +
                    "zoned_value TIMESTAMP(7) WITH TIME ZONE, " +
                    "duration_value INTERVAL DAY TO SECOND(7))",
                    ct);
                await database.ExecuteAsync(
                    "INSERT INTO temporal_precision VALUES " +
                    "(1, '12:00:00.0000001', '2026-08-05 12:00:00.0000001', " +
                    "'2026-08-05 12:00:00.0000001-07:00', '-00:00:00.0000001'), " +
                    "(2, '12:00:00.0000002', '2026-08-05 12:00:00.0000002', " +
                    "'2026-08-05 12:00:00.0000002-07:00', '00:00:00.0000001'), " +
                    "(3, '12:00:00.0000010', '2026-08-05 12:00:00.0000010', " +
                    "'2026-08-05 12:00:00.0000010-07:00', '00:00:00.0000010')",
                    ct);
            }

            await using Database reopened = await Database.OpenAsync(path, ct);
            TableSchema schema = reopened.GetTableSchema("temporal_precision")!;
            Assert.All(
                schema.Columns.Skip(1),
                static column => Assert.Equal(7, column.DeclaredType!.FractionalSecondsPrecision));

            await using (QueryResult first = await reopened.ExecuteAsync(
                             "SELECT clock_value, stamp_value, zoned_value, duration_value " +
                             "FROM temporal_precision WHERE id = 1",
                             ct))
            {
                DbValue[] row = Assert.Single(await first.ToListAsync(ct));
                Assert.Equal("12:00:00.0000001", row[0].AsText);
                Assert.Equal("2026-08-05 12:00:00.0000001", row[1].AsText);
                Assert.Equal("2026-08-05 19:00:00.0000001+00:00", row[2].AsText);
                Assert.Equal("-00:00:00.0000001", row[3].AsText);
            }

            foreach ((string Column, string Boundary) comparison in new[]
                     {
                         ("clock_value", "12:00:00.0000001"),
                         ("stamp_value", "2026-08-05 12:00:00.0000001"),
                         ("zoned_value", "2026-08-05 12:00:00.0000001-07:00"),
                         ("duration_value", "-00:00:00.0000001"),
                     })
            {
                await using QueryResult result = await reopened.ExecuteAsync(
                    $"SELECT id FROM temporal_precision " +
                    $"WHERE {comparison.Column} > '{comparison.Boundary}' " +
                    $"ORDER BY {comparison.Column}",
                    ct);
                Assert.Equal(
                    new long[] { 2, 3 },
                    (await result.ToListAsync(ct)).Select(static row => row[0].AsInteger));
            }

            await using (QueryResult casts = await reopened.ExecuteAsync(
                             "SELECT " +
                             "CAST('12:00:00.0000001' AS TIME(7)), " +
                             "CAST('2026-08-05 12:00:00.0000001' AS TIMESTAMP(7)), " +
                             "CAST('2026-08-05 12:00:00.0000001-07:00' AS TIMESTAMP(7) WITH TIME ZONE), " +
                             "CAST('-00:00:00.0000001' AS INTERVAL DAY TO SECOND(7))",
                             ct))
            {
                DbValue[] row = Assert.Single(await casts.ToListAsync(ct));
                Assert.Equal("12:00:00.0000001", row[0].AsText);
                Assert.Equal("2026-08-05 12:00:00.0000001", row[1].AsText);
                Assert.Equal("2026-08-05 19:00:00.0000001+00:00", row[2].AsText);
                Assert.Equal("-00:00:00.0000001", row[3].AsText);
            }
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
            if (File.Exists(path + ".wal"))
                File.Delete(path + ".wal");
        }
    }
}
