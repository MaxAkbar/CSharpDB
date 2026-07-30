using System.Globalization;
using System.Text;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class AlterColumnConversionPropertyTests
{
    private const int Seed = 0x4C7E_2026;
    private const long ExactRealIntegerLimit = 1L << 53;

    [Fact]
    public async Task DeterministicExactIntegers_RoundTripThroughRealRewrite()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Database database = await Database.OpenInMemoryAsync(ct);
        await database.ExecuteAsync(
            "CREATE TABLE exact_numeric_values " +
            "(id INTEGER PRIMARY KEY, value INTEGER NOT NULL)",
            ct);

        var values = new List<long>
        {
            -ExactRealIntegerLimit,
            -ExactRealIntegerLimit + 1,
            -1,
            0,
            1,
            ExactRealIntegerLimit - 1,
            ExactRealIntegerLimit,
        };
        var random = new Random(Seed);
        for (int index = 0; index < 64; index++)
        {
            values.Add(
                random.NextInt64(
                    -ExactRealIntegerLimit,
                    ExactRealIntegerLimit + 1));
        }

        string insertValues = string.Join(
            ", ",
            values.Select(
                (value, index) =>
                    $"({index + 1}, {value.ToString(CultureInfo.InvariantCulture)})"));
        await database.ExecuteAsync(
            $"INSERT INTO exact_numeric_values VALUES {insertValues}",
            ct);

        await database.ExecuteAsync(
            "ALTER TABLE exact_numeric_values ALTER COLUMN value TYPE REAL",
            ct);
        await using (QueryResult realResult = await database.ExecuteAsync(
                         "SELECT value FROM exact_numeric_values ORDER BY id",
                         ct))
        {
            IReadOnlyList<DbValue[]> rows = await realResult.ToListAsync(ct);
            Assert.Equal(values.Count, rows.Count);
            for (int index = 0; index < values.Count; index++)
            {
                Assert.Equal(DbType.Real, rows[index][0].Type);
                Assert.Equal((double)values[index], rows[index][0].AsReal);
            }
        }

        await database.ExecuteAsync(
            "ALTER TABLE exact_numeric_values ALTER COLUMN value TYPE INTEGER",
            ct);
        await using QueryResult integerResult = await database.ExecuteAsync(
            "SELECT value FROM exact_numeric_values ORDER BY id",
            ct);
        IReadOnlyList<DbValue[]> integerRows =
            await integerResult.ToListAsync(ct);

        Assert.Equal(values.Count, integerRows.Count);
        for (int index = 0; index < values.Count; index++)
            Assert.Equal(values[index], integerRows[index][0].AsInteger);
    }

    [Fact]
    public async Task DeterministicUnicodeText_RoundTripsThroughBlobRewrite()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Database database = await Database.OpenInMemoryAsync(ct);
        await database.ExecuteAsync(
            "CREATE TABLE unicode_values " +
            "(id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            ct);

        var values = new List<string>
        {
            string.Empty,
            "'",
            "ASCII",
            "café",
            "東京",
            "🙂",
            "e\u0301",
        };
        string[] glyphs =
        [
            "a", "Z", "0", " ", "'", "é", "e\u0301", "東", "京", "🙂", "🚀",
        ];
        var random = new Random(Seed);
        for (int valueIndex = 0; valueIndex < 48; valueIndex++)
        {
            var builder = new StringBuilder();
            int glyphCount = random.Next(0, 13);
            for (int glyphIndex = 0; glyphIndex < glyphCount; glyphIndex++)
                builder.Append(glyphs[random.Next(glyphs.Length)]);
            values.Add(builder.ToString());
        }

        string insertValues = string.Join(
            ", ",
            values.Select(
                (value, index) =>
                    $"({index + 1}, '{value.Replace("'", "''", StringComparison.Ordinal)}')"));
        await database.ExecuteAsync(
            $"INSERT INTO unicode_values VALUES {insertValues}",
            ct);

        await database.ExecuteAsync(
            "ALTER TABLE unicode_values ALTER COLUMN value TYPE BLOB",
            ct);
        await using (QueryResult blobResult = await database.ExecuteAsync(
                         "SELECT value FROM unicode_values ORDER BY id",
                         ct))
        {
            IReadOnlyList<DbValue[]> rows = await blobResult.ToListAsync(ct);
            Assert.Equal(values.Count, rows.Count);
            for (int index = 0; index < values.Count; index++)
            {
                Assert.Equal(DbType.Blob, rows[index][0].Type);
                Assert.Equal(
                    Encoding.UTF8.GetBytes(values[index]),
                    rows[index][0].AsBlob);
            }
        }

        await database.ExecuteAsync(
            "ALTER TABLE unicode_values ALTER COLUMN value TYPE TEXT",
            ct);
        await using QueryResult textResult = await database.ExecuteAsync(
            "SELECT value FROM unicode_values ORDER BY id",
            ct);
        IReadOnlyList<DbValue[]> textRows = await textResult.ToListAsync(ct);

        Assert.Equal(values.Count, textRows.Count);
        for (int index = 0; index < values.Count; index++)
            Assert.Equal(values[index], textRows[index][0].AsText);
    }
}
