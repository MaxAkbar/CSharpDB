using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class BooleanTemporalTypeSemanticsTests
{
    [Fact]
    public async Task BooleanAliasesNormalizeNumericValuesWhileBitStringsRemainDistinct()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Database database = await Database.OpenInMemoryAsync(ct);
        await database.ExecuteAsync(
            "CREATE TABLE boolean_values (" +
            "id INTEGER PRIMARY KEY, bare_bit BIT, bool_value BOOL, boolean_value BOOLEAN, " +
            "fixed_bits BIT(3), varying_bits VARBIT(5))",
            ct);
        await database.ExecuteAsync(
            "INSERT INTO boolean_values VALUES (1, -12, 0.25, 0, '101', '11')",
            ct);

        TableSchema schema = database.GetTableSchema("boolean_values")!;
        Assert.Equal(SqlTypeKind.Boolean, schema.Columns[1].EffectiveType.Kind);
        Assert.Equal(SqlTypeKind.Boolean, schema.Columns[2].EffectiveType.Kind);
        Assert.Equal(SqlTypeKind.Boolean, schema.Columns[3].EffectiveType.Kind);
        Assert.Equal(SqlTypeDescriptor.Create(SqlTypeKind.Bit, length: 3), schema.Columns[4].EffectiveType);
        Assert.Equal(SqlTypeDescriptor.Create(SqlTypeKind.VarBit, length: 5), schema.Columns[5].EffectiveType);

        await using (QueryResult stored = await database.ExecuteAsync(
                         "SELECT bare_bit, bool_value, boolean_value, fixed_bits, varying_bits " +
                         "FROM boolean_values",
                         ct))
        {
            DbValue[] row = Assert.Single(await stored.ToListAsync(ct));
            Assert.Equal(1L, row[0].AsInteger);
            Assert.Equal(1L, row[1].AsInteger);
            Assert.Equal(0L, row[2].AsInteger);
            Assert.Equal(new byte[] { 0xA0 }, row[3].AsBlob);
            Assert.Equal(new byte[] { 0xC0 }, row[4].AsBlob);
        }

        await using QueryResult casts = await database.ExecuteAsync(
            "SELECT CAST(-2 AS BIT), CAST(0.5 AS BOOLEAN), CAST(0 AS BOOL)",
            ct);
        DbValue[] castRow = Assert.Single(await casts.ToListAsync(ct));
        Assert.Equal(new long[] { 1, 1, 0 }, castRow.Select(static value => value.AsInteger));
        Assert.All(
            casts.Schema,
            static column => Assert.Equal(SqlTypeKind.Boolean, column.EffectiveType.Kind));

        await using QueryResult booleanFunctions = await database.ExecuteAsync(
            "SELECT CBOOL(-4), ISNULL(NULL), ORDINAL_STARTS_WITH('abc', 'a')",
            ct);
        Assert.All(
            booleanFunctions.Schema,
            static column => Assert.Equal(SqlTypeKind.Boolean, column.EffectiveType.Kind));

        Assert.True(DbBuiltInScalarFunctions.TryEvaluate(
            "CBOOL",
            [DbValue.FromReal(double.Epsilon)],
            out DbValue smallestNonZero));
        Assert.Equal(1L, smallestNonZero.AsInteger);
        Assert.True(DbBuiltInScalarFunctions.TryEvaluate(
            "CBOOL",
            [DbValue.FromReal(double.NaN)],
            out DbValue nonFinite));
        Assert.True(nonFinite.IsNull);
    }

    [Fact]
    public async Task BooleanExpressionsRequireAnExplicitIntegerCastForArithmetic()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Database database = await Database.OpenInMemoryAsync(ct);
        await database.ExecuteAsync("CREATE TABLE flags (id INTEGER PRIMARY KEY, flag BIT)", ct);
        await database.ExecuteAsync("INSERT INTO flags VALUES (1, 1)", ct);

        foreach (string sql in new[]
                 {
                     "SELECT flag + 1 FROM flags",
                     "SELECT flag - 1 FROM flags",
                     "SELECT flag * 2 FROM flags",
                     "SELECT flag / 2 FROM flags",
                     "SELECT -flag FROM flags",
                     "SELECT (id = 1) + 1 FROM flags",
                     "SELECT CBOOL(1) + 1 FROM flags",
                     "SELECT ISNULL(NULL) + 1 FROM flags",
                 })
        {
            CSharpDbException error = await Assert.ThrowsAsync<CSharpDbException>(
                async () => await database.ExecuteAsync(sql, ct));
            Assert.Equal(ErrorCode.TypeMismatch, error.Code);
            Assert.Contains("BOOLEAN", error.Message, StringComparison.OrdinalIgnoreCase);
        }

        await using QueryResult explicitCast = await database.ExecuteAsync(
            "SELECT CAST(flag AS INTEGER) + 1 FROM flags",
            ct);
        Assert.Equal(
            2L,
            Assert.Single(await explicitCast.ToListAsync(ct))[0].AsInteger);
    }

    [Theory]
    [InlineData("ROWVERSION")]
    [InlineData("TIMESTAMP")]
    public async Task RowVersionTypeAliasesUseExistingGeneratedBlobStorage(string typeSql)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Database database = await Database.OpenInMemoryAsync(ct);
        await database.ExecuteAsync(
            $"CREATE TABLE versioned (id INTEGER PRIMARY KEY, version {typeSql})",
            ct);

        ColumnDefinition version = database.GetTableSchema("versioned")!.Columns[1];
        Assert.Equal(SqlTypeKind.Blob, version.EffectiveType.Kind);
        Assert.True(version.IsRowVersion);
        Assert.False(version.Nullable);

        await using QueryResult insert = await database.ExecuteAsync(
            "INSERT INTO versioned (id) VALUES (1)",
            ct);
        Assert.True(insert.TryGetGeneratedRowVersion(out byte[] token));
        Assert.Equal(8, token.Length);
    }

    [Fact]
    public async Task DateTime2AndDateTimeOffsetAreCanonicalTemporalSpellings()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Database database = await Database.OpenInMemoryAsync(ct);
        await database.ExecuteAsync(
            "CREATE TABLE temporal_values (" +
            "wall_time DATETIME2(3), offset_time DATETIMEOFFSET(4), " +
            "portable_offset TIMESTAMP(5) WITH TIME ZONE)",
            ct);

        TableSchema schema = database.GetTableSchema("temporal_values")!;
        Assert.Equal("DATETIME2(3)", schema.Columns[0].EffectiveType.ToSql());
        Assert.Equal("DATETIMEOFFSET(4)", schema.Columns[1].EffectiveType.ToSql());
        Assert.Equal("DATETIMEOFFSET(5)", schema.Columns[2].EffectiveType.ToSql());

        await using QueryResult result = await database.ExecuteAsync(
            "SELECT " +
            "CAST('2026-08-05 14:30:15.123' AS DATETIME2(3)), " +
            "CAST('2026-08-05 14:30:15.1234-07:00' AS DATETIMEOFFSET(4)), " +
            "CAST('2026-08-05 14:30:15.12345-07:00' AS TIMESTAMP(5) WITH TIME ZONE)",
            ct);

        Assert.Equal("DATETIME2(3)", result.Schema[0].EffectiveType.ToSql());
        Assert.Equal("DATETIMEOFFSET(4)", result.Schema[1].EffectiveType.ToSql());
        Assert.Equal("DATETIMEOFFSET(5)", result.Schema[2].EffectiveType.ToSql());
    }
}
