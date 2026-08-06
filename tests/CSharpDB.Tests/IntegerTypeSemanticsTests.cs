using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class IntegerTypeSemanticsTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task IntegerRange_IsEnforcedAcrossAssignmentsCastsDefaultsUpdatesAndRewrites()
    {
        await using Database database = await Database.OpenInMemoryAsync(Ct);
        await database.ExecuteAsync(
            "CREATE TABLE integer_values (" +
            "id BIGINT PRIMARY KEY, value INTEGER DEFAULT 2147483647, wide BIGINT)",
            Ct);
        await database.ExecuteAsync(
            "INSERT INTO integer_values (id, value, wide) VALUES " +
            "(1, -2147483648, -9223372036854775808), " +
            "(2, 2147483647, 9223372036854775807), " +
            "(3, DEFAULT, 2147483648)",
            Ct);

        await AssertTypeMismatchAsync(
            database,
            "INSERT INTO integer_values VALUES (4, 2147483648, 0)");
        await AssertTypeMismatchAsync(database, "SELECT CAST(2147483648 AS INTEGER)");
        await AssertTypeMismatchAsync(
            database,
            "UPDATE integer_values SET value = 2147483648 WHERE id = 1");

        await using (QueryResult unchanged = await database.ExecuteAsync(
            "SELECT value FROM integer_values WHERE id = 1",
            Ct))
        {
            Assert.Equal(
                int.MinValue,
                Assert.Single(await unchanged.ToListAsync(Ct))[0].AsInteger);
        }

        await database.ExecuteAsync(
            "CREATE TABLE rewrite_values (id BIGINT PRIMARY KEY, value BIGINT)",
            Ct);
        await database.ExecuteAsync(
            "INSERT INTO rewrite_values VALUES (1, 2147483648)",
            Ct);
        await AssertTypeMismatchAsync(
            database,
            "ALTER TABLE rewrite_values ALTER COLUMN value TYPE INTEGER");
        Assert.Equal(
            SqlTypeKind.BigInt,
            database.GetTableSchema("rewrite_values")!.Columns[1].EffectiveType.Kind);
    }

    [Fact]
    public async Task LiteralsAndArithmetic_UseIntegerUntilBigIntPromotionAndCheckTheirOwnRanges()
    {
        await using Database database = await Database.OpenInMemoryAsync(Ct);
        await using QueryResult result = await database.ExecuteAsync(
            "SELECT 2147483647 AS small_literal, " +
            "2147483648 AS large_literal, " +
            "-2147483648 AS minimum_literal, " +
            "2147483647 + 0 AS integer_sum, " +
            "CAST(2147483647 AS BIGINT) + 1 AS bigint_sum",
            Ct);

        DbValue[] row = Assert.Single(await result.ToListAsync(Ct));
        Assert.Equal(
            [
                SqlTypeKind.Integer,
                SqlTypeKind.BigInt,
                SqlTypeKind.Integer,
                SqlTypeKind.Integer,
                SqlTypeKind.BigInt,
            ],
            result.Schema.Select(static column => column.EffectiveType.Kind).ToArray());
        Assert.Equal(2_147_483_648L, row[4].AsInteger);

        await AssertOverflowAsync(
            database,
            "SELECT CAST(2147483647 AS INTEGER) + 1");
        await AssertOverflowAsync(
            database,
            "SELECT -CAST(-2147483648 AS INTEGER)");
        await AssertOverflowAsync(
            database,
            "SELECT CAST(9223372036854775807 AS BIGINT) + 1");

        await database.ExecuteAsync(
            "CREATE TABLE arithmetic_values (value INTEGER NOT NULL)",
            Ct);
        await database.ExecuteAsync(
            "INSERT INTO arithmetic_values VALUES (2147483647)",
            Ct);
        await AssertOverflowAsync(
            database,
            "SELECT value + 1 FROM arithmetic_values");
    }

    [Fact]
    public async Task Identities_StopAtTheirLogicalIntegerCeilings()
    {
        await using Database database = await Database.OpenInMemoryAsync(Ct);
        await database.ExecuteAsync(
            "CREATE TABLE int_identity (id INTEGER PRIMARY KEY IDENTITY, value TEXT)",
            Ct);
        await database.ExecuteAsync(
            "ALTER TABLE int_identity RESEED 2147483647",
            Ct);
        await database.ExecuteAsync(
            "INSERT INTO int_identity (value) VALUES ('last')",
            Ct);

        await using (QueryResult lastInteger = await database.ExecuteAsync(
            "SELECT id FROM int_identity",
            Ct))
        {
            Assert.Equal(
                int.MaxValue,
                Assert.Single(await lastInteger.ToListAsync(Ct))[0].AsInteger);
        }

        CSharpDbException integerExhausted = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await database.ExecuteAsync(
                "INSERT INTO int_identity (value) VALUES ('too far')",
                Ct));
        Assert.Equal(ErrorCode.ConstraintViolation, integerExhausted.Code);
        Assert.Contains("INTEGER identity", integerExhausted.Message, StringComparison.Ordinal);

        await database.ExecuteAsync(
            "CREATE TABLE bigint_identity (id BIGINT PRIMARY KEY IDENTITY, value TEXT)",
            Ct);
        await database.ExecuteAsync(
            "ALTER TABLE bigint_identity RESEED 9223372036854775807",
            Ct);
        await database.ExecuteAsync(
            "INSERT INTO bigint_identity (value) VALUES ('last')",
            Ct);

        await using (QueryResult lastBigInt = await database.ExecuteAsync(
            "SELECT id FROM bigint_identity",
            Ct))
        {
            Assert.Equal(
                long.MaxValue,
                Assert.Single(await lastBigInt.ToListAsync(Ct))[0].AsInteger);
        }

        CSharpDbException bigIntExhausted = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await database.ExecuteAsync(
                "INSERT INTO bigint_identity (value) VALUES ('too far')",
                Ct));
        Assert.Equal(ErrorCode.ConstraintViolation, bigIntExhausted.Code);
        Assert.Contains("BIGINT identity", bigIntExhausted.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task IntegerFunctions_ExposeBoundedAndUnboundedLogicalMetadata()
    {
        Assert.True(DbBuiltInFunctionRegistry.TryGet("CINT", out var cint));
        Assert.True(DbBuiltInFunctionRegistry.TryGet("CLNG", out var clng));
        Assert.NotSame(cint, clng);
        Assert.Equal(SqlTypeKind.Integer, cint.DeclaredReturnType!.Kind);
        Assert.Equal(SqlTypeKind.BigInt, clng.DeclaredReturnType!.Kind);

        await using Database database = await Database.OpenInMemoryAsync(Ct);
        await database.ExecuteAsync(
            "CREATE TABLE function_values (value INTEGER)",
            Ct);
        await database.ExecuteAsync(
            "INSERT INTO function_values VALUES (1), (2)",
            Ct);

        await using QueryResult result = await database.ExecuteAsync(
            "SELECT CINT(2147483647), CINT(2147483648), CLNG(2147483648), " +
            "LEN('abc'), COUNT(*) FROM function_values",
            Ct);
        DbValue[] row = Assert.Single(await result.ToListAsync(Ct));
        Assert.Equal(int.MaxValue, row[0].AsInteger);
        Assert.True(row[1].IsNull);
        Assert.Equal(2_147_483_648L, row[2].AsInteger);
        Assert.Equal(3L, row[3].AsInteger);
        Assert.Equal(2L, row[4].AsInteger);
        Assert.Equal(
            [
                SqlTypeKind.Integer,
                SqlTypeKind.Integer,
                SqlTypeKind.BigInt,
                SqlTypeKind.BigInt,
                SqlTypeKind.BigInt,
            ],
            result.Schema.Select(static column => column.EffectiveType.Kind).ToArray());

        await using QueryResult ranks = await database.ExecuteAsync(
            "SELECT ROW_NUMBER() OVER (ORDER BY value), " +
            "RANK() OVER (ORDER BY value), DENSE_RANK() OVER (ORDER BY value) " +
            "FROM function_values",
            Ct);
        Assert.All(
            ranks.Schema,
            static column => Assert.Equal(SqlTypeKind.BigInt, column.EffectiveType.Kind));
    }

    private static async Task AssertTypeMismatchAsync(Database database, string sql)
    {
        CSharpDbException exception = await Assert.ThrowsAsync<CSharpDbException>(
            async () =>
            {
                await using QueryResult result = await database.ExecuteAsync(sql, Ct);
                _ = await result.ToListAsync(Ct);
            });
        Assert.Equal(ErrorCode.TypeMismatch, exception.Code);
    }

    private static async Task AssertOverflowAsync(Database database, string sql)
    {
        await Assert.ThrowsAsync<OverflowException>(
            async () =>
            {
                await using QueryResult result = await database.ExecuteAsync(sql, Ct);
                _ = await result.ToListAsync(Ct);
            });
    }
}
