using CSharpDB.Engine;
using CSharpDB.Native;
using CSharpDB.Primitives;
using System.Runtime.InteropServices;

namespace CSharpDB.Tests;

public sealed class NativePreparedStatementTests
{
    [Fact]
    public void NativeValueTypeCodes_PreserveLegacyIdsAndAppendDecimal()
    {
        Assert.Equal(0, (int)DbType.Null);
        Assert.Equal(1, (int)DbType.Integer);
        Assert.Equal(2, (int)DbType.Real);
        Assert.Equal(3, (int)DbType.Text);
        Assert.Equal(4, (int)DbType.Blob);
        Assert.Equal(5, (int)DbType.Decimal);
    }

    [Fact]
    public void StringCache_StoresColumnNamesAndDeclaredTypesIndependently()
    {
        var resultHandle = new IntPtr(42_450);
        try
        {
            IntPtr name = StringCache.GetOrAdd(resultHandle, 0, "amount");
            IntPtr type = StringCache.GetOrAddColumnType(
                resultHandle,
                0,
                "DECIMAL(18,4)");

            Assert.Equal("amount", Marshal.PtrToStringUTF8(name));
            Assert.Equal("DECIMAL(18,4)", Marshal.PtrToStringUTF8(type));
            Assert.NotEqual(name, type);
        }
        finally
        {
            StringCache.Remove(resultHandle);
        }
    }

    [Fact]
    public async Task ExecuteAsync_ReusesParameterizedInsertAcrossValues()
    {
        var ct = TestContext.Current.CancellationToken;

        await using var db = await Database.OpenInMemoryAsync(ct);
        await using (var create = await db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);", ct))
        {
            Assert.Equal(0, create.RowsAffected);
        }

        var statement = NativePreparedStatement.Create(db, "INSERT INTO t VALUES (@id, @name);");

        statement.BindInt64("@id", 1);
        statement.BindText("@name", "Alice");
        await using (var insert = await statement.ExecuteAsync(ct))
        {
            Assert.Equal(1, insert.RowsAffected);
        }

        statement.BindInt64("@id", 2);
        statement.BindText("@name", "Bob");
        await using (var insert = await statement.ExecuteAsync(ct))
        {
            Assert.Equal(1, insert.RowsAffected);
        }

        await using var query = await db.ExecuteAsync("SELECT name FROM t ORDER BY id;", ct);
        Assert.True(await query.MoveNextAsync(ct));
        Assert.Equal("Alice", query.Current[0].AsText);
        Assert.True(await query.MoveNextAsync(ct));
        Assert.Equal("Bob", query.Current[0].AsText);
        Assert.False(await query.MoveNextAsync(ct));
    }

    [Fact]
    public async Task ExecuteAsync_UnsupportedPreparedTemplate_FallsBackToSqlBinding()
    {
        var ct = TestContext.Current.CancellationToken;

        await using var db = await Database.OpenInMemoryAsync(ct);
        await using (var create = await db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY);", ct))
        {
            Assert.Equal(0, create.RowsAffected);
        }

        await using (var insert = await db.ExecuteAsync("INSERT INTO t VALUES (1);", ct))
        {
            Assert.Equal(1, insert.RowsAffected);
        }

        await using (var insert = await db.ExecuteAsync("INSERT INTO t VALUES (2);", ct))
        {
            Assert.Equal(1, insert.RowsAffected);
        }

        var statement = NativePreparedStatement.Create(db, "SELECT id FROM t ORDER BY id LIMIT @lim;");
        statement.BindInt64("@lim", 1);

        await using var result = await statement.ExecuteAsync(ct);
        Assert.True(result.IsQuery);
        Assert.True(await result.MoveNextAsync(ct));
        Assert.Equal(1L, result.Current[0].AsInteger);
        Assert.False(await result.MoveNextAsync(ct));
    }

    [Fact]
    public async Task ExecuteAsync_BindsExactDecimalCoefficientAndScale()
    {
        var ct = TestContext.Current.CancellationToken;

        await using var db = await Database.OpenInMemoryAsync(ct);
        await using (var create = await db.ExecuteAsync(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, amount DECIMAL(18,6));",
            ct))
        {
            Assert.Equal(0, create.RowsAffected);
        }

        var statement = NativePreparedStatement.Create(
            db,
            "INSERT INTO t (id, amount) VALUES (@id, @amount);");
        statement.BindInt64("@id", 1);
        statement.BindDecimal("@amount", 123_456_789_012_345_678L, 6);

        await using (var insert = await statement.ExecuteAsync(ct))
        {
            Assert.Equal(1, insert.RowsAffected);
        }

        await using var query = await db.ExecuteAsync("SELECT amount FROM t;", ct);
        Assert.Equal("DECIMAL(18,6)", query.Schema[0].EffectiveType.ToSql());
        Assert.True(await query.MoveNextAsync(ct));
        Assert.Equal(123_456_789_012_345_678L, query.Current[0].DecimalCoefficient);
        Assert.Equal(6, query.Current[0].DecimalScale);
        Assert.False(await query.MoveNextAsync(ct));
    }
    [Fact]
    public async Task ExecuteAsync_BindsBitStringsAndBlobsWithoutLosingBitLength()
    {
        var ct = TestContext.Current.CancellationToken;

        await using var db = await Database.OpenInMemoryAsync(ct);
        await using (var create = await db.ExecuteAsync(
            "CREATE TABLE native_bits (" +
            "id INTEGER PRIMARY KEY, short_bits VARBIT(8), " +
            "full_bits VARBIT(8), payload BLOB);",
            ct))
        {
            Assert.Equal(0, create.RowsAffected);
        }

        var statement = NativePreparedStatement.Create(
            db,
            "INSERT INTO native_bits VALUES (@id, @short, @full, @payload);");
        statement.BindInt64("@id", 1);
        statement.BindBitString("@short", [0x80], 1);
        statement.BindBitString("@full", [0x80], 8);
        statement.BindBlob("@payload", [0x80]);

        await using (var insert = await statement.ExecuteAsync(ct))
            Assert.Equal(1, insert.RowsAffected);

        await using var query = await db.ExecuteAsync(
            "SELECT short_bits, full_bits, payload FROM native_bits;",
            ct);
        Assert.True(await query.MoveNextAsync(ct));
        DbValue shortBits = query.Current[0];
        DbValue fullBits = query.Current[1];
        DbValue blob = query.Current[2];

        Assert.Equal(new byte[] { 0x80 }, shortBits.AsBlob);
        Assert.Equal(new byte[] { 0x80 }, fullBits.AsBlob);
        Assert.Equal(1, NativeExports.GetBitLength(shortBits));
        Assert.Equal(8, NativeExports.GetBitLength(fullBits));
        Assert.Equal(-1, NativeExports.GetBitLength(blob));
        Assert.False(blob.IsBitString);
        Assert.Throws<ArgumentException>(() =>
            statement.BindBitString("@short", [0x81], 1));
    }
}
