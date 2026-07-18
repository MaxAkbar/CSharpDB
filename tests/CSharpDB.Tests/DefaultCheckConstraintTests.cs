using CSharpDB.Engine;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class DefaultCheckConstraintTests : IAsyncLifetime
{
    private readonly string _dbPath =
        Path.Combine(Path.GetTempPath(), $"csharpdb_default_check_{Guid.NewGuid():N}.db");
    private Database _db = null!;

    public async ValueTask InitializeAsync() =>
        _db = await Database.OpenAsync(_dbPath, TestContext.Current.CancellationToken);

    public async ValueTask DisposeAsync()
    {
        await _db.DisposeAsync();
        if (File.Exists(_dbPath))
            File.Delete(_dbPath);
        if (File.Exists(_dbPath + ".wal"))
            File.Delete(_dbPath + ".wal");
    }

    [Fact]
    public async Task Defaults_DistinguishOmittedExplicitNullAndDefaultMarker()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync(
            "CREATE TABLE items (" +
            "id INTEGER PRIMARY KEY, " +
            "quantity INTEGER NOT NULL DEFAULT 7, " +
            "status TEXT DEFAULT 'new')",
            ct);

        await _db.ExecuteAsync("INSERT INTO items DEFAULT VALUES", ct);
        await _db.ExecuteAsync("INSERT INTO items (quantity, status) VALUES (DEFAULT, NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO items (quantity) VALUES (3)", ct);

        await using var result = await _db.ExecuteAsync(
            "SELECT id, quantity, status FROM items ORDER BY id",
            ct);
        var rows = await result.ToListAsync(ct);

        Assert.Equal(3, rows.Count);
        Assert.Equal(7L, rows[0][1].AsInteger);
        Assert.Equal("new", rows[0][2].AsText);
        Assert.Equal(7L, rows[1][1].AsInteger);
        Assert.True(rows[1][2].IsNull);
        Assert.Equal(3L, rows[2][1].AsInteger);
        Assert.Equal("new", rows[2][2].AsText);
    }

    [Fact]
    public async Task NotNullAndChecks_RejectWritesWithSqlThreeValuedLogic()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync(
            "CREATE TABLE amounts (" +
            "id INTEGER PRIMARY KEY, " +
            "amount INTEGER CONSTRAINT ck_amount_nonnegative CHECK (amount >= 0), " +
            "label TEXT NOT NULL, " +
            "CHECK (id > 0))",
            ct);

        // UNKNOWN passes CHECK; NOT NULL is enforced separately.
        await _db.ExecuteAsync("INSERT INTO amounts VALUES (1, NULL, 'unknown')", ct);

        var checkError = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _db.ExecuteAsync("INSERT INTO amounts VALUES (2, -1, 'bad')", ct));
        Assert.Equal(ErrorCode.ConstraintViolation, checkError.Code);
        Assert.Contains("ck_amount_nonnegative", checkError.Message, StringComparison.Ordinal);

        var nullError = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _db.ExecuteAsync("INSERT INTO amounts VALUES (2, 1, NULL)", ct));
        Assert.Equal(ErrorCode.ConstraintViolation, nullError.Code);
        Assert.Contains("NOT NULL", nullError.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Update_PrevalidatesAllRowsBeforeMutating()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync(
            "CREATE TABLE inventory (" +
            "id INTEGER PRIMARY KEY, " +
            "quantity INTEGER NOT NULL, " +
            "CONSTRAINT ck_inventory_quantity CHECK (quantity >= 0))",
            ct);
        await _db.ExecuteAsync("INSERT INTO inventory VALUES (1, 20), (2, 5)", ct);

        var error = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _db.ExecuteAsync("UPDATE inventory SET quantity = quantity - 10", ct));
        Assert.Equal(ErrorCode.ConstraintViolation, error.Code);

        await using var result = await _db.ExecuteAsync(
            "SELECT quantity FROM inventory ORDER BY id",
            ct);
        var rows = await result.ToListAsync(ct);
        Assert.Equal(20L, rows[0][0].AsInteger);
        Assert.Equal(5L, rows[1][0].AsInteger);
    }

    [Fact]
    public async Task DefaultsAndChecks_SurviveReopenAndAppearInCatalogs()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync(
            "CREATE TABLE persisted (" +
            "id INTEGER PRIMARY KEY, " +
            "score INTEGER DEFAULT 5, " +
            "CONSTRAINT ck_persisted_score CHECK (score BETWEEN 0 AND 10))",
            ct);

        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, ct);

        await _db.ExecuteAsync("INSERT INTO persisted (id) VALUES (1)", ct);
        var error = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _db.ExecuteAsync("UPDATE persisted SET score = 11 WHERE id = 1", ct));
        Assert.Equal(ErrorCode.ConstraintViolation, error.Code);

        await using var columns = await _db.ExecuteAsync(
            "SELECT column_default FROM sys.columns " +
            "WHERE table_name = 'persisted' AND column_name = 'score'",
            ct);
        Assert.Equal("5", Assert.Single(await columns.ToListAsync(ct))[0].AsText);

        await using var checks = await _db.ExecuteAsync(
            "SELECT constraint_name, table_name, column_name, expression_sql " +
            "FROM sys.check_constraints WHERE table_name = 'persisted'",
            ct);
        var check = Assert.Single(await checks.ToListAsync(ct));
        Assert.Equal("ck_persisted_score", check[0].AsText);
        Assert.Equal("persisted", check[1].AsText);
        Assert.True(check[2].IsNull);
        Assert.Contains("BETWEEN", check[3].AsText, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task DirectInsertBatch_UsesSharedNotNullValidation()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE direct_rows (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)", ct);

        InsertBatch batch = _db.PrepareInsertBatch("direct_rows");
        batch.AddRow(DbValue.FromInteger(1), DbValue.Null);
        var error = await Assert.ThrowsAsync<CSharpDbException>(async () => await batch.ExecuteAsync(ct));

        Assert.Equal(ErrorCode.ConstraintViolation, error.Code);
        await using var result = await _db.ExecuteAsync("SELECT COUNT(*) FROM direct_rows", ct);
        Assert.Equal(0L, Assert.Single(await result.ToListAsync(ct))[0].AsInteger);
    }

    [Fact]
    public async Task RenamePreservesAndRewritesDefaultAndCheckMetadata()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync(
            "CREATE TABLE rename_me (" +
            "id INTEGER PRIMARY KEY, " +
            "score INTEGER DEFAULT 5 CONSTRAINT ck_rename_score CHECK (score >= 0))",
            ct);

        await _db.ExecuteAsync("ALTER TABLE rename_me RENAME COLUMN score TO points", ct);
        await _db.ExecuteAsync("ALTER TABLE rename_me RENAME TO renamed", ct);
        await _db.ExecuteAsync("INSERT INTO renamed (id) VALUES (1)", ct);

        var error = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _db.ExecuteAsync("UPDATE renamed SET points = -1 WHERE id = 1", ct));
        Assert.Equal(ErrorCode.ConstraintViolation, error.Code);

        await using var rowResult = await _db.ExecuteAsync("SELECT points FROM renamed WHERE id = 1", ct);
        Assert.Equal(5L, Assert.Single(await rowResult.ToListAsync(ct))[0].AsInteger);

        await using var checkResult = await _db.ExecuteAsync(
            "SELECT column_name, expression_sql FROM sys.check_constraints " +
            "WHERE constraint_name = 'ck_rename_score'",
            ct);
        var check = Assert.Single(await checkResult.ToListAsync(ct));
        Assert.Equal("points", check[0].AsText);
        Assert.Contains("points", check[1].AsText, StringComparison.OrdinalIgnoreCase);

        var dropError = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _db.ExecuteAsync("ALTER TABLE renamed DROP COLUMN points", ct));
        Assert.Equal(ErrorCode.ConstraintViolation, dropError.Code);
    }

    [Fact]
    public async Task AlterColumn_DefaultMetadataAppliesToFutureRowsAndSurvivesReopen()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE altered_defaults (id INTEGER PRIMARY KEY, status TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO altered_defaults VALUES (1, NULL)", ct);

        await _db.ExecuteAsync(
            "ALTER TABLE altered_defaults ALTER COLUMN status SET DEFAULT 'new'",
            ct);
        await _db.ExecuteAsync("INSERT INTO altered_defaults (id) VALUES (2)", ct);

        await using (var rows = await _db.ExecuteAsync(
            "SELECT status FROM altered_defaults ORDER BY id",
            ct))
        {
            List<DbValue[]> values = await rows.ToListAsync(ct);
            Assert.True(values[0][0].IsNull);
            Assert.Equal("new", values[1][0].AsText);
        }

        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, ct);
        Assert.Equal("'new'", _db.GetTableSchema("altered_defaults")!.Columns[1].DefaultSql);

        await _db.ExecuteAsync("ALTER TABLE altered_defaults ALTER COLUMN status DROP DEFAULT", ct);
        await _db.ExecuteAsync("INSERT INTO altered_defaults (id) VALUES (3)", ct);
        await using var afterDrop = await _db.ExecuteAsync(
            "SELECT status FROM altered_defaults WHERE id = 3",
            ct);
        Assert.True(Assert.Single(await afterDrop.ToListAsync(ct))[0].IsNull);
    }

    [Fact]
    public async Task AlterColumn_NotNullValidatesExistingRowsBeforeChangingMetadata()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE altered_nullability (id INTEGER PRIMARY KEY, value TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO altered_nullability VALUES (1, NULL), (2, 'present')", ct);

        CSharpDbException error = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _db.ExecuteAsync(
                "ALTER TABLE altered_nullability ALTER COLUMN value SET NOT NULL",
                ct));
        Assert.Equal(ErrorCode.ConstraintViolation, error.Code);
        Assert.True(_db.GetTableSchema("altered_nullability")!.Columns[1].Nullable);

        await _db.ExecuteAsync("UPDATE altered_nullability SET value = 'filled' WHERE id = 1", ct);
        await _db.ExecuteAsync(
            "ALTER TABLE altered_nullability ALTER COLUMN value SET NOT NULL",
            ct);
        Assert.False(_db.GetTableSchema("altered_nullability")!.Columns[1].Nullable);

        await _db.ExecuteAsync(
            "ALTER TABLE altered_nullability ALTER COLUMN value DROP NOT NULL",
            ct);
        await _db.ExecuteAsync("INSERT INTO altered_nullability VALUES (3, NULL)", ct);
    }

    [Fact]
    public async Task AlterTable_NamedCheckIsValidatedAgainstExistingRowsAndCanBeDropped()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE altered_checks (id INTEGER PRIMARY KEY, score INTEGER)", ct);
        await _db.ExecuteAsync("INSERT INTO altered_checks VALUES (1, -1)", ct);

        CSharpDbException rejected = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _db.ExecuteAsync(
                "ALTER TABLE altered_checks ADD CONSTRAINT ck_altered_checks_score CHECK (score >= 0)",
                ct));
        Assert.Equal(ErrorCode.ConstraintViolation, rejected.Code);
        Assert.Empty(_db.GetTableSchema("altered_checks")!.CheckConstraints);

        await _db.ExecuteAsync("UPDATE altered_checks SET score = 1 WHERE id = 1", ct);
        await _db.ExecuteAsync(
            "ALTER TABLE altered_checks ADD CONSTRAINT ck_altered_checks_score CHECK (score >= 0)",
            ct);
        await _db.ExecuteAsync(
            "ALTER TABLE altered_checks DROP CONSTRAINT ck_altered_checks_score",
            ct);
        await _db.ExecuteAsync("UPDATE altered_checks SET score = -1 WHERE id = 1", ct);
    }

    [Theory]
    [InlineData("CREATE TABLE unsafe_default (id INTEGER, value INTEGER DEFAULT id)")]
    [InlineData("CREATE TABLE unsafe_function (id INTEGER DEFAULT DATE())")]
    [InlineData("CREATE TABLE unsafe_check (id INTEGER, CHECK (DATE() IS NOT NULL))")]
    [InlineData("CREATE TABLE unsafe_column_check (a INTEGER CHECK (b > 0), b INTEGER)")]
    public async Task UnsupportedDefaultAndCheckExpressions_AreRejected(string sql)
    {
        var error = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _db.ExecuteAsync(sql, TestContext.Current.CancellationToken));

        Assert.True(error.Code is ErrorCode.SyntaxError or ErrorCode.ColumnNotFound);
    }
}
