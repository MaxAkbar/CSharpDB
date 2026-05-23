using CSharpDB.Engine;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class DataHygieneEngineTests : IAsyncLifetime
{
    private readonly string _dbPath;
    private Database _db = null!;

    public DataHygieneEngineTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_hygiene_test_{Guid.NewGuid():N}.db");
    }

    public async ValueTask InitializeAsync()
    {
        _db = await Database.OpenAsync(_dbPath, TestContext.Current.CancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        await _db.DisposeAsync();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        if (File.Exists(_dbPath + ".wal")) File.Delete(_dbPath + ".wal");
    }

    [Fact]
    public async Task FindDuplicates_AndDedup_RespectNullsCollationAndKeepLast()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE customers (id INTEGER PRIMARY KEY, email TEXT COLLATE NOCASE, name TEXT)", ct);
        await _db.ExecuteAsync(
            "INSERT INTO customers VALUES (1, 'A@EXAMPLE.COM', 'A'), (2, 'a@example.com', 'B'), (3, NULL, 'N1'), (4, NULL, 'N2'), (5, 'b@example.com', 'C')",
            ct);

        await using (var duplicates = await _db.ExecuteAsync("FIND DUPLICATES IN customers ON email", ct))
        {
            var rows = await duplicates.ToListAsync(ct);
            Assert.Equal(2, rows.Count);
            Assert.Contains(rows, row => row[1].AsInteger == 2 && row[2].AsInteger == 1 && row[4].AsText == "2");
            Assert.Contains(rows, row => row[0].AsText == "[NULL]" && row[2].AsInteger == 3 && row[4].AsText == "4");
        }

        await using (var dedup = await _db.ExecuteAsync("DEDUP customers ON email KEEP LAST", ct))
        {
            var row = Assert.Single(await dedup.ToListAsync(ct));
            Assert.Equal(2L, row[1].AsInteger);
            Assert.Equal(2L, row[2].AsInteger);
            Assert.Equal(2L, row[3].AsInteger);
        }

        await using var remaining = await _db.ExecuteAsync("SELECT id FROM customers ORDER BY id", ct);
        var ids = (await remaining.ToListAsync(ct)).Select(row => row[0].AsInteger).ToArray();
        Assert.Equal([2L, 4L, 5L], ids);
    }

    [Fact]
    public async Task MergeDuplicates_FillsUnambiguousNullWinnerValues_ThenDeletesDuplicateRows()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE contacts (id INTEGER PRIMARY KEY, email TEXT, phone TEXT, city TEXT)", ct);
        await _db.ExecuteAsync(
            "INSERT INTO contacts VALUES (1, 'a@example.com', '111', NULL), (2, 'a@example.com', NULL, 'Seattle'), (3, 'a@example.com', NULL, 'Seattle')",
            ct);

        await using (var merge = await _db.ExecuteAsync("MERGE DUPLICATES contacts ON email", ct))
        {
            var row = Assert.Single(await merge.ToListAsync(ct));
            Assert.Equal(1L, row[1].AsInteger);
            Assert.Equal(1L, row[2].AsInteger);
            Assert.Equal(2L, row[3].AsInteger);
            Assert.Equal(0L, row[4].AsInteger);
            Assert.True(row[5].IsNull);
        }

        await using var result = await _db.ExecuteAsync("SELECT id, phone, city FROM contacts", ct);
        var remaining = Assert.Single(await result.ToListAsync(ct));
        Assert.Equal(1L, remaining[0].AsInteger);
        Assert.Equal("111", remaining[1].AsText);
        Assert.Equal("Seattle", remaining[2].AsText);
    }

    [Fact]
    public async Task ValidationRules_AreHiddenFromTables_ExposedInCatalog_AndValidateRows()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE customers (id INTEGER PRIMARY KEY, email TEXT)", ct);
        await _db.ExecuteAsync(
            "CREATE VALIDATION RULE valid_email ON customers.email AS email LIKE '%@%' MESSAGE 'Email must contain @'",
            ct);

        Assert.Equal(0L, await ScalarIntAsync("SELECT COUNT(*) FROM sys.tables WHERE table_name = '__validation_rules'", ct));
        Assert.Equal(1L, await ScalarIntAsync("SELECT COUNT(*) FROM sys.validation_rules WHERE rule_name = 'valid_email'", ct));
        Assert.Equal(1L, await ScalarIntAsync("SELECT COUNT(*) FROM sys_validation_rules WHERE rule_name = 'valid_email'", ct));

        await _db.ExecuteAsync("INSERT INTO customers VALUES (1, 'a@example.com'), (2, 'missing-at'), (3, NULL)", ct);
        await using (var validation = await _db.ExecuteAsync("VALIDATE TABLE customers", ct))
        {
            var rows = await validation.ToListAsync(ct);
            Assert.Equal(2, rows.Count);
            Assert.All(rows, row => Assert.Equal("valid_email", row[0].AsText));
            Assert.Contains(rows, row => row[3].AsInteger == 2);
            Assert.Contains(rows, row => row[3].AsInteger == 3);
        }

        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, ct);
        Assert.Equal(1L, await ScalarIntAsync("SELECT COUNT(*) FROM sys.validation_rules WHERE rule_name = 'valid_email'", ct));
    }

    [Fact]
    public async Task FindOrphans_ExplicitReference_SkipsNullsAndReportsMissingParents()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE books (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync("CREATE TABLE bookings (id INTEGER PRIMARY KEY, book_id INTEGER)", ct);
        await _db.ExecuteAsync("INSERT INTO books VALUES (1)", ct);
        await _db.ExecuteAsync("INSERT INTO bookings VALUES (1, 1), (2, 99), (3, NULL)", ct);

        await using var orphans = await _db.ExecuteAsync("FIND ORPHANS IN bookings.book_id REFERENCES books.id", ct);
        var row = Assert.Single(await orphans.ToListAsync(ct));
        Assert.True(row[0].IsNull);
        Assert.Equal("bookings", row[1].AsText);
        Assert.Equal("book_id", row[2].AsText);
        Assert.Equal(2L, row[3].AsInteger);
        Assert.Equal("99", row[4].AsText);
        Assert.Equal("books", row[5].AsText);
        Assert.Equal("id", row[6].AsText);
    }

    private async Task<long> ScalarIntAsync(string sql, CancellationToken ct)
    {
        await using var result = await _db.ExecuteAsync(sql, ct);
        DbValue[] row = Assert.Single(await result.ToListAsync(ct));
        return row[0].AsInteger;
    }
}
