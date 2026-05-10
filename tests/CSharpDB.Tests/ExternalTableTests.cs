using CSharpDB.Engine;
using CSharpDB.ImportExport.TableArchives;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public class ExternalTableTests : IAsyncLifetime
{
    private readonly string _dbPath;
    private readonly string _dbDirectory;
    private Database _db = null!;

    public ExternalTableTests()
    {
        _dbDirectory = Path.Combine(Path.GetTempPath(), $"csharpdb_external_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_dbDirectory);
        _dbPath = Path.Combine(_dbDirectory, "external.db");
    }

    public async ValueTask InitializeAsync()
    {
        _db = await Database.OpenAsync(_dbPath);
    }

    public async ValueTask DisposeAsync()
    {
        await _db.DisposeAsync();
        if (Directory.Exists(_dbDirectory))
            Directory.Delete(_dbDirectory, recursive: true);
    }

    [Fact]
    public async Task ExternalTable_CanSelectJoinFilterProjectAndListMetadata()
    {
        var ct = TestContext.Current.CancellationToken;
        await CreateCustomersAsync(ct);
        string archivePath = await ExportCustomersAsync("exports/customers.csdbtable", ct);

        await _db.ExecuteAsync("CREATE EXTERNAL TABLE archived_customers FROM 'exports/customers.csdbtable';", ct);

        await using (var result = await _db.ExecuteAsync(
            "SELECT id, name, status FROM archived_customers WHERE id >= 2 ORDER BY id",
            ct))
        {
            var rows = await result.ToListAsync(ct);
            Assert.Equal(2, rows.Count);
            Assert.Equal(2, rows[0][0].AsInteger);
            Assert.Equal("Bob", rows[0][1].AsText);
            Assert.Equal("active", rows[0][2].AsText);
        }

        await using (var join = await _db.ExecuteAsync(
            "SELECT c.name, a.status FROM customers c JOIN archived_customers a ON a.id = c.id WHERE c.id = 3",
            ct))
        {
            var row = Assert.Single(await join.ToListAsync(ct));
            Assert.Equal("Cara", row[0].AsText);
            Assert.Equal("inactive", row[1].AsText);
        }

        await using (var metadata = await _db.ExecuteAsync(
            "SELECT table_name, path, source_table_name, row_count FROM sys.external_tables WHERE table_name = 'archived_customers'",
            ct))
        {
            var row = Assert.Single(await metadata.ToListAsync(ct));
            Assert.Equal("archived_customers", row[0].AsText);
            Assert.Equal("exports/customers.csdbtable", row[1].AsText);
            Assert.Equal("customers", row[2].AsText);
            Assert.Equal(3, row[3].AsInteger);
        }

        await using (var hidden = await _db.ExecuteAsync(
            "SELECT COUNT(*) FROM sys.tables WHERE table_name = '__external_tables'",
            ct))
        {
            Assert.Equal(0, Assert.Single(await hidden.ToListAsync(ct))[0].AsInteger);
        }

        Assert.True(File.Exists(archivePath));
    }

    [Theory]
    [InlineData("INSERT INTO archived_customers VALUES (4, 'Drew', 'active');")]
    [InlineData("UPDATE archived_customers SET status = 'active' WHERE id = 1;")]
    [InlineData("DELETE FROM archived_customers WHERE id = 1;")]
    [InlineData("ALTER TABLE archived_customers ADD COLUMN note TEXT;")]
    [InlineData("CREATE INDEX idx_archived_customers_id ON archived_customers(id);")]
    public async Task ExternalTable_RejectsWriteAndSchemaMutationStatements(string sql)
    {
        var ct = TestContext.Current.CancellationToken;
        await CreateCustomersAsync(ct);
        await ExportCustomersAsync("exports/customers.csdbtable", ct);
        await _db.ExecuteAsync("CREATE EXTERNAL TABLE archived_customers FROM 'exports/customers.csdbtable';", ct);

        var error = await Assert.ThrowsAsync<CSharpDbException>(async () => await _db.ExecuteAsync(sql, ct));
        Assert.Equal(ErrorCode.SyntaxError, error.Code);
        Assert.Contains("read-only", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ExternalTable_PrimaryKeyLookupUsesNativeArchiveIndex()
    {
        var ct = TestContext.Current.CancellationToken;
        await CreateCustomersAsync(ct);
        string archivePath = await ExportCustomersAsync("exports/customers.csdbtable", ct);
        Assert.True(await TableArchiveReader.HasIntegerPrimaryKeyIndexAsync(archivePath, ct));

        await _db.ExecuteAsync("CREATE EXTERNAL TABLE archived_customers FROM 'exports/customers.csdbtable';", ct);

        await using (var result = await _db.ExecuteAsync(
            "SELECT name FROM archived_customers WHERE id = 2 AND status = 'active'",
            ct))
        {
            var row = Assert.Single(await result.ToListAsync(ct));
            Assert.Equal("Bob", row[0].AsText);
        }

        await using (var missing = await _db.ExecuteAsync(
            "SELECT name FROM archived_customers WHERE id = 42",
            ct))
        {
            Assert.Empty(await missing.ToListAsync(ct));
        }
    }

    [Fact]
    public async Task DropExternalTable_RemovesRegistration()
    {
        var ct = TestContext.Current.CancellationToken;
        await CreateCustomersAsync(ct);
        await ExportCustomersAsync("exports/customers.csdbtable", ct);
        await _db.ExecuteAsync("CREATE EXTERNAL TABLE archived_customers FROM 'exports/customers.csdbtable';", ct);

        await _db.ExecuteAsync("DROP EXTERNAL TABLE archived_customers;", ct);

        await using var metadata = await _db.ExecuteAsync("SELECT COUNT(*) FROM sys.external_tables", ct);
        Assert.Equal(0, Assert.Single(await metadata.ToListAsync(ct))[0].AsInteger);
    }

    [Fact]
    public async Task DropExternalTable_ThenCreateExternalTable_ReplacesArchivePath()
    {
        var ct = TestContext.Current.CancellationToken;
        await CreateCustomersAsync(ct);
        await ExportCustomersAsync("exports/customers-old.csdbtable", ct);
        await _db.ExecuteAsync("CREATE EXTERNAL TABLE archived_customers FROM 'exports/customers-old.csdbtable';", ct);

        await _db.ExecuteAsync("INSERT INTO customers VALUES (4, 'Drew', 'active')", ct);
        await ExportCustomersAsync("exports/customers-new.csdbtable", ct);
        await _db.ExecuteAsync("DROP EXTERNAL TABLE IF EXISTS archived_customers;", ct);

        await using (var allAfterDrop = await _db.ExecuteAsync("SELECT COUNT(*) FROM sys.external_tables", ct))
        {
            Assert.Equal(0, Assert.Single(await allAfterDrop.ToListAsync(ct))[0].AsInteger);
        }

        await using (var afterDrop = await _db.ExecuteAsync(
            "SELECT COUNT(*) FROM sys.external_tables WHERE table_name = 'archived_customers'",
            ct))
        {
            Assert.Equal(0, Assert.Single(await afterDrop.ToListAsync(ct))[0].AsInteger);
        }

        await _db.ExecuteAsync("CREATE EXTERNAL TABLE archived_customers FROM 'exports/customers-new.csdbtable';", ct);

        await using (var metadata = await _db.ExecuteAsync(
            "SELECT path, row_count FROM sys.external_tables WHERE table_name = 'archived_customers'",
            ct))
        {
            var row = Assert.Single(await metadata.ToListAsync(ct));
            Assert.Equal("exports/customers-new.csdbtable", row[0].AsText);
            Assert.Equal(4, row[1].AsInteger);
        }

        await using var count = await _db.ExecuteAsync("SELECT COUNT(*) FROM archived_customers", ct);
        Assert.Equal(4, Assert.Single(await count.ToListAsync(ct))[0].AsInteger);
    }

    private async Task CreateCustomersAsync(CancellationToken ct)
    {
        await _db.ExecuteAsync("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, status TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO customers VALUES (1, 'Alice', 'active')", ct);
        await _db.ExecuteAsync("INSERT INTO customers VALUES (2, 'Bob', 'active')", ct);
        await _db.ExecuteAsync("INSERT INTO customers VALUES (3, 'Cara', 'inactive')", ct);
    }

    private async Task<string> ExportCustomersAsync(string relativeArchivePath, CancellationToken ct)
    {
        string archivePath = Path.Combine(_dbDirectory, relativeArchivePath);
        Directory.CreateDirectory(Path.GetDirectoryName(archivePath)!);
        TableSchema schema = _db.GetTableSchema("customers")!;
        await using var result = await _db.ExecuteAsync("SELECT * FROM customers ORDER BY id", ct);
        var rows = await result.ToListAsync(ct);
        await TableArchiveWriter.WriteAsync(archivePath, schema, TableArchiveWriter.ToAsyncRows(rows, ct), ct);
        return archivePath;
    }
}
