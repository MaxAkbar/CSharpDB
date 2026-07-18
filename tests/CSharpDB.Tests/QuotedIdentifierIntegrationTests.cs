using CSharpDB.Engine;

namespace CSharpDB.Tests;

public sealed class QuotedIdentifierIntegrationTests : IAsyncLifetime
{
    private readonly string _databasePath =
        Path.Combine(Path.GetTempPath(), $"csharpdb_quoted_identifiers_{Guid.NewGuid():N}.db");
    private Database _database = null!;

    public async ValueTask InitializeAsync()
        => _database = await Database.OpenAsync(_databasePath, ct: TestContext.Current.CancellationToken);

    public async ValueTask DisposeAsync()
    {
        await _database.DisposeAsync();
        if (File.Exists(_databasePath))
            File.Delete(_databasePath);
        if (File.Exists(_databasePath + ".wal"))
            File.Delete(_databasePath + ".wal");
    }

    [Fact]
    public async Task QuotedReservedAndWhitespaceIdentifiers_RoundTrip()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE \"select\" (\"order\" INTEGER PRIMARY KEY, \"display name\" TEXT)",
            ct);
        await _database.ExecuteAsync(
            "INSERT INTO \"select\" (\"order\", \"display name\") VALUES (1, 'quoted')",
            ct);

        await using var result = await _database.ExecuteAsync(
            "SELECT \"display name\" FROM \"select\" WHERE \"order\" = 1",
            ct);
        var rows = await result.ToListAsync(ct);

        Assert.Single(rows);
        Assert.Equal("quoted", rows[0][0].AsText);
    }

    [Fact]
    public async Task QuotedIdentifiers_PreserveEscapedDoubleQuotes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE \"say \"\"hello\"\"\" (\"value\" INTEGER)",
            ct);
        await _database.ExecuteAsync(
            "INSERT INTO \"say \"\"hello\"\"\" VALUES (42)",
            ct);

        await using var result = await _database.ExecuteAsync(
            "SELECT \"value\" FROM \"say \"\"hello\"\"\"",
            ct);
        var rows = await result.ToListAsync(ct);

        Assert.Single(rows);
        Assert.Equal(42, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task QuotedIdentifiers_InPersistedView_RoundTripAfterReopen()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE \"order detail\" (\"select\" INTEGER PRIMARY KEY, \"line value\" TEXT)",
            ct);
        await _database.ExecuteAsync(
            "INSERT INTO \"order detail\" (\"select\", \"line value\") VALUES (1, 'persisted')",
            ct);
        await _database.ExecuteAsync(
            "CREATE VIEW \"view name\" AS SELECT \"line value\" AS \"display value\" FROM \"order detail\"",
            ct);

        await _database.DisposeAsync();
        _database = await Database.OpenAsync(_databasePath, ct: ct);

        await using var result = await _database.ExecuteAsync(
            "SELECT \"display value\" FROM \"view name\"",
            ct);
        var rows = await result.ToListAsync(ct);

        Assert.Single(rows);
        Assert.Equal("persisted", rows[0][0].AsText);
    }
}
