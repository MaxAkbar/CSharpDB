using CSharpDB.Engine;

namespace CSharpDB.Tests;

public sealed class InMemoryDatabaseTests : IDisposable
{
    private readonly List<string> _paths = new();
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private record UserDoc(string Name, int Age);

    [Fact]
    public async Task OpenInMemory_SaveToFileAndReopen_PersistsSqlTables()
    {
        string filePath = NewTempDbPath();

        await using (var db = await Database.OpenInMemoryAsync(Ct))
        {
            await db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", Ct);
            await db.ExecuteAsync("INSERT INTO users VALUES (1, 'Alice')", Ct);
            await db.ExecuteAsync("INSERT INTO users VALUES (2, 'Bob')", Ct);

            await db.SaveToFileAsync(filePath, Ct);
        }

        await using var reopened = await Database.OpenAsync(filePath, Ct);
        await using var result = await reopened.ExecuteAsync("SELECT name FROM users ORDER BY id", Ct);
        var rows = await result.ToListAsync(Ct);

        Assert.Equal(2, rows.Count);
        Assert.Equal("Alice", rows[0][0].AsText);
        Assert.Equal("Bob", rows[1][0].AsText);
    }

    [Fact]
    public async Task LoadIntoMemory_FromDatabaseAndWal_PreservesCommittedSqlState()
    {
        string filePath = NewTempDbPath();
        bool walExists;

        await using (var diskDb = await Database.OpenAsync(filePath, Ct))
        {
            await diskDb.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)", Ct);
            await diskDb.ExecuteAsync("INSERT INTO t VALUES (1, 'from-wal')", Ct);
            walExists = File.Exists(filePath + ".wal");
        }

        Assert.True(walExists);

        await using var memoryDb = await Database.LoadIntoMemoryAsync(filePath, Ct);
        await using var result = await memoryDb.ExecuteAsync("SELECT name FROM t WHERE id = 1", Ct);
        var rows = await result.ToListAsync(Ct);

        Assert.Single(rows);
        Assert.Equal("from-wal", rows[0][0].AsText);
    }

    [Fact]
    public async Task Collections_RoundTripThroughSaveAndLoad()
    {
        string filePath = NewTempDbPath();

        await using (var db = await Database.OpenInMemoryAsync(Ct))
        {
            var users = await db.GetCollectionAsync<UserDoc>("users", Ct);
            await users.PutAsync("user:1", new UserDoc("Alice", 30), Ct);
            await users.PutAsync("user:2", new UserDoc("Bob", 28), Ct);

            await db.SaveToFileAsync(filePath, Ct);
        }

        await using var loaded = await Database.LoadIntoMemoryAsync(filePath, Ct);
        var reopenedUsers = await loaded.GetCollectionAsync<UserDoc>("users", Ct);

        Assert.Equal(2, await reopenedUsers.CountAsync(Ct));
        Assert.Equal("Alice", (await reopenedUsers.GetAsync("user:1", Ct))!.Name);
        Assert.Equal("Bob", (await reopenedUsers.GetAsync("user:2", Ct))!.Name);
    }

    [Fact]
    public async Task RelationalTablesAndCollections_CoexistAfterRoundTrip()
    {
        string filePath = NewTempDbPath();

        await using (var db = await Database.OpenInMemoryAsync(Ct))
        {
            await db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", Ct);
            await db.ExecuteAsync("INSERT INTO users VALUES (1, 'SQL Alice')", Ct);

            var collection = await db.GetCollectionAsync<UserDoc>("users", Ct);
            await collection.PutAsync("user:1", new UserDoc("Collection Alice", 30), Ct);

            await db.SaveToFileAsync(filePath, Ct);
        }

        await using var loaded = await Database.LoadIntoMemoryAsync(filePath, Ct);
        await using var sqlResult = await loaded.ExecuteAsync("SELECT name FROM users WHERE id = 1", Ct);
        var sqlRows = await sqlResult.ToListAsync(Ct);
        var reopenedCollection = await loaded.GetCollectionAsync<UserDoc>("users", Ct);

        Assert.Single(sqlRows);
        Assert.Equal("SQL Alice", sqlRows[0][0].AsText);
        Assert.Equal("Collection Alice", (await reopenedCollection.GetAsync("user:1", Ct))!.Name);
    }

    [Fact]
    public async Task SaveToFileAsync_WithActiveTransaction_Throws()
    {
        string filePath = NewTempDbPath();

        await using var db = await Database.OpenInMemoryAsync(Ct);
        await db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY)", Ct);
        await db.BeginTransactionAsync(Ct);

        await Assert.ThrowsAsync<InvalidOperationException>(() => db.SaveToFileAsync(filePath, Ct).AsTask());
        await db.RollbackAsync(Ct);
    }

    [Fact]
    public async Task SaveToFileAsync_WithActiveReaderSession_Throws()
    {
        string filePath = NewTempDbPath();

        await using var db = await Database.OpenInMemoryAsync(Ct);
        await db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)", Ct);
        await db.ExecuteAsync("INSERT INTO t VALUES (1, 'Alice')", Ct);

        using var reader = db.CreateReaderSession();
        await Assert.ThrowsAsync<InvalidOperationException>(() => db.SaveToFileAsync(filePath, Ct).AsTask());
    }

    public void Dispose()
    {
        foreach (string path in _paths)
        {
            TryDelete(path);
            TryDelete(path + ".wal");
        }
    }

    private string NewTempDbPath()
    {
        string path = Path.Combine(Path.GetTempPath(), $"csharpdb_memory_test_{Guid.NewGuid():N}.db");
        _paths.Add(path);
        return path;
    }

    private static void TryDelete(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);
        }
        catch
        {
            // Best-effort temp file cleanup.
        }
    }
}
