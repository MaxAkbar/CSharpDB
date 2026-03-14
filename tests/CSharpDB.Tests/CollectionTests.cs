using System.Reflection;
using System.Text.Json;
using CSharpDB.Primitives;
using CSharpDB.Engine;
using CSharpDB.Storage.BTrees;
using CSharpDB.Storage.Serialization;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Tests;

public class CollectionTests : IAsyncLifetime
{
    private static readonly JsonSerializerOptions s_collectionJsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false,
    };

    private readonly string _dbPath;
    private Database _db = null!;

    public CollectionTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_collection_test_{Guid.NewGuid():N}.db");
    }

    public async ValueTask InitializeAsync()
    {
        _db = await Database.OpenAsync(_dbPath);
    }

    public async ValueTask DisposeAsync()
    {
        await _db.DisposeAsync();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        if (File.Exists(_dbPath + ".wal")) File.Delete(_dbPath + ".wal");
    }

    // Test models
    private record User(string Name, int Age, string Email);
    private record Product(string Sku, string Title, decimal Price);

    // ===== Tier 1: Basic CRUD =====

    [Fact]
    public async Task PutAndGet_BasicRoundTrip()
    {
        var users = await _db.GetCollectionAsync<User>("users", TestContext.Current.CancellationToken);
        var alice = new User("Alice", 30, "alice@example.com");

        await users.PutAsync("user:1", alice, TestContext.Current.CancellationToken);
        var result = await users.GetAsync("user:1", TestContext.Current.CancellationToken);

        Assert.NotNull(result);
        Assert.Equal("Alice", result!.Name);
        Assert.Equal(30, result.Age);
        Assert.Equal("alice@example.com", result.Email);
    }

    [Fact]
    public async Task Put_UpdateExistingDocument()
    {
        var users = await _db.GetCollectionAsync<User>("users", TestContext.Current.CancellationToken);

        await users.PutAsync("user:1", new User("Alice", 30, "alice@old.com"), TestContext.Current.CancellationToken);
        await users.PutAsync("user:1", new User("Alice", 31, "alice@new.com"), TestContext.Current.CancellationToken);

        var result = await users.GetAsync("user:1", TestContext.Current.CancellationToken);
        Assert.NotNull(result);
        Assert.Equal(31, result!.Age);
        Assert.Equal("alice@new.com", result.Email);
    }

    [Fact]
    public async Task Get_NonexistentKey_ReturnsNull()
    {
        var users = await _db.GetCollectionAsync<User>("users", TestContext.Current.CancellationToken);
        var result = await users.GetAsync("nonexistent", TestContext.Current.CancellationToken);
        Assert.Null(result);
    }

    [Fact]
    public async Task Delete_ExistingKey_ReturnsTrue()
    {
        var users = await _db.GetCollectionAsync<User>("users", TestContext.Current.CancellationToken);
        await users.PutAsync("user:1", new User("Alice", 30, "alice@example.com"), TestContext.Current.CancellationToken);

        bool deleted = await users.DeleteAsync("user:1", TestContext.Current.CancellationToken);
        Assert.True(deleted);

        var result = await users.GetAsync("user:1", TestContext.Current.CancellationToken);
        Assert.Null(result);
    }

    [Fact]
    public async Task Delete_NonexistentKey_ReturnsFalse()
    {
        var users = await _db.GetCollectionAsync<User>("users", TestContext.Current.CancellationToken);
        bool deleted = await users.DeleteAsync("nonexistent", TestContext.Current.CancellationToken);
        Assert.False(deleted);
    }

    [Fact]
    public async Task Count_EmptyCollection_ReturnsZero()
    {
        var users = await _db.GetCollectionAsync<User>("users", TestContext.Current.CancellationToken);
        long count = await users.CountAsync(TestContext.Current.CancellationToken);
        Assert.Equal(0, count);
    }

    [Fact]
    public async Task Count_AfterInserts()
    {
        var users = await _db.GetCollectionAsync<User>("users", TestContext.Current.CancellationToken);
        await users.PutAsync("user:1", new User("Alice", 30, "a@b.com"), TestContext.Current.CancellationToken);
        await users.PutAsync("user:2", new User("Bob", 25, "b@b.com"), TestContext.Current.CancellationToken);
        await users.PutAsync("user:3", new User("Charlie", 35, "c@b.com"), TestContext.Current.CancellationToken);

        long count = await users.CountAsync(TestContext.Current.CancellationToken);
        Assert.Equal(3, count);
    }

    [Fact]
    public async Task Count_AfterDelete()
    {
        var users = await _db.GetCollectionAsync<User>("users", TestContext.Current.CancellationToken);
        await users.PutAsync("user:1", new User("Alice", 30, "a@b.com"), TestContext.Current.CancellationToken);
        await users.PutAsync("user:2", new User("Bob", 25, "b@b.com"), TestContext.Current.CancellationToken);
        await users.DeleteAsync("user:1", TestContext.Current.CancellationToken);

        long count = await users.CountAsync(TestContext.Current.CancellationToken);
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task Scan_AllDocuments()
    {
        var users = await _db.GetCollectionAsync<User>("users", TestContext.Current.CancellationToken);
        await users.PutAsync("user:1", new User("Alice", 30, "a@b.com"), TestContext.Current.CancellationToken);
        await users.PutAsync("user:2", new User("Bob", 25, "b@b.com"), TestContext.Current.CancellationToken);
        await users.PutAsync("user:3", new User("Charlie", 35, "c@b.com"), TestContext.Current.CancellationToken);

        var docs = new List<KeyValuePair<string, User>>();
        await foreach (var kvp in users.ScanAsync(TestContext.Current.CancellationToken))
            docs.Add(kvp);

        Assert.Equal(3, docs.Count);
        var names = docs.Select(d => d.Value.Name).OrderBy(n => n).ToList();
        Assert.Equal(new[] { "Alice", "Bob", "Charlie" }, names);
    }

    [Fact]
    public async Task Scan_EmptyCollection()
    {
        var users = await _db.GetCollectionAsync<User>("users", TestContext.Current.CancellationToken);
        var docs = new List<KeyValuePair<string, User>>();
        await foreach (var kvp in users.ScanAsync(TestContext.Current.CancellationToken))
            docs.Add(kvp);

        Assert.Empty(docs);
    }

    [Fact]
    public async Task Put_ModerateDocument()
    {
        var users = await _db.GetCollectionAsync<User>("users", TestContext.Current.CancellationToken);
        // Keep within B+tree page size (4096 bytes). JSON overhead + RecordEncoder ~ 100 bytes.
        string longEmail = new string('x', 500) + "@example.com";
        var bigUser = new User("ModerateDoc", 99, longEmail);

        await users.PutAsync("big:1", bigUser, TestContext.Current.CancellationToken);
        var result = await users.GetAsync("big:1", TestContext.Current.CancellationToken);

        Assert.NotNull(result);
        Assert.Equal(longEmail, result!.Email);
    }

    [Fact]
    public async Task MultipleCollections_Independent()
    {
        var users = await _db.GetCollectionAsync<User>("users", TestContext.Current.CancellationToken);
        var products = await _db.GetCollectionAsync<Product>("products", TestContext.Current.CancellationToken);

        await users.PutAsync("u:1", new User("Alice", 30, "a@b.com"), TestContext.Current.CancellationToken);
        await products.PutAsync("p:1", new Product("SKU001", "Widget", 19.99m), TestContext.Current.CancellationToken);

        Assert.Equal(1, await users.CountAsync(TestContext.Current.CancellationToken));
        Assert.Equal(1, await products.CountAsync(TestContext.Current.CancellationToken));

        var user = await users.GetAsync("u:1", TestContext.Current.CancellationToken);
        var product = await products.GetAsync("p:1", TestContext.Current.CancellationToken);
        Assert.Equal("Alice", user!.Name);
        Assert.Equal("Widget", product!.Title);
    }

    // ===== Tier 2: Filtered queries =====

    [Fact]
    public async Task Find_WithPredicate()
    {
        var users = await _db.GetCollectionAsync<User>("users", TestContext.Current.CancellationToken);
        await users.PutAsync("u:1", new User("Alice", 30, "a@b.com"), TestContext.Current.CancellationToken);
        await users.PutAsync("u:2", new User("Bob", 25, "b@b.com"), TestContext.Current.CancellationToken);
        await users.PutAsync("u:3", new User("Charlie", 35, "c@b.com"), TestContext.Current.CancellationToken);
        await users.PutAsync("u:4", new User("Diana", 22, "d@b.com"), TestContext.Current.CancellationToken);

        var results = new List<KeyValuePair<string, User>>();
        await foreach (var kvp in users.FindAsync(u => u.Age > 28, TestContext.Current.CancellationToken))
            results.Add(kvp);

        Assert.Equal(2, results.Count);
        var names = results.Select(r => r.Value.Name).OrderBy(n => n).ToList();
        Assert.Equal(new[] { "Alice", "Charlie" }, names);
    }

    [Fact]
    public async Task Find_NoMatches_ReturnsEmpty()
    {
        var users = await _db.GetCollectionAsync<User>("users", TestContext.Current.CancellationToken);
        await users.PutAsync("u:1", new User("Alice", 30, "a@b.com"), TestContext.Current.CancellationToken);

        var results = new List<KeyValuePair<string, User>>();
        await foreach (var kvp in users.FindAsync(u => u.Age > 100, TestContext.Current.CancellationToken))
            results.Add(kvp);

        Assert.Empty(results);
    }

    [Fact]
    public async Task Find_AllMatch()
    {
        var users = await _db.GetCollectionAsync<User>("users", TestContext.Current.CancellationToken);
        await users.PutAsync("u:1", new User("Alice", 30, "a@b.com"), TestContext.Current.CancellationToken);
        await users.PutAsync("u:2", new User("Bob", 25, "b@b.com"), TestContext.Current.CancellationToken);

        var results = new List<KeyValuePair<string, User>>();
        await foreach (var kvp in users.FindAsync(u => u.Age > 0, TestContext.Current.CancellationToken))
            results.Add(kvp);

        Assert.Equal(2, results.Count);
    }

    // ===== Transaction tests =====

    [Fact]
    public async Task ExplicitTransaction_BatchOperations()
    {
        var users = await _db.GetCollectionAsync<User>("users", TestContext.Current.CancellationToken);

        await _db.BeginTransactionAsync(TestContext.Current.CancellationToken);
        await users.PutAsync("u:1", new User("Alice", 30, "a@b.com"), TestContext.Current.CancellationToken);
        await users.PutAsync("u:2", new User("Bob", 25, "b@b.com"), TestContext.Current.CancellationToken);
        await users.PutAsync("u:3", new User("Charlie", 35, "c@b.com"), TestContext.Current.CancellationToken);
        await _db.CommitAsync(TestContext.Current.CancellationToken);

        Assert.Equal(3, await users.CountAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ExplicitTransaction_Rollback()
    {
        var users = await _db.GetCollectionAsync<User>("users", TestContext.Current.CancellationToken);

        // Put one doc that persists
        await users.PutAsync("u:0", new User("Eve", 28, "e@b.com"), TestContext.Current.CancellationToken);

        await _db.BeginTransactionAsync(TestContext.Current.CancellationToken);
        await users.PutAsync("u:1", new User("Alice", 30, "a@b.com"), TestContext.Current.CancellationToken);
        await users.PutAsync("u:2", new User("Bob", 25, "b@b.com"), TestContext.Current.CancellationToken);
        await _db.RollbackAsync(TestContext.Current.CancellationToken);

        // Only the pre-transaction doc should exist
        Assert.Equal(1, await users.CountAsync(TestContext.Current.CancellationToken));
        Assert.NotNull(await users.GetAsync("u:0", TestContext.Current.CancellationToken));
        Assert.Null(await users.GetAsync("u:1", TestContext.Current.CancellationToken));
        Assert.Null(await users.GetAsync("u:2", TestContext.Current.CancellationToken));
    }

    // ===== Cross-API isolation =====

    [Fact]
    public async Task Collection_DoesNotConflict_WithSqlTables()
    {
        // Create a SQL table named "users"
        await _db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", TestContext.Current.CancellationToken);
        await _db.ExecuteAsync("INSERT INTO users VALUES (1, 'SQL Alice')", TestContext.Current.CancellationToken);

        // Create a collection also named "users" (stored as "_col_users" internally)
        var users = await _db.GetCollectionAsync<User>("users", TestContext.Current.CancellationToken);
        await users.PutAsync("u:1", new User("Collection Alice", 30, "a@b.com"), TestContext.Current.CancellationToken);

        // SQL table still has its row
        await using var result = await _db.ExecuteAsync("SELECT name FROM users WHERE id = 1", TestContext.Current.CancellationToken);
        var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(rows);
        Assert.Equal("SQL Alice", rows[0][0].AsText);

        // Collection still has its document
        var doc = await users.GetAsync("u:1", TestContext.Current.CancellationToken);
        Assert.Equal("Collection Alice", doc!.Name);
    }

    [Fact]
    public async Task GetCollectionNames()
    {
        await _db.GetCollectionAsync<User>("users", TestContext.Current.CancellationToken);
        await _db.GetCollectionAsync<Product>("products", TestContext.Current.CancellationToken);
        await _db.GetCollectionAsync<User>("logs", TestContext.Current.CancellationToken);

        var names = _db.GetCollectionNames().OrderBy(n => n).ToList();
        Assert.Equal(new[] { "logs", "products", "users" }, names);
    }

    // ===== Persistence across reopen =====

    [Fact]
    public async Task Data_Persists_AcrossReopen()
    {
        // Insert data
        var users = await _db.GetCollectionAsync<User>("users", TestContext.Current.CancellationToken);
        await users.PutAsync("u:1", new User("Alice", 30, "a@b.com"), TestContext.Current.CancellationToken);

        // Close and reopen
        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, TestContext.Current.CancellationToken);

        // Data should still be there
        var users2 = await _db.GetCollectionAsync<User>("users", TestContext.Current.CancellationToken);
        var result = await users2.GetAsync("u:1", TestContext.Current.CancellationToken);
        Assert.NotNull(result);
        Assert.Equal("Alice", result!.Name);
    }

    [Fact]
    public async Task Put_UpdateExistingDocument_PersistsAcrossReopen_WhenRootDoesNotChange()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users", ct);

        await users.PutAsync("u:1", new User("Alice", 30, "old@example.com"), ct);
        uint rootBefore = GetCollectionRootPageId(users);

        await users.PutAsync("u:1", new User("Alice", 31, "new@example.com"), ct);

        Assert.Equal(rootBefore, GetCollectionRootPageId(users));

        await ReopenDatabaseAsync(ct);

        var reopened = await _db.GetCollectionAsync<User>("users", ct);
        var result = await reopened.GetAsync("u:1", ct);
        Assert.NotNull(result);
        Assert.Equal(31, result!.Age);
        Assert.Equal("new@example.com", result.Email);
    }

    [Fact]
    public async Task Delete_ExistingDocument_PersistsAcrossReopen_WhenRootDoesNotChange()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users", ct);

        await users.PutAsync("u:1", new User("Alice", 30, "alice@example.com"), ct);
        uint rootBefore = GetCollectionRootPageId(users);

        Assert.True(await users.DeleteAsync("u:1", ct));
        Assert.Equal(rootBefore, GetCollectionRootPageId(users));

        await ReopenDatabaseAsync(ct);

        var reopened = await _db.GetCollectionAsync<User>("users", ct);
        Assert.Equal(0, await reopened.CountAsync(ct));
        Assert.Null(await reopened.GetAsync("u:1", ct));
    }

    [Fact]
    public async Task AutoCommitInsert_PersistsAcrossReopen_AfterRootSplit()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users", ct);
        uint initialRoot = GetCollectionRootPageId(users);

        int inserted = 0;
        while (GetCollectionRootPageId(users) == initialRoot && inserted < 2048)
        {
            await users.PutAsync(
                $"u:{inserted}",
                new User($"User{inserted}", 20 + (inserted % 40), $"u{inserted}@example.com"),
                ct);
            inserted++;
        }

        Assert.NotEqual(initialRoot, GetCollectionRootPageId(users));

        await ReopenDatabaseAsync(ct);

        var reopened = await _db.GetCollectionAsync<User>("users", ct);
        Assert.Equal(inserted, await reopened.CountAsync(ct));
        Assert.Equal("User0", (await reopened.GetAsync("u:0", ct))!.Name);
        Assert.Equal($"User{inserted - 1}", (await reopened.GetAsync($"u:{inserted - 1}", ct))!.Name);
    }

    [Fact]
    public async Task AutoCommitDelete_PersistsAcrossReopen_AfterRootCollapse()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users", ct);
        uint initialRoot = GetCollectionRootPageId(users);
        var keys = new List<string>();

        while (GetCollectionRootPageId(users) == initialRoot && keys.Count < 2048)
        {
            string key = $"u:{keys.Count}";
            keys.Add(key);
            await users.PutAsync(
                key,
                new User($"User{keys.Count - 1}", 20 + (keys.Count % 40), $"{key}@example.com"),
                ct);
        }

        uint expandedRoot = GetCollectionRootPageId(users);
        Assert.NotEqual(initialRoot, expandedRoot);

        for (int i = 0; i < keys.Count - 1; i++)
            Assert.True(await users.DeleteAsync(keys[i], ct));

        Assert.NotEqual(expandedRoot, GetCollectionRootPageId(users));

        await ReopenDatabaseAsync(ct);

        var reopened = await _db.GetCollectionAsync<User>("users", ct);
        Assert.Equal(1, await reopened.CountAsync(ct));
        var remaining = await reopened.GetAsync(keys[^1], ct);
        Assert.NotNull(remaining);
        Assert.Equal($"User{keys.Count - 1}", remaining!.Name);
    }

    [Fact]
    public async Task LegacyBackingRows_AreReadableThroughCollection()
    {
        var ct = TestContext.Current.CancellationToken;

        var users = await _db.GetCollectionAsync<User>("users", ct);
        await InsertLegacyBackingRowAsync(users, "u:legacy", new User("Legacy Alice", 41, "legacy@example.com"), ct);
        var legacy = await users.GetAsync("u:legacy", ct);

        Assert.NotNull(legacy);
        Assert.Equal("Legacy Alice", legacy!.Name);
        Assert.Equal(41, legacy.Age);
        Assert.Equal("legacy@example.com", legacy.Email);

        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, ct);

        var reopened = await _db.GetCollectionAsync<User>("users", ct);
        var reopenedLegacy = await reopened.GetAsync("u:legacy", ct);

        Assert.NotNull(reopenedLegacy);
        Assert.Equal("Legacy Alice", reopenedLegacy!.Name);
    }

    [Fact]
    public async Task PutAndDelete_CanOperateOnLegacyBackingRows()
    {
        var ct = TestContext.Current.CancellationToken;

        var users = await _db.GetCollectionAsync<User>("users", ct);
        await InsertLegacyBackingRowAsync(users, "u:update", new User("Old Alice", 40, "old@example.com"), ct);
        await InsertLegacyBackingRowAsync(users, "u:delete", new User("Delete Me", 22, "delete@example.com"), ct);

        await users.PutAsync("u:update", new User("Updated Alice", 42, "updated@example.com"), ct);
        bool deleted = await users.DeleteAsync("u:delete", ct);

        Assert.True(deleted);
        Assert.Equal(1, await users.CountAsync(ct));

        var updated = await users.GetAsync("u:update", ct);
        Assert.NotNull(updated);
        Assert.Equal("Updated Alice", updated!.Name);
        Assert.Equal(42, updated.Age);
        Assert.Equal("updated@example.com", updated.Email);
        Assert.Null(await users.GetAsync("u:delete", ct));

        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, ct);

        var reopened = await _db.GetCollectionAsync<User>("users", ct);
        Assert.Equal(1, await reopened.CountAsync(ct));
        Assert.Equal("Updated Alice", (await reopened.GetAsync("u:update", ct))!.Name);
        Assert.Null(await reopened.GetAsync("u:delete", ct));
    }

    [Fact]
    public async Task InternalBackingTableRows_WrittenByCollection_AreQueryableViaSql()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users", ct);

        await users.PutAsync("u:1", new User("Sql Visible Alice", 30, "sqlalice@example.com"), ct);
        await users.PutAsync("u:2", new User("Sql Visible Bob", 31, "sqlbob@example.com"), ct);

        await using var result = await _db.ExecuteAsync(
            "SELECT _key, _doc FROM _col_users WHERE _key = 'u:1'",
            ct);
        var rows = await result.ToListAsync(ct);

        Assert.Single(rows);
        Assert.Equal("u:1", rows[0][0].AsText);
        Assert.Equal(
            "{\"name\":\"Sql Visible Alice\",\"age\":30,\"email\":\"sqlalice@example.com\"}",
            rows[0][1].AsText);
    }

    [Fact]
    public async Task InternalBackingTableRows_WrittenByCollection_RemainQueryableViaSqlAfterReopen()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users", ct);

        await users.PutAsync("u:1", new User("Sql Reopen Alice", 35, "reopen@example.com"), ct);

        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, ct);

        await using var result = await _db.ExecuteAsync(
            "SELECT _doc FROM _col_users WHERE _key = 'u:1'",
            ct);
        var rows = await result.ToListAsync(ct);

        Assert.Single(rows);
        Assert.Equal(
            "{\"name\":\"Sql Reopen Alice\",\"age\":35,\"email\":\"reopen@example.com\"}",
            rows[0][0].AsText);
    }

    [Fact]
    public async Task CustomRecordSerializer_WithCollectionMarkerPrefix_RemainsCompatible()
    {
        var ct = TestContext.Current.CancellationToken;
        var options = new DatabaseOptions
        {
            StorageEngineOptions = new StorageEngineOptions
            {
                SerializerProvider = new PrefixCollisionSerializerProvider(),
            },
        };

        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, options, ct);

        await _db.ExecuteAsync("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)", ct);
        await _db.ExecuteAsync("INSERT INTO people VALUES (1, 'Serializer Alice')", ct);

        var users = await _db.GetCollectionAsync<User>("users", ct);
        var expected = new User("Custom Serializer Alice", 33, "custom@example.com");
        await users.PutAsync("u:1", expected, ct);

        byte[] rawPayload = await GetBackingPayloadAsync(users, "u:1", ct);
        Assert.Equal((byte)0xC1, rawPayload[0]);

        await using (var sqlTableResult = await _db.ExecuteAsync("SELECT name FROM people WHERE id = 1", ct))
        {
            var sqlTableRows = await sqlTableResult.ToListAsync(ct);
            Assert.Single(sqlTableRows);
            Assert.Equal("Serializer Alice", sqlTableRows[0][0].AsText);
        }

        var stored = await users.GetAsync("u:1", ct);
        Assert.NotNull(stored);
        Assert.Equal(expected, stored);

        string expectedJson = JsonSerializer.Serialize(expected, s_collectionJsonOptions);
        await using (var collectionSqlResult = await _db.ExecuteAsync(
            "SELECT _key, _doc FROM _col_users WHERE _key = 'u:1'",
            ct))
        {
            var collectionSqlRows = await collectionSqlResult.ToListAsync(ct);
            Assert.Single(collectionSqlRows);
            Assert.Equal("u:1", collectionSqlRows[0][0].AsText);
            Assert.Equal(expectedJson, collectionSqlRows[0][1].AsText);
        }

        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, options, ct);

        await using (var reopenedSqlTableResult = await _db.ExecuteAsync("SELECT name FROM people WHERE id = 1", ct))
        {
            var reopenedSqlTableRows = await reopenedSqlTableResult.ToListAsync(ct);
            Assert.Single(reopenedSqlTableRows);
            Assert.Equal("Serializer Alice", reopenedSqlTableRows[0][0].AsText);
        }

        var reopenedUsers = await _db.GetCollectionAsync<User>("users", ct);
        Assert.Equal(expected, await reopenedUsers.GetAsync("u:1", ct));

        await using var reopenedCollectionSqlResult = await _db.ExecuteAsync(
            "SELECT _doc FROM _col_users WHERE _key = 'u:1'",
            ct);
        var reopenedCollectionSqlRows = await reopenedCollectionSqlResult.ToListAsync(ct);
        Assert.Single(reopenedCollectionSqlRows);
        Assert.Equal(expectedJson, reopenedCollectionSqlRows[0][0].AsText);
    }

    [Fact]
    public async Task ExplicitTransaction_PersistsCollectionRootsOnCommit()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users", ct);

        await _db.BeginTransactionAsync(ct);
        for (int i = 0; i < 400; i++)
        {
            await users.PutAsync(
                $"u:{i}",
                new User($"User{i}", 20 + (i % 40), $"u{i}@example.com"),
                ct);
        }
        await _db.CommitAsync(ct);

        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, ct);

        var reopened = await _db.GetCollectionAsync<User>("users", ct);
        Assert.Equal(400, await reopened.CountAsync(ct));
        Assert.Equal("User0", (await reopened.GetAsync("u:0", ct))!.Name);
        Assert.Equal("User399", (await reopened.GetAsync("u:399", ct))!.Name);
    }

    private async Task InsertLegacyBackingRowAsync<TDocument>(
        Collection<TDocument> collection,
        string key,
        TDocument document,
        CancellationToken ct)
    {
        var tree = GetCollectionTree(collection);

        string json = JsonSerializer.Serialize(document, s_collectionJsonOptions);
        byte[] payload = new DefaultRecordSerializer().Encode(
            new[] { DbValue.FromText(key), DbValue.FromText(json) });

        await _db.BeginTransactionAsync(ct);
        try
        {
            await tree.InsertAsync(Collection<TDocument>.HashDocumentKey(key), payload, ct);
            await _db.CommitAsync(ct);
        }
        catch
        {
            await _db.RollbackAsync(ct);
            throw;
        }
    }

    private static async Task<byte[]> GetBackingPayloadAsync<TDocument>(
        Collection<TDocument> collection,
        string key,
        CancellationToken ct)
    {
        var tree = GetCollectionTree(collection);

        var payload = await tree.FindMemoryAsync(Collection<TDocument>.HashDocumentKey(key), ct)
            ?? throw new InvalidOperationException("Collection payload not found.");

        return payload.ToArray();
    }

    private async Task ReopenDatabaseAsync(CancellationToken ct)
    {
        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, ct);
    }

    private static uint GetCollectionRootPageId<TDocument>(Collection<TDocument> collection)
        => GetCollectionTree(collection).RootPageId;

    private static BTree GetCollectionTree<TDocument>(Collection<TDocument> collection)
    {
        var treeField = typeof(Collection<TDocument>).GetField("_tree", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("Collection tree field not found.");
        return (BTree?)treeField.GetValue(collection)
            ?? throw new InvalidOperationException("Collection tree was not initialized.");
    }

    private sealed class PrefixCollisionSerializerProvider : ISerializerProvider
    {
        public IRecordSerializer RecordSerializer { get; } = new PrefixCollisionRecordSerializer();

        public ISchemaSerializer SchemaSerializer { get; } = new DefaultSchemaSerializer();
    }

    private sealed class PrefixCollisionRecordSerializer : IRecordSerializer
    {
        private const byte Prefix = 0xC1;
        private readonly IRecordSerializer _inner = new DefaultRecordSerializer();

        public byte[] Encode(ReadOnlySpan<DbValue> values)
        {
            byte[] encoded = _inner.Encode(values);
            byte[] prefixed = new byte[encoded.Length + 1];
            prefixed[0] = Prefix;
            encoded.CopyTo(prefixed.AsSpan(1));
            return prefixed;
        }

        public DbValue[] Decode(ReadOnlySpan<byte> buffer) => _inner.Decode(StripPrefix(buffer));

        public int DecodeInto(ReadOnlySpan<byte> buffer, Span<DbValue> destination)
            => _inner.DecodeInto(StripPrefix(buffer), destination);

        public void DecodeSelectedInto(ReadOnlySpan<byte> buffer, Span<DbValue> destination, ReadOnlySpan<int> selectedColumnIndices)
            => _inner.DecodeSelectedInto(StripPrefix(buffer), destination, selectedColumnIndices);

        public DbValue[] DecodeUpTo(ReadOnlySpan<byte> buffer, int maxColumnIndexInclusive)
            => _inner.DecodeUpTo(StripPrefix(buffer), maxColumnIndexInclusive);

        public DbValue DecodeColumn(ReadOnlySpan<byte> buffer, int columnIndex)
            => _inner.DecodeColumn(StripPrefix(buffer), columnIndex);

        public bool TryColumnTextEquals(ReadOnlySpan<byte> buffer, int columnIndex, ReadOnlySpan<byte> expectedUtf8, out bool equals)
            => _inner.TryColumnTextEquals(StripPrefix(buffer), columnIndex, expectedUtf8, out equals);

        public bool IsColumnNull(ReadOnlySpan<byte> buffer, int columnIndex)
            => _inner.IsColumnNull(StripPrefix(buffer), columnIndex);

        public bool TryDecodeNumericColumn(
            ReadOnlySpan<byte> buffer,
            int columnIndex,
            out long intValue,
            out double realValue,
            out bool isReal)
            => _inner.TryDecodeNumericColumn(StripPrefix(buffer), columnIndex, out intValue, out realValue, out isReal);

        private static ReadOnlySpan<byte> StripPrefix(ReadOnlySpan<byte> buffer)
        {
            if (buffer.IsEmpty || buffer[0] != Prefix)
                throw new InvalidOperationException("Expected prefixed row payload.");

            return buffer[1..];
        }
    }
}
