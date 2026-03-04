using CSharpDB.Engine;

namespace CSharpDB.Tests;

public class CollectionTests : IAsyncLifetime
{
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
}
