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
        var users = await _db.GetCollectionAsync<User>("users");
        var alice = new User("Alice", 30, "alice@example.com");

        await users.PutAsync("user:1", alice);
        var result = await users.GetAsync("user:1");

        Assert.NotNull(result);
        Assert.Equal("Alice", result!.Name);
        Assert.Equal(30, result.Age);
        Assert.Equal("alice@example.com", result.Email);
    }

    [Fact]
    public async Task Put_UpdateExistingDocument()
    {
        var users = await _db.GetCollectionAsync<User>("users");

        await users.PutAsync("user:1", new User("Alice", 30, "alice@old.com"));
        await users.PutAsync("user:1", new User("Alice", 31, "alice@new.com"));

        var result = await users.GetAsync("user:1");
        Assert.NotNull(result);
        Assert.Equal(31, result!.Age);
        Assert.Equal("alice@new.com", result.Email);
    }

    [Fact]
    public async Task Get_NonexistentKey_ReturnsNull()
    {
        var users = await _db.GetCollectionAsync<User>("users");
        var result = await users.GetAsync("nonexistent");
        Assert.Null(result);
    }

    [Fact]
    public async Task Delete_ExistingKey_ReturnsTrue()
    {
        var users = await _db.GetCollectionAsync<User>("users");
        await users.PutAsync("user:1", new User("Alice", 30, "alice@example.com"));

        bool deleted = await users.DeleteAsync("user:1");
        Assert.True(deleted);

        var result = await users.GetAsync("user:1");
        Assert.Null(result);
    }

    [Fact]
    public async Task Delete_NonexistentKey_ReturnsFalse()
    {
        var users = await _db.GetCollectionAsync<User>("users");
        bool deleted = await users.DeleteAsync("nonexistent");
        Assert.False(deleted);
    }

    [Fact]
    public async Task Count_EmptyCollection_ReturnsZero()
    {
        var users = await _db.GetCollectionAsync<User>("users");
        long count = await users.CountAsync();
        Assert.Equal(0, count);
    }

    [Fact]
    public async Task Count_AfterInserts()
    {
        var users = await _db.GetCollectionAsync<User>("users");
        await users.PutAsync("user:1", new User("Alice", 30, "a@b.com"));
        await users.PutAsync("user:2", new User("Bob", 25, "b@b.com"));
        await users.PutAsync("user:3", new User("Charlie", 35, "c@b.com"));

        long count = await users.CountAsync();
        Assert.Equal(3, count);
    }

    [Fact]
    public async Task Count_AfterDelete()
    {
        var users = await _db.GetCollectionAsync<User>("users");
        await users.PutAsync("user:1", new User("Alice", 30, "a@b.com"));
        await users.PutAsync("user:2", new User("Bob", 25, "b@b.com"));
        await users.DeleteAsync("user:1");

        long count = await users.CountAsync();
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task Scan_AllDocuments()
    {
        var users = await _db.GetCollectionAsync<User>("users");
        await users.PutAsync("user:1", new User("Alice", 30, "a@b.com"));
        await users.PutAsync("user:2", new User("Bob", 25, "b@b.com"));
        await users.PutAsync("user:3", new User("Charlie", 35, "c@b.com"));

        var docs = new List<KeyValuePair<string, User>>();
        await foreach (var kvp in users.ScanAsync())
            docs.Add(kvp);

        Assert.Equal(3, docs.Count);
        var names = docs.Select(d => d.Value.Name).OrderBy(n => n).ToList();
        Assert.Equal(new[] { "Alice", "Bob", "Charlie" }, names);
    }

    [Fact]
    public async Task Scan_EmptyCollection()
    {
        var users = await _db.GetCollectionAsync<User>("users");
        var docs = new List<KeyValuePair<string, User>>();
        await foreach (var kvp in users.ScanAsync())
            docs.Add(kvp);

        Assert.Empty(docs);
    }

    [Fact]
    public async Task Put_ModerateDocument()
    {
        var users = await _db.GetCollectionAsync<User>("users");
        // Keep within B+tree page size (4096 bytes). JSON overhead + RecordEncoder ~ 100 bytes.
        string longEmail = new string('x', 500) + "@example.com";
        var bigUser = new User("ModerateDoc", 99, longEmail);

        await users.PutAsync("big:1", bigUser);
        var result = await users.GetAsync("big:1");

        Assert.NotNull(result);
        Assert.Equal(longEmail, result!.Email);
    }

    [Fact]
    public async Task MultipleCollections_Independent()
    {
        var users = await _db.GetCollectionAsync<User>("users");
        var products = await _db.GetCollectionAsync<Product>("products");

        await users.PutAsync("u:1", new User("Alice", 30, "a@b.com"));
        await products.PutAsync("p:1", new Product("SKU001", "Widget", 19.99m));

        Assert.Equal(1, await users.CountAsync());
        Assert.Equal(1, await products.CountAsync());

        var user = await users.GetAsync("u:1");
        var product = await products.GetAsync("p:1");
        Assert.Equal("Alice", user!.Name);
        Assert.Equal("Widget", product!.Title);
    }

    // ===== Tier 2: Filtered queries =====

    [Fact]
    public async Task Find_WithPredicate()
    {
        var users = await _db.GetCollectionAsync<User>("users");
        await users.PutAsync("u:1", new User("Alice", 30, "a@b.com"));
        await users.PutAsync("u:2", new User("Bob", 25, "b@b.com"));
        await users.PutAsync("u:3", new User("Charlie", 35, "c@b.com"));
        await users.PutAsync("u:4", new User("Diana", 22, "d@b.com"));

        var results = new List<KeyValuePair<string, User>>();
        await foreach (var kvp in users.FindAsync(u => u.Age > 28))
            results.Add(kvp);

        Assert.Equal(2, results.Count);
        var names = results.Select(r => r.Value.Name).OrderBy(n => n).ToList();
        Assert.Equal(new[] { "Alice", "Charlie" }, names);
    }

    [Fact]
    public async Task Find_NoMatches_ReturnsEmpty()
    {
        var users = await _db.GetCollectionAsync<User>("users");
        await users.PutAsync("u:1", new User("Alice", 30, "a@b.com"));

        var results = new List<KeyValuePair<string, User>>();
        await foreach (var kvp in users.FindAsync(u => u.Age > 100))
            results.Add(kvp);

        Assert.Empty(results);
    }

    [Fact]
    public async Task Find_AllMatch()
    {
        var users = await _db.GetCollectionAsync<User>("users");
        await users.PutAsync("u:1", new User("Alice", 30, "a@b.com"));
        await users.PutAsync("u:2", new User("Bob", 25, "b@b.com"));

        var results = new List<KeyValuePair<string, User>>();
        await foreach (var kvp in users.FindAsync(u => u.Age > 0))
            results.Add(kvp);

        Assert.Equal(2, results.Count);
    }

    // ===== Transaction tests =====

    [Fact]
    public async Task ExplicitTransaction_BatchOperations()
    {
        var users = await _db.GetCollectionAsync<User>("users");

        await _db.BeginTransactionAsync();
        await users.PutAsync("u:1", new User("Alice", 30, "a@b.com"));
        await users.PutAsync("u:2", new User("Bob", 25, "b@b.com"));
        await users.PutAsync("u:3", new User("Charlie", 35, "c@b.com"));
        await _db.CommitAsync();

        Assert.Equal(3, await users.CountAsync());
    }

    [Fact]
    public async Task ExplicitTransaction_Rollback()
    {
        var users = await _db.GetCollectionAsync<User>("users");

        // Put one doc that persists
        await users.PutAsync("u:0", new User("Eve", 28, "e@b.com"));

        await _db.BeginTransactionAsync();
        await users.PutAsync("u:1", new User("Alice", 30, "a@b.com"));
        await users.PutAsync("u:2", new User("Bob", 25, "b@b.com"));
        await _db.RollbackAsync();

        // Only the pre-transaction doc should exist
        Assert.Equal(1, await users.CountAsync());
        Assert.NotNull(await users.GetAsync("u:0"));
        Assert.Null(await users.GetAsync("u:1"));
        Assert.Null(await users.GetAsync("u:2"));
    }

    // ===== Cross-API isolation =====

    [Fact]
    public async Task Collection_DoesNotConflict_WithSqlTables()
    {
        // Create a SQL table named "users"
        await _db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
        await _db.ExecuteAsync("INSERT INTO users VALUES (1, 'SQL Alice')");

        // Create a collection also named "users" (stored as "_col_users" internally)
        var users = await _db.GetCollectionAsync<User>("users");
        await users.PutAsync("u:1", new User("Collection Alice", 30, "a@b.com"));

        // SQL table still has its row
        await using var result = await _db.ExecuteAsync("SELECT name FROM users WHERE id = 1");
        var rows = await result.ToListAsync();
        Assert.Single(rows);
        Assert.Equal("SQL Alice", rows[0][0].AsText);

        // Collection still has its document
        var doc = await users.GetAsync("u:1");
        Assert.Equal("Collection Alice", doc!.Name);
    }

    [Fact]
    public async Task GetCollectionNames()
    {
        await _db.GetCollectionAsync<User>("users");
        await _db.GetCollectionAsync<Product>("products");
        await _db.GetCollectionAsync<User>("logs");

        var names = _db.GetCollectionNames().OrderBy(n => n).ToList();
        Assert.Equal(new[] { "logs", "products", "users" }, names);
    }

    // ===== Persistence across reopen =====

    [Fact]
    public async Task Data_Persists_AcrossReopen()
    {
        // Insert data
        var users = await _db.GetCollectionAsync<User>("users");
        await users.PutAsync("u:1", new User("Alice", 30, "a@b.com"));

        // Close and reopen
        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath);

        // Data should still be there
        var users2 = await _db.GetCollectionAsync<User>("users");
        var result = await users2.GetAsync("u:1");
        Assert.NotNull(result);
        Assert.Equal("Alice", result!.Name);
    }
}
