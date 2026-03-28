using System.Text.Json;
using System.Text;
using System.Reflection;
using System.Globalization;
using CSharpDB.Primitives;
using CSharpDB.Engine;
using CSharpDB.Storage.BTrees;
using CSharpDB.Storage.Catalog;
using CSharpDB.Storage.Indexing;
using CSharpDB.Storage.Serialization;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Tests;

public sealed class CollectionIndexTests : IAsyncLifetime
{
    private readonly string _dbPath;
    private Database _db = null!;

    public CollectionIndexTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_collection_index_test_{Guid.NewGuid():N}.db");
    }

    public async ValueTask InitializeAsync()
    {
        _db = await Database.OpenAsync(_dbPath);
    }

    public async ValueTask DisposeAsync()
    {
        await _db.DisposeAsync();
        if (File.Exists(_dbPath))
            File.Delete(_dbPath);
        if (File.Exists(_dbPath + ".wal"))
            File.Delete(_dbPath + ".wal");
    }

    private record User(string Name, int Age, string Email);
    private record Address(string City, int ZipCode);
    private record UserWithAddress(string Name, Address Address);
    private record UserWithTags(string Name, string[] Tags, List<int> Scores);
    private record OrderLine(string Sku, int Quantity);
    private record UserWithOrders(string Name, OrderLine[] Orders);
    private record TemporalUser(string Name, Guid SessionId, DateOnly EventDate, TimeOnly StartTime);

    [Fact]
    public async Task EnsureIndex_BackfillsExistingDocuments_ForIntegerField()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users", ct);

        await users.PutAsync("u:1", new User("Alice", 30, "alice@example.com"), ct);
        await users.PutAsync("u:2", new User("Bob", 25, "bob@example.com"), ct);
        await users.PutAsync("u:3", new User("Charlie", 30, "charlie@example.com"), ct);

        await users.EnsureIndexAsync(x => x.Age, ct);

        var matches = await CollectAsync(users.FindByIndexAsync(x => x.Age, 30, ct), ct);

        Assert.Equal(2, matches.Count);
        Assert.Equal(["u:1", "u:3"], matches.Select(x => x.Key).OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task EnsureIndex_BackfillsExistingDocuments_ForStringField()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users", ct);

        await users.PutAsync("u:1", new User("Alice", 30, "alpha@example.com"), ct);
        await users.PutAsync("u:2", new User("Bob", 31, "beta@example.com"), ct);
        await users.PutAsync("u:3", new User("Cara", 32, "alpha@example.com"), ct);

        await users.EnsureIndexAsync(x => x.Email, ct);

        var matches = await CollectAsync(users.FindByIndexAsync(x => x.Email, "alpha@example.com", ct), ct);

        Assert.Equal(2, matches.Count);
        Assert.Equal(["u:1", "u:3"], matches.Select(x => x.Key).OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task EnsureIndex_BackfillsExistingDocuments_ForStringField_WithNoCaseCollation()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users_email_nocase", ct);

        await users.PutAsync("u:1", new User("Alice", 30, "Alpha@example.com"), ct);
        await users.PutAsync("u:2", new User("Bob", 31, "alpha@example.com"), ct);
        await users.PutAsync("u:3", new User("Cara", 32, "beta@example.com"), ct);

        await users.EnsureIndexAsync(x => x.Email, "NOCASE", ct);

        var matches = await CollectAsync(users.FindByIndexAsync(x => x.Email, "ALPHA@example.com", ct), ct);

        Assert.Equal(["u:1", "u:2"], matches.Select(x => x.Key).OrderBy(x => x).ToArray());

        var binding = GetBinding(users, nameof(User.Email));
        Assert.Equal("NOCASE", binding.Collation);

        var catalog = GetCollectionCatalog(users);
        var index = Assert.Single(catalog.GetIndexesForTable(GetCollectionCatalogTableName(users)));
        Assert.Single(index.ColumnCollations);
        Assert.Equal("NOCASE", index.ColumnCollations[0]);
    }

    [Fact]
    public async Task EnsureIndex_BackfillsExistingDocuments_ForStringField_WithNoCaseAiCollation()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users_name_nocase_ai", ct);

        await users.PutAsync("u:1", new User("José", 30, "jose@example.com"), ct);
        await users.PutAsync("u:2", new User("JOSE", 31, "jose2@example.com"), ct);
        await users.PutAsync("u:3", new User("Joëlle", 32, "joelle@example.com"), ct);

        await users.EnsureIndexAsync(x => x.Name, "NOCASE_AI", ct);

        var matches = await CollectAsync(users.FindByIndexAsync(x => x.Name, "jose", ct), ct);

        Assert.Equal(["u:1", "u:2"], matches.Select(x => x.Key).OrderBy(x => x).ToArray());

        var binding = GetBinding(users, nameof(User.Name));
        Assert.Equal("NOCASE_AI", binding.Collation);
    }

    [Fact]
    public async Task EnsureIndex_BackfillsExistingDocuments_ForStringField_WithIcuCollation()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users_name_icu", ct);
        const string locale = "en-US";
        string collation = $"ICU:{locale}";

        await users.PutAsync("u:1", new User("résumé", 30, "resume1@example.com"), ct);
        await users.PutAsync("u:2", new User("résumé", 31, "resume2@example.com"), ct);
        await users.PutAsync("u:3", new User("resume", 32, "resume3@example.com"), ct);

        await users.EnsureIndexAsync(x => x.Name, collation, ct);

        var matches = await CollectAsync(users.FindByIndexAsync(x => x.Name, "résumé", ct), ct);
        var compareInfo = CultureInfo.GetCultureInfo(locale).CompareInfo;
        string[] allKeys = ["u:1", "u:2", "u:3"];
        string[] expected = allKeys
            .Where(key =>
            {
                string name = key switch
                {
                    "u:1" => "résumé",
                    "u:2" => "résumé",
                    _ => "resume",
                };

                return compareInfo.Compare(name, "résumé", CompareOptions.None) == 0;
            })
            .ToArray();

        Assert.Equal(expected.OrderBy(static key => key).ToArray(), matches.Select(x => x.Key).OrderBy(x => x).ToArray());

        var binding = GetBinding(users, nameof(User.Name));
        Assert.Equal($"ICU:{CultureInfo.GetCultureInfo(locale).Name}", binding.Collation);
    }

    [Fact]
    public async Task EnsureIndex_BackfillsExistingDocuments_WithManyDuplicateIntegerKeys()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users", ct);

        const int userCount = 20_000;
        const int batchSize = 500;
        for (int start = 0; start < userCount; start += batchSize)
        {
            await _db.BeginTransactionAsync(ct);
            try
            {
                int end = Math.Min(start + batchSize, userCount);
                for (int i = start; i < end; i++)
                {
                    await users.PutAsync(
                        $"u:{i}",
                        new User($"User{i}", i % 256, $"user{i}@example.com"),
                        ct);
                }

                await _db.CommitAsync(ct);
            }
            catch
            {
                await _db.RollbackAsync(ct);
                throw;
            }
        }

        await users.EnsureIndexAsync(x => x.Age, ct);

        var matches = await CollectAsync(users.FindByIndexAsync(x => x.Age, 5, ct), ct);

        int expectedCount = ((userCount - 1 - 5) / 256) + 1;
        Assert.Equal(expectedCount, matches.Count);
        Assert.All(matches, match => Assert.Equal(5, match.Value.Age));
        Assert.Equal(expectedCount, matches.Select(match => match.Key).Distinct(StringComparer.Ordinal).Count());
    }

    [Fact]
    public async Task FindByIndex_FallsBackToScan_WhenIndexDoesNotExist()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users", ct);

        await users.PutAsync("u:1", new User("Alice", 30, "alice@example.com"), ct);
        await users.PutAsync("u:2", new User("Bob", 31, "bob@example.com"), ct);

        var matches = await CollectAsync(users.FindByIndexAsync(x => x.Age, 31, ct), ct);

        Assert.Single(matches);
        Assert.Equal("u:2", matches[0].Key);
    }

    [Fact]
    public async Task EnsureIndex_BackfillsExistingDocuments_ForNestedPathString()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<UserWithAddress>("users_nested", ct);

        await users.PutAsync("u:1", new UserWithAddress("Alice", new Address("Seattle", 98101)), ct);
        await users.PutAsync("u:2", new UserWithAddress("Bob", new Address("Portland", 97201)), ct);
        await users.PutAsync("u:3", new UserWithAddress("Cara", new Address("Seattle", 98109)), ct);

        await users.EnsureIndexAsync("$.address.city", ct);

        var matches = await CollectAsync(users.FindByIndexAsync("$.address.city", "Seattle", ct), ct);

        Assert.Equal(2, matches.Count);
        Assert.Equal(["u:1", "u:3"], matches.Select(x => x.Key).OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task EnsureIndex_BackfillsExistingDocuments_ForGuidField()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<TemporalUser>("users_guid", ct);
        Guid shared = Guid.Parse("2f8c4c7e-0d1b-4d26-9b70-61c5d9342ef0");

        await users.PutAsync("u:1", new TemporalUser("Alice", shared, new DateOnly(2026, 3, 10), new TimeOnly(9, 0)), ct);
        await users.PutAsync("u:2", new TemporalUser("Bob", Guid.Parse("37fa4200-acaf-4a56-81d2-b4a85de6c9d1"), new DateOnly(2026, 3, 11), new TimeOnly(10, 0)), ct);
        await users.PutAsync("u:3", new TemporalUser("Cara", shared, new DateOnly(2026, 3, 12), new TimeOnly(11, 0)), ct);

        await users.EnsureIndexAsync(x => x.SessionId, ct);

        var matches = await CollectAsync(users.FindByIndexAsync(x => x.SessionId, shared, ct), ct);

        Assert.Equal(["u:1", "u:3"], matches.Select(x => x.Key).OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task FindByIndex_PathString_ReusesSelectorIndex()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<UserWithAddress>("users_nested", ct);

        await users.PutAsync("u:1", new UserWithAddress("Alice", new Address("Seattle", 98101)), ct);
        await users.PutAsync("u:2", new UserWithAddress("Bob", new Address("Portland", 97201)), ct);

        await users.EnsureIndexAsync(x => x.Address.City, ct);
        await users.EnsureIndexAsync("$.address.city", ct);

        var matches = await CollectAsync(users.FindByIndexAsync("$.address.city", "Seattle", ct), ct);

        Assert.Single(matches);
        Assert.Equal("u:1", matches[0].Key);

        var catalog = GetCollectionCatalog(users);
        Assert.Single(catalog.GetIndexesForTable(GetCollectionCatalogTableName(users)));
    }

    [Fact]
    public async Task FindByPath_PathString_ReusesSelectorIndex()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<UserWithAddress>("users_nested_path_query", ct);

        await users.PutAsync("u:1", new UserWithAddress("Alice", new Address("Seattle", 98101)), ct);
        await users.PutAsync("u:2", new UserWithAddress("Bob", new Address("Portland", 97201)), ct);

        await users.EnsureIndexAsync(x => x.Address.City, ct);
        await users.EnsureIndexAsync("$.address.city", ct);

        var matches = await CollectAsync(users.FindByPathAsync("$.address.city", "Seattle", ct), ct);

        Assert.Single(matches);
        Assert.Equal("u:1", matches[0].Key);

        var catalog = GetCollectionCatalog(users);
        Assert.Single(catalog.GetIndexesForTable(GetCollectionCatalogTableName(users)));
    }

    [Fact]
    public async Task FindByIndex_PathString_FallsBackToScan_WhenIndexDoesNotExist()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<UserWithAddress>("users_nested", ct);

        await users.PutAsync("u:1", new UserWithAddress("Alice", new Address("Seattle", 98101)), ct);
        await users.PutAsync("u:2", new UserWithAddress("Bob", new Address("Portland", 97201)), ct);

        var matches = await CollectAsync(users.FindByIndexAsync("$.address.city", "Portland", ct), ct);

        Assert.Single(matches);
        Assert.Equal("u:2", matches[0].Key);
    }

    [Fact]
    public async Task FindByPath_PathString_FallsBackToDirectPayloadScan_WhenIndexDoesNotExist()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<UserWithAddress>("users_nested_path_scan", ct);

        await users.PutAsync("u:1", new UserWithAddress("Alice", new Address("Seattle", 98101)), ct);
        await users.PutAsync("u:2", new UserWithAddress("Bob", new Address("Portland", 97201)), ct);

        var matches = await CollectAsync(users.FindByPathAsync("$.address.city", "Portland", ct), ct);

        Assert.Single(matches);
        Assert.Equal("u:2", matches[0].Key);
    }

    [Fact]
    public async Task EnsureIndex_BackfillsExistingDocuments_ForNestedArrayObjectPath()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<UserWithOrders>("users_nested_array", ct);

        await users.PutAsync("u:1", new UserWithOrders("Alice", [new OrderLine("sku-alpha", 1), new OrderLine("sku-beta", 2)]), ct);
        await users.PutAsync("u:2", new UserWithOrders("Bob", [new OrderLine("sku-gamma", 1)]), ct);
        await users.PutAsync("u:3", new UserWithOrders("Cara", [new OrderLine("sku-beta", 5)]), ct);

        await users.EnsureIndexAsync("$.orders[].sku", ct);

        var matches = await CollectAsync(users.FindByIndexAsync("$.orders[].sku", "sku-beta", ct), ct);

        Assert.Equal(["u:1", "u:3"], matches.Select(x => x.Key).OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task FindByPath_NestedArrayObjectPath_UsesIndexedContainsSemantics()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<UserWithOrders>("users_nested_array_path_query", ct);

        await users.PutAsync("u:1", new UserWithOrders("Alice", [new OrderLine("sku-alpha", 1), new OrderLine("sku-beta", 2)]), ct);
        await users.PutAsync("u:2", new UserWithOrders("Bob", [new OrderLine("sku-gamma", 1)]), ct);
        await users.PutAsync("u:3", new UserWithOrders("Cara", [new OrderLine("sku-beta", 5)]), ct);
        await users.EnsureIndexAsync("$.orders[].sku", ct);

        var matches = await CollectAsync(users.FindByPathAsync("$.orders[].sku", "sku-beta", ct), ct);

        Assert.Equal(["u:1", "u:3"], matches.Select(x => x.Key).OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task FindByPath_NestedArrayObjectPath_FallsBackToDirectPayloadScan_WhenIndexDoesNotExist()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<UserWithOrders>("users_nested_array_path_scan", ct);

        await users.PutAsync("u:1", new UserWithOrders("Alice", [new OrderLine("sku-alpha", 1), new OrderLine("sku-beta", 2)]), ct);
        await users.PutAsync("u:2", new UserWithOrders("Bob", [new OrderLine("sku-gamma", 1)]), ct);

        var matches = await CollectAsync(users.FindByPathAsync("Orders[].Sku", "sku-beta", ct), ct);

        Assert.Single(matches);
        Assert.Equal("u:1", matches[0].Key);
    }

    [Fact]
    public async Task FindByPathRange_IntegerPath_UsesOrderedIndex()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users_age_range", ct);

        await users.PutAsync("u:1", new User("Alice", 20, "alice@example.com"), ct);
        await users.PutAsync("u:2", new User("Bob", 30, "bob@example.com"), ct);
        await users.PutAsync("u:3", new User("Cara", 40, "cara@example.com"), ct);
        await users.PutAsync("u:4", new User("Dana", 50, "dana@example.com"), ct);
        await users.EnsureIndexAsync(x => x.Age, ct);

        var matches = await CollectAsync(users.FindByPathRangeAsync("Age", 25, 45, ct: ct), ct);

        Assert.Equal(["u:2", "u:3"], matches.Select(x => x.Key).OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task FindByPathRange_IntegerPath_FallsBackToDirectPayloadScan_WhenIndexDoesNotExist()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users_age_range_scan", ct);

        await users.PutAsync("u:1", new User("Alice", 20, "alice@example.com"), ct);
        await users.PutAsync("u:2", new User("Bob", 30, "bob@example.com"), ct);
        await users.PutAsync("u:3", new User("Cara", 40, "cara@example.com"), ct);

        var matches = await CollectAsync(users.FindByPathRangeAsync("Age", 25, 35, ct: ct), ct);

        Assert.Single(matches);
        Assert.Equal("u:2", matches[0].Key);
    }

    [Fact]
    public async Task FindByPathRange_TextPath_UsesOrderedTextIndex()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users_email_range", ct);

        await users.PutAsync("u:1", new User("Alice", 20, "alpha@example.com"), ct);
        await users.PutAsync("u:2", new User("Bob", 30, "beta@example.com"), ct);
        await users.PutAsync("u:3", new User("Cara", 40, "charlie@example.com"), ct);
        await users.PutAsync("u:4", new User("Dana", 50, "delta@example.com"), ct);
        await users.EnsureIndexAsync(x => x.Email, ct);

        var binding = GetBinding(users, nameof(User.Email));
        long betaIndexKey = OrderedTextIndexKeyCodec.ComputeKey("beta@example.com");
        byte[] bucketPayload = await binding.IndexStore.FindAsync(betaIndexKey, ct)
            ?? throw new InvalidOperationException("Expected ordered text index payload.");
        Assert.True(OrderedTextIndexPayloadCodec.IsEncoded(bucketPayload));

        var matches = await CollectAsync(
            users.FindByPathRangeAsync("Email", "beta@example.com", "charlie@example.com", ct: ct),
            ct);

        Assert.Equal(["u:2", "u:3"], matches.Select(x => x.Key).OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task FindByPathRange_TextPath_UsesNoCaseOrderedTextIndex()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users_name_range_nocase", ct);

        await users.PutAsync("u:1", new User("Alpha", 20, "alpha@example.com"), ct);
        await users.PutAsync("u:2", new User("BRAVO", 30, "bravo@example.com"), ct);
        await users.PutAsync("u:3", new User("charlie", 40, "charlie@example.com"), ct);
        await users.PutAsync("u:4", new User("delta", 50, "delta@example.com"), ct);
        await users.EnsureIndexAsync(x => x.Name, "NOCASE", ct);

        var binding = GetBinding(users, nameof(User.Name));
        Assert.True(binding.TryBuildKeyFromValue("BRAVO", out long bravoIndexKey));
        byte[] bucketPayload = await binding.IndexStore.FindAsync(bravoIndexKey, ct)
            ?? throw new InvalidOperationException("Expected ordered text index payload.");
        Assert.True(OrderedTextIndexPayloadCodec.IsEncoded(bucketPayload));

        var matches = await CollectAsync(
            users.FindByPathRangeAsync("Name", "bravo", "charlie", ct: ct),
            ct);

        Assert.Equal(["u:2", "u:3"], matches.Select(x => x.Key).OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task FindByPathRange_TextPath_UsesIcuOrderedTextIndex()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users_name_range_icu", ct);
        const string locale = "sv-SE";
        string collation = $"ICU:{locale}";
        (string Key, string Name)[] rows =
        [
            ("u:1", "a"),
            ("u:2", "z"),
            ("u:3", "å"),
            ("u:4", "ä"),
            ("u:5", "ö"),
        ];

        foreach (var row in rows)
            await users.PutAsync(row.Key, new User(row.Name, 20, $"{row.Key}@example.com"), ct);

        await users.EnsureIndexAsync(x => x.Name, collation, ct);

        var binding = GetBinding(users, nameof(User.Name));
        Assert.True(binding.TryBuildKeyFromValue("z", out long zIndexKey));
        byte[] bucketPayload = await binding.IndexStore.FindAsync(zIndexKey, ct)
            ?? throw new InvalidOperationException("Expected ICU ordered text index payload.");
        Assert.True(OrderedTextIndexPayloadCodec.IsEncoded(bucketPayload));

        var matches = await CollectAsync(users.FindByPathRangeAsync("Name", "z", "ä", ct: ct), ct);

        var compareInfo = CultureInfo.GetCultureInfo(locale).CompareInfo;
        string[] expected = rows
            .Where(row =>
                compareInfo.Compare(row.Name, "z", CompareOptions.None) >= 0 &&
                compareInfo.Compare(row.Name, "ä", CompareOptions.None) <= 0)
            .Select(static row => row.Key)
            .OrderBy(static key => key)
            .ToArray();

        Assert.Equal(expected, matches.Select(x => x.Key).OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task FindByPathRange_DateOnlyPath_UsesOrderedTextIndex()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<TemporalUser>("users_event_date_range", ct);

        await users.PutAsync("u:1", new TemporalUser("Alice", Guid.Parse("2f8c4c7e-0d1b-4d26-9b70-61c5d9342ef0"), new DateOnly(2026, 3, 10), new TimeOnly(9, 0)), ct);
        await users.PutAsync("u:2", new TemporalUser("Bob", Guid.Parse("37fa4200-acaf-4a56-81d2-b4a85de6c9d1"), new DateOnly(2026, 3, 11), new TimeOnly(10, 0)), ct);
        await users.PutAsync("u:3", new TemporalUser("Cara", Guid.Parse("3f826528-a8f8-4bb0-a58e-c9515ac1021f"), new DateOnly(2026, 3, 12), new TimeOnly(11, 0)), ct);
        await users.PutAsync("u:4", new TemporalUser("Dana", Guid.Parse("5be0d5f8-22b1-43ed-b913-50255c4e8aa1"), new DateOnly(2026, 3, 13), new TimeOnly(12, 0)), ct);
        await users.EnsureIndexAsync(x => x.EventDate, ct);

        var binding = GetBinding(users, nameof(TemporalUser.EventDate));
        long lowerKey = OrderedTextIndexKeyCodec.ComputeKey(new DateOnly(2026, 3, 11).ToString("O"));
        byte[] bucketPayload = await binding.IndexStore.FindAsync(lowerKey, ct)
            ?? throw new InvalidOperationException("Expected ordered text index payload.");
        Assert.True(OrderedTextIndexPayloadCodec.IsEncoded(bucketPayload));

        var matches = await CollectAsync(
            users.FindByPathRangeAsync("EventDate", new DateOnly(2026, 3, 11), new DateOnly(2026, 3, 12), ct: ct),
            ct);

        Assert.Equal(["u:2", "u:3"], matches.Select(x => x.Key).OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task FindByPath_TextPath_HandlesOrderedPrefixBucketCollisions()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users_email_prefix_collisions", ct);

        await users.PutAsync("u:1", new User("Alice", 20, "prefix-alpha@example.com"), ct);
        await users.PutAsync("u:2", new User("Bob", 30, "prefix-beta@example.com"), ct);
        await users.PutAsync("u:3", new User("Cara", 40, "prefix-gamma@example.com"), ct);
        await users.EnsureIndexAsync(x => x.Email, ct);

        var exact = await CollectAsync(users.FindByPathAsync("Email", "prefix-beta@example.com", ct), ct);
        Assert.Single(exact);
        Assert.Equal("u:2", exact[0].Key);

        var range = await CollectAsync(
            users.FindByPathRangeAsync("Email", "prefix-beta@example.com", "prefix-gamma@example.com", ct: ct),
            ct);

        Assert.Equal(["u:2", "u:3"], range.Select(x => x.Key).OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task EnsureIndex_PathString_RejectsArraySegments()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<UserWithAddress>("users_nested", ct);

        var ex = await Assert.ThrowsAsync<NotSupportedException>(
            async () => await users.EnsureIndexAsync("$.address[0].city", ct));

        Assert.Contains("array", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task EnsureIndex_BackfillsExistingDocuments_ForStringArrayPath()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<UserWithTags>("users_tags", ct);

        await users.PutAsync("u:1", new UserWithTags("Alice", ["red", "green"], [10, 20]), ct);
        await users.PutAsync("u:2", new UserWithTags("Bob", ["blue"], [20, 30]), ct);
        await users.PutAsync("u:3", new UserWithTags("Cara", ["green", "yellow"], [40]), ct);

        await users.EnsureIndexAsync("$.tags[]", ct);

        var matches = await CollectAsync(users.FindByIndexAsync("$.tags[]", "green", ct), ct);

        Assert.Equal(2, matches.Count);
        Assert.Equal(["u:1", "u:3"], matches.Select(x => x.Key).OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task EnsureIndex_BackfillsExistingDocuments_ForIntegerArrayPath()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<UserWithTags>("users_scores", ct);

        await users.PutAsync("u:1", new UserWithTags("Alice", ["red"], [10, 20]), ct);
        await users.PutAsync("u:2", new UserWithTags("Bob", ["blue"], [20, 30]), ct);
        await users.PutAsync("u:3", new UserWithTags("Cara", ["green"], [40]), ct);

        await users.EnsureIndexAsync("Scores[]", ct);

        var matches = await CollectAsync(users.FindByIndexAsync("Scores[]", 20, ct), ct);

        Assert.Equal(2, matches.Count);
        Assert.Equal(["u:1", "u:2"], matches.Select(x => x.Key).OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task FindByIndex_ArrayPath_FallsBackToScan_WhenIndexDoesNotExist()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<UserWithTags>("users_tags_scan", ct);

        await users.PutAsync("u:1", new UserWithTags("Alice", ["red", "green"], [10, 20]), ct);
        await users.PutAsync("u:2", new UserWithTags("Bob", ["blue"], [30]), ct);

        var matches = await CollectAsync(users.FindByIndexAsync("$.tags[]", "green", ct), ct);

        Assert.Single(matches);
        Assert.Equal("u:1", matches[0].Key);
    }

    [Fact]
    public async Task FindByPath_ArrayPath_UsesContainsSemantics()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<UserWithTags>("users_tags_path_query", ct);

        await users.PutAsync("u:1", new UserWithTags("Alice", ["red", "green"], [10, 20]), ct);
        await users.PutAsync("u:2", new UserWithTags("Bob", ["blue"], [30]), ct);
        await users.PutAsync("u:3", new UserWithTags("Cara", ["green", "yellow"], [40]), ct);
        await users.EnsureIndexAsync("$.tags[]", ct);

        var matches = await CollectAsync(users.FindByPathAsync("$.tags[]", "green", ct), ct);

        Assert.Equal(2, matches.Count);
        Assert.Equal(["u:1", "u:3"], matches.Select(x => x.Key).OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task FindByPath_ArrayPath_UsesNoCaseContainsSemantics()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<UserWithTags>("users_tags_path_query_nocase", ct);

        await users.PutAsync("u:1", new UserWithTags("Alice", ["Red", "Green"], [10, 20]), ct);
        await users.PutAsync("u:2", new UserWithTags("Bob", ["blue"], [30]), ct);
        await users.PutAsync("u:3", new UserWithTags("Cara", ["green", "yellow"], [40]), ct);
        await users.EnsureIndexAsync("$.tags[]", "NOCASE", ct);

        var matches = await CollectAsync(users.FindByPathAsync("$.tags[]", "GREEN", ct), ct);

        Assert.Equal(["u:1", "u:3"], matches.Select(x => x.Key).OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task FindByPath_ArrayPath_FallsBackToDirectPayloadScan_WhenIndexDoesNotExist()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<UserWithTags>("users_tags_path_scan", ct);

        await users.PutAsync("u:1", new UserWithTags("Alice", ["red", "green"], [10, 20]), ct);
        await users.PutAsync("u:2", new UserWithTags("Bob", ["blue"], [30]), ct);

        var matches = await CollectAsync(users.FindByPathAsync("Tags[]", "green", ct), ct);

        Assert.Single(matches);
        Assert.Equal("u:1", matches[0].Key);
    }

    [Fact]
    public async Task FindByPathRange_ArrayPath_RejectsArrayPaths()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<UserWithTags>("users_tags_path_range", ct);

        await users.PutAsync("u:1", new UserWithTags("Alice", ["red", "green"], [10, 20]), ct);

        var ex = await Assert.ThrowsAsync<NotSupportedException>(
            async () => await CollectAsync(users.FindByPathRangeAsync("Tags[]", "green", "yellow", ct: ct), ct));

        Assert.Contains("scalar", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Put_UpdateExistingDocument_UpdatesArrayCollectionIndex()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<UserWithTags>("users_tags_update", ct);

        await users.PutAsync("u:1", new UserWithTags("Alice", ["red", "green"], [10, 20]), ct);
        await users.EnsureIndexAsync("Tags[]", ct);

        await users.PutAsync("u:1", new UserWithTags("Alice", ["blue"], [10, 20]), ct);

        Assert.Empty(await CollectAsync(users.FindByIndexAsync("Tags[]", "green", ct), ct));

        var updated = await CollectAsync(users.FindByIndexAsync("Tags[]", "blue", ct), ct);
        Assert.Single(updated);
        Assert.Equal("u:1", updated[0].Key);
    }

    [Fact]
    public async Task ArrayCollectionIndex_DeduplicatesDuplicateElementsPerDocument()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<UserWithTags>("users_tags_dupes", ct);

        await users.PutAsync("u:1", new UserWithTags("Alice", ["green", "green", "green"], [10, 10]), ct);
        await users.EnsureIndexAsync("Tags[]", ct);

        var matches = await CollectAsync(users.FindByIndexAsync("Tags[]", "green", ct), ct);
        var binding = GetBinding(users, "Tags[]");
        var matcher = CollectionIndexBinding<UserWithTags>.CreateTransient("Tags[]");
        Assert.True(matcher.TryBuildKeyFromValue("green", out long indexKey));
        byte[] payload = await binding.IndexStore.FindAsync(indexKey, ct)
            ?? throw new InvalidOperationException("Expected array collection index payload.");
        Assert.True(OrderedTextIndexPayloadCodec.TryGetMatchingRowIdPayloadSlice(payload, "green", out ReadOnlyMemory<byte> rowIdPayload));

        Assert.Single(matches);
        Assert.Equal("u:1", matches[0].Key);
        Assert.Equal(1, RowIdPayloadCodec.GetCount(rowIdPayload.Span));
    }

    [Fact]
    public async Task Put_UpdateExistingDocument_UpdatesCollectionIndex()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users", ct);

        await users.PutAsync("u:1", new User("Alice", 30, "alice@example.com"), ct);
        await users.EnsureIndexAsync(x => x.Age, ct);

        await users.PutAsync("u:1", new User("Alice", 31, "alice@example.com"), ct);

        Assert.Empty(await CollectAsync(users.FindByIndexAsync(x => x.Age, 30, ct), ct));

        var updated = await CollectAsync(users.FindByIndexAsync(x => x.Age, 31, ct), ct);
        Assert.Single(updated);
        Assert.Equal("u:1", updated[0].Key);
    }

    [Fact]
    public async Task Delete_RemovesDocumentFromCollectionIndex()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users", ct);

        await users.PutAsync("u:1", new User("Alice", 30, "alice@example.com"), ct);
        await users.EnsureIndexAsync(x => x.Age, ct);

        Assert.True(await users.DeleteAsync("u:1", ct));
        Assert.Empty(await CollectAsync(users.FindByIndexAsync(x => x.Age, 30, ct), ct));
    }

    [Fact]
    public async Task CollectionIndexes_SurviveReopen()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users", ct);

        await users.PutAsync("u:1", new User("Alice", 30, "alice@example.com"), ct);
        await users.PutAsync("u:2", new User("Bob", 30, "bob@example.com"), ct);
        await users.EnsureIndexAsync(x => x.Age, ct);

        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, ct);

        var reopened = await _db.GetCollectionAsync<User>("users", ct);
        var matches = await CollectAsync(reopened.FindByIndexAsync(x => x.Age, 30, ct), ct);

        Assert.Equal(2, matches.Count);
        Assert.Equal(["u:1", "u:2"], matches.Select(x => x.Key).OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task CollectionIndexes_WithNoCaseCollation_SurviveReopen()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users_nocase_reopen", ct);

        await users.PutAsync("u:1", new User("Alice", 30, "Alpha@example.com"), ct);
        await users.PutAsync("u:2", new User("Bob", 30, "alpha@example.com"), ct);
        await users.EnsureIndexAsync(x => x.Email, "NOCASE", ct);

        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, ct);

        var reopened = await _db.GetCollectionAsync<User>("users_nocase_reopen", ct);
        var matches = await CollectAsync(reopened.FindByPathAsync("Email", "ALPHA@example.com", ct), ct);

        Assert.Equal(["u:1", "u:2"], matches.Select(x => x.Key).OrderBy(x => x).ToArray());

        var catalog = GetCollectionCatalog(reopened);
        var index = Assert.Single(catalog.GetIndexesForTable(GetCollectionCatalogTableName(reopened)));
        Assert.Single(index.ColumnCollations);
        Assert.Equal("NOCASE", index.ColumnCollations[0]);
    }

    [Fact]
    public async Task Put_UpdateExistingDocument_PersistsIndexedWritesAcrossReopen()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users", ct);

        await users.PutAsync("u:1", new User("Alice", 30, "alice@example.com"), ct);
        await users.EnsureIndexAsync(x => x.Age, ct);
        await users.PutAsync("u:1", new User("Alice", 31, "alice@example.com"), ct);

        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, ct);

        var reopened = await _db.GetCollectionAsync<User>("users", ct);
        Assert.Empty(await CollectAsync(reopened.FindByIndexAsync(x => x.Age, 30, ct), ct));

        var updated = await CollectAsync(reopened.FindByIndexAsync(x => x.Age, 31, ct), ct);
        Assert.Single(updated);
        Assert.Equal("u:1", updated[0].Key);
    }

    [Fact]
    public async Task Delete_PersistsIndexedWritesAcrossReopen()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users", ct);

        await users.PutAsync("u:1", new User("Alice", 30, "alice@example.com"), ct);
        await users.PutAsync("u:2", new User("Bob", 30, "bob@example.com"), ct);
        await users.EnsureIndexAsync(x => x.Age, ct);
        Assert.True(await users.DeleteAsync("u:1", ct));

        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, ct);

        var reopened = await _db.GetCollectionAsync<User>("users", ct);
        var matches = await CollectAsync(reopened.FindByIndexAsync(x => x.Age, 30, ct), ct);
        Assert.Single(matches);
        Assert.Equal("u:2", matches[0].Key);
    }

    [Fact]
    public async Task ExplicitTransaction_Rollback_RevertsCollectionIndexWrites()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users", ct);

        await users.PutAsync("u:1", new User("Alice", 20, "alice@example.com"), ct);
        await users.EnsureIndexAsync(x => x.Age, ct);

        await _db.BeginTransactionAsync(ct);
        await users.PutAsync("u:1", new User("Alice", 21, "alice@example.com"), ct);
        await users.PutAsync("u:2", new User("Bob", 30, "bob@example.com"), ct);
        await _db.RollbackAsync(ct);

        var age20 = await CollectAsync(users.FindByIndexAsync(x => x.Age, 20, ct), ct);
        var age21 = await CollectAsync(users.FindByIndexAsync(x => x.Age, 21, ct), ct);
        var age30 = await CollectAsync(users.FindByIndexAsync(x => x.Age, 30, ct), ct);

        Assert.Single(age20);
        Assert.Equal("u:1", age20[0].Key);
        Assert.Empty(age21);
        Assert.Empty(age30);
    }

    [Fact]
    public async Task EnsureIndex_CannotRunInsideExplicitTransaction()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users", ct);
        await users.PutAsync("u:1", new User("Alice", 20, "alice@example.com"), ct);

        await _db.BeginTransactionAsync(ct);
        try
        {
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(
                async () => await users.EnsureIndexAsync(x => x.Age, ct));
            Assert.Contains("explicit transaction", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            await _db.RollbackAsync(ct);
        }
    }

    [Fact]
    public async Task EnsureIndex_RejectsCollationForNonTextPath()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users_age_collation", ct);

        var ex = await Assert.ThrowsAsync<NotSupportedException>(
            async () => await users.EnsureIndexAsync(x => x.Age, "NOCASE", ct));

        Assert.Contains("text", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task EnsureIndex_RejectsCollectionCollationMismatch()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users_collation_mismatch", ct);

        await users.EnsureIndexAsync(x => x.Email, "NOCASE", ct);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await users.EnsureIndexAsync("Email", "BINARY", ct));

        Assert.Contains("already exists", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("NOCASE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task CollectionIndexes_WorkWithCustomRecordSerializer()
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

        var users = await _db.GetCollectionAsync<User>("users", ct);
        await users.PutAsync("u:1", new User("Alice", 30, "alice@example.com"), ct);
        await users.PutAsync("u:2", new User("Bob", 30, "bob@example.com"), ct);
        await users.EnsureIndexAsync(x => x.Age, ct);

        var matches = await CollectAsync(users.FindByIndexAsync(x => x.Age, 30, ct), ct);
        Assert.Equal(2, matches.Count);

        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, options, ct);

        var reopened = await _db.GetCollectionAsync<User>("users", ct);
        var reopenedMatches = await CollectAsync(reopened.FindByIndexAsync(x => x.Age, 30, ct), ct);
        Assert.Equal(2, reopenedMatches.Count);
    }

    [Fact]
    public async Task FindByIndex_SkipsMissingRowIds_FromIndexPayload()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users", ct);

        await users.PutAsync("u:1", new User("Alice", 30, "alice@example.com"), ct);
        await users.EnsureIndexAsync(x => x.Age, ct);

        var binding = GetBinding(users, nameof(User.Age));
        await InsertRowIdAsync(binding.IndexStore, 30, long.MaxValue - 1, ct);

        var matches = await CollectAsync(users.FindByIndexAsync(x => x.Age, 30, ct), ct);

        Assert.Single(matches);
        Assert.Equal("u:1", matches[0].Key);
    }

    [Fact]
    public async Task FindByIndex_FiltersTextCandidates_WhenIndexPayloadContainsWrongRow()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users", ct);

        await users.PutAsync("u:1", new User("Alice", 30, "alpha@example.com"), ct);
        await users.PutAsync("u:2", new User("Bob", 31, "beta@example.com"), ct);
        await users.EnsureIndexAsync(x => x.Email, ct);

        var binding = GetBinding(users, nameof(User.Email));
        Assert.True(binding.TryBuildKeyFromValue("alpha@example.com", out long alphaIndexKey));

        long wrongRowId = await FindStoredRowIdAsync(users, "u:2", ct);
        byte[] existingPayload = await binding.IndexStore.FindAsync(alphaIndexKey, ct)
            ?? throw new InvalidOperationException("Expected ordered text index payload.");
        byte[] updatedPayload = OrderedTextIndexPayloadCodec.Insert(
            existingPayload,
            "alpha@example.com",
            wrongRowId,
            out bool changed);
        Assert.True(changed);
        await WriteIndexPayloadAsync(binding.IndexStore, alphaIndexKey, updatedPayload, ct);

        var matches = await CollectAsync(users.FindByIndexAsync(x => x.Email, "alpha@example.com", ct), ct);

        Assert.Single(matches);
        Assert.Equal("u:1", matches[0].Key);
    }

    [Fact]
    public async Task CollectionIndexes_MaintainLegacyUnsortedRowIdPayloads()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users", ct);

        await users.PutAsync("u:1", new User("Alice", 30, "alice@example.com"), ct);
        await users.PutAsync("u:2", new User("Bob", 30, "bob@example.com"), ct);
        await users.PutAsync("u:3", new User("Cara", 30, "cara@example.com"), ct);
        await users.EnsureIndexAsync(x => x.Age, ct);

        var binding = GetBinding(users, nameof(User.Age));
        byte[] existingPayload = await binding.IndexStore.FindAsync(30, ct)
            ?? throw new InvalidOperationException("Expected collection index payload.");
        byte[] legacyPayload = ReverseRowIdPayload(existingPayload);
        await WriteIndexPayloadAsync(binding.IndexStore, 30, legacyPayload, ct);

        await users.PutAsync("u:4", new User("Dana", 30, "dana@example.com"), ct);

        var afterInsert = await CollectAsync(users.FindByIndexAsync(x => x.Age, 30, ct), ct);
        Assert.Equal(4, afterInsert.Count);
        Assert.Equal(
            ["u:1", "u:2", "u:3", "u:4"],
            afterInsert.Select(match => match.Key).OrderBy(key => key).ToArray());

        Assert.True(await users.DeleteAsync("u:2", ct));

        var afterDelete = await CollectAsync(users.FindByIndexAsync(x => x.Age, 30, ct), ct);
        Assert.Equal(3, afterDelete.Count);
        Assert.Equal(
            ["u:1", "u:3", "u:4"],
            afterDelete.Select(match => match.Key).OrderBy(key => key).ToArray());
    }

    [Fact]
    public async Task Delete_UsesDirectPayloadIndexCleanup_WhenStoredDocumentCannotDeserialize()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users", ct);

        await InsertMalformedDirectPayloadAsync(
            users,
            "u:broken",
            """{"name":"Broken","age":"not-an-int","email":"broken@example.com"}""",
            ct);
        await users.EnsureIndexAsync(x => x.Email, ct);

        Assert.True(await users.DeleteAsync("u:broken", ct));
        Assert.Empty(await CollectAsync(users.FindByIndexAsync(x => x.Email, "broken@example.com", ct), ct));
    }

    [Fact]
    public async Task Put_UpdateExistingDocument_UsesDirectPayloadIndexCleanup_WhenStoredDocumentCannotDeserialize()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users", ct);

        await InsertMalformedDirectPayloadAsync(
            users,
            "u:broken",
            """{"name":"Broken","age":"not-an-int","email":"broken@example.com"}""",
            ct);
        await users.EnsureIndexAsync(x => x.Email, ct);

        await users.PutAsync("u:broken", new User("Fixed", 33, "fixed@example.com"), ct);

        Assert.Empty(await CollectAsync(users.FindByIndexAsync(x => x.Email, "broken@example.com", ct), ct));

        var matches = await CollectAsync(users.FindByIndexAsync(x => x.Email, "fixed@example.com", ct), ct);
        Assert.Single(matches);
        Assert.Equal("u:broken", matches[0].Key);

        var stored = await users.GetAsync("u:broken", ct);
        Assert.NotNull(stored);
        Assert.Equal("Fixed", stored!.Name);
        Assert.Equal(33, stored.Age);
        Assert.Equal("fixed@example.com", stored.Email);
    }

    private static async Task<List<KeyValuePair<string, TDocument>>> CollectAsync<TDocument>(
        IAsyncEnumerable<KeyValuePair<string, TDocument>> source,
        CancellationToken ct)
    {
        var items = new List<KeyValuePair<string, TDocument>>();
        await foreach (var item in source.WithCancellation(ct))
            items.Add(item);
        return items;
    }

    private async Task InsertMalformedDirectPayloadAsync<TDocument>(
        Collection<TDocument> collection,
        string key,
        string json,
        CancellationToken ct)
    {
        var tree = GetCollectionTree(collection);
        var catalog = GetCollectionCatalog(collection);
        string catalogTableName = GetCollectionCatalogTableName(collection);
        byte[] payload = CollectionPayloadCodec.Encode(key, Encoding.UTF8.GetBytes(json));

        await _db.BeginTransactionAsync(ct);
        try
        {
            await tree.InsertAsync(Collection<TDocument>.HashDocumentKey(key), payload, ct);
            await catalog.AdjustTableRowCountAsync(catalogTableName, 1, ct);
            await _db.CommitAsync(ct);
        }
        catch
        {
            await _db.RollbackAsync(ct);
            throw;
        }
    }

    private static CollectionIndexBinding<TDocument> GetBinding<TDocument>(
        Collection<TDocument> collection,
        string fieldPath)
    {
        var indexesField = typeof(Collection<TDocument>).GetField("_indexes", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("Collection indexes field not found.");
        var indexes = (Dictionary<string, CollectionIndexBinding<TDocument>>)indexesField.GetValue(collection)!
            ?? throw new InvalidOperationException("Collection indexes were not initialized.");

        return indexes[fieldPath];
    }

    private static BTree GetCollectionTree<TDocument>(Collection<TDocument> collection)
    {
        var treeField = typeof(Collection<TDocument>).GetField("_tree", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("Collection tree field not found.");
        return (BTree?)treeField.GetValue(collection)
            ?? throw new InvalidOperationException("Collection tree was not initialized.");
    }

    private static CollectionDocumentCodec<TDocument> GetCollectionCodec<TDocument>(Collection<TDocument> collection)
    {
        var codecField = typeof(Collection<TDocument>).GetField("_codec", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("Collection codec field not found.");
        return (CollectionDocumentCodec<TDocument>?)codecField.GetValue(collection)
            ?? throw new InvalidOperationException("Collection codec was not initialized.");
    }

    private static SchemaCatalog GetCollectionCatalog<TDocument>(Collection<TDocument> collection)
    {
        var catalogField = typeof(Collection<TDocument>).GetField("_catalog", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("Collection catalog field not found.");
        return (SchemaCatalog?)catalogField.GetValue(collection)
            ?? throw new InvalidOperationException("Collection catalog was not initialized.");
    }

    private static string GetCollectionCatalogTableName<TDocument>(Collection<TDocument> collection)
    {
        var tableNameField = typeof(Collection<TDocument>).GetField("_catalogTableName", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("Collection catalog table name field not found.");
        return (string?)tableNameField.GetValue(collection)
            ?? throw new InvalidOperationException("Collection catalog table name was not initialized.");
    }

    private static async Task<long> FindStoredRowIdAsync<TDocument>(
        Collection<TDocument> collection,
        string key,
        CancellationToken ct)
    {
        var tree = GetCollectionTree(collection);
        var codec = GetCollectionCodec(collection);
        long startHash = Collection<TDocument>.HashDocumentKey(key);

        for (int probe = 0; probe < 128; probe++)
        {
            long probeHash = (startHash + probe) & 0x7FFFFFFFFFFFFFFF;
            var payload = await tree.FindMemoryAsync(probeHash, ct);
            if (payload is not { } payloadMemory)
                break;

            if (codec.PayloadMatchesKey(payloadMemory.Span, key))
                return probeHash;
        }

        throw new InvalidOperationException($"Stored row for key '{key}' was not found.");
    }

    private async Task InsertRowIdAsync(
        IIndexStore indexStore,
        long indexKey,
        long rowId,
        CancellationToken ct)
    {
        byte[]? existing = await indexStore.FindAsync(indexKey, ct);
        if (existing == null)
        {
            await indexStore.InsertAsync(indexKey, RowIdPayloadCodec.CreateSingle(rowId), ct);
            return;
        }

        if (!RowIdPayloadCodec.TryInsert(existing, rowId, out byte[] payload))
            return;

        await _db.BeginTransactionAsync(ct);
        try
        {
            await indexStore.DeleteAsync(indexKey, ct);
            await indexStore.InsertAsync(indexKey, payload, ct);
            await _db.CommitAsync(ct);
        }
        catch
        {
            await _db.RollbackAsync(ct);
            throw;
        }
    }

    private async Task WriteIndexPayloadAsync(
        IIndexStore indexStore,
        long indexKey,
        byte[] payload,
        CancellationToken ct)
    {
        await _db.BeginTransactionAsync(ct);
        try
        {
            await indexStore.DeleteAsync(indexKey, ct);
            await indexStore.InsertAsync(indexKey, payload, ct);
            await _db.CommitAsync(ct);
        }
        catch
        {
            await _db.RollbackAsync(ct);
            throw;
        }
    }

    private static byte[] ReverseRowIdPayload(ReadOnlySpan<byte> payload)
    {
        int count = RowIdPayloadCodec.GetCount(payload);
        byte[] reversed = new byte[payload.Length];
        for (int i = 0; i < count; i++)
        {
            long rowId = RowIdPayloadCodec.ReadAt(payload, count - 1 - i);
            long destinationOffset = (long)i * RowIdPayloadCodec.RowIdSize;
            BitConverter.TryWriteBytes(reversed.AsSpan((int)destinationOffset, RowIdPayloadCodec.RowIdSize), rowId);
        }

        return reversed;
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

        public void DecodeSelectedCompactInto(ReadOnlySpan<byte> buffer, Span<DbValue> destination, ReadOnlySpan<int> selectedColumnIndices)
            => _inner.DecodeSelectedCompactInto(StripPrefix(buffer), destination, selectedColumnIndices);

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
