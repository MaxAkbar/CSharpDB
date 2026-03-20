using CSharpDB.Engine;

var sampleDirectory = AppContext.BaseDirectory;
var dbPath = Path.Combine(sampleDirectory, "collection-indexing-demo.db");

if (File.Exists(dbPath))
    File.Delete(dbPath);

await using var db = await Database.OpenAsync(dbPath);
var users = await db.GetCollectionAsync<UserDocument>("users");

await SeedAsync(users);
await CreateIndexesAsync(users);

await PrintMatchesAsync(
    "Email equality via top-level scalar index",
    users.FindByIndexAsync("Email", "ada@acme.io"));

await PrintMatchesAsync(
    "City equality via nested object path index",
    users.FindByPathAsync("Address.City", "Seattle"));

await PrintMatchesAsync(
    "Tag membership via array-element path index",
    users.FindByPathAsync("$.tags[]", "premium"));

await PrintMatchesAsync(
    "Order SKU lookup via nested array-object path index",
    users.FindByPathAsync("$.orders[].sku", "SKU-2005"));

await PrintMatchesAsync(
    "Loyalty points range via scalar range index",
    users.FindByPathRangeAsync("LoyaltyPoints", 1000, 3000));

await PrintMatchesAsync(
    "Email ordered text range [a, h)",
    users.FindByPathRangeAsync("Email", "a", "h", upperInclusive: false));

Console.WriteLine();
Console.WriteLine($"Database written to: {dbPath}");

static async Task SeedAsync(Collection<UserDocument> users)
{
    await users.PutAsync("user:ada", new UserDocument(
        Email: "ada@acme.io",
        LoyaltyPoints: 1200,
        Address: new AddressDocument("Seattle", "WA"),
        Tags: ["premium", "beta"],
        Orders:
        [
            new OrderDocument("SKU-1001", 2),
            new OrderDocument("SKU-2005", 1)
        ]));

    await users.PutAsync("user:grace", new UserDocument(
        Email: "grace@acme.io",
        LoyaltyPoints: 420,
        Address: new AddressDocument("Portland", "OR"),
        Tags: ["standard"],
        Orders:
        [
            new OrderDocument("SKU-2005", 3)
        ]));

    await users.PutAsync("user:linus", new UserDocument(
        Email: "linus@acme.io",
        LoyaltyPoints: 2450,
        Address: new AddressDocument("Seattle", "WA"),
        Tags: ["premium"],
        Orders:
        [
            new OrderDocument("SKU-9000", 1)
        ]));
}

static async Task CreateIndexesAsync(Collection<UserDocument> users)
{
    await users.EnsureIndexAsync(u => u.Email);
    await users.EnsureIndexAsync(u => u.LoyaltyPoints);
    await users.EnsureIndexAsync("$.address.city");
    await users.EnsureIndexAsync("$.tags[]");
    await users.EnsureIndexAsync("$.orders[].sku");
}

static async Task PrintMatchesAsync(
    string title,
    IAsyncEnumerable<KeyValuePair<string, UserDocument>> matches)
{
    Console.WriteLine();
    Console.WriteLine(title);

    await foreach (var match in matches)
    {
        Console.WriteLine(
            $"  {match.Key} | {match.Value.Email} | {match.Value.Address.City} | points={match.Value.LoyaltyPoints}");
    }
}

public sealed record UserDocument(
    string Email,
    int LoyaltyPoints,
    AddressDocument Address,
    string[] Tags,
    OrderDocument[] Orders);

public sealed record AddressDocument(string City, string State);

public sealed record OrderDocument(string Sku, int Quantity);
