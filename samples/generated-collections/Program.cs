using System.Text.Json.Serialization;
using CSharpDB.Engine;

var sampleDirectory = AppContext.BaseDirectory;
var dbPath = Path.Combine(sampleDirectory, "generated-collections-demo.db");

DeleteIfExists(dbPath);
DeleteIfExists(dbPath + ".wal");

await using var db = await Database.OpenAsync(dbPath);
var customers = await db.GetGeneratedCollectionAsync<CustomerRecord>("customers");

await SeedAsync(customers);
await CreateIndexesAsync(customers);

await PrintMatchesAsync(
    "Email equality via generated descriptor",
    customers.FindByIndexAsync(CustomerRecord.Collection.Email, "ada@acme.io"));

await PrintMatchesAsync(
    "City equality via nested generated descriptor",
    customers.FindByIndexAsync(CustomerRecord.Collection.Address_City, "Seattle"));

await PrintMatchesAsync(
    "Tag membership via generated array descriptor",
    customers.FindByIndexAsync(CustomerRecord.Collection.Tags, "priority"));

await PrintMatchesAsync(
    "Order SKU lookup via generated nested-array descriptor",
    customers.FindByIndexAsync(CustomerRecord.Collection.Orders_Sku, "SKU-2005"));

await PrintMatchesAsync(
    "Loyalty points range via generated scalar descriptor",
    customers.FindByRangeAsync(CustomerRecord.Collection.LoyaltyPoints, 1000, 3000));

await PrintMatchesAsync(
    "Zip code range via generated nested scalar descriptor",
    customers.FindByRangeAsync(CustomerRecord.Collection.Address_ZipCode, 98100, 98110));

Console.WriteLine();
Console.WriteLine($"Database written to: {dbPath}");
Console.WriteLine("The public descriptor names stay on CLR member names even though Email serializes as 'email_address'.");

static async Task SeedAsync(GeneratedCollection<CustomerRecord> customers)
{
    await customers.PutAsync("customer:ada", new CustomerRecord(
        Email: "ada@acme.io",
        LoyaltyPoints: 1200,
        Address: new AddressRecord("Seattle", 98101),
        Tags: ["priority", "beta"],
        Orders:
        [
            new OrderRecord("SKU-1001", 2),
            new OrderRecord("SKU-2005", 1)
        ]));

    await customers.PutAsync("customer:grace", new CustomerRecord(
        Email: "grace@acme.io",
        LoyaltyPoints: 420,
        Address: new AddressRecord("Portland", 97201),
        Tags: ["standard"],
        Orders:
        [
            new OrderRecord("SKU-2005", 3)
        ]));

    await customers.PutAsync("customer:linus", new CustomerRecord(
        Email: "linus@acme.io",
        LoyaltyPoints: 2450,
        Address: new AddressRecord("Seattle", 98109),
        Tags: ["priority"],
        Orders:
        [
            new OrderRecord("SKU-9000", 1)
        ]));
}

static async Task CreateIndexesAsync(GeneratedCollection<CustomerRecord> customers)
{
    await customers.EnsureIndexAsync(CustomerRecord.Collection.Email);
    await customers.EnsureIndexAsync(CustomerRecord.Collection.LoyaltyPoints);
    await customers.EnsureIndexAsync(CustomerRecord.Collection.Address_City);
    await customers.EnsureIndexAsync(CustomerRecord.Collection.Address_ZipCode);
    await customers.EnsureIndexAsync(CustomerRecord.Collection.Tags);
    await customers.EnsureIndexAsync(CustomerRecord.Collection.Orders_Sku);
}

static async Task PrintMatchesAsync(
    string title,
    IAsyncEnumerable<KeyValuePair<string, CustomerRecord>> matches)
{
    Console.WriteLine();
    Console.WriteLine(title);

    await foreach (KeyValuePair<string, CustomerRecord> match in matches)
    {
        Console.WriteLine(
            $"  {match.Key} | {match.Value.Email} | {match.Value.Address.City} {match.Value.Address.ZipCode} | points={match.Value.LoyaltyPoints}");
    }
}

static void DeleteIfExists(string path)
{
    if (File.Exists(path))
        File.Delete(path);
}

[CollectionModel(typeof(CustomerRecordJsonContext))]
internal sealed partial record CustomerRecord(
    [property: JsonPropertyName("email_address")] string Email,
    [property: JsonPropertyName("loyalty_points")] int LoyaltyPoints,
    AddressRecord Address,
    IReadOnlyList<string> Tags,
    IReadOnlyList<OrderRecord> Orders);

internal sealed partial record AddressRecord(string City, int ZipCode);

internal sealed partial record OrderRecord(string Sku, int Quantity);

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(CustomerRecord))]
internal sealed partial class CustomerRecordJsonContext : JsonSerializerContext;
