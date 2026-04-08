using System.Text.Json.Serialization;
using CSharpDB.Engine;

CancellationToken ct = CancellationToken.None;

await using var db = await Database.OpenInMemoryAsync(ct);
var customers = await db.GetGeneratedCollectionAsync<TrimSmokeCustomer>("customers", ct);

await customers.PutAsync(
    "c1",
    new TrimSmokeCustomer(
        "Alice",
        [new TrimSmokeOrder("sku-1", 2), new TrimSmokeOrder("sku-2", 1)],
        new TrimSmokeAddress("Seattle", 98101)),
    ct);
await customers.PutAsync(
    "c2",
    new TrimSmokeCustomer(
        "Bob",
        [new TrimSmokeOrder("sku-3", 4)],
        new TrimSmokeAddress("Portland", 97201)),
    ct);
await customers.PutAsync(
    "c3",
    new TrimSmokeCustomer(
        "Cara",
        [new TrimSmokeOrder("sku-2", 3)],
        new TrimSmokeAddress("Seattle", 98109)),
    ct);

await customers.EnsureIndexAsync(TrimSmokeCustomer.Collection.Orders_Sku, ct);
await customers.EnsureIndexAsync(TrimSmokeCustomer.Collection.Address_City, ct);

var skuMatches = await CollectAsync(
    customers.FindByIndexAsync(TrimSmokeCustomer.Collection.Orders_Sku, "sku-2", ct),
    ct);
var cityMatches = await CollectAsync(
    customers.FindByIndexAsync(TrimSmokeCustomer.Collection.Address_City, "Seattle", ct),
    ct);

if (skuMatches.Count != 2 || cityMatches.Count != 2)
    throw new InvalidOperationException("Generated collection trim smoke validation failed.");

Console.WriteLine("Generated collection trim smoke passed.");

static async Task<List<KeyValuePair<string, TDocument>>> CollectAsync<TDocument>(
    IAsyncEnumerable<KeyValuePair<string, TDocument>> source,
    CancellationToken ct)
{
    var items = new List<KeyValuePair<string, TDocument>>();
    await foreach (KeyValuePair<string, TDocument> item in source.WithCancellation(ct))
        items.Add(item);

    return items;
}

[CollectionModel(typeof(TrimSmokeCustomerJsonContext))]
internal sealed partial record TrimSmokeCustomer(
    string Name,
    IReadOnlyList<TrimSmokeOrder> Orders,
    TrimSmokeAddress Address);

internal sealed partial record TrimSmokeOrder(string Sku, int Quantity);

internal sealed partial record TrimSmokeAddress(string City, int ZipCode);

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(TrimSmokeCustomer))]
internal sealed partial class TrimSmokeCustomerJsonContext : JsonSerializerContext;
