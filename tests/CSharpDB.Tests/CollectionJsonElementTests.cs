using System.Text.Json;
using CSharpDB.Engine;

namespace CSharpDB.Tests;

public sealed class CollectionJsonElementTests
{
    private sealed record TypedCollectionDocument(
        string Name,
        int Count,
        bool Active,
        string[] Tags,
        TypedCollectionNested Nested);

    private sealed record TypedCollectionNested(string City);

    [Fact]
    public async Task JsonElementCollection_RoundTripsDirectPayloadDocuments()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var db = await Database.OpenInMemoryAsync(ct);
        var collection = await db.GetCollectionAsync<JsonElement>("json_docs", ct);

        JsonElement document;
        using (var json = JsonDocument.Parse("""{"name":"json","meta":{"count":2}}"""))
            document = json.RootElement.Clone();

        await collection.PutAsync("doc-1", document, ct);
        JsonElement loaded = await collection.GetAsync("doc-1", ct);

        Assert.Equal("json", loaded.GetProperty("name").GetString());
        Assert.Equal(2, loaded.GetProperty("meta").GetProperty("count").GetInt32());
    }

    [Fact]
    public async Task JsonElementCollection_ReadsBinaryTypedDocuments()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var db = await Database.OpenInMemoryAsync(ct);
        var typedCollection = await db.GetCollectionAsync<TypedCollectionDocument>("typed_docs", ct);

        await typedCollection.PutAsync(
            "doc-1",
            new TypedCollectionDocument(
                "scanner",
                2,
                true,
                ["alpha", "beta"],
                new TypedCollectionNested("Seattle")),
            ct);

        var jsonCollection = await db.GetCollectionAsync<JsonElement>("typed_docs", ct);

        JsonElement loaded = await jsonCollection.GetAsync("doc-1", ct);
        Assert.Equal(JsonValueKind.Object, loaded.ValueKind);
        Assert.Equal("scanner", loaded.GetProperty("name").GetString());
        Assert.Equal(2, loaded.GetProperty("count").GetInt32());
        Assert.True(loaded.GetProperty("active").GetBoolean());
        Assert.Equal("beta", loaded.GetProperty("tags")[1].GetString());
        Assert.Equal("Seattle", loaded.GetProperty("nested").GetProperty("city").GetString());

        var scanned = new List<KeyValuePair<string, JsonElement>>();
        await foreach (var item in jsonCollection.ScanAsync(ct))
            scanned.Add(item);

        var row = Assert.Single(scanned);
        Assert.Equal("doc-1", row.Key);
        Assert.Equal("scanner", row.Value.GetProperty("name").GetString());
    }
}
