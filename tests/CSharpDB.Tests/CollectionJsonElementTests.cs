using System.Text.Json;
using CSharpDB.Engine;

namespace CSharpDB.Tests;

public sealed class CollectionJsonElementTests
{
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
}
