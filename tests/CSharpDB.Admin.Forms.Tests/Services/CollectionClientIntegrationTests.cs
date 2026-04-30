using System.Text.Json;

namespace CSharpDB.Admin.Forms.Tests.Services;

public sealed class CollectionClientIntegrationTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task CollectionClient_CreateBrowseUpdateAndDeleteDocument()
    {
        await using var db = await TestDatabaseScope.CreateAsync("admin_collections");

        JsonElement initialDocument;
        using (var json = JsonDocument.Parse("""{"name":"Ada","active":true}"""))
            initialDocument = json.RootElement.Clone();

        await db.Client.PutDocumentAsync("profiles", "user-1", initialDocument, Ct);

        var collectionNames = await db.Client.GetCollectionNamesAsync(Ct);
        Assert.Contains("profiles", collectionNames);

        var page = await db.Client.BrowseCollectionAsync("profiles", page: 1, pageSize: 25, ct: Ct);
        var browsed = Assert.Single(page.Documents);
        Assert.Equal("user-1", browsed.Key);
        Assert.Equal("Ada", browsed.Document.GetProperty("name").GetString());

        JsonElement updatedDocument;
        using (var json = JsonDocument.Parse("""{"name":"Ada Lovelace","active":false}"""))
            updatedDocument = json.RootElement.Clone();

        await db.Client.PutDocumentAsync("profiles", "user-1", updatedDocument, Ct);

        JsonElement? loaded = await db.Client.GetDocumentAsync("profiles", "user-1", Ct);
        Assert.NotNull(loaded);
        Assert.Equal("Ada Lovelace", loaded.Value.GetProperty("name").GetString());
        Assert.False(loaded.Value.GetProperty("active").GetBoolean());

        Assert.True(await db.Client.DeleteDocumentAsync("profiles", "user-1", Ct));
        Assert.Null(await db.Client.GetDocumentAsync("profiles", "user-1", Ct));

        await db.Client.DropCollectionAsync("profiles", Ct);
        collectionNames = await db.Client.GetCollectionNamesAsync(Ct);
        Assert.DoesNotContain("profiles", collectionNames);
    }
}
