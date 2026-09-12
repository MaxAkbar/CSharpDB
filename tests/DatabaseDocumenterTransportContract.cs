using CSharpDB.Client;
using CSharpDB.DevOps;

internal static class DatabaseDocumenterTransportContract
{
    internal static async Task VerifyAsync(ICSharpDbClient client, CancellationToken ct)
    {
        var generator = new DatabaseDocumenterService();
        var document = await generator.GenerateAsync(client, "Transport database", ct: ct);
        Xunit.Assert.Equal(1, document.DocumentationVersion);
        var table = document.Entries.Single(e => e.Kind == "Table" && e.Name == "Orders");
        var store = new DatabaseDocumentationStore();
        await store.SaveAsync(client, table.Id, table.Fingerprint, table.Revision, "Shared description with 'quotes' and λ.", ct);
        var updated = await generator.GenerateAsync(client, "Transport database", ct: ct);
        var entry = updated.Entries.Single(e => e.Id == table.Id);
        Xunit.Assert.Equal("Shared description with 'quotes' and λ.", entry.Description);
        Xunit.Assert.Equal(1, entry.Revision);
        Xunit.Assert.NotEqual(document.CatalogVersion, updated.CatalogVersion);
        Xunit.Assert.Contains("Shared description", DatabaseDocumentRenderer.RenderHtml(updated));
        Xunit.Assert.Contains("Shared description", DatabaseDocumentRenderer.RenderMarkdown(updated));
        await Xunit.Assert.ThrowsAsync<InvalidOperationException>(() => store.SaveAsync(client, table.Id, table.Fingerprint, 0, "Stale edit", ct));
        await store.SaveAsync(client, entry.Id, entry.Fingerprint, entry.Revision, null, ct);
        entry = (await generator.GenerateAsync(client, ct: ct)).Entries.Single(e => e.Id == table.Id);
        Xunit.Assert.Null(entry.Description);
        Xunit.Assert.Equal(2, entry.Revision);
        Xunit.Assert.DoesNotContain("__documentation_annotations", await client.GetTableNamesAsync(ct));
    }
}
