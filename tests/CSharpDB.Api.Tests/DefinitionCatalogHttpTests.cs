using CSharpDB.Client;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;

namespace CSharpDB.Api.Tests;

public sealed class DefinitionCatalogHttpTests
{
    [Fact]
    public async Task Catalog_EmbeddedAndRestParityWithPaginationAndNativeDefinitions()
    {
        string path = Path.Combine(Path.GetTempPath(), $"definitions-http-{Guid.NewGuid():N}.db");
        var ct = TestContext.Current.CancellationToken;
        try
        {
            var expected = await DefinitionCatalogTransportContract.SeedAsync(path, ct);
            await using var factory = new Factory(path); using var http = factory.CreateClient();
            await using var client = CSharpDbClient.Create(new CSharpDbClientOptions { Transport = CSharpDbTransport.Http, Endpoint = http.BaseAddress!.ToString(), HttpClient = http });
            await DefinitionCatalogTransportContract.VerifyAsync(client, expected, ct);
            await DatabaseDocumenterTransportContract.VerifyAsync(client, ct);
            using var invalid = await http.GetAsync("api/catalog/definitions?pageSize=65", ct);
            Assert.Equal(System.Net.HttpStatusCode.BadRequest, invalid.StatusCode);
        }
        finally { foreach (string file in new[] { path, path + ".wal", path + ".shm", path + ".archive.csdbtable" }) await ApiTestFileCleanup.DeleteIfExistsAsync(file); }
    }
    private sealed class Factory(string path) : WebApplicationFactory<Program>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder) => builder.UseEnvironment("Development").ConfigureAppConfiguration((_, config) => config.AddInMemoryCollection(new Dictionary<string,string?> { ["ConnectionStrings:CSharpDB"] = "Data Source=" + path }));
    }
}
