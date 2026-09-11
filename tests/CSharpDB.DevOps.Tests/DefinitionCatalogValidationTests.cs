using System.Reflection;
using CSharpDB.Client;
using CSharpDB.Client.Models;

namespace CSharpDB.DevOps.Tests;

public sealed class DefinitionCatalogValidationTests
{
    public interface ICatalogClient : ICSharpDbClient, ICSharpDbDefinitionCatalogReader { }
    public class CatalogProxy : DispatchProxy
    {
        public Func<DefinitionCatalogPage> Page = null!;
        protected override object? Invoke(MethodInfo? method, object?[]? args) => method!.Name == "ReadDefinitionCatalogAsync" ? Task.FromResult(Page()) : throw new NotSupportedException();
    }
    private static ICatalogClient Client(Func<DefinitionCatalogPage> page) { var client = DispatchProxy.Create<ICatalogClient, CatalogProxy>(); ((CatalogProxy)(object)client).Page = page; return client; }

    [Fact]
    public async Task RepeatedPageAndChangingVersionAreRecoverableErrors()
    {
        var ct = TestContext.Current.CancellationToken;
        await Assert.ThrowsAsync<InvalidOperationException>(() => DefinitionCatalogService.ReadAsync(Client(() => new() { CatalogVersion = "a", ContinuationToken = "same" }), ct: ct));
        int count = 0;
        await Assert.ThrowsAsync<InvalidOperationException>(() => DefinitionCatalogService.ReadAsync(Client(() => new() { CatalogVersion = (++count).ToString(), ContinuationToken = "next" + count }), ct: ct));
    }

    [Fact]
    public async Task MissingInconsistentAndCorruptFragmentsAreRejected()
    {
        var record = DefinitionExplorerTests.Definition("View", "v", "SELECT 1"); var ct = TestContext.Current.CancellationToken;
        foreach (var records in new IReadOnlyList<DefinitionCatalogRecord>[]
        {
            [record with { PartCount = 2 }],
            [record with { Source = "changed" }],
            [record with { PartCount = 2, PartIndex = 0, Source = "SELECT " }, record with { PartCount = 2, PartIndex = 1, Source = "1", OwnerName = "wrong route" }],
        }) await Assert.ThrowsAsync<InvalidOperationException>(() => DefinitionCatalogService.ReadAsync(Client(() => new() { CatalogVersion = "1", Records = records }), ct: ct));
        var valid = await DefinitionCatalogService.ReadAsync(Client(() => new() { CatalogVersion = "1", Records = [record with { PartCount = 2, Source = "SELECT " }, record with { PartCount = 2, PartIndex = 1, Source = "1" }] }), ct: ct);
        Assert.Equal("SELECT 1", Assert.Single(valid.Definitions).Source);
    }
}
