using System.Reflection;
using System.Text.Json;
using CSharpDB.Client.Internal;

namespace CSharpDB.Tests;

public sealed class ClientCollectionConcurrencyTests
{
    [Theory]
    [InlineData("names")]
    [InlineData("count")]
    [InlineData("browse")]
    [InlineData("get")]
    [InlineData("put")]
    [InlineData("delete")]
    [InlineData("drop")]
    public async Task CollectionOperations_WaitForCatalogOwner_AndHonorCancellation(string operation)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var client = EngineTransportClient.CreatePrivateMemory(
            "collection-concurrency", loadFromPath: null, directDatabaseOptions: null);
        using JsonDocument document = JsonDocument.Parse("""{"name":"original"}""");
        await client.GetInfoAsync(ct);
        await client.PutDocumentAsync("docs", "one", document.RootElement, ct);

        // Hold the same admission gate as GetInfoAsync's startup catalog DDL.
        // In-memory operations otherwise complete synchronously, so no sleep or
        // thread-pool timing is needed to expose a collection call bypassing it.
        var gate = Assert.IsType<SemaphoreSlim>(typeof(EngineTransportClient)
            .GetField("_lock", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(client));
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(ct);
        await gate.WaitAsync(ct);
        Task? pending = null;
        try
        {
            pending = InvokeOperationAsync(cancellation.Token);
            Assert.False(pending.IsCompleted);
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
        }
        finally
        {
            cancellation.Cancel();
            gate.Release();
            if (pending is not null)
            {
                try
                {
                    await pending;
                }
                catch (OperationCanceledException)
                {
                    // The cancelled waiter must not prevent later operations.
                }
            }
        }

        JsonElement? retained = await client.GetDocumentAsync("docs", "one", ct);
        Assert.True(retained.HasValue);
        Assert.Equal("original", retained.Value.GetProperty("name").GetString());
        Assert.Equal(1, await client.GetCollectionCountAsync("docs", ct));
        await InvokeOperationAsync(ct);

        Task InvokeOperationAsync(CancellationToken token) => operation switch
        {
            "names" => client.GetCollectionNamesAsync(token),
            "count" => client.GetCollectionCountAsync("docs", token),
            "browse" => client.BrowseCollectionAsync("docs", ct: token),
            "get" => client.GetDocumentAsync("docs", "one", token),
            "put" => client.PutDocumentAsync("docs", "two", document.RootElement, token),
            "delete" => client.DeleteDocumentAsync("docs", "one", token),
            "drop" => client.DropCollectionAsync("docs", token),
            _ => throw new ArgumentOutOfRangeException(nameof(operation)),
        };
    }
}
