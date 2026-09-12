using CSharpDB.Client.Models;

namespace CSharpDB.Client.Internal;

internal sealed partial class HttpTransportClient : ICSharpDbDefinitionCatalogReader
{
    public async Task<DefinitionCatalogPage> ReadDefinitionCatalogAsync(string? continuationToken = null, int pageSize = 64, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        try
        {
            using var response = await SendAsync(HttpMethod.Get,
                BuildUri($"api/catalog/definitions?pageSize={pageSize}&continuationToken={Uri.EscapeDataString(continuationToken ?? "")}"), null, ct);
            if (response.StatusCode is System.Net.HttpStatusCode.NotFound or System.Net.HttpStatusCode.NotImplemented)
                throw new NotSupportedException("The server does not support definition inspection. Upgrade the server.");
            return await ReadRequiredAsync<DefinitionCatalogPage>(response, ct);
        }
        catch (Exception ex) when (ct.IsCancellationRequested && ex is IOException or HttpRequestException)
        { throw new OperationCanceledException("Definition catalog loading was cancelled.", ex, ct); }
    }
}
