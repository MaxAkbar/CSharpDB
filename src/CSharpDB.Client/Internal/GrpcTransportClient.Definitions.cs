using System.Text.Json;
using CSharpDB.Client.Grpc;
using CSharpDB.Client.Models;
using Grpc.Core;

namespace CSharpDB.Client.Internal;

internal sealed partial class GrpcTransportClient : ICSharpDbDefinitionCatalogReader
{
    public async Task<DefinitionCatalogPage> ReadDefinitionCatalogAsync(string? continuationToken = null, int pageSize = 64, CancellationToken ct = default)
    {
        try
        {
            var response = await _client.ReadDefinitionCatalogAsync(new DefinitionCatalogRequest
            { ContinuationToken = continuationToken ?? "", PageSize = pageSize }, cancellationToken: ct);
            return JsonSerializer.Deserialize<DefinitionCatalogPage>(response.JsonUtf8.Span, new JsonSerializerOptions(JsonSerializerDefaults.Web))
                ?? throw new InvalidOperationException("The server returned an empty definition catalog.");
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.Unimplemented)
        { throw new NotSupportedException("The server does not support definition inspection. Upgrade the server.", ex); }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.Cancelled && ct.IsCancellationRequested)
        { throw new OperationCanceledException("Definition inspection canceled.", ex, ct); }
    }
}
