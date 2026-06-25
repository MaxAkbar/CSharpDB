using CSharpDB.Engine;

namespace CSharpDB.Client;

public sealed class CSharpDbClientOptions
{
    public CSharpDbTransport? Transport { get; init; }
    public string? Endpoint { get; init; }
    public string? ConnectionString { get; init; }
    public string? DataSource { get; init; }
    public HttpClient? HttpClient { get; init; }
    public string? ApiKey { get; init; }
    public string ApiKeyHeaderName { get; init; } = "X-CSharpDB-Api-Key";
    public CSharpDbRouteContext? RouteContext { get; init; }
    public DatabaseOptions? DirectDatabaseOptions { get; init; }
    public HybridDatabaseOptions? HybridDatabaseOptions { get; init; }
}
