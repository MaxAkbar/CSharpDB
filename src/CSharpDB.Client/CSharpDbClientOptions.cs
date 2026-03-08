namespace CSharpDB.Client;

public sealed class CSharpDbClientOptions
{
    public CSharpDbTransport? Transport { get; init; }
    public string? Endpoint { get; init; }
    public string? ConnectionString { get; init; }
    public string? DataSource { get; init; }
    public HttpClient? HttpClient { get; init; }
}
