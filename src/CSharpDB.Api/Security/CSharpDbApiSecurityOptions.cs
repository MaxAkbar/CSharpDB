namespace CSharpDB.Api.Security;

public sealed class CSharpDbApiSecurityOptions
{
    public const string DefaultApiKeyHeaderName = "X-CSharpDB-Api-Key";

    public CSharpDbRemoteSecurityMode Mode { get; set; } = CSharpDbRemoteSecurityMode.None;

    public string? ApiKey { get; set; }

    public string ApiKeyHeaderName { get; set; } = DefaultApiKeyHeaderName;
}
