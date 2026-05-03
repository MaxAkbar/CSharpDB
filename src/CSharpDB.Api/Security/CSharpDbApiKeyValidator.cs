using System.Security.Cryptography;
using System.Text;

namespace CSharpDB.Api.Security;

public static class CSharpDbApiKeyValidator
{
    public static bool IsAuthorized(CSharpDbApiSecurityOptions options, string? suppliedApiKey)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (options.Mode == CSharpDbRemoteSecurityMode.None)
            return true;

        if (options.Mode != CSharpDbRemoteSecurityMode.ApiKey)
            return false;

        if (string.IsNullOrEmpty(options.ApiKey) || string.IsNullOrEmpty(suppliedApiKey))
            return false;

        byte[] expectedHash = SHA256.HashData(Encoding.UTF8.GetBytes(options.ApiKey));
        byte[] suppliedHash = SHA256.HashData(Encoding.UTF8.GetBytes(suppliedApiKey));
        return CryptographicOperations.FixedTimeEquals(expectedHash, suppliedHash);
    }

    public static string NormalizeHeaderName(string? headerName, bool forGrpc = false)
    {
        string normalized = string.IsNullOrWhiteSpace(headerName)
            ? CSharpDbApiSecurityOptions.DefaultApiKeyHeaderName
            : headerName.Trim();

        return forGrpc ? normalized.ToLowerInvariant() : normalized;
    }
}
