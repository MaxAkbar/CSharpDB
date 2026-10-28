namespace CSharpDB.Client;

/// <summary>Controls the HTTP protocol used by the gRPC transport.</summary>
public enum CSharpDbGrpcMode
{
    /// <summary>
    /// Preserves compatibility: plain HTTP uses gRPC-Web over HTTP/1.1 unless
    /// a custom HttpClient is supplied; HTTPS uses native gRPC.
    /// </summary>
    Auto = 0,

    /// <summary>
    /// Uses native gRPC and requires HTTP/2, including on a plain HTTP endpoint.
    /// An unencrypted server endpoint must be configured for HTTP/2 only.
    /// </summary>
    Native = 1,
}
