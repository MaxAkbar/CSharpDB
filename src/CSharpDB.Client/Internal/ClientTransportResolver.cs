using System.Data.Common;

namespace CSharpDB.Client.Internal;

internal static class ClientTransportResolver
{
    public static ICSharpDbClient Create(CSharpDbClientOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var resolution = Resolve(options);
        return resolution.Transport switch
        {
            CSharpDbTransport.Direct => new EngineTransportClient(resolution.DatabasePath!),
            CSharpDbTransport.Http => throw CreateNotImplementedTransportException(CSharpDbTransport.Http),
            CSharpDbTransport.Grpc => throw CreateNotImplementedTransportException(CSharpDbTransport.Grpc),
            CSharpDbTransport.Tcp => throw CreateNotImplementedTransportException(CSharpDbTransport.Tcp),
            CSharpDbTransport.NamedPipes => throw CreateNotImplementedTransportException(CSharpDbTransport.NamedPipes),
            _ => throw new CSharpDbClientConfigurationException($"Unsupported transport '{resolution.Transport}'."),
        };
    }

    private static Resolution Resolve(CSharpDbClientOptions options)
    {
        string? endpoint = string.IsNullOrWhiteSpace(options.Endpoint) ? null : options.Endpoint.Trim();
        Uri? endpointUri = null;
        if (endpoint is not null && Uri.TryCreate(endpoint, UriKind.Absolute, out var parsedEndpointUri))
            endpointUri = parsedEndpointUri;

        var transport = options.Transport ?? InferTransport(endpoint, endpointUri);
        return transport switch
        {
            CSharpDbTransport.Direct => ResolveDirect(options, ResolveDirectEndpointPath(endpoint, endpointUri)),
            CSharpDbTransport.Http => ResolveHttp(options, endpointUri),
            CSharpDbTransport.Grpc => ResolveGrpc(options, endpointUri),
            CSharpDbTransport.Tcp => ResolveTcp(options, endpointUri),
            CSharpDbTransport.NamedPipes => ResolveNamedPipes(options, endpointUri),
            _ => throw new CSharpDbClientConfigurationException($"Unsupported transport '{transport}'."),
        };
    }

    private static Resolution ResolveDirect(CSharpDbClientOptions options, string? endpointPath)
    {
        string? dataSource = string.IsNullOrWhiteSpace(options.DataSource)
            ? null
            : NormalizePath(options.DataSource);

        string? connectionString = string.IsNullOrWhiteSpace(options.ConnectionString)
            ? null
            : options.ConnectionString.Trim();

        string? connectionStringPath = null;
        if (connectionString is not null)
        {
            var builder = new DbConnectionStringBuilder
            {
                ConnectionString = connectionString,
            };

            if (!TryGetDataSource(builder, out string? dataSourceValue) || string.IsNullOrWhiteSpace(dataSourceValue))
                throw new CSharpDbClientConfigurationException("ConnectionString must include a Data Source.");

            connectionStringPath = NormalizePath(dataSourceValue);
        }

        string? resolvedPath = endpointPath ?? dataSource ?? connectionStringPath;
        if (resolvedPath is null)
            throw new CSharpDbClientConfigurationException("Direct transport requires Endpoint, DataSource, or ConnectionString.");

        if (dataSource is not null && !PathsEqual(resolvedPath, dataSource))
            throw new CSharpDbClientConfigurationException("DataSource does not match the resolved direct target.");

        if (connectionStringPath is not null && !PathsEqual(resolvedPath, connectionStringPath))
            throw new CSharpDbClientConfigurationException("ConnectionString Data Source does not match the resolved direct target.");

        return new Resolution
        {
            Transport = CSharpDbTransport.Direct,
            DatabasePath = resolvedPath,
        };
    }

    private static Resolution ResolveHttp(CSharpDbClientOptions options, Uri? endpointUri)
    {
        EnsureNoDirectInputs(options, CSharpDbTransport.Http);
        EnsureEndpointScheme(endpointUri, CSharpDbTransport.Http, Uri.UriSchemeHttp, Uri.UriSchemeHttps);

        return new Resolution
        {
            Transport = CSharpDbTransport.Http,
        };
    }

    private static Resolution ResolveGrpc(CSharpDbClientOptions options, Uri? endpointUri)
    {
        EnsureNoDirectInputs(options, CSharpDbTransport.Grpc);
        EnsureEndpointScheme(endpointUri, CSharpDbTransport.Grpc, Uri.UriSchemeHttp, Uri.UriSchemeHttps);

        return new Resolution
        {
            Transport = CSharpDbTransport.Grpc,
        };
    }

    private static Resolution ResolveTcp(CSharpDbClientOptions options, Uri? endpointUri)
    {
        EnsureNoDirectInputs(options, CSharpDbTransport.Tcp);
        EnsureEndpointScheme(endpointUri, CSharpDbTransport.Tcp, "tcp");

        return new Resolution
        {
            Transport = CSharpDbTransport.Tcp,
        };
    }

    private static Resolution ResolveNamedPipes(CSharpDbClientOptions options, Uri? endpointUri)
    {
        EnsureNoDirectInputs(options, CSharpDbTransport.NamedPipes);
        EnsureEndpointScheme(endpointUri, CSharpDbTransport.NamedPipes, "pipe", "npipe");

        return new Resolution
        {
            Transport = CSharpDbTransport.NamedPipes,
        };
    }

    private static CSharpDbTransport InferTransport(string? endpoint, Uri? endpointUri)
    {
        if (endpointUri is not null)
        {
            if (string.Equals(endpointUri.Scheme, Uri.UriSchemeFile, StringComparison.OrdinalIgnoreCase))
                return CSharpDbTransport.Direct;

            if (endpointUri.Scheme is "http" or "https")
                return CSharpDbTransport.Http;

            if (string.Equals(endpointUri.Scheme, "tcp", StringComparison.OrdinalIgnoreCase))
                return CSharpDbTransport.Tcp;

            if (endpointUri.Scheme is "pipe" or "npipe")
                return CSharpDbTransport.NamedPipes;

            throw new CSharpDbClientConfigurationException($"Unsupported endpoint scheme '{endpointUri.Scheme}'.");
        }

        if (endpoint is not null && !LooksLikeFilePath(endpoint))
            throw new CSharpDbClientConfigurationException("Transport could not be inferred from Endpoint. Set Transport explicitly.");

        return CSharpDbTransport.Direct;
    }

    private static string? ResolveDirectEndpointPath(string? endpoint, Uri? endpointUri)
    {
        if (endpoint is null)
            return null;

        if (endpointUri is not null)
        {
            if (string.Equals(endpointUri.Scheme, Uri.UriSchemeFile, StringComparison.OrdinalIgnoreCase))
                return NormalizePath(endpointUri.LocalPath);

            throw new CSharpDbClientConfigurationException("Transport 'Direct' requires a file path or file:// endpoint.");
        }

        if (!LooksLikeFilePath(endpoint))
            throw new CSharpDbClientConfigurationException("Transport 'Direct' requires a file path or file:// endpoint.");

        return NormalizePath(endpoint);
    }

    private static void EnsureNoDirectInputs(CSharpDbClientOptions options, CSharpDbTransport transport)
    {
        if (!string.IsNullOrWhiteSpace(options.DataSource) || !string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            throw new CSharpDbClientConfigurationException(
                $"Transport '{transport}' does not use DataSource or ConnectionString. Use Endpoint instead.");
        }
    }

    private static void EnsureEndpointScheme(Uri? endpointUri, CSharpDbTransport transport, params string[] allowedSchemes)
    {
        if (endpointUri is null)
        {
            throw new CSharpDbClientConfigurationException(
                $"Transport '{transport}' requires an Endpoint with one of: {string.Join(", ", allowedSchemes)}.");
        }

        if (allowedSchemes.Any(scheme => string.Equals(endpointUri.Scheme, scheme, StringComparison.OrdinalIgnoreCase)))
            return;

        throw new CSharpDbClientConfigurationException(
            $"Transport '{transport}' requires an Endpoint with one of: {string.Join(", ", allowedSchemes)}.");
    }

    private static CSharpDbClientConfigurationException CreateNotImplementedTransportException(CSharpDbTransport transport)
        => new($"Transport '{transport}' is not implemented in CSharpDB.Client yet.");

    private static bool TryGetDataSource(DbConnectionStringBuilder builder, out string? dataSource)
    {
        foreach (string key in builder.Keys.Cast<string>())
        {
            if (!key.Equals("Data Source", StringComparison.OrdinalIgnoreCase) &&
                !key.Equals("DataSource", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            dataSource = Convert.ToString(builder[key], System.Globalization.CultureInfo.InvariantCulture);
            return true;
        }

        dataSource = null;
        return false;
    }

    private static bool LooksLikeFilePath(string value)
    {
        if (Path.IsPathRooted(value))
            return true;

        return !value.Contains("://", StringComparison.Ordinal)
            && (value.Contains(Path.DirectorySeparatorChar)
                || value.Contains(Path.AltDirectorySeparatorChar)
                || value.StartsWith(".", StringComparison.Ordinal)
                || value.EndsWith(".db", StringComparison.OrdinalIgnoreCase));
    }

    private static string NormalizePath(string value)
        => Path.GetFullPath(value);

    private static bool PathsEqual(string left, string right)
        => string.Equals(left, right, OperatingSystem.IsWindows()
            ? StringComparison.OrdinalIgnoreCase
            : StringComparison.Ordinal);

    private sealed class Resolution
    {
        public required CSharpDbTransport Transport { get; init; }
        public string? DatabasePath { get; init; }
    }
}
