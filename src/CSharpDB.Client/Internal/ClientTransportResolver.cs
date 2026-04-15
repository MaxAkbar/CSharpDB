using CSharpDB.Engine;
using System.Data.Common;

namespace CSharpDB.Client.Internal;

internal static class ClientTransportResolver
{
    private const string PrivateMemoryDataSource = ":memory:";
    private const string MemoryDataSourcePrefix = ":memory:";

    public static ICSharpDbClient Create(CSharpDbClientOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var resolution = Resolve(options);
        return resolution.Transport switch
        {
            CSharpDbTransport.Direct => CreateDirectClient(resolution),
            CSharpDbTransport.Http => new HttpTransportClient(resolution.EndpointUri!, options.HttpClient),
            CSharpDbTransport.Grpc => new GrpcTransportClient(resolution.EndpointUri!, options.HttpClient),
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
            CSharpDbTransport.NamedPipes => ResolveNamedPipes(options, endpointUri),
            _ => throw new CSharpDbClientConfigurationException($"Unsupported transport '{transport}'."),
        };
    }

    private static Resolution ResolveDirect(CSharpDbClientOptions options, string? endpointPath)
    {
        DirectTarget? endpointTarget = endpointPath is null
            ? null
            : new DirectTarget(DirectTargetKind.File, NormalizePath(endpointPath), LoadFromPath: null);

        DirectTarget? dataSourceTarget = string.IsNullOrWhiteSpace(options.DataSource)
            ? null
            : ParseDirectTarget(options.DataSource, loadFromPath: null);

        string? connectionString = string.IsNullOrWhiteSpace(options.ConnectionString)
            ? null
            : options.ConnectionString.Trim();

        DirectTarget? connectionStringTarget = null;
        if (connectionString is not null)
        {
            var builder = new DbConnectionStringBuilder
            {
                ConnectionString = connectionString,
            };

            if (!TryGetDataSource(builder, out string? dataSourceValue) || string.IsNullOrWhiteSpace(dataSourceValue))
                throw new CSharpDbClientConfigurationException("ConnectionString must include a Data Source.");

            connectionStringTarget = ParseDirectTarget(
                dataSourceValue,
                TryGetOptionalValue(builder, "Load From", out string? loadFromValue) ? loadFromValue : null);
        }

        DirectTarget? resolvedTarget = endpointTarget ?? dataSourceTarget ?? connectionStringTarget;
        if (resolvedTarget is null)
            throw new CSharpDbClientConfigurationException("Direct transport requires Endpoint, DataSource, or ConnectionString.");

        if (dataSourceTarget is not null && !DirectTargetsEqual(resolvedTarget, dataSourceTarget))
            throw new CSharpDbClientConfigurationException("DataSource does not match the resolved direct target.");

        if (connectionStringTarget is not null && !DirectTargetsEqual(resolvedTarget, connectionStringTarget))
        {
            throw new CSharpDbClientConfigurationException(
                "ConnectionString Data Source does not match the resolved direct target.");
        }

        if (resolvedTarget.Kind == DirectTargetKind.PrivateMemory && options.HybridDatabaseOptions is not null)
        {
            throw new CSharpDbClientConfigurationException(
                "HybridDatabaseOptions are only supported for file-backed direct transports.");
        }

        if (resolvedTarget.Kind == DirectTargetKind.NamedSharedMemory)
        {
            throw new CSharpDbClientConfigurationException(
                "Named shared in-memory direct targets are not implemented in CSharpDB.Client yet.");
        }

        return new Resolution
        {
            Transport = CSharpDbTransport.Direct,
            DirectTarget = resolvedTarget,
            DirectDatabaseOptions = options.DirectDatabaseOptions,
            HybridDatabaseOptions = options.HybridDatabaseOptions,
        };
    }

    private static Resolution ResolveHttp(CSharpDbClientOptions options, Uri? endpointUri)
    {
        EnsureNoDirectInputs(options, CSharpDbTransport.Http);
        EnsureEndpointScheme(endpointUri, CSharpDbTransport.Http, Uri.UriSchemeHttp, Uri.UriSchemeHttps);

        return new Resolution
        {
            Transport = CSharpDbTransport.Http,
            EndpointUri = endpointUri,
        };
    }

    private static Resolution ResolveGrpc(CSharpDbClientOptions options, Uri? endpointUri)
    {
        EnsureNoDirectInputs(options, CSharpDbTransport.Grpc);
        EnsureEndpointScheme(endpointUri, CSharpDbTransport.Grpc, Uri.UriSchemeHttp, Uri.UriSchemeHttps);

        return new Resolution
        {
            Transport = CSharpDbTransport.Grpc,
            EndpointUri = endpointUri,
        };
    }

    private static Resolution ResolveNamedPipes(CSharpDbClientOptions options, Uri? endpointUri)
    {
        EnsureNoDirectInputs(options, CSharpDbTransport.NamedPipes);
        EnsureEndpointScheme(endpointUri, CSharpDbTransport.NamedPipes, "pipe", "npipe");

        return new Resolution
        {
            Transport = CSharpDbTransport.NamedPipes,
            EndpointUri = endpointUri,
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
        if (options.DirectDatabaseOptions is not null)
        {
            throw new CSharpDbClientConfigurationException(
                $"Transport '{transport}' does not support DirectDatabaseOptions.");
        }

        if (options.HybridDatabaseOptions is not null)
        {
            throw new CSharpDbClientConfigurationException(
                $"Transport '{transport}' does not support HybridDatabaseOptions.");
        }

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

    private static bool TryGetOptionalValue(DbConnectionStringBuilder builder, string key, out string? value)
    {
        foreach (string existingKey in builder.Keys.Cast<string>())
        {
            if (!existingKey.Equals(key, StringComparison.OrdinalIgnoreCase))
                continue;

            value = Convert.ToString(builder[existingKey], System.Globalization.CultureInfo.InvariantCulture);
            return true;
        }

        value = null;
        return false;
    }

    private static ICSharpDbClient CreateDirectClient(Resolution resolution)
    {
        DirectTarget directTarget = resolution.DirectTarget
            ?? throw new CSharpDbClientConfigurationException("Direct transport requires a resolved direct target.");

        return directTarget.Kind switch
        {
            DirectTargetKind.File => new EngineTransportClient(
                directTarget.Key,
                resolution.DirectDatabaseOptions,
                resolution.HybridDatabaseOptions),
            DirectTargetKind.PrivateMemory => new EngineTransportClient(
                directTarget.DisplayName,
                CreatePrivateMemoryOpenDatabaseAsync(directTarget.LoadFromPath, resolution.DirectDatabaseOptions),
                resolution.DirectDatabaseOptions,
                hybridDatabaseOptions: null),
            _ => throw new CSharpDbClientConfigurationException(
                $"Direct target kind '{directTarget.Kind}' is not implemented in CSharpDB.Client yet."),
        };
    }

    private static Func<string, CancellationToken, Task<Database>> CreatePrivateMemoryOpenDatabaseAsync(
        string? loadFromPath,
        DatabaseOptions? directDatabaseOptions)
    {
        var options = directDatabaseOptions ?? new DatabaseOptions();
        return string.IsNullOrWhiteSpace(loadFromPath)
            ? (_, ct) => Database.OpenInMemoryAsync(options, ct).AsTask()
            : (_, ct) => Database.LoadIntoMemoryAsync(loadFromPath, options, ct).AsTask();
    }

    private static DirectTarget ParseDirectTarget(string dataSource, string? loadFromPath)
    {
        if (string.Equals(dataSource, PrivateMemoryDataSource, StringComparison.OrdinalIgnoreCase))
        {
            return new DirectTarget(
                DirectTargetKind.PrivateMemory,
                PrivateMemoryDataSource,
                NormalizeOptionalPath(loadFromPath));
        }

        if (dataSource.StartsWith(MemoryDataSourcePrefix, StringComparison.OrdinalIgnoreCase) &&
            dataSource.Length > MemoryDataSourcePrefix.Length)
        {
            string name = dataSource[MemoryDataSourcePrefix.Length..];
            if (string.IsNullOrWhiteSpace(name))
                throw new CSharpDbClientConfigurationException("Named in-memory databases require a non-empty name.");

            return new DirectTarget(
                DirectTargetKind.NamedSharedMemory,
                dataSource,
                NormalizeOptionalPath(loadFromPath));
        }

        if (!string.IsNullOrWhiteSpace(loadFromPath))
        {
            throw new CSharpDbClientConfigurationException(
                "Load From is only supported for in-memory direct targets.");
        }

        return new DirectTarget(DirectTargetKind.File, NormalizePath(dataSource), LoadFromPath: null);
    }

    private static string? NormalizeOptionalPath(string? path)
        => string.IsNullOrWhiteSpace(path) ? null : NormalizePath(path);

    private static bool DirectTargetsEqual(DirectTarget left, DirectTarget right)
        => left.Kind == right.Kind
            && string.Equals(left.Key, right.Key, OperatingSystem.IsWindows()
                ? StringComparison.OrdinalIgnoreCase
                : StringComparison.Ordinal)
            && PathsEqualNullable(left.LoadFromPath, right.LoadFromPath);

    private static bool PathsEqualNullable(string? left, string? right)
    {
        if (left is null || right is null)
            return left is null && right is null;

        return PathsEqual(left, right);
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
        public DirectTarget? DirectTarget { get; init; }
        public Uri? EndpointUri { get; init; }
        public DatabaseOptions? DirectDatabaseOptions { get; init; }
        public HybridDatabaseOptions? HybridDatabaseOptions { get; init; }
    }

    private enum DirectTargetKind
    {
        File,
        PrivateMemory,
        NamedSharedMemory,
    }

    private sealed record DirectTarget(DirectTargetKind Kind, string Key, string? LoadFromPath)
    {
        public string DisplayName => Key;
    }
}
