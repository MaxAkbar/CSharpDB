using CSharpDB.Client;
using CSharpDB.Engine;
using CSharpDB.Primitives;
using Microsoft.Extensions.Configuration;
using CSharpDbObservabilityOptions = CSharpDB.Observability.CSharpDbObservabilityOptions;

namespace CSharpDB.Admin.Configuration;

public enum AdminHostOpenMode
{
    HybridIncrementalDurable = 0,
    Direct = 1,
}

public sealed class AdminHostDatabaseOptions
{
    public AdminHostOpenMode OpenMode { get; init; } = AdminHostOpenMode.HybridIncrementalDurable;

    public ImplicitInsertExecutionMode ImplicitInsertExecutionMode { get; init; } =
        ImplicitInsertExecutionMode.ConcurrentWriteTransactions;

    public bool UseWriteOptimizedPreset { get; init; } = true;

    public string[] HotTableNames { get; init; } = [];

    public string[] HotCollectionNames { get; init; } = [];
}

public static class AdminClientOptionsBuilder
{
    private const string FallbackConnectionString = "Data Source=csharpdb.db";

    public static AdminHostDatabaseOptions BindHostDatabaseOptions(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var bound = configuration.GetSection("CSharpDB:HostDatabase").Get<AdminHostDatabaseOptions>()
            ?? new AdminHostDatabaseOptions();

        return new AdminHostDatabaseOptions
        {
            OpenMode = bound.OpenMode,
            ImplicitInsertExecutionMode = bound.ImplicitInsertExecutionMode,
            UseWriteOptimizedPreset = bound.UseWriteOptimizedPreset,
            HotTableNames = bound.HotTableNames ?? [],
            HotCollectionNames = bound.HotCollectionNames ?? [],
        };
    }

    public static CSharpDbClientOptions Build(
        IConfiguration configuration,
        AdminHostDatabaseOptions hostDatabaseOptions,
        CSharpDbTransport? transport,
        string? endpoint,
        DbFunctionRegistry? functions = null)
        => Build(
            configuration,
            hostDatabaseOptions,
            transport,
            endpoint,
            functions,
            observabilityOptions: null);

    internal static CSharpDbClientOptions Build(
        IConfiguration configuration,
        AdminHostDatabaseOptions hostDatabaseOptions,
        CSharpDbTransport? transport,
        string? endpoint,
        DbFunctionRegistry? functions,
        CSharpDbObservabilityOptions? observabilityOptions)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(hostDatabaseOptions);

        if (!string.IsNullOrWhiteSpace(endpoint))
        {
            if (transport == CSharpDbTransport.Direct || (transport is null && EndpointLooksLikeDirectPath(endpoint)))
            {
                return BuildDirectEndpoint(endpoint, hostDatabaseOptions, transport, functions, observabilityOptions);
            }

            return new CSharpDbClientOptions
            {
                Transport = transport,
                Endpoint = endpoint,
            };
        }

        if (transport is not null && transport != CSharpDbTransport.Direct)
        {
            return new CSharpDbClientOptions
            {
                Transport = transport,
            };
        }

        return BuildDirectConnectionString(
            configuration.GetConnectionString("CSharpDB") ?? FallbackConnectionString,
            hostDatabaseOptions,
            transport,
            functions,
            observabilityOptions);
    }

    public static CSharpDbClientOptions BuildDirectDataSource(
        string dataSource,
        AdminHostDatabaseOptions hostDatabaseOptions,
        DbFunctionRegistry? functions = null)
        => BuildDirectDataSource(
            dataSource,
            hostDatabaseOptions,
            functions,
            observabilityOptions: null);

    internal static CSharpDbClientOptions BuildDirectDataSource(
        string dataSource,
        AdminHostDatabaseOptions hostDatabaseOptions,
        DbFunctionRegistry? functions,
        CSharpDbObservabilityOptions? observabilityOptions)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(dataSource);
        ArgumentNullException.ThrowIfNull(hostDatabaseOptions);

        return new CSharpDbClientOptions
        {
            Transport = CSharpDbTransport.Direct,
            DataSource = dataSource,
            DirectDatabaseOptions = BuildDirectDatabaseOptions(hostDatabaseOptions, functions, observabilityOptions),
            HybridDatabaseOptions = BuildHybridDatabaseOptionsOrNull(hostDatabaseOptions),
        };
    }

    public static DatabaseOptions BuildDirectDatabaseOptions(
        AdminHostDatabaseOptions hostDatabaseOptions,
        DbFunctionRegistry? functions = null)
        => BuildDirectDatabaseOptions(
            hostDatabaseOptions,
            functions,
            observabilityOptions: null);

    internal static DatabaseOptions BuildDirectDatabaseOptions(
        AdminHostDatabaseOptions hostDatabaseOptions,
        DbFunctionRegistry? functions,
        CSharpDbObservabilityOptions? observabilityOptions)
    {
        ArgumentNullException.ThrowIfNull(hostDatabaseOptions);

        var options = new DatabaseOptions
        {
            Functions = functions ?? DbFunctionRegistry.Empty,
            ImplicitInsertExecutionMode = hostDatabaseOptions.ImplicitInsertExecutionMode,
            ObservabilityOptions = observabilityOptions,
        };

        if (!hostDatabaseOptions.UseWriteOptimizedPreset)
            return options;

        return options.ConfigureStorageEngine(builder => builder.UseWriteOptimizedPreset());
    }

    public static HybridDatabaseOptions? BuildHybridDatabaseOptionsOrNull(AdminHostDatabaseOptions hostDatabaseOptions)
    {
        ArgumentNullException.ThrowIfNull(hostDatabaseOptions);

        if (hostDatabaseOptions.OpenMode != AdminHostOpenMode.HybridIncrementalDurable)
            return null;

        return new HybridDatabaseOptions
        {
            PersistenceMode = HybridPersistenceMode.IncrementalDurable,
            HotTableNames = hostDatabaseOptions.HotTableNames,
            HotCollectionNames = hostDatabaseOptions.HotCollectionNames,
        };
    }

    private static CSharpDbClientOptions BuildDirectConnectionString(
        string connectionString,
        AdminHostDatabaseOptions hostDatabaseOptions,
        CSharpDbTransport? transport,
        DbFunctionRegistry? functions,
        CSharpDbObservabilityOptions? observabilityOptions)
    {
        return new CSharpDbClientOptions
        {
            Transport = transport,
            ConnectionString = connectionString,
            DirectDatabaseOptions = BuildDirectDatabaseOptions(hostDatabaseOptions, functions, observabilityOptions),
            HybridDatabaseOptions = BuildHybridDatabaseOptionsOrNull(hostDatabaseOptions),
        };
    }

    private static CSharpDbClientOptions BuildDirectEndpoint(
        string endpoint,
        AdminHostDatabaseOptions hostDatabaseOptions,
        CSharpDbTransport? transport,
        DbFunctionRegistry? functions,
        CSharpDbObservabilityOptions? observabilityOptions)
    {
        return new CSharpDbClientOptions
        {
            Transport = transport,
            Endpoint = endpoint,
            DirectDatabaseOptions = BuildDirectDatabaseOptions(hostDatabaseOptions, functions, observabilityOptions),
            HybridDatabaseOptions = BuildHybridDatabaseOptionsOrNull(hostDatabaseOptions),
        };
    }

    private static bool EndpointLooksLikeDirectPath(string endpoint)
    {
        if (Uri.TryCreate(endpoint, UriKind.Absolute, out var uri))
            return string.Equals(uri.Scheme, Uri.UriSchemeFile, StringComparison.OrdinalIgnoreCase);

        return !endpoint.Contains("://", StringComparison.Ordinal);
    }
}
