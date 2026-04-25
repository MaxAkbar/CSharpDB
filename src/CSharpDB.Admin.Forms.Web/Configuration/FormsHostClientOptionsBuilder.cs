using CSharpDB.Client;
using CSharpDB.Engine;
using Microsoft.Extensions.Configuration;

namespace CSharpDB.Admin.Forms.Web.Configuration;

public enum FormsHostOpenMode
{
    HybridIncrementalDurable = 0,
    Direct = 1,
}

public sealed class FormsHostDatabaseOptions
{
    public FormsHostOpenMode OpenMode { get; init; } = FormsHostOpenMode.HybridIncrementalDurable;

    public ImplicitInsertExecutionMode ImplicitInsertExecutionMode { get; init; } =
        ImplicitInsertExecutionMode.ConcurrentWriteTransactions;

    public bool UseWriteOptimizedPreset { get; init; } = true;

    public string[] HotTableNames { get; init; } = [];

    public string[] HotCollectionNames { get; init; } = [];
}

public static class FormsHostClientOptionsBuilder
{
    private const string FallbackDataSource = "csharpdb.db";

    public static FormsHostDatabaseOptions BindHostDatabaseOptions(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        FormsHostDatabaseOptions bound = configuration.GetSection("CSharpDB:HostDatabase").Get<FormsHostDatabaseOptions>()
            ?? new FormsHostDatabaseOptions();

        return new FormsHostDatabaseOptions
        {
            OpenMode = bound.OpenMode,
            ImplicitInsertExecutionMode = bound.ImplicitInsertExecutionMode,
            UseWriteOptimizedPreset = bound.UseWriteOptimizedPreset,
            HotTableNames = bound.HotTableNames ?? [],
            HotCollectionNames = bound.HotCollectionNames ?? [],
        };
    }

    public static CSharpDbClientOptions Build(IConfiguration configuration, FormsHostDatabaseOptions hostDatabaseOptions)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(hostDatabaseOptions);

        string? endpoint = configuration["CSharpDB:Endpoint"];
        string? dataSource = configuration["CSharpDB:DataSource"];
        CSharpDbTransport? transport = ParseTransport(configuration["CSharpDB:Transport"]);
        string? connectionString = configuration.GetConnectionString("CSharpDB");

        if (!string.IsNullOrWhiteSpace(endpoint))
        {
            if (transport == CSharpDbTransport.Direct || (transport is null && EndpointLooksLikeDirectPath(endpoint)))
                return BuildDirectDataSource(NormalizeDirectPath(endpoint), hostDatabaseOptions);

            return new CSharpDbClientOptions
            {
                Transport = transport,
                Endpoint = endpoint,
            };
        }

        if (!string.IsNullOrWhiteSpace(dataSource))
            return BuildDirectDataSource(dataSource, hostDatabaseOptions);

        if (!string.IsNullOrWhiteSpace(connectionString))
        {
            return new CSharpDbClientOptions
            {
                Transport = transport ?? CSharpDbTransport.Direct,
                ConnectionString = connectionString,
                DirectDatabaseOptions = BuildDirectDatabaseOptions(hostDatabaseOptions),
                HybridDatabaseOptions = BuildHybridDatabaseOptionsOrNull(hostDatabaseOptions),
            };
        }

        if (transport is not null && transport != CSharpDbTransport.Direct)
            return new CSharpDbClientOptions { Transport = transport };

        return BuildDirectDataSource(FallbackDataSource, hostDatabaseOptions);
    }

    public static CSharpDbClientOptions BuildDirectDataSource(string dataSource, FormsHostDatabaseOptions hostDatabaseOptions)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(dataSource);
        ArgumentNullException.ThrowIfNull(hostDatabaseOptions);

        return new CSharpDbClientOptions
        {
            Transport = CSharpDbTransport.Direct,
            DataSource = dataSource,
            DirectDatabaseOptions = BuildDirectDatabaseOptions(hostDatabaseOptions),
            HybridDatabaseOptions = BuildHybridDatabaseOptionsOrNull(hostDatabaseOptions),
        };
    }

    public static DatabaseOptions BuildDirectDatabaseOptions(FormsHostDatabaseOptions hostDatabaseOptions)
    {
        ArgumentNullException.ThrowIfNull(hostDatabaseOptions);

        var options = new DatabaseOptions
        {
            ImplicitInsertExecutionMode = hostDatabaseOptions.ImplicitInsertExecutionMode,
        };

        if (!hostDatabaseOptions.UseWriteOptimizedPreset)
            return options;

        return options.ConfigureStorageEngine(builder => builder.UseWriteOptimizedPreset());
    }

    public static HybridDatabaseOptions? BuildHybridDatabaseOptionsOrNull(FormsHostDatabaseOptions hostDatabaseOptions)
    {
        ArgumentNullException.ThrowIfNull(hostDatabaseOptions);

        if (hostDatabaseOptions.OpenMode != FormsHostOpenMode.HybridIncrementalDurable)
            return null;

        return new HybridDatabaseOptions
        {
            PersistenceMode = HybridPersistenceMode.IncrementalDurable,
            HotTableNames = hostDatabaseOptions.HotTableNames,
            HotCollectionNames = hostDatabaseOptions.HotCollectionNames,
        };
    }

    private static CSharpDbTransport? ParseTransport(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        return value.Trim().ToLowerInvariant() switch
        {
            "direct" => CSharpDbTransport.Direct,
            "http" => CSharpDbTransport.Http,
            "grpc" => CSharpDbTransport.Grpc,
            "namedpipes" => CSharpDbTransport.NamedPipes,
            "named-pipes" => CSharpDbTransport.NamedPipes,
            "npipe" => CSharpDbTransport.NamedPipes,
            "pipe" => CSharpDbTransport.NamedPipes,
            _ => throw new InvalidOperationException($"Unsupported transport '{value}'."),
        };
    }

    private static bool EndpointLooksLikeDirectPath(string endpoint)
    {
        if (Uri.TryCreate(endpoint, UriKind.Absolute, out Uri? uri))
            return string.Equals(uri.Scheme, Uri.UriSchemeFile, StringComparison.OrdinalIgnoreCase);

        return !endpoint.Contains("://", StringComparison.Ordinal);
    }

    private static string NormalizeDirectPath(string endpoint)
    {
        if (Uri.TryCreate(endpoint, UriKind.Absolute, out Uri? uri) &&
            string.Equals(uri.Scheme, Uri.UriSchemeFile, StringComparison.OrdinalIgnoreCase))
        {
            return uri.LocalPath;
        }

        return endpoint;
    }
}
