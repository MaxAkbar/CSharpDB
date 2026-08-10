using CSharpDB.Client;
using CSharpDB.Engine;
using CSharpDB.Observability;
using Microsoft.Extensions.Configuration;
using ClientTransport = CSharpDB.Client.CSharpDbTransport;

namespace CSharpDB.Daemon.Configuration;

public enum DaemonHostOpenMode
{
    HybridIncrementalDurable = 0,
    Direct = 1,
}

public sealed class DaemonHostDatabaseOptions
{
    public DaemonHostOpenMode OpenMode { get; init; } = DaemonHostOpenMode.HybridIncrementalDurable;

    public ImplicitInsertExecutionMode ImplicitInsertExecutionMode { get; init; } =
        ImplicitInsertExecutionMode.ConcurrentWriteTransactions;

    public bool UseWriteOptimizedPreset { get; init; } = true;

    public string[] HotTableNames { get; init; } = [];

    public string[] HotCollectionNames { get; init; } = [];
}

internal static class DaemonClientOptionsBuilder
{
    private const string FallbackConnectionString = "Data Source=csharpdb.db";

    public static DaemonHostDatabaseOptions BindHostDatabaseOptions(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var bound = configuration.GetSection("CSharpDB:HostDatabase").Get<DaemonHostDatabaseOptions>()
            ?? new DaemonHostDatabaseOptions();

        return new DaemonHostDatabaseOptions
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
        DaemonHostDatabaseOptions hostDatabaseOptions,
        CSharpDbObservabilityOptions observabilityOptions)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(hostDatabaseOptions);
        ArgumentNullException.ThrowIfNull(observabilityOptions);

        return new CSharpDbClientOptions
        {
            Transport = ClientTransport.Direct,
            ConnectionString = configuration.GetConnectionString("CSharpDB") ?? FallbackConnectionString,
            DirectDatabaseOptions = BuildDirectDatabaseOptions(hostDatabaseOptions, observabilityOptions),
            HybridDatabaseOptions = hostDatabaseOptions.OpenMode == DaemonHostOpenMode.HybridIncrementalDurable
                ? BuildHybridDatabaseOptions(hostDatabaseOptions)
                : null,
        };
    }

    private static DatabaseOptions BuildDirectDatabaseOptions(
        DaemonHostDatabaseOptions hostDatabaseOptions,
        CSharpDbObservabilityOptions observabilityOptions)
    {
        var options = new DatabaseOptions
        {
            ImplicitInsertExecutionMode = hostDatabaseOptions.ImplicitInsertExecutionMode,
            ObservabilityOptions = observabilityOptions,
        };

        if (!hostDatabaseOptions.UseWriteOptimizedPreset)
            return options;

        return options.ConfigureStorageEngine(builder => builder.UseWriteOptimizedPreset());
    }

    private static HybridDatabaseOptions BuildHybridDatabaseOptions(DaemonHostDatabaseOptions hostDatabaseOptions)
    {
        return new HybridDatabaseOptions
        {
            PersistenceMode = HybridPersistenceMode.IncrementalDurable,
            HotTableNames = hostDatabaseOptions.HotTableNames,
            HotCollectionNames = hostDatabaseOptions.HotCollectionNames,
        };
    }
}
