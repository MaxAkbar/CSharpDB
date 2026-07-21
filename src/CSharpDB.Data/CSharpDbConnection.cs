using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using CSharpDB.Client;
using CSharpDB.Primitives;
using CSharpDB.Engine;

namespace CSharpDB.Data;

public sealed class CSharpDbConnection : DbConnection
{
    private const string PrivateMemoryDataSource = ":memory:";
    private const string MemoryDataSourcePrefix = ":memory:";

    // Keep prepared plans only as long as the caller's connection-string object
    // remains live. This avoids an unbounded process-wide cache while still
    // covering the normal ADO.NET pattern of reusing one configured string.
    private static readonly ConditionalWeakTable<string, SharedPooledOpenPlan>
        s_sharedPooledOpenPlans = new();

    private string _connectionString = "";
    private CSharpDbConnectionStringBuilder? _connectionStringBuilder;
    private ConnectionOpenPlan? _connectionOpenPlan;
    private SharedPooledOpenPlan? _sharedPooledOpenPlan;
    private DatabaseOptions? _directDatabaseOptions;
    private HybridDatabaseOptions? _hybridDatabaseOptions;
    private ConnectionState _state = ConnectionState.Closed;
    private ICSharpDbSession? _session;
    private CSharpDbTransaction? _currentTransaction;

    internal HttpClient? TransportHttpClient { get; set; }

    public CSharpDbConnection() { }

    public CSharpDbConnection(string connectionString)
    {
        _connectionString = connectionString;
    }

    public CSharpDbConnection(string connectionString, DatabaseOptions directDatabaseOptions)
        : this(connectionString)
    {
        DirectDatabaseOptions = directDatabaseOptions ?? throw new ArgumentNullException(nameof(directDatabaseOptions));
    }

    public CSharpDbConnection(
        string connectionString,
        DatabaseOptions? directDatabaseOptions,
        HybridDatabaseOptions? hybridDatabaseOptions)
        : this(connectionString)
    {
        DirectDatabaseOptions = directDatabaseOptions;
        HybridDatabaseOptions = hybridDatabaseOptions;
    }

    internal CSharpDbConnection(string connectionString, HttpClient? transportHttpClient)
    {
        _connectionString = connectionString;
        TransportHttpClient = transportHttpClient;
    }

    [AllowNull]
    public override string ConnectionString
    {
        get => _connectionString;
        set
        {
            _connectionString = value ?? "";
            _connectionStringBuilder = null;
            _connectionOpenPlan = null;
            _sharedPooledOpenPlan = null;
        }
    }

    public override string Database => "";
    public override string DataSource
    {
        get
        {
            CSharpDbConnectionStringBuilder builder = GetConnectionStringBuilder();
            return string.IsNullOrWhiteSpace(builder.DataSource)
                ? builder.Endpoint
                : builder.DataSource;
        }
    }
    public override string ServerVersion => "1.0";
    public override ConnectionState State => _state;

    public DatabaseOptions? DirectDatabaseOptions
    {
        get => _directDatabaseOptions;
        set
        {
            _directDatabaseOptions = value;
            _connectionOpenPlan = null;
            _sharedPooledOpenPlan = null;
        }
    }

    public HybridDatabaseOptions? HybridDatabaseOptions
    {
        get => _hybridDatabaseOptions;
        set
        {
            _hybridDatabaseOptions = value;
            _connectionOpenPlan = null;
            _sharedPooledOpenPlan = null;
        }
    }

    public override DataTable GetSchema()
        => GetSchema(DbMetaDataCollectionNames.MetaDataCollections, null);

    public override DataTable GetSchema(string collectionName)
    {
        ArgumentNullException.ThrowIfNull(collectionName);
        return GetSchema(collectionName, null);
    }

    public override DataTable GetSchema(string collectionName, string?[]? restrictionValues)
    {
        ArgumentNullException.ThrowIfNull(collectionName);
        return CSharpDbSchemaProvider.GetSchema(this, collectionName, restrictionValues);
    }

    // ─── Open / Close ────────────────────────────────────────────────

    public override void Open()
        => OpenAsync(CancellationToken.None).GetAwaiter().GetResult();

    public override async Task OpenAsync(CancellationToken cancellationToken)
    {
        if (_state == ConnectionState.Open)
            throw new InvalidOperationException("Connection is already open.");

        try
        {
            SharedPooledOpenPlan? sharedPooledPlan = GetSharedPooledOpenPlan();
            if (sharedPooledPlan is not null)
            {
                _session = await OpenPreparedPooledSessionAsync(
                    sharedPooledPlan,
                    cancellationToken);
            }
            else
            {
                ConnectionOpenPlan plan = GetConnectionOpenPlan();
                TryCacheSharedPooledOpenPlan(plan);
                _session = await OpenConfiguredSessionAsync(plan, cancellationToken);
            }

            _state = ConnectionState.Open;
        }
        catch (CSharpDbException ex)
        {
            _session = null;
            _state = ConnectionState.Closed;
            throw new CSharpDbDataException(ex);
        }
        catch
        {
            _session = null;
            _state = ConnectionState.Closed;
            throw;
        }
    }

    public override void Close()
        => CloseAsync().GetAwaiter().GetResult();

    public override async Task CloseAsync()
    {
        if (_state == ConnectionState.Closed) return;

        try
        {
            if (_currentTransaction is not null)
            {
                await _currentTransaction.DisposeAsync();
                _currentTransaction = null;
            }

            var session = _session;
            _session = null;

            if (session is null)
                return;

            await session.DisposeAsync();
        }
        finally
        {
            _state = ConnectionState.Closed;
        }
    }

    // ─── Transactions ────────────────────────────────────────────────

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        var task = BeginDbTransactionAsync(isolationLevel, CancellationToken.None);
        return task.IsCompletedSuccessfully
            ? task.Result
            : task.AsTask().GetAwaiter().GetResult();
    }

    protected override async ValueTask<DbTransaction> BeginDbTransactionAsync(
        IsolationLevel isolationLevel, CancellationToken cancellationToken)
    {
        if (_state != ConnectionState.Open)
            throw new InvalidOperationException("Connection is not open.");
        if (_currentTransaction is not null)
            throw new InvalidOperationException("A transaction is already active.");

        try
        {
            await GetSession().BeginTransactionAsync(cancellationToken);
        }
        catch (CSharpDbException ex)
        {
            throw new CSharpDbDataException(ex);
        }

        _currentTransaction = new CSharpDbTransaction(this, isolationLevel);
        return _currentTransaction;
    }

    // ─── Command ─────────────────────────────────────────────────────

    public new CSharpDbCommand CreateCommand() => new() { Connection = this };

    protected override DbCommand CreateDbCommand() => new CSharpDbCommand { Connection = this };

    public override void ChangeDatabase(string databaseName)
        => throw new NotSupportedException("CSharpDB does not support ChangeDatabase.");

    public static void ClearPool(string connectionString)
        => ClearPoolAsync(connectionString).GetAwaiter().GetResult();

    public static async ValueTask ClearPoolAsync(string connectionString)
    {
        var builder = new CSharpDbConnectionStringBuilder(connectionString);
        if (string.IsNullOrWhiteSpace(builder.DataSource))
            return;

        var target = ParseTarget(builder.DataSource);
        switch (target.Kind)
        {
            case ConnectionTargetKind.File:
                await CSharpDbConnectionPoolRegistry.ClearPoolsAsync(
                    key => string.Equals(key.DataSource, target.Key, StringComparison.Ordinal));
                break;
            case ConnectionTargetKind.NamedSharedMemory:
                await SharedMemoryDatabaseRegistry.ClearAsync(target.Key);
                break;
        }
    }

    public static void ClearAllPools()
        => ClearAllPoolsAsync().GetAwaiter().GetResult();

    public static async ValueTask ClearAllPoolsAsync()
    {
        await CSharpDbConnectionPoolRegistry.ClearAllAsync();
        await SharedMemoryDatabaseRegistry.ClearAllAsync();
    }

    public async ValueTask SaveToFileAsync(string filePath, CancellationToken cancellationToken = default)
    {
        try
        {
            await GetSession().SaveToFileAsync(filePath, cancellationToken);
        }
        catch (CSharpDbException ex)
        {
            throw new CSharpDbDataException(ex);
        }
    }

    // ─── Internal access ─────────────────────────────────────────────

    internal ICSharpDbSession GetSession()
    {
        if (_session is null || _state != ConnectionState.Open)
            throw new InvalidOperationException("Connection is not open.");
        return _session;
    }

    internal void ClearTransaction() => _currentTransaction = null;

    internal async ValueTask ExecuteTransactionControlAsync(
        string command,
        CancellationToken cancellationToken)
    {
        if (command.Equals("START TRANSACTION", StringComparison.OrdinalIgnoreCase)
            || command.Equals("BEGIN TRANSACTION", StringComparison.OrdinalIgnoreCase))
        {
            await BeginDbTransactionAsync(IsolationLevel.Serializable, cancellationToken);
            return;
        }

        CSharpDbTransaction transaction = _currentTransaction
            ?? throw new InvalidOperationException("No transaction is active.");

        if (command.Equals("COMMIT", StringComparison.OrdinalIgnoreCase))
            await transaction.CommitAsync(cancellationToken);
        else if (command.Equals("ROLLBACK", StringComparison.OrdinalIgnoreCase))
            await transaction.RollbackAsync(cancellationToken);
        else
            throw new ArgumentOutOfRangeException(nameof(command), command, "Unknown transaction control command.");
    }

    internal static int GetPoolCountForTest() => CSharpDbConnectionPoolRegistry.GetPoolCountForTest();
    internal static int GetSharedMemoryHostCountForTest() => SharedMemoryDatabaseRegistry.GetHostCountForTest();
    internal static bool HasSharedPooledOpenPlanForTest(string connectionString)
        => s_sharedPooledOpenPlans.TryGetValue(connectionString, out _);

    internal static int GetIdlePoolSizeForTest(string connectionString)
        => GetIdlePoolSizeForTest(connectionString, directDatabaseOptions: null, hybridDatabaseOptions: null);

    internal static int GetIdlePoolSizeForTest(
        string connectionString,
        DatabaseOptions? directDatabaseOptions,
        HybridDatabaseOptions? hybridDatabaseOptions)
    {
        var builder = new CSharpDbConnectionStringBuilder(connectionString);
        if (!builder.Pooling || string.IsNullOrWhiteSpace(builder.DataSource))
            return 0;

        var target = ParseTarget(builder.DataSource);
        if (target.Kind != ConnectionTargetKind.File)
            return 0;

        ResolvedEmbeddedConfiguration configuration = CSharpDbEmbeddedConfigurationResolver.Resolve(
            builder,
            directDatabaseOptions,
            hybridDatabaseOptions);

        return CSharpDbConnectionPoolRegistry.GetIdleCountForTest(
            CreatePoolKey(target.Key, builder.MaxPoolSize, configuration));
    }

    // ─── Schema introspection ─────────────────────────────────────

    public IReadOnlyCollection<string> GetTableNames()
        => GetSession().GetTableNames();

    public TableSchema? GetTableSchema(string tableName)
        => GetSession().GetTableSchema(tableName);

    public IReadOnlyCollection<IndexSchema> GetIndexes()
        => GetSession().GetIndexes();

    public IReadOnlyCollection<string> GetViewNames()
        => GetSession().GetViewNames();

    public string? GetViewSql(string viewName)
        => GetSession().GetViewSql(viewName);

    public IReadOnlyCollection<TriggerSchema> GetTriggers()
        => GetSession().GetTriggers();

    // ─── Dispose ─────────────────────────────────────────────────────

    protected override void Dispose(bool disposing)
    {
        if (disposing && _state == ConnectionState.Open)
            Close();
        base.Dispose(disposing);
    }

    public override async ValueTask DisposeAsync()
    {
        if (_state == ConnectionState.Open)
            await CloseAsync();
    }

    private async ValueTask<ICSharpDbSession> OpenConfiguredSessionAsync(
        ConnectionOpenPlan plan,
        CancellationToken cancellationToken)
    {
        CSharpDbConnectionStringBuilder builder = plan.Builder;
        CSharpDbTransport? configuredTransport = plan.ConfiguredTransport;
        bool hasEmbeddedTuning = plan.HasEmbeddedTuning;

        if (!string.IsNullOrWhiteSpace(builder.Endpoint))
        {
            if (builder.AdaptiveQueryReoptimization)
            {
                throw new InvalidOperationException(
                    "Adaptive Query Reoptimization is only supported for direct embedded connections; enable it on the remote host instead.");
            }

            if (hasEmbeddedTuning)
            {
                throw new InvalidOperationException(
                    "Embedded storage tuning is only supported for direct embedded connections.");
            }

            if (configuredTransport is null)
            {
                throw new InvalidOperationException(
                    "Transport is required when Endpoint is specified. Use values like 'Grpc' or 'Http'.");
            }

            if (configuredTransport == CSharpDbTransport.Direct)
                throw new InvalidOperationException("Use Data Source for direct embedded CSharpDB connections.");

            return await OpenRemoteSessionAsync(builder, configuredTransport.Value, cancellationToken);
        }

        if (configuredTransport is not null && configuredTransport != CSharpDbTransport.Direct)
        {
            if (hasEmbeddedTuning)
            {
                throw new InvalidOperationException(
                    "Embedded storage tuning is only supported for direct embedded connections.");
            }

            throw new InvalidOperationException("Endpoint is required for non-direct transports.");
        }

        if (string.IsNullOrWhiteSpace(builder.DataSource))
            throw new InvalidOperationException("Data Source is required in the connection string.");

        EmbeddedOpenPlan embeddedPlan = plan.Embedded
            ?? throw new InvalidOperationException("The embedded connection configuration is unavailable.");
        return await OpenSessionAsync(embeddedPlan, builder, cancellationToken);
    }

    private static async ValueTask<ICSharpDbSession> OpenSessionAsync(
        EmbeddedOpenPlan plan,
        CSharpDbConnectionStringBuilder builder,
        CancellationToken cancellationToken)
    {
        return plan.Target.Kind switch
        {
            ConnectionTargetKind.File => await OpenFileSessionAsync(
                plan.Target.Key,
                builder,
                plan.Configuration,
                plan.PoolKey,
                cancellationToken),
            ConnectionTargetKind.PrivateMemory => await OpenPrivateMemorySessionAsync(
                builder.LoadFrom,
                plan.Configuration,
                cancellationToken),
            ConnectionTargetKind.NamedSharedMemory => await SharedMemoryDatabaseRegistry.OpenSessionAsync(
                plan.Target.Key,
                NormalizeOptionalFilePath(builder.LoadFrom),
                cancellationToken),
            _ => throw new InvalidOperationException("Unsupported connection target."),
        };
    }

    private static async ValueTask<ICSharpDbSession> OpenFileSessionAsync(
        string normalizedPath,
        CSharpDbConnectionStringBuilder builder,
        ResolvedEmbeddedConfiguration configuration,
        PoolKey? poolKey,
        CancellationToken cancellationToken)
    {
        if (!string.IsNullOrWhiteSpace(builder.LoadFrom))
            throw new InvalidOperationException("Load From is only supported for in-memory data sources.");

        if (builder.Pooling)
        {
            PoolKey key = poolKey
                ?? throw new InvalidOperationException("The pooled connection configuration is unavailable.");
            return await CSharpDbConnectionPoolRegistry.OpenPooledSessionAsync(
                key,
                ct => OpenEmbeddedDatabaseAsync(normalizedPath, configuration, ct),
                cancellationToken);
        }

        return await CSharpDbConnectionPoolRegistry.OpenDirectSessionAsync(
            normalizedPath,
            ct => OpenEmbeddedDatabaseAsync(normalizedPath, configuration, ct),
            cancellationToken);
    }

    private static async ValueTask<ICSharpDbSession> OpenPreparedPooledSessionAsync(
        SharedPooledOpenPlan plan,
        CancellationToken cancellationToken)
    {
        return await CSharpDbConnectionPoolRegistry.OpenPooledSessionAsync(
            plan.PoolKey,
            ct => OpenEmbeddedDatabaseAsync(plan.NormalizedPath, plan.Configuration, ct),
            cancellationToken);
    }

    private static async ValueTask<Engine.Database> OpenEmbeddedDatabaseAsync(
        string normalizedPath,
        ResolvedEmbeddedConfiguration configuration,
        CancellationToken cancellationToken)
    {
        return configuration.EffectiveHybridDatabaseOptions is null
            ? await Engine.Database.OpenAsync(
                normalizedPath,
                configuration.EffectiveDirectDatabaseOptions,
                cancellationToken)
            : await Engine.Database.OpenHybridAsync(
                normalizedPath,
                configuration.EffectiveDirectDatabaseOptions,
                configuration.EffectiveHybridDatabaseOptions,
                cancellationToken);
    }

    private static async ValueTask<ICSharpDbSession> OpenPrivateMemorySessionAsync(
        string? loadFromPath,
        ResolvedEmbeddedConfiguration configuration,
        CancellationToken cancellationToken)
    {
        Engine.Database database = string.IsNullOrWhiteSpace(loadFromPath)
            ? await Engine.Database.OpenInMemoryAsync(
                configuration.EffectiveDirectDatabaseOptions,
                cancellationToken)
            : await Engine.Database.LoadIntoMemoryAsync(
                NormalizeDataSourcePath(loadFromPath),
                configuration.EffectiveDirectDatabaseOptions,
                cancellationToken);

        return new DirectDatabaseSession(database);
    }

    private async ValueTask<ICSharpDbSession> OpenRemoteSessionAsync(
        CSharpDbConnectionStringBuilder builder,
        CSharpDbTransport transport,
        CancellationToken cancellationToken)
    {
        if (!string.IsNullOrWhiteSpace(builder.DataSource))
            throw new InvalidOperationException("Data Source is not used for daemon-backed connections. Use Endpoint instead.");

        if (!string.IsNullOrWhiteSpace(builder.LoadFrom))
            throw new InvalidOperationException("Load From is only supported for embedded in-memory data sources.");

        if (builder.Pooling)
            throw new InvalidOperationException("Pooling is not supported for daemon-backed connections.");

        string endpoint = builder.Endpoint.Trim();
        ICSharpDbClient client = await OpenClientAsync(new CSharpDbClientOptions
        {
            Transport = transport,
            Endpoint = endpoint,
            HttpClient = TransportHttpClient,
            RouteContext = BuildRouteContext(builder),
        }, cancellationToken);

        return new RemoteDatabaseSession(client);
    }

    private static PoolKey CreatePoolKey(
        string normalizedPath,
        int maxPoolSize,
        ResolvedEmbeddedConfiguration configuration)
    {
        return new PoolKey(
            normalizedPath,
            maxPoolSize,
            configuration.EffectiveOpenMode,
            configuration.EffectiveStoragePreset,
            configuration.EffectiveAdaptiveQueryReoptimization,
            configuration.ExplicitDirectDatabaseOptions,
            configuration.ExplicitHybridDatabaseOptions);
    }

    private CSharpDbConnectionStringBuilder GetConnectionStringBuilder()
        => _connectionStringBuilder ??= new CSharpDbConnectionStringBuilder(_connectionString);

    private SharedPooledOpenPlan? GetSharedPooledOpenPlan()
    {
        if (_sharedPooledOpenPlan is not null)
            return _sharedPooledOpenPlan;

        if (DirectDatabaseOptions is not null || HybridDatabaseOptions is not null)
            return null;

        if (s_sharedPooledOpenPlans.TryGetValue(
                _connectionString,
                out SharedPooledOpenPlan? sharedPooledPlan))
        {
            _sharedPooledOpenPlan = sharedPooledPlan;
        }

        return sharedPooledPlan;
    }

    private void TryCacheSharedPooledOpenPlan(ConnectionOpenPlan plan)
    {
        // Relative paths depend on the process working directory, while explicit
        // option objects participate in pool identity. Keep both on the per-
        // connection plan so sharing cannot freeze or merge those semantics.
        if (DirectDatabaseOptions is not null
            || HybridDatabaseOptions is not null
            || plan.CurrentDirectoryKey is not null
            || plan.Embedded is not
            {
                Target.Kind: ConnectionTargetKind.File,
                PoolKey: { } poolKey,
            } embedded)
        {
            return;
        }

        var candidate = new SharedPooledOpenPlan(
            embedded.Target.Key,
            embedded.Configuration,
            poolKey);
        _sharedPooledOpenPlan = s_sharedPooledOpenPlans.GetValue(
            _connectionString,
            _ => candidate);
    }

    private ConnectionOpenPlan GetConnectionOpenPlan()
    {
        if (_connectionOpenPlan is { CurrentDirectoryKey: null } absoluteOrNonFilePlan)
            return absoluteOrNonFilePlan;

        if (_connectionOpenPlan is { CurrentDirectoryKey: { } relativeDirectory } relativePlan
            && string.Equals(
                relativeDirectory,
                Environment.CurrentDirectory,
                StringComparison.Ordinal))
        {
            return relativePlan;
        }

        CSharpDbConnectionStringBuilder builder = GetConnectionStringBuilder();
        string? currentDirectoryKey = GetCurrentDirectoryCacheKey(builder);
        CSharpDbTransport? configuredTransport = ParseTransportOrNull(builder.Transport);
        bool hasEmbeddedTuning = CSharpDbEmbeddedConfigurationResolver.HasRequestedTuning(
            builder,
            DirectDatabaseOptions,
            HybridDatabaseOptions);
        EmbeddedOpenPlan? embedded = null;

        if (string.IsNullOrWhiteSpace(builder.Endpoint)
            && (configuredTransport is null || configuredTransport == CSharpDbTransport.Direct)
            && !string.IsNullOrWhiteSpace(builder.DataSource))
        {
            ConnectionTarget target = ParseTarget(builder.DataSource, currentDirectoryKey);
            ResolvedEmbeddedConfiguration configuration = CSharpDbEmbeddedConfigurationResolver.Resolve(
                builder,
                DirectDatabaseOptions,
                HybridDatabaseOptions);

            ValidateEmbeddedTuningSupport(target, configuration);
            PoolKey? poolKey = target.Kind == ConnectionTargetKind.File
                && string.IsNullOrWhiteSpace(builder.LoadFrom)
                && builder.Pooling
                ? CreatePoolKey(target.Key, builder.MaxPoolSize, configuration)
                : null;
            embedded = new EmbeddedOpenPlan(target, configuration, poolKey);
        }

        return _connectionOpenPlan = new ConnectionOpenPlan(
            builder,
            configuredTransport,
            hasEmbeddedTuning,
            currentDirectoryKey,
            embedded);
    }

    private static string? GetCurrentDirectoryCacheKey(
        CSharpDbConnectionStringBuilder builder)
    {
        if (!string.IsNullOrWhiteSpace(builder.Endpoint)
            || string.IsNullOrWhiteSpace(builder.DataSource)
            || string.Equals(
                builder.DataSource,
                PrivateMemoryDataSource,
                StringComparison.OrdinalIgnoreCase)
            || builder.DataSource.StartsWith(
                MemoryDataSourcePrefix,
                StringComparison.OrdinalIgnoreCase)
            || Path.IsPathFullyQualified(builder.DataSource))
        {
            return null;
        }

        return Environment.CurrentDirectory;
    }

    private static void ValidateEmbeddedTuningSupport(
        ConnectionTarget target,
        ResolvedEmbeddedConfiguration configuration)
    {
        if (!configuration.HasRequestedTuning)
            return;

        switch (target.Kind)
        {
            case ConnectionTargetKind.File:
                return;
            case ConnectionTargetKind.PrivateMemory when configuration.EffectiveHybridDatabaseOptions is not null:
                throw new InvalidOperationException(
                    "HybridDatabaseOptions and hybrid embedded open modes are only supported for file-backed direct connections.");
            case ConnectionTargetKind.PrivateMemory:
                return;
            case ConnectionTargetKind.NamedSharedMemory:
                throw new InvalidOperationException(
                    "Embedded storage tuning is not supported for named shared-memory databases.");
            default:
                throw new InvalidOperationException("Unsupported connection target.");
        }
    }

    private static async ValueTask<ICSharpDbClient> OpenClientAsync(
        CSharpDbClientOptions options,
        CancellationToken cancellationToken)
    {
        var client = CSharpDbClient.Create(options);
        try
        {
            await client.GetInfoAsync(cancellationToken);
            return client;
        }
        catch
        {
            await client.DisposeAsync();
            throw;
        }
    }

    private static ConnectionTarget ParseTarget(
        string dataSource,
        string? relativeTo = null)
    {
        if (string.Equals(dataSource, PrivateMemoryDataSource, StringComparison.OrdinalIgnoreCase))
            return new ConnectionTarget(ConnectionTargetKind.PrivateMemory, PrivateMemoryDataSource);

        if (dataSource.StartsWith(MemoryDataSourcePrefix, StringComparison.OrdinalIgnoreCase) &&
            dataSource.Length > MemoryDataSourcePrefix.Length)
        {
            string name = dataSource[MemoryDataSourcePrefix.Length..];
            if (string.IsNullOrWhiteSpace(name))
                throw new InvalidOperationException("Named in-memory databases require a non-empty name.");

            return new ConnectionTarget(ConnectionTargetKind.NamedSharedMemory, NormalizeMemoryName(name));
        }

        return new ConnectionTarget(
            ConnectionTargetKind.File,
            NormalizeDataSourcePath(dataSource, relativeTo));
    }

    private static string? NormalizeOptionalFilePath(string? path)
        => string.IsNullOrWhiteSpace(path) ? null : NormalizeDataSourcePath(path);

    private static string NormalizeDataSourcePath(
        string path,
        string? relativeTo = null)
    {
        string fullPath = relativeTo is null
            ? Path.GetFullPath(path)
            : Path.GetFullPath(path, relativeTo);
        return OperatingSystem.IsWindows() ? fullPath.ToUpperInvariant() : fullPath;
    }

    private static string NormalizeMemoryName(string name)
        => OperatingSystem.IsWindows() ? name.ToUpperInvariant() : name;

    private static CSharpDbTransport? ParseTransportOrNull(string value)
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
            _ => throw new InvalidOperationException(
                $"Unsupported Transport '{value}'. Expected Direct, Http, Grpc, or NamedPipes."),
        };
    }

    private static CSharpDbRouteContext? BuildRouteContext(CSharpDbConnectionStringBuilder builder)
    {
        bool hasKeyspace = !string.IsNullOrWhiteSpace(builder.ShardKeyspace);
        bool hasKey = !string.IsNullOrWhiteSpace(builder.ShardKey);
        if (!hasKeyspace && !hasKey)
            return null;

        if (!hasKeyspace || !hasKey)
            throw new InvalidOperationException("Both 'Shard Keyspace' and 'Shard Key' are required for sharded CSharpDB connections.");

        return new CSharpDbRouteContext
        {
            Keyspace = builder.ShardKeyspace.Trim(),
            Key = builder.ShardKey.Trim(),
        };
    }

    private enum ConnectionTargetKind
    {
        File,
        PrivateMemory,
        NamedSharedMemory,
    }

    private readonly record struct ConnectionTarget(ConnectionTargetKind Kind, string Key);

    private sealed record ConnectionOpenPlan(
        CSharpDbConnectionStringBuilder Builder,
        CSharpDbTransport? ConfiguredTransport,
        bool HasEmbeddedTuning,
        string? CurrentDirectoryKey,
        EmbeddedOpenPlan? Embedded);

    private sealed record EmbeddedOpenPlan(
        ConnectionTarget Target,
        ResolvedEmbeddedConfiguration Configuration,
        PoolKey? PoolKey);

    private sealed record SharedPooledOpenPlan(
        string NormalizedPath,
        ResolvedEmbeddedConfiguration Configuration,
        PoolKey PoolKey);
}
