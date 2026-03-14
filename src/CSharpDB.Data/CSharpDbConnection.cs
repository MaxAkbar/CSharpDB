using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using CSharpDB.Primitives;
using CSharpDB.Engine;

namespace CSharpDB.Data;

public sealed class CSharpDbConnection : DbConnection
{
    private const string PrivateMemoryDataSource = ":memory:";
    private const string MemoryDataSourcePrefix = ":memory:";

    private string _connectionString = "";
    private ConnectionState _state = ConnectionState.Closed;
    private ICSharpDbSession? _session;
    private CSharpDbTransaction? _currentTransaction;

    public CSharpDbConnection() { }

    public CSharpDbConnection(string connectionString)
    {
        _connectionString = connectionString;
    }

    [AllowNull]
    public override string ConnectionString
    {
        get => _connectionString;
        set => _connectionString = value ?? "";
    }

    public override string Database => "";
    public override string DataSource => new CSharpDbConnectionStringBuilder(_connectionString).DataSource;
    public override string ServerVersion => "1.0";
    public override ConnectionState State => _state;

    // ─── Open / Close ────────────────────────────────────────────────

    public override void Open()
        => OpenAsync(CancellationToken.None).GetAwaiter().GetResult();

    public override async Task OpenAsync(CancellationToken cancellationToken)
    {
        if (_state == ConnectionState.Open)
            throw new InvalidOperationException("Connection is already open.");

        var builder = new CSharpDbConnectionStringBuilder(_connectionString);
        if (string.IsNullOrWhiteSpace(builder.DataSource))
            throw new InvalidOperationException("Data Source is required in the connection string.");

        try
        {
            var target = ParseTarget(builder.DataSource);
            _session = await OpenSessionAsync(target, builder, cancellationToken);
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

    protected override DbCommand CreateDbCommand() => new CSharpDbCommand { Connection = this };

    public override void ChangeDatabase(string databaseName)
        => throw new NotSupportedException("CSharpDB is a single-file database.");

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
            case ConnectionTargetKind.File when builder.Pooling:
                await CSharpDbConnectionPoolRegistry.ClearPoolAsync(new PoolKey(target.Key, builder.MaxPoolSize));
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

    internal static int GetPoolCountForTest() => CSharpDbConnectionPoolRegistry.GetPoolCountForTest();
    internal static int GetSharedMemoryHostCountForTest() => SharedMemoryDatabaseRegistry.GetHostCountForTest();

    internal static int GetIdlePoolSizeForTest(string connectionString)
    {
        var builder = new CSharpDbConnectionStringBuilder(connectionString);
        if (!builder.Pooling || string.IsNullOrWhiteSpace(builder.DataSource))
            return 0;

        var target = ParseTarget(builder.DataSource);
        if (target.Kind != ConnectionTargetKind.File)
            return 0;

        return CSharpDbConnectionPoolRegistry.GetIdleCountForTest(new PoolKey(target.Key, builder.MaxPoolSize));
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

    private static async ValueTask<ICSharpDbSession> OpenSessionAsync(
        ConnectionTarget target,
        CSharpDbConnectionStringBuilder builder,
        CancellationToken cancellationToken)
    {
        return target.Kind switch
        {
            ConnectionTargetKind.File => await OpenFileSessionAsync(target.Key, builder, cancellationToken),
            ConnectionTargetKind.PrivateMemory => await OpenPrivateMemorySessionAsync(builder.LoadFrom, cancellationToken),
            ConnectionTargetKind.NamedSharedMemory => await SharedMemoryDatabaseRegistry.OpenSessionAsync(
                target.Key,
                NormalizeOptionalFilePath(builder.LoadFrom),
                cancellationToken),
            _ => throw new InvalidOperationException("Unsupported connection target."),
        };
    }

    private static async ValueTask<ICSharpDbSession> OpenFileSessionAsync(
        string normalizedPath,
        CSharpDbConnectionStringBuilder builder,
        CancellationToken cancellationToken)
    {
        if (!string.IsNullOrWhiteSpace(builder.LoadFrom))
            throw new InvalidOperationException("Load From is only supported for in-memory data sources.");

        if (builder.Pooling)
        {
            var key = new PoolKey(normalizedPath, builder.MaxPoolSize);
            var pool = CSharpDbConnectionPoolRegistry.GetOrCreate(key);
            var database = await pool.RentAsync(cancellationToken);
            return new DirectDatabaseSession(database, pool.ReturnAsync);
        }

        return new DirectDatabaseSession(await Engine.Database.OpenAsync(normalizedPath, cancellationToken));
    }

    private static async ValueTask<ICSharpDbSession> OpenPrivateMemorySessionAsync(
        string? loadFromPath,
        CancellationToken cancellationToken)
    {
        Database database = string.IsNullOrWhiteSpace(loadFromPath)
            ? await Engine.Database.OpenInMemoryAsync(cancellationToken)
            : await Engine.Database.LoadIntoMemoryAsync(NormalizeDataSourcePath(loadFromPath), cancellationToken);

        return new DirectDatabaseSession(database);
    }

    private static ConnectionTarget ParseTarget(string dataSource)
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

        return new ConnectionTarget(ConnectionTargetKind.File, NormalizeDataSourcePath(dataSource));
    }

    private static string? NormalizeOptionalFilePath(string? path)
        => string.IsNullOrWhiteSpace(path) ? null : NormalizeDataSourcePath(path);

    private static string NormalizeDataSourcePath(string path)
    {
        string fullPath = Path.GetFullPath(path);
        return OperatingSystem.IsWindows() ? fullPath.ToUpperInvariant() : fullPath;
    }

    private static string NormalizeMemoryName(string name)
        => OperatingSystem.IsWindows() ? name.ToUpperInvariant() : name;

    private enum ConnectionTargetKind
    {
        File,
        PrivateMemory,
        NamedSharedMemory,
    }

    private readonly record struct ConnectionTarget(ConnectionTargetKind Kind, string Key);
}
