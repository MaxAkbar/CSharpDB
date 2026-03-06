using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using CSharpDB.Core;
using CSharpDB.Engine;

namespace CSharpDB.Data;

public sealed class CSharpDbConnection : DbConnection
{
    private string _connectionString = "";
    private ConnectionState _state = ConnectionState.Closed;
    private Database? _database;
    private CSharpDbConnectionPool? _pool;
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
        string path = builder.DataSource;
        if (string.IsNullOrWhiteSpace(path))
            throw new InvalidOperationException("Data Source is required in the connection string.");

        string normalizedPath = NormalizeDataSourcePath(path);

        try
        {
            CSharpDbConnectionPool? openedPool = null;
            Database openedDatabase;

            if (builder.Pooling)
            {
                var key = new PoolKey(normalizedPath, builder.MaxPoolSize);
                openedPool = CSharpDbConnectionPoolRegistry.GetOrCreate(key);
                openedDatabase = await openedPool.RentAsync(cancellationToken);
            }
            else
            {
                openedDatabase = await Engine.Database.OpenAsync(normalizedPath, cancellationToken);
            }

            _pool = openedPool;
            _database = openedDatabase;
            _state = ConnectionState.Open;
        }
        catch (Core.CSharpDbException ex)
        {
            _database = null;
            _pool = null;
            _state = ConnectionState.Closed;
            throw new CSharpDbDataException(ex);
        }
        catch
        {
            _database = null;
            _pool = null;
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

            var database = _database;
            var pool = _pool;
            _database = null;
            _pool = null;

            if (database is null)
                return;

            if (pool is null)
                await database.DisposeAsync();
            else
                await pool.ReturnAsync(database);
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
            await _database!.BeginTransactionAsync(cancellationToken);
        }
        catch (Core.CSharpDbException ex)
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
        string path = builder.DataSource;
        if (!builder.Pooling || string.IsNullOrWhiteSpace(path))
            return;

        string normalizedPath = NormalizeDataSourcePath(path);
        var key = new PoolKey(normalizedPath, builder.MaxPoolSize);
        await CSharpDbConnectionPoolRegistry.ClearPoolAsync(key);
    }

    public static void ClearAllPools()
        => ClearAllPoolsAsync().GetAwaiter().GetResult();

    public static ValueTask ClearAllPoolsAsync()
        => CSharpDbConnectionPoolRegistry.ClearAllAsync();

    // ─── Internal access ─────────────────────────────────────────────

    internal Database GetDatabase()
    {
        if (_database is null || _state != ConnectionState.Open)
            throw new InvalidOperationException("Connection is not open.");
        return _database;
    }

    internal void ClearTransaction() => _currentTransaction = null;

    internal static int GetPoolCountForTest() => CSharpDbConnectionPoolRegistry.GetPoolCountForTest();

    internal static int GetIdlePoolSizeForTest(string connectionString)
    {
        var builder = new CSharpDbConnectionStringBuilder(connectionString);
        string path = builder.DataSource;
        if (!builder.Pooling || string.IsNullOrWhiteSpace(path))
            return 0;

        string normalizedPath = NormalizeDataSourcePath(path);
        return CSharpDbConnectionPoolRegistry.GetIdleCountForTest(new PoolKey(normalizedPath, builder.MaxPoolSize));
    }

    // ─── Schema introspection ─────────────────────────────────────

    public IReadOnlyCollection<string> GetTableNames()
        => GetDatabase().GetTableNames();

    public TableSchema? GetTableSchema(string tableName)
        => GetDatabase().GetTableSchema(tableName);

    public IReadOnlyCollection<IndexSchema> GetIndexes()
        => GetDatabase().GetIndexes();

    public IReadOnlyCollection<string> GetViewNames()
        => GetDatabase().GetViewNames();

    public string? GetViewSql(string viewName)
        => GetDatabase().GetViewSql(viewName);

    public IReadOnlyCollection<TriggerSchema> GetTriggers()
        => GetDatabase().GetTriggers();

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

    private static string NormalizeDataSourcePath(string path)
    {
        string fullPath = Path.GetFullPath(path);
        return OperatingSystem.IsWindows() ? fullPath.ToUpperInvariant() : fullPath;
    }
}
