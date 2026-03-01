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

        string path = new CSharpDbConnectionStringBuilder(_connectionString).DataSource;
        if (string.IsNullOrWhiteSpace(path))
            throw new InvalidOperationException("Data Source is required in the connection string.");

        try
        {
            _database = await Engine.Database.OpenAsync(path, cancellationToken);
            _state = ConnectionState.Open;
        }
        catch (Core.CSharpDbException ex)
        {
            throw new CSharpDbDataException(ex);
        }
    }

    public override void Close()
        => CloseAsync().GetAwaiter().GetResult();

    public override async Task CloseAsync()
    {
        if (_state == ConnectionState.Closed) return;

        if (_currentTransaction is not null)
        {
            await _currentTransaction.DisposeAsync();
            _currentTransaction = null;
        }

        if (_database is not null)
        {
            await _database.DisposeAsync();
            _database = null;
        }

        _state = ConnectionState.Closed;
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

    // ─── Internal access ─────────────────────────────────────────────

    internal Database GetDatabase()
    {
        if (_database is null || _state != ConnectionState.Open)
            throw new InvalidOperationException("Connection is not open.");
        return _database;
    }

    internal void ClearTransaction() => _currentTransaction = null;

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
}
