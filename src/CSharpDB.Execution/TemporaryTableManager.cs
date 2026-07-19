using System.Runtime.CompilerServices;
using CSharpDB.Primitives;
using CSharpDB.Storage.BTrees;
using CSharpDB.Storage.Catalog;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Execution;

internal sealed class TemporaryTableManager : IAsyncDisposable
{
    private readonly StorageEngineOptions _storageOptions;
    private readonly SemaphoreSlim _writeGate = new(1, 1);
    private readonly AsyncLocal<object?> _currentSessionKey = new();
    private readonly Dictionary<object, StorageEngineContext> _sessionContexts = new();
    private StorageEngineContext? _defaultContext;
    private int _contextCount;

    public TemporaryTableManager(StorageEngineOptions storageOptions)
    {
        ArgumentNullException.ThrowIfNull(storageOptions);
        _storageOptions = storageOptions;
    }

    public bool HasAnyTableContext => Volatile.Read(ref _contextCount) != 0;

    internal bool HasCurrentSessionContext => GetCurrentContextOrNull() is not null;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool HasTable(string tableName)
    {
        if (!HasAnyTableContext)
            return false;

        return GetCurrentContextOrNull()?.Catalog.GetTable(tableName) is not null;
    }

    public bool TryGetTable(string tableName, out TableSchema schema)
    {
        if (GetCurrentContextOrNull()?.Catalog.GetTable(tableName) is { } found)
        {
            schema = found;
            return true;
        }

        schema = null!;
        return false;
    }

    public IReadOnlyCollection<string> GetTableNames() =>
        GetCurrentContextOrNull()?.Catalog.GetTableNames() ?? Array.Empty<string>();

    public BTree GetTableTree(string tableName)
    {
        StorageEngineContext context = GetRequiredContext();
        return context.Catalog.GetTableTree(tableName, context.Pager);
    }

    public bool TryGetExactTableRowCount(string tableName, out long rowCount)
    {
        if (GetCurrentContextOrNull() is { } context)
            return context.Catalog.TryGetExactTableRowCount(tableName, out rowCount);

        rowCount = 0;
        return false;
    }

    public IDisposable EnterSessionScope(object sessionKey)
    {
        ArgumentNullException.ThrowIfNull(sessionKey);
        object? previous = _currentSessionKey.Value;
        _currentSessionKey.Value = sessionKey;
        return new SessionScope(this, previous);
    }

    public async ValueTask CreateTableAsync(TableSchema schema, CancellationToken ct)
    {
        await _writeGate.WaitAsync(ct);
        try
        {
            StorageEngineContext context = await EnsureContextAsync(ct);
            await ExecuteWriteAsync(
                context,
                async token =>
                {
                    await context.Catalog.CreateTableAsync(schema, token);
                    await context.Catalog.PersistRootPageChangesAsync(schema.TableName, token);
                },
                ct);
        }
        finally
        {
            _writeGate.Release();
        }
    }

    public async ValueTask DropTableAsync(string tableName, CancellationToken ct)
    {
        await _writeGate.WaitAsync(ct);
        try
        {
            StorageEngineContext? context = GetCurrentContextOrNull();
            if (context is null)
                throw new CSharpDbException(ErrorCode.TableNotFound, $"Temporary table '{tableName}' not found.");

            await ExecuteWriteAsync(
                context,
                async token => await context.Catalog.DropTableAsync(tableName, token),
                ct);
        }
        finally
        {
            _writeGate.Release();
        }
    }

    public async ValueTask ExecuteWriteAsync(Func<StorageEngineContext, CancellationToken, ValueTask> action, CancellationToken ct)
    {
        await _writeGate.WaitAsync(ct);
        try
        {
            StorageEngineContext context = await EnsureContextAsync(ct);
            await ExecuteWriteAsync(context, token => action(context, token), ct);
        }
        finally
        {
            _writeGate.Release();
        }
    }

    public async ValueTask AdjustTableRowCountAsync(string tableName, long delta, CancellationToken ct)
    {
        StorageEngineContext context = GetRequiredContext();
        await context.Catalog.AdjustTableRowCountKnownExactAsync(tableName, delta, ct);
        await context.Catalog.PersistRootPageChangesAsync(tableName, ct);
    }

    public ValueTask PersistRootPageChangesAsync(string tableName, CancellationToken ct)
        => GetRequiredContext().Catalog.PersistRootPageChangesAsync(tableName, ct);

    public async ValueTask ClearAsync()
    {
        await _writeGate.WaitAsync();
        try
        {
            object? sessionKey = _currentSessionKey.Value;
            if (sessionKey is not null)
            {
                await ClearSessionContextAsync(sessionKey);
                return;
            }

            if (_defaultContext is not null)
            {
                StorageEngineContext defaultContext = _defaultContext;
                _defaultContext = null;
                Interlocked.Decrement(ref _contextCount);
                await defaultContext.Pager.DisposeAsync();
            }

            if (_sessionContexts.Count > 0)
            {
                StorageEngineContext[] sessionContexts = _sessionContexts.Values.ToArray();
                int removedCount = _sessionContexts.Count;
                _sessionContexts.Clear();
                Interlocked.Add(ref _contextCount, -removedCount);
                foreach (StorageEngineContext context in sessionContexts)
                    await context.Pager.DisposeAsync();
            }
        }
        finally
        {
            _writeGate.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await ClearAsync();
        _writeGate.Dispose();
    }

    private async ValueTask<StorageEngineContext> EnsureContextAsync(CancellationToken ct)
    {
        if (GetCurrentContextOrNull() is { } existing)
            return existing;

        StorageEngineContext created = await InMemoryStorageEngineFactory.OpenAsync(_storageOptions, ct: ct);
        SetCurrentContext(created);
        return created;
    }

    private StorageEngineContext GetRequiredContext()
        => GetCurrentContextOrNull()
           ?? throw new CSharpDbException(ErrorCode.TableNotFound, "No temporary tables are defined for this session.");

    private StorageEngineContext? GetCurrentContextOrNull()
    {
        object? sessionKey = _currentSessionKey.Value;
        if (sessionKey is null)
            return _defaultContext;

        return _sessionContexts.TryGetValue(sessionKey, out StorageEngineContext? context)
            ? context
            : null;
    }

    private void SetCurrentContext(StorageEngineContext context)
    {
        object? sessionKey = _currentSessionKey.Value;
        if (sessionKey is null)
        {
            if (_defaultContext is null)
                Interlocked.Increment(ref _contextCount);

            _defaultContext = context;
            return;
        }

        if (!_sessionContexts.ContainsKey(sessionKey))
            Interlocked.Increment(ref _contextCount);

        _sessionContexts[sessionKey] = context;
    }

    private async ValueTask ClearSessionContextAsync(object sessionKey)
    {
        if (!_sessionContexts.Remove(sessionKey, out StorageEngineContext? context))
            return;

        Interlocked.Decrement(ref _contextCount);
        await context.Pager.DisposeAsync();
    }

    private static async ValueTask ExecuteWriteAsync(
        StorageEngineContext context,
        Func<CancellationToken, ValueTask> action,
        CancellationToken ct)
    {
        PagerCommitResult commit = PagerCommitResult.Completed;
        bool began = false;
        try
        {
            await context.Pager.BeginTransactionAsync(ct);
            began = true;
            await action(ct);
            commit = await context.Pager.BeginCommitAsync(ct);
            began = false;
        }
        catch
        {
            if (began)
            {
                try { await context.Pager.RollbackAsync(CancellationToken.None); }
                catch { }
            }

            throw;
        }

        await commit.WaitAsync(ct);
    }

    private sealed class SessionScope : IDisposable
    {
        private readonly TemporaryTableManager _owner;
        private readonly object? _previousSessionKey;
        private bool _disposed;

        public SessionScope(TemporaryTableManager owner, object? previousSessionKey)
        {
            _owner = owner;
            _previousSessionKey = previousSessionKey;
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            _owner._currentSessionKey.Value = _previousSessionKey;
            _disposed = true;
        }
    }
}
