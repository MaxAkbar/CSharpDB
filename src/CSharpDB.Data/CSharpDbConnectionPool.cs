using System.Collections.Concurrent;
using CSharpDB.Engine;

namespace CSharpDB.Data;

internal static class CSharpDbConnectionPoolRegistry
{
    private static readonly ConcurrentDictionary<PoolKey, CSharpDbConnectionPool> s_pools = new();

    internal static CSharpDbConnectionPool GetOrCreate(PoolKey key)
        => s_pools.GetOrAdd(key, static staticKey => new CSharpDbConnectionPool(staticKey.DataSource, staticKey.MaxPoolSize));

    internal static async ValueTask ClearPoolAsync(PoolKey key)
    {
        if (s_pools.TryRemove(key, out var pool))
            await pool.DisableAsync();
    }

    internal static async ValueTask ClearAllAsync()
    {
        var pools = s_pools.ToArray();
        s_pools.Clear();

        foreach (var pair in pools)
            await pair.Value.DisableAsync();
    }

    internal static int GetPoolCountForTest() => s_pools.Count;

    internal static int GetIdleCountForTest(PoolKey key)
    {
        return s_pools.TryGetValue(key, out var pool)
            ? pool.IdleCount
            : 0;
    }
}

internal readonly record struct PoolKey(string DataSource, int MaxPoolSize);

internal sealed class CSharpDbConnectionPool
{
    private readonly string _dataSource;
    private readonly int _maxPoolSize;
    private readonly object _gate = new();
    private readonly Stack<Database> _idle = new();
    private bool _disabled;

    internal CSharpDbConnectionPool(string dataSource, int maxPoolSize)
    {
        _dataSource = dataSource;
        _maxPoolSize = maxPoolSize;
    }

    internal int IdleCount
    {
        get
        {
            lock (_gate)
                return _idle.Count;
        }
    }

    internal async ValueTask<Database> RentAsync(CancellationToken ct)
    {
        lock (_gate)
        {
            if (!_disabled && _idle.Count > 0)
                return _idle.Pop();
        }

        return await Database.OpenAsync(_dataSource, ct);
    }

    internal async ValueTask ReturnAsync(Database database)
    {
        ArgumentNullException.ThrowIfNull(database);

        bool disposeImmediately;
        lock (_gate)
        {
            disposeImmediately = _disabled || _idle.Count >= _maxPoolSize;
            if (!disposeImmediately)
                _idle.Push(database);
        }

        if (disposeImmediately)
            await database.DisposeAsync();
    }

    internal async ValueTask DisableAsync()
    {
        Database[] idleDatabases;
        lock (_gate)
        {
            _disabled = true;
            idleDatabases = _idle.ToArray();
            _idle.Clear();
        }

        foreach (var database in idleDatabases)
            await database.DisposeAsync();
    }
}
