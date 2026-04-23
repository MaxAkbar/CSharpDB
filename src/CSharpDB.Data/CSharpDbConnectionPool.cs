using System.Collections.Concurrent;
using CSharpDB.Client;

namespace CSharpDB.Data;

internal static class CSharpDbConnectionPoolRegistry
{
    private static readonly ConcurrentDictionary<PoolKey, CSharpDbConnectionPool> s_pools = new();

    internal static CSharpDbConnectionPool GetOrCreate(
        PoolKey key,
        string connectionString,
        Func<string, CancellationToken, ValueTask<ICSharpDbClient>> openClientAsync)
        => s_pools.GetOrAdd(
            key,
            static (staticKey, state) => new CSharpDbConnectionPool(
                state.ConnectionString,
                staticKey.MaxPoolSize,
                state.OpenClientAsync),
            (ConnectionString: connectionString, OpenClientAsync: openClientAsync));

    internal static async ValueTask ClearPoolAsync(PoolKey key)
    {
        if (s_pools.TryRemove(key, out var pool))
            await pool.DisableAsync();
    }

    internal static async ValueTask ClearPoolsAsync(Func<PoolKey, bool> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);

        KeyValuePair<PoolKey, CSharpDbConnectionPool>[] pools = s_pools
            .Where(pair => predicate(pair.Key))
            .ToArray();

        foreach ((PoolKey key, CSharpDbConnectionPool pool) in pools)
        {
            if (s_pools.TryRemove(key, out var removedPool))
                await removedPool.DisableAsync();
            else
                await pool.DisableAsync();
        }
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

internal readonly record struct PoolKey(
    string DataSource,
    int MaxPoolSize,
    CSharpDbEmbeddedOpenMode EffectiveOpenMode,
    CSharpDbStoragePreset? EffectiveStoragePreset,
    object? ExplicitDirectDatabaseOptions,
    object? ExplicitHybridDatabaseOptions);

internal sealed class CSharpDbConnectionPool
{
    private readonly string _connectionString;
    private readonly int _maxPoolSize;
    private readonly Func<string, CancellationToken, ValueTask<ICSharpDbClient>> _openClientAsync;
    private readonly object _gate = new();
    private readonly Stack<ICSharpDbClient> _idle = new();
    private bool _disabled;

    internal CSharpDbConnectionPool(
        string connectionString,
        int maxPoolSize,
        Func<string, CancellationToken, ValueTask<ICSharpDbClient>> openClientAsync)
    {
        _connectionString = connectionString;
        _maxPoolSize = maxPoolSize;
        _openClientAsync = openClientAsync;
    }

    internal int IdleCount
    {
        get
        {
            lock (_gate)
                return _idle.Count;
        }
    }

    internal async ValueTask<ICSharpDbClient> RentAsync(CancellationToken ct)
    {
        lock (_gate)
        {
            if (!_disabled && _idle.Count > 0)
                return _idle.Pop();
        }

        return await _openClientAsync(_connectionString, ct);
    }

    internal async ValueTask ReturnAsync(ICSharpDbClient client)
    {
        ArgumentNullException.ThrowIfNull(client);

        bool disposeImmediately;
        lock (_gate)
        {
            disposeImmediately = _disabled || _idle.Count >= _maxPoolSize;
            if (!disposeImmediately)
                _idle.Push(client);
        }

        if (disposeImmediately)
            await client.DisposeAsync();
    }

    internal async ValueTask DisableAsync()
    {
        ICSharpDbClient[] idleClients;
        lock (_gate)
        {
            _disabled = true;
            idleClients = _idle.ToArray();
            _idle.Clear();
        }

        foreach (var client in idleClients)
            await client.DisposeAsync();
    }
}
