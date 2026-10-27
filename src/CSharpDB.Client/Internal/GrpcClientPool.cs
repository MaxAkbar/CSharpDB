using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace CSharpDB.Client.Internal;

/// <summary>Pools validated gRPC clients; every checkout gets fresh logical session state.</summary>
internal static class GrpcClientPoolRegistry
{
    private static readonly ConcurrentDictionary<GrpcPoolKey, GrpcClientPool> Pools = new();
    // Client options and route contexts are immutable after initialization. Weak
    // keys avoid retaining one-off configurations after their callers release them.
    private static readonly ConditionalWeakTable<CSharpDbClientOptions, Uri> ValidatedEndpoints = new();
    // Retire unused families as well as their sockets, including high-cardinality routes.
    private static readonly Timer IdleTimer = new(static state => { _ = PruneAsync(); }, null,
        TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(30));

    public static async ValueTask<GrpcTransportClient> RentAsync(
        CSharpDbClientOptions options, int maxPoolSize, CancellationToken ct)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxPoolSize, 1);
        var endpoint = ValidatedEndpoints.GetValue(options, ClientTransportResolver.ResolvePooledGrpcEndpoint);
        var key = new GrpcPoolKey(options, endpoint, maxPoolSize);
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            var pool = Pools.GetOrAdd(key, static (poolKey, clientOptions) =>
                new GrpcClientPool(clientOptions, poolKey.MaxPoolSize), options);
            var session = await pool.TryRentAsync(ct).ConfigureAwait(false);
            if (session is not null) return session;
            Pools.TryRemove(new KeyValuePair<GrpcPoolKey, GrpcClientPool>(key, pool));
        }
    }

    public static async ValueTask ClearAsync(string? endpoint = null)
    {
        string? normalized = endpoint is null ? null : new Uri(endpoint, UriKind.Absolute).AbsoluteUri;
        foreach (var pair in Pools)
            if ((normalized is null || pair.Key.Endpoint == normalized) &&
                Pools.TryRemove(new KeyValuePair<GrpcPoolKey, GrpcClientPool>(pair.Key, pair.Value)))
                await pair.Value.RetireAsync().ConfigureAwait(false);
    }

    private static async Task PruneAsync()
    {
        foreach (var pair in Pools)
        {
            if (!pair.Value.TryRetireIdle(TimeSpan.FromMinutes(2))) continue;
            Pools.TryRemove(new KeyValuePair<GrpcPoolKey, GrpcClientPool>(pair.Key, pair.Value));
            try { await pair.Value.RetireAsync().ConfigureAwait(false); }
            catch { /* A background prune must not terminate the process. */ }
        }
    }

    private readonly struct GrpcPoolKey : IEquatable<GrpcPoolKey>
    {
        public string Endpoint { get; }
        public int MaxPoolSize { get; }
        private readonly CSharpDbGrpcMode _mode;
        private readonly CSharpDbGrpcCallMode _callMode;
        private readonly HttpClient? _httpClient;
        private readonly string? _keyspace, _routeKey, _apiKey;
        private readonly string _apiKeyHeader;

        public GrpcPoolKey(CSharpDbClientOptions options, Uri endpoint, int maxPoolSize)
        {
            Endpoint = endpoint.AbsoluteUri;
            MaxPoolSize = maxPoolSize;
            _mode = options.GrpcMode;
            _callMode = options.GrpcCallMode;
            _httpClient = options.HttpClient;
            _keyspace = options.RouteContext?.Keyspace;
            _routeKey = options.RouteContext?.Key;
            _apiKey = options.ApiKey;
            _apiKeyHeader = options.ApiKeyHeaderName;
        }

        public bool Equals(GrpcPoolKey other)
            => Endpoint == other.Endpoint && MaxPoolSize == other.MaxPoolSize &&
               _mode == other._mode && _callMode == other._callMode && ReferenceEquals(_httpClient, other._httpClient) &&
               _keyspace == other._keyspace && _routeKey == other._routeKey &&
               _apiKey == other._apiKey && _apiKeyHeader == other._apiKeyHeader;
        public override bool Equals(object? obj) => obj is GrpcPoolKey other && Equals(other);
        public override int GetHashCode()
        {
            var hash = new HashCode();
            hash.Add(Endpoint); hash.Add(MaxPoolSize); hash.Add(_mode);
            hash.Add(_callMode);
            hash.Add(_httpClient is null ? 0 : System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode(_httpClient));
            hash.Add(_keyspace); hash.Add(_routeKey); hash.Add(_apiKey); hash.Add(_apiKeyHeader);
            return hash.ToHashCode();
        }
    }
}

internal sealed class GrpcClientPool
{
    private readonly object _gate = new();
    private readonly Stack<GrpcTransportClient> _idle = new();
    private readonly SemaphoreSlim _slots;
    private readonly CancellationTokenSource _retirement = new();
    private readonly CSharpDbClientOptions _options;
    private readonly Func<GrpcTransportClient, bool, ValueTask> _return;
    private bool _retired;
    private int _active;
    private long _lastUse = Stopwatch.GetTimestamp();

    public GrpcClientPool(CSharpDbClientOptions options, int maxPoolSize)
    {
        _options = options;
        _slots = new SemaphoreSlim(maxPoolSize, maxPoolSize);
        _return = ReturnAsync;
    }

    public async ValueTask<GrpcTransportClient?> TryRentAsync(CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        if (!_slots.Wait(0))
        {
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, _retirement.Token);
            try { await _slots.WaitAsync(linked.Token).ConfigureAwait(false); }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested && _retirement.IsCancellationRequested)
            { return null; }
        }

        GrpcTransportClient? client;
        lock (_gate)
        {
            if (_retired) { _slots.Release(); return null; }
            _active++;
            _lastUse = Stopwatch.GetTimestamp();
            client = _idle.TryPop(out var idle) ? idle : null;
        }
        try
        {
            if (client is null)
            {
                client = (GrpcTransportClient)ClientTransportResolver.Create(_options);
                await client.GetInfoAsync(ct).ConfigureAwait(false);
            }
            ct.ThrowIfCancellationRequested();
            return client.CreateLease(_return);
        }
        catch
        {
            try { if (client is not null) await client.DisposeAsync().ConfigureAwait(false); }
            finally { lock (_gate) _active--; _slots.Release(); }
            throw;
        }
    }

    private async ValueTask ReturnAsync(GrpcTransportClient client, bool reusable)
    {
        bool dispose;
        lock (_gate)
        {
            _active--;
            _lastUse = Stopwatch.GetTimestamp();
            dispose = _retired || !reusable;
            if (!dispose) _idle.Push(client);
        }
        try { if (dispose) await client.DisposeAsync().ConfigureAwait(false); }
        finally { _slots.Release(); }
    }

    public bool TryRetireIdle(TimeSpan idleAge)
    {
        lock (_gate)
        {
            if (_retired || _active != 0 || Stopwatch.GetElapsedTime(_lastUse) < idleAge) return false;
            _retired = true;
            return true;
        }
    }

    public async ValueTask RetireAsync()
    {
        GrpcTransportClient[] idle;
        lock (_gate)
        {
            _retired = true;
            idle = _idle.ToArray();
            _idle.Clear();
        }
        _retirement.Cancel();
        foreach (var client in idle) await client.DisposeAsync().ConfigureAwait(false);
        // Active clients are disposed on return. Waiters reopen through the registry.
    }
}
