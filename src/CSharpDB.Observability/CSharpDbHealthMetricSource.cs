using System.Diagnostics.Metrics;

namespace CSharpDB.Observability;

/// <summary>
/// Registers one cached host-state source for the bounded
/// <c>csharpdb.health.status</c> observable gauge. Registration is best effort:
/// a duplicate alias or a full bounded registry returns <see langword="null"/>.
/// Dispose removes the source immediately so host objects are not retained.
/// </summary>
public sealed class CSharpDbHealthMetricSource : IDisposable
{
    private readonly string _databaseAlias;
    private readonly KeyValuePair<string, object?>[][] _tags;
    private CSharpDbHostState? _hostState;
    private IDisposable? _registryRegistration;
    private int _disposed;

    private CSharpDbHealthMetricSource(
        CSharpDbHostState hostState,
        string databaseAlias)
    {
        _hostState = hostState;
        _databaseAlias = databaseAlias;
        _tags =
        [
            CreateTags(
                CSharpDbHealthCheckKind.Liveness,
                CSharpDbHealthStatus.Healthy,
                databaseAlias),
            CreateTags(
                CSharpDbHealthCheckKind.Liveness,
                CSharpDbHealthStatus.Unhealthy,
                databaseAlias),
            CreateTags(
                CSharpDbHealthCheckKind.Readiness,
                CSharpDbHealthStatus.Healthy,
                databaseAlias),
            CreateTags(
                CSharpDbHealthCheckKind.Readiness,
                CSharpDbHealthStatus.Unhealthy,
                databaseAlias),
        ];
    }

    public string DatabaseAlias => _databaseAlias;

    /// <summary>
    /// Tries to register one host health source. At most one live source per
    /// validated alias and at most 64 total sources are retained.
    /// </summary>
    public static CSharpDbHealthMetricSource? TryCreate(
        CSharpDbHostState hostState,
        string databaseAlias)
    {
        ArgumentNullException.ThrowIfNull(hostState);
        if (!CSharpDbObservabilityOptions.IsValidDatabaseAlias(databaseAlias))
        {
            throw new ArgumentException(
                "A safe bounded database alias is required.",
                nameof(databaseAlias));
        }

        try
        {
            CSharpDbMetrics.EnsureInitialized();
            var source = new CSharpDbHealthMetricSource(
                hostState,
                databaseAlias);
            IDisposable? registration =
                CSharpDbHealthMetricsRegistry.TryRegister(source);
            if (registration is null)
            {
                source.Dispose();
                return null;
            }

            source._registryRegistration = registration;
            return source;
        }
        catch
        {
            // Health metrics are best effort and cannot prevent host startup.
            return null;
        }
    }

    internal bool TryObserve(
        out Measurement<long> liveness,
        out Measurement<long> readiness)
    {
        liveness = default;
        readiness = default;
        if (Volatile.Read(ref _disposed) != 0)
            return false;

        CSharpDbHostState? hostState = Volatile.Read(ref _hostState);
        if (hostState is null)
            return false;

        try
        {
            CSharpDbHostStateSnapshot snapshot = hostState.Snapshot;
            liveness = new Measurement<long>(
                1,
                _tags[snapshot.IsLive ? 0 : 1]);
            readiness = new Measurement<long>(
                1,
                _tags[snapshot.IsReady ? 2 : 3]);
            return Volatile.Read(ref _disposed) == 0;
        }
        catch
        {
            return false;
        }
    }

    public void Dispose()
    {
        if (Interlocked.CompareExchange(ref _disposed, 1, 0) != 0)
            return;

        Volatile.Write(ref _hostState, null);
        Interlocked.Exchange(ref _registryRegistration, null)?.Dispose();
        Volatile.Write(ref _disposed, 2);
    }

    private static KeyValuePair<string, object?>[] CreateTags(
        CSharpDbHealthCheckKind checkKind,
        CSharpDbHealthStatus status,
        string databaseAlias)
        =>
        [
            new(
                CSharpDbMetricTagNames.CheckKind,
                CSharpDbMetricTagValues.HealthCheckKind(checkKind)),
            new(
                CSharpDbMetricTagNames.Status,
                CSharpDbMetricTagValues.HealthStatus(status)),
            new(CSharpDbMetricTagNames.DatabaseAlias, databaseAlias),
        ];
}

internal static class CSharpDbHealthMetricsRegistry
{
    private static readonly object s_gate = new();
    private static readonly CSharpDbHealthMetricSource?[] s_sources =
        new CSharpDbHealthMetricSource?[
            CSharpDbDiagnostics.MaximumConfiguredDatabaseAliases];

    internal static IDisposable? TryRegister(
        CSharpDbHealthMetricSource source)
    {
        ArgumentNullException.ThrowIfNull(source);
        lock (s_gate)
        {
            if (s_sources.Any(candidate =>
                    candidate is not null &&
                    string.Equals(
                        candidate.DatabaseAlias,
                        source.DatabaseAlias,
                        StringComparison.Ordinal)))
            {
                return null;
            }

            int index = Array.FindIndex(
                s_sources,
                static candidate => candidate is null);
            if (index < 0)
                return null;

            Volatile.Write(ref s_sources[index], source);
            return new Registration(index, source);
        }
    }

    internal static IEnumerable<Measurement<long>> Observe()
    {
        CSharpDbHealthMetricSource?[] sources;
        lock (s_gate)
            sources = (CSharpDbHealthMetricSource?[])s_sources.Clone();

        var measurements = new Measurement<long>[sources.Length * 2];
        int count = 0;
        foreach (CSharpDbHealthMetricSource? source in sources)
        {
            if (source?.TryObserve(
                    out Measurement<long> liveness,
                    out Measurement<long> readiness) != true)
            {
                continue;
            }

            measurements[count++] = liveness;
            measurements[count++] = readiness;
        }

        return count == measurements.Length
            ? measurements
            : measurements[..count];
    }

    internal static int RegisteredCount
    {
        get
        {
            lock (s_gate)
                return s_sources.Count(static source => source is not null);
        }
    }

    private sealed class Registration(
        int sourceIndex,
        CSharpDbHealthMetricSource source) : IDisposable
    {
        private CSharpDbHealthMetricSource? _source = source;

        public void Dispose()
        {
            CSharpDbHealthMetricSource? registered = Interlocked.Exchange(
                ref _source,
                null);
            if (registered is null)
                return;

            lock (s_gate)
            {
                if (ReferenceEquals(s_sources[sourceIndex], registered))
                    Volatile.Write(ref s_sources[sourceIndex], null);
            }
        }
    }
}
