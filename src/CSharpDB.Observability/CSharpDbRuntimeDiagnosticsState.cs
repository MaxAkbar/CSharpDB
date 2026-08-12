using System.Text.Json;

namespace CSharpDB.Observability;

/// <summary>
/// Process-local identity and immutable configuration for one diagnostics
/// runtime. The state is intentionally instance-owned so independent databases,
/// hosts, and tests never share identity or counter epochs through a global.
/// </summary>
internal sealed partial class CSharpDbRuntimeDiagnosticsState : IDisposable
{
    private readonly object _componentGate = new();
    private readonly byte[] _serializedOptions;
    private readonly CSharpDbRuntimeIdentity _identity;
    private readonly CSharpDbRuntimeMetrics? _runtimeMetrics;
    private Dictionary<Type, object>? _components;
    private long _counterEpoch;
    private int _disposed;

    internal CSharpDbRuntimeDiagnosticsState(
        CSharpDbObservabilityOptions? options = null,
        TimeProvider? timeProvider = null)
        : this(options, new CSharpDbRuntimeIdentity(timeProvider))
    {
    }

    private CSharpDbRuntimeDiagnosticsState(
        CSharpDbObservabilityOptions? options,
        CSharpDbRuntimeIdentity identity)
    {
        ArgumentNullException.ThrowIfNull(identity);
        CSharpDbObservabilityOptions effectiveOptions = options ?? new CSharpDbObservabilityOptions();
        effectiveOptions.Validate();

        _serializedOptions = JsonSerializer.SerializeToUtf8Bytes(
            effectiveOptions,
            CSharpDbObservabilityJsonContext.Default.CSharpDbObservabilityOptions);
        CSharpDbObservabilityOptions snapshot = DeserializeOptions();

        _identity = identity;
        _counterEpoch = identity.CounterEpoch;
        IsEnabled = snapshot.Enabled;
        HistoryEnabled = snapshot.Enabled && snapshot.History.Enabled;
        TracingEnabled = snapshot.Enabled && snapshot.OpenTelemetry.Enabled;
        MetricsEnabled = snapshot.Enabled &&
            (snapshot.OpenTelemetry.Enabled || snapshot.Prometheus.Enabled);
        DatabaseAlias = snapshot.DatabaseAlias;
        ActiveQueryCapacity = snapshot.History.ActiveQueryCapacity;
        RecentQueryCapacity = snapshot.History.RecentQueryCapacity;
        RecentQueryRetention = snapshot.History.Retention;
        RecentOperationCapacity = snapshot.History.RecentOperationCapacity;
        RecentOperationRetention = snapshot.History.Retention;
        LongRunningQueryThreshold = snapshot.LongRunningQueryThreshold;
        SessionAbandonmentThreshold = snapshot.SessionAbandonmentThreshold;
        if (MetricsEnabled)
        {
            _runtimeMetrics = CSharpDbRuntimeMetrics.TryCreate(
                DatabaseAlias,
                identity.TimeProvider);
        }
    }

    internal string ServerInstanceId => _identity.ServerInstanceId;
    internal TimeProvider TimeProvider => _identity.TimeProvider;
    internal bool IsEnabled { get; }
    internal bool HistoryEnabled { get; }
    internal bool TracingEnabled { get; }
    internal bool MetricsEnabled { get; }
    internal string DatabaseAlias { get; }
    internal int ActiveQueryCapacity { get; }
    internal int RecentQueryCapacity { get; }
    internal TimeSpan RecentQueryRetention { get; }
    internal int RecentOperationCapacity { get; }
    internal TimeSpan RecentOperationRetention { get; }
    internal TimeSpan LongRunningQueryThreshold { get; }
    internal TimeSpan SessionAbandonmentThreshold { get; }
    internal CSharpDbRuntimeMetrics? RuntimeMetrics =>
        Volatile.Read(ref _disposed) == 0 ? _runtimeMetrics : null;
    internal long CounterEpoch => Interlocked.Read(ref _counterEpoch);

    internal long AdvanceCounterEpoch()
    {
        long epoch = _identity.AdvanceCounterEpoch();
        Interlocked.Exchange(ref _counterEpoch, epoch);
        return epoch;
    }

    internal void CompleteCounterFamilyOpen(bool replacesExistingFamily)
        => Interlocked.Exchange(
            ref _counterEpoch,
            _identity.CompleteCounterFamilyOpen(replacesExistingFamily));

    internal CSharpDbRuntimeDiagnosticsState CreateForOptions(
        CSharpDbObservabilityOptions? options)
        => new(options, _identity);

    internal CSharpDbObservabilityOptions CreateOptionsSnapshot()
        => DeserializeOptions();

    internal T GetOrCreateComponent<T>(Func<T> factory)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(factory);
        Type componentType = typeof(T);
        lock (_componentGate)
        {
            ObjectDisposedException.ThrowIf(
                Volatile.Read(ref _disposed) != 0,
                this);
            if (_components is not null &&
                _components.TryGetValue(componentType, out object? existing))
            {
                return (T)existing;
            }
        }

        // Construction can allocate, start a timer, or invoke user-provided
        // test hooks, so it must never run under the state cache lock.
        T candidate = factory()
            ?? throw new InvalidOperationException(
                $"The runtime component factory for '{componentType.FullName}' returned null.");
        T? winner = null;
        bool disposeCandidate = false;
        lock (_componentGate)
        {
            if (Volatile.Read(ref _disposed) != 0)
            {
                disposeCandidate = true;
            }
            else if (_components is not null &&
                     _components.TryGetValue(componentType, out object? existing))
            {
                winner = (T)existing;
                disposeCandidate = true;
            }
            else
            {
                (_components ??= new Dictionary<Type, object>())
                    .Add(componentType, candidate);
                winner = candidate;
            }
        }

        if (disposeCandidate)
            DisposeComponent(candidate);
        if (winner is null)
            throw new ObjectDisposedException(GetType().FullName);

        return winner;
    }

    internal bool TryGetComponent<T>(out T? component)
        where T : class
    {
        lock (_componentGate)
        {
            if (Volatile.Read(ref _disposed) == 0 &&
                _components is not null &&
                _components.TryGetValue(typeof(T), out object? existing))
            {
                component = (T)existing;
                return true;
            }

            component = null;
            return false;
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        // Unregister the static observable source before disposing providers so
        // a concurrent collection cannot retain or re-enter a retired runtime.
        _runtimeMetrics?.Dispose();

        object[] components;
        lock (_componentGate)
        {
            components = _components?.Values.ToArray() ?? [];
            _components = null;
        }

        foreach (object component in components)
            DisposeComponent(component);
    }

    internal DiagnosticsSnapshotMetadata CreateMetadata(
        DiagnosticsScope scope,
        DiagnosticsAvailability availability,
        DiagnosticsSource source,
        string? databaseAlias = null,
        bool recordsTruncated = false,
        bool fieldsTruncated = false)
        => DiagnosticsSnapshotMetadata.Create(
            ServerInstanceId,
            CounterEpoch,
            scope,
            availability,
            source,
            databaseAlias ?? DatabaseAlias,
            recordsTruncated,
            fieldsTruncated,
            TimeProvider);

    private CSharpDbObservabilityOptions DeserializeOptions()
        => JsonSerializer.Deserialize(
               _serializedOptions,
               CSharpDbObservabilityJsonContext.Default.CSharpDbObservabilityOptions)
           ?? throw new InvalidOperationException("The diagnostics option snapshot could not be restored.");

    private static void DisposeComponent(object component)
    {
        try
        {
            if (component is IDisposable disposable)
                disposable.Dispose();
        }
        catch
        {
            // Runtime diagnostics teardown is best-effort and must never turn
            // a successful database/client disposal into an application error.
        }
    }
}

/// <summary>
/// Identity shared by per-database diagnostics configurations owned by one
/// client or host lifetime. Database switches can replace alias/options without
/// pretending that the server process restarted.
/// </summary>
internal sealed class CSharpDbRuntimeIdentity
{
    private readonly CSharpDbCounterEpoch _counterEpoch = new();
    private int _hasCompletedCounterFamilyOpen;

    internal CSharpDbRuntimeIdentity(TimeProvider? timeProvider = null)
    {
        ServerInstanceId = CSharpDbDiagnostics.CreateServerInstanceId();
        TimeProvider = timeProvider ?? TimeProvider.System;
    }

    internal string ServerInstanceId { get; }
    internal TimeProvider TimeProvider { get; }
    internal long CounterEpoch => _counterEpoch.Value;

    internal long AdvanceCounterEpoch() => _counterEpoch.Advance();

    internal long CompleteCounterFamilyOpen(bool replacesExistingFamily)
    {
        bool hadExistingFamily =
            Interlocked.Exchange(ref _hasCompletedCounterFamilyOpen, 1) != 0;
        if (replacesExistingFamily && hadExistingFamily)
            return _counterEpoch.Advance();

        return _counterEpoch.Value;
    }
}
