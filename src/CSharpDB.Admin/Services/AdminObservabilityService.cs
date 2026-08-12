using CSharpDB.Admin.Configuration;
using CSharpDB.Admin.Models;
using CSharpDB.Client;
using CSharpDB.Observability;

namespace CSharpDB.Admin.Services;

internal sealed class AdminObservabilityService : IAsyncDisposable
{
    private readonly ICSharpDbObservabilityClient _client;
    private readonly AdminObservabilityOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly DatabaseClientHolder? _databaseClientHolder;
    private readonly TabManagerService? _tabManager;
    private readonly object _sync = new();
    private readonly object _admissionSync = new();
    private readonly SemaphoreSlim _refreshGate = new(1, 1);
    private CancellationTokenSource? _activeCancellation;
    private CancellationTokenSource? _pollCancellation;
    private CancellationTokenSource? _planCancellation;
    private CancellationTokenSource? _detailCancellation;
    private CancellationTokenSource? _staleCancellation;
    private AdminObservabilityViewState _current;
    private bool _active;
    private bool _disposed;
    private bool _admissionOpen;
    private long _admissionGeneration;
    private long _generation;
    private long _planGeneration;
    private long _detailGeneration;
    private string? _serverInstanceId;
    private string? _databaseAlias;
    private long? _counterEpoch;
    private CounterPoint? _previousCounters;
    private readonly List<AdminObservabilityMetricSample> _samples = [];

    public AdminObservabilityService(
        DatabaseClientHolder client,
        AdminObservabilityOptions options,
        TabManagerService tabManager)
        : this(client, options, TimeProvider.System, client, tabManager)
    {
    }

    internal AdminObservabilityService(
        ICSharpDbObservabilityClient client,
        AdminObservabilityOptions options,
        TimeProvider timeProvider,
        DatabaseClientHolder? databaseClientHolder = null,
        TabManagerService? tabManager = null)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(timeProvider);
        options.Validate();

        _client = client;
        _options = options;
        _timeProvider = timeProvider;
        _databaseClientHolder = databaseClientHolder;
        _tabManager = tabManager;
        if (_databaseClientHolder is not null)
            _databaseClientHolder.DatabaseChanged += OnDatabaseChanged;
        if (_tabManager is not null)
            _tabManager.StateChanged += OnTabManagerChanged;
        _current = new AdminObservabilityViewState
        {
            RefreshInterval = options.RefreshInterval,
            MaximumRefreshInterval = options.StaleAfter < AdminObservabilityOptions.MaximumRefreshInterval
                ? options.StaleAfter
                : AdminObservabilityOptions.MaximumRefreshInterval,
        };
    }

    public event Action? StateChanged;

    public AdminObservabilityViewState Current
    {
        get
        {
            lock (_sync)
            {
                DateTimeOffset? captured = _current.SnapshotCapturedAtUtc;
                TimeSpan? age = captured is null ? null : NonNegative(SafeUtcNow() - captured.Value);
                return _current with
                {
                    SnapshotAge = age,
                    IsStale = age is { } value && value >= _options.StaleAfter,
                };
            }
        }
    }

    public Task SetActiveAsync(bool active)
    {
        bool changed;
        lock (_sync)
        {
            ThrowIfDisposed();
            changed = _active != active;
            if (!changed)
                return Task.CompletedTask;

            _generation++;
            _active = active && IsTabActiveLocked();
            lock (_admissionSync)
            {
                _admissionOpen = false;
                _admissionGeneration = _generation;
                CancelAndDispose(ref _pollCancellation);
                CancelAndDispose(ref _staleCancellation);
                CancelAndDispose(ref _activeCancellation);
            }

            if (_active)
            {
                _activeCancellation = new CancellationTokenSource();
                lock (_admissionSync)
                {
                    _admissionOpen = true;
                    _admissionGeneration = _generation;
                }
                _current = _current with
                {
                    IsLoading = !_current.IsPaused && _current.LastSuccessfulRefreshUtc is null,
                    IsRefreshing = false,
                    StatusText = _current.IsPaused ? "Paused" : "Loading diagnostics",
                };
                StartStaleNotificationLocked();
                if (!_current.IsPaused)
                    StartPollingLocked();
            }
            else
            {
                ClearSensitiveLocked();
                _current = _current with
                {
                    IsLoading = false,
                    IsRefreshing = false,
                    StatusText = "Inactive",
                };
            }
        }

        RaiseStateChanged();
        return Task.CompletedTask;
    }

    public void SetPaused(bool paused)
    {
        lock (_sync)
        {
            ThrowIfDisposed();
            if (_current.IsPaused == paused)
                return;

            _generation++;
            lock (_admissionSync)
            {
                _admissionOpen = false;
                _admissionGeneration = _generation;
                CancelAndDispose(ref _pollCancellation);
                CancelAndDispose(ref _staleCancellation);
            }
            _current = _current with
            {
                IsPaused = paused,
                IsLoading = !paused && _active && _current.LastSuccessfulRefreshUtc is null,
                IsRefreshing = false,
                StatusText = paused ? "Paused" : (_active ? "Resuming diagnostics" : "Inactive"),
            };
            if (_active)
            {
                StartStaleNotificationLocked();
                if (!paused)
                    StartPollingLocked();
            }
            lock (_admissionSync)
            {
                _admissionOpen = _active;
                _admissionGeneration = _generation;
            }
        }

        RaiseStateChanged();
    }

    public void SetRefreshInterval(TimeSpan refreshInterval)
    {
        if (refreshInterval < AdminObservabilityOptions.MinimumRefreshInterval ||
            refreshInterval > AdminObservabilityOptions.MaximumRefreshInterval ||
            refreshInterval > _options.StaleAfter)
        {
            throw new ArgumentOutOfRangeException(nameof(refreshInterval));
        }

        lock (_sync)
        {
            ThrowIfDisposed();
            if (_current.RefreshInterval == refreshInterval)
                return;

            _generation++;
            lock (_admissionSync)
            {
                _admissionOpen = false;
                _admissionGeneration = _generation;
                CancelAndDispose(ref _pollCancellation);
                CancelAndDispose(ref _staleCancellation);
            }
            _current = _current with { RefreshInterval = refreshInterval };
            if (_active)
            {
                StartStaleNotificationLocked();
                if (!_current.IsPaused)
                    StartPollingLocked();
            }
            lock (_admissionSync)
            {
                _admissionOpen = _active;
                _admissionGeneration = _generation;
            }
        }

        RaiseStateChanged();
    }

    public void SetScope(string? shardAlias)
    {
        if (shardAlias is not null && string.IsNullOrWhiteSpace(shardAlias))
            throw new ArgumentException("A shard alias cannot be blank.", nameof(shardAlias));

        lock (_sync)
        {
            ThrowIfDisposed();
            if (string.Equals(_current.SelectedScope, shardAlias, StringComparison.Ordinal))
                return;

            _generation++;
            lock (_admissionSync)
            {
                _admissionOpen = false;
                _admissionGeneration = _generation;
                CancelAndDispose(ref _pollCancellation);
                CancelAndDispose(ref _activeCancellation);
                CancelAndDispose(ref _staleCancellation);
            }
            if (_active)
                _activeCancellation = new CancellationTokenSource();
            lock (_admissionSync)
            {
                _admissionOpen = _active;
                _admissionGeneration = _generation;
            }
            ResetIdentityLocked(clearOrdinaryData: true);
            _current = _current with
            {
                SelectedScope = shardAlias,
                ScopeNotice = null,
                LastSuccessfulRefreshUtc = null,
                SnapshotCapturedAtUtc = null,
                SnapshotAge = null,
                IsStale = false,
                IsLoading = _active && !_current.IsPaused,
                StatusText = !_active
                    ? "Inactive"
                    : _current.IsPaused
                        ? "Paused; refresh manually to load the selected scope"
                        : "Loading selected scope",
            };
            if (_active && !_current.IsPaused)
                StartPollingLocked();
        }

        RaiseStateChanged();
    }

    public Task RefreshAsync(CancellationToken cancellationToken = default)
    {
        long generation;
        CancellationToken activeToken;
        lock (_sync)
        {
            ThrowIfDisposed();
            if (!_active || _activeCancellation is null)
                return Task.CompletedTask;
            generation = _generation;
            activeToken = _activeCancellation.Token;
        }

        return RefreshCoreAsync(generation, activeToken, cancellationToken);
    }

    public Task LoadPlanAsync(
        OpaqueDiagnosticsId operationId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operationId);
        return LoadValueAsync(
            ct => _client.GetQueryPlanDiagnosticsAsync(operationId, ct),
            static (state, value) => state with { SelectedPlan = value },
            SensitiveRequestKind.Plan,
            cancellationToken);
    }

    public Task RevealQueryDetailAsync(
        OpaqueDiagnosticsId operationId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operationId);
        return LoadValueAsync(
            ct => _client.GetQueryDetailAsync(operationId, ct),
            static (state, value) => state with { RevealedDetail = value },
            SensitiveRequestKind.Detail,
            cancellationToken);
    }

    public void ClearSensitiveDetail()
    {
        lock (_sync)
        {
            ThrowIfDisposed();
            ClearSensitiveLocked();
        }
        RaiseStateChanged();
    }

    public ValueTask DisposeAsync()
    {
        lock (_sync)
        {
            if (_disposed)
                return ValueTask.CompletedTask;
            _disposed = true;
            _active = false;
            _generation++;
            lock (_admissionSync)
            {
                _admissionOpen = false;
                _admissionGeneration = _generation;
                CancelAndDispose(ref _pollCancellation);
                CancelAndDispose(ref _activeCancellation);
                CancelAndDispose(ref _staleCancellation);
            }
            ClearSensitiveLocked();
            _samples.Clear();
            _current = _current with
            {
                IsLoading = false,
                IsRefreshing = false,
                StatusText = "Disposed",
                Samples = Array.Empty<AdminObservabilityMetricSample>(),
            };
        }
        if (_databaseClientHolder is not null)
            _databaseClientHolder.DatabaseChanged -= OnDatabaseChanged;
        if (_tabManager is not null)
            _tabManager.StateChanged -= OnTabManagerChanged;
        return ValueTask.CompletedTask;
    }

    private void StartPollingLocked()
    {
        _pollCancellation = CancellationTokenSource.CreateLinkedTokenSource(
            _activeCancellation!.Token);
        long generation = _generation;
        CancellationToken token = _pollCancellation.Token;
        _ = PollAsync(generation, token);
    }

    private void StartStaleNotificationLocked()
    {
        CancelAndDispose(ref _staleCancellation);
        if (!_active || _current.SnapshotCapturedAtUtc is null ||
            _activeCancellation is null)
        {
            return;
        }

        TimeSpan age = NonNegative(SafeUtcNow() - _current.SnapshotCapturedAtUtc.Value);
        TimeSpan delay = _options.StaleAfter - age;
        if (delay < TimeSpan.Zero)
            delay = TimeSpan.Zero;
        _staleCancellation = CancellationTokenSource.CreateLinkedTokenSource(
            _activeCancellation.Token);
        long generation = _generation;
        _ = NotifyAtStaleBoundaryAsync(delay, generation, _staleCancellation.Token);
    }

    private async Task NotifyAtStaleBoundaryAsync(
        TimeSpan delay,
        long generation,
        CancellationToken cancellationToken)
    {
        try
        {
            await Task.Yield();
            if (delay > TimeSpan.Zero)
                await Task.Delay(delay, _timeProvider, cancellationToken).ConfigureAwait(false);
            lock (_sync)
            {
                if (!CanPublishLocked(generation))
                    return;
            }
            RaiseStateChanged();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }

    private async Task PollAsync(long generation, CancellationToken cancellationToken)
    {
        try
        {
            // Keep component activation prompt even when a custom client blocks
            // synchronously before returning its Task.
            await Task.Yield();
            while (true)
            {
                await RefreshCoreAsync(generation, cancellationToken, CancellationToken.None)
                    .ConfigureAwait(false);
                TimeSpan interval;
                lock (_sync)
                {
                    if (!CanPublishLocked(generation) || _current.IsPaused)
                        return;
                    interval = _current.RefreshInterval;
                }
                await Task.Delay(interval, _timeProvider, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch
        {
            // Individual requests are already projected to safe availability.
            // A loop-level failure is contained so the UI circuit remains healthy.
        }
    }

    private async Task RefreshCoreAsync(
        long generation,
        CancellationToken activeToken,
        CancellationToken callerToken)
    {
        if (!await _refreshGate.WaitAsync(0, CancellationToken.None).ConfigureAwait(false))
            return;

        try
        {
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(activeToken, callerToken);
            CancellationToken ct = linked.Token;
            lock (_sync)
            {
                if (!CanPublishLocked(generation))
                    return;
                _current = _current with
                {
                    IsLoading = _current.LastSuccessfulRefreshUtc is null,
                    IsRefreshing = true,
                    StatusText = "Refreshing diagnostics",
                };
            }
            RaiseStateChanged();

            Task<Capture<DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>>> runtimeTask =
                CaptureAutomaticAsync(token => _client.GetRuntimeDiagnosticsAsync(token), generation, ct);
            Task<Capture<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>>> activeQueriesTask =
                CaptureAutomaticAsync(token => _client.GetActiveQueriesAsync(_options.MaximumRecords, token), generation, ct);
            Task<Capture<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>>>> recentQueriesTask =
                CaptureAutomaticAsync(token => _client.GetRecentQueriesAsync(_options.MaximumRecords, token), generation, ct);
            Task<Capture<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>>> sessionsTask =
                CaptureAutomaticAsync(token => _client.GetSessionsAsync(_options.MaximumRecords, token), generation, ct);
            Task<Capture<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<MaintenanceOperationSnapshot>>>> activeMaintenanceTask =
                CaptureAutomaticAsync(token => _client.GetActiveMaintenanceOperationsAsync(_options.MaximumRecords, token), generation, ct);
            Task<Capture<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<MaintenanceOperationSnapshot>>>> recentMaintenanceTask =
                CaptureAutomaticAsync(token => _client.GetRecentMaintenanceOperationsAsync(_options.MaximumRecords, token), generation, ct);

            await Task.WhenAll(
                runtimeTask,
                activeQueriesTask,
                recentQueriesTask,
                sessionsTask,
                activeMaintenanceTask,
                recentMaintenanceTask).ConfigureAwait(false);

            Capture<DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>> runtimeCapture = await runtimeTask.ConfigureAwait(false);
            Capture<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>> activeQueriesCapture = await activeQueriesTask.ConfigureAwait(false);
            Capture<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>>> recentQueriesCapture = await recentQueriesTask.ConfigureAwait(false);
            Capture<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>> sessionsCapture = await sessionsTask.ConfigureAwait(false);
            Capture<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<MaintenanceOperationSnapshot>>> activeMaintenanceCapture = await activeMaintenanceTask.ConfigureAwait(false);
            Capture<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<MaintenanceOperationSnapshot>>> recentMaintenanceCapture = await recentMaintenanceTask.ConfigureAwait(false);

            lock (_sync)
            {
                if (!CanPublishLocked(generation))
                    return;

                string? scope = _current.SelectedScope;
                if (scope is not null && runtimeCapture.Value is { } runtimeTopology &&
                    runtimeTopology.Shards?.Any(shard =>
                        string.Equals(shard.ShardAlias, scope, StringComparison.Ordinal)) != true)
                {
                    if (runtimeTopology.ShardsTruncated == true)
                    {
                        _current = _current with
                        {
                            ScopeNotice = $"Shard '{scope}' is outside this bounded response; selection retained and diagnostics are unavailable.",
                        };
                    }
                    else
                    {
                        string missingScope = scope;
                        scope = null;
                        ResetIdentityLocked(clearOrdinaryData: false);
                        _current = _current with
                        {
                            SelectedScope = null,
                            ScopeNotice = $"Shard '{missingScope}' is no longer present; showing aggregate diagnostics.",
                        };
                    }
                }
                else if (scope is not null)
                {
                    _current = _current with { ScopeNotice = null };
                }
                AdminObservabilityValue<RuntimeDiagnosticsSnapshot> runtime =
                    ProjectRuntime(runtimeCapture, scope, out RuntimeDiagnosticsSnapshot? selectedRuntime);
                AdminObservabilityCollection<ActiveQuerySnapshot> activeQueries =
                    ProjectCollection(activeQueriesCapture, scope);
                AdminObservabilityCollection<RecentQuerySnapshot> recentQueries =
                    ProjectCollection(recentQueriesCapture, scope);
                AdminObservabilityCollection<SessionDiagnosticsSnapshot> sessions =
                    ProjectCollection(sessionsCapture, scope);
                AdminObservabilityCollection<MaintenanceOperationSnapshot> activeMaintenance =
                    ProjectCollection(activeMaintenanceCapture, scope);
                AdminObservabilityCollection<MaintenanceOperationSnapshot> recentMaintenance =
                    ProjectCollection(recentMaintenanceCapture, scope);

                bool reset = selectedRuntime is not null && IdentityOrCountersChanged(selectedRuntime);
                if (reset)
                    ResetSamplesAndSensitiveLocked();

                AdminObservabilityValue<StorageRuntimeDiagnosticsSnapshot> storage =
                    selectedRuntime is null
                        ? AdminObservabilityViewState.EmptyValue<StorageRuntimeDiagnosticsSnapshot>(
                            runtime.Availability,
                            StatusFor(runtime.Availability))
                        : ProjectSection(selectedRuntime.Storage);
                AdminObservabilityValue<WalRuntimeDiagnosticsSnapshot> wal =
                    selectedRuntime is null
                        ? AdminObservabilityViewState.EmptyValue<WalRuntimeDiagnosticsSnapshot>(
                            runtime.Availability,
                            StatusFor(runtime.Availability))
                        : ProjectSection(selectedRuntime.Wal);

                if (selectedRuntime is not null)
                    AppendSampleLocked(selectedRuntime, recentQueries);

                DateTimeOffset? lastSuccess = selectedRuntime is null
                    ? _current.LastSuccessfulRefreshUtc
                    : SafeUtcNow();
                bool runtimeSectionFailure = selectedRuntime is not null &&
                    (selectedRuntime.Queries.Availability != DiagnosticsAvailability.Available ||
                     selectedRuntime.Connections.Availability != DiagnosticsAvailability.Available ||
                     selectedRuntime.Storage.Availability != DiagnosticsAvailability.Available ||
                     selectedRuntime.Wal.Availability != DiagnosticsAvailability.Available ||
                     selectedRuntime.ActiveMaintenance.Availability != DiagnosticsAvailability.Available ||
                     selectedRuntime.Health.Availability != DiagnosticsAvailability.Available);
                bool anyFailure = runtime.Availability != DiagnosticsAvailability.Available ||
                    runtimeSectionFailure ||
                    storage.Availability != DiagnosticsAvailability.Available ||
                    wal.Availability != DiagnosticsAvailability.Available ||
                    activeQueries.Availability != DiagnosticsAvailability.Available ||
                    recentQueries.Availability != DiagnosticsAvailability.Available ||
                    sessions.Availability != DiagnosticsAvailability.Available ||
                    activeMaintenance.Availability != DiagnosticsAvailability.Available ||
                    recentMaintenance.Availability != DiagnosticsAvailability.Available;

                _current = _current with
                {
                    IsLoading = false,
                    IsRefreshing = false,
                    LastSuccessfulRefreshUtc = lastSuccess,
                    SnapshotCapturedAtUtc = selectedRuntime?.Metadata.CapturedAtUtc ?? _current.SnapshotCapturedAtUtc,
                    StatusText = lastSuccess is null
                        ? StatusFor(runtime.Availability)
                        : anyFailure ? "Updated with unavailable sections" : "Up to date",
                    HasUnavailableSections = anyFailure,
                    ScopeOptions = BuildScopeOptions(runtimeCapture.Value, scope),
                    ShardCapacity = runtimeCapture.Value?.ShardCapacity,
                    DroppedShardCount = runtimeCapture.Value?.DroppedShardCount ?? 0,
                    ShardsTruncated = runtimeCapture.Value?.ShardsTruncated == true,
                    Runtime = runtime,
                    Storage = storage,
                    Wal = wal,
                    ActiveQueries = activeQueries,
                    RecentQueries = recentQueries,
                    Sessions = sessions,
                    ActiveMaintenance = activeMaintenance,
                    RecentMaintenance = recentMaintenance,
                    Samples = _samples.ToArray(),
                };
                if (selectedRuntime is not null)
                    StartStaleNotificationLocked();
            }
            RaiseStateChanged();
        }
        catch (OperationCanceledException) when (activeToken.IsCancellationRequested || callerToken.IsCancellationRequested)
        {
            bool changed = false;
            lock (_sync)
            {
                if (CanPublishLocked(generation))
                {
                    _current = _current with { IsLoading = false, IsRefreshing = false };
                    changed = true;
                }
            }
            if (changed)
                RaiseStateChanged();
        }
        finally
        {
            _refreshGate.Release();
        }
    }

    private async Task LoadValueAsync<T>(
        Func<CancellationToken, Task<DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<T>>>> load,
        Func<AdminObservabilityViewState, AdminObservabilityValue<T>, AdminObservabilityViewState> set,
        SensitiveRequestKind requestKind,
        CancellationToken callerToken)
        where T : class, IRuntimeDiagnosticsSnapshot
    {
        long generation;
        string? scope;
        CancellationToken activeToken;
        CancellationToken requestToken;
        long requestGeneration;
        lock (_sync)
        {
            ThrowIfDisposed();
            if (!_active || _activeCancellation is null || !IsTabActiveLocked())
                return;
            generation = _generation;
            scope = _current.SelectedScope;
            activeToken = _activeCancellation.Token;
            if (requestKind == SensitiveRequestKind.Plan)
            {
                CancelAndDispose(ref _planCancellation);
                _planCancellation = new CancellationTokenSource();
                requestToken = _planCancellation.Token;
                requestGeneration = ++_planGeneration;
            }
            else
            {
                CancelAndDispose(ref _detailCancellation);
                _detailCancellation = new CancellationTokenSource();
                requestToken = _detailCancellation.Token;
                requestGeneration = ++_detailGeneration;
            }
            _current = set(_current, AdminObservabilityViewState.EmptyValue<T>(statusText: "Loading"));
            if (typeof(T) == typeof(QueryPlanDiagnosticsSnapshot))
                _current = _current with { HasPlanRequest = true };
            if (typeof(T) == typeof(QueryDetailSnapshot))
                _current = _current with { HasDetailRequest = true };
        }
        RaiseStateChanged();

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(activeToken, requestToken, callerToken);
        Capture<DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<T>>> capture;
        try
        {
            capture = await CaptureAsync(() => load(linked.Token), linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (
            requestToken.IsCancellationRequested || activeToken.IsCancellationRequested)
        {
            return;
        }
        AdminObservabilityValue<T> projected = ProjectValue(capture, scope);

        lock (_sync)
        {
            if (!CanPublishLocked(generation) ||
                !IsCurrentRequestLocked(requestKind, requestGeneration) ||
                !string.Equals(scope, _current.SelectedScope, StringComparison.Ordinal))
                return;
            if (projected.Value is not null && !MatchesCurrentIdentityLocked(projected.Value.Metadata))
            {
                projected = AdminObservabilityViewState.EmptyValue<T>(
                    DiagnosticsAvailability.Unavailable,
                    "The diagnostics identity changed; request again");
            }
            _current = set(_current, projected);
        }
        RaiseStateChanged();
    }

    private AdminObservabilityValue<RuntimeDiagnosticsSnapshot> ProjectRuntime(
        Capture<DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>> capture,
        string? scope,
        out RuntimeDiagnosticsSnapshot? selected)
    {
        if (capture.Value is null)
        {
            selected = null;
            return AdminObservabilityViewState.EmptyValue<RuntimeDiagnosticsSnapshot>(
                capture.FailureAvailability,
                StatusFor(capture.FailureAvailability));
        }

        Selection<RuntimeDiagnosticsSnapshot> selection = Select(capture.Value, scope);
        DiagnosticsAvailability availability = selection.Value?.Metadata.Availability ?? selection.Availability;
        selected = availability == DiagnosticsAvailability.Available ? selection.Value : null;
        bool topologyTruncated = capture.Value.ShardsTruncated == true;
        string status = StatusFor(availability);
        if (topologyTruncated)
            status += "; shard list truncated";
        return new AdminObservabilityValue<RuntimeDiagnosticsSnapshot>(
            availability,
            availability == DiagnosticsAvailability.Available ? selection.Value : null,
            selection.Value?.Metadata.FieldsTruncated == true,
            status);
    }

    private static AdminObservabilityValue<T> ProjectValue<T>(
        Capture<DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<T>>> capture,
        string? scope)
        where T : class, IRuntimeDiagnosticsSnapshot
    {
        if (capture.Value is null)
        {
            return AdminObservabilityViewState.EmptyValue<T>(
                capture.FailureAvailability,
                StatusFor(capture.FailureAvailability));
        }
        Selection<DiagnosticsValueSnapshot<T>> selection = Select(capture.Value, scope);
        DiagnosticsAvailability availability = selection.Value?.Metadata.Availability ?? selection.Availability;
        return new AdminObservabilityValue<T>(
            availability,
            availability == DiagnosticsAvailability.Available ? selection.Value?.Value : null,
            selection.Value?.Metadata.FieldsTruncated == true,
            StatusFor(availability));
    }

    private static AdminObservabilityCollection<T> ProjectCollection<T>(
        Capture<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<T>>> capture,
        string? scope)
        where T : class, IRuntimeDiagnosticsSnapshot
    {
        if (capture.Value is null)
        {
            return AdminObservabilityViewState.EmptyCollection<T>(
                capture.FailureAvailability,
                StatusFor(capture.FailureAvailability));
        }
        Selection<DiagnosticsCollectionSnapshot<T>> selection = Select(capture.Value, scope);
        DiagnosticsAvailability availability = selection.Value?.Metadata.Availability ?? selection.Availability;
        DiagnosticsCollectionSnapshot<T>? value = selection.Value;
        bool recordsTruncated = value?.IsTruncated == true || value?.Metadata.RecordsTruncated == true;
        bool fieldsTruncated = value?.Metadata.FieldsTruncated == true;
        string status = StatusFor(availability);
        if (fieldsTruncated)
            status += "; fields truncated";
        return new AdminObservabilityCollection<T>(
            availability,
            availability == DiagnosticsAvailability.Available
                ? value?.Records ?? Array.Empty<T>()
                : Array.Empty<T>(),
            recordsTruncated,
            value?.DroppedCount ?? 0,
            status)
        {
            FieldsTruncated = fieldsTruncated,
        };
    }

    private static AdminObservabilityValue<T> ProjectSection<T>(DiagnosticsSection<T>? section)
        where T : class, IRuntimeDiagnosticsSnapshot
    {
        DiagnosticsAvailability availability = section?.Availability ?? DiagnosticsAvailability.Unavailable;
        return new AdminObservabilityValue<T>(
            availability,
            availability == DiagnosticsAvailability.Available ? section?.Value : null,
            section?.Value?.Metadata.FieldsTruncated == true,
            StatusFor(availability));
    }

    private static Selection<T> Select<T>(DiagnosticsTopologySnapshot<T> topology, string? scope)
        where T : class, IRuntimeDiagnosticsSnapshot
    {
        if (scope is null)
            return new Selection<T>(topology.Aggregate.Metadata.Availability, topology.Aggregate);
        ShardDiagnosticsSection<T>? shard = topology.Shards?.FirstOrDefault(
            candidate => string.Equals(candidate.ShardAlias, scope, StringComparison.Ordinal));
        return shard is null
            ? new Selection<T>(DiagnosticsAvailability.Unavailable, null)
            : new Selection<T>(shard.Availability, shard.Value);
    }

    private static IReadOnlyList<AdminObservabilityScopeOption> BuildScopeOptions(
        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>? topology,
        string? selectedScope)
    {
        if (topology is null)
            return Array.Empty<AdminObservabilityScopeOption>();
        var result = new List<AdminObservabilityScopeOption>
        {
            new(null, "Aggregate", topology.Aggregate.Metadata.Availability),
        };
        if (topology.Shards is not null)
        {
            result.AddRange(topology.Shards.Select(shard => new AdminObservabilityScopeOption(
                shard.ShardAlias,
                shard.ShardAlias,
                shard.Value?.Metadata.Availability ?? shard.Availability)));
        }
        if (selectedScope is not null &&
            !result.Any(option => string.Equals(option.Value, selectedScope, StringComparison.Ordinal)))
        {
            result.Add(new AdminObservabilityScopeOption(
                selectedScope,
                selectedScope,
                DiagnosticsAvailability.Unavailable));
        }
        return result;
    }

    private bool IdentityOrCountersChanged(RuntimeDiagnosticsSnapshot runtime)
    {
        DiagnosticsSnapshotMetadata metadata = runtime.Metadata;
        CounterPoint current = CounterPoint.From(runtime);
        bool identityChanged = _serverInstanceId is not null &&
            (!string.Equals(_serverInstanceId, metadata.ServerInstanceId, StringComparison.Ordinal) ||
             !string.Equals(_databaseAlias, metadata.DatabaseAlias, StringComparison.Ordinal) ||
             _counterEpoch != metadata.CounterEpoch);
        bool regression = _previousCounters is { } previous && current.HasRegressionFrom(previous);

        _serverInstanceId = metadata.ServerInstanceId;
        _databaseAlias = metadata.DatabaseAlias;
        _counterEpoch = metadata.CounterEpoch;
        return identityChanged || regression;
    }

    private void AppendSampleLocked(
        RuntimeDiagnosticsSnapshot runtime,
        AdminObservabilityCollection<RecentQuerySnapshot> recentQueries)
    {
        CounterPoint current = CounterPoint.From(runtime);
        CounterPoint? previous = _previousCounters;
        double elapsedSeconds = previous is null
            ? 0
            : (runtime.Metadata.CapturedAtUtc - previous.Value.CapturedAtUtc).TotalSeconds;
        double? queryRate = elapsedSeconds > 0 && current.RequestCount is { } requests && previous?.RequestCount is { } oldRequests
            ? (requests - oldRequests) / elapsedSeconds
            : null;
        double? errorRate = elapsedSeconds > 0 && current.ErrorCount is { } errors && previous?.ErrorCount is { } oldErrors
            ? (errors - oldErrors) / elapsedSeconds
            : null;
        double? walGrowth = elapsedSeconds > 0 && current.WalLogicalBytes is { } wal && previous?.WalLogicalBytes is { } oldWal
            ? (wal - oldWal) / elapsedSeconds
            : null;
        double? averageLatency = recentQueries.Availability != DiagnosticsAvailability.Available ||
            recentQueries.Records.Count == 0
            ? null
            : recentQueries.Records.Average(record => record.Duration.TotalMilliseconds);

        _samples.Add(new AdminObservabilityMetricSample(
            runtime.Metadata.CapturedAtUtc,
            queryRate is null ? null : Math.Max(0, queryRate.Value),
            errorRate is null ? null : Math.Max(0, errorRate.Value),
            averageLatency is null ? null : Math.Max(0, averageLatency.Value),
            walGrowth));
        if (_samples.Count > _options.SampleCapacity)
            _samples.RemoveRange(0, _samples.Count - _options.SampleCapacity);
        _previousCounters = current;
    }

    private void ResetIdentityLocked(bool clearOrdinaryData)
    {
        _serverInstanceId = null;
        _databaseAlias = null;
        _counterEpoch = null;
        ResetSamplesAndSensitiveLocked();
        if (clearOrdinaryData)
        {
            _current = _current with
            {
                Runtime = AdminObservabilityViewState.EmptyValue<RuntimeDiagnosticsSnapshot>(),
                Storage = AdminObservabilityViewState.EmptyValue<StorageRuntimeDiagnosticsSnapshot>(),
                Wal = AdminObservabilityViewState.EmptyValue<WalRuntimeDiagnosticsSnapshot>(),
                ActiveQueries = AdminObservabilityViewState.EmptyCollection<ActiveQuerySnapshot>(),
                RecentQueries = AdminObservabilityViewState.EmptyCollection<RecentQuerySnapshot>(),
                Sessions = AdminObservabilityViewState.EmptyCollection<SessionDiagnosticsSnapshot>(),
                ActiveMaintenance = AdminObservabilityViewState.EmptyCollection<MaintenanceOperationSnapshot>(),
                RecentMaintenance = AdminObservabilityViewState.EmptyCollection<MaintenanceOperationSnapshot>(),
                HasUnavailableSections = false,
            };
        }
    }

    private void ResetSamplesAndSensitiveLocked()
    {
        _previousCounters = null;
        _samples.Clear();
        ClearSensitiveLocked();
        _current = _current with { Samples = Array.Empty<AdminObservabilityMetricSample>() };
    }

    private void ClearSensitiveLocked()
    {
        _planGeneration++;
        _detailGeneration++;
        CancelAndDispose(ref _planCancellation);
        CancelAndDispose(ref _detailCancellation);
        _current = _current with
        {
            SelectedPlan = AdminObservabilityViewState.EmptyValue<QueryPlanDiagnosticsSnapshot>(
                DiagnosticsAvailability.NotApplicable,
                "Not requested"),
            RevealedDetail = AdminObservabilityViewState.EmptyValue<QueryDetailSnapshot>(
                DiagnosticsAvailability.NotApplicable,
                "Not requested"),
            HasPlanRequest = false,
            HasDetailRequest = false,
        };
    }

    private bool CanPublishLocked(long generation)
        => !_disposed && _active && IsTabActiveLocked() && _generation == generation;

    private bool IsTabActiveLocked()
        => _tabManager is null || _tabManager.ActiveTab?.Kind == TabKind.Observability;

    private bool IsCurrentRequestLocked(SensitiveRequestKind kind, long requestGeneration)
        => kind == SensitiveRequestKind.Plan
            ? _planGeneration == requestGeneration
            : _detailGeneration == requestGeneration;

    private bool MatchesCurrentIdentityLocked(DiagnosticsSnapshotMetadata metadata)
        => _serverInstanceId is null ||
           (string.Equals(_serverInstanceId, metadata.ServerInstanceId, StringComparison.Ordinal) &&
            string.Equals(_databaseAlias, metadata.DatabaseAlias, StringComparison.Ordinal) &&
            _counterEpoch == metadata.CounterEpoch);

    private void ThrowIfDisposed()
        => ObjectDisposedException.ThrowIf(_disposed, this);

    private void RaiseStateChanged()
    {
        Action? handler;
        lock (_sync)
            handler = _disposed ? null : StateChanged;
        if (handler is null)
            return;

        foreach (Delegate subscriber in handler.GetInvocationList())
        {
            try { ((Action)subscriber)(); }
            catch { }
        }
    }

    private void OnDatabaseChanged()
    {
        lock (_sync)
        {
            if (_disposed)
                return;

            _generation++;
            lock (_admissionSync)
            {
                _admissionOpen = false;
                _admissionGeneration = _generation;
                CancelAndDispose(ref _pollCancellation);
                CancelAndDispose(ref _activeCancellation);
                CancelAndDispose(ref _staleCancellation);
            }
            if (_active)
                _activeCancellation = new CancellationTokenSource();
            ResetIdentityLocked(clearOrdinaryData: true);
            _current = _current with
            {
                IsRefreshing = false,
                LastSuccessfulRefreshUtc = null,
                SnapshotCapturedAtUtc = null,
                SnapshotAge = null,
                IsStale = false,
                SelectedScope = null,
                ScopeNotice = "Database changed; showing aggregate diagnostics.",
                ScopeOptions = Array.Empty<AdminObservabilityScopeOption>(),
                ShardCapacity = null,
                DroppedShardCount = 0,
                ShardsTruncated = false,
                IsLoading = _active && !_current.IsPaused,
                StatusText = !_active
                    ? "Inactive"
                    : _current.IsPaused
                        ? "Paused; refresh manually to load the new database"
                        : "Database changed; reconnecting diagnostics",
            };
            if (_active && !_current.IsPaused)
                StartPollingLocked();
            lock (_admissionSync)
            {
                _admissionOpen = _active;
                _admissionGeneration = _generation;
            }
        }
        RaiseStateChanged();
    }

    private void OnTabManagerChanged()
    {
        lock (_sync)
        {
            if (_disposed || !_active || IsTabActiveLocked())
                return;

            _active = false;
            _generation++;
            lock (_admissionSync)
            {
                _admissionOpen = false;
                _admissionGeneration = _generation;
                CancelAndDispose(ref _pollCancellation);
                CancelAndDispose(ref _activeCancellation);
                CancelAndDispose(ref _staleCancellation);
            }
            ClearSensitiveLocked();
            _current = _current with
            {
                IsLoading = false,
                IsRefreshing = false,
                StatusText = "Inactive",
            };
        }
        RaiseStateChanged();
    }

    private static async Task<Capture<T>> CaptureAsync<T>(
        Func<Task<T>> operation,
        CancellationToken cancellationToken)
        where T : class
    {
        try
        {
            return new Capture<T>(await operation().ConfigureAwait(false), DiagnosticsAvailability.Available);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (CSharpDbObservabilityNotSupportedException)
        {
            return new Capture<T>(null, DiagnosticsAvailability.Unsupported);
        }
        catch (CSharpDbObservabilityAccessDeniedException)
        {
            return new Capture<T>(null, DiagnosticsAvailability.Denied);
        }
        catch (UnauthorizedAccessException)
        {
            return new Capture<T>(null, DiagnosticsAvailability.Denied);
        }
        catch
        {
            return new Capture<T>(null, DiagnosticsAvailability.Unavailable);
        }
    }

    private Task<Capture<T>> CaptureAutomaticAsync<T>(
        Func<CancellationToken, Task<T>> operation,
        long generation,
        CancellationToken cancellationToken)
        where T : class
    {
        lock (_admissionSync)
        {
            if (!_admissionOpen || _admissionGeneration != generation)
                return Task.FromResult(new Capture<T>(null, DiagnosticsAvailability.Unavailable));
            // The delegate is invoked while admission is held. Lifecycle
            // closure waits for this short synchronous boundary, so after a
            // hide/switch callback returns no new diagnostics call can begin.
            return CaptureAsync(() => operation(cancellationToken), cancellationToken);
        }
    }

    private static string StatusFor(DiagnosticsAvailability availability)
        => availability switch
        {
            DiagnosticsAvailability.Available => "Available",
            DiagnosticsAvailability.Unsupported => "Unsupported by this server or transport",
            DiagnosticsAvailability.Disabled => "Disabled by server policy",
            DiagnosticsAvailability.Denied => "Access denied",
            DiagnosticsAvailability.NotApplicable => "Not applicable",
            _ => "Unavailable",
        };

    private static TimeSpan NonNegative(TimeSpan value)
        => value < TimeSpan.Zero ? TimeSpan.Zero : value;

    private DateTimeOffset SafeUtcNow()
    {
        try { return _timeProvider.GetUtcNow().ToUniversalTime(); }
        catch { return TimeProvider.System.GetUtcNow(); }
    }

    private static void CancelAndDispose(ref CancellationTokenSource? source)
    {
        CancellationTokenSource? prior = source;
        source = null;
        if (prior is null)
            return;
        try { prior.Cancel(); }
        catch (ObjectDisposedException) { }
        prior.Dispose();
    }

    private readonly record struct Capture<T>(T? Value, DiagnosticsAvailability FailureAvailability)
        where T : class;

    private readonly record struct Selection<T>(DiagnosticsAvailability Availability, T? Value)
        where T : class;

    private enum SensitiveRequestKind
    {
        Plan,
        Detail,
    }

    private readonly record struct CounterPoint(
        DateTimeOffset CapturedAtUtc,
        long? RequestCount,
        long? ErrorCount,
        long? WalLogicalBytes)
    {
        public static CounterPoint From(RuntimeDiagnosticsSnapshot runtime)
        {
            QueryDiagnosticsSummary? queries = runtime.Queries.Value;
            return new CounterPoint(
                runtime.Metadata.CapturedAtUtc,
                queries?.RequestCount,
                queries?.FailedCount,
                runtime.Wal.Value?.LogicalBytes);
        }

        public bool HasRegressionFrom(CounterPoint previous)
            => Less(RequestCount, previous.RequestCount) ||
               Less(ErrorCount, previous.ErrorCount);

        private static bool Less(long? current, long? previous)
            => current.HasValue && previous.HasValue && current.Value < previous.Value;

    }
}
