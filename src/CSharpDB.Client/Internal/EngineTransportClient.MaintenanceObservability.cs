using CSharpDB.Engine;
using CSharpDB.Observability;
using ObservabilityTransport = CSharpDB.Observability.CSharpDbTransport;

namespace CSharpDB.Client.Internal;

internal sealed partial class EngineTransportClient
{
    private MaintenanceRuntimeDiagnostics? _maintenanceRuntimeDiagnostics;
    private int _activeMaintenanceLifetimes;
    private TaskCompletionSource? _maintenanceLifetimesDrained;

    private bool IsClientMaintenanceObservationEnabled()
        => !CSharpDbOperationScope.IsDiagnosticsSuppressed &&
           Volatile.Read(ref _runtimeDatabaseFamily)
               .RuntimeDiagnosticsState?.IsEnabled == true;

    private MaintenanceObservation? StartClientMaintenanceObservation(
        MaintenanceOperationKind kind,
        MaintenanceOperationPhase initialPhase,
        CSharpDbOperationClass operationClass,
        CSharpDbLogEventDefinition<CSharpDbLifecycleCompletedEvent> lifecycleEvent,
        ClientMaintenanceLifetimeLease maintenanceLifetime)
    {
        if (CSharpDbOperationScope.IsDiagnosticsSuppressed)
            return null;

        try
        {
            CSharpDbRuntimeDiagnosticsState? runtimeState =
                maintenanceLifetime.RuntimeState;
            if (runtimeState?.IsEnabled != true)
                return null;

            bool traceRequested = CSharpDbActivityOperation.ShouldStart(
                runtimeState.TracingEnabled);
            CSharpDbActivityOperation? activityOperation = null;
            CSharpDbOperationContext context;
            if (traceRequested)
            {
                activityOperation = CSharpDbActivityOperation.Start(
                    operationClass,
                    (
                        Target: this,
                        RuntimeState: runtimeState,
                        OperationClass: operationClass),
                    static state => state.Target.CreateMaintenanceContext(
                        state.RuntimeState,
                        state.OperationClass),
                    out context);
            }
            else
            {
                context = CreateMaintenanceContext(
                    runtimeState,
                    operationClass);
            }
            LifecycleOperation? lifecycleOperation =
                LifecycleObservability.StartExact(
                    runtimeState.CreateOptionsSnapshot(),
                    lifecycleEvent,
                    operationClass,
                    context,
                    activityOperation,
                    runtimeState);
            MaintenanceRuntimeDiagnostics.MaintenanceRuntimeOperation?
                runtimeOperation = null;
            try
            {
                MaintenanceRuntimeDiagnostics registry =
                    GetOrCreateClientMaintenanceRuntimeDiagnostics(
                        runtimeState);
                runtimeOperation = registry.TryStart(
                    context,
                    kind,
                    initialPhase);
            }
            catch
            {
                // Typed lifecycle logging remains useful if the bounded
                // runtime-history sink cannot be created or has retired.
            }
            if (runtimeOperation is null &&
                lifecycleOperation is null &&
                activityOperation is null)
                return null;

            return new MaintenanceObservation(
                context,
                runtimeOperation,
                lifecycleOperation,
                activityOperation);
        }
        catch
        {
            // Maintenance diagnostics are best-effort and must never alter
            // admission or execution of the maintenance operation itself.
            return null;
        }
    }

    private CSharpDbOperationContext CreateMaintenanceContext(
        CSharpDbRuntimeDiagnosticsState runtimeState,
        CSharpDbOperationClass operationClass)
    {
        CSharpDbOperationContext? parent = CSharpDbOperationScope.Current;
        ObservabilityTransport transport = CSharpDbOperationScope.CurrentTransport;
        OpaqueDiagnosticsId? sessionId = CSharpDbOperationScope.CurrentSessionId;
        if (transport == ObservabilityTransport.Embedded)
        {
            transport = ObservabilityTransport.Direct;
            sessionId = GetOrCreateDiagnosticsSessionId();
        }

        return parent is null
            ? CSharpDbOperationContext.CreateRequest(
                operationClass,
                transport,
                runtimeState.DatabaseAlias,
                sessionId,
                runtimeState.TimeProvider)
            : CSharpDbOperationContext.CreateRequest(
                parent,
                operationClass,
                runtimeState.TimeProvider);
    }

    private MaintenanceRuntimeDiagnostics
        GetOrCreateClientMaintenanceRuntimeDiagnostics(
            CSharpDbRuntimeDiagnosticsState runtimeState)
    {
        MaintenanceRuntimeDiagnostics? existing = Volatile.Read(
            ref _maintenanceRuntimeDiagnostics);
        if (existing is not null)
            return existing;

        var candidate = new MaintenanceRuntimeDiagnostics(
            runtimeState.RecentOperationCapacity,
            runtimeState.RecentOperationRetention,
            runtimeState.TimeProvider);
        existing = Interlocked.CompareExchange(
            ref _maintenanceRuntimeDiagnostics,
            candidate,
            null);
        if (existing is null)
            return candidate;

        candidate.Dispose();
        return existing;
    }

    private MaintenanceRuntimeDiagnostics? TryGetClientMaintenanceDiagnostics()
        => Volatile.Read(ref _maintenanceRuntimeDiagnostics);

    private ClientMaintenanceLifetimeLease RegisterClientMaintenanceLifetime()
    {
        CSharpDbRuntimeDiagnosticsState? runtimeState;
        lock (_disposeGate)
        {
            ObjectDisposedException.ThrowIf(_disposeTask is not null, this);
            if (_activeMaintenanceLifetimes++ == 0)
            {
                _maintenanceLifetimesDrained = new TaskCompletionSource(
                    TaskCreationOptions.RunContinuationsAsynchronously);
            }

        }

        lock (_runtimeDiagnosticsLifetimeGate)
        {
            runtimeState = Volatile.Read(
                ref _runtimeDatabaseFamily).RuntimeDiagnosticsState;
            if (runtimeState is not null)
                RetainRuntimeDiagnosticsStateLocked(runtimeState);
        }

        return new ClientMaintenanceLifetimeLease(this, runtimeState);
    }

    private Task? GetMaintenanceLifetimesDrainedTask()
    {
        lock (_disposeGate)
        {
            return _activeMaintenanceLifetimes == 0
                ? null
                : _maintenanceLifetimesDrained!.Task;
        }
    }

    private void UnregisterClientMaintenanceLifetime()
    {
        TaskCompletionSource? drained = null;
        lock (_disposeGate)
        {
            if (_activeMaintenanceLifetimes <= 0)
                return;

            if (--_activeMaintenanceLifetimes == 0)
            {
                drained = _maintenanceLifetimesDrained;
                _maintenanceLifetimesDrained = null;
            }
        }

        drained?.TrySetResult();
    }

    private void DisposeClientMaintenanceDiagnostics()
        => Interlocked.Exchange(
            ref _maintenanceRuntimeDiagnostics,
            null)?.Dispose();

    private sealed class ClientMaintenanceLifetimeLease : IDisposable
    {
        private EngineTransportClient? _owner;
        private CSharpDbRuntimeDiagnosticsState? _runtimeState;

        internal ClientMaintenanceLifetimeLease(
            EngineTransportClient owner,
            CSharpDbRuntimeDiagnosticsState? runtimeState)
        {
            _owner = owner;
            _runtimeState = runtimeState;
        }

        internal CSharpDbRuntimeDiagnosticsState? RuntimeState =>
            Volatile.Read(ref _runtimeState);

        public void Dispose()
        {
            EngineTransportClient? owner = Interlocked.Exchange(
                ref _owner,
                null);
            if (owner is null)
                return;

            CSharpDbRuntimeDiagnosticsState? runtimeState =
                Interlocked.Exchange(ref _runtimeState, null);
            try
            {
                if (runtimeState is not null)
                {
                    owner.ReleaseRuntimeDiagnosticsStateOwnership(runtimeState);
                }
            }
            finally
            {
                owner.UnregisterClientMaintenanceLifetime();
            }
        }
    }
}
