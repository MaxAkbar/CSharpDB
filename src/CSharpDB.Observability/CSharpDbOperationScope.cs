namespace CSharpDB.Observability;

/// <summary>
/// Carries immutable operation correlation through asynchronous execution.
/// Transport-only scopes let HTTP and gRPC hosts establish their boundary
/// before a query operation exists.
/// </summary>
public static class CSharpDbOperationScope
{
    private static readonly AsyncLocal<ScopeFrame?> s_current = new();

    public static CSharpDbOperationContext? Current
    {
        get
        {
            for (ScopeFrame? frame = s_current.Value; frame is not null; frame = frame.Parent)
            {
                if (frame.Operation is not null)
                    return frame.Operation;
            }

            return null;
        }
    }

    public static CSharpDbTransport CurrentTransport
    {
        get
        {
            for (ScopeFrame? frame = s_current.Value; frame is not null; frame = frame.Parent)
            {
                if (frame.Transport is CSharpDbTransport transport)
                    return transport;
                if (frame.Operation is not null)
                    return frame.Operation.Transport;
            }

            return CSharpDbTransport.Embedded;
        }
    }

    public static OpaqueDiagnosticsId? CurrentSessionId
    {
        get
        {
            for (ScopeFrame? frame = s_current.Value; frame is not null; frame = frame.Parent)
            {
                if (frame.SessionId is not null)
                    return frame.SessionId;
                if (frame.Operation?.SessionId is not null)
                    return frame.Operation.SessionId;
            }

            return null;
        }
    }

    /// <summary>
    /// Gets the time the current logical query spent waiting before it could
    /// begin execution. Boundary adapters set this only after admission so the
    /// engine can include pre-dispatch waits without changing the operation's
    /// identity or start time.
    /// </summary>
    public static TimeSpan CurrentQueryQueueDuration
    {
        get
        {
            for (ScopeFrame? frame = s_current.Value; frame is not null; frame = frame.Parent)
            {
                if (frame.QueryQueueDuration is TimeSpan duration)
                    return duration;
            }

            return TimeSpan.Zero;
        }
    }

    /// <summary>
    /// Gets whether the current asynchronous flow is executing client or host
    /// housekeeping that must not be counted as a user database operation.
    /// </summary>
    public static bool IsDiagnosticsSuppressed
    {
        get
        {
            for (ScopeFrame? frame = s_current.Value; frame is not null; frame = frame.Parent)
            {
                if (frame.SuppressDiagnostics)
                    return true;
            }

            return false;
        }
    }

    /// <summary>
    /// Gets whether the current flow should retain runtime history while
    /// suppressing duplicate built-in diagnostic events owned by an outer
    /// logical operation boundary.
    /// </summary>
    internal static bool AreDiagnosticEventsSuppressed
    {
        get
        {
            for (ScopeFrame? frame = s_current.Value; frame is not null; frame = frame.Parent)
            {
                if (frame.SuppressDiagnosticEvents)
                    return true;
            }

            return false;
        }
    }

    public static IDisposable Enter(CSharpDbOperationContext operation)
    {
        ArgumentNullException.ThrowIfNull(operation);
        return Push(new ScopeFrame(s_current.Value, operation, transport: null));
    }

    /// <summary>
    /// Carries an internal runtime-registry lease with the public correlation
    /// context so an outer admission layer and Engine can transfer one active
    /// record without exposing the implementation in public contracts.
    /// </summary>
    internal static IDisposable Enter(
        CSharpDbOperationContext operation,
        object queryRuntimeOperation)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(queryRuntimeOperation);
        return Push(new ScopeFrame(
            s_current.Value,
            operation,
            transport: null,
            queryRuntimeOperation: queryRuntimeOperation));
    }

    internal static IDisposable Enter(
        CSharpDbOperationContext operation,
        object? queryRuntimeOperation,
        CSharpDbActivityOperation? activityOperation)
    {
        ArgumentNullException.ThrowIfNull(operation);
        return Push(new ScopeFrame(
            s_current.Value,
            operation,
            transport: null,
            queryRuntimeOperation: queryRuntimeOperation,
            activityOperation: activityOperation));
    }

    internal static IDisposable Enter(
        CSharpDbOperationContext operation,
        CSharpDbActivityOperation? activityOperation)
    {
        ArgumentNullException.ThrowIfNull(operation);
        return Push(new ScopeFrame(
            s_current.Value,
            operation,
            transport: null,
            activityOperation: activityOperation));
    }

    /// <summary>
    /// Carries the listener-interest decision made by an outer serialized
    /// adapter together with the exact operation frame. Engine adoption uses
    /// this immutable snapshot instead of consulting listeners again after an
    /// admission wait.
    /// </summary>
    internal static IDisposable Enter(
        CSharpDbOperationContext operation,
        object? queryRuntimeOperation,
        CSharpDbQueryEventInterestSnapshot queryEventInterest,
        CSharpDbDeferredDiagnosticBoundary? queryEventBoundary = null,
        CSharpDbActivityOperation? activityOperation = null)
    {
        ArgumentNullException.ThrowIfNull(operation);
        return Push(new ScopeFrame(
            s_current.Value,
            operation,
            transport: null,
            queryRuntimeOperation: queryRuntimeOperation,
            queryEventInterest: queryEventInterest,
            queryEventBoundary: queryEventBoundary,
            activityOperation: activityOperation));
    }

    public static IDisposable EnterTransport(CSharpDbTransport transport)
        => EnterTransport(transport, sessionId: null);

    /// <summary>
    /// Carries host transport and session correlation without owning a
    /// diagnostic delivery buffer. Inner serialization boundaries therefore
    /// snapshot, defer, and flush their events independently.
    /// </summary>
    public static IDisposable EnterTransport(
        CSharpDbTransport transport,
        OpaqueDiagnosticsId? sessionId)
    {
        if (transport == CSharpDbTransport.Unknown || !Enum.IsDefined(transport))
            throw new ArgumentOutOfRangeException(nameof(transport));

        return Push(new ScopeFrame(
            s_current.Value,
            operation: null,
            transport,
            sessionId));
    }

    public static IDisposable EnterBoundary(
        CSharpDbTransport transport,
        OpaqueDiagnosticsId? sessionId = null)
    {
        if (transport == CSharpDbTransport.Unknown || !Enum.IsDefined(transport))
            throw new ArgumentOutOfRangeException(nameof(transport));

        CSharpDbDiagnosticEventBuffer buffer = CreateDiagnosticEventBuffer();
        return Push(
            new ScopeFrame(
                s_current.Value,
                operation: null,
                transport,
                sessionId,
                diagnosticEventBuffer: buffer),
            buffer);
    }

    /// <summary>
    /// Makes a measured pre-execution wait available to query instrumentation
    /// in the current asynchronous flow. The duration is correlation metadata;
    /// it does not create a new operation or change transport/session scopes.
    /// </summary>
    public static IDisposable EnterQueryQueueDuration(TimeSpan queueDuration)
    {
        if (queueDuration < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(queueDuration));

        return Push(new ScopeFrame(
            s_current.Value,
            operation: null,
            transport: null,
            queryQueueDuration: queueDuration));
    }

    /// <summary>
    /// Suppresses built-in diagnostics for internal housekeeping on the
    /// current asynchronous flow. It does not alter execution or logging
    /// performed explicitly by the application.
    /// </summary>
    public static IDisposable SuppressDiagnostics()
        => Push(new ScopeFrame(
            s_current.Value,
            operation: null,
            transport: null,
            suppressDiagnostics: true));

    /// <summary>
    /// Suppresses built-in listener events without suppressing config-driven
    /// runtime counters and bounded histories.
    /// </summary>
    internal static IDisposable SuppressDiagnosticEvents()
        => Push(new ScopeFrame(
            s_current.Value,
            operation: null,
            transport: null,
            suppressDiagnosticEvents: true));

    internal static CSharpDbDeferredDiagnosticBoundary CreateDeferredBoundary(
        CSharpDbTransport transport,
        OpaqueDiagnosticsId? sessionId = null)
    {
        if (transport == CSharpDbTransport.Unknown || !Enum.IsDefined(transport))
            throw new ArgumentOutOfRangeException(nameof(transport));

        return new CSharpDbDeferredDiagnosticBoundary(
            transport,
            sessionId,
            CreateDiagnosticEventBuffer());
    }

    internal static IDisposable EnterDeferredBoundary(
        CSharpDbDeferredDiagnosticBoundary boundary)
    {
        ArgumentNullException.ThrowIfNull(boundary);
        return Push(new ScopeFrame(
            s_current.Value,
            operation: null,
            boundary.Transport,
            boundary.SessionId,
            diagnosticEventBuffer: boundary.Buffer));
    }

    internal static CSharpDbDiagnosticEventBuffer? CurrentDiagnosticEventBuffer
        => FindDiagnosticEventBuffer(s_current.Value);

    internal static object? CurrentQueryRuntimeOperation
    {
        get
        {
            for (ScopeFrame? frame = s_current.Value; frame is not null; frame = frame.Parent)
            {
                // Boundary, transport, and queue-duration frames do not
                // establish an operation, so they may be skipped. The first
                // operation frame is authoritative: a distinct child without
                // a lease must never inherit its parent's runtime owner.
                if (frame.Operation is not null)
                    return frame.QueryRuntimeOperation;
            }

            return null;
        }
    }

    /// <summary>
    /// Captures the authoritative operation and its exact runtime lease from
    /// one ambient-frame walk. Planner callbacks use the pair together and
    /// must never combine a nested operation with a parent's runtime lease.
    /// </summary>
    internal static CSharpDbQueryRuntimeBinding CaptureQueryRuntimeBinding()
    {
        for (ScopeFrame? frame = s_current.Value; frame is not null; frame = frame.Parent)
        {
            if (frame.Operation is not null)
            {
                return new CSharpDbQueryRuntimeBinding(
                    frame.Operation,
                    frame.QueryRuntimeOperation);
            }
        }

        return default;
    }

    internal static CSharpDbQueryEventInterestSnapshot?
        CurrentQueryEventInterest
    {
        get
        {
            for (ScopeFrame? frame = s_current.Value; frame is not null; frame = frame.Parent)
            {
                // Like the runtime lease, listener interest belongs only to
                // the first operation frame. A nested operation cannot inherit
                // a parent adapter's start-time decision.
                if (frame.Operation is not null)
                    return frame.QueryEventInterest;
            }

            return null;
        }
    }

    internal static CSharpDbDeferredDiagnosticBoundary? CurrentQueryEventBoundary
    {
        get
        {
            for (ScopeFrame? frame = s_current.Value; frame is not null; frame = frame.Parent)
            {
                if (frame.Operation is not null)
                    return frame.QueryEventBoundary;
            }

            return null;
        }
    }

    internal static CSharpDbActivityOperation? CurrentActivityOperation
    {
        get
        {
            for (ScopeFrame? frame = s_current.Value; frame is not null; frame = frame.Parent)
            {
                if (frame.Operation is not null)
                    return frame.ActivityOperation;
            }

            return null;
        }
    }

    /// <summary>
    /// Captures the ambient values needed to begin one Engine query while
    /// walking the AsyncLocal scope chain once. Individual public accessors
    /// remain available for callers that need only one value.
    /// </summary>
    internal static CSharpDbQueryScopeSnapshot CaptureQueryScope()
    {
        CSharpDbOperationContext? operation = null;
        CSharpDbTransport transport = CSharpDbTransport.Embedded;
        OpaqueDiagnosticsId? sessionId = null;
        TimeSpan queryQueueDuration = TimeSpan.Zero;
        object? queryRuntimeOperation = null;
        CSharpDbQueryEventInterestSnapshot? queryEventInterest = null;
        CSharpDbDeferredDiagnosticBoundary? queryEventBoundary = null;
        CSharpDbActivityOperation? activityOperation = null;
        bool operationCaptured = false;
        bool transportCaptured = false;
        bool sessionCaptured = false;
        bool queueDurationCaptured = false;
        bool suppressDiagnostics = false;
        bool suppressDiagnosticEvents = false;

        for (ScopeFrame? frame = s_current.Value; frame is not null; frame = frame.Parent)
        {
            suppressDiagnostics |= frame.SuppressDiagnostics;
            suppressDiagnosticEvents |= frame.SuppressDiagnosticEvents;

            if (!transportCaptured)
            {
                if (frame.Transport is CSharpDbTransport frameTransport)
                {
                    transport = frameTransport;
                    transportCaptured = true;
                }
                else if (frame.Operation is not null)
                {
                    transport = frame.Operation.Transport;
                    transportCaptured = true;
                }
            }

            if (!sessionCaptured)
            {
                OpaqueDiagnosticsId? frameSessionId =
                    frame.SessionId ?? frame.Operation?.SessionId;
                if (frameSessionId is not null)
                {
                    sessionId = frameSessionId;
                    sessionCaptured = true;
                }
            }

            if (!queueDurationCaptured &&
                frame.QueryQueueDuration is TimeSpan frameQueueDuration)
            {
                queryQueueDuration = frameQueueDuration;
                queueDurationCaptured = true;
            }

            if (!operationCaptured && frame.Operation is not null)
            {
                operation = frame.Operation;
                queryRuntimeOperation = frame.QueryRuntimeOperation;
                queryEventInterest = frame.QueryEventInterest;
                queryEventBoundary = frame.QueryEventBoundary;
                activityOperation = frame.ActivityOperation;
                operationCaptured = true;
            }
        }

        return new CSharpDbQueryScopeSnapshot(
            operation,
            transport,
            sessionId,
            queryQueueDuration,
            suppressDiagnostics,
            suppressDiagnosticEvents,
            queryRuntimeOperation,
            queryEventInterest,
            queryEventBoundary,
            activityOperation);
    }

    private static CSharpDbDiagnosticEventBuffer CreateDiagnosticEventBuffer()
    {
        CSharpDbDiagnosticEventBuffer? parent = FindDiagnosticEventBuffer(s_current.Value);
        return parent?.CreateChild() ??
            CSharpDbDiagnostics.EventPublisher.CreateBoundaryBuffer();
    }

    private static CSharpDbDiagnosticEventBuffer? FindDiagnosticEventBuffer(
        ScopeFrame? frame)
    {
        for (; frame is not null; frame = frame.Parent)
        {
            if (frame.DiagnosticEventBuffer is not null)
                return frame.DiagnosticEventBuffer;
        }

        return null;
    }

    private static ScopeFrame Push(
        ScopeFrame frame,
        CSharpDbDiagnosticEventBuffer? ownedDiagnosticEventBuffer = null)
    {
        frame.SetOwnedDiagnosticEventBuffer(ownedDiagnosticEventBuffer);
        s_current.Value = frame;
        return frame;
    }

    private static ScopeFrame? RemoveFrame(ScopeFrame? frame, ScopeFrame token)
    {
        if (frame is null)
            return null;
        if (ReferenceEquals(frame.RemovalToken, token))
            return frame.Parent;

        ScopeFrame? newParent = RemoveFrame(frame.Parent, token);
        return ReferenceEquals(newParent, frame.Parent)
            ? frame
            : new ScopeFrame(
                newParent,
                frame.Operation,
                frame.Transport,
                frame.SessionId,
                frame.SuppressDiagnostics,
                frame.QueryQueueDuration,
                frame.DiagnosticEventBuffer,
                frame.QueryRuntimeOperation,
                frame.QueryEventInterest,
                frame.QueryEventBoundary,
                frame.ActivityOperation,
                frame.SuppressDiagnosticEvents,
                frame.RemovalToken);
    }

    private sealed class ScopeFrame : IDisposable
    {
        private CSharpDbDiagnosticEventBuffer? _ownedDiagnosticEventBuffer;
        private int _disposed;

        public ScopeFrame(
            ScopeFrame? parent,
            CSharpDbOperationContext? operation,
            CSharpDbTransport? transport,
            OpaqueDiagnosticsId? sessionId = null,
            bool suppressDiagnostics = false,
            TimeSpan? queryQueueDuration = null,
            CSharpDbDiagnosticEventBuffer? diagnosticEventBuffer = null,
            object? queryRuntimeOperation = null,
            CSharpDbQueryEventInterestSnapshot? queryEventInterest = null,
            CSharpDbDeferredDiagnosticBoundary? queryEventBoundary = null,
            CSharpDbActivityOperation? activityOperation = null,
            bool suppressDiagnosticEvents = false)
            : this(
                parent,
                operation,
                transport,
                sessionId,
                suppressDiagnostics,
                queryQueueDuration,
                diagnosticEventBuffer,
                queryRuntimeOperation,
                queryEventInterest,
                queryEventBoundary,
                activityOperation,
                suppressDiagnosticEvents,
                removalToken: null)
        {
        }

        public ScopeFrame(
            ScopeFrame? parent,
            CSharpDbOperationContext? operation,
            CSharpDbTransport? transport,
            OpaqueDiagnosticsId? sessionId,
            bool suppressDiagnostics,
            TimeSpan? queryQueueDuration,
            CSharpDbDiagnosticEventBuffer? diagnosticEventBuffer,
            object? queryRuntimeOperation,
            CSharpDbQueryEventInterestSnapshot? queryEventInterest,
            CSharpDbDeferredDiagnosticBoundary? queryEventBoundary,
            CSharpDbActivityOperation? activityOperation,
            bool suppressDiagnosticEvents,
            ScopeFrame? removalToken)
        {
            Parent = parent;
            Operation = operation;
            Transport = transport;
            SessionId = sessionId;
            SuppressDiagnostics = suppressDiagnostics;
            QueryQueueDuration = queryQueueDuration;
            DiagnosticEventBuffer = diagnosticEventBuffer;
            QueryRuntimeOperation = queryRuntimeOperation;
            QueryEventInterest = queryEventInterest;
            QueryEventBoundary = queryEventBoundary;
            ActivityOperation = activityOperation;
            SuppressDiagnosticEvents = suppressDiagnosticEvents;
            RemovalToken = removalToken ?? this;
        }

        public ScopeFrame? Parent { get; }
        public CSharpDbOperationContext? Operation { get; }
        public CSharpDbTransport? Transport { get; }
        public OpaqueDiagnosticsId? SessionId { get; }
        public bool SuppressDiagnostics { get; }
        public TimeSpan? QueryQueueDuration { get; }
        public CSharpDbDiagnosticEventBuffer? DiagnosticEventBuffer { get; }
        public object? QueryRuntimeOperation { get; }
        public CSharpDbQueryEventInterestSnapshot? QueryEventInterest { get; }
        public CSharpDbDeferredDiagnosticBoundary? QueryEventBoundary { get; }
        public CSharpDbActivityOperation? ActivityOperation { get; }
        public bool SuppressDiagnosticEvents { get; }
        public ScopeFrame RemovalToken { get; }

        internal void SetOwnedDiagnosticEventBuffer(
            CSharpDbDiagnosticEventBuffer? diagnosticEventBuffer)
            => _ownedDiagnosticEventBuffer = diagnosticEventBuffer;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;

            ScopeFrame? current = s_current.Value;
            ScopeFrame? updated = RemoveFrame(current, this);
            if (ReferenceEquals(current, updated))
                return;

            s_current.Value = updated;

            CSharpDbDiagnosticEventBuffer? buffer = Interlocked.Exchange(
                ref _ownedDiagnosticEventBuffer,
                null);
            if (buffer is null)
                return;

            CSharpDbDiagnosticEventBuffer? parent =
                FindDiagnosticEventBuffer(updated);
            if (parent is null)
                buffer.Flush(CSharpDbDiagnostics.EventPublisher);
            else
                buffer.MergeInto(parent);
        }
    }
}

/// <summary>
/// Immutable internal listener-interest decision associated with one query
/// operation. This is correlation metadata, not a subscription or payload.
/// </summary>
internal readonly record struct CSharpDbQueryEventInterestSnapshot(
    bool QueryEventsEnabled,
    bool SlowQueryEventsEnabled,
    bool LongRunningQueryEventsEnabled);

internal readonly record struct CSharpDbQueryScopeSnapshot(
    CSharpDbOperationContext? Operation,
    CSharpDbTransport Transport,
    OpaqueDiagnosticsId? SessionId,
    TimeSpan QueryQueueDuration,
    bool IsDiagnosticsSuppressed,
    bool AreDiagnosticEventsSuppressed,
    object? QueryRuntimeOperation,
    CSharpDbQueryEventInterestSnapshot? QueryEventInterest,
    CSharpDbDeferredDiagnosticBoundary? QueryEventBoundary,
    CSharpDbActivityOperation? ActivityOperation);

internal readonly record struct CSharpDbQueryRuntimeBinding(
    CSharpDbOperationContext? Operation,
    object? RuntimeOperation);
