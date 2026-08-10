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

    public static IDisposable Enter(CSharpDbOperationContext operation)
    {
        ArgumentNullException.ThrowIfNull(operation);
        return Push(new ScopeFrame(s_current.Value, operation, transport: null));
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

    private static ScopeLease Push(
        ScopeFrame frame,
        CSharpDbDiagnosticEventBuffer? ownedDiagnosticEventBuffer = null)
    {
        s_current.Value = frame;
        return new ScopeLease(frame.Token, ownedDiagnosticEventBuffer);
    }

    private static ScopeFrame? RemoveFrame(ScopeFrame? frame, object token)
    {
        if (frame is null)
            return null;
        if (ReferenceEquals(frame.Token, token))
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
                frame.Token);
    }

    private sealed class ScopeFrame
    {
        public ScopeFrame(
            ScopeFrame? parent,
            CSharpDbOperationContext? operation,
            CSharpDbTransport? transport,
            OpaqueDiagnosticsId? sessionId = null,
            bool suppressDiagnostics = false,
            TimeSpan? queryQueueDuration = null,
            CSharpDbDiagnosticEventBuffer? diagnosticEventBuffer = null)
            : this(
                parent,
                operation,
                transport,
                sessionId,
                suppressDiagnostics,
                queryQueueDuration,
                diagnosticEventBuffer,
                new object())
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
            object token)
        {
            Parent = parent;
            Operation = operation;
            Transport = transport;
            SessionId = sessionId;
            SuppressDiagnostics = suppressDiagnostics;
            QueryQueueDuration = queryQueueDuration;
            DiagnosticEventBuffer = diagnosticEventBuffer;
            Token = token;
        }

        public ScopeFrame? Parent { get; }
        public CSharpDbOperationContext? Operation { get; }
        public CSharpDbTransport? Transport { get; }
        public OpaqueDiagnosticsId? SessionId { get; }
        public bool SuppressDiagnostics { get; }
        public TimeSpan? QueryQueueDuration { get; }
        public CSharpDbDiagnosticEventBuffer? DiagnosticEventBuffer { get; }
        public object Token { get; }
    }

    private sealed class ScopeLease(
        object token,
        CSharpDbDiagnosticEventBuffer? diagnosticEventBuffer) : IDisposable
    {
        private object? _token = token;
        private CSharpDbDiagnosticEventBuffer? _diagnosticEventBuffer =
            diagnosticEventBuffer;

        public void Dispose()
        {
            object? activeToken = Interlocked.Exchange(ref _token, null);
            if (activeToken is null)
                return;

            ScopeFrame? current = s_current.Value;
            ScopeFrame? updated = RemoveFrame(current, activeToken);
            if (ReferenceEquals(current, updated))
                return;

            s_current.Value = updated;

            CSharpDbDiagnosticEventBuffer? buffer = Interlocked.Exchange(
                ref _diagnosticEventBuffer,
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
