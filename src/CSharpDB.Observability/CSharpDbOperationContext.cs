using System.Diagnostics;

namespace CSharpDB.Observability;

public sealed record CSharpDbOperationContext
{
    private readonly TimeProvider _timeProvider;

    private CSharpDbOperationContext(
        OpaqueDiagnosticsId operationId,
        OpaqueDiagnosticsId? parentOperationId,
        CSharpDbOperationClass operationClass,
        CSharpDbOperationRole role,
        DateTimeOffset startedAtUtc,
        long startingTimestamp,
        DiagnosticsTraceId? traceId,
        CSharpDbTransport transport,
        string databaseAlias,
        OpaqueDiagnosticsId? sessionId,
        QueryFingerprint? queryFingerprint,
        TimeProvider timeProvider)
    {
        OperationId = operationId;
        ParentOperationId = parentOperationId;
        OperationClass = operationClass;
        Role = role;
        StartedAtUtc = startedAtUtc;
        StartingTimestamp = startingTimestamp;
        TraceId = traceId;
        Transport = transport;
        DatabaseAlias = databaseAlias;
        SessionId = sessionId;
        QueryFingerprint = queryFingerprint;
        _timeProvider = timeProvider;
    }

    public OpaqueDiagnosticsId OperationId { get; }
    public OpaqueDiagnosticsId? ParentOperationId { get; }
    public CSharpDbOperationClass OperationClass { get; }
    public CSharpDbOperationRole Role { get; }
    public DateTimeOffset StartedAtUtc { get; }
    public long StartingTimestamp { get; }
    public DiagnosticsTraceId? TraceId { get; }
    public CSharpDbTransport Transport { get; }
    public string DatabaseAlias { get; }
    public OpaqueDiagnosticsId? SessionId { get; }
    public QueryFingerprint? QueryFingerprint { get; }

    public bool CountsAsRequest
        => Role is CSharpDbOperationRole.Root or CSharpDbOperationRole.Request;

    public bool CountsAsStatement
        => OperationClass == CSharpDbOperationClass.Query &&
           Role is CSharpDbOperationRole.Root or CSharpDbOperationRole.Statement;

    public static CSharpDbOperationContext CreateRoot(
        CSharpDbOperationClass operationClass,
        CSharpDbTransport transport,
        string databaseAlias,
        OpaqueDiagnosticsId? sessionId = null,
        QueryFingerprint? queryFingerprint = null,
        TimeProvider? timeProvider = null)
        => Create(
            parentOperationId: null,
            operationClass,
            CSharpDbOperationRole.Root,
            transport,
            databaseAlias,
            sessionId,
            queryFingerprint,
            CaptureCurrentTraceId(),
            timeProvider);

    public static CSharpDbOperationContext CreateRequest(
        CSharpDbOperationClass operationClass,
        CSharpDbTransport transport,
        string databaseAlias,
        OpaqueDiagnosticsId? sessionId = null,
        TimeProvider? timeProvider = null)
        => Create(
            parentOperationId: null,
            operationClass,
            CSharpDbOperationRole.Request,
            transport,
            databaseAlias,
            sessionId,
            queryFingerprint: null,
            CaptureCurrentTraceId(),
            timeProvider);

    public static CSharpDbOperationContext CreateRequest(
        CSharpDbOperationContext parent,
        CSharpDbOperationClass operationClass)
    {
        ArgumentNullException.ThrowIfNull(parent);

        return Create(
            parent.OperationId,
            operationClass,
            CSharpDbOperationRole.Request,
            parent.Transport,
            parent.DatabaseAlias,
            parent.SessionId,
            queryFingerprint: null,
            parent.TraceId,
            parent._timeProvider);
    }

    internal static CSharpDbOperationContext CreateRequest(
        CSharpDbOperationContext parent,
        CSharpDbOperationClass operationClass,
        TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(parent);
        ArgumentNullException.ThrowIfNull(timeProvider);

        return Create(
            parent.OperationId,
            operationClass,
            CSharpDbOperationRole.Request,
            parent.Transport,
            parent.DatabaseAlias,
            parent.SessionId,
            queryFingerprint: null,
            parent.TraceId,
            timeProvider);
    }

    public static CSharpDbOperationContext CreateStatement(
        CSharpDbOperationContext parent,
        QueryFingerprint queryFingerprint)
    {
        ArgumentNullException.ThrowIfNull(parent);
        ArgumentNullException.ThrowIfNull(queryFingerprint);

        return CreateStatementCore(parent, queryFingerprint);
    }

    public static CSharpDbOperationContext CreateStatement(CSharpDbOperationContext parent)
    {
        ArgumentNullException.ThrowIfNull(parent);
        return CreateStatementCore(parent, queryFingerprint: null);
    }

    internal static CSharpDbOperationContext CreateStatement(
        CSharpDbOperationContext parent,
        QueryFingerprint? queryFingerprint,
        TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(parent);
        ArgumentNullException.ThrowIfNull(timeProvider);
        return Create(
            parent.OperationId,
            CSharpDbOperationClass.Query,
            CSharpDbOperationRole.Statement,
            parent.Transport,
            parent.DatabaseAlias,
            parent.SessionId,
            queryFingerprint,
            parent.TraceId,
            timeProvider);
    }

    public static CSharpDbOperationContext CreateInternal(
        CSharpDbOperationContext parent,
        CSharpDbOperationClass operationClass,
        CSharpDbTransport transport,
        string databaseAlias,
        QueryFingerprint? queryFingerprint = null)
    {
        ArgumentNullException.ThrowIfNull(parent);

        return Create(
            parent.OperationId,
            operationClass,
            CSharpDbOperationRole.Internal,
            transport,
            databaseAlias,
            parent.SessionId,
            queryFingerprint,
            parent.TraceId,
            parent._timeProvider);
    }

    private static CSharpDbOperationContext CreateStatementCore(
        CSharpDbOperationContext parent,
        QueryFingerprint? queryFingerprint)
    {
        return Create(
            parent.OperationId,
            CSharpDbOperationClass.Query,
            CSharpDbOperationRole.Statement,
            parent.Transport,
            parent.DatabaseAlias,
            parent.SessionId,
            queryFingerprint,
            parent.TraceId,
            parent._timeProvider);
    }

    public TimeSpan GetElapsedTime()
        => _timeProvider.GetElapsedTime(StartingTimestamp);

    internal long GetTimestamp()
        => _timeProvider.GetTimestamp();

    internal TimeSpan GetElapsedTime(long endingTimestamp)
        => _timeProvider.GetElapsedTime(StartingTimestamp, endingTimestamp);

    /// <summary>
    /// Rebinds only trace correlation after a logical operation activity has
    /// been started. Identity, timing, parentage, and value semantics remain
    /// unchanged; callers must use this before any runtime registry observes
    /// the context.
    /// </summary>
    internal CSharpDbOperationContext WithCurrentTraceId()
    {
        DiagnosticsTraceId? traceId = CaptureCurrentTraceId();
        if (EqualityComparer<DiagnosticsTraceId?>.Default.Equals(
                TraceId,
                traceId))
        {
            return this;
        }

        return new CSharpDbOperationContext(
            OperationId,
            ParentOperationId,
            OperationClass,
            Role,
            StartedAtUtc,
            StartingTimestamp,
            traceId,
            Transport,
            DatabaseAlias,
            SessionId,
            QueryFingerprint,
            _timeProvider);
    }

    public DateTimeOffset GetUtcNow()
        => _timeProvider.GetUtcNow();

    internal static CSharpDbOperationContext CreateCapturedRoot(
        OpaqueDiagnosticsId operationId,
        CSharpDbTransport transport,
        string databaseAlias,
        QueryFingerprint? queryFingerprint,
        TimeProvider timeProvider,
        DateTimeOffset startedAtUtc,
        long startingTimestamp)
        => CreateCapturedRoot(
            operationId,
            CSharpDbOperationClass.Query,
            transport,
            databaseAlias,
            queryFingerprint,
            timeProvider,
            startedAtUtc,
            startingTimestamp);

    internal static CSharpDbOperationContext CreateCapturedRoot(
        OpaqueDiagnosticsId operationId,
        CSharpDbOperationClass operationClass,
        CSharpDbTransport transport,
        string databaseAlias,
        QueryFingerprint? queryFingerprint,
        TimeProvider timeProvider,
        DateTimeOffset startedAtUtc,
        long startingTimestamp)
    {
        ArgumentNullException.ThrowIfNull(operationId);
        ArgumentNullException.ThrowIfNull(timeProvider);
        if (operationClass == CSharpDbOperationClass.Unknown ||
            !Enum.IsDefined(operationClass))
        {
            throw new ArgumentOutOfRangeException(nameof(operationClass));
        }
        if (transport == CSharpDbTransport.Unknown || !Enum.IsDefined(transport))
            throw new ArgumentOutOfRangeException(nameof(transport));
        if (!CSharpDbObservabilityOptions.IsValidDatabaseAlias(databaseAlias))
            throw new ArgumentException("A safe database alias is required.", nameof(databaseAlias));

        return new CSharpDbOperationContext(
            operationId,
            parentOperationId: null,
            operationClass,
            CSharpDbOperationRole.Root,
            startedAtUtc,
            startingTimestamp,
            traceId: null,
            transport,
            databaseAlias,
            sessionId: null,
            queryFingerprint,
            timeProvider);
    }

    /// <summary>
    /// Runtime ownership is mutable implementation state and must not change
    /// this public record's immutable correlation value semantics.
    /// </summary>
    public bool Equals(CSharpDbOperationContext? other)
        => other is not null &&
           EqualityContract == other.EqualityContract &&
           EqualityComparer<TimeProvider>.Default.Equals(
               _timeProvider,
               other._timeProvider) &&
           EqualityComparer<OpaqueDiagnosticsId>.Default.Equals(
               OperationId,
               other.OperationId) &&
           EqualityComparer<OpaqueDiagnosticsId?>.Default.Equals(
               ParentOperationId,
               other.ParentOperationId) &&
           OperationClass == other.OperationClass &&
           Role == other.Role &&
           StartedAtUtc == other.StartedAtUtc &&
           StartingTimestamp == other.StartingTimestamp &&
           EqualityComparer<DiagnosticsTraceId?>.Default.Equals(
               TraceId,
               other.TraceId) &&
           Transport == other.Transport &&
           string.Equals(DatabaseAlias, other.DatabaseAlias, StringComparison.Ordinal) &&
           EqualityComparer<OpaqueDiagnosticsId?>.Default.Equals(
               SessionId,
               other.SessionId) &&
           EqualityComparer<QueryFingerprint?>.Default.Equals(
               QueryFingerprint,
               other.QueryFingerprint);

    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(EqualityContract);
        hash.Add(_timeProvider);
        hash.Add(OperationId);
        hash.Add(ParentOperationId);
        hash.Add(OperationClass);
        hash.Add(Role);
        hash.Add(StartedAtUtc);
        hash.Add(StartingTimestamp);
        hash.Add(TraceId);
        hash.Add(Transport);
        hash.Add(DatabaseAlias, StringComparer.Ordinal);
        hash.Add(SessionId);
        hash.Add(QueryFingerprint);
        return hash.ToHashCode();
    }

    /// <summary>
    /// Claims the single runtime-history owner for this exact logical
    /// operation. The claim lives with the immutable correlation context so
    /// duplicate registries do not need a per-operation weak-table entry.
    /// A registry rebind transfers the already-held claim explicitly.
    /// </summary>
    internal bool TryClaimRuntimeDiagnostics(object owner)
    {
        ArgumentNullException.ThrowIfNull(owner);
        return OperationId.TryClaimRuntimeDiagnostics(owner);
    }

    internal bool TryTransferRuntimeDiagnostics(
        object previousOwner,
        object newOwner)
    {
        ArgumentNullException.ThrowIfNull(previousOwner);
        ArgumentNullException.ThrowIfNull(newOwner);

        return OperationId.TryTransferRuntimeDiagnostics(previousOwner, newOwner);
    }

    internal void ReleaseRuntimeDiagnostics(object owner)
    {
        ArgumentNullException.ThrowIfNull(owner);
        OperationId.ReleaseRuntimeDiagnostics(owner);
    }

    private static CSharpDbOperationContext Create(
        OpaqueDiagnosticsId? parentOperationId,
        CSharpDbOperationClass operationClass,
        CSharpDbOperationRole role,
        CSharpDbTransport transport,
        string databaseAlias,
        OpaqueDiagnosticsId? sessionId,
        QueryFingerprint? queryFingerprint,
        DiagnosticsTraceId? traceId,
        TimeProvider? timeProvider)
    {
        if (operationClass == CSharpDbOperationClass.Unknown || !Enum.IsDefined(operationClass))
            throw new ArgumentOutOfRangeException(nameof(operationClass));
        if (transport == CSharpDbTransport.Unknown || !Enum.IsDefined(transport))
            throw new ArgumentOutOfRangeException(nameof(transport));
        if (!CSharpDbObservabilityOptions.IsValidDatabaseAlias(databaseAlias))
            throw new ArgumentException("A safe database alias is required.", nameof(databaseAlias));

        TimeProvider clock = timeProvider ?? TimeProvider.System;
        return new CSharpDbOperationContext(
            OpaqueDiagnosticsId.Create(),
            parentOperationId,
            operationClass,
            role,
            clock.GetUtcNow(),
            clock.GetTimestamp(),
            traceId,
            transport,
            databaseAlias,
            sessionId,
            queryFingerprint,
            clock);
    }

    private static DiagnosticsTraceId? CaptureCurrentTraceId()
    {
        ActivityTraceId currentTraceId = Activity.Current?.TraceId ?? default;
        return currentTraceId == default
            ? null
            : DiagnosticsTraceId.FromActivityTraceId(currentTraceId);
    }
}

public sealed class CSharpDbCounterEpoch
{
    private long _value;

    public CSharpDbCounterEpoch()
    {
    }

    internal CSharpDbCounterEpoch(long initialValue)
    {
        if (initialValue < 0)
            throw new ArgumentOutOfRangeException(nameof(initialValue));

        _value = initialValue;
    }

    public long Value => Interlocked.Read(ref _value);

    public long Advance()
    {
        while (true)
        {
            long current = Interlocked.Read(ref _value);
            if (current == long.MaxValue)
                return current;

            long next = current + 1;
            if (Interlocked.CompareExchange(ref _value, next, current) == current)
                return next;
        }
    }
}
