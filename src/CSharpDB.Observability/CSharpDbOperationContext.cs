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

    public static CSharpDbOperationContext CreateStatement(
        CSharpDbOperationContext parent,
        QueryFingerprint queryFingerprint)
    {
        ArgumentNullException.ThrowIfNull(parent);
        ArgumentNullException.ThrowIfNull(queryFingerprint);

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

    public long Value => Interlocked.Read(ref _value);

    public long Advance() => Interlocked.Increment(ref _value);
}
