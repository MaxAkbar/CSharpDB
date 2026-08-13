using CSharpDB.Observability;

namespace CSharpDB.Api.Diagnostics;

/// <summary>
/// Identity-envelope-free host request state. A diagnostics endpoint stamps
/// runtime identity and capture metadata only after combining contributors.
/// </summary>
internal sealed record HostRequestDiagnosticsRawSnapshot(
    OpaqueDiagnosticsId SessionId,
    DateTimeOffset CreatedAtUtc,
    DateTimeOffset LastActiveAtUtc,
    CSharpDbTransport Transport,
    // Only correlation already ambient when this boundary was registered.
    // Capture never reads another request's AsyncLocal state.
    OpaqueDiagnosticsId? CurrentOperationId);

internal sealed record HostRequestDiagnosticsRawCollection(
    IReadOnlyList<HostRequestDiagnosticsRawSnapshot> Records,
    int Capacity,
    long DroppedCount,
    bool IsTruncated);

internal interface ICSharpDbHostRequestDiagnosticsContributor
{
    HostRequestDiagnosticsRawCollection Capture();
}

/// <summary>
/// Enabled-only, bounded process-local registry of in-flight HTTP and gRPC
/// request sessions. It never accepts transport request objects, so paths,
/// headers, SQL, remote addresses, and bearer capabilities cannot enter state.
/// </summary>
internal sealed class CSharpDbHostRequestDiagnostics :
    ICSharpDbHostRequestDiagnosticsContributor
{
    private readonly object _gate = new();
    private readonly Dictionary<string, RequestState> _requests =
        new(StringComparer.Ordinal);
    private readonly int _capacity;
    private readonly TimeProvider _timeProvider;
    private long _droppedCount;
    private long _lastUtcTicks;

    internal CSharpDbHostRequestDiagnostics(
        int capacity,
        TimeProvider? timeProvider = null)
    {
        if (capacity is <= 0 or > CSharpDbObservabilityOptions.MaximumActiveOperationCapacity)
        {
            throw new ArgumentOutOfRangeException(
                nameof(capacity),
                capacity,
                $"Host request diagnostics capacity must be between 1 and {CSharpDbObservabilityOptions.MaximumActiveOperationCapacity}.");
        }

        _capacity = capacity;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _lastUtcTicks = GetInitialUtcNow().UtcTicks;
    }

    internal IDisposable? TryBeginRequest(
        OpaqueDiagnosticsId sessionId,
        CSharpDbTransport transport,
        OpaqueDiagnosticsId? currentOperationId)
    {
        ArgumentNullException.ThrowIfNull(sessionId);
        if (transport is CSharpDbTransport.Unknown or CSharpDbTransport.Embedded ||
            !Enum.IsDefined(transport))
        {
            throw new ArgumentOutOfRangeException(nameof(transport));
        }

        DateTimeOffset createdAtUtc = GetUtcNowOrLast();
        lock (_gate)
        {
            if (_requests.Count >= _capacity ||
                _requests.ContainsKey(sessionId.Value))
            {
                _droppedCount = SaturatingIncrement(_droppedCount);
                return null;
            }

            _requests.Add(
                sessionId.Value,
                new RequestState(
                    sessionId,
                    createdAtUtc,
                    createdAtUtc,
                    transport,
                    currentOperationId));
        }

        return new RequestLease(this, sessionId.Value);
    }

    public HostRequestDiagnosticsRawCollection Capture()
    {
        RequestStateCopy[] copies;
        long droppedCount;
        lock (_gate)
        {
            copies = new RequestStateCopy[_requests.Count];
            int index = 0;
            foreach (RequestState state in _requests.Values)
                copies[index++] = state.Copy();
            droppedCount = _droppedCount;
        }

        Array.Sort(
            copies,
            static (left, right) => StringComparer.Ordinal.Compare(
                left.SessionId.Value,
                right.SessionId.Value));
        var records = new HostRequestDiagnosticsRawSnapshot[copies.Length];
        for (int index = 0; index < copies.Length; index++)
        {
            RequestStateCopy copy = copies[index];
            records[index] = new HostRequestDiagnosticsRawSnapshot(
                copy.SessionId,
                copy.CreatedAtUtc,
                copy.LastActiveAtUtc,
                copy.Transport,
                copy.CurrentOperationId);
        }

        return new HostRequestDiagnosticsRawCollection(
            Array.AsReadOnly(records),
            _capacity,
            droppedCount,
            droppedCount > 0);
    }

    private void CompleteRequest(string sessionId)
    {
        lock (_gate)
            _requests.Remove(sessionId);
    }

    private DateTimeOffset GetInitialUtcNow()
    {
        try
        {
            return NormalizeUtc(_timeProvider.GetUtcNow());
        }
        catch
        {
            return DateTimeOffset.UnixEpoch;
        }
    }

    private DateTimeOffset GetUtcNowOrLast()
    {
        try
        {
            DateTimeOffset now = NormalizeUtc(_timeProvider.GetUtcNow());
            Interlocked.Exchange(ref _lastUtcTicks, now.UtcTicks);
            return now;
        }
        catch
        {
            return new DateTimeOffset(
                Volatile.Read(ref _lastUtcTicks),
                TimeSpan.Zero);
        }
    }

    private static DateTimeOffset NormalizeUtc(DateTimeOffset value)
        => value.Offset == TimeSpan.Zero
            ? value
            : value.ToUniversalTime();

    private static long SaturatingIncrement(long value)
        => value == long.MaxValue ? long.MaxValue : value + 1;

    private sealed class RequestState(
        OpaqueDiagnosticsId sessionId,
        DateTimeOffset createdAtUtc,
        DateTimeOffset lastActiveAtUtc,
        CSharpDbTransport transport,
        OpaqueDiagnosticsId? currentOperationId)
    {
        internal OpaqueDiagnosticsId SessionId { get; } = sessionId;
        internal DateTimeOffset CreatedAtUtc { get; } = createdAtUtc;
        internal DateTimeOffset LastActiveAtUtc { get; } = lastActiveAtUtc;
        internal CSharpDbTransport Transport { get; } = transport;
        internal OpaqueDiagnosticsId? CurrentOperationId { get; } =
            currentOperationId;

        internal RequestStateCopy Copy()
            => new(
                SessionId,
                CreatedAtUtc,
                LastActiveAtUtc,
                Transport,
                CurrentOperationId);
    }

    private readonly record struct RequestStateCopy(
        OpaqueDiagnosticsId SessionId,
        DateTimeOffset CreatedAtUtc,
        DateTimeOffset LastActiveAtUtc,
        CSharpDbTransport Transport,
        OpaqueDiagnosticsId? CurrentOperationId);

    private sealed class RequestLease(
        CSharpDbHostRequestDiagnostics owner,
        string sessionId) : IDisposable
    {
        private CSharpDbHostRequestDiagnostics? _owner = owner;

        public void Dispose()
            => Interlocked.Exchange(ref _owner, null)?.CompleteRequest(sessionId);
    }
}
