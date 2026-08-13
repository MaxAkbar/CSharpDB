namespace CSharpDB.Observability;

/// <summary>
/// Internal reusable diagnostic boundary for operations whose consumption
/// outlives the call that admitted them. Listener interest is snapshotted when
/// the boundary is created, before an owning serialization gate is acquired.
/// Entries only make that snapshot ambient; the owner explicitly flushes once
/// after its gate has been released.
/// </summary>
internal sealed class CSharpDbDeferredDiagnosticBoundary : IDisposable
{
    private readonly CSharpDbDiagnosticEventBuffer _buffer;
    private readonly object _stateGate = new();
    private readonly TaskCompletionSource _flushCompletion = new(
        TaskCreationOptions.RunContinuationsAsynchronously);
    private int _activeEntries;
    private bool _disposeRequested;
    private bool _flushClaimed;

    internal CSharpDbDeferredDiagnosticBoundary(
        CSharpDbTransport transport,
        OpaqueDiagnosticsId? sessionId,
        CSharpDbDiagnosticEventBuffer buffer)
    {
        Transport = transport;
        SessionId = sessionId;
        _buffer = buffer ?? throw new ArgumentNullException(nameof(buffer));
    }

    internal CSharpDbTransport Transport { get; }

    internal OpaqueDiagnosticsId? SessionId { get; }

    internal CSharpDbDiagnosticEventBuffer Buffer => _buffer;

    internal Task FlushCompletion => _flushCompletion.Task;

    internal IDisposable Enter()
    {
        lock (_stateGate)
        {
            // A retained lifetime deliberately keeps a deferred boundary open
            // after its outer adapter has requested disposal. Forward-only
            // consumers may therefore re-enter it until the final owner
            // releases that lifetime and the flush is claimed.
            if (_flushClaimed)
            {
                throw new ObjectDisposedException(
                    nameof(CSharpDbDeferredDiagnosticBoundary));
            }

            _activeEntries++;
        }

        try
        {
            return new EntryLease(
                this,
                CSharpDbOperationScope.EnterDeferredBoundary(this));
        }
        catch
        {
            ExitEntry();
            throw;
        }
    }

    internal IDisposable? TryAcquireLifetime()
    {
        lock (_stateGate)
        {
            if (_flushClaimed)
                return null;

            _activeEntries++;
            return new EntryLease(this, scope: null);
        }
    }

    public void Dispose()
    {
        bool flush;
        lock (_stateGate)
        {
            if (_disposeRequested)
                return;

            _disposeRequested = true;
            flush = TryClaimFlush();
        }

        if (flush)
            Flush();
    }

    private void ExitEntry()
    {
        bool flush;
        lock (_stateGate)
        {
            _activeEntries--;
            flush = TryClaimFlush();
        }

        if (flush)
            Flush();
    }

    private bool TryClaimFlush()
    {
        if (!_disposeRequested || _activeEntries != 0 || _flushClaimed)
        {
            return false;
        }

        _flushClaimed = true;
        return true;
    }

    private void Flush()
    {
        try
        {
            _buffer.Flush(CSharpDbDiagnostics.EventPublisher);
        }
        finally
        {
            _flushCompletion.TrySetResult();
        }
    }

    private sealed class EntryLease(
        CSharpDbDeferredDiagnosticBoundary owner,
        IDisposable? scope) : IDisposable
    {
        private CSharpDbDeferredDiagnosticBoundary? _owner = owner;
        private IDisposable? _scope = scope;

        public void Dispose()
        {
            CSharpDbDeferredDiagnosticBoundary? activeOwner =
                Interlocked.Exchange(ref _owner, null);
            if (activeOwner is null)
                return;

            try
            {
                Interlocked.Exchange(ref _scope, null)?.Dispose();
            }
            finally
            {
                activeOwner.ExitEntry();
            }
        }
    }
}
