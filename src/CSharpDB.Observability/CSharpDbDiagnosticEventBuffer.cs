namespace CSharpDB.Observability;

/// <summary>
/// Defers synchronous DiagnosticListener callbacks until the owning client or
/// host boundary has released its serialization locks. Delivery is bounded and
/// best-effort, matching the diagnostic publisher's no-throw contract.
/// </summary>
internal sealed class CSharpDbDiagnosticEventBuffer
{
    // One composite request can execute at most 4,096 statements. Each
    // statement and its parent have one required terminal, one once-only
    // long-running notification, and independently one optional slow-query
    // terminal event. Separate budgets prevent either secondary family from
    // displacing required outcomes or each other. Headroom covers an enclosing
    // logical operation and future additive boundary events without relying on
    // a brittle total-count constant.
    private const int SupportedCompositeStatementCount = 4_096;
    private const int CompositeParentCount = 1;
    private const int BoundaryHeadroom = 128;
    private const int MaximumTerminalEvents =
        SupportedCompositeStatementCount + CompositeParentCount + BoundaryHeadroom;
    private const int MaximumOptionalEvents =
        SupportedCompositeStatementCount + CompositeParentCount + BoundaryHeadroom;
    private const int MaximumLongRunningEvents =
        SupportedCompositeStatementCount + CompositeParentCount + BoundaryHeadroom;
    private const int MaximumOperationalEvents = BoundaryHeadroom;

    private readonly object _gate = new();
    private readonly CSharpDbDiagnosticEventPublisher _publisher;
    private readonly HashSet<string> _enabledEventNames;
    private readonly Queue<BufferedDiagnosticEvent> _terminalEvents = new();
    private readonly Queue<BufferedDiagnosticEvent> _longRunningEvents = new();
    private readonly Queue<BufferedDiagnosticEvent> _optionalEvents = new();
    private readonly Queue<BufferedDiagnosticEvent> _operationalEvents = new();
    private long _nextSequence;

    internal CSharpDbDiagnosticEventBuffer(
        CSharpDbDiagnosticEventPublisher publisher,
        HashSet<string> enabledEventNames)
    {
        _publisher = publisher ?? throw new ArgumentNullException(nameof(publisher));
        _enabledEventNames = enabledEventNames ??
            throw new ArgumentNullException(nameof(enabledEventNames));
    }

    internal bool IsOwnedBy(CSharpDbDiagnosticEventPublisher publisher)
        => ReferenceEquals(_publisher, publisher);

    internal bool IsEnabled(string eventName)
        => _enabledEventNames.Contains(eventName);

    internal CSharpDbDiagnosticEventBuffer CreateChild()
        => new(_publisher, _enabledEventNames);

    internal void Enqueue(string eventName, object payload)
    {
        lock (_gate)
        {
            var item = new BufferedDiagnosticEvent(
                _nextSequence++,
                eventName,
                payload);
            switch (Classify(eventName))
            {
                case BufferedEventClass.QueryTerminal:
                    EnqueueBounded(
                        _terminalEvents,
                        MaximumTerminalEvents,
                        item);
                    break;
                case BufferedEventClass.Optional:
                    // Optional overflow is deterministic: retain the newest
                    // slow events, including the composite parent's event,
                    // and discard the oldest optional entry only.
                    EnqueueBounded(
                        _optionalEvents,
                        MaximumOptionalEvents,
                        item);
                    break;
                case BufferedEventClass.LongRunning:
                    // A long-running notification is once-only and cannot be
                    // reconstructed after a deferred boundary flushes. Keep a
                    // dedicated composite-sized budget so final slow events do
                    // not evict it at the maximum statement fan-out.
                    EnqueueBounded(
                        _longRunningEvents,
                        MaximumLongRunningEvents,
                        item);
                    break;
                default:
                    // Operational headroom is isolated from query terminals;
                    // an excessive unsupported stream retains its newest state.
                    EnqueueBounded(
                        _operationalEvents,
                        MaximumOperationalEvents,
                        item);
                    break;
            }
        }
    }

    private static void EnqueueBounded(
        Queue<BufferedDiagnosticEvent> events,
        int capacity,
        BufferedDiagnosticEvent item)
    {
        if (events.Count == capacity)
            events.Dequeue();

        events.Enqueue(item);
    }

    internal void MergeInto(CSharpDbDiagnosticEventBuffer parent)
    {
        ArgumentNullException.ThrowIfNull(parent);
        foreach (BufferedDiagnosticEvent item in Drain())
            parent.Enqueue(item.EventName, item.Payload);
    }

    internal void Flush(CSharpDbDiagnosticEventPublisher publisher)
    {
        ArgumentNullException.ThrowIfNull(publisher);
        foreach (BufferedDiagnosticEvent item in Drain())
            publisher.WriteBuffered(item.EventName, item.Payload);
    }

    private BufferedDiagnosticEvent[] Drain()
    {
        lock (_gate)
        {
            int count =
                _terminalEvents.Count +
                _longRunningEvents.Count +
                _optionalEvents.Count +
                _operationalEvents.Count;
            if (count == 0)
                return [];

            var items = new BufferedDiagnosticEvent[count];
            int offset = 0;
            _terminalEvents.CopyTo(items, offset);
            offset += _terminalEvents.Count;
            _longRunningEvents.CopyTo(items, offset);
            offset += _longRunningEvents.Count;
            _optionalEvents.CopyTo(items, offset);
            offset += _optionalEvents.Count;
            _operationalEvents.CopyTo(items, offset);
            Array.Sort(
                items,
                static (left, right) => left.Sequence.CompareTo(right.Sequence));

            _terminalEvents.Clear();
            _longRunningEvents.Clear();
            _optionalEvents.Clear();
            _operationalEvents.Clear();
            return items;
        }
    }

    private static BufferedEventClass Classify(string eventName)
    {
        if (string.Equals(
                eventName,
                CSharpDbLogEvents.LongRunningQuery.Name,
                StringComparison.Ordinal))
        {
            return BufferedEventClass.LongRunning;
        }

        if (string.Equals(
                eventName,
                CSharpDbLogEvents.SlowQuery.Name,
                StringComparison.Ordinal))
        {
            return BufferedEventClass.Optional;
        }

        if (string.Equals(
                eventName,
                CSharpDbLogEvents.QueryCompleted.Name,
                StringComparison.Ordinal) ||
            string.Equals(
                eventName,
                CSharpDbLogEvents.QueryFailed.Name,
                StringComparison.Ordinal) ||
            string.Equals(
                eventName,
                CSharpDbLogEvents.QueryCanceled.Name,
                StringComparison.Ordinal))
        {
            return BufferedEventClass.QueryTerminal;
        }

        return BufferedEventClass.Operational;
    }

    private readonly record struct BufferedDiagnosticEvent(
        long Sequence,
        string EventName,
        object Payload);

    private enum BufferedEventClass
    {
        QueryTerminal,
        LongRunning,
        Optional,
        Operational,
    }
}
