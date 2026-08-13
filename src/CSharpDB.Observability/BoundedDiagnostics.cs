using System.Collections.ObjectModel;
using System.Text.Json.Serialization;

namespace CSharpDB.Observability;

public sealed class BoundedDiagnosticsSnapshot<T>
{
    [JsonConstructor]
    public BoundedDiagnosticsSnapshot(
        IReadOnlyList<T> records,
        long droppedCount,
        bool isTruncated)
    {
        ArgumentNullException.ThrowIfNull(records);
        if (droppedCount < 0)
            throw new ArgumentOutOfRangeException(nameof(droppedCount));
        if (droppedCount > 0 && !isTruncated)
            throw new ArgumentException("A snapshot with dropped records must be marked truncated.", nameof(isTruncated));

        Records = new ReadOnlyCollection<T>(records.ToArray());
        DroppedCount = droppedCount;
        IsTruncated = isTruncated;
    }

    public BoundedDiagnosticsSnapshot(
        IEnumerable<T> records,
        long droppedCount,
        bool isTruncated)
        : this(
            records?.ToArray() ?? throw new ArgumentNullException(nameof(records)),
            droppedCount,
            isTruncated)
    {
    }

    public IReadOnlyList<T> Records { get; }
    public long DroppedCount { get; }
    public bool IsTruncated { get; }
}

/// <summary>
/// A process-local, capacity- and retention-bounded history. Records are
/// returned newest first and are copied before publication.
/// </summary>
public sealed class BoundedDiagnosticHistory<T>
{
    private readonly object _gate = new();
    private readonly int _capacity;
    private readonly TimeSpan _retention;
    private readonly TimeProvider _timeProvider;
    private readonly Queue<Entry> _entries;
    private long _droppedCount;

    public BoundedDiagnosticHistory(
        int capacity,
        TimeSpan retention,
        TimeProvider? timeProvider = null)
    {
        if (capacity <= 0 || capacity > CSharpDbObservabilityOptions.MaximumHistoryCapacity)
            throw new ArgumentOutOfRangeException(nameof(capacity));
        if (retention <= TimeSpan.Zero || retention > CSharpDbObservabilityOptions.MaximumRetention)
            throw new ArgumentOutOfRangeException(nameof(retention));

        _capacity = capacity;
        _retention = retention;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _entries = new Queue<Entry>(capacity);
    }

    public int Capacity => _capacity;
    public TimeSpan Retention => _retention;

    public void Add(T record)
    {
        long now = _timeProvider.GetTimestamp();
        lock (_gate)
        {
            PruneExpired(now);
            while (_entries.Count >= _capacity)
            {
                _entries.Dequeue();
                _droppedCount++;
            }

            _entries.Enqueue(new Entry(now, record));
        }
    }

    public BoundedDiagnosticsSnapshot<T> GetSnapshot()
    {
        lock (_gate)
        {
            PruneExpired(_timeProvider.GetTimestamp());
            T[] records = _entries.Reverse().Select(static entry => entry.Value).ToArray();
            return new BoundedDiagnosticsSnapshot<T>(
                records,
                _droppedCount,
                _droppedCount > 0);
        }
    }

    private void PruneExpired(long now)
    {
        while (_entries.TryPeek(out Entry? entry) &&
               entry is not null &&
               _timeProvider.GetElapsedTime(entry.RecordedAtTimestamp, now) > _retention)
        {
            _entries.Dequeue();
            _droppedCount++;
        }
    }

    private sealed record Entry(long RecordedAtTimestamp, T Value);
}

/// <summary>
/// A bounded active-operation map. Capacity overflow is visible through a
/// rejection count; active records are never silently evicted.
/// </summary>
public sealed class BoundedActiveOperationRegistry<TKey, TValue>
    where TKey : notnull
{
    private readonly object _gate = new();
    private readonly Dictionary<TKey, TValue> _records;
    private readonly int _capacity;
    private long _rejectedCount;

    public BoundedActiveOperationRegistry(
        int capacity,
        IEqualityComparer<TKey>? comparer = null)
    {
        if (capacity <= 0 || capacity > CSharpDbObservabilityOptions.MaximumActiveOperationCapacity)
            throw new ArgumentOutOfRangeException(nameof(capacity));

        _capacity = capacity;
        _records = new Dictionary<TKey, TValue>(capacity, comparer);
    }

    public int Capacity => _capacity;

    public bool TryAdd(TKey key, TValue value)
    {
        lock (_gate)
        {
            if (_records.ContainsKey(key))
                return false;
            if (_records.Count >= _capacity)
            {
                _rejectedCount++;
                return false;
            }

            _records.Add(key, value);
            return true;
        }
    }

    public bool TryUpdate(TKey key, TValue value)
    {
        lock (_gate)
        {
            if (!_records.ContainsKey(key))
                return false;

            _records[key] = value;
            return true;
        }
    }

    public bool TryRemove(TKey key, out TValue? value)
    {
        lock (_gate)
            return _records.Remove(key, out value);
    }

    public BoundedDiagnosticsSnapshot<TValue> GetSnapshot(int maximumRecords)
    {
        if (maximumRecords <= 0)
            throw new ArgumentOutOfRangeException(nameof(maximumRecords));

        lock (_gate)
        {
            TValue[] records = _records.Values.Take(maximumRecords).ToArray();
            bool truncated = records.Length < _records.Count || _rejectedCount > 0;
            return new BoundedDiagnosticsSnapshot<TValue>(
                records,
                _rejectedCount,
                truncated);
        }
    }
}
