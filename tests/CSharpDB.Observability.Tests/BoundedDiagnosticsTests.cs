using CSharpDB.Observability;

namespace CSharpDB.Observability.Tests;

public sealed class BoundedDiagnosticsTests
{
    [Fact]
    public void History_EnforcesCapacityRetentionOrderAndDroppedCount()
    {
        var clock = new ManualTimeProvider(new DateTimeOffset(2026, 8, 9, 12, 0, 0, TimeSpan.Zero));
        var history = new BoundedDiagnosticHistory<string>(
            capacity: 2,
            retention: TimeSpan.FromMinutes(1),
            clock);

        history.Add("first");
        clock.Advance(TimeSpan.FromSeconds(1));
        history.Add("second");
        clock.Advance(TimeSpan.FromSeconds(1));
        history.Add("third");

        BoundedDiagnosticsSnapshot<string> capacitySnapshot = history.GetSnapshot();
        Assert.Equal(new[] { "third", "second" }, capacitySnapshot.Records);
        Assert.Equal(1, capacitySnapshot.DroppedCount);
        Assert.True(capacitySnapshot.IsTruncated);

        clock.Advance(TimeSpan.FromMinutes(2));
        BoundedDiagnosticsSnapshot<string> expiredSnapshot = history.GetSnapshot();
        Assert.Empty(expiredSnapshot.Records);
        Assert.Equal(3, expiredSnapshot.DroppedCount);
        Assert.True(expiredSnapshot.IsTruncated);
    }

    [Fact]
    public void ActiveRegistry_NeverEvictsActiveRecordsAndReportsOverflow()
    {
        var registry = new BoundedActiveOperationRegistry<int, string>(capacity: 100);

        Parallel.For(0, 1_000, index => registry.TryAdd(index, $"operation-{index}"));

        BoundedDiagnosticsSnapshot<string> snapshot = registry.GetSnapshot(100);
        Assert.Equal(100, snapshot.Records.Count);
        Assert.Equal(900, snapshot.DroppedCount);
        Assert.True(snapshot.IsTruncated);

        int removed = 0;
        for (int index = 0; index < 1_000; index++)
        {
            if (registry.TryRemove(index, out _))
                removed++;
        }

        Assert.Equal(100, removed);
        Assert.Empty(registry.GetSnapshot(100).Records);
    }

    [Fact]
    public void HistoryRetention_UsesMonotonicTimeAcrossUtcClockAdjustments()
    {
        var clock = new ManualTimeProvider(new DateTimeOffset(2026, 8, 9, 12, 0, 0, TimeSpan.Zero));
        var history = new BoundedDiagnosticHistory<string>(
            capacity: 2,
            retention: TimeSpan.FromMinutes(1),
            clock);
        history.Add("record");

        clock.AdjustUtc(TimeSpan.FromDays(-1));
        clock.Advance(TimeSpan.FromSeconds(30));
        Assert.Single(history.GetSnapshot().Records);

        clock.AdjustUtc(TimeSpan.FromDays(2));
        clock.Advance(TimeSpan.FromSeconds(31));
        Assert.Empty(history.GetSnapshot().Records);
    }

    private sealed class ManualTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        private DateTimeOffset _utcNow = utcNow;
        private long _timestamp;

        public override DateTimeOffset GetUtcNow() => _utcNow;
        public override long GetTimestamp() => _timestamp;
        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public void Advance(TimeSpan elapsed)
        {
            _utcNow += elapsed;
            _timestamp += elapsed.Ticks;
        }

        public void AdjustUtc(TimeSpan adjustment)
            => _utcNow += adjustment;
    }
}
