using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Time;

namespace CSharpDB.Tests;

public sealed class CheckpointPolicyTests
{
    [Fact]
    public void FrameCountCheckpointPolicy_RejectsNonPositiveThreshold()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new FrameCountCheckpointPolicy(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new FrameCountCheckpointPolicy(-1));
    }

    [Fact]
    public void FrameCountCheckpointPolicy_ChecksThresholdAndReaderCount()
    {
        var policy = new FrameCountCheckpointPolicy(3);

        Assert.False(policy.ShouldCheckpoint(new PagerCheckpointContext(CommittedFrameCount: 2, ActiveReaderCount: 0)));
        Assert.False(policy.ShouldCheckpoint(new PagerCheckpointContext(CommittedFrameCount: 3, ActiveReaderCount: 1)));
        Assert.True(policy.ShouldCheckpoint(new PagerCheckpointContext(CommittedFrameCount: 3, ActiveReaderCount: 0)));
        Assert.True(policy.ShouldCheckpoint(new PagerCheckpointContext(CommittedFrameCount: 10, ActiveReaderCount: 0)));
    }

    [Fact]
    public void AnyCheckpointPolicy_RejectsNullPolicies()
    {
        Assert.Throws<ArgumentNullException>(() => new AnyCheckpointPolicy(null!));
    }

    [Fact]
    public void AnyCheckpointPolicy_ReturnsTrueWhenAnyPolicyMatches_AndShortCircuits()
    {
        var first = new StubCheckpointPolicy(result: false);
        var second = new StubCheckpointPolicy(result: true);
        var third = new StubCheckpointPolicy(result: true);
        var policy = new AnyCheckpointPolicy(first, second, third);

        bool shouldCheckpoint = policy.ShouldCheckpoint(new PagerCheckpointContext(CommittedFrameCount: 0, ActiveReaderCount: 0));

        Assert.True(shouldCheckpoint);
        Assert.Equal(1, first.CallCount);
        Assert.Equal(1, second.CallCount);
        Assert.Equal(0, third.CallCount);
    }

    [Fact]
    public void AnyCheckpointPolicy_ReturnsFalseWhenNoPoliciesMatch()
    {
        var policy = new AnyCheckpointPolicy(
            new StubCheckpointPolicy(result: false),
            new StubCheckpointPolicy(result: false));

        Assert.False(policy.ShouldCheckpoint(new PagerCheckpointContext(CommittedFrameCount: 0, ActiveReaderCount: 0)));
    }

    [Fact]
    public void TimeIntervalCheckpointPolicy_RejectsNonPositiveInterval()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new TimeIntervalCheckpointPolicy(TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => new TimeIntervalCheckpointPolicy(TimeSpan.FromMilliseconds(-1)));
    }

    [Fact]
    public void TimeIntervalCheckpointPolicy_TriggersWhenIntervalElapses()
    {
        var clock = new FakeClock(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var policy = new TimeIntervalCheckpointPolicy(TimeSpan.FromSeconds(10), clock);
        var context = new PagerCheckpointContext(CommittedFrameCount: 0, ActiveReaderCount: 0);

        Assert.False(policy.ShouldCheckpoint(context));

        clock.Advance(TimeSpan.FromSeconds(10));
        Assert.True(policy.ShouldCheckpoint(context));

        Assert.False(policy.ShouldCheckpoint(context));

        clock.Advance(TimeSpan.FromSeconds(9));
        Assert.False(policy.ShouldCheckpoint(context));

        clock.Advance(TimeSpan.FromSeconds(1));
        Assert.True(policy.ShouldCheckpoint(context));
    }

    [Fact]
    public void TimeIntervalCheckpointPolicy_DoesNotTriggerWhileReadersAreActive()
    {
        var clock = new FakeClock(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var policy = new TimeIntervalCheckpointPolicy(TimeSpan.FromSeconds(5), clock);

        clock.Advance(TimeSpan.FromSeconds(5));
        Assert.False(policy.ShouldCheckpoint(new PagerCheckpointContext(CommittedFrameCount: 0, ActiveReaderCount: 1)));

        Assert.True(policy.ShouldCheckpoint(new PagerCheckpointContext(CommittedFrameCount: 0, ActiveReaderCount: 0)));
    }

    private sealed class StubCheckpointPolicy : ICheckpointPolicy
    {
        private readonly bool _result;

        public StubCheckpointPolicy(bool result)
        {
            _result = result;
        }

        public int CallCount { get; private set; }

        public bool ShouldCheckpoint(PagerCheckpointContext context)
        {
            CallCount++;
            return _result;
        }
    }

    private sealed class FakeClock : IClock
    {
        private DateTimeOffset _utcNow;

        public FakeClock(DateTimeOffset utcNow)
        {
            _utcNow = utcNow;
        }

        public DateTimeOffset UtcNow => _utcNow;

        public void Advance(TimeSpan delta)
        {
            _utcNow = _utcNow.Add(delta);
        }
    }
}
