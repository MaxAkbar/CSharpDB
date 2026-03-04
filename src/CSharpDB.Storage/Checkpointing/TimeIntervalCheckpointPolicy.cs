using System.Threading;

namespace CSharpDB.Storage.Checkpointing;

/// <summary>
/// Triggers checkpoints at a fixed wall-clock interval when no readers are active.
/// </summary>
public sealed class TimeIntervalCheckpointPolicy : ICheckpointPolicy
{
    private readonly TimeSpan _interval;
    private readonly IClock _clock;
    private long _nextCheckpointUtcTicks;

    public TimeIntervalCheckpointPolicy(TimeSpan interval, IClock? clock = null)
    {
        if (interval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(interval), "Interval must be greater than zero.");

        _interval = interval;
        _clock = clock ?? new SystemClock();
        _nextCheckpointUtcTicks = _clock.UtcNow.UtcTicks + _interval.Ticks;
    }

    public bool ShouldCheckpoint(PagerCheckpointContext context)
    {
        if (context.ActiveReaderCount != 0)
            return false;

        long nowTicks = _clock.UtcNow.UtcTicks;
        long nextTicks = Volatile.Read(ref _nextCheckpointUtcTicks);
        if (nowTicks < nextTicks)
            return false;

        Interlocked.Exchange(ref _nextCheckpointUtcTicks, nowTicks + _interval.Ticks);
        return true;
    }
}
