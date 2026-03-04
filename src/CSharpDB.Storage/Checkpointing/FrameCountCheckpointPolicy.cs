namespace CSharpDB.Storage.Checkpointing;

/// <summary>
/// Default auto-checkpoint policy: checkpoint when committed frame count
/// reaches a threshold and no snapshot readers are active.
/// </summary>
public sealed class FrameCountCheckpointPolicy : ICheckpointPolicy
{
    public FrameCountCheckpointPolicy(int threshold)
    {
        if (threshold <= 0)
            throw new ArgumentOutOfRangeException(nameof(threshold), "Threshold must be greater than zero.");
        Threshold = threshold;
    }

    public int Threshold { get; }

    public bool ShouldCheckpoint(PagerCheckpointContext context)
    {
        return context.CommittedFrameCount >= Threshold &&
               context.ActiveReaderCount == 0;
    }
}

