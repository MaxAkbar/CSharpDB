namespace CSharpDB.Storage.Checkpointing;

/// <summary>
/// Triggers a checkpoint when estimated committed WAL bytes reach a threshold
/// and no snapshot readers are active.
/// </summary>
public sealed class WalSizeCheckpointPolicy : ICheckpointPolicy
{
    public WalSizeCheckpointPolicy(long thresholdBytes)
    {
        if (thresholdBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(thresholdBytes), "Threshold must be greater than zero.");
        ThresholdBytes = thresholdBytes;
    }

    public long ThresholdBytes { get; }

    public bool ShouldCheckpoint(PagerCheckpointContext context)
    {
        return context.ActiveReaderCount == 0 &&
               context.EstimatedWalBytes >= ThresholdBytes;
    }
}
