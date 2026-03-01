namespace CSharpDB.Storage.Checkpointing;

/// <summary>
/// Defines when the pager should attempt an automatic checkpoint.
/// </summary>
public interface ICheckpointPolicy
{
    bool ShouldCheckpoint(PagerCheckpointContext context);
}

/// <summary>
/// Lightweight context passed to checkpoint policies.
/// </summary>
public readonly record struct PagerCheckpointContext(
    int CommittedFrameCount,
    int ActiveReaderCount);

