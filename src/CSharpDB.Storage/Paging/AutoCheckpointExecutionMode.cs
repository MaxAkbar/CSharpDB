namespace CSharpDB.Storage.Paging;

/// <summary>
/// Controls whether auto-checkpoints run inline with the triggering commit
/// or are scheduled asynchronously after the commit completes.
/// </summary>
public enum AutoCheckpointExecutionMode
{
    Foreground = 0,
    Background = 1,
}
