namespace CSharpDB.Storage.Checkpointing;

/// <summary>
/// Triggers a checkpoint when any configured policy returns true.
/// </summary>
public sealed class AnyCheckpointPolicy : ICheckpointPolicy
{
    private readonly IReadOnlyList<ICheckpointPolicy> _policies;

    public AnyCheckpointPolicy(params ICheckpointPolicy[] policies)
    {
        ArgumentNullException.ThrowIfNull(policies);
        _policies = policies;
    }

    public bool ShouldCheckpoint(PagerCheckpointContext context)
    {
        for (int i = 0; i < _policies.Count; i++)
        {
            if (_policies[i].ShouldCheckpoint(context))
                return true;
        }

        return false;
    }
}
