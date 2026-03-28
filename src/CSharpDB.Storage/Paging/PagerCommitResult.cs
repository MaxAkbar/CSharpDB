namespace CSharpDB.Storage.Paging;

/// <summary>
/// Represents completion of a pager commit after durable WAL flush and post-commit
/// pager finalization have both completed.
/// </summary>
public readonly struct PagerCommitResult
{
    private readonly Task _completion;

    public PagerCommitResult(Task completion)
    {
        _completion = completion ?? Task.CompletedTask;
    }

    public static PagerCommitResult Completed { get; } = new(Task.CompletedTask);

    public ValueTask WaitAsync(CancellationToken cancellationToken = default)
    {
        if (_completion.IsCompletedSuccessfully)
            return ValueTask.CompletedTask;

        return cancellationToken.CanBeCanceled
            ? new ValueTask(_completion.WaitAsync(cancellationToken))
            : new ValueTask(_completion);
    }
}
