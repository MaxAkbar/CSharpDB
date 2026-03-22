namespace CSharpDB.Storage.Wal;

/// <summary>
/// Represents completion of a WAL commit after durability and reader visibility are guaranteed.
/// </summary>
public readonly struct WalCommitResult
{
    private readonly Task _completion;

    public WalCommitResult(Task completion)
    {
        _completion = completion ?? Task.CompletedTask;
    }

    public static WalCommitResult Completed { get; } = new(Task.CompletedTask);

    public ValueTask WaitAsync(CancellationToken cancellationToken = default)
    {
        if (_completion.IsCompletedSuccessfully)
            return ValueTask.CompletedTask;

        return cancellationToken.CanBeCanceled
            ? new ValueTask(_completion.WaitAsync(cancellationToken))
            : new ValueTask(_completion);
    }
}
