namespace CSharpDB.Client;

/// <summary>Optional direct-file capability for operations that must exclude competing writers.</summary>
public interface ICSharpDbExclusiveSessionProvider
{
    bool SupportsExclusiveSessions { get; }
    ValueTask<CSharpDbExclusiveSession> OpenExclusiveSessionAsync(CancellationToken ct = default);
}

/// <summary>
/// Reserves the originating client and exposes a private client with a file-writer barrier.
/// Dispose this lease after all private transactions finish. Do not use the originating
/// client from inside the lease; its ordinary operations wait for release.
/// </summary>
public sealed class CSharpDbExclusiveSession(ICSharpDbClient client, Func<ValueTask> release) : IAsyncDisposable
{
    private Func<ValueTask>? _release = release;
    public ICSharpDbClient Client { get; } = client;
    public ValueTask DisposeAsync() => Interlocked.Exchange(ref _release, null)?.Invoke() ?? ValueTask.CompletedTask;
}
