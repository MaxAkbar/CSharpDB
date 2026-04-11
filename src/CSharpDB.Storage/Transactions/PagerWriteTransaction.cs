namespace CSharpDB.Storage.Transactions;

internal sealed class PagerWriteTransaction : IAsyncDisposable
{
    private readonly Pager _pager;
    private readonly PagerTransactionState _state;
    private int _disposed;

    internal PagerWriteTransaction(Pager pager, PagerTransactionState state)
    {
        _pager = pager ?? throw new ArgumentNullException(nameof(pager));
        _state = state ?? throw new ArgumentNullException(nameof(state));
    }

    internal PagerTransactionState State => _state;

    internal IDisposable Bind() => _pager.BindTransaction(_state);

    internal IDisposable BindSnapshotRead()
        => new CombinedBinding(Bind(), _pager.SuppressLogicalReadTracking());

    internal async ValueTask<PagerCommitResult> BeginCommitAsync(CancellationToken ct = default)
    {
        using var binding = Bind();
        return await _pager.BeginCommitAsync(ct);
    }

    internal async ValueTask CommitAsync(CancellationToken ct = default)
    {
        using var binding = Bind();
        await _pager.CommitAsync(ct);
    }

    internal async ValueTask RollbackAsync(CancellationToken ct = default)
    {
        using var binding = Bind();
        await _pager.RollbackAsync(ct);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        if (_state.Completed || _state.CommitStarted)
            return;

        await RollbackAsync();
    }

    private sealed class CombinedBinding : IDisposable
    {
        private readonly IDisposable _binding;
        private readonly IDisposable _suppression;
        private int _disposed;

        public CombinedBinding(IDisposable binding, IDisposable suppression)
        {
            _binding = binding ?? throw new ArgumentNullException(nameof(binding));
            _suppression = suppression ?? throw new ArgumentNullException(nameof(suppression));
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;

            try
            {
                _suppression.Dispose();
            }
            finally
            {
                _binding.Dispose();
            }
        }
    }
}
