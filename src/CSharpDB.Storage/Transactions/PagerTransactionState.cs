using CSharpDB.Storage.Wal;

namespace CSharpDB.Storage.Transactions;

internal sealed class PagerTransactionState
{
    private readonly Action _releaseSnapshot;
    private int _snapshotReleased;

    internal PagerTransactionState(
        long transactionId,
        long startVersion,
        WalSnapshot snapshot,
        uint pageCount,
        uint schemaRootPage,
        uint freelistHead,
        uint changeCounter,
        Action releaseSnapshot)
    {
        TransactionId = transactionId;
        StartVersion = startVersion;
        Snapshot = snapshot ?? throw new ArgumentNullException(nameof(snapshot));
        PageCount = pageCount;
        SchemaRootPage = schemaRootPage;
        FreelistHead = freelistHead;
        ChangeCounter = changeCounter;
        _releaseSnapshot = releaseSnapshot ?? throw new ArgumentNullException(nameof(releaseSnapshot));
    }

    internal long TransactionId { get; }

    internal long StartVersion { get; }

    internal WalSnapshot Snapshot { get; }

    internal Dictionary<uint, byte[]> ModifiedPages { get; } = new();

    internal Dictionary<uint, long> ResolvedWriteConflictVersions { get; } = new();

    internal HashSet<uint> DirtyPages { get; } = new();

    internal HashSet<LogicalConflictKey> LogicalReadKeys { get; } = [];

    internal HashSet<LogicalConflictRange> LogicalReadRanges { get; } = [];

    internal HashSet<LogicalConflictKey> LogicalWriteKeys { get; } = [];

    internal uint PageCount { get; set; }

    internal uint SchemaRootPage { get; set; }

    internal uint FreelistHead { get; set; }

    internal uint ChangeCounter { get; set; }

    internal bool HasSchemaRootPageOverride { get; set; }

    internal bool HasFreelistHeadOverride { get; set; }

    internal bool HasPageCountOverride { get; set; }

    internal bool CommitStarted { get; set; }

    internal bool Completed { get; set; }

    internal bool HasSchemaWriteLock { get; set; }

    internal void ReleaseSnapshot()
    {
        if (Interlocked.Exchange(ref _snapshotReleased, 1) != 0)
            return;

        _releaseSnapshot();
    }
}
