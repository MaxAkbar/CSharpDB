using CSharpDB.Storage.Catalog;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Engine;

public sealed partial class Database
{
    private readonly object _readerCatalogGate = new();
    private ReaderCatalogState? _readerCatalogState;
    private int _catalogPublications;
    private bool _readerCatalogCopyUnsafe;

    internal sealed class ReaderCatalogState(WalSnapshot snapshot, CatalogReadSnapshot? catalog,
        bool capturedFromSharedCatalog = false)
    {
        internal readonly WalSnapshot Snapshot = snapshot;
        internal CatalogReadSnapshot? Catalog = catalog;
        internal readonly bool CapturedFromSharedCatalog = capturedFromSharedCatalog;
        private SemaphoreSlim? _loadGate;
        internal SemaphoreSlim LoadGate => LazyInitializer.EnsureInitialized(ref _loadGate, static () => new(1, 1));
    }

    // The caller owns the legacy write-operation gate, before starting a transaction.
    // Preserve committed metadata for readers admitted while the live catalog is dirty.
    // Without this, a reader can lose exact counts and scan the whole table for COUNT(*).
    private void CaptureReaderCatalogBeforeWrite()
    {
        // Databases that have never opened a reader incur no capture work.
        if (Volatile.Read(ref _readerCatalogState) is null) return;
        lock (_readerCatalogGate)
        {
            if (_catalogPublications != 0 || _readerCatalogCopyUnsafe || !_sharedStateGate.Wait(0)) return;
            try
            {
                WalSnapshot snapshot = _pager.AcquireReaderSnapshot();
                try
                {
                    if (!_inTransaction && _pager.CanCaptureCommittedCatalog(snapshot))
                    {
                        var catalog = _catalog.CaptureReadSnapshot(_readerCatalogState?.Catalog);
                        _readerCatalogState = new ReaderCatalogState(snapshot, catalog, capturedFromSharedCatalog: true);
                    }
                }
                finally { _pager.ReleaseReaderSnapshot(snapshot); }
            }
            finally { _sharedStateGate.Release(); }
        }
    }

    private ReaderSession CreateReaderSessionCore(IReadOnlyDictionary<string, long>? snapshotRowCounts = null)
    {
        lock (_readerCatalogGate)
        {
            WalSnapshot snapshot = _pager.AcquireReaderSnapshot();
            try
            {
                ReaderCatalogState? state = _readerCatalogState;
                bool samePageState = state is not null && state.Snapshot.SharesPageState(snapshot);
                if (!samePageState || !state!.CapturedFromSharedCatalog)
                {
                    CatalogReadSnapshot? catalog = null;
                    // Neither gate waits: an active writer uses the snapshot-backed fallback.
                    // The publication barrier covers WAL publication through shared-catalog sync.
                    if (_catalogPublications == 0 && !_readerCatalogCopyUnsafe && _writeOperationGate.Wait(0))
                    {
                        try
                        {
                            if (_sharedStateGate.Wait(0))
                            {
                                try
                                {
                                    if (!_inTransaction && _pager.CanCaptureCommittedCatalog(snapshot))
                                        catalog = _catalog.CaptureReadSnapshot(state?.Catalog);
                                }
                                finally { _sharedStateGate.Release(); }
                            }
                        }
                        finally { _writeOperationGate.Release(); }
                    }
                    // A fallback deliberately distrusts persisted exact counts. Retry capture
                    // after publication finishes, even when the committed pages have not changed.
                    // Replace the state only on success, retaining the old fallback's load gate
                    // and catalog for its readers while a writer still prevents safe capture.
                    if (catalog is not null || !samePageState)
                        state = new ReaderCatalogState(snapshot, catalog, catalog is not null);
                    _readerCatalogState = state;
                }

                return new ReaderSession(this, _pager, state!, _recordSerializer, snapshot,
                    _statementCache, _functions, _planner.AdaptiveQueryReoptimization,
                    _planner.WindowExecution, _queryObservability, _queryPlanRuntimeObserver, snapshotRowCounts);
            }
            catch
            {
                _pager.ReleaseReaderSnapshot(snapshot);
                throw;
            }
        }
    }

    private async ValueTask<CatalogReadSnapshot> LoadReaderCatalogAsync(
        ReaderCatalogState state, WalSnapshot snapshot, CancellationToken ct)
    {
        CatalogReadSnapshot? catalog = Volatile.Read(ref state.Catalog);
        if (catalog is not null) return catalog;
        await state.LoadGate.WaitAsync(ct);
        try
        {
            catalog = state.Catalog;
            if (catalog is not null) return catalog;
            using Pager snapshotPager = _pager.CreateSnapshotReader(snapshot);
            catalog = await SchemaCatalog.LoadReadSnapshotAsync(snapshotPager, _schemaSerializer,
                _indexProvider, _catalogStore, _advisoryStatisticsPersistenceMode, ct);
            Volatile.Write(ref state.Catalog, catalog);
            return catalog;
        }
        finally { state.LoadGate.Release(); }
    }

    internal void BeginReaderCatalogPublication()
    {
        lock (_readerCatalogGate) _catalogPublications++;
    }

    internal void EndReaderCatalogPublication(bool synchronized)
    {
        lock (_readerCatalogGate)
        {
            _catalogPublications--;
            // A failed commit can leave live metadata ahead of committed pages. Subsequent
            // reader states load from their snapshot rather than trusting recovery timing.
            if (!synchronized) _readerCatalogCopyUnsafe = true;
        }
    }

}
