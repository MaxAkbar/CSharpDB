using CSharpDB.Primitives;
using CSharpDB.Storage.Caching;
using CSharpDB.Storage.Internal;
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;

namespace CSharpDB.Storage.Paging;

/// <summary>
/// The Pager owns all page I/O and dirty tracking. It mediates between the B+tree layer and the storage device.
/// Uses a Write-Ahead Log (WAL) for crash recovery and concurrent reader support.
/// </summary>
public sealed class Pager : IAsyncDisposable, IDisposable
{
    private static readonly LogicalConflictKey SchemaConflictKey = new("schema:global", 0);
    private static readonly ConcurrentDictionary<string, string> LogicalIndexResourceNames = new();
    private static readonly ConcurrentDictionary<string, string> LogicalTableRowResourceNames = new();
    private static readonly ConcurrentDictionary<(string TableName, string ColumnName), string> LogicalTableColumnResourceNames = new();
    private readonly IStorageDevice _device;
    private readonly IPageReadProvider _pageReads;
    private readonly IPageReadProvider _speculativePageReads;
    private readonly bool _ownsPageReadProviders;
    private readonly IWriteAheadLog _wal;
    private readonly WalIndex _walIndex;
    private readonly IPageOperationInterceptor _interceptor;
    private readonly bool _hasInterceptor;
    private readonly PageBufferManager _buffers;
    private readonly IPageAllocator _allocator;
    private readonly TransactionCoordinator? _transactions;
    private readonly CheckpointCoordinator? _checkpoints;
    private readonly Func<CancellationToken, ValueTask>? _checkpointAction;
    private readonly AsyncLocal<PagerTransactionState?> _ambientTransaction = new();
    private readonly AsyncLocal<int> _suppressedLogicalReadTrackingDepth = new();
    private readonly object _legacyLogicalWriteGate = new();
    private readonly HashSet<LogicalConflictKey> _legacyLogicalWriteKeys = [];
    private int _disposeRequested;

    // Non-null for read-only snapshot pager instances
    private readonly WalSnapshot? _readerSnapshot;
    private readonly bool _isSnapshotReader;
    private long _walAppendCount;
    private long _walAppendTicks;
    private long _explicitCommitLockWaitCount;
    private long _explicitCommitLockWaitTicks;
    private long _explicitCommitLockHoldCount;
    private long _explicitCommitLockHoldTicks;
    private long _explicitConflictResolutionCount;
    private long _explicitConflictResolutionTicks;
    private long _explicitLeafRebaseAttemptCount;
    private long _explicitLeafRebaseSuccessCount;
    private long _explicitLeafRebaseStructuralRejectCount;
    private long _explicitLeafRebaseCapacityRejectCount;
    private long _explicitLeafRebaseRejectNonInsertOnlyCount;
    private long _explicitLeafRebaseRejectDuplicateKeyCount;
    private long _explicitLeafRebaseRejectSplitFallbackPreconditionCount;
    private long _explicitLeafRebaseRejectSplitFallbackMissingTraversalCount;
    private long _explicitLeafRebaseRejectSplitFallbackDirtyAncestorCount;
    private long _explicitLeafRebaseRejectSplitFallbackParentBoundaryCount;
    private long _explicitLeafRebaseRejectSplitFallbackTargetPageDirtyCount;
    private long _explicitLeafRebaseRejectDirtyParentMissingParentPageCount;
    private long _explicitLeafRebaseRejectDirtyParentTransactionLeafNotSplitCount;
    private long _explicitLeafRebaseRejectDirtyParentBaseBoundaryMissingCount;
    private long _explicitLeafRebaseRejectDirtyParentInsertionShapeCount;
    private long _explicitLeafRebaseRejectDirtyParentInsertionMismatchCount;
    private long _explicitLeafRebaseRejectDirtyParentMissingLocalRightPageCount;
    private long _explicitLeafRebaseRejectDirtyParentLocalSplitShapeCount;
    private long _explicitLeafRebaseRejectDirtyParentRebaseFailureCount;
    private long _explicitLeafRebaseRejectDirtyParentDescribedInsertionMatchCount;
    private long _explicitLeafRebaseRejectSplitFallbackShapeCount;
    private long _explicitLeafRebaseRejectOtherCount;
    private long _explicitInteriorRebaseAttemptCount;
    private long _explicitInteriorRebaseSuccessCount;
    private long _explicitInteriorRebaseStructuralRejectCount;
    private long _explicitInteriorRebaseCapacityRejectCount;
    private long _explicitPendingCommitWaitCount;
    private long _explicitPendingCommitWaitTicks;
    private long _explicitHeaderPreparationCount;
    private long _explicitHeaderPreparationTicks;
    private long _explicitPendingCommitReservationCount;
    private long _explicitPendingCommitReservationTicks;
    private long _finalizeCommitCount;
    private long _finalizeCommitTicks;
    private long _checkpointDecisionCount;
    private long _checkpointDecisionTicks;
    private long _backgroundCheckpointStartCount;
    private long _btreeLeafSplitCount;
    private long _btreeRightEdgeLeafSplitCount;
    private long _btreeInteriorInsertCount;
    private long _btreeRightEdgeInteriorInsertCount;
    private long _btreeInteriorSplitCount;
    private long _btreeRightEdgeInteriorSplitCount;
    private long _btreeRootSplitCount;
    private readonly object _explicitCommitStateGate = new();
    private uint _scheduledExplicitPageCount;
    private uint _scheduledExplicitSchemaRootPage;
    private uint _scheduledExplicitFreelistHead;
    private uint _scheduledExplicitChangeCounter;
    private int _pendingExplicitCommitCount;
    private long _lastAppliedExplicitCommitVersion;
    private TaskCompletionSource<bool> _explicitCommitStateChanged = CreateExplicitCommitStateChangedSource();

    // File header state (cached in memory)
    private uint _pageCount;
    private uint _schemaRootPage;
    private uint _freelistHead;
    private uint _changeCounter;

    public uint PageCount
    {
        get => GetCurrentTransaction() is { } tx ? tx.PageCount : _pageCount;
        internal set
        {
            if (GetCurrentTransaction() is { } tx)
            {
                tx.PageCount = value;
                tx.HasPageCountOverride = true;
                return;
            }

            _pageCount = value;
            _transactions?.EnsureNextReservedPageIdAtLeast(value);
        }
    }

    public uint SchemaRootPage
    {
        get => GetCurrentTransaction() is { } tx ? tx.SchemaRootPage : _schemaRootPage;
        set
        {
            if (GetCurrentTransaction() is { } tx)
            {
                tx.SchemaRootPage = value;
                tx.HasSchemaRootPageOverride = true;
                return;
            }

            _schemaRootPage = value;
        }
    }

    public uint FreelistHead
    {
        get => GetCurrentTransaction() is { } tx ? tx.FreelistHead : _freelistHead;
        set
        {
            if (GetCurrentTransaction() is { } tx)
            {
                tx.FreelistHead = value;
                tx.HasFreelistHeadOverride = true;
                return;
            }

            _freelistHead = value;
        }
    }

    public uint ChangeCounter
    {
        get => GetCurrentTransaction() is { } tx ? tx.ChangeCounter : _changeCounter;
        private set
        {
            if (GetCurrentTransaction() is { } tx)
            {
                tx.ChangeCounter = value;
                return;
            }

            _changeCounter = value;
        }
    }
    public int ActiveReaderCount => _checkpoints?.ActiveReaderCount ?? 0;

    internal bool IsExplicitWriteTransactionActive => GetCurrentTransaction() is not null;

    internal WalFlushDiagnosticsSnapshot GetWalFlushDiagnosticsSnapshot()
    {
        return _wal is IWalRuntimeDiagnosticsProvider diagnosticsProvider
            ? diagnosticsProvider.GetWalFlushDiagnosticsSnapshot()
            : WalFlushDiagnosticsSnapshot.Empty;
    }

    internal void ResetWalFlushDiagnostics()
    {
        if (_wal is IWalRuntimeDiagnosticsProvider diagnosticsProvider)
            diagnosticsProvider.ResetWalFlushDiagnostics();
    }

    internal CommitPathDiagnosticsSnapshot GetCommitPathDiagnosticsSnapshot()
    {
        CommitPathDiagnosticsSnapshot walDiagnostics = _wal is ICommitPathDiagnosticsProvider diagnosticsProvider
            ? diagnosticsProvider.GetCommitPathDiagnosticsSnapshot()
            : CommitPathDiagnosticsSnapshot.Empty;

        return new CommitPathDiagnosticsSnapshot(
            WalAppendCount: Interlocked.Read(ref _walAppendCount),
            WalAppendTicks: Interlocked.Read(ref _walAppendTicks),
            ExplicitCommitLockWaitCount: Interlocked.Read(ref _explicitCommitLockWaitCount),
            ExplicitCommitLockWaitTicks: Interlocked.Read(ref _explicitCommitLockWaitTicks),
            ExplicitCommitLockHoldCount: Interlocked.Read(ref _explicitCommitLockHoldCount),
            ExplicitCommitLockHoldTicks: Interlocked.Read(ref _explicitCommitLockHoldTicks),
            ExplicitConflictResolutionCount: Interlocked.Read(ref _explicitConflictResolutionCount),
            ExplicitConflictResolutionTicks: Interlocked.Read(ref _explicitConflictResolutionTicks),
            ExplicitLeafRebaseAttemptCount: Interlocked.Read(ref _explicitLeafRebaseAttemptCount),
            ExplicitLeafRebaseSuccessCount: Interlocked.Read(ref _explicitLeafRebaseSuccessCount),
            ExplicitLeafRebaseStructuralRejectCount: Interlocked.Read(ref _explicitLeafRebaseStructuralRejectCount),
            ExplicitLeafRebaseCapacityRejectCount: Interlocked.Read(ref _explicitLeafRebaseCapacityRejectCount),
            ExplicitLeafRebaseRejectNonInsertOnlyCount: Interlocked.Read(ref _explicitLeafRebaseRejectNonInsertOnlyCount),
            ExplicitLeafRebaseRejectDuplicateKeyCount: Interlocked.Read(ref _explicitLeafRebaseRejectDuplicateKeyCount),
            ExplicitLeafRebaseRejectSplitFallbackPreconditionCount: Interlocked.Read(ref _explicitLeafRebaseRejectSplitFallbackPreconditionCount),
            ExplicitLeafRebaseRejectSplitFallbackMissingTraversalCount: Interlocked.Read(ref _explicitLeafRebaseRejectSplitFallbackMissingTraversalCount),
            ExplicitLeafRebaseRejectSplitFallbackDirtyAncestorCount: Interlocked.Read(ref _explicitLeafRebaseRejectSplitFallbackDirtyAncestorCount),
            ExplicitLeafRebaseRejectSplitFallbackParentBoundaryCount: Interlocked.Read(ref _explicitLeafRebaseRejectSplitFallbackParentBoundaryCount),
            ExplicitLeafRebaseRejectSplitFallbackTargetPageDirtyCount: Interlocked.Read(ref _explicitLeafRebaseRejectSplitFallbackTargetPageDirtyCount),
            ExplicitLeafRebaseRejectDirtyParentMissingParentPageCount: Interlocked.Read(ref _explicitLeafRebaseRejectDirtyParentMissingParentPageCount),
            ExplicitLeafRebaseRejectDirtyParentTransactionLeafNotSplitCount: Interlocked.Read(ref _explicitLeafRebaseRejectDirtyParentTransactionLeafNotSplitCount),
            ExplicitLeafRebaseRejectDirtyParentBaseBoundaryMissingCount: Interlocked.Read(ref _explicitLeafRebaseRejectDirtyParentBaseBoundaryMissingCount),
            ExplicitLeafRebaseRejectDirtyParentInsertionShapeCount: Interlocked.Read(ref _explicitLeafRebaseRejectDirtyParentInsertionShapeCount),
            ExplicitLeafRebaseRejectDirtyParentInsertionMismatchCount: Interlocked.Read(ref _explicitLeafRebaseRejectDirtyParentInsertionMismatchCount),
            ExplicitLeafRebaseRejectDirtyParentMissingLocalRightPageCount: Interlocked.Read(ref _explicitLeafRebaseRejectDirtyParentMissingLocalRightPageCount),
            ExplicitLeafRebaseRejectDirtyParentLocalSplitShapeCount: Interlocked.Read(ref _explicitLeafRebaseRejectDirtyParentLocalSplitShapeCount),
            ExplicitLeafRebaseRejectDirtyParentRebaseFailureCount: Interlocked.Read(ref _explicitLeafRebaseRejectDirtyParentRebaseFailureCount),
            ExplicitLeafRebaseRejectDirtyParentDescribedInsertionMatchCount: Interlocked.Read(ref _explicitLeafRebaseRejectDirtyParentDescribedInsertionMatchCount),
            ExplicitLeafRebaseRejectSplitFallbackShapeCount: Interlocked.Read(ref _explicitLeafRebaseRejectSplitFallbackShapeCount),
            ExplicitLeafRebaseRejectOtherCount: Interlocked.Read(ref _explicitLeafRebaseRejectOtherCount),
            ExplicitInteriorRebaseAttemptCount: Interlocked.Read(ref _explicitInteriorRebaseAttemptCount),
            ExplicitInteriorRebaseSuccessCount: Interlocked.Read(ref _explicitInteriorRebaseSuccessCount),
            ExplicitInteriorRebaseStructuralRejectCount: Interlocked.Read(ref _explicitInteriorRebaseStructuralRejectCount),
            ExplicitInteriorRebaseCapacityRejectCount: Interlocked.Read(ref _explicitInteriorRebaseCapacityRejectCount),
            ExplicitPendingCommitWaitCount: Interlocked.Read(ref _explicitPendingCommitWaitCount),
            ExplicitPendingCommitWaitTicks: Interlocked.Read(ref _explicitPendingCommitWaitTicks),
            ExplicitHeaderPreparationCount: Interlocked.Read(ref _explicitHeaderPreparationCount),
            ExplicitHeaderPreparationTicks: Interlocked.Read(ref _explicitHeaderPreparationTicks),
            ExplicitPendingCommitReservationCount: Interlocked.Read(ref _explicitPendingCommitReservationCount),
            ExplicitPendingCommitReservationTicks: Interlocked.Read(ref _explicitPendingCommitReservationTicks),
            DurableBatchWindowWaitCount: walDiagnostics.DurableBatchWindowWaitCount,
            DurableBatchWindowWaitTicks: walDiagnostics.DurableBatchWindowWaitTicks,
            PendingCommitWriteCount: walDiagnostics.PendingCommitWriteCount,
            PendingCommitWriteTicks: walDiagnostics.PendingCommitWriteTicks,
            PendingCommitDrainCount: walDiagnostics.PendingCommitDrainCount,
            PendingCommitDrainTicks: walDiagnostics.PendingCommitDrainTicks,
            BufferedFlushCount: walDiagnostics.BufferedFlushCount,
            BufferedFlushTicks: walDiagnostics.BufferedFlushTicks,
            DurableFlushCount: walDiagnostics.DurableFlushCount,
            DurableFlushTicks: walDiagnostics.DurableFlushTicks,
            PublishBatchCount: walDiagnostics.PublishBatchCount,
            PublishBatchTicks: walDiagnostics.PublishBatchTicks,
            FinalizeCommitCount: Interlocked.Read(ref _finalizeCommitCount),
            FinalizeCommitTicks: Interlocked.Read(ref _finalizeCommitTicks),
            CheckpointDecisionCount: Interlocked.Read(ref _checkpointDecisionCount),
            CheckpointDecisionTicks: Interlocked.Read(ref _checkpointDecisionTicks),
            BackgroundCheckpointStartCount: Interlocked.Read(ref _backgroundCheckpointStartCount),
            BTreeLeafSplitCount: Interlocked.Read(ref _btreeLeafSplitCount),
            BTreeRightEdgeLeafSplitCount: Interlocked.Read(ref _btreeRightEdgeLeafSplitCount),
            BTreeInteriorInsertCount: Interlocked.Read(ref _btreeInteriorInsertCount),
            BTreeRightEdgeInteriorInsertCount: Interlocked.Read(ref _btreeRightEdgeInteriorInsertCount),
            BTreeInteriorSplitCount: Interlocked.Read(ref _btreeInteriorSplitCount),
            BTreeRightEdgeInteriorSplitCount: Interlocked.Read(ref _btreeRightEdgeInteriorSplitCount),
            BTreeRootSplitCount: Interlocked.Read(ref _btreeRootSplitCount),
            MaxPendingCommitCount: walDiagnostics.MaxPendingCommitCount,
            MaxPendingCommitBytes: walDiagnostics.MaxPendingCommitBytes);
    }

    internal void ResetCommitPathDiagnostics()
    {
        if (_wal is ICommitPathDiagnosticsProvider diagnosticsProvider)
            diagnosticsProvider.ResetCommitPathDiagnostics();

        Interlocked.Exchange(ref _walAppendCount, 0);
        Interlocked.Exchange(ref _walAppendTicks, 0);
        Interlocked.Exchange(ref _explicitCommitLockWaitCount, 0);
        Interlocked.Exchange(ref _explicitCommitLockWaitTicks, 0);
        Interlocked.Exchange(ref _explicitCommitLockHoldCount, 0);
        Interlocked.Exchange(ref _explicitCommitLockHoldTicks, 0);
        Interlocked.Exchange(ref _explicitConflictResolutionCount, 0);
        Interlocked.Exchange(ref _explicitConflictResolutionTicks, 0);
        Interlocked.Exchange(ref _explicitLeafRebaseAttemptCount, 0);
        Interlocked.Exchange(ref _explicitLeafRebaseSuccessCount, 0);
        Interlocked.Exchange(ref _explicitLeafRebaseStructuralRejectCount, 0);
        Interlocked.Exchange(ref _explicitLeafRebaseCapacityRejectCount, 0);
        Interlocked.Exchange(ref _explicitLeafRebaseRejectNonInsertOnlyCount, 0);
        Interlocked.Exchange(ref _explicitLeafRebaseRejectDuplicateKeyCount, 0);
        Interlocked.Exchange(ref _explicitLeafRebaseRejectSplitFallbackPreconditionCount, 0);
        Interlocked.Exchange(ref _explicitLeafRebaseRejectSplitFallbackMissingTraversalCount, 0);
        Interlocked.Exchange(ref _explicitLeafRebaseRejectSplitFallbackDirtyAncestorCount, 0);
        Interlocked.Exchange(ref _explicitLeafRebaseRejectSplitFallbackParentBoundaryCount, 0);
        Interlocked.Exchange(ref _explicitLeafRebaseRejectSplitFallbackTargetPageDirtyCount, 0);
        Interlocked.Exchange(ref _explicitLeafRebaseRejectDirtyParentMissingParentPageCount, 0);
        Interlocked.Exchange(ref _explicitLeafRebaseRejectDirtyParentTransactionLeafNotSplitCount, 0);
        Interlocked.Exchange(ref _explicitLeafRebaseRejectDirtyParentBaseBoundaryMissingCount, 0);
        Interlocked.Exchange(ref _explicitLeafRebaseRejectDirtyParentInsertionShapeCount, 0);
        Interlocked.Exchange(ref _explicitLeafRebaseRejectDirtyParentInsertionMismatchCount, 0);
        Interlocked.Exchange(ref _explicitLeafRebaseRejectDirtyParentMissingLocalRightPageCount, 0);
        Interlocked.Exchange(ref _explicitLeafRebaseRejectDirtyParentLocalSplitShapeCount, 0);
        Interlocked.Exchange(ref _explicitLeafRebaseRejectDirtyParentRebaseFailureCount, 0);
        Interlocked.Exchange(ref _explicitLeafRebaseRejectDirtyParentDescribedInsertionMatchCount, 0);
        Interlocked.Exchange(ref _explicitLeafRebaseRejectSplitFallbackShapeCount, 0);
        Interlocked.Exchange(ref _explicitLeafRebaseRejectOtherCount, 0);
        Interlocked.Exchange(ref _explicitInteriorRebaseAttemptCount, 0);
        Interlocked.Exchange(ref _explicitInteriorRebaseSuccessCount, 0);
        Interlocked.Exchange(ref _explicitInteriorRebaseStructuralRejectCount, 0);
        Interlocked.Exchange(ref _explicitInteriorRebaseCapacityRejectCount, 0);
        Interlocked.Exchange(ref _explicitPendingCommitWaitCount, 0);
        Interlocked.Exchange(ref _explicitPendingCommitWaitTicks, 0);
        Interlocked.Exchange(ref _explicitHeaderPreparationCount, 0);
        Interlocked.Exchange(ref _explicitHeaderPreparationTicks, 0);
        Interlocked.Exchange(ref _explicitPendingCommitReservationCount, 0);
        Interlocked.Exchange(ref _explicitPendingCommitReservationTicks, 0);
        Interlocked.Exchange(ref _finalizeCommitCount, 0);
        Interlocked.Exchange(ref _finalizeCommitTicks, 0);
        Interlocked.Exchange(ref _checkpointDecisionCount, 0);
        Interlocked.Exchange(ref _checkpointDecisionTicks, 0);
        Interlocked.Exchange(ref _backgroundCheckpointStartCount, 0);
        Interlocked.Exchange(ref _btreeLeafSplitCount, 0);
        Interlocked.Exchange(ref _btreeRightEdgeLeafSplitCount, 0);
        Interlocked.Exchange(ref _btreeInteriorInsertCount, 0);
        Interlocked.Exchange(ref _btreeRightEdgeInteriorInsertCount, 0);
        Interlocked.Exchange(ref _btreeInteriorSplitCount, 0);
        Interlocked.Exchange(ref _btreeRightEdgeInteriorSplitCount, 0);
        Interlocked.Exchange(ref _btreeRootSplitCount, 0);
    }

    // Configurable behavior
    private readonly PagerOptions _options;
    private readonly byte[] _fileHeaderBuffer = new byte[PageConstants.FileHeaderSize];
    private readonly byte[] _walHeaderPageBuffer = new byte[PageConstants.PageSize];

    /// <summary>
    /// Legacy threshold property preserved for compatibility.
    /// Prefer configuring <see cref="CheckpointPolicy"/> via <see cref="PagerOptions"/>.
    /// </summary>
    public int CheckpointThreshold { get; set; } = PageConstants.DefaultCheckpointThreshold;

    /// <summary>
    /// Auto-checkpoint policy used after commits.
    /// </summary>
    public ICheckpointPolicy CheckpointPolicy { get; set; }

    /// <summary>Primary constructor for writer pager.</summary>
    private Pager(IStorageDevice device, IWriteAheadLog wal, WalIndex walIndex, PagerOptions? options = null)
    {
        _device = device;
        _options = options ?? new PagerOptions();
        ValidateOptions(_options);
        _pageReads = CreatePageReadProvider(device, _options);
        _speculativePageReads = CreateSpeculativePageReadProvider(device, _options, _pageReads);
        _ownsPageReadProviders = true;
        _wal = wal;
        _walIndex = walIndex;
        _interceptor = _options.CreateInterceptor();
        _hasInterceptor = _interceptor is not NoOpPageOperationInterceptor;
        var cache = _options.CreatePageCache();
        if (_options.OnCachePageEvicted != null && cache is IPageCacheEvictionEvents evictingCache)
            evictingCache.PageEvicted += _options.OnCachePageEvicted;
        _buffers = new PageBufferManager(
            cache,
            _pageReads,
            _speculativePageReads,
            _options.MaxCachedWalReadPages,
            _wal,
            _walIndex,
            readerSnapshot: null,
            isSnapshotReader: false,
            _interceptor);
        _transactions = new TransactionCoordinator();
        _checkpoints = new CheckpointCoordinator();
        _allocator = new PageAllocator(
            _buffers,
            () => FreelistHead,
            value => FreelistHead = value,
            () => PageCount,
            value => PageCount = value,
            GetPageAsync,
            MarkDirtyAsync,
            () => _isSnapshotReader);
        _checkpointAction = RunCheckpointCoreAsync;
        CheckpointPolicy = _options.CheckpointPolicy;

        if (_options.CheckpointPolicy is FrameCountCheckpointPolicy framePolicy)
            CheckpointThreshold = framePolicy.Threshold;
    }

    /// <summary>Constructor for read-only snapshot pager.</summary>
    private Pager(
        IStorageDevice device,
        IPageReadProvider pageReads,
        IPageReadProvider speculativePageReads,
        IWriteAheadLog wal,
        WalIndex walIndex,
        WalSnapshot snapshot,
        bool ownsPageReadProviders,
        PagerOptions? options = null)
    {
        _device = device;
        _pageReads = pageReads;
        _speculativePageReads = speculativePageReads;
        _ownsPageReadProviders = ownsPageReadProviders;
        _wal = wal;
        _walIndex = walIndex;
        _options = options ?? new PagerOptions();
        ValidateOptions(_options);
        _interceptor = _options.CreateInterceptor();
        _hasInterceptor = _interceptor is not NoOpPageOperationInterceptor;
        _readerSnapshot = snapshot;
        _isSnapshotReader = true;
        var cache = _options.CreatePageCache();
        if (_options.OnCachePageEvicted != null && cache is IPageCacheEvictionEvents evictingCache)
            evictingCache.PageEvicted += _options.OnCachePageEvicted;
        _buffers = new PageBufferManager(
            cache,
            _pageReads,
            _speculativePageReads,
            _options.MaxCachedWalReadPages,
            _wal,
            _walIndex,
            _readerSnapshot,
            _isSnapshotReader,
            _interceptor);
        _allocator = new PageAllocator(
            _buffers,
            () => FreelistHead,
            value => FreelistHead = value,
            () => PageCount,
            value => PageCount = value,
            GetPageAsync,
            MarkDirtyAsync,
            () => _isSnapshotReader);
        CheckpointPolicy = _options.CheckpointPolicy;

        if (_options.CheckpointPolicy is FrameCountCheckpointPolicy framePolicy)
            CheckpointThreshold = framePolicy.Threshold;
        // No writer lock, checkpoint lock, or checkpoint action needed for readers
    }

    public static async ValueTask<Pager> CreateAsync(IStorageDevice device, IWriteAheadLog wal,
        WalIndex walIndex, PagerOptions? options = null, CancellationToken ct = default)
    {
        var pager = new Pager(device, wal, walIndex, options);
        if (device.Length >= PageConstants.PageSize)
            await pager.ReadFileHeaderAsync(ct);
        return pager;
    }

    /// <summary>
    /// Create a read-only Pager that uses a WAL snapshot for all reads.
    /// Used for concurrent reader sessions.
    /// </summary>
    public Pager CreateSnapshotReader(WalSnapshot snapshot)
    {
        bool needsDedicatedProviders =
            _pageReads is MemoryMappedPageReadProvider ||
            _speculativePageReads is MemoryMappedPageReadProvider;

        var pageReads = needsDedicatedProviders
            ? CreatePageReadProvider(_device, _options)
            : _pageReads;

        var speculativePageReads = needsDedicatedProviders
            ? CreateSpeculativePageReadProvider(_device, _options, pageReads)
            : _speculativePageReads;

        var reader = new Pager(
            _device,
            pageReads,
            speculativePageReads,
            _wal,
            _walIndex,
            snapshot,
            needsDedicatedProviders,
            _options);
        // Copy header state so schema catalog can be loaded
        reader.PageCount = PageCount;
        reader.SchemaRootPage = SchemaRootPage;
        reader.FreelistHead = FreelistHead;
        reader.ChangeCounter = ChangeCounter;
        return reader;
    }

    /// <summary>
    /// Initialize a brand new database file.
    /// </summary>
    public async ValueTask InitializeNewDatabaseAsync(CancellationToken ct = default)
    {
        PageCount = 1; // page 0 exists
        SchemaRootPage = 0;
        FreelistHead = 0;
        ChangeCounter = 0;

        // Ensure page 0 exists
        var page0 = new byte[PageConstants.PageSize];
        WriteFileHeaderTo(page0);
        await _device.SetLengthAsync(PageConstants.PageSize, ct);
        await _device.WriteAsync(0, page0, ct);
        await _device.FlushAsync(ct);
        _buffers.SetCached(0, page0);
        RefreshPageReadProviderMapping();
        if (!_wal.IsOpen)
            await _wal.OpenAsync(PageCount, ct);
    }

    private async ValueTask ReadFileHeaderAsync(CancellationToken ct = default)
    {
        var header = _fileHeaderBuffer;
        await _device.ReadAsync(0, header, ct);

        // Validate magic
        if (header[0] != PageConstants.MagicBytes[0] ||
            header[1] != PageConstants.MagicBytes[1] ||
            header[2] != PageConstants.MagicBytes[2] ||
            header[3] != PageConstants.MagicBytes[3])
        {
            throw new CSharpDbException(ErrorCode.CorruptDatabase, "Invalid database file (bad magic bytes).");
        }

        int version = BitConverter.ToInt32(header, PageConstants.VersionOffset);
        if (version != PageConstants.FormatVersion)
            throw new CSharpDbException(ErrorCode.CorruptDatabase, $"Unsupported format version {version}.");

        PageCount = BitConverter.ToUInt32(header, PageConstants.PageCountOffset);
        SchemaRootPage = BitConverter.ToUInt32(header, PageConstants.SchemaRootPageOffset);
        FreelistHead = BitConverter.ToUInt32(header, PageConstants.FreelistHeadOffset);
        ChangeCounter = BitConverter.ToUInt32(header, PageConstants.ChangeCounterOffset);
    }

    private void WriteFileHeaderTo(Span<byte> page0)
    {
        PageConstants.MagicBytes.AsSpan().CopyTo(page0[PageConstants.MagicOffset..]);
        BitConverter.TryWriteBytes(page0[PageConstants.VersionOffset..], PageConstants.FormatVersion);
        BitConverter.TryWriteBytes(page0[PageConstants.PageSizeOffset..], PageConstants.PageSize);
        BitConverter.TryWriteBytes(page0[PageConstants.PageCountOffset..], PageCount);
        BitConverter.TryWriteBytes(page0[PageConstants.SchemaRootPageOffset..], SchemaRootPage);
        BitConverter.TryWriteBytes(page0[PageConstants.FreelistHeadOffset..], FreelistHead);
        BitConverter.TryWriteBytes(page0[PageConstants.ChangeCounterOffset..], ChangeCounter);
    }

    /// <summary>
    /// Synchronous cache-only page lookup. Returns null if the page is not in cache.
    /// Used by the sync fast path for point lookups to avoid async overhead.
    /// </summary>
    public byte[]? TryGetCachedPage(uint pageId)
    {
        if (GetCurrentTransaction() is { } tx)
        {
            if (tx.ModifiedPages.TryGetValue(pageId, out var modified))
                return modified;

            return null;
        }

        return _buffers.TryGetCachedPage(pageId);
    }

    internal byte[]? TryGetCachedPageAndRecordRead(uint pageId)
    {
        if (GetCurrentTransaction() is { } tx)
        {
            if (tx.ModifiedPages.TryGetValue(pageId, out var modified))
                return modified;

            return null;
        }

        return _buffers.TryGetCachedPageAndRecordRead(pageId);
    }

    internal bool TryGetCachedPageReadBuffer(uint pageId, out PageReadBuffer page)
    {
        if (GetCurrentTransaction() is { } tx)
        {
            if (tx.ModifiedPages.TryGetValue(pageId, out var modified))
            {
                page = PageReadBuffer.FromOwnedBuffer(modified);
                return true;
            }

            return _buffers.TryGetSnapshotCachedPageReadBuffer(pageId, tx.Snapshot, out page);
        }

        return _buffers.TryGetCachedPageReadBuffer(pageId, out page);
    }

    internal bool TryGetCachedPageReadBufferAndRecordRead(uint pageId, out PageReadBuffer page)
    {
        if (GetCurrentTransaction() is { } tx)
        {
            if (tx.ModifiedPages.TryGetValue(pageId, out var modified))
            {
                page = PageReadBuffer.FromOwnedBuffer(modified);
                return true;
            }

            return _buffers.TryGetSnapshotCachedPageReadBuffer(pageId, tx.Snapshot, out page);
        }

        return _buffers.TryGetCachedPageReadBufferAndRecordRead(pageId, out page);
    }

    public ValueTask<byte[]> GetPageAsync(uint pageId, CancellationToken ct = default)
    {
        if (GetCurrentTransaction() is { } tx)
            return GetTransactionPageAsync(tx, pageId, ct);

        return _buffers.GetPageAsync(pageId, ct);
    }

    internal ValueTask<PageReadBuffer> GetPageReadAsync(uint pageId, CancellationToken ct = default)
    {
        if (GetCurrentTransaction() is { } tx)
            return GetTransactionPageReadAsync(tx, pageId, ct);

        return _buffers.GetPageReadAsync(pageId, ct);
    }

    internal ValueTask<PageReadBuffer> GetSnapshotPageReadAsync(
        uint pageId,
        WalSnapshot snapshot,
        CancellationToken ct = default)
    {
        return _buffers.GetSnapshotPageReadAsync(pageId, snapshot, ct);
    }

    internal ValueTask<PageReadBuffer> ReadPageUncachedAsync(uint pageId, CancellationToken ct = default)
    {
        if (GetCurrentTransaction() is { } tx)
        {
            if (tx.ModifiedPages.TryGetValue(pageId, out var modified))
                return ValueTask.FromResult(PageReadBuffer.FromOwnedBuffer(modified));

            return _buffers.GetSnapshotPageReadAsync(pageId, tx.Snapshot, ct);
        }

        return _buffers.ReadPageUncachedAsync(pageId, ct);
    }

    internal bool TryGetSnapshotCachedPageReadBuffer(
        uint pageId,
        WalSnapshot snapshot,
        out PageReadBuffer page)
    {
        return _buffers.TryGetSnapshotCachedPageReadBuffer(pageId, snapshot, out page);
    }

    internal bool CanSpeculativePageReads =>
        _options.EnableSequentialLeafReadAhead &&
        !_buffers.HasInterceptor &&
        GetCurrentTransaction() is null &&
        _transactions?.InTransaction != true;

    internal bool UsesReadOnlyPageViews =>
        _options.UseMemoryMappedReads ||
        _options.MaxCachedWalReadPages > 0;

    public ValueTask MarkDirtyAsync(uint pageId, CancellationToken ct = default)
    {
        if (GetCurrentTransaction() is { } tx)
        {
            tx.DirtyPages.Add(pageId);
            return ValueTask.CompletedTask;
        }

        return _buffers.MarkDirtyAsync(
            pageId,
            inTransaction: true,
            GetPageAsync,
            ct);
    }

    /// <summary>
    /// Allocate a new page. Tries the freelist first, then extends the page count.
    /// In WAL mode, the DB file is NOT extended here — that happens during checkpoint.
    /// </summary>
    public async ValueTask<uint> AllocatePageAsync(CancellationToken ct = default)
    {
        if (GetCurrentTransaction() is { } tx)
        {
            return await AllocateTransactionPageAsync(tx, ct);
        }

        return await _allocator.AllocatePageAsync(ct);
    }

    /// <summary>
    /// Free a page by adding it to the freelist.
    /// </summary>
    public ValueTask FreePageAsync(uint pageId, CancellationToken ct = default)
    {
        if (GetCurrentTransaction() is { } tx)
            return FreeTransactionPageAsync(tx, pageId, ct);

        return _allocator.FreePageAsync(pageId, ct);
    }

    public async ValueTask BeginTransactionAsync(CancellationToken ct = default)
    {
        if (_isSnapshotReader)
            throw new InvalidOperationException("Cannot begin transactions on a read-only snapshot pager.");
        if (GetCurrentTransaction() != null || _transactions?.InTransaction == true)
            throw new CSharpDbException(ErrorCode.Unknown, "Nested transactions are not supported.");

        await WaitForBackgroundCheckpointAsync(ct);
        if (!_wal.IsOpen)
            await _wal.OpenAsync(PageCount, ct);
        await _transactions!.BeginAsync(_wal, _options.WriterLockTimeout, ct);
        ClearLegacyLogicalWriteTracking();
    }

    public async ValueTask CommitAsync(CancellationToken ct = default)
    {
        PagerCommitResult commit = await BeginCommitAsync(ct);
        await commit.WaitAsync(ct);
    }

    /// <summary>
    /// Starts committing the active transaction and returns a handle that completes
    /// after durable WAL flush and pager post-commit finalization finish.
    /// </summary>
    public ValueTask<PagerCommitResult> BeginCommitAsync(CancellationToken ct = default)
    {
        PagerTransactionState? tx = GetCurrentTransaction();
        if (tx is not null)
        {
            if (tx.Completed)
                throw new CSharpDbException(ErrorCode.Unknown, "No active transaction to commit.");
            if (tx.CommitStarted)
                throw new CSharpDbException(ErrorCode.Unknown, "Commit already started for the current transaction.");

            tx.CommitStarted = true;
            _ambientTransaction.Value = null;
            return BeginExplicitCommitAsync(tx, ct);
        }

        if (_transactions is null || !_transactions.InTransaction)
            throw new CSharpDbException(ErrorCode.Unknown, "No active transaction to commit.");

        return BeginLegacyCommitAsync(ct);
    }

    public async ValueTask RollbackAsync(CancellationToken ct = default)
    {
        PagerTransactionState? tx = GetCurrentTransaction();
        if (tx is not null)
        {
            tx.Completed = true;
            _ambientTransaction.Value = null;
            tx.ReleaseSnapshot();
            return;
        }

        if (_transactions is null || !_transactions.TryBeginRollback())
            return;

        ClearLegacyLogicalWriteTracking();
        await _wal.RollbackAsync(ct);
        _transactions.CompleteRollback();
        await ResetPagerStateFromCommittedStorageAsync(ct);
    }

    internal async ValueTask<PagerWriteTransaction> BeginWriteTransactionAsync(CancellationToken ct = default)
    {
        if (_isSnapshotReader)
            throw new InvalidOperationException("Cannot begin transactions on a read-only snapshot pager.");

        if (_transactions is not null)
            await _transactions.WaitForSchemaBeginAsync(ct);

        await WaitForBackgroundCheckpointAsync(ct);
        if (!_wal.IsOpen)
            await _wal.OpenAsync(PageCount, ct);

        IDisposable? beginBarrier = _transactions is not null
            ? await _transactions.AcquireBeginBarrierAsync(ct)
            : null;

        try
        {
            long transactionId = _transactions?.CreateTransactionId() ?? 0;
            long startVersion = _transactions?.CurrentCommitVersion ?? 0;
            WalSnapshot snapshot = AcquireReaderSnapshot();
            _transactions?.RegisterExplicitTransaction(transactionId, startVersion);
            var state = new PagerTransactionState(
                transactionId,
                startVersion,
                snapshot,
                _pageCount,
                _schemaRootPage,
                _freelistHead,
                _changeCounter,
                () =>
                {
                    try
                    {
                        ReleaseReaderSnapshot(snapshot);
                    }
                    finally
                    {
                        _transactions?.UnregisterExplicitTransaction(transactionId);
                    }
                });
            state.LogicalReadKeys.Add(SchemaConflictKey);
            return new PagerWriteTransaction(this, state);
        }
        finally
        {
            beginBarrier?.Dispose();
        }
    }

    internal IDisposable BindTransaction(PagerTransactionState state)
    {
        ArgumentNullException.ThrowIfNull(state);

        PagerTransactionState? previous = _ambientTransaction.Value;
        _ambientTransaction.Value = state;
        return new AmbientTransactionBinding(_ambientTransaction, previous);
    }

    internal IDisposable SuppressLogicalReadTracking()
    {
        int previousDepth = _suppressedLogicalReadTrackingDepth.Value;
        _suppressedLogicalReadTrackingDepth.Value = previousDepth + 1;
        return new LogicalReadTrackingSuppression(_suppressedLogicalReadTrackingDepth, previousDepth);
    }

    internal void RecordLogicalIndexRead(string indexName, long key)
        => RecordLogicalRead(BuildLogicalIndexResourceName(indexName), key);

    internal void RecordLogicalIndexRangeRead(string indexName, IndexScanRange range)
        => RecordLogicalRange(BuildLogicalIndexResourceName(indexName), range);

    internal void RecordLogicalIndexWrite(string indexName, long key)
        => RecordLogicalWrite(BuildLogicalIndexResourceName(indexName), key);

    internal void RecordLogicalTableRowRead(string tableName, long rowId)
        => RecordLogicalRead(BuildLogicalTableRowResourceName(tableName), rowId);

    internal void RecordLogicalTableRowRangeRead(string tableName, IndexScanRange range)
        => RecordLogicalRange(BuildLogicalTableRowResourceName(tableName), range);

    internal void RecordLogicalTableRowWrite(string tableName, long rowId)
        => RecordLogicalWrite(BuildLogicalTableRowResourceName(tableName), rowId);

    internal void RecordLogicalTableColumnRangeRead(string tableName, string columnName, IndexScanRange range)
        => RecordLogicalRange(BuildLogicalTableColumnResourceName(tableName, columnName), range);

    internal void RecordLogicalTableColumnWrite(string tableName, string columnName, long key)
        => RecordLogicalWrite(BuildLogicalTableColumnResourceName(tableName, columnName), key);

    internal void RecordLogicalResourceWrite(string resourceName, long key)
        => RecordLogicalWrite(resourceName, key);

    internal void RecordBTreeLeafSplit(bool rightEdge)
    {
        Interlocked.Increment(ref _btreeLeafSplitCount);
        if (rightEdge)
            Interlocked.Increment(ref _btreeRightEdgeLeafSplitCount);
    }

    internal void RecordBTreeInteriorInsert(bool rightEdge)
    {
        Interlocked.Increment(ref _btreeInteriorInsertCount);
        if (rightEdge)
            Interlocked.Increment(ref _btreeRightEdgeInteriorInsertCount);
    }

    internal void RecordBTreeInteriorSplit(bool rightEdge)
    {
        Interlocked.Increment(ref _btreeInteriorSplitCount);
        if (rightEdge)
            Interlocked.Increment(ref _btreeRightEdgeInteriorSplitCount);
    }

    internal void RecordBTreeRootSplit()
        => Interlocked.Increment(ref _btreeRootSplitCount);

    internal void RecordExplicitLeafInsertTraversal(uint rootPageId, List<uint> traversalPath)
    {
        PagerTransactionState? tx = GetCurrentTransaction();
        if (tx is null || traversalPath.Count == 0)
            return;

        uint leafPageId = traversalPath[^1];
        tx.ExplicitLeafInsertPaths[leafPageId] = new ExplicitLeafInsertPath(rootPageId, traversalPath.ToArray());
    }

    private void RecordExplicitLeafRebaseDiagnostics(InsertOnlyRebaseResult result)
    {
        if (result == InsertOnlyRebaseResult.NotApplicable)
            return;

        Interlocked.Increment(ref _explicitLeafRebaseAttemptCount);
        switch (result)
        {
            case InsertOnlyRebaseResult.Success:
                Interlocked.Increment(ref _explicitLeafRebaseSuccessCount);
                break;
            case InsertOnlyRebaseResult.StructuralReject:
                Interlocked.Increment(ref _explicitLeafRebaseStructuralRejectCount);
                break;
            case InsertOnlyRebaseResult.CapacityReject:
                Interlocked.Increment(ref _explicitLeafRebaseCapacityRejectCount);
                break;
        }
    }

    private void RecordExplicitLeafStructuralRejectReason(ExplicitLeafStructuralRejectReason reason)
    {
        switch (reason)
        {
            case ExplicitLeafStructuralRejectReason.NonInsertOnlyDelta:
                Interlocked.Increment(ref _explicitLeafRebaseRejectNonInsertOnlyCount);
                break;
            case ExplicitLeafStructuralRejectReason.DuplicateKey:
                Interlocked.Increment(ref _explicitLeafRebaseRejectDuplicateKeyCount);
                break;
            case ExplicitLeafStructuralRejectReason.SplitFallbackPrecondition:
                Interlocked.Increment(ref _explicitLeafRebaseRejectSplitFallbackPreconditionCount);
                break;
            case ExplicitLeafStructuralRejectReason.SplitFallbackShape:
                Interlocked.Increment(ref _explicitLeafRebaseRejectSplitFallbackShapeCount);
                break;
            case ExplicitLeafStructuralRejectReason.Other:
                Interlocked.Increment(ref _explicitLeafRebaseRejectOtherCount);
                break;
        }
    }

    private void RecordExplicitLeafSplitFallbackRejectReason(ExplicitLeafSplitFallbackRejectReason reason)
    {
        switch (reason)
        {
            case ExplicitLeafSplitFallbackRejectReason.MissingTraversal:
                Interlocked.Increment(ref _explicitLeafRebaseRejectSplitFallbackMissingTraversalCount);
                break;
            case ExplicitLeafSplitFallbackRejectReason.DirtyAncestor:
                Interlocked.Increment(ref _explicitLeafRebaseRejectSplitFallbackDirtyAncestorCount);
                break;
            case ExplicitLeafSplitFallbackRejectReason.ParentBoundaryMissing:
                Interlocked.Increment(ref _explicitLeafRebaseRejectSplitFallbackParentBoundaryCount);
                break;
            case ExplicitLeafSplitFallbackRejectReason.TargetPageDirty:
                Interlocked.Increment(ref _explicitLeafRebaseRejectSplitFallbackTargetPageDirtyCount);
                break;
        }
    }

    private void RecordDirtyParentLeafSplitRecoveryRejectReason(DirtyParentLeafSplitRecoveryRejectReason reason)
    {
        switch (reason)
        {
            case DirtyParentLeafSplitRecoveryRejectReason.MissingParentPage:
                Interlocked.Increment(ref _explicitLeafRebaseRejectDirtyParentMissingParentPageCount);
                break;
            case DirtyParentLeafSplitRecoveryRejectReason.TransactionLeafNotSplit:
                Interlocked.Increment(ref _explicitLeafRebaseRejectDirtyParentTransactionLeafNotSplitCount);
                break;
            case DirtyParentLeafSplitRecoveryRejectReason.BaseParentBoundaryMissing:
                Interlocked.Increment(ref _explicitLeafRebaseRejectDirtyParentBaseBoundaryMissingCount);
                break;
            case DirtyParentLeafSplitRecoveryRejectReason.ParentInsertionShape:
                Interlocked.Increment(ref _explicitLeafRebaseRejectDirtyParentInsertionShapeCount);
                break;
            case DirtyParentLeafSplitRecoveryRejectReason.ParentInsertionMismatch:
                Interlocked.Increment(ref _explicitLeafRebaseRejectDirtyParentInsertionMismatchCount);
                break;
            case DirtyParentLeafSplitRecoveryRejectReason.MissingLocalRightPage:
                Interlocked.Increment(ref _explicitLeafRebaseRejectDirtyParentMissingLocalRightPageCount);
                break;
            case DirtyParentLeafSplitRecoveryRejectReason.LocalSplitShape:
                Interlocked.Increment(ref _explicitLeafRebaseRejectDirtyParentLocalSplitShapeCount);
                break;
            case DirtyParentLeafSplitRecoveryRejectReason.RebaseFailure:
                Interlocked.Increment(ref _explicitLeafRebaseRejectDirtyParentRebaseFailureCount);
                break;
        }
    }

    private void RecordExplicitInteriorRebaseDiagnostics(InsertOnlyRebaseResult result)
    {
        if (result == InsertOnlyRebaseResult.NotApplicable)
            return;

        Interlocked.Increment(ref _explicitInteriorRebaseAttemptCount);
        switch (result)
        {
            case InsertOnlyRebaseResult.Success:
                Interlocked.Increment(ref _explicitInteriorRebaseSuccessCount);
                break;
            case InsertOnlyRebaseResult.StructuralReject:
                Interlocked.Increment(ref _explicitInteriorRebaseStructuralRejectCount);
                break;
            case InsertOnlyRebaseResult.CapacityReject:
                Interlocked.Increment(ref _explicitInteriorRebaseCapacityRejectCount);
                break;
        }
    }

    internal async ValueTask AcquireSchemaWriteLockAsync(CancellationToken ct = default)
    {
        if (GetCurrentTransaction() is not { } tx || _transactions is null)
            return;

        if (!tx.HasSchemaWriteLock)
        {
            await _transactions.AcquireSchemaExclusiveAsync(tx.TransactionId, ct);
            tx.HasSchemaWriteLock = true;
        }

        tx.LogicalWriteKeys.Add(SchemaConflictKey);
    }

    private PagerTransactionState? GetCurrentTransaction() => _ambientTransaction.Value;

    private void RecordLogicalRead(string resourceName, long key)
    {
        if (GetCurrentTransaction() is not { } tx)
            return;
        if (_suppressedLogicalReadTrackingDepth.Value > 0)
            return;

        tx.LogicalReadKeys.Add(new LogicalConflictKey(resourceName, key));
    }

    private void RecordLogicalWrite(string resourceName, long key)
    {
        if (GetCurrentTransaction() is not { } tx)
        {
            if (_transactions?.InTransaction == true)
            {
                lock (_legacyLogicalWriteGate)
                {
                    _legacyLogicalWriteKeys.Add(new LogicalConflictKey(resourceName, key));
                }
            }

            return;
        }

        tx.LogicalWriteKeys.Add(new LogicalConflictKey(resourceName, key));
    }

    private void RecordLogicalRange(string resourceName, IndexScanRange range)
    {
        if (GetCurrentTransaction() is not { } tx)
            return;
        if (_suppressedLogicalReadTrackingDepth.Value > 0)
            return;

        tx.LogicalReadRanges.Add(new LogicalConflictRange(resourceName, range));
    }

    private async ValueTask<byte[]> GetTransactionPageAsync(
        PagerTransactionState tx,
        uint pageId,
        CancellationToken ct)
    {
        if (tx.ModifiedPages.TryGetValue(pageId, out var modified))
            return modified;

        byte[] clonedPage = await CloneSnapshotPageAsync(tx, pageId, ct);
        tx.ModifiedPages[pageId] = clonedPage;
        return clonedPage;
    }

    private async ValueTask<PageReadBuffer> GetTransactionPageReadAsync(
        PagerTransactionState tx,
        uint pageId,
        CancellationToken ct)
    {
        if (tx.ModifiedPages.TryGetValue(pageId, out var modified))
            return PageReadBuffer.FromOwnedBuffer(modified);

        return await _buffers.GetSnapshotPageReadAsync(pageId, tx.Snapshot, ct);
    }

    private async ValueTask<byte[]> CloneSnapshotPageAsync(
        PagerTransactionState tx,
        uint pageId,
        CancellationToken ct)
    {
        PageReadBuffer sourcePage = await _buffers.GetSnapshotPageReadAsync(pageId, tx.Snapshot, ct);
        byte[] clone = GC.AllocateUninitializedArray<byte>(PageConstants.PageSize);
        sourcePage.Memory.Span.CopyTo(clone);
        return clone;
    }

    private async ValueTask<uint> AllocateTransactionPageAsync(PagerTransactionState tx, CancellationToken ct)
    {
        if (_isSnapshotReader)
            throw new InvalidOperationException("Cannot allocate pages on a read-only snapshot pager.");

        if (tx.PendingFreelistPageIds.Count > 0)
        {
            int lastIndex = tx.PendingFreelistPageIds.Count - 1;
            uint reusablePageId = tx.PendingFreelistPageIds[lastIndex];
            tx.PendingFreelistPageIds.RemoveAt(lastIndex);

            byte[] page = await GetTransactionPageAsync(tx, reusablePageId, ct);
            Array.Clear(page);
            tx.DirtyPages.Add(reusablePageId);
            tx.HasFreelistHeadOverride = true;
            return reusablePageId;
        }

        if (tx.FreelistHead != PageConstants.NullPageId)
        {
            uint freelistPageId = tx.FreelistHead;
            byte[] freePage = await GetTransactionPageAsync(tx, freelistPageId, ct);
            tx.FreelistHead = ReadFreelistNextPageId(freelistPageId, freePage);
            tx.HasFreelistHeadOverride = true;
            tx.ConsumedFreelistPageIds.Add(freelistPageId);
            Array.Clear(freePage);
            tx.DirtyPages.Add(freelistPageId);
            return freelistPageId;
        }

        uint newPageId = _transactions!.ReserveNewPageId();
        tx.PageCount = Math.Max(tx.PageCount, newPageId + 1);
        tx.HasPageCountOverride = true;
        tx.ModifiedPages[newPageId] = new byte[PageConstants.PageSize];
        tx.DirtyPages.Add(newPageId);
        return newPageId;
    }

    private async ValueTask FreeTransactionPageAsync(
        PagerTransactionState tx,
        uint pageId,
        CancellationToken ct)
    {
        if (_isSnapshotReader)
            throw new InvalidOperationException("Cannot free pages on a read-only snapshot pager.");
        if (pageId == PageConstants.NullPageId)
            throw new CSharpDbException(ErrorCode.CorruptDatabase, "Cannot free the database header page.");

        byte[] page = await GetTransactionPageAsync(tx, pageId, ct);
        Array.Clear(page);
        WriteFreelistLink(pageId, page, PageConstants.NullPageId);
        tx.PendingFreelistPageIds.Add(pageId);
        tx.HasFreelistHeadOverride = true;
        tx.DirtyPages.Add(pageId);
    }

    private async ValueTask WaitForPendingExplicitCommitReservationsAsync(CancellationToken ct)
    {
        while (true)
        {
            Task waitTask;
            lock (_explicitCommitStateGate)
            {
                if (_pendingExplicitCommitCount == 0)
                    return;

                waitTask = _explicitCommitStateChanged.Task;
            }

            await waitTask.WaitAsync(ct);
        }
    }

    private static uint ReadFreelistNextPageId(uint pageId, ReadOnlySpan<byte> page)
    {
        int contentOffset = PageConstants.ContentOffset(pageId);
        if (page[contentOffset + PageConstants.PageTypeOffset] != PageConstants.PageTypeFreelist)
        {
            throw new CSharpDbException(
                ErrorCode.CorruptDatabase,
                $"Freelist page {pageId} has unexpected page type 0x{page[contentOffset + PageConstants.PageTypeOffset]:X2}.");
        }

        return BitConverter.ToUInt32(page.Slice(contentOffset + PageConstants.FreelistNextOffset, sizeof(uint)));
    }

    private static void WriteFreelistLink(uint pageId, Span<byte> page, uint nextPageId)
    {
        int contentOffset = PageConstants.ContentOffset(pageId);
        BitConverter.TryWriteBytes(page.Slice(contentOffset + PageConstants.FreelistNextOffset, sizeof(uint)), nextPageId);
        page[contentOffset + PageConstants.PageTypeOffset] = PageConstants.PageTypeFreelist;
    }

    private async ValueTask<byte[]> GetCommittedWritablePageAsync(
        PagerTransactionState tx,
        uint pageId,
        CancellationToken ct)
    {
        if (tx.ModifiedPages.TryGetValue(pageId, out byte[]? modified))
            return modified;

        PageReadBuffer committedPage = await _buffers.GetPageReadAsync(pageId, ct);
        byte[] clone = GC.AllocateUninitializedArray<byte>(PageConstants.PageSize);
        committedPage.Memory.Span.CopyTo(clone);
        tx.ModifiedPages[pageId] = clone;
        tx.DirtyPages.Add(pageId);
        return clone;
    }

    private async ValueTask<uint> RebaseExplicitFreelistAsync(PagerTransactionState tx, CancellationToken ct)
    {
        if (_transactions is null)
            return tx.FreelistHead;

        if (tx.ConsumedFreelistPageIds.Count == 0 && tx.PendingFreelistPageIds.Count == 0)
            return tx.FreelistHead;

        await WaitForPendingExplicitCommitReservationsAsync(ct);

        uint currentHead = _freelistHead;
        uint rebasedHead = currentHead;

        if (tx.ConsumedFreelistPageIds.Count > 0)
        {
            uint previousKeptPageId = PageConstants.NullPageId;
            bool skippedSincePreviousKept = false;
            uint currentPageId = currentHead;
            var visited = new HashSet<uint>();

            rebasedHead = PageConstants.NullPageId;
            while (currentPageId != PageConstants.NullPageId)
            {
                if (!visited.Add(currentPageId))
                {
                    throw new CSharpDbException(
                        ErrorCode.CorruptDatabase,
                        $"Freelist chain contains a cycle at page {currentPageId}.");
                }

                PageReadBuffer currentPage = await _buffers.GetPageReadAsync(currentPageId, ct);
                uint nextPageId = ReadFreelistNextPageId(currentPageId, currentPage.Memory.Span);
                if (tx.ConsumedFreelistPageIds.Contains(currentPageId))
                {
                    skippedSincePreviousKept = true;
                    currentPageId = nextPageId;
                    continue;
                }

                if (rebasedHead == PageConstants.NullPageId)
                {
                    rebasedHead = currentPageId;
                }
                else if (skippedSincePreviousKept)
                {
                    byte[] previousPage = await GetCommittedWritablePageAsync(tx, previousKeptPageId, ct);
                    WriteFreelistLink(previousKeptPageId, previousPage, currentPageId);
                    tx.ResolvedWriteConflictVersions[previousKeptPageId] = _transactions.CurrentCommitVersion;
                }

                previousKeptPageId = currentPageId;
                skippedSincePreviousKept = false;
                currentPageId = nextPageId;
            }

            if (previousKeptPageId != PageConstants.NullPageId && skippedSincePreviousKept)
            {
                byte[] previousPage = await GetCommittedWritablePageAsync(tx, previousKeptPageId, ct);
                WriteFreelistLink(previousKeptPageId, previousPage, PageConstants.NullPageId);
                tx.ResolvedWriteConflictVersions[previousKeptPageId] = _transactions.CurrentCommitVersion;
            }
        }

        for (int i = 0; i < tx.PendingFreelistPageIds.Count; i++)
        {
            uint freedPageId = tx.PendingFreelistPageIds[i];
            byte[] freedPage = await GetTransactionPageAsync(tx, freedPageId, ct);
            WriteFreelistLink(freedPageId, freedPage, rebasedHead);
            rebasedHead = freedPageId;
        }

        tx.FreelistHead = rebasedHead;
        tx.HasFreelistHeadOverride = true;
        return rebasedHead;
    }

    private static uint[] GetExplicitWritePageIds(PagerTransactionState tx)
    {
        uint[] writePageIds = tx.DirtyPages
            .Where(static pageId => pageId != 0)
            .ToArray();
        if (writePageIds.Length > 1)
            Array.Sort(writePageIds);

        return writePageIds;
    }

    private static bool HaveExplicitWritePageIdsChanged(uint[] priorWritePageIds, HashSet<uint> dirtyPages)
    {
        int dirtyWritePageCount = dirtyPages.Contains(0)
            ? dirtyPages.Count - 1
            : dirtyPages.Count;
        if (dirtyWritePageCount != priorWritePageIds.Length)
            return true;

        for (int i = 0; i < priorWritePageIds.Length; i++)
        {
            if (!dirtyPages.Contains(priorWritePageIds[i]))
                return true;
        }

        return false;
    }

    private async ValueTask<PagerCommitResult> BeginLegacyCommitAsync(CancellationToken ct)
    {
        try
        {
            ChangeCounter++;

            byte[] page0 = await GetPageAsync(0, ct);
            WriteFileHeaderTo(page0);
            _buffers.AddDirty(0);
            int dirtyCount = _buffers.DirtyPages.Count;

            return _hasInterceptor
                ? await BeginLegacyCommitWithInterceptorAsync(dirtyCount, ct)
                : await BeginLegacyCommitFastAsync(dirtyCount, ct);
        }
        catch
        {
            await RollbackAsync(ct);
            await ResetPagerStateFromCommittedStorageAsync(ct);
            throw;
        }
    }

    private async ValueTask<PagerCommitResult> BeginLegacyCommitWithInterceptorAsync(int dirtyCount, CancellationToken ct)
    {
        await _interceptor.OnCommitStartAsync(dirtyCount, ct);

        uint[]? orderedDirtyPageIds = null;
        int orderedDirtyCount = 0;
        TransactionCoordinator.PendingCommitReservation? pendingCommitReservation = null;
        try
        {
            EnforceReaderWalBackpressure(dirtyCount);
            orderedDirtyPageIds = RentSortedPageIds(_buffers.DirtyPages, out orderedDirtyCount);
            long walAppendStartTicks = Stopwatch.GetTimestamp();

            for (int i = 0; i < orderedDirtyCount; i++)
            {
                uint pageId = orderedDirtyPageIds[i];
                if (!_buffers.TryGetDirtyPage(pageId, out var data))
                {
                    throw new CSharpDbException(
                        ErrorCode.Unknown,
                        $"Dirty page {pageId} could not be materialized during commit.");
                }

                bool writeSucceeded = false;
                await _interceptor.OnBeforeWriteAsync(pageId, ct);
                try
                {
                    await _wal.AppendFrameAsync(pageId, data, ct);
                    writeSucceeded = true;
                }
                finally
                {
                    await _interceptor.OnAfterWriteAsync(pageId, writeSucceeded, ct);
                }
            }

            WalCommitResult commitResult = await PrepareWalCommitAsync(ct);
            RecordWalAppendDiagnostics(walAppendStartTicks);
            _buffers.ClearDirty();
            long transactionId = _transactions!.ReleaseWriterAfterCommitAppend();
            if (CanBypassLegacyPendingCommitReservation(commitResult))
            {
                ClearLegacyLogicalWriteTracking();
                return new PagerCommitResult(CompleteLegacyCommitWithoutPendingReservationWithInterceptorAsync(
                    transactionId,
                    dirtyCount));
            }

            pendingCommitReservation = ReserveLegacyPendingCommit(orderedDirtyPageIds, orderedDirtyCount);
            return new PagerCommitResult(CompleteLegacyCommitWithInterceptorAsync(
                commitResult,
                transactionId,
                pendingCommitReservation,
                dirtyCount));
        }
        catch
        {
            if (pendingCommitReservation is not null)
                _transactions!.RevertPendingCommit(pendingCommitReservation);

            ClearLegacyLogicalWriteTracking();
            await _interceptor.OnCommitEndAsync(dirtyCount, succeeded: false, ct);
            throw;
        }
        finally
        {
            if (orderedDirtyPageIds is not null)
                ArrayPool<uint>.Shared.Return(orderedDirtyPageIds, clearArray: false);
        }
    }

    private async ValueTask<PagerCommitResult> BeginLegacyCommitFastAsync(int dirtyCount, CancellationToken ct)
    {
        EnforceReaderWalBackpressure(dirtyCount);

        uint[]? orderedDirtyPageIds = null;
        int orderedDirtyCount = 0;
        WalFrameWrite[]? frameBatch = null;
        int frameCount = 0;
        TransactionCoordinator.PendingCommitReservation? pendingCommitReservation = null;
        try
        {
            long walAppendStartTicks = Stopwatch.GetTimestamp();
            orderedDirtyPageIds = RentSortedPageIds(_buffers.DirtyPages, out orderedDirtyCount);
            frameBatch = ArrayPool<WalFrameWrite>.Shared.Rent(dirtyCount);
            for (int i = 0; i < orderedDirtyCount; i++)
            {
                uint pageId = orderedDirtyPageIds[i];
                if (!_buffers.TryGetDirtyPage(pageId, out var data))
                {
                    throw new CSharpDbException(
                        ErrorCode.Unknown,
                        $"Dirty page {pageId} could not be materialized during commit.");
                }

                frameBatch[frameCount++] = new WalFrameWrite(pageId, data);
            }

            WalCommitResult commitResult = frameCount > 0
                ? await _wal.AppendFramesAndCommitAsync(frameBatch.AsMemory(0, frameCount), PageCount, ct)
                : await _wal.CommitAsync(PageCount, ct);

            RecordWalAppendDiagnostics(walAppendStartTicks);
            _buffers.ClearDirty();
            long transactionId = _transactions!.ReleaseWriterAfterCommitAppend();
            if (CanBypassLegacyPendingCommitReservation(commitResult))
            {
                ClearLegacyLogicalWriteTracking();
                return new PagerCommitResult(CompleteLegacyCommitWithoutPendingReservationAsync(transactionId));
            }

            pendingCommitReservation = ReserveLegacyPendingCommit(orderedDirtyPageIds, orderedDirtyCount);
            return new PagerCommitResult(CompleteLegacyCommitAsync(commitResult, transactionId, pendingCommitReservation));
        }
        catch
        {
            if (pendingCommitReservation is not null)
                _transactions!.RevertPendingCommit(pendingCommitReservation);

            ClearLegacyLogicalWriteTracking();
            throw;
        }
        finally
        {
            if (frameBatch is not null)
            {
                frameBatch.AsSpan(0, frameCount).Clear();
                ArrayPool<WalFrameWrite>.Shared.Return(frameBatch, clearArray: false);
            }

            if (orderedDirtyPageIds is not null)
                ArrayPool<uint>.Shared.Return(orderedDirtyPageIds, clearArray: false);
        }
    }

    private async Task CompleteLegacyCommitAsync(
        WalCommitResult commitResult,
        long transactionId,
        TransactionCoordinator.PendingCommitReservation pendingCommitReservation)
    {
        try
        {
            await commitResult.WaitAsync();
            _transactions!.PublishPendingCommit(pendingCommitReservation);
            await FinalizeLegacyCommitAndCheckpointAsync(transactionId, CancellationToken.None);
        }
        catch
        {
            _transactions!.RevertPendingCommit(pendingCommitReservation);
            await ResetPagerStateFromCommittedStorageAsync(CancellationToken.None);
            throw;
        }
    }

    private Task CompleteLegacyCommitWithoutPendingReservationAsync(long transactionId)
        => CompleteLegacyCommitWithoutPendingReservationCoreAsync(transactionId);

    private async Task CompleteLegacyCommitWithoutPendingReservationCoreAsync(long transactionId)
    {
        try
        {
            await FinalizeLegacyCommitAndCheckpointAsync(transactionId, CancellationToken.None);
        }
        catch
        {
            await ResetPagerStateFromCommittedStorageAsync(CancellationToken.None);
            throw;
        }
    }

    private async Task CompleteLegacyCommitWithInterceptorAsync(
        WalCommitResult commitResult,
        long transactionId,
        TransactionCoordinator.PendingCommitReservation pendingCommitReservation,
        int dirtyCount)
    {
        bool commitSucceeded = false;
        try
        {
            await commitResult.WaitAsync();
            _transactions!.PublishPendingCommit(pendingCommitReservation);
            await FinalizeLegacyCommitAndCheckpointAsync(transactionId, CancellationToken.None);
            commitSucceeded = true;
        }
        catch
        {
            _transactions!.RevertPendingCommit(pendingCommitReservation);
            await ResetPagerStateFromCommittedStorageAsync(CancellationToken.None);
            throw;
        }
        finally
        {
            await _interceptor.OnCommitEndAsync(dirtyCount, commitSucceeded, CancellationToken.None);
        }
    }

    private Task CompleteLegacyCommitWithoutPendingReservationWithInterceptorAsync(
        long transactionId,
        int dirtyCount)
        => CompleteLegacyCommitWithoutPendingReservationWithInterceptorCoreAsync(transactionId, dirtyCount);

    private async Task CompleteLegacyCommitWithoutPendingReservationWithInterceptorCoreAsync(
        long transactionId,
        int dirtyCount)
    {
        bool commitSucceeded = false;
        try
        {
            await FinalizeLegacyCommitAndCheckpointAsync(transactionId, CancellationToken.None);
            commitSucceeded = true;
        }
        catch
        {
            await ResetPagerStateFromCommittedStorageAsync(CancellationToken.None);
            throw;
        }
        finally
        {
            await _interceptor.OnCommitEndAsync(dirtyCount, commitSucceeded, CancellationToken.None);
        }
    }

    private ValueTask<WalCommitResult> PrepareWalCommitAsync(CancellationToken ct)
        => _wal.CommitAsync(PageCount, ct);

    private async ValueTask FinalizeLegacyCommitAndCheckpointAsync(long transactionId, CancellationToken ct)
    {
        long finalizeStartTicks = Stopwatch.GetTimestamp();
        _transactions!.CompleteCommit(transactionId);

        long checkpointDecisionStartTicks = Stopwatch.GetTimestamp();
        bool shouldCheckpoint = _checkpoints!.ShouldCheckpoint(
            CheckpointPolicy,
            _walIndex.FrameCount,
            CheckpointThreshold,
            EstimateCommittedWalBytes(_walIndex.FrameCount));
        if (shouldCheckpoint)
            _checkpoints.RequestDeferredCheckpoint();
        RecordCheckpointDecisionDiagnostics(checkpointDecisionStartTicks);

        bool backgroundMode = _options.AutoCheckpointExecutionMode == AutoCheckpointExecutionMode.Background;
        bool shouldRunForegroundCheckpoint =
            !backgroundMode &&
            _checkpoints.HasPendingCheckpointRequest;
        RecordFinalizeCommitDiagnostics(finalizeStartTicks);

        if (backgroundMode)
        {
            ScheduleBackgroundCheckpointIfNeeded();
            return;
        }

        if (shouldRunForegroundCheckpoint)
            await RunPostCommitForegroundCheckpointAsync(ct);
    }

    private async ValueTask RunPostCommitForegroundCheckpointAsync(CancellationToken ct)
    {
        try
        {
            await CheckpointAsync(ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch
        {
            _checkpoints?.RequestDeferredCheckpoint();
        }
    }

    private async ValueTask<PagerCommitResult> BeginExplicitCommitAsync(PagerTransactionState tx, CancellationToken ct)
    {
        uint[] writePageIds = GetExplicitWritePageIds(tx);
        int dirtyCount = writePageIds.Length + 1; // page 0 is synthesized for every successful commit
        bool commitStarted = false;

        try
        {
            if (writePageIds.Length == 0 &&
                !tx.HasPageCountOverride &&
                !tx.HasSchemaRootPageOverride &&
                !tx.HasFreelistHeadOverride)
            {
                if (_hasInterceptor)
                {
                    await _interceptor.OnCommitStartAsync(dirtyCount, ct);
                    commitStarted = true;
                }

                ValidateLogicalConflicts(tx);
                tx.Completed = true;
                tx.ReleaseSnapshot();
                if (_hasInterceptor)
                    await _interceptor.OnCommitEndAsync(dirtyCount, succeeded: true, CancellationToken.None);
                return PagerCommitResult.Completed;
            }

            WalCommitResult walCommit = default;
            byte[] page0 = Array.Empty<byte>();
            IDisposable? pendingCommitWindow = null;
            ExplicitCommitHeaderReservation? headerReservation = null;
            TransactionCoordinator.PendingCommitReservation? pendingCommitReservation = null;
            bool commitQueued = false;

            try
            {
                while (true)
                {
                    bool waitForCommitStateChange = false;
                    long commitLockWaitStartTicks = Stopwatch.GetTimestamp();
                    IDisposable? commitLock = _transactions is not null
                        ? await _transactions.AcquireCommitLockAsync(ct)
                        : null;
                    RecordExplicitCommitLockWaitDiagnostics(commitLockWaitStartTicks);
                    long commitLockHoldStartTicks = commitLock is not null ? Stopwatch.GetTimestamp() : 0;
                    try
                    {
                        long conflictResolutionStartTicks = Stopwatch.GetTimestamp();
                        bool needsFreelistRebase = tx.ConsumedFreelistPageIds.Count > 0 || tx.PendingFreelistPageIds.Count > 0;
                        while (true)
                        {
                            writePageIds = GetExplicitWritePageIds(tx);
                            bool refreshWritePageIds = false;
                            while (_transactions is not null &&
                                   TryFindExplicitWriteConflict(tx, writePageIds, out uint conflictPageId, out long conflictVersion))
                            {
                                if (conflictVersion > _transactions.CurrentCommitVersion)
                                {
                                    waitForCommitStateChange = true;
                                    break;
                                }

                                if (await TryResolveExplicitWriteConflictAsync(tx, conflictPageId, conflictVersion, ct))
                                {
                                    if (HaveExplicitWritePageIdsChanged(writePageIds, tx.DirtyPages))
                                    {
                                        refreshWritePageIds = true;
                                        break;
                                    }

                                    continue;
                                }

                                throw new CSharpDbConflictException(
                                    $"Transaction conflict detected while committing page {conflictPageId}. The transaction must be retried.");
                            }

                            if (waitForCommitStateChange)
                                break;

                            if (refreshWritePageIds)
                                continue;

                            if (needsFreelistRebase)
                            {
                                await RebaseExplicitFreelistAsync(tx, ct);
                                needsFreelistRebase = false;
                                continue;
                            }

                            break;
                        }

                        if (!waitForCommitStateChange)
                        {
                            ValidateLogicalConflicts(tx);
                            RecordExplicitConflictResolutionDiagnostics(conflictResolutionStartTicks);
                            dirtyCount = writePageIds.Length + 1; // page 0 is synthesized for every successful commit
                            if (_hasInterceptor)
                            {
                                await _interceptor.OnCommitStartAsync(dirtyCount, ct);
                                commitStarted = true;
                            }

                            EnforceReaderWalBackpressure(dirtyCount, ignoredActiveReaders: 1);

                            long pendingCommitWaitStartTicks = Stopwatch.GetTimestamp();
                            pendingCommitWindow = _transactions is not null
                                ? await _transactions.EnterPendingCommitWindowAsync(ct)
                                : null;
                            RecordExplicitPendingCommitWaitDiagnostics(pendingCommitWaitStartTicks);

                            long headerPreparationStartTicks = Stopwatch.GetTimestamp();
                            headerReservation = ReserveExplicitCommitHeaderState(tx);
                            page0 = await BuildCommittedHeaderPageAsync(
                                tx,
                                headerReservation.PageCount,
                                headerReservation.SchemaRootPage,
                                headerReservation.FreelistHead,
                                headerReservation.ChangeCounter,
                                ct);
                            RecordExplicitHeaderPreparationDiagnostics(headerPreparationStartTicks);

                            long walAppendStartTicks = Stopwatch.GetTimestamp();

                            if (_hasInterceptor)
                            {
                                _wal.BeginTransaction();
                                try
                                {
                                    await AppendPageWithInterceptorAsync(0, page0, ct);
                                    for (int i = 0; i < writePageIds.Length; i++)
                                        await AppendPageWithInterceptorAsync(writePageIds[i], tx.ModifiedPages[writePageIds[i]], ct);

                                    walCommit = await _wal.CommitAsync(headerReservation.PageCount, ct);
                                }
                                catch
                                {
                                    try { await _wal.RollbackAsync(ct); } catch { }
                                    throw;
                                }
                            }
                            else
                            {
                                WalFrameWrite[] frameBatch = new WalFrameWrite[dirtyCount];
                                frameBatch[0] = new WalFrameWrite(0, page0);
                                for (int i = 0; i < writePageIds.Length; i++)
                                    frameBatch[i + 1] = new WalFrameWrite(writePageIds[i], tx.ModifiedPages[writePageIds[i]]);

                                walCommit = await _wal.AppendFramesAndCommitAsync(frameBatch, headerReservation.PageCount, ct);
                            }

                            RecordWalAppendDiagnostics(walAppendStartTicks);
                            long pendingCommitReservationStartTicks = Stopwatch.GetTimestamp();
                            pendingCommitReservation = _transactions?.ReservePendingCommit(writePageIds, tx.LogicalWriteKeys)
                                ?? throw new InvalidOperationException("Explicit write transactions require a transaction coordinator.");
                            RecordExplicitPendingCommitReservationDiagnostics(pendingCommitReservationStartTicks);
                            commitQueued = true;
                        }
                    }
                    finally
                    {
                        RecordExplicitCommitLockHoldDiagnostics(commitLockHoldStartTicks);
                        commitLock?.Dispose();
                    }

                    if (!waitForCommitStateChange)
                        break;

                    await _transactions!.WaitForCommitStateChangeAsync(ct);
                }
            }
            catch
            {
                if (!commitQueued)
                {
                    pendingCommitWindow?.Dispose();
                    if (headerReservation is not null)
                        RevertExplicitCommitState(headerReservation);
                }

                throw;
            }

            return new PagerCommitResult(
                CompleteExplicitCommitAsync(
                    tx,
                    dirtyCount,
                    writePageIds,
                    page0,
                    headerReservation!,
                    pendingCommitReservation!,
                    walCommit,
                    pendingCommitWindow));
        }
        catch
        {
            tx.Completed = true;
            tx.ReleaseSnapshot();
            if (_hasInterceptor && commitStarted)
                await _interceptor.OnCommitEndAsync(dirtyCount, succeeded: false, CancellationToken.None);
            throw;
        }
    }

    private async Task CompleteExplicitCommitAsync(
        PagerTransactionState tx,
        int dirtyCount,
        uint[] writePageIds,
        byte[] page0,
        ExplicitCommitHeaderReservation headerReservation,
        TransactionCoordinator.PendingCommitReservation pendingCommitReservation,
        WalCommitResult walCommit,
        IDisposable? pendingCommitWindow)
    {
        bool commitSucceeded = false;
        try
        {
            await walCommit.WaitAsync();

            long finalizeStartTicks = Stopwatch.GetTimestamp();
            _transactions?.PublishPendingCommit(pendingCommitReservation);
            PublishExplicitCommitState(tx, writePageIds, page0, headerReservation, pendingCommitReservation.CommitVersion);

            long checkpointDecisionStartTicks = Stopwatch.GetTimestamp();
            if (_checkpoints is not null &&
                _checkpoints.ShouldCheckpoint(
                    CheckpointPolicy,
                    _walIndex.FrameCount,
                    CheckpointThreshold,
                    EstimateCommittedWalBytes(_walIndex.FrameCount)))
            {
                _checkpoints.RequestDeferredCheckpoint();
            }

            RecordCheckpointDecisionDiagnostics(checkpointDecisionStartTicks);
            RecordFinalizeCommitDiagnostics(finalizeStartTicks);
            commitSucceeded = true;
            tx.Completed = true;
        }
        catch
        {
            _transactions?.RevertPendingCommit(pendingCommitReservation);
            await ResetPagerStateFromCommittedStorageAsync(CancellationToken.None);
            RevertExplicitCommitState(headerReservation);
            tx.Completed = true;
            throw;
        }
        finally
        {
            tx.ReleaseSnapshot();
            pendingCommitWindow?.Dispose();

            if (_hasInterceptor)
                await _interceptor.OnCommitEndAsync(dirtyCount, commitSucceeded, CancellationToken.None);

            if (commitSucceeded)
                await RunPostCommitCheckpointIfNeededAsync(CancellationToken.None);
        }
    }

    private ExplicitCommitHeaderReservation ReserveExplicitCommitHeaderState(PagerTransactionState tx)
    {
        lock (_explicitCommitStateGate)
        {
            if (_pendingExplicitCommitCount == 0)
            {
                _scheduledExplicitPageCount = _pageCount;
                _scheduledExplicitSchemaRootPage = _schemaRootPage;
                _scheduledExplicitFreelistHead = _freelistHead;
                _scheduledExplicitChangeCounter = _changeCounter;
            }

            uint newPageCount = Math.Max(_scheduledExplicitPageCount, tx.PageCount);
            uint newSchemaRootPage = tx.HasSchemaRootPageOverride ? tx.SchemaRootPage : _scheduledExplicitSchemaRootPage;
            uint newFreelistHead = tx.HasFreelistHeadOverride ? tx.FreelistHead : _scheduledExplicitFreelistHead;
            uint newChangeCounter = checked(_scheduledExplicitChangeCounter + 1);

            _scheduledExplicitPageCount = newPageCount;
            _scheduledExplicitSchemaRootPage = newSchemaRootPage;
            _scheduledExplicitFreelistHead = newFreelistHead;
            _scheduledExplicitChangeCounter = newChangeCounter;
            _pendingExplicitCommitCount++;
            SignalExplicitCommitStateChanged_NoLock();

            return new ExplicitCommitHeaderReservation(
                newPageCount,
                newSchemaRootPage,
                newFreelistHead,
                newChangeCounter);
        }
    }

    private void PublishExplicitCommitState(
        PagerTransactionState tx,
        uint[] writePageIds,
        byte[] page0,
        ExplicitCommitHeaderReservation headerReservation,
        long commitVersion)
    {
        lock (_explicitCommitStateGate)
        {
            if (commitVersion > _lastAppliedExplicitCommitVersion)
            {
                _pageCount = headerReservation.PageCount;
                _schemaRootPage = headerReservation.SchemaRootPage;
                _freelistHead = headerReservation.FreelistHead;
                _changeCounter = headerReservation.ChangeCounter;
                _buffers.SetCached(0, page0);
                _lastAppliedExplicitCommitVersion = commitVersion;
                _transactions?.EnsureNextReservedPageIdAtLeast(_pageCount);
            }

            for (int i = 0; i < writePageIds.Length; i++)
                _buffers.SetCached(writePageIds[i], tx.ModifiedPages[writePageIds[i]]);

            if (_pendingExplicitCommitCount > 0)
                _pendingExplicitCommitCount--;

            if (_pendingExplicitCommitCount == 0)
            {
                _scheduledExplicitPageCount = _pageCount;
                _scheduledExplicitSchemaRootPage = _schemaRootPage;
                _scheduledExplicitFreelistHead = _freelistHead;
                _scheduledExplicitChangeCounter = _changeCounter;
            }

            SignalExplicitCommitStateChanged_NoLock();
        }
    }

    private void RevertExplicitCommitState(ExplicitCommitHeaderReservation headerReservation)
    {
        lock (_explicitCommitStateGate)
        {
            if (_pendingExplicitCommitCount > 0)
                _pendingExplicitCommitCount--;

            if (_pendingExplicitCommitCount == 0)
            {
                _scheduledExplicitPageCount = _pageCount;
                _scheduledExplicitSchemaRootPage = _schemaRootPage;
                _scheduledExplicitFreelistHead = _freelistHead;
                _scheduledExplicitChangeCounter = _changeCounter;
            }

            SignalExplicitCommitStateChanged_NoLock();
        }
    }

    private async ValueTask<byte[]> BuildCommittedHeaderPageAsync(
        PagerTransactionState tx,
        uint pageCount,
        uint schemaRootPage,
        uint freelistHead,
        uint changeCounter,
        CancellationToken ct)
    {
        byte[] page0;
        if (tx.ModifiedPages.TryGetValue(0, out var existingHeaderPage))
        {
            page0 = existingHeaderPage;
        }
        else
        {
            PageReadBuffer headerSource = await _buffers.GetSnapshotPageReadAsync(0, tx.Snapshot, ct);
            page0 = GC.AllocateUninitializedArray<byte>(PageConstants.PageSize);
            headerSource.Memory.Span.CopyTo(page0);
        }

        PageConstants.MagicBytes.AsSpan().CopyTo(page0.AsSpan(PageConstants.MagicOffset, PageConstants.MagicBytes.Length));
        BitConverter.TryWriteBytes(page0.AsSpan(PageConstants.VersionOffset, sizeof(int)), PageConstants.FormatVersion);
        BitConverter.TryWriteBytes(page0.AsSpan(PageConstants.PageSizeOffset, sizeof(int)), PageConstants.PageSize);
        BitConverter.TryWriteBytes(page0.AsSpan(PageConstants.PageCountOffset, sizeof(uint)), pageCount);
        BitConverter.TryWriteBytes(page0.AsSpan(PageConstants.SchemaRootPageOffset, sizeof(uint)), schemaRootPage);
        BitConverter.TryWriteBytes(page0.AsSpan(PageConstants.FreelistHeadOffset, sizeof(uint)), freelistHead);
        BitConverter.TryWriteBytes(page0.AsSpan(PageConstants.ChangeCounterOffset, sizeof(uint)), changeCounter);
        return page0;
    }

    private async ValueTask AppendPageWithInterceptorAsync(uint pageId, byte[] pageData, CancellationToken ct)
    {
        bool writeSucceeded = false;
        await _interceptor.OnBeforeWriteAsync(pageId, ct);
        try
        {
            await _wal.AppendFrameAsync(pageId, pageData, ct);
            writeSucceeded = true;
        }
        finally
        {
            await _interceptor.OnAfterWriteAsync(pageId, writeSucceeded, ct);
        }
    }

    private async ValueTask RunPostCommitCheckpointIfNeededAsync(CancellationToken ct)
    {
        if (_checkpoints is null || !_checkpoints.HasPendingCheckpointRequest)
            return;

        if (_options.AutoCheckpointExecutionMode == AutoCheckpointExecutionMode.Background)
        {
            ScheduleBackgroundCheckpointIfNeeded();
            return;
        }

        try
        {
            await CheckpointAsync(ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch
        {
            _checkpoints.RequestDeferredCheckpoint();
        }
    }

    private void SignalExplicitCommitStateChanged_NoLock()
    {
        TaskCompletionSource<bool> completed = _explicitCommitStateChanged;
        _explicitCommitStateChanged = CreateExplicitCommitStateChangedSource();
        completed.TrySetResult(true);
    }

    private static TaskCompletionSource<bool> CreateExplicitCommitStateChangedSource()
        => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private sealed record ExplicitCommitHeaderReservation(
        uint PageCount,
        uint SchemaRootPage,
        uint FreelistHead,
        uint ChangeCounter);

    private sealed class AmbientTransactionBinding : IDisposable
    {
        private readonly AsyncLocal<PagerTransactionState?> _ambientTransaction;
        private readonly PagerTransactionState? _previous;
        private int _disposed;

        public AmbientTransactionBinding(
            AsyncLocal<PagerTransactionState?> ambientTransaction,
            PagerTransactionState? previous)
        {
            _ambientTransaction = ambientTransaction;
            _previous = previous;
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;

            _ambientTransaction.Value = _previous;
        }
    }

    private sealed class LogicalReadTrackingSuppression : IDisposable
    {
        private readonly AsyncLocal<int> _suppressedLogicalReadTrackingDepth;
        private readonly int _previousDepth;
        private int _disposed;

        public LogicalReadTrackingSuppression(
            AsyncLocal<int> suppressedLogicalReadTrackingDepth,
            int previousDepth)
        {
            _suppressedLogicalReadTrackingDepth = suppressedLogicalReadTrackingDepth;
            _previousDepth = previousDepth;
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;

            _suppressedLogicalReadTrackingDepth.Value = _previousDepth;
        }
    }

    private void ReadFileHeaderFrom(byte[] page0)
    {
        PageCount = BitConverter.ToUInt32(page0, PageConstants.PageCountOffset);
        SchemaRootPage = BitConverter.ToUInt32(page0, PageConstants.SchemaRootPageOffset);
        FreelistHead = BitConverter.ToUInt32(page0, PageConstants.FreelistHeadOffset);
        ChangeCounter = BitConverter.ToUInt32(page0, PageConstants.ChangeCounterOffset);
    }

    private async ValueTask ResetPagerStateFromCommittedStorageAsync(CancellationToken ct)
    {
        _buffers.ClearAll();

        // Re-read header from DB file (WAL may have committed data, so check WAL too)
        if (_device.Length >= PageConstants.PageSize)
            await ReadFileHeaderAsync(ct);

        // If WAL has committed page 0, re-read header from WAL version
        if (_walIndex.TryGetLatest(0, out long walOffset))
        {
            await _wal.ReadPageIntoAsync(walOffset, _walHeaderPageBuffer, ct);
            ReadFileHeaderFrom(_walHeaderPageBuffer);
        }
    }

    /// <summary>
    /// Recover from a crash by opening/scanning the WAL.
    /// </summary>
    public async ValueTask RecoverAsync(CancellationToken ct = default)
    {
        if (_hasInterceptor)
        {
            bool recoverySucceeded = false;
            await _interceptor.OnRecoveryStartAsync(ct);
            try
            {
                await RecoverCoreAsync(ct);
                recoverySucceeded = true;
            }
            finally
            {
                await _interceptor.OnRecoveryEndAsync(recoverySucceeded, ct);
            }
        }
        else
        {
            await RecoverCoreAsync(ct);
        }
    }

    private async ValueTask RecoverCoreAsync(CancellationToken ct)
    {
        // WAL recovery: open/scan the WAL file, rebuild index
        await _wal.OpenAsync(PageCount, ct);

        // If WAL has committed data, checkpoint to bring DB file up to date
        if (_walIndex.FrameCount > 0)
        {
            // Read the latest header from WAL if page 0 was committed
            if (_walIndex.TryGetLatest(0, out long walOffset))
            {
                await _wal.ReadPageIntoAsync(walOffset, _walHeaderPageBuffer, ct);
                ReadFileHeaderFrom(_walHeaderPageBuffer);
            }

            await CheckpointAsync(ct);
        }
    }

    /// <summary>
    /// Checkpoint: copy committed WAL pages to the DB file, then reset the WAL.
    /// </summary>
    public async ValueTask CheckpointAsync(CancellationToken ct = default)
    {
        if (_isSnapshotReader)
            throw new InvalidOperationException("Cannot checkpoint on a read-only snapshot pager.");

        await WaitForBackgroundCheckpointAsync(ct);

        await RunCheckpointWithInterceptorsAsync(ct);
        if (!HasCheckpointWorkPending())
            _checkpoints?.ClearDeferredCheckpointRequest();
    }

    private async ValueTask<bool> RunCheckpointWithInterceptorsAsync(CancellationToken ct)
    {
        if (_checkpoints is null || _checkpointAction is null)
            return false;

        int frameCount = _walIndex.FrameCount;
        bool checkpointRan = false;

        if (_hasInterceptor)
        {
            bool checkpointSucceeded = false;
            await _interceptor.OnCheckpointStartAsync(frameCount, ct);
            try
            {
                await _checkpoints.RunCheckpointAsync(
                    frameCount,
                    async innerCt =>
                    {
                        checkpointRan = true;
                        await _checkpointAction(innerCt);
                    },
                    ct);
                if (await TryFinalizeCheckpointAsync(ct))
                    checkpointRan = true;
                checkpointSucceeded = checkpointRan;
            }
            finally
            {
                await _interceptor.OnCheckpointEndAsync(frameCount, checkpointSucceeded, ct);
            }
        }
        else
        {
            await _checkpoints.RunCheckpointAsync(
                frameCount,
                async innerCt =>
                {
                    checkpointRan = true;
                    await _checkpointAction(innerCt);
                },
                ct);
            if (await TryFinalizeCheckpointAsync(ct))
                checkpointRan = true;
        }

        return checkpointRan;
    }

    private async ValueTask RunBackgroundCheckpointStepWithInterceptorsAsync(CancellationToken ct)
    {
        if (_checkpoints is null)
            return;

        while (true)
        {
            if (!HasCheckpointWorkPending())
            {
                _checkpoints.ClearDeferredCheckpointRequest();
                return;
            }

            bool checkpointRan = await RunBackgroundCheckpointStepAsync(ct);
            if (!checkpointRan)
                return;

            if (!HasCheckpointWorkPending())
            {
                _checkpoints.ClearDeferredCheckpointRequest();
                return;
            }

            if (_checkpoints.TryGetMinimumRetainedWalOffset(out _) && _wal.IsCheckpointCopyComplete)
                return;
        }
    }

    private async ValueTask<bool> RunBackgroundCheckpointStepAsync(CancellationToken ct)
    {
        if (_checkpoints is null)
            return false;
        if (GetCurrentTransaction() is not null || _transactions?.InTransaction == true || _wal.HasPendingCommitWork)
            return false;

        int frameCount = _walIndex.FrameCount;
        bool checkpointRan = false;

        if (_hasInterceptor)
        {
            bool checkpointSucceeded = false;
            await _interceptor.OnCheckpointStartAsync(frameCount, ct);
            try
            {
                await _checkpoints.RunCheckpointAsync(
                    frameCount,
                    async innerCt =>
                    {
                        checkpointRan = true;
                        await RunCheckpointStepCoreAsync(innerCt);
                    },
                    ct);
                if (await TryFinalizeCheckpointAsync(ct))
                    checkpointRan = true;
                checkpointSucceeded = checkpointRan;
            }
            finally
            {
                await _interceptor.OnCheckpointEndAsync(frameCount, checkpointSucceeded, ct);
            }
        }
        else
        {
            await _checkpoints.RunCheckpointAsync(
                frameCount,
                async innerCt =>
                {
                    checkpointRan = true;
                    await RunCheckpointStepCoreAsync(innerCt);
                },
                ct);
            if (await TryFinalizeCheckpointAsync(ct))
                checkpointRan = true;
        }

        return checkpointRan;
    }

    private async ValueTask RunCheckpointCoreAsync(CancellationToken ct)
    {
        if (_walIndex.FrameCount == 0 && !_wal.HasPendingCheckpoint)
            return;

        bool wasCopyComplete = _wal.IsCheckpointCopyComplete;
        if (!wasCopyComplete)
        {
            await _wal.CheckpointAsync(_device, PageCount, ct, allowFinalize: false);
            if (_wal.IsCheckpointCopyComplete)
                await RefreshStateAfterCheckpointCompletionAsync(ct);
        }
    }

    private async ValueTask RunCheckpointStepCoreAsync(CancellationToken ct)
    {
        if (_walIndex.FrameCount == 0 && !_wal.HasPendingCheckpoint)
            return;

        bool wasCopyComplete = _wal.IsCheckpointCopyComplete;
        if (!wasCopyComplete)
        {
            bool completed = await _wal.CheckpointStepAsync(
                _device,
                PageCount,
                _options.AutoCheckpointMaxPagesPerStep,
                ct,
                allowFinalize: false);

            if (completed || _wal.IsCheckpointCopyComplete)
                await RefreshStateAfterCheckpointCompletionAsync(ct);
        }
    }

    private async ValueTask<bool> TryFinalizeCheckpointAsync(CancellationToken ct)
    {
        if (_checkpoints is null ||
            _checkpoints.TryGetMinimumRetainedWalOffset(out _) ||
            !_wal.HasPendingCheckpoint ||
            !_wal.IsCheckpointCopyComplete)
        {
            return false;
        }

        IDisposable? checkpointBarrier = _transactions is not null
            ? await _transactions.AcquireCheckpointBarrierAsync(ct)
            : null;

        try
        {
            bool finalized = false;
            int frameCount = _walIndex.FrameCount;
            await _checkpoints.RunCheckpointAsync(
                frameCount,
                async innerCt =>
                {
                    if (_checkpoints.TryGetMinimumRetainedWalOffset(out _) ||
                        !_wal.HasPendingCheckpoint ||
                        !_wal.IsCheckpointCopyComplete)
                    {
                        return;
                    }

                    await _wal.CheckpointAsync(_device, PageCount, innerCt, allowFinalize: true);
                    ProcessCrashInjector.TripIfRequested(
                        "checkpoint-after-wal-finalize",
                        "checkpoint-after-wal-finalize");

                    if (!_wal.HasPendingCheckpoint)
                        await RefreshStateAfterCheckpointCompletionAsync(innerCt);

                    finalized = true;
                },
                ct);

            if (_checkpoints.TryGetMinimumRetainedWalOffset(out _) ||
                !_wal.HasPendingCheckpoint ||
                !_wal.IsCheckpointCopyComplete)
            {
                return finalized;
            }

            return finalized;
        }
        finally
        {
            checkpointBarrier?.Dispose();
        }
    }

    private async ValueTask RefreshStateAfterCheckpointCompletionAsync(CancellationToken ct)
    {
        _buffers.InvalidateCheckpointTransientReads(_options.PreserveOwnedPagesOnCheckpoint);
        RefreshPageReadProviderMapping();
        if (_device.Length >= PageConstants.PageSize)
            await ReadFileHeaderAsync(ct);
    }

    /// <summary>
    /// Acquire a reader snapshot for concurrent reads.
    /// Increments active reader count to prevent checkpoint during reads.
    /// </summary>
    public WalSnapshot AcquireReaderSnapshot()
    {
        long? minimumWalOffset = GetMinimumWalOffsetForNewSnapshot();
        if (_checkpoints == null)
            return _walIndex.TakeSnapshot(minimumWalOffset);

        return _checkpoints.AcquireReaderSnapshot(_walIndex, minimumWalOffset);
    }

    /// <summary>
    /// Release a reader snapshot.
    /// Decrements active reader count, allowing checkpoint to proceed.
    /// </summary>
    public void ReleaseReaderSnapshot(WalSnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);

        bool drained = _checkpoints?.ReleaseReaderSnapshot(snapshot) == true;
        if (drained)
            ScheduleBackgroundCheckpointIfNeeded();
    }

    private long? GetMinimumWalOffsetForNewSnapshot()
    {
        if (!_wal.IsCheckpointCopyComplete ||
            !_wal.TryGetCheckpointRetainedWalStartOffset(out long retainedWalStartOffset))
        {
            return null;
        }

        return retainedWalStartOffset;
    }

    public async ValueTask DisposeAsync()
    {
        Interlocked.Exchange(ref _disposeRequested, 1);

        if (_isSnapshotReader)
        {
            if (_ownsPageReadProviders)
                await DisposePageReadProviderAsync();
            return; // Snapshot readers don't own resources
        }

        if (GetCurrentTransaction() is not null || _transactions?.InTransaction == true)
            await RollbackAsync();

        await WaitForBackgroundCheckpointAsync();

        // Final checkpoint before close
        if (_walIndex.FrameCount > 0)
        {
            try { await CheckpointAsync(); } catch { }
        }

        // Delete WAL file on clean close
        await _wal.CloseAndDeleteAsync();

        if (_ownsPageReadProviders)
            await DisposePageReadProviderAsync();

        if (_device is IAsyncDisposable asyncDisposable)
            await asyncDisposable.DisposeAsync();
        else
            _device.Dispose();

        _checkpoints?.Dispose();
        _transactions?.Dispose();
    }

    public async ValueTask SaveToFileAsync(string filePath, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);

        if (_isSnapshotReader)
            throw new InvalidOperationException("Cannot save from a read-only snapshot pager.");
        if (GetCurrentTransaction() is not null || _transactions?.InTransaction == true)
            throw new InvalidOperationException("Cannot save while a transaction is active.");
        if (ActiveReaderCount > 0)
            throw new InvalidOperationException("Cannot save while reader snapshots are active.");

        await WaitForBackgroundCheckpointAsync(ct);

        if (_walIndex.FrameCount > 0)
            await CheckpointAsync(ct);

        long logicalLength = Math.Max((long)PageCount * PageConstants.PageSize, PageConstants.PageSize);
        string destinationPath = Path.GetFullPath(filePath);
        string? directory = Path.GetDirectoryName(destinationPath);
        if (!string.IsNullOrEmpty(directory))
            Directory.CreateDirectory(directory);

        string tempPath = destinationPath + $".tmp.{Guid.NewGuid():N}";
        var buffer = new byte[64 * 1024];

        try
        {
            await using (var stream = new FileStream(
                             tempPath,
                             FileMode.Create,
                             FileAccess.Write,
                             FileShare.None,
                             bufferSize: buffer.Length,
                             useAsync: true))
            {
                await StorageDeviceCopyBatcher.CopyDeviceRangeToStreamAsync(
                    _device,
                    sourceOffset: 0,
                    byteCount: logicalLength,
                    stream,
                    ct,
                    chunkBytes: buffer.Length);

                await stream.FlushAsync(ct);
            }

            File.Move(tempPath, destinationPath, overwrite: true);
        }
        catch
        {
            try
            {
                if (File.Exists(tempPath))
                    File.Delete(tempPath);
            }
            catch
            {
                // Best-effort cleanup for temporary snapshot files.
            }

            throw;
        }
    }

    public void Dispose()
    {
        Interlocked.Exchange(ref _disposeRequested, 1);

        if (_isSnapshotReader)
        {
            if (_ownsPageReadProviders)
                DisposePageReadProvider();
            return;
        }

        if (GetCurrentTransaction() is not null || _transactions?.InTransaction == true)
            RollbackAsync().AsTask().GetAwaiter().GetResult();
        WaitForBackgroundCheckpointAsync().AsTask().GetAwaiter().GetResult();
        if (_ownsPageReadProviders)
            DisposePageReadProvider();
        _device.Dispose();
        _checkpoints?.Dispose();
        _transactions?.Dispose();
    }

    private void ScheduleBackgroundCheckpointIfNeeded()
    {
        if (_isSnapshotReader ||
            _checkpoints is null ||
            Volatile.Read(ref _disposeRequested) != 0 ||
            _options.AutoCheckpointExecutionMode != AutoCheckpointExecutionMode.Background ||
            GetCurrentTransaction() is not null ||
            _transactions?.InTransaction == true ||
            _wal.HasPendingCommitWork)
        {
            return;
        }

        if (_checkpoints.TryStartBackgroundCheckpoint(RunBackgroundCheckpointStepWithInterceptorsAsync))
            Interlocked.Increment(ref _backgroundCheckpointStartCount);
    }

    private ValueTask WaitForBackgroundCheckpointAsync(CancellationToken ct = default)
    {
        if (_isSnapshotReader ||
            _checkpoints is null ||
            _options.AutoCheckpointExecutionMode != AutoCheckpointExecutionMode.Background)
        {
            return ValueTask.CompletedTask;
        }

        return _checkpoints.WaitForBackgroundCheckpointAsync(ct);
    }

    private static long EstimateCommittedWalBytes(int frameCount)
    {
        if (frameCount <= 0)
            return PageConstants.WalHeaderSize;
        return PageConstants.WalHeaderSize + (long)frameCount * PageConstants.WalFrameSize;
    }

    private bool HasCheckpointWorkPending()
        => _walIndex.FrameCount > 0 || _wal.HasPendingCheckpoint;

    private void ValidateLogicalConflicts(PagerTransactionState tx)
    {
        if (_transactions is null)
            return;

        if (_transactions.HasLogicalConflict(tx.LogicalReadKeys, tx.StartVersion, out LogicalConflictKey conflictKey))
        {
            throw new CSharpDbConflictException(
                $"Transaction conflict detected while validating logical key {conflictKey}. The transaction must be retried.");
        }

        if (_transactions.HasLogicalRangeConflict(tx.LogicalReadRanges, tx.StartVersion, out conflictKey))
        {
            throw new CSharpDbConflictException(
                $"Transaction conflict detected while validating logical range containing {conflictKey}. The transaction must be retried.");
        }
    }

    private bool TryFindExplicitWriteConflict(
        PagerTransactionState tx,
        uint[] writePageIds,
        out uint conflictPageId,
        out long conflictVersion)
    {
        if (_transactions is null)
        {
            conflictPageId = PageConstants.NullPageId;
            conflictVersion = 0;
            return false;
        }

        for (int i = 0; i < writePageIds.Length; i++)
        {
            uint pageId = writePageIds[i];
            if (!_transactions.TryGetPageLastWriteVersion(pageId, out long lastWriteVersion))
                continue;

            long resolvedVersion = tx.ResolvedWriteConflictVersions.TryGetValue(pageId, out long priorResolvedVersion)
                ? Math.Max(tx.StartVersion, priorResolvedVersion)
                : tx.StartVersion;
            if (lastWriteVersion <= resolvedVersion)
                continue;

            conflictPageId = pageId;
            conflictVersion = lastWriteVersion;
            return true;
        }

        conflictPageId = PageConstants.NullPageId;
        conflictVersion = 0;
        return false;
    }

    private async ValueTask<bool> TryResolveExplicitWriteConflictAsync(
        PagerTransactionState tx,
        uint conflictPageId,
        long conflictVersion,
        CancellationToken ct)
    {
        if (_transactions is null ||
            !tx.ModifiedPages.TryGetValue(conflictPageId, out byte[]? transactionPage))
        {
            return false;
        }

        PageReadBuffer basePage = await _buffers.GetSnapshotPageReadAsync(conflictPageId, tx.Snapshot, ct);
        PageReadBuffer committedPage = await _buffers.GetPageReadAsync(conflictPageId, ct);
        if (transactionPage.AsSpan().SequenceEqual(committedPage.Memory.Span))
        {
            tx.ResolvedWriteConflictVersions[conflictPageId] = conflictVersion;
            return true;
        }

        InsertOnlyRebaseResult leafRebaseResult = LeafInsertRebaseHelper.TryRebaseInsertOnlyLeafPage(
            conflictPageId,
            basePage.Memory,
            committedPage.Memory,
            transactionPage,
            out byte[]? rebasedPage,
            out LeafInsertRebaseRejectReason leafRejectReason);
        ExplicitLeafSplitFallbackRejectReason splitFallbackRejectReason = ExplicitLeafSplitFallbackRejectReason.None;
        switch (leafRebaseResult)
        {
            case InsertOnlyRebaseResult.Success:
                RecordExplicitLeafRebaseDiagnostics(InsertOnlyRebaseResult.Success);
                tx.ModifiedPages[conflictPageId] = rebasedPage!;
                tx.ResolvedWriteConflictVersions[conflictPageId] = conflictVersion;
                return true;

            case InsertOnlyRebaseResult.CapacityReject:
                if (await TryResolveExplicitLeafCapacityConflictAsync(
                        tx,
                        conflictPageId,
                        conflictVersion,
                        basePage.Memory,
                        committedPage.Memory,
                        transactionPage,
                        ct))
                {
                    RecordExplicitLeafRebaseDiagnostics(InsertOnlyRebaseResult.Success);
                    return true;
                }
                break;

            case InsertOnlyRebaseResult.StructuralReject:
                (bool splitFallbackResolved, ExplicitLeafSplitFallbackRejectReason splitFallbackReason) =
                    await TryResolveExplicitCommittedLeafSplitConflictAsync(
                        tx,
                        conflictPageId,
                        conflictVersion,
                        basePage.Memory,
                        committedPage.Memory,
                        transactionPage,
                        ct);
                splitFallbackRejectReason = splitFallbackReason;
                if (splitFallbackResolved)
                {
                    RecordExplicitLeafRebaseDiagnostics(InsertOnlyRebaseResult.Success);
                    return true;
                }
                break;
        }

        if (leafRebaseResult == InsertOnlyRebaseResult.StructuralReject)
        {
            if (leafRejectReason == LeafInsertRebaseRejectReason.NextLeafChanged)
                RecordExplicitLeafSplitFallbackRejectReason(splitFallbackRejectReason);

            RecordExplicitLeafStructuralRejectReason(
                ClassifyExplicitLeafStructuralRejectReason(leafRejectReason, splitFallbackRejectReason));
        }

        RecordExplicitLeafRebaseDiagnostics(leafRebaseResult);

        if (leafRebaseResult != InsertOnlyRebaseResult.Success)
        {
            InsertOnlyRebaseResult interiorRebaseResult = InteriorInsertRebaseHelper.TryRebaseInsertOnlyInteriorPage(
                conflictPageId,
                basePage.Memory,
                committedPage.Memory,
                transactionPage,
                out rebasedPage);
            RecordExplicitInteriorRebaseDiagnostics(interiorRebaseResult);
            if (interiorRebaseResult != InsertOnlyRebaseResult.Success)
                return false;
        }

        tx.ModifiedPages[conflictPageId] = rebasedPage!;
        tx.ResolvedWriteConflictVersions[conflictPageId] = conflictVersion;
        return true;
    }

    private async ValueTask<bool> TryResolveExplicitLeafCapacityConflictAsync(
        PagerTransactionState tx,
        uint conflictPageId,
        long conflictVersion,
        ReadOnlyMemory<byte> basePage,
        ReadOnlyMemory<byte> committedPage,
        byte[] transactionPage,
        CancellationToken ct)
    {
        if (_transactions is null ||
            !tx.ExplicitLeafInsertPaths.TryGetValue(conflictPageId, out ExplicitLeafInsertPath traversal) ||
            traversal.PageIds.Length < 2 ||
            traversal.PageIds[^1] != conflictPageId)
        {
            return false;
        }

        for (int i = 0; i < traversal.PageIds.Length - 1; i++)
        {
            if (tx.DirtyPages.Contains(traversal.PageIds[i]))
                return false;
        }

        InsertOnlyRebaseResult splitPlanResult = LeafInsertRebaseHelper.TryPlanSplitInsertOnlyLeafPage(
            conflictPageId,
            basePage,
            committedPage,
            transactionPage,
            out LeafInsertSplitPlan? splitPlan);
        if (splitPlanResult != InsertOnlyRebaseResult.Success)
            return false;

        uint parentPageId = traversal.PageIds[^2];
        PageReadBuffer parentPage = await _buffers.GetPageReadAsync(parentPageId, ct);
        var committedParent = new ReadOnlySlottedPage(parentPage.Memory, parentPageId);
        if (!TryFindInteriorChildBoundary(committedParent, conflictPageId, out uint rightBoundaryChild))
            return false;

        uint placeholderChildId = FindUnusedPlaceholderPageId(committedParent);
        InsertOnlyRebaseResult parentPlanResult = InteriorInsertRebaseHelper.TryApplyCommittedInteriorInsertion(
            parentPageId,
            parentPage.Memory,
            conflictPageId,
            rightBoundaryChild,
            splitPlan!.SplitKey,
            placeholderChildId,
            out _);
        if (parentPlanResult != InsertOnlyRebaseResult.Success)
            return false;

        uint newPageId = await AllocateTransactionPageAsync(tx, ct);
        byte[] rebasedLeftPage = LeafInsertRebaseHelper.BuildLeafPage(
            conflictPageId,
            splitPlan.LeftCells,
            newPageId);
        byte[] rebasedRightPage = LeafInsertRebaseHelper.BuildLeafPage(
            newPageId,
            splitPlan.RightCells,
            splitPlan.OriginalNextLeafPageId);
        InsertOnlyRebaseResult parentApplyResult = InteriorInsertRebaseHelper.TryApplyCommittedInteriorInsertion(
            parentPageId,
            parentPage.Memory,
            conflictPageId,
            rightBoundaryChild,
            splitPlan.SplitKey,
            newPageId,
            out byte[]? rebasedParentPage);
        if (parentApplyResult != InsertOnlyRebaseResult.Success)
            return false;

        tx.ModifiedPages[conflictPageId] = rebasedLeftPage;
        tx.ModifiedPages[newPageId] = rebasedRightPage;
        tx.ModifiedPages[parentPageId] = rebasedParentPage!;
        tx.DirtyPages.Add(conflictPageId);
        tx.DirtyPages.Add(parentPageId);
        tx.DirtyPages.Add(newPageId);

        long resolvedVersion = Math.Max(conflictVersion, _transactions.CurrentCommitVersion);
        tx.ResolvedWriteConflictVersions[conflictPageId] = resolvedVersion;
        tx.ResolvedWriteConflictVersions[parentPageId] = resolvedVersion;

        RecordBTreeLeafSplit(splitPlan.RightEdgeSplit);
        RecordBTreeInteriorInsert(rightBoundaryChild == PageConstants.NullPageId);
        return true;
    }

    private async ValueTask<(bool Success, ExplicitLeafSplitFallbackRejectReason RejectReason)> TryResolveExplicitCommittedLeafSplitConflictAsync(
        PagerTransactionState tx,
        uint conflictPageId,
        long conflictVersion,
        ReadOnlyMemory<byte> basePage,
        ReadOnlyMemory<byte> committedPage,
        byte[] transactionPage,
        CancellationToken ct)
    {
        if (_transactions is null ||
            !tx.ExplicitLeafInsertPaths.TryGetValue(conflictPageId, out ExplicitLeafInsertPath traversal) ||
            traversal.PageIds.Length < 2 ||
            traversal.PageIds[^1] != conflictPageId)
        {
            return (false, ExplicitLeafSplitFallbackRejectReason.MissingTraversal);
        }

        uint parentPageId = traversal.PageIds[^2];
        int dirtyAncestorCount = 0;
        for (int i = 0; i < traversal.PageIds.Length - 1; i++)
        {
            if (tx.DirtyPages.Contains(traversal.PageIds[i]))
                dirtyAncestorCount++;
        }

        if (dirtyAncestorCount > 0)
        {
            if (dirtyAncestorCount != 1 || !tx.DirtyPages.Contains(parentPageId))
                return (false, ExplicitLeafSplitFallbackRejectReason.DirtyAncestor);

            return await TryResolveExplicitCommittedLeafSplitConflictWithDirtyParentAsync(
                tx,
                conflictPageId,
                conflictVersion,
                parentPageId,
                basePage,
                committedPage,
                transactionPage,
                ct);
        }

        PageReadBuffer parentPage = await _buffers.GetPageReadAsync(parentPageId, ct);
        var committedParent = new ReadOnlySlottedPage(parentPage.Memory, parentPageId);
        if (!TryFindInteriorChildBoundary(committedParent, conflictPageId, out uint splitRightPageId) ||
            splitRightPageId == PageConstants.NullPageId)
        {
            return (false, ExplicitLeafSplitFallbackRejectReason.ParentBoundaryMissing);
        }

        if (tx.DirtyPages.Contains(splitRightPageId))
        {
            return (false, ExplicitLeafSplitFallbackRejectReason.TargetPageDirty);
        }

        var committedLeftLeaf = new ReadOnlySlottedPage(committedPage, conflictPageId);
        if (committedLeftLeaf.PageType != PageConstants.PageTypeLeaf ||
            committedLeftLeaf.RightChildOrNextLeaf != splitRightPageId)
        {
            return (false, ExplicitLeafSplitFallbackRejectReason.InvalidCommittedShape);
        }

        PageReadBuffer splitRightPage = await _buffers.GetPageReadAsync(splitRightPageId, ct);
        InsertOnlyRebaseResult splitRebaseResult = LeafInsertRebaseHelper.TryRebaseCommittedSplitLeafPages(
            conflictPageId,
            splitRightPageId,
            basePage,
            committedPage,
            splitRightPage.Memory,
            transactionPage,
            out byte[]? rebasedLeftPage,
            out byte[]? rebasedRightPage,
            out LeafInsertRebaseRejectReason splitRejectReason);
        if (splitRebaseResult != InsertOnlyRebaseResult.Success)
        {
            ExplicitLeafSplitFallbackRejectReason splitFallbackRejectReason =
                splitRejectReason == LeafInsertRebaseRejectReason.InvalidCommittedSplitShape
                    ? ExplicitLeafSplitFallbackRejectReason.InvalidCommittedShape
                    : ExplicitLeafSplitFallbackRejectReason.InvalidCommittedShape;
            return (false, splitFallbackRejectReason);
        }

        ApplyResolvedCommittedLeafSplitPages(
            tx,
            conflictPageId,
            splitRightPageId,
            conflictVersion,
            rebasedLeftPage!,
            rebasedRightPage!);
        return (true, ExplicitLeafSplitFallbackRejectReason.None);
    }

    private async ValueTask<(bool Success, ExplicitLeafSplitFallbackRejectReason RejectReason)> TryResolveExplicitCommittedLeafSplitConflictWithDirtyParentAsync(
        PagerTransactionState tx,
        uint conflictPageId,
        long conflictVersion,
        uint parentPageId,
        ReadOnlyMemory<byte> basePage,
        ReadOnlyMemory<byte> committedPage,
        byte[] transactionPage,
        CancellationToken ct)
    {
        if (!tx.ModifiedPages.TryGetValue(parentPageId, out byte[]? transactionParentPage))
        {
            RecordDirtyParentLeafSplitRecoveryRejectReason(DirtyParentLeafSplitRecoveryRejectReason.MissingParentPage);
            return (false, ExplicitLeafSplitFallbackRejectReason.DirtyAncestor);
        }

        var baseLeaf = new ReadOnlySlottedPage(basePage, conflictPageId);
        var transactionLeaf = new ReadOnlySlottedPage(transactionPage, conflictPageId);
        if (baseLeaf.PageType != PageConstants.PageTypeLeaf ||
            transactionLeaf.PageType != PageConstants.PageTypeLeaf)
        {
            RecordDirtyParentLeafSplitRecoveryRejectReason(DirtyParentLeafSplitRecoveryRejectReason.TransactionLeafNotSplit);
            return (false, ExplicitLeafSplitFallbackRejectReason.DirtyAncestor);
        }

        uint originalNextLeafPageId = baseLeaf.RightChildOrNextLeaf;
        uint transactionSplitRightPageId = transactionLeaf.RightChildOrNextLeaf;
        if (transactionSplitRightPageId == PageConstants.NullPageId ||
            transactionSplitRightPageId == originalNextLeafPageId)
        {
            RecordDirtyParentLeafSplitRecoveryRejectReason(DirtyParentLeafSplitRecoveryRejectReason.TransactionLeafNotSplit);
            return (false, ExplicitLeafSplitFallbackRejectReason.DirtyAncestor);
        }

        if (!tx.ModifiedPages.TryGetValue(transactionSplitRightPageId, out byte[]? transactionSplitRightPage))
        {
            RecordDirtyParentLeafSplitRecoveryRejectReason(DirtyParentLeafSplitRecoveryRejectReason.MissingLocalRightPage);
            return (false, ExplicitLeafSplitFallbackRejectReason.DirtyAncestor);
        }

        if (!LeafInsertRebaseHelper.TryReadFirstLeafKey(
                transactionSplitRightPageId,
                transactionSplitRightPage,
                out long transactionSplitKey))
        {
            RecordDirtyParentLeafSplitRecoveryRejectReason(DirtyParentLeafSplitRecoveryRejectReason.LocalSplitShape);
            return (false, ExplicitLeafSplitFallbackRejectReason.DirtyAncestor);
        }

        PageReadBuffer baseParentPage = await _buffers.GetSnapshotPageReadAsync(parentPageId, tx.Snapshot, ct);
        var baseParent = new ReadOnlySlottedPage(baseParentPage.Memory, parentPageId);
        if (!TryFindInteriorChildBoundary(baseParent, conflictPageId, out uint originalRightBoundaryChild))
        {
            RecordDirtyParentLeafSplitRecoveryRejectReason(DirtyParentLeafSplitRecoveryRejectReason.BaseParentBoundaryMissing);
            return (false, ExplicitLeafSplitFallbackRejectReason.DirtyAncestor);
        }

        InsertOnlyRebaseResult baseParentPlanResult = InteriorInsertRebaseHelper.TryApplyCommittedInteriorInsertion(
            parentPageId,
            baseParentPage.Memory,
            conflictPageId,
            originalRightBoundaryChild,
            transactionSplitKey,
            transactionSplitRightPageId,
            out byte[]? expectedTransactionParentPageFromBase);

        PageReadBuffer committedParentPage = await _buffers.GetPageReadAsync(parentPageId, ct);
        var committedParent = new ReadOnlySlottedPage(committedParentPage.Memory, parentPageId);
        if (!TryFindInteriorChildBoundary(committedParent, conflictPageId, out uint splitRightPageId) ||
            splitRightPageId == PageConstants.NullPageId)
        {
            return (false, ExplicitLeafSplitFallbackRejectReason.ParentBoundaryMissing);
        }

        if (tx.DirtyPages.Contains(splitRightPageId) || transactionSplitRightPageId == splitRightPageId)
            return (false, ExplicitLeafSplitFallbackRejectReason.TargetPageDirty);

        var committedLeftLeaf = new ReadOnlySlottedPage(committedPage, conflictPageId);
        if (committedLeftLeaf.PageType != PageConstants.PageTypeLeaf ||
            committedLeftLeaf.RightChildOrNextLeaf != splitRightPageId)
        {
            return (false, ExplicitLeafSplitFallbackRejectReason.InvalidCommittedShape);
        }

        bool matchesBaseParentShape =
            baseParentPlanResult == InsertOnlyRebaseResult.Success &&
            expectedTransactionParentPageFromBase is not null &&
            transactionParentPage.AsSpan().SequenceEqual(expectedTransactionParentPageFromBase);
        if (!matchesBaseParentShape)
        {
            InsertOnlyRebaseResult committedParentPlanResult = InteriorInsertRebaseHelper.TryApplyCommittedInteriorInsertion(
                parentPageId,
                committedParentPage.Memory,
                conflictPageId,
                originalRightBoundaryChild,
                transactionSplitKey,
                transactionSplitRightPageId,
                out byte[]? expectedTransactionParentPageFromCommitted);
            bool matchesCommittedParentShape =
                committedParentPlanResult == InsertOnlyRebaseResult.Success &&
                expectedTransactionParentPageFromCommitted is not null &&
                transactionParentPage.AsSpan().SequenceEqual(expectedTransactionParentPageFromCommitted);
            bool canDiscardSimpleLeafSplitParent =
                tx.ExplicitLeafInsertPaths.Count == 1 &&
                tx.DirtyPages.Count == 3 &&
                tx.ModifiedPages.ContainsKey(conflictPageId) &&
                tx.ModifiedPages.ContainsKey(parentPageId) &&
                tx.ModifiedPages.ContainsKey(transactionSplitRightPageId);
            if (!matchesCommittedParentShape && !canDiscardSimpleLeafSplitParent)
            {
                InsertOnlyRebaseResult describedInsertionResult = InteriorInsertRebaseHelper.TryDescribeSingleInsertedInteriorEntry(
                    parentPageId,
                    baseParentPage.Memory,
                    transactionParentPage,
                    out InteriorInsertRebaseHelper.InteriorInsertion describedInsertion);
                if (describedInsertionResult == InsertOnlyRebaseResult.Success &&
                    describedInsertion.LeftChild == conflictPageId &&
                    describedInsertion.Key == transactionSplitKey &&
                    describedInsertion.NewChild == transactionSplitRightPageId)
                {
                    Interlocked.Increment(ref _explicitLeafRebaseRejectDirtyParentDescribedInsertionMatchCount);
                }

                RecordDirtyParentLeafSplitRecoveryRejectReason(
                    baseParentPlanResult != InsertOnlyRebaseResult.Success &&
                    committedParentPlanResult != InsertOnlyRebaseResult.Success
                        ? DirtyParentLeafSplitRecoveryRejectReason.ParentInsertionShape
                        : DirtyParentLeafSplitRecoveryRejectReason.ParentInsertionMismatch);
                return (false, ExplicitLeafSplitFallbackRejectReason.DirtyAncestor);
            }
        }

        InsertOnlyRebaseResult collectInsertedCellsResult = LeafInsertRebaseHelper.TryCollectInsertedLeafCellsFromSplitPages(
            conflictPageId,
            transactionSplitRightPageId,
            basePage,
            transactionPage,
            transactionSplitRightPage,
            out List<byte[]>? transactionInsertedCells,
            out _);
        if (collectInsertedCellsResult != InsertOnlyRebaseResult.Success)
        {
            RecordDirtyParentLeafSplitRecoveryRejectReason(DirtyParentLeafSplitRecoveryRejectReason.LocalSplitShape);
            return (false, ExplicitLeafSplitFallbackRejectReason.DirtyAncestor);
        }

        PageReadBuffer splitRightPage = await _buffers.GetPageReadAsync(splitRightPageId, ct);
        InsertOnlyRebaseResult splitRebaseResult = LeafInsertRebaseHelper.TryRebaseCommittedSplitLeafPagesWithInsertedCells(
            conflictPageId,
            splitRightPageId,
            basePage,
            committedPage,
            splitRightPage.Memory,
            transactionInsertedCells!,
            out byte[]? rebasedLeftPage,
            out byte[]? rebasedRightPage,
            out LeafInsertRebaseRejectReason splitRejectReason);
        if (splitRebaseResult != InsertOnlyRebaseResult.Success)
        {
            RecordDirtyParentLeafSplitRecoveryRejectReason(DirtyParentLeafSplitRecoveryRejectReason.RebaseFailure);
            ExplicitLeafSplitFallbackRejectReason splitFallbackRejectReason =
                splitRejectReason == LeafInsertRebaseRejectReason.InvalidCommittedSplitShape
                    ? ExplicitLeafSplitFallbackRejectReason.InvalidCommittedShape
                    : ExplicitLeafSplitFallbackRejectReason.DirtyAncestor;
            return (false, splitFallbackRejectReason);
        }

        tx.ModifiedPages.Remove(parentPageId);
        tx.DirtyPages.Remove(parentPageId);
        tx.ResolvedWriteConflictVersions.Remove(parentPageId);

        await FreeTransactionPageAsync(tx, transactionSplitRightPageId, ct);
        ApplyResolvedCommittedLeafSplitPages(
            tx,
            conflictPageId,
            splitRightPageId,
            conflictVersion,
            rebasedLeftPage!,
            rebasedRightPage!);
        return (true, ExplicitLeafSplitFallbackRejectReason.None);
    }

    private void ApplyResolvedCommittedLeafSplitPages(
        PagerTransactionState tx,
        uint conflictPageId,
        uint splitRightPageId,
        long conflictVersion,
        byte[] rebasedLeftPage,
        byte[] rebasedRightPage)
    {
        tx.ModifiedPages[conflictPageId] = rebasedLeftPage;
        tx.ModifiedPages[splitRightPageId] = rebasedRightPage;
        tx.DirtyPages.Add(conflictPageId);
        tx.DirtyPages.Add(splitRightPageId);

        long resolvedVersion = Math.Max(conflictVersion, _transactions!.CurrentCommitVersion);
        tx.ResolvedWriteConflictVersions[conflictPageId] = resolvedVersion;
        tx.ResolvedWriteConflictVersions[splitRightPageId] = resolvedVersion;
    }

    private static ExplicitLeafStructuralRejectReason ClassifyExplicitLeafStructuralRejectReason(
        LeafInsertRebaseRejectReason leafRejectReason,
        ExplicitLeafSplitFallbackRejectReason splitFallbackRejectReason)
        => leafRejectReason switch
        {
            LeafInsertRebaseRejectReason.NonInsertOnlyDelta => ExplicitLeafStructuralRejectReason.NonInsertOnlyDelta,
            LeafInsertRebaseRejectReason.DuplicateKey => ExplicitLeafStructuralRejectReason.DuplicateKey,
            LeafInsertRebaseRejectReason.NextLeafChanged => splitFallbackRejectReason switch
            {
                ExplicitLeafSplitFallbackRejectReason.MissingTraversal => ExplicitLeafStructuralRejectReason.SplitFallbackPrecondition,
                ExplicitLeafSplitFallbackRejectReason.DirtyAncestor => ExplicitLeafStructuralRejectReason.SplitFallbackPrecondition,
                ExplicitLeafSplitFallbackRejectReason.ParentBoundaryMissing => ExplicitLeafStructuralRejectReason.SplitFallbackPrecondition,
                ExplicitLeafSplitFallbackRejectReason.TargetPageDirty => ExplicitLeafStructuralRejectReason.SplitFallbackPrecondition,
                ExplicitLeafSplitFallbackRejectReason.InvalidCommittedShape => ExplicitLeafStructuralRejectReason.SplitFallbackShape,
                _ => ExplicitLeafStructuralRejectReason.Other,
            },
            _ => ExplicitLeafStructuralRejectReason.Other,
        };

    private static bool TryFindInteriorChildBoundary(
        ReadOnlySlottedPage interior,
        uint leftChild,
        out uint rightBoundaryChild)
    {
        int cellCount = interior.CellCount;
        for (int i = 0; i < cellCount; i++)
        {
            uint currentChild = ReadInteriorLeftChild(interior.GetCellMemory(i).Span);
            if (currentChild != leftChild)
                continue;

            rightBoundaryChild = i + 1 < cellCount
                ? ReadInteriorLeftChild(interior.GetCellMemory(i + 1).Span)
                : interior.RightChildOrNextLeaf;
            return true;
        }

        if (interior.RightChildOrNextLeaf == leftChild)
        {
            rightBoundaryChild = PageConstants.NullPageId;
            return true;
        }

        rightBoundaryChild = PageConstants.NullPageId;
        return false;
    }

    private static uint FindUnusedPlaceholderPageId(ReadOnlySlottedPage interior)
    {
        uint candidate = uint.MaxValue;
        while (candidate != PageConstants.NullPageId)
        {
            if (!ContainsChildPageId(interior, candidate))
                return candidate;

            candidate--;
        }

        throw new CSharpDbException(ErrorCode.CorruptDatabase, "Could not reserve a placeholder child page id for commit-time interior planning.");
    }

    private static bool ContainsChildPageId(ReadOnlySlottedPage interior, uint pageId)
    {
        for (int i = 0; i < interior.CellCount; i++)
        {
            if (ReadInteriorLeftChild(interior.GetCellMemory(i).Span) == pageId)
                return true;
        }

        return interior.RightChildOrNextLeaf == pageId;
    }

    private static uint ReadInteriorLeftChild(ReadOnlySpan<byte> cell)
        => System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(cell.Slice(1, sizeof(uint)));

    private TransactionCoordinator.PendingCommitReservation ReserveLegacyPendingCommit(uint[] orderedDirtyPageIds, int orderedDirtyCount)
    {
        lock (_legacyLogicalWriteGate)
        {
            TransactionCoordinator.PendingCommitReservation reservation = _transactions!.ReservePendingCommit(
                orderedDirtyPageIds,
                orderedDirtyCount,
                _legacyLogicalWriteKeys);
            _legacyLogicalWriteKeys.Clear();
            return reservation;
        }
    }

    private bool CanBypassLegacyPendingCommitReservation(WalCommitResult commitResult)
        => commitResult.IsCompletedSuccessfully &&
           _transactions is not null &&
           !_transactions.HasActiveExplicitTransactions;

    private void ClearLegacyLogicalWriteTracking()
    {
        lock (_legacyLogicalWriteGate)
        {
            _legacyLogicalWriteKeys.Clear();
        }
    }

    private void RecordWalAppendDiagnostics(long startTicks)
    {
        if (startTicks == 0)
            return;

        Interlocked.Increment(ref _walAppendCount);
        Interlocked.Add(ref _walAppendTicks, Stopwatch.GetTimestamp() - startTicks);
    }

    private void RecordExplicitCommitLockWaitDiagnostics(long startTicks)
    {
        if (startTicks == 0)
            return;

        Interlocked.Increment(ref _explicitCommitLockWaitCount);
        Interlocked.Add(ref _explicitCommitLockWaitTicks, Stopwatch.GetTimestamp() - startTicks);
    }

    private void RecordExplicitConflictResolutionDiagnostics(long startTicks)
    {
        if (startTicks == 0)
            return;

        Interlocked.Increment(ref _explicitConflictResolutionCount);
        Interlocked.Add(ref _explicitConflictResolutionTicks, Stopwatch.GetTimestamp() - startTicks);
    }

    private void RecordExplicitCommitLockHoldDiagnostics(long startTicks)
    {
        if (startTicks == 0)
            return;

        Interlocked.Increment(ref _explicitCommitLockHoldCount);
        Interlocked.Add(ref _explicitCommitLockHoldTicks, Stopwatch.GetTimestamp() - startTicks);
    }

    private void RecordExplicitPendingCommitWaitDiagnostics(long startTicks)
    {
        if (startTicks == 0)
            return;

        Interlocked.Increment(ref _explicitPendingCommitWaitCount);
        Interlocked.Add(ref _explicitPendingCommitWaitTicks, Stopwatch.GetTimestamp() - startTicks);
    }

    private void RecordExplicitHeaderPreparationDiagnostics(long startTicks)
    {
        if (startTicks == 0)
            return;

        Interlocked.Increment(ref _explicitHeaderPreparationCount);
        Interlocked.Add(ref _explicitHeaderPreparationTicks, Stopwatch.GetTimestamp() - startTicks);
    }

    private void RecordExplicitPendingCommitReservationDiagnostics(long startTicks)
    {
        if (startTicks == 0)
            return;

        Interlocked.Increment(ref _explicitPendingCommitReservationCount);
        Interlocked.Add(ref _explicitPendingCommitReservationTicks, Stopwatch.GetTimestamp() - startTicks);
    }

    private void RecordFinalizeCommitDiagnostics(long startTicks)
    {
        if (startTicks == 0)
            return;

        Interlocked.Increment(ref _finalizeCommitCount);
        Interlocked.Add(ref _finalizeCommitTicks, Stopwatch.GetTimestamp() - startTicks);
    }

    private void RecordCheckpointDecisionDiagnostics(long startTicks)
    {
        if (startTicks == 0)
            return;

        Interlocked.Increment(ref _checkpointDecisionCount);
        Interlocked.Add(ref _checkpointDecisionTicks, Stopwatch.GetTimestamp() - startTicks);
    }

    private static uint[] RentSortedPageIds(IReadOnlyCollection<uint> pageIds, out int count)
    {
        count = pageIds.Count;
        int capacity = count > 0 ? count : 1;
        uint[] orderedPageIds = ArrayPool<uint>.Shared.Rent(capacity);

        int index = 0;
        foreach (uint pageId in pageIds)
        {
            orderedPageIds[index++] = pageId;
        }

        if (index > 1)
            Array.Sort(orderedPageIds, 0, index);

        count = index;
        return orderedPageIds;
    }

    private static void ValidateOptions(PagerOptions options)
    {
        if (options.MaxWalBytesWhenReadersActive is <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options.MaxWalBytesWhenReadersActive),
                options.MaxWalBytesWhenReadersActive,
                "MaxWalBytesWhenReadersActive must be greater than zero when configured.");
        }

        if (options.AutoCheckpointMaxPagesPerStep <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options.AutoCheckpointMaxPagesPerStep),
                options.AutoCheckpointMaxPagesPerStep,
                "AutoCheckpointMaxPagesPerStep must be greater than zero.");
        }

        if (options.MaxCachedWalReadPages < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options.MaxCachedWalReadPages),
                options.MaxCachedWalReadPages,
                "MaxCachedWalReadPages must be greater than or equal to zero.");
        }
    }

    private static IPageReadProvider CreatePageReadProvider(IStorageDevice device, PagerOptions? options)
    {
        if (options?.UseMemoryMappedReads == true && device is FileStorageDevice fileDevice)
            return new MemoryMappedPageReadProvider(fileDevice);

        return new StorageDevicePageReadProvider(device);
    }

    private static IPageReadProvider CreateSpeculativePageReadProvider(
        IStorageDevice device,
        PagerOptions? options,
        IPageReadProvider primaryPageReads)
    {
        if (options?.UseMemoryMappedReads == true && device is FileStorageDevice)
            return primaryPageReads;

        if (device is FileStorageDevice fileDevice)
            return new StorageDevicePageReadProvider(fileDevice, useSequentialAccessHint: true);

        return primaryPageReads;
    }

    private void RefreshPageReadProviderMapping()
    {
        if (_pageReads is MemoryMappedPageReadProvider mappedReads)
            mappedReads.RefreshMapping();
    }

    private async ValueTask DisposePageReadProviderAsync()
    {
        if (ReferenceEquals(_pageReads, _device))
            return;

        await DisposePageReadProviderInstanceAsync(_speculativePageReads);
        if (!ReferenceEquals(_speculativePageReads, _pageReads))
            await DisposePageReadProviderInstanceAsync(_pageReads);
    }

    private void DisposePageReadProvider()
    {
        if (ReferenceEquals(_pageReads, _device))
            return;

        DisposePageReadProviderInstance(_speculativePageReads);
        if (!ReferenceEquals(_speculativePageReads, _pageReads))
            DisposePageReadProviderInstance(_pageReads);
    }

    private async ValueTask DisposePageReadProviderInstanceAsync(IPageReadProvider pageReads)
    {
        if (ReferenceEquals(pageReads, _device))
            return;

        if (pageReads is IAsyncDisposable asyncPageReads)
            await asyncPageReads.DisposeAsync();
        else if (pageReads is IDisposable disposablePageReads)
            disposablePageReads.Dispose();
    }

    private void DisposePageReadProviderInstance(IPageReadProvider pageReads)
    {
        if (ReferenceEquals(pageReads, _device))
            return;

        if (pageReads is IDisposable disposablePageReads)
            disposablePageReads.Dispose();
    }

    private void EnforceReaderWalBackpressure(int dirtyPageCount, int ignoredActiveReaders = 0)
    {
        if (dirtyPageCount <= 0)
            return;

        long? limitBytes = _options.MaxWalBytesWhenReadersActive;
        if (!limitBytes.HasValue)
            return;

        int activeReaders = (_checkpoints?.ActiveReaderCount ?? 0) - ignoredActiveReaders;
        if (activeReaders <= 0)
            return;

        long projectedBytes = EstimateCommittedWalBytes(_walIndex.FrameCount + dirtyPageCount);
        if (projectedBytes <= limitBytes.Value)
            return;

        throw new CSharpDbException(
            ErrorCode.Busy,
            $"WAL growth limit exceeded while snapshot readers are active (activeReaders={activeReaders}, projectedWalBytes={projectedBytes}, limitBytes={limitBytes.Value}).");
    }

    private static string BuildLogicalIndexResourceName(string indexName)
        => LogicalIndexResourceNames.GetOrAdd(indexName, static name => $"index:{name}");

    internal static string BuildLogicalTableRowResourceName(string tableName)
        => LogicalTableRowResourceNames.GetOrAdd(tableName, static name => $"table:{name}:rowid");

    internal static string BuildLogicalTableColumnResourceName(string tableName, string columnName)
        => LogicalTableColumnResourceNames.GetOrAdd(
            (tableName, columnName),
            static names => $"table:{names.TableName}:column:{names.ColumnName}");
}
