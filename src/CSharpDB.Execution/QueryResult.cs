using System.Runtime.CompilerServices;
using CSharpDB.Primitives;

namespace CSharpDB.Execution;

public sealed class QueryResult : IAsyncDisposable
{
    private const byte DisposedFlag = 1 << 0;
    private const byte OperatorExhaustedFlag = 1 << 1;
    private const byte LifecycleTerminatedFlag = 1 << 2;
    private const byte LifecycleStartedFlag = 1 << 3;

    private static readonly QueryResult ZeroRowsAffectedResult = new(0);
    private static readonly QueryResult OneRowAffectedResult = new(1);
    private static readonly object DirectLifecycleInstallSentinel = new();
    private static readonly ConditionalWeakTable<QueryResult, GeneratedIntegerKeyMetadata> s_generatedIntegerKeys = new();
    private static readonly ConditionalWeakTable<QueryResult, GeneratedRowVersionMetadata> s_generatedRowVersions = new();

    private readonly IOperator? _operator;
    private readonly IBatchOperator? _batchOperator;
    private Func<ValueTask>? _disposeCallback;
    private object? _executionFeatures;
    private bool _opened;
    private byte _lifecycleFlags;
    private DbValue[]? _batchCurrentRow;
    private int _batchRowIndex;
    private bool _batchExhausted;

    // Sync fast path: pre-materialized single-row result (bypasses operator pipeline)
    private readonly bool _hasSyncLookupResult;
    private readonly bool _hasSyncScalarResult;
    private readonly DbValue[]? _syncRow;
    private readonly DbValue _syncScalar;
    private DbValue[]? _syncScalarRow;
    private bool _syncRowConsumed;

    public ColumnDefinition[] Schema { get; }
    public int RowsAffected { get; }
    public bool IsQuery => _operator != null || _batchOperator != null || _hasSyncLookupResult || _hasSyncScalarResult;

    /// <summary>
    /// For SELECT queries.
    /// </summary>
    public QueryResult(IOperator op)
    {
        op = PhysicalPlanCapture.WrapIfActive(op);
        PhysicalPlanCapture.MarkRootIfActive(op);
        _operator = op;
        _batchOperator = null;
        _disposeCallback = null;
        _batchRowIndex = -1;
        _hasSyncLookupResult = false;
        _hasSyncScalarResult = false;
        Schema = op.OutputSchema;
        RowsAffected = 0;
    }

    private QueryResult(IBatchOperator op)
    {
        if (op is IOperator rowOperator)
        {
            op = (IBatchOperator)PhysicalPlanCapture.WrapIfActive(rowOperator);
            PhysicalPlanCapture.MarkRootIfActive((IOperator)op);
        }

        _operator = null;
        _batchOperator = op;
        _disposeCallback = null;
        _batchRowIndex = -1;
        _hasSyncLookupResult = false;
        _hasSyncScalarResult = false;
        Schema = op.OutputSchema;
        RowsAffected = 0;
    }

    /// <summary>
    /// For DML/DDL statements (INSERT, UPDATE, DELETE, CREATE, DROP).
    /// </summary>
    public QueryResult(int rowsAffected)
    {
        _operator = null;
        _batchOperator = null;
        _disposeCallback = null;
        _batchRowIndex = -1;
        _hasSyncLookupResult = false;
        _hasSyncScalarResult = false;
        Schema = Array.Empty<ColumnDefinition>();
        RowsAffected = rowsAffected;
    }

    /// <summary>
    /// For sync fast-path point lookups. Row is null when the key was not found (empty result).
    /// </summary>
    private QueryResult(DbValue[]? syncRow, ColumnDefinition[] schema)
    {
        _operator = null;
        _batchOperator = null;
        _disposeCallback = null;
        _batchRowIndex = -1;
        _hasSyncLookupResult = true;
        _hasSyncScalarResult = false;
        _syncRow = syncRow;
        _syncRowConsumed = false;
        Schema = schema;
        RowsAffected = 0;
    }

    /// <summary>
    /// For sync fast-path scalar results that can defer row materialization until Current/ToList is requested.
    /// </summary>
    private QueryResult(DbValue syncScalar, ColumnDefinition[] schema)
    {
        _operator = null;
        _batchOperator = null;
        _disposeCallback = null;
        _batchRowIndex = -1;
        _hasSyncLookupResult = false;
        _hasSyncScalarResult = true;
        _syncScalar = syncScalar;
        _syncRowConsumed = false;
        Schema = schema;
        RowsAffected = 0;
    }

    /// <summary>
    /// Create a QueryResult for a sync fast-path point lookup.
    /// Row is null when the key was not found.
    /// </summary>
    internal static QueryResult FromSyncLookup(DbValue[]? row, ColumnDefinition[] schema)
        => new(row, schema);

    /// <summary>
    /// Create a QueryResult for a sync fast-path scalar result.
    /// The row wrapper is allocated lazily only if the caller inspects Current or materializes rows.
    /// </summary>
    internal static QueryResult FromSyncScalar(DbValue value, ColumnDefinition[] schema)
        => new(value, schema);

    internal static QueryResult FromBatchOperator(IBatchOperator op)
        => new(op);

    internal void SetObserver(IQueryResultObserver observer)
    {
        ArgumentNullException.ThrowIfNull(observer);

        if (!IsQuery)
            throw new InvalidOperationException("Observers can only be registered for query results.");
        if (HasLifecycleStarted)
            throw new InvalidOperationException("The QueryResult lifecycle has already started.");

        var registration = new QueryResultObserverRegistration(observer);
        var spinner = new SpinWait();
        while (true)
        {
            if (HasLifecycleStarted)
                throw new InvalidOperationException("The QueryResult lifecycle has already started.");

            object? features = Volatile.Read(ref _executionFeatures);
            if (ReferenceEquals(features, DirectLifecycleInstallSentinel))
            {
                spinner.SpinOnce();
                continue;
            }
            if (features is IQueryResultDirectLifecycleRegistration direct &&
                !direct.IsDirectLifecycleCommitted)
            {
                spinner.SpinOnce();
                continue;
            }

            if (GetObserverRegistration(features) is not null)
                throw new InvalidOperationException("An observer is already registered for this QueryResult.");

            bool runtimeScopeRequired = features is RuntimeScopeRequiredFeature;
            object? innerFeatures = UnwrapRuntimeScopeRequirement(features);
            object replacementInner = innerFeatures switch
            {
                null => registration,
                Func<IDisposable> executionScopeFactory =>
                    new QueryResultExecutionFeatures(executionScopeFactory, registration),
                _ => throw new InvalidOperationException("Invalid QueryResult execution features."),
            };
            object replacement = runtimeScopeRequired
                ? new RuntimeScopeRequiredFeature(replacementInner)
                : replacementInner;

            if (Interlocked.CompareExchange(ref _executionFeatures, replacement, features) != features)
                continue;

            if ((HasLifecycleStarted || registration.HasLifecycleStarted) &&
                TryDetachObserver(registration))
                throw new InvalidOperationException("The QueryResult lifecycle has already started.");

            return;
        }
    }

    internal QueryResultDirectLifecycleInstallResult TrySetDirectLifecycleRegistration(
        IQueryResultDirectLifecycleRegistration registration)
    {
        ArgumentNullException.ThrowIfNull(registration);

        if (!IsQuery || HasLifecycleStarted)
            return QueryResultDirectLifecycleInstallResult.TooLateOrConflicting;

        var spinner = new SpinWait();
        while (true)
        {
            if (HasLifecycleStarted)
                return QueryResultDirectLifecycleInstallResult.TooLateOrConflicting;

            object? features = Volatile.Read(ref _executionFeatures);
            if (features is RuntimeScopeRequiredFeature ||
                features is Func<IDisposable>)
                return QueryResultDirectLifecycleInstallResult.NeedsPromotion;
            if (ReferenceEquals(features, DirectLifecycleInstallSentinel))
            {
                spinner.SpinOnce();
                continue;
            }
            if (GetObserverRegistration(features) is not null || features is not null)
                return QueryResultDirectLifecycleInstallResult.TooLateOrConflicting;

            if (Interlocked.CompareExchange(
                    ref _executionFeatures,
                    DirectLifecycleInstallSentinel,
                    null) is not null)
            {
                continue;
            }

            if (HasLifecycleStarted)
            {
                Interlocked.CompareExchange(
                    ref _executionFeatures,
                    null,
                    DirectLifecycleInstallSentinel);
                return QueryResultDirectLifecycleInstallResult.TooLateOrConflicting;
            }

            if (!ReferenceEquals(
                    Interlocked.CompareExchange(
                        ref _executionFeatures,
                        registration,
                        DirectLifecycleInstallSentinel),
                    DirectLifecycleInstallSentinel))
            {
                return QueryResultDirectLifecycleInstallResult.TooLateOrConflicting;
            }

            // Read the registration first because a custom/internal
            // registration may briefly block while its installation state is
            // published. Re-read QueryResult afterward so lifecycle progress
            // during that window cannot be missed by the rollback check.
            bool registrationLifecycleStarted = registration.HasLifecycleStarted;
            if ((HasLifecycleStarted || registrationLifecycleStarted) &&
                TryDetachObserver(registration))
            {
                return QueryResultDirectLifecycleInstallResult.TooLateOrConflicting;
            }

            return QueryResultDirectLifecycleInstallResult.Installed;
        }
    }

    internal IOperator? PhysicalRootOperator =>
        _operator ?? _batchOperator as IOperator;

    internal bool RequiresRuntimeExecutionScope
        => Volatile.Read(ref _executionFeatures) is RuntimeScopeRequiredFeature;

    internal void RequireRuntimeExecutionScope()
    {
        var spinner = new SpinWait();
        while (true)
        {
            object? features = Volatile.Read(ref _executionFeatures);
            if (features is RuntimeScopeRequiredFeature)
                return;
            if (ReferenceEquals(features, DirectLifecycleInstallSentinel))
            {
                spinner.SpinOnce();
                continue;
            }
            if (features is IQueryResultDirectLifecycleRegistration direct)
            {
                if (direct.IsDirectLifecycleCommitted)
                {
                    throw new InvalidOperationException(
                        "A runtime execution scope cannot be required after a direct lifecycle registration.");
                }

                spinner.SpinOnce();
                continue;
            }

            var required = new RuntimeScopeRequiredFeature(features);
            if (ReferenceEquals(
                    Interlocked.CompareExchange(
                        ref _executionFeatures,
                        required,
                        features),
                    features))
            {
                return;
            }
        }
    }

    internal static QueryResult FromRowsAffected(int rowsAffected)
        => rowsAffected switch
        {
            0 => ZeroRowsAffectedResult,
            1 => OneRowAffectedResult,
            _ => new QueryResult(rowsAffected),
        };

    internal static QueryResult FromRowsAffected(int rowsAffected, long? generatedIntegerKey)
        => FromRowsAffected(rowsAffected, generatedIntegerKey, generatedRowVersion: null);

    internal static QueryResult FromRowsAffected(
        int rowsAffected,
        long? generatedIntegerKey,
        byte[]? generatedRowVersion)
    {
        if (generatedIntegerKey.HasValue || generatedRowVersion is not null)
        {
            var result = new QueryResult(rowsAffected);
            if (generatedIntegerKey.HasValue)
                s_generatedIntegerKeys.Add(result, new GeneratedIntegerKeyMetadata(generatedIntegerKey.Value));
            if (generatedRowVersion is not null)
                s_generatedRowVersions.Add(result, new GeneratedRowVersionMetadata(generatedRowVersion));
            return result;
        }

        return FromRowsAffected(rowsAffected);
    }

    internal static QueryResult FromRowsAffected(int rowsAffected, byte[]? generatedRowVersion)
        => FromRowsAffected(rowsAffected, generatedIntegerKey: null, generatedRowVersion);

    internal bool TryGetGeneratedIntegerKey(out long generatedIntegerKey)
    {
        if (s_generatedIntegerKeys.TryGetValue(this, out GeneratedIntegerKeyMetadata? metadata))
        {
            generatedIntegerKey = metadata.Value;
            return true;
        }

        generatedIntegerKey = default;
        return false;
    }

    internal bool TryGetGeneratedRowVersion(out byte[] generatedRowVersion)
    {
        if (s_generatedRowVersions.TryGetValue(this, out GeneratedRowVersionMetadata? metadata))
        {
            generatedRowVersion = (byte[])metadata.Value.Clone();
            return true;
        }

        generatedRowVersion = Array.Empty<byte>();
        return false;
    }

    private sealed class GeneratedIntegerKeyMetadata(long value)
    {
        internal long Value { get; } = value;
    }

    private sealed class GeneratedRowVersionMetadata(byte[] value)
    {
        internal byte[] Value { get; } = (byte[])value.Clone();
    }

    public static QueryResult FromMaterializedRows(ColumnDefinition[] schema, List<DbValue[]> rows)
        => new QueryResult(new MaterializedRowsOperator(schema, rows));

    internal void SetDisposeCallback(Func<ValueTask> disposeCallback)
    {
        ArgumentNullException.ThrowIfNull(disposeCallback);

        if (_disposeCallback != null)
            throw new InvalidOperationException("A dispose callback is already registered for this QueryResult.");

        _disposeCallback = disposeCallback;
    }

    internal void AppendDisposeCallback(Func<ValueTask> disposeCallback)
    {
        ArgumentNullException.ThrowIfNull(disposeCallback);

        Func<ValueTask>? existing = _disposeCallback;
        _disposeCallback = existing is null
            ? disposeCallback
            : () => InvokeDisposeCallbacksAsync(existing, disposeCallback);
    }

    internal void SetExecutionScopeFactory(Func<IDisposable> executionScopeFactory)
    {
        ArgumentNullException.ThrowIfNull(executionScopeFactory);

        var spinner = new SpinWait();
        while (true)
        {
            object? features = Volatile.Read(ref _executionFeatures);
            if (ReferenceEquals(features, DirectLifecycleInstallSentinel))
            {
                spinner.SpinOnce();
                continue;
            }
            if (features is IQueryResultDirectLifecycleRegistration direct &&
                !direct.IsDirectLifecycleCommitted)
            {
                spinner.SpinOnce();
                continue;
            }
            if (GetExecutionScopeFactory(features) is not null)
                throw new InvalidOperationException("An execution scope factory is already registered for this QueryResult.");

            bool runtimeScopeRequired = features is RuntimeScopeRequiredFeature;
            object? innerFeatures = UnwrapRuntimeScopeRequirement(features);
            object replacementInner = innerFeatures switch
            {
                null => executionScopeFactory,
                IQueryResultDirectLifecycleRegistration =>
                    throw new InvalidOperationException(
                        "An execution scope cannot be added after a direct lifecycle registration."),
                IQueryResultLifecycleRegistration registration =>
                    new QueryResultExecutionFeatures(executionScopeFactory, registration),
                _ => throw new InvalidOperationException("Invalid QueryResult execution features."),
            };
            object replacement = runtimeScopeRequired
                ? new RuntimeScopeRequiredFeature(replacementInner)
                : replacementInner;

            if (Interlocked.CompareExchange(ref _executionFeatures, replacement, features) == features)
                return;
        }
    }

    /// <summary>
    /// Adds an outer execution scope while preserving an already registered
    /// inner scope (for example a transaction storage binding). Scopes are
    /// acquired outer-to-inner and always released in reverse order.
    /// </summary>
    internal void PrependExecutionScopeFactory(
        Func<IDisposable> executionScopeFactory)
    {
        ArgumentNullException.ThrowIfNull(executionScopeFactory);

        var spinner = new SpinWait();
        while (true)
        {
            object? features = Volatile.Read(ref _executionFeatures);
            if (ReferenceEquals(features, DirectLifecycleInstallSentinel))
            {
                spinner.SpinOnce();
                continue;
            }
            if (features is IQueryResultDirectLifecycleRegistration direct &&
                !direct.IsDirectLifecycleCommitted)
            {
                spinner.SpinOnce();
                continue;
            }

            bool runtimeScopeRequired = features is RuntimeScopeRequiredFeature;
            object? innerFeatures = UnwrapRuntimeScopeRequirement(features);
            Func<IDisposable>? existing = GetExecutionScopeFactory(innerFeatures);
            Func<IDisposable> composed = existing is null
                ? executionScopeFactory
                : ComposeExecutionScopeFactories(executionScopeFactory, existing);

            object replacementInner = innerFeatures switch
            {
                null => composed,
                Func<IDisposable> => composed,
                IQueryResultDirectLifecycleRegistration =>
                    throw new InvalidOperationException(
                        "An execution scope cannot be added after a direct lifecycle registration."),
                IQueryResultLifecycleRegistration registration =>
                    new QueryResultExecutionFeatures(composed, registration),
                QueryResultExecutionFeatures combined =>
                    new QueryResultExecutionFeatures(
                        composed,
                        combined.ObserverRegistration),
                _ => throw new InvalidOperationException(
                    "Invalid QueryResult execution features."),
            };
            object replacement = runtimeScopeRequired
                ? new RuntimeScopeRequiredFeature(replacementInner)
                : replacementInner;

            if (Interlocked.CompareExchange(ref _executionFeatures, replacement, features) == features)
                return;
        }
    }

    public async IAsyncEnumerable<DbValue[]> GetRowsAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        if (_hasSyncLookupResult || _hasSyncScalarResult)
        {
            while (await MoveNextAsync(ct))
                yield return GetCurrentRowForEnumeration(clone: false);

            yield break;
        }

        if (_operator != null)
        {
            bool cloneRows;
            try
            {
                cloneRows = _operator.ReusesCurrentRowBuffer;
                if (cloneRows && _operator is IRowBufferReuseController controller)
                {
                    controller.SetReuseCurrentRowBuffer(false);
                    cloneRows = _operator.ReusesCurrentRowBuffer;
                }
            }
            catch (OperationCanceledException ex)
            {
                CompleteObserver(QueryResultCompletionReason.Canceled, ex);
                throw;
            }
            catch (Exception ex)
            {
                CompleteObserver(QueryResultCompletionReason.Failed, ex);
                throw;
            }

            while (await MoveNextAsync(ct))
                yield return GetCurrentRowForEnumeration(cloneRows);

            yield break;
        }

        if (_batchOperator == null)
            yield break;

        while (await MoveNextAsync(ct))
            yield return GetCurrentRowForEnumeration(clone: true);
    }

    public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        // Sync fast path
        if (_hasSyncLookupResult || _hasSyncScalarResult)
        {
            if (_syncRowConsumed)
            {
                object? terminalFeatures = Volatile.Read(ref _executionFeatures);
                if (terminalFeatures is null)
                    return ValueTask.FromResult(false);

                IQueryResultDirectLifecycleRegistration? terminalDirectRegistration =
                    terminalFeatures as IQueryResultDirectLifecycleRegistration;
                bool useDirectTerminalLifecycle =
                    terminalDirectRegistration?.IsDirectLifecycleCommitted == true;
                if (useDirectTerminalLifecycle)
                {
                    terminalDirectRegistration!.CompleteSynchronousResult(
                        QueryResultCompletionReason.Exhausted,
                        GetSynchronousRowsProduced());
                }
                else
                {
                    CompleteObserver(
                        terminalFeatures,
                        QueryResultCompletionReason.Exhausted);
                }
                return ValueTask.FromResult(false);
            }

            _syncRowConsumed = true;
            bool hasRow = !_hasSyncLookupResult || _syncRow is not null;

            // Publish lifecycle progress before reading the feature slot. A
            // registration that won before publication is observed here; a
            // registration racing afterward sees the published lifecycle and
            // must roll itself back instead of appearing successfully attached.
            object? features = Volatile.Read(ref _executionFeatures);
            if (features is null)
                return ValueTask.FromResult(hasRow);

            IQueryResultDirectLifecycleRegistration? directRegistration =
                features as IQueryResultDirectLifecycleRegistration;
            bool useDirectLifecycle =
                directRegistration?.IsDirectLifecycleCommitted == true;
            if (!hasRow)
            {
                if (useDirectLifecycle)
                {
                    directRegistration!.CompleteSynchronousResult(
                        QueryResultCompletionReason.Exhausted,
                        rowsProduced: 0);
                }
                else
                {
                    CompleteObserver(
                        features,
                        QueryResultCompletionReason.Exhausted);
                }
                return ValueTask.FromResult(false);
            }

            if (useDirectLifecycle)
                directRegistration!.OnRowProduced();
            else
                NotifyRowProduced(features);
            return ValueTask.FromResult(true);
        }

        if (_operator != null)
        {
            object? features = Volatile.Read(ref _executionFeatures);
            if (features is null)
                return MoveNextOperatorWithoutFeaturesAsync(ct);
            if (features is IQueryResultDirectLifecycleRegistration directRegistration)
            {
                if (IsOperatorExhausted)
                {
                    CompleteObserver(
                        directRegistration,
                        QueryResultCompletionReason.Exhausted);
                    return ValueTask.FromResult(false);
                }

                return MoveNextOperatorWithDirectLifecycleAsync(
                    directRegistration,
                    ct);
            }

            if (IsOperatorExhausted)
            {
                CompleteObserver(QueryResultCompletionReason.Exhausted);
                return ValueTask.FromResult(false);
            }

            return MoveNextOperatorAsync(ct);
        }

        if (_batchOperator == null || _batchExhausted)
        {
            if (_batchExhausted)
                CompleteObserver(QueryResultCompletionReason.Exhausted);
            return ValueTask.FromResult(false);
        }

        object? batchFeatures = Volatile.Read(ref _executionFeatures);
        if (batchFeatures is null)
            return MoveNextBatchWithoutFeaturesAsync(ct);
        if (batchFeatures is IQueryResultDirectLifecycleRegistration batchDirectRegistration)
        {
            return MoveNextBatchWithDirectLifecycleAsync(
                batchDirectRegistration,
                ct);
        }

        return MoveNextBatchAsync(ct);
    }

    public DbValue[] Current
    {
        get
        {
            if (_hasSyncLookupResult)
            {
                if (_syncRow == null)
                    throw new InvalidOperationException("No active query row.");

                return _syncRow;
            }

            if (_hasSyncScalarResult)
                return GetOrCreateSyncScalarRow();

            if (_operator != null)
                return _operator.Current;

            if (_batchCurrentRow != null)
                return _batchCurrentRow;

            throw new InvalidOperationException("No active query result.");
        }
    }

    /// <summary>
    /// Materialize all result rows into a list.
    /// </summary>
    public async ValueTask<List<DbValue[]>> ToListAsync(CancellationToken ct = default)
    {
        try
        {
            if (_hasSyncLookupResult || _hasSyncScalarResult)
            {
                var syncRows = new List<DbValue[]>(1);
                if (!_syncRowConsumed)
                {
                    _syncRowConsumed = true;
                    if (_hasSyncLookupResult)
                    {
                        if (_syncRow != null)
                            syncRows.Add(_syncRow);
                    }
                    else
                    {
                        syncRows.Add(GetOrCreateSyncScalarRow());
                    }
                }

                NotifyRowsProduced(syncRows.Count);
                CompleteObserver(QueryResultCompletionReason.Exhausted);
                return syncRows;
            }

            if (_operator != null)
            {
                if (IsOperatorExhausted)
                {
                    CompleteObserver(QueryResultCompletionReason.Exhausted);
                    return new List<DbValue[]>(0);
                }

                bool cloneRows = _operator.ReusesCurrentRowBuffer;
                if (cloneRows && _operator is IRowBufferReuseController controller)
                {
                    controller.SetReuseCurrentRowBuffer(false);
                    cloneRows = _operator.ReusesCurrentRowBuffer;
                }

                bool openedNow = false;
                if (!_opened)
                {
                    using IDisposable? scope = EnterExecutionScope();
                    await _operator.OpenAsync(ct);
                    _opened = true;
                    openedNow = true;
                }

                if (openedNow &&
                    !cloneRows &&
                    _operator is IMaterializedRowsProvider materialized &&
                    materialized.TryTakeMaterializedRows(out var materializedRows))
                {
                    NotifyRowsProduced(materializedRows.Count);
                    MarkOperatorExhausted();
                    CompleteObserver(QueryResultCompletionReason.Exhausted);
                    return materializedRows;
                }

                int initialCapacity = 0;
                if (_operator is IEstimatedRowCountProvider estimated &&
                    estimated.EstimatedRowCount is int rowCount &&
                    rowCount > 0)
                {
                    initialCapacity = rowCount;
                }

                if (openedNow &&
                    _operator is IBatchBackedRowOperator batchBacked)
                {
                    List<DbValue[]> batchRows = await MaterializeBatchRowsAsync(
                        batchBacked.BatchSource,
                        initialCapacity,
                        -1,
                        GetExecutionScopeFactory(),
                        GetRowProducedCallback(),
                        ct);
                    MarkOperatorExhausted();
                    CompleteObserver(QueryResultCompletionReason.Exhausted);
                    return batchRows;
                }

                var list = initialCapacity > 0
                    ? new List<DbValue[]>(initialCapacity)
                    : new List<DbValue[]>();
                while (true)
                {
                    bool hasRow;
                    using (IDisposable? scope = EnterExecutionScope())
                    {
                        hasRow = await _operator.MoveNextAsync(ct);
                    }

                    if (!hasRow)
                        break;

                    var row = _operator.Current;
                    list.Add(cloneRows ? (DbValue[])row.Clone() : row);
                    NotifyRowProduced();
                }

                MarkOperatorExhausted();
                CompleteObserver(QueryResultCompletionReason.Exhausted);
                return list;
            }

            if (_batchOperator == null)
                return new List<DbValue[]>(0);

            if (!_opened)
            {
                using IDisposable? scope = EnterExecutionScope();
                await _batchOperator.OpenAsync(ct);
                _opened = true;
                _batchRowIndex = -1;
                _batchCurrentRow = null;
                _batchExhausted = false;
            }

            if (_batchExhausted)
            {
                CompleteObserver(QueryResultCompletionReason.Exhausted);
                return new List<DbValue[]>(0);
            }

            if (_batchOperator is IMaterializedRowsProvider batchMaterialized &&
                _batchRowIndex < 0 &&
                _batchOperator.CurrentBatch.Count == 0 &&
                batchMaterialized.TryTakeMaterializedRows(out var directRows))
            {
                NotifyRowsProduced(directRows.Count);
                _batchExhausted = true;
                CompleteObserver(QueryResultCompletionReason.Exhausted);
                return directRows;
            }

            int batchInitialCapacity = 0;
            if (_batchOperator is IEstimatedRowCountProvider batchEstimated &&
                batchEstimated.EstimatedRowCount is int batchRowCount &&
                batchRowCount > 0)
            {
                batchInitialCapacity = batchRowCount;
            }

            var rows = await MaterializeBatchRowsAsync(
                _batchOperator,
                batchInitialCapacity,
                _batchRowIndex,
                GetExecutionScopeFactory(),
                GetRowProducedCallback(),
                ct);
            _batchCurrentRow = null;
            _batchExhausted = true;
            CompleteObserver(QueryResultCompletionReason.Exhausted);
            return rows;
        }
        catch (OperationCanceledException ex)
        {
            CompleteObserver(QueryResultCompletionReason.Canceled, ex);
            throw;
        }
        catch (Exception ex)
        {
            CompleteObserver(QueryResultCompletionReason.Failed, ex);
            throw;
        }
    }

    private DbValue[] GetOrCreateSyncScalarRow()
        => _syncScalarRow ??= [_syncScalar];

    private bool HasLifecycleStarted =>
        _opened ||
        _syncRowConsumed ||
        _batchExhausted ||
        (_lifecycleFlags & (
            DisposedFlag |
            OperatorExhaustedFlag |
            LifecycleTerminatedFlag |
            LifecycleStartedFlag)) != 0;

    private bool IsOperatorExhausted =>
        (_lifecycleFlags & OperatorExhaustedFlag) != 0;

    private void MarkOperatorExhausted()
        => _lifecycleFlags |= OperatorExhaustedFlag;

    private Action? GetRowProducedCallback()
        => GetObserverRegistration() is null ? null : NotifyRowProduced;

    private void NotifyRowProduced()
        => GetObserverRegistration()?.OnRowProduced();

    private static void NotifyRowProduced(object? features)
        => GetObserverRegistration(features)?.OnRowProduced();

    private void NotifyRowsProduced(int count)
    {
        IQueryResultLifecycleRegistration? registration = GetObserverRegistration();
        if (registration is null)
            return;

        for (int rowIndex = 0; rowIndex < count; rowIndex++)
            registration.OnRowProduced();
    }

    private void CompleteObserver(QueryResultCompletionReason reason, Exception? error = null)
    {
        if (reason is QueryResultCompletionReason.Failed or QueryResultCompletionReason.Canceled)
            _lifecycleFlags |= LifecycleTerminatedFlag;

        GetObserverRegistration()?.Complete(reason, error);
    }

    private void CompleteObserver(
        object? features,
        QueryResultCompletionReason reason,
        Exception? error = null)
    {
        if (reason is QueryResultCompletionReason.Failed or QueryResultCompletionReason.Canceled)
            _lifecycleFlags |= LifecycleTerminatedFlag;

        GetObserverRegistration(features)?.Complete(reason, error);
    }

    private void CompleteObserver(
        IQueryResultLifecycleRegistration registration,
        QueryResultCompletionReason reason,
        Exception? error = null)
    {
        if (reason is QueryResultCompletionReason.Failed or QueryResultCompletionReason.Canceled)
            _lifecycleFlags |= LifecycleTerminatedFlag;

        registration.Complete(reason, error);
    }

    private DbValue[] GetCurrentRowForEnumeration(bool clone)
    {
        try
        {
            DbValue[] row = Current;
            return clone ? (DbValue[])row.Clone() : row;
        }
        catch (OperationCanceledException ex)
        {
            CompleteObserver(QueryResultCompletionReason.Canceled, ex);
            throw;
        }
        catch (Exception ex)
        {
            CompleteObserver(QueryResultCompletionReason.Failed, ex);
            throw;
        }
    }

    private DbValue[] MaterializeBatchRow(RowBatch batch, int rowIndex)
    {
        int columnCount = batch.ColumnCount;
        _batchCurrentRow ??= columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];
        if (_batchCurrentRow.Length != columnCount)
            _batchCurrentRow = columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];

        batch.CopyRowTo(rowIndex, _batchCurrentRow);
        return _batchCurrentRow;
    }

    private static async ValueTask<List<DbValue[]>> MaterializeBatchRowsAsync(
        IBatchOperator batchSource,
        int initialCapacity,
        int currentBatchRowIndex,
        Func<IDisposable>? executionScopeFactory,
        Action? rowProduced,
        CancellationToken ct = default)
    {
        var list = initialCapacity > 0
            ? new List<DbValue[]>(initialCapacity)
            : new List<DbValue[]>();

        RowBatch batch = batchSource.CurrentBatch;
        int startRowIndex = Math.Max(0, currentBatchRowIndex + 1);
        for (int rowIndex = startRowIndex; rowIndex < batch.Count; rowIndex++)
        {
            var row = batch.ColumnCount == 0 ? Array.Empty<DbValue>() : new DbValue[batch.ColumnCount];
            batch.CopyRowTo(rowIndex, row);
            list.Add(row);
            rowProduced?.Invoke();
        }

        while (true)
        {
            bool hasNextBatch;
            using (IDisposable? scope = executionScopeFactory?.Invoke())
            {
                hasNextBatch = await batchSource.MoveNextBatchAsync(ct);
            }

            if (!hasNextBatch)
                break;

            batch = batchSource.CurrentBatch;
            int columnCount = batch.ColumnCount;
            for (int rowIndex = 0; rowIndex < batch.Count; rowIndex++)
            {
                var row = columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];
                batch.CopyRowTo(rowIndex, row);
                list.Add(row);
                rowProduced?.Invoke();
            }
        }

        return list;
    }

    public ValueTask DisposeAsync()
    {
        if ((_lifecycleFlags & DisposedFlag) != 0)
            return ValueTask.CompletedTask;

        _lifecycleFlags |= DisposedFlag;
        object? features = Volatile.Read(ref _executionFeatures);
        if ((_hasSyncLookupResult || _hasSyncScalarResult) &&
            _disposeCallback is null &&
            features is IQueryResultDirectLifecycleRegistration directRegistration &&
            directRegistration.IsDirectLifecycleCommitted)
        {
            directRegistration.CompleteSynchronousResult(
                QueryResultCompletionReason.Disposed,
                GetSynchronousRowsProduced());
            return ValueTask.CompletedTask;
        }

        IQueryResultLifecycleRegistration? registration =
            GetObserverRegistration(features);
        if (registration is null)
        {
            if (_operator != null)
                return DisposeOperatorAsync();

            if (_batchOperator != null)
                return DisposeBatchOperatorAsync();

            if (_disposeCallback != null)
                return _disposeCallback();

            return ValueTask.CompletedTask;
        }

        if (!registration.TryStartDisposal())
            return ValueTask.CompletedTask;

        return DisposeObservedAsync(registration);
    }

    private long GetSynchronousRowsProduced()
        => _syncRowConsumed &&
           (!_hasSyncLookupResult || _syncRow is not null)
            ? 1
            : 0;

    private async ValueTask DisposeObservedAsync(
        IQueryResultLifecycleRegistration registration)
    {
        try
        {
            if (_operator != null)
            {
                if (registration is IQueryResultDirectLifecycleRegistration &&
                    ReferenceEquals(
                        Volatile.Read(ref _executionFeatures),
                        registration))
                {
                    await DisposeOperatorWithoutFeaturesAsync();
                }
                else
                {
                    await DisposeOperatorAsync();
                }
            }
            else if (_batchOperator != null)
            {
                if (registration is IQueryResultDirectLifecycleRegistration &&
                    ReferenceEquals(
                        Volatile.Read(ref _executionFeatures),
                        registration))
                {
                    await DisposeBatchOperatorWithoutFeaturesAsync();
                }
                else
                {
                    await DisposeBatchOperatorAsync();
                }
            }
            else if (_disposeCallback != null)
            {
                await _disposeCallback();
            }

            CompleteObserver(registration, QueryResultCompletionReason.Disposed);
        }
        catch (OperationCanceledException ex)
        {
            CompleteObserver(registration, QueryResultCompletionReason.Canceled, ex);
            throw;
        }
        catch (Exception ex)
        {
            CompleteObserver(registration, QueryResultCompletionReason.Failed, ex);
            throw;
        }
    }

    private IDisposable? EnterExecutionScope() => GetExecutionScopeFactory()?.Invoke();

    // Preserve the original no-feature streaming path. This is the common
    // disabled-observability case and deliberately avoids per-row observer,
    // scope, and exception-projection checks.
    private async ValueTask<bool> MoveNextOperatorWithoutFeaturesAsync(CancellationToken ct)
    {
        if (_operator == null)
            return false;

        if (!_opened)
        {
            _lifecycleFlags |= LifecycleStartedFlag;
            await _operator.OpenAsync(ct);
            _opened = true;
        }

        return await _operator.MoveNextAsync(ct);
    }

    private async ValueTask<bool> MoveNextBatchWithoutFeaturesAsync(CancellationToken ct)
    {
        if (_batchOperator == null || _batchExhausted)
            return false;

        if (!_opened)
        {
            _lifecycleFlags |= LifecycleStartedFlag;
            await _batchOperator.OpenAsync(ct);
            _opened = true;
            _batchRowIndex = -1;
            _batchCurrentRow = null;
            _batchExhausted = false;
        }

        while (true)
        {
            RowBatch batch = _batchOperator.CurrentBatch;
            if (_batchRowIndex + 1 < batch.Count)
            {
                _batchRowIndex++;
                _batchCurrentRow = MaterializeBatchRow(batch, _batchRowIndex);
                return true;
            }

            if (!await _batchOperator.MoveNextBatchAsync(ct))
            {
                _batchCurrentRow = null;
                _batchExhausted = true;
                return false;
            }

            _batchRowIndex = -1;
        }
    }

    private async ValueTask<bool> MoveNextOperatorWithDirectLifecycleAsync(
        IQueryResultDirectLifecycleRegistration registration,
        CancellationToken ct)
    {
        if (_operator == null)
            return false;

        try
        {
            if (!_opened)
            {
                await _operator.OpenAsync(ct);
                _opened = true;
            }

            bool hasRow = await _operator.MoveNextAsync(ct);
            if (hasRow)
            {
                registration.OnRowProduced();
            }
            else
            {
                MarkOperatorExhausted();
                CompleteObserver(registration, QueryResultCompletionReason.Exhausted);
            }

            return hasRow;
        }
        catch (OperationCanceledException ex)
        {
            CompleteObserver(registration, QueryResultCompletionReason.Canceled, ex);
            throw;
        }
        catch (Exception ex)
        {
            CompleteObserver(registration, QueryResultCompletionReason.Failed, ex);
            throw;
        }
    }

    private async ValueTask<bool> MoveNextBatchWithDirectLifecycleAsync(
        IQueryResultDirectLifecycleRegistration registration,
        CancellationToken ct)
    {
        if (_batchOperator == null || _batchExhausted)
            return false;

        try
        {
            if (!_opened)
            {
                await _batchOperator.OpenAsync(ct);
                _opened = true;
                _batchRowIndex = -1;
                _batchCurrentRow = null;
                _batchExhausted = false;
            }

            while (true)
            {
                RowBatch batch = _batchOperator.CurrentBatch;
                if (_batchRowIndex + 1 < batch.Count)
                {
                    _batchRowIndex++;
                    _batchCurrentRow = MaterializeBatchRow(batch, _batchRowIndex);
                    registration.OnRowProduced();
                    return true;
                }

                if (!await _batchOperator.MoveNextBatchAsync(ct))
                {
                    _batchCurrentRow = null;
                    _batchExhausted = true;
                    CompleteObserver(registration, QueryResultCompletionReason.Exhausted);
                    return false;
                }

                _batchRowIndex = -1;
            }
        }
        catch (OperationCanceledException ex)
        {
            CompleteObserver(registration, QueryResultCompletionReason.Canceled, ex);
            throw;
        }
        catch (Exception ex)
        {
            CompleteObserver(registration, QueryResultCompletionReason.Failed, ex);
            throw;
        }
    }

    private async ValueTask<bool> MoveNextOperatorAsync(CancellationToken ct)
    {
        if (_operator == null)
            return false;

        try
        {
            if (!_opened)
            {
                using IDisposable? openScope = EnterExecutionScope();
                await _operator.OpenAsync(ct);
                _opened = true;
            }

            bool hasRow;
            using (IDisposable? moveScope = EnterExecutionScope())
            {
                hasRow = await _operator.MoveNextAsync(ct);
            }

            if (hasRow)
            {
                NotifyRowProduced();
            }
            else
            {
                MarkOperatorExhausted();
                CompleteObserver(QueryResultCompletionReason.Exhausted);
            }

            return hasRow;
        }
        catch (OperationCanceledException ex)
        {
            CompleteObserver(QueryResultCompletionReason.Canceled, ex);
            throw;
        }
        catch (Exception ex)
        {
            CompleteObserver(QueryResultCompletionReason.Failed, ex);
            throw;
        }
    }

    private async ValueTask<bool> MoveNextBatchAsync(CancellationToken ct)
    {
        if (_batchOperator == null || _batchExhausted)
            return false;

        try
        {
            if (!_opened)
            {
                using IDisposable? scope = EnterExecutionScope();
                await _batchOperator.OpenAsync(ct);
                _opened = true;
                _batchRowIndex = -1;
                _batchCurrentRow = null;
                _batchExhausted = false;
            }

            while (true)
            {
                RowBatch batch = _batchOperator.CurrentBatch;
                if (_batchRowIndex + 1 < batch.Count)
                {
                    _batchRowIndex++;
                    _batchCurrentRow = MaterializeBatchRow(batch, _batchRowIndex);
                    NotifyRowProduced();
                    return true;
                }

                using IDisposable? scope = EnterExecutionScope();
                if (!await _batchOperator.MoveNextBatchAsync(ct))
                {
                    _batchCurrentRow = null;
                    _batchExhausted = true;
                    CompleteObserver(QueryResultCompletionReason.Exhausted);
                    return false;
                }

                _batchRowIndex = -1;
            }
        }
        catch (OperationCanceledException ex)
        {
            CompleteObserver(QueryResultCompletionReason.Canceled, ex);
            throw;
        }
        catch (Exception ex)
        {
            CompleteObserver(QueryResultCompletionReason.Failed, ex);
            throw;
        }
    }

    private async ValueTask DisposeOperatorAsync()
    {
        try
        {
            if (_operator != null)
            {
                using IDisposable? scope = EnterExecutionScope();
                await _operator.DisposeAsync();
            }
        }
        finally
        {
            if (_disposeCallback != null)
                await _disposeCallback();
        }
    }

    private async ValueTask DisposeOperatorWithoutFeaturesAsync()
    {
        try
        {
            if (_operator != null)
                await _operator.DisposeAsync();
        }
        finally
        {
            if (_disposeCallback != null)
                await _disposeCallback();
        }
    }

    private async ValueTask DisposeBatchOperatorAsync()
    {
        try
        {
            if (_batchOperator != null)
            {
                using IDisposable? scope = EnterExecutionScope();
                await _batchOperator.DisposeAsync();
            }
        }
        finally
        {
            if (_disposeCallback != null)
                await _disposeCallback();
        }
    }

    private async ValueTask DisposeBatchOperatorWithoutFeaturesAsync()
    {
        try
        {
            if (_batchOperator != null)
                await _batchOperator.DisposeAsync();
        }
        finally
        {
            if (_disposeCallback != null)
                await _disposeCallback();
        }
    }

    private static async ValueTask InvokeDisposeCallbacksAsync(
        Func<ValueTask> first,
        Func<ValueTask> second)
    {
        try
        {
            await first();
        }
        finally
        {
            await second();
        }
    }

    private static Func<IDisposable> ComposeExecutionScopeFactories(
        Func<IDisposable> outerScopeFactory,
        Func<IDisposable> innerScopeFactory)
        => () => CompositeExecutionScope.Enter(
            outerScopeFactory,
            innerScopeFactory);

    private IQueryResultLifecycleRegistration? GetObserverRegistration()
        => GetObserverRegistration(Volatile.Read(ref _executionFeatures));

    private static IQueryResultLifecycleRegistration? GetObserverRegistration(object? features)
        => UnwrapRuntimeScopeRequirement(features) switch
        {
            IQueryResultLifecycleRegistration registration => registration,
            QueryResultExecutionFeatures combined => combined.ObserverRegistration,
            _ => null,
        };

    private Func<IDisposable>? GetExecutionScopeFactory()
        => GetExecutionScopeFactory(Volatile.Read(ref _executionFeatures));

    private static Func<IDisposable>? GetExecutionScopeFactory(object? features)
        => UnwrapRuntimeScopeRequirement(features) switch
        {
            Func<IDisposable> executionScopeFactory => executionScopeFactory,
            QueryResultExecutionFeatures combined => combined.ExecutionScopeFactory,
            _ => null,
        };

    private static object? UnwrapRuntimeScopeRequirement(object? features)
        => features is RuntimeScopeRequiredFeature required
            ? required.Features
            : features;

    private bool TryDetachObserver(IQueryResultLifecycleRegistration registration)
    {
        while (true)
        {
            object? features = Volatile.Read(ref _executionFeatures);
            bool runtimeScopeRequired = features is RuntimeScopeRequiredFeature;
            object? innerFeatures = UnwrapRuntimeScopeRequirement(features);
            object? replacement;
            if (ReferenceEquals(innerFeatures, registration))
            {
                replacement = null;
            }
            else if (innerFeatures is QueryResultExecutionFeatures combined &&
                     ReferenceEquals(combined.ObserverRegistration, registration))
            {
                replacement = combined.ExecutionScopeFactory;
            }
            else
            {
                return false;
            }

            if (runtimeScopeRequired)
                replacement = new RuntimeScopeRequiredFeature(replacement);

            if (ReferenceEquals(
                    Interlocked.CompareExchange(ref _executionFeatures, replacement, features),
                    features))
            {
                return true;
            }
        }
    }

    private sealed class RuntimeScopeRequiredFeature(object? features)
    {
        internal object? Features { get; } = features;
    }

    private sealed class QueryResultExecutionFeatures(
        Func<IDisposable> executionScopeFactory,
        IQueryResultLifecycleRegistration observerRegistration)
    {
        internal Func<IDisposable> ExecutionScopeFactory { get; } = executionScopeFactory;

        internal IQueryResultLifecycleRegistration ObserverRegistration { get; } = observerRegistration;
    }

    private sealed class QueryResultObserverRegistration :
        IQueryResultLifecycleRegistration
    {
        // Completion is published in two steps. Reserving it freezes the row
        // count; making it ready publishes the terminal payload. The first-row
        // flag hands terminal callback ownership to whichever side clears or
        // observes it last, so completion can never overtake the first-row
        // callback without putting a lock on every streamed row.
        private const long CompletionReservedFlag = long.MinValue;
        private const long CompletionReadyFlag = 1L << 62;
        private const long FirstRowCallbackInProgressFlag = 1L << 61;
        private const long RowObserverFlag = 1L << 60;
        private const int CompletionReasonShift = 58;
        private const long CompletionReasonMask = 3L << CompletionReasonShift;
        private const long RowCountMask = (1L << CompletionReasonShift) - 1;

        private readonly IQueryResultObserver _observer;
        private Exception? _completionError;
        private long _completionState;
        private int _disposeStarted;

        internal QueryResultObserverRegistration(IQueryResultObserver observer)
        {
            _observer = observer;
            _completionState = observer is IQueryResultRowObserver
                ? RowObserverFlag
                : 0;
        }

        public bool HasLifecycleStarted =>
            (Volatile.Read(ref _completionState) & ~RowObserverFlag) != 0 ||
            Volatile.Read(ref _disposeStarted) != 0;

        public bool TryStartDisposal()
        {
            if (Interlocked.Exchange(ref _disposeStarted, 1) != 0)
                return false;

            InvokeDisposingSafely(_observer);
            return true;
        }

        public void OnRowProduced()
        {
            long observed;
            while (true)
            {
                observed = Volatile.Read(ref _completionState);
                if ((observed & CompletionReservedFlag) != 0)
                    return;

                long rowsProduced = observed & RowCountMask;
                long updated = rowsProduced == RowCountMask
                    ? observed
                    : observed + 1;
                if (rowsProduced == 0)
                    updated |= FirstRowCallbackInProgressFlag;
                if (Interlocked.CompareExchange(
                        ref _completionState,
                        updated,
                        observed) == observed)
                {
                    break;
                }
            }

            // Observer code may re-enter QueryResult or diagnostics. Never
            // invoke it while changing the terminal row-count state.
            if ((observed & RowCountMask) == 0)
            {
                InvokeFirstRowSafely(_observer);
                if ((observed & RowObserverFlag) != 0)
                    InvokeRowSafely((IQueryResultRowObserver)_observer);

                long beforeFirstRowCompleted = Interlocked.And(
                    ref _completionState,
                    ~FirstRowCallbackInProgressFlag);
                if ((beforeFirstRowCompleted & CompletionReadyFlag) != 0)
                {
                    Exception? error = Interlocked.Exchange(
                        ref _completionError,
                        null);
                    InvokeCompletedSafely(
                        _observer,
                        CreateCompletion(beforeFirstRowCompleted, error));
                }
                return;
            }

            if ((observed & RowObserverFlag) != 0)
                InvokeRowSafely((IQueryResultRowObserver)_observer);
        }

        public void Complete(QueryResultCompletionReason reason, Exception? error)
        {
            long observed;
            while (true)
            {
                observed = Volatile.Read(ref _completionState);
                if ((observed & CompletionReservedFlag) != 0)
                    return;

                if (Interlocked.CompareExchange(
                        ref _completionState,
                        observed |
                            CompletionReservedFlag |
                            ((long)reason << CompletionReasonShift),
                        observed) == observed)
                {
                    break;
                }
            }

            if ((observed & FirstRowCallbackInProgressFlag) != 0)
            {
                // Only the pathological first-row/terminal race needs to keep
                // its error after Complete returns. Reuse the registration's
                // former row-observer reference slot and encode the small
                // completion reason in the packed state, so neither the normal
                // object size nor the exceptional path adds an allocation.
                Volatile.Write(ref _completionError, error);
            }

            long beforeCompletionReady = Interlocked.Or(
                ref _completionState,
                CompletionReadyFlag);

            // A first-row callback may synchronously re-enter completion. In
            // that case it owns the pending terminal callback when it clears
            // its in-progress flag. Otherwise this completion owns it. The
            // two interlocked transitions have one total order, so exactly one
            // side invokes the observer without waiting or deadlocking.
            if ((beforeCompletionReady & FirstRowCallbackInProgressFlag) == 0)
            {
                Interlocked.Exchange(ref _completionError, null);
                InvokeCompletedSafely(
                    _observer,
                    new QueryResultCompletion(
                        reason,
                        observed & RowCountMask,
                        error));
            }
        }

        private static QueryResultCompletion CreateCompletion(
            long completionState,
            Exception? error)
            => new(
                (QueryResultCompletionReason)(
                    (completionState & CompletionReasonMask) >>
                    CompletionReasonShift),
                completionState & RowCountMask,
                error);

        private static void InvokeFirstRowSafely(IQueryResultObserver observer)
        {
            try
            {
                observer.OnFirstRowProduced();
            }
            catch
            {
                // Diagnostic observers must never change query behavior.
            }
        }

        private static void InvokeRowSafely(IQueryResultRowObserver observer)
        {
            try
            {
                observer.OnRowProduced();
            }
            catch
            {
                // Diagnostic observers must never change query behavior.
            }
        }

        private static void InvokeDisposingSafely(IQueryResultObserver observer)
        {
            try
            {
                observer.OnDisposing();
            }
            catch
            {
                // Diagnostic observers must never change query behavior.
            }
        }

        private static void InvokeCompletedSafely(
            IQueryResultObserver observer,
            QueryResultCompletion completion)
        {
            try
            {
                observer.OnCompleted(completion);
            }
            catch
            {
                // Diagnostic observers must never change query behavior.
            }
        }
    }

    private sealed class CompositeExecutionScope : IDisposable
    {
        private IDisposable? _outer;
        private IDisposable? _inner;

        private CompositeExecutionScope(IDisposable outer, IDisposable inner)
        {
            _outer = outer;
            _inner = inner;
        }

        internal static CompositeExecutionScope Enter(
            Func<IDisposable> outerScopeFactory,
            Func<IDisposable> innerScopeFactory)
        {
            IDisposable outer = outerScopeFactory();
            try
            {
                IDisposable inner = innerScopeFactory();
                return new CompositeExecutionScope(outer, inner);
            }
            catch
            {
                outer.Dispose();
                throw;
            }
        }

        public void Dispose()
        {
            IDisposable? inner = Interlocked.Exchange(ref _inner, null);
            IDisposable? outer = Interlocked.Exchange(ref _outer, null);
            try
            {
                inner?.Dispose();
            }
            finally
            {
                outer?.Dispose();
            }
        }
    }

    private sealed class MaterializedRowsOperator : IOperator, IMaterializedRowsProvider, IEstimatedRowCountProvider
    {
        private List<DbValue[]>? _rows;
        private int _index = -1;

        internal MaterializedRowsOperator(ColumnDefinition[] outputSchema, List<DbValue[]> rows)
        {
            OutputSchema = outputSchema;
            _rows = rows;
        }

        public ColumnDefinition[] OutputSchema { get; }
        public bool ReusesCurrentRowBuffer => false;
        public int? EstimatedRowCount => _rows?.Count ?? 0;

        public DbValue[] Current => _rows is not null && _index >= 0 && _index < _rows.Count
            ? _rows[_index]
            : throw new InvalidOperationException("No active query row.");

        public ValueTask OpenAsync(CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            return ValueTask.CompletedTask;
        }

        public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();

            if (_rows is null)
                return ValueTask.FromResult(false);

            int nextIndex = _index + 1;
            if (nextIndex >= _rows.Count)
                return ValueTask.FromResult(false);

            _index = nextIndex;
            return ValueTask.FromResult(true);
        }

        public bool TryTakeMaterializedRows(out List<DbValue[]> rows)
        {
            rows = _rows ?? new List<DbValue[]>();
            _rows = null;
            return true;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
