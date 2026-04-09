using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Text;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Execution;

internal enum PreDecodeFilterKind
{
    Comparison,
    IsNull,
    IsNotNull,
    IntegerIn,
    NumericIn,
    TextIn,
}

internal readonly struct PreDecodeFilterSpec
{
    public PreDecodeFilterKind Kind { get; }
    public int ColumnIndex { get; }
    public BinaryOp Op { get; }
    public DbValue Literal { get; }
    public RecordColumnAccessor? Accessor { get; }
    public byte[]? TextBytes { get; }
    public long[]? IntegerSet { get; }
    public double[]? NumericSet { get; }
    public string[]? TextSet { get; }
    public byte[][]? TextSetBytes { get; }

    public PreDecodeFilterSpec(IRecordSerializer serializer, int columnIndex, BinaryOp op, DbValue literal)
    {
        Kind = PreDecodeFilterKind.Comparison;
        ColumnIndex = columnIndex;
        Op = op;
        Literal = literal;
        Accessor = BoundColumnAccessHelper.TryCreate(serializer, columnIndex);
        TextBytes = literal.Type == DbType.Text &&
            (op == BinaryOp.Equals || op == BinaryOp.NotEquals)
            ? Encoding.UTF8.GetBytes(literal.AsText)
            : null;
        IntegerSet = null;
        NumericSet = null;
        TextSet = null;
        TextSetBytes = null;
    }

    public PreDecodeFilterSpec(IRecordSerializer serializer, int columnIndex, bool isNotNull)
    {
        Kind = isNotNull ? PreDecodeFilterKind.IsNotNull : PreDecodeFilterKind.IsNull;
        ColumnIndex = columnIndex;
        Op = BinaryOp.Equals;
        Literal = DbValue.Null;
        Accessor = BoundColumnAccessHelper.TryCreate(serializer, columnIndex);
        TextBytes = null;
        IntegerSet = null;
        NumericSet = null;
        TextSet = null;
        TextSetBytes = null;
    }

    public PreDecodeFilterSpec(IRecordSerializer serializer, int columnIndex, BatchPushdownFilter filter)
    {
        ColumnIndex = columnIndex;
        Accessor = BoundColumnAccessHelper.TryCreate(serializer, columnIndex);
        Op = filter.Op;
        Literal = filter.Literal;
        IntegerSet = filter.IntegerSet;
        NumericSet = filter.NumericSet;
        TextSet = filter.TextSet;

        switch (filter.Kind)
        {
            case BatchPushdownFilterKind.IntegerIn:
                Kind = PreDecodeFilterKind.IntegerIn;
                TextBytes = null;
                TextSetBytes = null;
                break;

            case BatchPushdownFilterKind.NumericIn:
                Kind = PreDecodeFilterKind.NumericIn;
                TextBytes = null;
                TextSetBytes = null;
                break;

            case BatchPushdownFilterKind.TextIn:
                Kind = PreDecodeFilterKind.TextIn;
                TextBytes = null;
                TextSetBytes = filter.TextSet is { Length: > 0 }
                    ? filter.TextSet.Select(static value => Encoding.UTF8.GetBytes(value)).ToArray()
                    : null;
                break;

            default:
                Kind = PreDecodeFilterKind.Comparison;
                TextBytes = filter.Literal.Type == DbType.Text &&
                    (filter.Op == BinaryOp.Equals || filter.Op == BinaryOp.NotEquals)
                    ? Encoding.UTF8.GetBytes(filter.Literal.AsText)
                    : null;
                TextSetBytes = null;
                break;
        }
    }
}

internal static class BoundColumnAccessHelper
{
    public static int[] NormalizeColumnIndices(ReadOnlySpan<int> columnIndices, int columnCount, out int maxColumnIndex)
    {
        if (columnIndices.Length == 0)
        {
            maxColumnIndex = -1;
            return Array.Empty<int>();
        }

        var sorted = columnIndices.ToArray();
        Array.Sort(sorted);

        int uniqueCount = 0;
        int last = int.MinValue;
        for (int i = 0; i < sorted.Length; i++)
        {
            int current = sorted[i];
            if (current < 0 || current >= columnCount)
                continue;
            if (uniqueCount > 0 && current == last)
                continue;

            sorted[uniqueCount++] = current;
            last = current;
        }

        if (uniqueCount == 0)
        {
            maxColumnIndex = -1;
            return Array.Empty<int>();
        }

        if (uniqueCount != sorted.Length)
            Array.Resize(ref sorted, uniqueCount);

        maxColumnIndex = sorted[uniqueCount - 1];
        return sorted;
    }

    public static RecordColumnAccessor? TryCreate(IRecordSerializer serializer, int columnIndex)
        => serializer is DefaultRecordSerializer ? new RecordColumnAccessor(columnIndex) : null;

    public static RecordColumnAccessor?[] CreateAccessors(IRecordSerializer serializer, ReadOnlySpan<int> columnIndices)
    {
        var accessors = new RecordColumnAccessor?[columnIndices.Length];
        for (int i = 0; i < columnIndices.Length; i++)
            accessors[i] = TryCreate(serializer, columnIndices[i]);
        return accessors;
    }

    public static byte[][]? CreateTextLiteralBytes(ReadOnlySpan<DbValue> keyComponents)
    {
        byte[][]? textBytes = null;
        for (int i = 0; i < keyComponents.Length; i++)
        {
            if (keyComponents[i].Type != DbType.Text)
                continue;

            textBytes ??= new byte[keyComponents.Length][];
            textBytes[i] = Encoding.UTF8.GetBytes(keyComponents[i].AsText);
        }

        return textBytes;
    }

    public static bool MatchesKeyComponents(
        ReadOnlySpan<byte> payload,
        IRecordSerializer serializer,
        ReadOnlySpan<int> columnIndices,
        ReadOnlySpan<DbValue> keyComponents,
        RecordColumnAccessor?[]? accessors,
        byte[][]? textBytes,
        string?[]? keyCollations = null)
    {
        if (columnIndices.Length != keyComponents.Length)
            return false;

        for (int i = 0; i < columnIndices.Length; i++)
        {
            DbValue expected = keyComponents[i];
            string? collation = keyCollations is { Length: > 0 } ? keyCollations[i] : null;
            byte[]? expectedTextBytes = textBytes is { Length: > 0 } ? textBytes[i] : null;
            if (expectedTextBytes != null && CollationSupport.IsBinaryOrDefault(collation))
            {
                bool supported;
                bool equals;
                if (accessors is { Length: > 0 } && accessors[i] is { } boundAccessor)
                    supported = boundAccessor.TryTextEquals(payload, expectedTextBytes, out equals);
                else
                    supported = serializer.TryColumnTextEquals(payload, columnIndices[i], expectedTextBytes, out equals);

                if (supported)
                {
                    if (!equals)
                        return false;

                    continue;
                }
            }

            DbValue actual = accessors is { Length: > 0 } && accessors[i] is { } decodeAccessor
                ? decodeAccessor.Decode(payload)
                : serializer.DecodeColumn(payload, columnIndices[i]);
            if (actual.IsNull || DbValue.Compare(CollationSupport.NormalizeIndexValue(actual, collation), expected) != 0)
                return false;
        }

        return true;
    }

    public static bool EvaluateValueFilter(DbValue value, BinaryOp op, DbValue literal)
    {
        int cmp = DbValue.Compare(value, literal);
        return op switch
        {
            BinaryOp.Equals => cmp == 0,
            BinaryOp.NotEquals => cmp != 0,
            BinaryOp.LessThan => cmp < 0,
            BinaryOp.GreaterThan => cmp > 0,
            BinaryOp.LessOrEqual => cmp <= 0,
            BinaryOp.GreaterOrEqual => cmp >= 0,
            _ => true,
        };
    }

    public static bool EvaluatePreDecodeFilter(
        ReadOnlySpan<byte> payload,
        IRecordSerializer serializer,
        RecordColumnAccessor? accessor,
        int columnIndex,
        byte[]? textBytes,
        BinaryOp op,
        DbValue literal)
    {
        if (textBytes != null)
        {
            bool supported;
            bool textEquals;
            if (accessor is { } boundAccessor)
                supported = boundAccessor.TryTextEquals(payload, textBytes, out textEquals);
            else
                supported = serializer.TryColumnTextEquals(payload, columnIndex, textBytes, out textEquals);

            if (supported)
                return op == BinaryOp.Equals ? textEquals : !textEquals;
        }

        DbValue filterValue = accessor is { } decodeAccessor
            ? decodeAccessor.Decode(payload)
            : serializer.DecodeColumn(payload, columnIndex);
        return EvaluateValueFilter(filterValue, op, literal);
    }

    public static bool EvaluatePreDecodeFilters(
        ReadOnlySpan<byte> payload,
        IRecordSerializer serializer,
        ReadOnlySpan<PreDecodeFilterSpec> filters)
    {
        for (int i = 0; i < filters.Length; i++)
        {
            if (!EvaluatePreDecodeFilter(payload, serializer, filters[i]))
            {
                return false;
            }
        }

        return true;
    }

    public static bool EvaluatePreDecodeFilter(
        ReadOnlySpan<byte> payload,
        IRecordSerializer serializer,
        in PreDecodeFilterSpec filter)
    {
        return filter.Kind switch
        {
            PreDecodeFilterKind.IsNull => BoundColumnAccessHelper.IsNull(payload, serializer, filter.Accessor, filter.ColumnIndex),
            PreDecodeFilterKind.IsNotNull => !BoundColumnAccessHelper.IsNull(payload, serializer, filter.Accessor, filter.ColumnIndex),
            PreDecodeFilterKind.IntegerIn => EvaluateIntegerInFilter(payload, serializer, filter),
            PreDecodeFilterKind.NumericIn => EvaluateNumericInFilter(payload, serializer, filter),
            PreDecodeFilterKind.TextIn => EvaluateTextInFilter(payload, serializer, filter),
            _ => EvaluatePreDecodeFilter(
                payload,
                serializer,
                filter.Accessor,
                filter.ColumnIndex,
                filter.TextBytes,
                filter.Op,
                filter.Literal),
        };
    }

    private static bool EvaluateIntegerInFilter(
        ReadOnlySpan<byte> payload,
        IRecordSerializer serializer,
        in PreDecodeFilterSpec filter)
    {
        if (filter.IntegerSet is not { Length: > 0 } integerSet ||
            !TryDecodeNumeric(payload, serializer, filter.Accessor, filter.ColumnIndex, out long intValue, out _, out bool isReal) ||
            isReal)
        {
            return false;
        }

        for (int i = 0; i < integerSet.Length; i++)
        {
            if (intValue == integerSet[i])
                return true;
        }

        return false;
    }

    private static bool EvaluateNumericInFilter(
        ReadOnlySpan<byte> payload,
        IRecordSerializer serializer,
        in PreDecodeFilterSpec filter)
    {
        if (filter.NumericSet is not { Length: > 0 } numericSet ||
            !TryDecodeNumeric(payload, serializer, filter.Accessor, filter.ColumnIndex, out long intValue, out double realValue, out bool isReal))
        {
            return false;
        }

        double actual = isReal ? realValue : intValue;
        for (int i = 0; i < numericSet.Length; i++)
        {
            if (actual == numericSet[i])
                return true;
        }

        return false;
    }

    private static bool EvaluateTextInFilter(
        ReadOnlySpan<byte> payload,
        IRecordSerializer serializer,
        in PreDecodeFilterSpec filter)
    {
        if (filter.TextSet is not { Length: > 0 } textSet)
            return false;

        if (filter.TextSetBytes is { Length: > 0 } textSetBytes)
        {
            bool usedFastPath = true;
            for (int i = 0; i < textSetBytes.Length; i++)
            {
                bool supported;
                bool equals;
                if (filter.Accessor is { } accessor)
                    supported = accessor.TryTextEquals(payload, textSetBytes[i], out equals);
                else
                    supported = serializer.TryColumnTextEquals(payload, filter.ColumnIndex, textSetBytes[i], out equals);

                if (!supported)
                {
                    usedFastPath = false;
                    break;
                }

                if (equals)
                    return true;
            }

            if (usedFastPath)
                return false;
        }

        DbValue actualValue = Decode(payload, serializer, filter.Accessor, filter.ColumnIndex);
        if (actualValue.IsNull || actualValue.Type != DbType.Text)
            return false;

        string actual = actualValue.AsText;
        for (int i = 0; i < textSet.Length; i++)
        {
            if (string.Equals(actual, textSet[i], StringComparison.Ordinal))
                return true;
        }

        return false;
    }

    public static bool IsNull(
        ReadOnlySpan<byte> payload,
        IRecordSerializer serializer,
        RecordColumnAccessor? accessor,
        int columnIndex)
        => accessor is { } boundAccessor
            ? boundAccessor.IsNull(payload)
            : serializer.IsColumnNull(payload, columnIndex);

    public static bool TryDecodeNumeric(
        ReadOnlySpan<byte> payload,
        IRecordSerializer serializer,
        RecordColumnAccessor? accessor,
        int columnIndex,
        out long intValue,
        out double realValue,
        out bool isReal)
    {
        try
        {
            return accessor is { } boundAccessor
                ? boundAccessor.TryDecodeNumeric(payload, out intValue, out realValue, out isReal)
                : serializer.TryDecodeNumericColumn(payload, columnIndex, out intValue, out realValue, out isReal);
        }
        catch (InvalidOperationException)
        {
            intValue = 0;
            realValue = 0;
            isReal = false;
            return false;
        }
        catch (CSharpDbException ex) when (ex.Code == ErrorCode.TypeMismatch)
        {
            intValue = 0;
            realValue = 0;
            isReal = false;
            return false;
        }
    }

    public static DbValue Decode(
        ReadOnlySpan<byte> payload,
        IRecordSerializer serializer,
        RecordColumnAccessor? accessor,
        int columnIndex)
        => accessor is { } boundAccessor
            ? boundAccessor.Decode(payload)
            : serializer.DecodeColumn(payload, columnIndex);
}

/// <summary>
/// Full table scan operator — reads all rows from a B+tree via cursor.
/// </summary>
public sealed class TableScanOperator : IOperator, IBatchOperator, IRowBufferReuseController, IBatchBufferReuseController, IPreDecodeFilterSupport, IEstimatedRowCountProvider, IEncodedPayloadSource
{
    private const int DefaultBatchSize = 64;

    private readonly BTree _tree;
    private readonly TableSchema _schema;
    private readonly IRecordSerializer _recordSerializer;
    private readonly int? _estimatedRowCount;
    private BTreeCursor? _cursor;
    private ReadOnlyMemory<byte> _currentPayload;
    private DbValue[]? _rowBuffer;
    private bool _reuseCurrentRowBuffer = true;
    private bool _reuseCurrentBatch = true;
    private int? _maxDecodedColumnIndex;
    private int[]? _decodedColumnIndices;
    private PreDecodeFilterSpec _preDecodeFilter;
    private bool _hasPreDecodeFilter;
    private PreDecodeFilterSpec[]? _additionalPreDecodeFilters;
    private RowBatch _currentBatch;

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => _reuseCurrentRowBuffer;
    public int? EstimatedRowCount => _estimatedRowCount;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
    public long CurrentRowId { get; private set; }
    public ReadOnlyMemory<byte> CurrentPayload => _currentPayload;
    internal int? DecodedColumnUpperBound => _maxDecodedColumnIndex;

    public TableScanOperator(
        BTree tree,
        TableSchema schema,
        IRecordSerializer? recordSerializer = null,
        int? estimatedRowCount = null)
    {
        _tree = tree;
        _schema = schema;
        _recordSerializer = recordSerializer ?? new DefaultRecordSerializer();
        _estimatedRowCount = estimatedRowCount > 0 ? estimatedRowCount : null;
        OutputSchema = schema.Columns as ColumnDefinition[] ?? schema.Columns.ToArray();
        _currentBatch = CreateBatch(GetTargetColumnCount());
    }

    /// <summary>
    /// Hint the scan to decode only columns up to this index.
    /// Used by scalar aggregate paths to avoid decoding unused trailing columns.
    /// </summary>
    public void SetDecodedColumnUpperBound(int maxColumnIndex)
    {
        _decodedColumnIndices = null;
        _maxDecodedColumnIndex = maxColumnIndex;
    }

    /// <summary>
    /// Hint the scan to decode only specific column ordinals.
    /// The provided indices must reference the source row schema.
    /// </summary>
    public void SetDecodedColumnIndices(ReadOnlySpan<int> columnIndices)
    {
        if (columnIndices.Length == 0)
        {
            _decodedColumnIndices = Array.Empty<int>();
            _maxDecodedColumnIndex = -1;
            return;
        }

        var sorted = columnIndices.ToArray();
        Array.Sort(sorted);

        int uniqueCount = 0;
        int last = int.MinValue;
        for (int i = 0; i < sorted.Length; i++)
        {
            int current = sorted[i];
            if (current < 0 || current >= _schema.Columns.Count)
                continue;
            if (uniqueCount > 0 && current == last)
                continue;

            sorted[uniqueCount++] = current;
            last = current;
        }

        if (uniqueCount == 0)
        {
            _decodedColumnIndices = Array.Empty<int>();
            _maxDecodedColumnIndex = -1;
            return;
        }

        if (uniqueCount != sorted.Length)
            Array.Resize(ref sorted, uniqueCount);

        _decodedColumnIndices = sorted;
        _maxDecodedColumnIndex = sorted[uniqueCount - 1];
    }

    /// <summary>
    /// Hint the scan to evaluate a simple predicate from encoded payload first.
    /// Rows that fail this predicate are skipped before full row decode.
    /// </summary>
    public void SetPreDecodeFilter(int columnIndex, BinaryOp op, DbValue literal)
        => ((IPreDecodeFilterSupport)this).SetPreDecodeFilter(new PreDecodeFilterSpec(_recordSerializer, columnIndex, op, literal));

    void IPreDecodeFilterSupport.SetPreDecodeFilter(in PreDecodeFilterSpec filter)
    {
        if (_hasPreDecodeFilter)
        {
            AppendAdditionalPreDecodeFilter(filter);
            return;
        }

        _preDecodeFilter = filter;
        _hasPreDecodeFilter = true;
    }

    public ValueTask OpenAsync(CancellationToken ct = default)
    {
        _cursor = _tree.CreateCursor();
        _currentPayload = ReadOnlyMemory<byte>.Empty;
        _rowBuffer = null;
        _currentBatch = CreateBatch(GetTargetColumnCount());
        Current = Array.Empty<DbValue>();
        return ValueTask.CompletedTask;
    }

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (_cursor == null) return false;

        while (await _cursor.MoveNextAsync(ct))
        {
            _currentPayload = _cursor.CurrentValue;
            var payload = _currentPayload.Span;
            if (_hasPreDecodeFilter)
            {
                if (!EvaluatePreDecodeFilter(payload))
                    continue;
            }

            CurrentRowId = _cursor.CurrentKey;
            int targetColumnCount = _maxDecodedColumnIndex.HasValue
                ? Math.Max(0, _maxDecodedColumnIndex.Value + 1)
                : _schema.Columns.Count;
            var decodedColumnIndices = _decodedColumnIndices;

            if (decodedColumnIndices is { Length: > 0 })
            {
                if (_reuseCurrentRowBuffer)
                {
                    EnsureRowBuffer(targetColumnCount);
                    if (targetColumnCount > 0)
                        Array.Fill(_rowBuffer!, DbValue.Null, 0, targetColumnCount);
                    _recordSerializer.DecodeSelectedInto(payload, _rowBuffer!, decodedColumnIndices);
                    Current = _rowBuffer!;
                }
                else
                {
                    var row = targetColumnCount == 0 ? Array.Empty<DbValue>() : new DbValue[targetColumnCount];
                    if (targetColumnCount > 0)
                    {
                        Array.Fill(row, DbValue.Null);
                        _recordSerializer.DecodeSelectedInto(payload, row, decodedColumnIndices);
                    }

                    Current = row;
                }
                return true;
            }

            if (_reuseCurrentRowBuffer)
            {
                EnsureRowBuffer(targetColumnCount);
                int decodedCount = _recordSerializer.DecodeInto(payload, _rowBuffer!);
                if (decodedCount < targetColumnCount)
                    Array.Fill(_rowBuffer!, DbValue.Null, decodedCount, targetColumnCount - decodedCount);

                Current = _rowBuffer!;
            }
            else
            {
                var row = targetColumnCount == 0 ? Array.Empty<DbValue>() : new DbValue[targetColumnCount];
                int decodedCount = _recordSerializer.DecodeInto(payload, row);
                if (decodedCount < targetColumnCount)
                    Array.Fill(row, DbValue.Null, decodedCount, targetColumnCount - decodedCount);

                Current = row;
            }
            return true;
        }

        _currentPayload = ReadOnlyMemory<byte>.Empty;
        return false;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public void SetReuseCurrentRowBuffer(bool reuse)
    {
        _reuseCurrentRowBuffer = reuse;
        if (!reuse)
        {
            _rowBuffer = null;
            Current = Array.Empty<DbValue>();
        }
    }

    public async ValueTask<bool> MoveNextBatchAsync(CancellationToken ct = default)
    {
        if (_cursor == null)
            return false;

        int targetColumnCount = GetTargetColumnCount();
        var batch = _reuseCurrentBatch ? EnsureBatch(targetColumnCount) : CreateBatch(targetColumnCount);
        batch.Reset();

        while (batch.Count < batch.Capacity && await _cursor.MoveNextAsync(ct))
        {
            _currentPayload = _cursor.CurrentValue;
            var payload = _currentPayload.Span;
            if (_hasPreDecodeFilter && !EvaluatePreDecodeFilter(payload))
                continue;

            CurrentRowId = _cursor.CurrentKey;
            int rowIndex = batch.Count;
            DecodePayloadInto(payload, targetColumnCount, batch.GetWritableRowSpan(rowIndex));
            batch.CommitWrittenRow(rowIndex);
        }

        _currentBatch = batch;
        if (batch.Count == 0)
            _currentPayload = ReadOnlyMemory<byte>.Empty;

        return batch.Count > 0;
    }

    bool IBatchOperator.ReusesCurrentBatch => _reuseCurrentBatch;
    RowBatch IBatchOperator.CurrentBatch => _currentBatch;

    public void SetReuseCurrentBatch(bool reuse)
    {
        _reuseCurrentBatch = reuse;
        if (!reuse)
            _currentBatch = CreateBatch(GetTargetColumnCount());
    }

    private void EnsureRowBuffer(int columnCount)
    {
        if (_rowBuffer == null || _rowBuffer.Length != columnCount)
            _rowBuffer = columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];
    }

    private int GetTargetColumnCount()
        => _maxDecodedColumnIndex.HasValue
            ? Math.Max(0, _maxDecodedColumnIndex.Value + 1)
            : _schema.Columns.Count;

    private RowBatch EnsureBatch(int columnCount)
    {
        if (_currentBatch.ColumnCount != columnCount)
            _currentBatch = CreateBatch(columnCount);

        return _currentBatch;
    }

    private static RowBatch CreateBatch(int columnCount) => new(columnCount, DefaultBatchSize);

    private void DecodePayloadInto(ReadOnlySpan<byte> payload, int targetColumnCount, Span<DbValue> destination)
    {
        var decodedColumnIndices = _decodedColumnIndices;
        if (decodedColumnIndices is { Length: > 0 })
        {
            if (targetColumnCount > 0)
                destination[..targetColumnCount].Fill(DbValue.Null);

            _recordSerializer.DecodeSelectedInto(payload, destination, decodedColumnIndices);
            return;
        }

        int decodedCount = _recordSerializer.DecodeInto(payload, destination);
        if (decodedCount < targetColumnCount)
            destination[decodedCount..targetColumnCount].Fill(DbValue.Null);
    }

    private bool EvaluatePreDecodeFilter(ReadOnlySpan<byte> payload)
    {
        if (!BoundColumnAccessHelper.EvaluatePreDecodeFilter(payload, _recordSerializer, _preDecodeFilter))
        {
            return false;
        }

        return _additionalPreDecodeFilters is not { Length: > 0 }
            || BoundColumnAccessHelper.EvaluatePreDecodeFilters(payload, _recordSerializer, _additionalPreDecodeFilters);
    }

    private void AppendAdditionalPreDecodeFilter(in PreDecodeFilterSpec filter)
    {
        if (_additionalPreDecodeFilters == null)
        {
            _additionalPreDecodeFilters = [filter];
            return;
        }

        int length = _additionalPreDecodeFilters.Length;
        Array.Resize(ref _additionalPreDecodeFilters, length + 1);
        _additionalPreDecodeFilters[length] = filter;
    }

    internal DbValue[] DecodeFullRow(ReadOnlySpan<byte> payload)
    {
        var decoded = _recordSerializer.Decode(payload);
        if (decoded.Length >= _schema.Columns.Count)
            return decoded;

        var padded = new DbValue[_schema.Columns.Count];
        decoded.CopyTo(padded, 0);
        for (int i = decoded.Length; i < padded.Length; i++)
            padded[i] = DbValue.Null;
        return padded;
    }

    internal async ValueTask<DbValue[]?> DecodeFullRowByRowIdAsync(long rowId, CancellationToken ct = default)
    {
        ReadOnlyMemory<byte>? payload;
        if (_tree.TryFindCachedMemory(rowId, out var cachedPayload))
        {
            payload = cachedPayload;
        }
        else
        {
            payload = await _tree.FindMemoryAsync(rowId, ct);
        }

        return payload is { } payloadMemory ? DecodeFullRow(payloadMemory.Span) : null;
    }
}

/// <summary>
/// Filter operator — applies a WHERE predicate.
/// </summary>
public sealed class FilterOperator : IOperator, IBatchOperator, IRowBufferReuseController, IBatchBufferReuseController, IUnaryOperatorSource
{
    private const int DefaultBatchSize = 64;

    private readonly IOperator _source;
    private readonly Func<DbValue[], DbValue> _predicateEvaluator;
    private readonly SpanExpressionEvaluator? _spanPredicateEvaluator;
    private readonly IFilterProjectionBatchPlan? _batchPlan;
    private bool _reuseCurrentBatch = true;
    private RowBatch _currentBatch;
    private DbValue[]? _predicateRowBuffer;
    private RowSelection? _batchSelection;
    private int _sourceBatchRowIndex = -1;

    public ColumnDefinition[] OutputSchema => _source.OutputSchema;
    public bool ReusesCurrentRowBuffer => _source.ReusesCurrentRowBuffer;
    public DbValue[] Current => _source.Current;
    IOperator IUnaryOperatorSource.Source => _source;

    public FilterOperator(IOperator source, Expression predicate, TableSchema schema)
        : this(source, ExpressionCompiler.CompileSpan(predicate, schema))
    {
    }

    public FilterOperator(IOperator source, Func<DbValue[], DbValue> predicateEvaluator)
    {
        _source = source;
        _predicateEvaluator = predicateEvaluator;
        _batchPlan = null;
        _currentBatch = CreateBatch(source.OutputSchema.Length);
    }

    internal FilterOperator(IOperator source, SpanExpressionEvaluator predicateEvaluator)
        : this(source, predicateEvaluator, batchPlan: null)
    {
    }

    internal FilterOperator(IOperator source, SpanExpressionEvaluator predicateEvaluator, IFilterProjectionBatchPlan? batchPlan)
    {
        _source = source;
        _predicateEvaluator = row => predicateEvaluator(row);
        _spanPredicateEvaluator = predicateEvaluator;
        _batchPlan = batchPlan;
        _currentBatch = CreateBatch(source.OutputSchema.Length);
    }

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        _currentBatch = CreateBatch(_source.OutputSchema.Length);
        _predicateRowBuffer = null;
        _batchSelection = null;
        _sourceBatchRowIndex = -1;
        await _source.OpenAsync(ct);
    }

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        while (await _source.MoveNextAsync(ct))
        {
            if (EvaluatePredicate(_source.Current))
                return true;
        }
        return false;
    }

    public ValueTask DisposeAsync() => _source.DisposeAsync();

    public void SetReuseCurrentRowBuffer(bool reuse)
    {
        if (_source is IRowBufferReuseController controller)
            controller.SetReuseCurrentRowBuffer(reuse);
    }

    public async ValueTask<bool> MoveNextBatchAsync(CancellationToken ct = default)
    {
        var batchSource = BatchSourceHelper.TryGetBatchSource(_source);
        if (batchSource != null && _batchPlan != null)
            return await MoveNextBatchFromBatchSourcePlanAsync(batchSource, ct);

        if (batchSource != null)
            return await MoveNextBatchFromBatchSourceAsync(batchSource, ct);

        var batch = _reuseCurrentBatch ? EnsureBatch(_source.OutputSchema.Length) : CreateBatch(_source.OutputSchema.Length);
        batch.Reset();

        while (batch.Count < batch.Capacity && await _source.MoveNextAsync(ct))
        {
            if (!EvaluatePredicate(_source.Current))
                continue;

            batch.AppendRow(_source.Current);
        }

        _currentBatch = batch;
        return batch.Count > 0;
    }

    private async ValueTask<bool> MoveNextBatchFromBatchSourcePlanAsync(IBatchOperator batchSource, CancellationToken ct)
    {
        int columnCount = _batchPlan!.OutputColumnCount;

        while (await batchSource.MoveNextBatchAsync(ct))
        {
            RowBatch sourceBatch = batchSource.CurrentBatch;
            var batch = _reuseCurrentBatch
                ? EnsureBatch(columnCount, Math.Max(DefaultBatchSize, sourceBatch.Count))
                : CreateBatch(columnCount, Math.Max(DefaultBatchSize, sourceBatch.Count));
            var selection = EnsureBatchSelection(sourceBatch.Count);

            if (_batchPlan.Execute(sourceBatch, selection, batch) > 0)
            {
                _currentBatch = batch;
                return true;
            }
        }

        var emptyBatch = _reuseCurrentBatch ? EnsureBatch(columnCount) : CreateBatch(columnCount);
        emptyBatch.Reset();
        _currentBatch = emptyBatch;
        return false;
    }

    bool IBatchOperator.ReusesCurrentBatch => _reuseCurrentBatch;
    RowBatch IBatchOperator.CurrentBatch => _currentBatch;

    public void SetReuseCurrentBatch(bool reuse)
    {
        _reuseCurrentBatch = reuse;
        if (_source is IBatchBufferReuseController controller)
            controller.SetReuseCurrentBatch(reuse);

        if (!reuse)
            _currentBatch = CreateBatch(_source.OutputSchema.Length);
    }

    private async ValueTask<bool> MoveNextBatchFromBatchSourceAsync(IBatchOperator batchSource, CancellationToken ct)
    {
        RowBatch sourceBatch = batchSource.CurrentBatch;
        int columnCount = sourceBatch.ColumnCount;
        var batch = _reuseCurrentBatch ? EnsureBatch(columnCount) : CreateBatch(columnCount);
        batch.Reset();

        while (batch.Count < batch.Capacity)
        {
            while (_sourceBatchRowIndex + 1 < sourceBatch.Count && batch.Count < batch.Capacity)
            {
                _sourceBatchRowIndex++;
                if (!EvaluatePredicate(sourceBatch.GetRowSpan(_sourceBatchRowIndex)))
                    continue;

                int targetRowIndex = batch.Count;
                sourceBatch.GetRowSpan(_sourceBatchRowIndex).CopyTo(batch.GetWritableRowSpan(targetRowIndex));
                batch.CommitWrittenRow(targetRowIndex);
            }

            if (batch.Count >= batch.Capacity)
                break;

            if (!await batchSource.MoveNextBatchAsync(ct))
                break;

            sourceBatch = batchSource.CurrentBatch;
            columnCount = sourceBatch.ColumnCount;
            if (batch.ColumnCount != columnCount)
            {
                batch = _reuseCurrentBatch ? EnsureBatch(columnCount) : CreateBatch(columnCount);
                batch.Reset();
            }

            _sourceBatchRowIndex = -1;
        }

        _currentBatch = batch;
        return batch.Count > 0;
    }

    private bool EvaluatePredicate(DbValue[] row)
    {
        if (_spanPredicateEvaluator != null)
            return _spanPredicateEvaluator(row).IsTruthy;

        return _predicateEvaluator(row).IsTruthy;
    }

    private bool EvaluatePredicate(ReadOnlySpan<DbValue> row)
    {
        if (_spanPredicateEvaluator != null)
            return _spanPredicateEvaluator(row).IsTruthy;

        int columnCount = row.Length;
        if (_predicateRowBuffer == null || _predicateRowBuffer.Length != columnCount)
            _predicateRowBuffer = columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];

        row.CopyTo(_predicateRowBuffer);
        return _predicateEvaluator(_predicateRowBuffer).IsTruthy;
    }

    private RowBatch EnsureBatch(int columnCount, int capacity = DefaultBatchSize)
    {
        if (_currentBatch.ColumnCount != columnCount || _currentBatch.Capacity != capacity)
            _currentBatch = CreateBatch(columnCount, capacity);

        return _currentBatch;
    }

    private RowSelection EnsureBatchSelection(int capacity)
    {
        _batchSelection ??= new RowSelection(capacity);
        _batchSelection.EnsureCapacity(capacity);
        return _batchSelection;
    }

    private static RowBatch CreateBatch(int columnCount, int capacity = DefaultBatchSize) => new(columnCount, capacity);
}

/// <summary>
/// Projection operator — selects and reorders columns.
/// </summary>
public sealed class ProjectionOperator : IOperator, IBatchOperator, IRowBufferReuseController, IBatchBufferReuseController, IUnaryOperatorSource
{
    private const int DefaultBatchSize = 64;

    private readonly IOperator _source;
    private readonly int[] _columnIndices;
    private readonly Func<DbValue[], DbValue>[]? _expressionEvaluators;
    private readonly SpanExpressionEvaluator[]? _spanExpressionEvaluators;
    private readonly IFilterProjectionBatchPlan? _batchPlan;
    private bool _reuseCurrentRowBuffer = true;
    private bool _reuseCurrentBatch = true;
    private DbValue[]? _rowBuffer;
    private DbValue[][]? _batchRows;
    private DbValue[]? _batchSourceRowBuffer;
    private RowSelection? _batchSelection;
    private RowBatch? _pendingSourceBatch;
    private RowBatch _currentBatch = new(0, DefaultBatchSize);
    private int _pendingSourceBatchRowIndex;
    private int _batchIndex;
    private int _batchCount;

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => _reuseCurrentRowBuffer;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
    IOperator IUnaryOperatorSource.Source => _source;

    public ProjectionOperator(IOperator source, int[] columnIndices, ColumnDefinition[] outputSchema, TableSchema schema, Expression[]? expressions = null)
    {
        _source = source;
        _columnIndices = columnIndices;
        if (expressions != null)
        {
            _spanExpressionEvaluators = new SpanExpressionEvaluator[expressions.Length];
            for (int i = 0; i < expressions.Length; i++)
                _spanExpressionEvaluators[i] = ExpressionCompiler.CompileSpan(expressions[i], schema);
        }
        OutputSchema = outputSchema;
    }

    public ProjectionOperator(
        IOperator source,
        int[] columnIndices,
        ColumnDefinition[] outputSchema,
        Func<DbValue[], DbValue>[] expressionEvaluators)
        : this(source, columnIndices, outputSchema, expressionEvaluators, null)
    {
    }

    internal ProjectionOperator(
        IOperator source,
        int[] columnIndices,
        ColumnDefinition[] outputSchema,
        Func<DbValue[], DbValue>[] expressionEvaluators,
        IFilterProjectionBatchPlan? batchPlan)
    {
        _source = source;
        _columnIndices = columnIndices;
        _expressionEvaluators = expressionEvaluators;
        _batchPlan = batchPlan;
        OutputSchema = outputSchema;
    }

    internal ProjectionOperator(
        IOperator source,
        int[] columnIndices,
        ColumnDefinition[] outputSchema,
        SpanExpressionEvaluator[] expressionEvaluators,
        IFilterProjectionBatchPlan? batchPlan,
        bool useSpanEvaluators)
    {
        _source = source;
        _columnIndices = columnIndices;
        _spanExpressionEvaluators = expressionEvaluators;
        _batchPlan = batchPlan;
        OutputSchema = outputSchema;
    }

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        _rowBuffer = null;
        _batchRows = null;
        _batchSourceRowBuffer = null;
        _batchSelection = null;
        _pendingSourceBatch = null;
        _pendingSourceBatchRowIndex = 0;
        _batchIndex = 0;
        _batchCount = 0;
        Current = Array.Empty<DbValue>();
        await _source.OpenAsync(ct);
    }

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (HasExpressionEvaluators())
        {
            if (_batchIndex < _batchCount)
            {
                Current = _batchRows![_batchIndex++];
                return true;
            }

            if (!await FillExpressionBatchAsync(ct))
            {
                Current = Array.Empty<DbValue>();
                return false;
            }

            Current = _batchRows![_batchIndex++];
            return true;
        }

        if (!await _source.MoveNextAsync(ct))
        {
            Current = Array.Empty<DbValue>();
            return false;
        }

        int valueCount = _columnIndices.Length;
        if (CanPassThroughSourceRow(valueCount))
        {
            Current = _source.Current;
            return true;
        }

        var target = _reuseCurrentRowBuffer
            ? EnsureRowBuffer(valueCount)
            : valueCount == 0 ? Array.Empty<DbValue>() : new DbValue[valueCount];

        for (int i = 0; i < valueCount; i++)
            target[i] = _source.Current[_columnIndices[i]];

        Current = target;
        return true;
    }

    public ValueTask DisposeAsync() => _source.DisposeAsync();

    public async ValueTask<bool> MoveNextBatchAsync(CancellationToken ct = default)
    {
        var batchSource = BatchSourceHelper.TryGetBatchSource(_source);
        if (batchSource != null && _batchPlan != null)
            return await MoveNextBatchFromBatchSourcePlanAsync(batchSource, ct);

        return batchSource != null
            ? await MoveNextBatchFromBatchSourceAsync(batchSource, ct)
            : await MoveNextBatchFromRowSourceAsync(ct);
    }

    bool IBatchOperator.ReusesCurrentBatch => _reuseCurrentBatch;
    RowBatch IBatchOperator.CurrentBatch => _currentBatch;

    public void SetReuseCurrentRowBuffer(bool reuse)
    {
        _reuseCurrentRowBuffer = reuse;
        if (!reuse)
        {
            _rowBuffer = null;
            _batchRows = null;
            _batchIndex = 0;
            _batchCount = 0;
            Current = Array.Empty<DbValue>();
        }
    }

    public void SetReuseCurrentBatch(bool reuse)
    {
        _reuseCurrentBatch = reuse;
        _pendingSourceBatch = null;
        _pendingSourceBatchRowIndex = 0;
        if (!reuse)
            _currentBatch = CreateBatch(GetOutputColumnCount());
    }

    private async ValueTask<bool> FillExpressionBatchAsync(CancellationToken ct)
    {
        _batchIndex = 0;
        _batchCount = 0;

        while (_batchCount < DefaultBatchSize && await _source.MoveNextAsync(ct))
        {
            DbValue[] row = GetBatchRow(_batchCount, GetExpressionOutputColumnCount() ?? 0);
            WriteProjectedRow(_source.Current, row);

            StoreBatchRow(_batchCount, row);
            _batchCount++;
        }

        return _batchCount > 0;
    }

    private async ValueTask<bool> MoveNextBatchFromRowSourceAsync(CancellationToken ct)
    {
        int columnCount = GetOutputColumnCount();
        var batch = _reuseCurrentBatch ? EnsureBatch(columnCount) : CreateBatch(columnCount);
        batch.Reset();

        while (batch.Count < batch.Capacity && await _source.MoveNextAsync(ct))
        {
            WriteProjectedRow(_source.Current, batch.GetWritableRowSpan(batch.Count));
            batch.CommitWrittenRow(batch.Count);
        }

        _currentBatch = batch;
        return batch.Count > 0;
    }

    private async ValueTask<bool> MoveNextBatchFromBatchSourceAsync(IBatchOperator batchSource, CancellationToken ct)
    {
        int columnCount = GetOutputColumnCount();
        var batch = _reuseCurrentBatch ? EnsureBatch(columnCount) : CreateBatch(columnCount);
        batch.Reset();

        while (batch.Count < batch.Capacity)
        {
            if (_pendingSourceBatch == null || _pendingSourceBatchRowIndex >= _pendingSourceBatch.Count)
            {
                if (!await batchSource.MoveNextBatchAsync(ct))
                    break;

                _pendingSourceBatch = batchSource.CurrentBatch;
                _pendingSourceBatchRowIndex = 0;
            }

            while (batch.Count < batch.Capacity &&
                   _pendingSourceBatch != null &&
                   _pendingSourceBatchRowIndex < _pendingSourceBatch.Count)
            {
                WriteProjectedRow(_pendingSourceBatch, _pendingSourceBatchRowIndex, batch.GetWritableRowSpan(batch.Count));
                _pendingSourceBatchRowIndex++;
                batch.CommitWrittenRow(batch.Count);
            }

            if (_pendingSourceBatch != null && _pendingSourceBatchRowIndex >= _pendingSourceBatch.Count)
            {
                _pendingSourceBatch = null;
                _pendingSourceBatchRowIndex = 0;
            }
        }

        _currentBatch = batch;
        return batch.Count > 0;
    }

    private async ValueTask<bool> MoveNextBatchFromBatchSourcePlanAsync(IBatchOperator batchSource, CancellationToken ct)
    {
        int columnCount = _batchPlan!.OutputColumnCount;

        while (await batchSource.MoveNextBatchAsync(ct))
        {
            RowBatch sourceBatch = batchSource.CurrentBatch;
            var batch = _reuseCurrentBatch
                ? EnsureBatch(columnCount, Math.Max(DefaultBatchSize, sourceBatch.Count))
                : CreateBatch(columnCount, Math.Max(DefaultBatchSize, sourceBatch.Count));
            var selection = EnsureBatchSelection(sourceBatch.Count);

            if (_batchPlan.Execute(sourceBatch, selection, batch) > 0)
            {
                _currentBatch = batch;
                return true;
            }
        }

        var emptyBatch = _reuseCurrentBatch ? EnsureBatch(columnCount) : CreateBatch(columnCount);
        emptyBatch.Reset();
        _currentBatch = emptyBatch;
        return false;
    }

    private DbValue[] EnsureRowBuffer(int columnCount)
    {
        if (_rowBuffer == null || _rowBuffer.Length != columnCount)
            _rowBuffer = columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];

        return _rowBuffer;
    }

    private RowBatch EnsureBatch(int columnCount, int capacity = DefaultBatchSize)
    {
        if (_currentBatch.ColumnCount != columnCount || _currentBatch.Capacity != capacity)
            _currentBatch = CreateBatch(columnCount, capacity);

        return _currentBatch;
    }

    private DbValue[] GetBatchRow(int batchSlot, int columnCount)
    {
        if (!_reuseCurrentRowBuffer)
            return columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];

        _batchRows ??= new DbValue[DefaultBatchSize][];
        DbValue[]? row = _batchRows[batchSlot];
        if (row == null || row.Length != columnCount)
        {
            row = columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];
            _batchRows[batchSlot] = row;
        }

        return row;
    }

    private int GetOutputColumnCount()
        => GetExpressionOutputColumnCount() ?? _columnIndices.Length;

    private int? GetExpressionOutputColumnCount()
        => _spanExpressionEvaluators?.Length ?? _expressionEvaluators?.Length;

    private bool HasExpressionEvaluators()
        => _spanExpressionEvaluators != null || _expressionEvaluators != null;

    private static RowBatch CreateBatch(int columnCount, int capacity = DefaultBatchSize) => new(columnCount, capacity);

    private RowSelection EnsureBatchSelection(int capacity)
    {
        _batchSelection ??= new RowSelection(capacity);
        _batchSelection.EnsureCapacity(capacity);
        return _batchSelection;
    }

    private void WriteProjectedRow(ReadOnlySpan<DbValue> sourceRow, Span<DbValue> destination)
    {
        if (_spanExpressionEvaluators != null)
        {
            WriteExpressionProjectedRow(sourceRow, destination);
            return;
        }

        if (_expressionEvaluators != null)
        {
            var sourceRowBuffer = EnsureBatchSourceRowBuffer(sourceRow.Length);
            sourceRow.CopyTo(sourceRowBuffer);
            WriteExpressionProjectedRow(sourceRowBuffer, destination);
            return;
        }

        for (int i = 0; i < _columnIndices.Length; i++)
            destination[i] = sourceRow[_columnIndices[i]];
    }

    private void WriteProjectedRow(RowBatch sourceBatch, int rowIndex, Span<DbValue> destination)
    {
        if (_spanExpressionEvaluators != null)
        {
            WriteExpressionProjectedRow(sourceBatch.GetRowSpan(rowIndex), destination);
            return;
        }

        if (_expressionEvaluators != null)
        {
            var sourceRowBuffer = EnsureBatchSourceRowBuffer(sourceBatch.ColumnCount);
            sourceBatch.CopyRowTo(rowIndex, sourceRowBuffer);
            WriteExpressionProjectedRow(sourceRowBuffer, destination);
            return;
        }

        var sourceRow = sourceBatch.GetRowSpan(rowIndex);
        for (int i = 0; i < _columnIndices.Length; i++)
            destination[i] = sourceRow[_columnIndices[i]];
    }

    private void WriteExpressionProjectedRow(ReadOnlySpan<DbValue> sourceRow, Span<DbValue> destination)
    {
        if (_spanExpressionEvaluators != null)
        {
            for (int i = 0; i < _spanExpressionEvaluators.Length; i++)
                destination[i] = _spanExpressionEvaluators[i](sourceRow);

            return;
        }

        var sourceRowBuffer = EnsureBatchSourceRowBuffer(sourceRow.Length);
        sourceRow.CopyTo(sourceRowBuffer);
        WriteExpressionProjectedRow(sourceRowBuffer, destination);
    }

    private void WriteExpressionProjectedRow(DbValue[] sourceRow, Span<DbValue> destination)
    {
        for (int i = 0; i < _expressionEvaluators!.Length; i++)
            destination[i] = _expressionEvaluators[i](sourceRow);
    }

    private DbValue[] EnsureBatchSourceRowBuffer(int columnCount)
    {
        if (_batchSourceRowBuffer == null || _batchSourceRowBuffer.Length != columnCount)
            _batchSourceRowBuffer = columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];

        return _batchSourceRowBuffer;
    }

    private void StoreBatchRow(int batchSlot, DbValue[] row)
    {
        _batchRows ??= new DbValue[DefaultBatchSize][];
        _batchRows[batchSlot] = row;
    }

    private bool CanPassThroughSourceRow(int valueCount)
    {
        if (_source.ReusesCurrentRowBuffer)
            return false;

        var sourceRow = _source.Current;
        if (sourceRow.Length != valueCount)
            return false;

        for (int i = 0; i < valueCount; i++)
        {
            if (_columnIndices[i] != i)
                return false;
        }

        return true;
    }
}

/// <summary>
/// Fused filter + projection operator for common scan/lookup paths.
/// Avoids an extra row-by-row iterator layer when a query needs both.
/// </summary>
public sealed class FilterProjectionOperator : IOperator, IBatchOperator, IRowBufferReuseController, IBatchBufferReuseController, IEstimatedRowCountProvider, IUnaryOperatorSource
{
    private const int DefaultBatchSize = 64;

    private readonly IOperator _source;
    private readonly Func<DbValue[], DbValue> _predicateEvaluator;
    private readonly SpanExpressionEvaluator? _spanPredicateEvaluator;
    private readonly int[] _columnIndices;
    private readonly Func<DbValue[], DbValue>[]? _expressionEvaluators;
    private readonly SpanExpressionEvaluator[]? _spanExpressionEvaluators;
    private readonly IFilterProjectionBatchPlan? _batchPlan;
    private bool _reuseCurrentRowBuffer = true;
    private bool _reuseCurrentBatch = true;
    private DbValue[]? _rowBuffer;
    private DbValue[][]? _batchRows;
    private DbValue[]? _batchSourceRowBuffer;
    private RowSelection? _batchSelection;
    private RowBatch? _pendingSourceBatch;
    private RowBatch _currentBatch = new(0, DefaultBatchSize);
    private int _pendingSourceBatchRowIndex;
    private int _batchIndex;
    private int _batchCount;

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => _reuseCurrentRowBuffer;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
    public int? EstimatedRowCount => _source is IEstimatedRowCountProvider estimated ? estimated.EstimatedRowCount : null;
    IOperator IUnaryOperatorSource.Source => _source;

    public FilterProjectionOperator(
        IOperator source,
        Func<DbValue[], DbValue> predicateEvaluator,
        int[] columnIndices,
        ColumnDefinition[] outputSchema)
        : this(source, predicateEvaluator, columnIndices, outputSchema, batchPlan: null)
    {
    }

    internal FilterProjectionOperator(
        IOperator source,
        Func<DbValue[], DbValue> predicateEvaluator,
        int[] columnIndices,
        ColumnDefinition[] outputSchema,
        IFilterProjectionBatchPlan? batchPlan)
    {
        _source = source;
        _predicateEvaluator = predicateEvaluator;
        _columnIndices = columnIndices;
        _batchPlan = batchPlan;
        OutputSchema = outputSchema;
    }

    internal FilterProjectionOperator(
        IOperator source,
        SpanExpressionEvaluator predicateEvaluator,
        int[] columnIndices,
        ColumnDefinition[] outputSchema,
        IFilterProjectionBatchPlan? batchPlan,
        bool useSpanEvaluator)
    {
        _source = source;
        _predicateEvaluator = row => predicateEvaluator(row);
        _spanPredicateEvaluator = predicateEvaluator;
        _columnIndices = columnIndices;
        _batchPlan = batchPlan;
        OutputSchema = outputSchema;
    }

    public FilterProjectionOperator(
        IOperator source,
        Func<DbValue[], DbValue> predicateEvaluator,
        ColumnDefinition[] outputSchema,
        Func<DbValue[], DbValue>[] expressionEvaluators)
        : this(source, predicateEvaluator, outputSchema, expressionEvaluators, null)
    {
    }

    internal FilterProjectionOperator(
        IOperator source,
        Func<DbValue[], DbValue> predicateEvaluator,
        ColumnDefinition[] outputSchema,
        Func<DbValue[], DbValue>[] expressionEvaluators,
        IFilterProjectionBatchPlan? batchPlan)
    {
        _source = source;
        _predicateEvaluator = predicateEvaluator;
        _columnIndices = Array.Empty<int>();
        _expressionEvaluators = expressionEvaluators;
        _batchPlan = batchPlan;
        OutputSchema = outputSchema;
    }

    internal FilterProjectionOperator(
        IOperator source,
        SpanExpressionEvaluator predicateEvaluator,
        ColumnDefinition[] outputSchema,
        SpanExpressionEvaluator[] expressionEvaluators,
        IFilterProjectionBatchPlan? batchPlan,
        bool useSpanEvaluator)
    {
        _source = source;
        _predicateEvaluator = row => predicateEvaluator(row);
        _spanPredicateEvaluator = predicateEvaluator;
        _columnIndices = Array.Empty<int>();
        _spanExpressionEvaluators = expressionEvaluators;
        _batchPlan = batchPlan;
        OutputSchema = outputSchema;
    }

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        _rowBuffer = null;
        _batchRows = null;
        _batchSourceRowBuffer = null;
        _batchSelection = null;
        _pendingSourceBatch = null;
        _pendingSourceBatchRowIndex = 0;
        _batchIndex = 0;
        _batchCount = 0;
        Current = Array.Empty<DbValue>();
        await _source.OpenAsync(ct);
    }

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (HasExpressionEvaluators())
        {
            if (_batchIndex < _batchCount)
            {
                Current = _batchRows![_batchIndex++];
                return true;
            }

            if (!await FillExpressionBatchAsync(ct))
            {
                Current = Array.Empty<DbValue>();
                return false;
            }

            Current = _batchRows![_batchIndex++];
            return true;
        }

        while (await _source.MoveNextAsync(ct))
        {
            if (!_predicateEvaluator(_source.Current).IsTruthy)
                continue;

            int columnCount = _columnIndices.Length;
            if (CanPassThroughSourceRow(columnCount))
            {
                Current = _source.Current;
                return true;
            }

            var projectionTarget = _reuseCurrentRowBuffer
                ? EnsureRowBuffer(columnCount)
                : columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];

            for (int i = 0; i < columnCount; i++)
                projectionTarget[i] = _source.Current[_columnIndices[i]];

            Current = projectionTarget;
            return true;
        }

        Current = Array.Empty<DbValue>();
        return false;
    }

    public ValueTask DisposeAsync() => _source.DisposeAsync();

    public async ValueTask<bool> MoveNextBatchAsync(CancellationToken ct = default)
    {
        var batchSource = BatchSourceHelper.TryGetBatchSource(_source);
        if (batchSource != null && _batchPlan != null)
            return await MoveNextBatchFromBatchSourcePlanAsync(batchSource, ct);

        return batchSource != null
            ? await MoveNextBatchFromBatchSourceAsync(batchSource, ct)
            : await MoveNextBatchFromRowSourceAsync(ct);
    }

    bool IBatchOperator.ReusesCurrentBatch => _reuseCurrentBatch;
    RowBatch IBatchOperator.CurrentBatch => _currentBatch;

    public void SetReuseCurrentRowBuffer(bool reuse)
    {
        _reuseCurrentRowBuffer = reuse;
        if (!reuse)
        {
            _rowBuffer = null;
            _batchRows = null;
            _batchIndex = 0;
            _batchCount = 0;
            Current = Array.Empty<DbValue>();
        }
    }

    public void SetReuseCurrentBatch(bool reuse)
    {
        _reuseCurrentBatch = reuse;
        _pendingSourceBatch = null;
        _pendingSourceBatchRowIndex = 0;
        if (!reuse)
            _currentBatch = CreateBatch(GetOutputColumnCount());
    }

    private async ValueTask<bool> FillExpressionBatchAsync(CancellationToken ct)
    {
        _batchIndex = 0;
        _batchCount = 0;

        while (_batchCount < DefaultBatchSize && await _source.MoveNextAsync(ct))
        {
            if (!EvaluatePredicate(_source.Current))
                continue;

            DbValue[] row = GetBatchRow(_batchCount, GetExpressionOutputColumnCount() ?? 0);
            WriteProjectedRow(_source.Current, row);

            StoreBatchRow(_batchCount, row);
            _batchCount++;
        }

        return _batchCount > 0;
    }

    private async ValueTask<bool> MoveNextBatchFromRowSourceAsync(CancellationToken ct)
    {
        int columnCount = GetOutputColumnCount();
        var batch = _reuseCurrentBatch ? EnsureBatch(columnCount) : CreateBatch(columnCount);
        batch.Reset();

        while (batch.Count < batch.Capacity && await _source.MoveNextAsync(ct))
        {
            if (!EvaluatePredicate(_source.Current))
                continue;

            WriteProjectedRow(_source.Current, batch.GetWritableRowSpan(batch.Count));
            batch.CommitWrittenRow(batch.Count);
        }

        _currentBatch = batch;
        return batch.Count > 0;
    }

    private async ValueTask<bool> MoveNextBatchFromBatchSourceAsync(IBatchOperator batchSource, CancellationToken ct)
    {
        int columnCount = GetOutputColumnCount();
        var batch = _reuseCurrentBatch ? EnsureBatch(columnCount) : CreateBatch(columnCount);
        batch.Reset();

        while (batch.Count < batch.Capacity)
        {
            if (_pendingSourceBatch == null || _pendingSourceBatchRowIndex >= _pendingSourceBatch.Count)
            {
                if (!await batchSource.MoveNextBatchAsync(ct))
                    break;

                _pendingSourceBatch = batchSource.CurrentBatch;
                _pendingSourceBatchRowIndex = 0;
            }

            while (batch.Count < batch.Capacity &&
                   _pendingSourceBatch != null &&
                   _pendingSourceBatchRowIndex < _pendingSourceBatch.Count)
            {
                var sourceRow = _pendingSourceBatch.GetRowSpan(_pendingSourceBatchRowIndex);
                _pendingSourceBatchRowIndex++;

                if (!EvaluatePredicate(sourceRow))
                    continue;

                WriteProjectedRow(sourceRow, batch.GetWritableRowSpan(batch.Count));
                batch.CommitWrittenRow(batch.Count);
            }

            if (_pendingSourceBatch != null && _pendingSourceBatchRowIndex >= _pendingSourceBatch.Count)
            {
                _pendingSourceBatch = null;
                _pendingSourceBatchRowIndex = 0;
            }
        }

        _currentBatch = batch;
        return batch.Count > 0;
    }

    private async ValueTask<bool> MoveNextBatchFromBatchSourcePlanAsync(IBatchOperator batchSource, CancellationToken ct)
    {
        int columnCount = _batchPlan!.OutputColumnCount;

        while (await batchSource.MoveNextBatchAsync(ct))
        {
            RowBatch sourceBatch = batchSource.CurrentBatch;
            var batch = _reuseCurrentBatch
                ? EnsureBatch(columnCount, Math.Max(DefaultBatchSize, sourceBatch.Count))
                : CreateBatch(columnCount, Math.Max(DefaultBatchSize, sourceBatch.Count));
            var selection = EnsureBatchSelection(sourceBatch.Count);

            if (_batchPlan.Execute(sourceBatch, selection, batch) > 0)
            {
                _currentBatch = batch;
                return true;
            }
        }

        var emptyBatch = _reuseCurrentBatch ? EnsureBatch(columnCount) : CreateBatch(columnCount);
        emptyBatch.Reset();
        _currentBatch = emptyBatch;
        return false;
    }

    private DbValue[] EnsureRowBuffer(int columnCount)
    {
        if (_rowBuffer == null || _rowBuffer.Length != columnCount)
            _rowBuffer = columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];

        return _rowBuffer;
    }

    private RowBatch EnsureBatch(int columnCount, int capacity = DefaultBatchSize)
    {
        if (_currentBatch.ColumnCount != columnCount || _currentBatch.Capacity != capacity)
            _currentBatch = CreateBatch(columnCount, capacity);

        return _currentBatch;
    }

    private DbValue[] GetBatchRow(int batchSlot, int columnCount)
    {
        if (!_reuseCurrentRowBuffer)
            return columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];

        _batchRows ??= new DbValue[DefaultBatchSize][];
        DbValue[]? row = _batchRows[batchSlot];
        if (row == null || row.Length != columnCount)
        {
            row = columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];
            _batchRows[batchSlot] = row;
        }

        return row;
    }

    private int GetOutputColumnCount()
        => GetExpressionOutputColumnCount() ?? _columnIndices.Length;

    private int? GetExpressionOutputColumnCount()
        => _spanExpressionEvaluators?.Length ?? _expressionEvaluators?.Length;

    private bool HasExpressionEvaluators()
        => _spanExpressionEvaluators != null || _expressionEvaluators != null;

    private static RowBatch CreateBatch(int columnCount, int capacity = DefaultBatchSize) => new(columnCount, capacity);

    private RowSelection EnsureBatchSelection(int capacity)
    {
        _batchSelection ??= new RowSelection(capacity);
        _batchSelection.EnsureCapacity(capacity);
        return _batchSelection;
    }

    private void WriteProjectedRow(ReadOnlySpan<DbValue> sourceRow, Span<DbValue> destination)
    {
        if (_spanExpressionEvaluators != null)
        {
            WriteExpressionProjectedRow(sourceRow, destination);
            return;
        }

        if (_expressionEvaluators != null)
        {
            var sourceRowBuffer = EnsureBatchSourceRowBuffer(sourceRow.Length);
            sourceRow.CopyTo(sourceRowBuffer);
            WriteExpressionProjectedRow(sourceRowBuffer, destination);
            return;
        }

        for (int i = 0; i < _columnIndices.Length; i++)
            destination[i] = sourceRow[_columnIndices[i]];
    }

    private void WriteProjectedRow(RowBatch sourceBatch, int rowIndex, Span<DbValue> destination)
    {
        if (_spanExpressionEvaluators != null)
        {
            WriteExpressionProjectedRow(sourceBatch.GetRowSpan(rowIndex), destination);
            return;
        }

        if (_expressionEvaluators != null)
        {
            var sourceRowBuffer = EnsureBatchSourceRowBuffer(sourceBatch.ColumnCount);
            sourceBatch.CopyRowTo(rowIndex, sourceRowBuffer);
            WriteExpressionProjectedRow(sourceRowBuffer, destination);
            return;
        }

        var sourceRow = sourceBatch.GetRowSpan(rowIndex);
        for (int i = 0; i < _columnIndices.Length; i++)
            destination[i] = sourceRow[_columnIndices[i]];
    }

    private void WriteExpressionProjectedRow(ReadOnlySpan<DbValue> sourceRow, Span<DbValue> destination)
    {
        if (_spanExpressionEvaluators != null)
        {
            for (int i = 0; i < _spanExpressionEvaluators.Length; i++)
                destination[i] = _spanExpressionEvaluators[i](sourceRow);

            return;
        }

        var sourceRowBuffer = EnsureBatchSourceRowBuffer(sourceRow.Length);
        sourceRow.CopyTo(sourceRowBuffer);
        WriteExpressionProjectedRow(sourceRowBuffer, destination);
    }

    private void WriteExpressionProjectedRow(DbValue[] sourceRow, Span<DbValue> destination)
    {
        for (int i = 0; i < _expressionEvaluators!.Length; i++)
            destination[i] = _expressionEvaluators[i](sourceRow);
    }

    private bool EvaluatePredicate(DbValue[] row)
    {
        if (_spanPredicateEvaluator != null)
            return _spanPredicateEvaluator(row).IsTruthy;

        return _predicateEvaluator(row).IsTruthy;
    }

    private bool EvaluatePredicate(ReadOnlySpan<DbValue> row)
    {
        if (_spanPredicateEvaluator != null)
            return _spanPredicateEvaluator(row).IsTruthy;

        var sourceRowBuffer = EnsureBatchSourceRowBuffer(row.Length);
        row.CopyTo(sourceRowBuffer);
        return _predicateEvaluator(sourceRowBuffer).IsTruthy;
    }

    private DbValue[] EnsureBatchSourceRowBuffer(int columnCount)
    {
        if (_batchSourceRowBuffer == null || _batchSourceRowBuffer.Length != columnCount)
            _batchSourceRowBuffer = columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];

        return _batchSourceRowBuffer;
    }

    private void StoreBatchRow(int batchSlot, DbValue[] row)
    {
        _batchRows ??= new DbValue[DefaultBatchSize][];
        _batchRows[batchSlot] = row;
    }

    private bool CanPassThroughSourceRow(int valueCount)
    {
        if (_source.ReusesCurrentRowBuffer)
            return false;

        var sourceRow = _source.Current;
        if (sourceRow.Length != valueCount)
            return false;

        for (int i = 0; i < valueCount; i++)
        {
            if (_columnIndices[i] != i)
                return false;
        }

        return true;
    }
}

/// <summary>
/// Direct table-scan filter/projection path that decodes only the referenced columns
/// into a compact row layout for simple single-table scans.
/// </summary>
public sealed class CompactTableScanProjectionOperator : IOperator, IBatchOperator, IRowBufferReuseController, IBatchBufferReuseController, IPreDecodeFilterSupport, IEstimatedRowCountProvider
{
    private const int DefaultBatchSize = 64;

    private readonly BTree _tree;
    private readonly IRecordSerializer _recordSerializer;
    private readonly int[] _decodedColumnIndices;
    private readonly int[] _projectionColumnIndices;
    private readonly Func<DbValue[], DbValue>[]? _expressionEvaluators;
    private readonly int? _estimatedRowCount;
    private bool _reuseCurrentRowBuffer = true;
    private bool _reuseCurrentBatch = true;
    private BTreeCursor? _cursor;
    private DbValue[]? _decodedRowBuffer;
    private DbValue[]? _projectedRowBuffer;
    private DbValue[][]? _decodedBatchRows;
    private DbValue[][]? _projectedBatchRows;
    private RowSelection? _batchSelection;
    private int _batchIndex;
    private int _batchCount;
    private Func<DbValue[], DbValue>? _predicateEvaluator;
    private IFilterProjectionBatchPlan? _batchPlan;
    private PreDecodeFilterSpec _preDecodeFilter;
    private bool _hasPreDecodeFilter;
    private PreDecodeFilterSpec[]? _additionalPreDecodeFilters;
    private RowBatch _currentBatch;
    private RowBatch _decodedSourceBatch = new(0, DefaultBatchSize);

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => _reuseCurrentRowBuffer;
    public int? EstimatedRowCount => _estimatedRowCount;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();

    public CompactTableScanProjectionOperator(
        BTree tree,
        int[] decodedColumnIndices,
        int[] projectionColumnIndices,
        ColumnDefinition[] outputSchema,
        IRecordSerializer? recordSerializer = null,
        int? estimatedRowCount = null)
    {
        _tree = tree;
        _decodedColumnIndices = decodedColumnIndices;
        _projectionColumnIndices = projectionColumnIndices;
        _recordSerializer = recordSerializer ?? new DefaultRecordSerializer();
        _estimatedRowCount = estimatedRowCount > 0 ? estimatedRowCount : null;
        OutputSchema = outputSchema;
        _currentBatch = CreateBatch(outputSchema.Length);
    }

    public CompactTableScanProjectionOperator(
        BTree tree,
        int[] decodedColumnIndices,
        ColumnDefinition[] outputSchema,
        Func<DbValue[], DbValue>[] expressionEvaluators,
        IRecordSerializer? recordSerializer = null,
        int? estimatedRowCount = null)
    {
        _tree = tree;
        _decodedColumnIndices = decodedColumnIndices;
        _projectionColumnIndices = Array.Empty<int>();
        _expressionEvaluators = expressionEvaluators;
        _recordSerializer = recordSerializer ?? new DefaultRecordSerializer();
        _estimatedRowCount = estimatedRowCount > 0 ? estimatedRowCount : null;
        OutputSchema = outputSchema;
        _currentBatch = CreateBatch(outputSchema.Length);
    }

    public void SetPredicateEvaluator(Func<DbValue[], DbValue>? predicateEvaluator)
        => _predicateEvaluator = predicateEvaluator;

    internal void SetBatchPlan(IFilterProjectionBatchPlan? batchPlan)
        => _batchPlan = batchPlan;

    public void SetPreDecodeFilter(int columnIndex, BinaryOp op, DbValue literal)
        => ((IPreDecodeFilterSupport)this).SetPreDecodeFilter(new PreDecodeFilterSpec(_recordSerializer, columnIndex, op, literal));

    void IPreDecodeFilterSupport.SetPreDecodeFilter(in PreDecodeFilterSpec filter)
    {
        if (_hasPreDecodeFilter)
        {
            AppendAdditionalPreDecodeFilter(filter);
            return;
        }

        _preDecodeFilter = filter;
        _hasPreDecodeFilter = true;
    }

    public ValueTask OpenAsync(CancellationToken ct = default)
    {
        _cursor = _tree.CreateCursor();
        _decodedRowBuffer = null;
        _projectedRowBuffer = null;
        _batchIndex = 0;
        _batchCount = 0;
        _batchSelection = null;
        _currentBatch = CreateBatch(OutputSchema.Length);
        _decodedSourceBatch = CreateBatch(_decodedColumnIndices.Length);
        Current = Array.Empty<DbValue>();
        return ValueTask.CompletedTask;
    }

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (_cursor == null)
            return false;

        if (_expressionEvaluators == null)
            return await MoveNextRowByRowAsync(ct);

        if (_batchIndex < _batchCount)
        {
            Current = _projectedBatchRows![_batchIndex++];
            return true;
        }

        if (!await FillBatchAsync(ct))
        {
            Current = Array.Empty<DbValue>();
            return false;
        }

        Current = _projectedBatchRows![_batchIndex++];
        return true;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public void SetReuseCurrentRowBuffer(bool reuse)
    {
        _reuseCurrentRowBuffer = reuse;
        if (!reuse)
        {
            _decodedRowBuffer = null;
            _projectedRowBuffer = null;
            _decodedBatchRows = null;
            _projectedBatchRows = null;
            _batchIndex = 0;
            _batchCount = 0;
            Current = Array.Empty<DbValue>();
        }
    }

    public async ValueTask<bool> MoveNextBatchAsync(CancellationToken ct = default)
    {
        if (_cursor == null)
            return false;

        if (_batchPlan != null)
            return await MoveNextPlannedBatchAsync(ct);

        var batch = _reuseCurrentBatch ? EnsureBatch(OutputSchema.Length) : CreateBatch(OutputSchema.Length);
        batch.Reset();

        while (batch.Count < batch.Capacity && await _cursor.MoveNextAsync(ct))
        {
            ReadOnlySpan<byte> payload = _cursor.CurrentValue.Span;
            if (_hasPreDecodeFilter && !EvaluatePreDecodeFilter(payload))
                continue;

            int rowIndex = batch.Count;
            if (!TryProjectPayloadInto(payload, batch.GetWritableRowSpan(rowIndex)))
                continue;

            batch.CommitWrittenRow(rowIndex);
        }

        _currentBatch = batch;
        return batch.Count > 0;
    }

    bool IBatchOperator.ReusesCurrentBatch => _reuseCurrentBatch;
    RowBatch IBatchOperator.CurrentBatch => _currentBatch;

    public void SetReuseCurrentBatch(bool reuse)
    {
        _reuseCurrentBatch = reuse;
        if (!reuse)
            _currentBatch = CreateBatch(OutputSchema.Length);
    }

    private async ValueTask<bool> MoveNextPlannedBatchAsync(CancellationToken ct)
    {
        if (_cursor == null)
            return false;

        int sourceColumnCount = _decodedColumnIndices.Length;
        int outputColumnCount = _batchPlan!.OutputColumnCount;

        while (true)
        {
            var sourceBatch = EnsureDecodedSourceBatch(sourceColumnCount);
            sourceBatch.Reset();

            while (sourceBatch.Count < sourceBatch.Capacity && await _cursor.MoveNextAsync(ct))
            {
                ReadOnlySpan<byte> payload = _cursor.CurrentValue.Span;
                if (_hasPreDecodeFilter && !EvaluatePreDecodeFilter(payload))
                    continue;

                int rowIndex = sourceBatch.Count;
                if (sourceColumnCount > 0)
                    _recordSerializer.DecodeSelectedCompactInto(payload, sourceBatch.GetWritableRowSpan(rowIndex), _decodedColumnIndices);

                sourceBatch.CommitWrittenRow(rowIndex);
            }

            if (sourceBatch.Count == 0)
            {
                var emptyBatch = _reuseCurrentBatch ? EnsureBatch(outputColumnCount) : CreateBatch(outputColumnCount);
                emptyBatch.Reset();
                _currentBatch = emptyBatch;
                return false;
            }

            var batch = _reuseCurrentBatch ? EnsureBatch(outputColumnCount) : CreateBatch(outputColumnCount);
            batch.Reset();

            var selection = EnsureBatchSelection(sourceBatch.Count);
            if (_batchPlan.Execute(sourceBatch, selection, batch) > 0)
            {
                _currentBatch = batch;
                return true;
            }
        }
    }

    private async ValueTask<bool> FillBatchAsync(CancellationToken ct)
    {
        if (_cursor == null)
            return false;

        bool passThroughDecodedRow = CanPassThroughDecodedRow();
        _batchIndex = 0;
        _batchCount = 0;

        while (_batchCount < DefaultBatchSize && await _cursor.MoveNextAsync(ct))
        {
            ReadOnlySpan<byte> payload = _cursor.CurrentValue.Span;
            if (_hasPreDecodeFilter && !EvaluatePreDecodeFilter(payload))
                continue;

            DbValue[] decodedRow = DecodeRow(payload, _batchCount);
            if (_predicateEvaluator != null && !_predicateEvaluator(decodedRow).IsTruthy)
                continue;

            DbValue[] outputRow;
            if (_expressionEvaluators != null)
            {
                outputRow = GetProjectedBatchRow(_batchCount, _expressionEvaluators.Length);
                for (int i = 0; i < _expressionEvaluators.Length; i++)
                    outputRow[i] = _expressionEvaluators[i](decodedRow);
            }
            else if (passThroughDecodedRow)
            {
                outputRow = decodedRow;
            }
            else
            {
                outputRow = GetProjectedBatchRow(_batchCount, _projectionColumnIndices.Length);
                for (int i = 0; i < _projectionColumnIndices.Length; i++)
                    outputRow[i] = decodedRow[_projectionColumnIndices[i]];
            }

            StoreProjectedBatchRow(_batchCount, outputRow);
            _batchCount++;
        }

        return _batchCount > 0;
    }

    private async ValueTask<bool> MoveNextRowByRowAsync(CancellationToken ct)
    {
        if (_cursor == null)
            return false;

        while (await _cursor.MoveNextAsync(ct))
        {
            ReadOnlySpan<byte> payload = _cursor.CurrentValue.Span;
            if (_hasPreDecodeFilter && !EvaluatePreDecodeFilter(payload))
                continue;

            DbValue[] decodedRow = DecodeSingleRow(payload);
            if (_predicateEvaluator != null && !_predicateEvaluator(decodedRow).IsTruthy)
                continue;

            if (CanPassThroughDecodedRow())
            {
                Current = decodedRow;
                return true;
            }

            DbValue[] outputRow = GetProjectedRowBuffer(_projectionColumnIndices.Length);
            for (int i = 0; i < _projectionColumnIndices.Length; i++)
                outputRow[i] = decodedRow[_projectionColumnIndices[i]];

            Current = outputRow;
            return true;
        }

        Current = Array.Empty<DbValue>();
        return false;
    }

    private bool TryProjectPayloadInto(ReadOnlySpan<byte> payload, Span<DbValue> destination)
    {
        int decodeCount = _decodedColumnIndices.Length;
        if (CanPassThroughDecodedRow() && _predicateEvaluator == null)
        {
            if (decodeCount > 0)
                _recordSerializer.DecodeSelectedCompactInto(payload, destination, _decodedColumnIndices);

            return true;
        }

        DbValue[] decodedRow = EnsureDecodedRowBuffer(decodeCount);
        if (decodeCount > 0)
            _recordSerializer.DecodeSelectedCompactInto(payload, decodedRow, _decodedColumnIndices);

        if (_predicateEvaluator != null && !_predicateEvaluator(decodedRow).IsTruthy)
            return false;

        if (_expressionEvaluators != null)
        {
            for (int i = 0; i < _expressionEvaluators.Length; i++)
                destination[i] = _expressionEvaluators[i](decodedRow);
        }
        else
        {
            for (int i = 0; i < _projectionColumnIndices.Length; i++)
                destination[i] = decodedRow[_projectionColumnIndices[i]];
        }

        return true;
    }

    private DbValue[] DecodeSingleRow(ReadOnlySpan<byte> payload)
    {
        int decodeCount = _decodedColumnIndices.Length;
        if (decodeCount == 0)
            return Array.Empty<DbValue>();

        DbValue[] row = _reuseCurrentRowBuffer
            ? EnsureDecodedRowBuffer(decodeCount)
            : new DbValue[decodeCount];
        _recordSerializer.DecodeSelectedCompactInto(payload, row, _decodedColumnIndices);
        return row;
    }

    private DbValue[] EnsureDecodedRowBuffer(int columnCount)
    {
        if (_decodedRowBuffer == null || _decodedRowBuffer.Length != columnCount)
            _decodedRowBuffer = columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];

        return _decodedRowBuffer;
    }

    private DbValue[] GetProjectedRowBuffer(int columnCount)
    {
        if (!_reuseCurrentRowBuffer)
            return columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];

        if (_projectedRowBuffer == null || _projectedRowBuffer.Length != columnCount)
            _projectedRowBuffer = columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];

        return _projectedRowBuffer;
    }

    private DbValue[] DecodeRow(ReadOnlySpan<byte> payload, int batchSlot)
    {
        int decodeCount = _decodedColumnIndices.Length;
        if (decodeCount == 0)
            return Array.Empty<DbValue>();

        DbValue[] row = GetDecodedBatchRow(batchSlot, decodeCount);
        _recordSerializer.DecodeSelectedCompactInto(payload, row, _decodedColumnIndices);
        return row;
    }

    private DbValue[] GetDecodedBatchRow(int batchSlot, int columnCount)
    {
        if (!_reuseCurrentRowBuffer)
            return columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];

        _decodedBatchRows ??= new DbValue[DefaultBatchSize][];
        DbValue[]? row = _decodedBatchRows[batchSlot];
        if (row == null || row.Length != columnCount)
        {
            row = columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];
            _decodedBatchRows[batchSlot] = row;
        }

        return row;
    }

    private DbValue[] GetProjectedBatchRow(int batchSlot, int columnCount)
    {
        if (!_reuseCurrentRowBuffer)
            return columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];

        _projectedBatchRows ??= new DbValue[DefaultBatchSize][];
        DbValue[]? row = _projectedBatchRows[batchSlot];
        if (row == null || row.Length != columnCount)
        {
            row = columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];
            _projectedBatchRows[batchSlot] = row;
        }

        return row;
    }

    private void StoreProjectedBatchRow(int batchSlot, DbValue[] row)
    {
        _projectedBatchRows ??= new DbValue[DefaultBatchSize][];
        _projectedBatchRows[batchSlot] = row;
    }

    private RowBatch EnsureBatch(int columnCount)
    {
        if (_currentBatch.ColumnCount != columnCount)
            _currentBatch = CreateBatch(columnCount);

        return _currentBatch;
    }

    private static RowBatch CreateBatch(int columnCount) => new(columnCount, DefaultBatchSize);

    private RowBatch EnsureDecodedSourceBatch(int columnCount)
    {
        if (_decodedSourceBatch.ColumnCount != columnCount)
            _decodedSourceBatch = CreateBatch(columnCount);

        return _decodedSourceBatch;
    }

    private RowSelection EnsureBatchSelection(int capacity)
    {
        _batchSelection ??= new RowSelection(capacity);
        _batchSelection.EnsureCapacity(capacity);
        return _batchSelection;
    }

    private bool CanPassThroughDecodedRow()
    {
        if (_expressionEvaluators != null || _projectionColumnIndices.Length != _decodedColumnIndices.Length)
            return false;

        for (int i = 0; i < _projectionColumnIndices.Length; i++)
        {
            if (_projectionColumnIndices[i] != i)
                return false;
        }

        return true;
    }

    private bool EvaluatePreDecodeFilter(ReadOnlySpan<byte> payload)
    {
        if (!BoundColumnAccessHelper.EvaluatePreDecodeFilter(payload, _recordSerializer, _preDecodeFilter))
        {
            return false;
        }

        return _additionalPreDecodeFilters is not { Length: > 0 }
            || BoundColumnAccessHelper.EvaluatePreDecodeFilters(payload, _recordSerializer, _additionalPreDecodeFilters);
    }

    private void AppendAdditionalPreDecodeFilter(in PreDecodeFilterSpec filter)
    {
        if (_additionalPreDecodeFilters == null)
        {
            _additionalPreDecodeFilters = [filter];
            return;
        }

        int length = _additionalPreDecodeFilters.Length;
        Array.Resize(ref _additionalPreDecodeFilters, length + 1);
        _additionalPreDecodeFilters[length] = filter;
    }
}

/// <summary>
/// Compact projection path that reuses an existing payload-producing source
/// and decodes only the referenced columns into a compact row layout.
/// </summary>
public sealed class CompactPayloadProjectionOperator : IOperator, IBatchOperator, IRowBufferReuseController, IBatchBufferReuseController, IEstimatedRowCountProvider, IUnaryOperatorSource
{
    private const int DefaultBatchSize = 64;

    private readonly IOperator _source;
    private readonly IEncodedPayloadSource _payloadSource;
    private readonly IRecordSerializer _recordSerializer;
    private readonly int[] _decodedColumnIndices;
    private readonly int[] _projectionColumnIndices;
    private readonly Func<DbValue[], DbValue>[]? _expressionEvaluators;
    private bool _reuseCurrentRowBuffer = true;
    private bool _reuseCurrentBatch = true;
    private DbValue[]? _decodedRowBuffer;
    private DbValue[]? _projectedRowBuffer;
    private DbValue[][]? _decodedBatchRows;
    private DbValue[][]? _projectedBatchRows;
    private RowSelection? _batchSelection;
    private int _batchIndex;
    private int _batchCount;
    private Func<DbValue[], DbValue>? _predicateEvaluator;
    private IFilterProjectionBatchPlan? _batchPlan;
    private RowBatch _currentBatch;
    private RowBatch _decodedSourceBatch = new(0, DefaultBatchSize);

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => _reuseCurrentRowBuffer;
    public int? EstimatedRowCount => _source is IEstimatedRowCountProvider estimated ? estimated.EstimatedRowCount : null;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
    IOperator IUnaryOperatorSource.Source => _source;

    public CompactPayloadProjectionOperator(
        IOperator source,
        IRecordSerializer recordSerializer,
        int[] decodedColumnIndices,
        int[] projectionColumnIndices,
        ColumnDefinition[] outputSchema)
    {
        _source = source;
        _payloadSource = source as IEncodedPayloadSource
            ?? throw new ArgumentException("Source must expose encoded payload.", nameof(source));
        _recordSerializer = recordSerializer;
        _decodedColumnIndices = decodedColumnIndices;
        _projectionColumnIndices = projectionColumnIndices;
        OutputSchema = outputSchema;
        _currentBatch = CreateBatch(outputSchema.Length);
    }

    public CompactPayloadProjectionOperator(
        IOperator source,
        IRecordSerializer recordSerializer,
        int[] decodedColumnIndices,
        ColumnDefinition[] outputSchema,
        Func<DbValue[], DbValue>[] expressionEvaluators)
    {
        _source = source;
        _payloadSource = source as IEncodedPayloadSource
            ?? throw new ArgumentException("Source must expose encoded payload.", nameof(source));
        _recordSerializer = recordSerializer;
        _decodedColumnIndices = decodedColumnIndices;
        _projectionColumnIndices = Array.Empty<int>();
        _expressionEvaluators = expressionEvaluators;
        OutputSchema = outputSchema;
        _currentBatch = CreateBatch(outputSchema.Length);
    }

    public void SetPredicateEvaluator(Func<DbValue[], DbValue>? predicateEvaluator)
        => _predicateEvaluator = predicateEvaluator;

    internal void SetBatchPlan(IFilterProjectionBatchPlan? batchPlan)
        => _batchPlan = batchPlan;

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        SuppressSourceDecode();
        _decodedRowBuffer = null;
        _projectedRowBuffer = null;
        _decodedBatchRows = null;
        _projectedBatchRows = null;
        _batchIndex = 0;
        _batchCount = 0;
        _batchSelection = null;
        _currentBatch = CreateBatch(OutputSchema.Length);
        _decodedSourceBatch = CreateBatch(_decodedColumnIndices.Length);
        Current = Array.Empty<DbValue>();
        await _source.OpenAsync(ct);
    }

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (_expressionEvaluators == null)
            return await MoveNextRowByRowAsync(ct);

        if (_batchIndex < _batchCount)
        {
            Current = _projectedBatchRows![_batchIndex++];
            return true;
        }

        if (!await FillExpressionBatchAsync(ct))
        {
            Current = Array.Empty<DbValue>();
            return false;
        }

        Current = _projectedBatchRows![_batchIndex++];
        return true;
    }

    private async ValueTask<bool> MoveNextRowByRowAsync(CancellationToken ct)
    {
        while (await _source.MoveNextAsync(ct))
        {
            DbValue[] decodedRow = DecodeRow(_payloadSource.CurrentPayload.Span);
            if (_predicateEvaluator != null && !_predicateEvaluator(decodedRow).IsTruthy)
                continue;

            if (CanPassThroughDecodedRow())
            {
                Current = decodedRow;
                return true;
            }

            DbValue[] outputRow = GetProjectedRowBuffer(_projectionColumnIndices.Length);
            for (int i = 0; i < _projectionColumnIndices.Length; i++)
                outputRow[i] = decodedRow[_projectionColumnIndices[i]];

            Current = outputRow;
            return true;
        }

        Current = Array.Empty<DbValue>();
        return false;
    }

    public ValueTask DisposeAsync() => _source.DisposeAsync();

    public void SetReuseCurrentRowBuffer(bool reuse)
    {
        _reuseCurrentRowBuffer = reuse;
        if (!reuse)
        {
            _decodedRowBuffer = null;
            _projectedRowBuffer = null;
            _decodedBatchRows = null;
            _projectedBatchRows = null;
            _batchIndex = 0;
            _batchCount = 0;
            Current = Array.Empty<DbValue>();
        }
    }

    public async ValueTask<bool> MoveNextBatchAsync(CancellationToken ct = default)
    {
        if (_batchPlan != null)
            return await MoveNextPlannedBatchAsync(ct);

        var batch = _reuseCurrentBatch ? EnsureBatch(OutputSchema.Length) : CreateBatch(OutputSchema.Length);
        batch.Reset();

        while (batch.Count < batch.Capacity && await _source.MoveNextAsync(ct))
        {
            int rowIndex = batch.Count;
            if (!TryProjectPayloadInto(_payloadSource.CurrentPayload.Span, batch.GetWritableRowSpan(rowIndex)))
                continue;

            batch.CommitWrittenRow(rowIndex);
        }

        _currentBatch = batch;
        return batch.Count > 0;
    }

    bool IBatchOperator.ReusesCurrentBatch => _reuseCurrentBatch;
    RowBatch IBatchOperator.CurrentBatch => _currentBatch;

    public void SetReuseCurrentBatch(bool reuse)
    {
        _reuseCurrentBatch = reuse;
        if (!reuse)
            _currentBatch = CreateBatch(OutputSchema.Length);
    }

    private async ValueTask<bool> MoveNextPlannedBatchAsync(CancellationToken ct)
    {
        int sourceColumnCount = _decodedColumnIndices.Length;
        int outputColumnCount = _batchPlan!.OutputColumnCount;

        while (true)
        {
            var sourceBatch = EnsureDecodedSourceBatch(sourceColumnCount);
            sourceBatch.Reset();

            while (sourceBatch.Count < sourceBatch.Capacity && await _source.MoveNextAsync(ct))
            {
                ReadOnlySpan<byte> payload = _payloadSource.CurrentPayload.Span;
                int rowIndex = sourceBatch.Count;
                if (sourceColumnCount > 0)
                    _recordSerializer.DecodeSelectedCompactInto(payload, sourceBatch.GetWritableRowSpan(rowIndex), _decodedColumnIndices);

                sourceBatch.CommitWrittenRow(rowIndex);
            }

            if (sourceBatch.Count == 0)
            {
                var emptyBatch = _reuseCurrentBatch ? EnsureBatch(outputColumnCount) : CreateBatch(outputColumnCount);
                emptyBatch.Reset();
                _currentBatch = emptyBatch;
                return false;
            }

            var batch = _reuseCurrentBatch ? EnsureBatch(outputColumnCount) : CreateBatch(outputColumnCount);
            batch.Reset();

            var selection = EnsureBatchSelection(sourceBatch.Count);
            if (_batchPlan.Execute(sourceBatch, selection, batch) > 0)
            {
                _currentBatch = batch;
                return true;
            }
        }
    }

    private void SuppressSourceDecode()
    {
        switch (_source)
        {
            case IndexScanOperator indexScan:
                indexScan.SetDecodedColumnIndices(Array.Empty<int>());
                break;
            case IndexOrderedScanOperator orderedScan:
                orderedScan.SetDecodedColumnIndices(Array.Empty<int>());
                break;
            case UniqueIndexLookupOperator uniqueLookup:
                uniqueLookup.SetDecodedColumnIndices(Array.Empty<int>());
                break;
        }
    }

    private bool TryProjectPayloadInto(ReadOnlySpan<byte> payload, Span<DbValue> destination)
    {
        int decodeCount = _decodedColumnIndices.Length;
        if (CanPassThroughDecodedRow() && _predicateEvaluator == null)
        {
            if (decodeCount > 0)
                _recordSerializer.DecodeSelectedCompactInto(payload, destination, _decodedColumnIndices);
            return true;
        }

        DbValue[] decodedRow = EnsureDecodedRowBuffer(decodeCount);
        if (decodeCount > 0)
            _recordSerializer.DecodeSelectedCompactInto(payload, decodedRow, _decodedColumnIndices);

        if (_predicateEvaluator != null && !_predicateEvaluator(decodedRow).IsTruthy)
            return false;

        if (_expressionEvaluators != null)
        {
            for (int i = 0; i < _expressionEvaluators.Length; i++)
                destination[i] = _expressionEvaluators[i](decodedRow);
        }
        else
        {
            for (int i = 0; i < _projectionColumnIndices.Length; i++)
                destination[i] = decodedRow[_projectionColumnIndices[i]];
        }

        return true;
    }

    private DbValue[] DecodeRow(ReadOnlySpan<byte> payload)
    {
        int decodeCount = _decodedColumnIndices.Length;
        if (decodeCount == 0)
            return Array.Empty<DbValue>();

        DbValue[] row = _reuseCurrentRowBuffer
            ? EnsureDecodedRowBuffer(decodeCount)
            : new DbValue[decodeCount];
        _recordSerializer.DecodeSelectedCompactInto(payload, row, _decodedColumnIndices);
        return row;
    }

    private DbValue[] EnsureDecodedRowBuffer(int columnCount)
    {
        if (_decodedRowBuffer == null || _decodedRowBuffer.Length != columnCount)
            _decodedRowBuffer = columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];

        return _decodedRowBuffer;
    }

    private DbValue[] GetProjectedRowBuffer(int columnCount)
    {
        if (!_reuseCurrentRowBuffer)
            return columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];

        if (_projectedRowBuffer == null || _projectedRowBuffer.Length != columnCount)
            _projectedRowBuffer = columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];

        return _projectedRowBuffer;
    }

    private async ValueTask<bool> FillExpressionBatchAsync(CancellationToken ct)
    {
        _batchIndex = 0;
        _batchCount = 0;

        while (_batchCount < DefaultBatchSize && await _source.MoveNextAsync(ct))
        {
            DbValue[] decodedRow = DecodeBatchRow(_payloadSource.CurrentPayload.Span, _batchCount);
            if (_predicateEvaluator != null && !_predicateEvaluator(decodedRow).IsTruthy)
                continue;

            DbValue[] projectedRow = GetProjectedBatchRow(_batchCount, _expressionEvaluators!.Length);
            for (int i = 0; i < _expressionEvaluators.Length; i++)
                projectedRow[i] = _expressionEvaluators[i](decodedRow);

            StoreProjectedBatchRow(_batchCount, projectedRow);
            _batchCount++;
        }

        return _batchCount > 0;
    }

    private DbValue[] DecodeBatchRow(ReadOnlySpan<byte> payload, int batchSlot)
    {
        int decodeCount = _decodedColumnIndices.Length;
        if (decodeCount == 0)
            return Array.Empty<DbValue>();

        DbValue[] row = GetDecodedBatchRow(batchSlot, decodeCount);
        _recordSerializer.DecodeSelectedCompactInto(payload, row, _decodedColumnIndices);
        return row;
    }

    private DbValue[] GetDecodedBatchRow(int batchSlot, int columnCount)
    {
        if (!_reuseCurrentRowBuffer)
            return columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];

        _decodedBatchRows ??= new DbValue[DefaultBatchSize][];
        DbValue[]? row = _decodedBatchRows[batchSlot];
        if (row == null || row.Length != columnCount)
        {
            row = columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];
            _decodedBatchRows[batchSlot] = row;
        }

        return row;
    }

    private DbValue[] GetProjectedBatchRow(int batchSlot, int columnCount)
    {
        if (!_reuseCurrentRowBuffer)
            return columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];

        _projectedBatchRows ??= new DbValue[DefaultBatchSize][];
        DbValue[]? row = _projectedBatchRows[batchSlot];
        if (row == null || row.Length != columnCount)
        {
            row = columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];
            _projectedBatchRows[batchSlot] = row;
        }

        return row;
    }

    private void StoreProjectedBatchRow(int batchSlot, DbValue[] row)
    {
        _projectedBatchRows ??= new DbValue[DefaultBatchSize][];
        _projectedBatchRows[batchSlot] = row;
    }

    private RowBatch EnsureBatch(int columnCount)
    {
        if (_currentBatch.ColumnCount != columnCount)
            _currentBatch = CreateBatch(columnCount);

        return _currentBatch;
    }

    private static RowBatch CreateBatch(int columnCount) => new(columnCount, DefaultBatchSize);

    private RowBatch EnsureDecodedSourceBatch(int columnCount)
    {
        if (_decodedSourceBatch.ColumnCount != columnCount)
            _decodedSourceBatch = CreateBatch(columnCount);

        return _decodedSourceBatch;
    }

    private RowSelection EnsureBatchSelection(int capacity)
    {
        _batchSelection ??= new RowSelection(capacity);
        _batchSelection.EnsureCapacity(capacity);
        return _batchSelection;
    }

    private bool CanPassThroughDecodedRow()
    {
        if (_expressionEvaluators != null || _projectionColumnIndices.Length != _decodedColumnIndices.Length)
            return false;

        for (int i = 0; i < _projectionColumnIndices.Length; i++)
        {
            if (_projectionColumnIndices[i] != i)
                return false;
        }

        return true;
    }
}

/// <summary>
/// DISTINCT operator — emits each unique row once.
/// </summary>
public sealed class DistinctOperator : IOperator, IBatchOperator, IBatchBufferReuseController
{
    private const int DefaultBatchSize = 64;

    private readonly IOperator _source;
    private readonly HashSet<DistinctRowKey> _seenRows = new(new DistinctRowKeyComparer());
    private readonly HashSet<DbValue> _seenSingleValues = new();
    private readonly bool _singleColumnFastPath;
    private readonly bool _orderedSingleColumnFastPath;
    private bool _reuseCurrentBatch = true;
    private RowBatch _currentBatch = new(0, DefaultBatchSize);
    private RowBatch? _pendingSourceBatch;
    private int _pendingSourceBatchRowIndex;
    private bool _hasLastOrderedSingleValue;
    private DbValue _lastOrderedSingleValue = DbValue.Null;

    public ColumnDefinition[] OutputSchema => _source.OutputSchema;
    public bool ReusesCurrentRowBuffer => false;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();

    public DistinctOperator(IOperator source, bool inputIsOrdered = false)
    {
        _source = source;
        _singleColumnFastPath = source.OutputSchema.Length == 1;
        _orderedSingleColumnFastPath = inputIsOrdered && _singleColumnFastPath;
    }

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        _seenRows.Clear();
        _seenSingleValues.Clear();
        _currentBatch = CreateBatch(_source.OutputSchema.Length);
        _pendingSourceBatch = null;
        _pendingSourceBatchRowIndex = 0;
        _hasLastOrderedSingleValue = false;
        _lastOrderedSingleValue = DbValue.Null;
        Current = Array.Empty<DbValue>();
        await _source.OpenAsync(ct);
    }

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (_orderedSingleColumnFastPath)
        {
            while (await _source.MoveNextAsync(ct))
            {
                DbValue[] sourceRow = _source.Current;
                if (!TryAddOrderedSingleValue(sourceRow[0]))
                    continue;

                Current = _source.ReusesCurrentRowBuffer ? (DbValue[])sourceRow.Clone() : sourceRow;
                return true;
            }

            return false;
        }

        while (await _source.MoveNextAsync(ct))
        {
            DbValue[] sourceRow = _source.Current;
            if (TryAddDistinctRow(sourceRow, cloneRow: _source.ReusesCurrentRowBuffer, out var ownedRow))
            {
                Current = ownedRow;
                return true;
            }
        }

        return false;
    }

    public ValueTask DisposeAsync() => _source.DisposeAsync();

    public async ValueTask<bool> MoveNextBatchAsync(CancellationToken ct = default)
    {
        var batch = _reuseCurrentBatch ? EnsureBatch(_source.OutputSchema.Length) : CreateBatch(_source.OutputSchema.Length);
        batch.Reset();

        var batchSource = BatchSourceHelper.TryGetBatchSource(_source);
        if (_orderedSingleColumnFastPath)
        {
            if (batchSource != null)
            {
                while (batch.Count < batch.Capacity)
                {
                    if (_pendingSourceBatch == null || _pendingSourceBatchRowIndex >= _pendingSourceBatch.Count)
                    {
                        if (!await batchSource.MoveNextBatchAsync(ct))
                            break;

                        _pendingSourceBatch = batchSource.CurrentBatch;
                        _pendingSourceBatchRowIndex = 0;
                    }

                    while (batch.Count < batch.Capacity &&
                           _pendingSourceBatch != null &&
                           _pendingSourceBatchRowIndex < _pendingSourceBatch.Count)
                    {
                        var sourceRow = _pendingSourceBatch.GetRowSpan(_pendingSourceBatchRowIndex);
                        _pendingSourceBatchRowIndex++;

                        if (!TryAddOrderedSingleValue(sourceRow[0]))
                            continue;

                        sourceRow.CopyTo(batch.GetWritableRowSpan(batch.Count));
                        batch.CommitWrittenRow(batch.Count);
                    }

                    if (_pendingSourceBatch != null && _pendingSourceBatchRowIndex >= _pendingSourceBatch.Count)
                    {
                        _pendingSourceBatch = null;
                        _pendingSourceBatchRowIndex = 0;
                    }
                }
            }
            else
            {
                while (batch.Count < batch.Capacity && await _source.MoveNextAsync(ct))
                {
                    if (!TryAddOrderedSingleValue(_source.Current[0]))
                        continue;

                    batch.AppendRow(_source.Current);
                }
            }

            _currentBatch = batch;
            return batch.Count > 0;
        }

        if (batchSource != null)
        {
            while (batch.Count < batch.Capacity)
            {
                if (_pendingSourceBatch == null || _pendingSourceBatchRowIndex >= _pendingSourceBatch.Count)
                {
                    if (!await batchSource.MoveNextBatchAsync(ct))
                        break;

                    _pendingSourceBatch = batchSource.CurrentBatch;
                    _pendingSourceBatchRowIndex = 0;
                }

                while (batch.Count < batch.Capacity &&
                       _pendingSourceBatch != null &&
                       _pendingSourceBatchRowIndex < _pendingSourceBatch.Count)
                {
                    var sourceRow = _pendingSourceBatch.GetRowSpan(_pendingSourceBatchRowIndex);
                    _pendingSourceBatchRowIndex++;

                    if (!TryAddDistinctRow(sourceRow))
                        continue;

                    sourceRow.CopyTo(batch.GetWritableRowSpan(batch.Count));
                    batch.CommitWrittenRow(batch.Count);
                }

                if (_pendingSourceBatch != null && _pendingSourceBatchRowIndex >= _pendingSourceBatch.Count)
                {
                    _pendingSourceBatch = null;
                    _pendingSourceBatchRowIndex = 0;
                }
            }
        }
        else
        {
            while (batch.Count < batch.Capacity && await _source.MoveNextAsync(ct))
            {
                if (!TryAddDistinctRow(_source.Current, cloneRow: _source.ReusesCurrentRowBuffer, out _))
                    continue;

                batch.AppendRow(_source.Current);
            }
        }

        _currentBatch = batch;
        return batch.Count > 0;
    }

    bool IBatchOperator.ReusesCurrentBatch => _reuseCurrentBatch;
    RowBatch IBatchOperator.CurrentBatch => _currentBatch;

    public void SetReuseCurrentBatch(bool reuse)
    {
        _reuseCurrentBatch = reuse;
        _pendingSourceBatch = null;
        _pendingSourceBatchRowIndex = 0;
        if (!reuse)
            _currentBatch = CreateBatch(_source.OutputSchema.Length);
    }

    private static int ComputeHashCode(DbValue[] row)
    {
        var hash = new HashCode();
        for (int i = 0; i < row.Length; i++)
            hash.Add(row[i]);
        return hash.ToHashCode();
    }

    private bool TryAddDistinctRow(DbValue[] row, bool cloneRow, out DbValue[] ownedRow)
    {
        ownedRow = cloneRow ? (DbValue[])row.Clone() : row;
        var rowKey = new DistinctRowKey(ownedRow, ComputeHashCode(ownedRow));
        return _seenRows.Add(rowKey);
    }

    private bool TryAddDistinctRow(ReadOnlySpan<DbValue> row)
    {
        if (_singleColumnFastPath)
            return _seenSingleValues.Add(row[0]);

        var ownedRow = row.ToArray();
        var rowKey = new DistinctRowKey(ownedRow, ComputeHashCode(ownedRow));
        return _seenRows.Add(rowKey);
    }

    private bool TryAddOrderedSingleValue(DbValue value)
    {
        if (_hasLastOrderedSingleValue && _lastOrderedSingleValue.Equals(value))
            return false;

        _lastOrderedSingleValue = value;
        _hasLastOrderedSingleValue = true;
        return true;
    }

    private RowBatch EnsureBatch(int columnCount)
    {
        if (_currentBatch.ColumnCount != columnCount)
            _currentBatch = CreateBatch(columnCount);

        return _currentBatch;
    }

    private static RowBatch CreateBatch(int columnCount) => new(columnCount, DefaultBatchSize);

    private readonly struct DistinctRowKey
    {
        public DbValue[] Values { get; }
        public int HashCode { get; }

        public DistinctRowKey(DbValue[] values, int hashCode)
        {
            Values = values;
            HashCode = hashCode;
        }
    }

    private sealed class DistinctRowKeyComparer : IEqualityComparer<DistinctRowKey>
    {
        public bool Equals(DistinctRowKey x, DistinctRowKey y)
        {
            if (x.HashCode != y.HashCode)
                return false;
            if (x.Values.Length != y.Values.Length)
                return false;

            for (int i = 0; i < x.Values.Length; i++)
            {
                if (!x.Values[i].Equals(y.Values[i]))
                    return false;
            }

            return true;
        }

        public int GetHashCode(DistinctRowKey obj) => obj.HashCode;
    }
}

/// <summary>
/// Offset operator — skips the first N rows from the source.
/// </summary>
public sealed class OffsetOperator : IOperator, IBatchOperator, IRowBufferReuseController, IBatchBufferReuseController, IUnaryOperatorSource
{
    private const int DefaultBatchSize = 64;

    private readonly IOperator _source;
    private readonly int _offset;
    private int _skipped;
    private bool _reuseCurrentBatch = true;
    private RowBatch _currentBatch;
    private int _sourceBatchRowIndex = -1;

    public ColumnDefinition[] OutputSchema => _source.OutputSchema;
    public bool ReusesCurrentRowBuffer => _source.ReusesCurrentRowBuffer;
    public DbValue[] Current => _source.Current;
    IOperator IUnaryOperatorSource.Source => _source;

    public OffsetOperator(IOperator source, int offset)
    {
        _source = source;
        _offset = offset;
        _currentBatch = CreateBatch(source.OutputSchema.Length);
    }

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        _skipped = 0;
        _currentBatch = CreateBatch(_source.OutputSchema.Length);
        _sourceBatchRowIndex = -1;
        await _source.OpenAsync(ct);
    }

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        while (_skipped < _offset)
        {
            if (!await _source.MoveNextAsync(ct)) return false;
            _skipped++;
        }
        return await _source.MoveNextAsync(ct);
    }

    public ValueTask DisposeAsync() => _source.DisposeAsync();

    public void SetReuseCurrentRowBuffer(bool reuse)
    {
        if (_source is IRowBufferReuseController controller)
            controller.SetReuseCurrentRowBuffer(reuse);
    }

    public async ValueTask<bool> MoveNextBatchAsync(CancellationToken ct = default)
    {
        var batchSource = BatchSourceHelper.TryGetBatchSource(_source);
        if (batchSource != null)
            return await MoveNextBatchFromBatchSourceAsync(batchSource, ct);

        var batch = _reuseCurrentBatch ? EnsureBatch(_source.OutputSchema.Length) : CreateBatch(_source.OutputSchema.Length);
        batch.Reset();

        while (_skipped < _offset)
        {
            if (!await _source.MoveNextAsync(ct))
            {
                _currentBatch = batch;
                return false;
            }

            _skipped++;
        }

        while (batch.Count < batch.Capacity && await _source.MoveNextAsync(ct))
            batch.AppendRow(_source.Current);

        _currentBatch = batch;
        return batch.Count > 0;
    }

    bool IBatchOperator.ReusesCurrentBatch => _reuseCurrentBatch;
    RowBatch IBatchOperator.CurrentBatch => _currentBatch;

    public void SetReuseCurrentBatch(bool reuse)
    {
        _reuseCurrentBatch = reuse;
        if (!reuse)
            _currentBatch = CreateBatch(_source.OutputSchema.Length);
    }

    private async ValueTask<bool> MoveNextBatchFromBatchSourceAsync(IBatchOperator batchSource, CancellationToken ct)
    {
        RowBatch sourceBatch = batchSource.CurrentBatch;
        int columnCount = sourceBatch.ColumnCount;
        var batch = _reuseCurrentBatch ? EnsureBatch(columnCount) : CreateBatch(columnCount);
        batch.Reset();

        while (batch.Count < batch.Capacity)
        {
            while (_sourceBatchRowIndex + 1 < sourceBatch.Count && batch.Count < batch.Capacity)
            {
                _sourceBatchRowIndex++;
                if (_skipped < _offset)
                {
                    _skipped++;
                    continue;
                }

                int targetRowIndex = batch.Count;
                sourceBatch.GetRowSpan(_sourceBatchRowIndex).CopyTo(batch.GetWritableRowSpan(targetRowIndex));
                batch.CommitWrittenRow(targetRowIndex);
            }

            if (batch.Count >= batch.Capacity)
                break;

            if (!await batchSource.MoveNextBatchAsync(ct))
                break;

            sourceBatch = batchSource.CurrentBatch;
            columnCount = sourceBatch.ColumnCount;
            if (batch.ColumnCount != columnCount)
            {
                batch = _reuseCurrentBatch ? EnsureBatch(columnCount) : CreateBatch(columnCount);
                batch.Reset();
            }

            _sourceBatchRowIndex = -1;
        }

        _currentBatch = batch;
        return batch.Count > 0;
    }

    private RowBatch EnsureBatch(int columnCount)
    {
        if (_currentBatch.ColumnCount != columnCount)
            _currentBatch = CreateBatch(columnCount);

        return _currentBatch;
    }

    private static RowBatch CreateBatch(int columnCount) => new(columnCount, DefaultBatchSize);
}

/// <summary>
/// Limit operator — caps the number of output rows.
/// </summary>
public sealed class LimitOperator : IOperator, IBatchOperator, IRowBufferReuseController, IBatchBufferReuseController, IEstimatedRowCountProvider, IUnaryOperatorSource
{
    private const int DefaultBatchSize = 64;
    private const int SmallLimitRowModeThreshold = 4;

    private readonly IOperator _source;
    private readonly int _limit;
    private int _count;
    private bool _reuseCurrentBatch = true;
    private RowBatch _currentBatch;
    private int _sourceBatchRowIndex = -1;

    public ColumnDefinition[] OutputSchema => _source.OutputSchema;
    public bool ReusesCurrentRowBuffer => _source.ReusesCurrentRowBuffer;
    public DbValue[] Current => _source.Current;
    public int? EstimatedRowCount => _limit >= 0 ? _limit : 0;
    IOperator IUnaryOperatorSource.Source => _source;

    public LimitOperator(IOperator source, int limit)
    {
        _source = source;
        _limit = limit;
        _currentBatch = CreateBatch(source.OutputSchema.Length);
    }

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        _count = 0;
        _currentBatch = CreateBatch(_source.OutputSchema.Length);
        _sourceBatchRowIndex = -1;
        await _source.OpenAsync(ct);
    }

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (_count >= _limit) return false;
        if (!await _source.MoveNextAsync(ct)) return false;
        _count++;
        return true;
    }

    public ValueTask DisposeAsync() => _source.DisposeAsync();

    public void SetReuseCurrentRowBuffer(bool reuse)
    {
        if (_source is IRowBufferReuseController controller)
            controller.SetReuseCurrentRowBuffer(reuse);
    }

    public async ValueTask<bool> MoveNextBatchAsync(CancellationToken ct = default)
    {
        if (_count >= _limit)
            return false;

        int remaining = _limit - _count;
        if (remaining <= SmallLimitRowModeThreshold && _sourceBatchRowIndex < 0)
        {
            var smallBatch = _reuseCurrentBatch ? EnsureBatch(_source.OutputSchema.Length) : CreateBatch(_source.OutputSchema.Length);
            smallBatch.Reset();

            while (smallBatch.Count < remaining && _count < _limit && await _source.MoveNextAsync(ct))
            {
                smallBatch.AppendRow(_source.Current);
                _count++;
            }

            _currentBatch = smallBatch;
            return smallBatch.Count > 0;
        }

        var batchSource = BatchSourceHelper.TryGetBatchSource(_source);
        if (batchSource != null)
            return await MoveNextBatchFromBatchSourceAsync(batchSource, ct);

        var batch = _reuseCurrentBatch ? EnsureBatch(_source.OutputSchema.Length) : CreateBatch(_source.OutputSchema.Length);
        batch.Reset();

        while (batch.Count < batch.Capacity && _count < _limit && await _source.MoveNextAsync(ct))
        {
            batch.AppendRow(_source.Current);
            _count++;
        }

        _currentBatch = batch;
        return batch.Count > 0;
    }

    bool IBatchOperator.ReusesCurrentBatch => _reuseCurrentBatch;
    RowBatch IBatchOperator.CurrentBatch => _currentBatch;

    public void SetReuseCurrentBatch(bool reuse)
    {
        _reuseCurrentBatch = reuse;
        if (!reuse)
            _currentBatch = CreateBatch(_source.OutputSchema.Length);
    }

    private async ValueTask<bool> MoveNextBatchFromBatchSourceAsync(IBatchOperator batchSource, CancellationToken ct)
    {
        RowBatch sourceBatch = batchSource.CurrentBatch;
        int columnCount = sourceBatch.ColumnCount;
        var batch = _reuseCurrentBatch ? EnsureBatch(columnCount) : CreateBatch(columnCount);
        batch.Reset();

        while (batch.Count < batch.Capacity && _count < _limit)
        {
            while (_sourceBatchRowIndex + 1 < sourceBatch.Count && batch.Count < batch.Capacity && _count < _limit)
            {
                _sourceBatchRowIndex++;
                int targetRowIndex = batch.Count;
                sourceBatch.GetRowSpan(_sourceBatchRowIndex).CopyTo(batch.GetWritableRowSpan(targetRowIndex));
                batch.CommitWrittenRow(targetRowIndex);
                _count++;
            }

            if (batch.Count >= batch.Capacity || _count >= _limit)
                break;

            int remaining = _limit - _count;
            if (batch.Count == 0 &&
                _sourceBatchRowIndex < 0 &&
                sourceBatch.Count > 0 &&
                sourceBatch.Count <= remaining &&
                (_reuseCurrentBatch || !batchSource.ReusesCurrentBatch))
            {
                _currentBatch = sourceBatch;
                _count += sourceBatch.Count;
                _sourceBatchRowIndex = sourceBatch.Count - 1;
                return true;
            }

            if (!await batchSource.MoveNextBatchAsync(ct))
                break;

            sourceBatch = batchSource.CurrentBatch;
            columnCount = sourceBatch.ColumnCount;
            if (batch.ColumnCount != columnCount)
            {
                batch = _reuseCurrentBatch ? EnsureBatch(columnCount) : CreateBatch(columnCount);
                batch.Reset();
            }

            _sourceBatchRowIndex = -1;
        }

        _currentBatch = batch;
        return batch.Count > 0;
    }

    private RowBatch EnsureBatch(int columnCount)
    {
        if (_currentBatch.ColumnCount != columnCount)
            _currentBatch = CreateBatch(columnCount);

        return _currentBatch;
    }

    private static RowBatch CreateBatch(int columnCount) => new(columnCount, DefaultBatchSize);
}

/// <summary>
/// Tracks DISTINCT aggregate argument values with a compact integer fast path.
/// </summary>
internal sealed class AggregateDistinctValueSet
{
    // Bitmap covers [0, 1_048_575] and stays compact (~128 KB).
    private const int IntegerBitmapMaxValue = (1 << 20) - 1;
    private const int IntegerBitmapWordCount = (IntegerBitmapMaxValue + 64) / 64;
    private const int BitmapPromotionDistinctCount = 1_024;

    private ulong[]? _integerBitmap;
    private HashSet<long>? _integerValues;
    private HashSet<DbValue>? _values;

    public bool Add(DbValue value)
    {
        if (value.Type == DbType.Integer)
            return AddInteger(value.AsInteger);

        EnsureGeneralSet();
        return _values!.Add(value);
    }

    public bool AddInteger(long value)
    {
        if (_values != null)
            return _values.Add(DbValue.FromInteger(value));

        if (_integerBitmap != null)
        {
            bool? bitmapAdded = TryAddToBitmap(value);
            if (bitmapAdded.HasValue)
                return bitmapAdded.Value;

            EnsureIntegerSet();
            return _integerValues!.Add(value);
        }

        _integerValues ??= new HashSet<long>();
        bool added = _integerValues.Add(value);
        if (!added)
            return false;

        if (_integerValues.Count == BitmapPromotionDistinctCount && CanPromoteToBitmap())
            PromoteIntegerSetToBitmap();

        return true;
    }

    public bool AddNumeric(long intValue, double realValue, bool isReal)
        => isReal ? Add(DbValue.FromReal(realValue)) : AddInteger(intValue);

    private bool? TryAddToBitmap(long value)
    {
        if (value < 0 || value > IntegerBitmapMaxValue)
            return null;

        _integerBitmap ??= new ulong[IntegerBitmapWordCount];
        int bitIndex = (int)value;
        int wordIndex = bitIndex >> 6;
        ulong mask = 1UL << (bitIndex & 63);

        ref ulong word = ref _integerBitmap[wordIndex];
        if ((word & mask) != 0)
            return false;

        word |= mask;
        return true;
    }

    private void EnsureIntegerSet()
    {
        if (_integerValues != null)
            return;

        _integerValues = new HashSet<long>();
        if (_integerBitmap == null)
            return;

        for (int wordIndex = 0; wordIndex < _integerBitmap.Length; wordIndex++)
        {
            ulong word = _integerBitmap[wordIndex];
            while (word != 0)
            {
                int bit = System.Numerics.BitOperations.TrailingZeroCount(word);
                _integerValues.Add((wordIndex << 6) + bit);
                word &= word - 1;
            }
        }

        _integerBitmap = null;
    }

    private bool CanPromoteToBitmap()
    {
        return _integerValues != null
            && _integerValues.Count >= BitmapPromotionDistinctCount
            && _integerValues.All(static value => value >= 0 && value <= IntegerBitmapMaxValue);
    }

    private void PromoteIntegerSetToBitmap()
    {
        _integerBitmap = new ulong[IntegerBitmapWordCount];
        foreach (long integerValue in _integerValues!)
        {
            int bitIndex = (int)integerValue;
            _integerBitmap[bitIndex >> 6] |= 1UL << (bitIndex & 63);
        }

        _integerValues = null;
    }

    private void EnsureGeneralSet()
    {
        if (_values != null)
            return;

        _values = _integerValues != null
            ? new HashSet<DbValue>(_integerValues.Count)
            : new HashSet<DbValue>();

        if (_integerValues != null)
        {
            foreach (long integerValue in _integerValues)
                _values.Add(DbValue.FromInteger(integerValue));
        }
        else if (_integerBitmap != null)
        {
            for (int wordIndex = 0; wordIndex < _integerBitmap.Length; wordIndex++)
            {
                ulong word = _integerBitmap[wordIndex];
                while (word != 0)
                {
                    int bit = System.Numerics.BitOperations.TrailingZeroCount(word);
                    _values.Add(DbValue.FromInteger((wordIndex << 6) + bit));
                    word &= word - 1;
                }
            }
        }

        _integerValues = null;
        _integerBitmap = null;
    }
}

/// <summary>
/// Hash aggregate operator — groups rows and computes aggregate functions.
/// Used for GROUP BY and queries with aggregate functions (COUNT, SUM, AVG, MIN, MAX).
/// </summary>
public sealed class HashAggregateOperator : IOperator, IEstimatedRowCountProvider, IMaterializedRowsProvider
{
    private enum SimpleGroupedBatchAggregateKind
    {
        CountStar,
        CountValue,
        Sum,
        Avg,
        Min,
        Max,
    }

    private readonly struct SimpleGroupedBatchAggregateTerm
    {
        public SimpleGroupedBatchAggregateTerm(SimpleGroupedBatchAggregateKind kind, int columnIndex, bool isDistinct)
        {
            Kind = kind;
            ColumnIndex = columnIndex;
            IsDistinct = isDistinct;
        }

        public SimpleGroupedBatchAggregateKind Kind { get; }
        public int ColumnIndex { get; }
        public bool IsDistinct { get; }
    }

    private sealed class SimpleGroupedBatchPlan
    {
        public SimpleGroupedBatchPlan(int[] groupColumnIndices, SimpleGroupedBatchAggregateTerm[] aggregateTerms)
        {
            GroupColumnIndices = groupColumnIndices;
            AggregateTerms = aggregateTerms;
        }

        public int[] GroupColumnIndices { get; }
        public SimpleGroupedBatchAggregateTerm[] AggregateTerms { get; }
    }

    private sealed class SimpleGroupedBatchKeyPlan
    {
        public SimpleGroupedBatchKeyPlan(BatchProjectionTerm[] groupKeyTerms)
        {
            GroupKeyTerms = groupKeyTerms;
        }

        public BatchProjectionTerm[] GroupKeyTerms { get; }
    }

    private readonly IOperator _source;
    private readonly List<SelectColumn> _selectColumns;
    private readonly List<Expression>? _groupByExprs;
    private readonly Expression? _havingExpr;
    private readonly TableSchema _inputSchema;
    private readonly List<FunctionCallExpression> _aggregateFunctions = new();
    private readonly Dictionary<FunctionCallExpression, int> _aggregateIndices = new();
    private readonly SpanExpressionEvaluator[]? _groupByEvaluators;
    private readonly bool _groupByIsConstant;
    private readonly SimpleGroupedBatchKeyPlan? _simpleGroupedBatchKeyPlan;
    private readonly SimpleGroupedBatchPlan? _simpleGroupedBatchPlan;
    private List<DbValue[]>? _results;
    private int _index;
    private DbValue[]? _batchRowBuffer;

    private static readonly GroupKeyComparer s_groupKeyComparer = new();

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => false;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
    public int? EstimatedRowCount => _results?.Count;

    public HashAggregateOperator(
        IOperator source,
        List<SelectColumn> selectColumns,
        List<Expression>? groupByExprs,
        Expression? havingExpr,
        TableSchema inputSchema,
        ColumnDefinition[] outputSchema)
    {
        _source = source;
        _selectColumns = selectColumns;
        _groupByExprs = groupByExprs;
        _havingExpr = havingExpr;
        _inputSchema = inputSchema;
        OutputSchema = outputSchema;
        if (_groupByExprs is { Count: > 0 })
        {
            _groupByEvaluators = BuildGroupByEvaluators(_groupByExprs, _inputSchema);
            _groupByIsConstant = _groupByExprs.All(e => e is LiteralExpression);
        }

        foreach (var col in _selectColumns)
        {
            if (col.Expression != null)
                CollectAggregates(col.Expression);
        }

        if (_havingExpr != null)
            CollectAggregates(_havingExpr);

        _simpleGroupedBatchKeyPlan = TryCreateSimpleGroupedBatchKeyPlan();
        _simpleGroupedBatchPlan = TryCreateSimpleGroupedBatchPlan();
    }

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        await _source.OpenAsync(ct);

        _results = new List<DbValue[]>();
        _batchRowBuffer = null;
        var batchSource = BatchSourceHelper.TryGetBatchSource(_source);
        bool hasGroupBy = _groupByExprs is { Count: > 0 };
        if (hasGroupBy)
        {
            if (_groupByIsConstant)
            {
                GroupState? singleGroup = null;
                await ConsumeSourceRowsAsync(row =>
                {
                    if (singleGroup == null)
                    {
                        singleGroup = new GroupState(
                            firstRow: (DbValue[])row.Clone(),
                            aggregateStates: CreateAggregateStates());
                    }
                    singleGroup.Accumulate(row);
                }, ct);

                if (singleGroup != null)
                    EmitGroupResult(singleGroup);
            }
            else if (CanUseSimpleGroupedBatchPlan())
            {
                foreach (var group in await BuildSimpleBatchGroupsAsync(ct))
                    EmitGroupResult(group);
            }
            else if (CanUseSimpleGroupedBatchKeyPlan())
            {
                foreach (var group in await BuildSimpleBatchKeyGroupsAsync(ct))
                    EmitGroupResult(group);
            }
            else if (batchSource != null)
            {
                foreach (var group in await BuildGenericBatchGroupsAsync(batchSource, ct))
                    EmitGroupResult(group);
            }
            else
            {
                // Stream rows into group accumulators; preserve first-seen group order.
                var groups = new List<GroupState>();
                if (_groupByEvaluators is { Length: 1 })
                {
                    var groupByEvaluator = _groupByEvaluators[0];
                    var groupIndex = new Dictionary<DbValue, int>();
                    await ConsumeSourceRowsAsync(row =>
                    {
                        var key = groupByEvaluator(row);
                        if (!groupIndex.TryGetValue(key, out int idx))
                        {
                            idx = groups.Count;
                            groupIndex[key] = idx;
                            groups.Add(new GroupState(
                                firstRow: (DbValue[])row.Clone(),
                                aggregateStates: CreateAggregateStates()));
                        }

                        groups[idx].Accumulate(row);
                    }, ct);
                }
                else
                {
                    var groupIndex = new Dictionary<GroupKey, int>(s_groupKeyComparer);
                    await ConsumeSourceRowsAsync(row =>
                    {
                        var key = BuildGroupKey(row);
                        if (!groupIndex.TryGetValue(key, out int idx))
                        {
                            idx = groups.Count;
                            groupIndex[key] = idx;
                            groups.Add(new GroupState(
                                firstRow: (DbValue[])row.Clone(),
                                aggregateStates: CreateAggregateStates()));
                        }

                        groups[idx].Accumulate(row);
                    }, ct);
                }

                foreach (var group in groups)
                    EmitGroupResult(group);
            }
        }
        else
        {
            // No GROUP BY: aggregate entire table as one implicit group.
            var singleGroup = new GroupState(firstRow: null, aggregateStates: CreateAggregateStates());
            await ConsumeSourceRowsAsync(row =>
            {
                singleGroup.FirstRow ??= (DbValue[])row.Clone();
                singleGroup.Accumulate(row);
            }, ct);

            EmitGroupResult(singleGroup);
        }

        _index = -1;
    }

    public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        _index++;
        if (_index >= _results!.Count) return ValueTask.FromResult(false);
        Current = _results[_index];
        return ValueTask.FromResult(true);
    }

    public bool TryTakeMaterializedRows(out List<DbValue[]> rows)
    {
        if (_results == null)
        {
            rows = new List<DbValue[]>(0);
            return false;
        }

        rows = _results;
        _results = null;
        _index = -1;
        Current = Array.Empty<DbValue>();
        return true;
    }

    public ValueTask DisposeAsync() => _source.DisposeAsync();

    private bool CanUseSimpleGroupedBatchPlan()
        => _simpleGroupedBatchPlan != null &&
           BatchSourceHelper.TryGetBatchSource(_source) != null;

    private bool CanUseSimpleGroupedBatchKeyPlan()
        => _simpleGroupedBatchKeyPlan != null &&
           BatchSourceHelper.TryGetBatchSource(_source) != null;

    private async ValueTask<List<GroupState>> BuildSimpleBatchGroupsAsync(CancellationToken ct)
    {
        var batchSource = BatchSourceHelper.TryGetBatchSource(_source)
            ?? throw new InvalidOperationException("Batch source is required for grouped batch plan.");
        var plan = _simpleGroupedBatchPlan
            ?? throw new InvalidOperationException("Grouped batch plan was not created.");

        var groups = new List<GroupState>();
        if (plan.GroupColumnIndices.Length == 1)
        {
            var groupIndex = new Dictionary<DbValue, int>();
            int groupColumnIndex = plan.GroupColumnIndices[0];

            while (await batchSource.MoveNextBatchAsync(ct))
            {
                var batch = batchSource.CurrentBatch;
                for (int rowIndex = 0; rowIndex < batch.Count; rowIndex++)
                {
                    var row = batch.GetRowSpan(rowIndex);
                    DbValue key = row[groupColumnIndex];
                    if (!groupIndex.TryGetValue(key, out int index))
                    {
                        index = groups.Count;
                        groupIndex[key] = index;
                        groups.Add(new GroupState(
                            firstRow: row.ToArray(),
                            aggregateStates: CreateAggregateStates()));
                    }

                    groups[index].Accumulate(row, plan.AggregateTerms);
                }
            }
        }
        else
        {
            var groupIndex = new Dictionary<GroupKey, int>(s_groupKeyComparer);

            while (await batchSource.MoveNextBatchAsync(ct))
            {
                var batch = batchSource.CurrentBatch;
                for (int rowIndex = 0; rowIndex < batch.Count; rowIndex++)
                {
                    var row = batch.GetRowSpan(rowIndex);
                    var key = BuildSimpleBatchGroupKey(row, plan.GroupColumnIndices);
                    if (!groupIndex.TryGetValue(key, out int index))
                    {
                        index = groups.Count;
                        groupIndex[key] = index;
                        groups.Add(new GroupState(
                            firstRow: row.ToArray(),
                            aggregateStates: CreateAggregateStates()));
                    }

                    groups[index].Accumulate(row, plan.AggregateTerms);
                }
            }
        }

        return groups;
    }

    private async ValueTask<List<GroupState>> BuildGenericBatchGroupsAsync(IBatchOperator batchSource, CancellationToken ct)
    {
        var groups = new List<GroupState>();
        if (_groupByEvaluators is { Length: 1 })
        {
            var groupByEvaluator = _groupByEvaluators[0];
            var groupIndex = new Dictionary<DbValue, int>();

            while (await batchSource.MoveNextBatchAsync(ct))
            {
                var batch = batchSource.CurrentBatch;
                for (int rowIndex = 0; rowIndex < batch.Count; rowIndex++)
                {
                    var row = batch.GetRowSpan(rowIndex);
                    var key = groupByEvaluator(row);
                    if (!groupIndex.TryGetValue(key, out int idx))
                    {
                        idx = groups.Count;
                        groupIndex[key] = idx;
                        groups.Add(new GroupState(
                            firstRow: row.ToArray(),
                            aggregateStates: CreateAggregateStates()));
                    }

                    groups[idx].Accumulate(row, ref _batchRowBuffer);
                }
            }

            return groups;
        }

        var multiColumnGroupIndex = new Dictionary<GroupKey, int>(s_groupKeyComparer);
        while (await batchSource.MoveNextBatchAsync(ct))
        {
            var batch = batchSource.CurrentBatch;
            for (int rowIndex = 0; rowIndex < batch.Count; rowIndex++)
            {
                var row = batch.GetRowSpan(rowIndex);
                var key = BuildGroupKey(row);
                if (!multiColumnGroupIndex.TryGetValue(key, out int idx))
                {
                    idx = groups.Count;
                    multiColumnGroupIndex[key] = idx;
                    groups.Add(new GroupState(
                        firstRow: row.ToArray(),
                        aggregateStates: CreateAggregateStates()));
                }

                groups[idx].Accumulate(row, ref _batchRowBuffer);
            }
        }

        return groups;
    }

    private async ValueTask<List<GroupState>> BuildSimpleBatchKeyGroupsAsync(CancellationToken ct)
    {
        var batchSource = BatchSourceHelper.TryGetBatchSource(_source)
            ?? throw new InvalidOperationException("Batch source is required for grouped batch key plan.");
        var plan = _simpleGroupedBatchKeyPlan
            ?? throw new InvalidOperationException("Grouped batch key plan was not created.");

        var groups = new List<GroupState>();
        if (plan.GroupKeyTerms.Length == 1)
        {
            var groupIndex = new Dictionary<DbValue, int>();
            var groupKeyTerm = plan.GroupKeyTerms[0];

            while (await batchSource.MoveNextBatchAsync(ct))
            {
                var batch = batchSource.CurrentBatch;
                for (int rowIndex = 0; rowIndex < batch.Count; rowIndex++)
                {
                    var row = batch.GetRowSpan(rowIndex);
                    DbValue key = groupKeyTerm.Evaluate(row);
                    if (!groupIndex.TryGetValue(key, out int index))
                    {
                        index = groups.Count;
                        groupIndex[key] = index;
                        groups.Add(new GroupState(
                            firstRow: row.ToArray(),
                            aggregateStates: CreateAggregateStates()));
                    }

                    groups[index].Accumulate(row, ref _batchRowBuffer);
                }
            }
        }
        else
        {
            var groupIndex = new Dictionary<GroupKey, int>(s_groupKeyComparer);

            while (await batchSource.MoveNextBatchAsync(ct))
            {
                var batch = batchSource.CurrentBatch;
                for (int rowIndex = 0; rowIndex < batch.Count; rowIndex++)
                {
                    var row = batch.GetRowSpan(rowIndex);
                    var key = BuildSimpleBatchGroupKey(row, plan.GroupKeyTerms);
                    if (!groupIndex.TryGetValue(key, out int index))
                    {
                        index = groups.Count;
                        groupIndex[key] = index;
                        groups.Add(new GroupState(
                            firstRow: row.ToArray(),
                            aggregateStates: CreateAggregateStates()));
                    }

                    groups[index].Accumulate(row, ref _batchRowBuffer);
                }
            }
        }

        return groups;
    }

    private async ValueTask ConsumeSourceRowsAsync(Action<DbValue[]> rowAction, CancellationToken ct)
    {
        var batchSource = BatchSourceHelper.TryGetBatchSource(_source);
        if (batchSource == null)
        {
            while (await _source.MoveNextAsync(ct))
                rowAction(_source.Current);

            return;
        }

        while (await batchSource.MoveNextBatchAsync(ct))
        {
            var batch = batchSource.CurrentBatch;
            var rowBuffer = EnsureBatchRowBuffer(batch.ColumnCount);
            for (int rowIndex = 0; rowIndex < batch.Count; rowIndex++)
            {
                batch.CopyRowTo(rowIndex, rowBuffer);
                rowAction(rowBuffer);
            }
        }
    }

    private DbValue[] EnsureBatchRowBuffer(int columnCount)
    {
        if (_batchRowBuffer == null || _batchRowBuffer.Length != columnCount)
            _batchRowBuffer = columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];

        return _batchRowBuffer;
    }

    private SimpleGroupedBatchKeyPlan? TryCreateSimpleGroupedBatchKeyPlan()
    {
        if (BatchSourceHelper.TryGetBatchSource(_source) == null ||
            _groupByExprs is not { Count: > 0 })
        {
            return null;
        }

        var keyTerms = BatchPlanCompiler.TryBindProjectionTerms(_groupByExprs, _inputSchema);
        return keyTerms == null ? null : new SimpleGroupedBatchKeyPlan(keyTerms);
    }

    private SimpleGroupedBatchPlan? TryCreateSimpleGroupedBatchPlan()
    {
        if (BatchSourceHelper.TryGetBatchSource(_source) == null)
            return null;

        if (_groupByExprs is not { Count: > 0 })
        {
            return null;
        }

        var groupColumnIndices = new int[_groupByExprs.Count];
        for (int i = 0; i < _groupByExprs.Count; i++)
        {
            if (_groupByExprs[i] is not ColumnRefExpression groupColumnRef ||
                !TryResolveColumnIndex(groupColumnRef, _inputSchema, out groupColumnIndices[i]))
            {
                return null;
            }
        }

        var aggregateTerms = new SimpleGroupedBatchAggregateTerm[_aggregateFunctions.Count];
        for (int i = 0; i < _aggregateFunctions.Count; i++)
        {
            if (!TryBindSimpleGroupedBatchAggregateTerm(_aggregateFunctions[i], out aggregateTerms[i]))
                return null;
        }

        return new SimpleGroupedBatchPlan(groupColumnIndices, aggregateTerms);
    }

    private bool TryBindSimpleGroupedBatchAggregateTerm(
        FunctionCallExpression func,
        out SimpleGroupedBatchAggregateTerm term)
    {
        term = default;

        if (func.IsStarArg)
        {
            if (func.FunctionName != "COUNT")
                return false;

            term = new SimpleGroupedBatchAggregateTerm(SimpleGroupedBatchAggregateKind.CountStar, -1, isDistinct: false);
            return true;
        }

        if (func.Arguments.Count != 1 ||
            func.Arguments[0] is not ColumnRefExpression argumentColumn ||
            !TryResolveColumnIndex(argumentColumn, _inputSchema, out int columnIndex))
        {
            return false;
        }

        var columnType = _inputSchema.Columns[columnIndex].Type;
        switch (func.FunctionName)
        {
            case "COUNT":
                term = new SimpleGroupedBatchAggregateTerm(SimpleGroupedBatchAggregateKind.CountValue, columnIndex, func.IsDistinct);
                return true;
            case "SUM":
                if (columnType is not DbType.Integer and not DbType.Real)
                    return false;
                term = new SimpleGroupedBatchAggregateTerm(SimpleGroupedBatchAggregateKind.Sum, columnIndex, func.IsDistinct);
                return true;
            case "AVG":
                if (columnType is not DbType.Integer and not DbType.Real)
                    return false;
                term = new SimpleGroupedBatchAggregateTerm(SimpleGroupedBatchAggregateKind.Avg, columnIndex, func.IsDistinct);
                return true;
            case "MIN":
                term = new SimpleGroupedBatchAggregateTerm(SimpleGroupedBatchAggregateKind.Min, columnIndex, func.IsDistinct);
                return true;
            case "MAX":
                term = new SimpleGroupedBatchAggregateTerm(SimpleGroupedBatchAggregateKind.Max, columnIndex, func.IsDistinct);
                return true;
            default:
                return false;
        }
    }

    private static bool TryResolveColumnIndex(ColumnRefExpression column, TableSchema schema, out int columnIndex)
    {
        columnIndex = column.TableAlias != null
            ? schema.GetQualifiedColumnIndex(column.TableAlias, column.ColumnName)
            : schema.GetColumnIndex(column.ColumnName);
        return columnIndex >= 0;
    }

    private static GroupKey BuildSimpleBatchGroupKey(ReadOnlySpan<DbValue> row, int[] groupColumnIndices)
    {
        var values = new DbValue[groupColumnIndices.Length];
        var hash = new HashCode();
        for (int i = 0; i < groupColumnIndices.Length; i++)
        {
            DbValue value = (uint)groupColumnIndices[i] < (uint)row.Length
                ? row[groupColumnIndices[i]]
                : DbValue.Null;
            values[i] = value;
            hash.Add(value);
        }

        return new GroupKey(values, hash.ToHashCode());
    }

    private static GroupKey BuildSimpleBatchGroupKey(ReadOnlySpan<DbValue> row, BatchProjectionTerm[] groupKeyTerms)
    {
        var values = new DbValue[groupKeyTerms.Length];
        var hash = new HashCode();
        for (int i = 0; i < groupKeyTerms.Length; i++)
        {
            DbValue value = groupKeyTerms[i].Evaluate(row);
            values[i] = value;
            hash.Add(value);
        }

        return new GroupKey(values, hash.ToHashCode());
    }

    private void CollectAggregates(Expression expr)
    {
        switch (expr)
        {
            case FunctionCallExpression func:
                if (ScalarFunctionEvaluator.IsAggregateFunction(func.FunctionName))
                {
                    if (!_aggregateIndices.ContainsKey(func))
                    {
                        _aggregateIndices.Add(func, _aggregateFunctions.Count);
                        _aggregateFunctions.Add(func);
                    }
                }
                else
                {
                    for (int i = 0; i < func.Arguments.Count; i++)
                        CollectAggregates(func.Arguments[i]);
                }
                break;
            case BinaryExpression bin:
                CollectAggregates(bin.Left);
                CollectAggregates(bin.Right);
                break;
            case UnaryExpression un:
                CollectAggregates(un.Operand);
                break;
            case CollateExpression collate:
                CollectAggregates(collate.Operand);
                break;
        }
    }

    private AggregateState[] CreateAggregateStates()
    {
        var states = new AggregateState[_aggregateFunctions.Count];
        for (int i = 0; i < states.Length; i++)
            states[i] = new AggregateState(_aggregateFunctions[i], _inputSchema);
        return states;
    }

    private void EmitGroupResult(GroupState group)
    {
        var outputRow = new DbValue[_selectColumns.Count];
        for (int i = 0; i < _selectColumns.Count; i++)
        {
            if (_selectColumns[i].IsStar)
                outputRow[i] = DbValue.Null;
            else
                outputRow[i] = EvalWithAggregates(_selectColumns[i].Expression!, group);
        }

        if (_havingExpr != null)
        {
            var havingResult = EvalWithAggregates(_havingExpr, group);
            if (!havingResult.IsTruthy) return;
        }

        _results!.Add(outputRow);
    }

    private GroupKey BuildGroupKey(ReadOnlySpan<DbValue> row)
    {
        if (_groupByEvaluators == null || _groupByEvaluators.Length == 0)
            return GroupKey.Empty;

        var values = new DbValue[_groupByEvaluators.Length];
        var hash = new HashCode();
        for (int i = 0; i < _groupByEvaluators.Length; i++)
        {
            var val = _groupByEvaluators[i](row);
            values[i] = val;
            hash.Add(val);
        }

        return new GroupKey(values, hash.ToHashCode());
    }

    private static SpanExpressionEvaluator[] BuildGroupByEvaluators(List<Expression> expressions, TableSchema schema)
    {
        var evaluators = new SpanExpressionEvaluator[expressions.Count];
        for (int i = 0; i < expressions.Count; i++)
            evaluators[i] = BuildGroupByEvaluator(expressions[i], schema);
        return evaluators;
    }

    private static SpanExpressionEvaluator BuildGroupByEvaluator(Expression expr, TableSchema schema)
    {
        return ExpressionCompiler.CompileSpan(expr, schema);
    }

    private static Func<DbValue[], DbValue> BuildColumnEvaluator(ColumnRefExpression col, TableSchema schema)
    {
        int idx = col.TableAlias != null
            ? schema.GetQualifiedColumnIndex(col.TableAlias, col.ColumnName)
            : schema.GetColumnIndex(col.ColumnName);

        if (idx < 0)
        {
            string colName = col.TableAlias != null
                ? $"{col.TableAlias}.{col.ColumnName}"
                : col.ColumnName;
            throw new CSharpDbException(ErrorCode.ColumnNotFound, $"Column '{colName}' not found.");
        }

        return row => idx < row.Length ? row[idx] : DbValue.Null;
    }

    private static Func<DbValue[], DbValue> BuildLiteralEvaluator(LiteralExpression lit)
    {
        var value = lit.Value == null
            ? DbValue.Null
            : lit.LiteralType switch
            {
                TokenType.IntegerLiteral => DbValue.FromInteger((long)lit.Value),
                TokenType.RealLiteral => DbValue.FromReal((double)lit.Value),
                TokenType.StringLiteral => DbValue.FromText((string)lit.Value),
                TokenType.Null => DbValue.Null,
                _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unknown literal type: {lit.LiteralType}"),
            };

        return _ => value;
    }

    /// <summary>
    /// Evaluates an expression tree, replacing aggregate function calls with computed results.
    /// Non-aggregate expressions are evaluated against the first row of the group.
    /// </summary>
    private DbValue EvalWithAggregates(Expression expr, GroupState group)
    {
        return expr switch
        {
            FunctionCallExpression func => ScalarFunctionEvaluator.IsAggregateFunction(func.FunctionName)
                ? EvaluateAggregate(func, group)
                : ScalarFunctionEvaluator.Evaluate(func, arg => EvalWithAggregates(arg, group)),
            BinaryExpression bin => EvalBinaryWithAgg(bin, group),
            UnaryExpression un => EvalUnaryWithAgg(un, group),
            CollateExpression collate => EvalWithAggregates(collate.Operand, group),
            _ => group.FirstRow != null
                ? ExpressionEvaluator.Evaluate(expr, group.FirstRow, _inputSchema)
                : DbValue.Null,
        };
    }

    private DbValue EvaluateAggregate(FunctionCallExpression func, GroupState group)
    {
        if (_aggregateIndices.TryGetValue(func, out int idx))
            return group.AggregateStates[idx].GetFinalValue();

        throw new CSharpDbException(ErrorCode.Unknown, $"Unknown aggregate function: {func.FunctionName}");
    }

    private DbValue EvalBinaryWithAgg(BinaryExpression bin, GroupState group)
    {
        var left = EvalWithAggregates(bin.Left, group);
        var right = EvalWithAggregates(bin.Right, group);

        return bin.Op switch
        {
            BinaryOp.Equals => BoolToDb(DbValue.Compare(left, right) == 0),
            BinaryOp.NotEquals => BoolToDb(DbValue.Compare(left, right) != 0),
            BinaryOp.LessThan => BoolToDb(DbValue.Compare(left, right) < 0),
            BinaryOp.GreaterThan => BoolToDb(DbValue.Compare(left, right) > 0),
            BinaryOp.LessOrEqual => BoolToDb(DbValue.Compare(left, right) <= 0),
            BinaryOp.GreaterOrEqual => BoolToDb(DbValue.Compare(left, right) >= 0),
            BinaryOp.And => BoolToDb(left.IsTruthy && right.IsTruthy),
            BinaryOp.Or => BoolToDb(left.IsTruthy || right.IsTruthy),
            BinaryOp.Plus => ArithOp(left, right, (a, b) => a + b, (a, b) => a + b),
            BinaryOp.Minus => ArithOp(left, right, (a, b) => a - b, (a, b) => a - b),
            BinaryOp.Multiply => ArithOp(left, right, (a, b) => a * b, (a, b) => a * b),
            BinaryOp.Divide => ArithOp(left, right, (a, b) => b != 0 ? a / b : 0, (a, b) => b != 0 ? a / b : 0),
            _ => DbValue.Null,
        };
    }

    private DbValue EvalUnaryWithAgg(UnaryExpression un, GroupState group)
    {
        var operand = EvalWithAggregates(un.Operand, group);
        return un.Op switch
        {
            TokenType.Not => BoolToDb(!operand.IsTruthy),
            TokenType.Minus when operand.Type == DbType.Integer => DbValue.FromInteger(-operand.AsInteger),
            TokenType.Minus when operand.Type == DbType.Real => DbValue.FromReal(-operand.AsReal),
            _ => DbValue.Null,
        };
    }

    private static DbValue BoolToDb(bool v) => DbValue.FromInteger(v ? 1 : 0);

    private static DbValue ArithOp(DbValue l, DbValue r, Func<long, long, long> intOp, Func<double, double, double> realOp)
    {
        if (l.IsNull || r.IsNull) return DbValue.Null;
        if (l.Type == DbType.Real || r.Type == DbType.Real) return DbValue.FromReal(realOp(l.AsReal, r.AsReal));
        if (l.Type == DbType.Integer && r.Type == DbType.Integer) return DbValue.FromInteger(intOp(l.AsInteger, r.AsInteger));
        return DbValue.Null;
    }

    private sealed class GroupState
    {
        public DbValue[]? FirstRow { get; set; }
        public AggregateState[] AggregateStates { get; }

        public GroupState(DbValue[]? firstRow, AggregateState[] aggregateStates)
        {
            FirstRow = firstRow;
            AggregateStates = aggregateStates;
        }

        public void Accumulate(DbValue[] row)
        {
            for (int i = 0; i < AggregateStates.Length; i++)
                AggregateStates[i].Accumulate(row);
        }

        public void Accumulate(ReadOnlySpan<DbValue> row, SimpleGroupedBatchAggregateTerm[] terms)
        {
            for (int i = 0; i < AggregateStates.Length; i++)
                AggregateStates[i].Accumulate(row, terms[i]);
        }

        public void Accumulate(ReadOnlySpan<DbValue> row, ref DbValue[]? rowBuffer)
        {
            for (int i = 0; i < AggregateStates.Length; i++)
                AggregateStates[i].Accumulate(row, ref rowBuffer);
        }
    }

    private readonly struct GroupKey
    {
        public static readonly GroupKey Empty = new(Array.Empty<DbValue>(), 0);

        public DbValue[] Values { get; }
        public int HashCode { get; }

        public GroupKey(DbValue[] values, int hashCode)
        {
            Values = values;
            HashCode = hashCode;
        }
    }

    private sealed class GroupKeyComparer : IEqualityComparer<GroupKey>
    {
        public bool Equals(GroupKey x, GroupKey y)
        {
            if (x.HashCode != y.HashCode) return false;
            if (x.Values.Length != y.Values.Length) return false;

            for (int i = 0; i < x.Values.Length; i++)
            {
                if (!x.Values[i].Equals(y.Values[i]))
                    return false;
            }
            return true;
        }

        public int GetHashCode(GroupKey obj) => obj.HashCode;
    }

    private sealed class AggregateState
    {
        private readonly string _name;
        private readonly SpanExpressionEvaluator? _argumentEvaluator;
        private readonly bool _isDistinct;
        private readonly bool _isStarArg;
        private readonly int _directColumnIndex;
        private readonly bool _hasLiteralArgument;
        private readonly DbValue _literalArgument;

        private AggregateDistinctValueSet? _distinctValues;
        private long _count;
        private double _sum;
        private bool _hasReal;
        private bool _hasAny;
        private DbValue? _best;

        public AggregateState(FunctionCallExpression func, TableSchema schema)
        {
            _name = func.FunctionName;
            _argumentEvaluator = BuildAggregateArgumentEvaluator(func, schema);
            _isDistinct = func.IsDistinct;
            _isStarArg = func.IsStarArg;
            _directColumnIndex = TryResolveDirectColumnIndex(func, schema);
            _hasLiteralArgument = TryResolveLiteralArgument(func, out _literalArgument);
            Reset();
        }

        public void Reset()
        {
            _distinctValues = _isDistinct ? new AggregateDistinctValueSet() : null;
            _count = 0;
            _sum = 0;
            _hasReal = false;
            _hasAny = false;
            _best = null;
        }

        public void Accumulate(DbValue[] row)
        {
            if (_name == "COUNT")
            {
                if (_isStarArg)
                {
                    _count++;
                    return;
                }

                var val = _argumentEvaluator!(row);
                if (val.IsNull) return;
                if (_distinctValues != null && !_distinctValues.Add(val)) return;
                _count++;
                return;
            }

            if (_name is "SUM" or "AVG")
            {
                var val = _argumentEvaluator!(row);
                if (val.IsNull) return;
                if (_distinctValues != null && !_distinctValues.Add(val)) return;
                _hasAny = true;
                if (val.Type == DbType.Real) _hasReal = true;
                _sum += val.Type == DbType.Real ? val.AsReal : val.AsInteger;
                _count++;
                return;
            }

            if (_name is "MIN" or "MAX")
            {
                var val = _argumentEvaluator!(row);
                if (val.IsNull) return;

                if (_best == null)
                {
                    _best = val;
                    return;
                }

                int cmp = DbValue.Compare(val, _best.Value);
                if ((_name == "MIN" && cmp < 0) || (_name == "MAX" && cmp > 0))
                    _best = val;
                return;
            }

            throw new CSharpDbException(ErrorCode.Unknown, $"Unknown aggregate function: {_name}");
        }

        public void Accumulate(ReadOnlySpan<DbValue> row, SimpleGroupedBatchAggregateTerm term)
        {
            switch (term.Kind)
            {
                case SimpleGroupedBatchAggregateKind.CountStar:
                    _count++;
                    return;

                case SimpleGroupedBatchAggregateKind.CountValue:
                {
                    DbValue value = GetValue(row, term.ColumnIndex);
                    if (value.IsNull) return;
                    if (term.IsDistinct && _distinctValues != null && !_distinctValues.Add(value)) return;
                    _count++;
                    return;
                }

                case SimpleGroupedBatchAggregateKind.Sum:
                case SimpleGroupedBatchAggregateKind.Avg:
                {
                    DbValue value = GetValue(row, term.ColumnIndex);
                    if (value.IsNull) return;
                    if (term.IsDistinct && _distinctValues != null && !_distinctValues.Add(value)) return;
                    _hasAny = true;
                    if (value.Type == DbType.Real) _hasReal = true;
                    _sum += value.Type == DbType.Real ? value.AsReal : value.AsInteger;
                    _count++;
                    return;
                }

                case SimpleGroupedBatchAggregateKind.Min:
                case SimpleGroupedBatchAggregateKind.Max:
                {
                    DbValue value = GetValue(row, term.ColumnIndex);
                    if (value.IsNull) return;

                    if (_best == null)
                    {
                        _best = value;
                        return;
                    }

                    int cmp = DbValue.Compare(value, _best.Value);
                    if ((term.Kind == SimpleGroupedBatchAggregateKind.Min && cmp < 0) ||
                        (term.Kind == SimpleGroupedBatchAggregateKind.Max && cmp > 0))
                    {
                        _best = value;
                    }

                    return;
                }
            }

            throw new CSharpDbException(ErrorCode.Unknown, $"Unknown aggregate function: {_name}");
        }

        public void Accumulate(ReadOnlySpan<DbValue> row, ref DbValue[]? rowBuffer)
        {
            if (_name == "COUNT")
            {
                if (_isStarArg)
                {
                    _count++;
                    return;
                }

                var val = EvaluateArgument(row, ref rowBuffer);
                if (val.IsNull) return;
                if (_distinctValues != null && !_distinctValues.Add(val)) return;
                _count++;
                return;
            }

            if (_name is "SUM" or "AVG")
            {
                var val = EvaluateArgument(row, ref rowBuffer);
                if (val.IsNull) return;
                if (_distinctValues != null && !_distinctValues.Add(val)) return;
                _hasAny = true;
                if (val.Type == DbType.Real) _hasReal = true;
                _sum += val.Type == DbType.Real ? val.AsReal : val.AsInteger;
                _count++;
                return;
            }

            if (_name is "MIN" or "MAX")
            {
                var val = EvaluateArgument(row, ref rowBuffer);
                if (val.IsNull) return;

                if (_best == null)
                {
                    _best = val;
                    return;
                }

                int cmp = DbValue.Compare(val, _best.Value);
                if ((_name == "MIN" && cmp < 0) || (_name == "MAX" && cmp > 0))
                    _best = val;
                return;
            }

            throw new CSharpDbException(ErrorCode.Unknown, $"Unknown aggregate function: {_name}");
        }

        private DbValue EvaluateArgument(ReadOnlySpan<DbValue> row, ref DbValue[]? rowBuffer)
        {
            if (_directColumnIndex >= 0)
                return (uint)_directColumnIndex < (uint)row.Length ? row[_directColumnIndex] : DbValue.Null;

            if (_hasLiteralArgument)
                return _literalArgument;

            return _argumentEvaluator!(row);
        }

        private static DbValue GetValue(ReadOnlySpan<DbValue> row, int columnIndex)
            => (uint)columnIndex < (uint)row.Length ? row[columnIndex] : DbValue.Null;

        private static SpanExpressionEvaluator? BuildAggregateArgumentEvaluator(FunctionCallExpression func, TableSchema schema)
        {
            if (func.IsStarArg || func.Arguments.Count == 0)
                return null;

            return ExpressionCompiler.CompileSpan(func.Arguments[0], schema);
        }

        private static int TryResolveDirectColumnIndex(FunctionCallExpression func, TableSchema schema)
        {
            if (func.IsStarArg ||
                func.Arguments.Count != 1 ||
                func.Arguments[0] is not ColumnRefExpression col)
            {
                return -1;
            }

            return col.TableAlias != null
                ? schema.GetQualifiedColumnIndex(col.TableAlias, col.ColumnName)
                : schema.GetColumnIndex(col.ColumnName);
        }

        private static bool TryResolveLiteralArgument(FunctionCallExpression func, out DbValue value)
        {
            value = DbValue.Null;

            if (func.IsStarArg ||
                func.Arguments.Count != 1 ||
                func.Arguments[0] is not LiteralExpression lit)
            {
                return false;
            }

            value = lit.Value == null
                ? DbValue.Null
                : lit.LiteralType switch
                {
                    TokenType.IntegerLiteral => DbValue.FromInteger((long)lit.Value),
                    TokenType.RealLiteral => DbValue.FromReal((double)lit.Value),
                    TokenType.StringLiteral => DbValue.FromText((string)lit.Value),
                    TokenType.Null => DbValue.Null,
                    _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unknown literal type: {lit.LiteralType}"),
                };
            return true;
        }

        private static Func<DbValue[], DbValue> BuildColumnEvaluator(ColumnRefExpression col, TableSchema schema)
        {
            int idx = col.TableAlias != null
                ? schema.GetQualifiedColumnIndex(col.TableAlias, col.ColumnName)
                : schema.GetColumnIndex(col.ColumnName);

            if (idx < 0)
            {
                string colName = col.TableAlias != null
                    ? $"{col.TableAlias}.{col.ColumnName}"
                    : col.ColumnName;
                throw new CSharpDbException(ErrorCode.ColumnNotFound, $"Column '{colName}' not found.");
            }

            return row => idx < row.Length ? row[idx] : DbValue.Null;
        }

        private static Func<DbValue[], DbValue> BuildLiteralEvaluator(LiteralExpression lit)
        {
            var value = lit.Value == null
                ? DbValue.Null
                : lit.LiteralType switch
                {
                    TokenType.IntegerLiteral => DbValue.FromInteger((long)lit.Value),
                    TokenType.RealLiteral => DbValue.FromReal((double)lit.Value),
                    TokenType.StringLiteral => DbValue.FromText((string)lit.Value),
                    TokenType.Null => DbValue.Null,
                    _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unknown literal type: {lit.LiteralType}"),
                };

            return _ => value;
        }

        public DbValue GetFinalValue()
        {
            if (_name == "COUNT")
                return DbValue.FromInteger(_count);

            if (_name == "SUM")
            {
                if (!_hasAny) return DbValue.FromInteger(0);
                return _hasReal ? DbValue.FromReal(_sum) : DbValue.FromInteger((long)_sum);
            }

            if (_name == "AVG")
            {
                if (!_hasAny) return DbValue.Null;
                return DbValue.FromReal(_sum / _count);
            }

            if (_name is "MIN" or "MAX")
                return _best ?? DbValue.Null;

            throw new CSharpDbException(ErrorCode.Unknown, $"Unknown aggregate function: {_name}");
        }
    }
}

/// <summary>
/// Scalar aggregate operator — computes aggregate expressions over a single implicit group
/// without materializing all source rows.
/// Used for aggregate queries that do not have GROUP BY.
/// </summary>
public sealed class ScalarAggregateOperator : IOperator, IEstimatedRowCountProvider
{
    private readonly IOperator _source;
    private readonly List<SelectColumn> _selectColumns;
    private readonly Expression? _havingExpr;
    private readonly TableSchema _inputSchema;
    private readonly Dictionary<FunctionCallExpression, AggregateState> _aggregateStates = new();
    private readonly List<FunctionCallExpression> _aggregateFunctions = new();
    private readonly AggregateState[] _aggregateStateList;

    private bool _emitResult;
    private DbValue[]? _firstRow;
    private DbValue[]? _batchRowBuffer;

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => false;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
    public int? EstimatedRowCount => 1;

    public ScalarAggregateOperator(
        IOperator source,
        List<SelectColumn> selectColumns,
        Expression? havingExpr,
        TableSchema inputSchema,
        ColumnDefinition[] outputSchema)
    {
        _source = source;
        _selectColumns = selectColumns;
        _havingExpr = havingExpr;
        _inputSchema = inputSchema;
        OutputSchema = outputSchema;

        foreach (var col in _selectColumns)
        {
            if (col.Expression != null)
                CollectAggregates(col.Expression);
        }

        if (_havingExpr != null)
            CollectAggregates(_havingExpr);

        _aggregateStateList = new AggregateState[_aggregateFunctions.Count];
        for (int i = 0; i < _aggregateFunctions.Count; i++)
        {
            var func = _aggregateFunctions[i];
            var state = new AggregateState(func, _inputSchema);
            _aggregateStateList[i] = state;
            _aggregateStates[func] = state;
        }
    }

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        await _source.OpenAsync(ct);

        for (int i = 0; i < _aggregateStateList.Length; i++)
            _aggregateStateList[i].Reset();

        _firstRow = null;
        _batchRowBuffer = null;
        var batchSource = BatchSourceHelper.TryGetBatchSource(_source);

        if (_aggregateFunctions.Count == 0)
        {
            if (batchSource != null)
            {
                await CaptureFirstBatchRowAsync(batchSource, ct);
            }
            else
            {
                await ConsumeRowSourceRowsAsync(row =>
                {
                    _firstRow ??= (DbValue[])row.Clone();
                }, ct, stopAfterFirst: true);
            }
        }
        else
        {
            if (batchSource != null)
            {
                await AccumulateBatchSourceAsync(batchSource, ct);
            }
            else
            {
                await ConsumeRowSourceRowsAsync(row =>
                {
                    _firstRow ??= (DbValue[])row.Clone();

                    for (int i = 0; i < _aggregateStateList.Length; i++)
                        _aggregateStateList[i].Accumulate(row);
                }, ct);
            }
        }

        _emitResult = true;
        if (_havingExpr != null)
        {
            var havingResult = EvalWithAggregates(_havingExpr, _firstRow);
            if (!havingResult.IsTruthy)
                _emitResult = false;
        }

        if (_emitResult)
        {
            var outputRow = new DbValue[_selectColumns.Count];
            for (int i = 0; i < _selectColumns.Count; i++)
            {
                if (_selectColumns[i].IsStar)
                    outputRow[i] = DbValue.Null;
                else
                    outputRow[i] = EvalWithAggregates(_selectColumns[i].Expression!, _firstRow);
            }
            Current = outputRow;
        }
        else
        {
            Current = Array.Empty<DbValue>();
        }
    }

    public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (!_emitResult) return ValueTask.FromResult(false);
        _emitResult = false;
        return ValueTask.FromResult(true);
    }

    public ValueTask DisposeAsync() => _source.DisposeAsync();

    private async ValueTask CaptureFirstBatchRowAsync(IBatchOperator batchSource, CancellationToken ct)
    {
        while (await batchSource.MoveNextBatchAsync(ct))
        {
            var batch = batchSource.CurrentBatch;
            if (batch.Count == 0)
                continue;

            _firstRow = batch.GetRowSpan(0).ToArray();
            return;
        }
    }

    private async ValueTask AccumulateBatchSourceAsync(IBatchOperator batchSource, CancellationToken ct)
    {
        while (await batchSource.MoveNextBatchAsync(ct))
        {
            var batch = batchSource.CurrentBatch;
            for (int rowIndex = 0; rowIndex < batch.Count; rowIndex++)
            {
                var row = batch.GetRowSpan(rowIndex);
                _firstRow ??= row.ToArray();

                for (int i = 0; i < _aggregateStateList.Length; i++)
                    _aggregateStateList[i].Accumulate(row, ref _batchRowBuffer);
            }
        }
    }

    private async ValueTask ConsumeRowSourceRowsAsync(Action<DbValue[]> rowAction, CancellationToken ct, bool stopAfterFirst = false)
    {
        while (await _source.MoveNextAsync(ct))
        {
            rowAction(_source.Current);
            if (stopAfterFirst)
                break;
        }
    }

    private DbValue[] EnsureBatchRowBuffer(int columnCount)
    {
        if (_batchRowBuffer == null || _batchRowBuffer.Length != columnCount)
            _batchRowBuffer = columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];

        return _batchRowBuffer;
    }

    private void CollectAggregates(Expression expr)
    {
        switch (expr)
        {
            case FunctionCallExpression func:
                if (ScalarFunctionEvaluator.IsAggregateFunction(func.FunctionName))
                {
                    if (!_aggregateStates.ContainsKey(func))
                    {
                        _aggregateStates.Add(func, new AggregateState(func, _inputSchema));
                        _aggregateFunctions.Add(func);
                    }
                }
                else
                {
                    for (int i = 0; i < func.Arguments.Count; i++)
                        CollectAggregates(func.Arguments[i]);
                }
                break;
            case BinaryExpression bin:
                CollectAggregates(bin.Left);
                CollectAggregates(bin.Right);
                break;
            case UnaryExpression un:
                CollectAggregates(un.Operand);
                break;
            case CollateExpression collate:
                CollectAggregates(collate.Operand);
                break;
        }
    }

    private DbValue EvalWithAggregates(Expression expr, DbValue[]? firstRow)
    {
        return expr switch
        {
            FunctionCallExpression func => ScalarFunctionEvaluator.IsAggregateFunction(func.FunctionName)
                ? EvaluateAggregate(func)
                : ScalarFunctionEvaluator.Evaluate(func, arg => EvalWithAggregates(arg, firstRow)),
            BinaryExpression bin => EvalBinaryWithAgg(bin, firstRow),
            UnaryExpression un => EvalUnaryWithAgg(un, firstRow),
            CollateExpression collate => EvalWithAggregates(collate.Operand, firstRow),
            _ => firstRow != null
                ? ExpressionEvaluator.Evaluate(expr, firstRow, _inputSchema)
                : DbValue.Null,
        };
    }

    private DbValue EvaluateAggregate(FunctionCallExpression func)
    {
        if (_aggregateStates.TryGetValue(func, out var state))
            return state.GetFinalValue();

        throw new CSharpDbException(ErrorCode.Unknown, $"Unknown aggregate function: {func.FunctionName}");
    }

    private DbValue EvalBinaryWithAgg(BinaryExpression bin, DbValue[]? firstRow)
    {
        var left = EvalWithAggregates(bin.Left, firstRow);
        var right = EvalWithAggregates(bin.Right, firstRow);

        return bin.Op switch
        {
            BinaryOp.Equals => BoolToDb(DbValue.Compare(left, right) == 0),
            BinaryOp.NotEquals => BoolToDb(DbValue.Compare(left, right) != 0),
            BinaryOp.LessThan => BoolToDb(DbValue.Compare(left, right) < 0),
            BinaryOp.GreaterThan => BoolToDb(DbValue.Compare(left, right) > 0),
            BinaryOp.LessOrEqual => BoolToDb(DbValue.Compare(left, right) <= 0),
            BinaryOp.GreaterOrEqual => BoolToDb(DbValue.Compare(left, right) >= 0),
            BinaryOp.And => BoolToDb(left.IsTruthy && right.IsTruthy),
            BinaryOp.Or => BoolToDb(left.IsTruthy || right.IsTruthy),
            BinaryOp.Plus => ArithOp(left, right, (a, b) => a + b, (a, b) => a + b),
            BinaryOp.Minus => ArithOp(left, right, (a, b) => a - b, (a, b) => a - b),
            BinaryOp.Multiply => ArithOp(left, right, (a, b) => a * b, (a, b) => a * b),
            BinaryOp.Divide => ArithOp(left, right, (a, b) => b != 0 ? a / b : 0, (a, b) => b != 0 ? a / b : 0),
            _ => DbValue.Null,
        };
    }

    private DbValue EvalUnaryWithAgg(UnaryExpression un, DbValue[]? firstRow)
    {
        var operand = EvalWithAggregates(un.Operand, firstRow);
        return un.Op switch
        {
            TokenType.Not => BoolToDb(!operand.IsTruthy),
            TokenType.Minus when operand.Type == DbType.Integer => DbValue.FromInteger(-operand.AsInteger),
            TokenType.Minus when operand.Type == DbType.Real => DbValue.FromReal(-operand.AsReal),
            _ => DbValue.Null,
        };
    }

    private static DbValue BoolToDb(bool v) => DbValue.FromInteger(v ? 1 : 0);

    private static DbValue ArithOp(DbValue l, DbValue r, Func<long, long, long> intOp, Func<double, double, double> realOp)
    {
        if (l.IsNull || r.IsNull) return DbValue.Null;
        if (l.Type == DbType.Real || r.Type == DbType.Real) return DbValue.FromReal(realOp(l.AsReal, r.AsReal));
        if (l.Type == DbType.Integer && r.Type == DbType.Integer) return DbValue.FromInteger(intOp(l.AsInteger, r.AsInteger));
        return DbValue.Null;
    }

    private sealed class AggregateState
    {
        private readonly string _name;
        private readonly SpanExpressionEvaluator? _argumentEvaluator;
        private readonly bool _isDistinct;
        private readonly bool _isStarArg;
        private readonly int _directColumnIndex;
        private readonly bool _hasLiteralArgument;
        private readonly DbValue _literalArgument;

        private AggregateDistinctValueSet? _distinctValues;
        private long _count;
        private double _sum;
        private bool _hasReal;
        private bool _hasAny;
        private DbValue? _best;

        public AggregateState(FunctionCallExpression func, TableSchema schema)
        {
            _name = func.FunctionName;
            _argumentEvaluator = BuildAggregateArgumentEvaluator(func, schema);
            _isDistinct = func.IsDistinct;
            _isStarArg = func.IsStarArg;
            _directColumnIndex = TryResolveDirectColumnIndex(func, schema);
            _hasLiteralArgument = TryResolveLiteralArgument(func, out _literalArgument);
            Reset();
        }

        public void Reset()
        {
            _distinctValues = _isDistinct ? new AggregateDistinctValueSet() : null;
            _count = 0;
            _sum = 0;
            _hasReal = false;
            _hasAny = false;
            _best = null;
        }

        public void Accumulate(DbValue[] row)
        {
            if (_name == "COUNT")
            {
                if (_isStarArg)
                {
                    _count++;
                    return;
                }

                var val = _argumentEvaluator!(row);
                if (val.IsNull) return;
                if (_distinctValues != null && !_distinctValues.Add(val)) return;
                _count++;
                return;
            }

            if (_name is "SUM" or "AVG")
            {
                var val = _argumentEvaluator!(row);
                if (val.IsNull) return;
                if (_distinctValues != null && !_distinctValues.Add(val)) return;
                _hasAny = true;
                if (val.Type == DbType.Real) _hasReal = true;
                _sum += val.Type == DbType.Real ? val.AsReal : val.AsInteger;
                _count++;
                return;
            }

            if (_name is "MIN" or "MAX")
            {
                var val = _argumentEvaluator!(row);
                if (val.IsNull) return;

                if (_best == null)
                {
                    _best = val;
                    return;
                }

                int cmp = DbValue.Compare(val, _best.Value);
                if ((_name == "MIN" && cmp < 0) || (_name == "MAX" && cmp > 0))
                    _best = val;
                return;
            }

            throw new CSharpDbException(ErrorCode.Unknown, $"Unknown aggregate function: {_name}");
        }

        public void Accumulate(ReadOnlySpan<DbValue> row, ref DbValue[]? rowBuffer)
        {
            if (_name == "COUNT")
            {
                if (_isStarArg)
                {
                    _count++;
                    return;
                }

                var val = EvaluateArgument(row, ref rowBuffer);
                if (val.IsNull) return;
                if (_distinctValues != null && !_distinctValues.Add(val)) return;
                _count++;
                return;
            }

            if (_name is "SUM" or "AVG")
            {
                var val = EvaluateArgument(row, ref rowBuffer);
                if (val.IsNull) return;
                if (_distinctValues != null && !_distinctValues.Add(val)) return;
                _hasAny = true;
                if (val.Type == DbType.Real) _hasReal = true;
                _sum += val.Type == DbType.Real ? val.AsReal : val.AsInteger;
                _count++;
                return;
            }

            if (_name is "MIN" or "MAX")
            {
                var val = EvaluateArgument(row, ref rowBuffer);
                if (val.IsNull) return;

                if (_best == null)
                {
                    _best = val;
                    return;
                }

                int cmp = DbValue.Compare(val, _best.Value);
                if ((_name == "MIN" && cmp < 0) || (_name == "MAX" && cmp > 0))
                    _best = val;
                return;
            }

            throw new CSharpDbException(ErrorCode.Unknown, $"Unknown aggregate function: {_name}");
        }

        private DbValue EvaluateArgument(ReadOnlySpan<DbValue> row, ref DbValue[]? rowBuffer)
        {
            if (_directColumnIndex >= 0)
                return (uint)_directColumnIndex < (uint)row.Length ? row[_directColumnIndex] : DbValue.Null;

            if (_hasLiteralArgument)
                return _literalArgument;

            return _argumentEvaluator!(row);
        }

        private static SpanExpressionEvaluator? BuildAggregateArgumentEvaluator(FunctionCallExpression func, TableSchema schema)
        {
            if (func.IsStarArg || func.Arguments.Count == 0)
                return null;

            return ExpressionCompiler.CompileSpan(func.Arguments[0], schema);
        }

        private static int TryResolveDirectColumnIndex(FunctionCallExpression func, TableSchema schema)
        {
            if (func.IsStarArg ||
                func.Arguments.Count != 1 ||
                func.Arguments[0] is not ColumnRefExpression col)
            {
                return -1;
            }

            return col.TableAlias != null
                ? schema.GetQualifiedColumnIndex(col.TableAlias, col.ColumnName)
                : schema.GetColumnIndex(col.ColumnName);
        }

        private static bool TryResolveLiteralArgument(FunctionCallExpression func, out DbValue value)
        {
            value = DbValue.Null;

            if (func.IsStarArg ||
                func.Arguments.Count != 1 ||
                func.Arguments[0] is not LiteralExpression lit)
            {
                return false;
            }

            value = lit.Value == null
                ? DbValue.Null
                : lit.LiteralType switch
                {
                    TokenType.IntegerLiteral => DbValue.FromInteger((long)lit.Value),
                    TokenType.RealLiteral => DbValue.FromReal((double)lit.Value),
                    TokenType.StringLiteral => DbValue.FromText((string)lit.Value),
                    TokenType.Null => DbValue.Null,
                    _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unknown literal type: {lit.LiteralType}"),
                };
            return true;
        }

        private static Func<DbValue[], DbValue> BuildColumnEvaluator(ColumnRefExpression col, TableSchema schema)
        {
            int idx = col.TableAlias != null
                ? schema.GetQualifiedColumnIndex(col.TableAlias, col.ColumnName)
                : schema.GetColumnIndex(col.ColumnName);

            if (idx < 0)
            {
                string colName = col.TableAlias != null
                    ? $"{col.TableAlias}.{col.ColumnName}"
                    : col.ColumnName;
                throw new CSharpDbException(ErrorCode.ColumnNotFound, $"Column '{colName}' not found.");
            }

            return row => idx < row.Length ? row[idx] : DbValue.Null;
        }

        private static Func<DbValue[], DbValue> BuildLiteralEvaluator(LiteralExpression lit)
        {
            var value = lit.Value == null
                ? DbValue.Null
                : lit.LiteralType switch
                {
                    TokenType.IntegerLiteral => DbValue.FromInteger((long)lit.Value),
                    TokenType.RealLiteral => DbValue.FromReal((double)lit.Value),
                    TokenType.StringLiteral => DbValue.FromText((string)lit.Value),
                    TokenType.Null => DbValue.Null,
                    _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unknown literal type: {lit.LiteralType}"),
                };

            return _ => value;
        }

        public DbValue GetFinalValue()
        {
            if (_name == "COUNT")
                return DbValue.FromInteger(_count);

            if (_name == "SUM")
            {
                if (!_hasAny) return DbValue.FromInteger(0);
                return _hasReal ? DbValue.FromReal(_sum) : DbValue.FromInteger((long)_sum);
            }

            if (_name == "AVG")
            {
                if (!_hasAny) return DbValue.Null;
                return DbValue.FromReal(_sum / _count);
            }

            if (_name is "MIN" or "MAX")
                return _best ?? DbValue.Null;

            throw new CSharpDbException(ErrorCode.Unknown, $"Unknown aggregate function: {_name}");
        }
    }
}

/// <summary>
/// Sort operator — materializes all input then sorts. Used for ORDER BY.
/// </summary>
public sealed class SortOperator : IOperator, IBatchOperator, IBatchBufferReuseController, IEstimatedRowCountProvider, IMaterializedRowsProvider
{
    private const string LateMaterializationOverrideEnvVar = "CSHARPDB_SORT_LATE_MATERIALIZATION";
    private const int LateMaterializationMinRowCount = 25_000;
    private const int LateMaterializationEstimatedRowWidthThreshold = 384;
    private const int TypedPrecomputedKeyMinRowCount = 50_000;
    private const int DefaultBatchSize = 64;

    private enum PrecomputedSingleKeyKind
    {
        None,
        Integer,
        Real,
        Text,
    }

    private enum SingleClauseComparerKind
    {
        Default,
        Integer,
        Real,
        Text,
        TextCollated,
    }

    private enum LateMaterializationOverrideMode
    {
        Auto,
        ForceOff,
        ForceOn,
    }

    private readonly struct CompiledSortClause
    {
        public readonly Expression Expression;
        public readonly int ColumnIndex;
        public readonly int KeyIndex;
        public readonly int Direction;
        public readonly string? Collation;
        public readonly Func<DbValue[], DbValue>? KeyEvaluator;

        public CompiledSortClause(
            Expression expression,
            int columnIndex,
            int keyIndex,
            bool descending,
            string? collation,
            Func<DbValue[], DbValue>? keyEvaluator)
        {
            Expression = expression;
            ColumnIndex = columnIndex;
            KeyIndex = keyIndex;
            Direction = descending ? -1 : 1;
            Collation = collation;
            KeyEvaluator = keyEvaluator;
        }

        public DbValue EvaluateRow(DbValue[] row)
        {
            return ColumnIndex >= 0 && ColumnIndex < row.Length ? row[ColumnIndex] : DbValue.Null;
        }

        public DbValue EvaluateSortKey(DbValue[] row)
        {
            return KeyEvaluator != null ? KeyEvaluator(row) : EvaluateRow(row);
        }
    }

    private readonly IOperator _source;
    private readonly CompiledSortClause[] _compiledOrderBy;
    private readonly int _precomputedKeyCount;
    private readonly bool _hasSingleOrderByClause;
    private readonly int _singleClauseColumnIndex;
    private readonly int _singleClauseKeyIndex;
    private readonly int _singleClauseDirection;
    private readonly SingleClauseComparerKind _singleClauseComparerKind;
    private readonly string? _singleClauseCollation;
    private readonly TableSchema _schema;
    private TableScanOperator? _lateMaterializedTableScan;
    private List<DbValue[]>? _sortedRows;
    private int[]? _sortedRowIndices;
    private long[]? _lateMaterializedRowIds;
    private PrecomputedSingleKeyKind _lateMaterializedSingleKeyKind;
    private long[]? _lateMaterializedIntKeys;
    private double[]? _lateMaterializedRealKeys;
    private string?[]? _lateMaterializedTextKeys;
    private bool[]? _lateMaterializedNulls;
    private DbValue[][]? _precomputedKeyColumns;
    private PrecomputedSingleKeyKind _singlePrecomputedKeyKind;
    private long[]? _singlePrecomputedIntKeys;
    private double[]? _singlePrecomputedRealKeys;
    private string?[]? _singlePrecomputedTextKeys;
    private bool[]? _singlePrecomputedNulls;
    private int _pooledRowCount;
    private int _index;
    private bool _reuseCurrentBatch = true;
    private RowBatch _currentBatch = new(0, DefaultBatchSize);

    public ColumnDefinition[] OutputSchema => _source.OutputSchema;
    public bool ReusesCurrentRowBuffer => false;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
    public int? EstimatedRowCount => _sortedRows?.Count ?? _lateMaterializedRowIds?.Length;

    public SortOperator(IOperator source, List<OrderByClause> orderBy, TableSchema schema)
    {
        _source = source;
        _schema = schema;
        _compiledOrderBy = CompileOrderBy(orderBy, schema, out _precomputedKeyCount);
        if (_compiledOrderBy.Length == 1)
        {
            var clause = _compiledOrderBy[0];
            _hasSingleOrderByClause = true;
            _singleClauseColumnIndex = clause.ColumnIndex;
            _singleClauseKeyIndex = clause.KeyIndex;
            _singleClauseDirection = clause.Direction;
            _singleClauseCollation = clause.Collation;
            _singleClauseComparerKind = ResolveSingleClauseComparerKind(clause, schema);
        }
    }

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        var batchSource = BatchSourceHelper.TryGetBatchSource(_source);
        _lateMaterializedTableScan = null;
        if (batchSource == null && _source is IRowBufferReuseController controller)
            controller.SetReuseCurrentRowBuffer(false);

        await _source.OpenAsync(ct);
        bool cloneRows = _source.ReusesCurrentRowBuffer;
        ReleasePooledBuffers();
        int initialRowCapacity = _source is IEstimatedRowCountProvider estimated &&
                                 estimated.EstimatedRowCount is int estimatedRowCount &&
                                 estimatedRowCount > 0
            ? estimatedRowCount
            : 0;
        _sortedRows = initialRowCapacity > 0
            ? new List<DbValue[]>(initialRowCapacity)
            : new List<DbValue[]>();
        _sortedRowIndices = null;
        _precomputedKeyColumns = null;
        _currentBatch = CreateBatch(_source.OutputSchema.Length);

        if (await TryOpenWithTableScanLateMaterializationAsync(ct))
        {
            _index = -1;
            return;
        }

        if (batchSource != null)
        {
            while (await batchSource.MoveNextBatchAsync(ct))
            {
                var sourceBatch = batchSource.CurrentBatch;
                for (int rowIndex = 0; rowIndex < sourceBatch.Count; rowIndex++)
                    _sortedRows.Add(sourceBatch.GetRowSpan(rowIndex).ToArray());
            }
        }
        else
        {
            while (await _source.MoveNextAsync(ct))
            {
                var row = _source.Current;
                _sortedRows.Add(cloneRows ? (DbValue[])row.Clone() : row);
            }
        }

        int rowCount = _sortedRows.Count;
        if (TryPrecomputeSingleClauseKeys(_sortedRows, rowCount))
        {
            _pooledRowCount = rowCount;
            _sortedRowIndices = ArrayPool<int>.Shared.Rent(rowCount);
            for (int i = 0; i < rowCount; i++)
                _sortedRowIndices[i] = i;
            _sortedRowIndices.AsSpan(0, rowCount).Sort(CompareRowIndices);
        }
        else if (_precomputedKeyCount == 0)
        {
            _sortedRows.Sort(CompareRows);
        }
        else
        {
            _pooledRowCount = rowCount;
            if (!TryPrecomputeSingleClauseKeys(_sortedRows, rowCount))
            {
                _precomputedKeyColumns = new DbValue[_precomputedKeyCount][];
                for (int i = 0; i < _precomputedKeyColumns.Length; i++)
                    _precomputedKeyColumns[i] = ArrayPool<DbValue>.Shared.Rent(rowCount);

                for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
                {
                    var row = _sortedRows[rowIndex];
                    for (int clauseIndex = 0; clauseIndex < _compiledOrderBy.Length; clauseIndex++)
                    {
                        var clause = _compiledOrderBy[clauseIndex];
                        if (clause.KeyIndex < 0) continue;
                        _precomputedKeyColumns[clause.KeyIndex][rowIndex] = clause.EvaluateSortKey(row);
                    }
                }
            }

            _sortedRowIndices = ArrayPool<int>.Shared.Rent(rowCount);
            for (int i = 0; i < rowCount; i++)
                _sortedRowIndices[i] = i;
            _sortedRowIndices.AsSpan(0, rowCount).Sort(CompareRowIndices);
        }

        _index = -1;
    }

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (_lateMaterializedTableScan != null && _lateMaterializedRowIds != null)
        {
            while (++_index < _lateMaterializedRowIds.Length)
            {
                int sortedIndex = _sortedRowIndices != null ? _sortedRowIndices[_index] : _index;
                var fullRow = await _lateMaterializedTableScan.DecodeFullRowByRowIdAsync(_lateMaterializedRowIds[sortedIndex], ct);
                if (fullRow == null)
                    continue;

                Current = fullRow;
                return true;
            }

            return false;
        }

        _index++;
        if (_sortedRows == null || _index >= _sortedRows.Count)
            return false;

        if (_sortedRowIndices != null)
        {
            Current = _sortedRows[_sortedRowIndices[_index]];
            return true;
        }

        Current = _sortedRows[_index];
        return true;
    }

    public async ValueTask<bool> MoveNextBatchAsync(CancellationToken ct = default)
    {
        if (_lateMaterializedTableScan != null && _lateMaterializedRowIds != null)
        {
            var lateBatch = _reuseCurrentBatch ? EnsureBatch(_source.OutputSchema.Length) : CreateBatch(_source.OutputSchema.Length);
            lateBatch.Reset();

            while (lateBatch.Count < lateBatch.Capacity && _index + 1 < _lateMaterializedRowIds.Length)
            {
                _index++;
                int sortedIndex = _sortedRowIndices != null ? _sortedRowIndices[_index] : _index;
                var fullRow = await _lateMaterializedTableScan.DecodeFullRowByRowIdAsync(_lateMaterializedRowIds[sortedIndex], ct);
                if (fullRow == null)
                    continue;

                fullRow.CopyTo(lateBatch.GetWritableRowSpan(lateBatch.Count));
                lateBatch.CommitWrittenRow(lateBatch.Count);
            }

            _currentBatch = lateBatch;
            return lateBatch.Count > 0;
        }

        if (_sortedRows == null)
            return false;

        var batch = _reuseCurrentBatch ? EnsureBatch(_source.OutputSchema.Length) : CreateBatch(_source.OutputSchema.Length);
        batch.Reset();

        while (batch.Count < batch.Capacity && _index + 1 < _sortedRows.Count)
        {
            _index++;
            GetSortedRow(_index).CopyTo(batch.GetWritableRowSpan(batch.Count));
            batch.CommitWrittenRow(batch.Count);
        }

        _currentBatch = batch;
        return batch.Count > 0;
    }

    bool IBatchOperator.ReusesCurrentBatch => _reuseCurrentBatch;
    RowBatch IBatchOperator.CurrentBatch => _currentBatch;

    public void SetReuseCurrentBatch(bool reuse)
    {
        _reuseCurrentBatch = reuse;
        if (!reuse)
            _currentBatch = CreateBatch(_source.OutputSchema.Length);
    }

    public ValueTask DisposeAsync()
    {
        ReleasePooledBuffers();
        _sortedRows = null;
        _index = -1;
        Current = Array.Empty<DbValue>();
        return _source.DisposeAsync();
    }

    public bool TryTakeMaterializedRows(out List<DbValue[]> rows)
    {
        if (_lateMaterializedTableScan != null || _lateMaterializedRowIds != null)
        {
            rows = new List<DbValue[]>(0);
            return false;
        }

        // When row indices are used, logical ordering is represented outside _sortedRows.
        // Keep the normal iterator path for that case.
        if (_sortedRows == null || _sortedRowIndices != null)
        {
            rows = new List<DbValue[]>(0);
            return false;
        }

        rows = _sortedRows;
        _sortedRows = null;
        _index = -1;
        Current = Array.Empty<DbValue>();
        ReleasePooledBuffers();
        return true;
    }

    private DbValue[] GetSortedRow(int logicalIndex)
    {
        if (_sortedRows == null)
            return Array.Empty<DbValue>();

        return _sortedRowIndices != null
            ? _sortedRows[_sortedRowIndices[logicalIndex]]
            : _sortedRows[logicalIndex];
    }

    private RowBatch EnsureBatch(int columnCount)
    {
        if (_currentBatch.ColumnCount != columnCount)
            _currentBatch = CreateBatch(columnCount);

        return _currentBatch;
    }

    private static RowBatch CreateBatch(int columnCount) => new(columnCount, DefaultBatchSize);

    private async ValueTask<bool> TryOpenWithTableScanLateMaterializationAsync(CancellationToken ct)
    {
        var lateMaterializationOverride = ResolveLateMaterializationOverride();
        if (lateMaterializationOverride == LateMaterializationOverrideMode.ForceOff)
            return false;

        if (_source is not TableScanOperator tableScan)
            return false;
        if (_compiledOrderBy.Length != 1 ||
            _singleClauseColumnIndex < 0 ||
            _singleClauseComparerKind == SingleClauseComparerKind.Default)
        {
            return false;
        }
        if (tableScan.DecodedColumnUpperBound.HasValue)
            return false;
        if (lateMaterializationOverride != LateMaterializationOverrideMode.ForceOn &&
            !ShouldUseTableScanLateMaterialization())
        {
            return false;
        }

        tableScan.SetDecodedColumnIndices([_singleClauseColumnIndex]);

        int initialCapacity = _source is IEstimatedRowCountProvider estimated &&
                              estimated.EstimatedRowCount is int estimatedRowCount &&
                              estimatedRowCount > 0
            ? estimatedRowCount
            : 0;

        var rowIds = initialCapacity > 0 ? new List<long>(initialCapacity) : new List<long>();
        var nulls = initialCapacity > 0 ? new List<bool>(initialCapacity) : new List<bool>();

        switch (_singleClauseComparerKind)
        {
            case SingleClauseComparerKind.Integer:
            {
                var keys = initialCapacity > 0 ? new List<long>(initialCapacity) : new List<long>();
                while (await _source.MoveNextAsync(ct))
                {
                    var row = _source.Current;
                    var value = _singleClauseColumnIndex < row.Length ? row[_singleClauseColumnIndex] : DbValue.Null;
                    if (!value.IsNull && value.Type != DbType.Integer)
                        return false;

                    rowIds.Add(tableScan.CurrentRowId);
                    nulls.Add(value.IsNull);
                    keys.Add(value.IsNull ? default : value.AsInteger);
                }

                _lateMaterializedRowIds = rowIds.ToArray();
                _lateMaterializedNulls = nulls.ToArray();
                _lateMaterializedIntKeys = keys.ToArray();
                _lateMaterializedSingleKeyKind = PrecomputedSingleKeyKind.Integer;
                break;
            }

            case SingleClauseComparerKind.Real:
            {
                var keys = initialCapacity > 0 ? new List<double>(initialCapacity) : new List<double>();
                while (await _source.MoveNextAsync(ct))
                {
                    var row = _source.Current;
                    var value = _singleClauseColumnIndex < row.Length ? row[_singleClauseColumnIndex] : DbValue.Null;
                    if (!value.IsNull && value.Type is not (DbType.Integer or DbType.Real))
                        return false;

                    rowIds.Add(tableScan.CurrentRowId);
                    nulls.Add(value.IsNull);
                    keys.Add(value.IsNull ? default : value.AsReal);
                }

                _lateMaterializedRowIds = rowIds.ToArray();
                _lateMaterializedNulls = nulls.ToArray();
                _lateMaterializedRealKeys = keys.ToArray();
                _lateMaterializedSingleKeyKind = PrecomputedSingleKeyKind.Real;
                break;
            }

            case SingleClauseComparerKind.Text:
            {
                var keys = initialCapacity > 0 ? new List<string?>(initialCapacity) : new List<string?>();
                while (await _source.MoveNextAsync(ct))
                {
                    var row = _source.Current;
                    var value = _singleClauseColumnIndex < row.Length ? row[_singleClauseColumnIndex] : DbValue.Null;
                    if (!value.IsNull && value.Type != DbType.Text)
                        return false;

                    rowIds.Add(tableScan.CurrentRowId);
                    nulls.Add(value.IsNull);
                    keys.Add(value.IsNull ? null : value.AsText);
                }

                _lateMaterializedRowIds = rowIds.ToArray();
                _lateMaterializedNulls = nulls.ToArray();
                _lateMaterializedTextKeys = keys.ToArray();
                _lateMaterializedSingleKeyKind = PrecomputedSingleKeyKind.Text;
                break;
            }

            default:
                return false;
        }

        int rowCount = _lateMaterializedRowIds.Length;
        _lateMaterializedTableScan = tableScan;
        _sortedRows = null;

        if (rowCount == 0)
        {
            _sortedRowIndices = null;
            return true;
        }

        _sortedRowIndices = ArrayPool<int>.Shared.Rent(rowCount);
        for (int i = 0; i < rowCount; i++)
            _sortedRowIndices[i] = i;

        _sortedRowIndices.AsSpan(0, rowCount).Sort(CompareRowIndices);
        return true;
    }

    private bool ShouldUseTableScanLateMaterialization()
    {
        if (_source is not IEstimatedRowCountProvider estimated ||
            estimated.EstimatedRowCount is not int estimatedRowCount ||
            estimatedRowCount < LateMaterializationMinRowCount)
        {
            return false;
        }

        return EstimateOutputRowWidth(OutputSchema) >= LateMaterializationEstimatedRowWidthThreshold;
    }

    private static LateMaterializationOverrideMode ResolveLateMaterializationOverride()
    {
        string? value = Environment.GetEnvironmentVariable(LateMaterializationOverrideEnvVar);
        if (string.IsNullOrWhiteSpace(value))
            return LateMaterializationOverrideMode.Auto;

        return value.Trim().ToLowerInvariant() switch
        {
            "0" or "off" or "false" or "disabled" => LateMaterializationOverrideMode.ForceOff,
            "1" or "on" or "true" or "enabled" or "force" => LateMaterializationOverrideMode.ForceOn,
            _ => LateMaterializationOverrideMode.Auto,
        };
    }

    private static int EstimateOutputRowWidth(ColumnDefinition[] outputSchema)
    {
        int width = 0;
        for (int i = 0; i < outputSchema.Length; i++)
        {
            width += outputSchema[i].Type switch
            {
                DbType.Integer => 8,
                DbType.Real => 8,
                DbType.Text => 128,
                DbType.Null => 8,
                _ => 16,
            };
        }

        return width;
    }

    private int CompareRows(DbValue[] a, DbValue[] b)
    {
        if (_hasSingleOrderByClause)
        {
            int idx = _singleClauseColumnIndex;
            var va = idx < a.Length ? a[idx] : DbValue.Null;
            var vb = idx < b.Length ? b[idx] : DbValue.Null;
            return CompareSingleClauseValues(va, vb) * _singleClauseDirection;
        }

        for (int i = 0; i < _compiledOrderBy.Length; i++)
        {
            var clause = _compiledOrderBy[i];
            var va = clause.EvaluateRow(a);
            var vb = clause.EvaluateRow(b);
            int cmp = CollationSupport.Compare(va, vb, clause.Collation);
            if (cmp != 0) return cmp * clause.Direction;
        }

        return 0;
    }

    private int CompareRowIndices(int aIndex, int bIndex)
    {
        if (_hasSingleOrderByClause)
        {
            if (_lateMaterializedSingleKeyKind != PrecomputedSingleKeyKind.None ||
                _singlePrecomputedKeyKind != PrecomputedSingleKeyKind.None)
            {
                return CompareSingleClausePrecomputedKeys(aIndex, bIndex) * _singleClauseDirection;
            }

            var singleRows = _sortedRows!;
            var singleKeyColumns = _precomputedKeyColumns!;
            var va = _singleClauseKeyIndex >= 0
                ? singleKeyColumns[_singleClauseKeyIndex][aIndex]
                : (_singleClauseColumnIndex < singleRows[aIndex].Length
                    ? singleRows[aIndex][_singleClauseColumnIndex]
                    : DbValue.Null);
            var vb = _singleClauseKeyIndex >= 0
                ? singleKeyColumns[_singleClauseKeyIndex][bIndex]
                : (_singleClauseColumnIndex < singleRows[bIndex].Length
                    ? singleRows[bIndex][_singleClauseColumnIndex]
                    : DbValue.Null);
            if (_singleClauseComparerKind == SingleClauseComparerKind.Default)
                return CollationSupport.Compare(va, vb, _singleClauseCollation) * _singleClauseDirection;
            return CompareSingleClauseValues(va, vb) * _singleClauseDirection;
        }

        var rows = _sortedRows!;
        var keyColumns = _precomputedKeyColumns!;
        for (int i = 0; i < _compiledOrderBy.Length; i++)
        {
            var clause = _compiledOrderBy[i];
            var va = clause.KeyIndex >= 0
                ? keyColumns[clause.KeyIndex][aIndex]
                : clause.EvaluateRow(rows[aIndex]);
            var vb = clause.KeyIndex >= 0
                ? keyColumns[clause.KeyIndex][bIndex]
                : clause.EvaluateRow(rows[bIndex]);
            int cmp = CollationSupport.Compare(va, vb, clause.Collation);
            if (cmp != 0) return cmp * clause.Direction;
        }

        return 0;
    }

    private static CompiledSortClause[] CompileOrderBy(List<OrderByClause> orderBy, TableSchema schema, out int precomputedKeyCount)
    {
        precomputedKeyCount = 0;
        var compiled = new CompiledSortClause[orderBy.Count];

        for (int i = 0; i < orderBy.Count; i++)
        {
            var clause = orderBy[i];
            int columnIndex = ResolveColumnIndex(clause.Expression, schema);
            int keyIndex = columnIndex >= 0 ? -1 : precomputedKeyCount++;
            string? collation = CollationSupport.ResolveExpressionCollation(clause.Expression, schema);
            Func<DbValue[], DbValue>? keyEvaluator = keyIndex >= 0
                ? ExpressionCompiler.Compile(clause.Expression, schema)
                : null;
            compiled[i] = new CompiledSortClause(clause.Expression, columnIndex, keyIndex, clause.Descending, collation, keyEvaluator);
        }

        return compiled;
    }

    private static int ResolveColumnIndex(Expression expression, TableSchema schema)
    {
        expression = CollationSupport.StripCollation(expression);
        if (expression is not ColumnRefExpression col)
            return -1;

        int idx = col.TableAlias != null
            ? schema.GetQualifiedColumnIndex(col.TableAlias, col.ColumnName)
            : schema.GetColumnIndex(col.ColumnName);

        return idx;
    }

    private static SingleClauseComparerKind ResolveSingleClauseComparerKind(
        CompiledSortClause clause,
        TableSchema schema)
    {
        if (clause.KeyIndex >= 0 || clause.ColumnIndex < 0 || clause.ColumnIndex >= schema.Columns.Count)
            return SingleClauseComparerKind.Default;

        return schema.Columns[clause.ColumnIndex].Type switch
        {
            DbType.Integer => SingleClauseComparerKind.Integer,
            DbType.Real => SingleClauseComparerKind.Real,
            DbType.Text => CollationSupport.IsBinaryOrDefault(clause.Collation)
                ? SingleClauseComparerKind.Text
                : SingleClauseComparerKind.TextCollated,
            _ => SingleClauseComparerKind.Default,
        };
    }

    private int CompareSingleClauseValues(DbValue a, DbValue b)
    {
        if (a.IsNull && b.IsNull) return 0;
        if (a.IsNull) return -1;
        if (b.IsNull) return 1;

        switch (_singleClauseComparerKind)
        {
            case SingleClauseComparerKind.Integer:
                if (a.Type == DbType.Integer && b.Type == DbType.Integer)
                    return a.AsInteger.CompareTo(b.AsInteger);
                break;

            case SingleClauseComparerKind.Real:
                if (a.Type is DbType.Integer or DbType.Real
                    && b.Type is DbType.Integer or DbType.Real)
                    return a.AsReal.CompareTo(b.AsReal);
                break;

            case SingleClauseComparerKind.Text:
                if (a.Type == DbType.Text && b.Type == DbType.Text)
                    return string.Compare(a.AsText, b.AsText, StringComparison.Ordinal);
                break;

            case SingleClauseComparerKind.TextCollated:
                if (a.Type == DbType.Text && b.Type == DbType.Text)
                    return CollationSupport.CompareText(a.AsText, b.AsText, _singleClauseCollation);
                break;
        }

        return CollationSupport.Compare(a, b, _singleClauseCollation);
    }

    private bool TryPrecomputeSingleClauseKeys(List<DbValue[]> rows, int rowCount)
    {
        _singlePrecomputedKeyKind = PrecomputedSingleKeyKind.None;
        _singlePrecomputedIntKeys = null;
        _singlePrecomputedRealKeys = null;
        _singlePrecomputedTextKeys = null;
        _singlePrecomputedNulls = null;

        if (!_hasSingleOrderByClause
            || rowCount < TypedPrecomputedKeyMinRowCount
            || _compiledOrderBy.Length != 1)
            return false;

        var clause = _compiledOrderBy[0];
        bool useComputedKeys = clause.KeyIndex >= 0;
        if (useComputedKeys && clause.KeyEvaluator == null)
            return false;
        if (!useComputedKeys &&
            (_singleClauseColumnIndex < 0 || _singleClauseComparerKind == SingleClauseComparerKind.Default))
            return false;

        bool[]? nulls = null;
        long[]? intValues = null;
        double[]? realValues = null;
        string?[]? textValues = null;
        DbValue[]? genericValues = null;
        PrecomputedSingleKeyKind kind = PrecomputedSingleKeyKind.None;
        try
        {
            nulls = ArrayPool<bool>.Shared.Rent(rowCount);
            Array.Clear(nulls, 0, rowCount);

            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
            {
                var row = rows[rowIndex];
                var key = useComputedKeys
                    ? clause.EvaluateSortKey(row)
                    : (_singleClauseColumnIndex < row.Length ? row[_singleClauseColumnIndex] : DbValue.Null);
                if (genericValues != null)
                {
                    genericValues[rowIndex] = key;
                    continue;
                }

                if (key.IsNull)
                {
                    nulls[rowIndex] = true;
                    continue;
                }

                switch (key.Type)
                {
                    case DbType.Integer:
                    {
                        if (kind == PrecomputedSingleKeyKind.None)
                        {
                            kind = PrecomputedSingleKeyKind.Integer;
                            intValues = ArrayPool<long>.Shared.Rent(rowCount);
                        }

                        if (kind == PrecomputedSingleKeyKind.Integer)
                        {
                            intValues![rowIndex] = key.AsInteger;
                            continue;
                        }

                        if (kind == PrecomputedSingleKeyKind.Real)
                        {
                            realValues![rowIndex] = key.AsReal;
                            continue;
                        }

                        break;
                    }

                    case DbType.Real:
                    {
                        if (kind == PrecomputedSingleKeyKind.None)
                        {
                            kind = PrecomputedSingleKeyKind.Real;
                            realValues = ArrayPool<double>.Shared.Rent(rowCount);
                        }
                        else if (kind == PrecomputedSingleKeyKind.Integer)
                        {
                            kind = PrecomputedSingleKeyKind.Real;
                            var upgraded = ArrayPool<double>.Shared.Rent(rowCount);
                            for (int i = 0; i < rowIndex; i++)
                            {
                                if (!nulls[i])
                                    upgraded[i] = intValues![i];
                            }

                            ArrayPool<long>.Shared.Return(intValues!, clearArray: false);
                            intValues = null;
                            realValues = upgraded;
                        }

                        if (kind == PrecomputedSingleKeyKind.Real)
                        {
                            realValues![rowIndex] = key.AsReal;
                            continue;
                        }

                        break;
                    }

                    case DbType.Text:
                    {
                        if (kind == PrecomputedSingleKeyKind.None)
                        {
                            kind = PrecomputedSingleKeyKind.Text;
                            textValues = ArrayPool<string?>.Shared.Rent(rowCount);
                            Array.Clear(textValues, 0, rowCount);
                        }

                        if (kind == PrecomputedSingleKeyKind.Text)
                        {
                            textValues![rowIndex] = key.AsText;
                            continue;
                        }

                        break;
                    }
                }

                // Mixed or unsupported types: fallback to generic DbValue key storage.
                genericValues = ArrayPool<DbValue>.Shared.Rent(rowCount);
                for (int i = 0; i < rowIndex; i++)
                {
                    if (nulls[i])
                    {
                        genericValues[i] = DbValue.Null;
                        continue;
                    }

                    genericValues[i] = kind switch
                    {
                        PrecomputedSingleKeyKind.Integer => DbValue.FromInteger(intValues![i]),
                        PrecomputedSingleKeyKind.Real => DbValue.FromReal(realValues![i]),
                        PrecomputedSingleKeyKind.Text => DbValue.FromText(textValues![i]!),
                        _ => DbValue.Null,
                    };
                }

                genericValues[rowIndex] = key;

                if (intValues != null)
                {
                    ArrayPool<long>.Shared.Return(intValues, clearArray: false);
                    intValues = null;
                }

                if (realValues != null)
                {
                    ArrayPool<double>.Shared.Return(realValues, clearArray: false);
                    realValues = null;
                }

                if (textValues != null)
                {
                    if (rowIndex > 0)
                        Array.Clear(textValues, 0, rowIndex);
                    ArrayPool<string?>.Shared.Return(textValues, clearArray: false);
                    textValues = null;
                }
            }

            if (genericValues != null)
            {
                if (!useComputedKeys)
                {
                    Array.Clear(genericValues, 0, rowCount);
                    ArrayPool<DbValue>.Shared.Return(genericValues, clearArray: false);
                    ArrayPool<bool>.Shared.Return(nulls, clearArray: true);
                    return false;
                }

                _precomputedKeyColumns = new DbValue[_precomputedKeyCount][];
                _precomputedKeyColumns[_singleClauseKeyIndex] = genericValues;
                ArrayPool<bool>.Shared.Return(nulls, clearArray: true);
                return true;
            }

            if (kind == PrecomputedSingleKeyKind.None)
            {
                if (!useComputedKeys)
                {
                    ArrayPool<bool>.Shared.Return(nulls, clearArray: true);
                    return false;
                }

                // All keys are NULL; generic keys preserve ordering semantics.
                var allNullKeys = ArrayPool<DbValue>.Shared.Rent(rowCount);
                Array.Fill(allNullKeys, DbValue.Null, 0, rowCount);
                _precomputedKeyColumns = new DbValue[_precomputedKeyCount][];
                _precomputedKeyColumns[_singleClauseKeyIndex] = allNullKeys;
                ArrayPool<bool>.Shared.Return(nulls, clearArray: true);
                return true;
            }

            _singlePrecomputedKeyKind = kind;
            _singlePrecomputedIntKeys = intValues;
            _singlePrecomputedRealKeys = realValues;
            _singlePrecomputedTextKeys = textValues;
            _singlePrecomputedNulls = nulls;
            return true;
        }
        catch
        {
            if (nulls != null)
                ArrayPool<bool>.Shared.Return(nulls, clearArray: true);

            if (intValues != null)
                ArrayPool<long>.Shared.Return(intValues, clearArray: false);

            if (realValues != null)
                ArrayPool<double>.Shared.Return(realValues, clearArray: false);

            if (textValues != null)
            {
                Array.Clear(textValues, 0, rowCount);
                ArrayPool<string?>.Shared.Return(textValues, clearArray: false);
            }

            if (genericValues != null)
            {
                Array.Clear(genericValues, 0, rowCount);
                ArrayPool<DbValue>.Shared.Return(genericValues, clearArray: false);
            }

            throw;
        }
    }

    private int CompareSingleClausePrecomputedKeys(int aIndex, int bIndex)
    {
        if (_lateMaterializedSingleKeyKind != PrecomputedSingleKeyKind.None)
        {
            var lateNulls = _lateMaterializedNulls!;
            bool lateANull = lateNulls[aIndex];
            bool lateBNull = lateNulls[bIndex];
            if (lateANull || lateBNull)
            {
                if (lateANull == lateBNull) return 0;
                return lateANull ? -1 : 1;
            }

            return _lateMaterializedSingleKeyKind switch
            {
                PrecomputedSingleKeyKind.Integer => _lateMaterializedIntKeys![aIndex].CompareTo(_lateMaterializedIntKeys![bIndex]),
                PrecomputedSingleKeyKind.Real => _lateMaterializedRealKeys![aIndex].CompareTo(_lateMaterializedRealKeys![bIndex]),
                PrecomputedSingleKeyKind.Text => _singleClauseComparerKind == SingleClauseComparerKind.TextCollated
                    ? CollationSupport.CompareText(_lateMaterializedTextKeys![aIndex]!, _lateMaterializedTextKeys![bIndex]!, _singleClauseCollation)
                    : string.Compare(_lateMaterializedTextKeys![aIndex], _lateMaterializedTextKeys![bIndex], StringComparison.Ordinal),
                _ => 0,
            };
        }

        var nulls = _singlePrecomputedNulls!;
        bool aNull = nulls[aIndex];
        bool bNull = nulls[bIndex];
        if (aNull || bNull)
        {
            if (aNull == bNull) return 0;
            return aNull ? -1 : 1;
        }

        return _singlePrecomputedKeyKind switch
        {
            PrecomputedSingleKeyKind.Integer => _singlePrecomputedIntKeys![aIndex].CompareTo(_singlePrecomputedIntKeys![bIndex]),
            PrecomputedSingleKeyKind.Real => _singlePrecomputedRealKeys![aIndex].CompareTo(_singlePrecomputedRealKeys![bIndex]),
            PrecomputedSingleKeyKind.Text => _singleClauseComparerKind == SingleClauseComparerKind.TextCollated
                ? CollationSupport.CompareText(_singlePrecomputedTextKeys![aIndex]!, _singlePrecomputedTextKeys![bIndex]!, _singleClauseCollation)
                : string.Compare(_singlePrecomputedTextKeys![aIndex], _singlePrecomputedTextKeys![bIndex], StringComparison.Ordinal),
            _ => 0,
        };
    }

    private void ReleasePooledBuffers()
    {
        _lateMaterializedTableScan = null;
        _lateMaterializedRowIds = null;

        if (_lateMaterializedTextKeys != null)
        {
            Array.Clear(_lateMaterializedTextKeys, 0, _lateMaterializedTextKeys.Length);
            _lateMaterializedTextKeys = null;
        }

        _lateMaterializedIntKeys = null;
        _lateMaterializedRealKeys = null;
        _lateMaterializedNulls = null;
        _lateMaterializedSingleKeyKind = PrecomputedSingleKeyKind.None;

        if (_singlePrecomputedIntKeys != null)
        {
            ArrayPool<long>.Shared.Return(_singlePrecomputedIntKeys, clearArray: false);
            _singlePrecomputedIntKeys = null;
        }

        if (_singlePrecomputedRealKeys != null)
        {
            ArrayPool<double>.Shared.Return(_singlePrecomputedRealKeys, clearArray: false);
            _singlePrecomputedRealKeys = null;
        }

        if (_singlePrecomputedTextKeys != null)
        {
            if (_pooledRowCount > 0)
                Array.Clear(_singlePrecomputedTextKeys, 0, _pooledRowCount);
            ArrayPool<string?>.Shared.Return(_singlePrecomputedTextKeys, clearArray: false);
            _singlePrecomputedTextKeys = null;
        }

        if (_singlePrecomputedNulls != null)
        {
            if (_pooledRowCount > 0)
                Array.Clear(_singlePrecomputedNulls, 0, _pooledRowCount);
            ArrayPool<bool>.Shared.Return(_singlePrecomputedNulls, clearArray: false);
            _singlePrecomputedNulls = null;
        }

        _singlePrecomputedKeyKind = PrecomputedSingleKeyKind.None;

        if (_precomputedKeyColumns != null)
        {
            for (int i = 0; i < _precomputedKeyColumns.Length; i++)
            {
                var column = _precomputedKeyColumns[i];
                if (column == null) continue;
                if (_pooledRowCount > 0)
                    Array.Clear(column, 0, _pooledRowCount);
                ArrayPool<DbValue>.Shared.Return(column, clearArray: false);
            }

            _precomputedKeyColumns = null;
        }

        if (_sortedRowIndices != null)
        {
            ArrayPool<int>.Shared.Return(_sortedRowIndices, clearArray: false);
            _sortedRowIndices = null;
        }

        _pooledRowCount = 0;
    }
}

/// <summary>
/// ORDER BY with a bounded top-N heap.
/// Keeps only the best N rows in memory and does a final in-memory sort
/// over that bounded set.
/// </summary>
public sealed class TopNSortOperator : IOperator, IBatchOperator, IBatchBufferReuseController, IEstimatedRowCountProvider, IMaterializedRowsProvider
{
    private const int DefaultBatchSize = 64;

    private readonly struct CompiledSortClause
    {
        public readonly Expression Expression;
        public readonly int ColumnIndex;
        public readonly int MaxReferencedColumnIndex;
        public readonly int KeyIndex;
        public readonly int Direction;
        public readonly string? Collation;
        public readonly Func<DbValue[], DbValue>? KeyEvaluator;

        public CompiledSortClause(
            Expression expression,
            int columnIndex,
            int maxReferencedColumnIndex,
            int keyIndex,
            bool descending,
            string? collation,
            Func<DbValue[], DbValue>? keyEvaluator)
        {
            Expression = expression;
            ColumnIndex = columnIndex;
            MaxReferencedColumnIndex = maxReferencedColumnIndex;
            KeyIndex = keyIndex;
            Direction = descending ? -1 : 1;
            Collation = collation;
            KeyEvaluator = keyEvaluator;
        }

        public DbValue EvaluateRow(DbValue[] row)
        {
            return ColumnIndex >= 0 && ColumnIndex < row.Length ? row[ColumnIndex] : DbValue.Null;
        }

        public DbValue EvaluateSortKey(DbValue[] row)
        {
            return KeyEvaluator != null ? KeyEvaluator(row) : EvaluateRow(row);
        }
    }

    private readonly struct RankedRow
    {
        public readonly DbValue[] Row;
        public readonly DbValue[]? Keys;
        public readonly DbValue SingleKey;
        public readonly bool HasSingleKey;

        public RankedRow(DbValue[] row, DbValue[]? keys)
        {
            Row = row;
            Keys = keys;
            SingleKey = DbValue.Null;
            HasSingleKey = false;
        }

        public RankedRow(DbValue[] row, DbValue singleKey)
        {
            Row = row;
            Keys = null;
            SingleKey = singleKey;
            HasSingleKey = true;
        }
    }

    private readonly struct RowIdRankedRow
    {
        public readonly RankedRow Ranked;
        public readonly long RowId;

        public RowIdRankedRow(RankedRow ranked, long rowId)
        {
            Ranked = ranked;
            RowId = rowId;
        }
    }

    private readonly struct SingleKeyRowIdRankedRow
    {
        public readonly DbValue Key;
        public readonly long RowId;

        public SingleKeyRowIdRankedRow(DbValue key, long rowId)
        {
            Key = key;
            RowId = rowId;
        }
    }

    private readonly IOperator _source;
    private readonly CompiledSortClause[] _compiledOrderBy;
    private readonly int _precomputedKeyCount;
    private readonly bool _singleComputedKeyFastPath;
    private readonly int _singleComputedKeyClauseIndex;
    private readonly int _topN;

    private List<DbValue[]>? _sortedRows;
    private int _index;
    private bool _reuseCurrentBatch = true;
    private RowBatch _currentBatch = new(0, DefaultBatchSize);

    public ColumnDefinition[] OutputSchema => _source.OutputSchema;
    public bool ReusesCurrentRowBuffer => false;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
    public int? EstimatedRowCount => _topN;

    public TopNSortOperator(IOperator source, List<OrderByClause> orderBy, TableSchema schema, int topN)
    {
        _source = source;
        _compiledOrderBy = CompileOrderBy(orderBy, schema, out _precomputedKeyCount);
        _singleComputedKeyClauseIndex = FindSingleComputedKeyClauseIndex(_compiledOrderBy, _precomputedKeyCount);
        _singleComputedKeyFastPath = _singleComputedKeyClauseIndex >= 0;
        _topN = Math.Max(0, topN);
    }

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        _sortedRows = _topN > 0
            ? new List<DbValue[]>(_topN)
            : new List<DbValue[]>();
        _currentBatch = CreateBatch(_source.OutputSchema.Length);

        if (_topN == 0)
        {
            await _source.OpenAsync(ct);
            _index = -1;
            return;
        }

        if (await TryOpenWithTableScanLateMaterializationAsync(ct))
        {
            _index = -1;
            return;
        }

        await _source.OpenAsync(ct);

        var heap = new List<RankedRow>(_topN);
        bool sourceReusesCurrentRowBuffer = _source.ReusesCurrentRowBuffer;
        while (await _source.MoveNextAsync(ct))
        {
            var row = _source.Current;
            var rankedRow = BuildRankedRow(row);

            if (heap.Count < _topN)
            {
                HeapPush(heap, EnsureOwnedRow(rankedRow, sourceReusesCurrentRowBuffer));
                continue;
            }

            // Root stores the current worst row among the retained set.
            // Replace only when the new row is strictly better.
            if (CompareRankedRows(rankedRow, heap[0]) < 0)
            {
                heap[0] = EnsureOwnedRow(rankedRow, sourceReusesCurrentRowBuffer);
                HeapSiftDown(heap, 0);
            }
        }

        heap.Sort(CompareRankedRows);
        _sortedRows.Capacity = heap.Count;
        for (int i = 0; i < heap.Count; i++)
            _sortedRows.Add(heap[i].Row);

        _index = -1;
    }

    public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        _index++;
        if (_sortedRows == null || _index >= _sortedRows.Count)
            return ValueTask.FromResult(false);

        Current = _sortedRows[_index];
        return ValueTask.FromResult(true);
    }

    public ValueTask<bool> MoveNextBatchAsync(CancellationToken ct = default)
    {
        if (_sortedRows == null)
            return ValueTask.FromResult(false);

        var batch = _reuseCurrentBatch ? EnsureBatch(_source.OutputSchema.Length) : CreateBatch(_source.OutputSchema.Length);
        batch.Reset();

        while (batch.Count < batch.Capacity && _index + 1 < _sortedRows.Count)
        {
            _index++;
            _sortedRows[_index].CopyTo(batch.GetWritableRowSpan(batch.Count));
            batch.CommitWrittenRow(batch.Count);
        }

        _currentBatch = batch;
        return ValueTask.FromResult(batch.Count > 0);
    }

    bool IBatchOperator.ReusesCurrentBatch => _reuseCurrentBatch;
    RowBatch IBatchOperator.CurrentBatch => _currentBatch;

    public void SetReuseCurrentBatch(bool reuse)
    {
        _reuseCurrentBatch = reuse;
        if (!reuse)
            _currentBatch = CreateBatch(_source.OutputSchema.Length);
    }

    public ValueTask DisposeAsync()
    {
        _sortedRows = null;
        _index = -1;
        Current = Array.Empty<DbValue>();
        return _source.DisposeAsync();
    }

    public bool TryTakeMaterializedRows(out List<DbValue[]> rows)
    {
        if (_sortedRows == null)
        {
            rows = new List<DbValue[]>(0);
            return false;
        }

        rows = _sortedRows;
        _sortedRows = null;
        _index = -1;
        Current = Array.Empty<DbValue>();
        return true;
    }

    private async ValueTask<bool> TryOpenWithTableScanLateMaterializationAsync(CancellationToken ct)
    {
        if (_source is not TableScanOperator tableScan)
            return false;
        if (tableScan.DecodedColumnUpperBound.HasValue)
            return false;

        int maxOrderByColumnIndex = -1;
        for (int i = 0; i < _compiledOrderBy.Length; i++)
        {
            int columnIndex = _compiledOrderBy[i].MaxReferencedColumnIndex;
            if (columnIndex < 0)
                return false;

            if (columnIndex > maxOrderByColumnIndex)
                maxOrderByColumnIndex = columnIndex;
        }

        if (maxOrderByColumnIndex < 0)
            return false;

        // Decode only ORDER BY columns while scanning. Full row decode happens only for retained Top N rows.
        tableScan.SetDecodedColumnUpperBound(maxOrderByColumnIndex);

        await _source.OpenAsync(ct);

        if (_compiledOrderBy.Length == 1)
        {
            var clause = _compiledOrderBy[0];
            var singleKeyHeap = new List<SingleKeyRowIdRankedRow>(_topN);

            while (await _source.MoveNextAsync(ct))
            {
                var sourceRow = _source.Current;
                var key = clause.KeyIndex >= 0
                    ? clause.EvaluateSortKey(sourceRow)
                    : clause.EvaluateRow(sourceRow);
                var candidate = new SingleKeyRowIdRankedRow(
                    key,
                    tableScan.CurrentRowId);

                if (singleKeyHeap.Count < _topN)
                {
                    HeapPushSingleKeyRowId(singleKeyHeap, candidate);
                    continue;
                }

                // Root stores the current worst row among the retained set.
                // Replace only when the new row is strictly better.
                if (CompareSingleKeyRowIdRankedRows(candidate, singleKeyHeap[0]) < 0)
                {
                    singleKeyHeap[0] = candidate;
                    HeapSiftDownSingleKeyRowId(singleKeyHeap, 0);
                }
            }

            singleKeyHeap.Sort(CompareSingleKeyRowIdRankedRows);
            _sortedRows!.Capacity = singleKeyHeap.Count;
            for (int i = 0; i < singleKeyHeap.Count; i++)
            {
                var fullRow = await tableScan.DecodeFullRowByRowIdAsync(singleKeyHeap[i].RowId, ct);
                if (fullRow != null)
                    _sortedRows.Add(fullRow);
            }

            return true;
        }

        bool sourceReusesCurrentRowBuffer = _source.ReusesCurrentRowBuffer;
        var heap = new List<RowIdRankedRow>(_topN);

        while (await _source.MoveNextAsync(ct))
        {
            var rankedRow = BuildRankedRow(_source.Current);
            long rowId = tableScan.CurrentRowId;

            if (heap.Count < _topN)
            {
                var owned = EnsureOwnedRow(rankedRow, sourceReusesCurrentRowBuffer);
                HeapPushRowId(heap, new RowIdRankedRow(owned, rowId));
                continue;
            }

            // Root stores the current worst row among the retained set.
            // Replace only when the new row is strictly better.
            if (CompareRankedRows(rankedRow, heap[0].Ranked) < 0)
            {
                var owned = EnsureOwnedRow(rankedRow, sourceReusesCurrentRowBuffer);
                heap[0] = new RowIdRankedRow(owned, rowId);
                HeapSiftDownRowId(heap, 0);
            }
        }

        heap.Sort(CompareRowIdRankedRows);
        _sortedRows!.Capacity = heap.Count;
        for (int i = 0; i < heap.Count; i++)
        {
            var fullRow = await tableScan.DecodeFullRowByRowIdAsync(heap[i].RowId, ct);
            if (fullRow != null)
                _sortedRows.Add(fullRow);
        }

        return true;
    }

    private RowBatch EnsureBatch(int columnCount)
    {
        if (_currentBatch.ColumnCount != columnCount)
            _currentBatch = CreateBatch(columnCount);

        return _currentBatch;
    }

    private static RowBatch CreateBatch(int columnCount) => new(columnCount, DefaultBatchSize);

    private RankedRow BuildRankedRow(DbValue[] row)
    {
        if (_precomputedKeyCount == 0)
            return new RankedRow(row, null);

        if (_singleComputedKeyFastPath)
        {
            var singleClause = _compiledOrderBy[_singleComputedKeyClauseIndex];
            return new RankedRow(row, singleClause.EvaluateSortKey(row));
        }

        var keys = new DbValue[_precomputedKeyCount];
        for (int i = 0; i < _compiledOrderBy.Length; i++)
        {
            var clause = _compiledOrderBy[i];
            if (clause.KeyIndex < 0) continue;
            keys[clause.KeyIndex] = clause.EvaluateSortKey(row);
        }

        return new RankedRow(row, keys);
    }
    private int CompareRankedRows(RankedRow left, RankedRow right)
    {
        for (int i = 0; i < _compiledOrderBy.Length; i++)
        {
            var clause = _compiledOrderBy[i];
            DbValue leftValue;
            DbValue rightValue;
            if (clause.KeyIndex >= 0)
            {
                if (_singleComputedKeyFastPath)
                {
                    leftValue = left.SingleKey;
                    rightValue = right.SingleKey;
                }
                else
                {
                    leftValue = left.Keys![clause.KeyIndex];
                    rightValue = right.Keys![clause.KeyIndex];
                }
            }
            else
            {
                leftValue = clause.EvaluateRow(left.Row);
                rightValue = clause.EvaluateRow(right.Row);
            }

            int cmp = CollationSupport.Compare(leftValue, rightValue, clause.Collation);
            if (cmp != 0)
                return cmp * clause.Direction;
        }

        return 0;
    }

    private int CompareRowIdRankedRows(RowIdRankedRow left, RowIdRankedRow right)
    {
        return CompareRankedRows(left.Ranked, right.Ranked);
    }

    private int CompareSingleKeyRowIdRankedRows(SingleKeyRowIdRankedRow left, SingleKeyRowIdRankedRow right)
    {
        int cmp = CollationSupport.Compare(left.Key, right.Key, _compiledOrderBy[0].Collation);
        if (cmp == 0)
            return 0;

        return cmp * _compiledOrderBy[0].Direction;
    }

    private void HeapPush(List<RankedRow> heap, RankedRow value)
    {
        heap.Add(value);
        HeapSiftUp(heap, heap.Count - 1);
    }

    private void HeapSiftUp(List<RankedRow> heap, int index)
    {
        while (index > 0)
        {
            int parent = (index - 1) >> 1;
            // Max-heap by ORDER BY rank: "larger" means worse.
            if (CompareRankedRows(heap[index], heap[parent]) <= 0)
                break;

            (heap[parent], heap[index]) = (heap[index], heap[parent]);
            index = parent;
        }
    }

    private void HeapSiftDown(List<RankedRow> heap, int index)
    {
        int count = heap.Count;
        while (true)
        {
            int left = (index << 1) + 1;
            if (left >= count)
                return;

            int right = left + 1;
            int largest = left;
            if (right < count && CompareRankedRows(heap[right], heap[left]) > 0)
                largest = right;

            if (CompareRankedRows(heap[largest], heap[index]) <= 0)
                return;

            (heap[index], heap[largest]) = (heap[largest], heap[index]);
            index = largest;
        }
    }

    private void HeapPushRowId(List<RowIdRankedRow> heap, RowIdRankedRow value)
    {
        heap.Add(value);
        HeapSiftUpRowId(heap, heap.Count - 1);
    }

    private void HeapSiftUpRowId(List<RowIdRankedRow> heap, int index)
    {
        while (index > 0)
        {
            int parent = (index - 1) >> 1;
            // Max-heap by ORDER BY rank: "larger" means worse.
            if (CompareRowIdRankedRows(heap[index], heap[parent]) <= 0)
                break;

            (heap[parent], heap[index]) = (heap[index], heap[parent]);
            index = parent;
        }
    }

    private void HeapSiftDownRowId(List<RowIdRankedRow> heap, int index)
    {
        int count = heap.Count;
        while (true)
        {
            int left = (index << 1) + 1;
            if (left >= count)
                return;

            int right = left + 1;
            int largest = left;
            if (right < count && CompareRowIdRankedRows(heap[right], heap[left]) > 0)
                largest = right;

            if (CompareRowIdRankedRows(heap[largest], heap[index]) <= 0)
                return;

            (heap[index], heap[largest]) = (heap[largest], heap[index]);
            index = largest;
        }
    }

    private void HeapPushSingleKeyRowId(List<SingleKeyRowIdRankedRow> heap, SingleKeyRowIdRankedRow value)
    {
        heap.Add(value);
        HeapSiftUpSingleKeyRowId(heap, heap.Count - 1);
    }

    private void HeapSiftUpSingleKeyRowId(List<SingleKeyRowIdRankedRow> heap, int index)
    {
        while (index > 0)
        {
            int parent = (index - 1) >> 1;
            // Max-heap by ORDER BY rank: "larger" means worse.
            if (CompareSingleKeyRowIdRankedRows(heap[index], heap[parent]) <= 0)
                break;

            (heap[parent], heap[index]) = (heap[index], heap[parent]);
            index = parent;
        }
    }

    private void HeapSiftDownSingleKeyRowId(List<SingleKeyRowIdRankedRow> heap, int index)
    {
        int count = heap.Count;
        while (true)
        {
            int left = (index << 1) + 1;
            if (left >= count)
                return;

            int right = left + 1;
            int largest = left;
            if (right < count && CompareSingleKeyRowIdRankedRows(heap[right], heap[left]) > 0)
                largest = right;

            if (CompareSingleKeyRowIdRankedRows(heap[largest], heap[index]) <= 0)
                return;

            (heap[index], heap[largest]) = (heap[largest], heap[index]);
            index = largest;
        }
    }

    private static RankedRow EnsureOwnedRow(RankedRow row, bool sourceReusesCurrentRowBuffer)
    {
        if (!sourceReusesCurrentRowBuffer)
            return row;

        return row.HasSingleKey
            ? new RankedRow((DbValue[])row.Row.Clone(), row.SingleKey)
            : new RankedRow((DbValue[])row.Row.Clone(), row.Keys);
    }

    private static CompiledSortClause[] CompileOrderBy(List<OrderByClause> orderBy, TableSchema schema, out int precomputedKeyCount)
    {
        precomputedKeyCount = 0;
        var compiled = new CompiledSortClause[orderBy.Count];

        for (int i = 0; i < orderBy.Count; i++)
        {
            var clause = orderBy[i];
            int columnIndex = ResolveColumnIndex(clause.Expression, schema);
            int maxReferencedColumnIndex = -1;
            bool hasKnownMaxReferencedColumn = TryResolveMaxReferencedColumn(
                clause.Expression,
                schema,
                out maxReferencedColumnIndex);
            int keyIndex = columnIndex >= 0 ? -1 : precomputedKeyCount++;
            string? collation = CollationSupport.ResolveExpressionCollation(clause.Expression, schema);
            Func<DbValue[], DbValue>? keyEvaluator = keyIndex >= 0
                ? ExpressionCompiler.Compile(clause.Expression, schema)
                : null;
            compiled[i] = new CompiledSortClause(
                clause.Expression,
                columnIndex,
                hasKnownMaxReferencedColumn ? maxReferencedColumnIndex : -1,
                keyIndex,
                clause.Descending,
                collation,
                keyEvaluator);
        }

        return compiled;
    }

    private static int ResolveColumnIndex(Expression expression, TableSchema schema)
    {
        expression = CollationSupport.StripCollation(expression);
        if (expression is not ColumnRefExpression col)
            return -1;

        int idx = col.TableAlias != null
            ? schema.GetQualifiedColumnIndex(col.TableAlias, col.ColumnName)
            : schema.GetColumnIndex(col.ColumnName);

        return idx;
    }

    private static bool TryResolveMaxReferencedColumn(
        Expression expression,
        TableSchema schema,
        out int maxReferencedColumnIndex)
    {
        maxReferencedColumnIndex = -1;
        return TryAccumulateMaxReferencedColumn(expression, schema, ref maxReferencedColumnIndex);
    }

    private static bool TryAccumulateMaxReferencedColumn(
        Expression expression,
        TableSchema schema,
        ref int maxReferencedColumnIndex)
    {
        switch (expression)
        {
            case LiteralExpression:
            case ParameterExpression:
                return true;
            case ColumnRefExpression column:
            {
                int columnIndex = column.TableAlias != null
                    ? schema.GetQualifiedColumnIndex(column.TableAlias, column.ColumnName)
                    : schema.GetColumnIndex(column.ColumnName);
                if (columnIndex < 0)
                    return false;

                if (columnIndex > maxReferencedColumnIndex)
                    maxReferencedColumnIndex = columnIndex;
                return true;
            }
            case BinaryExpression binaryExpression:
                return TryAccumulateMaxReferencedColumn(binaryExpression.Left, schema, ref maxReferencedColumnIndex)
                    && TryAccumulateMaxReferencedColumn(binaryExpression.Right, schema, ref maxReferencedColumnIndex);
            case UnaryExpression unaryExpression:
                return TryAccumulateMaxReferencedColumn(unaryExpression.Operand, schema, ref maxReferencedColumnIndex);
            case CollateExpression collateExpression:
                return TryAccumulateMaxReferencedColumn(collateExpression.Operand, schema, ref maxReferencedColumnIndex);
            case LikeExpression likeExpression:
                return TryAccumulateMaxReferencedColumn(likeExpression.Operand, schema, ref maxReferencedColumnIndex)
                    && TryAccumulateMaxReferencedColumn(likeExpression.Pattern, schema, ref maxReferencedColumnIndex)
                    && (likeExpression.EscapeChar == null ||
                        TryAccumulateMaxReferencedColumn(likeExpression.EscapeChar, schema, ref maxReferencedColumnIndex));
            case InExpression inExpression:
            {
                if (!TryAccumulateMaxReferencedColumn(inExpression.Operand, schema, ref maxReferencedColumnIndex))
                    return false;

                for (int i = 0; i < inExpression.Values.Count; i++)
                {
                    if (!TryAccumulateMaxReferencedColumn(inExpression.Values[i], schema, ref maxReferencedColumnIndex))
                        return false;
                }

                return true;
            }
            case BetweenExpression betweenExpression:
                return TryAccumulateMaxReferencedColumn(betweenExpression.Operand, schema, ref maxReferencedColumnIndex)
                    && TryAccumulateMaxReferencedColumn(betweenExpression.Low, schema, ref maxReferencedColumnIndex)
                    && TryAccumulateMaxReferencedColumn(betweenExpression.High, schema, ref maxReferencedColumnIndex);
            case IsNullExpression isNullExpression:
                return TryAccumulateMaxReferencedColumn(isNullExpression.Operand, schema, ref maxReferencedColumnIndex);
            case FunctionCallExpression functionCall:
            {
                if (functionCall.IsStarArg)
                    return true;

                for (int i = 0; i < functionCall.Arguments.Count; i++)
                {
                    if (!TryAccumulateMaxReferencedColumn(functionCall.Arguments[i], schema, ref maxReferencedColumnIndex))
                        return false;
                }

                return true;
            }
            default:
                return false;
        }
    }

    private static int FindSingleComputedKeyClauseIndex(CompiledSortClause[] compiledOrderBy, int precomputedKeyCount)
    {
        if (precomputedKeyCount != 1)
            return -1;

        for (int i = 0; i < compiledOrderBy.Length; i++)
        {
            if (compiledOrderBy[i].KeyIndex == 0)
                return i;
        }

        return -1;
    }
}

/// <summary>
/// Hash-join operator for equi-joins (with optional residual predicate).
/// Supports INNER, LEFT OUTER, and RIGHT OUTER joins.
/// </summary>
public sealed class HashJoinOperator : IOperator, IBatchOperator, IBatchBackedRowOperator, IProjectionPushdownTarget, IEstimatedRowCountProvider, IBatchBufferReuseController
{
    private const int DefaultBatchSize = 64;

    private readonly IOperator _left;
    private readonly IOperator _right;
    private readonly JoinType _joinType;
    private readonly bool _buildRightSide;
    private readonly int[] _leftKeyIndices;
    private readonly int[] _rightKeyIndices;
    private readonly Expression? _residualConditionExpression;
    private readonly TableSchema _compositeSchema;
    private readonly JoinSpanExpressionEvaluator? _residualPredicate;
    private JoinSpanExpressionEvaluator? _compactedResidualPredicate;
    private readonly int _leftColCount;
    private readonly int _rightColCount;
    private readonly bool _singleKeyFastPath;
    private readonly int _singleBuildKeyIndex;
    private readonly int _singleProbeKeyIndex;
    private readonly int? _buildRowCapacityHint;
    private readonly int? _buildKeyCapacityHint;
    private readonly int _buildBucketInitialCapacity;
    private readonly int? _estimatedRowCount;
    private Dictionary<HashJoinKey, SingleKeyBucket>? _hashTable;
    private Dictionary<DbValue, SingleKeyBucket>? _singleKeyHashTable;
    private List<DbValue[]>? _allRightRows;
    private HashSet<DbValue[]>? _matchedRightRows;
    private DbValue[]? _activeProbeRow;
    private DbValue[]? _activeSingleBuildRow;
    private List<DbValue[]>? _activeBuildMatches;
    private int _activeBuildMatchIndex;
    private bool _activeProbeMatched;
    private bool _probeExhausted;
    private int _rightOuterEmitIndex;
    private int[]? _projectionColumnIndices;
    private ProjectionAccessor[]? _projectionAccessors;
    private bool _buildRowCompactionEnabled;
    private int[]? _buildRequiredColumnIndices;
    private int[]? _buildColumnToCompactIndexMap;
    private IBatchOperator? _probeBatchSource;
    private RowBatch? _pendingProbeBatch;
    private int _pendingProbeBatchRowIndex;
    private DbValue[]? _probeBatchRowBuffer;
    private bool _reuseCurrentBatch = true;
    private RowBatch _currentBatch = new(0, DefaultBatchSize);

    public ColumnDefinition[] OutputSchema { get; private set; }
    public bool ReusesCurrentRowBuffer => false;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
    public int? EstimatedRowCount => _estimatedRowCount;
    IBatchOperator IBatchBackedRowOperator.BatchSource => this;

    public HashJoinOperator(
        IOperator left,
        IOperator right,
        JoinType joinType,
        Expression? residualCondition,
        TableSchema compositeSchema,
        int leftColCount,
        int rightColCount,
        int[] leftKeyIndices,
        int[] rightKeyIndices,
        bool buildRightSide = true,
        int? buildRowCapacityHint = null,
        int? estimatedOutputRowCount = null)
    {
        _left = left;
        _right = right;
        _joinType = joinType;
        _buildRightSide = buildRightSide;
        _leftKeyIndices = leftKeyIndices;
        _rightKeyIndices = rightKeyIndices;
        _residualConditionExpression = residualCondition;
        _compositeSchema = compositeSchema;
        _residualPredicate = residualCondition != null
            ? ExpressionCompiler.CompileJoinSpan(residualCondition, compositeSchema, leftColCount)
            : null;
        _compactedResidualPredicate = null;
        _leftColCount = leftColCount;
        _rightColCount = rightColCount;
        _singleKeyFastPath = leftKeyIndices.Length == 1 && rightKeyIndices.Length == 1;
        if (_singleKeyFastPath)
        {
            _singleBuildKeyIndex = _buildRightSide ? rightKeyIndices[0] : leftKeyIndices[0];
            _singleProbeKeyIndex = _buildRightSide ? leftKeyIndices[0] : rightKeyIndices[0];
        }
        else
        {
            _singleBuildKeyIndex = -1;
            _singleProbeKeyIndex = -1;
        }

        _buildRowCapacityHint = buildRowCapacityHint > 0 ? buildRowCapacityHint : null;
        _buildKeyCapacityHint = DeriveBuildKeyCapacityHint(_buildRowCapacityHint);
        _buildBucketInitialCapacity = DeriveBuildBucketInitialCapacity(_buildRowCapacityHint, _buildKeyCapacityHint);
        _estimatedRowCount = estimatedOutputRowCount > 0 ? estimatedOutputRowCount : null;
        OutputSchema = _compositeSchema.Columns as ColumnDefinition[] ?? _compositeSchema.Columns.ToArray();
    }

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        if (_joinType is not (JoinType.Inner or JoinType.LeftOuter or JoinType.RightOuter))
            throw new CSharpDbException(ErrorCode.Unknown, $"Unsupported hash join type: {_joinType}");
        if (!_buildRightSide && _joinType != JoinType.Inner)
            throw new CSharpDbException(ErrorCode.Unknown, "Swapped hash build side is supported for INNER JOIN only.");

        ConfigureBuildRowCompaction();
        ConfigureProjectionAccessors();

        if (_left is IRowBufferReuseController leftController)
        {
            bool leftIsBuildSide = !_buildRightSide;
            // Probe rows are consumed immediately, so reuse is always safe.
            // Build rows are retained unless compaction is enabled (we copy only required columns).
            leftController.SetReuseCurrentRowBuffer(!leftIsBuildSide || _buildRowCompactionEnabled);
        }

        if (_right is IRowBufferReuseController rightController)
        {
            bool rightIsBuildSide = _buildRightSide;
            // Probe rows are consumed immediately, so reuse is always safe.
            // Build rows are retained unless compaction is enabled (we copy only required columns).
            rightController.SetReuseCurrentRowBuffer(!rightIsBuildSide || _buildRowCompactionEnabled);
        }

        await _left.OpenAsync(ct);
        await _right.OpenAsync(ct);

        _hashTable = _singleKeyFastPath
            ? null
            : _buildKeyCapacityHint.HasValue
                ? new Dictionary<HashJoinKey, SingleKeyBucket>(_buildKeyCapacityHint.Value, HashJoinKeyComparer.Instance)
                : new Dictionary<HashJoinKey, SingleKeyBucket>(HashJoinKeyComparer.Instance);
        _singleKeyHashTable = _singleKeyFastPath
            ? _buildKeyCapacityHint.HasValue
                ? new Dictionary<DbValue, SingleKeyBucket>(_buildKeyCapacityHint.Value)
                : new Dictionary<DbValue, SingleKeyBucket>()
            : null;
        _allRightRows = _buildRightSide && _joinType == JoinType.RightOuter
            ? _buildRowCapacityHint.HasValue
                ? new List<DbValue[]>(_buildRowCapacityHint.Value)
                : new List<DbValue[]>()
            : null;
        _matchedRightRows = _buildRightSide && _joinType == JoinType.RightOuter
            ? _buildRowCapacityHint.HasValue
                ? new HashSet<DbValue[]>(_buildRowCapacityHint.Value, ReferenceEqualityComparer.Instance)
                : new HashSet<DbValue[]>(ReferenceEqualityComparer.Instance)
            : null;
        _probeBatchSource = BatchSourceHelper.TryGetBatchSource(ProbeSource);
        _pendingProbeBatch = null;
        _pendingProbeBatchRowIndex = 0;
        _probeBatchRowBuffer = null;
        _currentBatch = CreateBatch(OutputSchema.Length);
        _compactedResidualPredicate = null;
        if (_buildRowCompactionEnabled && _residualConditionExpression != null)
        {
            _compactedResidualPredicate = ExpressionCompiler.CompileJoinSpan(
                _residualConditionExpression,
                _compositeSchema,
                _leftColCount,
                leftColumnMap: _buildRightSide ? null : _buildColumnToCompactIndexMap,
                rightColumnMap: _buildRightSide ? _buildColumnToCompactIndexMap : null);
        }

        await ConsumeBuildRowsAsync(ct);

        Current = Array.Empty<DbValue>();
        ResetProbeState();
    }

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        while (true)
        {
            if (_activeProbeRow != null)
            {
                if (_activeSingleBuildRow != null)
                {
                    var singleBuildMatch = _activeSingleBuildRow;
                    _activeSingleBuildRow = null;

                    if (PassesResidual(_activeProbeRow, singleBuildMatch))
                    {
                        var combined = CombineProbeAndBuildRows(_activeProbeRow, singleBuildMatch);
                        _activeProbeMatched = true;
                        if (_buildRightSide && _joinType == JoinType.RightOuter)
                            _matchedRightRows?.Add(singleBuildMatch);

                        Current = combined;
                        return true;
                    }
                }

                while (_activeBuildMatches != null && _activeBuildMatchIndex < _activeBuildMatches.Count)
                {
                    var buildMatch = _activeBuildMatches[_activeBuildMatchIndex++];
                    if (!PassesResidual(_activeProbeRow, buildMatch))
                        continue;

                    var combined = CombineProbeAndBuildRows(_activeProbeRow, buildMatch);
                    _activeProbeMatched = true;
                    if (_buildRightSide && _joinType == JoinType.RightOuter)
                        _matchedRightRows?.Add(buildMatch);

                    Current = combined;
                    return true;
                }

                if (_buildRightSide && !_activeProbeMatched && _joinType == JoinType.LeftOuter)
                {
                    Current = CreateLeftOuterRowFromProbe(_activeProbeRow);
                    ClearActiveProbeState();
                    return true;
                }

                ClearActiveProbeState();
                continue;
            }

            if (!_probeExhausted)
            {
                var probeRow = await TryMoveNextProbeRowAsync(ct);
                if (probeRow != null)
                {
                    ActivateProbeRow(probeRow);
                    continue;
                }

                _probeExhausted = true;
            }

            if (_buildRightSide &&
                _joinType == JoinType.RightOuter &&
                TryEmitUnmatchedRightRow(out var unmatchedRightRow))
            {
                Current = unmatchedRightRow;
                return true;
            }

            return false;
        }
    }

    public async ValueTask<bool> MoveNextBatchAsync(CancellationToken ct = default)
        => _joinType switch
        {
            JoinType.Inner => await MoveNextBatchFromNativeInnerPathAsync(ct),
            JoinType.LeftOuter when _buildRightSide => await MoveNextBatchFromNativeLeftOuterPathAsync(ct),
            JoinType.RightOuter when _buildRightSide => await MoveNextBatchFromNativeRightOuterPathAsync(ct),
            _ => await MoveNextBatchFromRowsAsync(ct),
        };

    private async ValueTask<bool> MoveNextBatchFromNativeInnerPathAsync(CancellationToken ct)
    {
        var batch = _reuseCurrentBatch ? EnsureBatch(OutputSchema.Length) : CreateBatch(OutputSchema.Length);
        batch.Reset();

        while (batch.Count < batch.Capacity)
        {
            if (_activeProbeRow != null)
            {
                if (TryAppendCurrentInnerMatch(batch))
                    continue;

                ClearActiveProbeState();
                continue;
            }

            if (_probeExhausted)
                break;

            var probeRow = await TryMoveNextProbeRowAsync(ct);
            if (probeRow == null)
            {
                _probeExhausted = true;
                break;
            }

            ActivateProbeRow(probeRow);
        }

        _currentBatch = batch;
        return batch.Count > 0;
    }

    private async ValueTask<bool> MoveNextBatchFromNativeLeftOuterPathAsync(CancellationToken ct)
    {
        var batch = _reuseCurrentBatch ? EnsureBatch(OutputSchema.Length) : CreateBatch(OutputSchema.Length);
        batch.Reset();

        while (batch.Count < batch.Capacity)
        {
            if (_activeProbeRow != null)
            {
                if (TryAppendCurrentLeftOuterMatch(batch))
                    continue;

                ClearActiveProbeState();
                continue;
            }

            if (_probeExhausted)
                break;

            var probeRow = await TryMoveNextProbeRowAsync(ct);
            if (probeRow == null)
            {
                _probeExhausted = true;
                break;
            }

            ActivateProbeRow(probeRow);
        }

        _currentBatch = batch;
        return batch.Count > 0;
    }

    private async ValueTask<bool> MoveNextBatchFromNativeRightOuterPathAsync(CancellationToken ct)
    {
        var batch = _reuseCurrentBatch ? EnsureBatch(OutputSchema.Length) : CreateBatch(OutputSchema.Length);
        batch.Reset();

        while (batch.Count < batch.Capacity)
        {
            if (_activeProbeRow != null)
            {
                if (TryAppendCurrentRightOuterMatch(batch))
                    continue;

                ClearActiveProbeState();
                continue;
            }

            if (!_probeExhausted)
            {
                var probeRow = await TryMoveNextProbeRowAsync(ct);
                if (probeRow != null)
                {
                    ActivateProbeRow(probeRow);
                    continue;
                }

                _probeExhausted = true;
            }

            if (TryAppendNextRightOuterUnmatchedRow(batch))
                continue;

            break;
        }

        _currentBatch = batch;
        return batch.Count > 0;
    }

    private async ValueTask<bool> MoveNextBatchFromRowsAsync(CancellationToken ct)
    {
        var batch = _reuseCurrentBatch ? EnsureBatch(OutputSchema.Length) : CreateBatch(OutputSchema.Length);
        batch.Reset();

        while (batch.Count < batch.Capacity && await MoveNextAsync(ct))
        {
            Current.CopyTo(batch.GetWritableRowSpan(batch.Count));
            batch.CommitWrittenRow(batch.Count);
        }

        _currentBatch = batch;
        return batch.Count > 0;
    }

    bool IBatchOperator.ReusesCurrentBatch => _reuseCurrentBatch;
    RowBatch IBatchOperator.CurrentBatch => _currentBatch;

    public void SetReuseCurrentBatch(bool reuse)
    {
        _reuseCurrentBatch = reuse;
        if (!reuse)
            _currentBatch = CreateBatch(OutputSchema.Length);
    }

    public async ValueTask DisposeAsync()
    {
        await _left.DisposeAsync();
        await _right.DisposeAsync();
    }

    public bool TrySetOutputProjection(int[] columnIndices, ColumnDefinition[] outputSchema)
    {
        if (columnIndices == null)
            throw new ArgumentNullException(nameof(columnIndices));
        if (outputSchema == null)
            throw new ArgumentNullException(nameof(outputSchema));

        int compositeColumnCount = _leftColCount + _rightColCount;
        for (int i = 0; i < columnIndices.Length; i++)
        {
            int columnIndex = columnIndices[i];
            if (columnIndex < 0 || columnIndex >= compositeColumnCount)
                return false;
        }

        _projectionColumnIndices = (int[])columnIndices.Clone();
        OutputSchema = outputSchema;
        TryApplyDecodeBoundPushdown();
        ConfigureProjectionAccessors();
        return true;
    }

    private void TryApplyDecodeBoundPushdown()
    {
        if (_projectionColumnIndices == null)
            return;

        // Residual predicates are evaluated on full combined rows; keep full decode in that case.
        if (_residualConditionExpression != null)
            return;

        var leftFlags = new bool[_leftColCount];
        var rightFlags = new bool[_rightColCount];
        int leftRequiredCount = 0;
        int rightRequiredCount = 0;

        void MarkLeftRequired(int columnIndex)
        {
            if ((uint)columnIndex >= (uint)_leftColCount || leftFlags[columnIndex])
                return;

            leftFlags[columnIndex] = true;
            leftRequiredCount++;
        }

        void MarkRightRequired(int columnIndex)
        {
            if ((uint)columnIndex >= (uint)_rightColCount || rightFlags[columnIndex])
                return;

            rightFlags[columnIndex] = true;
            rightRequiredCount++;
        }

        for (int i = 0; i < _projectionColumnIndices.Length; i++)
        {
            int projectionIndex = _projectionColumnIndices[i];
            if (projectionIndex < _leftColCount)
            {
                MarkLeftRequired(projectionIndex);
            }
            else
            {
                MarkRightRequired(projectionIndex - _leftColCount);
            }
        }

        for (int i = 0; i < _leftKeyIndices.Length; i++)
            MarkLeftRequired(_leftKeyIndices[i]);

        for (int i = 0; i < _rightKeyIndices.Length; i++)
            MarkRightRequired(_rightKeyIndices[i]);

        int[] leftRequiredColumns = BuildRequiredColumnIndices(leftFlags, leftRequiredCount);
        int[] rightRequiredColumns = BuildRequiredColumnIndices(rightFlags, rightRequiredCount);

        if (!TrySetDecodedColumnIndices(_left, leftRequiredColumns) && leftRequiredColumns.Length > 0)
            TrySetDecodedColumnUpperBound(_left, leftRequiredColumns[^1]);

        if (!TrySetDecodedColumnIndices(_right, rightRequiredColumns) && rightRequiredColumns.Length > 0)
            TrySetDecodedColumnUpperBound(_right, rightRequiredColumns[^1]);
    }

    private static int[] BuildRequiredColumnIndices(bool[] flags, int count)
    {
        if (count <= 0)
            return Array.Empty<int>();

        var columns = new int[count];
        int cursor = 0;
        for (int i = 0; i < flags.Length; i++)
        {
            if (!flags[i])
                continue;

            columns[cursor++] = i;
        }

        return columns;
    }

    private static bool TrySetDecodedColumnIndices(IOperator op, int[] columnIndices)
    {
        if (columnIndices.Length == 0)
            return true;

        switch (op)
        {
            case TableScanOperator tableScan:
                tableScan.SetDecodedColumnIndices(columnIndices);
                return true;
            case PrimaryKeyLookupOperator primaryKeyLookup:
                primaryKeyLookup.SetDecodedColumnIndices(columnIndices);
                return true;
            case IndexScanOperator indexScan:
                indexScan.SetDecodedColumnIndices(columnIndices);
                return true;
            case UniqueIndexLookupOperator uniqueIndexLookup:
                uniqueIndexLookup.SetDecodedColumnIndices(columnIndices);
                return true;
            case IndexOrderedScanOperator orderedScan:
                orderedScan.SetDecodedColumnIndices(columnIndices);
                return true;
            default:
                return false;
        }
    }

    private static void TrySetDecodedColumnUpperBound(IOperator op, int maxColumnIndex)
    {
        if (maxColumnIndex < 0)
            return;

        switch (op)
        {
            case TableScanOperator tableScan:
                tableScan.SetDecodedColumnUpperBound(maxColumnIndex);
                break;
            case PrimaryKeyLookupOperator primaryKeyLookup:
                primaryKeyLookup.SetDecodedColumnUpperBound(maxColumnIndex);
                break;
            case IndexScanOperator indexScan:
                indexScan.SetDecodedColumnUpperBound(maxColumnIndex);
                break;
            case UniqueIndexLookupOperator uniqueIndexLookup:
                uniqueIndexLookup.SetDecodedColumnUpperBound(maxColumnIndex);
                break;
            case IndexOrderedScanOperator orderedScan:
                orderedScan.SetDecodedColumnUpperBound(maxColumnIndex);
                break;
        }
    }

    private void ConfigureProjectionAccessors()
    {
        var projection = _projectionColumnIndices;
        if (projection == null || projection.Length == 0)
        {
            _projectionAccessors = null;
            return;
        }

        var accessors = new ProjectionAccessor[projection.Length];
        for (int i = 0; i < projection.Length; i++)
        {
            int columnIndex = projection[i];
            if (columnIndex < _leftColCount)
            {
                bool sourceIsBuild = !_buildRightSide;
                accessors[i] = new ProjectionAccessor(
                    sourceIsLeft: true,
                    sourceIndex: ResolveProjectedSourceIndex(columnIndex, sourceIsBuild));
            }
            else
            {
                int rightIndex = columnIndex - _leftColCount;
                bool sourceIsBuild = _buildRightSide;
                accessors[i] = new ProjectionAccessor(
                    sourceIsLeft: false,
                    sourceIndex: ResolveProjectedSourceIndex(rightIndex, sourceIsBuild));
            }
        }

        _projectionAccessors = accessors;
    }

    private int ResolveProjectedSourceIndex(int columnIndex, bool sourceIsBuild)
    {
        if (!sourceIsBuild || !_buildRowCompactionEnabled)
            return columnIndex;

        var columnToCompactIndex = _buildColumnToCompactIndexMap
            ?? throw new InvalidOperationException("Build row compaction map is not configured.");
        if ((uint)columnIndex >= (uint)columnToCompactIndex.Length)
            return -1;

        return columnToCompactIndex[columnIndex];
    }

    private IOperator BuildSource => _buildRightSide ? _right : _left;
    private IOperator ProbeSource => _buildRightSide ? _left : _right;
    private int[] BuildKeyIndices => _buildRightSide ? _rightKeyIndices : _leftKeyIndices;
    private int[] ProbeKeyIndices => _buildRightSide ? _leftKeyIndices : _rightKeyIndices;
    private int BuildColumnCount => _buildRightSide ? _rightColCount : _leftColCount;

    private RowBatch EnsureBatch(int columnCount)
    {
        if (_currentBatch.ColumnCount != columnCount)
            _currentBatch = CreateBatch(columnCount);

        return _currentBatch;
    }

    private static RowBatch CreateBatch(int columnCount) => new(columnCount, DefaultBatchSize);

    private async ValueTask ConsumeBuildRowsAsync(CancellationToken ct)
    {
        var buildSource = BuildSource;
        var buildBatchSource = BatchSourceHelper.TryGetBatchSource(buildSource);
        if (buildBatchSource == null)
        {
            while (await buildSource.MoveNextAsync(ct))
            {
                var buildCurrent = buildSource.Current;
                DbValue[] buildRow;
                if (_buildRowCompactionEnabled)
                {
                    buildRow = CompactBuildRow(buildCurrent);
                    AddBuildRow(buildRow, buildCurrent);
                }
                else
                {
                    buildRow = buildSource.ReusesCurrentRowBuffer
                        ? (DbValue[])buildCurrent.Clone()
                        : buildCurrent;
                    AddBuildRow(buildRow);
                }
            }

            return;
        }

        while (await buildBatchSource.MoveNextBatchAsync(ct))
        {
            var batch = buildBatchSource.CurrentBatch;
            for (int rowIndex = 0; rowIndex < batch.Count; rowIndex++)
            {
                var buildCurrent = batch.GetRowSpan(rowIndex);
                if (_buildRowCompactionEnabled)
                {
                    var buildRow = CompactBuildRow(buildCurrent);
                    AddBuildRow(buildRow, buildCurrent);
                }
                else
                {
                    AddBuildRow(buildCurrent.ToArray());
                }
            }
        }
    }

    private async ValueTask<DbValue[]?> TryMoveNextProbeRowAsync(CancellationToken ct)
    {
        if (_probeBatchSource == null)
        {
            var probeSource = ProbeSource;
            if (await probeSource.MoveNextAsync(ct))
                return probeSource.Current;

            return null;
        }

        while (true)
        {
            if (_pendingProbeBatch != null && _pendingProbeBatchRowIndex < _pendingProbeBatch.Count)
            {
                var batch = _pendingProbeBatch;
                var probeRow = EnsureProbeBatchRowBuffer(batch.ColumnCount);
                batch.CopyRowTo(_pendingProbeBatchRowIndex, probeRow);
                _pendingProbeBatchRowIndex++;
                if (_pendingProbeBatchRowIndex >= batch.Count)
                {
                    _pendingProbeBatch = null;
                    _pendingProbeBatchRowIndex = 0;
                }

                return probeRow;
            }

            if (!await _probeBatchSource.MoveNextBatchAsync(ct))
                return null;

            _pendingProbeBatch = _probeBatchSource.CurrentBatch;
            _pendingProbeBatchRowIndex = 0;
        }
    }

    private void ConfigureBuildRowCompaction()
    {
        _buildRowCompactionEnabled = false;
        _buildRequiredColumnIndices = null;
        _buildColumnToCompactIndexMap = null;

        var projection = _projectionColumnIndices;
        if (projection == null || projection.Length == 0)
            return;

        int buildColumnCount = BuildColumnCount;
        if (buildColumnCount <= 0)
            return;

        var requiredFlags = new bool[buildColumnCount];
        int requiredCount = 0;

        void MarkRequired(int buildColumnIndex)
        {
            if ((uint)buildColumnIndex >= (uint)buildColumnCount)
                return;
            if (requiredFlags[buildColumnIndex])
                return;

            requiredFlags[buildColumnIndex] = true;
            requiredCount++;
        }

        var buildKeys = BuildKeyIndices;
        for (int i = 0; i < buildKeys.Length; i++)
            MarkRequired(buildKeys[i]);

        for (int i = 0; i < projection.Length; i++)
        {
            int projectionIndex = projection[i];
            if (_buildRightSide)
            {
                if (projectionIndex < _leftColCount)
                    continue;
                MarkRequired(projectionIndex - _leftColCount);
            }
            else
            {
                if (projectionIndex >= _leftColCount)
                    continue;
                MarkRequired(projectionIndex);
            }
        }

        if (_residualConditionExpression != null &&
            !TryMarkBuildSideColumnsForExpression(_residualConditionExpression, MarkRequired))
        {
            _buildRowCompactionEnabled = false;
            _buildRequiredColumnIndices = null;
            _buildColumnToCompactIndexMap = null;
            return;
        }

        if (requiredCount <= 0 || requiredCount >= buildColumnCount)
            return;

        var requiredColumns = new int[requiredCount];
        int cursor = 0;
        for (int i = 0; i < requiredFlags.Length; i++)
        {
            if (!requiredFlags[i])
                continue;
            requiredColumns[cursor++] = i;
        }

        var columnToCompactIndex = new int[buildColumnCount];
        Array.Fill(columnToCompactIndex, -1);
        for (int i = 0; i < requiredColumns.Length; i++)
            columnToCompactIndex[requiredColumns[i]] = i;

        _buildRequiredColumnIndices = requiredColumns;
        _buildColumnToCompactIndexMap = columnToCompactIndex;
        _buildRowCompactionEnabled = true;
    }

    private bool TryMarkBuildSideColumnsForExpression(Expression expression, Action<int> markBuildColumn)
    {
        switch (expression)
        {
            case LiteralExpression:
            case ParameterExpression:
                return true;
            case ColumnRefExpression columnRef:
            {
                int compositeColumnIndex = columnRef.TableAlias != null
                    ? _compositeSchema.GetQualifiedColumnIndex(columnRef.TableAlias, columnRef.ColumnName)
                    : _compositeSchema.GetColumnIndex(columnRef.ColumnName);
                if (compositeColumnIndex < 0)
                    return false;

                if (TryMapCompositeColumnToBuildColumn(compositeColumnIndex, out int buildColumnIndex))
                    markBuildColumn(buildColumnIndex);
                return true;
            }
            case BinaryExpression binaryExpression:
                return TryMarkBuildSideColumnsForExpression(binaryExpression.Left, markBuildColumn)
                    && TryMarkBuildSideColumnsForExpression(binaryExpression.Right, markBuildColumn);
            case UnaryExpression unaryExpression:
                return TryMarkBuildSideColumnsForExpression(unaryExpression.Operand, markBuildColumn);
            case CollateExpression collateExpression:
                return TryMarkBuildSideColumnsForExpression(collateExpression.Operand, markBuildColumn);
            case LikeExpression likeExpression:
                return TryMarkBuildSideColumnsForExpression(likeExpression.Operand, markBuildColumn)
                    && TryMarkBuildSideColumnsForExpression(likeExpression.Pattern, markBuildColumn)
                    && (likeExpression.EscapeChar == null ||
                        TryMarkBuildSideColumnsForExpression(likeExpression.EscapeChar, markBuildColumn));
            case InExpression inExpression:
            {
                if (!TryMarkBuildSideColumnsForExpression(inExpression.Operand, markBuildColumn))
                    return false;

                for (int i = 0; i < inExpression.Values.Count; i++)
                {
                    if (!TryMarkBuildSideColumnsForExpression(inExpression.Values[i], markBuildColumn))
                        return false;
                }

                return true;
            }
            case BetweenExpression betweenExpression:
                return TryMarkBuildSideColumnsForExpression(betweenExpression.Operand, markBuildColumn)
                    && TryMarkBuildSideColumnsForExpression(betweenExpression.Low, markBuildColumn)
                    && TryMarkBuildSideColumnsForExpression(betweenExpression.High, markBuildColumn);
            case IsNullExpression isNullExpression:
                return TryMarkBuildSideColumnsForExpression(isNullExpression.Operand, markBuildColumn);
            case FunctionCallExpression functionCall:
            {
                if (functionCall.IsStarArg)
                    return true;

                for (int i = 0; i < functionCall.Arguments.Count; i++)
                {
                    if (!TryMarkBuildSideColumnsForExpression(functionCall.Arguments[i], markBuildColumn))
                        return false;
                }

                return true;
            }
            default:
                return false;
        }
    }

    private bool TryMapCompositeColumnToBuildColumn(int compositeColumnIndex, out int buildColumnIndex)
    {
        if (_buildRightSide)
        {
            if (compositeColumnIndex < _leftColCount)
            {
                buildColumnIndex = -1;
                return false;
            }

            buildColumnIndex = compositeColumnIndex - _leftColCount;
            return true;
        }

        if (compositeColumnIndex >= _leftColCount)
        {
            buildColumnIndex = -1;
            return false;
        }

        buildColumnIndex = compositeColumnIndex;
        return true;
    }

    private DbValue[] CompactBuildRow(ReadOnlySpan<DbValue> buildRow)
    {
        var requiredColumns = _buildRequiredColumnIndices
            ?? throw new InvalidOperationException("Build row compaction columns are not configured.");
        var compact = new DbValue[requiredColumns.Length];
        for (int i = 0; i < requiredColumns.Length; i++)
        {
            int sourceIndex = requiredColumns[i];
            compact[i] = sourceIndex < buildRow.Length
                ? buildRow[sourceIndex]
                : DbValue.Null;
        }

        return compact;
    }

    private void AddBuildRow(DbValue[] buildRow)
        => AddBuildRow(buildRow, buildRow);

    private void AddBuildRow(DbValue[] buildRow, ReadOnlySpan<DbValue> buildKeySourceRow)
    {
        if (_buildRightSide && _allRightRows != null)
            _allRightRows.Add(buildRow);

        if (_singleKeyFastPath)
        {
            var keyValue = ExtractSingleJoinKey(buildKeySourceRow, _singleBuildKeyIndex);
            ref var singleBucket = ref CollectionsMarshal.GetValueRefOrAddDefault(
                _singleKeyHashTable!,
                keyValue,
                out bool singleExists);
            if (singleExists)
            {
                singleBucket.Add(buildRow);
            }
            else
            {
                singleBucket = SingleKeyBucket.Create(buildRow);
            }

            return;
        }

        var joinKey = ExtractJoinKey(buildKeySourceRow, BuildKeyIndices);
        ref var bucketRef = ref CollectionsMarshal.GetValueRefOrAddDefault(
            _hashTable!,
            joinKey,
            out bool hashExists);
        if (hashExists)
        {
            bucketRef.Add(buildRow, _buildBucketInitialCapacity);
        }
        else
        {
            bucketRef = SingleKeyBucket.Create(buildRow);
        }
    }

    private bool TryEmitUnmatchedRightRow(out DbValue[] row)
    {
        row = Array.Empty<DbValue>();
        if (_allRightRows == null)
            return false;

        while (_rightOuterEmitIndex < _allRightRows.Count)
        {
            var rightRow = _allRightRows[_rightOuterEmitIndex++];
            if (_matchedRightRows != null && _matchedRightRows.Contains(rightRow))
                continue;

            row = CreateRightOuterRowFromBuild(rightRow);
            return true;
        }

        return false;
    }

    private bool TryAppendNextRightOuterUnmatchedRow(RowBatch batch)
    {
        if (_allRightRows == null)
            return false;

        while (_rightOuterEmitIndex < _allRightRows.Count)
        {
            var rightRow = _allRightRows[_rightOuterEmitIndex++];
            if (_matchedRightRows != null && _matchedRightRows.Contains(rightRow))
                continue;

            AppendRightOuterRowFromBuild(batch, rightRow);
            return true;
        }

        return false;
    }

    private void ActivateProbeRow(DbValue[] probeRow)
    {
        _activeProbeRow = probeRow;
        _activeSingleBuildRow = null;
        _activeBuildMatches = null;
        _activeProbeMatched = false;
        _activeBuildMatchIndex = 0;

        if (_singleKeyFastPath)
        {
            var keyValue = ExtractSingleJoinKey(probeRow, _singleProbeKeyIndex);
            if (_singleKeyHashTable!.TryGetValue(keyValue, out var bucket))
            {
                _activeSingleBuildRow = bucket.SingleMatch;
                _activeBuildMatches = bucket.MultipleMatches;
            }

            return;
        }

        var key = ExtractJoinKey(probeRow, ProbeKeyIndices);
        if (_hashTable!.TryGetValue(key, out var keyedBucket))
        {
            _activeSingleBuildRow = keyedBucket.SingleMatch;
            _activeBuildMatches = keyedBucket.MultipleMatches;
        }
    }

    private bool TryAppendCurrentInnerMatch(RowBatch batch)
    {
        if (_activeProbeRow == null)
            return false;

        if (_activeSingleBuildRow != null)
        {
            var singleBuildMatch = _activeSingleBuildRow;
            _activeSingleBuildRow = null;
            if (!PassesResidual(_activeProbeRow, singleBuildMatch))
                return false;

            AppendCombinedProbeAndBuildRow(batch, _activeProbeRow, singleBuildMatch);
            _activeProbeMatched = true;
            return true;
        }

        while (_activeBuildMatches != null && _activeBuildMatchIndex < _activeBuildMatches.Count)
        {
            var buildMatch = _activeBuildMatches[_activeBuildMatchIndex++];
            if (!PassesResidual(_activeProbeRow, buildMatch))
                continue;

            AppendCombinedProbeAndBuildRow(batch, _activeProbeRow, buildMatch);
            _activeProbeMatched = true;
            return true;
        }

        return false;
    }

    private bool TryAppendCurrentLeftOuterMatch(RowBatch batch)
    {
        if (_activeProbeRow == null)
            return false;

        if (_activeSingleBuildRow != null)
        {
            var singleBuildMatch = _activeSingleBuildRow;
            _activeSingleBuildRow = null;
            if (PassesResidual(_activeProbeRow, singleBuildMatch))
            {
                AppendCombinedProbeAndBuildRow(batch, _activeProbeRow, singleBuildMatch);
                _activeProbeMatched = true;
                return true;
            }
        }

        while (_activeBuildMatches != null && _activeBuildMatchIndex < _activeBuildMatches.Count)
        {
            var buildMatch = _activeBuildMatches[_activeBuildMatchIndex++];
            if (!PassesResidual(_activeProbeRow, buildMatch))
                continue;

            AppendCombinedProbeAndBuildRow(batch, _activeProbeRow, buildMatch);
            _activeProbeMatched = true;
            return true;
        }

        if (_activeProbeMatched)
            return false;

        AppendLeftOuterRowFromProbe(batch, _activeProbeRow);
        _activeProbeMatched = true;
        return true;
    }

    private bool TryAppendCurrentRightOuterMatch(RowBatch batch)
    {
        if (_activeProbeRow == null)
            return false;

        if (_activeSingleBuildRow != null)
        {
            var singleBuildMatch = _activeSingleBuildRow;
            _activeSingleBuildRow = null;
            if (PassesResidual(_activeProbeRow, singleBuildMatch))
            {
                AppendCombinedProbeAndBuildRow(batch, _activeProbeRow, singleBuildMatch);
                _activeProbeMatched = true;
                _matchedRightRows?.Add(singleBuildMatch);
                return true;
            }
        }

        while (_activeBuildMatches != null && _activeBuildMatchIndex < _activeBuildMatches.Count)
        {
            var buildMatch = _activeBuildMatches[_activeBuildMatchIndex++];
            if (!PassesResidual(_activeProbeRow, buildMatch))
                continue;

            AppendCombinedProbeAndBuildRow(batch, _activeProbeRow, buildMatch);
            _activeProbeMatched = true;
            _matchedRightRows?.Add(buildMatch);
            return true;
        }

        return false;
    }

    private void ResetProbeState()
    {
        _activeProbeRow = null;
        _activeSingleBuildRow = null;
        _activeBuildMatches = null;
        _activeBuildMatchIndex = 0;
        _activeProbeMatched = false;
        _probeExhausted = false;
        _rightOuterEmitIndex = 0;
    }

    private void ClearActiveProbeState()
    {
        _activeProbeRow = null;
        _activeSingleBuildRow = null;
        _activeBuildMatches = null;
        _activeBuildMatchIndex = 0;
        _activeProbeMatched = false;
    }

    private bool PassesResidual(DbValue[] probeRow, DbValue[] buildRow)
    {
        if (_residualPredicate == null)
            return true;

        if (!_buildRowCompactionEnabled)
        {
            return _buildRightSide
                ? _residualPredicate(probeRow, buildRow).IsTruthy
                : _residualPredicate(buildRow, probeRow).IsTruthy;
        }

        return _buildRightSide
            ? _compactedResidualPredicate!(probeRow, buildRow).IsTruthy
            : _compactedResidualPredicate!(buildRow, probeRow).IsTruthy;
    }

    private DbValue[] CombineProbeAndBuildRows(DbValue[] probeRow, DbValue[] buildRow)
    {
        // Output row layout is always [left | right] regardless of build side.
        var leftRow = _buildRightSide ? probeRow : buildRow;
        var rightRow = _buildRightSide ? buildRow : probeRow;
        var projection = _projectionColumnIndices;
        return projection == null
            ? CombineRows(leftRow, rightRow)
            : ProjectRows(
                leftRow,
                rightRow,
                projection,
                leftIsBuildSide: !_buildRightSide,
                rightIsBuildSide: _buildRightSide);
    }

    private void AppendCombinedProbeAndBuildRow(RowBatch batch, DbValue[] probeRow, DbValue[] buildRow)
    {
        int rowIndex = batch.Count;
        WriteCombinedProbeAndBuildRow(probeRow, buildRow, batch.GetWritableRowSpan(rowIndex));
        batch.CommitWrittenRow(rowIndex);
    }

    private void AppendLeftOuterRowFromProbe(RowBatch batch, DbValue[] probeRow)
    {
        int rowIndex = batch.Count;
        WriteLeftOuterRowFromProbe(probeRow, batch.GetWritableRowSpan(rowIndex));
        batch.CommitWrittenRow(rowIndex);
    }

    private void AppendRightOuterRowFromBuild(RowBatch batch, DbValue[] buildRow)
    {
        int rowIndex = batch.Count;
        WriteRightOuterRowFromBuild(buildRow, batch.GetWritableRowSpan(rowIndex));
        batch.CommitWrittenRow(rowIndex);
    }

    private void WriteCombinedProbeAndBuildRow(DbValue[] probeRow, DbValue[] buildRow, Span<DbValue> destination)
    {
        var leftRow = _buildRightSide ? probeRow : buildRow;
        var rightRow = _buildRightSide ? buildRow : probeRow;
        var projection = _projectionColumnIndices;
        if (projection == null)
        {
            WriteRows(
                leftRow,
                rightRow,
                destination,
                leftIsBuildSide: !_buildRightSide,
                rightIsBuildSide: _buildRightSide);
            return;
        }

        WriteProjectedRows(
            leftRow,
            rightRow,
            projection,
            destination,
            leftIsBuildSide: !_buildRightSide,
            rightIsBuildSide: _buildRightSide);
    }

    private void WriteLeftOuterRowFromProbe(DbValue[] probeRow, Span<DbValue> destination)
    {
        var projection = _projectionColumnIndices;
        if (projection == null)
        {
            WriteRows(probeRow, Array.Empty<DbValue>(), destination);
            return;
        }

        WriteProjectedRows(probeRow, Array.Empty<DbValue>(), projection, destination);
    }

    private void WriteRightOuterRowFromBuild(DbValue[] buildRow, Span<DbValue> destination)
    {
        var projection = _projectionColumnIndices;
        if (projection == null)
        {
            WriteRows(Array.Empty<DbValue>(), buildRow, destination, rightIsBuildSide: true);
            return;
        }

        WriteProjectedRows(Array.Empty<DbValue>(), buildRow, projection, destination, rightIsBuildSide: true);
    }

    private DbValue[] CreateLeftOuterRowFromProbe(DbValue[] probeRow)
    {
        if (_buildRightSide)
        {
            var projection = _projectionColumnIndices;
            return projection == null
                ? CombineWithNulls(probeRow, _rightColCount, padRight: true)
                : ProjectLeftWithNullRight(probeRow, projection);
        }

        // Swapped build side is only used for INNER joins, so this is defensive.
        var leftRow = Array.Empty<DbValue>();
        var rightRow = probeRow;
        var projected = _projectionColumnIndices;
        return projected == null
            ? CombineRows(leftRow, rightRow)
            : ProjectRows(leftRow, rightRow, projected);
    }

    private DbValue[] CreateRightOuterRowFromBuild(DbValue[] buildRow)
    {
        if (_buildRightSide)
        {
            var projection = _projectionColumnIndices;
            return projection == null
                ? CombineWithNulls(buildRow, _leftColCount, padRight: false)
                : ProjectNullLeftWithRight(buildRow, projection, rightIsBuildSide: true);
        }

        // Swapped build side is only used for INNER joins, so this is defensive.
        var leftRow = buildRow;
        var rightRow = Array.Empty<DbValue>();
        var projected = _projectionColumnIndices;
        return projected == null
            ? CombineRows(leftRow, rightRow)
            : ProjectRows(leftRow, rightRow, projected, leftIsBuildSide: true, rightIsBuildSide: false);
    }

    private static DbValue ExtractSingleJoinKey(DbValue[] row, int keyIndex)
    {
        return keyIndex < row.Length ? row[keyIndex] : DbValue.Null;
    }

    private static DbValue ExtractSingleJoinKey(ReadOnlySpan<DbValue> row, int keyIndex)
    {
        return keyIndex < row.Length ? row[keyIndex] : DbValue.Null;
    }

    private static HashJoinKey ExtractJoinKey(ReadOnlySpan<DbValue> row, int[] keyIndices)
    {
        var values = new DbValue[keyIndices.Length];
        for (int i = 0; i < keyIndices.Length; i++)
        {
            int keyIndex = keyIndices[i];
            values[i] = keyIndex < row.Length ? row[keyIndex] : DbValue.Null;
        }

        return new HashJoinKey(values);
    }

    private DbValue[] EnsureProbeBatchRowBuffer(int columnCount)
    {
        if (_probeBatchRowBuffer == null || _probeBatchRowBuffer.Length != columnCount)
            _probeBatchRowBuffer = columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];

        return _probeBatchRowBuffer;
    }

    private static int? DeriveBuildKeyCapacityHint(int? buildRowCapacityHint)
    {
        if (!buildRowCapacityHint.HasValue || buildRowCapacityHint.Value <= 0)
            return null;

        // Build row count is a poor proxy for distinct join keys on duplicate-heavy joins.
        // Keep a bounded hint to avoid oversized dictionary backing arrays.
        const int maxDistinctKeyCapacityHint = 8_192;
        return Math.Min(buildRowCapacityHint.Value, maxDistinctKeyCapacityHint);
    }

    private static int DeriveBuildBucketInitialCapacity(int? buildRowCapacityHint, int? buildKeyCapacityHint)
    {
        if (!buildRowCapacityHint.HasValue ||
            !buildKeyCapacityHint.HasValue ||
            buildKeyCapacityHint.Value <= 0)
        {
            return 1;
        }

        int averageMatchesPerKey = buildRowCapacityHint.Value / buildKeyCapacityHint.Value;
        return Math.Clamp(averageMatchesPerKey, 1, 8);
    }

    private static DbValue[] CombineRows(DbValue[] left, DbValue[] right)
    {
        var combined = new DbValue[left.Length + right.Length];
        left.CopyTo(combined, 0);
        right.CopyTo(combined, left.Length);
        return combined;
    }

    /// <summary>
    /// Creates a combined row with NULLs on one side.
    /// If padRight=true: [dataRow | NULLs]. If padRight=false: [NULLs | dataRow].
    /// </summary>
    private static DbValue[] CombineWithNulls(DbValue[] dataRow, int nullCount, bool padRight)
    {
        var combined = new DbValue[dataRow.Length + nullCount];
        if (padRight)
        {
            dataRow.CopyTo(combined, 0);
            for (int i = dataRow.Length; i < combined.Length; i++)
                combined[i] = DbValue.Null;
        }
        else
        {
            for (int i = 0; i < nullCount; i++)
                combined[i] = DbValue.Null;
            dataRow.CopyTo(combined, nullCount);
        }
        return combined;
    }

    private DbValue[] ProjectLeftWithNullRight(DbValue[] leftRow, int[] projection)
    {
        if (projection.Length == 0)
            return Array.Empty<DbValue>();

        var projected = new DbValue[projection.Length];
        for (int i = 0; i < projection.Length; i++)
        {
            int columnIndex = projection[i];
            if (columnIndex < _leftColCount)
            {
                projected[i] = columnIndex < leftRow.Length
                    ? leftRow[columnIndex]
                    : DbValue.Null;
            }
            else
            {
                projected[i] = DbValue.Null;
            }
        }

        return projected;
    }

    private DbValue[] ProjectNullLeftWithRight(DbValue[] rightRow, int[] projection, bool rightIsBuildSide = false)
    {
        if (projection.Length == 0)
            return Array.Empty<DbValue>();

        var projected = new DbValue[projection.Length];
        for (int i = 0; i < projection.Length; i++)
        {
            int columnIndex = projection[i];
            if (columnIndex < _leftColCount)
            {
                projected[i] = DbValue.Null;
            }
            else
            {
                int rightIndex = columnIndex - _leftColCount;
                projected[i] = rightIsBuildSide
                    ? GetBuildSideColumnValue(rightRow, rightIndex)
                    : rightIndex < rightRow.Length
                        ? rightRow[rightIndex]
                        : DbValue.Null;
            }
        }

        return projected;
    }

    private DbValue[] ProjectRows(
        DbValue[] leftRow,
        DbValue[] rightRow,
        int[] projection,
        bool leftIsBuildSide = false,
        bool rightIsBuildSide = false)
    {
        if (projection.Length == 0)
            return Array.Empty<DbValue>();

        var projected = new DbValue[projection.Length];
        if (ReferenceEquals(projection, _projectionColumnIndices) && _projectionAccessors != null)
        {
            WriteProjectedRowsWithAccessors(leftRow, rightRow, _projectionAccessors, projected);
            return projected;
        }

        for (int i = 0; i < projection.Length; i++)
        {
            int columnIndex = projection[i];
            if (columnIndex < _leftColCount)
            {
                projected[i] = leftIsBuildSide
                    ? GetBuildSideColumnValue(leftRow, columnIndex)
                    : columnIndex < leftRow.Length
                        ? leftRow[columnIndex]
                        : DbValue.Null;
            }
            else
            {
                int rightIndex = columnIndex - _leftColCount;
                projected[i] = rightIsBuildSide
                    ? GetBuildSideColumnValue(rightRow, rightIndex)
                    : rightIndex < rightRow.Length
                        ? rightRow[rightIndex]
                        : DbValue.Null;
            }
        }

        return projected;
    }

    private void WriteRows(
        DbValue[] leftRow,
        DbValue[] rightRow,
        Span<DbValue> destination,
        bool leftIsBuildSide = false,
        bool rightIsBuildSide = false)
    {
        for (int i = 0; i < _leftColCount; i++)
        {
            destination[i] = leftIsBuildSide
                ? GetBuildSideColumnValue(leftRow, i)
                : i < leftRow.Length
                    ? leftRow[i]
                    : DbValue.Null;
        }

        for (int i = 0; i < _rightColCount; i++)
        {
            destination[_leftColCount + i] = rightIsBuildSide
                ? GetBuildSideColumnValue(rightRow, i)
                : i < rightRow.Length
                    ? rightRow[i]
                    : DbValue.Null;
        }
    }

    private void WriteProjectedRows(
        DbValue[] leftRow,
        DbValue[] rightRow,
        int[] projection,
        Span<DbValue> destination,
        bool leftIsBuildSide = false,
        bool rightIsBuildSide = false)
    {
        if (ReferenceEquals(projection, _projectionColumnIndices) && _projectionAccessors != null)
        {
            WriteProjectedRowsWithAccessors(leftRow, rightRow, _projectionAccessors, destination);
            return;
        }

        for (int i = 0; i < projection.Length; i++)
        {
            int columnIndex = projection[i];
            if (columnIndex < _leftColCount)
            {
                destination[i] = leftIsBuildSide
                    ? GetBuildSideColumnValue(leftRow, columnIndex)
                    : columnIndex < leftRow.Length
                        ? leftRow[columnIndex]
                        : DbValue.Null;
            }
            else
            {
                int rightIndex = columnIndex - _leftColCount;
                destination[i] = rightIsBuildSide
                    ? GetBuildSideColumnValue(rightRow, rightIndex)
                    : rightIndex < rightRow.Length
                        ? rightRow[rightIndex]
                        : DbValue.Null;
            }
        }
    }

    private static void WriteProjectedRowsWithAccessors(
        DbValue[] leftRow,
        DbValue[] rightRow,
        ProjectionAccessor[] accessors,
        Span<DbValue> destination)
    {
        for (int i = 0; i < accessors.Length; i++)
        {
            ref readonly ProjectionAccessor accessor = ref accessors[i];
            DbValue[] sourceRow = accessor.SourceIsLeft ? leftRow : rightRow;
            int sourceIndex = accessor.SourceIndex;
            destination[i] = sourceIndex >= 0 && sourceIndex < sourceRow.Length
                ? sourceRow[sourceIndex]
                : DbValue.Null;
        }
    }

    private DbValue GetBuildSideColumnValue(DbValue[] buildRow, int buildColumnIndex)
    {
        if (!_buildRowCompactionEnabled)
            return buildColumnIndex < buildRow.Length ? buildRow[buildColumnIndex] : DbValue.Null;

        var columnToCompactIndex = _buildColumnToCompactIndexMap
            ?? throw new InvalidOperationException("Build row compaction map is not configured.");
        if ((uint)buildColumnIndex >= (uint)columnToCompactIndex.Length)
            return DbValue.Null;

        int compactIndex = columnToCompactIndex[buildColumnIndex];
        return compactIndex >= 0 && compactIndex < buildRow.Length
            ? buildRow[compactIndex]
            : DbValue.Null;
    }

    private readonly struct ProjectionAccessor(bool sourceIsLeft, int sourceIndex)
    {
        public bool SourceIsLeft { get; } = sourceIsLeft;
        public int SourceIndex { get; } = sourceIndex;
    }

    private struct SingleKeyBucket
    {
        private const int DefaultMultiMatchInitialCapacity = 16;

        public DbValue[]? SingleMatch;
        public List<DbValue[]>? MultipleMatches;

        public static SingleKeyBucket Create(DbValue[] buildRow)
        {
            return new SingleKeyBucket
            {
                SingleMatch = buildRow,
                MultipleMatches = null,
            };
        }

        public void Add(DbValue[] buildRow, int multiMatchInitialCapacity = DefaultMultiMatchInitialCapacity)
        {
            if (MultipleMatches != null)
            {
                MultipleMatches.Add(buildRow);
                return;
            }

            if (SingleMatch != null)
            {
                // Skewed joins often produce multiple matches per key.
                // Start above 1 to avoid immediate growth churn on duplicate-heavy joins.
                MultipleMatches = new List<DbValue[]>(Math.Max(2, multiMatchInitialCapacity))
                {
                    SingleMatch,
                    buildRow,
                };
                SingleMatch = null;
                return;
            }

            SingleMatch = buildRow;
        }
    }

    private readonly struct HashJoinKey
    {
        public readonly DbValue[] Values;

        public HashJoinKey(DbValue[] values)
        {
            Values = values;
        }
    }

    private sealed class HashJoinKeyComparer : IEqualityComparer<HashJoinKey>
    {
        public static readonly HashJoinKeyComparer Instance = new();

        public bool Equals(HashJoinKey x, HashJoinKey y)
        {
            if (ReferenceEquals(x.Values, y.Values))
                return true;

            if (x.Values.Length != y.Values.Length)
                return false;

            for (int i = 0; i < x.Values.Length; i++)
            {
                if (!x.Values[i].Equals(y.Values[i]))
                    return false;
            }

            return true;
        }

        public int GetHashCode(HashJoinKey obj)
        {
            var hash = new HashCode();
            for (int i = 0; i < obj.Values.Length; i++)
                hash.Add(obj.Values[i]);
            return hash.ToHashCode();
        }
    }
}

/// <summary>
/// Index nested-loop join operator.
/// Uses a right-side PRIMARY KEY or unique single-column index for lookup joins.
/// Supports INNER and LEFT OUTER joins.
/// </summary>
public sealed class IndexNestedLoopJoinOperator : IOperator, IBatchOperator, IBatchBackedRowOperator, IProjectionPushdownTarget, IEstimatedRowCountProvider, IBatchBufferReuseController
{
    private const int DefaultBatchSize = 64;

    private readonly IOperator _outer;
    private readonly BTree _innerTableTree;
    private readonly IIndexStore? _innerIndexStore;
    private readonly ICacheAwareIndexStore? _innerCacheAwareIndexStore;
    private readonly JoinType _joinType;
    private readonly int _outerKeyIndex;
    private readonly int _leftColCount;
    private readonly int _rightColCount;
    private readonly int? _estimatedRowCount;
    private readonly Expression? _residualConditionExpression;
    private readonly JoinSpanExpressionEvaluator? _residualPredicate;
    private readonly TableSchema _compositeSchema;
    private readonly IRecordSerializer _recordSerializer;
    private DbValue[]? _activeOuterRow;
    private bool _activeOuterMatched;
    private bool _pendingPrimaryRowId;
    private long _primaryRowId;
    private ReadOnlyMemory<byte> _pendingIndexPayload;
    private int _pendingIndexOffset;
    private DbValue[]? _rightRowBuffer;
    private int[]? _projectionColumnIndices;
    private int[]? _decodedRightColumnIndices;
    private int? _maxDecodedRightColumnIndex;
    private IBatchOperator? _outerBatchSource;
    private RowBatch? _pendingOuterBatch;
    private int _pendingOuterBatchRowIndex;
    private DbValue[]? _outerBatchRowBuffer;
    private bool _reuseCurrentBatch = true;
    private RowBatch _currentBatch = new(0, DefaultBatchSize);

    public ColumnDefinition[] OutputSchema { get; private set; }
    public bool ReusesCurrentRowBuffer => false;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
    public int? EstimatedRowCount => _estimatedRowCount;
    IBatchOperator IBatchBackedRowOperator.BatchSource => this;

    public IndexNestedLoopJoinOperator(
        IOperator outer,
        BTree innerTableTree,
        IIndexStore? innerIndexStore,
        JoinType joinType,
        int outerKeyIndex,
        int leftColCount,
        int rightColCount,
        Expression? residualCondition,
        TableSchema compositeSchema,
        IRecordSerializer? recordSerializer = null,
        int? estimatedOutputRowCount = null)
    {
        _outer = outer;
        _innerTableTree = innerTableTree;
        _innerIndexStore = innerIndexStore;
        _innerCacheAwareIndexStore = innerIndexStore as ICacheAwareIndexStore;
        _joinType = joinType;
        _outerKeyIndex = outerKeyIndex;
        _leftColCount = leftColCount;
        _rightColCount = rightColCount;
        _estimatedRowCount = estimatedOutputRowCount > 0 ? estimatedOutputRowCount : null;
        _residualConditionExpression = residualCondition;
        _residualPredicate = residualCondition != null
            ? ExpressionCompiler.CompileJoinSpan(residualCondition, compositeSchema, leftColCount)
            : null;
        _compositeSchema = compositeSchema;
        _recordSerializer = recordSerializer ?? new DefaultRecordSerializer();
        OutputSchema = compositeSchema.Columns as ColumnDefinition[] ?? compositeSchema.Columns.ToArray();
    }

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        if (_outer is IRowBufferReuseController outerController)
            // Outer rows are consumed before advancing the outer source again.
            outerController.SetReuseCurrentRowBuffer(true);

        await _outer.OpenAsync(ct);
        Current = Array.Empty<DbValue>();
        int decodedRightColumnCount = GetDecodedRightColumnCount();
        _rightRowBuffer = decodedRightColumnCount == 0
            ? Array.Empty<DbValue>()
            : new DbValue[decodedRightColumnCount];
        _outerBatchSource = BatchSourceHelper.TryGetBatchSource(_outer);
        _pendingOuterBatch = null;
        _pendingOuterBatchRowIndex = 0;
        _outerBatchRowBuffer = null;
        _currentBatch = CreateBatch(OutputSchema.Length);
        ResetActiveOuterState();
    }

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        while (true)
        {
            if (_activeOuterRow != null)
            {
                while (TryReadPendingRowId(out long rowId))
                {
                    ReadOnlyMemory<byte>? payload;
                    if (_innerTableTree.TryFindCachedMemory(rowId, out var cachedPayload))
                    {
                        payload = cachedPayload;
                    }
                    else
                    {
                        payload = await _innerTableTree.FindMemoryAsync(rowId, ct);
                    }
                    if (payload is not { } payloadMemory)
                        continue;

                    var rightRow = DecodeRightRowIntoBuffer(payloadMemory.Span);
                    if (!PassesResidual(_activeOuterRow, rightRow))
                        continue;

                    var combined = CreateMatchedRow(_activeOuterRow, rightRow);
                    _activeOuterMatched = true;
                    Current = combined;
                    return true;
                }

                if (!_activeOuterMatched && _joinType == JoinType.LeftOuter)
                {
                    Current = CreateLeftOuterRow(_activeOuterRow);
                    ResetActiveOuterState();
                    return true;
                }

                ResetActiveOuterState();
                continue;
            }

            var outerRow = await TryMoveNextOuterRowAsync(ct);
            if (outerRow == null)
                return false;
            var keyValue = _outerKeyIndex < outerRow.Length ? outerRow[_outerKeyIndex] : DbValue.Null;
            if (!TryConvertLookupKey(keyValue, out long lookupKey))
            {
                if (_joinType == JoinType.LeftOuter)
                {
                    Current = CreateLeftOuterRow(outerRow);
                    return true;
                }

                continue;
            }

            _activeOuterRow = outerRow;
            _activeOuterMatched = false;

            if (_innerIndexStore == null)
            {
                _pendingPrimaryRowId = true;
                _primaryRowId = lookupKey;
                _pendingIndexPayload = ReadOnlyMemory<byte>.Empty;
                _pendingIndexOffset = 0;
            }
            else
            {
                byte[]? indexPayload;
                if (_innerCacheAwareIndexStore != null &&
                    _innerCacheAwareIndexStore.TryFindCached(lookupKey, out var cachedIndexPayload))
                {
                    indexPayload = cachedIndexPayload;
                }
                else
                {
                    indexPayload = await _innerIndexStore.FindAsync(lookupKey, ct);
                }

                _pendingPrimaryRowId = false;
                _pendingIndexPayload = indexPayload == null
                    ? ReadOnlyMemory<byte>.Empty
                    : indexPayload;
                _pendingIndexOffset = 0;
            }
        }
    }

    public async ValueTask<bool> MoveNextBatchAsync(CancellationToken ct = default)
    {
        var batch = _reuseCurrentBatch ? EnsureBatch(OutputSchema.Length) : CreateBatch(OutputSchema.Length);
        batch.Reset();

        while (batch.Count < batch.Capacity)
        {
            if (_activeOuterRow != null)
            {
                if (await TryAppendCurrentOuterMatchAsync(batch, ct))
                    continue;

                ResetActiveOuterState();
                continue;
            }

            var outerRow = await TryMoveNextOuterRowAsync(ct);
            if (outerRow == null)
                break;

            var keyValue = _outerKeyIndex < outerRow.Length ? outerRow[_outerKeyIndex] : DbValue.Null;
            if (!TryConvertLookupKey(keyValue, out long lookupKey))
            {
                if (_joinType == JoinType.LeftOuter)
                {
                    AppendLeftOuterRow(batch, outerRow);
                    continue;
                }

                continue;
            }

            _activeOuterRow = outerRow;
            _activeOuterMatched = false;

            if (_innerIndexStore == null)
            {
                _pendingPrimaryRowId = true;
                _primaryRowId = lookupKey;
                _pendingIndexPayload = ReadOnlyMemory<byte>.Empty;
                _pendingIndexOffset = 0;
            }
            else
            {
                byte[]? indexPayload;
                if (_innerCacheAwareIndexStore != null &&
                    _innerCacheAwareIndexStore.TryFindCached(lookupKey, out var cachedIndexPayload))
                {
                    indexPayload = cachedIndexPayload;
                }
                else
                {
                    indexPayload = await _innerIndexStore.FindAsync(lookupKey, ct);
                }

                _pendingPrimaryRowId = false;
                _pendingIndexPayload = indexPayload == null
                    ? ReadOnlyMemory<byte>.Empty
                    : indexPayload;
                _pendingIndexOffset = 0;
            }
        }

        _currentBatch = batch;
        return batch.Count > 0;
    }

    public ValueTask DisposeAsync() => _outer.DisposeAsync();

    bool IBatchOperator.ReusesCurrentBatch => _reuseCurrentBatch;
    RowBatch IBatchOperator.CurrentBatch => _currentBatch;

    public void SetReuseCurrentBatch(bool reuse)
    {
        _reuseCurrentBatch = reuse;
        if (!reuse)
            _currentBatch = CreateBatch(OutputSchema.Length);
    }

    public bool TrySetOutputProjection(int[] columnIndices, ColumnDefinition[] outputSchema)
    {
        if (columnIndices == null)
            throw new ArgumentNullException(nameof(columnIndices));
        if (outputSchema == null)
            throw new ArgumentNullException(nameof(outputSchema));

        int compositeColumnCount = _leftColCount + _rightColCount;
        for (int i = 0; i < columnIndices.Length; i++)
        {
            int columnIndex = columnIndices[i];
            if (columnIndex < 0 || columnIndex >= compositeColumnCount)
                return false;
        }

        _projectionColumnIndices = (int[])columnIndices.Clone();
        OutputSchema = outputSchema;
        TryApplyDecodeBoundPushdown();
        return true;
    }

    private void TryApplyDecodeBoundPushdown()
    {
        if (_projectionColumnIndices == null)
            return;

        var outerFlags = new bool[_leftColCount];
        var rightFlags = new bool[_rightColCount];
        int outerRequiredCount = 0;
        int rightRequiredCount = 0;

        void MarkOuterRequired(int columnIndex)
        {
            if ((uint)columnIndex >= (uint)_leftColCount || outerFlags[columnIndex])
                return;

            outerFlags[columnIndex] = true;
            outerRequiredCount++;
        }

        void MarkRightRequired(int columnIndex)
        {
            if ((uint)columnIndex >= (uint)_rightColCount || rightFlags[columnIndex])
                return;

            rightFlags[columnIndex] = true;
            rightRequiredCount++;
        }

        MarkOuterRequired(_outerKeyIndex);
        for (int i = 0; i < _projectionColumnIndices.Length; i++)
        {
            int projectionIndex = _projectionColumnIndices[i];
            if (projectionIndex < _leftColCount)
            {
                MarkOuterRequired(projectionIndex);
            }
            else
            {
                MarkRightRequired(projectionIndex - _leftColCount);
            }
        }

        if (_residualConditionExpression != null &&
            !TryMarkConditionColumnsForExpression(_residualConditionExpression, MarkOuterRequired, MarkRightRequired))
        {
            return;
        }

        int[] outerRequiredColumns = BuildRequiredColumnIndices(outerFlags, outerRequiredCount);
        _decodedRightColumnIndices = BuildRequiredColumnIndices(rightFlags, rightRequiredCount);
        _maxDecodedRightColumnIndex = _decodedRightColumnIndices.Length == 0
            ? -1
            : _decodedRightColumnIndices[^1];

        if (!TrySetDecodedColumnIndices(_outer, outerRequiredColumns) && outerRequiredColumns.Length > 0)
            TrySetDecodedColumnUpperBound(_outer, outerRequiredColumns[^1]);
    }

    private bool TryMarkConditionColumnsForExpression(
        Expression expression,
        Action<int> markOuterColumn,
        Action<int> markRightColumn)
    {
        switch (expression)
        {
            case LiteralExpression:
            case ParameterExpression:
                return true;
            case ColumnRefExpression columnRef:
            {
                int compositeColumnIndex = columnRef.TableAlias != null
                    ? _compositeSchema.GetQualifiedColumnIndex(columnRef.TableAlias, columnRef.ColumnName)
                    : _compositeSchema.GetColumnIndex(columnRef.ColumnName);
                if (compositeColumnIndex < 0)
                    return false;

                if (compositeColumnIndex < _leftColCount)
                    markOuterColumn(compositeColumnIndex);
                else
                    markRightColumn(compositeColumnIndex - _leftColCount);

                return true;
            }
            case BinaryExpression binaryExpression:
                return TryMarkConditionColumnsForExpression(binaryExpression.Left, markOuterColumn, markRightColumn)
                    && TryMarkConditionColumnsForExpression(binaryExpression.Right, markOuterColumn, markRightColumn);
            case UnaryExpression unaryExpression:
                return TryMarkConditionColumnsForExpression(unaryExpression.Operand, markOuterColumn, markRightColumn);
            case CollateExpression collateExpression:
                return TryMarkConditionColumnsForExpression(collateExpression.Operand, markOuterColumn, markRightColumn);
            case LikeExpression likeExpression:
                return TryMarkConditionColumnsForExpression(likeExpression.Operand, markOuterColumn, markRightColumn)
                    && TryMarkConditionColumnsForExpression(likeExpression.Pattern, markOuterColumn, markRightColumn)
                    && (likeExpression.EscapeChar == null
                        || TryMarkConditionColumnsForExpression(likeExpression.EscapeChar, markOuterColumn, markRightColumn));
            case InExpression inExpression:
            {
                if (!TryMarkConditionColumnsForExpression(inExpression.Operand, markOuterColumn, markRightColumn))
                    return false;

                for (int i = 0; i < inExpression.Values.Count; i++)
                {
                    if (!TryMarkConditionColumnsForExpression(inExpression.Values[i], markOuterColumn, markRightColumn))
                        return false;
                }

                return true;
            }
            case BetweenExpression betweenExpression:
                return TryMarkConditionColumnsForExpression(betweenExpression.Operand, markOuterColumn, markRightColumn)
                    && TryMarkConditionColumnsForExpression(betweenExpression.Low, markOuterColumn, markRightColumn)
                    && TryMarkConditionColumnsForExpression(betweenExpression.High, markOuterColumn, markRightColumn);
            case IsNullExpression isNullExpression:
                return TryMarkConditionColumnsForExpression(isNullExpression.Operand, markOuterColumn, markRightColumn);
            case FunctionCallExpression functionCall:
            {
                if (functionCall.IsStarArg)
                    return true;

                for (int i = 0; i < functionCall.Arguments.Count; i++)
                {
                    if (!TryMarkConditionColumnsForExpression(functionCall.Arguments[i], markOuterColumn, markRightColumn))
                        return false;
                }

                return true;
            }
            default:
                return false;
        }
    }

    private static int[] BuildRequiredColumnIndices(bool[] flags, int count)
    {
        if (count <= 0)
            return Array.Empty<int>();

        var columns = new int[count];
        int cursor = 0;
        for (int i = 0; i < flags.Length; i++)
        {
            if (!flags[i])
                continue;

            columns[cursor++] = i;
        }

        return columns;
    }

    private static bool TrySetDecodedColumnIndices(IOperator op, int[] columnIndices)
    {
        if (columnIndices.Length == 0)
            return true;

        switch (op)
        {
            case TableScanOperator tableScan:
                tableScan.SetDecodedColumnIndices(columnIndices);
                return true;
            case PrimaryKeyLookupOperator primaryKeyLookup:
                primaryKeyLookup.SetDecodedColumnIndices(columnIndices);
                return true;
            case IndexScanOperator indexScan:
                indexScan.SetDecodedColumnIndices(columnIndices);
                return true;
            case UniqueIndexLookupOperator uniqueIndexLookup:
                uniqueIndexLookup.SetDecodedColumnIndices(columnIndices);
                return true;
            case IndexOrderedScanOperator orderedScan:
                orderedScan.SetDecodedColumnIndices(columnIndices);
                return true;
            default:
                return false;
        }
    }

    private static void TrySetDecodedColumnUpperBound(IOperator op, int maxColumnIndex)
    {
        if (maxColumnIndex < 0)
            return;

        switch (op)
        {
            case TableScanOperator tableScan:
                tableScan.SetDecodedColumnUpperBound(maxColumnIndex);
                break;
            case PrimaryKeyLookupOperator primaryKeyLookup:
                primaryKeyLookup.SetDecodedColumnUpperBound(maxColumnIndex);
                break;
            case IndexScanOperator indexScan:
                indexScan.SetDecodedColumnUpperBound(maxColumnIndex);
                break;
            case UniqueIndexLookupOperator uniqueIndexLookup:
                uniqueIndexLookup.SetDecodedColumnUpperBound(maxColumnIndex);
                break;
            case IndexOrderedScanOperator orderedScan:
                orderedScan.SetDecodedColumnUpperBound(maxColumnIndex);
                break;
        }
    }

    private static bool TryConvertLookupKey(DbValue value, out long key)
    {
        key = 0;

        if (value.IsNull)
            return false;

        if (value.Type == DbType.Integer)
        {
            key = value.AsInteger;
            return true;
        }

        if (value.Type == DbType.Real)
        {
            double real = value.AsReal;
            if (real < long.MinValue || real > long.MaxValue)
                return false;

            double truncated = Math.Truncate(real);
            if (truncated != real)
                return false;

            key = (long)real;
            return true;
        }

        return false;
    }

    private bool PassesResidual(DbValue[] leftRow, DbValue[] rightRow)
    {
        if (_residualPredicate == null)
            return true;

        return _residualPredicate(leftRow, rightRow).IsTruthy;
    }

    private bool TryReadPendingRowId(out long rowId)
    {
        if (_pendingPrimaryRowId)
        {
            _pendingPrimaryRowId = false;
            rowId = _primaryRowId;
            return true;
        }

        if (_pendingIndexOffset + 8 <= _pendingIndexPayload.Length)
        {
            rowId = BinaryPrimitives.ReadInt64LittleEndian(
                _pendingIndexPayload.Span.Slice(_pendingIndexOffset, 8));
            _pendingIndexOffset += 8;
            return true;
        }

        rowId = 0;
        return false;
    }

    private async ValueTask<DbValue[]?> TryMoveNextOuterRowAsync(CancellationToken ct)
    {
        if (_outerBatchSource == null)
        {
            if (await _outer.MoveNextAsync(ct))
                return _outer.Current;

            return null;
        }

        while (true)
        {
            if (_pendingOuterBatch != null && _pendingOuterBatchRowIndex < _pendingOuterBatch.Count)
            {
                var batch = _pendingOuterBatch;
                var outerRow = EnsureOuterBatchRowBuffer(batch.ColumnCount);
                batch.CopyRowTo(_pendingOuterBatchRowIndex, outerRow);
                _pendingOuterBatchRowIndex++;
                if (_pendingOuterBatchRowIndex >= batch.Count)
                {
                    _pendingOuterBatch = null;
                    _pendingOuterBatchRowIndex = 0;
                }

                return outerRow;
            }

            if (!await _outerBatchSource.MoveNextBatchAsync(ct))
                return null;

            _pendingOuterBatch = _outerBatchSource.CurrentBatch;
            _pendingOuterBatchRowIndex = 0;
        }
    }

    private async ValueTask<bool> TryAppendCurrentOuterMatchAsync(RowBatch batch, CancellationToken ct)
    {
        if (_activeOuterRow == null)
            return false;

        while (TryReadPendingRowId(out long rowId))
        {
            ReadOnlyMemory<byte>? payload;
            if (_innerTableTree.TryFindCachedMemory(rowId, out var cachedPayload))
            {
                payload = cachedPayload;
            }
            else
            {
                payload = await _innerTableTree.FindMemoryAsync(rowId, ct);
            }

            if (payload is not { } payloadMemory)
                continue;

            var rightRow = DecodeRightRowIntoBuffer(payloadMemory.Span);
            if (!PassesResidual(_activeOuterRow, rightRow))
                continue;

            AppendMatchedRow(batch, _activeOuterRow, rightRow);
            _activeOuterMatched = true;
            return true;
        }

        if (!_activeOuterMatched && _joinType == JoinType.LeftOuter)
        {
            AppendLeftOuterRow(batch, _activeOuterRow);
            _activeOuterMatched = true;
            return true;
        }

        return false;
    }

    private void ResetActiveOuterState()
    {
        _activeOuterRow = null;
        _activeOuterMatched = false;
        _pendingPrimaryRowId = false;
        _primaryRowId = 0;
        _pendingIndexPayload = ReadOnlyMemory<byte>.Empty;
        _pendingIndexOffset = 0;
    }

    private RowBatch EnsureBatch(int columnCount)
    {
        if (_currentBatch.ColumnCount != columnCount)
            _currentBatch = CreateBatch(columnCount);

        return _currentBatch;
    }

    private static RowBatch CreateBatch(int columnCount) => new(columnCount, DefaultBatchSize);

    private DbValue[] DecodeRightRowIntoBuffer(ReadOnlySpan<byte> payload)
    {
        var row = _rightRowBuffer;
        if (row == null)
        {
            int decodedRightColumnCount = GetDecodedRightColumnCount();
            row = decodedRightColumnCount == 0
                ? Array.Empty<DbValue>()
                : new DbValue[decodedRightColumnCount];
            _rightRowBuffer = row;
        }

        var decodedRightColumnIndices = _decodedRightColumnIndices;
        if (decodedRightColumnIndices is { Length: > 0 })
        {
            Array.Fill(row, DbValue.Null);
            _recordSerializer.DecodeSelectedInto(payload, row, decodedRightColumnIndices);
            return row;
        }

        if (row.Length == 0)
            return row;

        int decoded = _recordSerializer.DecodeInto(payload, row);
        if (decoded < row.Length)
            Array.Fill(row, DbValue.Null, decoded, row.Length - decoded);
        return row;
    }

    private DbValue[] EnsureOuterBatchRowBuffer(int columnCount)
    {
        if (_outerBatchRowBuffer == null || _outerBatchRowBuffer.Length != columnCount)
            _outerBatchRowBuffer = columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];

        return _outerBatchRowBuffer;
    }

    private int GetDecodedRightColumnCount()
        => _maxDecodedRightColumnIndex.HasValue
            ? Math.Max(0, _maxDecodedRightColumnIndex.Value + 1)
            : _rightColCount;

    private DbValue[] CreateMatchedRow(DbValue[] leftRow, DbValue[] rightRow)
    {
        var projection = _projectionColumnIndices;
        return projection == null
            ? CombineRows(leftRow, rightRow)
            : ProjectRows(leftRow, rightRow, projection);
    }

    private void AppendMatchedRow(RowBatch batch, DbValue[] leftRow, DbValue[] rightRow)
    {
        int rowIndex = batch.Count;
        WriteMatchedRow(leftRow, rightRow, batch.GetWritableRowSpan(rowIndex));
        batch.CommitWrittenRow(rowIndex);
    }

    private void WriteMatchedRow(DbValue[] leftRow, DbValue[] rightRow, Span<DbValue> destination)
    {
        var projection = _projectionColumnIndices;
        if (projection == null)
        {
            WriteRows(leftRow, rightRow, destination);
            return;
        }

        WriteProjectedRows(leftRow, rightRow, projection, destination);
    }

    private DbValue[] CreateLeftOuterRow(DbValue[] leftRow)
    {
        var projection = _projectionColumnIndices;
        return projection == null
            ? CombineWithNulls(leftRow, _rightColCount, padRight: true)
            : ProjectLeftWithNullRight(leftRow, projection);
    }

    private void AppendLeftOuterRow(RowBatch batch, DbValue[] leftRow)
    {
        int rowIndex = batch.Count;
        WriteLeftOuterRow(leftRow, batch.GetWritableRowSpan(rowIndex));
        batch.CommitWrittenRow(rowIndex);
    }

    private void WriteLeftOuterRow(DbValue[] leftRow, Span<DbValue> destination)
    {
        var projection = _projectionColumnIndices;
        if (projection == null)
        {
            WriteRows(leftRow, Array.Empty<DbValue>(), destination);
            return;
        }

        WriteProjectedRows(leftRow, Array.Empty<DbValue>(), projection, destination);
    }

    private static DbValue[] CombineRows(DbValue[] left, DbValue[] right)
    {
        var combined = new DbValue[left.Length + right.Length];
        left.CopyTo(combined, 0);
        right.CopyTo(combined, left.Length);
        return combined;
    }

    /// <summary>
    /// Creates a combined row with NULLs on one side.
    /// If padRight=true: [dataRow | NULLs]. If padRight=false: [NULLs | dataRow].
    /// </summary>
    private static DbValue[] CombineWithNulls(DbValue[] dataRow, int nullCount, bool padRight)
    {
        var combined = new DbValue[dataRow.Length + nullCount];
        if (padRight)
        {
            dataRow.CopyTo(combined, 0);
            for (int i = dataRow.Length; i < combined.Length; i++)
                combined[i] = DbValue.Null;
        }
        else
        {
            for (int i = 0; i < nullCount; i++)
                combined[i] = DbValue.Null;
            dataRow.CopyTo(combined, nullCount);
        }
        return combined;
    }

    private DbValue[] ProjectLeftWithNullRight(DbValue[] leftRow, int[] projection)
    {
        if (projection.Length == 0)
            return Array.Empty<DbValue>();

        var projected = new DbValue[projection.Length];
        for (int i = 0; i < projection.Length; i++)
        {
            int columnIndex = projection[i];
            if (columnIndex < _leftColCount)
            {
                projected[i] = columnIndex < leftRow.Length
                    ? leftRow[columnIndex]
                    : DbValue.Null;
            }
            else
            {
                projected[i] = DbValue.Null;
            }
        }

        return projected;
    }

    private DbValue[] ProjectRows(DbValue[] leftRow, DbValue[] rightRow, int[] projection)
    {
        if (projection.Length == 0)
            return Array.Empty<DbValue>();

        var projected = new DbValue[projection.Length];
        for (int i = 0; i < projection.Length; i++)
        {
            int columnIndex = projection[i];
            if (columnIndex < _leftColCount)
            {
                projected[i] = columnIndex < leftRow.Length
                    ? leftRow[columnIndex]
                    : DbValue.Null;
            }
            else
            {
                int rightIndex = columnIndex - _leftColCount;
                projected[i] = rightIndex < rightRow.Length
                    ? rightRow[rightIndex]
                    : DbValue.Null;
            }
        }

        return projected;
    }

    private void WriteRows(DbValue[] leftRow, DbValue[] rightRow, Span<DbValue> destination)
    {
        for (int i = 0; i < _leftColCount; i++)
            destination[i] = i < leftRow.Length ? leftRow[i] : DbValue.Null;

        for (int i = 0; i < _rightColCount; i++)
            destination[_leftColCount + i] = i < rightRow.Length ? rightRow[i] : DbValue.Null;
    }

    private void WriteProjectedRows(DbValue[] leftRow, DbValue[] rightRow, int[] projection, Span<DbValue> destination)
    {
        for (int i = 0; i < projection.Length; i++)
        {
            int columnIndex = projection[i];
            if (columnIndex < _leftColCount)
            {
                destination[i] = columnIndex < leftRow.Length
                    ? leftRow[columnIndex]
                    : DbValue.Null;
            }
            else
            {
                int rightIndex = columnIndex - _leftColCount;
                destination[i] = rightIndex < rightRow.Length
                    ? rightRow[rightIndex]
                    : DbValue.Null;
            }
        }
    }
}

/// <summary>
/// Index nested-loop join operator for hashed secondary indexes.
/// Supports exact equality lookups over single-column text indexes and
/// composite integer/text indexes on the right side.
/// </summary>
public sealed class HashedIndexNestedLoopJoinOperator : IOperator, IBatchOperator, IBatchBackedRowOperator, IProjectionPushdownTarget, IEstimatedRowCountProvider, IBatchBufferReuseController
{
    private const int DefaultBatchSize = 64;
    private const int CoveredProjectionLeftSlot = -2;
    private const int CoveredProjectionRowIdSlot = -1;

    private readonly IOperator _outer;
    private readonly BTree _innerTableTree;
    private readonly IIndexStore _innerIndexStore;
    private readonly JoinType _joinType;
    private readonly int[] _outerKeyIndices;
    private readonly int[] _rightKeyColumnIndices;
    private readonly string?[] _rightKeyCollations;
    private readonly bool _usesOrderedTextPayload;
    private readonly int _leftColCount;
    private readonly int _rightColCount;
    private readonly int _rightPrimaryKeyColumnIndex;
    private readonly int? _estimatedRowCount;
    private readonly JoinSpanExpressionEvaluator? _residualPredicate;
    private readonly IRecordSerializer _recordSerializer;
    private DbValue[]? _activeOuterRow;
    private bool _activeOuterMatched;
    private ReadOnlyMemory<byte> _pendingRowIdPayload;
    private int _pendingRowIdOffset;
    private bool _rowIdsVerifiedByIndexPayload;
    private DbValue[]? _currentKeyComponents;
    private byte[][]? _currentKeyTextBytes;
    private DbValue[]? _lookupKeyComponentBuffer;
    private byte[][]? _lookupKeyTextBytesBuffer;
    private Dictionary<string, byte[]>? _lookupTextByteCache;
    private RecordColumnAccessor?[]? _keyAccessors;
    private DbValue[]? _rightRowBuffer;
    private int[]? _projectionColumnIndices;
    private int[]? _decodedRightColumnIndices;
    private int? _maxDecodedRightColumnIndex;
    private bool _canProjectRightFromIndexPayload;
    private int[]? _coveredProjectionKeyComponentMap;
    private IBatchOperator? _outerBatchSource;
    private RowBatch? _pendingOuterBatch;
    private int _pendingOuterBatchRowIndex;
    private DbValue[]? _outerBatchRowBuffer;
    private bool _reuseCurrentBatch = true;
    private RowBatch _currentBatch = new(0, DefaultBatchSize);

    public ColumnDefinition[] OutputSchema { get; private set; }
    public bool ReusesCurrentRowBuffer => false;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
    public int? EstimatedRowCount => _estimatedRowCount;
    IBatchOperator IBatchBackedRowOperator.BatchSource => this;

    public HashedIndexNestedLoopJoinOperator(
        IOperator outer,
        BTree innerTableTree,
        IIndexStore innerIndexStore,
        JoinType joinType,
        ReadOnlySpan<int> outerKeyIndices,
        ReadOnlySpan<int> rightKeyColumnIndices,
        ReadOnlySpan<string?> rightKeyCollations,
        int leftColCount,
        int rightColCount,
        int rightPrimaryKeyColumnIndex,
        Expression? residualCondition,
        TableSchema compositeSchema,
        bool usesOrderedTextPayload = false,
        IRecordSerializer? recordSerializer = null,
        int? estimatedOutputRowCount = null)
    {
        _outer = outer;
        _innerTableTree = innerTableTree;
        _innerIndexStore = innerIndexStore;
        _joinType = joinType;
        _outerKeyIndices = outerKeyIndices.ToArray();
        _rightKeyColumnIndices = rightKeyColumnIndices.ToArray();
        _rightKeyCollations = rightKeyCollations.ToArray();
        _usesOrderedTextPayload = usesOrderedTextPayload;
        _leftColCount = leftColCount;
        _rightColCount = rightColCount;
        _rightPrimaryKeyColumnIndex = rightPrimaryKeyColumnIndex;
        _estimatedRowCount = estimatedOutputRowCount > 0 ? estimatedOutputRowCount : null;
        _residualPredicate = residualCondition != null
            ? ExpressionCompiler.CompileJoinSpan(residualCondition, compositeSchema, leftColCount)
            : null;
        _recordSerializer = recordSerializer ?? new DefaultRecordSerializer();
        OutputSchema = compositeSchema.Columns as ColumnDefinition[] ?? compositeSchema.Columns.ToArray();
    }

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        if (_outer is IRowBufferReuseController outerController)
            outerController.SetReuseCurrentRowBuffer(true);

        await _outer.OpenAsync(ct);
        Current = Array.Empty<DbValue>();
        int decodedRightColumnCount = GetDecodedRightColumnCount();
        _rightRowBuffer = decodedRightColumnCount == 0
            ? Array.Empty<DbValue>()
            : new DbValue[decodedRightColumnCount];
        _outerBatchSource = BatchSourceHelper.TryGetBatchSource(_outer);
        _pendingOuterBatch = null;
        _pendingOuterBatchRowIndex = 0;
        _outerBatchRowBuffer = null;
        _lookupKeyComponentBuffer = null;
        _lookupKeyTextBytesBuffer = null;
        _lookupTextByteCache = null;
        _currentBatch = CreateBatch(OutputSchema.Length);
        ResetActiveOuterState();
    }

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        while (true)
        {
            if (_activeOuterRow != null)
            {
                while (TryReadPendingRowId(out long rowId))
                {
                    if (_rowIdsVerifiedByIndexPayload && _canProjectRightFromIndexPayload)
                    {
                        Current = CreateCoveredMatchedRow(_activeOuterRow, rowId);
                        _activeOuterMatched = true;
                        return true;
                    }

                    ReadOnlyMemory<byte>? payload;
                    if (_innerTableTree.TryFindCachedMemory(rowId, out var cachedPayload))
                    {
                        payload = cachedPayload;
                    }
                    else
                    {
                        payload = await _innerTableTree.FindMemoryAsync(rowId, ct);
                    }

                    if (payload is not { } payloadMemory)
                        continue;

                    if (!_rowIdsVerifiedByIndexPayload && !MatchesExpectedKeyComponents(payloadMemory.Span))
                        continue;

                    var rightRow = DecodeRightRowIntoBuffer(payloadMemory.Span);
                    if (!PassesResidual(_activeOuterRow, rightRow))
                        continue;

                    Current = CreateMatchedRow(_activeOuterRow, rightRow);
                    _activeOuterMatched = true;
                    return true;
                }

                if (!_activeOuterMatched && _joinType == JoinType.LeftOuter)
                {
                    Current = CreateLeftOuterRow(_activeOuterRow);
                    ResetActiveOuterState();
                    return true;
                }

                ResetActiveOuterState();
                continue;
            }

            var outerRow = await TryMoveNextOuterRowAsync(ct);
            if (outerRow == null)
                return false;
            if (!TryBuildLookupKey(outerRow, out long lookupKey, out var keyComponents, out var keyTextBytes))
            {
                if (_joinType == JoinType.LeftOuter)
                {
                    Current = CreateLeftOuterRow(outerRow);
                    return true;
                }

                continue;
            }

            _activeOuterRow = outerRow;
            _activeOuterMatched = false;
            _currentKeyComponents = keyComponents;
            _currentKeyTextBytes = keyTextBytes;

            byte[]? indexPayload;
            if (_innerIndexStore is ICacheAwareIndexStore cacheAware &&
                cacheAware.TryFindCached(lookupKey, out var cachedIndexPayload))
            {
                indexPayload = cachedIndexPayload;
            }
            else
            {
                indexPayload = await _innerIndexStore.FindAsync(lookupKey, ct);
            }

            InitializePendingRowIds(indexPayload);
        }
    }

    public async ValueTask<bool> MoveNextBatchAsync(CancellationToken ct = default)
    {
        var batch = _reuseCurrentBatch ? EnsureBatch(OutputSchema.Length) : CreateBatch(OutputSchema.Length);
        batch.Reset();

        while (batch.Count < batch.Capacity)
        {
            if (_activeOuterRow != null)
            {
                if (await TryAppendCurrentOuterMatchAsync(batch, ct))
                    continue;

                ResetActiveOuterState();
                continue;
            }

            var outerRow = await TryMoveNextOuterRowAsync(ct);
            if (outerRow == null)
                break;

            if (!TryBuildLookupKey(outerRow, out long lookupKey, out var keyComponents, out var keyTextBytes))
            {
                if (_joinType == JoinType.LeftOuter)
                {
                    AppendLeftOuterRow(batch, outerRow);
                    continue;
                }

                continue;
            }

            _activeOuterRow = outerRow;
            _activeOuterMatched = false;
            _currentKeyComponents = keyComponents;
            _currentKeyTextBytes = keyTextBytes;

            byte[]? indexPayload;
            if (_innerIndexStore is ICacheAwareIndexStore cacheAware &&
                cacheAware.TryFindCached(lookupKey, out var cachedIndexPayload))
            {
                indexPayload = cachedIndexPayload;
            }
            else
            {
                indexPayload = await _innerIndexStore.FindAsync(lookupKey, ct);
            }

            InitializePendingRowIds(indexPayload);
        }

        _currentBatch = batch;
        return batch.Count > 0;
    }

    public ValueTask DisposeAsync() => _outer.DisposeAsync();

    bool IBatchOperator.ReusesCurrentBatch => _reuseCurrentBatch;
    RowBatch IBatchOperator.CurrentBatch => _currentBatch;

    public void SetReuseCurrentBatch(bool reuse)
    {
        _reuseCurrentBatch = reuse;
        if (!reuse)
            _currentBatch = CreateBatch(OutputSchema.Length);
    }

    public bool TrySetOutputProjection(int[] columnIndices, ColumnDefinition[] outputSchema)
    {
        if (columnIndices == null)
            throw new ArgumentNullException(nameof(columnIndices));
        if (outputSchema == null)
            throw new ArgumentNullException(nameof(outputSchema));

        int compositeColumnCount = _leftColCount + _rightColCount;
        for (int i = 0; i < columnIndices.Length; i++)
        {
            int columnIndex = columnIndices[i];
            if (columnIndex < 0 || columnIndex >= compositeColumnCount)
                return false;
        }

        _projectionColumnIndices = (int[])columnIndices.Clone();
        OutputSchema = outputSchema;
        TryApplyDecodeBoundPushdown();
        _canProjectRightFromIndexPayload = CanProjectRightFromIndexPayload();
        _coveredProjectionKeyComponentMap = _canProjectRightFromIndexPayload
            ? BuildCoveredProjectionKeyComponentMap(_projectionColumnIndices)
            : null;
        return true;
    }

    private void TryApplyDecodeBoundPushdown()
    {
        if (_projectionColumnIndices == null)
            return;

        if (_residualPredicate != null)
            return;

        var outerFlags = new bool[_leftColCount];
        var rightFlags = new bool[_rightColCount];
        int outerRequiredCount = 0;
        int rightRequiredCount = 0;

        void MarkOuterRequired(int columnIndex)
        {
            if ((uint)columnIndex >= (uint)_leftColCount || outerFlags[columnIndex])
                return;

            outerFlags[columnIndex] = true;
            outerRequiredCount++;
        }

        void MarkRightRequired(int columnIndex)
        {
            if ((uint)columnIndex >= (uint)_rightColCount || rightFlags[columnIndex])
                return;

            rightFlags[columnIndex] = true;
            rightRequiredCount++;
        }

        for (int i = 0; i < _outerKeyIndices.Length; i++)
            MarkOuterRequired(_outerKeyIndices[i]);

        for (int i = 0; i < _projectionColumnIndices.Length; i++)
        {
            int projectionIndex = _projectionColumnIndices[i];
            if (projectionIndex < _leftColCount)
                MarkOuterRequired(projectionIndex);
            else
                MarkRightRequired(projectionIndex - _leftColCount);
        }

        int[] outerRequiredColumns = BuildRequiredColumnIndices(outerFlags, outerRequiredCount);
        _decodedRightColumnIndices = BuildRequiredColumnIndices(rightFlags, rightRequiredCount);
        _maxDecodedRightColumnIndex = _decodedRightColumnIndices.Length == 0
            ? -1
            : _decodedRightColumnIndices[^1];

        if (!TrySetDecodedColumnIndices(_outer, outerRequiredColumns) && outerRequiredColumns.Length > 0)
            TrySetDecodedColumnUpperBound(_outer, outerRequiredColumns[^1]);
    }

    private static int[] BuildRequiredColumnIndices(bool[] flags, int count)
    {
        if (count <= 0)
            return Array.Empty<int>();

        var columns = new int[count];
        int cursor = 0;
        for (int i = 0; i < flags.Length; i++)
        {
            if (!flags[i])
                continue;

            columns[cursor++] = i;
        }

        return columns;
    }

    private static bool TrySetDecodedColumnIndices(IOperator op, int[] columnIndices)
    {
        if (columnIndices.Length == 0)
            return true;

        switch (op)
        {
            case TableScanOperator tableScan:
                tableScan.SetDecodedColumnIndices(columnIndices);
                return true;
            case PrimaryKeyLookupOperator primaryKeyLookup:
                primaryKeyLookup.SetDecodedColumnIndices(columnIndices);
                return true;
            case IndexScanOperator indexScan:
                indexScan.SetDecodedColumnIndices(columnIndices);
                return true;
            case UniqueIndexLookupOperator uniqueIndexLookup:
                uniqueIndexLookup.SetDecodedColumnIndices(columnIndices);
                return true;
            case IndexOrderedScanOperator orderedScan:
                orderedScan.SetDecodedColumnIndices(columnIndices);
                return true;
            default:
                return false;
        }
    }

    private static void TrySetDecodedColumnUpperBound(IOperator op, int maxColumnIndex)
    {
        if (maxColumnIndex < 0)
            return;

        switch (op)
        {
            case TableScanOperator tableScan:
                tableScan.SetDecodedColumnUpperBound(maxColumnIndex);
                break;
            case PrimaryKeyLookupOperator primaryKeyLookup:
                primaryKeyLookup.SetDecodedColumnUpperBound(maxColumnIndex);
                break;
            case IndexScanOperator indexScan:
                indexScan.SetDecodedColumnUpperBound(maxColumnIndex);
                break;
            case UniqueIndexLookupOperator uniqueIndexLookup:
                uniqueIndexLookup.SetDecodedColumnUpperBound(maxColumnIndex);
                break;
            case IndexOrderedScanOperator orderedScan:
                orderedScan.SetDecodedColumnUpperBound(maxColumnIndex);
                break;
        }
    }

    private bool TryBuildLookupKey(DbValue[] outerRow, out long lookupKey, out DbValue[] keyComponents, out byte[][]? keyTextBytes)
    {
        lookupKey = 0;
        keyTextBytes = null;
        keyComponents = EnsureLookupKeyComponentBuffer(_outerKeyIndices.Length);
        byte[][]? textBytes = _lookupKeyTextBytesBuffer;
        if (textBytes != null)
            Array.Clear(textBytes, 0, textBytes.Length);

        bool hasTextComponents = false;
        for (int i = 0; i < _outerKeyIndices.Length; i++)
        {
            int outerKeyIndex = _outerKeyIndices[i];
            var value = outerKeyIndex < outerRow.Length
                ? outerRow[outerKeyIndex]
                : DbValue.Null;

            if (value.IsNull || value.Type is not (DbType.Integer or DbType.Text))
                return false;

            string? collation = i < _rightKeyCollations.Length ? _rightKeyCollations[i] : null;
            var normalized = CollationSupport.NormalizeIndexValue(value, collation);
            keyComponents[i] = normalized;
            if (normalized.Type == DbType.Text)
            {
                textBytes ??= EnsureLookupKeyTextBytesBuffer(_outerKeyIndices.Length);
                textBytes[i] = GetCachedLookupTextBytes(normalized.AsText);
                hasTextComponents = true;
            }
        }

        keyTextBytes = hasTextComponents ? textBytes : null;

        if (_usesOrderedTextPayload)
        {
            if (keyComponents is not [var textComponent] || textComponent.Type != DbType.Text)
                return false;

            lookupKey = OrderedTextIndexKeyCodec.ComputeKey(textComponent.AsText);
            return true;
        }

        lookupKey = IndexMaintenanceHelper.ComputeIndexKey(keyComponents);
        return true;
    }

    private void InitializePendingRowIds(byte[]? payload)
    {
        _pendingRowIdOffset = 0;
        _rowIdsVerifiedByIndexPayload = false;

        if (_usesOrderedTextPayload && payload is { Length: > 0 })
        {
            if (!OrderedTextIndexPayloadCodec.IsEncoded(payload))
            {
                throw new InvalidOperationException(
                    "SQL text index payload format mismatch detected. Rebuild the index before continuing.");
            }

            if (_currentKeyComponents is [var expectedText] &&
                expectedText.Type == DbType.Text &&
                OrderedTextIndexPayloadCodec.TryGetMatchingRowIdPayloadSlice(
                    payload,
                    expectedText.AsText,
                    out var orderedMatchingRowIds))
            {
                _pendingRowIdPayload = orderedMatchingRowIds;
                _rowIdsVerifiedByIndexPayload = true;
                return;
            }

            _pendingRowIdPayload = ReadOnlyMemory<byte>.Empty;
            return;
        }

        if (_currentKeyComponents is { Length: > 0 } &&
            payload is { Length: > 0 } &&
            HashedIndexPayloadCodec.TryGetMatchingRowIdPayloadSlice(payload, _currentKeyComponents, _currentKeyTextBytes, out var matchingRowIds))
        {
            _pendingRowIdPayload = matchingRowIds;
            _rowIdsVerifiedByIndexPayload = true;
            return;
        }

        _pendingRowIdPayload = payload ?? ReadOnlyMemory<byte>.Empty;
    }

    private bool MatchesExpectedKeyComponents(ReadOnlySpan<byte> payload)
    {
        if (_currentKeyComponents is not { Length: > 0 })
            return false;

        return BoundColumnAccessHelper.MatchesKeyComponents(
            payload,
            _recordSerializer,
            _rightKeyColumnIndices,
            _currentKeyComponents,
            EnsureKeyAccessors(),
            _currentKeyTextBytes);
    }

    private RecordColumnAccessor?[] EnsureKeyAccessors()
        => _keyAccessors ??= BoundColumnAccessHelper.CreateAccessors(_recordSerializer, _rightKeyColumnIndices);

    private bool PassesResidual(DbValue[] leftRow, DbValue[] rightRow)
    {
        if (_residualPredicate == null)
            return true;

        return _residualPredicate(leftRow, rightRow).IsTruthy;
    }

    private bool TryReadPendingRowId(out long rowId)
    {
        if (_pendingRowIdOffset + sizeof(long) > _pendingRowIdPayload.Length)
        {
            rowId = 0;
            return false;
        }

        rowId = BinaryPrimitives.ReadInt64LittleEndian(
            _pendingRowIdPayload.Span.Slice(_pendingRowIdOffset, sizeof(long)));
        _pendingRowIdOffset += sizeof(long);
        return true;
    }

    private async ValueTask<DbValue[]?> TryMoveNextOuterRowAsync(CancellationToken ct)
    {
        if (_outerBatchSource == null)
        {
            if (await _outer.MoveNextAsync(ct))
                return _outer.Current;

            return null;
        }

        while (true)
        {
            if (_pendingOuterBatch != null && _pendingOuterBatchRowIndex < _pendingOuterBatch.Count)
            {
                var batch = _pendingOuterBatch;
                var outerRow = EnsureOuterBatchRowBuffer(batch.ColumnCount);
                batch.CopyRowTo(_pendingOuterBatchRowIndex, outerRow);
                _pendingOuterBatchRowIndex++;
                if (_pendingOuterBatchRowIndex >= batch.Count)
                {
                    _pendingOuterBatch = null;
                    _pendingOuterBatchRowIndex = 0;
                }

                return outerRow;
            }

            if (!await _outerBatchSource.MoveNextBatchAsync(ct))
                return null;

            _pendingOuterBatch = _outerBatchSource.CurrentBatch;
            _pendingOuterBatchRowIndex = 0;
        }
    }

    private async ValueTask<bool> TryAppendCurrentOuterMatchAsync(RowBatch batch, CancellationToken ct)
    {
        if (_activeOuterRow == null)
            return false;

        while (TryReadPendingRowId(out long rowId))
        {
            if (_rowIdsVerifiedByIndexPayload && _canProjectRightFromIndexPayload)
            {
                AppendCoveredMatchedRow(batch, _activeOuterRow, rowId);
                _activeOuterMatched = true;
                return true;
            }

            ReadOnlyMemory<byte>? payload;
            if (_innerTableTree.TryFindCachedMemory(rowId, out var cachedPayload))
            {
                payload = cachedPayload;
            }
            else
            {
                payload = await _innerTableTree.FindMemoryAsync(rowId, ct);
            }

            if (payload is not { } payloadMemory)
                continue;

            if (!_rowIdsVerifiedByIndexPayload && !MatchesExpectedKeyComponents(payloadMemory.Span))
                continue;

            var rightRow = DecodeRightRowIntoBuffer(payloadMemory.Span);
            if (!PassesResidual(_activeOuterRow, rightRow))
                continue;

            AppendMatchedRow(batch, _activeOuterRow, rightRow);
            _activeOuterMatched = true;
            return true;
        }

        if (!_activeOuterMatched && _joinType == JoinType.LeftOuter)
        {
            AppendLeftOuterRow(batch, _activeOuterRow);
            _activeOuterMatched = true;
            return true;
        }

        return false;
    }

    private void ResetActiveOuterState()
    {
        _activeOuterRow = null;
        _activeOuterMatched = false;
        _pendingRowIdPayload = ReadOnlyMemory<byte>.Empty;
        _pendingRowIdOffset = 0;
        _rowIdsVerifiedByIndexPayload = false;
        _currentKeyComponents = null;
        _currentKeyTextBytes = null;
    }

    private RowBatch EnsureBatch(int columnCount)
    {
        if (_currentBatch.ColumnCount != columnCount)
            _currentBatch = CreateBatch(columnCount);

        return _currentBatch;
    }

    private static RowBatch CreateBatch(int columnCount) => new(columnCount, DefaultBatchSize);

    private DbValue[] DecodeRightRowIntoBuffer(ReadOnlySpan<byte> payload)
    {
        var row = _rightRowBuffer;
        if (row == null)
        {
            int decodedRightColumnCount = GetDecodedRightColumnCount();
            row = decodedRightColumnCount == 0
                ? Array.Empty<DbValue>()
                : new DbValue[decodedRightColumnCount];
            _rightRowBuffer = row;
        }

        var decodedRightColumnIndices = _decodedRightColumnIndices;
        if (decodedRightColumnIndices is { Length: > 0 })
        {
            Array.Fill(row, DbValue.Null);
            _recordSerializer.DecodeSelectedInto(payload, row, decodedRightColumnIndices);
            return row;
        }

        if (row.Length == 0)
            return row;

        int decoded = _recordSerializer.DecodeInto(payload, row);
        if (decoded < row.Length)
            Array.Fill(row, DbValue.Null, decoded, row.Length - decoded);
        return row;
    }

    private DbValue[] EnsureOuterBatchRowBuffer(int columnCount)
    {
        if (_outerBatchRowBuffer == null || _outerBatchRowBuffer.Length != columnCount)
            _outerBatchRowBuffer = columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];

        return _outerBatchRowBuffer;
    }

    private int GetDecodedRightColumnCount()
        => _maxDecodedRightColumnIndex.HasValue
            ? Math.Max(0, _maxDecodedRightColumnIndex.Value + 1)
            : _rightColCount;

    private DbValue[] CreateMatchedRow(DbValue[] leftRow, DbValue[] rightRow)
    {
        var projection = _projectionColumnIndices;
        return projection == null
            ? CombineRows(leftRow, rightRow)
            : ProjectRows(leftRow, rightRow, projection);
    }

    private void AppendMatchedRow(RowBatch batch, DbValue[] leftRow, DbValue[] rightRow)
    {
        int rowIndex = batch.Count;
        WriteMatchedRow(leftRow, rightRow, batch.GetWritableRowSpan(rowIndex));
        batch.CommitWrittenRow(rowIndex);
    }

    private void WriteMatchedRow(DbValue[] leftRow, DbValue[] rightRow, Span<DbValue> destination)
    {
        var projection = _projectionColumnIndices;
        if (projection == null)
        {
            WriteRows(leftRow, rightRow, destination);
            return;
        }

        WriteProjectedRows(leftRow, rightRow, projection, destination);
    }

    private bool CanProjectRightFromIndexPayload()
    {
        if (_projectionColumnIndices == null || _residualPredicate != null)
            return false;

        for (int i = 0; i < _projectionColumnIndices.Length; i++)
        {
            int projectionIndex = _projectionColumnIndices[i];
            if (projectionIndex < _leftColCount)
                continue;

            int rightColumnIndex = projectionIndex - _leftColCount;
            if (rightColumnIndex == _rightPrimaryKeyColumnIndex)
                continue;

            bool matchedKeyColumn = false;
            for (int j = 0; j < _rightKeyColumnIndices.Length; j++)
            {
                if (_rightKeyColumnIndices[j] == rightColumnIndex)
                {
                    matchedKeyColumn = true;
                    break;
                }
            }

            if (!matchedKeyColumn)
                return false;
        }

        return true;
    }

    private DbValue[] CreateCoveredMatchedRow(DbValue[] leftRow, long rowId)
    {
        var projection = _projectionColumnIndices;
        var keyComponents = _currentKeyComponents;
        if (projection == null || keyComponents == null)
            return Array.Empty<DbValue>();

        if (projection.Length == 0)
            return Array.Empty<DbValue>();

        var projected = new DbValue[projection.Length];
        DbValue rowIdValue = DbValue.FromInteger(rowId);
        var coveredProjectionMap = _coveredProjectionKeyComponentMap;
        for (int i = 0; i < projection.Length; i++)
        {
            int mapping = coveredProjectionMap is { Length: > 0 } ? coveredProjectionMap[i] : int.MinValue;
            if (mapping == CoveredProjectionLeftSlot)
            {
                int projectionIndex = projection[i];
                projected[i] = projectionIndex < leftRow.Length
                    ? leftRow[projectionIndex]
                    : DbValue.Null;
                continue;
            }

            if (mapping == CoveredProjectionRowIdSlot)
            {
                projected[i] = rowIdValue;
                continue;
            }

            projected[i] = mapping >= 0
                ? keyComponents[mapping]
                : DbValue.Null;
        }

        return projected;
    }

    private void AppendCoveredMatchedRow(RowBatch batch, DbValue[] leftRow, long rowId)
    {
        int rowIndex = batch.Count;
        WriteCoveredMatchedRow(leftRow, rowId, batch.GetWritableRowSpan(rowIndex));
        batch.CommitWrittenRow(rowIndex);
    }

    private void WriteCoveredMatchedRow(DbValue[] leftRow, long rowId, Span<DbValue> destination)
    {
        var projection = _projectionColumnIndices;
        if (projection == null)
        {
            WriteRows(leftRow, Array.Empty<DbValue>(), destination);
            return;
        }

        WriteCoveredProjectedRows(leftRow, rowId, projection, destination);
    }

    private DbValue[] CreateLeftOuterRow(DbValue[] leftRow)
    {
        var projection = _projectionColumnIndices;
        return projection == null
            ? CombineWithNulls(leftRow, _rightColCount, padRight: true)
            : ProjectLeftWithNullRight(leftRow, projection);
    }

    private void AppendLeftOuterRow(RowBatch batch, DbValue[] leftRow)
    {
        int rowIndex = batch.Count;
        WriteLeftOuterRow(leftRow, batch.GetWritableRowSpan(rowIndex));
        batch.CommitWrittenRow(rowIndex);
    }

    private void WriteLeftOuterRow(DbValue[] leftRow, Span<DbValue> destination)
    {
        var projection = _projectionColumnIndices;
        if (projection == null)
        {
            WriteRows(leftRow, Array.Empty<DbValue>(), destination);
            return;
        }

        WriteProjectedRows(leftRow, Array.Empty<DbValue>(), projection, destination);
    }

    private static DbValue[] CombineRows(DbValue[] left, DbValue[] right)
    {
        var combined = new DbValue[left.Length + right.Length];
        left.CopyTo(combined, 0);
        right.CopyTo(combined, left.Length);
        return combined;
    }

    private static DbValue[] CombineWithNulls(DbValue[] dataRow, int nullCount, bool padRight)
    {
        var combined = new DbValue[dataRow.Length + nullCount];
        if (padRight)
        {
            dataRow.CopyTo(combined, 0);
            for (int i = dataRow.Length; i < combined.Length; i++)
                combined[i] = DbValue.Null;
        }
        else
        {
            for (int i = 0; i < nullCount; i++)
                combined[i] = DbValue.Null;
            dataRow.CopyTo(combined, nullCount);
        }
        return combined;
    }

    private DbValue[] ProjectLeftWithNullRight(DbValue[] leftRow, int[] projection)
    {
        if (projection.Length == 0)
            return Array.Empty<DbValue>();

        var projected = new DbValue[projection.Length];
        for (int i = 0; i < projection.Length; i++)
        {
            int columnIndex = projection[i];
            if (columnIndex < _leftColCount)
            {
                projected[i] = columnIndex < leftRow.Length
                    ? leftRow[columnIndex]
                    : DbValue.Null;
            }
            else
            {
                projected[i] = DbValue.Null;
            }
        }

        return projected;
    }

    private DbValue[] ProjectRows(DbValue[] leftRow, DbValue[] rightRow, int[] projection)
    {
        if (projection.Length == 0)
            return Array.Empty<DbValue>();

        var projected = new DbValue[projection.Length];
        for (int i = 0; i < projection.Length; i++)
        {
            int columnIndex = projection[i];
            if (columnIndex < _leftColCount)
            {
                projected[i] = columnIndex < leftRow.Length
                    ? leftRow[columnIndex]
                    : DbValue.Null;
            }
            else
            {
                int rightIndex = columnIndex - _leftColCount;
                projected[i] = rightIndex < rightRow.Length
                    ? rightRow[rightIndex]
                    : DbValue.Null;
            }
        }

        return projected;
    }

    private void WriteRows(DbValue[] leftRow, DbValue[] rightRow, Span<DbValue> destination)
    {
        for (int i = 0; i < _leftColCount; i++)
            destination[i] = i < leftRow.Length ? leftRow[i] : DbValue.Null;

        for (int i = 0; i < _rightColCount; i++)
            destination[_leftColCount + i] = i < rightRow.Length ? rightRow[i] : DbValue.Null;
    }

    private void WriteProjectedRows(DbValue[] leftRow, DbValue[] rightRow, int[] projection, Span<DbValue> destination)
    {
        for (int i = 0; i < projection.Length; i++)
        {
            int columnIndex = projection[i];
            if (columnIndex < _leftColCount)
            {
                destination[i] = columnIndex < leftRow.Length
                    ? leftRow[columnIndex]
                    : DbValue.Null;
            }
            else
            {
                int rightIndex = columnIndex - _leftColCount;
                destination[i] = rightIndex < rightRow.Length
                    ? rightRow[rightIndex]
                    : DbValue.Null;
            }
        }
    }

    private void WriteCoveredProjectedRows(DbValue[] leftRow, long rowId, int[] projection, Span<DbValue> destination)
    {
        DbValue rowIdValue = DbValue.FromInteger(rowId);
        var keyComponents = _currentKeyComponents;
        var coveredProjectionMap = _coveredProjectionKeyComponentMap;

        for (int i = 0; i < projection.Length; i++)
        {
            int mapping = coveredProjectionMap is { Length: > 0 } ? coveredProjectionMap[i] : int.MinValue;
            if (mapping == CoveredProjectionLeftSlot)
            {
                int columnIndex = projection[i];
                destination[i] = columnIndex < leftRow.Length
                    ? leftRow[columnIndex]
                    : DbValue.Null;
                continue;
            }

            if (mapping == CoveredProjectionRowIdSlot)
            {
                destination[i] = rowIdValue;
                continue;
            }

            destination[i] = mapping >= 0 && keyComponents != null
                ? keyComponents[mapping]
                : DbValue.Null;
        }
    }

    private DbValue[] EnsureLookupKeyComponentBuffer(int componentCount)
    {
        if (_lookupKeyComponentBuffer == null || _lookupKeyComponentBuffer.Length != componentCount)
            _lookupKeyComponentBuffer = componentCount == 0 ? Array.Empty<DbValue>() : new DbValue[componentCount];

        return _lookupKeyComponentBuffer;
    }

    private byte[][] EnsureLookupKeyTextBytesBuffer(int componentCount)
    {
        if (_lookupKeyTextBytesBuffer == null || _lookupKeyTextBytesBuffer.Length != componentCount)
            _lookupKeyTextBytesBuffer = new byte[componentCount][];

        return _lookupKeyTextBytesBuffer;
    }

    private byte[] GetCachedLookupTextBytes(string value)
    {
        _lookupTextByteCache ??= new Dictionary<string, byte[]>(StringComparer.Ordinal);
        if (_lookupTextByteCache.TryGetValue(value, out var bytes))
            return bytes;

        bytes = Encoding.UTF8.GetBytes(value);
        _lookupTextByteCache[value] = bytes;
        return bytes;
    }

    private int[] BuildCoveredProjectionKeyComponentMap(int[] projection)
    {
        var mapping = new int[projection.Length];
        for (int i = 0; i < projection.Length; i++)
        {
            int columnIndex = projection[i];
            if (columnIndex < _leftColCount)
            {
                mapping[i] = CoveredProjectionLeftSlot;
                continue;
            }

            int rightColumnIndex = columnIndex - _leftColCount;
            if (rightColumnIndex == _rightPrimaryKeyColumnIndex)
            {
                mapping[i] = CoveredProjectionRowIdSlot;
                continue;
            }

            mapping[i] = -3;
            for (int j = 0; j < _rightKeyColumnIndices.Length; j++)
            {
                if (_rightKeyColumnIndices[j] == rightColumnIndex)
                {
                    mapping[i] = j;
                    break;
                }
            }
        }

        return mapping;
    }
}

/// <summary>
/// Nested-loop join operator — materializes both sides and computes the join.
/// Supports INNER, LEFT OUTER, RIGHT OUTER, and CROSS joins.
/// </summary>
public sealed class NestedLoopJoinOperator : IOperator, IBatchOperator, IBatchBackedRowOperator, IProjectionPushdownTarget, IEstimatedRowCountProvider, IBatchBufferReuseController
{
    private const int DefaultBatchSize = 64;

    private readonly IOperator _left;
    private readonly IOperator _right;
    private readonly JoinType _joinType;
    private readonly Expression? _conditionExpression;
    private readonly JoinSpanExpressionEvaluator? _conditionEvaluator;
    private readonly TableSchema _compositeSchema;
    private readonly int _leftColCount;
    private readonly int _rightColCount;
    private readonly int? _estimatedRowCount;
    private readonly int? _rightRowCapacityHint;
    private int[]? _projectionColumnIndices;
    private List<DbValue[]>? _rightRows;
    private DbValue[]? _currentLeftRow;
    private int _currentRightIndex;
    private bool _currentLeftMatched;
    private bool _leftExhausted;
    private bool[]? _rightMatched;
    private int _rightOuterEmitIndex;
    private IBatchOperator? _leftBatchSource;
    private RowBatch? _pendingLeftBatch;
    private int _pendingLeftBatchRowIndex;
    private DbValue[]? _leftBatchRowBuffer;
    private bool _reuseCurrentBatch = true;
    private RowBatch _currentBatch = new(0, DefaultBatchSize);

    public ColumnDefinition[] OutputSchema { get; private set; }
    public bool ReusesCurrentRowBuffer => false;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
    public int? EstimatedRowCount => _estimatedRowCount;
    IBatchOperator IBatchBackedRowOperator.BatchSource => this;

    public NestedLoopJoinOperator(
        IOperator left, IOperator right,
        JoinType joinType, Expression? condition,
        TableSchema compositeSchema,
        int leftColCount, int rightColCount,
        int? estimatedOutputRowCount = null,
        int? rightRowCapacityHint = null)
    {
        _left = left;
        _right = right;
        _joinType = joinType;
        _conditionExpression = condition;
        _conditionEvaluator = condition != null
            ? ExpressionCompiler.CompileJoinSpan(condition, compositeSchema, leftColCount)
            : null;
        _compositeSchema = compositeSchema;
        _leftColCount = leftColCount;
        _rightColCount = rightColCount;
        _estimatedRowCount = estimatedOutputRowCount > 0 ? estimatedOutputRowCount : null;
        _rightRowCapacityHint = rightRowCapacityHint > 0 ? rightRowCapacityHint : null;
        OutputSchema = compositeSchema.Columns as ColumnDefinition[] ?? compositeSchema.Columns.ToArray();
    }

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        if (_left is IRowBufferReuseController leftController)
            // Left side is streamed row-by-row; reuse is safe and lowers per-row allocations.
            leftController.SetReuseCurrentRowBuffer(true);
        if (_right is IRowBufferReuseController rightController)
            // Right side is materialized, so request owned rows directly.
            rightController.SetReuseCurrentRowBuffer(false);

        await _left.OpenAsync(ct);
        await _right.OpenAsync(ct);
        _leftBatchSource = BatchSourceHelper.TryGetBatchSource(_left);
        _pendingLeftBatch = null;
        _pendingLeftBatchRowIndex = 0;
        _leftBatchRowBuffer = null;
        _currentBatch = CreateBatch(OutputSchema.Length);

        // Materialize right side once; left side is streamed.
        var rightRows = _rightRowCapacityHint.HasValue
            ? new List<DbValue[]>(_rightRowCapacityHint.Value)
            : new List<DbValue[]>();
        var rightBatchSource = BatchSourceHelper.TryGetBatchSource(_right);
        if (rightBatchSource == null)
        {
            bool cloneRightRows = _right.ReusesCurrentRowBuffer;
            while (await _right.MoveNextAsync(ct))
                rightRows.Add(cloneRightRows ? (DbValue[])_right.Current.Clone() : _right.Current);
        }
        else
        {
            while (await rightBatchSource.MoveNextBatchAsync(ct))
            {
                var batch = rightBatchSource.CurrentBatch;
                for (int rowIndex = 0; rowIndex < batch.Count; rowIndex++)
                {
                    var row = batch.ColumnCount == 0 ? Array.Empty<DbValue>() : new DbValue[batch.ColumnCount];
                    batch.CopyRowTo(rowIndex, row);
                    rightRows.Add(row);
                }
            }
        }

        _rightRows = rightRows;
        _rightMatched = _joinType == JoinType.RightOuter
            ? new bool[rightRows.Count]
            : null;
        _currentLeftRow = null;
        _currentRightIndex = 0;
        _currentLeftMatched = false;
        _leftExhausted = false;
        _rightOuterEmitIndex = 0;

        Current = Array.Empty<DbValue>();
    }

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        var rightRows = _rightRows ?? throw new InvalidOperationException("Operator is not open.");

        while (true)
        {
            if (_currentLeftRow == null && !_leftExhausted)
            {
                if (await TryMoveNextLeftRowAsync(ct) is { } leftRow)
                {
                    _currentLeftRow = leftRow;
                    _currentRightIndex = 0;
                    _currentLeftMatched = false;
                }
                else
                {
                    _leftExhausted = true;
                }
            }

            if (_currentLeftRow != null)
            {
                while (_currentRightIndex < rightRows.Count)
                {
                    int rightIndex = _currentRightIndex++;
                    var rightRow = rightRows[rightIndex];
                    if (!PassesCondition(_currentLeftRow, rightRow))
                        continue;

                    _currentLeftMatched = true;
                    if (_joinType == JoinType.RightOuter)
                        _rightMatched?[rightIndex] = true;

                    Current = CreateMatchedRow(_currentLeftRow, rightRow);
                    return true;
                }

                if (_joinType == JoinType.LeftOuter && !_currentLeftMatched)
                {
                    Current = CreateLeftOuterRow(_currentLeftRow);
                    _currentLeftRow = null;
                    return true;
                }

                _currentLeftRow = null;
                continue;
            }

            if (_joinType == JoinType.RightOuter && _rightMatched != null)
            {
                while (_rightOuterEmitIndex < rightRows.Count)
                {
                    int rightIndex = _rightOuterEmitIndex++;
                    if (_rightMatched[rightIndex])
                        continue;

                    Current = CreateRightOuterRow(rightRows[rightIndex]);
                    return true;
                }
            }

            return false;
        }
    }

    public async ValueTask<bool> MoveNextBatchAsync(CancellationToken ct = default)
    {
        var batch = _reuseCurrentBatch ? EnsureBatch(OutputSchema.Length) : CreateBatch(OutputSchema.Length);
        batch.Reset();

        var rightRows = _rightRows ?? throw new InvalidOperationException("Operator is not open.");

        while (batch.Count < batch.Capacity)
        {
            if (_currentLeftRow == null && !_leftExhausted)
            {
                if (await TryMoveNextLeftRowAsync(ct) is { } leftRow)
                {
                    _currentLeftRow = leftRow;
                    _currentRightIndex = 0;
                    _currentLeftMatched = false;
                }
                else
                {
                    _leftExhausted = true;
                }
            }

            if (_currentLeftRow != null)
            {
                while (_currentRightIndex < rightRows.Count && batch.Count < batch.Capacity)
                {
                    int rightIndex = _currentRightIndex++;
                    var rightRow = rightRows[rightIndex];
                    if (!PassesCondition(_currentLeftRow, rightRow))
                        continue;

                    _currentLeftMatched = true;
                    if (_joinType == JoinType.RightOuter)
                        _rightMatched?[rightIndex] = true;

                    AppendMatchedRow(batch, _currentLeftRow, rightRow);
                }

                if (_currentRightIndex < rightRows.Count)
                    continue;

                if (_joinType == JoinType.LeftOuter && !_currentLeftMatched)
                    AppendLeftOuterRow(batch, _currentLeftRow);

                _currentLeftRow = null;
                continue;
            }

            if (_joinType == JoinType.RightOuter && _rightMatched != null && _leftExhausted)
            {
                while (_rightOuterEmitIndex < rightRows.Count && batch.Count < batch.Capacity)
                {
                    int rightIndex = _rightOuterEmitIndex++;
                    if (_rightMatched[rightIndex])
                        continue;

                    AppendRightOuterRow(batch, rightRows[rightIndex]);
                }

                if (batch.Count > 0)
                    break;
            }

            if (_leftExhausted)
                break;
        }

        _currentBatch = batch;
        return batch.Count > 0;
    }

    public async ValueTask DisposeAsync()
    {
        await _left.DisposeAsync();
        await _right.DisposeAsync();
    }

    bool IBatchOperator.ReusesCurrentBatch => _reuseCurrentBatch;
    RowBatch IBatchOperator.CurrentBatch => _currentBatch;

    public void SetReuseCurrentBatch(bool reuse)
    {
        _reuseCurrentBatch = reuse;
        if (!reuse)
            _currentBatch = CreateBatch(OutputSchema.Length);
    }

    public bool TrySetOutputProjection(int[] columnIndices, ColumnDefinition[] outputSchema)
    {
        if (columnIndices == null)
            throw new ArgumentNullException(nameof(columnIndices));
        if (outputSchema == null)
            throw new ArgumentNullException(nameof(outputSchema));

        int compositeColumnCount = _leftColCount + _rightColCount;
        for (int i = 0; i < columnIndices.Length; i++)
        {
            int columnIndex = columnIndices[i];
            if (columnIndex < 0 || columnIndex >= compositeColumnCount)
                return false;
        }

        _projectionColumnIndices = (int[])columnIndices.Clone();
        OutputSchema = outputSchema;
        TryApplyDecodeBoundPushdown();
        return true;
    }

    private void TryApplyDecodeBoundPushdown()
    {
        if (_projectionColumnIndices == null)
            return;

        var leftFlags = new bool[_leftColCount];
        var rightFlags = new bool[_rightColCount];
        int leftRequiredCount = 0;
        int rightRequiredCount = 0;

        void MarkLeftRequired(int columnIndex)
        {
            if ((uint)columnIndex >= (uint)_leftColCount || leftFlags[columnIndex])
                return;

            leftFlags[columnIndex] = true;
            leftRequiredCount++;
        }

        void MarkRightRequired(int columnIndex)
        {
            if ((uint)columnIndex >= (uint)_rightColCount || rightFlags[columnIndex])
                return;

            rightFlags[columnIndex] = true;
            rightRequiredCount++;
        }

        for (int i = 0; i < _projectionColumnIndices.Length; i++)
        {
            int projectionIndex = _projectionColumnIndices[i];
            if (projectionIndex < _leftColCount)
            {
                MarkLeftRequired(projectionIndex);
            }
            else
            {
                MarkRightRequired(projectionIndex - _leftColCount);
            }
        }

        if (_conditionExpression != null &&
            !TryMarkConditionColumnsForExpression(_conditionExpression, MarkLeftRequired, MarkRightRequired))
        {
            return;
        }

        int[] leftRequiredColumns = BuildRequiredColumnIndices(leftFlags, leftRequiredCount);
        int[] rightRequiredColumns = BuildRequiredColumnIndices(rightFlags, rightRequiredCount);

        if (!TrySetDecodedColumnIndices(_left, leftRequiredColumns) && leftRequiredColumns.Length > 0)
            TrySetDecodedColumnUpperBound(_left, leftRequiredColumns[^1]);

        if (!TrySetDecodedColumnIndices(_right, rightRequiredColumns) && rightRequiredColumns.Length > 0)
            TrySetDecodedColumnUpperBound(_right, rightRequiredColumns[^1]);
    }

    private bool TryMarkConditionColumnsForExpression(
        Expression expression,
        Action<int> markLeftColumn,
        Action<int> markRightColumn)
    {
        switch (expression)
        {
            case LiteralExpression:
            case ParameterExpression:
                return true;
            case ColumnRefExpression columnRef:
            {
                int compositeColumnIndex = columnRef.TableAlias != null
                    ? _compositeSchema.GetQualifiedColumnIndex(columnRef.TableAlias, columnRef.ColumnName)
                    : _compositeSchema.GetColumnIndex(columnRef.ColumnName);
                if (compositeColumnIndex < 0)
                    return false;

                if (compositeColumnIndex < _leftColCount)
                    markLeftColumn(compositeColumnIndex);
                else
                    markRightColumn(compositeColumnIndex - _leftColCount);

                return true;
            }
            case BinaryExpression binaryExpression:
                return TryMarkConditionColumnsForExpression(binaryExpression.Left, markLeftColumn, markRightColumn)
                    && TryMarkConditionColumnsForExpression(binaryExpression.Right, markLeftColumn, markRightColumn);
            case UnaryExpression unaryExpression:
                return TryMarkConditionColumnsForExpression(unaryExpression.Operand, markLeftColumn, markRightColumn);
            case CollateExpression collateExpression:
                return TryMarkConditionColumnsForExpression(collateExpression.Operand, markLeftColumn, markRightColumn);
            case LikeExpression likeExpression:
                return TryMarkConditionColumnsForExpression(likeExpression.Operand, markLeftColumn, markRightColumn)
                    && TryMarkConditionColumnsForExpression(likeExpression.Pattern, markLeftColumn, markRightColumn)
                    && (likeExpression.EscapeChar == null
                        || TryMarkConditionColumnsForExpression(likeExpression.EscapeChar, markLeftColumn, markRightColumn));
            case InExpression inExpression:
            {
                if (!TryMarkConditionColumnsForExpression(inExpression.Operand, markLeftColumn, markRightColumn))
                    return false;

                for (int i = 0; i < inExpression.Values.Count; i++)
                {
                    if (!TryMarkConditionColumnsForExpression(inExpression.Values[i], markLeftColumn, markRightColumn))
                        return false;
                }

                return true;
            }
            case BetweenExpression betweenExpression:
                return TryMarkConditionColumnsForExpression(betweenExpression.Operand, markLeftColumn, markRightColumn)
                    && TryMarkConditionColumnsForExpression(betweenExpression.Low, markLeftColumn, markRightColumn)
                    && TryMarkConditionColumnsForExpression(betweenExpression.High, markLeftColumn, markRightColumn);
            case IsNullExpression isNullExpression:
                return TryMarkConditionColumnsForExpression(isNullExpression.Operand, markLeftColumn, markRightColumn);
            case FunctionCallExpression functionCall:
            {
                if (functionCall.IsStarArg)
                    return true;

                for (int i = 0; i < functionCall.Arguments.Count; i++)
                {
                    if (!TryMarkConditionColumnsForExpression(functionCall.Arguments[i], markLeftColumn, markRightColumn))
                        return false;
                }

                return true;
            }
            default:
                return false;
        }
    }

    private static int[] BuildRequiredColumnIndices(bool[] flags, int count)
    {
        if (count <= 0)
            return Array.Empty<int>();

        var columns = new int[count];
        int cursor = 0;
        for (int i = 0; i < flags.Length; i++)
        {
            if (!flags[i])
                continue;

            columns[cursor++] = i;
        }

        return columns;
    }

    private static bool TrySetDecodedColumnIndices(IOperator op, int[] columnIndices)
    {
        if (columnIndices.Length == 0)
            return true;

        switch (op)
        {
            case TableScanOperator tableScan:
                tableScan.SetDecodedColumnIndices(columnIndices);
                return true;
            case PrimaryKeyLookupOperator primaryKeyLookup:
                primaryKeyLookup.SetDecodedColumnIndices(columnIndices);
                return true;
            case IndexScanOperator indexScan:
                indexScan.SetDecodedColumnIndices(columnIndices);
                return true;
            case UniqueIndexLookupOperator uniqueIndexLookup:
                uniqueIndexLookup.SetDecodedColumnIndices(columnIndices);
                return true;
            case IndexOrderedScanOperator orderedScan:
                orderedScan.SetDecodedColumnIndices(columnIndices);
                return true;
            default:
                return false;
        }
    }

    private static void TrySetDecodedColumnUpperBound(IOperator op, int maxColumnIndex)
    {
        if (maxColumnIndex < 0)
            return;

        switch (op)
        {
            case TableScanOperator tableScan:
                tableScan.SetDecodedColumnUpperBound(maxColumnIndex);
                break;
            case PrimaryKeyLookupOperator primaryKeyLookup:
                primaryKeyLookup.SetDecodedColumnUpperBound(maxColumnIndex);
                break;
            case IndexScanOperator indexScan:
                indexScan.SetDecodedColumnUpperBound(maxColumnIndex);
                break;
            case UniqueIndexLookupOperator uniqueIndexLookup:
                uniqueIndexLookup.SetDecodedColumnUpperBound(maxColumnIndex);
                break;
            case IndexOrderedScanOperator orderedScan:
                orderedScan.SetDecodedColumnUpperBound(maxColumnIndex);
                break;
        }
    }

    private bool PassesCondition(DbValue[] left, DbValue[] right)
    {
        if (_conditionEvaluator == null)
            return true;

        return _conditionEvaluator(left, right).IsTruthy;
    }

    private async ValueTask<DbValue[]?> TryMoveNextLeftRowAsync(CancellationToken ct)
    {
        if (_leftBatchSource == null)
        {
            if (await _left.MoveNextAsync(ct))
                return _left.Current;

            return null;
        }

        while (true)
        {
            if (_pendingLeftBatch != null && _pendingLeftBatchRowIndex < _pendingLeftBatch.Count)
            {
                var batch = _pendingLeftBatch;
                var leftRow = EnsureLeftBatchRowBuffer(batch.ColumnCount);
                batch.CopyRowTo(_pendingLeftBatchRowIndex, leftRow);
                _pendingLeftBatchRowIndex++;
                if (_pendingLeftBatchRowIndex >= batch.Count)
                {
                    _pendingLeftBatch = null;
                    _pendingLeftBatchRowIndex = 0;
                }

                return leftRow;
            }

            if (!await _leftBatchSource.MoveNextBatchAsync(ct))
                return null;

            _pendingLeftBatch = _leftBatchSource.CurrentBatch;
            _pendingLeftBatchRowIndex = 0;
        }
    }

    private DbValue[] EnsureLeftBatchRowBuffer(int columnCount)
    {
        if (_leftBatchRowBuffer == null || _leftBatchRowBuffer.Length != columnCount)
            _leftBatchRowBuffer = columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];

        return _leftBatchRowBuffer;
    }

    private RowBatch EnsureBatch(int columnCount)
    {
        if (_currentBatch.ColumnCount != columnCount)
            _currentBatch = CreateBatch(columnCount);

        return _currentBatch;
    }

    private static RowBatch CreateBatch(int columnCount) => new(columnCount, DefaultBatchSize);


    private DbValue[] CreateMatchedRow(DbValue[] leftRow, DbValue[] rightRow)
    {
        var projection = _projectionColumnIndices;
        return projection == null
            ? CombineRows(leftRow, rightRow)
            : ProjectRows(leftRow, rightRow, projection);
    }

    private DbValue[] CreateLeftOuterRow(DbValue[] leftRow)
    {
        var projection = _projectionColumnIndices;
        return projection == null
            ? CombineWithNulls(leftRow, _rightColCount, padRight: true)
            : ProjectLeftWithNullRight(leftRow, projection);
    }

    private DbValue[] CreateRightOuterRow(DbValue[] rightRow)
    {
        var projection = _projectionColumnIndices;
        return projection == null
            ? CombineWithNulls(rightRow, _leftColCount, padRight: false)
            : ProjectNullLeftWithRight(rightRow, projection);
    }

    private void AppendMatchedRow(RowBatch batch, DbValue[] leftRow, DbValue[] rightRow)
    {
        int rowIndex = batch.Count;
        WriteMatchedRow(leftRow, rightRow, batch.GetWritableRowSpan(rowIndex));
        batch.CommitWrittenRow(rowIndex);
    }

    private void WriteMatchedRow(DbValue[] leftRow, DbValue[] rightRow, Span<DbValue> destination)
    {
        var projection = _projectionColumnIndices;
        if (projection == null)
        {
            WriteRows(leftRow, rightRow, destination);
            return;
        }

        WriteProjectedRows(leftRow, rightRow, projection, destination);
    }

    private void AppendLeftOuterRow(RowBatch batch, DbValue[] leftRow)
    {
        int rowIndex = batch.Count;
        WriteLeftOuterRow(leftRow, batch.GetWritableRowSpan(rowIndex));
        batch.CommitWrittenRow(rowIndex);
    }

    private void WriteLeftOuterRow(DbValue[] leftRow, Span<DbValue> destination)
    {
        var projection = _projectionColumnIndices;
        if (projection == null)
        {
            WriteRows(leftRow, Array.Empty<DbValue>(), destination);
            return;
        }

        WriteProjectedRows(leftRow, Array.Empty<DbValue>(), projection, destination);
    }

    private void AppendRightOuterRow(RowBatch batch, DbValue[] rightRow)
    {
        int rowIndex = batch.Count;
        WriteRightOuterRow(rightRow, batch.GetWritableRowSpan(rowIndex));
        batch.CommitWrittenRow(rowIndex);
    }

    private void WriteRightOuterRow(DbValue[] rightRow, Span<DbValue> destination)
    {
        var projection = _projectionColumnIndices;
        if (projection == null)
        {
            WriteRows(Array.Empty<DbValue>(), rightRow, destination);
            return;
        }

        WriteNullLeftWithRight(rightRow, projection, destination);
    }

    private static DbValue[] CombineRows(DbValue[] left, DbValue[] right)
    {
        var combined = new DbValue[left.Length + right.Length];
        left.CopyTo(combined, 0);
        right.CopyTo(combined, left.Length);
        return combined;
    }

    /// <summary>
    /// Creates a combined row with NULLs on one side.
    /// If padRight=true: [dataRow | NULLs]. If padRight=false: [NULLs | dataRow].
    /// </summary>
    private static DbValue[] CombineWithNulls(DbValue[] dataRow, int nullCount, bool padRight)
    {
        var combined = new DbValue[dataRow.Length + nullCount];
        if (padRight)
        {
            dataRow.CopyTo(combined, 0);
            for (int i = dataRow.Length; i < combined.Length; i++)
                combined[i] = DbValue.Null;
        }
        else
        {
            for (int i = 0; i < nullCount; i++)
                combined[i] = DbValue.Null;
            dataRow.CopyTo(combined, nullCount);
        }
        return combined;
    }

    private DbValue[] ProjectLeftWithNullRight(DbValue[] leftRow, int[] projection)
    {
        if (projection.Length == 0)
            return Array.Empty<DbValue>();

        var projected = new DbValue[projection.Length];
        for (int i = 0; i < projection.Length; i++)
        {
            int columnIndex = projection[i];
            if (columnIndex < _leftColCount)
            {
                projected[i] = columnIndex < leftRow.Length
                    ? leftRow[columnIndex]
                    : DbValue.Null;
            }
            else
            {
                projected[i] = DbValue.Null;
            }
        }

        return projected;
    }

    private DbValue[] ProjectNullLeftWithRight(DbValue[] rightRow, int[] projection)
    {
        if (projection.Length == 0)
            return Array.Empty<DbValue>();

        var projected = new DbValue[projection.Length];
        for (int i = 0; i < projection.Length; i++)
        {
            int columnIndex = projection[i];
            if (columnIndex < _leftColCount)
            {
                projected[i] = DbValue.Null;
            }
            else
            {
                int rightIndex = columnIndex - _leftColCount;
                projected[i] = rightIndex < rightRow.Length
                    ? rightRow[rightIndex]
                    : DbValue.Null;
            }
        }

        return projected;
    }

    private DbValue[] ProjectRows(DbValue[] leftRow, DbValue[] rightRow, int[] projection)
    {
        if (projection.Length == 0)
            return Array.Empty<DbValue>();

        var projected = new DbValue[projection.Length];
        for (int i = 0; i < projection.Length; i++)
        {
            int columnIndex = projection[i];
            if (columnIndex < _leftColCount)
            {
                projected[i] = columnIndex < leftRow.Length
                    ? leftRow[columnIndex]
                    : DbValue.Null;
            }
            else
            {
                int rightIndex = columnIndex - _leftColCount;
                projected[i] = rightIndex < rightRow.Length
                    ? rightRow[rightIndex]
                    : DbValue.Null;
            }
        }

        return projected;
    }

    private void WriteRows(DbValue[] leftRow, DbValue[] rightRow, Span<DbValue> destination)
    {
        for (int i = 0; i < _leftColCount; i++)
            destination[i] = i < leftRow.Length ? leftRow[i] : DbValue.Null;

        for (int i = 0; i < _rightColCount; i++)
            destination[_leftColCount + i] = i < rightRow.Length ? rightRow[i] : DbValue.Null;
    }

    private void WriteProjectedRows(DbValue[] leftRow, DbValue[] rightRow, int[] projection, Span<DbValue> destination)
    {
        for (int i = 0; i < projection.Length; i++)
        {
            int columnIndex = projection[i];
            if (columnIndex < _leftColCount)
            {
                destination[i] = columnIndex < leftRow.Length
                    ? leftRow[columnIndex]
                    : DbValue.Null;
            }
            else
            {
                int rightIndex = columnIndex - _leftColCount;
                destination[i] = rightIndex < rightRow.Length
                    ? rightRow[rightIndex]
                    : DbValue.Null;
            }
        }
    }

    private void WriteNullLeftWithRight(DbValue[] rightRow, int[] projection, Span<DbValue> destination)
    {
        for (int i = 0; i < projection.Length; i++)
        {
            int columnIndex = projection[i];
            if (columnIndex < _leftColCount)
            {
                destination[i] = DbValue.Null;
            }
            else
            {
                int rightIndex = columnIndex - _leftColCount;
                destination[i] = rightIndex < rightRow.Length
                    ? rightRow[rightIndex]
                    : DbValue.Null;
            }
        }
    }
}

/// <summary>
/// Index scan operator — uses an index B+tree for equality lookups.
/// The index stores: key = indexed column value, payload = list of rowids (each 8 bytes).
/// For each matching rowid, looks up the actual row in the table's B+tree.
/// </summary>
public sealed class IndexScanOperator : IOperator, IBatchOperator, IRowBufferReuseController, IBatchBufferReuseController, IPreDecodeFilterSupport, IEstimatedRowCountProvider, IEncodedPayloadSource
{
    private const int DefaultBatchSize = 64;

    private readonly IIndexStore _indexStore;
    private readonly BTree _tableTree;
    private readonly TableSchema _schema;
    private readonly long _seekValue;
    private readonly IRecordSerializer _recordSerializer;
    private readonly int[]? _expectedKeyColumnIndices;
    private readonly DbValue[]? _expectedKeyComponents;
    private readonly string?[]? _expectedKeyCollations;
    private readonly RecordColumnAccessor?[]? _expectedKeyAccessors;
    private readonly byte[][]? _expectedKeyTextBytes;
    private readonly bool _usesOrderedTextPayload;
    private ReadOnlyMemory<byte> _rowIdPayload;
    private int _rowIdPayloadOffset;
    private bool _rowIdsVerifiedByIndexPayload;
    private DbValue[]? _rowBuffer;
    private bool _reuseCurrentRowBuffer = true;
    private bool _reuseCurrentBatch = true;
    private int? _estimatedRowCount;
    private int? _maxDecodedColumnIndex;
    private int[]? _decodedColumnIndices;
    private PreDecodeFilterSpec _preDecodeFilter;
    private bool _hasPreDecodeFilter;
    private PreDecodeFilterSpec[]? _additionalPreDecodeFilters;
    private ReadOnlyMemory<byte> _currentPayload;
    private RowBatch _currentBatch;

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => _reuseCurrentRowBuffer;
    public int? EstimatedRowCount => _estimatedRowCount;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
    public long CurrentRowId { get; private set; }
    public ReadOnlyMemory<byte> CurrentPayload => _currentPayload;
    internal IIndexStore IndexStore => _indexStore;
    internal BTree TableTree => _tableTree;
    internal long SeekValue => _seekValue;
    internal int[]? ExpectedKeyColumnIndices => _expectedKeyColumnIndices;
    internal DbValue[]? ExpectedKeyComponents => _expectedKeyComponents;
    internal byte[][]? ExpectedKeyTextBytes => _expectedKeyTextBytes;
    internal bool UsesOrderedTextPayload => _usesOrderedTextPayload;

    public IndexScanOperator(
        IIndexStore indexStore,
        BTree tableTree,
        TableSchema schema,
        long seekValue,
        IRecordSerializer? recordSerializer = null,
        int[]? expectedKeyColumnIndices = null,
        DbValue[]? expectedKeyComponents = null,
        string?[]? expectedKeyCollations = null,
        bool usesOrderedTextPayload = false)
    {
        _indexStore = indexStore;
        _tableTree = tableTree;
        _schema = schema;
        _seekValue = seekValue;
        _recordSerializer = recordSerializer ?? new DefaultRecordSerializer();
        _expectedKeyColumnIndices = expectedKeyColumnIndices;
        _expectedKeyComponents = expectedKeyComponents;
        _expectedKeyCollations = expectedKeyCollations;
        _usesOrderedTextPayload = usesOrderedTextPayload;
        if (expectedKeyColumnIndices is { Length: > 0 } && expectedKeyComponents is { Length: > 0 })
        {
            _expectedKeyAccessors = BoundColumnAccessHelper.CreateAccessors(_recordSerializer, expectedKeyColumnIndices);
            _expectedKeyTextBytes = BoundColumnAccessHelper.CreateTextLiteralBytes(expectedKeyComponents);
        }
        OutputSchema = schema.Columns as ColumnDefinition[] ?? schema.Columns.ToArray();
        _currentBatch = CreateBatch(GetTargetColumnCount());
    }

    /// <summary>
    /// Hint the lookup to decode only columns up to this index.
    /// </summary>
    public void SetDecodedColumnUpperBound(int maxColumnIndex)
    {
        _decodedColumnIndices = null;
        _maxDecodedColumnIndex = maxColumnIndex;
    }

    public void SetDecodedColumnIndices(ReadOnlySpan<int> columnIndices)
    {
        _decodedColumnIndices = BoundColumnAccessHelper.NormalizeColumnIndices(
            columnIndices,
            _schema.Columns.Count,
            out int maxColumnIndex);
        _maxDecodedColumnIndex = maxColumnIndex;
    }

    public void SetPreDecodeFilter(int columnIndex, BinaryOp op, DbValue literal)
        => ((IPreDecodeFilterSupport)this).SetPreDecodeFilter(new PreDecodeFilterSpec(_recordSerializer, columnIndex, op, literal));

    void IPreDecodeFilterSupport.SetPreDecodeFilter(in PreDecodeFilterSpec filter)
    {
        if (_hasPreDecodeFilter)
        {
            AppendAdditionalPreDecodeFilter(filter);
            return;
        }

        _preDecodeFilter = filter;
        _hasPreDecodeFilter = true;
    }

    public ValueTask OpenAsync(CancellationToken ct = default)
    {
        _currentPayload = ReadOnlyMemory<byte>.Empty;
        _currentBatch = CreateBatch(GetTargetColumnCount());
        if (_indexStore is ICacheAwareIndexStore cacheAware &&
            cacheAware.TryFindCached(_seekValue, out var cachedPayload))
        {
            InitializeRowIdPayload(cachedPayload);
            return ValueTask.CompletedTask;
        }

        return OpenUncachedAsync(ct);
    }

    private async ValueTask OpenUncachedAsync(CancellationToken ct)
    {
        InitializeRowIdPayload(await _indexStore.FindAsync(_seekValue, ct));
    }

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (!await TryMoveNextPayloadAsync(ct))
            return false;

        PopulateCurrentFromPayload(_currentPayload.Span);
        return true;
    }

    public async ValueTask<bool> MoveNextBatchAsync(CancellationToken ct = default)
    {
        int targetColumnCount = GetTargetColumnCount();
        var batch = _reuseCurrentBatch ? EnsureBatch(targetColumnCount) : CreateBatch(targetColumnCount);
        batch.Reset();

        while (batch.Count < batch.Capacity && await TryMoveNextPayloadAsync(ct))
        {
            int rowIndex = batch.Count;
            DecodePayloadInto(_currentPayload.Span, targetColumnCount, batch.GetWritableRowSpan(rowIndex));
            batch.CommitWrittenRow(rowIndex);
        }

        _currentBatch = batch;
        if (batch.Count == 0)
            _currentPayload = ReadOnlyMemory<byte>.Empty;

        return batch.Count > 0;
    }

    bool IBatchOperator.ReusesCurrentBatch => _reuseCurrentBatch;
    RowBatch IBatchOperator.CurrentBatch => _currentBatch;

    public void SetReuseCurrentBatch(bool reuse)
    {
        _reuseCurrentBatch = reuse;
        if (!reuse)
            _currentBatch = CreateBatch(GetTargetColumnCount());
    }

    private async ValueTask<bool> TryMoveNextPayloadAsync(CancellationToken ct)
    {
        while (true)
        {
            if (_rowIdPayloadOffset + 8 > _rowIdPayload.Length)
            {
                _currentPayload = ReadOnlyMemory<byte>.Empty;
                return false;
            }

            long rowId = BinaryPrimitives.ReadInt64LittleEndian(
                _rowIdPayload.Span.Slice(_rowIdPayloadOffset, 8));
            _rowIdPayloadOffset += 8;
            CurrentRowId = rowId;

            if (_tableTree.TryFindCachedMemory(rowId, out var cachedPayload))
            {
                if (cachedPayload is not { } cachedPayloadMemory)
                    continue;

                if (!_rowIdsVerifiedByIndexPayload &&
                    _expectedKeyComponents is { Length: > 0 } &&
                    !MatchesExpectedKeyComponents(cachedPayloadMemory.Span))
                {
                    continue;
                }

                if (_hasPreDecodeFilter && !EvaluatePreDecodeFilter(cachedPayloadMemory.Span))
                    continue;

                _currentPayload = cachedPayloadMemory;
                return true;
            }

            var payload = await _tableTree.FindMemoryAsync(rowId, ct);
            if (payload is { } payloadMemory &&
                (_rowIdsVerifiedByIndexPayload ||
                 _expectedKeyComponents is not { Length: > 0 } ||
                 MatchesExpectedKeyComponents(payloadMemory.Span)) &&
                (!_hasPreDecodeFilter || EvaluatePreDecodeFilter(payloadMemory.Span)))
            {
                _currentPayload = payloadMemory;
                return true;
            }
        }
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public void SetReuseCurrentRowBuffer(bool reuse)
    {
        _reuseCurrentRowBuffer = reuse;
        if (!reuse)
        {
            _rowBuffer = null;
            Current = Array.Empty<DbValue>();
        }
    }

    private void EnsureRowBuffer(int columnCount)
    {
        if (_rowBuffer == null || _rowBuffer.Length != columnCount)
            _rowBuffer = columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];
    }

    private int GetTargetColumnCount()
        => _maxDecodedColumnIndex.HasValue
            ? Math.Max(0, _maxDecodedColumnIndex.Value + 1)
            : _schema.Columns.Count;

    private RowBatch EnsureBatch(int columnCount)
    {
        if (_currentBatch.ColumnCount != columnCount)
            _currentBatch = CreateBatch(columnCount);

        return _currentBatch;
    }

    private static RowBatch CreateBatch(int columnCount) => new(columnCount, DefaultBatchSize);

    private void PopulateCurrentFromPayload(ReadOnlySpan<byte> payload)
    {
        int targetColumnCount = GetTargetColumnCount();
        var decodedColumnIndices = _decodedColumnIndices;

        if (decodedColumnIndices is { Length: > 0 })
        {
            if (_reuseCurrentRowBuffer)
            {
                EnsureRowBuffer(targetColumnCount);
                if (targetColumnCount > 0)
                    Array.Fill(_rowBuffer!, DbValue.Null, 0, targetColumnCount);
                _recordSerializer.DecodeSelectedInto(payload, _rowBuffer!, decodedColumnIndices);
                Current = _rowBuffer!;
                return;
            }

            var sparseRow = targetColumnCount == 0 ? Array.Empty<DbValue>() : new DbValue[targetColumnCount];
            if (targetColumnCount > 0)
            {
                Array.Fill(sparseRow, DbValue.Null);
                _recordSerializer.DecodeSelectedInto(payload, sparseRow, decodedColumnIndices);
            }

            Current = sparseRow;
            return;
        }

        if (_reuseCurrentRowBuffer)
        {
            EnsureRowBuffer(targetColumnCount);
            int decodedCount = _recordSerializer.DecodeInto(payload, _rowBuffer!);
            if (decodedCount < targetColumnCount)
                Array.Fill(_rowBuffer!, DbValue.Null, decodedCount, targetColumnCount - decodedCount);

            Current = _rowBuffer!;
        }
        else
        {
            var row = targetColumnCount == 0 ? Array.Empty<DbValue>() : new DbValue[targetColumnCount];
            int decodedCount = _recordSerializer.DecodeInto(payload, row);
            if (decodedCount < targetColumnCount)
                Array.Fill(row, DbValue.Null, decodedCount, targetColumnCount - decodedCount);

            Current = row;
        }
    }

    private void DecodePayloadInto(ReadOnlySpan<byte> payload, int targetColumnCount, Span<DbValue> destination)
    {
        var decodedColumnIndices = _decodedColumnIndices;
        if (decodedColumnIndices is { Length: > 0 })
        {
            if (targetColumnCount > 0)
                destination[..targetColumnCount].Fill(DbValue.Null);

            _recordSerializer.DecodeSelectedInto(payload, destination, decodedColumnIndices);
            return;
        }

        int decodedCount = _recordSerializer.DecodeInto(payload, destination);
        if (decodedCount < targetColumnCount)
            destination[decodedCount..targetColumnCount].Fill(DbValue.Null);
    }

    private bool EvaluatePreDecodeFilter(ReadOnlySpan<byte> payload)
    {
        if (!BoundColumnAccessHelper.EvaluatePreDecodeFilter(payload, _recordSerializer, _preDecodeFilter))
        {
            return false;
        }

        return _additionalPreDecodeFilters is not { Length: > 0 }
            || BoundColumnAccessHelper.EvaluatePreDecodeFilters(payload, _recordSerializer, _additionalPreDecodeFilters);
    }

    private void AppendAdditionalPreDecodeFilter(in PreDecodeFilterSpec filter)
    {
        if (_additionalPreDecodeFilters == null)
        {
            _additionalPreDecodeFilters = [filter];
            return;
        }

        int length = _additionalPreDecodeFilters.Length;
        Array.Resize(ref _additionalPreDecodeFilters, length + 1);
        _additionalPreDecodeFilters[length] = filter;
    }

    private void InitializeRowIdPayload(byte[]? payload)
    {
        _rowBuffer = null;
        Current = Array.Empty<DbValue>();
        _rowIdPayloadOffset = 0;
        _rowIdsVerifiedByIndexPayload = false;

        if (_usesOrderedTextPayload && payload is { Length: > 0 })
        {
            if (!OrderedTextIndexPayloadCodec.IsEncoded(payload))
            {
                throw new InvalidOperationException(
                    "SQL text index payload format mismatch detected. Rebuild the index before continuing.");
            }

            if (_expectedKeyComponents is [var expectedText] &&
                expectedText.Type == DbType.Text &&
                OrderedTextIndexPayloadCodec.TryGetMatchingRowIdPayloadSlice(
                    payload,
                    expectedText.AsText,
                    out var orderedMatchingRowIds))
            {
                _rowIdPayload = orderedMatchingRowIds;
                _estimatedRowCount = _rowIdPayload.Length / sizeof(long);
                _rowIdsVerifiedByIndexPayload = true;
                return;
            }

            _rowIdPayload = ReadOnlyMemory<byte>.Empty;
            _estimatedRowCount = 0;
            return;
        }

        if (_expectedKeyComponents is { Length: > 0 } &&
            payload is { Length: > 0 } &&
            HashedIndexPayloadCodec.TryGetMatchingRowIdPayloadSlice(payload, _expectedKeyComponents, _expectedKeyTextBytes, out var matchingRowIds))
        {
            _rowIdPayload = matchingRowIds;
            _estimatedRowCount = _rowIdPayload.Length / sizeof(long);
            _rowIdsVerifiedByIndexPayload = true;
            return;
        }

        _rowIdPayload = payload ?? ReadOnlyMemory<byte>.Empty;
        _estimatedRowCount = _rowIdPayload.Length / sizeof(long);
    }

    private bool MatchesExpectedKeyComponents(ReadOnlySpan<byte> payload)
    {
        if (_expectedKeyColumnIndices is not { Length: > 0 } ||
            _expectedKeyComponents is not { Length: > 0 })
        {
            return true;
        }

        return BoundColumnAccessHelper.MatchesKeyComponents(
            payload,
            _recordSerializer,
            _expectedKeyColumnIndices,
            _expectedKeyComponents,
            _expectedKeyAccessors,
            _expectedKeyTextBytes,
            _expectedKeyCollations);
    }
}

/// <summary>
/// Unique-index lookup operator — performs a direct secondary-index equality lookup
/// and resolves exactly one rowid from the index payload.
/// </summary>
public sealed class UniqueIndexLookupOperator : IOperator, IPreDecodeFilterSupport, IEstimatedRowCountProvider, IEncodedPayloadSource
{
    private readonly IIndexStore _indexStore;
    private readonly BTree _tableTree;
    private readonly TableSchema _schema;
    private readonly long _seekValue;
    private readonly IRecordSerializer _recordSerializer;
    private bool _consumed;
    private int? _maxDecodedColumnIndex;
    private int[]? _decodedColumnIndices;
    private PreDecodeFilterSpec _preDecodeFilter;
    private bool _hasPreDecodeFilter;
    private PreDecodeFilterSpec[]? _additionalPreDecodeFilters;
    private ReadOnlyMemory<byte> _currentPayload;

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => false;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
    public int? EstimatedRowCount => 1;
    public long CurrentRowId { get; private set; }
    public ReadOnlyMemory<byte> CurrentPayload => _currentPayload;
    internal IIndexStore IndexStore => _indexStore;
    internal BTree TableTree => _tableTree;
    internal long SeekValue => _seekValue;

    public UniqueIndexLookupOperator(
        IIndexStore indexStore,
        BTree tableTree,
        TableSchema schema,
        long seekValue,
        IRecordSerializer? recordSerializer = null)
    {
        _indexStore = indexStore;
        _tableTree = tableTree;
        _schema = schema;
        _seekValue = seekValue;
        _recordSerializer = recordSerializer ?? new DefaultRecordSerializer();
        OutputSchema = schema.Columns as ColumnDefinition[] ?? schema.Columns.ToArray();
    }

    /// <summary>
    /// Hint the lookup to decode only columns up to this index.
    /// </summary>
    public void SetDecodedColumnUpperBound(int maxColumnIndex)
    {
        _decodedColumnIndices = null;
        _maxDecodedColumnIndex = maxColumnIndex;
    }

    public void SetDecodedColumnIndices(ReadOnlySpan<int> columnIndices)
    {
        _decodedColumnIndices = BoundColumnAccessHelper.NormalizeColumnIndices(
            columnIndices,
            _schema.Columns.Count,
            out int maxColumnIndex);
        _maxDecodedColumnIndex = maxColumnIndex;
    }

    public void SetPreDecodeFilter(int columnIndex, BinaryOp op, DbValue literal)
        => ((IPreDecodeFilterSupport)this).SetPreDecodeFilter(new PreDecodeFilterSpec(_recordSerializer, columnIndex, op, literal));

    void IPreDecodeFilterSupport.SetPreDecodeFilter(in PreDecodeFilterSpec filter)
    {
        if (_hasPreDecodeFilter)
        {
            AppendAdditionalPreDecodeFilter(filter);
            return;
        }

        _preDecodeFilter = filter;
        _hasPreDecodeFilter = true;
    }

    public ValueTask OpenAsync(CancellationToken ct = default)
    {
        _consumed = false;
        _currentPayload = ReadOnlyMemory<byte>.Empty;
        return ValueTask.CompletedTask;
    }

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (_consumed) return false;
        _consumed = true;

        byte[]? indexPayload;
        if (_indexStore is ICacheAwareIndexStore cacheAware &&
            cacheAware.TryFindCached(_seekValue, out var cachedIndexPayload))
        {
            indexPayload = cachedIndexPayload;
        }
        else
        {
            indexPayload = await _indexStore.FindAsync(_seekValue, ct);
        }

        if (indexPayload == null || indexPayload.Length < 8)
            return false;

        long rowId = BinaryPrimitives.ReadInt64LittleEndian(indexPayload.AsSpan(0, 8));
        ReadOnlyMemory<byte>? rowPayload;
        if (_tableTree.TryFindCachedMemory(rowId, out var cachedRowPayload))
        {
            rowPayload = cachedRowPayload;
        }
        else
        {
            rowPayload = await _tableTree.FindMemoryAsync(rowId, ct);
        }

        if (rowPayload is not { } rowPayloadMemory)
            return false;

        if (_hasPreDecodeFilter)
        {
            if (!EvaluatePreDecodeFilter(rowPayloadMemory.Span))
                return false;
        }

        CurrentRowId = rowId;
        _currentPayload = rowPayloadMemory;
        int targetColumnCount = _maxDecodedColumnIndex.HasValue
            ? Math.Max(0, _maxDecodedColumnIndex.Value + 1)
            : _schema.Columns.Count;
        var decodedColumnIndices = _decodedColumnIndices;
        if (decodedColumnIndices is { Length: > 0 })
        {
            var row = targetColumnCount == 0 ? Array.Empty<DbValue>() : new DbValue[targetColumnCount];
            if (targetColumnCount > 0)
            {
                Array.Fill(row, DbValue.Null);
                _recordSerializer.DecodeSelectedInto(rowPayloadMemory.Span, row, decodedColumnIndices);
            }

            Current = row;
        }
        else
        {
            var decoded = _maxDecodedColumnIndex.HasValue
                ? _recordSerializer.DecodeUpTo(rowPayloadMemory.Span, _maxDecodedColumnIndex.Value)
                : _recordSerializer.Decode(rowPayloadMemory.Span);

            if (!_maxDecodedColumnIndex.HasValue && decoded.Length < _schema.Columns.Count)
            {
                var padded = new DbValue[_schema.Columns.Count];
                decoded.CopyTo(padded, 0);
                for (int i = decoded.Length; i < padded.Length; i++)
                    padded[i] = DbValue.Null;
                Current = padded;
            }
            else
            {
                Current = decoded;
            }
        }

        return true;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private bool EvaluatePreDecodeFilter(ReadOnlySpan<byte> payload)
    {
        if (!BoundColumnAccessHelper.EvaluatePreDecodeFilter(payload, _recordSerializer, _preDecodeFilter))
        {
            return false;
        }

        return _additionalPreDecodeFilters is not { Length: > 0 }
            || BoundColumnAccessHelper.EvaluatePreDecodeFilters(payload, _recordSerializer, _additionalPreDecodeFilters);
    }

    private void AppendAdditionalPreDecodeFilter(in PreDecodeFilterSpec filter)
    {
        if (_additionalPreDecodeFilters == null)
        {
            _additionalPreDecodeFilters = [filter];
            return;
        }

        int length = _additionalPreDecodeFilters.Length;
        Array.Resize(ref _additionalPreDecodeFilters, length + 1);
        _additionalPreDecodeFilters[length] = filter;
    }
}

/// <summary>
/// Ordered index scan operator — walks an index B+tree in key order and fetches table rows by rowid.
/// Used to satisfy ORDER BY on indexed INTEGER columns without a Sort operator.
/// </summary>
public sealed class IndexOrderedScanOperator : IOperator, IBatchOperator, IRowBufferReuseController, IBatchBufferReuseController, IPreDecodeFilterSupport, IEncodedPayloadSource
{
    private const int DefaultBatchSize = 64;

    private readonly IIndexStore _indexStore;
    private readonly BTree _tableTree;
    private readonly TableSchema _schema;
    private readonly int _keyColumnIndex;
    private readonly IndexScanRange _scanRange;
    private readonly IRecordSerializer _recordSerializer;
    private readonly bool _usesOrderedTextPayload;
    private readonly string? _orderedTextLowerBound;
    private readonly bool _orderedTextLowerInclusive;
    private readonly string? _orderedTextUpperBound;
    private readonly bool _orderedTextUpperInclusive;
    private IIndexCursor? _cursor;
    private ReadOnlyMemory<byte> _rowIdPayload;
    private int _rowIdPayloadOffset;
    private List<long>? _orderedTextRowIds;
    private int _orderedTextRowIdOffset;
    private DbValue[]? _rowBuffer;
    private bool _reuseCurrentRowBuffer = true;
    private bool _reuseCurrentBatch = true;
    private int? _maxDecodedColumnIndex;
    private int[]? _decodedColumnIndices;
    private PreDecodeFilterSpec _preDecodeFilter;
    private bool _hasPreDecodeFilter;
    private PreDecodeFilterSpec[]? _additionalPreDecodeFilters;
    private ReadOnlyMemory<byte> _currentPayload;
    private RowBatch _currentBatch;

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => _reuseCurrentRowBuffer;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
    public long CurrentRowId { get; private set; }
    public ReadOnlyMemory<byte> CurrentPayload => _currentPayload;

    internal IIndexStore IndexStore => _indexStore;
    internal BTree TableTree => _tableTree;
    internal int KeyColumnIndex => _keyColumnIndex;
    internal IndexScanRange ScanRange => _scanRange;
    internal bool UsesOrderedTextPayload => _usesOrderedTextPayload;

    public IndexOrderedScanOperator(
        IIndexStore indexStore,
        BTree tableTree,
        TableSchema schema,
        int keyColumnIndex,
        IndexScanRange scanRange,
        bool usesOrderedTextPayload = false,
        string? orderedTextLowerBound = null,
        bool orderedTextLowerInclusive = true,
        string? orderedTextUpperBound = null,
        bool orderedTextUpperInclusive = true,
        IRecordSerializer? recordSerializer = null)
    {
        _indexStore = indexStore;
        _tableTree = tableTree;
        _schema = schema;
        _keyColumnIndex = keyColumnIndex;
        _scanRange = scanRange;
        _usesOrderedTextPayload = usesOrderedTextPayload;
        _orderedTextLowerBound = orderedTextLowerBound;
        _orderedTextLowerInclusive = orderedTextLowerInclusive;
        _orderedTextUpperBound = orderedTextUpperBound;
        _orderedTextUpperInclusive = orderedTextUpperInclusive;
        _recordSerializer = recordSerializer ?? new DefaultRecordSerializer();
        OutputSchema = schema.Columns as ColumnDefinition[] ?? schema.Columns.ToArray();
        _currentBatch = CreateBatch(GetTargetColumnCount());
    }

    /// <summary>
    /// Hint the scan to decode only columns up to this index.
    /// </summary>
    public void SetDecodedColumnUpperBound(int maxColumnIndex)
    {
        _decodedColumnIndices = null;
        _maxDecodedColumnIndex = maxColumnIndex;
    }

    public void SetDecodedColumnIndices(ReadOnlySpan<int> columnIndices)
    {
        _decodedColumnIndices = BoundColumnAccessHelper.NormalizeColumnIndices(
            columnIndices,
            _schema.Columns.Count,
            out int maxColumnIndex);
        _maxDecodedColumnIndex = maxColumnIndex;
    }

    public void SetPreDecodeFilter(int columnIndex, BinaryOp op, DbValue literal)
        => ((IPreDecodeFilterSupport)this).SetPreDecodeFilter(new PreDecodeFilterSpec(_recordSerializer, columnIndex, op, literal));

    void IPreDecodeFilterSupport.SetPreDecodeFilter(in PreDecodeFilterSpec filter)
    {
        if (_hasPreDecodeFilter)
        {
            AppendAdditionalPreDecodeFilter(filter);
            return;
        }

        _preDecodeFilter = filter;
        _hasPreDecodeFilter = true;
    }

    public ValueTask OpenAsync(CancellationToken ct = default)
    {
        _cursor = _indexStore.CreateCursor(_scanRange);
        _rowIdPayload = ReadOnlyMemory<byte>.Empty;
        _rowIdPayloadOffset = 0;
        _orderedTextRowIdOffset = 0;
        _orderedTextRowIds?.Clear();
        _currentPayload = ReadOnlyMemory<byte>.Empty;
        _rowBuffer = null;
        _currentBatch = CreateBatch(GetTargetColumnCount());
        Current = Array.Empty<DbValue>();
        return ValueTask.CompletedTask;
    }

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (!await TryMoveNextPayloadAsync(ct))
            return false;

        PopulateCurrentFromPayload(_currentPayload.Span);
        return true;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public void SetReuseCurrentRowBuffer(bool reuse)
    {
        _reuseCurrentRowBuffer = reuse;
        if (!reuse)
        {
            _rowBuffer = null;
            Current = Array.Empty<DbValue>();
        }
    }

    public async ValueTask<bool> MoveNextBatchAsync(CancellationToken ct = default)
    {
        int targetColumnCount = GetTargetColumnCount();
        var batch = _reuseCurrentBatch ? EnsureBatch(targetColumnCount) : CreateBatch(targetColumnCount);
        batch.Reset();

        while (batch.Count < batch.Capacity && await TryMoveNextPayloadAsync(ct))
        {
            int rowIndex = batch.Count;
            DecodePayloadInto(_currentPayload.Span, targetColumnCount, batch.GetWritableRowSpan(rowIndex));
            batch.CommitWrittenRow(rowIndex);
        }

        _currentBatch = batch;
        if (batch.Count == 0)
            _currentPayload = ReadOnlyMemory<byte>.Empty;

        return batch.Count > 0;
    }

    bool IBatchOperator.ReusesCurrentBatch => _reuseCurrentBatch;
    RowBatch IBatchOperator.CurrentBatch => _currentBatch;

    public void SetReuseCurrentBatch(bool reuse)
    {
        _reuseCurrentBatch = reuse;
        if (!reuse)
            _currentBatch = CreateBatch(GetTargetColumnCount());
    }

    private void EnsureRowBuffer(int columnCount)
    {
        if (_rowBuffer == null || _rowBuffer.Length != columnCount)
            _rowBuffer = columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];
    }

    private async ValueTask<bool> TryMoveNextPayloadAsync(CancellationToken ct)
    {
        if (_cursor == null)
            return false;

        while (true)
        {
            if (_usesOrderedTextPayload)
            {
                if (_orderedTextRowIds is { Count: > 0 } &&
                    _orderedTextRowIdOffset < _orderedTextRowIds.Count)
                {
                    long rowId = _orderedTextRowIds[_orderedTextRowIdOffset++];
                    ReadOnlyMemory<byte>? orderedPayload;
                    if (_tableTree.TryFindCachedMemory(rowId, out var cachedOrderedPayload))
                    {
                        orderedPayload = cachedOrderedPayload;
                    }
                    else
                    {
                        orderedPayload = await _tableTree.FindMemoryAsync(rowId, ct);
                    }

                    if (orderedPayload is not { } orderedPayloadMemory)
                        continue;

                    if (_hasPreDecodeFilter && !EvaluatePreDecodeFilter(orderedPayloadMemory.Span))
                        continue;

                    CurrentRowId = rowId;
                    _currentPayload = orderedPayloadMemory;
                    return true;
                }
            }
            else if (_rowIdPayloadOffset + 8 <= _rowIdPayload.Length)
            {
                long rowId = BinaryPrimitives.ReadInt64LittleEndian(
                    _rowIdPayload.Span.Slice(_rowIdPayloadOffset, 8));
                _rowIdPayloadOffset += 8;

                ReadOnlyMemory<byte>? payload;
                if (_tableTree.TryFindCachedMemory(rowId, out var cachedPayload))
                {
                    payload = cachedPayload;
                }
                else
                {
                    payload = await _tableTree.FindMemoryAsync(rowId, ct);
                }

                if (payload is not { } payloadMemory)
                    continue;

                if (_hasPreDecodeFilter && !EvaluatePreDecodeFilter(payloadMemory.Span))
                    continue;

                CurrentRowId = rowId;
                _currentPayload = payloadMemory;
                return true;
            }

            if (!await _cursor.MoveNextAsync(ct))
            {
                _currentPayload = ReadOnlyMemory<byte>.Empty;
                return false;
            }

            if (_usesOrderedTextPayload)
            {
                if (!OrderedTextIndexPayloadCodec.IsEncoded(_cursor.CurrentValue.Span))
                {
                    throw new InvalidOperationException(
                        "SQL text index payload format mismatch detected. Rebuild the index before continuing.");
                }

                _orderedTextRowIds ??= [];
                _orderedTextRowIds.Clear();
                if (!OrderedTextIndexPayloadCodec.TryCollectMatchingRowIdsInRange(
                        _cursor.CurrentValue.Span,
                        _orderedTextLowerBound,
                        _orderedTextLowerInclusive,
                        _orderedTextUpperBound,
                        _orderedTextUpperInclusive,
                        _orderedTextRowIds))
                {
                    throw new InvalidOperationException(
                        "SQL text index payload format mismatch detected. Rebuild the index before continuing.");
                }

                _orderedTextRowIdOffset = 0;
                continue;
            }

            _rowIdPayload = _cursor.CurrentValue;
            _rowIdPayloadOffset = 0;
        }
    }

    private int GetTargetColumnCount()
        => _maxDecodedColumnIndex.HasValue
            ? Math.Max(0, _maxDecodedColumnIndex.Value + 1)
            : _schema.Columns.Count;

    private RowBatch EnsureBatch(int columnCount)
    {
        if (_currentBatch.ColumnCount != columnCount)
            _currentBatch = CreateBatch(columnCount);

        return _currentBatch;
    }

    private static RowBatch CreateBatch(int columnCount) => new(columnCount, DefaultBatchSize);

    private void PopulateCurrentFromPayload(ReadOnlySpan<byte> payload)
    {
        int targetColumnCount = GetTargetColumnCount();
        var decodedColumnIndices = _decodedColumnIndices;

        if (decodedColumnIndices is { Length: > 0 })
        {
            if (_reuseCurrentRowBuffer)
            {
                EnsureRowBuffer(targetColumnCount);
                if (targetColumnCount > 0)
                    Array.Fill(_rowBuffer!, DbValue.Null, 0, targetColumnCount);
                _recordSerializer.DecodeSelectedInto(payload, _rowBuffer!, decodedColumnIndices);
                Current = _rowBuffer!;
                return;
            }

            var sparseRow = targetColumnCount == 0 ? Array.Empty<DbValue>() : new DbValue[targetColumnCount];
            if (targetColumnCount > 0)
            {
                Array.Fill(sparseRow, DbValue.Null);
                _recordSerializer.DecodeSelectedInto(payload, sparseRow, decodedColumnIndices);
            }

            Current = sparseRow;
            return;
        }

        if (_reuseCurrentRowBuffer)
        {
            EnsureRowBuffer(targetColumnCount);
            int decodedCount = _recordSerializer.DecodeInto(payload, _rowBuffer!);
            if (decodedCount < targetColumnCount)
                Array.Fill(_rowBuffer!, DbValue.Null, decodedCount, targetColumnCount - decodedCount);
            Current = _rowBuffer!;
            return;
        }

        var row = targetColumnCount == 0 ? Array.Empty<DbValue>() : new DbValue[targetColumnCount];
        int decoded = _recordSerializer.DecodeInto(payload, row);
        if (decoded < targetColumnCount)
            Array.Fill(row, DbValue.Null, decoded, targetColumnCount - decoded);
        Current = row;
    }

    private void DecodePayloadInto(ReadOnlySpan<byte> payload, int targetColumnCount, Span<DbValue> destination)
    {
        var decodedColumnIndices = _decodedColumnIndices;
        if (decodedColumnIndices is { Length: > 0 })
        {
            if (targetColumnCount > 0)
                destination[..targetColumnCount].Fill(DbValue.Null);

            _recordSerializer.DecodeSelectedInto(payload, destination, decodedColumnIndices);
            return;
        }

        int decodedCount = _recordSerializer.DecodeInto(payload, destination);
        if (decodedCount < targetColumnCount)
            destination[decodedCount..targetColumnCount].Fill(DbValue.Null);
    }

    private bool EvaluatePreDecodeFilter(ReadOnlySpan<byte> payload)
    {
        if (!BoundColumnAccessHelper.EvaluatePreDecodeFilter(payload, _recordSerializer, _preDecodeFilter))
        {
            return false;
        }

        return _additionalPreDecodeFilters is not { Length: > 0 }
            || BoundColumnAccessHelper.EvaluatePreDecodeFilters(payload, _recordSerializer, _additionalPreDecodeFilters);
    }

    private void AppendAdditionalPreDecodeFilter(in PreDecodeFilterSpec filter)
    {
        if (_additionalPreDecodeFilters == null)
        {
            _additionalPreDecodeFilters = [filter];
            return;
        }

        int length = _additionalPreDecodeFilters.Length;
        Array.Resize(ref _additionalPreDecodeFilters, length + 1);
        _additionalPreDecodeFilters[length] = filter;
    }
}

/// <summary>
/// Primary-key lookup operator — performs a direct B+tree key lookup against the table.
/// </summary>
public sealed class PrimaryKeyLookupOperator : IOperator, IPreDecodeFilterSupport, IRowBufferReuseController, IEstimatedRowCountProvider, IEncodedPayloadSource
{
    private readonly BTree _tableTree;
    private readonly TableSchema _schema;
    private readonly long _seekKey;
    private readonly IRecordSerializer _recordSerializer;
    private bool _consumed;
    private int? _maxDecodedColumnIndex;
    private int[]? _decodedColumnIndices;
    private PreDecodeFilterSpec _preDecodeFilter;
    private bool _hasPreDecodeFilter;
    private PreDecodeFilterSpec[]? _additionalPreDecodeFilters;
    private DbValue[]? _rowBuffer;
    private bool _reuseBuffer = true;
    private ReadOnlyMemory<byte> _currentPayload;

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => _reuseBuffer;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
    public int? EstimatedRowCount => 1;
    public long CurrentRowId { get; private set; }
    public ReadOnlyMemory<byte> CurrentPayload => _currentPayload;
    internal BTree TableTree => _tableTree;
    internal long SeekKey => _seekKey;

    public PrimaryKeyLookupOperator(
        BTree tableTree,
        TableSchema schema,
        long seekKey,
        IRecordSerializer? recordSerializer = null)
    {
        _tableTree = tableTree;
        _schema = schema;
        _seekKey = seekKey;
        _recordSerializer = recordSerializer ?? new DefaultRecordSerializer();
        OutputSchema = schema.Columns as ColumnDefinition[] ?? schema.Columns.ToArray();
    }

    public void SetReuseCurrentRowBuffer(bool reuse)
    {
        _reuseBuffer = reuse;
    }

    /// <summary>
    /// Hint the lookup to decode only columns up to this index.
    /// </summary>
    public void SetDecodedColumnUpperBound(int maxColumnIndex)
    {
        _decodedColumnIndices = null;
        _maxDecodedColumnIndex = maxColumnIndex;
    }

    public void SetDecodedColumnIndices(ReadOnlySpan<int> columnIndices)
    {
        _decodedColumnIndices = BoundColumnAccessHelper.NormalizeColumnIndices(
            columnIndices,
            _schema.Columns.Count,
            out int maxColumnIndex);
        _maxDecodedColumnIndex = maxColumnIndex;
    }

    public void SetPreDecodeFilter(int columnIndex, BinaryOp op, DbValue literal)
        => ((IPreDecodeFilterSupport)this).SetPreDecodeFilter(new PreDecodeFilterSpec(_recordSerializer, columnIndex, op, literal));

    void IPreDecodeFilterSupport.SetPreDecodeFilter(in PreDecodeFilterSpec filter)
    {
        if (_hasPreDecodeFilter)
        {
            AppendAdditionalPreDecodeFilter(filter);
            return;
        }

        _preDecodeFilter = filter;
        _hasPreDecodeFilter = true;
    }

    public ValueTask OpenAsync(CancellationToken ct = default)
    {
        _consumed = false;
        _currentPayload = ReadOnlyMemory<byte>.Empty;
        return ValueTask.CompletedTask;
    }

    public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (_consumed) return ValueTask.FromResult(false);
        _consumed = true;

        if (_tableTree.TryFindCachedMemory(_seekKey, out var cachedPayload))
            return ValueTask.FromResult(EmitFromPayload(cachedPayload));

        return MoveNextUncachedAsync(ct);
    }

    private async ValueTask<bool> MoveNextUncachedAsync(CancellationToken ct)
    {
        var payload = await _tableTree.FindMemoryAsync(_seekKey, ct);
        return EmitFromPayload(payload);
    }

    private bool EmitFromPayload(ReadOnlyMemory<byte>? payload)
    {
        if (payload is not { } payloadMemory)
        {
            _currentPayload = ReadOnlyMemory<byte>.Empty;
            return false;
        }

        var payloadSpan = payloadMemory.Span;
        if (_hasPreDecodeFilter && !EvaluatePreDecodeFilter(payloadSpan))
        {
            _currentPayload = ReadOnlyMemory<byte>.Empty;
            return false;
        }

        CurrentRowId = _seekKey;
        _currentPayload = payloadMemory;

        if (_reuseBuffer)
        {
            int targetCount = _maxDecodedColumnIndex.HasValue
                ? _maxDecodedColumnIndex.Value + 1
                : _schema.Columns.Count;
            if (_rowBuffer == null || _rowBuffer.Length < targetCount)
                _rowBuffer = new DbValue[targetCount];
            var decodedColumnIndices = _decodedColumnIndices;
            if (decodedColumnIndices is { Length: > 0 })
            {
                if (targetCount > 0)
                    Array.Fill(_rowBuffer, DbValue.Null, 0, targetCount);
                _recordSerializer.DecodeSelectedInto(payloadSpan, _rowBuffer.AsSpan(0, targetCount), decodedColumnIndices);
            }
            else
            {
                int decoded = _recordSerializer.DecodeInto(payloadSpan, _rowBuffer.AsSpan(0, targetCount));
                for (int i = decoded; i < targetCount; i++)
                    _rowBuffer[i] = DbValue.Null;
            }
            Current = _rowBuffer;
        }
        else
        {
            var decodedColumnIndices = _decodedColumnIndices;
            if (decodedColumnIndices is { Length: > 0 })
            {
                int targetCount = _maxDecodedColumnIndex.HasValue
                    ? Math.Max(0, _maxDecodedColumnIndex.Value + 1)
                    : _schema.Columns.Count;
                var row = targetCount == 0 ? Array.Empty<DbValue>() : new DbValue[targetCount];
                if (targetCount > 0)
                {
                    Array.Fill(row, DbValue.Null);
                    _recordSerializer.DecodeSelectedInto(payloadSpan, row, decodedColumnIndices);
                }

                Current = row;
            }
            else
            {
                var decoded = _maxDecodedColumnIndex.HasValue
                    ? _recordSerializer.DecodeUpTo(payloadSpan, _maxDecodedColumnIndex.Value)
                    : _recordSerializer.Decode(payloadSpan);

                if (!_maxDecodedColumnIndex.HasValue && decoded.Length < _schema.Columns.Count)
                {
                    var padded = new DbValue[_schema.Columns.Count];
                    decoded.CopyTo(padded, 0);
                    for (int i = decoded.Length; i < padded.Length; i++)
                        padded[i] = DbValue.Null;
                    Current = padded;
                }
                else
                {
                    Current = decoded;
                }
            }
        }

        return true;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private bool EvaluatePreDecodeFilter(ReadOnlySpan<byte> payload)
    {
        if (!BoundColumnAccessHelper.EvaluatePreDecodeFilter(payload, _recordSerializer, _preDecodeFilter))
        {
            return false;
        }

        return _additionalPreDecodeFilters is not { Length: > 0 }
            || BoundColumnAccessHelper.EvaluatePreDecodeFilters(payload, _recordSerializer, _additionalPreDecodeFilters);
    }

    private void AppendAdditionalPreDecodeFilter(in PreDecodeFilterSpec filter)
    {
        if (_additionalPreDecodeFilters == null)
        {
            _additionalPreDecodeFilters = [filter];
            return;
        }

        int length = _additionalPreDecodeFilters.Length;
        Array.Resize(ref _additionalPreDecodeFilters, length + 1);
        _additionalPreDecodeFilters[length] = filter;
    }
}

/// <summary>
/// Primary-key lookup projection fast path.
/// Verifies row existence via table key lookup and returns one row where every projected value is the PK key.
/// </summary>
public sealed class PrimaryKeyProjectionLookupOperator : IOperator, IEstimatedRowCountProvider
{
    private readonly BTree _tableTree;
    private readonly long _seekKey;
    private readonly DbValue[] _projectedRow;
    private bool _consumed;

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => false;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
    public int? EstimatedRowCount => 1;

    public PrimaryKeyProjectionLookupOperator(BTree tableTree, long seekKey, ColumnDefinition[] outputSchema)
    {
        _tableTree = tableTree;
        _seekKey = seekKey;
        OutputSchema = outputSchema;

        if (outputSchema.Length == 0)
        {
            _projectedRow = Array.Empty<DbValue>();
        }
        else
        {
            _projectedRow = new DbValue[outputSchema.Length];
            var keyValue = DbValue.FromInteger(seekKey);
            for (int i = 0; i < _projectedRow.Length; i++)
                _projectedRow[i] = keyValue;
        }
    }

    public ValueTask OpenAsync(CancellationToken ct = default)
    {
        _consumed = false;
        Current = Array.Empty<DbValue>();
        return ValueTask.CompletedTask;
    }

    public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (_consumed) return ValueTask.FromResult(false);
        _consumed = true;

        if (_tableTree.TryFindCachedMemory(_seekKey, out var cachedPayload))
        {
            if (cachedPayload == null)
                return ValueTask.FromResult(false);

            Current = _projectedRow;
            return ValueTask.FromResult(true);
        }

        return MoveNextUncachedAsync(ct);
    }

    private async ValueTask<bool> MoveNextUncachedAsync(CancellationToken ct)
    {
        // Preserve semantics by checking that the row actually exists.
        var payload = await _tableTree.FindMemoryAsync(_seekKey, ct);
        if (payload == null)
            return false;

        Current = _projectedRow;
        return true;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

/// <summary>
/// Unique secondary-index lookup projection fast path.
/// Returns any projection composed only of the rowid (integer primary key)
/// and the indexed integer lookup literal
/// without fetching the base table row.
/// </summary>
internal enum LookupProjectionValueKind
{
    RowId,
    LookupValue,
}

internal enum GroupedIndexAggregateProjectionKind
{
    GroupKey,
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

public enum GroupedIndexAggregateCountPredicateKind
{
    None,
    Equals,
    NotEquals,
    LessThan,
    GreaterThan,
    LessOrEqual,
    GreaterOrEqual,
}

public sealed class UniqueIndexProjectionLookupOperator : IOperator, IEstimatedRowCountProvider
{
    private readonly IIndexStore _indexStore;
    private readonly long _seekValue;
    private readonly LookupProjectionValueKind[] _projectionKinds;
    private readonly DbValue[] _projectedRow;
    private readonly bool _requiresRowId;
    private bool _consumed;

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => false;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
    public int? EstimatedRowCount => 1;

    public UniqueIndexProjectionLookupOperator(
        IIndexStore indexStore,
        long seekValue,
        ColumnDefinition[] outputSchema,
        ReadOnlySpan<int> projectionColumnIndices,
        int primaryKeyColumnIndex,
        int predicateColumnIndex)
    {
        _indexStore = indexStore;
        _seekValue = seekValue;
        OutputSchema = outputSchema;
        _projectionKinds = BuildProjectionKinds(projectionColumnIndices, primaryKeyColumnIndex, predicateColumnIndex);
        _requiresRowId = ProjectionRequiresRowId(_projectionKinds);

        if (outputSchema.Length == 0)
        {
            _projectedRow = Array.Empty<DbValue>();
        }
        else
        {
            _projectedRow = new DbValue[outputSchema.Length];
            DbValue lookupValue = DbValue.FromInteger(seekValue);
            for (int i = 0; i < _projectedRow.Length; i++)
            {
                _projectedRow[i] = _projectionKinds[i] == LookupProjectionValueKind.LookupValue
                    ? lookupValue
                    : DbValue.Null;
            }
        }
    }

    public ValueTask OpenAsync(CancellationToken ct = default)
    {
        _consumed = false;
        Current = Array.Empty<DbValue>();
        return ValueTask.CompletedTask;
    }

    public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (_consumed)
            return ValueTask.FromResult(false);

        _consumed = true;

        if (_indexStore is ICacheAwareIndexStore cacheAware &&
            cacheAware.TryFindCached(_seekValue, out var cachedPayload))
        {
            return ValueTask.FromResult(EmitFromIndexPayload(cachedPayload));
        }

        return MoveNextUncachedAsync(ct);
    }

    private async ValueTask<bool> MoveNextUncachedAsync(CancellationToken ct)
    {
        byte[]? payload = await _indexStore.FindAsync(_seekValue, ct);
        return EmitFromIndexPayload(payload);
    }

    private bool EmitFromIndexPayload(byte[]? payload)
    {
        if (payload == null || payload.Length < sizeof(long))
            return false;

        if (_requiresRowId && _projectedRow.Length > 0)
        {
            long rowId = BinaryPrimitives.ReadInt64LittleEndian(payload.AsSpan(0, sizeof(long)));
            DbValue rowIdValue = DbValue.FromInteger(rowId);
            for (int i = 0; i < _projectedRow.Length; i++)
            {
                if (_projectionKinds[i] == LookupProjectionValueKind.RowId)
                    _projectedRow[i] = rowIdValue;
            }
        }

        Current = _projectedRow;
        return true;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static LookupProjectionValueKind[] BuildProjectionKinds(
        ReadOnlySpan<int> projectionColumnIndices,
        int primaryKeyColumnIndex,
        int predicateColumnIndex)
    {
        var projectionKinds = new LookupProjectionValueKind[projectionColumnIndices.Length];
        for (int i = 0; i < projectionColumnIndices.Length; i++)
        {
            int columnIndex = projectionColumnIndices[i];
            if (columnIndex == primaryKeyColumnIndex)
            {
                projectionKinds[i] = LookupProjectionValueKind.RowId;
                continue;
            }

            if (columnIndex == predicateColumnIndex)
            {
                projectionKinds[i] = LookupProjectionValueKind.LookupValue;
                continue;
            }

            throw new InvalidOperationException("Covered index projection can only emit the primary key or predicate column.");
        }

        return projectionKinds;
    }

    private static bool ProjectionRequiresRowId(ReadOnlySpan<LookupProjectionValueKind> projectionKinds)
    {
        for (int i = 0; i < projectionKinds.Length; i++)
        {
            if (projectionKinds[i] == LookupProjectionValueKind.RowId)
                return true;
        }

        return false;
    }
}

/// <summary>
/// Secondary-index equality lookup projection fast path for non-unique indexes.
/// Returns any projection composed only of the rowid (integer primary key)
/// and the indexed integer lookup literal
/// for each matching rowid without fetching base table rows.
/// </summary>
public sealed class IndexScanProjectionOperator : IOperator, IEstimatedRowCountProvider
{
    private readonly IIndexStore _indexStore;
    private readonly long _seekValue;
    private readonly LookupProjectionValueKind[] _projectionKinds;
    private readonly DbValue[] _templateRow;
    private readonly bool _requiresRowId;
    private ReadOnlyMemory<byte> _rowIdPayload;
    private int _rowIdPayloadOffset;

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => true;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
    public int? EstimatedRowCount { get; private set; }

    public IndexScanProjectionOperator(
        IIndexStore indexStore,
        long seekValue,
        ColumnDefinition[] outputSchema,
        ReadOnlySpan<int> projectionColumnIndices,
        int primaryKeyColumnIndex,
        int predicateColumnIndex)
    {
        _indexStore = indexStore;
        _seekValue = seekValue;
        OutputSchema = outputSchema;
        _projectionKinds = BuildProjectionKinds(projectionColumnIndices, primaryKeyColumnIndex, predicateColumnIndex);
        _requiresRowId = ProjectionRequiresRowId(_projectionKinds);

        if (outputSchema.Length == 0)
        {
            _templateRow = Array.Empty<DbValue>();
        }
        else
        {
            _templateRow = new DbValue[outputSchema.Length];
            DbValue lookupValue = DbValue.FromInteger(seekValue);
            for (int i = 0; i < _templateRow.Length; i++)
            {
                _templateRow[i] = _projectionKinds[i] == LookupProjectionValueKind.LookupValue
                    ? lookupValue
                    : DbValue.Null;
            }
        }
    }

    public ValueTask OpenAsync(CancellationToken ct = default)
    {
        if (_indexStore is ICacheAwareIndexStore cacheAware &&
            cacheAware.TryFindCached(_seekValue, out var cachedPayload))
        {
            _rowIdPayload = cachedPayload ?? ReadOnlyMemory<byte>.Empty;
            _rowIdPayloadOffset = 0;
            EstimatedRowCount = _rowIdPayload.Length / sizeof(long);
            Current = Array.Empty<DbValue>();
            return ValueTask.CompletedTask;
        }

        return OpenUncachedAsync(ct);
    }

    private async ValueTask OpenUncachedAsync(CancellationToken ct)
    {
        byte[]? payload = await _indexStore.FindAsync(_seekValue, ct);
        _rowIdPayload = payload ?? ReadOnlyMemory<byte>.Empty;
        _rowIdPayloadOffset = 0;
        EstimatedRowCount = _rowIdPayload.Length / sizeof(long);
        Current = Array.Empty<DbValue>();
    }

    public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (_rowIdPayloadOffset + sizeof(long) > _rowIdPayload.Length)
            return ValueTask.FromResult(false);

        if (!_requiresRowId)
        {
            Current = _templateRow;
        }
        else
        {
            long rowId = BinaryPrimitives.ReadInt64LittleEndian(
                _rowIdPayload.Span.Slice(_rowIdPayloadOffset, sizeof(long)));
            DbValue rowIdValue = DbValue.FromInteger(rowId);
            for (int i = 0; i < _templateRow.Length; i++)
            {
                if (_projectionKinds[i] == LookupProjectionValueKind.RowId)
                    _templateRow[i] = rowIdValue;
            }

            Current = _templateRow;
        }

        _rowIdPayloadOffset += sizeof(long);
        return ValueTask.FromResult(true);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static LookupProjectionValueKind[] BuildProjectionKinds(
        ReadOnlySpan<int> projectionColumnIndices,
        int primaryKeyColumnIndex,
        int predicateColumnIndex)
    {
        var projectionKinds = new LookupProjectionValueKind[projectionColumnIndices.Length];
        for (int i = 0; i < projectionColumnIndices.Length; i++)
        {
            int columnIndex = projectionColumnIndices[i];
            if (columnIndex == primaryKeyColumnIndex)
            {
                projectionKinds[i] = LookupProjectionValueKind.RowId;
                continue;
            }

            if (columnIndex == predicateColumnIndex)
            {
                projectionKinds[i] = LookupProjectionValueKind.LookupValue;
                continue;
            }

            throw new InvalidOperationException("Covered index projection can only emit the primary key or predicate column.");
        }

        return projectionKinds;
    }

    private static bool ProjectionRequiresRowId(ReadOnlySpan<LookupProjectionValueKind> projectionKinds)
    {
        for (int i = 0; i < projectionKinds.Length; i++)
        {
            if (projectionKinds[i] == LookupProjectionValueKind.RowId)
                return true;
        }

        return false;
    }
}

/// <summary>
/// Covered equality lookup projection fast path for hashed secondary indexes.
/// Projects the integer primary key plus indexed key columns without fetching
/// base table rows when the hashed bucket stores explicit key components.
/// Legacy rowid-only hashed buckets fall back to row verification for correctness.
/// </summary>
public sealed class HashedIndexProjectionLookupOperator : IOperator, IEstimatedRowCountProvider
{
    private readonly IIndexStore _indexStore;
    private readonly BTree _tableTree;
    private readonly TableSchema _schema;
    private readonly long _seekValue;
    private readonly IRecordSerializer _recordSerializer;
    private readonly int[] _keyColumnIndices;
    private readonly DbValue[] _keyComponents;
    private readonly DbValue[] _templateRow;
    private readonly int[] _rowIdTargetIndices;
    private readonly bool _requiresRowId;
    private RecordColumnAccessor?[]? _keyAccessors;
    private byte[][]? _keyTextBytes;
    private bool _keyTextBytesInitialized;
    private ReadOnlyMemory<byte> _rowIdPayload;
    private int _rowIdPayloadOffset;
    private bool _rowIdsVerifiedByIndexPayload;

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => true;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
    public int? EstimatedRowCount { get; private set; }

    public HashedIndexProjectionLookupOperator(
        IIndexStore indexStore,
        BTree tableTree,
        TableSchema schema,
        long seekValue,
        ColumnDefinition[] outputSchema,
        ReadOnlySpan<int> projectionColumnIndices,
        ReadOnlySpan<int> keyColumnIndices,
        ReadOnlySpan<DbValue> keyComponents,
        IRecordSerializer? recordSerializer = null)
    {
        _indexStore = indexStore;
        _tableTree = tableTree;
        _schema = schema;
        _seekValue = seekValue;
        _recordSerializer = recordSerializer ?? new DefaultRecordSerializer();
        _keyColumnIndices = keyColumnIndices.ToArray();
        _keyComponents = keyComponents.ToArray();
        OutputSchema = outputSchema;
        _templateRow = BuildTemplateRow(
            projectionColumnIndices,
            schema.PrimaryKeyColumnIndex,
            _keyColumnIndices,
            _keyComponents,
            out _rowIdTargetIndices);
        _requiresRowId = _rowIdTargetIndices.Length != 0;
    }

    public ValueTask OpenAsync(CancellationToken ct = default)
    {
        if (_indexStore is ICacheAwareIndexStore cacheAware &&
            cacheAware.TryFindCached(_seekValue, out var cachedPayload))
        {
            InitializeRowIdPayload(cachedPayload);
            return ValueTask.CompletedTask;
        }

        return OpenUncachedAsync(ct);
    }

    private async ValueTask OpenUncachedAsync(CancellationToken ct)
    {
        InitializeRowIdPayload(await _indexStore.FindAsync(_seekValue, ct));
    }

    public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        while (true)
        {
            if (_rowIdPayloadOffset + sizeof(long) > _rowIdPayload.Length)
                return ValueTask.FromResult(false);

            if (!_requiresRowId)
            {
                _rowIdPayloadOffset += sizeof(long);
                Current = _templateRow;
                return ValueTask.FromResult(true);
            }

            long rowId = BinaryPrimitives.ReadInt64LittleEndian(
                _rowIdPayload.Span.Slice(_rowIdPayloadOffset, sizeof(long)));
            _rowIdPayloadOffset += sizeof(long);

            if (_rowIdsVerifiedByIndexPayload)
            {
                PopulateCurrent(rowId);
                return ValueTask.FromResult(true);
            }

            if (_tableTree.TryFindCachedMemory(rowId, out var cachedPayload))
            {
                if (cachedPayload is not { } cachedPayloadMemory)
                    continue;

                if (!MatchesExpectedKeyComponents(cachedPayloadMemory.Span))
                    continue;

                PopulateCurrent(rowId);
                return ValueTask.FromResult(true);
            }

            return MoveNextUncachedAsync(rowId, ct);
        }
    }

    private async ValueTask<bool> MoveNextUncachedAsync(long rowId, CancellationToken ct)
    {
        while (true)
        {
            var payload = await _tableTree.FindMemoryAsync(rowId, ct);
            if (payload is { } payloadMemory && MatchesExpectedKeyComponents(payloadMemory.Span))
            {
                PopulateCurrent(rowId);
                return true;
            }

            if (_rowIdPayloadOffset + sizeof(long) > _rowIdPayload.Length)
                return false;

            rowId = BinaryPrimitives.ReadInt64LittleEndian(
                _rowIdPayload.Span.Slice(_rowIdPayloadOffset, sizeof(long)));
            _rowIdPayloadOffset += sizeof(long);
        }
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private void InitializeRowIdPayload(byte[]? payload)
    {
        if (payload is { Length: > 0 } &&
            HashedIndexPayloadCodec.TryGetMatchingRowIdPayloadSlice(payload, _keyComponents, EnsureKeyTextBytes(), out var matchingRowIds))
        {
            _rowIdPayload = matchingRowIds;
            _rowIdsVerifiedByIndexPayload = true;
        }
        else
        {
            _rowIdPayload = payload ?? ReadOnlyMemory<byte>.Empty;
            _rowIdsVerifiedByIndexPayload = false;
        }

        _rowIdPayloadOffset = 0;
        EstimatedRowCount = _rowIdPayload.Length / sizeof(long);
        Current = Array.Empty<DbValue>();
    }

    private bool MatchesExpectedKeyComponents(ReadOnlySpan<byte> payload)
        => BoundColumnAccessHelper.MatchesKeyComponents(
            payload,
            _recordSerializer,
            _keyColumnIndices,
            _keyComponents,
            EnsureKeyAccessors(),
            EnsureKeyTextBytes());

    private void PopulateCurrent(long rowId)
    {
        if (!_requiresRowId)
        {
            Current = _templateRow;
            return;
        }

        DbValue rowIdValue = DbValue.FromInteger(rowId);
        for (int i = 0; i < _rowIdTargetIndices.Length; i++)
        {
            _templateRow[_rowIdTargetIndices[i]] = rowIdValue;
        }

        Current = _templateRow;
    }

    private static DbValue[] BuildTemplateRow(
        ReadOnlySpan<int> projectionColumnIndices,
        int primaryKeyColumnIndex,
        ReadOnlySpan<int> keyColumnIndices,
        ReadOnlySpan<DbValue> keyComponents,
        out int[] rowIdTargetIndices)
    {
        if (projectionColumnIndices.Length == 0)
        {
            rowIdTargetIndices = Array.Empty<int>();
            return Array.Empty<DbValue>();
        }

        var row = new DbValue[projectionColumnIndices.Length];
        var rowIdTargets = new List<int>();
        for (int i = 0; i < projectionColumnIndices.Length; i++)
        {
            int columnIndex = projectionColumnIndices[i];
            if (columnIndex == primaryKeyColumnIndex)
            {
                row[i] = DbValue.Null;
                rowIdTargets.Add(i);
                continue;
            }

            int keyComponentIndex = -1;
            for (int j = 0; j < keyColumnIndices.Length; j++)
            {
                if (keyColumnIndices[j] == columnIndex)
                {
                    keyComponentIndex = j;
                    break;
                }
            }

            if (keyComponentIndex < 0)
                throw new InvalidOperationException("Covered hashed index projection can only emit the primary key or indexed key columns.");

            row[i] = keyComponents[keyComponentIndex];
        }

        rowIdTargetIndices = rowIdTargets.Count == 0 ? Array.Empty<int>() : rowIdTargets.ToArray();
        return row;
    }

    private RecordColumnAccessor?[] EnsureKeyAccessors()
        => _keyAccessors ??= BoundColumnAccessHelper.CreateAccessors(_recordSerializer, _keyColumnIndices);

    private byte[][]? EnsureKeyTextBytes()
    {
        if (!_keyTextBytesInitialized)
        {
            _keyTextBytes = BoundColumnAccessHelper.CreateTextLiteralBytes(_keyComponents);
            _keyTextBytesInitialized = true;
        }

        return _keyTextBytes;
    }
}

/// <summary>
/// Ordered/range index projection fast path for integer indexes.
/// Emits projections composed only of the rowid (integer primary key)
/// and the current integer index key without fetching base table rows.
/// </summary>
public sealed class IndexOrderedProjectionScanOperator : IOperator, IBatchOperator, IBatchBufferReuseController
{
    private const int DefaultBatchSize = 64;

    private readonly IIndexStore _indexStore;
    private readonly IndexScanRange _scanRange;
    private readonly LookupProjectionValueKind[] _projectionKinds;
    private readonly DbValue[] _rowBuffer;
    private readonly bool _requiresRowId;
    private IIndexCursor? _cursor;
    private ReadOnlyMemory<byte> _rowIdPayload;
    private int _rowIdPayloadOffset;
    private bool _reuseCurrentBatch = true;
    private RowBatch _currentBatch;

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => true;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();

    public IndexOrderedProjectionScanOperator(
        IIndexStore indexStore,
        IndexScanRange scanRange,
        ColumnDefinition[] outputSchema,
        ReadOnlySpan<int> projectionColumnIndices,
        int primaryKeyColumnIndex,
        int predicateColumnIndex)
    {
        _indexStore = indexStore;
        _scanRange = scanRange;
        OutputSchema = outputSchema;
        _projectionKinds = BuildProjectionKinds(projectionColumnIndices, primaryKeyColumnIndex, predicateColumnIndex);
        _requiresRowId = ProjectionRequiresRowId(_projectionKinds);
        _rowBuffer = outputSchema.Length == 0 ? Array.Empty<DbValue>() : new DbValue[outputSchema.Length];
        _currentBatch = CreateBatch(outputSchema.Length);
    }

    public ValueTask OpenAsync(CancellationToken ct = default)
    {
        _cursor = _indexStore.CreateCursor(_scanRange);
        _rowIdPayload = ReadOnlyMemory<byte>.Empty;
        _rowIdPayloadOffset = 0;
        _currentBatch = CreateBatch(OutputSchema.Length);
        Current = Array.Empty<DbValue>();
        return ValueTask.CompletedTask;
    }

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (_cursor == null)
            return false;

        while (true)
        {
            if (_rowIdPayloadOffset + sizeof(long) <= _rowIdPayload.Length)
            {
                long rowId = BinaryPrimitives.ReadInt64LittleEndian(
                    _rowIdPayload.Span.Slice(_rowIdPayloadOffset, sizeof(long)));
                _rowIdPayloadOffset += sizeof(long);
                PopulateCurrent(_cursor.CurrentKey, rowId);
                Current = _rowBuffer;
                return true;
            }

            if (!await _cursor.MoveNextAsync(ct))
                return false;

            _rowIdPayload = _cursor.CurrentValue;
            _rowIdPayloadOffset = 0;
        }
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public async ValueTask<bool> MoveNextBatchAsync(CancellationToken ct = default)
    {
        if (_cursor == null)
            return false;

        var batch = _reuseCurrentBatch ? EnsureBatch(OutputSchema.Length) : CreateBatch(OutputSchema.Length);
        batch.Reset();

        while (batch.Count < batch.Capacity)
        {
            while (_rowIdPayloadOffset + sizeof(long) <= _rowIdPayload.Length && batch.Count < batch.Capacity)
            {
                long rowId = BinaryPrimitives.ReadInt64LittleEndian(
                    _rowIdPayload.Span.Slice(_rowIdPayloadOffset, sizeof(long)));
                _rowIdPayloadOffset += sizeof(long);

                int rowIndex = batch.Count;
                WriteProjectedRow(batch.GetWritableRowSpan(rowIndex), _cursor.CurrentKey, rowId);
                batch.CommitWrittenRow(rowIndex);
            }

            if (batch.Count >= batch.Capacity)
                break;

            if (!await _cursor.MoveNextAsync(ct))
                break;

            _rowIdPayload = _cursor.CurrentValue;
            _rowIdPayloadOffset = 0;
        }

        _currentBatch = batch;
        return batch.Count > 0;
    }

    bool IBatchOperator.ReusesCurrentBatch => _reuseCurrentBatch;
    RowBatch IBatchOperator.CurrentBatch => _currentBatch;

    public void SetReuseCurrentBatch(bool reuse)
    {
        _reuseCurrentBatch = reuse;
        if (!reuse)
            _currentBatch = CreateBatch(OutputSchema.Length);
    }

    private void PopulateCurrent(long keyValue, long rowId)
    {
        WriteProjectedRow(_rowBuffer, keyValue, rowId);
    }

    private void WriteProjectedRow(Span<DbValue> destination, long keyValue, long rowId)
    {
        DbValue key = DbValue.FromInteger(keyValue);
        DbValue rowIdValue = _requiresRowId ? DbValue.FromInteger(rowId) : DbValue.Null;
        for (int i = 0; i < _projectionKinds.Length; i++)
            destination[i] = _projectionKinds[i] == LookupProjectionValueKind.RowId ? rowIdValue : key;
    }

    private static LookupProjectionValueKind[] BuildProjectionKinds(
        ReadOnlySpan<int> projectionColumnIndices,
        int primaryKeyColumnIndex,
        int predicateColumnIndex)
    {
        var projectionKinds = new LookupProjectionValueKind[projectionColumnIndices.Length];
        for (int i = 0; i < projectionColumnIndices.Length; i++)
        {
            int columnIndex = projectionColumnIndices[i];
            if (columnIndex == primaryKeyColumnIndex)
            {
                projectionKinds[i] = LookupProjectionValueKind.RowId;
                continue;
            }

            if (columnIndex == predicateColumnIndex)
            {
                projectionKinds[i] = LookupProjectionValueKind.LookupValue;
                continue;
            }

            throw new InvalidOperationException("Covered ordered index projection can only emit the primary key or indexed key column.");
        }

        return projectionKinds;
    }

    private static bool ProjectionRequiresRowId(ReadOnlySpan<LookupProjectionValueKind> projectionKinds)
    {
        for (int i = 0; i < projectionKinds.Length; i++)
        {
            if (projectionKinds[i] == LookupProjectionValueKind.RowId)
                return true;
        }

        return false;
    }

    private RowBatch EnsureBatch(int columnCount)
    {
        if (_currentBatch.ColumnCount != columnCount)
            _currentBatch = CreateBatch(columnCount);

        return _currentBatch;
    }

    private static RowBatch CreateBatch(int columnCount) => new(columnCount, DefaultBatchSize);
}

/// <summary>
/// Scalar aggregate fast path over a direct INTEGER index.
/// Uses only index keys and row-id payload counts, avoiding base-row fetches.
/// </summary>
public sealed class IndexKeyAggregateOperator : IOperator, IEstimatedRowCountProvider
{
    private enum AggregateKind
    {
        Count,
        Sum,
        Avg,
        Min,
        Max,
    }

    private readonly IIndexStore _indexStore;
    private readonly IndexScanRange _scanRange;
    private readonly AggregateKind _kind;
    private readonly bool _isDistinct;
    private bool _emitted;

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => false;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
    public int? EstimatedRowCount => 1;

    public IndexKeyAggregateOperator(
        IIndexStore indexStore,
        IndexScanRange scanRange,
        string functionName,
        ColumnDefinition[] outputSchema,
        bool isDistinct = false)
    {
        _indexStore = indexStore;
        _scanRange = scanRange;
        _isDistinct = isDistinct;
        _kind = functionName switch
        {
            "COUNT" => AggregateKind.Count,
            "SUM" => AggregateKind.Sum,
            "AVG" => AggregateKind.Avg,
            "MIN" => AggregateKind.Min,
            "MAX" => AggregateKind.Max,
            _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unsupported index aggregate fast path: {functionName}"),
        };
        OutputSchema = outputSchema;
    }

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        _emitted = false;
        Current = Array.Empty<DbValue>();

        if (_kind == AggregateKind.Max)
        {
            long? maxKey = await _indexStore.FindMaxKeyAsync(_scanRange, ct);
            Current = new[] { maxKey.HasValue ? DbValue.FromInteger(maxKey.Value) : DbValue.Null };
            return;
        }

        var cursor = _indexStore.CreateCursor(_scanRange);
        long count = 0;
        double sum = 0;
        bool hasAny = false;

        while (await cursor.MoveNextAsync(ct))
        {
            int entryCount = RowIdPayloadCodec.GetCount(cursor.CurrentValue.Span);
            if (entryCount <= 0)
                continue;

            long key = cursor.CurrentKey;
            int aggregateCount = _isDistinct ? 1 : entryCount;
            switch (_kind)
            {
                case AggregateKind.Count:
                    count += aggregateCount;
                    hasAny = true;
                    break;
                case AggregateKind.Sum:
                    sum += _isDistinct ? key : (double)key * entryCount;
                    count += aggregateCount;
                    hasAny = true;
                    break;
                case AggregateKind.Avg:
                    sum += _isDistinct ? key : (double)key * entryCount;
                    count += aggregateCount;
                    hasAny = true;
                    break;
                case AggregateKind.Min:
                    Current = new[] { DbValue.FromInteger(key) };
                    return;
            }
        }

        DbValue aggregate = _kind switch
        {
            AggregateKind.Count => DbValue.FromInteger(count),
            AggregateKind.Sum => !hasAny ? DbValue.FromInteger(0) : DbValue.FromInteger((long)sum),
            AggregateKind.Avg => !hasAny ? DbValue.Null : DbValue.FromReal(sum / count),
            AggregateKind.Min => DbValue.Null,
            AggregateKind.Max => DbValue.Null,
            _ => DbValue.Null,
        };

        Current = new[] { aggregate };
    }

    public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (_emitted)
            return ValueTask.FromResult(false);

        _emitted = true;
        return ValueTask.FromResult(true);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

/// <summary>
/// GROUP BY aggregate fast path over a direct INTEGER index.
/// Streams distinct index keys and computes grouped aggregates without row fetches
/// or generic hash grouping when the group key and aggregate arguments all map to
/// the same indexed integer column.
/// </summary>
public sealed class IndexGroupedAggregateOperator : IOperator
{
    private readonly IIndexStore _indexStore;
    private readonly IndexScanRange _scanRange;
    private readonly int[] _projectionKinds;
    private readonly GroupedIndexAggregateCountPredicateKind _countPredicateKind;
    private readonly long _countPredicateValue;
    private readonly DbValue[] _rowBuffer;
    private IIndexCursor? _cursor;

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => true;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();

    public IndexGroupedAggregateOperator(
        IIndexStore indexStore,
        IndexScanRange scanRange,
        ColumnDefinition[] outputSchema,
        ReadOnlySpan<int> projectionKinds,
        GroupedIndexAggregateCountPredicateKind countPredicateKind = GroupedIndexAggregateCountPredicateKind.None,
        long countPredicateValue = 0)
    {
        _indexStore = indexStore;
        _scanRange = scanRange;
        OutputSchema = outputSchema;
        _projectionKinds = projectionKinds.ToArray();
        _countPredicateKind = countPredicateKind;
        _countPredicateValue = countPredicateValue;
        _rowBuffer = outputSchema.Length == 0 ? Array.Empty<DbValue>() : new DbValue[outputSchema.Length];
    }

    public ValueTask OpenAsync(CancellationToken ct = default)
    {
        _cursor = _indexStore.CreateCursor(_scanRange);
        Current = Array.Empty<DbValue>();
        return ValueTask.CompletedTask;
    }

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (_cursor == null)
            return false;

        while (await _cursor.MoveNextAsync(ct))
        {
            int entryCount = RowIdPayloadCodec.GetCount(_cursor.CurrentValue.Span);
            if (entryCount <= 0 || !SatisfiesCountPredicate(entryCount))
                continue;

            PopulateCurrent(_cursor.CurrentKey, entryCount);
            return true;
        }

        return false;
    }

    public ValueTask DisposeAsync()
    {
        _cursor = null;
        Current = Array.Empty<DbValue>();
        return ValueTask.CompletedTask;
    }

    private void PopulateCurrent(long key, int entryCount)
    {
        double avg = key;
        double sum = (double)key * entryCount;

        for (int i = 0; i < _projectionKinds.Length; i++)
        {
            _rowBuffer[i] = (GroupedIndexAggregateProjectionKind)_projectionKinds[i] switch
            {
                GroupedIndexAggregateProjectionKind.GroupKey => DbValue.FromInteger(key),
                GroupedIndexAggregateProjectionKind.Count => DbValue.FromInteger(entryCount),
                GroupedIndexAggregateProjectionKind.Sum => DbValue.FromInteger((long)sum),
                GroupedIndexAggregateProjectionKind.Avg => DbValue.FromReal(avg),
                GroupedIndexAggregateProjectionKind.Min => DbValue.FromInteger(key),
                GroupedIndexAggregateProjectionKind.Max => DbValue.FromInteger(key),
                _ => DbValue.Null,
            };
        }

        Current = _rowBuffer;
    }

    private bool SatisfiesCountPredicate(int entryCount)
    {
        return _countPredicateKind switch
        {
            GroupedIndexAggregateCountPredicateKind.None => true,
            GroupedIndexAggregateCountPredicateKind.Equals => entryCount == _countPredicateValue,
            GroupedIndexAggregateCountPredicateKind.NotEquals => entryCount != _countPredicateValue,
            GroupedIndexAggregateCountPredicateKind.LessThan => entryCount < _countPredicateValue,
            GroupedIndexAggregateCountPredicateKind.GreaterThan => entryCount > _countPredicateValue,
            GroupedIndexAggregateCountPredicateKind.LessOrEqual => entryCount <= _countPredicateValue,
            GroupedIndexAggregateCountPredicateKind.GreaterOrEqual => entryCount >= _countPredicateValue,
            _ => true,
        };
    }
}

/// <summary>
/// Grouped aggregate fast path over hashed composite index payloads.
/// Avoids base-row scans when GROUP BY matches the leftmost index prefix and
/// the result only needs the grouped key columns plus COUNT(*).
/// </summary>
public sealed class CompositeIndexGroupedAggregateOperator : IOperator, IEstimatedRowCountProvider, IMaterializedRowsProvider
{
    private readonly IIndexStore _indexStore;
    private readonly BTree _tableTree;
    private readonly IRecordSerializer _recordSerializer;
    private readonly int[] _groupColumnIndices;
    private readonly int[] _projectionKinds;
    private readonly RecordColumnAccessor?[] _groupAccessors;
    private List<DbValue[]>? _results;
    private int _index;

    private static readonly CompositeGroupKeyComparer s_groupComparer = new();

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => false;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
    public int? EstimatedRowCount => _results?.Count;

    public CompositeIndexGroupedAggregateOperator(
        IIndexStore indexStore,
        BTree tableTree,
        IRecordSerializer recordSerializer,
        ReadOnlySpan<int> groupColumnIndices,
        ColumnDefinition[] outputSchema,
        ReadOnlySpan<int> projectionKinds)
    {
        _indexStore = indexStore;
        _tableTree = tableTree;
        _recordSerializer = recordSerializer;
        _groupColumnIndices = groupColumnIndices.ToArray();
        _projectionKinds = projectionKinds.ToArray();
        _groupAccessors = BoundColumnAccessHelper.CreateAccessors(recordSerializer, _groupColumnIndices);
        OutputSchema = outputSchema;
    }

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        var groups = new List<GroupState>();
        var groupIndex = new Dictionary<CompositeGroupKey, int>(s_groupComparer);
        var cursor = _indexStore.CreateCursor(IndexScanRange.All);

        while (await cursor.MoveNextAsync(ct))
        {
            ReadOnlyMemory<byte> payloadMemory = cursor.CurrentValue;
            if (HashedIndexPayloadCodec.TryDecodeGroups(payloadMemory.Span, out int componentCount, out var decodedGroups) &&
                componentCount >= _groupColumnIndices.Length)
            {
                for (int i = 0; i < decodedGroups.Count; i++)
                {
                    int entryCount = RowIdPayloadCodec.GetCount(decodedGroups[i].RowIdPayload);
                    if (entryCount <= 0)
                        continue;

                    AccumulateGroup(decodedGroups[i].KeyComponents, entryCount, groups, groupIndex);
                }

                continue;
            }

            int rowIdCount = RowIdPayloadCodec.GetCount(payloadMemory.Span);
            for (int i = 0; i < rowIdCount; i++)
            {
                long rowId = RowIdPayloadCodec.ReadAt(payloadMemory.Span, i);
                var rowPayload = await _tableTree.FindMemoryAsync(rowId, ct);
                if (rowPayload == null)
                    continue;

                var groupComponents = DecodeGroupComponents(rowPayload.Value.Span);
                AccumulateGroup(groupComponents, 1, groups, groupIndex);
            }
        }

        _results = new List<DbValue[]>(groups.Count);
        for (int i = 0; i < groups.Count; i++)
            _results.Add(BuildOutputRow(groups[i]));

        _index = -1;
        Current = Array.Empty<DbValue>();
    }

    public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        _index++;
        if (_results == null || _index >= _results.Count)
            return ValueTask.FromResult(false);

        Current = _results[_index];
        return ValueTask.FromResult(true);
    }

    public bool TryTakeMaterializedRows(out List<DbValue[]> rows)
    {
        if (_results == null)
        {
            rows = new List<DbValue[]>(0);
            return false;
        }

        rows = _results;
        _results = null;
        _index = -1;
        Current = Array.Empty<DbValue>();
        return true;
    }

    public ValueTask DisposeAsync()
    {
        _results = null;
        _index = -1;
        Current = Array.Empty<DbValue>();
        return ValueTask.CompletedTask;
    }

    private void AccumulateGroup(
        ReadOnlySpan<DbValue> sourceComponents,
        int entryCount,
        List<GroupState> groups,
        Dictionary<CompositeGroupKey, int> groupIndex)
    {
        var prefixComponents = new DbValue[_groupColumnIndices.Length];
        sourceComponents[.._groupColumnIndices.Length].CopyTo(prefixComponents);

        var key = new CompositeGroupKey(prefixComponents, ComputeGroupHash(prefixComponents));
        if (groupIndex.TryGetValue(key, out int existingIndex))
        {
            groups[existingIndex].Count += entryCount;
            return;
        }

        groupIndex[key] = groups.Count;
        groups.Add(new GroupState(prefixComponents, entryCount));
    }

    private DbValue[] DecodeGroupComponents(ReadOnlySpan<byte> payload)
    {
        var components = new DbValue[_groupColumnIndices.Length];
        for (int i = 0; i < _groupColumnIndices.Length; i++)
        {
            components[i] = _groupAccessors[i] is { } accessor
                ? accessor.Decode(payload)
                : _recordSerializer.DecodeColumn(payload, _groupColumnIndices[i]);
        }

        return components;
    }

    private DbValue[] BuildOutputRow(GroupState group)
    {
        var row = new DbValue[_projectionKinds.Length];
        for (int i = 0; i < _projectionKinds.Length; i++)
        {
            int projectionKind = _projectionKinds[i];
            row[i] = projectionKind >= 0
                ? group.Components[projectionKind]
                : DbValue.FromInteger(group.Count);
        }

        return row;
    }

    private static int ComputeGroupHash(ReadOnlySpan<DbValue> components)
    {
        var hash = new HashCode();
        for (int i = 0; i < components.Length; i++)
            hash.Add(components[i]);
        return hash.ToHashCode();
    }

    private sealed class GroupState
    {
        public GroupState(DbValue[] components, long count)
        {
            Components = components;
            Count = count;
        }

        public DbValue[] Components { get; }
        public long Count { get; set; }
    }

    private readonly struct CompositeGroupKey
    {
        public CompositeGroupKey(DbValue[] components, int hashCode)
        {
            Components = components;
            HashCode = hashCode;
        }

        public DbValue[] Components { get; }
        public int HashCode { get; }
    }

    private sealed class CompositeGroupKeyComparer : IEqualityComparer<CompositeGroupKey>
    {
        public bool Equals(CompositeGroupKey x, CompositeGroupKey y)
        {
            if (x.HashCode != y.HashCode || x.Components.Length != y.Components.Length)
                return false;

            for (int i = 0; i < x.Components.Length; i++)
            {
                if (DbValue.Compare(x.Components[i], y.Components[i]) != 0)
                    return false;
            }

            return true;
        }

        public int GetHashCode(CompositeGroupKey obj) => obj.HashCode;
    }
}

/// <summary>
/// Scalar aggregate fast path over the table row key for INTEGER PRIMARY KEY tables.
/// Uses only the B-tree key stream and avoids row payload materialization.
/// </summary>
public sealed class TableKeyAggregateOperator : IOperator, IEstimatedRowCountProvider
{
    private enum AggregateKind
    {
        Count,
        Sum,
        Avg,
        Min,
        Max,
    }

    private readonly BTree _tableTree;
    private readonly IndexScanRange _scanRange;
    private readonly AggregateKind _kind;
    private bool _emitted;

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => false;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
    public int? EstimatedRowCount => 1;

    public TableKeyAggregateOperator(
        BTree tableTree,
        IndexScanRange scanRange,
        string functionName,
        ColumnDefinition[] outputSchema)
    {
        _tableTree = tableTree;
        _scanRange = scanRange;
        _kind = functionName switch
        {
            "COUNT" => AggregateKind.Count,
            "SUM" => AggregateKind.Sum,
            "AVG" => AggregateKind.Avg,
            "MIN" => AggregateKind.Min,
            "MAX" => AggregateKind.Max,
            _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unsupported table-key aggregate fast path: {functionName}"),
        };
        OutputSchema = outputSchema;
    }

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        _emitted = false;
        Current = Array.Empty<DbValue>();

        if (_kind == AggregateKind.Max)
        {
            long? maxKey = await _tableTree.FindMaxKeyAsync(_scanRange, ct);
            Current = new[] { maxKey.HasValue ? DbValue.FromInteger(maxKey.Value) : DbValue.Null };
            return;
        }

        var cursor = _tableTree.CreateCursor(_scanRange);
        if (!await MoveToFirstInRangeAsync(cursor, ct))
        {
            Current = new[] { CreateEmptyAggregate() };
            return;
        }

        if (_kind == AggregateKind.Min)
        {
            Current = new[] { DbValue.FromInteger(cursor.CurrentKey) };
            return;
        }

        long count = 0;
        double sum = 0;
        bool hasAny = false;

        do
        {
            long key = cursor.CurrentKey;
            if (IsPastUpperBound(key))
                break;

            switch (_kind)
            {
                case AggregateKind.Count:
                    count++;
                    hasAny = true;
                    break;
                case AggregateKind.Sum:
                    sum += key;
                    count++;
                    hasAny = true;
                    break;
                case AggregateKind.Avg:
                    sum += key;
                    count++;
                    hasAny = true;
                    break;
            }
        }
        while (await cursor.MoveNextAsync(ct));

        DbValue aggregate = _kind switch
        {
            AggregateKind.Count => DbValue.FromInteger(count),
            AggregateKind.Sum => !hasAny ? DbValue.FromInteger(0) : DbValue.FromInteger((long)sum),
            AggregateKind.Avg => !hasAny ? DbValue.Null : DbValue.FromReal(sum / count),
            AggregateKind.Min => DbValue.Null,
            AggregateKind.Max => DbValue.Null,
            _ => DbValue.Null,
        };

        Current = new[] { aggregate };
    }

    public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (_emitted)
            return ValueTask.FromResult(false);

        _emitted = true;
        return ValueTask.FromResult(true);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private DbValue CreateEmptyAggregate()
    {
        return _kind switch
        {
            AggregateKind.Count => DbValue.FromInteger(0),
            AggregateKind.Sum => DbValue.FromInteger(0),
            AggregateKind.Avg => DbValue.Null,
            AggregateKind.Min => DbValue.Null,
            AggregateKind.Max => DbValue.Null,
            _ => DbValue.Null,
        };
    }

    private async ValueTask<bool> MoveToFirstInRangeAsync(BTreeCursor cursor, CancellationToken ct)
    {
        if (_scanRange.LowerBound.HasValue)
        {
            if (!await cursor.SeekAsync(_scanRange.LowerBound.Value, ct))
                return false;

            if (!_scanRange.LowerInclusive && cursor.CurrentKey == _scanRange.LowerBound.Value)
            {
                if (!await cursor.MoveNextAsync(ct))
                    return false;
            }
        }
        else if (!await cursor.MoveNextAsync(ct))
        {
            return false;
        }

        return !IsPastUpperBound(cursor.CurrentKey);
    }

    private bool IsPastUpperBound(long key)
    {
        if (!_scanRange.UpperBound.HasValue)
            return false;

        return _scanRange.UpperInclusive
            ? key > _scanRange.UpperBound.Value
            : key >= _scanRange.UpperBound.Value;
    }
}

/// <summary>
/// Scalar SUM/AVG/COUNT/MIN/MAX fast path for point/range lookups from PK or a single-column index equality lookup.
/// Avoids generic operator-pipeline overhead by aggregating directly on payloads.
/// </summary>
public sealed class ScalarAggregateLookupOperator : IOperator, IEstimatedRowCountProvider
{
    private enum AggregateKind
    {
        Count,
        Sum,
        Avg,
        Min,
        Max,
    }

    private enum LookupKind
    {
        PrimaryKey,
        IndexEquality,
    }

    private readonly LookupKind _lookupKind;
    private readonly BTree _tableTree;
    private readonly IIndexStore? _indexStore;
    private readonly long _lookupValue;
    private readonly int _columnIndex;
    private readonly AggregateKind _kind;
    private readonly bool _isDistinct;
    private readonly IRecordSerializer _recordSerializer;
    private readonly RecordColumnAccessor? _columnAccessor;
    private bool _emitted;

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => false;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
    public int? EstimatedRowCount => 1;

    public ScalarAggregateLookupOperator(
        BTree tableTree,
        long seekKey,
        int columnIndex,
        string functionName,
        ColumnDefinition[] outputSchema,
        bool isDistinct = false,
        IRecordSerializer? recordSerializer = null)
    {
        _lookupKind = LookupKind.PrimaryKey;
        _tableTree = tableTree;
        _indexStore = null;
        _lookupValue = seekKey;
        _columnIndex = columnIndex;
        _isDistinct = isDistinct;
        _recordSerializer = recordSerializer ?? new DefaultRecordSerializer();
        _columnAccessor = BoundColumnAccessHelper.TryCreate(_recordSerializer, columnIndex);
        _kind = ParseKind(functionName);
        OutputSchema = outputSchema;
    }

    public ScalarAggregateLookupOperator(
        IIndexStore indexStore,
        BTree tableTree,
        long seekValue,
        int columnIndex,
        string functionName,
        ColumnDefinition[] outputSchema,
        bool isDistinct = false,
        IRecordSerializer? recordSerializer = null)
    {
        _lookupKind = LookupKind.IndexEquality;
        _indexStore = indexStore;
        _tableTree = tableTree;
        _lookupValue = seekValue;
        _columnIndex = columnIndex;
        _isDistinct = isDistinct;
        _recordSerializer = recordSerializer ?? new DefaultRecordSerializer();
        _columnAccessor = BoundColumnAccessHelper.TryCreate(_recordSerializer, columnIndex);
        _kind = ParseKind(functionName);
        OutputSchema = outputSchema;
    }

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        _emitted = false;
        Current = Array.Empty<DbValue>();

        long count = 0;
        double sum = 0;
        bool hasReal = false;
        bool hasAny = false;
        DbValue? best = null;
        AggregateDistinctValueSet? distinctValues = _isDistinct ? new AggregateDistinctValueSet() : null;
        bool cachedResultFinalized = false;

        void Accumulate(ReadOnlySpan<byte> payload)
        {
            if (_kind == AggregateKind.Count && distinctValues == null)
            {
                if (!BoundColumnAccessHelper.IsNull(payload, _recordSerializer, _columnAccessor, _columnIndex))
                    count++;
                return;
            }

            if ((_kind == AggregateKind.Sum || _kind == AggregateKind.Avg) && distinctValues == null)
            {
                if (!BoundColumnAccessHelper.TryDecodeNumeric(
                        payload,
                        _recordSerializer,
                        _columnAccessor,
                        _columnIndex,
                        out long intVal,
                        out double realVal,
                        out bool isReal))
                {
                    return;
                }

                hasAny = true;
                if (isReal)
                {
                    hasReal = true;
                    sum += realVal;
                }
                else
                {
                    sum += intVal;
                }
                count++;
                return;
            }

            if (distinctValues != null && _kind is AggregateKind.Count or AggregateKind.Sum or AggregateKind.Avg)
            {
                if (BoundColumnAccessHelper.TryDecodeNumeric(
                        payload,
                        _recordSerializer,
                        _columnAccessor,
                        _columnIndex,
                        out long intVal,
                        out double realVal,
                        out bool isReal))
                {
                    if (!distinctValues.AddNumeric(intVal, realVal, isReal))
                        return;

                    switch (_kind)
                    {
                        case AggregateKind.Count:
                            count++;
                            return;
                        case AggregateKind.Sum:
                        case AggregateKind.Avg:
                            hasAny = true;
                            if (isReal)
                            {
                                hasReal = true;
                                sum += realVal;
                            }
                            else
                            {
                                sum += intVal;
                            }

                            count++;
                            return;
                    }
                }
            }

            var val = BoundColumnAccessHelper.Decode(payload, _recordSerializer, _columnAccessor, _columnIndex);
            if (val.IsNull) return;
            if (distinctValues != null && !distinctValues.Add(val)) return;

            switch (_kind)
            {
                case AggregateKind.Count:
                    count++;
                    break;
                case AggregateKind.Sum:
                case AggregateKind.Avg:
                    hasAny = true;
                    if (val.Type == DbType.Real)
                    {
                        hasReal = true;
                        sum += val.AsReal;
                    }
                    else
                    {
                        sum += val.AsInteger;
                    }
                    count++;
                    break;
                case AggregateKind.Min:
                    if (best == null || DbValue.Compare(val, best.Value) < 0)
                        best = val;
                    break;
                case AggregateKind.Max:
                    if (best == null || DbValue.Compare(val, best.Value) > 0)
                        best = val;
                    break;
            }
        }

        bool TryAccumulateCachedPrimaryKey()
        {
            if (!_tableTree.TryFindCachedMemory(_lookupValue, out var cachedPayload))
                return false;

            if (cachedPayload is { } payloadMemory)
                Accumulate(payloadMemory.Span);

            return true;
        }

        bool TryAccumulateCachedIndexEquality()
        {
            if (_indexStore is not ICacheAwareIndexStore cacheAware ||
                !cacheAware.TryFindCached(_lookupValue, out var cachedIndexPayload))
            {
                return false;
            }

            if (cachedIndexPayload is not { Length: > 0 })
                return true;

            if (cachedIndexPayload.Length == RowIdPayloadCodec.RowIdSize)
            {
                long rowId = BinaryPrimitives.ReadInt64LittleEndian(cachedIndexPayload.AsSpan(0, RowIdPayloadCodec.RowIdSize));
                if (!_tableTree.TryFindCachedMemory(rowId, out var cachedRowPayload))
                    return false;

                FinalizeSingleRowAggregate(cachedRowPayload);
                return true;
            }

            int rowIdCount = RowIdPayloadCodec.GetCount(cachedIndexPayload);
            for (int i = 0; i < rowIdCount; i++)
            {
                long rowId = RowIdPayloadCodec.ReadAt(cachedIndexPayload, i);
                if (!_tableTree.TryFindCachedMemory(rowId, out var cachedRowPayload))
                    return false;

                if (cachedRowPayload is { } rowPayloadMemory)
                    Accumulate(rowPayloadMemory.Span);
            }

            return true;
        }

        void FinalizeSingleRowAggregate(ReadOnlyMemory<byte>? rowPayload)
        {
            DbValue aggregate;
            if (rowPayload is not { } rowPayloadMemory)
            {
                aggregate = _kind switch
                {
                    AggregateKind.Count => DbValue.FromInteger(0),
                    AggregateKind.Sum => DbValue.FromInteger(0),
                    AggregateKind.Avg => DbValue.Null,
                    AggregateKind.Min => DbValue.Null,
                    AggregateKind.Max => DbValue.Null,
                    _ => DbValue.Null,
                };
            }
            else
            {
                var payload = rowPayloadMemory.Span;
                aggregate = _kind switch
                {
                    AggregateKind.Count => BoundColumnAccessHelper.IsNull(payload, _recordSerializer, _columnAccessor, _columnIndex)
                        ? DbValue.FromInteger(0)
                        : DbValue.FromInteger(1),
                    AggregateKind.Sum => BoundColumnAccessHelper.TryDecodeNumeric(
                            payload,
                            _recordSerializer,
                            _columnAccessor,
                            _columnIndex,
                            out long intVal,
                            out double realVal,
                            out bool isReal)
                        ? isReal ? DbValue.FromReal(realVal) : DbValue.FromInteger(intVal)
                        : DbValue.FromInteger(0),
                    AggregateKind.Avg => BoundColumnAccessHelper.TryDecodeNumeric(
                            payload,
                            _recordSerializer,
                            _columnAccessor,
                            _columnIndex,
                            out long avgIntVal,
                            out double avgRealVal,
                            out bool avgIsReal)
                        ? avgIsReal ? DbValue.FromReal(avgRealVal) : DbValue.FromInteger(avgIntVal)
                        : DbValue.Null,
                    AggregateKind.Min => BoundColumnAccessHelper.Decode(payload, _recordSerializer, _columnAccessor, _columnIndex),
                    AggregateKind.Max => BoundColumnAccessHelper.Decode(payload, _recordSerializer, _columnAccessor, _columnIndex),
                    _ => DbValue.Null,
                };
            }

            Current = new[] { aggregate };
            cachedResultFinalized = true;
        }

        void FinalizeAggregate()
        {
            DbValue aggregate = _kind switch
            {
                AggregateKind.Count => DbValue.FromInteger(count),
                AggregateKind.Sum => !hasAny ? DbValue.FromInteger(0)
                    : hasReal ? DbValue.FromReal(sum) : DbValue.FromInteger((long)sum),
                AggregateKind.Avg => !hasAny ? DbValue.Null : DbValue.FromReal(sum / count),
                AggregateKind.Min => best ?? DbValue.Null,
                AggregateKind.Max => best ?? DbValue.Null,
                _ => DbValue.Null,
            };

            Current = new[] { aggregate };
        }

        bool usedCachedFastPath = _lookupKind switch
        {
            LookupKind.PrimaryKey => TryAccumulateCachedPrimaryKey(),
            LookupKind.IndexEquality => TryAccumulateCachedIndexEquality(),
            _ => false,
        };

        if (usedCachedFastPath)
        {
            if (!cachedResultFinalized)
                FinalizeAggregate();
            return;
        }

        if (_lookupKind == LookupKind.PrimaryKey)
        {
            var payload = await _tableTree.FindMemoryAsync(_lookupValue, ct);
            if (payload is { } payloadMemory)
                Accumulate(payloadMemory.Span);
        }
        else
        {
            var indexPayload = await _indexStore!.FindAsync(_lookupValue, ct);
            if (indexPayload != null)
            {
                int rowIdCount = indexPayload.Length / 8;
                for (int i = 0; i < rowIdCount; i++)
                {
                    long rowId = BinaryPrimitives.ReadInt64LittleEndian(indexPayload.AsSpan(i * 8, 8));
                    var payload = await _tableTree.FindMemoryAsync(rowId, ct);
                    if (payload is not { } payloadMemory) continue;
                    Accumulate(payloadMemory.Span);
                }
            }
        }

        FinalizeAggregate();
    }

    public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (_emitted) return ValueTask.FromResult(false);
        _emitted = true;
        return ValueTask.FromResult(true);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static AggregateKind ParseKind(string functionName)
    {
        return functionName switch
        {
            "COUNT" => AggregateKind.Count,
            "SUM" => AggregateKind.Sum,
            "AVG" => AggregateKind.Avg,
            "MIN" => AggregateKind.Min,
            "MAX" => AggregateKind.Max,
            _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unsupported aggregate fast path: {functionName}"),
        };
    }
}

/// <summary>
/// Scalar SUM/AVG/COUNT(column) fast path for a single table with no filters/grouping.
/// Scans the table B+tree directly and decodes only the target column.
/// Produces exactly one row.
/// </summary>
public sealed class ScalarAggregateTableOperator : IOperator, IEstimatedRowCountProvider
{
    private enum AggregateKind
    {
        Count,
        Sum,
        Avg,
        Min,
        Max,
    }

    private readonly BTree _tableTree;
    private readonly int _columnIndex;
    private readonly AggregateKind _kind;
    private readonly bool _isDistinct;
    private readonly bool _emitOnEmptyInput;
    private readonly IRecordSerializer _recordSerializer;
    private readonly RecordColumnAccessor? _columnAccessor;
    private bool _emitted;
    private bool _hasResult;

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => false;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
    public int? EstimatedRowCount => 1;

    public ScalarAggregateTableOperator(
        BTree tableTree,
        int columnIndex,
        string functionName,
        ColumnDefinition[] outputSchema,
        bool isDistinct = false,
        bool emitOnEmptyInput = true,
        IRecordSerializer? recordSerializer = null)
    {
        _tableTree = tableTree;
        _columnIndex = columnIndex;
        _isDistinct = isDistinct;
        _emitOnEmptyInput = emitOnEmptyInput;
        _recordSerializer = recordSerializer ?? new DefaultRecordSerializer();
        _columnAccessor = BoundColumnAccessHelper.TryCreate(_recordSerializer, columnIndex);
        _kind = functionName switch
        {
            "COUNT" => AggregateKind.Count,
            "SUM" => AggregateKind.Sum,
            "AVG" => AggregateKind.Avg,
            "MIN" => AggregateKind.Min,
            "MAX" => AggregateKind.Max,
            _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unsupported aggregate fast path: {functionName}"),
        };
        OutputSchema = outputSchema;
    }

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        _emitted = false;
        Current = Array.Empty<DbValue>();

        var cursor = _tableTree.CreateCursor();

        long count = 0;
        double sum = 0;
        bool hasReal = false;
        bool hasAny = false;
        DbValue? best = null;
        bool sawRow = false;
        AggregateDistinctValueSet? distinctValues = _isDistinct ? new AggregateDistinctValueSet() : null;

        while (await cursor.MoveNextAsync(ct))
        {
            sawRow = true;

            if (_kind == AggregateKind.Count && distinctValues == null)
            {
                if (!BoundColumnAccessHelper.IsNull(
                        cursor.CurrentValue.Span,
                        _recordSerializer,
                        _columnAccessor,
                        _columnIndex))
                {
                    count++;
                }
                continue;
            }

            if ((_kind == AggregateKind.Sum || _kind == AggregateKind.Avg) && distinctValues == null)
            {
                if (!BoundColumnAccessHelper.TryDecodeNumeric(
                        cursor.CurrentValue.Span,
                        _recordSerializer,
                        _columnAccessor,
                        _columnIndex,
                        out long intVal,
                        out double realVal,
                        out bool isReal))
                {
                    continue;
                }

                hasAny = true;
                if (isReal)
                {
                    hasReal = true;
                    sum += realVal;
                }
                else
                {
                    sum += intVal;
                }
                count++;
                continue;
            }

            if (distinctValues != null && _kind is AggregateKind.Count or AggregateKind.Sum or AggregateKind.Avg)
            {
                if (BoundColumnAccessHelper.TryDecodeNumeric(
                        cursor.CurrentValue.Span,
                        _recordSerializer,
                        _columnAccessor,
                        _columnIndex,
                        out long intVal,
                        out double realVal,
                        out bool isReal))
                {
                    if (!distinctValues.AddNumeric(intVal, realVal, isReal))
                        continue;

                    switch (_kind)
                    {
                        case AggregateKind.Count:
                            count++;
                            continue;
                        case AggregateKind.Sum:
                        case AggregateKind.Avg:
                            hasAny = true;
                            if (isReal)
                            {
                                hasReal = true;
                                sum += realVal;
                            }
                            else
                            {
                                sum += intVal;
                            }

                            count++;
                            continue;
                    }
                }
            }

            var val = BoundColumnAccessHelper.Decode(
                cursor.CurrentValue.Span,
                _recordSerializer,
                _columnAccessor,
                _columnIndex);
            if (val.IsNull) continue;
            if (distinctValues != null && !distinctValues.Add(val)) continue;

            switch (_kind)
            {
                case AggregateKind.Count:
                    count++;
                    break;
                case AggregateKind.Sum:
                case AggregateKind.Avg:
                    hasAny = true;
                    if (val.Type == DbType.Real)
                    {
                        hasReal = true;
                        sum += val.AsReal;
                    }
                    else
                    {
                        sum += val.AsInteger;
                    }
                    count++;
                    break;
                case AggregateKind.Min:
                    if (best == null || DbValue.Compare(val, best.Value) < 0)
                        best = val;
                    break;
                case AggregateKind.Max:
                    if (best == null || DbValue.Compare(val, best.Value) > 0)
                        best = val;
                    break;
            }
        }

        DbValue aggregate = _kind switch
        {
            AggregateKind.Count => DbValue.FromInteger(count),
            AggregateKind.Sum => !hasAny ? DbValue.FromInteger(0)
                : hasReal ? DbValue.FromReal(sum) : DbValue.FromInteger((long)sum),
            AggregateKind.Avg => !hasAny ? DbValue.Null : DbValue.FromReal(sum / count),
            AggregateKind.Min => best ?? DbValue.Null,
            AggregateKind.Max => best ?? DbValue.Null,
            _ => DbValue.Null,
        };

        _hasResult = _emitOnEmptyInput || sawRow;
        Current = _hasResult ? new[] { aggregate } : Array.Empty<DbValue>();
    }

    public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (_emitted || !_hasResult) return ValueTask.FromResult(false);
        _emitted = true;
        return ValueTask.FromResult(true);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

/// <summary>
/// Scalar aggregate fast path for a single table with a residual filter and no grouping.
/// Decodes only the referenced columns into a compact layout so predicates and aggregate
/// accumulation avoid sparse row buffers.
/// </summary>
public sealed class FilteredScalarAggregateTableOperator : IOperator, IEstimatedRowCountProvider
{
    private enum AggregateKind
    {
        Count,
        Sum,
        Avg,
        Min,
        Max,
    }

    private const int DefaultBatchSize = 64;

    private readonly BTree _tableTree;
    private readonly int _columnIndex;
    private readonly AggregateKind _kind;
    private readonly bool _isDistinct;
    private readonly bool _isCountStar;
    private readonly Func<DbValue[], DbValue> _predicateEvaluator;
    private readonly Func<DbValue[], DbValue>? _aggregateArgumentEvaluator;
    private readonly IRecordSerializer _recordSerializer;
    private readonly int[] _decodedColumnIndices;
    private readonly IScalarAggregateBatchPlan? _batchPlan;
    private readonly PreDecodeFilterSpec[] _preDecodeFilters;
    private bool _emitted;

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => false;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
    public int? EstimatedRowCount => 1;

    internal FilteredScalarAggregateTableOperator(
        BTree tableTree,
        int columnIndex,
        string functionName,
        ColumnDefinition[] outputSchema,
        Func<DbValue[], DbValue> predicateEvaluator,
        int[] decodedColumnIndices,
        Func<DbValue[], DbValue>? aggregateArgumentEvaluator = null,
        bool isDistinct = false,
        bool isCountStar = false,
        IRecordSerializer? recordSerializer = null,
        IScalarAggregateBatchPlan? batchPlan = null)
    {
        _tableTree = tableTree;
        _columnIndex = columnIndex;
        _isDistinct = isDistinct;
        _isCountStar = isCountStar;
        _predicateEvaluator = predicateEvaluator;
        _recordSerializer = recordSerializer ?? new DefaultRecordSerializer();
        _decodedColumnIndices = decodedColumnIndices;
        _batchPlan = batchPlan;
        _preDecodeFilters = CreatePreDecodeFilters(_batchPlan?.PushdownFilters);
        _kind = functionName switch
        {
            "COUNT" => AggregateKind.Count,
            "SUM" => AggregateKind.Sum,
            "AVG" => AggregateKind.Avg,
            "MIN" => AggregateKind.Min,
            "MAX" => AggregateKind.Max,
            _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unsupported aggregate fast path: {functionName}"),
        };
        OutputSchema = outputSchema;
        _aggregateArgumentEvaluator = aggregateArgumentEvaluator;
    }

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        _emitted = false;
        Current = Array.Empty<DbValue>();

        DbValue aggregate = _batchPlan != null
            ? await ExecuteBatchedAsync(ct)
            : await ExecuteRowByRowAsync(ct);

        Current = new[] { aggregate };
    }

    public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (_emitted)
            return ValueTask.FromResult(false);

        _emitted = true;
        return ValueTask.FromResult(true);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private async ValueTask<DbValue> ExecuteBatchedAsync(CancellationToken ct)
    {
        var cursor = _tableTree.CreateCursor();
        int columnCount = _decodedColumnIndices.Length;
        var batch = new RowBatch(columnCount, DefaultBatchSize);

        _batchPlan!.Reset();

        while (true)
        {
            batch.Reset();

            while (batch.Count < batch.Capacity && await cursor.MoveNextAsync(ct))
            {
                ReadOnlySpan<byte> payload = cursor.CurrentValue.Span;
                if (!EvaluatePreDecodeFilters(payload))
                    continue;

                int rowIndex = batch.Count;
                if (columnCount > 0)
                    _recordSerializer.DecodeSelectedCompactInto(payload, batch.GetWritableRowSpan(rowIndex), _decodedColumnIndices);
                batch.CommitWrittenRow(rowIndex);
            }

            if (batch.Count == 0)
                break;

            _batchPlan.Accumulate(batch);
        }

        return _batchPlan.GetResult();
    }

    private async ValueTask<DbValue> ExecuteRowByRowAsync(CancellationToken ct)
    {
        var cursor = _tableTree.CreateCursor();
        int columnCount = _decodedColumnIndices.Length;
        var decodeBuffer = columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];

        long count = 0;
        double sum = 0;
        bool hasReal = false;
        bool hasAny = false;
        DbValue? best = null;
        AggregateDistinctValueSet? distinctValues = _isDistinct ? new AggregateDistinctValueSet() : null;

        while (await cursor.MoveNextAsync(ct))
        {
            ReadOnlySpan<byte> payload = cursor.CurrentValue.Span;
            if (!EvaluatePreDecodeFilters(payload))
                continue;

            if (columnCount > 0)
                _recordSerializer.DecodeSelectedCompactInto(payload, decodeBuffer, _decodedColumnIndices);

            if (!_predicateEvaluator(decodeBuffer).IsTruthy)
                continue;

            if (_isCountStar)
            {
                count++;
                continue;
            }

            if (distinctValues != null &&
                _aggregateArgumentEvaluator == null &&
                (uint)_columnIndex < (uint)_decodedColumnIndices.Length &&
                _kind is AggregateKind.Count or AggregateKind.Sum or AggregateKind.Avg)
            {
                if (BoundColumnAccessHelper.TryDecodeNumeric(
                        payload,
                        _recordSerializer,
                        null,
                        _decodedColumnIndices[_columnIndex],
                        out long intVal,
                        out double realVal,
                        out bool isReal))
                {
                    if (!distinctValues.AddNumeric(intVal, realVal, isReal))
                        continue;

                    switch (_kind)
                    {
                        case AggregateKind.Count:
                            count++;
                            continue;
                        case AggregateKind.Sum:
                        case AggregateKind.Avg:
                            hasAny = true;
                            if (isReal)
                            {
                                hasReal = true;
                                sum += realVal;
                            }
                            else
                            {
                                sum += intVal;
                            }

                            count++;
                            continue;
                    }
                }
            }

            DbValue value = _aggregateArgumentEvaluator != null
                ? _aggregateArgumentEvaluator(decodeBuffer)
                : (uint)_columnIndex < (uint)decodeBuffer.Length
                    ? decodeBuffer[_columnIndex]
                    : DbValue.Null;
            if (value.IsNull)
                continue;
            if (distinctValues != null && !distinctValues.Add(value))
                continue;

            switch (_kind)
            {
                case AggregateKind.Count:
                    count++;
                    break;
                case AggregateKind.Sum:
                case AggregateKind.Avg:
                    hasAny = true;
                    if (value.Type == DbType.Real)
                    {
                        hasReal = true;
                        sum += value.AsReal;
                    }
                    else
                    {
                        sum += value.AsInteger;
                    }
                    count++;
                    break;
                case AggregateKind.Min:
                    if (best == null || DbValue.Compare(value, best.Value) < 0)
                        best = value;
                    break;
                case AggregateKind.Max:
                    if (best == null || DbValue.Compare(value, best.Value) > 0)
                        best = value;
                    break;
            }
        }

        return _kind switch
        {
            AggregateKind.Count => DbValue.FromInteger(count),
            AggregateKind.Sum => !hasAny ? DbValue.FromInteger(0)
                : hasReal ? DbValue.FromReal(sum) : DbValue.FromInteger((long)sum),
            AggregateKind.Avg => !hasAny ? DbValue.Null : DbValue.FromReal(sum / count),
            AggregateKind.Min => best ?? DbValue.Null,
            AggregateKind.Max => best ?? DbValue.Null,
            _ => DbValue.Null,
        };
    }

    private PreDecodeFilterSpec[] CreatePreDecodeFilters(BatchPushdownFilter[]? pushdownFilters)
    {
        if (pushdownFilters == null || pushdownFilters.Length == 0)
            return Array.Empty<PreDecodeFilterSpec>();

        var filters = new PreDecodeFilterSpec[pushdownFilters.Length];
        for (int i = 0; i < pushdownFilters.Length; i++)
        {
            var filter = pushdownFilters[i];
            if ((uint)filter.ColumnIndex >= (uint)_decodedColumnIndices.Length)
                return Array.Empty<PreDecodeFilterSpec>();

            filters[i] = new PreDecodeFilterSpec(
                _recordSerializer,
                _decodedColumnIndices[filter.ColumnIndex],
                filter);
        }

        return filters;
    }

    private bool EvaluatePreDecodeFilters(ReadOnlySpan<byte> payload)
        => _preDecodeFilters.Length == 0
            || BoundColumnAccessHelper.EvaluatePreDecodeFilters(payload, _recordSerializer, _preDecodeFilters);
}

/// <summary>
/// Scalar aggregate fast path over a payload-producing source such as an index lookup or ordered index scan.
/// Decodes only the referenced payload columns into a compact layout so indexed predicates can avoid falling
/// back to a full table scan when the aggregate argument is not the index key itself.
/// </summary>
public sealed class FilteredScalarAggregatePayloadOperator : IOperator, IEstimatedRowCountProvider
{
    private enum AggregateKind
    {
        Count,
        Sum,
        Avg,
        Min,
        Max,
    }

    private const int DefaultBatchSize = 64;

    private readonly IOperator _source;
    private readonly IEncodedPayloadSource _payloadSource;
    private readonly int _columnIndex;
    private readonly AggregateKind _kind;
    private readonly bool _isDistinct;
    private readonly bool _isCountStar;
    private readonly Func<DbValue[], DbValue>? _predicateEvaluator;
    private readonly Func<DbValue[], DbValue>? _aggregateArgumentEvaluator;
    private readonly IRecordSerializer _recordSerializer;
    private readonly int[] _decodedColumnIndices;
    private readonly IScalarAggregateBatchPlan? _batchPlan;
    private readonly PreDecodeFilterSpec[] _preDecodeFilters;
    private bool _emitted;

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => false;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
    public int? EstimatedRowCount => 1;

    internal FilteredScalarAggregatePayloadOperator(
        IOperator source,
        IRecordSerializer recordSerializer,
        int[] decodedColumnIndices,
        int columnIndex,
        string functionName,
        ColumnDefinition[] outputSchema,
        Func<DbValue[], DbValue>? predicateEvaluator = null,
        Func<DbValue[], DbValue>? aggregateArgumentEvaluator = null,
        bool isDistinct = false,
        bool isCountStar = false,
        IScalarAggregateBatchPlan? batchPlan = null)
    {
        _source = source ?? throw new ArgumentNullException(nameof(source));
        _payloadSource = source as IEncodedPayloadSource
            ?? throw new ArgumentException("Source must expose encoded payload.", nameof(source));
        _recordSerializer = recordSerializer ?? throw new ArgumentNullException(nameof(recordSerializer));
        _decodedColumnIndices = decodedColumnIndices ?? throw new ArgumentNullException(nameof(decodedColumnIndices));
        _columnIndex = columnIndex;
        _isDistinct = isDistinct;
        _isCountStar = isCountStar;
        _predicateEvaluator = predicateEvaluator;
        _aggregateArgumentEvaluator = aggregateArgumentEvaluator;
        _batchPlan = batchPlan;
        _preDecodeFilters = CreatePreDecodeFilters(_batchPlan?.PushdownFilters);
        _kind = functionName switch
        {
            "COUNT" => AggregateKind.Count,
            "SUM" => AggregateKind.Sum,
            "AVG" => AggregateKind.Avg,
            "MIN" => AggregateKind.Min,
            "MAX" => AggregateKind.Max,
            _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unsupported aggregate fast path: {functionName}"),
        };
        OutputSchema = outputSchema;
    }

    public async ValueTask OpenAsync(CancellationToken ct = default)
    {
        _emitted = false;
        Current = Array.Empty<DbValue>();
        await _source.OpenAsync(ct);

        DbValue aggregate = _batchPlan != null
            ? await ExecuteBatchedAsync(ct)
            : await ExecuteRowByRowAsync(ct);

        Current = new[] { aggregate };
    }

    public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (_emitted)
            return ValueTask.FromResult(false);

        _emitted = true;
        return ValueTask.FromResult(true);
    }

    public ValueTask DisposeAsync() => _source.DisposeAsync();

    private async ValueTask<DbValue> ExecuteBatchedAsync(CancellationToken ct)
    {
        int columnCount = _decodedColumnIndices.Length;
        var batch = new RowBatch(columnCount, DefaultBatchSize);

        _batchPlan!.Reset();

        while (true)
        {
            batch.Reset();

            while (batch.Count < batch.Capacity && await _source.MoveNextAsync(ct))
            {
                ReadOnlySpan<byte> payload = _payloadSource.CurrentPayload.Span;
                if (!EvaluatePreDecodeFilters(payload))
                    continue;

                int rowIndex = batch.Count;
                if (columnCount > 0)
                    _recordSerializer.DecodeSelectedCompactInto(payload, batch.GetWritableRowSpan(rowIndex), _decodedColumnIndices);
                batch.CommitWrittenRow(rowIndex);
            }

            if (batch.Count == 0)
                break;

            _batchPlan.Accumulate(batch);
        }

        return _batchPlan.GetResult();
    }

    private async ValueTask<DbValue> ExecuteRowByRowAsync(CancellationToken ct)
    {
        int columnCount = _decodedColumnIndices.Length;
        var decodeBuffer = columnCount == 0 ? Array.Empty<DbValue>() : new DbValue[columnCount];

        long count = 0;
        double sum = 0;
        bool hasReal = false;
        bool hasAny = false;
        DbValue? best = null;
        AggregateDistinctValueSet? distinctValues = _isDistinct ? new AggregateDistinctValueSet() : null;

        while (await _source.MoveNextAsync(ct))
        {
            ReadOnlySpan<byte> payload = _payloadSource.CurrentPayload.Span;
            if (!EvaluatePreDecodeFilters(payload))
                continue;

            if (columnCount > 0)
                _recordSerializer.DecodeSelectedCompactInto(payload, decodeBuffer, _decodedColumnIndices);

            if (_predicateEvaluator != null && !_predicateEvaluator(decodeBuffer).IsTruthy)
                continue;

            if (_isCountStar)
            {
                count++;
                continue;
            }

            if (distinctValues != null &&
                _aggregateArgumentEvaluator == null &&
                (uint)_columnIndex < (uint)_decodedColumnIndices.Length &&
                _kind is AggregateKind.Count or AggregateKind.Sum or AggregateKind.Avg)
            {
                if (BoundColumnAccessHelper.TryDecodeNumeric(
                        payload,
                        _recordSerializer,
                        null,
                        _decodedColumnIndices[_columnIndex],
                        out long intVal,
                        out double realVal,
                        out bool isReal))
                {
                    if (!distinctValues.AddNumeric(intVal, realVal, isReal))
                        continue;

                    switch (_kind)
                    {
                        case AggregateKind.Count:
                            count++;
                            continue;
                        case AggregateKind.Sum:
                        case AggregateKind.Avg:
                            hasAny = true;
                            if (isReal)
                            {
                                hasReal = true;
                                sum += realVal;
                            }
                            else
                            {
                                sum += intVal;
                            }

                            count++;
                            continue;
                    }
                }
            }

            DbValue value = _aggregateArgumentEvaluator != null
                ? _aggregateArgumentEvaluator(decodeBuffer)
                : (uint)_columnIndex < (uint)decodeBuffer.Length
                    ? decodeBuffer[_columnIndex]
                    : DbValue.Null;
            if (value.IsNull)
                continue;
            if (distinctValues != null && !distinctValues.Add(value))
                continue;

            switch (_kind)
            {
                case AggregateKind.Count:
                    count++;
                    break;
                case AggregateKind.Sum:
                case AggregateKind.Avg:
                    hasAny = true;
                    if (value.Type == DbType.Real)
                    {
                        hasReal = true;
                        sum += value.AsReal;
                    }
                    else
                    {
                        sum += value.AsInteger;
                    }
                    count++;
                    break;
                case AggregateKind.Min:
                    if (best == null || DbValue.Compare(value, best.Value) < 0)
                        best = value;
                    break;
                case AggregateKind.Max:
                    if (best == null || DbValue.Compare(value, best.Value) > 0)
                        best = value;
                    break;
            }
        }

        return _kind switch
        {
            AggregateKind.Count => DbValue.FromInteger(count),
            AggregateKind.Sum => !hasAny ? DbValue.FromInteger(0)
                : hasReal ? DbValue.FromReal(sum) : DbValue.FromInteger((long)sum),
            AggregateKind.Avg => !hasAny ? DbValue.Null : DbValue.FromReal(sum / count),
            AggregateKind.Min => best ?? DbValue.Null,
            AggregateKind.Max => best ?? DbValue.Null,
            _ => DbValue.Null,
        };
    }

    private PreDecodeFilterSpec[] CreatePreDecodeFilters(BatchPushdownFilter[]? pushdownFilters)
    {
        if (pushdownFilters == null || pushdownFilters.Length == 0)
            return Array.Empty<PreDecodeFilterSpec>();

        var filters = new PreDecodeFilterSpec[pushdownFilters.Length];
        for (int i = 0; i < pushdownFilters.Length; i++)
        {
            var filter = pushdownFilters[i];
            if ((uint)filter.ColumnIndex >= (uint)_decodedColumnIndices.Length)
                return Array.Empty<PreDecodeFilterSpec>();

            filters[i] = new PreDecodeFilterSpec(
                _recordSerializer,
                _decodedColumnIndices[filter.ColumnIndex],
                filter);
        }

        return filters;
    }

    private bool EvaluatePreDecodeFilters(ReadOnlySpan<byte> payload)
        => _preDecodeFilters.Length == 0
            || BoundColumnAccessHelper.EvaluatePreDecodeFilters(payload, _recordSerializer, _preDecodeFilters);
}

/// <summary>
/// COUNT(*) fast path for a single table with no filters.
/// Produces exactly one row with the table entry count.
/// </summary>
public sealed class CountStarTableOperator : IOperator, IEstimatedRowCountProvider
{
    private readonly BTree _tableTree;
    private readonly bool _ignoreCachedCount;
    private bool _emitted;

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => false;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
    public int? EstimatedRowCount => 1;

    public CountStarTableOperator(BTree tableTree, ColumnDefinition[] outputSchema, bool ignoreCachedCount = false)
    {
        _tableTree = tableTree;
        _ignoreCachedCount = ignoreCachedCount;
        OutputSchema = outputSchema;
    }

    public ValueTask OpenAsync(CancellationToken ct = default)
    {
        _emitted = false;
        Current = Array.Empty<DbValue>();
        return ValueTask.CompletedTask;
    }

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (_emitted) return false;
        _emitted = true;

        long count = _ignoreCachedCount
            ? await _tableTree.CountEntriesExactAsync(ct)
            : await _tableTree.CountEntriesAsync(ct);
        Current = new[] { DbValue.FromInteger(count) };
        return true;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

/// <summary>
/// Yields pre-materialized rows. Used for CTEs whose results have been computed upfront.
/// </summary>
public sealed class MaterializedOperator : IOperator, IEstimatedRowCountProvider, IMaterializedRowsProvider
{
    private List<DbValue[]>? _rows;
    private int _index;

    public ColumnDefinition[] OutputSchema { get; }
    public bool ReusesCurrentRowBuffer => false;
    public DbValue[] Current { get; private set; } = Array.Empty<DbValue>();
    public int? EstimatedRowCount => _rows?.Count;

    public MaterializedOperator(List<DbValue[]> rows, ColumnDefinition[] outputSchema)
    {
        _rows = rows;
        OutputSchema = outputSchema;
    }

    public ValueTask OpenAsync(CancellationToken ct = default)
    {
        _index = -1;
        return ValueTask.CompletedTask;
    }

    public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (_rows == null)
            return ValueTask.FromResult(false);

        _index++;
        if (_index >= _rows.Count) return ValueTask.FromResult(false);
        Current = _rows[_index];
        return ValueTask.FromResult(true);
    }

    public bool TryTakeMaterializedRows(out List<DbValue[]> rows)
    {
        if (_rows == null)
        {
            rows = new List<DbValue[]>(0);
            return false;
        }

        rows = _rows;
        _rows = null;
        _index = -1;
        Current = Array.Empty<DbValue>();
        return true;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
