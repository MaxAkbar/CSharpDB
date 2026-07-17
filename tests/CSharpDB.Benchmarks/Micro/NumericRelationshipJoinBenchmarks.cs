using System.Reflection;
using System.Text;
using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Sql;
using CSharpDB.Storage.BTrees;
using CSharpDB.Storage.Catalog;
using CSharpDB.Storage.Indexing;
using CSharpDB.Storage.Serialization;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Compares the current parent-driven numeric FK index lookup join with an ordered
/// merge of the parent PK tree and the same existing FK support index. The benchmark constructs
/// both operators directly so planner selection does not affect the comparison.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class NumericRelationshipJoinBenchmarks
{
    private const int ParentCount = 1_000;
    private const int PointParentId = 501;
    private const int SeedBatchSize = 500;

    private static readonly string ParentPadding = new('p', 128);
    private static readonly string ChildPadding = new('c', 128);

    private static readonly ColumnDefinition[] KeyProjectionSchema =
    [
        new ColumnDefinition { Name = "parent_id", Type = DbType.Integer, Nullable = false },
        new ColumnDefinition { Name = "child_parent_id", Type = DbType.Integer, Nullable = false },
    ];

    private static readonly ColumnDefinition[] PayloadProjectionSchema =
    [
        new ColumnDefinition { Name = "parent_payload", Type = DbType.Integer, Nullable = false },
        new ColumnDefinition { Name = "child_amount", Type = DbType.Integer, Nullable = false },
    ];

    [Params(1, 10, 100)]
    public int Fanout { get; set; }

    [Params(RelationshipScanShape.FullScan, RelationshipScanShape.Point)]
    public RelationshipScanShape ScanShape { get; set; }

    [Params(RelationshipProjection.Keys, RelationshipProjection.Payload)]
    public RelationshipProjection Projection { get; set; }

    private BenchmarkDatabase _bench = null!;
    private BTree _parentTree = null!;
    private BTree _childTree = null!;
    private IIndexStore _foreignKeyIndex = null!;
    private TableSchema _parentSchema = null!;
    private TableSchema _childSchema = null!;
    private TableSchema _compositeSchema = null!;
    private IRecordSerializer _recordSerializer = null!;
    private int _childForeignKeyColumnIndex;
    private int _expectedRowCount;

    [GlobalSetup]
    public void GlobalSetup()
        => GlobalSetupAsync().GetAwaiter().GetResult();

    [GlobalCleanup]
    public void GlobalCleanup()
        => _bench.Dispose();

    [Benchmark(Baseline = true, Description = "Current parent-driven FK index lookup join")]
    public Task<JoinResultSignature> CurrentIndexNestedLoopJoin()
        => ExecuteCurrentJoinAsync();

    [Benchmark(Description = "Ordered numeric relationship index scan")]
    public Task<JoinResultSignature> NumericRelationshipIndexJoin()
        => ExecuteNumericRelationshipJoinAsync();

    private async Task GlobalSetupAsync()
    {
        _bench = await BenchmarkDatabase.CreateWithSchemaAsync(
            $"CREATE TABLE relationship_parent (" +
            "id INTEGER PRIMARY KEY, payload INTEGER NOT NULL, padding TEXT NOT NULL)");

        await _bench.Db.ExecuteAsync(
            "CREATE TABLE relationship_child (" +
            "id INTEGER PRIMARY KEY, " +
            "parent_id INTEGER NOT NULL REFERENCES relationship_parent(id), " +
            "amount INTEGER NOT NULL, " +
            "padding TEXT NOT NULL)");

        await SeedParentsAsync();
        await SeedChildrenAsync();
        await _bench.Db.CheckpointAsync();
        await _bench.ReopenAsync();

        Database db = _bench.Db;
        var catalog = GetPrivateField<SchemaCatalog>(db, "_catalog");
        _recordSerializer = GetPrivateField<IRecordSerializer>(db, "_recordSerializer");

        _parentSchema = db.GetTableSchema("relationship_parent")
            ?? throw new InvalidOperationException("Parent benchmark schema was not found after reopen.");
        _childSchema = db.GetTableSchema("relationship_child")
            ?? throw new InvalidOperationException("Child benchmark schema was not found after reopen.");
        _compositeSchema = TableSchema.CreateJoinSchema(_parentSchema, _childSchema);

        ForeignKeyDefinition foreignKey = _childSchema.ForeignKeys.SingleOrDefault(static fk =>
                string.Equals(fk.ColumnName, "parent_id", StringComparison.OrdinalIgnoreCase) &&
                string.Equals(fk.ReferencedTableName, "relationship_parent", StringComparison.OrdinalIgnoreCase))
            ?? throw new InvalidOperationException("Expected numeric relationship foreign key was not found.");

        _childForeignKeyColumnIndex = _childSchema.GetColumnIndex(foreignKey.ColumnName);
        _parentTree = catalog.GetTableTree(_parentSchema.TableName);
        _childTree = catalog.GetTableTree(_childSchema.TableName);
        _foreignKeyIndex = catalog.GetIndexStore(foreignKey.SupportingIndexName);
        _expectedRowCount = ScanShape == RelationshipScanShape.FullScan
            ? checked(ParentCount * Fanout)
            : Fanout;

        JoinResultSignature current = await ExecuteCurrentJoinAsync();
        JoinResultSignature numericJoin = await ExecuteNumericRelationshipJoinAsync();
        JoinResultSignature expected = BuildExpectedSignature();

        if (current != numericJoin)
        {
            throw new InvalidOperationException(
                $"Join implementations produced different results. Fallback={current}; NumericRelationship={numericJoin}.");
        }

        if (current != expected)
        {
            throw new InvalidOperationException(
                $"Join benchmark result did not match seeded data. Actual={current}; Expected={expected}.");
        }
    }

    private Task<JoinResultSignature> ExecuteCurrentJoinAsync()
    {
        IOperator outer = ScanShape == RelationshipScanShape.FullScan
            ? new TableScanOperator(
                _parentTree,
                _parentSchema,
                _recordSerializer,
                estimatedRowCount: ParentCount)
            : new PrimaryKeyLookupOperator(
                _parentTree,
                _parentSchema,
                PointParentId,
                _recordSerializer);

        var join = new IndexNestedLoopJoinOperator(
            outer,
            _childTree,
            _foreignKeyIndex,
            JoinType.Inner,
            outerKeyIndex: _parentSchema.PrimaryKeyColumnIndex,
            leftColCount: _parentSchema.Columns.Count,
            rightColCount: _childSchema.Columns.Count,
            residualCondition: null,
            _compositeSchema,
            _recordSerializer,
            estimatedOutputRowCount: _expectedRowCount);

        ApplyProjection(join);
        return DrainAsync(join);
    }

    private Task<JoinResultSignature> ExecuteNumericRelationshipJoinAsync()
    {
        IndexScanRange scanRange = ScanShape == RelationshipScanShape.FullScan
            ? IndexScanRange.All
            : IndexScanRange.At(PointParentId);

        var join = new NumericRelationshipIndexJoinOperator(
            _parentTree,
            _childTree,
            _foreignKeyIndex,
            _parentSchema,
            _childSchema,
            _childForeignKeyColumnIndex,
            scanRange,
            _recordSerializer,
            estimatedRowCount: _expectedRowCount);

        ApplyProjection(join);
        return DrainAsync(join);
    }

    private void ApplyProjection(IProjectionPushdownTarget join)
    {
        int childOffset = _parentSchema.Columns.Count;
        int[] columnIndices;
        ColumnDefinition[] outputSchema;

        if (Projection == RelationshipProjection.Keys)
        {
            columnIndices =
            [
                _parentSchema.PrimaryKeyColumnIndex,
                childOffset + _childForeignKeyColumnIndex,
            ];
            outputSchema = KeyProjectionSchema;
        }
        else
        {
            columnIndices =
            [
                _parentSchema.GetColumnIndex("payload"),
                childOffset + _childSchema.GetColumnIndex("amount"),
            ];
            outputSchema = PayloadProjectionSchema;
        }

        if (!join.TrySetOutputProjection(columnIndices, outputSchema))
            throw new InvalidOperationException("Join operator rejected the benchmark projection.");
    }

    private static async Task<JoinResultSignature> DrainAsync(IOperator join)
    {
        if (join is not IBatchOperator batchJoin)
            throw new InvalidOperationException("Numeric relationship join benchmark requires batch-capable operators.");

        long rowCount = 0;
        long firstSum = 0;
        long secondSum = 0;
        ulong pairHashSum = 0;

        await join.OpenAsync();
        try
        {
            while (await batchJoin.MoveNextBatchAsync())
            {
                RowBatch batch = batchJoin.CurrentBatch;
                for (int rowIndex = 0; rowIndex < batch.Count; rowIndex++)
                {
                    ReadOnlySpan<DbValue> row = batch.GetRowSpan(rowIndex);
                    long first = row[0].AsInteger;
                    long second = row[1].AsInteger;

                    rowCount++;
                    firstSum += first;
                    secondSum += second;
                    pairHashSum += HashPair(first, second);
                }
            }
        }
        finally
        {
            await join.DisposeAsync();
        }

        return new JoinResultSignature(rowCount, firstSum, secondSum, pairHashSum);
    }

    private JoinResultSignature BuildExpectedSignature()
    {
        long rowCount = _expectedRowCount;
        long firstSum = 0;
        long secondSum = 0;
        ulong pairHashSum = 0;

        int firstParentId = ScanShape == RelationshipScanShape.FullScan ? 1 : PointParentId;
        int lastParentId = ScanShape == RelationshipScanShape.FullScan ? ParentCount : PointParentId;

        for (int parentId = firstParentId; parentId <= lastParentId; parentId++)
        {
            int firstChildId = checked(((parentId - 1) * Fanout) + 1);
            int lastChildId = checked(parentId * Fanout);
            for (int childId = firstChildId; childId <= lastChildId; childId++)
            {
                long first = Projection == RelationshipProjection.Keys
                    ? parentId
                    : parentId * 7L;
                long second = Projection == RelationshipProjection.Keys
                    ? parentId
                    : childId * 11L;

                firstSum += first;
                secondSum += second;
                pairHashSum += HashPair(first, second);
            }
        }

        return new JoinResultSignature(rowCount, firstSum, secondSum, pairHashSum);
    }

    private async Task SeedParentsAsync()
    {
        await _bench.Db.BeginTransactionAsync();
        try
        {
            for (int start = 1; start <= ParentCount; start += SeedBatchSize)
            {
                int end = Math.Min(ParentCount, start + SeedBatchSize - 1);
                var sql = new StringBuilder("INSERT INTO relationship_parent VALUES ");
                for (int id = start; id <= end; id++)
                {
                    if (id > start)
                        sql.Append(',');
                    sql.Append('(')
                        .Append(id)
                        .Append(',')
                        .Append(id * 7L)
                        .Append(",'p_")
                        .Append(ParentPadding)
                        .Append("')");
                }

                await _bench.Db.ExecuteAsync(sql.ToString());
            }

            await _bench.Db.CommitAsync();
        }
        catch
        {
            await _bench.Db.RollbackAsync();
            throw;
        }
    }

    private async Task SeedChildrenAsync()
    {
        int childCount = checked(ParentCount * Fanout);
        await _bench.Db.BeginTransactionAsync();
        try
        {
            for (int start = 1; start <= childCount; start += SeedBatchSize)
            {
                int end = Math.Min(childCount, start + SeedBatchSize - 1);
                var sql = new StringBuilder("INSERT INTO relationship_child VALUES ");
                for (int childId = start; childId <= end; childId++)
                {
                    if (childId > start)
                        sql.Append(',');

                    int parentId = ((childId - 1) / Fanout) + 1;
                    sql.Append('(')
                        .Append(childId)
                        .Append(',')
                        .Append(parentId)
                        .Append(',')
                        .Append(childId * 11L)
                        .Append(",'c_")
                        .Append(ChildPadding)
                        .Append("')");
                }

                await _bench.Db.ExecuteAsync(sql.ToString());
            }

            await _bench.Db.CommitAsync();
        }
        catch
        {
            await _bench.Db.RollbackAsync();
            throw;
        }
    }

    private static ulong HashPair(long first, long second)
    {
        unchecked
        {
            ulong value = (ulong)first + 0x9E3779B97F4A7C15UL;
            value = (value ^ (value >> 30)) * 0xBF58476D1CE4E5B9UL;
            value ^= (ulong)second + 0x94D049BB133111EBUL;
            value = (value ^ (value >> 27)) * 0x94D049BB133111EBUL;
            return value ^ (value >> 31);
        }
    }

    private static T GetPrivateField<T>(object target, string fieldName)
        where T : class
    {
        FieldInfo field = target.GetType().GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException(
                $"Field '{fieldName}' was not found on {target.GetType().Name}.");

        return field.GetValue(target) as T
            ?? throw new InvalidOperationException(
                $"Field '{fieldName}' on {target.GetType().Name} did not contain {typeof(T).Name}.");
    }

    public enum RelationshipScanShape
    {
        FullScan,
        Point,
    }

    public enum RelationshipProjection
    {
        Keys,
        Payload,
    }

    public readonly record struct JoinResultSignature(
        long RowCount,
        long FirstSum,
        long SecondSum,
        ulong PairHashSum);
}
