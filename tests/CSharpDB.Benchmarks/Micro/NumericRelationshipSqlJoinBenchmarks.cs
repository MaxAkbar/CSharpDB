using System.Reflection;
using System.Text;
using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// End-to-end SQL comparison for the numeric relationship join planner path. Every benchmark
/// executes the same cached SQL text against an identically seeded Database; only the
/// planner's internal diagnostic mode changes between Disabled, Auto, and Force.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class NumericRelationshipSqlJoinBenchmarks
{
    private const int ParentCount = 1_000;
    private const int SeedBatchSize = 500;

    private static readonly string ParentPadding = new('p', 128);
    private static readonly string ChildPadding = new('c', 128);

    [Params(1, 10, 100)]
    public int Fanout { get; set; }

    [Params(RelationshipSqlProjection.Keys, RelationshipSqlProjection.Payload)]
    public RelationshipSqlProjection Projection { get; set; }

    private BenchmarkDatabase _bench = null!;
    private QueryPlanner _planner = null!;
    private string _sql = null!;

    [GlobalSetup]
    public void GlobalSetup()
        => GlobalSetupAsync().GetAwaiter().GetResult();

    [GlobalCleanup]
    public void GlobalCleanup()
        => _bench.Dispose();

    [Benchmark(Baseline = true, Description = "SQL relationship join - fallback plan (optimization disabled)")]
    public Task<SqlJoinResultSignature> Disabled_FallbackPlan()
        => ExecuteAsync(NumericRelationshipJoinMode.Disabled);

    [Benchmark(Description = "SQL relationship join - cost-gated auto")]
    public Task<SqlJoinResultSignature> Auto_CostGatedPlan()
        => ExecuteAsync(NumericRelationshipJoinMode.Auto);

    [Benchmark(Description = "SQL relationship join - forced numeric relationship plan")]
    public Task<SqlJoinResultSignature> Force_NumericRelationshipPlan()
        => ExecuteAsync(NumericRelationshipJoinMode.Force);

    private async Task GlobalSetupAsync()
    {
        _bench = await BenchmarkDatabase.CreateWithSchemaAsync(
            "CREATE TABLE relationship_parent (" +
            "id INTEGER PRIMARY KEY, payload INTEGER NOT NULL, padding TEXT NOT NULL)");

        await _bench.Db.ExecuteAsync(
            "CREATE TABLE relationship_child (" +
            "id INTEGER PRIMARY KEY, " +
            "parent_id INTEGER NOT NULL REFERENCES relationship_parent(id), " +
            "amount INTEGER NOT NULL, " +
            "padding TEXT NOT NULL)");

        await SeedParentsAsync();
        await SeedChildrenAsync();
        await _bench.Db.ExecuteAsync("ANALYZE relationship_parent");
        await _bench.Db.ExecuteAsync("ANALYZE relationship_child");
        await _bench.Db.CheckpointAsync();
        await _bench.ReopenAsync();

        _planner = GetPrivateField<QueryPlanner>(_bench.Db, "_planner");
        _sql = Projection == RelationshipSqlProjection.Keys
            ? "SELECT p.id, c.parent_id FROM relationship_parent p " +
              "INNER JOIN relationship_child c ON p.id = c.parent_id"
            : "SELECT p.payload, c.amount FROM relationship_parent p " +
              "INNER JOIN relationship_child c ON p.id = c.parent_id";

        SqlJoinResultSignature expected = BuildExpectedSignature();
        SqlJoinResultSignature disabled = await ExecuteAsync(
            NumericRelationshipJoinMode.Disabled,
            expectNumericPlan: false);
        SqlJoinResultSignature forced = await ExecuteAsync(
            NumericRelationshipJoinMode.Force,
            expectNumericPlan: true);
        SqlJoinResultSignature automatic = await ExecuteAsync(
            NumericRelationshipJoinMode.Auto,
            expectNumericPlan: Projection == RelationshipSqlProjection.Keys);

        if (disabled != forced || disabled != automatic)
        {
            throw new InvalidOperationException(
                $"SQL relationship join modes produced different results. " +
                $"Disabled={disabled}; Auto={automatic}; Force={forced}.");
        }

        if (disabled != expected)
        {
            throw new InvalidOperationException(
                $"SQL relationship join did not match seeded data. Actual={disabled}; Expected={expected}.");
        }

        _planner.RelationshipJoinMode = NumericRelationshipJoinMode.Auto;
    }

    private async Task<SqlJoinResultSignature> ExecuteAsync(
        NumericRelationshipJoinMode mode,
        bool? expectNumericPlan = null)
    {
        _planner.RelationshipJoinMode = mode;

        long rowCount = 0;
        long firstSum = 0;
        long secondSum = 0;
        ulong pairHashSum = 0;

        await using QueryResult result = await _bench.Db.ExecuteAsync(_sql);
        if (expectNumericPlan.HasValue)
        {
            bool usesNumericPlan = ContainsOperator<NumericRelationshipIndexJoinOperator>(GetRootOperator(result));
            if (usesNumericPlan != expectNumericPlan.Value)
            {
                string expectedPlan = expectNumericPlan.Value ? "numeric relationship" : "fallback";
                throw new InvalidOperationException(
                    $"{mode} selected the wrong SQL relationship plan for {Projection}. " +
                    $"Expected the {expectedPlan} plan.");
            }
        }

        while (await result.MoveNextAsync())
        {
            DbValue[] row = result.Current;
            long first = row[0].AsInteger;
            long second = row[1].AsInteger;

            rowCount++;
            firstSum += first;
            secondSum += second;
            pairHashSum += HashPair(first, second);
        }

        return new SqlJoinResultSignature(rowCount, firstSum, secondSum, pairHashSum);
    }

    private SqlJoinResultSignature BuildExpectedSignature()
    {
        long rowCount = checked((long)ParentCount * Fanout);
        long firstSum = 0;
        long secondSum = 0;
        ulong pairHashSum = 0;

        for (int parentId = 1; parentId <= ParentCount; parentId++)
        {
            int firstChildId = checked(((parentId - 1) * Fanout) + 1);
            int lastChildId = checked(parentId * Fanout);
            for (int childId = firstChildId; childId <= lastChildId; childId++)
            {
                long first = Projection == RelationshipSqlProjection.Keys
                    ? parentId
                    : parentId * 7L;
                long second = Projection == RelationshipSqlProjection.Keys
                    ? parentId
                    : childId * 11L;

                firstSum += first;
                secondSum += second;
                pairHashSum += HashPair(first, second);
            }
        }

        return new SqlJoinResultSignature(rowCount, firstSum, secondSum, pairHashSum);
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

    private static IOperator GetRootOperator(QueryResult result)
    {
        FieldInfo operatorField = typeof(QueryResult).GetField(
                "_operator",
                BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("QueryResult operator field was not found.");
        var root = (IOperator?)operatorField.GetValue(result);

        if (root == null)
        {
            FieldInfo batchOperatorField = typeof(QueryResult).GetField(
                    "_batchOperator",
                    BindingFlags.Instance | BindingFlags.NonPublic)
                ?? throw new InvalidOperationException("QueryResult batch operator field was not found.");
            root = batchOperatorField.GetValue(result) as IOperator
                ?? throw new InvalidOperationException("QueryResult did not contain an operator root.");
        }

        return root is BatchToRowOperatorAdapter batchAdapter
            ? batchAdapter.BatchSource as IOperator
                ?? throw new InvalidOperationException("Batch adapter did not expose an operator root.")
            : root;
    }

    private static bool ContainsOperator<TOperator>(IOperator root)
        where TOperator : class, IOperator
    {
        for (IOperator? current = root;
             current != null;
             current = current is IUnaryOperatorSource unary ? unary.Source : null)
        {
            if (current is TOperator)
                return true;
        }

        return false;
    }

    public enum RelationshipSqlProjection
    {
        Keys,
        Payload,
    }

    public readonly record struct SqlJoinResultSignature(
        long RowCount,
        long FirstSum,
        long SecondSum,
        ulong PairHashSum);
}
