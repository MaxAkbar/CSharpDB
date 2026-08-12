using System.Reflection;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Benchmarks.Macro;
using CSharpDB.Engine;
using CSharpDB.Primitives;

namespace CSharpDB.Benchmarks.Tests;

public sealed class DurableSqlBatchingBenchmarkContractTests
{
    [Fact]
    public async Task RandomPermutationKeyFitsBenchmarkPrimaryKeySchema()
    {
        const BindingFlags privateStatic = BindingFlags.NonPublic | BindingFlags.Static;
        Type benchmarkType = typeof(DurableSqlBatchingBenchmark);
        Type? keyPatternType = benchmarkType.GetNestedType("KeyPattern", BindingFlags.NonPublic);
        MethodInfo? mapId = benchmarkType.GetMethod("MapId", privateStatic);
        FieldInfo? schemaField = benchmarkType.GetField("CreateBenchTableSql", privateStatic);
        Assert.NotNull(keyPatternType);
        Assert.NotNull(mapId);
        Assert.NotNull(schemaField);

        object randomPattern = Enum.Parse(keyPatternType, "Random");
        long firstRandomId = Assert.IsType<long>(mapId.Invoke(null, [1, randomPattern]));
        string schema = Assert.IsType<string>(schemaField.GetRawConstantValue());

        Assert.Equal(2_654_435_762L, firstRandomId);
        Assert.True(firstRandomId > int.MaxValue);
        Assert.Contains("id BIGINT PRIMARY KEY", schema, StringComparison.Ordinal);

        await using BenchmarkDatabase benchmark =
            await BenchmarkDatabase.CreateWithSchemaAsync(schema);
        InsertBatch batch = benchmark.Db.PrepareInsertBatch("bench", initialCapacity: 1);
        batch.AddRow(
            DbValue.FromInteger(firstRandomId),
            DbValue.FromInteger(1),
            DbValue.FromText("durable_batch"),
            DbValue.FromText("Alpha"));

        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        await benchmark.Db.BeginTransactionAsync(cancellationToken);
        Assert.Equal(1, await batch.ExecuteAsync(cancellationToken));
        await benchmark.Db.CommitAsync(cancellationToken);
    }
}
