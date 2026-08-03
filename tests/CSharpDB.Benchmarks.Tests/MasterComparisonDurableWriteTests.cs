using CSharpDB.Benchmarks.Macro;

namespace CSharpDB.Benchmarks.Tests;

public sealed class MasterComparisonDurableWriteTests
{
    [Fact]
    public void DurableWriteSourceScenarios_AreTheEightPersistentHybridStorageWrites()
    {
        string[] expected =
        [
            "Storage_FileBacked_Sql_SingleInsert_5s",
            "Storage_FileBacked_Sql_Batch100_5s",
            "Storage_HybridIncrementalDurable_Sql_SingleInsert_5s",
            "Storage_HybridIncrementalDurable_Sql_Batch100_5s",
            "Storage_FileBacked_Collection_Put_5s",
            "Storage_FileBacked_Collection_Batch100_5s",
            "Storage_HybridIncrementalDurable_Collection_Put_5s",
            "Storage_HybridIncrementalDurable_Collection_Batch100_5s",
        ];

        Assert.Equal(expected, HybridStorageModeBenchmark.MasterComparisonDurableWriteScenarioNames);
        Assert.DoesNotContain(
            HybridStorageModeBenchmark.MasterComparisonDurableWriteScenarioNames,
            static name => name.Contains("_InMemory_", StringComparison.Ordinal));
        Assert.DoesNotContain(
            HybridStorageModeBenchmark.MasterComparisonDurableWriteScenarioNames,
            static name => name.Contains("Lookup", StringComparison.Ordinal) ||
                name.Contains("Get_", StringComparison.Ordinal) ||
                name.Contains("Reads", StringComparison.Ordinal));
    }

    [Fact]
    public void HostedStableSourceScenarios_AreTheSixteenReadOrInMemoryHybridStorageRows()
    {
        string[] expected =
        [
            "Storage_FileBacked_Sql_PointLookup_20000",
            "Storage_FileBacked_Sql_ConcurrentReads_8readers",
            "Storage_FileBacked_Sql_ConcurrentReadsBurst32_8readers",
            "Storage_FileBacked_Collection_Get_20000",
            "Storage_HybridIncrementalDurable_Sql_PointLookup_20000",
            "Storage_HybridIncrementalDurable_Sql_ConcurrentReads_8readers",
            "Storage_HybridIncrementalDurable_Sql_ConcurrentReadsBurst32_8readers",
            "Storage_HybridIncrementalDurable_Collection_Get_20000",
            "Storage_InMemory_Sql_SingleInsert_5s",
            "Storage_InMemory_Sql_Batch100_5s",
            "Storage_InMemory_Sql_PointLookup_20000",
            "Storage_InMemory_Sql_ConcurrentReads_8readers",
            "Storage_InMemory_Sql_ConcurrentReadsBurst32_8readers",
            "Storage_InMemory_Collection_Put_5s",
            "Storage_InMemory_Collection_Batch100_5s",
            "Storage_InMemory_Collection_Get_20000",
        ];

        Assert.Equal(expected, HybridStorageModeBenchmark.MasterComparisonHostedStableScenarioNames);
    }

    [Fact]
    public void HybridSourceSubsets_AreACompleteDisjointPartitionOfTheLiveMasterContract()
    {
        string[] partition = HybridStorageModeBenchmark.MasterComparisonDurableWriteScenarioNames
            .Concat(HybridStorageModeBenchmark.MasterComparisonHostedStableScenarioNames)
            .OrderBy(static name => name, StringComparer.Ordinal)
            .ToArray();
        string[] liveContract = HybridStorageModeBenchmark.MasterComparisonScenarioNames
            .OrderBy(static name => name, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(liveContract, partition);
        Assert.Empty(
            HybridStorageModeBenchmark.MasterComparisonDurableWriteScenarioNames.Intersect(
                HybridStorageModeBenchmark.MasterComparisonHostedStableScenarioNames,
                StringComparer.Ordinal));
    }

    [Fact]
    public void DirectClientSubsets_AreACompleteDisjointPartitionOfTheLiveMasterContract()
    {
        DirectFileCacheTransportBenchmark.MasterComparisonScenario[] partition =
            DirectFileCacheTransportBenchmark.MasterComparisonDurableWriteScenarios
                .Concat(DirectFileCacheTransportBenchmark.MasterComparisonHostedStableScenarios)
                .OrderBy(static scenario => scenario)
                .ToArray();
        DirectFileCacheTransportBenchmark.MasterComparisonScenario[] liveContract =
            Enum.GetValues<DirectFileCacheTransportBenchmark.MasterComparisonScenario>();

        Assert.Equal(liveContract, partition);
        Assert.Empty(
            DirectFileCacheTransportBenchmark.MasterComparisonDurableWriteScenarios.Intersect(
                DirectFileCacheTransportBenchmark.MasterComparisonHostedStableScenarios));
        Assert.Equal(
            new[]
            {
                DirectFileCacheTransportBenchmark.MasterComparisonScenario.SqlSingleInsert,
                DirectFileCacheTransportBenchmark.MasterComparisonScenario.SqlBatchInsert,
            },
            DirectFileCacheTransportBenchmark.MasterComparisonDurableWriteScenarios);
        Assert.Equal(
            new[]
            {
                DirectFileCacheTransportBenchmark.MasterComparisonScenario.SqlPointLookup,
                DirectFileCacheTransportBenchmark.MasterComparisonScenario.SqlConcurrentReads,
            },
            DirectFileCacheTransportBenchmark.MasterComparisonHostedStableScenarios);
    }

    [Fact]
    public void DurableWriteRows_AreTheTenRequiredMasterComparisonRowsInStableOrder()
    {
        string[] expected =
        [
            "MasterComparison_Sql_FileBacked_SingleInsert",
            "MasterComparison_Sql_FileBacked_BatchInsertRows",
            "MasterComparison_Sql_HybridIncrementalDurable_SingleInsert",
            "MasterComparison_Sql_HybridIncrementalDurable_BatchInsertRows",
            "MasterComparison_Sql_DirectClientLocalProcess_SingleInsert",
            "MasterComparison_Sql_DirectClientLocalProcess_BatchInsertRows",
            "MasterComparison_Collection_FileBacked_SinglePut",
            "MasterComparison_Collection_FileBacked_BatchPutDocs",
            "MasterComparison_Collection_HybridIncrementalDurable_SinglePut",
            "MasterComparison_Collection_HybridIncrementalDurable_BatchPutDocs",
        ];

        Assert.Equal(expected, MasterComparisonBenchmark.DurableWriteRowNames);
        Assert.Equal(
            MasterComparisonBenchmark.DurableWriteRowNames.Count,
            MasterComparisonBenchmark.DurableWriteRowNames.Distinct(StringComparer.Ordinal).Count());
        Assert.DoesNotContain(
            MasterComparisonBenchmark.DurableWriteRowNames,
            static name => name.Contains("_InMemory_", StringComparison.Ordinal) ||
                name.Contains("Point", StringComparison.Ordinal) ||
                name.Contains("Reads", StringComparison.Ordinal));
    }

    [Fact]
    public void HostedStableRows_AreTheOtherEighteenMasterComparisonRows()
    {
        string[] expected =
        [
            "MasterComparison_Sql_FileBacked_PointLookup",
            "MasterComparison_Sql_FileBacked_ConcurrentReadsPerQuery",
            "MasterComparison_Sql_FileBacked_ConcurrentReadsBurst32",
            "MasterComparison_Sql_HybridIncrementalDurable_PointLookup",
            "MasterComparison_Sql_HybridIncrementalDurable_ConcurrentReadsPerQuery",
            "MasterComparison_Sql_HybridIncrementalDurable_ConcurrentReadsBurst32",
            "MasterComparison_Sql_DirectClientLocalProcess_PointLookup",
            "MasterComparison_Sql_DirectClientLocalProcess_ConcurrentReadsPerQuery",
            "MasterComparison_Sql_InMemory_SingleInsert",
            "MasterComparison_Sql_InMemory_BatchInsertRows",
            "MasterComparison_Sql_InMemory_PointLookup",
            "MasterComparison_Sql_InMemory_ConcurrentReadsPerQuery",
            "MasterComparison_Sql_InMemory_ConcurrentReadsBurst32",
            "MasterComparison_Collection_FileBacked_PointGet",
            "MasterComparison_Collection_HybridIncrementalDurable_PointGet",
            "MasterComparison_Collection_InMemory_SinglePut",
            "MasterComparison_Collection_InMemory_BatchPutDocs",
            "MasterComparison_Collection_InMemory_PointGet",
        ];

        Assert.Equal(expected, MasterComparisonBenchmark.HostedStableRowNames);
        Assert.Empty(
            MasterComparisonBenchmark.HostedStableRowNames.Intersect(
                MasterComparisonBenchmark.DurableWriteRowNames,
                StringComparer.Ordinal));
        Assert.Equal(
            28,
            MasterComparisonBenchmark.HostedStableRowNames
                .Concat(MasterComparisonBenchmark.DurableWriteRowNames)
                .Distinct(StringComparer.Ordinal)
                .Count());
    }
}
