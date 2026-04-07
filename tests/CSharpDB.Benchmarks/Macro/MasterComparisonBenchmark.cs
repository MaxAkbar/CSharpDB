using CSharpDB.Benchmarks.Infrastructure;

namespace CSharpDB.Benchmarks.Macro;

public static class MasterComparisonBenchmark
{
    private const int BatchSize = 100;

    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        var results = new List<BenchmarkResult>();

        results.AddRange(Remap(await HybridStorageModeBenchmark.RunAsync(), MapHybridStorageResult));
        results.AddRange(Remap(await DirectFileCacheTransportBenchmark.RunMasterComparisonSubsetAsync(), MapDirectClientResult));
        results.Sort(static (left, right) =>
        {
            int familyOrder = GetFamilyOrder(left.Name).CompareTo(GetFamilyOrder(right.Name));
            if (familyOrder != 0)
                return familyOrder;

            int operationOrder = GetOperationOrder(left.Name).CompareTo(GetOperationOrder(right.Name));
            if (operationOrder != 0)
                return operationOrder;

            return string.CompareOrdinal(left.Name, right.Name);
        });

        return results;
    }

    private static List<BenchmarkResult> Remap(
        IReadOnlyList<BenchmarkResult> sourceResults,
        Func<string, MasterComparisonMapping> map)
    {
        var remapped = new List<BenchmarkResult>(sourceResults.Count);
        foreach (BenchmarkResult sourceResult in sourceResults)
        {
            MasterComparisonMapping mapping = map(sourceResult.Name);
            remapped.Add(CloneForMasterComparison(sourceResult, mapping));
        }

        return remapped;
    }

    private static MasterComparisonMapping MapHybridStorageResult(string sourceName)
    {
        const string storagePrefix = "Storage_";
        const string sqlSeparator = "_Sql_";
        const string collectionSeparator = "_Collection_";

        if (!sourceName.StartsWith(storagePrefix, StringComparison.Ordinal))
            throw new InvalidOperationException($"Unexpected hybrid storage benchmark result '{sourceName}'.");

        bool isCollection = sourceName.Contains(collectionSeparator, StringComparison.Ordinal);
        string separator = isCollection ? collectionSeparator : sqlSeparator;
        int separatorIndex = sourceName.IndexOf(separator, StringComparison.Ordinal);
        if (separatorIndex < 0)
            throw new InvalidOperationException($"Unable to parse hybrid storage benchmark result '{sourceName}'.");

        string modeToken = sourceName[storagePrefix.Length..separatorIndex];
        string operationToken = sourceName[(separatorIndex + separator.Length)..];
        string surfaceToken = isCollection ? "Collection" : "Sql";
        string operationName = isCollection
            ? operationToken switch
            {
                "Put_5s" => "SinglePut",
                "Batch100_5s" => "BatchPutDocs",
                "Get_20000" => "PointGet",
                _ => throw new InvalidOperationException($"Unexpected collection master-table operation '{sourceName}'."),
            }
            : operationToken switch
            {
                "SingleInsert_5s" => "SingleInsert",
                "Batch100_5s" => "BatchInsertRows",
                "PointLookup_20000" => "PointLookup",
                "ConcurrentReads_8readers" => "ConcurrentReadsPerQuery",
                "ConcurrentReadsBurst32_8readers" => "ConcurrentReadsBurst32",
                _ => throw new InvalidOperationException($"Unexpected SQL master-table operation '{sourceName}'."),
            };

        return new MasterComparisonMapping(
            Name: $"MasterComparison_{surfaceToken}_{modeToken}_{operationName}",
            OpsMultiplier: operationToken.StartsWith("Batch100_", StringComparison.Ordinal) ? BatchSize : 1,
            ExtraInfoNote: operationToken.StartsWith("Batch100_", StringComparison.Ordinal)
                ? isCollection
                    ? $"throughput-unit=docs/sec from {BatchSize}-doc transactions"
                    : $"throughput-unit=rows/sec from {BatchSize}-row transactions"
                : null);
    }

    private static MasterComparisonMapping MapDirectClientResult(string sourceName)
    {
        const string directPrefix = "Direct_DirectLookupPreset_Sql_";
        if (!sourceName.StartsWith(directPrefix, StringComparison.Ordinal))
            throw new InvalidOperationException($"Unexpected direct client benchmark result '{sourceName}'.");

        string operationToken = sourceName[directPrefix.Length..];
        string operationName = operationToken switch
        {
            "SingleInsert_10s" => "SingleInsert",
            "Batch100_10s" => "BatchInsertRows",
            "PointLookup_20000" => "PointLookup",
            "ConcurrentReads_8readers" => "ConcurrentReadsPerQuery",
            _ => throw new InvalidOperationException($"Unexpected direct client master-table operation '{sourceName}'."),
        };

        return new MasterComparisonMapping(
            Name: $"MasterComparison_Sql_DirectClientLocalProcess_{operationName}",
            OpsMultiplier: operationToken.StartsWith("Batch100_", StringComparison.Ordinal) ? BatchSize : 1,
            ExtraInfoNote: operationToken.StartsWith("Batch100_", StringComparison.Ordinal)
                ? $"throughput-unit=rows/sec from {BatchSize}-row transactions"
                : null);
    }

    private static BenchmarkResult CloneForMasterComparison(BenchmarkResult source, MasterComparisonMapping mapping)
    {
        string sourceInfo = $"source={source.Name}";
        string? extraInfo = AppendExtraInfo(source.ExtraInfo, mapping.ExtraInfoNote, sourceInfo);

        return new BenchmarkResult
        {
            Name = mapping.Name,
            TotalOps = checked(source.TotalOps * mapping.OpsMultiplier),
            ElapsedMs = source.ElapsedMs,
            P50Ms = source.P50Ms,
            P90Ms = source.P90Ms,
            P95Ms = source.P95Ms,
            P99Ms = source.P99Ms,
            P999Ms = source.P999Ms,
            MinMs = source.MinMs,
            MaxMs = source.MaxMs,
            MeanMs = source.MeanMs,
            StdDevMs = source.StdDevMs,
            ExtraInfo = extraInfo,
        };
    }

    private static string? AppendExtraInfo(string? existing, params string?[] notes)
    {
        var values = new List<string>();
        if (!string.IsNullOrWhiteSpace(existing))
            values.Add(existing);

        foreach (string? note in notes)
        {
            if (!string.IsNullOrWhiteSpace(note))
                values.Add(note);
        }

        return values.Count == 0 ? null : string.Join("; ", values);
    }

    private static int GetFamilyOrder(string name)
    {
        return name switch
        {
            _ when name.StartsWith("MasterComparison_Sql_FileBacked_", StringComparison.Ordinal) => 0,
            _ when name.StartsWith("MasterComparison_Sql_HybridIncrementalDurable_", StringComparison.Ordinal) => 1,
            _ when name.StartsWith("MasterComparison_Sql_DirectClientLocalProcess_", StringComparison.Ordinal) => 2,
            _ when name.StartsWith("MasterComparison_Sql_InMemory_", StringComparison.Ordinal) => 3,
            _ when name.StartsWith("MasterComparison_Collection_FileBacked_", StringComparison.Ordinal) => 4,
            _ when name.StartsWith("MasterComparison_Collection_HybridIncrementalDurable_", StringComparison.Ordinal) => 5,
            _ when name.StartsWith("MasterComparison_Collection_InMemory_", StringComparison.Ordinal) => 6,
            _ => int.MaxValue,
        };
    }

    private static int GetOperationOrder(string name)
    {
        return name switch
        {
            _ when name.EndsWith("_SingleInsert", StringComparison.Ordinal) => 0,
            _ when name.EndsWith("_SinglePut", StringComparison.Ordinal) => 0,
            _ when name.EndsWith("_BatchInsertRows", StringComparison.Ordinal) => 1,
            _ when name.EndsWith("_BatchPutDocs", StringComparison.Ordinal) => 1,
            _ when name.EndsWith("_PointLookup", StringComparison.Ordinal) => 2,
            _ when name.EndsWith("_PointGet", StringComparison.Ordinal) => 2,
            _ when name.EndsWith("_ConcurrentReadsPerQuery", StringComparison.Ordinal) => 3,
            _ when name.EndsWith("_ConcurrentReadsBurst32", StringComparison.Ordinal) => 4,
            _ => int.MaxValue,
        };
    }

    private sealed record MasterComparisonMapping(string Name, int OpsMultiplier, string? ExtraInfoNote);
}
