using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;

namespace CSharpDB.Benchmarks.Macro;

public static class HybridColdOpenBenchmark
{
    private const int SeedCount = 200_000;
    private const int MeasuredIterations = 15;
    private const int SqlLookupId = 175_321;
    private const int CollectionLookupId = 175_321;

    private sealed record BenchDoc(string Name, int Value, string Category);

    private enum StorageMode
    {
        FileBacked,
        InMemory,
        HybridIncrementalDurable,
        HybridHotSetIncrementalDurable,
    }

    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        await using var inputs = await SeededInputs.CreateAsync();
        await PrimeCodePathsAsync();

        var results = new List<BenchmarkResult>();
        foreach (StorageMode mode in Enum.GetValues<StorageMode>())
        {
            results.Add(await RunSqlOpenOnlyAsync(mode, inputs.SqlFilePath));
            results.Add(await RunSqlOpenAndFirstLookupAsync(mode, inputs.SqlFilePath));
            results.Add(await RunCollectionOpenOnlyAsync(mode, inputs.CollectionFilePath));
            results.Add(await RunCollectionOpenAndFirstGetAsync(mode, inputs.CollectionFilePath));
        }

        return results;
    }

    private static Task<BenchmarkResult> RunSqlOpenOnlyAsync(StorageMode mode, string filePath)
    {
        return MacroBenchmarkRunner.RunAsync(
            $"{GetPrefix(mode)}_Sql_OpenOnly_{SeedCount}",
            warmupIterations: 0,
            measuredIterations: MeasuredIterations,
            async () =>
            {
                await using var db = await OpenSqlDatabaseAsync(mode, filePath);
            });
    }

    private static Task<BenchmarkResult> RunSqlOpenAndFirstLookupAsync(StorageMode mode, string filePath)
    {
        return MacroBenchmarkRunner.RunAsync(
            $"{GetPrefix(mode)}_Sql_OpenAndFirstLookup_{SeedCount}",
            warmupIterations: 0,
            measuredIterations: MeasuredIterations,
            async () =>
            {
                await using var db = await OpenSqlDatabaseAsync(mode, filePath);
                await using var result = await db.ExecuteAsync($"SELECT value FROM bench WHERE id = {SqlLookupId};");
                if (!await result.MoveNextAsync() || result.Current[0].AsInteger != SqlLookupId * 10L)
                    throw new InvalidOperationException($"Lookup for id={SqlLookupId} returned an unexpected result.");
            });
    }

    private static Task<BenchmarkResult> RunCollectionOpenOnlyAsync(StorageMode mode, string filePath)
    {
        return MacroBenchmarkRunner.RunAsync(
            $"{GetPrefix(mode)}_Collection_OpenOnly_{SeedCount}",
            warmupIterations: 0,
            measuredIterations: MeasuredIterations,
            async () =>
            {
                await using var db = await OpenCollectionDatabaseAsync(mode, filePath);
            });
    }

    private static Task<BenchmarkResult> RunCollectionOpenAndFirstGetAsync(StorageMode mode, string filePath)
    {
        return MacroBenchmarkRunner.RunAsync(
            $"{GetPrefix(mode)}_Collection_OpenAndFirstGet_{SeedCount}",
            warmupIterations: 0,
            measuredIterations: MeasuredIterations,
            async () =>
            {
                await using var db = await OpenCollectionDatabaseAsync(mode, filePath);
                var collection = await db.GetCollectionAsync<BenchDoc>("bench_docs");
                BenchDoc? document = await collection.GetAsync($"doc:{CollectionLookupId}");
                if (document is null || document.Value != CollectionLookupId)
                    throw new InvalidOperationException($"Document 'doc:{CollectionLookupId}' was not found or was invalid.");
            });
    }

    private static async Task PrimeCodePathsAsync()
    {
        string sqlFilePath = await InMemoryBenchmarkDatabaseFactory.CreateSeededSqlDatabaseAsync(
            "hybrid-cold-open-prime-sql",
            rowCount: 32);
        string collectionFilePath = await InMemoryBenchmarkDatabaseFactory.CreateSeededCollectionDatabaseAsync(
            "hybrid-cold-open-prime-col",
            rowCount: 32);

        try
        {
            foreach (StorageMode mode in Enum.GetValues<StorageMode>())
            {
                await using (var db = await OpenSqlDatabaseAsync(mode, sqlFilePath))
                {
                    await using var result = await db.ExecuteAsync("SELECT value FROM bench WHERE id = 7;");
                    _ = await result.MoveNextAsync();
                }

                await using (var db = await OpenCollectionDatabaseAsync(mode, collectionFilePath))
                {
                    var collection = await db.GetCollectionAsync<BenchDoc>("bench_docs");
                    _ = await collection.GetAsync("doc:7");
                }
            }
        }
        finally
        {
            InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(sqlFilePath);
            InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(collectionFilePath);
        }
    }

    private static ValueTask<Database> OpenSqlDatabaseAsync(StorageMode mode, string filePath)
    {
        return mode switch
        {
            StorageMode.FileBacked => Database.OpenAsync(filePath),
            StorageMode.InMemory => Database.LoadIntoMemoryAsync(filePath),
            StorageMode.HybridIncrementalDurable => Database.OpenHybridAsync(
                filePath,
                new DatabaseOptions(),
                new HybridDatabaseOptions
                {
                    PersistenceMode = HybridPersistenceMode.IncrementalDurable,
                }),
            StorageMode.HybridHotSetIncrementalDurable => Database.OpenHybridAsync(
                filePath,
                new DatabaseOptions(),
                new HybridDatabaseOptions
                {
                    PersistenceMode = HybridPersistenceMode.IncrementalDurable,
                    HotTableNames = new[] { "bench" },
                }),
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null),
        };
    }

    private static ValueTask<Database> OpenCollectionDatabaseAsync(StorageMode mode, string filePath)
    {
        return mode switch
        {
            StorageMode.FileBacked => Database.OpenAsync(filePath),
            StorageMode.InMemory => Database.LoadIntoMemoryAsync(filePath),
            StorageMode.HybridIncrementalDurable => Database.OpenHybridAsync(
                filePath,
                new DatabaseOptions(),
                new HybridDatabaseOptions
                {
                    PersistenceMode = HybridPersistenceMode.IncrementalDurable,
                }),
            StorageMode.HybridHotSetIncrementalDurable => Database.OpenHybridAsync(
                filePath,
                new DatabaseOptions(),
                new HybridDatabaseOptions
                {
                    PersistenceMode = HybridPersistenceMode.IncrementalDurable,
                    HotCollectionNames = new[] { "bench_docs" },
                }),
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null),
        };
    }

    private static string GetPrefix(StorageMode mode)
        => mode switch
        {
            StorageMode.FileBacked => "ColdOpen_FileBacked",
            StorageMode.InMemory => "ColdOpen_InMemory",
            StorageMode.HybridIncrementalDurable => "ColdOpen_HybridIncrementalDurable",
            StorageMode.HybridHotSetIncrementalDurable => "ColdOpen_HybridHotSetIncrementalDurable",
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null),
        };

    private sealed class SeededInputs : IAsyncDisposable
    {
        private SeededInputs(string sqlFilePath, string collectionFilePath)
        {
            SqlFilePath = sqlFilePath;
            CollectionFilePath = collectionFilePath;
        }

        public string SqlFilePath { get; }
        public string CollectionFilePath { get; }

        public static async Task<SeededInputs> CreateAsync()
        {
            string sqlFilePath = await InMemoryBenchmarkDatabaseFactory.CreateSeededSqlDatabaseAsync(
                "hybrid-cold-open-sql",
                SeedCount);
            string collectionFilePath = await InMemoryBenchmarkDatabaseFactory.CreateSeededCollectionDatabaseAsync(
                "hybrid-cold-open-col",
                SeedCount);
            return new SeededInputs(sqlFilePath, collectionFilePath);
        }

        public ValueTask DisposeAsync()
        {
            InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(SqlFilePath);
            InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(CollectionFilePath);
            return ValueTask.CompletedTask;
        }
    }
}
