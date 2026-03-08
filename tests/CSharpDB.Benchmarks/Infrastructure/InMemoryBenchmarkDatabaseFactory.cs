using CSharpDB.Engine;

namespace CSharpDB.Benchmarks.Infrastructure;

internal static class InMemoryBenchmarkDatabaseFactory
{
    public static async Task<string> CreateSeededSqlDatabaseAsync(string prefix, int rowCount)
    {
        string filePath = Path.Combine(Path.GetTempPath(), $"{prefix}_{Guid.NewGuid():N}.db");

        await using var db = await Database.OpenAsync(filePath);
        await db.ExecuteAsync("CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER, text_col TEXT, category TEXT)");

        const int batchSize = 500;
        for (int batchStart = 0; batchStart < rowCount; batchStart += batchSize)
        {
            await db.BeginTransactionAsync();
            int batchEnd = Math.Min(batchStart + batchSize, rowCount);
            for (int i = batchStart; i < batchEnd; i++)
            {
                await db.ExecuteAsync(
                    $"INSERT INTO bench VALUES ({i}, {i * 10L}, 'seed_{i}', '{GetCategory(i)}')");
            }
            await db.CommitAsync();
        }

        return filePath;
    }

    public static async Task<string> CreateSeededCollectionDatabaseAsync(string prefix, int rowCount)
    {
        string filePath = Path.Combine(Path.GetTempPath(), $"{prefix}_{Guid.NewGuid():N}.db");

        await using var db = await Database.OpenAsync(filePath);
        var collection = await db.GetCollectionAsync<CollectionBenchDoc>("bench_docs");

        const int batchSize = 500;
        for (int batchStart = 0; batchStart < rowCount; batchStart += batchSize)
        {
            await db.BeginTransactionAsync();
            int batchEnd = Math.Min(batchStart + batchSize, rowCount);
            for (int i = batchStart; i < batchEnd; i++)
            {
                await collection.PutAsync(
                    $"doc:{i}",
                    new CollectionBenchDoc($"User_{i}", i, GetCategory(i)));
            }
            await db.CommitAsync();
        }

        return filePath;
    }

    public static void DeleteDatabaseFiles(string? filePath)
    {
        if (string.IsNullOrWhiteSpace(filePath))
            return;

        try { if (File.Exists(filePath)) File.Delete(filePath); } catch { }
        try { if (File.Exists(filePath + ".wal")) File.Delete(filePath + ".wal"); } catch { }
    }

    private static string GetCategory(int id)
        => (id % 4) switch
        {
            0 => "Alpha",
            1 => "Beta",
            2 => "Gamma",
            _ => "Delta",
        };

    private sealed record CollectionBenchDoc(string Name, int Value, string Category);
}
