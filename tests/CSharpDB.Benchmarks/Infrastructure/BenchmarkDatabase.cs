using CSharpDB.Engine;

namespace CSharpDB.Benchmarks.Infrastructure;

/// <summary>
/// Helper to create, seed, and dispose temporary databases for benchmarks.
/// </summary>
public sealed class BenchmarkDatabase : IAsyncDisposable, IDisposable
{
    private readonly string _filePath;
    private readonly DatabaseOptions _options;
    private Database? _db;

    public string FilePath => _filePath;
    public Database Db => _db ?? throw new InvalidOperationException("Database not open.");

    private BenchmarkDatabase(string filePath, Database db, DatabaseOptions options)
    {
        _filePath = filePath;
        _options = options;
        _db = db;
    }

    /// <summary>
    /// Create a new temporary database with optional pre-seeded rows in a default table.
    /// Default schema: bench(id INTEGER PRIMARY KEY, value INTEGER, text_col TEXT, category TEXT)
    /// </summary>
    public static async Task<BenchmarkDatabase> CreateAsync(int? seedRowCount = null, DatabaseOptions? options = null)
    {
        var filePath = Path.Combine(Path.GetTempPath(), $"csharpdb_bench_{Guid.NewGuid():N}.db");
        var resolvedOptions = BenchmarkDurability.Apply(options);
        var db = await Database.OpenAsync(filePath, resolvedOptions);

        var bench = new BenchmarkDatabase(filePath, db, resolvedOptions);

        await db.ExecuteAsync(
            "CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER, text_col TEXT, category TEXT)");

        if (seedRowCount.HasValue && seedRowCount.Value > 0)
        {
            await bench.SeedDefaultTableAsync(seedRowCount.Value);
        }

        return bench;
    }

    /// <summary>
    /// Create a new temporary database with a custom schema.
    /// </summary>
    public static async Task<BenchmarkDatabase> CreateWithSchemaAsync(string createTableSql, DatabaseOptions? options = null)
    {
        var filePath = Path.Combine(Path.GetTempPath(), $"csharpdb_bench_{Guid.NewGuid():N}.db");
        var resolvedOptions = BenchmarkDurability.Apply(options);
        var db = await Database.OpenAsync(filePath, resolvedOptions);
        var bench = new BenchmarkDatabase(filePath, db, resolvedOptions);
        await db.ExecuteAsync(createTableSql);
        return bench;
    }

    /// <summary>
    /// Seed rows into a table using a factory function that generates INSERT SQL per row.
    /// Rows are inserted in batches of 500 within transactions for performance.
    /// </summary>
    public async Task SeedAsync(string tableName, int rowCount, Func<int, string> insertSqlFactory)
    {
        const int batchSize = 500;
        for (int i = 0; i < rowCount; i += batchSize)
        {
            await Db.BeginTransactionAsync();
            int end = Math.Min(i + batchSize, rowCount);
            for (int j = i; j < end; j++)
            {
                await Db.ExecuteAsync(insertSqlFactory(j));
            }
            await Db.CommitAsync();
        }
    }

    /// <summary>
    /// Seed the default bench table with deterministic data.
    /// </summary>
    private async Task SeedDefaultTableAsync(int rowCount)
    {
        var categories = new[] { "Alpha", "Beta", "Gamma", "Delta", "Epsilon" };
        var rng = new Random(42); // deterministic

        await SeedAsync("bench", rowCount, id =>
        {
            var cat = categories[id % categories.Length];
            var text = DataGenerator.RandomString(rng, 50);
            return $"INSERT INTO bench VALUES ({id}, {rng.Next(0, 1_000_000)}, '{text}', '{cat}')";
        });
    }

    /// <summary>
    /// Close the current database and reopen the same file.
    /// Useful for testing persistence / WAL recovery.
    /// </summary>
    public async Task<Database> ReopenAsync()
    {
        if (_db != null)
            await _db.DisposeAsync();
        _db = await Database.OpenAsync(_filePath, _options);
        return _db;
    }

    public async ValueTask DisposeAsync()
    {
        if (_db != null)
        {
            await _db.DisposeAsync();
            _db = null;
        }
        CleanupFiles();
    }

    public void Dispose()
    {
        if (_db != null)
        {
            _db.DisposeAsync().AsTask().GetAwaiter().GetResult();
            _db = null;
        }
        CleanupFiles();
    }

    private void CleanupFiles()
    {
        try { if (File.Exists(_filePath)) File.Delete(_filePath); } catch { }
        try { if (File.Exists(_filePath + ".wal")) File.Delete(_filePath + ".wal"); } catch { }
    }
}
