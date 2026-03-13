using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Primitives;
using CSharpDB.Engine;

namespace CSharpDB.Benchmarks.Macro;

public static class InMemoryBatchBenchmark
{
    private const int BatchSize = 100;
    private const int ResetAfterRows = 100_000;

    private sealed record BenchDoc(string Name, int Value, string Category);

    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        var results = new List<BenchmarkResult>();

        await using (var sqlRunner = await RotatingSqlBatchRunner.CreateAsync())
        {
            results.Add(await MacroBenchmarkRunner.RunForDurationAsync(
                "InMemory_Sql_Batch100_Rotating_10s",
                warmupDuration: TimeSpan.FromSeconds(2),
                measuredDuration: TimeSpan.FromSeconds(10),
                sqlRunner.ExecuteBatchAsync));
        }

        await using (var collectionRunner = await RotatingCollectionBatchRunner.CreateAsync())
        {
            results.Add(await MacroBenchmarkRunner.RunForDurationAsync(
                "InMemory_Collection_Batch100_Rotating_10s",
                warmupDuration: TimeSpan.FromSeconds(2),
                measuredDuration: TimeSpan.FromSeconds(10),
                collectionRunner.ExecuteBatchAsync));
        }

        return results;
    }

    private sealed class RotatingSqlBatchRunner : IAsyncDisposable
    {
        private Database _db = null!;
        private InsertBatch _batch = null!;
        private int _nextId = 2_000_000;
        private int _rowsInCurrentDatabase;

        private RotatingSqlBatchRunner()
        {
        }

        public static async Task<RotatingSqlBatchRunner> CreateAsync()
        {
            var runner = new RotatingSqlBatchRunner();
            await runner.ResetAsync();
            return runner;
        }

        public async Task ExecuteBatchAsync()
        {
            if (_rowsInCurrentDatabase + BatchSize > ResetAfterRows)
                await ResetAsync();

            _batch.Clear();
            await _db.BeginTransactionAsync();
            for (int i = 0; i < BatchSize; i++)
            {
                int id = ++_nextId;
                var row = new DbValue[4];
                row[0] = DbValue.FromInteger(id);
                row[1] = DbValue.FromInteger(id * 10L);
                row[2] = DbValue.FromText("memory_batch");
                row[3] = DbValue.FromText("Alpha");
                _batch.AddRow(row);
            }

            await _batch.ExecuteAsync();
            await _db.CommitAsync();
            _rowsInCurrentDatabase += BatchSize;
        }

        public async ValueTask DisposeAsync()
        {
            if (_db != null)
                await _db.DisposeAsync();
        }

        private async Task ResetAsync()
        {
            if (_db != null)
                await _db.DisposeAsync();

            _db = await Database.OpenInMemoryAsync();
            await _db.ExecuteAsync("CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER, text_col TEXT, category TEXT)");
            _batch = _db.PrepareInsertBatch("bench", BatchSize);
            _rowsInCurrentDatabase = 0;
        }
    }

    private sealed class RotatingCollectionBatchRunner : IAsyncDisposable
    {
        private Database _db = null!;
        private Collection<BenchDoc> _collection = null!;
        private int _nextId = 2_000_000;
        private int _rowsInCurrentDatabase;

        private RotatingCollectionBatchRunner()
        {
        }

        public static async Task<RotatingCollectionBatchRunner> CreateAsync()
        {
            var runner = new RotatingCollectionBatchRunner();
            await runner.ResetAsync();
            return runner;
        }

        public async Task ExecuteBatchAsync()
        {
            if (_rowsInCurrentDatabase + BatchSize > ResetAfterRows)
                await ResetAsync();

            await _db.BeginTransactionAsync();
            for (int i = 0; i < BatchSize; i++)
            {
                int id = ++_nextId;
                await _collection.PutAsync($"doc:{id}", new BenchDoc($"User_{id}", id, "Alpha"));
            }

            await _db.CommitAsync();
            _rowsInCurrentDatabase += BatchSize;
        }

        public async ValueTask DisposeAsync()
        {
            if (_db != null)
                await _db.DisposeAsync();
        }

        private async Task ResetAsync()
        {
            if (_db != null)
                await _db.DisposeAsync();

            _db = await Database.OpenInMemoryAsync();
            _collection = await _db.GetCollectionAsync<BenchDoc>("bench_docs");
            _rowsInCurrentDatabase = 0;
        }
    }
}
