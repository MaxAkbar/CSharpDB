using System.Diagnostics;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Data;

namespace CSharpDB.Benchmarks.Macro;

public static class SharedMemoryAdoNetBenchmark
{
    private static int _nextId = 1_000_000;

    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        string connectionString = $"Data Source=:memory:macro-shared-{Guid.NewGuid():N}";
        var results = new List<BenchmarkResult>();

        await using var writer = new CSharpDbConnection(connectionString);
        await using var reader = new CSharpDbConnection(connectionString);
        await writer.OpenAsync();
        await reader.OpenAsync();

        using (var setup = writer.CreateCommand())
        {
            setup.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)";
            await setup.ExecuteNonQueryAsync();

            for (int i = 0; i < 5_000; i++)
            {
                setup.CommandText = $"INSERT INTO t VALUES ({i}, 'seed_{i}')";
                await setup.ExecuteNonQueryAsync();
            }
        }

        using var readCmd = (CSharpDbCommand)reader.CreateCommand();
        readCmd.CommandText = "SELECT name FROM t WHERE id = 2500";

        using var writeCmd = (CSharpDbCommand)writer.CreateCommand();
        writeCmd.CommandText = "INSERT INTO t VALUES (@id, @name)";
        var idParameter = writeCmd.Parameters.AddWithValue("@id", 0);
        var nameParameter = writeCmd.Parameters.AddWithValue("@name", "");

        var duration = TimeSpan.FromSeconds(10);
        using var cts = new CancellationTokenSource(duration);

        var readHistogram = new LatencyHistogram();
        var writeHistogram = new LatencyHistogram();

        Task writerTask = Task.Run(async () =>
        {
            while (!cts.IsCancellationRequested)
            {
                var sw = Stopwatch.StartNew();
                try
                {
                    await using var tx = await writer.BeginTransactionAsync(cts.Token);
                    for (int i = 0; i < 10; i++)
                    {
                        int id = Interlocked.Increment(ref _nextId);
                        idParameter.Value = id;
                        nameParameter.Value = $"shared_{id}";
                        await writeCmd.ExecuteNonQueryAsync(cts.Token);
                    }
                    await tx.CommitAsync(cts.Token);
                    sw.Stop();
                    writeHistogram.Record(sw.Elapsed.TotalMilliseconds);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        });

        Task readerTask = Task.Run(async () =>
        {
            while (!cts.IsCancellationRequested)
            {
                var sw = Stopwatch.StartNew();
                try
                {
                    await readCmd.ExecuteScalarAsync(cts.Token);
                    sw.Stop();
                    readHistogram.Record(sw.Elapsed.TotalMilliseconds);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        });

        await Task.WhenAll(writerTask, readerTask);

        results.Add(BenchmarkResult.FromHistogram(
            "AdoNet_NamedShared_Contention_Reads",
            readHistogram,
            duration.TotalMilliseconds));
        results.Add(BenchmarkResult.FromHistogram(
            "AdoNet_NamedShared_Contention_WriteTxBatch10",
            writeHistogram,
            duration.TotalMilliseconds));

        CSharpDbConnection.ClearAllPools();
        return results;
    }
}
