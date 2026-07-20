using BenchmarkDotNet.Attributes;
using CSharpDB.Client;
using CSharpDB.Client.Models;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Isolates completion of already-active direct Client transaction sessions.
/// Transaction creation and mutation happen in iteration setup and are excluded
/// from the measured commit/rollback interval. Each transaction owns an
/// independent private in-memory database, so this diagnoses Client completion
/// coordination rather than shared-file or WAL writer contention.
/// </summary>
[BenchmarkCategory("Client", "Transactions", "Concurrency")]
[MemoryDiagnoser]
[MedianColumn]
[SimpleJob(launchCount: 3, warmupCount: 5, iterationCount: 25)]
public class ClientTransactionCompletionConcurrencyBenchmarks
{
    private ICSharpDbClient _client = null!;
    private List<string>? _transactionIds;
    private bool _iterationCompleted;
    private int _originalMinWorkerThreads;
    private int _originalMinCompletionPortThreads;

    [Params(2, 4, 8, 16)]
    public int TransactionCount { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        ThreadPool.GetMinThreads(
            out _originalMinWorkerThreads,
            out _originalMinCompletionPortThreads);
        if (_originalMinWorkerThreads < TransactionCount)
        {
            ThreadPool.SetMinThreads(
                TransactionCount,
                _originalMinCompletionPortThreads);
        }

        _client = CSharpDbClient.Create(new CSharpDbClientOptions
        {
            Transport = CSharpDbTransport.Direct,
            DataSource = ":memory:",
        });
    }

    [GlobalCleanup]
    public void GlobalCleanup()
        => GlobalCleanupAsync().GetAwaiter().GetResult();

    [IterationSetup]
    public void IterationSetup()
        => PrepareTransactionsAsync().GetAwaiter().GetResult();

    [IterationCleanup]
    public void IterationCleanup()
        => CleanupTransactionsAsync().GetAwaiter().GetResult();

    [Benchmark(Description = "Client transactions: sequential rollback completion")]
    public async Task CompleteRollbacksSequentiallyAsync()
    {
        foreach (string transactionId in GetTransactionIds())
            await _client.RollbackTransactionAsync(transactionId, CancellationToken.None);

        _iterationCompleted = true;
    }

    [Benchmark(Description = "Client transactions: concurrent rollback completion")]
    public async Task CompleteRollbacksConcurrentlyAsync()
    {
        await CompleteConcurrentlyAsync(commit: false);
        _iterationCompleted = true;
    }

    [Benchmark(Description = "Client transactions: sequential commit completion")]
    public async Task CompleteCommitsSequentiallyAsync()
    {
        foreach (string transactionId in GetTransactionIds())
            await _client.CommitTransactionAsync(transactionId, CancellationToken.None);

        _iterationCompleted = true;
    }

    [Benchmark(Description = "Client transactions: concurrent commit completion")]
    public async Task CompleteCommitsConcurrentlyAsync()
    {
        await CompleteConcurrentlyAsync(commit: true);
        _iterationCompleted = true;
    }

    private async Task PrepareTransactionsAsync()
    {
        _iterationCompleted = false;
        _transactionIds = new List<string>(TransactionCount);
        var uniqueTransactionIds = new HashSet<string>(StringComparer.Ordinal);

        try
        {
            for (int i = 0; i < TransactionCount; i++)
            {
                TransactionSessionInfo transaction =
                    await _client.BeginTransactionAsync(CancellationToken.None);
                string transactionId = transaction.TransactionId;
                if (string.IsNullOrWhiteSpace(transactionId))
                    throw new InvalidOperationException("The Client returned an empty transaction ID.");
                if (!uniqueTransactionIds.Add(transactionId))
                    throw new InvalidOperationException($"The Client returned duplicate transaction ID '{transactionId}'.");

                _transactionIds.Add(transactionId);

                SqlExecutionResult createResult = await _client.ExecuteInTransactionAsync(
                    transactionId,
                    "CREATE TABLE completion_work (id INTEGER PRIMARY KEY, value INTEGER);",
                    CancellationToken.None);
                EnsureStatementSucceeded(createResult, "CREATE TABLE");

                SqlExecutionResult insertResult = await _client.ExecuteInTransactionAsync(
                    transactionId,
                    $"INSERT INTO completion_work VALUES ({i + 1}, {(i + 1) * 10});",
                    CancellationToken.None);
                EnsureStatementSucceeded(insertResult, "INSERT", expectedRowsAffected: 1);
            }
        }
        catch
        {
            await CleanupTransactionsAsync();
            throw;
        }
    }

    private async Task CleanupTransactionsAsync()
    {
        if (_transactionIds is null)
            return;

        foreach (string transactionId in _transactionIds)
        {
            try
            {
                await _client.RollbackTransactionAsync(transactionId, CancellationToken.None);
                if (_iterationCompleted)
                    throw new InvalidOperationException($"Completed transaction '{transactionId}' remained active.");
            }
            catch (CSharpDbClientException ex) when (IsTransactionNotFound(ex, transactionId))
            {
                // Expected after a successful measured completion, and possible
                // after a partial completion when another operation failed.
            }
        }

        _transactionIds = null;
    }

    private async Task CompleteConcurrentlyAsync(bool commit)
    {
        IReadOnlyList<string> transactionIds = GetTransactionIds();
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Task[] completions = transactionIds
            .Select(transactionId => CompleteAfterStartAsync(transactionId, start.Task, commit))
            .ToArray();

        start.SetResult();
        await Task.WhenAll(completions);
    }

    private async Task CompleteAfterStartAsync(string transactionId, Task start, bool commit)
    {
        await start;
        if (commit)
            await _client.CommitTransactionAsync(transactionId, CancellationToken.None);
        else
            await _client.RollbackTransactionAsync(transactionId, CancellationToken.None);
    }

    private async Task GlobalCleanupAsync()
    {
        try
        {
            await CleanupTransactionsAsync();
        }
        finally
        {
            try
            {
                await _client.DisposeAsync();
            }
            finally
            {
                ThreadPool.SetMinThreads(
                    _originalMinWorkerThreads,
                    _originalMinCompletionPortThreads);
            }
        }
    }

    private static void EnsureStatementSucceeded(
        SqlExecutionResult result,
        string operation,
        int? expectedRowsAffected = null)
    {
        if (!string.IsNullOrWhiteSpace(result.Error) ||
            result.IsQuery ||
            (expectedRowsAffected.HasValue && result.RowsAffected != expectedRowsAffected.Value))
        {
            throw new InvalidOperationException(
                $"{operation} setup failed: error='{result.Error}', isQuery={result.IsQuery}, rowsAffected={result.RowsAffected}.");
        }
    }

    private static bool IsTransactionNotFound(CSharpDbClientException exception, string transactionId)
        => string.Equals(
            exception.Message,
            $"Transaction '{transactionId}' was not found.",
            StringComparison.Ordinal);

    private IReadOnlyList<string> GetTransactionIds()
        => _transactionIds
            ?? throw new InvalidOperationException("Transaction iteration setup did not run.");
}
