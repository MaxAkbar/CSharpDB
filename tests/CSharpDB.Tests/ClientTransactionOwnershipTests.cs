using CSharpDB.Client.Internal;
using CSharpDB.Engine;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class ClientTransactionOwnershipTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task CanceledCompletionDuringDisposal_WaitsForTheInFlightOperation()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        var options = new DatabaseOptions().ConfigureFunctions(functions => functions.AddScalar(
            "HoldTransaction", 0, new DbScalarFunctionOptions(DbType.Integer), (_, _) =>
            {
                entered.TrySetResult();
                if (!release.Wait(TimeSpan.FromSeconds(10), Ct))
                    throw new TimeoutException("Transaction operation was not released.");
                return DbValue.FromInteger(1);
            }));
        var client = EngineTransportClient.CreatePrivateMemory(":memory:dispose-cancel", null, options);
        try
        {
            string id = (await client.BeginTransactionAsync(Ct)).TransactionId;
            Task query = Task.Run(() => client.ExecuteInTransactionAsync(id, "SELECT HoldTransaction();", Ct), Ct);
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(10), Ct);
            using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            Task commit = client.CommitTransactionAsync(id, cancellation.Token);
            Task dispose = client.DisposeAsync().AsTask();
            cancellation.Cancel();
            Assert.False(commit.IsCompleted);
            Assert.False(dispose.IsCompleted);
            release.Set();
            await query;
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => commit);
            await dispose;
        }
        finally
        {
            release.Set();
            await client.DisposeAsync();
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task EightOverlappingTransactions_ShareOneOwner_AndSurviveOtherSessionsCompleting(bool hybrid)
    {
        string path = Path.Combine(Path.GetTempPath(), $"csharpdb_shared_transactions_{Guid.NewGuid():N}.db");
        int opens = 0;
        try
        {
            await using (var client = new EngineTransportClient(path, async (file, ct) =>
            {
                Interlocked.Increment(ref opens);
                return hybrid ? await Database.OpenHybridAsync(file, ct) : await Database.OpenAsync(file, ct);
            }))
            {
                Assert.Null((await client.ExecuteSqlAsync("CREATE TABLE ownership (id INTEGER PRIMARY KEY);", Ct)).Error);
                var transactions = new List<string>();
                for (int i = 0; i < 8; i++)
                    transactions.Add((await client.BeginTransactionAsync(Ct)).TransactionId);

                for (int i = 0; i < transactions.Count; i++)
                    await client.ExecuteInTransactionAsync(transactions[i], $"INSERT INTO ownership VALUES ({i});", Ct);

                // All sessions remain open when the first commit finishes. Closing
                // an independently opened database here used to delete their WAL.
                foreach (string transaction in transactions)
                    await client.CommitTransactionAsync(transaction, Ct);

                Assert.Equal(1, opens);
                Assert.Equal(8, await client.GetRowCountAsync("ownership", Ct));
            }

            await using var reopened = await Database.OpenAsync(path, Ct);
            await using var result = await reopened.ExecuteAsync("SELECT id FROM ownership ORDER BY id;", Ct);
            Assert.Equal(Enumerable.Range(0, 8).Select(i => (long)i),
                (await result.ToListAsync(Ct)).Select(row => row[0].AsInteger));
        }
        finally
        {
            foreach (string file in new[] { path, path + ".wal" })
            {
                try { File.Delete(file); }
                catch (IOException) { } // Preserve the original ownership failure.
            }
        }
    }

    [Fact]
    public async Task SnapshotHybrid_RejectsOverlappingSessions_AndPersistsCompletedTransaction()
    {
        string path = Path.Combine(Path.GetTempPath(), $"csharpdb_snapshot_transaction_{Guid.NewGuid():N}.db");
        try
        {
            await using (var client = new EngineTransportClient(path, hybridDatabaseOptions: new HybridDatabaseOptions
            { PersistenceMode = HybridPersistenceMode.Snapshot, PersistenceTriggers = HybridPersistenceTriggers.Dispose }))
            {
                string id = (await client.BeginTransactionAsync(Ct)).TransactionId;
                var busy = await Assert.ThrowsAsync<CSharpDbException>(() => client.BeginTransactionAsync(Ct));
                Assert.Equal(ErrorCode.Busy, busy.Code);
                await Assert.ThrowsAsync<CSharpDbException>(() => client.GetTableNamesAsync(Ct));
                await client.ExecuteInTransactionAsync(id, "CREATE TABLE snapshot_owner (id INTEGER PRIMARY KEY);", Ct);
                await client.ExecuteInTransactionAsync(id, "INSERT INTO snapshot_owner VALUES (1);", Ct);
                await client.CommitTransactionAsync(id, Ct);
                Assert.Equal(1, await client.GetRowCountAsync("snapshot_owner", Ct));
            }
            await using var reopened = await Database.OpenAsync(path, Ct);
            await using var result = await reopened.ExecuteAsync("SELECT id FROM snapshot_owner;", Ct);
            Assert.Equal(1L, Assert.Single(await result.ToListAsync(Ct))[0].AsInteger);
        }
        finally
        {
            File.Delete(path);
            File.Delete(path + ".wal");
        }
    }

    [Fact]
    public async Task OverlappingUpdates_ReturnConflict_WithoutLosingCommittedState()
    {
        await using var client = EngineTransportClient.CreatePrivateMemory(":memory:conflicts", null, null);
        Assert.Null((await client.ExecuteSqlAsync(
            "CREATE TABLE conflicts (id INTEGER PRIMARY KEY, value INTEGER); INSERT INTO conflicts VALUES (1, 0);", Ct)).Error);
        string first = (await client.BeginTransactionAsync(Ct)).TransactionId;
        string second = (await client.BeginTransactionAsync(Ct)).TransactionId;
        await client.ExecuteInTransactionAsync(first, "UPDATE conflicts SET value = 1 WHERE id = 1;", Ct);
        await client.ExecuteInTransactionAsync(second, "UPDATE conflicts SET value = 2 WHERE id = 1;", Ct);
        await client.CommitTransactionAsync(first, Ct);
        var conflict = await Assert.ThrowsAnyAsync<CSharpDbException>(() => client.CommitTransactionAsync(second, Ct));
        Assert.Equal(ErrorCode.TransactionConflict, conflict.Code);
        Assert.Equal(1L, (await client.ExecuteSqlAsync("SELECT value FROM conflicts WHERE id = 1;", Ct)).Rows![0][0]);

        string next = (await client.BeginTransactionAsync(Ct)).TransactionId;
        await client.ExecuteInTransactionAsync(next, "UPDATE conflicts SET value = 3 WHERE id = 1;", Ct);
        await client.CommitTransactionAsync(next, Ct);
        Assert.Equal(3L, (await client.ExecuteSqlAsync("SELECT value FROM conflicts WHERE id = 1;", Ct)).Rows![0][0]);
    }

    [Fact]
    public async Task TemporaryTables_StayIsolated_WhenAnotherTransactionCompletes()
    {
        await using var client = EngineTransportClient.CreatePrivateMemory(":memory:temporary", null, null);
        string first = (await client.BeginTransactionAsync(Ct)).TransactionId;
        string second = (await client.BeginTransactionAsync(Ct)).TransactionId;
        foreach (string transaction in new[] { first, second })
            await client.ExecuteInTransactionAsync(transaction, "CREATE TEMP TABLE isolated (id INTEGER);", Ct);
        await client.ExecuteInTransactionAsync(first, "INSERT INTO isolated VALUES (1);", Ct);
        await client.ExecuteInTransactionAsync(second, "INSERT INTO isolated VALUES (2);", Ct);
        await client.CommitTransactionAsync(first, Ct);
        // Exercise deferred cursor execution, which must retain the session scope.
        await using (var cursor = await client.TryOpenForwardOnlyQueryCursorAsync(second, "SELECT id FROM isolated;", Ct))
        {
            Assert.NotNull(cursor);
            Assert.Equal(2L, Assert.Single(await cursor.ReadNextAsync(10, Ct))[0]);
        }
        await client.RollbackTransactionAsync(second, Ct);
        string third = (await client.BeginTransactionAsync(Ct)).TransactionId;
        await Assert.ThrowsAsync<CSharpDbException>(() => client.ExecuteInTransactionAsync(third, "SELECT * FROM isolated;", Ct));
        await client.RollbackTransactionAsync(third, Ct);
    }
}
