using System.Data.Common;
using CSharpDB.Client;
using CSharpDB.Testing;

namespace CSharpDB.Data.Tests;

[Collection("ConnectionPoolState")]
public sealed class ConcurrentNetworkTransactionTests : IDisposable
{
    private readonly CancellationTokenSource _deadline =
        CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
    private CancellationToken Ct => _deadline.Token;
    public ConcurrentNetworkTransactionTests() => _deadline.CancelAfter(TimeSpan.FromSeconds(60));
    public void Dispose() => _deadline.Dispose();

    [Theory]
    [InlineData(CSharpDbGrpcMode.Auto, false)]
    [InlineData(CSharpDbGrpcMode.Auto, true)]
    [InlineData(CSharpDbGrpcMode.Native, false)]
    [InlineData(CSharpDbGrpcMode.Native, true)]
    public async Task ConflictingTransactions_ReportConflict_AndNeverReplayTheRejectedUpdate(
        CSharpDbGrpcMode mode, bool ado)
    {
        await using var host = new GrpcSocketDaemonFixture();
        string endpoint = host.EndpointFor(mode).AbsoluteUri;
        var options = new CSharpDbClientOptions
        { Transport = CSharpDbTransport.Grpc, GrpcMode = mode, Endpoint = endpoint };
        await using var observer = host.CreateClient(mode);
        Assert.Null((await observer.ExecuteSqlAsync(
            "CREATE TABLE rejected_tx (id INTEGER PRIMARY KEY, value INTEGER); INSERT INTO rejected_tx VALUES (1, 0);", Ct)).Error);
        try
        {
            if (ado)
            {
                string cs = $"Endpoint={endpoint};Transport=Grpc;Grpc Mode={mode};Pooling=true;Max Pool Size=2";
                await using var first = new CSharpDbConnection(cs);
                await using var second = new CSharpDbConnection(cs);
                await first.OpenAsync(Ct);
                await second.OpenAsync(Ct);
                await using var firstTx = await first.BeginTransactionAsync(Ct);
                await using var secondTx = await second.BeginTransactionAsync(Ct);
                using var command = first.CreateCommand();
                command.Transaction = firstTx;
                command.CommandText = "UPDATE rejected_tx SET value = 1 WHERE id = 1;";
                await command.ExecuteNonQueryAsync(Ct);
                using var other = second.CreateCommand();
                other.Transaction = secondTx;
                other.CommandText = "UPDATE rejected_tx SET value = 2 WHERE id = 1;";
                await other.ExecuteNonQueryAsync(Ct);
                await firstTx.CommitAsync(Ct);
                var error = await Assert.ThrowsAsync<CSharpDbDataException>(() => secondTx.CommitAsync(Ct));
                Assert.Equal(CSharpDB.Primitives.ErrorCode.TransactionConflict, error.ErrorCode);
            }
            else
            {
                await using var first = await CSharpDbClient.RentAsync(options, 2, Ct);
                await using var second = await CSharpDbClient.RentAsync(options, 2, Ct);
                string firstTx = (await first.BeginTransactionAsync(Ct)).TransactionId;
                string secondTx = (await second.BeginTransactionAsync(Ct)).TransactionId;
                await first.ExecuteInTransactionAsync(firstTx, "UPDATE rejected_tx SET value = 1 WHERE id = 1;", Ct);
                await second.ExecuteInTransactionAsync(secondTx, "UPDATE rejected_tx SET value = 2 WHERE id = 1;", Ct);
                await first.CommitTransactionAsync(firstTx, Ct);
                var error = await Assert.ThrowsAsync<CSharpDB.Primitives.CSharpDbException>(() => second.CommitTransactionAsync(secondTx, Ct));
                Assert.Equal(CSharpDB.Primitives.ErrorCode.TransactionConflict, error.Code);
            }
            await using var recovered = await CSharpDbClient.RentAsync(options, 2, Ct);
            Assert.Equal(1L, (await recovered.ExecuteSqlAsync("SELECT value FROM rejected_tx WHERE id = 1;", Ct)).Rows![0][0]);
            Assert.Null((await recovered.ExecuteSqlAsync("INSERT INTO rejected_tx VALUES (2, 3);", Ct)).Error);
            Assert.Equal(2L, (await observer.ExecuteSqlAsync("SELECT COUNT(*) FROM rejected_tx;", Ct)).Rows![0][0]);
        }
        finally { await CSharpDbClient.ClearPoolAsync(endpoint); }
    }

    [Theory]
    [InlineData(CSharpDbGrpcMode.Auto, false)]
    [InlineData(CSharpDbGrpcMode.Auto, true)]
    [InlineData(CSharpDbGrpcMode.Native, false)]
    [InlineData(CSharpDbGrpcMode.Native, true)]
    public async Task EightPooledTransactions_CommitRollbackAndAbandon_WithoutClosingSharedWal(
        CSharpDbGrpcMode mode, bool ado)
    {
        await using var host = new GrpcSocketDaemonFixture();
        string endpoint = host.EndpointFor(mode).AbsoluteUri;
        var options = new CSharpDbClientOptions
        { Transport = CSharpDbTransport.Grpc, GrpcMode = mode, Endpoint = endpoint };
        string cs = $"Endpoint={endpoint};Transport=Grpc;Grpc Mode={mode};Pooling=true;Max Pool Size=8";
        await using var observer = host.CreateClient(mode);
        Assert.Null((await observer.ExecuteSqlAsync("CREATE TABLE concurrent_tx (id INTEGER PRIMARY KEY);", Ct)).Error);
        try
        {
            for (int round = 0; round < 3; round++)
            {
                TestContext.Current.TestOutputHelper?.WriteLine($"{mode}, ADO={ado}: starting round {round}");
                var clients = new List<ICSharpDbClient>();
                var ids = new List<string>();
                var connections = new List<CSharpDbConnection>();
                var transactions = new List<DbTransaction>();
                try
                {
                    for (int i = 0; i < 8; i++)
                    {
                        string sql = $"INSERT INTO concurrent_tx VALUES ({round * 8 + i});";
                        if (ado)
                        {
                            var connection = new CSharpDbConnection(cs);
                            connections.Add(connection);
                            await connection.OpenAsync(Ct);
                            DbTransaction transaction = await connection.BeginTransactionAsync(Ct);
                            transactions.Add(transaction);
                            using var command = connection.CreateCommand();
                            command.Transaction = transaction;
                            command.CommandText = sql;
                            Assert.Equal(1, await command.ExecuteNonQueryAsync(Ct));
                        }
                        else
                        {
                            ICSharpDbClient client = await CSharpDbClient.RentAsync(options, 8, Ct);
                            clients.Add(client);
                            string id = (await client.BeginTransactionAsync(Ct)).TransactionId;
                            ids.Add(id);
                            Assert.Null((await client.ExecuteInTransactionAsync(id, sql, Ct)).Error);
                        }
                    }
                    Assert.Equal(round * 4L,
                        (await observer.ExecuteSqlAsync("SELECT COUNT(*) FROM concurrent_tx;", Ct)).Rows![0][0]);
                    TestContext.Current.TestOutputHelper?.WriteLine($"{mode}, ADO={ado}: all eight transactions open; finishing round {round}");

                    // Finish concurrently while the remaining sessions still pin
                    // their snapshots. Odd sessions roll back; the last is left
                    // unfinished and must be reset when its rental is returned.
                    await Task.WhenAll(Enumerable.Range(0, 7).Select(i => ado
                        ? (i % 2 == 0 ? transactions[i].CommitAsync(Ct) : transactions[i].RollbackAsync(Ct))
                        : (i % 2 == 0 ? clients[i].CommitTransactionAsync(ids[i], Ct)
                                      : clients[i].RollbackTransactionAsync(ids[i], Ct))));
                }
                finally
                {
                    foreach (DbTransaction transaction in transactions)
                        await transaction.DisposeAsync();
                    foreach (CSharpDbConnection connection in connections)
                        await connection.DisposeAsync();
                    foreach (ICSharpDbClient client in clients)
                        await client.DisposeAsync();
                }

                Assert.Equal((round + 1) * 4L,
                    (await observer.ExecuteSqlAsync("SELECT COUNT(*) FROM concurrent_tx;", Ct)).Rows![0][0]);
                TestContext.Current.TestOutputHelper?.WriteLine($"{mode}, ADO={ado}: round {round} verified");
            }
            Assert.Equal(Enumerable.Range(0, 24).Where(i => i % 2 == 0).Select(i => (long)i),
                (await observer.ExecuteSqlAsync("SELECT id FROM concurrent_tx ORDER BY id;", Ct)).Rows!.Select(row => (long)row[0]!));
        }
        finally { await CSharpDbClient.ClearPoolAsync(endpoint); }
    }
}
