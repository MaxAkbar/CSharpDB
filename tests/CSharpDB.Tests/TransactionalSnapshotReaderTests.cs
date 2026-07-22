using CSharpDB.Client;
using CSharpDB.Client.Internal;
using CSharpDB.Client.Models;
using CSharpDbException = CSharpDB.Primitives.CSharpDbException;

namespace CSharpDB.Tests;

public sealed class TransactionalSnapshotReaderTests
{
    [Fact]
    public async Task ReadTableSnapshotAsync_SeesTransactionalMetadataAndReleasesGate()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string databasePath = CreateDatabasePath("metadata");

        try
        {
            await using var client = new EngineTransportClient(databasePath);
            var reader = Assert.IsAssignableFrom<ICSharpDbTransactionalSnapshotReader>(client);
            TransactionSessionInfo transaction = await client.BeginTransactionAsync(ct);

            await ExecuteCheckedAsync(
                client,
                transaction.TransactionId,
                "CREATE TABLE snapshot_items (id INTEGER PRIMARY KEY IDENTITY, name TEXT NOT NULL);",
                ct);
            await ExecuteCheckedAsync(
                client,
                transaction.TransactionId,
                "CREATE INDEX ix_snapshot_items_name ON snapshot_items (name);",
                ct);
            await ExecuteCheckedAsync(
                client,
                transaction.TransactionId,
                "INSERT INTO snapshot_items (name) VALUES ('first');",
                ct);

            Assert.Null(await reader.ReadTableSnapshotAsync(
                transaction.TransactionId,
                "missing_items",
                ct));

            // The missing-table early return must not retain the transaction gate.
            await ExecuteCheckedAsync(
                client,
                transaction.TransactionId,
                "INSERT INTO snapshot_items (name) VALUES ('after-missing');",
                ct);

            TransactionTableSnapshot snapshot = Assert.IsType<TransactionTableSnapshot>(
                await reader.ReadTableSnapshotAsync(
                    transaction.TransactionId,
                    "snapshot_items",
                    ct));

            Assert.Equal("snapshot_items", snapshot.Schema.TableName);
            Assert.Equal(3, snapshot.Schema.NextRowId);
            Assert.Collection(
                snapshot.Schema.Columns,
                id =>
                {
                    Assert.Equal("id", id.Name);
                    Assert.Equal(CSharpDB.Client.Models.DbType.Integer, id.Type);
                    Assert.True(id.IsPrimaryKey);
                    Assert.True(id.IsIdentity);
                },
                name =>
                {
                    Assert.Equal("name", name.Name);
                    Assert.Equal(CSharpDB.Client.Models.DbType.Text, name.Type);
                    Assert.False(name.Nullable);
                });

            IndexSchema index = Assert.Single(snapshot.Indexes);
            Assert.Equal("ix_snapshot_items_name", index.IndexName);
            Assert.Equal("snapshot_items", index.TableName);
            Assert.Equal(new[] { "name" }, index.Columns);

            // The populated-snapshot return must also release the gate.
            await ExecuteCheckedAsync(
                client,
                transaction.TransactionId,
                "INSERT INTO snapshot_items (name) VALUES ('after-snapshot');",
                ct);
            await client.RollbackTransactionAsync(transaction.TransactionId, ct);
        }
        finally
        {
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task TransactionalCursor_HoldsGateUntilDisposedThenCommitCompletes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string databasePath = CreateDatabasePath("cursor_commit");

        try
        {
            await using var client = new EngineTransportClient(databasePath);
            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE TABLE cursor_items (id INTEGER PRIMARY KEY, name TEXT NOT NULL);",
                ct)).Error);

            TransactionSessionInfo transaction = await client.BeginTransactionAsync(ct);
            await ExecuteCheckedAsync(
                client,
                transaction.TransactionId,
                "INSERT INTO cursor_items VALUES (1, 'held');",
                ct);

            var reader = Assert.IsAssignableFrom<ICSharpDbTransactionalSnapshotReader>(client);
            ForwardOnlyQueryCursor cursor = Assert.IsType<ForwardOnlyQueryCursor>(
                await reader.TryOpenForwardOnlyQueryCursorAsync(
                    transaction.TransactionId,
                    "SELECT id, name FROM cursor_items ORDER BY id;",
                    ct));

            Task commitTask = Task.CompletedTask;
            try
            {
                List<object?[]> rows = await cursor.ReadNextAsync(1, ct);
                Assert.Equal(1L, Assert.Single(rows)[0]);

                commitTask = client.CommitTransactionAsync(
                    transaction.TransactionId,
                    CancellationToken.None);
                await Task.Delay(50, ct);
                Assert.False(commitTask.IsCompleted);
            }
            finally
            {
                await cursor.DisposeAsync();
            }

            await commitTask.WaitAsync(TimeSpan.FromSeconds(10), ct);
            Assert.Equal(1, await client.GetRowCountAsync("cursor_items", ct));
        }
        finally
        {
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task TransactionalCursor_NonQueryAndOpenFailureReleaseGate()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string databasePath = CreateDatabasePath("cursor_failures");

        try
        {
            await using var client = new EngineTransportClient(databasePath);
            TransactionSessionInfo transaction = await client.BeginTransactionAsync(ct);
            await ExecuteCheckedAsync(
                client,
                transaction.TransactionId,
                "CREATE TABLE gate_items (id INTEGER PRIMARY KEY);",
                ct);

            var reader = Assert.IsAssignableFrom<ICSharpDbTransactionalSnapshotReader>(client);
            Assert.Null(await reader.TryOpenForwardOnlyQueryCursorAsync(
                transaction.TransactionId,
                "INSERT INTO gate_items VALUES (1);",
                ct));

            // A non-query result is disposed before null is returned, and must release the gate.
            await ExecuteCheckedAsync(
                client,
                transaction.TransactionId,
                "INSERT INTO gate_items VALUES (2);",
                ct);

            await Assert.ThrowsAsync<CSharpDbException>(async () =>
                await reader.TryOpenForwardOnlyQueryCursorAsync(
                    transaction.TransactionId,
                    "SELECT FROM gate_items;",
                    ct));

            // Query planning/parsing failures must release the gate as well.
            await ExecuteCheckedAsync(
                client,
                transaction.TransactionId,
                "INSERT INTO gate_items VALUES (3);",
                ct);
            await client.RollbackTransactionAsync(transaction.TransactionId, ct);
        }
        finally
        {
            DeleteDatabaseFiles(databasePath);
        }
    }

    private static async Task ExecuteCheckedAsync(
        EngineTransportClient client,
        string transactionId,
        string sql,
        CancellationToken ct)
    {
        SqlExecutionResult result = await client.ExecuteInTransactionAsync(
            transactionId,
            sql,
            ct);
        Assert.Null(result.Error);
    }

    private static string CreateDatabasePath(string scenario)
        => Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_transactional_snapshot_{scenario}_{Guid.NewGuid():N}.db");

    private static void DeleteDatabaseFiles(string path)
    {
        DeleteIfExists(path);
        DeleteIfExists(path + ".wal");
        DeleteIfExists(path + ".lock");
    }

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }
}
