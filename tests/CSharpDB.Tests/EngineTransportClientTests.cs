using System.Text;
using System.Threading;
using CSharpDB.Client;
using CSharpDB.Client.Internal;
using CSharpDB.Client.Models;
using CSharpDB.Engine;
using CSharpDB.ImportExport.TableArchives;
using PrimitiveDbType = CSharpDB.Primitives.DbType;
using PrimitiveDbValue = CSharpDB.Primitives.DbValue;
using PrimitiveScalarFunctionOptions = CSharpDB.Primitives.DbScalarFunctionOptions;

namespace CSharpDB.Tests;

public sealed class EngineTransportClientTests
{
    [Fact]
    public async Task GetTableSchemaAsync_MapsRowVersionMetadata()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_engine_transport_rowversion_{Guid.NewGuid():N}.db");

        try
        {
            await using var client = new EngineTransportClient(dbPath);
            SqlExecutionResult create = await client.ExecuteSqlAsync(
                "CREATE TABLE transport_versions (id INTEGER PRIMARY KEY, version BLOB ROWVERSION NOT NULL);",
                TestContext.Current.CancellationToken);
            Assert.Null(create.Error);

            CSharpDB.Client.Models.TableSchema schema = Assert.IsType<CSharpDB.Client.Models.TableSchema>(
                await client.GetTableSchemaAsync(
                    "transport_versions",
                    TestContext.Current.CancellationToken));
            ColumnDefinition version = Assert.Single(schema.Columns, column => column.Name == "version");

            Assert.Equal(CSharpDB.Client.Models.DbType.Blob, version.Type);
            Assert.False(version.Nullable);
            Assert.True(version.IsRowVersion);
        }
        finally
        {
            if (File.Exists(dbPath))
                File.Delete(dbPath);
            if (File.Exists(dbPath + ".wal"))
                File.Delete(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task InsertRowAsync_EmptyValuesGeneratesRowVersionDefault()
    {
        string dbPath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_engine_transport_rowversion_insert_{Guid.NewGuid():N}.db");

        try
        {
            await using var client = new EngineTransportClient(dbPath);
            SqlExecutionResult create = await client.ExecuteSqlAsync(
                "CREATE TABLE generated_rows (version BLOB ROWVERSION NOT NULL);",
                TestContext.Current.CancellationToken);
            Assert.Null(create.Error);

            int inserted = await client.InsertRowAsync(
                "generated_rows",
                new Dictionary<string, object?>(),
                TestContext.Current.CancellationToken);
            Assert.Equal(1, inserted);

            SqlExecutionResult query = await client.ExecuteSqlAsync(
                "SELECT version FROM generated_rows",
                TestContext.Current.CancellationToken);
            object?[] row = Assert.Single(query.Rows!);
            Assert.Equal(
                new byte[] { 0, 0, 0, 0, 0, 0, 0, 1 },
                Assert.IsType<byte[]>(row[0]));
        }
        finally
        {
            if (File.Exists(dbPath))
                File.Delete(dbPath);
            if (File.Exists(dbPath + ".wal"))
                File.Delete(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task GetTableSchemaAsync_MapsDefaultsChecksAndLogicalKeys()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_engine_transport_schema_{Guid.NewGuid():N}.db");

        try
        {
            await using var client = new EngineTransportClient(dbPath);
            SqlExecutionResult create = await client.ExecuteSqlAsync(
                """
                CREATE TABLE transport_schema (
                    id INTEGER PRIMARY KEY,
                    tenant TEXT NOT NULL,
                    code TEXT DEFAULT 'new',
                    score INTEGER,
                    CONSTRAINT ck_transport_score CHECK (score >= 0),
                    CONSTRAINT uq_transport_tenant_code UNIQUE (tenant, code)
                );
                """,
                TestContext.Current.CancellationToken);
            Assert.Null(create.Error);

            CSharpDB.Client.Models.TableSchema? schema = await client.GetTableSchemaAsync(
                "transport_schema",
                TestContext.Current.CancellationToken);

            Assert.NotNull(schema);
            Assert.Equal("'new'", Assert.Single(schema!.Columns, column => column.Name == "code").DefaultSql);
            CheckConstraintDefinition check = Assert.Single(schema.CheckConstraints);
            Assert.Equal("ck_transport_score", check.ConstraintName);
            Assert.Contains("score", check.ExpressionSql, StringComparison.OrdinalIgnoreCase);
            KeyConstraintDefinition unique = Assert.Single(
                schema.KeyConstraints,
                key => key.Kind == KeyConstraintKind.Unique);
            Assert.Equal("uq_transport_tenant_code", unique.ConstraintName);
            Assert.Equal(["tenant", "code"], unique.Columns);
        }
        finally
        {
            if (File.Exists(dbPath))
                File.Delete(dbPath);
            if (File.Exists(dbPath + ".wal"))
                File.Delete(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task GetTableSchemaAsync_MapsOrderedCompositeForeignKeyColumns()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_engine_transport_composite_fk_{Guid.NewGuid():N}.db");

        try
        {
            await using var client = new EngineTransportClient(dbPath);
            SqlExecutionResult create = await client.ExecuteSqlAsync(
                """
                CREATE TABLE transport_parents (
                    tenant_id INTEGER,
                    code TEXT,
                    PRIMARY KEY (tenant_id, code)
                );
                CREATE TABLE transport_children (
                    id INTEGER PRIMARY KEY,
                    tenant_id INTEGER,
                    parent_code TEXT,
                    CONSTRAINT fk_transport_parent
                        FOREIGN KEY (tenant_id, parent_code)
                        REFERENCES transport_parents (tenant_id, code)
                        ON DELETE CASCADE
                );
                """,
                TestContext.Current.CancellationToken);
            Assert.Null(create.Error);

            CSharpDB.Client.Models.TableSchema schema = Assert.IsType<CSharpDB.Client.Models.TableSchema>(
                await client.GetTableSchemaAsync(
                    "transport_children",
                    TestContext.Current.CancellationToken));
            ForeignKeyDefinition foreignKey = Assert.Single(schema.ForeignKeys);

            Assert.Equal("tenant_id", foreignKey.ColumnName);
            Assert.Equal("tenant_id", foreignKey.ReferencedColumnName);
            Assert.Equal(["tenant_id", "parent_code"], foreignKey.ColumnNames);
            Assert.Equal(["tenant_id", "code"], foreignKey.ReferencedColumnNames);
            Assert.Equal(ForeignKeyOnDeleteAction.Cascade, foreignKey.OnDelete);
        }
        finally
        {
            if (File.Exists(dbPath))
                File.Delete(dbPath);
            if (File.Exists(dbPath + ".wal"))
                File.Delete(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task ReleaseCachedDatabaseAsync_CancellationKeepsPendingOpenCached()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_engine_transport_{Guid.NewGuid():N}.db");
        var openEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowOpen = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int openCount = 0;

        try
        {
            await using (var client = new EngineTransportClient(
                             dbPath,
                             async (path, ct) =>
                             {
                                 Interlocked.Increment(ref openCount);
                                 openEntered.TrySetResult();
                                 await allowOpen.Task;
                                 return await Database.OpenAsync(path, ct);
                             }))
            {
                Task<IReadOnlyList<string>> initialRequest = client.GetTableNamesAsync(CancellationToken.None);
                await openEntered.Task.WaitAsync(TestContext.Current.CancellationToken);

                using var cts = new CancellationTokenSource();
                Task releaseTask = client.ReleaseCachedDatabaseAsync(cts.Token).AsTask();
                cts.Cancel();

                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => releaseTask);

                allowOpen.TrySetResult();
                var tables = await initialRequest;
                Assert.Empty(tables);

                Database? cached = await client.TryGetDatabaseAsync(TestContext.Current.CancellationToken);
                Assert.NotNull(cached);
                Assert.Equal(1, Volatile.Read(ref openCount));
            }
        }
        finally
        {
            allowOpen.TrySetResult();

            if (File.Exists(dbPath))
                File.Delete(dbPath);
            if (File.Exists(dbPath + ".wal"))
                File.Delete(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task ReleaseCachedDatabaseAsync_BlocksNewGetsUntilOldCachedDatabaseIsDisposed()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_engine_transport_{Guid.NewGuid():N}.db");
        var firstOpenEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowFirstOpen = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int openCount = 0;

        try
        {
            await using var client = new EngineTransportClient(
                dbPath,
                async (path, ct) =>
                {
                    int sequence = Interlocked.Increment(ref openCount);
                    if (sequence == 1)
                    {
                        firstOpenEntered.TrySetResult();
                        await allowFirstOpen.Task;
                    }

                    return await Database.OpenAsync(path, ct);
                });

            _ = client.TryGetDatabaseAsync(CancellationToken.None);
            await firstOpenEntered.Task.WaitAsync(TestContext.Current.CancellationToken);

            Task releaseTask = client.ReleaseCachedDatabaseAsync(TestContext.Current.CancellationToken).AsTask();
            await Task.Delay(50, TestContext.Current.CancellationToken);

            Task<Database?> secondGetTask = client.TryGetDatabaseAsync(TestContext.Current.CancellationToken).AsTask();
            await Task.Delay(50, TestContext.Current.CancellationToken);

            Assert.Equal(1, Volatile.Read(ref openCount));
            Assert.False(secondGetTask.IsCompleted);

            allowFirstOpen.TrySetResult();
            await releaseTask;

            Database? reopened = await secondGetTask;
            Assert.NotNull(reopened);
            Assert.Equal(2, Volatile.Read(ref openCount));
        }
        finally
        {
            allowFirstOpen.TrySetResult();

            if (File.Exists(dbPath))
                File.Delete(dbPath);
            if (File.Exists(dbPath + ".wal"))
                File.Delete(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task GetDatabaseAsync_WhenReleaseStartsAfterWaitBegins_RetriesInsteadOfReturningDisposedDatabase()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_engine_transport_{Guid.NewGuid():N}.db");
        var firstOpenEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowFirstOpen = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int openCount = 0;

        try
        {
            await using var client = new EngineTransportClient(
                dbPath,
                async (path, ct) =>
                {
                    int sequence = Interlocked.Increment(ref openCount);
                    if (sequence == 1)
                    {
                        firstOpenEntered.TrySetResult();
                        await allowFirstOpen.Task;
                    }

                    return await Database.OpenAsync(path, ct);
                });

            Task<Database?> firstGetTask = client.TryGetDatabaseAsync(CancellationToken.None).AsTask();
            await firstOpenEntered.Task.WaitAsync(TestContext.Current.CancellationToken);

            Task<Database?> secondGetTask = client.TryGetDatabaseAsync(CancellationToken.None).AsTask();
            Task releaseTask = client.ReleaseCachedDatabaseAsync(TestContext.Current.CancellationToken).AsTask();

            allowFirstOpen.TrySetResult();
            await releaseTask;

            Database? firstResult = await firstGetTask;
            Database? secondResult = await secondGetTask;

            Assert.NotNull(firstResult);
            Assert.NotNull(secondResult);
            Assert.Equal(2, Volatile.Read(ref openCount));
        }
        finally
        {
            allowFirstOpen.TrySetResult();

            if (File.Exists(dbPath))
                File.Delete(dbPath);
            if (File.Exists(dbPath + ".wal"))
                File.Delete(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task GetRowByPkAsync_UsesTargetedLookupForExistingAndMissingRows()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_engine_transport_{Guid.NewGuid():N}.db");

        try
        {
            await using var client = new EngineTransportClient(dbPath);
            await client.ExecuteSqlAsync(
                """
                CREATE TABLE Users (
                    Id INTEGER PRIMARY KEY,
                    Name TEXT NOT NULL
                );
                INSERT INTO Users VALUES (1, 'Ada');
                INSERT INTO Users VALUES (2, 'Grace');
                INSERT INTO Users VALUES (3, 'Linus');
                """,
                TestContext.Current.CancellationToken);

            Dictionary<string, object?>? first = await client.GetRowByPkAsync("Users", "Id", 1L, TestContext.Current.CancellationToken);
            Dictionary<string, object?>? middle = await client.GetRowByPkAsync("Users", "Id", 2L, TestContext.Current.CancellationToken);
            Dictionary<string, object?>? last = await client.GetRowByPkAsync("Users", "Id", 3L, TestContext.Current.CancellationToken);
            Dictionary<string, object?>? missing = await client.GetRowByPkAsync("Users", "Id", 99L, TestContext.Current.CancellationToken);

            Assert.NotNull(first);
            Assert.Equal("Ada", first!["Name"]);
            Assert.NotNull(middle);
            Assert.Equal("Grace", middle!["Name"]);
            Assert.NotNull(last);
            Assert.Equal("Linus", last!["Name"]);
            Assert.Null(missing);
        }
        finally
        {
            if (File.Exists(dbPath))
                File.Delete(dbPath);
            if (File.Exists(dbPath + ".wal"))
                File.Delete(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task ExportTableArchiveAsync_WritesNativeArchiveFromDirectSnapshot()
    {
        string directory = Path.Combine(Path.GetTempPath(), $"csharpdb_table_export_{Guid.NewGuid():N}");
        string dbPath = Path.Combine(directory, "export.db");
        string archivePath = Path.Combine(directory, "exports", "customers.csdbtable");

        try
        {
            Directory.CreateDirectory(directory);
            await using var client = new EngineTransportClient(dbPath);
            await client.ExecuteSqlAsync(
                """
                CREATE TABLE Customers (
                    Id INTEGER PRIMARY KEY,
                    Name TEXT NOT NULL,
                    Balance REAL,
                    Payload BLOB
                );
                INSERT INTO Customers VALUES (1, 'Ada', 10.5, X'0102FF');
                INSERT INTO Customers VALUES (2, 'Grace', NULL, NULL);
                """,
                TestContext.Current.CancellationToken);

            var exporter = Assert.IsAssignableFrom<ICSharpDbTableArchiveExporter>(client);
            Assert.True(exporter.SupportsTableArchiveExport);

            var export = await exporter.ExportTableArchiveAsync(
                "Customers",
                archivePath,
                TestContext.Current.CancellationToken);

            Assert.Equal("Customers", export.TableName);
            Assert.Equal("customers.csdbtable", export.FileName);
            Assert.Equal(2, export.RowCount);
            Assert.True(File.Exists(archivePath));

            var schema = await TableArchiveReader.ReadTableSchemaAsync(
                archivePath,
                ct: TestContext.Current.CancellationToken);
            Assert.Equal("Customers", schema.TableName);
            Assert.Equal(4, schema.Columns.Count);
            Assert.True(schema.Columns[0].IsPrimaryKey);

            var rows = new List<CSharpDB.Primitives.DbValue[]>();
            await foreach (var row in TableArchiveReader.ReadRowsAsync(
                               archivePath,
                               TestContext.Current.CancellationToken))
            {
                rows.Add(row);
            }

            Assert.Equal(2, rows.Count);
            Assert.Equal(1, rows[0][0].AsInteger);
            Assert.Equal("Ada", rows[0][1].AsText);
            Assert.Equal(10.5, rows[0][2].AsReal);
            Assert.Equal(new byte[] { 0x01, 0x02, 0xff }, rows[0][3].AsBlob);
            Assert.Equal(2, rows[1][0].AsInteger);
            Assert.Equal("Grace", rows[1][1].AsText);
            Assert.True(rows[1][2].IsNull);
            Assert.True(rows[1][3].IsNull);
        }
        finally
        {
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task ExportTableArchiveAsync_ReportsProgressAndHonorsCancellation()
    {
        var testToken = TestContext.Current.CancellationToken;
        string directory = Path.Combine(Path.GetTempPath(), $"csharpdb_table_export_cancel_{Guid.NewGuid():N}");
        string dbPath = Path.Combine(directory, "export.db");
        string archivePath = Path.Combine(directory, "exports", "customers.csdbtable");

        try
        {
            Directory.CreateDirectory(directory);
            await using var client = new EngineTransportClient(dbPath);
            await client.ExecuteSqlAsync(
                "CREATE TABLE Customers (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);",
                testToken);

            for (int start = 1; start <= 5_000; start += 500)
            {
                var sql = new StringBuilder("INSERT INTO Customers (Id, Name) VALUES ");
                for (int i = 0; i < 500; i++)
                {
                    if (i > 0)
                        sql.Append(", ");

                    int id = start + i;
                    sql.Append('(')
                        .Append(id)
                        .Append(", 'Customer ")
                        .Append(id)
                        .Append("')");
                }

                sql.Append(';');
                await client.ExecuteSqlAsync(sql.ToString(), testToken);
            }

            var exporter = Assert.IsAssignableFrom<ICSharpDbTableArchiveProgressExporter>(client);
            using var exportCts = CancellationTokenSource.CreateLinkedTokenSource(testToken);
            long highestRowsExported = 0;
            var progress = new InlineProgress<TableArchiveExportProgress>(p =>
            {
                highestRowsExported = Math.Max(highestRowsExported, p.RowsExported);
                if (p.RowsExported >= 1_000)
                    exportCts.Cancel();
            });

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                await exporter.ExportTableArchiveAsync("Customers", archivePath, progress, exportCts.Token));

            Assert.True(highestRowsExported >= 1_000);
        }
        finally
        {
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task ClientManagedTransactions_ReuseOneDirectDatabaseAcrossCommitAndRollback()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_engine_transport_reuse_{Guid.NewGuid():N}.db");
        int openCount = 0;

        try
        {
            await using var client = new EngineTransportClient(
                dbPath,
                async (path, ct) =>
                {
                    Interlocked.Increment(ref openCount);
                    return await Database.OpenAsync(path, ct);
                });

            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE TABLE transaction_reuse (id INTEGER PRIMARY KEY);",
                TestContext.Current.CancellationToken)).Error);

            TransactionSessionInfo first = await client.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await client.ExecuteInTransactionAsync(
                first.TransactionId,
                "INSERT INTO transaction_reuse VALUES (1);",
                TestContext.Current.CancellationToken);
            await client.CommitTransactionAsync(first.TransactionId, TestContext.Current.CancellationToken);

            TransactionSessionInfo second = await client.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await client.ExecuteInTransactionAsync(
                second.TransactionId,
                "INSERT INTO transaction_reuse VALUES (2);",
                TestContext.Current.CancellationToken);
            await client.RollbackTransactionAsync(second.TransactionId, TestContext.Current.CancellationToken);

            TransactionSessionInfo third = await client.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await client.ExecuteInTransactionAsync(
                third.TransactionId,
                "INSERT INTO transaction_reuse VALUES (3);",
                TestContext.Current.CancellationToken);
            await client.CommitTransactionAsync(third.TransactionId, TestContext.Current.CancellationToken);

            SqlExecutionResult result = await client.ExecuteSqlAsync(
                "SELECT id FROM transaction_reuse ORDER BY id;",
                TestContext.Current.CancellationToken);

            Assert.Null(result.Error);
            Assert.Equal([1L, 3L], result.Rows!.Select(row => row[0]).ToArray());
            Assert.Equal(1, Volatile.Read(ref openCount));
        }
        finally
        {
            await DeleteDatabaseFilesAsync(dbPath);
        }
    }

    [Fact]
    public async Task ClientManagedTransactionHandoff_ClearsTemporaryStateAtBothBoundaries()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_engine_transport_temp_reuse_{Guid.NewGuid():N}.db");
        int openCount = 0;

        try
        {
            await using var client = new EngineTransportClient(
                dbPath,
                async (path, ct) =>
                {
                    Interlocked.Increment(ref openCount);
                    return await Database.OpenAsync(path, ct);
                });

            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE TEMP TABLE before_transaction (id INTEGER PRIMARY KEY);",
                TestContext.Current.CancellationToken)).Error);

            TransactionSessionInfo first = await client.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await Assert.ThrowsAsync<CSharpDB.Primitives.CSharpDbException>(async () =>
                await client.ExecuteInTransactionAsync(
                    first.TransactionId,
                    "SELECT * FROM before_transaction;",
                    TestContext.Current.CancellationToken));

            await client.ExecuteInTransactionAsync(
                first.TransactionId,
                "CREATE TEMP TABLE during_transaction (id INTEGER PRIMARY KEY);",
                TestContext.Current.CancellationToken);
            await client.CommitTransactionAsync(first.TransactionId, TestContext.Current.CancellationToken);

            TransactionSessionInfo second = await client.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await Assert.ThrowsAsync<CSharpDB.Primitives.CSharpDbException>(async () =>
                await client.ExecuteInTransactionAsync(
                    second.TransactionId,
                    "SELECT * FROM during_transaction;",
                    TestContext.Current.CancellationToken));
            await client.RollbackTransactionAsync(second.TransactionId, TestContext.Current.CancellationToken);

            Assert.Equal(1, Volatile.Read(ref openCount));
        }
        finally
        {
            await DeleteDatabaseFilesAsync(dbPath);
        }
    }

    [Fact]
    public async Task ClientManagedTransactionFailure_DoesNotRecycleUncertainDatabaseState()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_engine_transport_failed_reuse_{Guid.NewGuid():N}.db");
        int openCount = 0;

        try
        {
            await using var client = new EngineTransportClient(
                dbPath,
                async (path, ct) =>
                {
                    Interlocked.Increment(ref openCount);
                    return await Database.OpenAsync(path, ct);
                });

            Assert.Null((await client.ExecuteSqlAsync(
                """
                CREATE TABLE failed_transaction_reuse (id INTEGER PRIMARY KEY);
                INSERT INTO failed_transaction_reuse VALUES (1);
                """,
                TestContext.Current.CancellationToken)).Error);

            TransactionSessionInfo transaction = await client.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await Assert.ThrowsAsync<CSharpDB.Primitives.CSharpDbException>(async () =>
                await client.ExecuteInTransactionAsync(
                    transaction.TransactionId,
                    "INSERT INTO failed_transaction_reuse VALUES (1);",
                    TestContext.Current.CancellationToken));
            await Assert.ThrowsAsync<CSharpDB.Primitives.CSharpDbException>(async () =>
                await client.CommitTransactionAsync(
                    transaction.TransactionId,
                    TestContext.Current.CancellationToken));

            Assert.Equal(
                1,
                await client.GetRowCountAsync(
                    "failed_transaction_reuse",
                    TestContext.Current.CancellationToken));
            Assert.Equal(2, Volatile.Read(ref openCount));
        }
        finally
        {
            await DeleteDatabaseFilesAsync(dbPath);
        }
    }

    [Fact]
    public async Task ClientManagedTransactionCompletion_PreservesCompetingOrdinaryHandle()
    {
        Database competingDatabase = await Database.OpenInMemoryAsync(
            TestContext.Current.CancellationToken);
        bool competingDatabaseHandedOff = false;
        int openCount = 0;

        try
        {
            await using var client = new EngineTransportClient(
                ":memory:competing-handle",
                async (_, ct) =>
                {
                    if (Interlocked.Increment(ref openCount) == 2)
                    {
                        competingDatabaseHandedOff = true;
                        return competingDatabase;
                    }

                    return await Database.OpenInMemoryAsync(ct);
                });

            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE TABLE competing_handle (id INTEGER PRIMARY KEY);",
                TestContext.Current.CancellationToken)).Error);

            TransactionSessionInfo transaction = await client.BeginTransactionAsync(TestContext.Current.CancellationToken);
            Assert.Empty(await client.GetTableNamesAsync(TestContext.Current.CancellationToken));
            Assert.Equal(2, Volatile.Read(ref openCount));

            await client.RollbackTransactionAsync(transaction.TransactionId, TestContext.Current.CancellationToken);
            Assert.Same(
                competingDatabase,
                await client.TryGetDatabaseAsync(TestContext.Current.CancellationToken));
            Assert.Equal(2, Volatile.Read(ref openCount));
        }
        finally
        {
            if (!competingDatabaseHandedOff)
                await competingDatabase.DisposeAsync();
        }
    }

    [Fact]
    public async Task ClientManagedTransactionCompletion_DoesNotAdoptAfterTransientOrdinaryOpen()
    {
        int openCount = 0;

        await using var client = new EngineTransportClient(
            ":memory:transient-ordinary-open",
            async (_, ct) =>
            {
                Interlocked.Increment(ref openCount);
                return await Database.OpenInMemoryAsync(ct);
            });

        TransactionSessionInfo transaction = await client.BeginTransactionAsync(TestContext.Current.CancellationToken);
        Assert.Empty(await client.GetTableNamesAsync(TestContext.Current.CancellationToken));
        await client.ReleaseCachedDatabaseAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, Volatile.Read(ref openCount));

        await client.RollbackTransactionAsync(transaction.TransactionId, TestContext.Current.CancellationToken);
        Assert.Empty(await client.GetTableNamesAsync(TestContext.Current.CancellationToken));
        Assert.Equal(3, Volatile.Read(ref openCount));
    }

    [Fact]
    public async Task OverlappingClientManagedTransactions_DoNotAdoptStaleDatabaseHandle()
    {
        int openCount = 0;

        await using var client = new EngineTransportClient(
            ":memory:overlapping-transactions",
            async (_, ct) =>
            {
                Interlocked.Increment(ref openCount);
                return await Database.OpenInMemoryAsync(ct);
            });

        TransactionSessionInfo first = await client.BeginTransactionAsync(
            TestContext.Current.CancellationToken);
        TransactionSessionInfo second = await client.BeginTransactionAsync(
            TestContext.Current.CancellationToken);

        await client.ExecuteInTransactionAsync(
            first.TransactionId,
            "CREATE TABLE stale_overlap_schema (id INTEGER PRIMARY KEY);",
            TestContext.Current.CancellationToken);
        await client.CommitTransactionAsync(first.TransactionId, TestContext.Current.CancellationToken);
        await client.RollbackTransactionAsync(second.TransactionId, TestContext.Current.CancellationToken);

        Assert.Empty(await client.GetTableNamesAsync(TestContext.Current.CancellationToken));
        Assert.Equal(3, Volatile.Read(ref openCount));
    }

    [Fact]
    public async Task ClientManagedTransactions_HybridDisposeTriggerPersistsBeforeCompletionReturns()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_engine_transport_hybrid_boundary_{Guid.NewGuid():N}.db");
        var hybridOptions = new HybridDatabaseOptions
        {
            PersistenceMode = HybridPersistenceMode.Snapshot,
            PersistenceTriggers = HybridPersistenceTriggers.Dispose,
        };

        try
        {
            await using var client = new EngineTransportClient(
                dbPath,
                hybridDatabaseOptions: hybridOptions);
            TransactionSessionInfo transaction = await client.BeginTransactionAsync(
                TestContext.Current.CancellationToken);
            await client.ExecuteInTransactionAsync(
                transaction.TransactionId,
                "CREATE TABLE hybrid_boundary (id INTEGER PRIMARY KEY, value TEXT);",
                TestContext.Current.CancellationToken);
            await client.ExecuteInTransactionAsync(
                transaction.TransactionId,
                "INSERT INTO hybrid_boundary VALUES (1, 'persisted-on-dispose');",
                TestContext.Current.CancellationToken);
            await client.CommitTransactionAsync(transaction.TransactionId, TestContext.Current.CancellationToken);

            await using var reopened = await Database.OpenAsync(
                dbPath,
                TestContext.Current.CancellationToken);
            await using var result = await reopened.ExecuteAsync(
                "SELECT value FROM hybrid_boundary WHERE id = 1;",
                TestContext.Current.CancellationToken);
            var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
            Assert.Equal("persisted-on-dispose", Assert.Single(rows)[0].AsText);
        }
        finally
        {
            await DeleteDatabaseFilesAsync(dbPath);
        }
    }

    [Fact]
    public async Task ClientManagedTransactionCompletion_WaitsForInFlightStatementBeforeReuse()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_engine_transport_inflight_{Guid.NewGuid():N}.db");
        var executeEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var allowExecute = new ManualResetEventSlim();
        int openCount = 0;
        var options = new DatabaseOptions().ConfigureFunctions(functions =>
            functions.AddScalar(
                "WaitForCompletion",
                0,
                new PrimitiveScalarFunctionOptions(PrimitiveDbType.Integer),
                (_, _) =>
                {
                    executeEntered.TrySetResult();
                    allowExecute.Wait();
                    return PrimitiveDbValue.FromInteger(1);
                }));

        try
        {
            await using var client = new EngineTransportClient(
                dbPath,
                async (path, ct) =>
                {
                    Interlocked.Increment(ref openCount);
                    return await Database.OpenAsync(path, options, ct);
                },
                options);

            TransactionSessionInfo transaction = await client.BeginTransactionAsync(TestContext.Current.CancellationToken);
            Task<SqlExecutionResult> executeTask = Task.Run(
                async () => await client.ExecuteInTransactionAsync(
                    transaction.TransactionId,
                    "SELECT WaitForCompletion();",
                    TestContext.Current.CancellationToken),
                TestContext.Current.CancellationToken);
            await executeEntered.Task.WaitAsync(TestContext.Current.CancellationToken);

            Task commitTask = client.CommitTransactionAsync(
                transaction.TransactionId,
                TestContext.Current.CancellationToken);
            await Task.Delay(50, TestContext.Current.CancellationToken);
            Assert.False(commitTask.IsCompleted);

            allowExecute.Set();
            SqlExecutionResult result = await executeTask;
            await commitTask;

            Assert.Equal(1L, Assert.Single(result.Rows!)[0]);
            Assert.Equal(1, Volatile.Read(ref openCount));
            Assert.Empty(await client.GetTableNamesAsync(TestContext.Current.CancellationToken));
            Assert.Equal(1, Volatile.Read(ref openCount));
        }
        finally
        {
            allowExecute.Set();
            await DeleteDatabaseFilesAsync(dbPath);
        }
    }

    [Fact]
    public async Task ClientManagedTransactionCompletion_CancellationWhileWaitingLeavesTransactionUsable()
    {
        var executeEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var allowExecute = new ManualResetEventSlim();
        int openCount = 0;
        var options = new DatabaseOptions().ConfigureFunctions(functions =>
            functions.AddScalar(
                "WaitForCanceledCompletion",
                0,
                new PrimitiveScalarFunctionOptions(PrimitiveDbType.Integer),
                (_, _) =>
                {
                    executeEntered.TrySetResult();
                    allowExecute.Wait();
                    return PrimitiveDbValue.FromInteger(1);
                }));

        try
        {
            await using var client = new EngineTransportClient(
                ":memory:canceled-transaction-completion",
                async (_, ct) =>
                {
                    Interlocked.Increment(ref openCount);
                    return await Database.OpenInMemoryAsync(options, ct);
                },
                options);

            TransactionSessionInfo transaction = await client.BeginTransactionAsync(
                TestContext.Current.CancellationToken);
            Task<SqlExecutionResult> executeTask = Task.Run(
                async () => await client.ExecuteInTransactionAsync(
                    transaction.TransactionId,
                    "SELECT WaitForCanceledCompletion();",
                    TestContext.Current.CancellationToken),
                TestContext.Current.CancellationToken);
            await executeEntered.Task.WaitAsync(TestContext.Current.CancellationToken);

            using var cancellation = new CancellationTokenSource();
            Task canceledCommit = client.CommitTransactionAsync(
                transaction.TransactionId,
                cancellation.Token);
            Assert.False(canceledCommit.IsCompleted);
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => canceledCommit);

            allowExecute.Set();
            Assert.Equal(1L, Assert.Single((await executeTask).Rows!)[0]);

            await client.CommitTransactionAsync(
                transaction.TransactionId,
                TestContext.Current.CancellationToken);
            Assert.Empty(await client.GetTableNamesAsync(TestContext.Current.CancellationToken));
            Assert.Equal(1, Volatile.Read(ref openCount));
        }
        finally
        {
            allowExecute.Set();
        }
    }

    [Fact]
    public async Task DisposeAsync_WaitsForInFlightTransactionStatementAndIsIdempotent()
    {
        var executeEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var allowExecute = new ManualResetEventSlim();
        int openCount = 0;
        var options = new DatabaseOptions().ConfigureFunctions(functions =>
            functions.AddScalar(
                "WaitForClientDispose",
                0,
                new PrimitiveScalarFunctionOptions(PrimitiveDbType.Integer),
                (_, _) =>
                {
                    executeEntered.TrySetResult();
                    allowExecute.Wait();
                    return PrimitiveDbValue.FromInteger(1);
                }));
        var client = new EngineTransportClient(
            ":memory:transaction-dispose",
            async (_, ct) =>
            {
                Interlocked.Increment(ref openCount);
                return await Database.OpenInMemoryAsync(options, ct);
            },
            options);

        try
        {
            TransactionSessionInfo transaction = await client.BeginTransactionAsync(
                TestContext.Current.CancellationToken);
            Task<SqlExecutionResult> executeTask = Task.Run(
                async () => await client.ExecuteInTransactionAsync(
                    transaction.TransactionId,
                    "SELECT WaitForClientDispose();",
                    TestContext.Current.CancellationToken),
                TestContext.Current.CancellationToken);
            await executeEntered.Task.WaitAsync(TestContext.Current.CancellationToken);

            Task firstDispose = client.DisposeAsync().AsTask();
            Task secondDispose = client.DisposeAsync().AsTask();
            Assert.Same(firstDispose, secondDispose);
            Assert.False(firstDispose.IsCompleted);

            allowExecute.Set();
            Assert.Equal(1L, Assert.Single((await executeTask).Rows!)[0]);
            await firstDispose;
            await secondDispose;
            Assert.Equal(1, Volatile.Read(ref openCount));
        }
        finally
        {
            allowExecute.Set();
            await client.DisposeAsync();
        }
    }

    [Fact]
    public async Task BeginTransaction_ActiveSnapshotReaderRestoresCachedDatabase()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_engine_transport_snapshot_guard_{Guid.NewGuid():N}.db");
        int openCount = 0;

        try
        {
            await using var client = new EngineTransportClient(
                dbPath,
                async (path, ct) =>
                {
                    Interlocked.Increment(ref openCount);
                    return await Database.OpenAsync(path, ct);
                });

            Database database = Assert.IsType<Database>(
                await client.TryGetDatabaseAsync(TestContext.Current.CancellationToken));
            using (Database.ReaderSession reader = database.CreateReaderSession())
            {
                CSharpDbClientException exception = await Assert.ThrowsAsync<CSharpDbClientException>(
                    async () => await client.BeginTransactionAsync(TestContext.Current.CancellationToken));
                Assert.Contains("snapshot readers", exception.Message, StringComparison.OrdinalIgnoreCase);
            }

            TransactionSessionInfo transaction = await client.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await client.RollbackTransactionAsync(transaction.TransactionId, TestContext.Current.CancellationToken);
            Assert.Equal(1, Volatile.Read(ref openCount));
        }
        finally
        {
            await DeleteDatabaseFilesAsync(dbPath);
        }
    }

    private static async ValueTask DeleteDatabaseFilesAsync(string dbPath)
    {
        await DeleteIfExistsAsync(dbPath);
        await DeleteIfExistsAsync(dbPath + ".wal");
    }

    private static async ValueTask DeleteIfExistsAsync(string path)
    {
        if (!File.Exists(path))
            return;

        var timeout = System.Diagnostics.Stopwatch.StartNew();
        Exception? lastException = null;
        while (true)
        {
            try
            {
                File.Delete(path);
                return;
            }
            catch (IOException ex) when (timeout.Elapsed < TimeSpan.FromSeconds(2))
            {
                lastException = ex;
            }
            catch (UnauthorizedAccessException ex) when (timeout.Elapsed < TimeSpan.FromSeconds(2))
            {
                lastException = ex;
            }

            if (!File.Exists(path))
                return;
            if (timeout.Elapsed >= TimeSpan.FromSeconds(2))
                break;

            await Task.Delay(25);
        }

        throw new IOException(
            $"Failed to delete temporary database file '{path}' within the cleanup timeout.",
            lastException);
    }

    private sealed class InlineProgress<T>(Action<T> report) : IProgress<T>
    {
        public void Report(T value) => report(value);
    }
}
