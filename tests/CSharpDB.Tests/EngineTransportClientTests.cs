using System.Text;
using System.Threading;
using CSharpDB.Client;
using CSharpDB.Client.Internal;
using CSharpDB.Client.Models;
using CSharpDB.Engine;
using CSharpDB.ImportExport.TableArchives;

namespace CSharpDB.Tests;

public sealed class EngineTransportClientTests
{
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

    private sealed class InlineProgress<T>(Action<T> report) : IProgress<T>
    {
        public void Report(T value) => report(value);
    }
}
