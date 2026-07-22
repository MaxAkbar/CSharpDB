using CSharpDB.Admin.ImportExport.Contracts;
using CSharpDB.Admin.ImportExport.Services;
using CSharpDB.Client.Internal;
using CSharpDB.ImportExport.TableArchives;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class TableArchiveRestoreSafetyTests
{
    [Fact]
    public async Task Restore_ReplaysNextRowIdAndPersistsItAcrossReopen()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string archivePath = Path.Combine(Path.GetTempPath(), $"reseed_{Guid.NewGuid():N}.csdbtable");
        string databasePath = Path.Combine(Path.GetTempPath(), $"reseed_{Guid.NewGuid():N}.db");
        var schema = new TableSchema
        {
            TableName = "source_items",
            Columns =
            [
                new ColumnDefinition
                {
                    Name = "id",
                    Type = DbType.Integer,
                    Nullable = false,
                    IsPrimaryKey = true,
                    IsIdentity = true,
                },
                new ColumnDefinition { Name = "name", Type = DbType.Text, Nullable = false },
            ],
            NextRowId = 41,
        };

        try
        {
            await TableArchiveWriter.WriteAsync(
                archivePath,
                schema,
                TableArchiveWriter.ToAsyncRows(
                    [
                        [DbValue.FromInteger(1), DbValue.FromText("one")],
                        [DbValue.FromInteger(2), DbValue.FromText("two")],
                    ],
                    ct),
                ct);

            await using (var client = new EngineTransportClient(databasePath))
            {
                var service = new TableImportExportService(client, new TableArchiveDownloadStore());
                RestoreTableResult restored = await service.RestoreTableAsync(
                    new RestoreTableRequest
                    {
                        ArchivePath = archivePath,
                        TargetTableName = "restored_items",
                    },
                    ct);

                Assert.Equal(2, restored.RowsInserted);
                CSharpDB.Engine.Database database = Assert.IsType<CSharpDB.Engine.Database>(
                    await client.TryGetDatabaseAsync(ct));
                Assert.Equal(41, database.GetTableSchema("restored_items")!.NextRowId);
            }

            await using (var reopened = new EngineTransportClient(databasePath))
            {
                CSharpDB.Engine.Database database = Assert.IsType<CSharpDB.Engine.Database>(
                    await reopened.TryGetDatabaseAsync(ct));
                Assert.Equal(41, database.GetTableSchema("restored_items")!.NextRowId);

                CSharpDB.Client.Models.SqlExecutionResult insert = await reopened.ExecuteSqlAsync(
                    "INSERT INTO restored_items (name) VALUES ('three');",
                    ct);
                Assert.Null(insert.Error);

                CSharpDB.Client.Models.SqlExecutionResult query = await reopened.ExecuteSqlAsync(
                    "SELECT id FROM restored_items WHERE name = 'three';",
                    ct);
                Assert.Null(query.Error);
                Assert.Equal(41L, Convert.ToInt64(Assert.Single(query.Rows!)[0]));
            }
        }
        finally
        {
            DeleteIfExists(archivePath);
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task Restore_LateIndexFailureDoesNotExposePartialTarget()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string archivePath = Path.Combine(Path.GetTempPath(), $"staged_restore_{Guid.NewGuid():N}.csdbtable");
        string databasePath = Path.Combine(Path.GetTempPath(), $"staged_restore_{Guid.NewGuid():N}.db");
        var schema = new TableSchema
        {
            TableName = "source_items",
            Columns =
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false, IsPrimaryKey = true },
                new ColumnDefinition { Name = "name", Type = DbType.Text, Nullable = false },
            ],
            NextRowId = 3,
        };
        IndexSchema[] indexes =
        [
            new IndexSchema
            {
                IndexName = "ix_restore_conflict",
                TableName = "source_items",
                Columns = ["name"],
            },
        ];

        try
        {
            await TableArchiveWriter.WriteAsync(
                archivePath,
                schema,
                indexes,
                TableArchiveWriter.ToAsyncRows(
                    [
                        [DbValue.FromInteger(1), DbValue.FromText("one")],
                        [DbValue.FromInteger(2), DbValue.FromText("two")],
                    ],
                    ct),
                ct);

            await using var client = new EngineTransportClient(databasePath);
            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE TABLE blocker (id INTEGER PRIMARY KEY, name TEXT NOT NULL);",
                ct)).Error);
            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE INDEX ix_restore_conflict ON blocker (name);",
                ct)).Error);

            var service = new TableImportExportService(client, new TableArchiveDownloadStore());
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await service.RestoreTableAsync(
                    new RestoreTableRequest
                    {
                        ArchivePath = archivePath,
                        TargetTableName = "restored_items",
                    },
                    ct));

            Assert.Null(await client.GetTableSchemaAsync("restored_items", ct));
            Assert.DoesNotContain(
                await client.GetTableNamesAsync(ct),
                name => name.StartsWith("__csharpdb_restore_stage_v1_", StringComparison.OrdinalIgnoreCase));
            var journalCount = await client.ExecuteSqlAsync(
                "SELECT COUNT(*) FROM __csharpdb_restore_journal_v1;",
                ct);
            Assert.Null(journalCount.Error);
            Assert.Equal(
                0L,
                Convert.ToInt64(
                    Assert.Single(Assert.Single(journalCount.Rows!)),
                    System.Globalization.CultureInfo.InvariantCulture));
        }
        finally
        {
            DeleteIfExists(archivePath);
            DeleteDatabaseFiles(databasePath);
        }
    }

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
