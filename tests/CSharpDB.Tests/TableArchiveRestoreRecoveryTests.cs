using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Admin.ImportExport.Contracts;
using CSharpDB.Admin.ImportExport.Services;
using CSharpDB.Client;
using CSharpDB.Client.Internal;
using CSharpDB.Client.Models;
using CSharpDB.ImportExport.TableArchives;
using CSharpDB.Primitives;
using ClientColumnDefinition = CSharpDB.Client.Models.ColumnDefinition;
using ClientTableSchema = CSharpDB.Client.Models.TableSchema;
using PrimitiveColumnDefinition = CSharpDB.Primitives.ColumnDefinition;
using PrimitiveDbType = CSharpDB.Primitives.DbType;
using PrimitiveDbValue = CSharpDB.Primitives.DbValue;
using PrimitiveTableSchema = CSharpDB.Primitives.TableSchema;

namespace CSharpDB.Tests;

public sealed class TableArchiveRestoreRecoveryTests
{
    private const string JournalTableName = "__csharpdb_restore_journal_v1";
    private const string JournalConstraintName = "__csharpdb_restore_journal_contract_v1";
    private const string StagePrefix = "__csharpdb_restore_stage_v1_";
    private const string OwnerPrefix = "__csharpdb_restore_owner_v1_";

    [Fact]
    public async Task Restore_AtomicallyCleansOwnedAbandonedStageAndPreservesUnrelatedTable()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string archivePath = TemporaryPath("abandoned_restore", ".csdbtable");
        string databasePath = TemporaryPath("abandoned_restore", ".db");
        const string targetName = "recovered_items";
        const string ownerToken = "0123456789abcdef0123456789abcdef";

        try
        {
            await WriteSimpleArchiveAsync(archivePath, ct);
            string targetKey = ComputeTargetKey(targetName);
            string archiveToken = await ComputeArchiveTokenAsync(archivePath, targetName, ct);
            string stageName = StagePrefix + targetKey;
            string ownerConstraint = OwnerPrefix + ownerToken;
            long staleHeartbeat = DateTimeOffset.UtcNow.Subtract(TimeSpan.FromHours(1)).ToUnixTimeMilliseconds();

            await using (var setupClient = new EngineTransportClient(databasePath))
            {
                await ExecuteCheckedAsync(
                    setupClient,
                    $"""
                    CREATE TABLE {JournalTableName} (
                        target_key TEXT PRIMARY KEY,
                        staging_name TEXT NOT NULL,
                        target_name TEXT NOT NULL,
                        archive_token TEXT NOT NULL,
                        owner_token TEXT NOT NULL,
                        heartbeat_unix_ms INTEGER NOT NULL,
                        CONSTRAINT {JournalConstraintName} CHECK ((1 = 1))
                    );
                    INSERT INTO {JournalTableName} VALUES (
                        '{targetKey}', '{stageName}', '{targetName}', '{archiveToken}', '{ownerToken}', {staleHeartbeat});
                    CREATE TABLE "{stageName}" (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL,
                        CONSTRAINT "{ownerConstraint}" CHECK ((1 = 1))
                    );
                    INSERT INTO "{stageName}" VALUES (99, 'stale');
                    CREATE TABLE unrelated_user_table (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
                    INSERT INTO unrelated_user_table VALUES (7, 'keep');
                    """,
                    ct);
            }

            await using var client = new EngineTransportClient(databasePath);
            var service = new TableImportExportService(client, new TableArchiveDownloadStore());
            RestoreTableResult result = await service.RestoreTableAsync(
                new RestoreTableRequest
                {
                    ArchivePath = archivePath,
                    TargetTableName = targetName,
                },
                ct);

            Assert.Equal(2, result.RowsInserted);
            Assert.Null(await client.GetTableSchemaAsync(stageName, ct));
            Assert.NotNull(await client.GetTableSchemaAsync(targetName, ct));
            Assert.NotNull(await client.GetTableSchemaAsync("unrelated_user_table", ct));
            SqlExecutionResult restored = await client.ExecuteSqlAsync(
                $"SELECT id, name FROM {targetName} ORDER BY id;",
                ct);
            Assert.Null(restored.Error);
            Assert.Equal(2, restored.Rows!.Count);
            Assert.Equal(1L, restored.Rows[0][0]);
            Assert.Equal("one", restored.Rows[0][1]);
            Assert.Equal(2L, restored.Rows[1][0]);
            Assert.Equal("two", restored.Rows[1][1]);
            SqlExecutionResult unrelated = await client.ExecuteSqlAsync(
                "SELECT id, value FROM unrelated_user_table;",
                ct);
            Assert.Null(unrelated.Error);
            Assert.Equal(7L, Assert.Single(unrelated.Rows!)[0]);
            SqlExecutionResult journal = await client.ExecuteSqlAsync(
                $"SELECT COUNT(*) FROM {JournalTableName};",
                ct);
            Assert.Null(journal.Error);
            Assert.Equal(0L, Assert.Single(journal.Rows!)[0]);
        }
        finally
        {
            DeleteIfExists(archivePath);
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task Restore_UnjournaledDeterministicStageCollisionIsPreserved()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string archivePath = TemporaryPath("stage_collision", ".csdbtable");
        string databasePath = TemporaryPath("stage_collision", ".db");
        const string targetName = "collision_target";

        try
        {
            await WriteSimpleArchiveAsync(archivePath, ct);
            string stageName = StagePrefix + ComputeTargetKey(targetName);
            await using var client = new EngineTransportClient(databasePath);
            await ExecuteCheckedAsync(
                client,
                $"""
                CREATE TABLE "{stageName}" (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
                INSERT INTO "{stageName}" VALUES (77, 'user-owned');
                """,
                ct);

            var service = new TableImportExportService(client, new TableArchiveDownloadStore());
            InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await service.RestoreTableAsync(
                    new RestoreTableRequest
                    {
                        ArchivePath = archivePath,
                        TargetTableName = targetName,
                    },
                    ct));

            Assert.Contains("preserved", error.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Null(await client.GetTableSchemaAsync(targetName, ct));
            Assert.NotNull(await client.GetTableSchemaAsync(stageName, ct));
            SqlExecutionResult row = await client.ExecuteSqlAsync(
                $"SELECT id, name FROM \"{stageName}\";",
                ct);
            Assert.Null(row.Error);
            Assert.Equal(77L, Assert.Single(row.Rows!)[0]);
        }
        finally
        {
            DeleteIfExists(archivePath);
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task Restore_PostLoadSchemaMismatchFailsBeforeActivationAndCleansOwnedStage()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string archivePath = TemporaryPath("schema_validation", ".csdbtable");
        string databasePath = TemporaryPath("schema_validation", ".db");
        const string targetName = "validated_target";

        try
        {
            await WriteSimpleArchiveAsync(archivePath, ct);
            await using var inner = new EngineTransportClient(databasePath);
            ITransactionalTamperingClient proxy =
                DispatchProxy.Create<ITransactionalTamperingClient, TamperingClientProxy>();
            ((TamperingClientProxy)(object)proxy).Inner = inner;
            var service = new TableImportExportService(proxy, new TableArchiveDownloadStore());

            InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(async () =>
                await service.RestoreTableAsync(
                    new RestoreTableRequest
                    {
                        ArchivePath = archivePath,
                        TargetTableName = targetName,
                    },
                    ct));

            Assert.Contains("schema validation failed", error.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Null(await inner.GetTableSchemaAsync(targetName, ct));
            Assert.DoesNotContain(
                await inner.GetTableNamesAsync(ct),
                name => name.StartsWith(StagePrefix, StringComparison.OrdinalIgnoreCase));
            SqlExecutionResult journal = await inner.ExecuteSqlAsync(
                $"SELECT COUNT(*) FROM {JournalTableName};",
                ct);
            Assert.Null(journal.Error);
            Assert.Equal(0L, Assert.Single(journal.Rows!)[0]);
        }
        finally
        {
            DeleteIfExists(archivePath);
            DeleteDatabaseFiles(databasePath);
        }
    }

    public interface ITransactionalTamperingClient :
        ICSharpDbClient,
        ICSharpDbTransactionalSnapshotReader;

    public class TamperingClientProxy : DispatchProxy
    {
        private int _tampered;

        public required ICSharpDbClient Inner { get; set; }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            ArgumentNullException.ThrowIfNull(targetMethod);
            if (targetMethod.Name == nameof(ICSharpDbClient.GetTableSchemaAsync))
            {
                return GetTableSchemaAsync(
                    (string)args![0]!,
                    args.Length > 1 ? (CancellationToken)args[1]! : default);
            }

            try
            {
                return targetMethod.Invoke(Inner, args);
            }
            catch (TargetInvocationException error) when (error.InnerException is not null)
            {
                throw error.InnerException;
            }
        }

        private async Task<ClientTableSchema?> GetTableSchemaAsync(string tableName, CancellationToken ct)
        {
            ClientTableSchema? schema = await Inner.GetTableSchemaAsync(tableName, ct);
            if (schema is null ||
                !tableName.StartsWith(StagePrefix, StringComparison.OrdinalIgnoreCase) ||
                Interlocked.Exchange(ref _tampered, 1) != 0)
            {
                return schema;
            }

            ClientColumnDefinition[] columns = schema.Columns.Select(column => new ClientColumnDefinition
            {
                Name = column.Name,
                Type = column.Type,
                Nullable = string.Equals(column.Name, "name", StringComparison.OrdinalIgnoreCase)
                    ? true
                    : column.Nullable,
                IsPrimaryKey = column.IsPrimaryKey,
                IsIdentity = column.IsIdentity,
                IsRowVersion = column.IsRowVersion,
                Collation = column.Collation,
                DefaultSql = column.DefaultSql,
            }).ToArray();
            return new ClientTableSchema
            {
                TableName = schema.TableName,
                Columns = columns,
                ForeignKeys = schema.ForeignKeys,
                CheckConstraints = schema.CheckConstraints,
                KeyConstraints = schema.KeyConstraints,
                NextRowId = schema.NextRowId,
            };
        }
    }

    private static async Task WriteSimpleArchiveAsync(string path, CancellationToken ct)
    {
        var schema = new PrimitiveTableSchema
        {
            TableName = "source_items",
            Columns =
            [
                new PrimitiveColumnDefinition
                {
                    Name = "id",
                    Type = PrimitiveDbType.Integer,
                    Nullable = false,
                    IsPrimaryKey = true,
                },
                new PrimitiveColumnDefinition
                {
                    Name = "name",
                    Type = PrimitiveDbType.Text,
                    Nullable = false,
                },
            ],
            NextRowId = 3,
        };
        PrimitiveDbValue[][] rows =
        [
            [PrimitiveDbValue.FromInteger(1), PrimitiveDbValue.FromText("one")],
            [PrimitiveDbValue.FromInteger(2), PrimitiveDbValue.FromText("two")],
        ];
        await TableArchiveWriter.WriteAsync(
            path,
            schema,
            TableArchiveWriter.ToAsyncRows(rows, ct),
            ct);
    }

    private static async Task ExecuteCheckedAsync(
        ICSharpDbClient client,
        string sql,
        CancellationToken ct)
    {
        SqlExecutionResult result = await client.ExecuteSqlAsync(sql, ct);
        Assert.Null(result.Error);
    }

    private static string ComputeTargetKey(string targetName)
    {
        byte[] digest = SHA256.HashData(Encoding.UTF8.GetBytes(targetName.ToUpperInvariant()));
        return Convert.ToHexString(digest.AsSpan(0, 12)).ToLowerInvariant();
    }

    private static async Task<string> ComputeArchiveTokenAsync(
        string archivePath,
        string targetName,
        CancellationToken ct)
    {
        await using var stream = new FileStream(
            archivePath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            bufferSize: 64 * 1024,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        byte[] archiveDigest = await SHA256.HashDataAsync(stream, ct);
        byte[] targetBytes = Encoding.UTF8.GetBytes(targetName.ToUpperInvariant());
        byte[] identity = new byte[targetBytes.Length + 1 + archiveDigest.Length];
        targetBytes.CopyTo(identity, 0);
        archiveDigest.CopyTo(identity, targetBytes.Length + 1);
        byte[] operationDigest = SHA256.HashData(identity);
        return Convert.ToHexString(operationDigest.AsSpan(0, 16)).ToLowerInvariant();
    }

    private static string TemporaryPath(string prefix, string extension) =>
        Path.Combine(Path.GetTempPath(), $"{prefix}_{Guid.NewGuid():N}{extension}");

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
