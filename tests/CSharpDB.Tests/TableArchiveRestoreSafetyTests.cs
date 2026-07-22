using System.Reflection;
using System.Text.RegularExpressions;
using CSharpDB.Admin.ImportExport.Contracts;
using CSharpDB.Admin.ImportExport.Services;
using CSharpDB.Client;
using CSharpDB.Client.Internal;
using CSharpDB.ImportExport.TableArchives;
using CSharpDB.Primitives;
using SqlExecutionResult = CSharpDB.Client.Models.SqlExecutionResult;

namespace CSharpDB.Tests;

public sealed class TableArchiveRestoreSafetyTests
{
    [Fact]
    public async Task Restore_RejectsArchiveOverSnapshotLimitBeforeClaimingOrStaging()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string archivePath = Path.Combine(Path.GetTempPath(), $"snapshot_limit_{Guid.NewGuid():N}.csdbtable");
        string databasePath = Path.Combine(Path.GetTempPath(), $"snapshot_limit_{Guid.NewGuid():N}.db");
        var schema = new TableSchema
        {
            TableName = "source_items",
            Columns =
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false, IsPrimaryKey = true },
                new ColumnDefinition { Name = "name", Type = DbType.Text, Nullable = false },
            ],
        };

        try
        {
            await TableArchiveWriter.WriteAsync(
                archivePath,
                schema,
                TableArchiveWriter.ToAsyncRows(
                    [[DbValue.FromInteger(1), DbValue.FromText("one")]],
                    ct),
                ct);
            byte[] originalArchive = await File.ReadAllBytesAsync(archivePath, ct);
            Assert.True(originalArchive.Length > 1);

            await using var client = new EngineTransportClient(databasePath);
            var service = new TableImportExportService(
                client,
                new TableArchiveDownloadStore(),
                new TableArchiveRestoreOptions { MaxArchiveSnapshotBytes = 1 });

            InvalidDataException failure = await Assert.ThrowsAsync<InvalidDataException>(async () =>
                await service.RestoreTableAsync(
                    new RestoreTableRequest
                    {
                        ArchivePath = archivePath,
                        TargetTableName = "restored_items",
                    },
                    ct));

            Assert.Contains("restore snapshot limit", failure.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Null(await client.GetTableSchemaAsync("restored_items", ct));
            Assert.Null(await client.GetTableSchemaAsync("__csharpdb_restore_journal_v1", ct));
            Assert.DoesNotContain(
                await client.GetTableNamesAsync(ct),
                name => name.StartsWith("__csharpdb_restore_stage_v1_", StringComparison.OrdinalIgnoreCase));
            Assert.True(File.Exists(archivePath));
            Assert.Equal(originalArchive, await File.ReadAllBytesAsync(archivePath, ct));
        }
        finally
        {
            DeleteIfExists(archivePath);
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task Restore_ReconcilesSuccessfulActivationWhenCommitAcknowledgmentFails()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string archivePath = Path.Combine(Path.GetTempPath(), $"commit_receipt_{Guid.NewGuid():N}.csdbtable");
        string databasePath = Path.Combine(Path.GetTempPath(), $"commit_receipt_{Guid.NewGuid():N}.db");
        var schema = new TableSchema
        {
            TableName = "source_items",
            Columns =
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false, IsPrimaryKey = true },
                new ColumnDefinition { Name = "name", Type = DbType.Text, Nullable = false },
            ],
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

            await using var client = new EngineTransportClient(databasePath);
            ITransactionalFaultingClient faultingClient =
                DispatchProxy.Create<ITransactionalFaultingClient, CommitAcknowledgmentFailureProxy>();
            var fault = (CommitAcknowledgmentFailureProxy)faultingClient;
            fault.Inner = client;
            var service = new TableImportExportService(faultingClient, new TableArchiveDownloadStore());

            RestoreTableResult restored = await service.RestoreTableAsync(
                new RestoreTableRequest
                {
                    ArchivePath = archivePath,
                    TargetTableName = "restored_items",
                },
                ct);

            Assert.True(fault.CommitFailureInjected);
            Assert.Equal(2, restored.RowsInserted);
            Assert.Equal(2, await ScalarInt64Async(client, "SELECT COUNT(*) FROM restored_items;", ct));
            Assert.Equal(0, await ScalarInt64Async(
                client,
                "SELECT COUNT(*) FROM __csharpdb_restore_journal_v1;",
                ct));
            Assert.Equal(1, await ScalarInt64Async(
                client,
                "SELECT COUNT(*) FROM __csharpdb_restore_receipts_v1;",
                ct));
        }
        finally
        {
            DeleteIfExists(archivePath);
            DeleteDatabaseFiles(databasePath);
        }
    }

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

    [Fact]
    public async Task Restore_CanonicalRowMismatchDoesNotActivateAndCanRetry()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string archivePath = Path.Combine(Path.GetTempPath(), $"canonical_mismatch_{Guid.NewGuid():N}.csdbtable");
        string databasePath = Path.Combine(Path.GetTempPath(), $"canonical_mismatch_{Guid.NewGuid():N}.db");
        var schema = new TableSchema
        {
            TableName = "source_items",
            Columns =
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false, IsPrimaryKey = true },
                new ColumnDefinition { Name = "name", Type = DbType.Text, Nullable = false },
            ],
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

            await using var client = new EngineTransportClient(databasePath);
            ITransactionalFaultingClient faultingClient =
                DispatchProxy.Create<ITransactionalFaultingClient, CanonicalMismatchProxy>();
            var fault = (CanonicalMismatchProxy)faultingClient;
            fault.Inner = client;
            var service = new TableImportExportService(faultingClient, new TableArchiveDownloadStore());

            InvalidDataException failure = await Assert.ThrowsAsync<InvalidDataException>(async () =>
                await service.RestoreTableAsync(
                    new RestoreTableRequest
                    {
                        ArchivePath = archivePath,
                        TargetTableName = "restored_items",
                    },
                    ct));

            Assert.Contains("canonical row validation", failure.Message, StringComparison.OrdinalIgnoreCase);
            Assert.True(fault.MutationInjected);
            Assert.Null(await client.GetTableSchemaAsync("restored_items", ct));
            Assert.DoesNotContain(
                await client.GetTableNamesAsync(ct),
                name => name.StartsWith("__csharpdb_restore_stage_v1_", StringComparison.OrdinalIgnoreCase));
            Assert.Equal(0, await ScalarInt64Async(
                client,
                "SELECT COUNT(*) FROM __csharpdb_restore_journal_v1;",
                ct));

            var retry = new TableImportExportService(client, new TableArchiveDownloadStore());
            RestoreTableResult restored = await retry.RestoreTableAsync(
                new RestoreTableRequest
                {
                    ArchivePath = archivePath,
                    TargetTableName = "restored_items",
                },
                ct);

            Assert.Equal(2, restored.RowsInserted);
            Assert.Equal(2, await ScalarInt64Async(client, "SELECT COUNT(*) FROM restored_items;", ct));
            Assert.Equal(1, await ScalarInt64Async(
                client,
                "SELECT COUNT(*) FROM restored_items WHERE name = 'one';",
                ct));
        }
        finally
        {
            DeleteIfExists(archivePath);
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task Restore_CanonicalValidationStreamsMultiplePagesAndPreservesUnkeyedDuplicates()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string archivePath = Path.Combine(Path.GetTempPath(), $"canonical_multipage_{Guid.NewGuid():N}.csdbtable");
        string databasePath = Path.Combine(Path.GetTempPath(), $"canonical_multipage_{Guid.NewGuid():N}.db");
        var schema = new TableSchema
        {
            TableName = "source_events",
            Columns =
            [
                new ColumnDefinition { Name = "bucket", Type = DbType.Integer, Nullable = false },
                new ColumnDefinition { Name = "payload", Type = DbType.Text, Nullable = false },
            ],
        };
        DbValue[][] rows = Enumerable.Range(0, 1_205)
            .Select(index => new[]
            {
                DbValue.FromInteger(index % 7),
                DbValue.FromText(index % 3 == 0 ? "duplicate" : $"event-{index % 11}"),
            })
            .ToArray();

        try
        {
            await TableArchiveWriter.WriteAsync(
                archivePath,
                schema,
                TableArchiveWriter.ToAsyncRows(rows, ct),
                ct);

            await using var client = new EngineTransportClient(databasePath);
            var service = new TableImportExportService(client, new TableArchiveDownloadStore());
            RestoreTableResult restored = await service.RestoreTableAsync(
                new RestoreTableRequest
                {
                    ArchivePath = archivePath,
                    TargetTableName = "restored_events",
                },
                ct);

            Assert.Equal(rows.LongLength, restored.RowsInserted);
            Assert.Equal(rows.LongLength, await ScalarInt64Async(
                client,
                "SELECT COUNT(*) FROM restored_events;",
                ct));
            Assert.Equal(402, await ScalarInt64Async(
                client,
                "SELECT COUNT(*) FROM restored_events WHERE payload = 'duplicate';",
                ct));
        }
        finally
        {
            DeleteIfExists(archivePath);
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task Restore_UsesImmutableArchiveSnapshotWhenOriginalIsReplacedAfterStagingStarts()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string archivePath = Path.Combine(Path.GetTempPath(), $"snapshot_source_{Guid.NewGuid():N}.csdbtable");
        string replacementPath = Path.Combine(Path.GetTempPath(), $"snapshot_replacement_{Guid.NewGuid():N}.csdbtable");
        string databasePath = Path.Combine(Path.GetTempPath(), $"snapshot_restore_{Guid.NewGuid():N}.db");
        var schema = new TableSchema
        {
            TableName = "source_items",
            Columns =
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false, IsPrimaryKey = true },
                new ColumnDefinition { Name = "name", Type = DbType.Text, Nullable = false },
            ],
        };

        try
        {
            await TableArchiveWriter.WriteAsync(
                archivePath,
                schema,
                TableArchiveWriter.ToAsyncRows(
                    [
                        [DbValue.FromInteger(1), DbValue.FromText("from-original-one")],
                        [DbValue.FromInteger(2), DbValue.FromText("from-original-two")],
                    ],
                    ct),
                ct);
            await TableArchiveWriter.WriteAsync(
                replacementPath,
                schema,
                TableArchiveWriter.ToAsyncRows(
                    [
                        [DbValue.FromInteger(1), DbValue.FromText("from-replacement")],
                        [DbValue.FromInteger(2), DbValue.FromText("from-replacement")],
                        [DbValue.FromInteger(3), DbValue.FromText("from-replacement")],
                    ],
                    ct),
                ct);

            await using var client = new EngineTransportClient(databasePath);
            ITransactionalFaultingClient faultingClient =
                DispatchProxy.Create<ITransactionalFaultingClient, StageCreatePauseProxy>();
            var fault = (StageCreatePauseProxy)faultingClient;
            fault.Inner = client;
            var service = new TableImportExportService(faultingClient, new TableArchiveDownloadStore());
            Task<RestoreTableResult>? restoreTask = null;
            try
            {
                restoreTask = service.RestoreTableAsync(
                    new RestoreTableRequest
                    {
                        ArchivePath = archivePath,
                        TargetTableName = "restored_items",
                    },
                    ct);

                await fault.StageCreateObserved.WaitAsync(TimeSpan.FromSeconds(30), ct);
                File.Copy(replacementPath, archivePath, overwrite: true);
                fault.ReleaseStageCreate();

                RestoreTableResult restored = await restoreTask.WaitAsync(TimeSpan.FromSeconds(30), ct);
                Assert.Equal(2, restored.RowsInserted);
                Assert.Equal(2, await ScalarInt64Async(client, "SELECT COUNT(*) FROM restored_items;", ct));
                Assert.Equal(2, await ScalarInt64Async(
                    client,
                    "SELECT COUNT(*) FROM restored_items WHERE name LIKE 'from-original-%';",
                    ct));
                Assert.Equal(0, await ScalarInt64Async(
                    client,
                    "SELECT COUNT(*) FROM restored_items WHERE name = 'from-replacement';",
                    ct));
            }
            finally
            {
                fault.ReleaseStageCreate();
                if (restoreTask is { IsCompleted: false })
                {
                    try
                    {
                        await restoreTask.WaitAsync(TimeSpan.FromSeconds(10), CancellationToken.None);
                    }
                    catch
                    {
                        // Preserve the test failure while allowing the blocked restore to unwind.
                    }
                }
            }
        }
        finally
        {
            DeleteIfExists(archivePath);
            DeleteIfExists(replacementPath);
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task Restore_RejectsSchemaMutationBetweenPreflightAndTransactionalValidation()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string archivePath = Path.Combine(Path.GetTempPath(), $"schema_race_{Guid.NewGuid():N}.csdbtable");
        string databasePath = Path.Combine(Path.GetTempPath(), $"schema_race_{Guid.NewGuid():N}.db");
        var schema = new TableSchema
        {
            TableName = "source_items",
            Columns =
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false, IsPrimaryKey = true },
                new ColumnDefinition { Name = "name", Type = DbType.Text, Nullable = false },
            ],
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

            await using var client = new EngineTransportClient(databasePath);
            ITransactionalFaultingClient faultingClient =
                DispatchProxy.Create<ITransactionalFaultingClient, SchemaRaceProxy>();
            var fault = (SchemaRaceProxy)faultingClient;
            fault.Inner = client;
            var service = new TableImportExportService(faultingClient, new TableArchiveDownloadStore());

            InvalidDataException failure = await Assert.ThrowsAsync<InvalidDataException>(async () =>
                await service.RestoreTableAsync(
                    new RestoreTableRequest
                    {
                        ArchivePath = archivePath,
                        TargetTableName = "restored_items",
                    },
                    ct));

            Assert.True(fault.MutationInjected);
            Assert.Contains("schema validation", failure.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Null(await client.GetTableSchemaAsync("restored_items", ct));
        }
        finally
        {
            DeleteIfExists(archivePath);
            DeleteDatabaseFiles(databasePath);
        }
    }

    private static async Task<long> ScalarInt64Async(
        ICSharpDbClient client,
        string sql,
        CancellationToken ct)
    {
        SqlExecutionResult result = await client.ExecuteSqlAsync(sql, ct);
        Assert.Null(result.Error);
        return Convert.ToInt64(Assert.Single(Assert.Single(result.Rows!)));
    }

    public interface ITransactionalFaultingClient :
        ICSharpDbClient,
        ICSharpDbTransactionalSnapshotReader;

    private class CommitAcknowledgmentFailureProxy : DispatchProxy
    {
        public required ICSharpDbClient Inner { get; set; }

        public bool CommitFailureInjected { get; private set; }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            ArgumentNullException.ThrowIfNull(targetMethod);
            args ??= [];
            if (!CommitFailureInjected &&
                targetMethod.Name == nameof(ICSharpDbClient.CommitTransactionAsync) &&
                args.Length == 2 &&
                args[0] is string transactionId &&
                args[1] is CancellationToken ct)
            {
                return CommitThenLoseAcknowledgmentAsync(transactionId, ct);
            }

            return targetMethod.Invoke(Inner, args);
        }

        private async Task CommitThenLoseAcknowledgmentAsync(string transactionId, CancellationToken ct)
        {
            await Inner.CommitTransactionAsync(transactionId, ct);
            CommitFailureInjected = true;
            throw new IOException("The durable commit acknowledgment was lost.");
        }
    }

    private class CanonicalMismatchProxy : DispatchProxy
    {
        private static readonly Regex s_stageTable = new(
            "FROM\\s+(\"__csharpdb_restore_stage_v1_[0-9a-f]+\")",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

        public required ICSharpDbClient Inner { get; set; }

        public bool MutationInjected { get; private set; }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            ArgumentNullException.ThrowIfNull(targetMethod);
            args ??= [];
            if (!MutationInjected &&
                targetMethod.Name == nameof(ICSharpDbTransactionalSnapshotReader.TryOpenForwardOnlyQueryCursorAsync) &&
                args.Length == 3 &&
                args[0] is string transactionId &&
                args[1] is string sql &&
                args[2] is CancellationToken ct &&
                s_stageTable.Match(sql) is { Success: true } match)
            {
                MutationInjected = true;
                return MutateThenOpenCursorAsync(transactionId, match.Groups[1].Value, sql, ct);
            }

            return targetMethod.Invoke(Inner, args);
        }

        private async ValueTask<ForwardOnlyQueryCursor?> MutateThenOpenCursorAsync(
            string transactionId,
            string quotedStageTable,
            string originalQuery,
            CancellationToken ct)
        {
            SqlExecutionResult mutation = await Inner.ExecuteInTransactionAsync(
                transactionId,
                $"UPDATE {quotedStageTable} SET \"name\" = 'changed-after-load' WHERE \"id\" = 1;",
                ct);
            if (!string.IsNullOrWhiteSpace(mutation.Error) || mutation.RowsAffected != 1)
            {
                throw new InvalidOperationException(
                    mutation.Error ?? "The canonical mismatch test mutation did not affect one row.");
            }

            return await ((ICSharpDbTransactionalSnapshotReader)Inner)
                .TryOpenForwardOnlyQueryCursorAsync(transactionId, originalQuery, ct);
        }
    }

    private class StageCreatePauseProxy : DispatchProxy
    {
        private static readonly Regex s_stageCreate = new(
            "CREATE\\s+TABLE\\s+\"__csharpdb_restore_stage_v1_[0-9a-f]+\"",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        private readonly TaskCompletionSource _stageCreateObserved = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _releaseStageCreate = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        public required ICSharpDbClient Inner { get; set; }

        public Task StageCreateObserved => _stageCreateObserved.Task;

        public void ReleaseStageCreate() => _releaseStageCreate.TrySetResult();

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            ArgumentNullException.ThrowIfNull(targetMethod);
            args ??= [];
            if (targetMethod.Name == nameof(ICSharpDbClient.ExecuteSqlAsync) &&
                args.Length == 2 &&
                args[0] is string sql &&
                args[1] is CancellationToken ct &&
                s_stageCreate.IsMatch(sql))
            {
                _stageCreateObserved.TrySetResult();
                return ResumeStageCreateAsync(targetMethod, args, ct);
            }

            return targetMethod.Invoke(Inner, args);
        }

        private async Task<SqlExecutionResult> ResumeStageCreateAsync(
            MethodInfo targetMethod,
            object?[] args,
            CancellationToken ct)
        {
            await _releaseStageCreate.Task.WaitAsync(ct);
            return await (Task<SqlExecutionResult>)targetMethod.Invoke(Inner, args)!;
        }
    }

    private class SchemaRaceProxy : DispatchProxy
    {
        private static readonly Regex s_stageCreate = new(
            "CREATE\\s+TABLE\\s+(\"__csharpdb_restore_stage_v1_[0-9a-f]+\")",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        private string? _quotedStageTable;

        public required ICSharpDbClient Inner { get; set; }

        public bool MutationInjected { get; private set; }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            ArgumentNullException.ThrowIfNull(targetMethod);
            args ??= [];
            if (targetMethod.Name == nameof(ICSharpDbClient.ExecuteSqlAsync) &&
                args.Length == 2 &&
                args[0] is string sql &&
                s_stageCreate.Match(sql) is { Success: true } stageMatch)
            {
                _quotedStageTable = stageMatch.Groups[1].Value;
            }

            if (!MutationInjected &&
                targetMethod.Name == nameof(ICSharpDbClient.BeginTransactionAsync) &&
                args.Length == 1 &&
                args[0] is CancellationToken ct &&
                _quotedStageTable is not null)
            {
                MutationInjected = true;
                return MutateThenBeginAsync(_quotedStageTable, ct);
            }

            return targetMethod.Invoke(Inner, args);
        }

        private async Task<CSharpDB.Client.Models.TransactionSessionInfo> MutateThenBeginAsync(
            string quotedStageTable,
            CancellationToken ct)
        {
            SqlExecutionResult mutation = await Inner.ExecuteSqlAsync(
                $"ALTER TABLE {quotedStageTable} ADD COLUMN \"unexpected_after_preflight\" TEXT;",
                ct);
            if (!string.IsNullOrWhiteSpace(mutation.Error))
                throw new InvalidOperationException(mutation.Error);

            return await Inner.BeginTransactionAsync(ct);
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
