using Microsoft.Win32.SafeHandles;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Storage.Catalog;
using CSharpDB.Storage.Device;
using CSharpDB.Storage.Diagnostics;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.StorageEngine;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Tests;

public sealed class TransactionalStorageFaultRecoveryTests
{
    [Fact]
    public async Task ShadowRootRewrite_WalCommitFailure_PreservesOriginalStorageAfterReopen()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string databasePath = NewDatabasePath("rewrite");

        try
        {
            uint originalTableRoot;
            await using (Database seed = await Database.OpenAsync(databasePath, ct))
            {
                await seed.ExecuteAsync(
                    "CREATE TABLE rewrite_fault_items (" +
                    "id INTEGER PRIMARY KEY, amount INTEGER NOT NULL, tag TEXT NOT NULL)",
                    ct);
                await seed.ExecuteAsync(
                    "CREATE INDEX ix_rewrite_fault_amount " +
                    "ON rewrite_fault_items (amount)",
                    ct);
                await seed.ExecuteAsync(
                    "INSERT INTO rewrite_fault_items VALUES " +
                    "(1, 10, 'ten'), (2, 20, 'twenty')",
                    ct);
                await seed.CheckpointAsync(ct);
                originalTableRoot = seed.GetTableRootPage("rewrite_fault_items");
            }

            uint originalIndexRoot = await GetIndexRootAsync(
                databasePath,
                "ix_rewrite_fault_amount",
                ct);

            var failureFactory = new OneShotCommitFailureStorageEngineFactory();
            await using (Database faulted = await Database.OpenAsync(
                databasePath,
                new DatabaseOptions
                {
                    StorageEngineFactory = failureFactory,
                },
                ct))
            {
                failureFactory.ArmCommitFailure();
                CSharpDbException error = await Assert.ThrowsAsync<CSharpDbException>(
                    () => faulted.ExecuteAsync(
                        "ALTER TABLE rewrite_fault_items " +
                        "ALTER COLUMN amount TYPE REAL",
                        ct).AsTask());

                Assert.Equal(ErrorCode.WalError, error.Code);
                Assert.Equal(
                    originalTableRoot,
                    faulted.GetTableRootPage("rewrite_fault_items"));
                Assert.Equal(
                    DbType.Integer,
                    Assert.IsType<TableSchema>(
                        faulted.GetTableSchema("rewrite_fault_items"))
                        .Columns[1]
                        .Type);
                Assert.Equal(
                    30L,
                    await ScalarIntegerAsync(
                        faulted,
                        "SELECT SUM(amount) FROM rewrite_fault_items",
                        ct));
            }

            Assert.Equal(
                originalIndexRoot,
                await GetIndexRootAsync(
                    databasePath,
                    "ix_rewrite_fault_amount",
                    ct));

            await using (Database reopened = await Database.OpenAsync(databasePath, ct))
            {
                Assert.Equal(
                    originalTableRoot,
                    reopened.GetTableRootPage("rewrite_fault_items"));
                Assert.Equal(
                    DbType.Integer,
                    Assert.IsType<TableSchema>(
                        reopened.GetTableSchema("rewrite_fault_items"))
                        .Columns[1]
                        .Type);
                Assert.Equal(
                    30L,
                    await ScalarIntegerAsync(
                        reopened,
                        "SELECT SUM(amount) FROM rewrite_fault_items",
                        ct));

                await reopened.ExecuteAsync(
                    "INSERT INTO rewrite_fault_items VALUES (3, 30, 'thirty')",
                    ct);
                await reopened.CheckpointAsync(ct);
            }

            await using (Database verified = await Database.OpenAsync(databasePath, ct))
            {
                Assert.Equal(
                    3L,
                    await ScalarIntegerAsync(
                        verified,
                        "SELECT COUNT(*) FROM rewrite_fault_items",
                        ct));
                Assert.Equal(
                    30L,
                    await ScalarIntegerAsync(
                        verified,
                        "SELECT amount FROM rewrite_fault_items WHERE id = 3",
                        ct));
            }

            Assert.Equal(
                originalIndexRoot,
                await GetIndexRootAsync(
                    databasePath,
                    "ix_rewrite_fault_amount",
                    ct));
        }
        finally
        {
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task MultiLevelDeleteCascade_WalCommitFailure_RestoresEntireGraphAfterReopen()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string databasePath = NewDatabasePath("cascade");

        try
        {
            await using (Database seed = await Database.OpenAsync(databasePath, ct))
            {
                await seed.ExecuteAsync(
                    "CREATE TABLE cascade_fault_parents (id INTEGER PRIMARY KEY)",
                    ct);
                await seed.ExecuteAsync(
                    """
                    CREATE TABLE cascade_fault_children (
                        id INTEGER PRIMARY KEY,
                        parent_id INTEGER NOT NULL,
                        CONSTRAINT fk_cascade_fault_children_parent
                            FOREIGN KEY (parent_id)
                            REFERENCES cascade_fault_parents (id)
                            ON DELETE CASCADE
                    )
                    """,
                    ct);
                await seed.ExecuteAsync(
                    """
                    CREATE TABLE cascade_fault_grandchildren (
                        id INTEGER PRIMARY KEY,
                        child_id INTEGER NOT NULL,
                        CONSTRAINT fk_cascade_fault_grandchildren_child
                            FOREIGN KEY (child_id)
                            REFERENCES cascade_fault_children (id)
                            ON DELETE CASCADE
                    )
                    """,
                    ct);
                await seed.ExecuteAsync(
                    "INSERT INTO cascade_fault_parents VALUES (1), (2)",
                    ct);
                await seed.ExecuteAsync(
                    "INSERT INTO cascade_fault_children VALUES " +
                    "(10, 1), (11, 1), (20, 2)",
                    ct);
                await seed.ExecuteAsync(
                    "INSERT INTO cascade_fault_grandchildren VALUES " +
                    "(100, 10), (110, 11), (200, 20)",
                    ct);
                await seed.CheckpointAsync(ct);
            }

            var failureFactory = new OneShotCommitFailureStorageEngineFactory();
            await using (Database faulted = await Database.OpenAsync(
                databasePath,
                new DatabaseOptions
                {
                    StorageEngineFactory = failureFactory,
                },
                ct))
            {
                failureFactory.ArmCommitFailure();
                CSharpDbException error = await Assert.ThrowsAsync<CSharpDbException>(
                    () => faulted.ExecuteAsync(
                        "DELETE FROM cascade_fault_parents WHERE id = 1",
                        ct).AsTask());

                Assert.Equal(ErrorCode.WalError, error.Code);
                await AssertCascadeGraphIntactAsync(faulted, ct);
            }

            await AssertIndexesAreValidAsync(databasePath, ct);

            await using (Database reopened = await Database.OpenAsync(databasePath, ct))
            {
                await AssertCascadeGraphIntactAsync(reopened, ct);

                await reopened.ExecuteAsync(
                    "DELETE FROM cascade_fault_parents WHERE id = 1",
                    ct);
                Assert.Equal(
                    1L,
                    await ScalarIntegerAsync(
                        reopened,
                        "SELECT COUNT(*) FROM cascade_fault_parents",
                        ct));
                Assert.Equal(
                    1L,
                    await ScalarIntegerAsync(
                        reopened,
                        "SELECT COUNT(*) FROM cascade_fault_children",
                        ct));
                Assert.Equal(
                    1L,
                    await ScalarIntegerAsync(
                        reopened,
                        "SELECT COUNT(*) FROM cascade_fault_grandchildren",
                        ct));
                Assert.Equal(
                    200L,
                    await ScalarIntegerAsync(
                        reopened,
                        "SELECT id FROM cascade_fault_grandchildren",
                        ct));
                await reopened.CheckpointAsync(ct);
            }

            await using (Database verified = await Database.OpenAsync(databasePath, ct))
            {
                Assert.Equal(
                    1L,
                    await ScalarIntegerAsync(
                        verified,
                        "SELECT COUNT(*) FROM cascade_fault_parents WHERE id = 2",
                        ct));
                Assert.Equal(
                    1L,
                    await ScalarIntegerAsync(
                        verified,
                        "SELECT COUNT(*) FROM cascade_fault_children WHERE id = 20",
                        ct));
                Assert.Equal(
                    1L,
                    await ScalarIntegerAsync(
                        verified,
                        "SELECT COUNT(*) FROM cascade_fault_grandchildren WHERE id = 200",
                        ct));

                await verified.ExecuteAsync(
                    "INSERT INTO cascade_fault_parents VALUES (3)",
                    ct);
                await verified.CheckpointAsync(ct);
            }

            await using (Database finalReopen = await Database.OpenAsync(
                databasePath,
                ct))
            {
                Assert.Equal(
                    1L,
                    await ScalarIntegerAsync(
                        finalReopen,
                        "SELECT COUNT(*) FROM cascade_fault_parents WHERE id = 3",
                        ct));
            }

            await AssertIndexesAreValidAsync(databasePath, ct);
        }
        finally
        {
            DeleteDatabaseFiles(databasePath);
        }
    }

    private static async Task AssertCascadeGraphIntactAsync(
        Database database,
        CancellationToken ct)
    {
        Assert.Equal(
            2L,
            await ScalarIntegerAsync(
                database,
                "SELECT COUNT(*) FROM cascade_fault_parents",
                ct));
        Assert.Equal(
            3L,
            await ScalarIntegerAsync(
                database,
                "SELECT COUNT(*) FROM cascade_fault_children",
                ct));
        Assert.Equal(
            3L,
            await ScalarIntegerAsync(
                database,
                "SELECT COUNT(*) FROM cascade_fault_grandchildren",
                ct));
        Assert.Equal(
            2L,
            await ScalarIntegerAsync(
                database,
                "SELECT COUNT(*) FROM cascade_fault_children WHERE parent_id = 1",
                ct));
        Assert.Equal(
            2L,
            await ScalarIntegerAsync(
                database,
                "SELECT COUNT(*) FROM cascade_fault_grandchildren " +
                "WHERE child_id IN (10, 11)",
                ct));
    }

    private static async Task<long> ScalarIntegerAsync(
        Database database,
        string sql,
        CancellationToken ct)
    {
        await using QueryResult result = await database.ExecuteAsync(sql, ct);
        return Assert.Single(await result.ToListAsync(ct))[0].AsInteger;
    }

    private static async Task<uint> GetIndexRootAsync(
        string databasePath,
        string indexName,
        CancellationToken ct)
    {
        IndexInspectReport report = await IndexInspector.CheckAsync(
            databasePath,
            indexName,
            ct: ct);
        Assert.DoesNotContain(
            report.Issues,
            issue => issue.Severity == InspectSeverity.Error);
        return Assert.Single(report.Indexes).RootPage;
    }

    private static async Task AssertIndexesAreValidAsync(
        string databasePath,
        CancellationToken ct)
    {
        IndexInspectReport report = await IndexInspector.CheckAsync(
            databasePath,
            ct: ct);
        Assert.DoesNotContain(
            report.Issues,
            issue => issue.Severity == InspectSeverity.Error);
    }

    private static string NewDatabasePath(string scenario) =>
        Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_storage_fault_{scenario}_{Guid.NewGuid():N}.db");

    private static void DeleteDatabaseFiles(string databasePath)
    {
        foreach (string path in new[] { databasePath, databasePath + ".wal" })
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    private sealed class OneShotCommitFailureStorageEngineFactory :
        IStorageEngineFactory
    {
        private readonly OneShotCommitFailureWalFlushPolicy _flushPolicy =
            new OneShotCommitFailureWalFlushPolicy();

        public void ArmCommitFailure() => _flushPolicy.Arm();

        public async ValueTask<StorageEngineContext> OpenAsync(
            string filePath,
            StorageEngineOptions options,
            CancellationToken ct = default)
        {
            FileStorageDevice? device = null;
            Pager? pager = null;

            try
            {
                device = new FileStorageDevice(
                    filePath,
                    createNew: false,
                    options.PrimaryFileShare);
                var walIndex = new WalIndex();
                var wal = new WriteAheadLog(
                    filePath,
                    walIndex,
                    options.ChecksumProvider,
                    _flushPolicy,
                    options.DurableGroupCommit.BatchWindow,
                    options.WalPreallocationChunkBytes);
                pager = await Pager.CreateAsync(
                    device,
                    wal,
                    walIndex,
                    options.PagerOptions,
                    ct);
                await pager.RecoverAsync(ct);

                var schemaSerializer = options.SerializerProvider.SchemaSerializer;
                return new StorageEngineContext
                {
                    Pager = pager,
                    Catalog = await SchemaCatalog.CreateAsync(
                        pager,
                        schemaSerializer,
                        options.IndexProvider,
                        options.CatalogStore,
                        options.AdvisoryStatisticsPersistenceMode,
                        ct),
                    RecordSerializer = options.SerializerProvider.RecordSerializer,
                    SchemaSerializer = schemaSerializer,
                    IndexProvider = options.IndexProvider,
                    CatalogStore = options.CatalogStore,
                    ChecksumProvider = options.ChecksumProvider,
                    AdvisoryStatisticsPersistenceMode =
                        options.AdvisoryStatisticsPersistenceMode,
                };
            }
            catch
            {
                if (pager is not null)
                    await pager.DisposeAsync();
                if (device is not null)
                    await device.DisposeAsync();
                throw;
            }
        }
    }

    private sealed class OneShotCommitFailureWalFlushPolicy : IWalFlushPolicy
    {
        private int _failNextCommit;

        public bool AllowsWriteConcurrencyDuringCommitFlush => true;

        public void Arm() => Interlocked.Exchange(ref _failNextCommit, 1);

        public ValueTask FlushCommitAsync(
            SafeFileHandle handle,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (Interlocked.Exchange(ref _failNextCommit, 0) != 0)
            {
                return ValueTask.FromException(
                    new IOException("Injected WAL commit flush failure."));
            }

            return DurableWalFlushPolicy.Instance.FlushCommitAsync(
                handle,
                cancellationToken);
        }
    }
}
