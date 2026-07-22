using CSharpDB.Engine;
using CSharpDB.Migration.CSharpDb;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Tests;

public sealed class CSharpDbStagedMigrationTargetTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task FullSyntheticApply_CreatesBoundStagedTargetWithSchemaDataAndValidationSnapshot()
    {
        using var files = new TemporaryTargetDirectory();
        MigrationCatalog catalog = await InspectAsync(Ct);
        MigrationPlan plan = ReadyPlan(catalog, batchSize: 2);
        string targetIdentity;

        await using (var source = new SyntheticMigrationDataSource(catalog))
        await using (CSharpDbStagedMigrationTarget target = await CSharpDbStagedMigrationTarget.CreateNewAsync(
                         files.TargetPath,
                         plan,
                         catalog,
                         source.SnapshotIdentity,
                         cancellationToken: Ct))
        {
            MigrationApplyResult result = await ApplyAsync(plan, catalog, source, target, Ct);
            targetIdentity = target.TargetIdentity;

            Assert.Equal(MigrationApplyStatus.AwaitingValidation, result.Status);
            Assert.Equal(targetIdentity, result.TargetIdentity);
            Assert.True(Guid.TryParse(targetIdentity, out _));
            Assert.Equal(11, result.BatchesWritten);
            Assert.Equal(21, result.RowsWritten);

            await using IValidationSnapshot snapshot = await target.OpenValidationSnapshotAsync(Ct);
            Assert.Equal($"staged-target:{targetIdentity}:awaiting-validation", snapshot.SnapshotIdentity);
            await AssertSyntheticCountsAsync(snapshot, Ct);

            List<MigrationValidationRow> customers = await CollectAsync(
                snapshot.ReadRowsAsync("syn:table:customers-upper", Ct));
            int payloadIndex = IncludedColumnIds(catalog, plan, "syn:table:customers-upper")
                .ToList()
                .IndexOf("syn:column:customers-upper:payload");
            Assert.True(payloadIndex >= 0);
            Assert.Equal(4, customers.Count);
            Assert.Single(customers, row => row.Values[payloadIndex].IsNull);
            Assert.Equal(3, customers.Count(row => row.Values[payloadIndex].Type == DbType.Blob));
            Assert.Contains(
                customers,
                row => row.Values[payloadIndex].Type == DbType.Blob &&
                    row.Values[payloadIndex].AsBlob.AsSpan().SequenceEqual(
                        new byte[] { 0x43, 0x53, 0x44, 0x42, 0x01 }));
        }

        Assert.True(File.Exists(files.TargetPath));
        Assert.False(File.Exists(files.LeasePath));
        Assert.Equal(
            Enum.GetNames<MigrationSchemaStage>(),
            await ReadStageNamesAsync(files.TargetPath, Ct));
    }

    [Fact]
    public async Task ReopenResume_PreservesTargetIdentityAndSkipsEveryExactReceipt()
    {
        using var files = new TemporaryTargetDirectory();
        MigrationCatalog catalog = await InspectAsync(Ct);
        MigrationPlan plan = ReadyPlan(catalog, batchSize: 2);
        string targetIdentity;

        await using (var source = new SyntheticMigrationDataSource(catalog))
        await using (CSharpDbStagedMigrationTarget target = await CSharpDbStagedMigrationTarget.CreateNewAsync(
                         files.TargetPath,
                         plan,
                         catalog,
                         source.SnapshotIdentity,
                         cancellationToken: Ct))
        {
            targetIdentity = target.TargetIdentity;
            _ = await ApplyAsync(plan, catalog, source, target, Ct);
        }

        await using (var source = new SyntheticMigrationDataSource(catalog))
        await using (CSharpDbStagedMigrationTarget resumedTarget = await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                         files.TargetPath,
                         plan,
                         catalog,
                         source.SnapshotIdentity,
                         cancellationToken: Ct))
        {
            MigrationApplyResult resumed = await ApplyAsync(plan, catalog, source, resumedTarget, Ct);

            Assert.Equal(targetIdentity, resumedTarget.TargetIdentity);
            Assert.Equal(0, resumed.BatchesWritten);
            Assert.Equal(11, resumed.BatchesSkipped);
            Assert.Equal(0, resumed.RowsWritten);
            Assert.Equal(21, resumed.RowsSkipped);
            await using IValidationSnapshot snapshot = await resumedTarget.OpenValidationSnapshotAsync(Ct);
            await AssertSyntheticCountsAsync(snapshot, Ct);
        }
    }

    [Fact]
    public async Task CreateNew_RefusesExistingTargetWithoutMutatingIt()
    {
        using var files = new TemporaryTargetDirectory();
        MigrationCatalog catalog = await InspectAsync(Ct);
        MigrationPlan plan = ReadyPlan(catalog, batchSize: 2);
        byte[] original = [0x63, 0x73, 0x64, 0x62, 0x00, 0xff];
        await File.WriteAllBytesAsync(files.TargetPath, original, Ct);

        IOException error = await Assert.ThrowsAsync<IOException>(async () =>
        {
            await using CSharpDbStagedMigrationTarget unexpected =
                await CSharpDbStagedMigrationTarget.CreateNewAsync(
                    files.TargetPath,
                    plan,
                    catalog,
                    SyntheticMigrationDataSource.FixtureSnapshotIdentity,
                    cancellationToken: Ct);
        });

        Assert.Contains("already exists", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(original, await File.ReadAllBytesAsync(files.TargetPath, Ct));
        Assert.False(File.Exists(files.WalPath));
        Assert.False(File.Exists(files.LeasePath));
    }

    [Fact]
    public async Task CreateNew_RefusesOrphanWalWithoutCreatingOrMutatingTarget()
    {
        using var files = new TemporaryTargetDirectory();
        MigrationCatalog catalog = await InspectAsync(Ct);
        MigrationPlan plan = ReadyPlan(catalog, batchSize: 2);
        byte[] originalWal = [0xde, 0xad, 0xbe, 0xef];
        await File.WriteAllBytesAsync(files.WalPath, originalWal, Ct);

        IOException error = await Assert.ThrowsAsync<IOException>(async () =>
        {
            await using CSharpDbStagedMigrationTarget unexpected =
                await CSharpDbStagedMigrationTarget.CreateNewAsync(
                    files.TargetPath,
                    plan,
                    catalog,
                    SyntheticMigrationDataSource.FixtureSnapshotIdentity,
                    cancellationToken: Ct);
        });

        Assert.Contains("companion WAL", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.False(File.Exists(files.TargetPath));
        Assert.Equal(originalWal, await File.ReadAllBytesAsync(files.WalPath, Ct));
        Assert.False(File.Exists(files.LeasePath));
    }

    [Fact]
    public async Task Resume_RefusesChangedPlanCatalogAndSourceSnapshotBindings()
    {
        using var files = new TemporaryTargetDirectory();
        MigrationCatalog catalog = await InspectAsync(Ct);
        MigrationPlan plan = ReadyPlan(catalog, batchSize: 2);

        await using (CSharpDbStagedMigrationTarget target = await CSharpDbStagedMigrationTarget.CreateNewAsync(
                         files.TargetPath,
                         plan,
                         catalog,
                         SyntheticMigrationDataSource.FixtureSnapshotIdentity,
                         cancellationToken: Ct))
        {
        }

        MigrationPlan changedPlan = plan with
        {
            Load = plan.Load with { BatchSize = plan.Load.BatchSize + 1 },
        };
        await Assert.ThrowsAsync<InvalidDataException>(() => OpenAndDisposeAsync(
            files.TargetPath,
            changedPlan,
            catalog,
            SyntheticMigrationDataSource.FixtureSnapshotIdentity,
            Ct));

        MigrationCatalogObject changedObject = catalog.Objects.Single(
            item => item.ObjectId == "syn:table:reserved");
        MigrationCatalog changedCatalog = catalog with
        {
            Objects = catalog.Objects
                .Select(item => item.ObjectId == changedObject.ObjectId
                    ? item with { SourceName = item.SourceName + " changed" }
                    : item)
                .ToArray(),
        };
        MigrationPlan changedCatalogPlan = ReadyPlan(changedCatalog, batchSize: 2);
        await Assert.ThrowsAsync<InvalidDataException>(() => OpenAndDisposeAsync(
            files.TargetPath,
            changedCatalogPlan,
            changedCatalog,
            SyntheticMigrationDataSource.FixtureSnapshotIdentity,
            Ct));

        await Assert.ThrowsAsync<InvalidDataException>(() => OpenAndDisposeAsync(
            files.TargetPath,
            plan,
            catalog,
            "synthetic-snapshot:changed",
            Ct));

        Assert.True(File.Exists(files.TargetPath));
        Assert.False(File.Exists(files.LeasePath));
    }

    [Fact]
    public async Task Resume_RejectsSafelyTamperedReceiptDigest()
    {
        using var files = new TemporaryTargetDirectory();
        MigrationCatalog catalog = await InspectAsync(Ct);
        MigrationPlan plan = ReadyPlan(catalog, batchSize: 2);

        await using (var source = new SyntheticMigrationDataSource(catalog))
        await using (CSharpDbStagedMigrationTarget target = await CSharpDbStagedMigrationTarget.CreateNewAsync(
                         files.TargetPath,
                         plan,
                         catalog,
                         source.SnapshotIdentity,
                         cancellationToken: Ct))
        {
            _ = await ApplyAsync(plan, catalog, source, target, Ct);
        }

        await TamperReceiptDigestAsync(files.TargetPath, Ct);

        await using var resumedSource = new SyntheticMigrationDataSource(catalog);
        await using CSharpDbStagedMigrationTarget resumedTarget =
            await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                files.TargetPath,
                plan,
                catalog,
                resumedSource.SnapshotIdentity,
                cancellationToken: Ct);
        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await ApplyAsync(plan, catalog, resumedSource, resumedTarget, Ct));

        Assert.Contains("receipt mismatch", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(CSharpDbMigrationFaultPoint.BeforeRows, false)]
    [InlineData(CSharpDbMigrationFaultPoint.AfterRowsBeforeReceipt, false)]
    [InlineData(CSharpDbMigrationFaultPoint.AfterReceiptBeforeCommit, false)]
    [InlineData(CSharpDbMigrationFaultPoint.AfterCommit, true)]
    public async Task FaultInjection_RollsBackPreCommitOrResumesCommittedAfterAck(
        CSharpDbMigrationFaultPoint point,
        bool firstBatchCommitted)
    {
        using var files = new TemporaryTargetDirectory();
        MigrationCatalog catalog = await InspectAsync(Ct);
        MigrationPlan plan = ReadyPlan(catalog, batchSize: 2);
        var injector = new ThrowOnceFaultInjector(point);
        string targetIdentity;

        await using (var source = new SyntheticMigrationDataSource(catalog))
        await using (CSharpDbStagedMigrationTarget target = await CSharpDbStagedMigrationTarget.CreateNewAsync(
                         files.TargetPath,
                         plan,
                         catalog,
                         source.SnapshotIdentity,
                         injector,
                         Ct))
        {
            targetIdentity = target.TargetIdentity;
            await Assert.ThrowsAsync<InjectedMigrationFaultException>(async () =>
                await ApplyAsync(plan, catalog, source, target, Ct));
            Assert.True(injector.Fired);
        }

        await using (var source = new SyntheticMigrationDataSource(catalog))
        await using (CSharpDbStagedMigrationTarget resumedTarget = await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                         files.TargetPath,
                         plan,
                         catalog,
                         source.SnapshotIdentity,
                         cancellationToken: Ct))
        {
            MigrationApplyResult resumed = await ApplyAsync(plan, catalog, source, resumedTarget, Ct);

            Assert.Equal(targetIdentity, resumedTarget.TargetIdentity);
            Assert.Equal(firstBatchCommitted ? 10 : 11, resumed.BatchesWritten);
            Assert.Equal(firstBatchCommitted ? 1 : 0, resumed.BatchesSkipped);
            Assert.Equal(firstBatchCommitted ? 19 : 21, resumed.RowsWritten);
            Assert.Equal(firstBatchCommitted ? 2 : 0, resumed.RowsSkipped);
            await using IValidationSnapshot snapshot = await resumedTarget.OpenValidationSnapshotAsync(Ct);
            await AssertSyntheticCountsAsync(snapshot, Ct);
        }
    }

    [Fact]
    public async Task CancellationAfterReceiptInsertion_RollsBackRowsAndReceiptBeforeReopenResume()
    {
        using var files = new TemporaryTargetDirectory();
        MigrationCatalog catalog = await InspectAsync(Ct);
        MigrationPlan plan = ReadyPlan(catalog, batchSize: 2);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var injector = new CancelAtPointFaultInjector(
            CSharpDbMigrationFaultPoint.AfterReceiptBeforeCommit,
            cancellation);

        await using (var source = new SyntheticMigrationDataSource(catalog))
        await using (CSharpDbStagedMigrationTarget target = await CSharpDbStagedMigrationTarget.CreateNewAsync(
                         files.TargetPath,
                         plan,
                         catalog,
                         source.SnapshotIdentity,
                         injector,
                         Ct))
        {
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                await ApplyAsync(plan, catalog, source, target, cancellation.Token));
            Assert.True(injector.Fired);
        }

        await using (var source = new SyntheticMigrationDataSource(catalog))
        await using (CSharpDbStagedMigrationTarget resumedTarget = await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                         files.TargetPath,
                         plan,
                         catalog,
                         source.SnapshotIdentity,
                         cancellationToken: Ct))
        {
            MigrationApplyResult resumed = await ApplyAsync(plan, catalog, source, resumedTarget, Ct);

            Assert.Equal(11, resumed.BatchesWritten);
            Assert.Equal(0, resumed.BatchesSkipped);
            Assert.Equal(21, resumed.RowsWritten);
            await using IValidationSnapshot snapshot = await resumedTarget.OpenValidationSnapshotAsync(Ct);
            await AssertSyntheticCountsAsync(snapshot, Ct);
        }
    }

    [Fact]
    public async Task TargetLease_IsExclusiveUntilOwnerDisposesThenResumeCanAcquireIt()
    {
        using var files = new TemporaryTargetDirectory();
        MigrationCatalog catalog = await InspectAsync(Ct);
        MigrationPlan plan = ReadyPlan(catalog, batchSize: 2);
        string targetIdentity;

        await using (CSharpDbStagedMigrationTarget target = await CSharpDbStagedMigrationTarget.CreateNewAsync(
                         files.TargetPath,
                         plan,
                         catalog,
                         SyntheticMigrationDataSource.FixtureSnapshotIdentity,
                         cancellationToken: Ct))
        {
            targetIdentity = target.TargetIdentity;
            IOException error = await Assert.ThrowsAsync<IOException>(() => OpenAndDisposeAsync(
                files.TargetPath,
                plan,
                catalog,
                SyntheticMigrationDataSource.FixtureSnapshotIdentity,
                Ct));
            Assert.Contains("leased", error.Message, StringComparison.OrdinalIgnoreCase);
            Assert.True(File.Exists(files.LeasePath));
        }

        Assert.False(File.Exists(files.LeasePath));
        await using CSharpDbStagedMigrationTarget resumed = await CSharpDbStagedMigrationTarget.OpenResumeAsync(
            files.TargetPath,
            plan,
            catalog,
            SyntheticMigrationDataSource.FixtureSnapshotIdentity,
            cancellationToken: Ct);
        Assert.Equal(targetIdentity, resumed.TargetIdentity);
    }

    private static async Task<MigrationCatalog> InspectAsync(CancellationToken cancellationToken) =>
        await new SyntheticMigrationSourceInspector().InspectAsync(
            new MigrationInspectionRequest
            {
                TargetCSharpDbVersion = CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                IncludeProfile = true,
                ProfileSampleSize = 5,
            },
            cancellationToken);

    private static MigrationPlan ReadyPlan(MigrationCatalog catalog, int batchSize)
    {
        MigrationPlan plan = new MigrationPlanner().CreatePlan(
            catalog,
            new MigrationPlanningOptions { AcceptAllExclusions = true });
        return plan with { Load = plan.Load with { BatchSize = batchSize } };
    }

    private static async ValueTask<MigrationApplyResult> ApplyAsync(
        MigrationPlan plan,
        MigrationCatalog catalog,
        IMigrationDataSource source,
        IMigrationTarget target,
        CancellationToken cancellationToken) =>
        await new MigrationApplyRunner().ApplyAsync(
            new MigrationApplyRequest
            {
                Plan = plan,
                Catalog = catalog,
                Source = source,
                Target = target,
            },
            cancellationToken);

    private static async Task OpenAndDisposeAsync(
        string path,
        MigrationPlan plan,
        MigrationCatalog catalog,
        string snapshotIdentity,
        CancellationToken cancellationToken)
    {
        await using CSharpDbStagedMigrationTarget target = await CSharpDbStagedMigrationTarget.OpenResumeAsync(
            path,
            plan,
            catalog,
            snapshotIdentity,
            cancellationToken: cancellationToken);
    }

    private static async Task AssertSyntheticCountsAsync(
        IValidationSnapshot snapshot,
        CancellationToken cancellationToken)
    {
        Assert.Equal(3, await snapshot.CountAsync("syn:table:customers-lower", cancellationToken));
        Assert.Equal(4, await snapshot.CountAsync("syn:table:customers-upper", cancellationToken));
        Assert.Equal(12, await snapshot.CountAsync("syn:table:orders", cancellationToken));
        Assert.Equal(2, await snapshot.CountAsync("syn:table:reserved", cancellationToken));
    }

    private static string[] IncludedColumnIds(
        MigrationCatalog catalog,
        MigrationPlan plan,
        string tableObjectId)
    {
        IReadOnlySet<string> included = plan.Objects
            .Where(item => item.Included)
            .Select(item => item.SourceObjectId)
            .ToHashSet(StringComparer.Ordinal);
        return catalog.Objects
            .Where(item => item.Kind == MigrationObjectKind.Column &&
                item.ParentObjectId == tableObjectId && included.Contains(item.ObjectId))
            .OrderBy(item => item.ObjectId, StringComparer.Ordinal)
            .Select(item => item.ObjectId)
            .ToArray();
    }

    private static async Task<List<T>> CollectAsync<T>(IAsyncEnumerable<T> source)
    {
        var values = new List<T>();
        await foreach (T value in source)
            values.Add(value);
        return values;
    }

    private static async Task<string[]> ReadStageNamesAsync(
        string targetPath,
        CancellationToken cancellationToken)
    {
        await using Database database = await Database.OpenAsync(targetPath, cancellationToken);
        await using var result = await database.ExecuteAsync(
            "SELECT \"stage_name\" FROM \"__csharpdb_migration_stages\" ORDER BY \"stage_ordinal\"",
            cancellationToken);
        var stages = new List<string>();
        await foreach (DbValue[] row in result.GetRowsAsync(cancellationToken))
            stages.Add(row[0].AsText);
        return stages.ToArray();
    }

    private static async Task TamperReceiptDigestAsync(
        string targetPath,
        CancellationToken cancellationToken)
    {
        await using Database database = await Database.OpenAsync(targetPath, cancellationToken);
        await using var result = await database.ExecuteAsync(
            "UPDATE \"__csharpdb_migration_receipts\" " +
            $"SET \"batch_digest\" = '{new string('0', 64)}' " +
            "WHERE \"source_object_id\" = 'syn:table:customers-lower' AND \"batch_ordinal\" = 0",
            cancellationToken);
        Assert.Equal(1, result.RowsAffected);
    }

    private sealed class ThrowOnceFaultInjector(CSharpDbMigrationFaultPoint faultPoint)
        : ICSharpDbMigrationFaultInjector
    {
        private int _fired;

        public bool Fired => Volatile.Read(ref _fired) != 0;

        public ValueTask InjectAsync(
            CSharpDbMigrationFaultPoint point,
            MigrationTargetBatch batch,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (point == faultPoint && Interlocked.Exchange(ref _fired, 1) == 0)
                throw new InjectedMigrationFaultException(point);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class CancelAtPointFaultInjector(
        CSharpDbMigrationFaultPoint faultPoint,
        CancellationTokenSource cancellation) : ICSharpDbMigrationFaultInjector
    {
        private int _fired;

        public bool Fired => Volatile.Read(ref _fired) != 0;

        public ValueTask InjectAsync(
            CSharpDbMigrationFaultPoint point,
            MigrationTargetBatch batch,
            CancellationToken cancellationToken = default)
        {
            if (point == faultPoint && Interlocked.Exchange(ref _fired, 1) == 0)
                cancellation.Cancel();
            return ValueTask.CompletedTask;
        }
    }

    private sealed class InjectedMigrationFaultException(CSharpDbMigrationFaultPoint point)
        : IOException($"Injected migration fault at {point}.");

    private sealed class TemporaryTargetDirectory : IDisposable
    {
        public TemporaryTargetDirectory()
        {
            DirectoryPath = Path.Combine(
                Path.GetTempPath(),
                $"csharpdb-migration-target-tests-{Guid.NewGuid():N}");
            Directory.CreateDirectory(DirectoryPath);
            TargetPath = Path.Combine(DirectoryPath, "staged.csdb");
        }

        public string DirectoryPath { get; }

        public string TargetPath { get; }

        public string WalPath => TargetPath + ".wal";

        public string LeasePath => TargetPath + ".migration.lock";

        public void Dispose()
        {
            try
            {
                Directory.Delete(DirectoryPath, recursive: true);
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }
    }
}
