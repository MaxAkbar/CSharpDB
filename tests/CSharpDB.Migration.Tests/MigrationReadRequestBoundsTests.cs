using System.Runtime.CompilerServices;

namespace CSharpDB.Migration.Tests;

public sealed class MigrationReadRequestBoundsTests
{
    [Fact]
    public void DefaultsMatchTheMigrationLoadPolicy()
    {
        var request = new MigrationReadRequest { SourceObjectId = "source:object" };
        var load = new MigrationLoadPolicy();

        Assert.Equal(load.BatchSize, request.BatchSize);
        Assert.Equal(load.MaxBatchBytes, request.MaxBatchBytes);
        Assert.Equal(load.MaxValueBytes, request.MaxValueBytes);
    }

    [Fact]
    public async Task ApplyAndValidationPropagateThePlanReadBounds()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        (MigrationCatalog catalog, MigrationPlan plan) = await ReadyPlanAsync();
        plan = plan with
        {
            Load = plan.Load with
            {
                BatchSize = 17,
                MaxBatchBytes = 23_456_789,
                MaxValueBytes = 1_234_567,
            },
        };

        await using (var source = new CapturingDataSource(catalog.Source, plan.CatalogDigest))
        await using (var target = new EmptyMigrationTarget())
        {
            await new MigrationApplyRunner().ApplyAsync(
                new MigrationApplyRequest
                {
                    Plan = plan,
                    Catalog = catalog,
                    Source = source,
                    Target = target,
                },
                cancellationToken);

            Assert.NotEmpty(source.Requests);
            Assert.All(source.Requests, request => AssertBounds(request, plan.Load));
        }

        await using (var source = new CapturingDataSource(catalog.Source, plan.CatalogDigest))
        await using (var snapshot = new MigrationDataSourceValidationSnapshot(plan, catalog, source))
        {
            string sourceObjectId = catalog.Objects
                .Where(item => item.Kind is MigrationObjectKind.Table or MigrationObjectKind.Collection)
                .Select(item => item.ObjectId)
                .First(item => plan.Objects.Single(planned => planned.SourceObjectId == item).Included);

            Assert.Equal(0, await snapshot.CountAsync(sourceObjectId, cancellationToken));
            await foreach (MigrationValidationRow _ in snapshot.ReadRowsAsync(
                               sourceObjectId,
                               cancellationToken))
            {
                throw new Xunit.Sdk.XunitException("The empty source unexpectedly emitted a row.");
            }

            Assert.Equal(2, source.Requests.Count);
            Assert.All(source.Requests, request => AssertBounds(request, plan.Load));
        }
    }

    [Fact]
    public async Task CoordinatorsRejectACatalogBoundSourceWithDifferentPolicy()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        (MigrationCatalog catalog, MigrationPlan plan) = await ReadyPlanAsync();
        await using var source = new CapturingDataSource(catalog.Source, new string('0', 64));
        await using var target = new EmptyMigrationTarget();

        InvalidDataException applyError = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await new MigrationApplyRunner().ApplyAsync(
                new MigrationApplyRequest
                {
                    Plan = plan,
                    Catalog = catalog,
                    Source = source,
                    Target = target,
                },
                cancellationToken));
        Assert.Contains("catalog policy", applyError.Message, StringComparison.Ordinal);

        InvalidDataException validationError = Assert.Throws<InvalidDataException>(() =>
            new MigrationDataSourceValidationSnapshot(plan, catalog, source));
        Assert.Contains("catalog policy", validationError.Message, StringComparison.Ordinal);
    }

    private static void AssertBounds(MigrationReadRequest request, MigrationLoadPolicy load)
    {
        Assert.Equal(load.BatchSize, request.BatchSize);
        Assert.Equal(load.MaxBatchBytes, request.MaxBatchBytes);
        Assert.Equal(load.MaxValueBytes, request.MaxValueBytes);
    }

    private static async Task<(MigrationCatalog Catalog, MigrationPlan Plan)> ReadyPlanAsync()
    {
        MigrationCatalog catalog = await new SyntheticMigrationSourceInspector().InspectAsync(
            new MigrationInspectionRequest
            {
                TargetCSharpDbVersion = CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                IncludeProfile = true,
                ProfileSampleSize = 5,
            },
            TestContext.Current.CancellationToken);
        MigrationPlan plan = new MigrationPlanner().CreatePlan(catalog);
        return (catalog, plan with
        {
            AcceptedExclusionObjectIds = plan.Objects
                .Where(item => !item.Included)
                .Select(item => item.SourceObjectId)
                .OrderBy(item => item, StringComparer.Ordinal)
                .ToArray(),
        });
    }

    private sealed class CapturingDataSource(
        MigrationSourceIdentity source,
        string catalogDigest) : IMigrationDataSource, IMigrationCatalogBoundDataSource
    {
        public MigrationSourceIdentity Source { get; } = source;

        public string SnapshotIdentity => SyntheticMigrationDataSource.FixtureSnapshotIdentity;

        public string CatalogDigest { get; } = catalogDigest;

        public List<MigrationReadRequest> Requests { get; } = [];

        public async IAsyncEnumerable<MigrationDataBatch> ReadAsync(
            MigrationReadRequest request,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Requests.Add(request);
            await Task.Yield();
            yield break;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class EmptyMigrationTarget : IMigrationTarget
    {
        public string TargetIdentity => "target:read-request-bounds";

        public ValueTask ApplySchemaAsync(
            MigrationPlan plan,
            MigrationCatalog catalog,
            MigrationSchemaStage stage,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.CompletedTask;
        }

        public ValueTask<MigrationBatchReceipt> WriteBatchAsync(
            MigrationTargetBatch batch,
            CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("The empty source unexpectedly emitted a batch.");

        public ValueTask<MigrationBatchReceipt?> ReadReceiptAsync(
            string planDigest,
            string sourceObjectId,
            long batchOrdinal,
            CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("The empty source unexpectedly emitted a batch.");

        public async IAsyncEnumerable<MigrationBatchReceipt> ReadReceiptsAsync(
            string planDigest,
            string sourceObjectId,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await Task.Yield();
            yield break;
        }

        public ValueTask<IValidationSnapshot> OpenValidationSnapshotAsync(
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
