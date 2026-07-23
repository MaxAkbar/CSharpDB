using System.Runtime.CompilerServices;
using CSharpDB.Migration.CSharpDb;

namespace CSharpDB.Migration.Tests;

public sealed class MigrationFailFastContractTests
{
    private const string RejectedSourceValue = "TOP-SECRET-ROW-VALUE";

    [Fact]
    public void SourceRejectionFactoryAcceptsOnlyBoundedStableMetadata()
    {
        MigrationRowRejectedException error = MigrationRowRejectedException.CreateForSource(
            "MIG-SOURCE-VALUE-001",
            "source:table",
            "source:column",
            batchOrdinal: 2,
            sourceRowOrdinal: 3);

        Assert.Equal("MIG-SOURCE-VALUE-001", error.Code);
        Assert.IsType<InvalidDataException>(error.InnerException);
        Assert.Throws<ArgumentException>(() => MigrationRowRejectedException.CreateForSource(
            "MIG-source-value",
            "source:table",
            "source:column",
            0,
            0));
        Assert.Throws<ArgumentException>(() => MigrationRowRejectedException.CreateForSource(
            "MIG-SOURCE-VALUE-001",
            new string('x', 513),
            "source:column",
            0,
            0));
        Assert.Throws<ArgumentException>(() => MigrationRowRejectedException.CreateForSource(
            "MIG-SOURCE-VALUE-001",
            "source:table\r\nraw",
            "source:column",
            0,
            0));
    }

    [Fact]
    public async Task FailFast_ReplaysTheSameSafeFirstErrorAndNeverSubmitsItsBatch()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using var files = new TemporaryTargetDirectory();
        MigrationCatalog catalog = await InspectAsync(ct);
        MigrationPlan plan = ReadyPlan(catalog, batchSize: 2);
        MigrationRowRejectedException first;

        await using (var source = new RejectingSyntheticSource(catalog))
        await using (CSharpDbStagedMigrationTarget target = await CSharpDbStagedMigrationTarget.CreateNewAsync(
                         files.TargetPath,
                         plan,
                         catalog,
                         source.SnapshotIdentity,
                         cancellationToken: ct))
        {
            first = await Assert.ThrowsAsync<MigrationRowRejectedException>(async () =>
                await ApplyAsync(plan, catalog, source, target, ct));

            AssertRejection(first);
            MigrationBatchReceipt receipt = Assert.IsType<MigrationBatchReceipt>(
                await target.ReadReceiptAsync(
                    MigrationArtifactSerializer.ComputePlanDigest(plan),
                    "syn:table:customers-lower",
                    batchOrdinal: 0,
                    ct));
            Assert.Equal(2, receipt.RowCount);
            Assert.Null(await target.ReadReceiptAsync(
                receipt.PlanDigest,
                "syn:table:customers-lower",
                batchOrdinal: 1,
                ct));
        }

        await using (var source = new RejectingSyntheticSource(catalog))
        await using (CSharpDbStagedMigrationTarget target = await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                         files.TargetPath,
                         plan,
                         catalog,
                         source.SnapshotIdentity,
                         cancellationToken: ct))
        {
            MigrationRowRejectedException resumed =
                await Assert.ThrowsAsync<MigrationRowRejectedException>(async () =>
                    await ApplyAsync(plan, catalog, source, target, ct));

            AssertRejection(resumed);
            Assert.Equal(first.Message, resumed.Message);
            Assert.Equal(first.Code, resumed.Code);
            Assert.Equal(first.SourceObjectId, resumed.SourceObjectId);
            Assert.Equal(first.ColumnObjectId, resumed.ColumnObjectId);
            Assert.Equal(first.BatchOrdinal, resumed.BatchOrdinal);
            Assert.Equal(first.SourceRowOrdinal, resumed.SourceRowOrdinal);
            Assert.Null(await target.ReadReceiptAsync(
                MigrationArtifactSerializer.ComputePlanDigest(plan),
                "syn:table:customers-lower",
                batchOrdinal: 1,
                ct));
        }
    }

    [Fact]
    public async Task DurableRejectMode_NormalExecutionIsRefusedBeforeTargetMutation()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        MigrationCatalog catalog = await InspectAsync(ct);
        MigrationPlan ready = ReadyPlan(catalog, batchSize: 2);
        MigrationPlan unsupported = ready with
        {
            Load = ready.Load with
            {
                RejectMode = MigrationRejectMode.DeterministicRejects,
                RejectPolicy = new MigrationDeterministicRejectPolicy
                {
                    ContractVersion = MigrationRejectContract.DeterministicRejectsV1,
                    AllowedRuleIds = ["MIG-TEST-001"],
                    MaxRejectedRowsPerBatch = 1,
                    MaxRejectedRowsPerRun = 10,
                    MaxRawValueBytes = 1_024,
                    MaxRawValueBytesPerBatch = 8_192,
                    MaxRawValueBytesPerRun = 65_536,
                    MaxArtifactBytes = 131_072,
                },
            },
        };
        await using var source = new SyntheticMigrationDataSource(catalog);
        await using var target = new MutationProbeTarget();

        MigrationExecutionPolicyException error =
            await Assert.ThrowsAsync<MigrationExecutionPolicyException>(async () =>
                await ApplyAsync(unsupported, catalog, source, target, ct));

        Assert.Equal("MIG-APPLY-POLICY-REJECT-001", error.Code);
        Assert.Contains(MigrationRejectContract.DeterministicFailFastV1, error.Message, StringComparison.Ordinal);
        Assert.Equal(0, target.OperationCount);
    }

    [Fact]
    public async Task PreCanceledApply_DoesNotInvokeAnyTargetOperation()
    {
        CancellationToken testCancellation = TestContext.Current.CancellationToken;
        MigrationCatalog catalog = await InspectAsync(testCancellation);
        MigrationPlan plan = ReadyPlan(catalog, batchSize: 2);
        await using var source = new SyntheticMigrationDataSource(catalog);
        await using var target = new MutationProbeTarget();
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(testCancellation);
        await cancellation.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await ApplyAsync(plan, catalog, source, target, cancellation.Token));

        Assert.Equal(0, target.OperationCount);
    }

    private static void AssertRejection(MigrationRowRejectedException error)
    {
        Assert.Equal(MigrationRejectContract.DeterministicFailFastV1, error.ContractVersion);
        Assert.Equal("MIG-APPLY-KIND-001", error.Code);
        Assert.Equal("syn:table:customers-lower", error.SourceObjectId);
        Assert.Equal("syn:column:customers-lower:code-lower", error.ColumnObjectId);
        Assert.Equal(1, error.BatchOrdinal);
        Assert.Equal(2, error.SourceRowOrdinal);
        Assert.IsType<MigrationValueException>(error.InnerException);
        Assert.DoesNotContain(RejectedSourceValue, error.Message, StringComparison.Ordinal);
    }

    private static async ValueTask<MigrationApplyResult> ApplyAsync(
        MigrationPlan plan,
        MigrationCatalog catalog,
        IMigrationDataSource source,
        IMigrationTarget target,
        CancellationToken ct) =>
        await new MigrationApplyRunner().ApplyAsync(
            new MigrationApplyRequest
            {
                Plan = plan,
                Catalog = catalog,
                Source = source,
                Target = target,
            },
            ct);

    private static async Task<MigrationCatalog> InspectAsync(CancellationToken ct) =>
        await new SyntheticMigrationSourceInspector().InspectAsync(
            new MigrationInspectionRequest
            {
                TargetCSharpDbVersion = CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                IncludeProfile = true,
                ProfileSampleSize = 5,
            },
            ct);

    private static MigrationPlan ReadyPlan(MigrationCatalog catalog, int batchSize)
    {
        MigrationPlan plan = new MigrationPlanner().CreatePlan(
            catalog,
            new MigrationPlanningOptions { AcceptAllExclusions = true });
        return plan with { Load = plan.Load with { BatchSize = batchSize } };
    }

    private sealed class RejectingSyntheticSource : IMigrationDataSource
    {
        private readonly SyntheticMigrationDataSource _inner;

        public RejectingSyntheticSource(MigrationCatalog catalog) =>
            _inner = new SyntheticMigrationDataSource(catalog);

        public MigrationSourceIdentity Source => _inner.Source;

        public string SnapshotIdentity => _inner.SnapshotIdentity;

        public async IAsyncEnumerable<MigrationDataBatch> ReadAsync(
            MigrationReadRequest request,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await foreach (MigrationDataBatch batch in _inner.ReadAsync(request, cancellationToken))
            {
                if (batch.SourceObjectId == "syn:table:customers-lower" && batch.BatchOrdinal == 1)
                {
                    MigrationDataRow[] rows = batch.Rows.ToArray();
                    MigrationSourceValue[] values = rows[0].Values.ToArray();
                    values[0] = new MigrationSourceValue
                    {
                        Kind = MigrationSourceValueKind.SignedInteger,
                        CanonicalText = RejectedSourceValue,
                    };
                    values[1] = new MigrationSourceValue
                    {
                        Kind = MigrationSourceValueKind.SignedInteger,
                        CanonicalText = RejectedSourceValue,
                    };
                    rows[0] = rows[0] with { Values = values };
                    yield return batch with { Rows = rows };
                }
                else
                {
                    yield return batch;
                }
            }
        }

        public ValueTask DisposeAsync() => _inner.DisposeAsync();
    }

    private sealed class MutationProbeTarget : IMigrationTarget
    {
        public string TargetIdentity => "probe:target";

        public int OperationCount { get; private set; }

        public ValueTask ApplySchemaAsync(
            MigrationPlan plan,
            MigrationCatalog catalog,
            MigrationSchemaStage stage,
            CancellationToken cancellationToken = default)
        {
            OperationCount++;
            return ValueTask.CompletedTask;
        }

        public ValueTask<MigrationBatchReceipt> WriteBatchAsync(
            MigrationTargetBatch batch,
            CancellationToken cancellationToken = default)
        {
            OperationCount++;
            throw new InvalidOperationException("A pre-canceled apply reached the target writer.");
        }

        public ValueTask<MigrationBatchReceipt?> ReadReceiptAsync(
            string planDigest,
            string sourceObjectId,
            long batchOrdinal,
            CancellationToken cancellationToken = default)
        {
            OperationCount++;
            return ValueTask.FromResult<MigrationBatchReceipt?>(null);
        }

        public async IAsyncEnumerable<MigrationBatchReceipt> ReadReceiptsAsync(
            string planDigest,
            string sourceObjectId,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            OperationCount++;
            await Task.CompletedTask;
            yield break;
        }

        public ValueTask<IValidationSnapshot> OpenValidationSnapshotAsync(
            CancellationToken cancellationToken = default)
        {
            OperationCount++;
            throw new InvalidOperationException("A pre-canceled apply reached validation.");
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class TemporaryTargetDirectory : IDisposable
    {
        public TemporaryTargetDirectory()
        {
            DirectoryPath = Path.Combine(
                Path.GetTempPath(),
                $"csharpdb-migration-reject-tests-{Guid.NewGuid():N}");
            Directory.CreateDirectory(DirectoryPath);
            TargetPath = Path.Combine(DirectoryPath, "staged.csdb");
        }

        public string DirectoryPath { get; }

        public string TargetPath { get; }

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
