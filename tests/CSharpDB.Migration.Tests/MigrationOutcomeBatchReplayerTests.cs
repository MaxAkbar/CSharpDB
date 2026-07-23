using System.Runtime.CompilerServices;
using CSharpDB.Migration.CSharpDb;
using CSharpDB.Migration.Validation;

namespace CSharpDB.Migration.Tests;

public sealed class MigrationOutcomeBatchReplayerTests
{
    private const string RejectRuleId = "MIG-TEST-REJECT-001";

    [Fact]
    public async Task ValidationReplay_PreservesMixedAndAllRejectOutcomes()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        (MigrationCatalog catalog, MigrationPlan failFast) = await ReadyPlanAsync();
        MigrationPlan plan = WithDeterministicRejects(failFast);
        await using var inner = new SyntheticMigrationDataSource(catalog);
        await using var source = new RejectingReplaySource(inner);
        await using var snapshot = new MigrationDataSourceValidationSnapshot(plan, catalog, source);

        List<MigrationTargetBatch> batches = await CollectAsync(
            snapshot.ReplayOutcomeBatchesAsync(cancellationToken));

        MigrationTargetBatch mixed = Assert.Single(batches, batch =>
            batch.SourceObjectId == "syn:table:customers-lower" &&
            batch.BatchOrdinal == 0);
        Assert.Equal([0L], mixed.Rows.Select(row => row.SourceRowOrdinal));
        Assert.Equal([1L], mixed.RejectedRows.Select(row => row.SourceRowOrdinal));
        Assert.Equal(MigrationRejectDigest.Compute(mixed), mixed.RejectDigest);
        Assert.Equal(MigrationBatchDigest.Compute(mixed), mixed.BatchDigest);

        MigrationTargetBatch allReject = Assert.Single(batches, batch =>
            batch.SourceObjectId == "syn:table:reserved");
        Assert.Empty(allReject.Rows);
        Assert.Equal([0L, 1L], allReject.RejectedRows.Select(row => row.SourceRowOrdinal));
        Assert.Null(allReject.NextCursor);

        Assert.Equal(
            2,
            await snapshot.CountAsync(
                "syn:table:customers-lower",
                cancellationToken));
        Assert.Equal(
            0,
            await snapshot.CountAsync(
                "syn:table:reserved",
                cancellationToken));
        List<MigrationValidationRow> accepted = await CollectAsync(
            snapshot.ReadRowsAsync(
                "syn:table:customers-lower",
                cancellationToken));
        Assert.Equal(2, accepted.Count);
    }

    [Fact]
    public async Task ValidationReplay_BindsEveryReadAndReplaysIdenticalDigests()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        (MigrationCatalog catalog, MigrationPlan failFast) = await ReadyPlanAsync();
        MigrationPlan plan = WithDeterministicRejects(failFast);
        await using var inner = new SyntheticMigrationDataSource(catalog);
        await using var source = new RejectingReplaySource(inner);
        await using var snapshot = new MigrationDataSourceValidationSnapshot(plan, catalog, source);

        List<MigrationTargetBatch> first = await CollectAsync(
            snapshot.ReplayOutcomeBatchesAsync(cancellationToken));
        List<MigrationTargetBatch> second = await CollectAsync(
            snapshot.ReplayOutcomeBatchesAsync(cancellationToken));

        Assert.Equal(
            first.Select(batch => batch.BatchDigest),
            second.Select(batch => batch.BatchDigest));
        Assert.Equal(
            first.Select(batch => batch.RejectDigest),
            second.Select(batch => batch.RejectDigest));

        int includedObjectCount = catalog.Objects.Count(item =>
            item.Kind is MigrationObjectKind.Table or MigrationObjectKind.Collection &&
            plan.Objects.Single(planObject =>
                planObject.SourceObjectId == item.ObjectId).Included);
        Assert.Equal(includedObjectCount * 2, source.Requests.Count);
        Assert.All(source.Requests, request =>
        {
            Assert.Equal(plan.Load.BatchSize, request.BatchSize);
            Assert.Equal(plan.Load.MaxBatchBytes, request.MaxBatchBytes);
            Assert.Equal(plan.Load.MaxValueBytes, request.MaxValueBytes);
            Assert.Equal(
                MigrationRejectContract.DeterministicRejectsV1,
                request.RejectContractVersion);
            Assert.Same(plan.Load.RejectPolicy, request.RejectPolicy);
            Assert.Equal(source.SnapshotIdentity, request.SnapshotToken);
            Assert.Null(request.ResumeCursor);

            string[] expectedColumns = catalog.Objects
                .Where(item => item.Kind == MigrationObjectKind.Column &&
                    string.Equals(
                        item.ParentObjectId,
                        request.SourceObjectId,
                        StringComparison.Ordinal) &&
                    plan.Objects.Single(planObject =>
                        planObject.SourceObjectId == item.ObjectId).Included)
                .OrderBy(item => item.ObjectId, StringComparer.Ordinal)
                .Select(item => item.ObjectId)
                .ToArray();
            Assert.Equal(expectedColumns, request.ColumnObjectIds);
        });
    }

    [Fact]
    public async Task FailFastReplay_PreservesCurrentAndLegacyDigestContracts()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        (MigrationCatalog catalog, MigrationPlan plan) = await ReadyPlanAsync();
        await using var inner = new SyntheticMigrationDataSource(catalog);
        await using var source = new TrackingFailFastSource(inner);
        var current = new MigrationOutcomeBatchReplayer(
            plan,
            catalog,
            source,
            MigrationBatchDigest.Format);
        var legacy = new MigrationOutcomeBatchReplayer(
            plan,
            catalog,
            source,
            MigrationBatchDigest.LegacyFormat);

        List<MigrationTargetBatch> currentBatches = (await CollectAsync(
                current.ReplayAsync(cancellationToken)))
            .Select(item => item.Batch)
            .ToList();
        List<MigrationTargetBatch> legacyBatches = (await CollectAsync(
                legacy.ReplayAsync(cancellationToken)))
            .Select(item => item.Batch)
            .ToList();

        Assert.Equal(currentBatches.Count, legacyBatches.Count);
        Assert.All(currentBatches, batch =>
        {
            Assert.Equal(
                MigrationRejectContract.DeterministicFailFastV1,
                batch.RejectContractVersion);
            Assert.Empty(batch.RejectedRows);
            Assert.Equal(MigrationBatchDigest.Compute(batch), batch.BatchDigest);
        });
        Assert.All(legacyBatches, batch =>
            Assert.Equal(
                MigrationBatchDigest.Compute(batch, MigrationBatchDigest.LegacyFormat),
                batch.BatchDigest));
        Assert.Equal(
            currentBatches.Select(batch => batch.Rows.Select(row => row.SourceRowOrdinal).ToArray()),
            legacyBatches.Select(batch => batch.Rows.Select(row => row.SourceRowOrdinal).ToArray()));
        Assert.All(source.Requests, request =>
        {
            Assert.Equal(
                MigrationRejectContract.DeterministicFailFastV1,
                request.RejectContractVersion);
            Assert.Null(request.RejectPolicy);
        });
    }

    [Fact]
    public async Task Replay_RejectsANonterminalFinalCursor()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        (MigrationCatalog catalog, MigrationPlan plan) = await ReadyPlanAsync();
        await using var inner = new SyntheticMigrationDataSource(catalog);
        await using var source = new NonterminalCursorSource(inner);
        var replayer = new MigrationOutcomeBatchReplayer(plan, catalog, source);

        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(
            async () => await CollectAsync(replayer.ReplayAsync(cancellationToken)));

        Assert.Contains("did not terminate", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Replay_CapturesSnapshotIdentityBeforeStreaming()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        (MigrationCatalog catalog, MigrationPlan plan) = await ReadyPlanAsync();
        await using var inner = new SyntheticMigrationDataSource(catalog);
        await using var source = new MutableSnapshotIdentitySource(inner);
        var replayer = new MigrationOutcomeBatchReplayer(plan, catalog, source);
        source.SnapshotIdentity = "synthetic-snapshot:mutated";

        List<MigrationReplayedOutcomeBatch> batches = await CollectAsync(
            replayer.ReplayAsync(cancellationToken));

        Assert.NotEmpty(batches);
        Assert.Equal(
            SyntheticMigrationDataSource.FixtureSnapshotIdentity,
            replayer.SnapshotIdentity);
        Assert.All(batches, item => Assert.Equal(
            SyntheticMigrationDataSource.FixtureSnapshotIdentity,
            item.Batch.SourceSnapshotIdentity));
        Assert.All(source.Requests, request => Assert.Equal(
            SyntheticMigrationDataSource.FixtureSnapshotIdentity,
            request.SnapshotToken));
    }

    [Fact]
    public async Task RejectAwareValidation_ValidatesRealStagedTargetAndActivates()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        using var files = new TemporaryTargetDirectory();
        (MigrationCatalog catalog, MigrationPlan failFast) = await ReadyPlanAsync();
        MigrationPlan plan = WithDeterministicRejects(failFast);
        await using var inner = new SyntheticMigrationDataSource(catalog);
        await using var source = new RejectingReplaySource(inner);
        MigrationApplyResult applied;
        MigrationValidationRunResult validated;
        MigrationValidationActivationReceipt receipt;

        await using (CSharpDbStagedMigrationTarget target =
                     await CSharpDbStagedMigrationTarget.CreateNewAsync(
                files.TargetPath,
                plan,
                catalog,
                source.SnapshotIdentity,
                cancellationToken: cancellationToken))
        {
            applied = await new MigrationApplyRunner().ApplyAsync(
                new MigrationApplyRequest
                {
                    Plan = plan,
                    Catalog = catalog,
                    Source = source,
                    Target = target,
                },
                cancellationToken);
            await using var sourceValidation = new MigrationDataSourceValidationSnapshot(
                plan,
                catalog,
                source);

            validated = await new MigrationValidationRunner().ValidateAsync(
                new MigrationValidationRunRequest
                {
                    Plan = plan,
                    Catalog = catalog,
                    SourceSnapshot = sourceValidation,
                    Target = target,
                    Level = MigrationValidationLevel.Checksum,
                    ReportOutputPath = files.ReportPath,
                    ChecksumOptions = new PartitionedChecksumValidatorOptions
                    {
                        SpillRootDirectory = files.DirectoryPath,
                        SortMemoryBudgetBytes = ValidationHashRecord.SerializedLength * 4,
                        MaxSpillBytes = 32 * 1024 * 1024,
                        MergeFanIn = 2,
                        MaxOpenFiles = 3,
                        MaxOpenPartitionWriters = 4,
                        MaxMismatchDetailsPerPartition = 10,
                    },
                },
                cancellationToken);

            receipt = await target.ReadActivationReceiptAsync(cancellationToken) ??
                throw new Xunit.Sdk.XunitException("Expected a persisted activation receipt.");
        }

        Assert.True(applied.RejectedRowsWritten > 0);
        Assert.Equal(MigrationValidationStatus.Passed, validated.Report.Outcome);
        Assert.True(validated.Activated);
        Assert.True(File.Exists(files.ReportPath));
        Assert.Equal(source.SnapshotIdentity, validated.Report.Binding.SourceSnapshotIdentity);
        Assert.Equal(validated.ReportDigest, receipt.ReportDigest);
        Assert.Equal(validated.Report.Binding.TargetSnapshotIdentity, receipt.TargetSnapshotIdentity);

        await using CSharpDbStagedMigrationTarget reopened =
            await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                files.TargetPath,
                plan,
                catalog,
                source.SnapshotIdentity,
                cancellationToken: cancellationToken);
        Assert.Equal(receipt, await reopened.ReadActivationReceiptAsync(cancellationToken));
        await using IValidationSnapshot opened =
            await reopened.OpenValidationSnapshotAsync(cancellationToken);
        IMigrationRejectTargetValidationSnapshot outcomes =
            Assert.IsAssignableFrom<IMigrationRejectTargetValidationSnapshot>(opened);
        Assert.Equal(receipt.TargetSnapshotIdentity, outcomes.SnapshotIdentity);
        Assert.Equal(
            applied.BatchesWritten,
            (long)(await CollectAsync(outcomes.ReadOutcomeReceiptsAsync(
                applied.PlanDigest,
                cancellationToken))).Count);
        Assert.Equal(
            applied.RejectedRowsWritten,
            (long)(await CollectAsync(outcomes.ReadRejectLedgerAsync(
                applied.PlanDigest,
                cancellationToken))).Count);
    }

    private static async Task<(MigrationCatalog Catalog, MigrationPlan Plan)> ReadyPlanAsync()
    {
        MigrationCatalog catalog = await new SyntheticMigrationSourceInspector().InspectAsync(
            new MigrationInspectionRequest
            {
                TargetCSharpDbVersion = CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                IncludeProfile = true,
                ProfileSampleSize = 5,
            });
        MigrationPlan planned = new MigrationPlanner().CreatePlan(catalog);
        return (catalog, planned with
        {
            AcceptedExclusionObjectIds = planned.Objects
                .Where(item => !item.Included)
                .Select(item => item.SourceObjectId)
                .OrderBy(item => item, StringComparer.Ordinal)
                .ToArray(),
            Load = planned.Load with
            {
                BatchSize = 2,
            },
        });
    }

    private static MigrationPlan WithDeterministicRejects(MigrationPlan plan) =>
        plan with
        {
            Load = plan.Load with
            {
                RejectMode = MigrationRejectMode.DeterministicRejects,
                RejectPolicy = new MigrationDeterministicRejectPolicy
                {
                    ContractVersion = MigrationRejectContract.DeterministicRejectsV1,
                    AllowedRuleIds = [RejectRuleId],
                    MaxRejectedRowsPerBatch = plan.Load.BatchSize,
                    MaxRejectedRowsPerRun = 100,
                    MaxRawValueBytes = 1_024,
                    MaxRawValueBytesPerBatch = 8_192,
                    MaxRawValueBytesPerRun = 65_536,
                    MaxArtifactBytes = 131_072,
                },
            },
        };

    private static async Task<List<T>> CollectAsync<T>(IAsyncEnumerable<T> values)
    {
        var result = new List<T>();
        await foreach (T value in values)
            result.Add(value);
        return result;
    }

    private sealed class RejectingReplaySource(SyntheticMigrationDataSource inner) :
        IMigrationDataSource,
        IMigrationRejectAwareDataSource
    {
        private static readonly IReadOnlySet<string> s_rules =
            new HashSet<string>([RejectRuleId], StringComparer.Ordinal);

        internal List<MigrationReadRequest> Requests { get; } = [];

        public MigrationSourceIdentity Source => inner.Source;

        public string SnapshotIdentity => inner.SnapshotIdentity;

        public string RejectContractVersion =>
            MigrationRejectContract.DeterministicRejectsV1;

        public IReadOnlySet<string> SupportedRejectRuleIds => s_rules;

        public async IAsyncEnumerable<MigrationDataBatch> ReadAsync(
            MigrationReadRequest request,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            MigrationRejectReadPolicyValidator.Validate(request);
            Requests.Add(request);
            await foreach (MigrationDataBatch batch in inner.ReadAsync(
                                   request,
                                   cancellationToken)
                               .WithCancellation(cancellationToken))
            {
                int rejectedCount =
                    string.Equals(
                        batch.SourceObjectId,
                        "syn:table:customers-lower",
                        StringComparison.Ordinal) &&
                    batch.BatchOrdinal == 0
                        ? 1
                        : string.Equals(
                            batch.SourceObjectId,
                            "syn:table:reserved",
                            StringComparison.Ordinal)
                            ? batch.Rows.Count
                            : 0;
                if (rejectedCount == 0)
                {
                    yield return batch;
                    continue;
                }

                int acceptedCount = batch.Rows.Count - rejectedCount;
                MigrationRejectedRow[] rejectedRows = Enumerable.Range(
                        acceptedCount,
                        rejectedCount)
                    .Select(ordinal => new MigrationRejectedRow
                    {
                        SourceRowOrdinal = ordinal,
                        RuleId = RejectRuleId,
                        ColumnObjectId = request.ColumnObjectIds[0],
                        Evidence =
                        [
                            new MigrationRejectEvidence
                            {
                                Name = MigrationRejectLedgerCodec.RawValueEvidenceName,
                                Value = $"row-{ordinal}",
                            },
                        ],
                    })
                    .ToArray();
                yield return batch with
                {
                    Rows = batch.Rows.Take(acceptedCount).ToArray(),
                    RejectedRows = rejectedRows,
                };
            }
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class TrackingFailFastSource(SyntheticMigrationDataSource inner) :
        IMigrationDataSource
    {
        internal List<MigrationReadRequest> Requests { get; } = [];

        public MigrationSourceIdentity Source => inner.Source;

        public string SnapshotIdentity => inner.SnapshotIdentity;

        public async IAsyncEnumerable<MigrationDataBatch> ReadAsync(
            MigrationReadRequest request,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            Requests.Add(request);
            await foreach (MigrationDataBatch batch in inner.ReadAsync(
                                   request,
                                   cancellationToken)
                               .WithCancellation(cancellationToken))
            {
                yield return batch;
            }
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class NonterminalCursorSource(SyntheticMigrationDataSource inner) :
        IMigrationDataSource
    {
        public MigrationSourceIdentity Source => inner.Source;

        public string SnapshotIdentity => inner.SnapshotIdentity;

        public async IAsyncEnumerable<MigrationDataBatch> ReadAsync(
            MigrationReadRequest request,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await foreach (MigrationDataBatch batch in inner.ReadAsync(
                                   request,
                                   cancellationToken)
                               .WithCancellation(cancellationToken))
            {
                yield return batch.NextCursor is null
                    ? batch with { NextCursor = "cursor:nonterminal" }
                    : batch;
            }
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class MutableSnapshotIdentitySource(SyntheticMigrationDataSource inner) :
        IMigrationDataSource
    {
        internal List<MigrationReadRequest> Requests { get; } = [];

        public MigrationSourceIdentity Source => inner.Source;

        public string SnapshotIdentity { get; set; } = inner.SnapshotIdentity;

        public async IAsyncEnumerable<MigrationDataBatch> ReadAsync(
            MigrationReadRequest request,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            Requests.Add(request);
            await foreach (MigrationDataBatch batch in inner.ReadAsync(
                                   request,
                                   cancellationToken)
                               .WithCancellation(cancellationToken))
            {
                yield return batch;
            }
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class TemporaryTargetDirectory : IDisposable
    {
        internal TemporaryTargetDirectory()
        {
            DirectoryPath = Path.Combine(
                Path.GetTempPath(),
                $"csharpdb-reject-validation-{Guid.NewGuid():N}");
            Directory.CreateDirectory(DirectoryPath);
            TargetPath = Path.Combine(DirectoryPath, "staged.csdb");
            ReportPath = Path.Combine(DirectoryPath, "validation.json");
        }

        internal string DirectoryPath { get; }

        internal string TargetPath { get; }

        internal string ReportPath { get; }

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
