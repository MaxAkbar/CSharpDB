using System.Runtime.CompilerServices;
using CSharpDB.Migration.Validation;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Tests;

public sealed class MigrationValidationRunnerTests
{
    [Fact]
    public async Task ValidatePublishesDeterministicReportBeforeIdempotentActivation()
    {
        string root = CreateRoot();
        try
        {
            (MigrationCatalog catalog, MigrationPlan plan) = await ReadyPlanAsync();
            await using var source = new SyntheticMigrationDataSource(catalog);
            await using var sourceSnapshot = new MigrationDataSourceValidationSnapshot(plan, catalog, source);
            MaterializedSnapshot targetSnapshot = await MaterializeAsync(
                plan,
                catalog,
                sourceSnapshot,
                "target:snapshot:1");
            await using var target = new FakeValidationTarget(targetSnapshot);
            string reportPath = Path.Combine(root, "validation.json");
            var runner = new MigrationValidationRunner();
            MigrationValidationRunRequest request = Request(
                plan,
                catalog,
                sourceSnapshot,
                target,
                reportPath,
                root);

            MigrationValidationRunResult first = await runner.ValidateAsync(
                request,
                TestContext.Current.CancellationToken);
            MigrationValidationRunResult retry = await runner.ValidateAsync(
                request,
                TestContext.Current.CancellationToken);

            Assert.Equal(MigrationValidationStatus.Passed, first.Report.Outcome);
            Assert.True(first.Activated);
            Assert.True(retry.Activated);
            Assert.Equal(first.ReportDigest, retry.ReportDigest);
            Assert.Equal(first.ReportDigest, target.ActivationReceipt!.ReportDigest);
            Assert.Equal(2, target.ActivationAttempts);
            Assert.Equal(2, target.OpenSnapshotCount);
            string textReport = MigrationValidationTextFormatter.Format(first.Report);
            Assert.Equal(textReport, MigrationValidationTextFormatter.Format(retry.Report));
            Assert.Contains("Status: PASSED", textReport, StringComparison.Ordinal);
            Assert.Contains("Object syn:table:orders", textReport, StringComparison.Ordinal);
            Assert.DoesNotContain("Customer 1", textReport, StringComparison.Ordinal);
            Assert.True(File.Exists(reportPath));
            MigrationValidationReport restored = MigrationValidationReportSerializer.Deserialize(
                await File.ReadAllTextAsync(reportPath, TestContext.Current.CancellationToken));
            Assert.Equal(first.ReportDigest, MigrationValidationReportSerializer.ComputeDigest(restored));
            Assert.Equal(
                MigrationValidationReportSerializer.Serialize(first.Report),
                MigrationValidationReportSerializer.Serialize(restored));
            Assert.All(restored.Objects, item => Assert.Equal(256, item.Partitions.Count));
            Assert.DoesNotContain("Customer 1", await File.ReadAllTextAsync(
                reportPath,
                TestContext.Current.CancellationToken), StringComparison.Ordinal);
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task DifferenceProducesHashedEvidenceAndDoesNotActivate()
    {
        string root = CreateRoot();
        try
        {
            (MigrationCatalog catalog, MigrationPlan plan) = await ReadyPlanAsync();
            await using var source = new SyntheticMigrationDataSource(catalog);
            await using var sourceSnapshot = new MigrationDataSourceValidationSnapshot(plan, catalog, source);
            MaterializedSnapshot targetSnapshot = await MaterializeAsync(
                plan,
                catalog,
                sourceSnapshot,
                "target:snapshot:different");
            targetSnapshot.ChangeValue(
                "syn:table:customers-upper",
                rowIndex: 0,
                valueIndex: 3,
                DbValue.FromText("changed-without-reporting-raw-value"));
            await using var target = new FakeValidationTarget(targetSnapshot);
            string reportPath = Path.Combine(root, "different.json");

            MigrationValidationRunResult result = await new MigrationValidationRunner().ValidateAsync(
                Request(plan, catalog, sourceSnapshot, target, reportPath, root),
                TestContext.Current.CancellationToken);

            Assert.Equal(MigrationValidationStatus.Different, result.Report.Outcome);
            Assert.False(result.Activated);
            Assert.Null(target.ActivationReceipt);
            MigrationObjectValidationEvidence customers = Assert.Single(
                result.Report.Objects,
                item => item.SourceObjectId == "syn:table:customers-upper");
            MigrationValidationMismatchEvidence mismatch = Assert.Single(
                customers.Partitions.SelectMany(item => item.Mismatches));
            Assert.Equal(MigrationValidationMismatchKind.Changed, mismatch.Kind);
            string json = await File.ReadAllTextAsync(reportPath, TestContext.Current.CancellationToken);
            Assert.DoesNotContain("changed-without", json, StringComparison.Ordinal);
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Theory]
    [InlineData(MigrationSnapshotConsistencyStatus.NotEstablished)]
    [InlineData(MigrationSnapshotConsistencyStatus.Unavailable)]
    public async Task NonEstablishedConsistencyIsInconclusiveAndDoesNotActivate(
        MigrationSnapshotConsistencyStatus consistency)
    {
        string root = CreateRoot();
        try
        {
            (MigrationCatalog catalog, MigrationPlan plan) = await ReadyPlanAsync();
            await using var source = new SyntheticMigrationDataSource(catalog);
            await using var established = new MigrationDataSourceValidationSnapshot(plan, catalog, source);
            await using var sourceSnapshot = new ConsistencyOverrideSnapshot(
                established,
                consistency);
            MaterializedSnapshot targetSnapshot = await MaterializeAsync(
                plan,
                catalog,
                sourceSnapshot,
                "target:snapshot:best-effort");
            await using var target = new FakeValidationTarget(targetSnapshot);

            MigrationValidationRunResult result = await new MigrationValidationRunner().ValidateAsync(
                Request(
                    plan,
                    catalog,
                    sourceSnapshot,
                    target,
                    Path.Combine(root, "inconclusive.json"),
                    root),
                TestContext.Current.CancellationToken);

            Assert.Equal(MigrationValidationStatus.Inconclusive, result.Report.Outcome);
            Assert.Equal(
                consistency,
                result.Report.SnapshotConsistency.Status);
            Assert.Single(result.Report.Diagnostics);
            Assert.False(result.Activated);
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task UnknownConsistencyStatusIsRejectedAndDoesNotActivate()
    {
        string root = CreateRoot();
        try
        {
            (MigrationCatalog catalog, MigrationPlan plan) = await ReadyPlanAsync();
            await using var source = new SyntheticMigrationDataSource(catalog);
            await using var established = new MigrationDataSourceValidationSnapshot(plan, catalog, source);
            MaterializedSnapshot targetSnapshot = await MaterializeAsync(
                plan,
                catalog,
                established,
                "target:snapshot:unknown-consistency");
            await using var sourceSnapshot = new ConsistencyOverrideSnapshot(
                established,
                (MigrationSnapshotConsistencyStatus)999);
            await using var target = new FakeValidationTarget(targetSnapshot);

            await Assert.ThrowsAsync<InvalidDataException>(async () =>
                await new MigrationValidationRunner().ValidateAsync(
                    Request(
                        plan,
                        catalog,
                        sourceSnapshot,
                        target,
                        Path.Combine(root, "unknown-consistency.json"),
                        root),
                    TestContext.Current.CancellationToken));

            Assert.Null(target.ActivationReceipt);
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task SourceChangingAfterCountProducesCoherenceErrorAndDoesNotActivate()
    {
        string root = CreateRoot();
        try
        {
            (MigrationCatalog catalog, MigrationPlan plan) = await ReadyPlanAsync();
            await using var source = new SyntheticMigrationDataSource(catalog);
            await using var stable = new MigrationDataSourceValidationSnapshot(plan, catalog, source);
            MaterializedSnapshot targetSnapshot = await MaterializeAsync(
                plan,
                catalog,
                stable,
                "target:snapshot:stable-before-source-change");
            await using var changing = new AppendRowDuringReadSnapshot(
                stable,
                "syn:table:customers-lower");
            await using var target = new FakeValidationTarget(targetSnapshot);

            MigrationValidationRunResult result = await new MigrationValidationRunner().ValidateAsync(
                Request(
                    plan,
                    catalog,
                    changing,
                    target,
                    Path.Combine(root, "changing-source.json"),
                    root),
                TestContext.Current.CancellationToken);

            Assert.Equal(MigrationValidationStatus.Error, result.Report.Outcome);
            MigrationValidationDiagnosticEvidence diagnostic = Assert.Single(
                result.Report.Diagnostics,
                item => item.RuleId == "MIG-VALIDATE-SNAPSHOT-001");
            Assert.Equal("syn:table:customers-lower", diagnostic.ObjectId);
            Assert.False(result.Activated);
            Assert.Null(target.ActivationReceipt);
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task ExistingDifferentReportPreventsActivation()
    {
        string root = CreateRoot();
        try
        {
            (MigrationCatalog catalog, MigrationPlan plan) = await ReadyPlanAsync();
            await using var source = new SyntheticMigrationDataSource(catalog);
            await using var sourceSnapshot = new MigrationDataSourceValidationSnapshot(plan, catalog, source);
            MaterializedSnapshot targetSnapshot = await MaterializeAsync(
                plan,
                catalog,
                sourceSnapshot,
                "target:snapshot:report-failure");
            await using var target = new FakeValidationTarget(targetSnapshot);
            string reportPath = Path.Combine(root, "occupied.json");
            await File.WriteAllTextAsync(
                reportPath,
                "not-a-validation-report",
                TestContext.Current.CancellationToken);

            await Assert.ThrowsAsync<InvalidDataException>(
                async () => await new MigrationValidationRunner().ValidateAsync(
                    Request(plan, catalog, sourceSnapshot, target, reportPath, root),
                    TestContext.Current.CancellationToken));

            Assert.Null(target.ActivationReceipt);
            Assert.Equal("not-a-validation-report", await File.ReadAllTextAsync(
                reportPath,
                TestContext.Current.CancellationToken));
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task ExistingOversizedReportIsRejectedBeforeReadAndPreventsActivation()
    {
        string root = CreateRoot();
        try
        {
            (MigrationCatalog catalog, MigrationPlan plan) = await ReadyPlanAsync();
            await using var source = new SyntheticMigrationDataSource(catalog);
            await using var sourceSnapshot = new MigrationDataSourceValidationSnapshot(plan, catalog, source);
            MaterializedSnapshot targetSnapshot = await MaterializeAsync(
                plan,
                catalog,
                sourceSnapshot,
                "target:snapshot:oversized-report");
            await using var target = new FakeValidationTarget(targetSnapshot);
            string reportPath = Path.Combine(root, "oversized.json");
            await using (var stream = new FileStream(
                             reportPath,
                             FileMode.CreateNew,
                             FileAccess.Write,
                             FileShare.None))
            {
                stream.SetLength(MigrationValidationReportSerializer.MaximumArtifactBytes + 1L);
            }

            InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(async () =>
                await new MigrationValidationRunner().ValidateAsync(
                    Request(plan, catalog, sourceSnapshot, target, reportPath, root),
                    TestContext.Current.CancellationToken));

            Assert.Contains("maximum artifact", error.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Null(target.ActivationReceipt);
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task DeterministicRejectPolicyIsRejectedBeforeOpeningTargetOrPublishingReport()
    {
        string root = CreateRoot();
        try
        {
            (MigrationCatalog catalog, MigrationPlan ready) = await ReadyPlanAsync();
            await using var source = new SyntheticMigrationDataSource(catalog);
            await using var sourceSnapshot = new MigrationDataSourceValidationSnapshot(
                ready,
                catalog,
                source);
            MaterializedSnapshot targetSnapshot = await MaterializeAsync(
                ready,
                catalog,
                sourceSnapshot,
                "target:snapshot:reject-policy");
            await using var target = new FakeValidationTarget(targetSnapshot);
            MigrationPlan unsupported = WithDeterministicRejectPolicy(ready);
            string reportPath = Path.Combine(root, "must-not-exist.json");

            MigrationExecutionPolicyException error =
                await Assert.ThrowsAsync<MigrationExecutionPolicyException>(async () =>
                    await new MigrationValidationRunner().ValidateAsync(
                        Request(
                            unsupported,
                            catalog,
                            sourceSnapshot,
                            target,
                            reportPath,
                            root),
                        TestContext.Current.CancellationToken));

            Assert.Equal("MIG-VALIDATE-POLICY-REJECT-001", error.Code);
            Assert.Contains(
                MigrationRejectContract.DeterministicFailFastV1,
                error.Message,
                StringComparison.Ordinal);
            Assert.Equal(0, target.OpenSnapshotCount);
            Assert.Equal(0, target.ActivationAttempts);
            Assert.False(File.Exists(reportPath));
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task DataSourceSnapshotRejectsDeterministicRejectPolicyBeforeReadingSource()
    {
        (MigrationCatalog catalog, MigrationPlan ready) = await ReadyPlanAsync();
        MigrationPlan unsupported = WithDeterministicRejectPolicy(ready);
        await using var source = new NeverReadMigrationDataSource(
            ready.Source,
            SyntheticMigrationDataSource.FixtureSnapshotIdentity);

        MigrationExecutionPolicyException error =
            Assert.Throws<MigrationExecutionPolicyException>(() =>
                new MigrationDataSourceValidationSnapshot(unsupported, catalog, source));

        Assert.Equal("MIG-VALIDATE-POLICY-REJECT-001", error.Code);
        Assert.Equal(0, source.ReadCount);
    }

    private static MigrationValidationRunRequest Request(
        MigrationPlan plan,
        MigrationCatalog catalog,
        IMigrationEvidenceValidationSnapshot source,
        IMigrationTarget target,
        string reportPath,
        string spillRoot) => new()
    {
        Plan = plan,
        Catalog = catalog,
        SourceSnapshot = source,
        Target = target,
        Level = MigrationValidationLevel.Checksum,
        ReportOutputPath = reportPath,
        ChecksumOptions = new PartitionedChecksumValidatorOptions
        {
            SpillRootDirectory = spillRoot,
            SortMemoryBudgetBytes = ValidationHashRecord.SerializedLength * 4,
            MaxSpillBytes = 32 * 1024 * 1024,
            MergeFanIn = 2,
            MaxOpenFiles = 3,
            MaxOpenPartitionWriters = 4,
            MaxMismatchDetailsPerPartition = 10,
        },
    };

    private static async Task<MaterializedSnapshot> MaterializeAsync(
        MigrationPlan plan,
        MigrationCatalog catalog,
        IMigrationEvidenceValidationSnapshot source,
        string snapshotIdentity)
    {
        MigrationNormalizedSchema schema = await source.ReadSchemaAsync(TestContext.Current.CancellationToken);
        var rows = new Dictionary<string, List<MigrationValidationRow>>(StringComparer.Ordinal);
        foreach (MigrationCatalogObject item in catalog.Objects
                     .Where(item => item.Kind is MigrationObjectKind.Table or MigrationObjectKind.Collection)
                     .Where(item => plan.Objects.Single(planned => planned.SourceObjectId == item.ObjectId).Included))
        {
            var objectRows = new List<MigrationValidationRow>();
            await foreach (MigrationValidationRow row in source.ReadRowsAsync(
                item.ObjectId,
                TestContext.Current.CancellationToken))
            {
                objectRows.Add(new MigrationValidationRow
                {
                    StableKey = row.StableKey,
                    Values = row.Values.ToArray(),
                });
            }
            objectRows.Reverse();
            rows.Add(item.ObjectId, objectRows);
        }
        return new MaterializedSnapshot(snapshotIdentity, schema, rows);
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
        });
    }

    private static MigrationPlan WithDeterministicRejectPolicy(MigrationPlan plan) => plan with
    {
        Load = plan.Load with
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

    private sealed class FakeValidationTarget(MaterializedSnapshot snapshot) :
        IMigrationTarget,
        IMigrationValidationActivationTarget
    {
        public string TargetIdentity => "target:validation-test";

        public MigrationValidationActivationReceipt? ActivationReceipt { get; private set; }

        public int ActivationAttempts { get; private set; }

        public int OpenSnapshotCount { get; private set; }

        public ValueTask<IValidationSnapshot> OpenValidationSnapshotAsync(
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            OpenSnapshotCount++;
            return ValueTask.FromResult<IValidationSnapshot>(snapshot.Clone());
        }

        public ValueTask<MigrationValidationActivationReceipt?> ReadActivationReceiptAsync(
            CancellationToken cancellationToken = default) => ValueTask.FromResult(ActivationReceipt);

        public ValueTask ActivateAsync(
            MigrationValidationActivationPermit permit,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            ActivationAttempts++;
            MigrationValidationActivationReceipt receipt = permit.Receipt;
            if (ActivationReceipt is not null && ActivationReceipt != receipt)
                throw new InvalidDataException("Changed activation receipt.");
            ActivationReceipt = receipt;
            return ValueTask.CompletedTask;
        }

        public ValueTask ApplySchemaAsync(
            MigrationPlan plan,
            MigrationCatalog catalog,
            MigrationSchemaStage stage,
            CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<MigrationBatchReceipt> WriteBatchAsync(
            MigrationTargetBatch batch,
            CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<MigrationBatchReceipt?> ReadReceiptAsync(
            string planDigest,
            string sourceObjectId,
            long batchOrdinal,
            CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public async IAsyncEnumerable<MigrationBatchReceipt> ReadReceiptsAsync(
            string planDigest,
            string sourceObjectId,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            yield break;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class MaterializedSnapshot : IMigrationEvidenceValidationSnapshot
    {
        private readonly MigrationNormalizedSchema _schema;
        private readonly Dictionary<string, List<MigrationValidationRow>> _rows;
        private bool _disposed;

        internal MaterializedSnapshot(
            string snapshotIdentity,
            MigrationNormalizedSchema schema,
            Dictionary<string, List<MigrationValidationRow>> rows)
        {
            SnapshotIdentity = snapshotIdentity;
            _schema = schema;
            _rows = rows;
        }

        public string SnapshotIdentity { get; }

        public MigrationSnapshotConsistencyStatus ConsistencyStatus =>
            MigrationSnapshotConsistencyStatus.Established;

        public ValueTask<MigrationNormalizedSchema> ReadSchemaAsync(
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(_schema);
        }

        public ValueTask<long> CountAsync(
            string objectId,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(checked((long)_rows[objectId].Count));
        }

        public async IAsyncEnumerable<MigrationValidationRow> ReadRowsAsync(
            string objectId,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            foreach (MigrationValidationRow row in _rows[objectId])
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return row;
                await Task.Yield();
            }
        }

        internal void ChangeValue(string objectId, int rowIndex, int valueIndex, DbValue value)
        {
            MigrationValidationRow row = _rows[objectId][rowIndex];
            DbValue[] values = row.Values.ToArray();
            values[valueIndex] = value;
            _rows[objectId][rowIndex] = row with { Values = values };
        }

        internal MaterializedSnapshot Clone() => new(
            SnapshotIdentity,
            _schema,
            _rows.ToDictionary(
                item => item.Key,
                item => item.Value.Select(row => row with { Values = row.Values.ToArray() }).ToList(),
                StringComparer.Ordinal));

        public ValueTask DisposeAsync()
        {
            _disposed = true;
            return ValueTask.CompletedTask;
        }
    }

    private sealed class ConsistencyOverrideSnapshot(
        IMigrationEvidenceValidationSnapshot inner,
        MigrationSnapshotConsistencyStatus consistency) : IMigrationEvidenceValidationSnapshot
    {
        public string SnapshotIdentity => inner.SnapshotIdentity;

        public MigrationSnapshotConsistencyStatus ConsistencyStatus => consistency;

        public ValueTask<MigrationNormalizedSchema> ReadSchemaAsync(
            CancellationToken cancellationToken = default) => inner.ReadSchemaAsync(cancellationToken);

        public ValueTask<long> CountAsync(
            string objectId,
            CancellationToken cancellationToken = default) => inner.CountAsync(objectId, cancellationToken);

        public IAsyncEnumerable<MigrationValidationRow> ReadRowsAsync(
            string objectId,
            CancellationToken cancellationToken = default) => inner.ReadRowsAsync(objectId, cancellationToken);

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class AppendRowDuringReadSnapshot(
        IMigrationEvidenceValidationSnapshot inner,
        string changedObjectId) : IMigrationEvidenceValidationSnapshot
    {
        public string SnapshotIdentity => inner.SnapshotIdentity;

        public MigrationSnapshotConsistencyStatus ConsistencyStatus => inner.ConsistencyStatus;

        public ValueTask<MigrationNormalizedSchema> ReadSchemaAsync(
            CancellationToken cancellationToken = default) => inner.ReadSchemaAsync(cancellationToken);

        public ValueTask<long> CountAsync(
            string objectId,
            CancellationToken cancellationToken = default) => inner.CountAsync(objectId, cancellationToken);

        public async IAsyncEnumerable<MigrationValidationRow> ReadRowsAsync(
            string objectId,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            MigrationValidationRow? first = null;
            await foreach (MigrationValidationRow row in inner.ReadRowsAsync(objectId, cancellationToken)
                               .WithCancellation(cancellationToken))
            {
                first ??= row with { Values = row.Values.ToArray() };
                yield return row;
            }
            if (string.Equals(objectId, changedObjectId, StringComparison.Ordinal) && first is not null)
                yield return first;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class NeverReadMigrationDataSource(
        MigrationSourceIdentity source,
        string snapshotIdentity) : IMigrationDataSource
    {
        public MigrationSourceIdentity Source { get; } = source;

        public string SnapshotIdentity { get; } = snapshotIdentity;

        public int ReadCount { get; private set; }

        public IAsyncEnumerable<MigrationDataBatch> ReadAsync(
            MigrationReadRequest request,
            CancellationToken cancellationToken = default)
        {
            ReadCount++;
            throw new InvalidOperationException("Validation policy must be checked before source reads.");
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private static string CreateRoot()
    {
        string path = Path.Combine(Path.GetTempPath(), $"csharpdb-validation-runner-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static void DeleteRoot(string root)
    {
        if (Directory.Exists(root))
            Directory.Delete(root, recursive: true);
    }
}
