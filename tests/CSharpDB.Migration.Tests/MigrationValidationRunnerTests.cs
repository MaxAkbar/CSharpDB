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
            Assert.Null(request.BeforeActivationAsync);

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
    public async Task PreActivationCallbackRunsAfterPublishedReportAndBeforeActivation()
    {
        string root = CreateRoot();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            (MigrationCatalog catalog, MigrationPlan plan) = await ReadyPlanAsync();
            await using var source = new SyntheticMigrationDataSource(catalog);
            await using var sourceSnapshot = new MigrationDataSourceValidationSnapshot(
                plan,
                catalog,
                source);
            MaterializedSnapshot targetSnapshot = await MaterializeAsync(
                plan,
                catalog,
                sourceSnapshot,
                "target:snapshot:pre-activation-order");
            var sequence = new List<string>();
            await using var target = new FakeValidationTarget(
                targetSnapshot,
                () => sequence.Add("activate"));
            string reportPath = Path.Combine(root, "pre-activation-order.json");
            MigrationValidationRunRequest request = Request(
                plan,
                catalog,
                sourceSnapshot,
                target,
                reportPath,
                root) with
            {
                BeforeActivationAsync = async (context, callbackToken) =>
                {
                    callbackToken.ThrowIfCancellationRequested();
                    Assert.Equal(ct, callbackToken);
                    Assert.Equal(Path.GetFullPath(reportPath), context.ReportPath);
                    Assert.True(File.Exists(context.ReportPath));
                    MigrationValidationReport published =
                        MigrationValidationReportSerializer.Deserialize(
                            await File.ReadAllTextAsync(context.ReportPath, callbackToken));
                    Assert.Equal(MigrationValidationStatus.Passed, context.Report.Outcome);
                    Assert.Equal(
                        context.ReportDigest,
                        MigrationValidationReportSerializer.ComputeDigest(context.Report));
                    Assert.Equal(
                        context.ReportDigest,
                        MigrationValidationReportSerializer.ComputeDigest(published));
                    sequence.Add("callback");
                },
            };

            MigrationValidationRunResult result =
                await new MigrationValidationRunner().ValidateAsync(request, ct);

            Assert.True(result.Activated);
            Assert.Equal(["callback", "activate"], sequence);
            Assert.Equal(1, target.ActivationAttempts);
            Assert.Equal(result.ReportDigest, target.ActivationReceipt!.ReportDigest);
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task PreActivationCallbackFailureKeepsPublishedReportAndWithholdsActivation()
    {
        string root = CreateRoot();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            (MigrationCatalog catalog, MigrationPlan plan) = await ReadyPlanAsync();
            await using var source = new SyntheticMigrationDataSource(catalog);
            await using var sourceSnapshot = new MigrationDataSourceValidationSnapshot(
                plan,
                catalog,
                source);
            MaterializedSnapshot targetSnapshot = await MaterializeAsync(
                plan,
                catalog,
                sourceSnapshot,
                "target:snapshot:pre-activation-failure");
            await using var target = new FakeValidationTarget(targetSnapshot);
            string reportPath = Path.Combine(root, "pre-activation-failure.json");
            var expected = new InvalidOperationException("artifact publication failed");
            MigrationValidationRunRequest request = Request(
                plan,
                catalog,
                sourceSnapshot,
                target,
                reportPath,
                root) with
            {
                BeforeActivationAsync = (context, _) =>
                {
                    Assert.True(File.Exists(context.ReportPath));
                    return ValueTask.FromException(expected);
                },
            };

            InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
                async () => await new MigrationValidationRunner().ValidateAsync(request, ct));

            Assert.Same(expected, error);
            Assert.True(File.Exists(reportPath));
            Assert.Equal(0, target.ActivationAttempts);
            Assert.Null(target.ActivationReceipt);
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task PreActivationCallbackCancellationKeepsPublishedReportAndWithholdsActivation()
    {
        string root = CreateRoot();
        try
        {
            using var cancellation = new CancellationTokenSource();
            CancellationToken ct = cancellation.Token;
            (MigrationCatalog catalog, MigrationPlan plan) = await ReadyPlanAsync();
            await using var source = new SyntheticMigrationDataSource(catalog);
            await using var sourceSnapshot = new MigrationDataSourceValidationSnapshot(
                plan,
                catalog,
                source);
            MaterializedSnapshot targetSnapshot = await MaterializeAsync(
                plan,
                catalog,
                sourceSnapshot,
                "target:snapshot:pre-activation-cancellation");
            await using var target = new FakeValidationTarget(targetSnapshot);
            string reportPath = Path.Combine(root, "pre-activation-cancellation.json");
            bool callbackInvoked = false;
            MigrationValidationRunRequest request = Request(
                plan,
                catalog,
                sourceSnapshot,
                target,
                reportPath,
                root) with
            {
                BeforeActivationAsync = (_, callbackToken) =>
                {
                    Assert.Equal(ct, callbackToken);
                    callbackInvoked = true;
                    cancellation.Cancel();
                    return ValueTask.CompletedTask;
                },
            };

            OperationCanceledException error =
                await Assert.ThrowsAnyAsync<OperationCanceledException>(
                    async () => await new MigrationValidationRunner().ValidateAsync(request, ct));

            Assert.True(callbackInvoked);
            Assert.Equal(ct, error.CancellationToken);
            Assert.True(File.Exists(reportPath));
            Assert.Equal(0, target.ActivationAttempts);
            Assert.Null(target.ActivationReceipt);
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task NonPassedReportSkipsPreActivationCallbackAndActivation()
    {
        string root = CreateRoot();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            (MigrationCatalog catalog, MigrationPlan plan) = await ReadyPlanAsync();
            await using var source = new SyntheticMigrationDataSource(catalog);
            await using var sourceSnapshot = new MigrationDataSourceValidationSnapshot(
                plan,
                catalog,
                source);
            MaterializedSnapshot targetSnapshot = await MaterializeAsync(
                plan,
                catalog,
                sourceSnapshot,
                "target:snapshot:pre-activation-different");
            targetSnapshot.ChangeValue(
                "syn:table:customers-upper",
                rowIndex: 0,
                valueIndex: 3,
                DbValue.FromText("changed-before-pre-activation"));
            await using var target = new FakeValidationTarget(targetSnapshot);
            string reportPath = Path.Combine(root, "pre-activation-different.json");
            int callbackInvocations = 0;
            MigrationValidationRunRequest request = Request(
                plan,
                catalog,
                sourceSnapshot,
                target,
                reportPath,
                root) with
            {
                BeforeActivationAsync = (_, _) =>
                {
                    callbackInvocations++;
                    return ValueTask.CompletedTask;
                },
            };

            MigrationValidationRunResult result =
                await new MigrationValidationRunner().ValidateAsync(request, ct);

            Assert.Equal(MigrationValidationStatus.Different, result.Report.Outcome);
            Assert.False(result.Activated);
            Assert.Equal(0, callbackInvocations);
            Assert.Equal(0, target.ActivationAttempts);
            Assert.Null(target.ActivationReceipt);
            Assert.True(File.Exists(reportPath));
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

    [Theory]
    [InlineData("accepted-only")]
    [InlineData("mixed")]
    [InlineData("all-reject")]
    [InlineData("empty")]
    [InlineData("multi-object")]
    public async Task RejectOutcomeComparerAcceptsExactBoundedStreams(string scenario)
    {
        RejectOutcomeFixture fixture = await RejectOutcomeFixtureAsync(scenario);
        await using var source = new RejectReplaySnapshot(
            fixture.SourceSnapshotIdentity,
            fixture.Batches);
        await using var target = new RejectTargetOutcomeSnapshot(
            "target:snapshot:reject-outcomes",
            fixture.Receipts,
            fixture.Ledger);

        await new MigrationRejectOutcomeComparer().CompareAsync(
            fixture.Plan,
            fixture.Catalog,
            RejectValidationTarget.Identity,
            source,
            target,
            TestContext.Current.CancellationToken);

        Assert.True(target.PeakConcurrentMoves <= 1);
    }

    [Theory]
    [InlineData("receipt-tampered")]
    [InlineData("receipt-missing")]
    [InlineData("receipt-extra")]
    [InlineData("receipt-reordered")]
    [InlineData("ledger-tampered")]
    [InlineData("ledger-missing")]
    [InlineData("ledger-extra")]
    [InlineData("ledger-reordered")]
    [InlineData("source-reordered")]
    public async Task RejectOutcomeComparerFailsClosedForMismatchedOrUnexhaustedStreams(
        string mismatch)
    {
        RejectOutcomeFixture fixture = await RejectOutcomeFixtureAsync("two-batches");
        switch (mismatch)
        {
            case "receipt-tampered":
                fixture.Receipts[0] = fixture.Receipts[0] with
                {
                    RowCount = fixture.Receipts[0].RowCount + 1,
                };
                break;
            case "receipt-missing":
                fixture.Receipts.RemoveAt(0);
                break;
            case "receipt-extra":
                fixture.Receipts.Add(fixture.Receipts[^1]);
                break;
            case "receipt-reordered":
                fixture.Receipts.Reverse();
                break;
            case "ledger-tampered":
                MigrationRejectLedgerEntry entry = fixture.Ledger[0];
                fixture.Ledger[0] = entry with
                {
                    RejectedRow = entry.RejectedRow with
                    {
                        Evidence =
                        [
                            new MigrationRejectEvidence
                            {
                                Name = MigrationRejectLedgerCodec.RawValueEvidenceName,
                                Value = "private-ledger-value",
                            },
                        ],
                    },
                };
                break;
            case "ledger-missing":
                fixture.Ledger.RemoveAt(0);
                break;
            case "ledger-extra":
                fixture.Ledger.Add(fixture.Ledger[^1]);
                break;
            case "ledger-reordered":
                fixture.Ledger.Reverse();
                break;
            case "source-reordered":
                fixture.Batches.Reverse();
                break;
            default:
                throw new InvalidOperationException("Unknown test case.");
        }

        await using var source = new RejectReplaySnapshot(
            fixture.SourceSnapshotIdentity,
            fixture.Batches);
        await using var target = new RejectTargetOutcomeSnapshot(
            "target:snapshot:reject-mismatch",
            fixture.Receipts,
            fixture.Ledger);

        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await new MigrationRejectOutcomeComparer().CompareAsync(
                fixture.Plan,
                fixture.Catalog,
                RejectValidationTarget.Identity,
                source,
                target,
                TestContext.Current.CancellationToken));

        Assert.Equal(MigrationRejectOutcomeComparer.MismatchMessage, error.Message);
        Assert.DoesNotContain("private-ledger-value", error.ToString(), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("value")]
    [InlineData("batch")]
    public async Task RejectOutcomeComparerEnforcesReplayByteBoundsBeforeDigesting(string bound)
    {
        const string secret = "private-oversized-replay-value";
        RejectOutcomeFixture fixture = await RejectOutcomeFixtureAsync("accepted-only");
        MigrationPlan plan = fixture.Plan with
        {
            Load = fixture.Plan.Load with
            {
                MaxValueBytes = 1,
                MaxBatchBytes = string.Equals(bound, "batch", StringComparison.Ordinal)
                    ? 1
                    : fixture.Plan.Load.MaxBatchBytes,
            },
        };
        MigrationTargetBatch original = Assert.Single(fixture.Batches);
        MigrationTargetRow originalRow = Assert.Single(original.Rows);
        MigrationTargetRow row = string.Equals(bound, "value", StringComparison.Ordinal)
            ? originalRow with
            {
                Values = originalRow.Values
                    .Select((value, index) => index == 0 ? DbValue.FromText(secret) : value)
                    .ToArray(),
            }
            : originalRow;
        MigrationTargetBatch unsigned = original with
        {
            PlanDigest = MigrationArtifactSerializer.ComputePlanDigest(plan),
            BatchDigest = string.Empty,
            RejectDigest = string.Empty,
            Rows = [row],
        };
        MigrationTargetBatch rejectSealed = unsigned with
        {
            RejectDigest = MigrationRejectDigest.Compute(unsigned),
        };
        MigrationTargetBatch batch = rejectSealed with
        {
            BatchDigest = MigrationBatchDigest.Compute(rejectSealed),
        };
        await using var source = new RejectReplaySnapshot(
            fixture.SourceSnapshotIdentity,
            [batch]);
        await using var target = new RejectTargetOutcomeSnapshot(
            "target:snapshot:reject-byte-bound",
            [CreateOutcomeReceipt(batch)],
            []);

        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await new MigrationRejectOutcomeComparer().CompareAsync(
                plan,
                fixture.Catalog,
                RejectValidationTarget.Identity,
                source,
                target,
                TestContext.Current.CancellationToken));

        Assert.Equal(MigrationRejectOutcomeComparer.MismatchMessage, error.Message);
        Assert.DoesNotContain(secret, error.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task RejectOutcomeComparerMasksProviderEvidenceErrors()
    {
        RejectOutcomeFixture fixture = await RejectOutcomeFixtureAsync("mixed");
        await using var source = new RejectReplaySnapshot(
            fixture.SourceSnapshotIdentity,
            fixture.Batches);
        await using var target = new RejectTargetOutcomeSnapshot(
            "target:snapshot:private-provider-error",
            fixture.Receipts,
            fixture.Ledger,
            new InvalidOperationException("raw-secret-from-provider"));

        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await new MigrationRejectOutcomeComparer().CompareAsync(
                fixture.Plan,
                fixture.Catalog,
                RejectValidationTarget.Identity,
                source,
                target,
                TestContext.Current.CancellationToken));

        Assert.Equal(MigrationRejectOutcomeComparer.MismatchMessage, error.Message);
        Assert.DoesNotContain("raw-secret-from-provider", error.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task RejectOutcomeComparerSanitizesProviderCancellationMessages()
    {
        const string secret = "raw-secret-in-provider-cancellation";
        RejectOutcomeFixture fixture = await RejectOutcomeFixtureAsync("mixed");
        using var cancellation = new CancellationTokenSource();
        await using var source = new RejectReplaySnapshot(
            fixture.SourceSnapshotIdentity,
            fixture.Batches);
        await using var target = new RejectTargetOutcomeSnapshot(
            "target:snapshot:private-provider-cancellation",
            fixture.Receipts,
            fixture.Ledger,
            new OperationCanceledException(secret, innerException: null, cancellation.Token),
            cancellation.Cancel);

        OperationCanceledException error =
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                await new MigrationRejectOutcomeComparer().CompareAsync(
                    fixture.Plan,
                    fixture.Catalog,
                    RejectValidationTarget.Identity,
                    source,
                    target,
                    cancellation.Token));

        Assert.Equal(cancellation.Token, error.CancellationToken);
        Assert.DoesNotContain(secret, error.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task RejectEvidenceReadFailureIsMaskedBeforeReportPublicationOrActivation()
    {
        const string secret = "raw-secret-after-outcome-comparison";
        string root = CreateRoot();
        try
        {
            RejectOutcomeFixture fixture = await RejectOutcomeFixtureAsync("mixed");
            await using var source = new RejectReplaySnapshot(
                fixture.SourceSnapshotIdentity,
                fixture.Batches,
                new InvalidOperationException(secret));
            var targetSnapshot = new RejectTargetOutcomeSnapshot(
                "target:snapshot:private-evidence-error",
                fixture.Receipts,
                fixture.Ledger);
            await using var target = new RejectValidationTarget(targetSnapshot);
            string reportPath = Path.Combine(root, "must-not-publish.json");

            InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(async () =>
                await new MigrationValidationRunner().ValidateAsync(
                    Request(
                        fixture.Plan,
                        fixture.Catalog,
                        source,
                        target,
                        reportPath,
                        root),
                    TestContext.Current.CancellationToken));

            Assert.Equal(MigrationRejectOutcomeComparer.MismatchMessage, error.Message);
            Assert.DoesNotContain(secret, error.ToString(), StringComparison.Ordinal);
            Assert.Equal(1, source.SchemaReadCount);
            Assert.Equal(0, target.ActivationAttempts);
            Assert.False(File.Exists(reportPath));
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task RejectEvidenceCancellationIsSanitizedBeforeReportPublicationOrActivation()
    {
        const string secret = "raw-secret-after-cancelled-outcome-comparison";
        string root = CreateRoot();
        try
        {
            RejectOutcomeFixture fixture = await RejectOutcomeFixtureAsync("mixed");
            using var cancellation = new CancellationTokenSource();
            await using var source = new RejectReplaySnapshot(
                fixture.SourceSnapshotIdentity,
                fixture.Batches,
                new OperationCanceledException(secret, innerException: null, cancellation.Token),
                cancellation.Cancel);
            var targetSnapshot = new RejectTargetOutcomeSnapshot(
                "target:snapshot:private-evidence-cancellation",
                fixture.Receipts,
                fixture.Ledger);
            await using var target = new RejectValidationTarget(targetSnapshot);
            string reportPath = Path.Combine(root, "must-not-publish.json");

            OperationCanceledException error =
                await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                    await new MigrationValidationRunner().ValidateAsync(
                        Request(
                            fixture.Plan,
                            fixture.Catalog,
                            source,
                            target,
                            reportPath,
                            root),
                        cancellation.Token));

            Assert.Equal(cancellation.Token, error.CancellationToken);
            Assert.DoesNotContain(secret, error.ToString(), StringComparison.Ordinal);
            Assert.Equal(0, target.ActivationAttempts);
            Assert.False(File.Exists(reportPath));
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task RejectOutcomeMismatchPreventsEvidenceReadsReportPublicationAndActivation()
    {
        string root = CreateRoot();
        try
        {
            RejectOutcomeFixture fixture = await RejectOutcomeFixtureAsync("mixed");
            MigrationRejectLedgerEntry entry = fixture.Ledger[0];
            fixture.Ledger[0] = entry with
            {
                CanonicalEntryByteCount = entry.CanonicalEntryByteCount + 1,
            };
            await using var source = new RejectReplaySnapshot(
                fixture.SourceSnapshotIdentity,
                fixture.Batches);
            var targetSnapshot = new RejectTargetOutcomeSnapshot(
                "target:snapshot:no-publication",
                fixture.Receipts,
                fixture.Ledger);
            await using var target = new RejectValidationTarget(targetSnapshot);
            string reportPath = Path.Combine(root, "must-not-publish.json");

            InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(async () =>
                await new MigrationValidationRunner().ValidateAsync(
                    Request(
                        fixture.Plan,
                        fixture.Catalog,
                        source,
                        target,
                        reportPath,
                        root),
                    TestContext.Current.CancellationToken));

            Assert.Equal(MigrationRejectOutcomeComparer.MismatchMessage, error.Message);
            Assert.Equal(1, target.OpenSnapshotCount);
            Assert.Equal(0, target.ActivationAttempts);
            Assert.Equal(0, source.SchemaReadCount);
            Assert.Equal(0, targetSnapshot.SchemaReadCount);
            Assert.False(File.Exists(reportPath));
        }
        finally
        {
            DeleteRoot(root);
        }
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

    private static async Task<RejectOutcomeFixture> RejectOutcomeFixtureAsync(string scenario)
    {
        (MigrationCatalog catalog, MigrationPlan ready) = await ReadyPlanAsync();
        MigrationPlan plan = WithDeterministicRejectPolicy(ready);
        const string sourceSnapshotIdentity = "source:snapshot:reject-outcomes";
        Projection[] projections = catalog.Objects
            .Where(item => item.Kind is MigrationObjectKind.Table or MigrationObjectKind.Collection)
            .Where(item => plan.Objects.Single(planned =>
                string.Equals(
                    planned.SourceObjectId,
                    item.ObjectId,
                    StringComparison.Ordinal)).Included)
            .OrderBy(item => item.ObjectId, StringComparer.Ordinal)
            .Select(item => new Projection(
                item.ObjectId,
                catalog.Objects
                    .Where(column =>
                        column.Kind == MigrationObjectKind.Column &&
                        string.Equals(
                            column.ParentObjectId,
                            item.ObjectId,
                            StringComparison.Ordinal) &&
                        plan.Objects.Single(planned =>
                            string.Equals(
                                planned.SourceObjectId,
                                column.ObjectId,
                                StringComparison.Ordinal)).Included)
                    .OrderBy(column => column.ObjectId, StringComparer.Ordinal)
                    .Select(column => column.ObjectId)
                    .ToArray()))
            .ToArray();
        Assert.NotEmpty(projections);

        var batches = new List<MigrationTargetBatch>();
        switch (scenario)
        {
            case "accepted-only":
                batches.Add(CreateOutcomeBatch(
                    plan,
                    projections[0],
                    sourceSnapshotIdentity,
                    0,
                    null,
                    null,
                    [AcceptedRow(projections[0], 0)],
                    []));
                break;
            case "mixed":
                batches.Add(CreateOutcomeBatch(
                    plan,
                    projections[0],
                    sourceSnapshotIdentity,
                    0,
                    null,
                    null,
                    [AcceptedRow(projections[0], 0)],
                    [RejectedRow(projections[0], 1, "source-private-value")]));
                break;
            case "all-reject":
                batches.Add(CreateOutcomeBatch(
                    plan,
                    projections[0],
                    sourceSnapshotIdentity,
                    0,
                    null,
                    null,
                    [],
                    [RejectedRow(projections[0], 0, "source-private-value")]));
                break;
            case "empty":
                break;
            case "multi-object":
                Assert.True(projections.Length >= 2);
                batches.Add(CreateOutcomeBatch(
                    plan,
                    projections[0],
                    sourceSnapshotIdentity,
                    0,
                    null,
                    null,
                    [AcceptedRow(projections[0], 0)],
                    []));
                batches.Add(CreateOutcomeBatch(
                    plan,
                    projections[1],
                    sourceSnapshotIdentity,
                    0,
                    null,
                    null,
                    [],
                    [RejectedRow(projections[1], 0, "second-object-private-value")]));
                break;
            case "two-batches":
                batches.Add(CreateOutcomeBatch(
                    plan,
                    projections[0],
                    sourceSnapshotIdentity,
                    0,
                    null,
                    "cursor:1",
                    [],
                    [RejectedRow(projections[0], 0, "first-private-value")]));
                batches.Add(CreateOutcomeBatch(
                    plan,
                    projections[0],
                    sourceSnapshotIdentity,
                    1,
                    "cursor:1",
                    null,
                    [],
                    [RejectedRow(projections[0], 1, "second-private-value")]));
                break;
            default:
                throw new InvalidOperationException("Unknown test fixture.");
        }

        List<MigrationBatchReceipt> receipts = batches
            .Select(CreateOutcomeReceipt)
            .ToList();
        List<MigrationRejectLedgerEntry> ledger = batches
            .SelectMany(batch => batch.RejectedRows.Select(rejectedRow =>
                CreateLedgerEntry(batch, rejectedRow)))
            .ToList();
        return new RejectOutcomeFixture(
            catalog,
            plan,
            sourceSnapshotIdentity,
            batches,
            receipts,
            ledger);
    }

    private static MigrationTargetBatch CreateOutcomeBatch(
        MigrationPlan plan,
        Projection projection,
        string sourceSnapshotIdentity,
        long batchOrdinal,
        string? startCursor,
        string? nextCursor,
        IReadOnlyList<MigrationTargetRow> rows,
        IReadOnlyList<MigrationRejectedRow> rejectedRows)
    {
        var unsigned = new MigrationTargetBatch
        {
            PlanDigest = MigrationArtifactSerializer.ComputePlanDigest(plan),
            CatalogDigest = plan.CatalogDigest,
            SourceFingerprint = plan.Source.Fingerprint,
            SourceSnapshotIdentity = sourceSnapshotIdentity,
            SourceObjectId = projection.SourceObjectId,
            ColumnObjectIds = projection.ColumnObjectIds,
            BatchOrdinal = batchOrdinal,
            StartCursor = startCursor,
            NextCursor = nextCursor,
            BatchDigest = string.Empty,
            RejectContractVersion = MigrationRejectContract.DeterministicRejectsV1,
            RejectDigest = string.Empty,
            Rows = rows,
            RejectedRows = rejectedRows,
        };
        MigrationTargetBatch rejectSealed = unsigned with
        {
            RejectDigest = MigrationRejectDigest.Compute(unsigned),
        };
        return rejectSealed with
        {
            BatchDigest = MigrationBatchDigest.Compute(rejectSealed),
        };
    }

    private static MigrationTargetRow AcceptedRow(Projection projection, long sourceRowOrdinal) =>
        new()
        {
            SourceRowOrdinal = sourceRowOrdinal,
            StableKey = $"stable:{sourceRowOrdinal}",
            Values = projection.ColumnObjectIds.Select(_ => DbValue.Null).ToArray(),
        };

    private static MigrationRejectedRow RejectedRow(
        Projection projection,
        long sourceRowOrdinal,
        string rawValue) =>
        new()
        {
            SourceRowOrdinal = sourceRowOrdinal,
            RuleId = "MIG-TEST-001",
            ColumnObjectId = projection.ColumnObjectIds[0],
            Evidence =
            [
                new MigrationRejectEvidence
                {
                    Name = MigrationRejectLedgerCodec.RawValueEvidenceName,
                    Value = rawValue,
                },
            ],
        };

    private static MigrationBatchReceipt CreateOutcomeReceipt(MigrationTargetBatch batch) =>
        new()
        {
            TargetIdentity = RejectValidationTarget.Identity,
            PlanDigest = batch.PlanDigest,
            CatalogDigest = batch.CatalogDigest,
            SourceFingerprint = batch.SourceFingerprint,
            SourceSnapshotIdentity = batch.SourceSnapshotIdentity,
            SourceObjectId = batch.SourceObjectId,
            BatchOrdinal = batch.BatchOrdinal,
            StartCursor = batch.StartCursor,
            NextCursor = batch.NextCursor,
            BatchDigest = batch.BatchDigest,
            RejectContractVersion = batch.RejectContractVersion,
            RejectDigest = batch.RejectDigest,
            RowCount = batch.Rows.Count,
            RejectedRowCount = batch.RejectedRows.Count,
        };

    private static MigrationRejectLedgerEntry CreateLedgerEntry(
        MigrationTargetBatch batch,
        MigrationRejectedRow rejectedRow) =>
        new()
        {
            PlanDigest = batch.PlanDigest,
            SourceObjectId = batch.SourceObjectId,
            BatchOrdinal = batch.BatchOrdinal,
            RejectedRow = rejectedRow,
            RawValueByteCount = MigrationRejectLedgerCodec.GetRawValueByteCount(rejectedRow),
            CanonicalEntryByteCount = MigrationRejectLedgerCodec.GetCanonicalEntryByteCount(
                batch.SourceObjectId,
                batch.BatchOrdinal,
                rejectedRow),
        };

    private sealed record RejectOutcomeFixture(
        MigrationCatalog Catalog,
        MigrationPlan Plan,
        string SourceSnapshotIdentity,
        List<MigrationTargetBatch> Batches,
        List<MigrationBatchReceipt> Receipts,
        List<MigrationRejectLedgerEntry> Ledger);

    private sealed record Projection(
        string SourceObjectId,
        IReadOnlyList<string> ColumnObjectIds);

    private sealed class RejectReplaySnapshot(
        string snapshotIdentity,
        IReadOnlyList<MigrationTargetBatch> batches,
        Exception? evidenceError = null,
        Action? beforeEvidenceError = null) :
        IMigrationRejectReplayValidationSnapshot
    {
        public string SnapshotIdentity { get; } = snapshotIdentity;

        public MigrationSnapshotConsistencyStatus ConsistencyStatus =>
            MigrationSnapshotConsistencyStatus.Established;

        public int SchemaReadCount { get; private set; }

        public async IAsyncEnumerable<MigrationTargetBatch> ReplayOutcomeBatchesAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (MigrationTargetBatch batch in batches)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await Task.Yield();
                yield return batch;
            }
        }

        public ValueTask<MigrationNormalizedSchema> ReadSchemaAsync(
            CancellationToken cancellationToken = default)
        {
            SchemaReadCount++;
            beforeEvidenceError?.Invoke();
            throw evidenceError ??
                new InvalidOperationException("Outcome mismatch must precede schema reads.");
        }

        public ValueTask<long> CountAsync(
            string objectId,
            CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("Outcome mismatch must precede count reads.");

        public IAsyncEnumerable<MigrationValidationRow> ReadRowsAsync(
            string objectId,
            CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("Outcome mismatch must precede row reads.");

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class RejectTargetOutcomeSnapshot(
        string snapshotIdentity,
        IReadOnlyList<MigrationBatchReceipt> receipts,
        IReadOnlyList<MigrationRejectLedgerEntry> ledger,
        Exception? ledgerError = null,
        Action? beforeLedgerError = null) :
        IMigrationRejectTargetValidationSnapshot
    {
        private int _moveInProgress;
        private int _peakConcurrentMoves;

        public string SnapshotIdentity { get; } = snapshotIdentity;

        public MigrationSnapshotConsistencyStatus ConsistencyStatus =>
            MigrationSnapshotConsistencyStatus.Established;

        public int PeakConcurrentMoves => Volatile.Read(ref _peakConcurrentMoves);

        public int SchemaReadCount { get; private set; }

        public async IAsyncEnumerable<MigrationBatchReceipt> ReadOutcomeReceiptsAsync(
            string planDigest,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (MigrationBatchReceipt receipt in receipts)
            {
                await BeforeYieldAsync(cancellationToken);
                yield return receipt;
            }
        }

        public async IAsyncEnumerable<MigrationRejectLedgerEntry> ReadRejectLedgerAsync(
            string planDigest,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            if (ledgerError is not null)
            {
                beforeLedgerError?.Invoke();
                if (beforeLedgerError is null)
                    await BeforeYieldAsync(cancellationToken);
                throw ledgerError;
            }

            foreach (MigrationRejectLedgerEntry entry in ledger)
            {
                await BeforeYieldAsync(cancellationToken);
                yield return entry;
            }
        }

        public ValueTask<MigrationNormalizedSchema> ReadSchemaAsync(
            CancellationToken cancellationToken = default)
        {
            SchemaReadCount++;
            throw new InvalidOperationException("Outcome mismatch must precede schema reads.");
        }

        public ValueTask<long> CountAsync(
            string objectId,
            CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("Outcome mismatch must precede count reads.");

        public IAsyncEnumerable<MigrationValidationRow> ReadRowsAsync(
            string objectId,
            CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("Outcome mismatch must precede row reads.");

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        private async ValueTask BeforeYieldAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int concurrent = Interlocked.Increment(ref _moveInProgress);
            SetPeak(concurrent);
            try
            {
                if (concurrent != 1)
                    throw new InvalidOperationException("Target outcome streams were advanced concurrently.");
                await Task.Yield();
            }
            finally
            {
                Interlocked.Decrement(ref _moveInProgress);
            }
        }

        private void SetPeak(int value)
        {
            int current;
            do
            {
                current = Volatile.Read(ref _peakConcurrentMoves);
                if (current >= value)
                    return;
            }
            while (Interlocked.CompareExchange(
                       ref _peakConcurrentMoves,
                       value,
                       current) != current);
        }
    }

    private sealed class RejectValidationTarget(
        RejectTargetOutcomeSnapshot snapshot) :
        IMigrationTarget,
        IMigrationRejectLedgerTarget,
        IMigrationBatchDigestContractTarget,
        IMigrationValidationActivationTarget
    {
        internal const string Identity = "target:validation-reject-test";

        public string TargetIdentity => Identity;

        public string BatchDigestFormat => MigrationBatchDigest.Format;

        public int OpenSnapshotCount { get; private set; }

        public int ActivationAttempts { get; private set; }

        public ValueTask<IValidationSnapshot> OpenValidationSnapshotAsync(
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            OpenSnapshotCount++;
            return ValueTask.FromResult<IValidationSnapshot>(snapshot);
        }

        public IAsyncEnumerable<MigrationRejectLedgerEntry> ReadRejectLedgerAsync(
            string planDigest,
            CancellationToken cancellationToken = default) =>
            snapshot.ReadRejectLedgerAsync(planDigest, cancellationToken);

        public ValueTask<MigrationValidationActivationReceipt?> ReadActivationReceiptAsync(
            CancellationToken cancellationToken = default) =>
            ValueTask.FromResult<MigrationValidationActivationReceipt?>(null);

        public ValueTask ActivateAsync(
            MigrationValidationActivationPermit permit,
            CancellationToken cancellationToken = default)
        {
            ActivationAttempts++;
            return ValueTask.CompletedTask;
        }

        public ValueTask ApplySchemaAsync(
            MigrationPlan plan,
            MigrationCatalog catalog,
            MigrationSchemaStage stage,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public ValueTask<MigrationBatchReceipt> WriteBatchAsync(
            MigrationTargetBatch batch,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public ValueTask<MigrationBatchReceipt?> ReadReceiptAsync(
            string planDigest,
            string sourceObjectId,
            long batchOrdinal,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

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

    private sealed class FakeValidationTarget(
        MaterializedSnapshot snapshot,
        Action? beforeActivation = null) :
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
            beforeActivation?.Invoke();
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
