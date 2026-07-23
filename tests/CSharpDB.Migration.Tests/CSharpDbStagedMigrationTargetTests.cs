using CSharpDB.Engine;
using CSharpDB.Migration.CSharpDb;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Tests;

public sealed class CSharpDbStagedMigrationTargetTests
{
    private const string DeterministicRuleId = "MIG-CSV-ROW-001";
    private const string RejectSourceObjectId = "syn:table:customers-lower";
    private const string RejectColumnObjectId = "syn:column:customers-lower:code-lower";

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
            MigrationBatchReceipt receipt = Assert.IsType<MigrationBatchReceipt>(
                await target.ReadReceiptAsync(
                    result.PlanDigest,
                    "syn:table:customers-lower",
                    batchOrdinal: 0,
                    cancellationToken: Ct));
            Assert.Equal(
                MigrationRejectContract.DeterministicFailFastV1,
                receipt.RejectContractVersion);
            Assert.Matches("^[0-9a-f]{64}$", receipt.RejectDigest);
            Assert.Equal(0, receipt.RejectedRowCount);

            await using IValidationSnapshot snapshot = await target.OpenValidationSnapshotAsync(Ct);
            string snapshotPrefix =
                $"staged-target:{targetIdentity}:awaiting-validation:outcomes:";
            Assert.StartsWith(snapshotPrefix, snapshot.SnapshotIdentity, StringComparison.Ordinal);
            Assert.Matches(
                "^[0-9a-f]{64}$",
                snapshot.SnapshotIdentity[snapshotPrefix.Length..]);
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
    public async Task LegacyV1Target_AppendsAndResumesWithoutChangingItsContractTags()
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
        }
        await ConvertEmptyTargetToLegacyV1Async(files.TargetPath, Ct);

        var injector = new ThrowOnceFaultInjector(CSharpDbMigrationFaultPoint.AfterCommit);
        await using (var source = new SyntheticMigrationDataSource(catalog))
        await using (CSharpDbStagedMigrationTarget target = await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                         files.TargetPath,
                         plan,
                         catalog,
                         source.SnapshotIdentity,
                         faultInjector: injector,
                         cancellationToken: Ct))
        {
            Assert.Equal(MigrationBatchDigest.LegacyFormat, target.BatchDigestFormat);
            await Assert.ThrowsAsync<InjectedMigrationFaultException>(async () =>
                await ApplyAsync(plan, catalog, source, target, Ct));
            Assert.True(injector.Fired);
        }

        await using (var source = new SyntheticMigrationDataSource(catalog))
        await using (CSharpDbStagedMigrationTarget target = await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                         files.TargetPath,
                         plan,
                         catalog,
                         source.SnapshotIdentity,
                         cancellationToken: Ct))
        {
            MigrationApplyResult continued = await ApplyAsync(plan, catalog, source, target, Ct);
            Assert.Equal(10, continued.BatchesWritten);
            Assert.Equal(1, continued.BatchesSkipped);
            MigrationBatchReceipt receipt = Assert.IsType<MigrationBatchReceipt>(
                await target.ReadReceiptAsync(
                    continued.PlanDigest,
                    "syn:table:customers-lower",
                    batchOrdinal: 0,
                    cancellationToken: Ct));
            Assert.Equal(
                MigrationRejectContract.DeterministicFailFastV1,
                receipt.RejectContractVersion);
            Assert.Matches("^[0-9a-f]{64}$", receipt.RejectDigest);
            Assert.Equal(0, receipt.RejectedRowCount);
        }

        await using (var source = new SyntheticMigrationDataSource(catalog))
        await using (CSharpDbStagedMigrationTarget target = await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                         files.TargetPath,
                         plan,
                         catalog,
                         source.SnapshotIdentity,
                         cancellationToken: Ct))
        {
            MigrationApplyResult resumed = await ApplyAsync(plan, catalog, source, target, Ct);
            Assert.Equal(0, resumed.BatchesWritten);
            Assert.Equal(11, resumed.BatchesSkipped);

            await using IValidationSnapshot opened =
                await target.OpenValidationSnapshotAsync(Ct);
            IMigrationRejectTargetValidationSnapshot snapshot =
                Assert.IsAssignableFrom<IMigrationRejectTargetValidationSnapshot>(
                    opened);
            List<MigrationBatchReceipt> receipts = await CollectAsync(
                snapshot.ReadOutcomeReceiptsAsync(resumed.PlanDigest, Ct));
            Assert.Equal(11, receipts.Count);
            Assert.All(receipts, receipt =>
                Assert.Equal(
                    MigrationRejectContract.DeterministicFailFastV1,
                    receipt.RejectContractVersion));
            Assert.Empty(await CollectAsync(
                snapshot.ReadRejectLedgerAsync(resumed.PlanDigest, Ct)));
        }

        Assert.Equal(
            ("csharpdb-staged-migration-target/v1", "csharpdb-migration-batch-receipt/v1"),
            await ReadLegacyTagsAsync(files.TargetPath, Ct));
    }

    [Fact]
    public async Task DeterministicRejectTarget_AcceptedOnlyBatchReplaysWithoutLedgerRows()
    {
        using var files = new TemporaryTargetDirectory();
        MigrationCatalog catalog = await InspectAsync(Ct);
        MigrationPlan plan = ReadyDeterministicRejectPlan(catalog, batchSize: 3);
        MigrationTargetBatch batch = DeterministicBatch(
            plan,
            catalog,
            rows:
            [
                AcceptedRow(0, "zero"),
                AcceptedRow(1, "one"),
            ]);
        string targetIdentity;

        await using (CSharpDbStagedMigrationTarget target =
                     await CreateDeterministicTargetAsync(files, plan, catalog))
        {
            targetIdentity = target.TargetIdentity;
            MigrationBatchReceipt receipt = await target.WriteBatchAsync(batch, Ct);
            MigrationBatchReceipt replay = await target.WriteBatchAsync(batch, Ct);
            List<MigrationRejectLedgerEntry> ledger = await ReadLedgerAsync(target, plan);

            Assert.Equal(2, receipt.RowCount);
            Assert.Equal(0, receipt.RejectedRowCount);
            Assert.Equal(receipt, replay);
            Assert.Empty(ledger);
        }

        Assert.Equal(2, await CountTargetRowsAsync(files.TargetPath, plan, Ct));
        await using CSharpDbStagedMigrationTarget resumed =
            await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                files.TargetPath,
                plan,
                catalog,
                SyntheticMigrationDataSource.FixtureSnapshotIdentity,
                cancellationToken: Ct);
        Assert.Equal(targetIdentity, resumed.TargetIdentity);
        Assert.NotNull(await resumed.ReadReceiptAsync(
            batch.PlanDigest,
            batch.SourceObjectId,
            batch.BatchOrdinal,
            Ct));
        Assert.Empty(await ReadLedgerAsync(resumed, plan));
    }

    [Fact]
    public async Task DeterministicRejectTarget_MixedBatchPersistsAndReplaysBoundLedger()
    {
        using var files = new TemporaryTargetDirectory();
        MigrationCatalog catalog = await InspectAsync(Ct);
        MigrationPlan plan = ReadyDeterministicRejectPlan(catalog, batchSize: 3);
        MigrationRejectedRow rejected = RejectedRow(1, "bad-value");
        MigrationTargetBatch batch = DeterministicBatch(
            plan,
            catalog,
            rows:
            [
                AcceptedRow(0, "zero"),
                AcceptedRow(2, "two"),
            ],
            rejectedRows: [rejected]);

        await using (CSharpDbStagedMigrationTarget target =
                     await CreateDeterministicTargetAsync(files, plan, catalog))
        {
            MigrationBatchReceipt receipt = await target.WriteBatchAsync(batch, Ct);
            MigrationBatchReceipt replay = await target.WriteBatchAsync(batch, Ct);
            MigrationRejectLedgerEntry entry = Assert.Single(await ReadLedgerAsync(target, plan));

            Assert.Equal(2, receipt.RowCount);
            Assert.Equal(1, receipt.RejectedRowCount);
            Assert.Equal(receipt, replay);
            Assert.Equal(batch.PlanDigest, entry.PlanDigest);
            Assert.Equal(RejectSourceObjectId, entry.SourceObjectId);
            Assert.Equal(0, entry.BatchOrdinal);
            AssertRejectedRowEqual(rejected, entry.RejectedRow);
            Assert.Equal("bad-value".Length, entry.RawValueByteCount);
            Assert.True(entry.CanonicalEntryByteCount > entry.RawValueByteCount);
        }

        Assert.Equal(2, await CountTargetRowsAsync(files.TargetPath, plan, Ct));
        await using CSharpDbStagedMigrationTarget resumed =
            await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                files.TargetPath,
                plan,
                catalog,
                SyntheticMigrationDataSource.FixtureSnapshotIdentity,
                cancellationToken: Ct);
        MigrationBatchReceipt restored = Assert.IsType<MigrationBatchReceipt>(
            await resumed.ReadReceiptAsync(
                batch.PlanDigest,
                batch.SourceObjectId,
                batch.BatchOrdinal,
                Ct));
        Assert.Equal(1, restored.RejectedRowCount);
        AssertRejectedRowEqual(
            rejected,
            Assert.Single(await ReadLedgerAsync(resumed, plan)).RejectedRow);
    }

    [Fact]
    public async Task DeterministicRejectTarget_AllRejectBatchAdvancesWithNoTargetRows()
    {
        using var files = new TemporaryTargetDirectory();
        MigrationCatalog catalog = await InspectAsync(Ct);
        MigrationPlan plan = ReadyDeterministicRejectPlan(catalog, batchSize: 3);
        MigrationTargetBatch batch = DeterministicBatch(
            plan,
            catalog,
            rows: [],
            rejectedRows:
            [
                RejectedRow(0, "bad-zero"),
                RejectedRow(1, "bad-one"),
            ]);

        await using (CSharpDbStagedMigrationTarget target =
                     await CreateDeterministicTargetAsync(files, plan, catalog))
        {
            MigrationBatchReceipt receipt = await target.WriteBatchAsync(batch, Ct);
            MigrationBatchReceipt replay = await target.WriteBatchAsync(batch, Ct);
            List<MigrationRejectLedgerEntry> ledger = await ReadLedgerAsync(target, plan);

            Assert.Equal(0, receipt.RowCount);
            Assert.Equal(2, receipt.RejectedRowCount);
            Assert.Equal(receipt, replay);
            Assert.Equal([0L, 1L], ledger.Select(item => item.RejectedRow.SourceRowOrdinal));
        }

        Assert.Equal(0, await CountTargetRowsAsync(files.TargetPath, plan, Ct));
        await using CSharpDbStagedMigrationTarget resumed =
            await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                files.TargetPath,
                plan,
                catalog,
                SyntheticMigrationDataSource.FixtureSnapshotIdentity,
                cancellationToken: Ct);
        Assert.Equal(2, (await ReadLedgerAsync(resumed, plan)).Count);
    }

    [Fact]
    public async Task ValidationSnapshot_StreamsCompleteOrderedRejectOutcomesAcrossReopen()
    {
        using var files = new TemporaryTargetDirectory();
        MigrationCatalog catalog = await InspectAsync(Ct);
        MigrationPlan plan = ReadyDeterministicRejectPlan(catalog, batchSize: 3);
        string planDigest = MigrationArtifactSerializer.ComputePlanDigest(plan);
        MigrationTargetBatch first = DeterministicBatch(
            plan,
            catalog,
            rows:
            [
                AcceptedRow(0, "zero"),
                AcceptedRow(2, "two"),
            ],
            rejectedRows: [RejectedRow(1, "bad-one")],
            nextCursor: "cursor:3");
        MigrationTargetBatch second = DeterministicBatch(
            plan,
            catalog,
            rows: [AcceptedRow(4, "four")],
            rejectedRows:
            [
                RejectedRow(3, "bad-three"),
                RejectedRow(5, "bad-five"),
            ],
            batchOrdinal: 1,
            startCursor: "cursor:3");
        string snapshotIdentity;

        await using (CSharpDbStagedMigrationTarget target =
                     await CreateDeterministicTargetAsync(files, plan, catalog))
        {
            MigrationBatchReceipt firstReceipt = await target.WriteBatchAsync(first, Ct);
            MigrationBatchReceipt secondReceipt = await target.WriteBatchAsync(second, Ct);
            foreach (MigrationSchemaStage stage in Enum.GetValues<MigrationSchemaStage>().Skip(1))
                await target.ApplySchemaAsync(plan, catalog, stage, Ct);

            await using IValidationSnapshot opened =
                await target.OpenValidationSnapshotAsync(Ct);
            IMigrationRejectTargetValidationSnapshot snapshot =
                Assert.IsAssignableFrom<IMigrationRejectTargetValidationSnapshot>(opened);
            snapshotIdentity = snapshot.SnapshotIdentity;

            await using IAsyncEnumerator<MigrationBatchReceipt> receipts =
                snapshot.ReadOutcomeReceiptsAsync(planDigest, Ct).GetAsyncEnumerator(Ct);
            await using IAsyncEnumerator<MigrationRejectLedgerEntry> ledger =
                snapshot.ReadRejectLedgerAsync(planDigest, Ct).GetAsyncEnumerator(Ct);

            Assert.True(await receipts.MoveNextAsync());
            Assert.Equal(firstReceipt, receipts.Current);
            Assert.True(await ledger.MoveNextAsync());
            Assert.Equal(1, ledger.Current.RejectedRow.SourceRowOrdinal);
            Assert.True(await receipts.MoveNextAsync());
            Assert.Equal(secondReceipt, receipts.Current);
            Assert.True(await ledger.MoveNextAsync());
            Assert.Equal(3, ledger.Current.RejectedRow.SourceRowOrdinal);
            Assert.True(await ledger.MoveNextAsync());
            Assert.Equal(5, ledger.Current.RejectedRow.SourceRowOrdinal);
            Assert.False(await receipts.MoveNextAsync());
            Assert.False(await ledger.MoveNextAsync());

            const string unboundPlanDigest = "sensitive-plan-token";
            InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(
                async () => await CollectAsync(
                    snapshot.ReadOutcomeReceiptsAsync(unboundPlanDigest, Ct)));
            Assert.DoesNotContain(
                unboundPlanDigest,
                error.ToString(),
                StringComparison.Ordinal);
        }

        await using CSharpDbStagedMigrationTarget reopened =
            await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                files.TargetPath,
                plan,
                catalog,
                SyntheticMigrationDataSource.FixtureSnapshotIdentity,
                cancellationToken: Ct);
        await using IValidationSnapshot reopenedSnapshot =
            await reopened.OpenValidationSnapshotAsync(Ct);
        IMigrationRejectTargetValidationSnapshot rejectSnapshot =
            Assert.IsAssignableFrom<IMigrationRejectTargetValidationSnapshot>(
                reopenedSnapshot);

        Assert.Equal(snapshotIdentity, rejectSnapshot.SnapshotIdentity);
        Assert.Equal(
            [0L, 1L],
            (await CollectAsync(
                rejectSnapshot.ReadOutcomeReceiptsAsync(planDigest, Ct)))
            .Select(receipt => receipt.BatchOrdinal));
        Assert.Equal(
            [1L, 3L, 5L],
            (await CollectAsync(
                rejectSnapshot.ReadRejectLedgerAsync(planDigest, Ct)))
            .Select(entry => entry.RejectedRow.SourceRowOrdinal));
    }

    [Fact]
    public async Task DeterministicRejectTarget_FaultAfterRejectsRollsBackRowsLedgerAndReceipt()
    {
        using var files = new TemporaryTargetDirectory();
        MigrationCatalog catalog = await InspectAsync(Ct);
        MigrationPlan plan = ReadyDeterministicRejectPlan(catalog, batchSize: 3);
        MigrationTargetBatch batch = DeterministicBatch(
            plan,
            catalog,
            rows:
            [
                AcceptedRow(0, "zero"),
                AcceptedRow(2, "two"),
            ],
            rejectedRows: [RejectedRow(1, "bad-value")]);
        var injector = new ThrowOnceFaultInjector(
            CSharpDbMigrationFaultPoint.AfterRejectsBeforeReceipt);

        await using (CSharpDbStagedMigrationTarget target =
                     await CSharpDbStagedMigrationTarget.CreateNewAsync(
                         files.TargetPath,
                         plan,
                         catalog,
                         SyntheticMigrationDataSource.FixtureSnapshotIdentity,
                         injector,
                         Ct))
        {
            await target.ApplySchemaAsync(
                plan,
                catalog,
                MigrationSchemaStage.LoadEssential,
                Ct);
            await Assert.ThrowsAsync<InjectedMigrationFaultException>(async () =>
                await target.WriteBatchAsync(batch, Ct));
            Assert.True(injector.Fired);
        }

        Assert.Equal(0, await CountTargetRowsAsync(files.TargetPath, plan, Ct));
        await using CSharpDbStagedMigrationTarget resumed =
            await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                files.TargetPath,
                plan,
                catalog,
                SyntheticMigrationDataSource.FixtureSnapshotIdentity,
                cancellationToken: Ct);
        Assert.Null(await resumed.ReadReceiptAsync(
            batch.PlanDigest,
            batch.SourceObjectId,
            batch.BatchOrdinal,
            Ct));
        Assert.Empty(await ReadLedgerAsync(resumed, plan));
    }

    [Fact]
    public async Task DeterministicRejectTarget_NoncanonicalStoredEvidenceFailsReopen()
    {
        using var files = new TemporaryTargetDirectory();
        MigrationCatalog catalog = await InspectAsync(Ct);
        MigrationPlan plan = ReadyDeterministicRejectPlan(catalog, batchSize: 3);
        MigrationTargetBatch batch = DeterministicBatch(
            plan,
            catalog,
            rows:
            [
                AcceptedRow(0, "zero"),
                AcceptedRow(2, "two"),
            ],
            rejectedRows: [RejectedRow(1, "bad-value")]);

        await using (CSharpDbStagedMigrationTarget target =
                     await CreateDeterministicTargetAsync(files, plan, catalog))
        {
            _ = await target.WriteBatchAsync(batch, Ct);
        }
        await TamperRejectEvidenceJsonAsync(files.TargetPath, Ct);

        await Assert.ThrowsAsync<InvalidDataException>(async () =>
        {
            await using CSharpDbStagedMigrationTarget unexpected =
                await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                    files.TargetPath,
                    plan,
                    catalog,
                    SyntheticMigrationDataSource.FixtureSnapshotIdentity,
                    cancellationToken: Ct);
        });
    }

    [Fact]
    public async Task DeterministicRejectTarget_DisallowedRuleFailsBeforeMutationWithoutRawValueLeak()
    {
        const string secretRawValue = "TOP-SECRET-DISALLOWED-RAW-VALUE";
        using var files = new TemporaryTargetDirectory();
        MigrationCatalog catalog = await InspectAsync(Ct);
        MigrationPlan plan = ReadyDeterministicRejectPlan(catalog, batchSize: 2);
        MigrationRejectedRow rejected = RejectedRow(0, secretRawValue) with
        {
            RuleId = "MIG-CSV-ROW-999",
        };
        MigrationTargetBatch batch = DeterministicBatch(
            plan,
            catalog,
            rows: [],
            rejectedRows: [rejected]);

        await using (CSharpDbStagedMigrationTarget target =
                     await CreateDeterministicTargetAsync(files, plan, catalog))
        {
            InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(async () =>
                await target.WriteBatchAsync(batch, Ct));

            Assert.Contains("rule", error.Message, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain(secretRawValue, error.ToString(), StringComparison.Ordinal);
            Assert.Null(await target.ReadReceiptAsync(
                batch.PlanDigest,
                batch.SourceObjectId,
                batch.BatchOrdinal,
                Ct));
            Assert.Empty(await ReadLedgerAsync(target, plan));
        }

        Assert.Equal(0, await CountTargetRowsAsync(files.TargetPath, plan, Ct));
    }

    [Fact]
    public async Task DeterministicRejectTarget_CumulativeRunLimitSurvivesCommittedBatch()
    {
        using var files = new TemporaryTargetDirectory();
        MigrationCatalog catalog = await InspectAsync(Ct);
        MigrationPlan original = ReadyDeterministicRejectPlan(catalog, batchSize: 2);
        MigrationPlan plan = original with
        {
            Load = original.Load with
            {
                RejectPolicy = original.Load.RejectPolicy! with
                {
                    MaxRejectedRowsPerBatch = 1,
                    MaxRejectedRowsPerRun = 1,
                },
            },
        };
        MigrationTargetBatch first = DeterministicBatch(
            plan,
            catalog,
            rows: [],
            rejectedRows: [RejectedRow(0, "first")],
            batchOrdinal: 0,
            startCursor: null,
            nextCursor: "cursor:1");
        MigrationTargetBatch second = DeterministicBatch(
            plan,
            catalog,
            rows: [],
            rejectedRows: [RejectedRow(1, "second")],
            batchOrdinal: 1,
            startCursor: "cursor:1",
            nextCursor: null);

        await using CSharpDbStagedMigrationTarget target =
            await CreateDeterministicTargetAsync(files, plan, catalog);
        _ = await target.WriteBatchAsync(first, Ct);
        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await target.WriteBatchAsync(second, Ct));

        Assert.Contains("run limit", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.NotNull(await target.ReadReceiptAsync(
            first.PlanDigest,
            first.SourceObjectId,
            first.BatchOrdinal,
            Ct));
        Assert.Null(await target.ReadReceiptAsync(
            second.PlanDigest,
            second.SourceObjectId,
            second.BatchOrdinal,
            Ct));
        Assert.Single(await ReadLedgerAsync(target, plan));
    }

    [Fact]
    public async Task DeterministicRejectTarget_RejectsOrdinalGapAndWrongCursorBeforeMutation()
    {
        using var files = new TemporaryTargetDirectory();
        MigrationCatalog catalog = await InspectAsync(Ct);
        MigrationPlan plan = ReadyDeterministicRejectPlan(catalog, batchSize: 3);
        MigrationTargetBatch first = DeterministicBatch(
            plan,
            catalog,
            rows: [AcceptedRow(0, "zero")],
            nextCursor: "cursor:1");
        MigrationTargetBatch ordinalGap = DeterministicBatch(
            plan,
            catalog,
            rows: [AcceptedRow(1, "gap")],
            batchOrdinal: 2,
            startCursor: "cursor:1");
        MigrationTargetBatch wrongCursor = DeterministicBatch(
            plan,
            catalog,
            rows: [AcceptedRow(1, "wrong-cursor")],
            batchOrdinal: 1,
            startCursor: "cursor:wrong");

        await using (CSharpDbStagedMigrationTarget target =
                     await CreateDeterministicTargetAsync(files, plan, catalog))
        {
            _ = await target.WriteBatchAsync(first, Ct);

            await Assert.ThrowsAsync<InvalidDataException>(async () =>
                await target.WriteBatchAsync(ordinalGap, Ct));
            await Assert.ThrowsAsync<InvalidDataException>(async () =>
                await target.WriteBatchAsync(wrongCursor, Ct));

            Assert.Null(await target.ReadReceiptAsync(
                first.PlanDigest,
                first.SourceObjectId,
                batchOrdinal: 1,
                cancellationToken: Ct));
            Assert.Empty(await ReadLedgerAsync(target, plan));
        }

        Assert.Equal(1, await CountTargetRowsAsync(files.TargetPath, plan, Ct));
    }

    [Fact]
    public async Task DeterministicRejectTarget_RejectsAppendAfterTerminalBeforeMutation()
    {
        using var files = new TemporaryTargetDirectory();
        MigrationCatalog catalog = await InspectAsync(Ct);
        MigrationPlan plan = ReadyDeterministicRejectPlan(catalog, batchSize: 3);
        MigrationTargetBatch terminal = DeterministicBatch(
            plan,
            catalog,
            rows: [AcceptedRow(0, "terminal")]);
        MigrationTargetBatch append = DeterministicBatch(
            plan,
            catalog,
            rows: [AcceptedRow(1, "append")],
            batchOrdinal: 1);

        await using (CSharpDbStagedMigrationTarget target =
                     await CreateDeterministicTargetAsync(files, plan, catalog))
        {
            _ = await target.WriteBatchAsync(terminal, Ct);
            await Assert.ThrowsAsync<InvalidDataException>(async () =>
                await target.WriteBatchAsync(append, Ct));

            Assert.Null(await target.ReadReceiptAsync(
                terminal.PlanDigest,
                terminal.SourceObjectId,
                batchOrdinal: 1,
                cancellationToken: Ct));
        }

        Assert.Equal(1, await CountTargetRowsAsync(files.TargetPath, plan, Ct));
    }

    [Fact]
    public async Task DeterministicRejectTarget_RejectsPostLoadSchemaUntilChainIsTerminal()
    {
        using var files = new TemporaryTargetDirectory();
        MigrationCatalog catalog = await InspectAsync(Ct);
        MigrationPlan plan = ReadyDeterministicRejectPlan(catalog, batchSize: 3);
        MigrationTargetBatch nonterminal = DeterministicBatch(
            plan,
            catalog,
            rows: [AcceptedRow(0, "zero")],
            nextCursor: "cursor:1");
        MigrationTargetBatch terminal = DeterministicBatch(
            plan,
            catalog,
            rows: [AcceptedRow(1, "one")],
            batchOrdinal: 1,
            startCursor: "cursor:1");

        await using (CSharpDbStagedMigrationTarget target =
                     await CreateDeterministicTargetAsync(files, plan, catalog))
        {
            _ = await target.WriteBatchAsync(nonterminal, Ct);
            await Assert.ThrowsAsync<InvalidDataException>(async () =>
                await target.ApplySchemaAsync(
                    plan,
                    catalog,
                    MigrationSchemaStage.SecondaryIndexes,
                    Ct));

            _ = await target.WriteBatchAsync(terminal, Ct);
            await target.ApplySchemaAsync(
                plan,
                catalog,
                MigrationSchemaStage.SecondaryIndexes,
                Ct);
        }

        Assert.Equal(2, await CountTargetRowsAsync(files.TargetPath, plan, Ct));
    }

    [Fact]
    public async Task DeterministicRejectTarget_ChargesEveryEvidenceValueToSensitiveBudget()
    {
        using var files = new TemporaryTargetDirectory();
        MigrationCatalog catalog = await InspectAsync(Ct);
        MigrationPlan plan = ReadyDeterministicRejectPlan(catalog, batchSize: 1);
        plan = plan with
        {
            Load = plan.Load with
            {
                RejectPolicy = plan.Load.RejectPolicy! with
                {
                    MaxRawValueBytes = 4,
                },
            },
        };
        MigrationTargetBatch batch = DeterministicBatch(
            plan,
            catalog,
            rows: [],
            rejectedRows:
            [
                RejectedRow(
                    0,
                    "12345",
                    evidenceName: "decodedValue"),
            ]);

        await using (CSharpDbStagedMigrationTarget target =
                     await CreateDeterministicTargetAsync(files, plan, catalog))
        {
            await Assert.ThrowsAsync<InvalidDataException>(async () =>
                await target.WriteBatchAsync(batch, Ct));
            Assert.Null(await target.ReadReceiptAsync(
                batch.PlanDigest,
                batch.SourceObjectId,
                batch.BatchOrdinal,
                Ct));
            Assert.Empty(await ReadLedgerAsync(target, plan));
        }

        Assert.Equal(0, await CountTargetRowsAsync(files.TargetPath, plan, Ct));
    }

    [Fact]
    public async Task DeterministicRejectTarget_EnforcesCanonicalArtifactBudgetBeforeMutation()
    {
        using var files = new TemporaryTargetDirectory();
        MigrationCatalog catalog = await InspectAsync(Ct);
        MigrationPlan plan = ReadyDeterministicRejectPlan(catalog, batchSize: 1);
        plan = plan with
        {
            Load = plan.Load with
            {
                RejectPolicy = plan.Load.RejectPolicy! with
                {
                    MaxArtifactBytes =
                        MigrationRejectLedgerCodec.MinimumCanonicalArtifactBytes,
                },
            },
        };
        MigrationTargetBatch batch = DeterministicBatch(
            plan,
            catalog,
            rows: [],
            rejectedRows: [RejectedRow(0, "x")]);

        await using (CSharpDbStagedMigrationTarget target =
                     await CreateDeterministicTargetAsync(files, plan, catalog))
        {
            await Assert.ThrowsAsync<InvalidDataException>(async () =>
                await target.WriteBatchAsync(batch, Ct));
            Assert.Null(await target.ReadReceiptAsync(
                batch.PlanDigest,
                batch.SourceObjectId,
                batch.BatchOrdinal,
                Ct));
            Assert.Empty(await ReadLedgerAsync(target, plan));
        }

        Assert.Equal(0, await CountTargetRowsAsync(files.TargetPath, plan, Ct));
    }

    [Fact]
    public async Task DeterministicRejectTarget_ConcurrentBatchesCannotRaceRunRejectLimit()
    {
        using var files = new TemporaryTargetDirectory();
        MigrationCatalog catalog = await InspectAsync(Ct);
        MigrationPlan plan = ReadyDeterministicRejectPlan(catalog, batchSize: 1);
        plan = plan with
        {
            Load = plan.Load with
            {
                RejectPolicy = plan.Load.RejectPolicy! with
                {
                    MaxRejectedRowsPerRun = 1,
                },
            },
        };
        MigrationTargetBatch first = DeterministicBatch(
            plan,
            catalog,
            rows: [],
            rejectedRows: [RejectedRow(0, "first")],
            nextCursor: "cursor:1");
        MigrationTargetBatch second = DeterministicBatch(
            plan,
            catalog,
            rows: [],
            rejectedRows: [RejectedRow(1, "second")],
            batchOrdinal: 1,
            startCursor: "cursor:1");
        var injector = new BlockingBeforeRowsFaultInjector();

        await using CSharpDbStagedMigrationTarget target =
            await CSharpDbStagedMigrationTarget.CreateNewAsync(
                files.TargetPath,
                plan,
                catalog,
                SyntheticMigrationDataSource.FixtureSnapshotIdentity,
                injector,
                Ct);
        await target.ApplySchemaAsync(
            plan,
            catalog,
            MigrationSchemaStage.LoadEssential,
            Ct);

        Task<MigrationBatchReceipt> firstWrite = target.WriteBatchAsync(first, Ct).AsTask();
        await injector.WaitUntilBlockedAsync(Ct);
        Task<MigrationBatchReceipt> secondWrite = target.WriteBatchAsync(second, Ct).AsTask();
        try
        {
            injector.Release();
            _ = await firstWrite;
            await Assert.ThrowsAsync<InvalidDataException>(async () => await secondWrite);
        }
        finally
        {
            injector.Release();
        }

        Assert.NotNull(await target.ReadReceiptAsync(
            first.PlanDigest,
            first.SourceObjectId,
            first.BatchOrdinal,
            Ct));
        Assert.Null(await target.ReadReceiptAsync(
            second.PlanDigest,
            second.SourceObjectId,
            second.BatchOrdinal,
            Ct));
        Assert.Single(await ReadLedgerAsync(target, plan));
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

    [Fact]
    public async Task Resume_RejectsSafelyTamperedRejectDigest()
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

        await TamperRejectDigestAsync(files.TargetPath, Ct);

        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(() =>
            OpenAndDisposeAsync(
                files.TargetPath,
                plan,
                catalog,
                SyntheticMigrationDataSource.FixtureSnapshotIdentity,
                Ct));

        Assert.Contains("receipt", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("reject digest", error.Message, StringComparison.OrdinalIgnoreCase);
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

    private static MigrationPlan ReadyDeterministicRejectPlan(
        MigrationCatalog catalog,
        int batchSize)
    {
        MigrationPlan plan = ReadyPlan(catalog, batchSize);
        return plan with
        {
            Load = plan.Load with
            {
                RejectMode = MigrationRejectMode.DeterministicRejects,
                RejectPolicy = new MigrationDeterministicRejectPolicy
                {
                    ContractVersion = MigrationRejectContract.DeterministicRejectsV1,
                    AllowedRuleIds = [DeterministicRuleId],
                    MaxRejectedRowsPerBatch = batchSize,
                    MaxRejectedRowsPerRun = 100,
                    MaxRawValueBytes = 1_024,
                    MaxRawValueBytesPerBatch = 8_192,
                    MaxRawValueBytesPerRun = 65_536,
                    MaxArtifactBytes = 131_072,
                },
            },
        };
    }

    private static async ValueTask<CSharpDbStagedMigrationTarget> CreateDeterministicTargetAsync(
        TemporaryTargetDirectory files,
        MigrationPlan plan,
        MigrationCatalog catalog)
    {
        CSharpDbStagedMigrationTarget target =
            await CSharpDbStagedMigrationTarget.CreateNewAsync(
                files.TargetPath,
                plan,
                catalog,
                SyntheticMigrationDataSource.FixtureSnapshotIdentity,
                cancellationToken: Ct);
        try
        {
            await target.ApplySchemaAsync(
                plan,
                catalog,
                MigrationSchemaStage.LoadEssential,
                Ct);
            return target;
        }
        catch
        {
            await target.DisposeAsync();
            throw;
        }
    }

    private static MigrationTargetBatch DeterministicBatch(
        MigrationPlan plan,
        MigrationCatalog catalog,
        IReadOnlyList<MigrationTargetRow> rows,
        IReadOnlyList<MigrationRejectedRow>? rejectedRows = null,
        long batchOrdinal = 0,
        string? startCursor = null,
        string? nextCursor = null)
    {
        string[] columnObjectIds = IncludedColumnIds(
            catalog,
            plan,
            RejectSourceObjectId);
        var unsigned = new MigrationTargetBatch
        {
            PlanDigest = MigrationArtifactSerializer.ComputePlanDigest(plan),
            CatalogDigest = plan.CatalogDigest,
            SourceFingerprint = plan.Source.Fingerprint,
            SourceSnapshotIdentity = SyntheticMigrationDataSource.FixtureSnapshotIdentity,
            SourceObjectId = RejectSourceObjectId,
            ColumnObjectIds = columnObjectIds,
            BatchOrdinal = batchOrdinal,
            StartCursor = startCursor,
            NextCursor = nextCursor,
            BatchDigest = string.Empty,
            RejectContractVersion = MigrationRejectContract.DeterministicRejectsV1,
            Rows = rows,
            RejectedRows = rejectedRows ?? [],
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

    private static MigrationTargetRow AcceptedRow(long sourceRowOrdinal, string suffix) => new()
    {
        SourceRowOrdinal = sourceRowOrdinal,
        StableKey = suffix,
        Values =
        [
            DbValue.FromText($"lower-{suffix}"),
            DbValue.FromText($"upper-{suffix}"),
        ],
    };

    private static MigrationRejectedRow RejectedRow(
        long sourceRowOrdinal,
        string rawValue,
        string evidenceName = MigrationRejectLedgerCodec.RawValueEvidenceName) => new()
        {
            SourceRowOrdinal = sourceRowOrdinal,
            RuleId = DeterministicRuleId,
            ColumnObjectId = RejectColumnObjectId,
            Evidence =
        [
            new MigrationRejectEvidence
            {
                Name = evidenceName,
                Value = rawValue,
            },
        ],
        };

    private static void AssertRejectedRowEqual(
        MigrationRejectedRow expected,
        MigrationRejectedRow actual)
    {
        Assert.Equal(expected.SourceRowOrdinal, actual.SourceRowOrdinal);
        Assert.Equal(expected.RuleId, actual.RuleId);
        Assert.Equal(expected.ColumnObjectId, actual.ColumnObjectId);
        MigrationRejectEvidence expectedEvidence = Assert.Single(expected.Evidence);
        MigrationRejectEvidence actualEvidence = Assert.Single(actual.Evidence);
        Assert.Equal(expectedEvidence.Name, actualEvidence.Name);
        Assert.Equal(expectedEvidence.Value, actualEvidence.Value);
    }

    private static async Task<List<MigrationRejectLedgerEntry>> ReadLedgerAsync(
        CSharpDbStagedMigrationTarget target,
        MigrationPlan plan)
    {
        var entries = new List<MigrationRejectLedgerEntry>();
        await foreach (MigrationRejectLedgerEntry entry in target.ReadRejectLedgerAsync(
                           MigrationArtifactSerializer.ComputePlanDigest(plan),
                           Ct))
        {
            entries.Add(entry);
        }
        return entries;
    }

    private static async Task<long> CountTargetRowsAsync(
        string targetPath,
        MigrationPlan plan,
        CancellationToken cancellationToken)
    {
        string targetName = plan.Objects.Single(item =>
            string.Equals(
                item.SourceObjectId,
                RejectSourceObjectId,
                StringComparison.Ordinal)).TargetName!;
        string quoted = $"\"{targetName.Replace("\"", "\"\"", StringComparison.Ordinal)}\"";
        await using Database database = await Database.OpenAsync(targetPath, cancellationToken);
        await using var result = await database.ExecuteAsync(
            $"SELECT COUNT(*) FROM {quoted}",
            cancellationToken);
        Assert.True(await result.MoveNextAsync(cancellationToken));
        return result.Current[0].AsInteger;
    }

    private static async Task TamperRejectEvidenceJsonAsync(
        string targetPath,
        CancellationToken cancellationToken)
    {
        await using Database database = await Database.OpenAsync(targetPath, cancellationToken);
        await using var result = await database.ExecuteAsync(
            "UPDATE \"__csharpdb_migration_rejects\" " +
            "SET \"evidence_json\" = '[ {\"name\":\"rawValue\",\"value\":\"bad-value\"} ]' " +
            "WHERE \"source_object_id\" = 'syn:table:customers-lower' " +
            "AND \"batch_ordinal\" = 0 AND \"source_row_ordinal\" = 1",
            cancellationToken);
        Assert.Equal(1, result.RowsAffected);
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

    private static async Task ConvertEmptyTargetToLegacyV1Async(
        string targetPath,
        CancellationToken cancellationToken)
    {
        await using Database database = await Database.OpenAsync(targetPath, cancellationToken);
        bool transactionStarted = false;
        try
        {
            await database.BeginTransactionAsync(cancellationToken);
            transactionStarted = true;
            await ExecuteNonQueryAsync(
                database,
                "UPDATE \"__csharpdb_migration_state\" " +
                "SET \"target_tag\" = 'csharpdb-staged-migration-target/v1' " +
                "WHERE \"singleton\" = 1",
                cancellationToken);
            await ExecuteNonQueryAsync(
                database,
                "DROP TABLE \"__csharpdb_migration_rejects\"",
                cancellationToken);
            await ExecuteNonQueryAsync(
                database,
                "DROP TABLE \"__csharpdb_migration_receipts\"",
                cancellationToken);
            await ExecuteNonQueryAsync(
                database,
                "CREATE TABLE \"__csharpdb_migration_receipts\" (" +
                "\"receipt_tag\" TEXT NOT NULL, " +
                "\"target_identity\" TEXT NOT NULL, " +
                "\"plan_digest\" TEXT NOT NULL, " +
                "\"catalog_digest\" TEXT NOT NULL, " +
                "\"source_fingerprint\" TEXT NOT NULL, " +
                "\"source_snapshot_identity\" TEXT NOT NULL, " +
                "\"source_object_id\" TEXT NOT NULL, " +
                "\"batch_ordinal\" INTEGER NOT NULL, " +
                "\"start_cursor\" TEXT, " +
                "\"next_cursor\" TEXT, " +
                "\"batch_digest\" TEXT NOT NULL, " +
                "\"row_count\" INTEGER NOT NULL, " +
                "\"rejected_row_count\" INTEGER NOT NULL, " +
                "CONSTRAINT \"__csharpdb_migration_receipts_pk\" " +
                "PRIMARY KEY (\"plan_digest\", \"source_object_id\", \"batch_ordinal\"))",
                cancellationToken);
            await database.CommitAsync(CancellationToken.None);
        }
        catch
        {
            if (transactionStarted)
                await database.RollbackAsync(CancellationToken.None);
            throw;
        }
    }

    private static async Task<(string TargetTag, string ReceiptTag)> ReadLegacyTagsAsync(
        string targetPath,
        CancellationToken cancellationToken)
    {
        await using Database database = await Database.OpenAsync(targetPath, cancellationToken);
        await using var state = await database.ExecuteAsync(
            "SELECT \"target_tag\" FROM \"__csharpdb_migration_state\" WHERE \"singleton\" = 1",
            cancellationToken);
        Assert.True(await state.MoveNextAsync(cancellationToken));
        string targetTag = state.Current[0].AsText;

        await using var receipt = await database.ExecuteAsync(
            "SELECT \"receipt_tag\" FROM \"__csharpdb_migration_receipts\" " +
            "WHERE \"batch_ordinal\" = 0",
            cancellationToken);
        Assert.True(await receipt.MoveNextAsync(cancellationToken));
        return (targetTag, receipt.Current[0].AsText);
    }

    private static async Task ExecuteNonQueryAsync(
        Database database,
        string sql,
        CancellationToken cancellationToken)
    {
        await using var result = await database.ExecuteAsync(sql, cancellationToken);
    }

    private static async Task TamperRejectDigestAsync(
        string targetPath,
        CancellationToken cancellationToken)
    {
        await using Database database = await Database.OpenAsync(targetPath, cancellationToken);
        await using var result = await database.ExecuteAsync(
            "UPDATE \"__csharpdb_migration_receipts\" " +
            $"SET \"reject_digest\" = '{new string('0', 64)}' " +
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

    private sealed class BlockingBeforeRowsFaultInjector : ICSharpDbMigrationFaultInjector
    {
        private readonly TaskCompletionSource _blocked =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _release =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _fired;

        public async ValueTask InjectAsync(
            CSharpDbMigrationFaultPoint point,
            MigrationTargetBatch batch,
            CancellationToken cancellationToken = default)
        {
            if (point != CSharpDbMigrationFaultPoint.BeforeRows ||
                Interlocked.Exchange(ref _fired, 1) != 0)
            {
                return;
            }

            _blocked.TrySetResult();
            await _release.Task.WaitAsync(cancellationToken);
        }

        public Task WaitUntilBlockedAsync(CancellationToken cancellationToken) =>
            _blocked.Task.WaitAsync(cancellationToken);

        public void Release() => _release.TrySetResult();
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
