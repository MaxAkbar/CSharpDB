using CSharpDB.Migration.Canonicalization;
using CSharpDB.Migration.CSharpDb;
using CSharpDB.Migration.Validation;
using CSharpDB.Engine;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Tests;

public sealed class CSharpDbValidationActivationTests
{
    private const string GoldenTargetIdentity = "00000000-0000-0000-0000-000000000042";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ValidationSnapshot_ReportsEstablishedActualSchemaEvidence()
    {
        using var files = new TemporaryTargetDirectory();
        (MigrationCatalog catalog, MigrationPlan plan) = await ArtifactsAsync();

        await using var source = new SyntheticMigrationDataSource(catalog);
        await using CSharpDbStagedMigrationTarget target =
            await CSharpDbStagedMigrationTarget.CreateNewAsync(
                files.TargetPath,
                plan,
                catalog,
                source.SnapshotIdentity,
                cancellationToken: Ct);
        await ApplyAsync(plan, catalog, source, target);

        await using IValidationSnapshot opened = await target.OpenValidationSnapshotAsync(Ct);
        IMigrationEvidenceValidationSnapshot snapshot = Assert.IsAssignableFrom<IMigrationEvidenceValidationSnapshot>(opened);
        Assert.Equal(MigrationSnapshotConsistencyStatus.Established, snapshot.ConsistencyStatus);
        MigrationNormalizedSchema actual = await snapshot.ReadSchemaAsync(Ct);
        MigrationNormalizedSchema expected = MigrationNormalizedSchemaContract.CreateExpected(plan, catalog);

        IReadOnlyList<MigrationNormalizedSchemaDifference> differences =
            MigrationNormalizedSchemaContract.Compare(expected, actual);
        Assert.True(
            differences.Count == 0,
            string.Join(
                Environment.NewLine,
                differences.Select(item =>
                {
                    MigrationNormalizedSchemaObject? left = expected.Objects.SingleOrDefault(x => x.ObjectId == item.ObjectId);
                    MigrationNormalizedSchemaObject? right = actual.Objects.SingleOrDefault(x => x.ObjectId == item.ObjectId);
                    return $"{item.ObjectId}: " +
                        $"[{string.Join(",", left?.Attributes.Select(x => $"{x.Name}={x.Value}") ?? [])}] != " +
                        $"[{string.Join(",", right?.Attributes.Select(x => $"{x.Name}={x.Value}") ?? [])}]";
                })));
        Assert.Equal(expected.Digest, actual.Digest);
        Assert.Contains(actual.Objects, item => item.Kind == MigrationObjectKind.Table);
        Assert.Contains(actual.Objects, item => item.Kind == MigrationObjectKind.Column);
        Assert.Contains(actual.Objects, item => item.Kind == MigrationObjectKind.Index);
        Assert.Contains(actual.Objects, item => item.Kind == MigrationObjectKind.Key);
        Assert.Contains(actual.Objects, item => item.Kind == MigrationObjectKind.ForeignKey);
    }

    [Fact]
    public async Task Activate_PersistsReceiptAndLifecycleTogetherAcrossReopen()
    {
        using var files = new TemporaryTargetDirectory();
        (MigrationCatalog catalog, MigrationPlan plan) = await ArtifactsAsync();
        MigrationValidationActivationReceipt receipt;
        string snapshotIdentity;
        string planDigest = MigrationArtifactSerializer.ComputePlanDigest(plan);

        await using (var source = new SyntheticMigrationDataSource(catalog))
        await using (CSharpDbStagedMigrationTarget target =
                     await CSharpDbStagedMigrationTarget.CreateNewAsync(
                         files.TargetPath,
                         plan,
                         catalog,
                         source.SnapshotIdentity,
                         cancellationToken: Ct))
        {
            await ApplyAsync(plan, catalog, source, target);
            await using IValidationSnapshot beforeActivation =
                await target.OpenValidationSnapshotAsync(Ct);
            IMigrationRejectTargetValidationSnapshot beforeOutcomes =
                Assert.IsAssignableFrom<IMigrationRejectTargetValidationSnapshot>(
                    beforeActivation);
            snapshotIdentity = beforeOutcomes.SnapshotIdentity;
            MigrationValidationActivationPermit permit = await ActivationPermitAsync(
                target,
                plan,
                catalog,
                source.SnapshotIdentity,
                Path.Combine(files.DirectoryPath, "activation.json"));
            receipt = permit.Receipt;
            await target.ActivateAsync(permit, Ct);
            Assert.Equal(receipt, await target.ReadActivationReceiptAsync(Ct));

            Assert.Equal(snapshotIdentity, beforeOutcomes.SnapshotIdentity);
            await using IValidationSnapshot afterActivation =
                await target.OpenValidationSnapshotAsync(Ct);
            IMigrationRejectTargetValidationSnapshot afterOutcomes =
                Assert.IsAssignableFrom<IMigrationRejectTargetValidationSnapshot>(
                    afterActivation);
            Assert.Equal(snapshotIdentity, afterOutcomes.SnapshotIdentity);
            List<MigrationBatchReceipt> receipts = await CollectAsync(
                afterOutcomes.ReadOutcomeReceiptsAsync(planDigest, Ct));
            Assert.Equal(11, receipts.Count);
            Assert.Equal(
                receipts
                    .OrderBy(item => item.SourceObjectId, StringComparer.Ordinal)
                    .ThenBy(item => item.BatchOrdinal)
                    .ToArray(),
                receipts);
            Assert.Empty(await CollectAsync(
                afterOutcomes.ReadRejectLedgerAsync(planDigest, Ct)));
        }

        await using CSharpDbStagedMigrationTarget reopened =
            await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                files.TargetPath,
                plan,
                catalog,
                SyntheticMigrationDataSource.FixtureSnapshotIdentity,
                cancellationToken: Ct);
        Assert.Equal(receipt, await reopened.ReadActivationReceiptAsync(Ct));
        await using IValidationSnapshot snapshot = await reopened.OpenValidationSnapshotAsync(Ct);
        Assert.Equal(snapshotIdentity, snapshot.SnapshotIdentity);
        Assert.Equal(receipt.TargetSnapshotIdentity, snapshot.SnapshotIdentity);
    }

    [Fact]
    public async Task LegacySnapshotIdentity_ActivatedTargetReopensAndExactRetryIsIdempotent()
    {
        using var files = new TemporaryTargetDirectory();
        (MigrationCatalog catalog, MigrationPlan plan) = await ArtifactsAsync();
        MigrationValidationActivationPermit legacyPermit;

        await using (var source = new SyntheticMigrationDataSource(catalog))
        await using (CSharpDbStagedMigrationTarget target =
                     await CSharpDbStagedMigrationTarget.CreateNewAsync(
                         files.TargetPath,
                         plan,
                         catalog,
                         source.SnapshotIdentity,
                         cancellationToken: Ct))
        {
            await ApplyAsync(plan, catalog, source, target);
            string legacySnapshotIdentity =
                $"staged-target:{target.TargetIdentity}:awaiting-validation";
            legacyPermit = await ActivationPermitAsync(
                target,
                plan,
                catalog,
                source.SnapshotIdentity,
                Path.Combine(files.DirectoryPath, "legacy-activation.json"),
                targetSnapshotIdentityOverride: legacySnapshotIdentity);
        }

        await PersistLegacyActivationAsync(files.TargetPath, legacyPermit.Receipt);

        await using CSharpDbStagedMigrationTarget reopened =
            await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                files.TargetPath,
                plan,
                catalog,
                SyntheticMigrationDataSource.FixtureSnapshotIdentity,
                cancellationToken: Ct);
        Assert.Equal(legacyPermit.Receipt, await reopened.ReadActivationReceiptAsync(Ct));
        await using (IValidationSnapshot snapshot = await reopened.OpenValidationSnapshotAsync(Ct))
            Assert.Equal(legacyPermit.Receipt.TargetSnapshotIdentity, snapshot.SnapshotIdentity);

        await reopened.ActivateAsync(legacyPermit, Ct);
        await reopened.ActivateAsync(legacyPermit, Ct);

        Assert.Equal(legacyPermit.Receipt, await reopened.ReadActivationReceiptAsync(Ct));
    }

    [Fact]
    public async Task LegacySnapshotIdentity_AwaitingV2TargetCanActivatePublishedReport()
    {
        using var files = new TemporaryTargetDirectory();
        (MigrationCatalog catalog, MigrationPlan plan) = await ArtifactsAsync();
        MigrationValidationActivationPermit legacyPermit;

        await using (var source = new SyntheticMigrationDataSource(catalog))
        await using (CSharpDbStagedMigrationTarget target =
                     await CSharpDbStagedMigrationTarget.CreateNewAsync(
                         files.TargetPath,
                         plan,
                         catalog,
                         source.SnapshotIdentity,
                         cancellationToken: Ct))
        {
            await ApplyAsync(plan, catalog, source, target);
            legacyPermit = await ActivationPermitAsync(
                target,
                plan,
                catalog,
                source.SnapshotIdentity,
                Path.Combine(files.DirectoryPath, "legacy-pending-activation.json"),
                targetSnapshotIdentityOverride:
                    $"staged-target:{target.TargetIdentity}:awaiting-validation");
        }

        await RewriteTargetTagAsV2Async(files.TargetPath);

        await using CSharpDbStagedMigrationTarget reopened =
            await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                files.TargetPath,
                plan,
                catalog,
                SyntheticMigrationDataSource.FixtureSnapshotIdentity,
                cancellationToken: Ct);
        await reopened.ActivateAsync(legacyPermit, Ct);

        Assert.Equal(legacyPermit.Receipt, await reopened.ReadActivationReceiptAsync(Ct));
        await using IValidationSnapshot snapshot = await reopened.OpenValidationSnapshotAsync(Ct);
        Assert.Equal(legacyPermit.Receipt.TargetSnapshotIdentity, snapshot.SnapshotIdentity);
    }

    [Fact]
    public async Task ValidationSnapshot_OutcomeDigestMatchesGoldenVector()
    {
        using var files = new TemporaryTargetDirectory();
        (MigrationCatalog catalog, MigrationPlan plan) = await ArtifactsAsync();
        string originalTargetIdentity;

        await using (var source = new SyntheticMigrationDataSource(catalog))
        await using (CSharpDbStagedMigrationTarget target =
                     await CSharpDbStagedMigrationTarget.CreateNewAsync(
                         files.TargetPath,
                         plan,
                         catalog,
                         source.SnapshotIdentity,
                         cancellationToken: Ct))
        {
            originalTargetIdentity = target.TargetIdentity;
            await ApplyAsync(plan, catalog, source, target);
        }

        await RewriteTargetIdentityAsync(
            files.TargetPath,
            originalTargetIdentity,
            GoldenTargetIdentity);

        await using CSharpDbStagedMigrationTarget reopened =
            await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                files.TargetPath,
                plan,
                catalog,
                SyntheticMigrationDataSource.FixtureSnapshotIdentity,
                cancellationToken: Ct);
        await using IValidationSnapshot snapshot = await reopened.OpenValidationSnapshotAsync(Ct);

        string expectedIdentity =
            $"staged-target:{GoldenTargetIdentity}:awaiting-validation:outcomes:" +
            "f9bc8debd0435a03472236172bde3d45200cf0fc92f1df9c357e289367b4ab35";
        Assert.True(
            string.Equals(expectedIdentity, snapshot.SnapshotIdentity, StringComparison.Ordinal),
            $"Validation snapshot golden identity changed. Actual value: {snapshot.SnapshotIdentity}");
    }

    [Fact]
    public async Task Activate_ExactRetryIsIdempotent()
    {
        using var files = new TemporaryTargetDirectory();
        (MigrationCatalog catalog, MigrationPlan plan) = await ArtifactsAsync();
        await using var source = new SyntheticMigrationDataSource(catalog);
        await using CSharpDbStagedMigrationTarget target =
            await CSharpDbStagedMigrationTarget.CreateNewAsync(
                files.TargetPath,
                plan,
                catalog,
                source.SnapshotIdentity,
                cancellationToken: Ct);
        await ApplyAsync(plan, catalog, source, target);
        MigrationValidationActivationPermit permit = await ActivationPermitAsync(
            target,
            plan,
            catalog,
            source.SnapshotIdentity,
            Path.Combine(files.DirectoryPath, "retry.json"));
        MigrationValidationActivationReceipt receipt = permit.Receipt;

        await target.ActivateAsync(permit, Ct);
        await target.ActivateAsync(permit, Ct);

        Assert.Equal(receipt, await target.ReadActivationReceiptAsync(Ct));
    }

    [Fact]
    public async Task Activate_ChangedReportIsRejectedWithoutReplacingReceipt()
    {
        using var files = new TemporaryTargetDirectory();
        (MigrationCatalog catalog, MigrationPlan plan) = await ArtifactsAsync();
        await using var source = new SyntheticMigrationDataSource(catalog);
        await using CSharpDbStagedMigrationTarget target =
            await CSharpDbStagedMigrationTarget.CreateNewAsync(
                files.TargetPath,
                plan,
                catalog,
                source.SnapshotIdentity,
                cancellationToken: Ct);
        await ApplyAsync(plan, catalog, source, target);
        MigrationValidationActivationPermit originalPermit = await ActivationPermitAsync(
            target,
            plan,
            catalog,
            source.SnapshotIdentity,
            Path.Combine(files.DirectoryPath, "original.json"));
        MigrationValidationActivationReceipt original = originalPermit.Receipt;
        await target.ActivateAsync(originalPermit, Ct);

        MigrationValidationActivationPermit changed = await ActivationPermitAsync(
            target,
            plan,
            catalog,
            source.SnapshotIdentity,
            Path.Combine(files.DirectoryPath, "changed.json"),
            diagnosticSuffix: "changed");
        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await target.ActivateAsync(changed, Ct));

        Assert.Contains("different validation receipt", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(original, await target.ReadActivationReceiptAsync(Ct));
    }

    [Fact]
    public async Task Activate_BeforeAwaitingValidationIsRejectedWithoutReceipt()
    {
        using var files = new TemporaryTargetDirectory();
        (MigrationCatalog catalog, MigrationPlan plan) = await ArtifactsAsync();
        await using var source = new SyntheticMigrationDataSource(catalog);
        await using CSharpDbStagedMigrationTarget target =
            await CSharpDbStagedMigrationTarget.CreateNewAsync(
                files.TargetPath,
                plan,
                catalog,
                source.SnapshotIdentity,
                cancellationToken: Ct);
        MigrationValidationActivationPermit permit = await ActivationPermitAsync(
            target,
            plan,
            catalog,
            source.SnapshotIdentity,
            Path.Combine(files.DirectoryPath, "premature.json"));

        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await target.ActivateAsync(permit, Ct));

        Assert.Contains("cannot activate", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Null(await target.ReadActivationReceiptAsync(Ct));
    }

    [Fact]
    public async Task Activate_MissingPublishedReportIsRejectedWithoutReceipt()
    {
        using var files = new TemporaryTargetDirectory();
        (MigrationCatalog catalog, MigrationPlan plan) = await ArtifactsAsync();
        await using var source = new SyntheticMigrationDataSource(catalog);
        await using CSharpDbStagedMigrationTarget target =
            await CSharpDbStagedMigrationTarget.CreateNewAsync(
                files.TargetPath,
                plan,
                catalog,
                source.SnapshotIdentity,
                cancellationToken: Ct);
        await ApplyAsync(plan, catalog, source, target);
        MigrationValidationActivationPermit permit = await ActivationPermitAsync(
            target,
            plan,
            catalog,
            source.SnapshotIdentity,
            Path.Combine(files.DirectoryPath, "missing.json"));
        File.Delete(permit.PublishedReportPath);

        await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await target.ActivateAsync(permit, Ct));

        Assert.Null(await target.ReadActivationReceiptAsync(Ct));
    }

    [Fact]
    public async Task Activate_TamperedPublishedReportIsRejectedWithoutReceipt()
    {
        using var files = new TemporaryTargetDirectory();
        (MigrationCatalog catalog, MigrationPlan plan) = await ArtifactsAsync();
        await using var source = new SyntheticMigrationDataSource(catalog);
        await using CSharpDbStagedMigrationTarget target =
            await CSharpDbStagedMigrationTarget.CreateNewAsync(
                files.TargetPath,
                plan,
                catalog,
                source.SnapshotIdentity,
                cancellationToken: Ct);
        await ApplyAsync(plan, catalog, source, target);
        MigrationValidationActivationPermit permit = await ActivationPermitAsync(
            target,
            plan,
            catalog,
            source.SnapshotIdentity,
            Path.Combine(files.DirectoryPath, "tampered.json"));
        await File.AppendAllTextAsync(permit.PublishedReportPath, "{}", Ct);

        await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await target.ActivateAsync(permit, Ct));

        Assert.Null(await target.ReadActivationReceiptAsync(Ct));
    }

    [Fact]
    public async Task Activate_OversizedPublishedReportIsRejectedBeforeRead()
    {
        using var files = new TemporaryTargetDirectory();
        (MigrationCatalog catalog, MigrationPlan plan) = await ArtifactsAsync();
        await using var source = new SyntheticMigrationDataSource(catalog);
        await using CSharpDbStagedMigrationTarget target =
            await CSharpDbStagedMigrationTarget.CreateNewAsync(
                files.TargetPath,
                plan,
                catalog,
                source.SnapshotIdentity,
                cancellationToken: Ct);
        await ApplyAsync(plan, catalog, source, target);
        MigrationValidationActivationPermit permit = await ActivationPermitAsync(
            target,
            plan,
            catalog,
            source.SnapshotIdentity,
            Path.Combine(files.DirectoryPath, "oversized.json"));
        await using (var stream = new FileStream(
                         permit.PublishedReportPath,
                         FileMode.Open,
                         FileAccess.Write,
                         FileShare.None))
        {
            stream.SetLength(MigrationValidationReportSerializer.MaximumArtifactBytes + 1L);
        }

        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await target.ActivateAsync(permit, Ct));

        Assert.Contains("maximum artifact", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Null(await target.ReadActivationReceiptAsync(Ct));
    }

    [Fact]
    public async Task Activate_InconclusivePublishedReportIsRejectedWithoutReceipt()
    {
        using var files = new TemporaryTargetDirectory();
        (MigrationCatalog catalog, MigrationPlan plan) = await ArtifactsAsync();
        await using var source = new SyntheticMigrationDataSource(catalog);
        await using CSharpDbStagedMigrationTarget target =
            await CSharpDbStagedMigrationTarget.CreateNewAsync(
                files.TargetPath,
                plan,
                catalog,
                source.SnapshotIdentity,
                cancellationToken: Ct);
        await ApplyAsync(plan, catalog, source, target);
        MigrationValidationActivationPermit permit = await ActivationPermitAsync(
            target,
            plan,
            catalog,
            source.SnapshotIdentity,
            Path.Combine(files.DirectoryPath, "inconclusive.json"),
            consistency: MigrationSnapshotConsistencyStatus.NotEstablished);

        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await target.ActivateAsync(permit, Ct));

        Assert.Contains("passed report", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Null(await target.ReadActivationReceiptAsync(Ct));
    }

    [Fact]
    public async Task TargetPrimaryHandle_RefusesASecondDatabaseWriter()
    {
        using var files = new TemporaryTargetDirectory();
        (MigrationCatalog catalog, MigrationPlan plan) = await ArtifactsAsync();
        await using var source = new SyntheticMigrationDataSource(catalog);
        await using CSharpDbStagedMigrationTarget target =
            await CSharpDbStagedMigrationTarget.CreateNewAsync(
                files.TargetPath,
                plan,
                catalog,
                source.SnapshotIdentity,
                cancellationToken: Ct);

        await Assert.ThrowsAsync<IOException>(async () =>
        {
            await using Database unexpected = await Database.OpenAsync(files.TargetPath, Ct);
        });
    }

    [Fact]
    public async Task UnexpectedTargetObjectProducesHashedTargetOnlyDifferenceAndNoActivation()
    {
        using var files = new TemporaryTargetDirectory();
        (MigrationCatalog catalog, MigrationPlan plan) = await ArtifactsAsync();
        await using (var source = new SyntheticMigrationDataSource(catalog))
        await using (CSharpDbStagedMigrationTarget target =
                     await CSharpDbStagedMigrationTarget.CreateNewAsync(
                         files.TargetPath,
                         plan,
                         catalog,
                         source.SnapshotIdentity,
                         cancellationToken: Ct))
        {
            await ApplyAsync(plan, catalog, source, target);
        }

        const string extraTableName = "unexpected-private-name";
        await using (Database database = await Database.OpenAsync(files.TargetPath, Ct))
        await using (var result = await database.ExecuteAsync(
                         $"CREATE TABLE \"{extraTableName}\" (\"secret-column-name\" TEXT)",
                         Ct))
        {
        }

        await using var validationSource = new SyntheticMigrationDataSource(catalog);
        await using var sourceSnapshot = new MigrationDataSourceValidationSnapshot(
            plan,
            catalog,
            validationSource);
        await using CSharpDbStagedMigrationTarget reopened =
            await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                files.TargetPath,
                plan,
                catalog,
                validationSource.SnapshotIdentity,
                cancellationToken: Ct);
        await using (IValidationSnapshot opened = await reopened.OpenValidationSnapshotAsync(Ct))
        {
            var snapshot = Assert.IsAssignableFrom<IMigrationEvidenceValidationSnapshot>(opened);
            MigrationNormalizedSchema actual = await snapshot.ReadSchemaAsync(Ct);
            MigrationNormalizedSchema expected = MigrationNormalizedSchemaContract.CreateExpected(plan, catalog);
            MigrationNormalizedSchemaDifference extra = Assert.Single(
                MigrationNormalizedSchemaContract.Compare(expected, actual),
                item => item.ObjectId.StartsWith("target-extra:table:", StringComparison.Ordinal));
            Assert.Null(extra.SourceDefinitionDigest);
            Assert.NotNull(extra.TargetDefinitionDigest);
            Assert.DoesNotContain(extraTableName, extra.ObjectId, StringComparison.OrdinalIgnoreCase);
            MigrationNormalizedSchemaObject extraObject = Assert.Single(
                actual.Objects,
                item => item.ObjectId == extra.ObjectId);
            Assert.DoesNotContain(extraTableName, extraObject.TargetName, StringComparison.OrdinalIgnoreCase);
        }

        MigrationValidationRunResult validation = await new MigrationValidationRunner().ValidateAsync(
            new MigrationValidationRunRequest
            {
                Plan = plan,
                Catalog = catalog,
                SourceSnapshot = sourceSnapshot,
                Target = reopened,
                Level = MigrationValidationLevel.Checksum,
                ReportOutputPath = Path.Combine(files.DirectoryPath, "extra-object-report.json"),
                ChecksumOptions = ChecksumOptions(files.DirectoryPath),
            },
            Ct);

        Assert.Equal(MigrationValidationStatus.Different, validation.Report.Outcome);
        Assert.False(validation.Activated);
        Assert.Null(await reopened.ReadActivationReceiptAsync(Ct));
    }

    [Fact]
    public async Task ActivatedTarget_RejectsANewOtherwiseValidDataBatch()
    {
        using var files = new TemporaryTargetDirectory();
        (MigrationCatalog catalog, MigrationPlan plan) = await ArtifactsAsync();
        await using var source = new SyntheticMigrationDataSource(catalog);
        await using CSharpDbStagedMigrationTarget target =
            await CSharpDbStagedMigrationTarget.CreateNewAsync(
                files.TargetPath,
                plan,
                catalog,
                source.SnapshotIdentity,
                cancellationToken: Ct);
        await ApplyAsync(plan, catalog, source, target);

        const string objectId = "syn:table:customers-lower";
        MigrationValidationRow? existingRow = null;
        await using (IValidationSnapshot snapshot = await target.OpenValidationSnapshotAsync(Ct))
        {
            await foreach (MigrationValidationRow row in snapshot.ReadRowsAsync(objectId, Ct))
            {
                existingRow = row;
                break;
            }
        }
        Assert.NotNull(existingRow);
        string[] columnIds = catalog.Objects
            .Where(item => item.Kind == MigrationObjectKind.Column &&
                item.ParentObjectId == objectId &&
                plan.Objects.Single(planned => planned.SourceObjectId == item.ObjectId).Included)
            .OrderBy(item => item.ObjectId, StringComparer.Ordinal)
            .Select(item => item.ObjectId)
            .ToArray();
        var unsigned = new MigrationTargetBatch
        {
            PlanDigest = MigrationArtifactSerializer.ComputePlanDigest(plan),
            CatalogDigest = plan.CatalogDigest,
            SourceFingerprint = plan.Source.Fingerprint,
            SourceSnapshotIdentity = source.SnapshotIdentity,
            SourceObjectId = objectId,
            ColumnObjectIds = columnIds,
            BatchOrdinal = 100,
            BatchDigest = string.Empty,
            Rows =
            [
                new MigrationTargetRow
                {
                    SourceRowOrdinal = 0,
                    Values = existingRow.Values,
                },
            ],
        };
        unsigned = unsigned with
        {
            RejectDigest = MigrationRejectDigest.Compute(unsigned),
        };
        MigrationTargetBatch batch = unsigned with
        {
            BatchDigest = MigrationBatchDigest.Compute(unsigned),
        };
        MigrationValidationActivationPermit permit = await ActivationPermitAsync(
            target,
            plan,
            catalog,
            source.SnapshotIdentity,
            Path.Combine(files.DirectoryPath, "write-guard.json"));
        await target.ActivateAsync(permit, Ct);

        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await target.WriteBatchAsync(batch, Ct));

        Assert.Contains("refuse", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    private static async Task<(MigrationCatalog Catalog, MigrationPlan Plan)> ArtifactsAsync()
    {
        MigrationCatalog catalog = await new SyntheticMigrationSourceInspector().InspectAsync(
            new MigrationInspectionRequest
            {
                TargetCSharpDbVersion = CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                IncludeProfile = true,
                ProfileSampleSize = 5,
            },
            Ct);
        MigrationPlan plan = new MigrationPlanner().CreatePlan(
            catalog,
            new MigrationPlanningOptions { AcceptAllExclusions = true });
        return (catalog, plan with { Load = plan.Load with { BatchSize = 2 } });
    }

    private static async Task ApplyAsync(
        MigrationPlan plan,
        MigrationCatalog catalog,
        IMigrationDataSource source,
        IMigrationTarget target) =>
        _ = await new MigrationApplyRunner().ApplyAsync(
            new MigrationApplyRequest
            {
                Plan = plan,
                Catalog = catalog,
                Source = source,
                Target = target,
            },
            Ct);

    private static async Task<List<T>> CollectAsync<T>(IAsyncEnumerable<T> source)
    {
        var values = new List<T>();
        await foreach (T value in source)
            values.Add(value);
        return values;
    }

    private static PartitionedChecksumValidatorOptions ChecksumOptions(string root) => new()
    {
        SpillRootDirectory = root,
        SortMemoryBudgetBytes = ValidationHashRecord.SerializedLength * 4,
        MaxSpillBytes = 32 * 1024 * 1024,
        MergeFanIn = 2,
        MaxOpenFiles = 3,
        MaxOpenPartitionWriters = 4,
        MaxMismatchDetailsPerPartition = 10,
    };

    private static async Task<MigrationValidationActivationPermit> ActivationPermitAsync(
        CSharpDbStagedMigrationTarget target,
        MigrationPlan plan,
        MigrationCatalog catalog,
        string sourceSnapshotIdentity,
        string reportPath,
        MigrationSnapshotConsistencyStatus consistency = MigrationSnapshotConsistencyStatus.Established,
        string? diagnosticSuffix = null,
        string? targetSnapshotIdentityOverride = null)
    {
        string schemaDigest = MigrationNormalizedSchemaContract.CreateExpected(plan, catalog).Digest;
        string targetSnapshotIdentity;
        if (targetSnapshotIdentityOverride is not null)
        {
            targetSnapshotIdentity = targetSnapshotIdentityOverride;
        }
        else
        {
            try
            {
                await using IValidationSnapshot snapshot = await target.OpenValidationSnapshotAsync(Ct);
                targetSnapshotIdentity = snapshot.SnapshotIdentity;
            }
            catch (InvalidDataException)
            {
                targetSnapshotIdentity =
                    $"staged-target:{target.TargetIdentity}:validation-unavailable";
            }
        }
        MigrationValidationStatus outcome = consistency == MigrationSnapshotConsistencyStatus.Established
            ? MigrationValidationStatus.Passed
            : MigrationValidationStatus.Inconclusive;
        var report = new MigrationValidationReport
        {
            Binding = new MigrationValidationBinding
            {
                TargetCSharpDbVersion = plan.TargetCSharpDbVersion,
                PlanDigest = MigrationArtifactSerializer.ComputePlanDigest(plan),
                CatalogDigest = plan.CatalogDigest,
                CapabilityDigest = plan.CapabilityDigest,
                SourceIdentity = plan.Source.Identity,
                SourceFingerprint = plan.Source.Fingerprint,
                TargetIdentity = target.TargetIdentity,
                SourceSnapshotIdentity = sourceSnapshotIdentity,
                TargetSnapshotIdentity = targetSnapshotIdentity,
                CanonicalizationVersion = CanonicalRowCodec.CanonicalizationId,
                CanonicalizationContractDigest = CanonicalRowCodec.ContractHashHex,
            },
            Level = MigrationValidationLevel.Checksum,
            Outcome = outcome,
            SnapshotConsistency = new MigrationSnapshotConsistencyEvidence { Status = consistency },
            Schema = new MigrationSchemaValidationEvidence
            {
                Status = MigrationValidationStatus.Passed,
                SourceSchemaDigest = schemaDigest,
                TargetSchemaDigest = schemaDigest,
            },
            Diagnostics = diagnosticSuffix is null
                ? []
                :
                [
                    new MigrationValidationDiagnosticEvidence
                    {
                        DiagnosticId = $"test:activation:{diagnosticSuffix}",
                        RuleId = "TEST-ACTIVATION-001",
                        Severity = MigrationDiagnosticSeverity.Information,
                        Status = MigrationValidationStatus.Passed,
                        Evidence = MigrationEvidenceLevel.Bound,
                    },
                ],
        };
        string json = MigrationValidationReportSerializer.Serialize(report, writeIndented: true);
        await File.WriteAllTextAsync(reportPath, json, Ct);
        string digest = MigrationValidationReportSerializer.ComputeDigest(report);
        var receipt = new MigrationValidationActivationReceipt
        {
            TargetIdentity = target.TargetIdentity,
            PlanDigest = MigrationArtifactSerializer.ComputePlanDigest(plan),
            CatalogDigest = plan.CatalogDigest,
            SourceSnapshotIdentity = sourceSnapshotIdentity,
            TargetSnapshotIdentity = targetSnapshotIdentity,
            Level = MigrationValidationLevel.Checksum,
            CanonicalizationVersion = CanonicalRowCodec.CanonicalizationId,
            CanonicalizationContractDigest = CanonicalRowCodec.ContractHashHex,
            ReportDigest = digest,
        };
        return new MigrationValidationActivationPermit(receipt, reportPath);
    }

    private static async Task PersistLegacyActivationAsync(
        string targetPath,
        MigrationValidationActivationReceipt receipt)
    {
        await using Database database = await Database.OpenAsync(targetPath, Ct);
        bool transactionStarted = false;
        bool commitInvoked = false;
        try
        {
            await database.BeginTransactionAsync(Ct);
            transactionStarted = true;

            InsertBatch receiptInsert = database.PrepareInsertBatch(
                "__csharpdb_migration_validation_receipt",
                1);
            receiptInsert.AddRow(
                DbValue.FromInteger(1),
                DbValue.FromText(MigrationValidationActivationReceipt.ContractVersion),
                DbValue.FromText(receipt.TargetIdentity),
                DbValue.FromText(receipt.PlanDigest),
                DbValue.FromText(receipt.CatalogDigest),
                DbValue.FromText(receipt.SourceSnapshotIdentity),
                DbValue.FromText(receipt.TargetSnapshotIdentity),
                DbValue.FromInteger((long)receipt.Level),
                DbValue.FromText(receipt.CanonicalizationVersion),
                DbValue.FromText(receipt.CanonicalizationContractDigest),
                DbValue.FromText(receipt.ReportDigest));
            Assert.Equal(1, await receiptInsert.ExecuteAsync(Ct));

            await using (var update = await database.ExecuteAsync(
                             "UPDATE \"__csharpdb_migration_state\" " +
                             "SET \"target_tag\" = 'csharpdb-staged-migration-target/v2', " +
                             "\"lifecycle_state\" = 'activated' " +
                             "WHERE \"singleton\" = 1 " +
                             "AND \"lifecycle_state\" = 'awaiting-validation'",
                             Ct))
            {
                Assert.Equal(1, update.RowsAffected);
            }

            commitInvoked = true;
            await database.CommitAsync(CancellationToken.None);
        }
        catch
        {
            if (transactionStarted && !commitInvoked)
                await database.RollbackAsync(CancellationToken.None);
            throw;
        }
    }

    private static async Task RewriteTargetTagAsV2Async(string targetPath)
    {
        await using Database database = await Database.OpenAsync(targetPath, Ct);
        await using var update = await database.ExecuteAsync(
            "UPDATE \"__csharpdb_migration_state\" " +
            "SET \"target_tag\" = 'csharpdb-staged-migration-target/v2' " +
            "WHERE \"singleton\" = 1 " +
            "AND \"lifecycle_state\" = 'awaiting-validation'",
            Ct);
        Assert.Equal(1, update.RowsAffected);
    }

    private static async Task RewriteTargetIdentityAsync(
        string targetPath,
        string originalTargetIdentity,
        string replacementTargetIdentity)
    {
        await using Database database = await Database.OpenAsync(targetPath, Ct);
        await database.BeginTransactionAsync(Ct);
        bool commitInvoked = false;
        try
        {
            await AssertIdentityRewriteAsync(
                database,
                "__csharpdb_migration_state",
                originalTargetIdentity,
                replacementTargetIdentity,
                expectedRows: 1);
            await AssertIdentityRewriteAsync(
                database,
                "__csharpdb_migration_stages",
                originalTargetIdentity,
                replacementTargetIdentity,
                expectedRows: Enum.GetValues<MigrationSchemaStage>().Length);
            await AssertIdentityRewriteAsync(
                database,
                "__csharpdb_migration_receipts",
                originalTargetIdentity,
                replacementTargetIdentity,
                expectedRows: 11);

            commitInvoked = true;
            await database.CommitAsync(CancellationToken.None);
        }
        catch
        {
            if (!commitInvoked)
                await database.RollbackAsync(CancellationToken.None);
            throw;
        }
    }

    private static async Task AssertIdentityRewriteAsync(
        Database database,
        string tableName,
        string originalTargetIdentity,
        string replacementTargetIdentity,
        int expectedRows)
    {
        string sql =
            $"UPDATE \"{tableName}\" " +
            $"SET \"target_identity\" = '{replacementTargetIdentity}' " +
            $"WHERE \"target_identity\" = '{originalTargetIdentity}'";
        await using var update = await database.ExecuteAsync(sql, Ct);
        Assert.Equal(expectedRows, update.RowsAffected);
    }

    private sealed class TemporaryTargetDirectory : IDisposable
    {
        public TemporaryTargetDirectory()
        {
            DirectoryPath = Path.Combine(
                Path.GetTempPath(),
                $"csharpdb-validation-activation-tests-{Guid.NewGuid():N}");
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
