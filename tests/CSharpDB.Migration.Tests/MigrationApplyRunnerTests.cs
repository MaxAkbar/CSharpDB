using System.Runtime.CompilerServices;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Tests;

public sealed class MigrationApplyRunnerTests
{
    private const string DeterministicRuleId = "MIG-TEST-REJECT-001";

    private static readonly MigrationSchemaStage[] ExpectedStages =
    [
        MigrationSchemaStage.LoadEssential,
        MigrationSchemaStage.SecondaryIndexes,
        MigrationSchemaStage.Constraints,
        MigrationSchemaStage.Views,
        MigrationSchemaStage.Triggers,
    ];

    [Fact]
    public async Task Apply_WritesMultipleBoundedBatchesAndExactResumeSkipsThem()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        (MigrationCatalog catalog, MigrationPlan plan) = await CreateReadyPlanAsync(batchSize: 2);
        await using var source = new SyntheticMigrationDataSource(catalog);
        await using var target = new InMemoryMigrationTarget();
        var runner = new MigrationApplyRunner();
        var request = new MigrationApplyRequest
        {
            Plan = plan,
            Catalog = catalog,
            Source = source,
            Target = target,
        };

        MigrationApplyResult first = await runner.ApplyAsync(request, cancellationToken);
        MigrationApplyResult resumed = await runner.ApplyAsync(request, cancellationToken);

        Assert.Equal(MigrationApplyStatus.AwaitingValidation, first.Status);
        Assert.Equal(MigrationRejectContract.DeterministicFailFastV1, first.RejectContractVersion);
        Assert.Equal(11, first.BatchesWritten);
        Assert.Equal(0, first.BatchesSkipped);
        Assert.Equal(21, first.RowsWritten);
        Assert.Equal(0, first.RowsSkipped);
        Assert.Equal(0, first.RejectedRowsWritten);
        Assert.Equal(0, first.RejectedRowsSkipped);
        Assert.InRange(first.PeakBufferedRows, 1, plan.Load.BatchSize);
        Assert.InRange(first.PeakBufferedBytes, 1, plan.Load.MaxBatchBytes);

        Assert.Equal(0, resumed.BatchesWritten);
        Assert.Equal(11, resumed.BatchesSkipped);
        Assert.Equal(0, resumed.RowsWritten);
        Assert.Equal(21, resumed.RowsSkipped);
        Assert.Equal(0, resumed.RejectedRowsWritten);
        Assert.Equal(0, resumed.RejectedRowsSkipped);
        Assert.Equal(11, target.WrittenBatches.Count);
        Assert.All(target.WrittenBatches, batch => Assert.InRange(batch.Rows.Count, 1, plan.Load.BatchSize));
        Assert.Equal(ExpectedStages.Concat(ExpectedStages), target.SchemaStages);
    }

    [Fact]
    public async Task DeterministicReplay_WritesAndSkipsMixedOutcomesWithGlobalSourceOrdinals()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        (MigrationCatalog catalog, MigrationPlan failFast) = await CreateReadyPlanAsync(batchSize: 2);
        MigrationPlan plan = WithDeterministicRejects(failFast);
        await using var inner = new SyntheticMigrationDataSource(catalog);
        await using var source = new RejectingDataSource(
            inner,
            "syn:table:customers-lower",
            rejectWholeBatch: false);
        await using var target = new InMemoryMigrationTarget();
        var request = new MigrationApplyRequest
        {
            Plan = plan,
            Catalog = catalog,
            Source = source,
            Target = target,
        };
        var runner = new MigrationApplyRunner();

        MigrationApplyResult first = await runner.ApplyAsync(request, cancellationToken);
        MigrationApplyResult replay = await runner.ApplyAsync(request, cancellationToken);

        MigrationTargetBatch mixed = Assert.Single(target.WrittenBatches, batch =>
            batch.SourceObjectId == "syn:table:customers-lower" && batch.BatchOrdinal == 0);
        MigrationTargetBatch successor = Assert.Single(target.WrittenBatches, batch =>
            batch.SourceObjectId == "syn:table:customers-lower" && batch.BatchOrdinal == 1);
        Assert.Equal([0L], mixed.Rows.Select(row => row.SourceRowOrdinal));
        Assert.Equal([1L], mixed.RejectedRows.Select(row => row.SourceRowOrdinal));
        Assert.Equal([2L], successor.Rows.Select(row => row.SourceRowOrdinal));
        Assert.Equal(MigrationRejectDigest.Compute(mixed), mixed.RejectDigest);
        Assert.Equal(MigrationBatchDigest.Compute(mixed), mixed.BatchDigest);

        Assert.Equal(MigrationRejectContract.DeterministicRejectsV1, first.RejectContractVersion);
        Assert.Equal(20, first.RowsWritten);
        Assert.Equal(1, first.RejectedRowsWritten);
        Assert.Equal(0, first.RejectedRowsSkipped);
        Assert.Equal(0, replay.RowsWritten);
        Assert.Equal(20, replay.RowsSkipped);
        Assert.Equal(0, replay.RejectedRowsWritten);
        Assert.Equal(1, replay.RejectedRowsSkipped);
        Assert.True(first.PeakBufferedBytes >=
            MigrationRejectLedgerCodec.GetCanonicalEntryByteCount(
                mixed.SourceObjectId,
                mixed.BatchOrdinal,
                Assert.Single(mixed.RejectedRows)));
    }

    [Fact]
    public async Task DeterministicReplay_WritesAndSkipsAnAllRejectTerminalBatch()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        (MigrationCatalog catalog, MigrationPlan failFast) = await CreateReadyPlanAsync(batchSize: 2);
        MigrationPlan plan = WithDeterministicRejects(failFast);
        await using var inner = new SyntheticMigrationDataSource(catalog);
        await using var source = new RejectingDataSource(
            inner,
            "syn:table:reserved",
            rejectWholeBatch: true);
        await using var target = new InMemoryMigrationTarget();
        var request = new MigrationApplyRequest
        {
            Plan = plan,
            Catalog = catalog,
            Source = source,
            Target = target,
        };
        var runner = new MigrationApplyRunner();

        MigrationApplyResult first = await runner.ApplyAsync(request, cancellationToken);
        MigrationApplyResult replay = await runner.ApplyAsync(request, cancellationToken);

        MigrationTargetBatch terminal = Assert.Single(target.WrittenBatches, batch =>
            batch.SourceObjectId == "syn:table:reserved");
        Assert.Empty(terminal.Rows);
        Assert.Equal([0L, 1L], terminal.RejectedRows.Select(row => row.SourceRowOrdinal));
        Assert.Null(terminal.StartCursor);
        Assert.Null(terminal.NextCursor);
        Assert.Equal(19, first.RowsWritten);
        Assert.Equal(2, first.RejectedRowsWritten);
        Assert.Equal(19, replay.RowsSkipped);
        Assert.Equal(2, replay.RejectedRowsSkipped);
    }

    [Fact]
    public async Task DeterministicReplay_RejectsTamperedStoredRejectCountBeforeSkipping()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        (MigrationCatalog catalog, MigrationPlan failFast) = await CreateReadyPlanAsync(batchSize: 2);
        MigrationPlan plan = WithDeterministicRejects(failFast);
        await using var inner = new SyntheticMigrationDataSource(catalog);
        await using var source = new RejectingDataSource(
            inner,
            "syn:table:customers-lower",
            rejectWholeBatch: false);
        await using var target = new InMemoryMigrationTarget();
        var request = new MigrationApplyRequest
        {
            Plan = plan,
            Catalog = catalog,
            Source = source,
            Target = target,
        };
        var runner = new MigrationApplyRunner();
        await runner.ApplyAsync(request, cancellationToken);
        target.TamperRejectedRowCount("syn:table:customers-lower", batchOrdinal: 0);

        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(
            async () => await runner.ApplyAsync(request, cancellationToken));

        Assert.Contains("receipt mismatch", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(11, target.WrittenBatches.Count);
    }

    [Fact]
    public async Task DeterministicReplay_RejectsUnsupportedSourceBeforeTargetMutation()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        (MigrationCatalog catalog, MigrationPlan failFast) = await CreateReadyPlanAsync(batchSize: 2);
        MigrationPlan plan = WithDeterministicRejects(failFast);
        await using var source = new SyntheticMigrationDataSource(catalog);
        await using var target = new InMemoryMigrationTarget();

        MigrationExecutionPolicyException error =
            await Assert.ThrowsAsync<MigrationExecutionPolicyException>(async () =>
                await new MigrationApplyRunner().ApplyAsync(
                    new MigrationApplyRequest
                    {
                        Plan = plan,
                        Catalog = catalog,
                        Source = source,
                        Target = target,
                    },
                    cancellationToken));

        Assert.Equal("MIG-APPLY-POLICY-REJECT-001", error.Code);
        Assert.Empty(target.SchemaStages);
        Assert.Empty(target.WrittenBatches);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task DeterministicReplay_RejectsSourceContractOrRuleMismatchBeforeTargetMutation(
        bool advertiseWrongContract)
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        (MigrationCatalog catalog, MigrationPlan failFast) = await CreateReadyPlanAsync(batchSize: 2);
        MigrationPlan plan = WithDeterministicRejects(failFast);
        await using var inner = new SyntheticMigrationDataSource(catalog);
        await using var source = new RejectingDataSource(
            inner,
            "syn:table:customers-lower",
            rejectWholeBatch: false,
            advertisedRejectContractVersion: advertiseWrongContract
                ? MigrationRejectContract.DeterministicFailFastV1
                : null,
            supportedRejectRuleIds: advertiseWrongContract
                ? null
                : new HashSet<string>(StringComparer.Ordinal));
        await using var target = new InMemoryMigrationTarget();

        MigrationExecutionPolicyException error =
            await Assert.ThrowsAsync<MigrationExecutionPolicyException>(async () =>
                await new MigrationApplyRunner().ApplyAsync(
                    new MigrationApplyRequest
                    {
                        Plan = plan,
                        Catalog = catalog,
                        Source = source,
                        Target = target,
                    },
                    cancellationToken));

        Assert.Equal(
            advertiseWrongContract
                ? "MIG-APPLY-POLICY-REJECT-SOURCE-001"
                : "MIG-APPLY-POLICY-REJECT-RULE-001",
            error.Code);
        Assert.Empty(target.SchemaStages);
        Assert.Empty(target.WrittenBatches);
    }

    [Fact]
    public async Task DeterministicReplay_RejectsUnsupportedOrLegacyTargetBeforeMutation()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        (MigrationCatalog catalog, MigrationPlan failFast) = await CreateReadyPlanAsync(batchSize: 2);
        MigrationPlan plan = WithDeterministicRejects(failFast);

        await using (var inner = new SyntheticMigrationDataSource(catalog))
        await using (var source = new RejectingDataSource(
                         inner,
                         "syn:table:customers-lower",
                         rejectWholeBatch: false))
        await using (var target = new CapabilityProbeTarget())
        {
            MigrationExecutionPolicyException unsupported =
                await Assert.ThrowsAsync<MigrationExecutionPolicyException>(async () =>
                    await new MigrationApplyRunner().ApplyAsync(
                        new MigrationApplyRequest
                        {
                            Plan = plan,
                            Catalog = catalog,
                            Source = source,
                            Target = target,
                        },
                        cancellationToken));
            Assert.Equal("MIG-APPLY-POLICY-REJECT-TARGET-001", unsupported.Code);
            Assert.Equal(0, target.OperationCount);
        }

        await using (var inner = new SyntheticMigrationDataSource(catalog))
        await using (var source = new RejectingDataSource(
                         inner,
                         "syn:table:customers-lower",
                         rejectWholeBatch: false))
        await using (var target = new LegacyLedgerCapabilityProbeTarget())
        {
            MigrationExecutionPolicyException legacy =
                await Assert.ThrowsAsync<MigrationExecutionPolicyException>(async () =>
                    await new MigrationApplyRunner().ApplyAsync(
                        new MigrationApplyRequest
                        {
                            Plan = plan,
                            Catalog = catalog,
                            Source = source,
                            Target = target,
                        },
                        cancellationToken));
            Assert.Equal("MIG-APPLY-POLICY-REJECT-TARGET-001", legacy.Code);
            Assert.Equal(0, target.OperationCount);
        }
    }

    [Fact]
    public async Task Apply_RejectsAChangedReceiptDigestOnResume()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        (MigrationCatalog catalog, MigrationPlan plan) = await CreateReadyPlanAsync(batchSize: 2);
        await using var source = new SyntheticMigrationDataSource(catalog);
        await using var target = new InMemoryMigrationTarget();
        var runner = new MigrationApplyRunner();
        var request = new MigrationApplyRequest
        {
            Plan = plan,
            Catalog = catalog,
            Source = source,
            Target = target,
        };
        await runner.ApplyAsync(request, cancellationToken);
        target.TamperReceiptDigest("syn:table:customers-lower", batchOrdinal: 0);

        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(
            async () => await runner.ApplyAsync(request, cancellationToken));

        Assert.Contains("receipt mismatch", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(ExpectedStages.Append(MigrationSchemaStage.LoadEssential), target.SchemaStages);
        Assert.Equal(11, target.WrittenBatches.Count);
    }

    [Fact]
    public async Task Apply_RejectsAChangedRejectDigestOnResume()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        (MigrationCatalog catalog, MigrationPlan plan) = await CreateReadyPlanAsync(batchSize: 2);
        await using var source = new SyntheticMigrationDataSource(catalog);
        await using var target = new InMemoryMigrationTarget();
        var runner = new MigrationApplyRunner();
        var request = new MigrationApplyRequest
        {
            Plan = plan,
            Catalog = catalog,
            Source = source,
            Target = target,
        };
        await runner.ApplyAsync(request, cancellationToken);
        target.TamperRejectDigest("syn:table:customers-lower", batchOrdinal: 0);

        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(
            async () => await runner.ApplyAsync(request, cancellationToken));

        Assert.Contains("receipt mismatch", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(11, target.WrittenBatches.Count);
    }

    [Fact]
    public void ValueConverter_ConvertsBlobAndNullableNullWithoutChangingTags()
    {
        MigrationCatalogObject column = Column("column:payload", "VARBINARY(MAX)", nullable: true);
        MigrationTypeMapping mapping = ExactMapping(column, DbType.Blob);
        byte[] bytes = [0x00, 0x43, 0xff, 0x10];

        DbValue blob = MigrationValueConverter.Convert(
            new MigrationSourceValue
            {
                Kind = MigrationSourceValueKind.Binary,
                BinaryValue = bytes,
            },
            column,
            mapping,
            rowOrdinal: 7);
        DbValue nullValue = MigrationValueConverter.Convert(
            new MigrationSourceValue { Kind = MigrationSourceValueKind.Null },
            column,
            mapping,
            rowOrdinal: 8);

        Assert.Equal(DbType.Blob, blob.Type);
        Assert.Equal(bytes, blob.AsBlob);
        Assert.Equal(DbType.Null, nullValue.Type);
        Assert.True(nullValue.IsNull);
    }

    [Fact]
    public void ValueConverter_RejectsWrongKindNullabilityDecimalOverflowAndNonFiniteReal()
    {
        MigrationCatalogObject integerColumn = Column("column:integer", "INT64", nullable: false);
        MigrationValueException wrongKind = Assert.Throws<MigrationValueException>(() =>
            MigrationValueConverter.Convert(
                new MigrationSourceValue
                {
                    Kind = MigrationSourceValueKind.Text,
                    CanonicalText = "1",
                },
                integerColumn,
                ExactMapping(integerColumn, DbType.Integer),
                rowOrdinal: 1));
        Assert.Equal("MIG-APPLY-KIND-001", wrongKind.Code);

        MigrationCatalogObject textColumn = Column("column:required-text", "TEXT", nullable: false);
        MigrationValueException nullability = Assert.Throws<MigrationValueException>(() =>
            MigrationValueConverter.Convert(
                new MigrationSourceValue { Kind = MigrationSourceValueKind.Null },
                textColumn,
                ExactMapping(textColumn, DbType.Text),
                rowOrdinal: 2));
        Assert.Equal("MIG-APPLY-NULL-001", nullability.Code);

        MigrationCatalogObject decimalColumn = Column("column:decimal", "DECIMAL(4,2)", nullable: false);
        MigrationTypeMapping decimalMapping = ConvertedMapping(
            decimalColumn,
            DbType.Integer,
            "decimal-scaled-int64",
            new MigrationCatalogFacet { Name = "precision", Value = "4" },
            new MigrationCatalogFacet { Name = "scale", Value = "2" });
        MigrationValueException decimalOverflow = Assert.Throws<MigrationValueException>(() =>
            MigrationValueConverter.Convert(
                new MigrationSourceValue
                {
                    Kind = MigrationSourceValueKind.Decimal,
                    CanonicalText = "1000.00",
                },
                decimalColumn,
                decimalMapping,
                rowOrdinal: 3));
        Assert.Equal("MIG-APPLY-DECIMAL-002", decimalOverflow.Code);

        MigrationCatalogObject realColumn = Column("column:real", "DOUBLE", nullable: false);
        MigrationValueException nonFinite = Assert.Throws<MigrationValueException>(() =>
            MigrationValueConverter.Convert(
                new MigrationSourceValue
                {
                    Kind = MigrationSourceValueKind.FloatingPoint,
                    CanonicalText = "NaN",
                },
                realColumn,
                ExactMapping(realColumn, DbType.Real),
                rowOrdinal: 4));
        Assert.Equal("MIG-APPLY-REAL-001", nonFinite.Code);
    }

    [Fact]
    public void BatchDigest_IsDeterministicAndSensitiveToIdentityOrderAndPayload()
    {
        MigrationTargetBatch batch = TargetBatch();

        string digest = MigrationBatchDigest.Compute(batch);

        Assert.Equal(digest, MigrationBatchDigest.Compute(batch));
        Assert.Equal(64, digest.Length);
        Assert.NotEqual(
            digest,
            MigrationBatchDigest.Compute(Seal(batch with { NextCursor = "row:changed" })));
        Assert.NotEqual(
            digest,
            MigrationBatchDigest.Compute(Seal(batch with
            {
                ColumnObjectIds = batch.ColumnObjectIds.Reverse().ToArray(),
            })));
        Assert.NotEqual(
            digest,
            MigrationBatchDigest.Compute(Seal(batch with
            {
                Rows =
                [
                    batch.Rows[0] with
                    {
                        Values = [DbValue.FromInteger(42), DbValue.FromBlob([0x01, 0x03])],
                    },
                ],
            })));
    }

    [Fact]
    public async Task SyntheticSource_ProjectsBlobAndNullAndResumesAtTheRequestedCursor()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        MigrationCatalog catalog = await InspectAsync();
        await using var source = new SyntheticMigrationDataSource(catalog);
        var request = new MigrationReadRequest
        {
            SourceObjectId = "syn:table:customers-upper",
            ColumnObjectIds = ["syn:column:customers-upper:payload"],
            BatchSize = 2,
            SnapshotToken = source.SnapshotIdentity,
        };

        List<MigrationDataBatch> batches = await CollectAsync(source.ReadAsync(request, cancellationToken));
        List<MigrationDataBatch> resumed = await CollectAsync(source.ReadAsync(
            request with { ResumeCursor = "row:2" },
            cancellationToken));

        Assert.Equal(2, batches.Count);
        Assert.Equal("row:2", batches[0].NextCursor);
        Assert.Equal(MigrationSourceValueKind.Binary, batches[0].Rows[0].Values[0].Kind);
        Assert.Equal([0x43, 0x53, 0x44, 0x42, 0x01], batches[0].Rows[0].Values[0].BinaryValue.ToArray());
        Assert.Equal(MigrationSourceValueKind.Null, batches[0].Rows[1].Values[0].Kind);
        Assert.Null(batches[0].Rows[1].Values[0].CanonicalText);
        Assert.True(batches[0].Rows[1].Values[0].BinaryValue.IsEmpty);

        MigrationDataBatch resumedBatch = Assert.Single(resumed);
        Assert.Equal(1, resumedBatch.BatchOrdinal);
        Assert.Equal("row:2", resumedBatch.StartCursor);
        Assert.Equal(2, resumedBatch.Rows.Count);
    }

    [Fact]
    public async Task Apply_RejectsSourceBatchesThatExceedThePlannedRowBound()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        (MigrationCatalog catalog, MigrationPlan plan) = await CreateReadyPlanAsync(batchSize: 1);
        await using var inner = new SyntheticMigrationDataSource(catalog);
        await using var source = new OversizedBatchDataSource(inner);
        await using var target = new InMemoryMigrationTarget();

        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await new MigrationApplyRunner().ApplyAsync(
                new MigrationApplyRequest
                {
                    Plan = plan,
                    Catalog = catalog,
                    Source = source,
                    Target = target,
                },
                cancellationToken));

        Assert.Contains("maximum is 1", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Empty(target.WrittenBatches);
    }

    [Fact]
    public async Task Apply_RejectsAConvertedBatchThatExceedsThePlannedByteBound()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        (MigrationCatalog catalog, MigrationPlan plan) = await CreateReadyPlanAsync(
            batchSize: 2,
            maxBatchBytes: 20,
            maxValueBytes: 20);
        await using var source = new SyntheticMigrationDataSource(catalog);
        await using var target = new InMemoryMigrationTarget();

        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await new MigrationApplyRunner().ApplyAsync(
                new MigrationApplyRequest
                {
                    Plan = plan,
                    Catalog = catalog,
                    Source = source,
                    Target = target,
                },
                cancellationToken));

        Assert.Contains("MaxBatchBytes (20)", error.Message, StringComparison.Ordinal);
        Assert.Empty(target.WrittenBatches);
    }

    [Fact]
    public async Task DeterministicReplay_RejectsCombinedCanonicalBatchBytesAboveThePlanBound()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        (MigrationCatalog catalog, MigrationPlan failFast) = await CreateReadyPlanAsync(
            batchSize: 2,
            maxBatchBytes: 512,
            maxValueBytes: 512);
        MigrationPlan plan = WithDeterministicRejects(failFast);
        await using var inner = new SyntheticMigrationDataSource(catalog);
        await using var source = new RejectingDataSource(
            inner,
            "syn:table:customers-lower",
            rejectWholeBatch: false,
            rejectEvidenceValue: new string('x', 500));
        await using var target = new InMemoryMigrationTarget();

        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await new MigrationApplyRunner().ApplyAsync(
                new MigrationApplyRequest
                {
                    Plan = plan,
                    Catalog = catalog,
                    Source = source,
                    Target = target,
                },
                cancellationToken));

        Assert.Contains("MaxBatchBytes (512)", error.Message, StringComparison.Ordinal);
        Assert.Empty(target.WrittenBatches);
    }

    private static async Task<(MigrationCatalog Catalog, MigrationPlan Plan)> CreateReadyPlanAsync(
        int batchSize,
        long maxBatchBytes = 64L * 1024 * 1024,
        int maxValueBytes = 16 * 1024 * 1024)
    {
        MigrationCatalog catalog = await InspectAsync();
        MigrationPlan planned = new MigrationPlanner().CreatePlan(catalog);
        MigrationPlan plan = planned with
        {
            AcceptedExclusionObjectIds = planned.Objects
                .Where(item => !item.Included)
                .Select(item => item.SourceObjectId)
                .OrderBy(item => item, StringComparer.Ordinal)
                .ToArray(),
            Load = planned.Load with
            {
                BatchSize = batchSize,
                MaxBatchBytes = maxBatchBytes,
                MaxValueBytes = maxValueBytes,
            },
        };
        return (catalog, plan);
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
                    AllowedRuleIds = [DeterministicRuleId],
                    MaxRejectedRowsPerBatch = plan.Load.BatchSize,
                    MaxRejectedRowsPerRun = 100,
                    MaxRawValueBytes = 1_024,
                    MaxRawValueBytesPerBatch = 8_192,
                    MaxRawValueBytesPerRun = 65_536,
                    MaxArtifactBytes = 131_072,
                },
            },
        };

    private static async Task<MigrationCatalog> InspectAsync() =>
        await new SyntheticMigrationSourceInspector().InspectAsync(
            new MigrationInspectionRequest
            {
                TargetCSharpDbVersion = CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                IncludeProfile = true,
                ProfileSampleSize = 5,
            });

    private static MigrationCatalogObject Column(string objectId, string nativeType, bool nullable) => new()
    {
        ObjectId = objectId,
        Kind = MigrationObjectKind.Column,
        SourceName = objectId,
        NativeType = nativeType,
        Facets =
        [
            new MigrationCatalogFacet
            {
                Name = "nullable",
                Value = nullable ? "true" : "false",
            },
        ],
    };

    private static MigrationTypeMapping ExactMapping(MigrationCatalogObject column, DbType targetType) => new()
    {
        SourceObjectId = column.ObjectId,
        SourceNativeType = column.NativeType!,
        TargetType = targetType,
        Classification = MigrationMappingClassification.Exact,
        Profile = MigrationMappingProfile.Preserve,
        Coverage = NoCoverage(),
    };

    private static MigrationTypeMapping ConvertedMapping(
        MigrationCatalogObject column,
        DbType targetType,
        string conversionId,
        params MigrationCatalogFacet[] parameters) => new()
        {
            SourceObjectId = column.ObjectId,
            SourceNativeType = column.NativeType!,
            TargetType = targetType,
            Classification = MigrationMappingClassification.LosslessReencoded,
            Profile = MigrationMappingProfile.Preserve,
            Coverage = NoCoverage(),
            Conversion = new MigrationConversionDescriptor
            {
                ConversionId = conversionId,
                Version = 1,
                Parameters = parameters,
            },
        };

    private static MigrationProfileCoverage NoCoverage() => new()
    {
        Kind = MigrationCoverageKind.None,
        RequiresFullStreamValidation = true,
    };

    private static MigrationTargetBatch TargetBatch() => Seal(
        new MigrationTargetBatch
        {
            PlanDigest = new string('1', 64),
            CatalogDigest = new string('2', 64),
            SourceFingerprint = "source:fingerprint",
            SourceSnapshotIdentity = "source:snapshot",
            SourceObjectId = "table:sample",
            ColumnObjectIds = ["column:id", "column:payload"],
            BatchOrdinal = 3,
            StartCursor = "row:4",
            NextCursor = "row:5",
            BatchDigest = string.Empty,
            Rows =
            [
                new MigrationTargetRow
                {
                    SourceRowOrdinal = 4,
                    StableKey = "row-4",
                    Values = [DbValue.FromInteger(42), DbValue.FromBlob([0x01, 0x02])],
                },
            ],
        });

    private static MigrationTargetBatch Seal(MigrationTargetBatch batch)
    {
        batch = batch with { RejectDigest = MigrationRejectDigest.Compute(batch) };
        return batch with { BatchDigest = MigrationBatchDigest.Compute(batch) };
    }

    private static async Task<List<T>> CollectAsync<T>(IAsyncEnumerable<T> values)
    {
        var result = new List<T>();
        await foreach (T value in values)
            result.Add(value);
        return result;
    }

    private sealed class OversizedBatchDataSource(SyntheticMigrationDataSource inner) : IMigrationDataSource
    {
        public MigrationSourceIdentity Source => inner.Source;

        public string SnapshotIdentity => inner.SnapshotIdentity;

        public async IAsyncEnumerable<MigrationDataBatch> ReadAsync(
            MigrationReadRequest request,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await foreach (MigrationDataBatch batch in inner.ReadAsync(
                               request with { BatchSize = checked(request.BatchSize + 1) },
                               cancellationToken))
            {
                yield return batch;
                yield break;
            }
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class RejectingDataSource(
        SyntheticMigrationDataSource inner,
        string rejectedObjectId,
        bool rejectWholeBatch,
        string? advertisedRejectContractVersion = null,
        IReadOnlySet<string>? supportedRejectRuleIds = null,
        string? rejectEvidenceValue = null) :
        IMigrationDataSource,
        IMigrationRejectAwareDataSource
    {
        private static readonly IReadOnlySet<string> s_supportedRules =
            new HashSet<string>([DeterministicRuleId], StringComparer.Ordinal);

        public MigrationSourceIdentity Source => inner.Source;

        public string SnapshotIdentity => inner.SnapshotIdentity;

        public string RejectContractVersion =>
            advertisedRejectContractVersion ?? MigrationRejectContract.DeterministicRejectsV1;

        public IReadOnlySet<string> SupportedRejectRuleIds =>
            supportedRejectRuleIds ?? s_supportedRules;

        public async IAsyncEnumerable<MigrationDataBatch> ReadAsync(
            MigrationReadRequest request,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            MigrationRejectReadPolicyValidator.Validate(request);
            await foreach (MigrationDataBatch batch in inner.ReadAsync(
                                   request,
                                   cancellationToken)
                               .WithCancellation(cancellationToken))
            {
                if (!string.Equals(
                        batch.SourceObjectId,
                        rejectedObjectId,
                        StringComparison.Ordinal) ||
                    batch.BatchOrdinal != 0)
                {
                    yield return batch;
                    continue;
                }

                int rejectedCount = rejectWholeBatch ? batch.Rows.Count : 1;
                int acceptedCount = batch.Rows.Count - rejectedCount;
                MigrationRejectedRow[] rejectedRows = Enumerable.Range(
                        acceptedCount,
                        rejectedCount)
                    .Select(ordinal => new MigrationRejectedRow
                    {
                        SourceRowOrdinal = ordinal,
                        RuleId = DeterministicRuleId,
                        ColumnObjectId = request.ColumnObjectIds[0],
                        Evidence =
                        [
                            new MigrationRejectEvidence
                            {
                                Name = MigrationRejectLedgerCodec.RawValueEvidenceName,
                                Value = rejectEvidenceValue ?? $"row-{ordinal}",
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

    private sealed class InMemoryMigrationTarget :
        IMigrationTarget,
        IMigrationRejectLedgerTarget,
        IMigrationBatchDigestContractTarget
    {
        private readonly Dictionary<(string PlanDigest, string ObjectId, long Ordinal), MigrationBatchReceipt>
            _receipts = new();

        public string TargetIdentity { get; } = "memory:phase2-target";

        public string BatchDigestFormat => MigrationBatchDigest.Format;

        public List<MigrationSchemaStage> SchemaStages { get; } = [];

        public List<MigrationTargetBatch> WrittenBatches { get; } = [];

        public ValueTask ApplySchemaAsync(
            MigrationPlan plan,
            MigrationCatalog catalog,
            MigrationSchemaStage stage,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            SchemaStages.Add(stage);
            return ValueTask.CompletedTask;
        }

        public ValueTask<MigrationBatchReceipt> WriteBatchAsync(
            MigrationTargetBatch batch,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var receipt = new MigrationBatchReceipt
            {
                TargetIdentity = TargetIdentity,
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
            if (!_receipts.TryAdd(Key(batch.PlanDigest, batch.SourceObjectId, batch.BatchOrdinal), receipt))
                throw new InvalidOperationException("The in-memory target received a duplicate batch write.");
            WrittenBatches.Add(batch);
            return ValueTask.FromResult(receipt);
        }

        public ValueTask<MigrationBatchReceipt?> ReadReceiptAsync(
            string planDigest,
            string sourceObjectId,
            long batchOrdinal,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _receipts.TryGetValue(Key(planDigest, sourceObjectId, batchOrdinal), out MigrationBatchReceipt? receipt);
            return ValueTask.FromResult(receipt);
        }

        public async IAsyncEnumerable<MigrationBatchReceipt> ReadReceiptsAsync(
            string planDigest,
            string sourceObjectId,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            MigrationBatchReceipt[] receipts = _receipts.Values
                .Where(receipt =>
                    string.Equals(receipt.PlanDigest, planDigest, StringComparison.Ordinal) &&
                    string.Equals(receipt.SourceObjectId, sourceObjectId, StringComparison.Ordinal))
                .OrderBy(receipt => receipt.BatchOrdinal)
                .ToArray();
            foreach (MigrationBatchReceipt receipt in receipts)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return receipt;
                await Task.Yield();
            }
        }

        public ValueTask<IValidationSnapshot> OpenValidationSnapshotAsync(
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException("Validation is outside this in-memory apply test double.");

        public async IAsyncEnumerable<MigrationRejectLedgerEntry> ReadRejectLedgerAsync(
            string planDigest,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (MigrationTargetBatch batch in WrittenBatches
                         .Where(batch => string.Equals(
                             batch.PlanDigest,
                             planDigest,
                             StringComparison.Ordinal))
                         .OrderBy(batch => batch.SourceObjectId, StringComparer.Ordinal)
                         .ThenBy(batch => batch.BatchOrdinal))
            {
                foreach (MigrationRejectedRow rejectedRow in batch.RejectedRows)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    yield return new MigrationRejectLedgerEntry
                    {
                        PlanDigest = planDigest,
                        SourceObjectId = batch.SourceObjectId,
                        BatchOrdinal = batch.BatchOrdinal,
                        RejectedRow = rejectedRow,
                        RawValueByteCount =
                            MigrationRejectLedgerCodec.GetRawValueByteCount(rejectedRow),
                        CanonicalEntryByteCount =
                            MigrationRejectLedgerCodec.GetCanonicalEntryByteCount(
                                batch.SourceObjectId,
                                batch.BatchOrdinal,
                                rejectedRow),
                    };
                    await Task.Yield();
                }
            }
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        public void TamperReceiptDigest(string sourceObjectId, long batchOrdinal)
        {
            KeyValuePair<(string PlanDigest, string ObjectId, long Ordinal), MigrationBatchReceipt> item =
                _receipts.Single(pair =>
                    pair.Key.ObjectId == sourceObjectId && pair.Key.Ordinal == batchOrdinal);
            _receipts[item.Key] = item.Value with { BatchDigest = new string('0', 64) };
        }

        public void TamperRejectDigest(string sourceObjectId, long batchOrdinal)
        {
            KeyValuePair<(string PlanDigest, string ObjectId, long Ordinal), MigrationBatchReceipt> item =
                _receipts.Single(pair =>
                    pair.Key.ObjectId == sourceObjectId && pair.Key.Ordinal == batchOrdinal);
            _receipts[item.Key] = item.Value with { RejectDigest = new string('0', 64) };
        }

        public void TamperRejectedRowCount(string sourceObjectId, long batchOrdinal)
        {
            KeyValuePair<(string PlanDigest, string ObjectId, long Ordinal), MigrationBatchReceipt> item =
                _receipts.Single(pair =>
                    pair.Key.ObjectId == sourceObjectId && pair.Key.Ordinal == batchOrdinal);
            _receipts[item.Key] = item.Value with
            {
                RejectedRowCount = checked(item.Value.RejectedRowCount + 1),
            };
        }

        private static (string PlanDigest, string ObjectId, long Ordinal) Key(
            string planDigest,
            string objectId,
            long ordinal) => (planDigest, objectId, ordinal);
    }

    private class CapabilityProbeTarget :
        IMigrationTarget,
        IMigrationBatchDigestContractTarget
    {
        public virtual string BatchDigestFormat => MigrationBatchDigest.Format;

        public string TargetIdentity => "memory:capability-probe";

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
            throw new NotSupportedException();
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
            throw new NotSupportedException();
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class LegacyLedgerCapabilityProbeTarget :
        CapabilityProbeTarget,
        IMigrationRejectLedgerTarget
    {
        public override string BatchDigestFormat => MigrationBatchDigest.LegacyFormat;

        public async IAsyncEnumerable<MigrationRejectLedgerEntry> ReadRejectLedgerAsync(
            string planDigest,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.CompletedTask;
            yield break;
        }
    }
}
