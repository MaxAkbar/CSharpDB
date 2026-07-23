using System.Runtime.CompilerServices;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Tests;

public sealed class MigrationApplyRunnerTests
{
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
        Assert.InRange(first.PeakBufferedRows, 1, plan.Load.BatchSize);
        Assert.InRange(first.PeakBufferedBytes, 1, plan.Load.MaxBatchBytes);

        Assert.Equal(0, resumed.BatchesWritten);
        Assert.Equal(11, resumed.BatchesSkipped);
        Assert.Equal(0, resumed.RowsWritten);
        Assert.Equal(21, resumed.RowsSkipped);
        Assert.Equal(11, target.WrittenBatches.Count);
        Assert.All(target.WrittenBatches, batch => Assert.InRange(batch.Rows.Count, 1, plan.Load.BatchSize));
        Assert.Equal(ExpectedStages.Concat(ExpectedStages), target.SchemaStages);
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

    private sealed class InMemoryMigrationTarget : IMigrationTarget
    {
        private readonly Dictionary<(string PlanDigest, string ObjectId, long Ordinal), MigrationBatchReceipt>
            _receipts = new();

        public string TargetIdentity { get; } = "memory:phase2-target";

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
                RejectedRowCount = 0,
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

        private static (string PlanDigest, string ObjectId, long Ordinal) Key(
            string planDigest,
            string objectId,
            long ordinal) => (planDigest, objectId, ordinal);
    }
}
