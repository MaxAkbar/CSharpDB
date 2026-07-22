using System.Text;
using CSharpDB.Migration;
using CSharpDB.Migration.Files.Csv;

namespace CSharpDB.Migration.Files.Tests;

public sealed class CsvMigrationDataSourceTests
{
    [Fact]
    public async Task CreateRejectsASchemaBoundToADifferentSnapshot()
    {
        (CsvSourceSnapshot expectedSnapshot, CsvSchemaInferenceResult schema) = await InferAsync(
            "id\n1\n2\n",
            100);
        (CsvSourceSnapshot differentSnapshot, _) = await InferAsync(
            "id\n3\n4\n",
            100);
        await using (expectedSnapshot)
        await using (differentSnapshot)
        {
            ArgumentException error = await Assert.ThrowsAsync<ArgumentException>(
                async () => await CsvMigrationDataSource.CreateAsync(
                    schema,
                    differentSnapshot,
                    Catalog(schema),
                    Cancellation));
            Assert.Equal("snapshot", error.ParamName);
        }
    }

    [Fact]
    public async Task InvalidColumnProjectionsAreRejectedSynchronously()
    {
        const string csv = "left,right\na,b\nc,d\n";
        (CsvSourceSnapshot snapshot, CsvSchemaInferenceResult schema) = await InferAsync(csv, 100);
        await using (snapshot)
        await using (CsvMigrationDataSource source = await CsvMigrationDataSource.CreateAsync(
                         schema,
                         snapshot,
                         Catalog(schema),
                         Cancellation))
        {
            MigrationReadRequest request = Request(
                source,
                [CsvMigrationObjectIds.Column(0), CsvMigrationObjectIds.Column(1)],
                batchSize: 2);

            Assert.Throws<ArgumentException>(() => source.ReadAsync(
                request with { ColumnObjectIds = [] },
                Cancellation));
            Assert.Throws<ArgumentException>(() => source.ReadAsync(
                request with
                {
                    ColumnObjectIds =
                    [
                        CsvMigrationObjectIds.Column(0),
                        CsvMigrationObjectIds.Column(0),
                    ],
                },
                Cancellation));
            Assert.Throws<ArgumentException>(() => source.ReadAsync(
                request with { ColumnObjectIds = ["csv:column:01"] },
                Cancellation));
            Assert.Throws<ArgumentException>(() => source.ReadAsync(
                request with { ColumnObjectIds = [CsvMigrationObjectIds.Column(99)] },
                Cancellation));
        }
    }

    [Fact]
    public async Task EverySupportedLogicalTypeEmitsItsMatchingSourceKind()
    {
        const string csv =
            "text,flag,signed,unsigned,decimal,floating,guid,date,time,datetime,offset\n" +
            "alpha,true,-1,9223372036854775808,1.25,1e2,6f9619ff-8b86-d011-b42d-00cf4fc964ff,2024-02-29,12:34:56,2024-02-29T12:34:56,2024-02-29T12:34:56Z\n" +
            "beta,false,2,18446744073709551615,2.50,-0.5,7f9619ff-8b86-d011-b42d-00cf4fc964ff,2025-03-01,01:02:03.4,2025-03-01 01:02:03.4,2025-03-01T01:02:03.4+05:30\n";
        var options = new CsvSchemaInferenceOptions
        {
            ColumnOverrides =
            [
                new CsvColumnSchemaOverride
                {
                    ColumnIndex = 5,
                    ExpectedHeader = "floating",
                    LogicalType = CsvColumnLogicalType.FloatingPoint,
                    Nullable = false,
                },
            ],
        };
        (CsvSourceSnapshot snapshot, CsvSchemaInferenceResult schema) = await InferAsync(
            csv,
            100,
            schemaOptions: options);
        await using (snapshot)
        await using (CsvMigrationDataSource source = await CsvMigrationDataSource.CreateAsync(
                         schema,
                         snapshot,
                         Catalog(schema),
                         Cancellation))
        {
            string[] columns = Enumerable.Range(0, schema.Columns.Count)
                .Select(CsvMigrationObjectIds.Column)
                .ToArray();
            MigrationDataBatch batch = Assert.Single(await CollectAsync(source.ReadAsync(
                Request(source, columns, batchSize: 10),
                Cancellation)));
            MigrationDataRow first = batch.Rows[0];

            Assert.Equal(
                [
                    MigrationSourceValueKind.Text,
                    MigrationSourceValueKind.Boolean,
                    MigrationSourceValueKind.SignedInteger,
                    MigrationSourceValueKind.UnsignedInteger,
                    MigrationSourceValueKind.Decimal,
                    MigrationSourceValueKind.FloatingPoint,
                    MigrationSourceValueKind.Guid,
                    MigrationSourceValueKind.Date,
                    MigrationSourceValueKind.Time,
                    MigrationSourceValueKind.DateTime,
                    MigrationSourceValueKind.DateTimeOffset,
                ],
                first.Values.Select(value => value.Kind));
            Assert.Equal("100", first.Values[5].CanonicalText);
        }
    }

    [Fact]
    public async Task RepeatedReadsPreserveAnArbitraryProjectionAcrossTwelveColumns()
    {
        string[] headers = Enumerable.Range(0, 12).Select(index => $"c{index}").ToArray();
        string[] firstValues = Enumerable.Range(0, 12).Select(index => $"first-{index}").ToArray();
        string[] secondValues = Enumerable.Range(0, 12).Select(index => $"second-{index}").ToArray();
        string csv = string.Join(',', headers) + "\n" +
                     string.Join(',', firstValues) + "\n" +
                     string.Join(',', secondValues) + "\n";
        (CsvSourceSnapshot snapshot, CsvSchemaInferenceResult schema) = await InferAsync(csv, 100);
        await using (snapshot)
        await using (CsvMigrationDataSource source = await CsvMigrationDataSource.CreateAsync(
                         schema,
                         snapshot,
                         Catalog(schema),
                         Cancellation))
        {
            int[] projection = [10, 2, 11, 0, 9, 1, 8, 3, 7, 4, 6, 5];
            string[] columnIds = projection.Select(CsvMigrationObjectIds.Column).ToArray();
            MigrationReadRequest request = Request(source, columnIds, batchSize: 1);

            List<MigrationDataBatch> first = await CollectAsync(source.ReadAsync(request, Cancellation));
            List<MigrationDataBatch> repeated = await CollectAsync(source.ReadAsync(request, Cancellation));

            Assert.Equal(2, first.Count);
            Assert.Equal(first.Count, repeated.Count);
            for (int index = 0; index < first.Count; index++)
                AssertBatchEqual(first[index], repeated[index]);

            Assert.Equal(columnIds, first[0].ColumnObjectIds);
            Assert.Equal(
                projection.Select(index => firstValues[index]),
                TextValues(Assert.Single(first[0].Rows)));
            Assert.Equal(
                projection.Select(index => secondValues[index]),
                TextValues(Assert.Single(first[1].Rows)));
            Assert.All(first.SelectMany(batch => batch.Rows), row => Assert.Null(row.StableKey));
        }
    }

    [Fact]
    public async Task RowBatchesHaveAStableCursorChainAndEveryCursorResumesTheExactSuffix()
    {
        string csv = "id\n" + string.Join('\n', Enumerable.Range(1, 7)) + "\n";
        (CsvSourceSnapshot snapshot, CsvSchemaInferenceResult schema) = await InferAsync(csv, 100);
        await using (snapshot)
        await using (CsvMigrationDataSource source = await CsvMigrationDataSource.CreateAsync(
                         schema,
                         snapshot,
                         Catalog(schema),
                         Cancellation))
        {
            MigrationReadRequest request = Request(
                source,
                [CsvMigrationObjectIds.Column(0)],
                batchSize: 3);
            List<MigrationDataBatch> batches = await CollectAsync(
                source.ReadAsync(request, Cancellation));

            Assert.Equal([3, 3, 1], batches.Select(batch => batch.Rows.Count));
            Assert.Equal([0L, 1L, 2L], batches.Select(batch => batch.BatchOrdinal));
            Assert.Null(batches[0].StartCursor);
            Assert.Equal(batches[0].NextCursor, batches[1].StartCursor);
            Assert.Equal(batches[1].NextCursor, batches[2].StartCursor);
            Assert.Null(batches[2].NextCursor);

            for (int boundary = 0; boundary < batches.Count - 1; boundary++)
            {
                string cursor = Assert.IsType<string>(batches[boundary].NextCursor);
                List<MigrationDataBatch> resumed = await CollectAsync(source.ReadAsync(
                    request with { ResumeCursor = cursor },
                    Cancellation));
                Assert.Equal(batches.Count - boundary - 1, resumed.Count);
                for (int suffix = 0; suffix < resumed.Count; suffix++)
                    AssertBatchEqual(batches[boundary + suffix + 1], resumed[suffix]);
            }
        }
    }

    [Fact]
    public async Task ByteSplitCursorsResumeTheExactVariableLengthBatchSuffix()
    {
        const string csv = "value\na\nb\n1234567890\nc\nd\n";
        (CsvSourceSnapshot snapshot, CsvSchemaInferenceResult schema) = await InferAsync(csv, 100);
        await using (snapshot)
        await using (CsvMigrationDataSource source = await CsvMigrationDataSource.CreateAsync(
                         schema,
                         snapshot,
                         Catalog(schema),
                         Cancellation))
        {
            MigrationReadRequest request = Request(
                source,
                [CsvMigrationObjectIds.Column(0)],
                batchSize: 10,
                maxBatchBytes: 18,
                maxValueBytes: 15);
            List<MigrationDataBatch> batches = await CollectAsync(
                source.ReadAsync(request, Cancellation));

            Assert.Equal([2, 1, 2], batches.Select(batch => batch.Rows.Count));
            Assert.Equal([0L, 1L, 2L], batches.Select(batch => batch.BatchOrdinal));
            for (int boundary = 0; boundary < batches.Count - 1; boundary++)
            {
                string cursor = Assert.IsType<string>(batches[boundary].NextCursor);
                List<MigrationDataBatch> resumed = await CollectAsync(source.ReadAsync(
                    request with { ResumeCursor = cursor },
                    Cancellation));
                Assert.Equal(batches.Count - boundary - 1, resumed.Count);
                for (int suffix = 0; suffix < resumed.Count; suffix++)
                    AssertBatchEqual(batches[boundary + suffix + 1], resumed[suffix]);
            }
        }
    }

    [Fact]
    public async Task HeaderOnlyInputEmitsNoBatchAndExactRowBoundaryEndsWithNullCursor()
    {
        (CsvSourceSnapshot emptySnapshot, CsvSchemaInferenceResult emptySchema) = await InferAsync(
            "id,name\n",
            100);
        await using (emptySnapshot)
        await using (CsvMigrationDataSource emptySource = await CsvMigrationDataSource.CreateAsync(
                         emptySchema,
                         emptySnapshot,
                         Catalog(emptySchema),
                         Cancellation))
        {
            List<MigrationDataBatch> empty = await CollectAsync(emptySource.ReadAsync(
                Request(
                    emptySource,
                    [CsvMigrationObjectIds.Column(0), CsvMigrationObjectIds.Column(1)],
                    batchSize: 2),
                Cancellation));
            Assert.Empty(empty);
        }

        (CsvSourceSnapshot snapshot, CsvSchemaInferenceResult schema) = await InferAsync(
            "id\n1\n2\n3\n4\n",
            100);
        await using (snapshot)
        await using (CsvMigrationDataSource source = await CsvMigrationDataSource.CreateAsync(
                         schema,
                         snapshot,
                         Catalog(schema),
                         Cancellation))
        {
            List<MigrationDataBatch> batches = await CollectAsync(source.ReadAsync(
                Request(source, [CsvMigrationObjectIds.Column(0)], batchSize: 2),
                Cancellation));
            Assert.Equal([2, 2], batches.Select(batch => batch.Rows.Count));
            Assert.Equal(batches[0].NextCursor, batches[1].StartCursor);
            Assert.Null(batches[1].NextCursor);
        }
    }

    [Fact]
    public async Task NullAndEmptyAreEmittedDistinctlyAndMissingFailsAtItsExactRowAndColumn()
    {
        const string csv = "nullable,empty,required\nNULL,,present\nvalue,next\n";
        var readerOptions = new CsvReaderOptions { NullToken = "NULL" };
        (CsvSourceSnapshot snapshot, CsvSchemaInferenceResult schema) = await InferAsync(
            csv,
            100,
            readerOptions);
        await using (snapshot)
        await using (CsvMigrationDataSource source = await CsvMigrationDataSource.CreateAsync(
                         schema,
                         snapshot,
                         Catalog(schema),
                         Cancellation))
        {
            MigrationReadRequest request = Request(
                source,
                [
                    CsvMigrationObjectIds.Column(0),
                    CsvMigrationObjectIds.Column(1),
                    CsvMigrationObjectIds.Column(2),
                ],
                batchSize: 1);
            await using IAsyncEnumerator<MigrationDataBatch> records = source
                .ReadAsync(request, Cancellation)
                .GetAsyncEnumerator(Cancellation);

            Assert.True(await records.MoveNextAsync());
            MigrationDataRow first = Assert.Single(records.Current.Rows);
            Assert.Equal(MigrationSourceValueKind.Null, first.Values[0].Kind);
            Assert.Null(first.Values[0].CanonicalText);
            Assert.True(first.Values[0].BinaryValue.IsEmpty);
            Assert.Equal(MigrationSourceValueKind.Text, first.Values[1].Kind);
            Assert.Equal(string.Empty, first.Values[1].CanonicalText);
            Assert.Equal("present", first.Values[2].CanonicalText);

            MigrationRowRejectedException error = await Assert.ThrowsAsync<MigrationRowRejectedException>(
                async () => await records.MoveNextAsync());
            Assert.Equal(CsvMigrationDataRules.MissingField, error.Code);
            Assert.Equal(CsvMigrationObjectIds.Table, error.SourceObjectId);
            Assert.Equal(CsvMigrationObjectIds.Column(2), error.ColumnObjectId);
            Assert.Equal(1, error.BatchOrdinal);
            Assert.Equal(1, error.SourceRowOrdinal);
        }
    }

    [Fact]
    public async Task SampledTypeMismatchReplaysTheSameSafeFailureAfterTheSameCompletedBatch()
    {
        const string rejectedValue = "TOP-SECRET-LATE-TEXT";
        string csv = $"id\n1\n2\n{rejectedValue}\n";
        (CsvSourceSnapshot snapshot, CsvSchemaInferenceResult schema) = await InferAsync(csv, 2);
        await using (snapshot)
        await using (CsvMigrationDataSource source = await CsvMigrationDataSource.CreateAsync(
                         schema,
                         snapshot,
                         Catalog(schema),
                         Cancellation))
        {
            MigrationReadRequest request = Request(
                source,
                [CsvMigrationObjectIds.Column(0)],
                batchSize: 2);

            MigrationRowRejectedException first = await ReadFirstBatchThenFailureAsync(source, request);
            MigrationRowRejectedException replay = await ReadFirstBatchThenFailureAsync(source, request);

            Assert.Equal(CsvMigrationDataRules.TypeMismatch, first.Code);
            Assert.Equal(CsvMigrationObjectIds.Column(0), first.ColumnObjectId);
            Assert.Equal(1, first.BatchOrdinal);
            Assert.Equal(2, first.SourceRowOrdinal);
            Assert.Equal(first.Message, replay.Message);
            Assert.Equal(first.Code, replay.Code);
            Assert.Equal(first.BatchOrdinal, replay.BatchOrdinal);
            Assert.Equal(first.SourceRowOrdinal, replay.SourceRowOrdinal);
            Assert.DoesNotContain(rejectedValue, first.ToString(), StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task AnExactlyByteFullBatchIsEmittedBeforeALateTypeFailure()
    {
        const string csv = "id\n1\n2\nlate\n";
        (CsvSourceSnapshot snapshot, CsvSchemaInferenceResult schema) = await InferAsync(csv, 2);
        await using (snapshot)
        await using (CsvMigrationDataSource source = await CsvMigrationDataSource.CreateAsync(
                         schema,
                         snapshot,
                         Catalog(schema),
                         Cancellation))
        await using (IAsyncEnumerator<MigrationDataBatch> records = source.ReadAsync(
                         Request(
                             source,
                             [CsvMigrationObjectIds.Column(0)],
                             batchSize: 10,
                             maxBatchBytes: 18,
                             maxValueBytes: 6),
                         Cancellation)
                     .GetAsyncEnumerator(Cancellation))
        {
            Assert.True(await records.MoveNextAsync());
            Assert.Equal(["1", "2"], records.Current.Rows.Select(row => row.Values[0].CanonicalText));

            MigrationRowRejectedException error = await Assert.ThrowsAsync<MigrationRowRejectedException>(
                async () => await records.MoveNextAsync());
            Assert.Equal(CsvMigrationDataRules.TypeMismatch, error.Code);
            Assert.Equal(1, error.BatchOrdinal);
            Assert.Equal(2, error.SourceRowOrdinal);
        }
    }

    [Fact]
    public async Task ExplicitNumericOverrideNormalizesEveryStreamedValue()
    {
        const string csv = "id\n001\n+2\n";
        var options = new CsvSchemaInferenceOptions
        {
            ColumnOverrides =
            [
                new CsvColumnSchemaOverride
                {
                    ColumnIndex = 0,
                    ExpectedHeader = "id",
                    LogicalType = CsvColumnLogicalType.SignedInteger,
                    Nullable = false,
                },
            ],
        };
        (CsvSourceSnapshot snapshot, CsvSchemaInferenceResult schema) = await InferAsync(
            csv,
            100,
            schemaOptions: options);
        await using (snapshot)
        await using (CsvMigrationDataSource source = await CsvMigrationDataSource.CreateAsync(
                         schema,
                         snapshot,
                         Catalog(schema),
                         Cancellation))
        {
            List<MigrationDataBatch> batches = await CollectAsync(source.ReadAsync(
                Request(source, [CsvMigrationObjectIds.Column(0)], batchSize: 10),
                Cancellation));
            MigrationDataRow[] rows = Assert.Single(batches).Rows.ToArray();

            Assert.Equal(["1", "2"], rows.Select(row => row.Values[0].CanonicalText));
            Assert.All(
                rows,
                row => Assert.Equal(MigrationSourceValueKind.SignedInteger, row.Values[0].Kind));
        }
    }

    [Fact]
    public async Task Utf8CanonicalBoundsAcceptEqualitySplitBeforeOverflowAndRejectOversizedValues()
    {
        const string csv = "value\n😀\n😀\n😀\n";
        (CsvSourceSnapshot snapshot, CsvSchemaInferenceResult schema) = await InferAsync(csv, 100);
        await using (snapshot)
        await using (CsvMigrationDataSource source = await CsvMigrationDataSource.CreateAsync(
                         schema,
                         snapshot,
                         Catalog(schema),
                         Cancellation))
        {
            MigrationReadRequest exact = Request(
                source,
                [CsvMigrationObjectIds.Column(0)],
                batchSize: 10,
                maxBatchBytes: 18,
                maxValueBytes: 9);
            List<MigrationDataBatch> batches = await CollectAsync(
                source.ReadAsync(exact, Cancellation));
            Assert.Equal([2, 1], batches.Select(batch => batch.Rows.Count));
            Assert.Equal(batches[0].NextCursor, batches[1].StartCursor);
            Assert.Null(batches[1].NextCursor);

            MigrationReadRequest tooSmall = exact with
            {
                MaxBatchBytes = 18,
                MaxValueBytes = 8,
            };
            MigrationRowRejectedException error = await Assert.ThrowsAsync<MigrationRowRejectedException>(
                async () => await CollectAsync(source.ReadAsync(tooSmall, Cancellation)));
            Assert.Equal(CsvMigrationDataRules.ValueSizeExceeded, error.Code);
            Assert.Equal(0, error.BatchOrdinal);
            Assert.Equal(0, error.SourceRowOrdinal);
        }
    }

    [Fact]
    public async Task EmptyTextUsesItsExactFiveByteSourceCanonicalValueBound()
    {
        const string csv = "id,left,right\n1,,\n";
        (CsvSourceSnapshot snapshot, CsvSchemaInferenceResult schema) = await InferAsync(csv, 100);
        await using (snapshot)
        await using (CsvMigrationDataSource source = await CsvMigrationDataSource.CreateAsync(
                         schema,
                         snapshot,
                         Catalog(schema),
                         Cancellation))
        {
            MigrationReadRequest exact = Request(
                source,
                [CsvMigrationObjectIds.Column(1), CsvMigrationObjectIds.Column(2)],
                batchSize: 10,
                maxBatchBytes: 10,
                maxValueBytes: 5);
            MigrationDataRow row = Assert.Single(Assert.Single(
                await CollectAsync(source.ReadAsync(exact, Cancellation))).Rows);
            Assert.All(row.Values, value => Assert.Equal(MigrationSourceValueKind.Text, value.Kind));
            Assert.All(row.Values, value => Assert.Equal(string.Empty, value.CanonicalText));

            MigrationReadRequest oneByteTooSmall = exact with { MaxValueBytes = 4 };
            MigrationRowRejectedException error = await Assert.ThrowsAsync<MigrationRowRejectedException>(
                async () => await CollectAsync(source.ReadAsync(oneByteTooSmall, Cancellation)));
            Assert.Equal(CsvMigrationDataRules.ValueSizeExceeded, error.Code);
            Assert.Equal(CsvMigrationObjectIds.Column(1), error.ColumnObjectId);
            Assert.Equal(0, error.BatchOrdinal);
            Assert.Equal(0, error.SourceRowOrdinal);
        }
    }

    [Fact]
    public async Task ShortNumericValuesRetainTheNineByteConservativeBatchEstimate()
    {
        const string csv = "id\n1\n2\n";
        (CsvSourceSnapshot snapshot, CsvSchemaInferenceResult schema) = await InferAsync(csv, 100);
        await using (snapshot)
        await using (CsvMigrationDataSource source = await CsvMigrationDataSource.CreateAsync(
                         schema,
                         snapshot,
                         Catalog(schema),
                         Cancellation))
        {
            MigrationReadRequest request = Request(
                source,
                [CsvMigrationObjectIds.Column(0)],
                batchSize: 10,
                maxBatchBytes: 12,
                maxValueBytes: 6);

            List<MigrationDataBatch> batches = await CollectAsync(
                source.ReadAsync(request, Cancellation));
            Assert.Equal([1, 1], batches.Select(batch => batch.Rows.Count));
            Assert.Equal(["1", "2"], batches.Select(batch =>
                Assert.Single(Assert.Single(batch.Rows).Values).CanonicalText));
            Assert.Equal(batches[0].NextCursor, batches[1].StartCursor);
            Assert.Null(batches[1].NextCursor);
        }
    }

    [Fact]
    public async Task ARowThatCannotFitTheBatchFailsInsteadOfBeingSplit()
    {
        const string csv = "left,right\n😀,😀\n";
        (CsvSourceSnapshot snapshot, CsvSchemaInferenceResult schema) = await InferAsync(csv, 100);
        await using (snapshot)
        await using (CsvMigrationDataSource source = await CsvMigrationDataSource.CreateAsync(
                         schema,
                         snapshot,
                         Catalog(schema),
                         Cancellation))
        {
            MigrationReadRequest request = Request(
                source,
                [CsvMigrationObjectIds.Column(0), CsvMigrationObjectIds.Column(1)],
                batchSize: 10,
                maxBatchBytes: 17,
                maxValueBytes: 9);

            MigrationRowRejectedException error = await Assert.ThrowsAsync<MigrationRowRejectedException>(
                async () => await CollectAsync(source.ReadAsync(request, Cancellation)));
            Assert.Equal(CsvMigrationDataRules.RowSizeExceeded, error.Code);
            Assert.Equal(0, error.BatchOrdinal);
            Assert.Equal(0, error.SourceRowOrdinal);
        }
    }

    [Fact]
    public async Task SnapshotTokensAndCursorScopeAreValidatedBeforeEnumeration()
    {
        const string csv = "id\n1\n2\n";
        (CsvSourceSnapshot snapshot, CsvSchemaInferenceResult schema) = await InferAsync(csv, 100);
        await using (snapshot)
        await using (CsvMigrationDataSource source = await CsvMigrationDataSource.CreateAsync(
                         schema,
                         snapshot,
                         Catalog(schema),
                         Cancellation))
        {
            MigrationReadRequest request = Request(
                source,
                [CsvMigrationObjectIds.Column(0)],
                batchSize: 1);
            Assert.Throws<InvalidDataException>(() => source.ReadAsync(
                request with { SnapshotToken = "wrong-snapshot" },
                Cancellation));

            List<MigrationDataBatch> batches = await CollectAsync(
                source.ReadAsync(request, Cancellation));
            string cursor = Assert.IsType<string>(batches[0].NextCursor);
            char replacement = cursor[^1] == '0' ? '1' : '0';
            string tampered = cursor[..^1] + replacement;
            string[] positionParts = cursor.Split('/');
            positionParts[1] = "999";
            string tamperedPosition = string.Join('/', positionParts);

            Assert.Throws<InvalidDataException>(() => source.ReadAsync(
                request with { ResumeCursor = tampered },
                Cancellation));
            Assert.Throws<InvalidDataException>(() => source.ReadAsync(
                request with { ResumeCursor = tamperedPosition },
                Cancellation));
            Assert.Throws<InvalidDataException>(() => source.ReadAsync(
                request with { ResumeCursor = new string('x', 10_000) },
                Cancellation));
            Assert.Throws<InvalidDataException>(() => source.ReadAsync(
                request with { ResumeCursor = cursor, BatchSize = 2 },
                Cancellation));
            Assert.Throws<InvalidDataException>(() => source.ReadAsync(
                request with { ResumeCursor = cursor, SnapshotToken = null },
                Cancellation));
        }
    }

    [Fact]
    public async Task CreationRejectsACatalogThatDriftsFromTheInferencePolicy()
    {
        (CsvSourceSnapshot snapshot, CsvSchemaInferenceResult schema) = await InferAsync(
            "id\n1\n2\n",
            100);
        await using (snapshot)
        {
            MigrationCatalog catalog = Catalog(schema);
            MigrationCatalog drifted = catalog with
            {
                Objects = catalog.Objects
                    .Select(item => item.ObjectId == CsvMigrationObjectIds.Table
                        ? item with { SourceName = "different_table" }
                        : item)
                    .ToArray(),
            };

            await Assert.ThrowsAsync<ArgumentException>(async () =>
                await CsvMigrationDataSource.CreateAsync(
                    schema,
                    snapshot,
                    drifted,
                    Cancellation));
        }
    }

    [Fact]
    public async Task CancellationBeforeAndAfterACompletedBatchDoesNotPoisonLaterReads()
    {
        const string csv = "id\n1\n2\n3\n";
        (CsvSourceSnapshot snapshot, CsvSchemaInferenceResult schema) = await InferAsync(csv, 100);
        await using (snapshot)
        await using (CsvMigrationDataSource source = await CsvMigrationDataSource.CreateAsync(
                         schema,
                         snapshot,
                         Catalog(schema),
                         Cancellation))
        {
            MigrationReadRequest request = Request(
                source,
                [CsvMigrationObjectIds.Column(0)],
                batchSize: 1);
            using (var preCanceled = CancellationTokenSource.CreateLinkedTokenSource(Cancellation))
            {
                await preCanceled.CancelAsync();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                    await CollectWithTokenAsync(
                        source.ReadAsync(request, preCanceled.Token),
                        preCanceled.Token));
            }

            using (var interrupted = CancellationTokenSource.CreateLinkedTokenSource(Cancellation))
            {
                await using IAsyncEnumerator<MigrationDataBatch> records = source
                    .ReadAsync(request, interrupted.Token)
                    .GetAsyncEnumerator(interrupted.Token);
                Assert.True(await records.MoveNextAsync());
                Assert.Equal(
                    "1",
                    Assert.Single(Assert.Single(records.Current.Rows).Values).CanonicalText);
                await interrupted.CancelAsync();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                    await records.MoveNextAsync());
            }

            List<MigrationDataBatch> replay = await CollectAsync(
                source.ReadAsync(request, Cancellation));
            Assert.Equal(["1", "2", "3"], replay.Select(batch =>
                Assert.Single(Assert.Single(batch.Rows).Values).CanonicalText));
        }
    }

    [Fact]
    public async Task DisposingTheSourceIsIdempotentAndDoesNotDisposeTheCallerSnapshot()
    {
        const string csv = "id\n1\n2\n";
        (CsvSourceSnapshot snapshot, CsvSchemaInferenceResult schema) = await InferAsync(csv, 100);
        await using (snapshot)
        {
            CsvMigrationDataSource source = await CsvMigrationDataSource.CreateAsync(
                schema,
                snapshot,
                Catalog(schema),
                Cancellation);
            MigrationReadRequest request = Request(
                source,
                [CsvMigrationObjectIds.Column(0)],
                batchSize: 2);

            await source.DisposeAsync();
            await source.DisposeAsync();
            Assert.Throws<ObjectDisposedException>(() => source.ReadAsync(request, Cancellation));

            await using Stream snapshotReader = snapshot.OpenRead();
            Assert.True(snapshotReader.CanRead);

            await using CsvMigrationDataSource replacement = await CsvMigrationDataSource.CreateAsync(
                schema,
                snapshot,
                Catalog(schema),
                Cancellation);
            List<MigrationDataBatch> batches = await CollectAsync(replacement.ReadAsync(
                Request(replacement, [CsvMigrationObjectIds.Column(0)], batchSize: 2),
                Cancellation));
            Assert.Equal(2, Assert.Single(batches).Rows.Count);
        }
    }

    private static CancellationToken Cancellation => TestContext.Current.CancellationToken;

    private static MigrationCatalog Catalog(CsvSchemaInferenceResult schema) =>
        schema.CreateCatalog(CSharpDbCapabilityCatalogLoader.CurrentTargetVersion);

    private static MigrationReadRequest Request(
        CsvMigrationDataSource source,
        IReadOnlyList<string> columns,
        int batchSize,
        long maxBatchBytes = 64L * 1024 * 1024,
        int maxValueBytes = 16 * 1024 * 1024) => new()
        {
            SourceObjectId = CsvMigrationObjectIds.Table,
            ColumnObjectIds = columns,
            BatchSize = batchSize,
            MaxBatchBytes = maxBatchBytes,
            MaxValueBytes = maxValueBytes,
            SnapshotToken = source.SnapshotIdentity,
        };

    private static async ValueTask<(CsvSourceSnapshot Snapshot, CsvSchemaInferenceResult Schema)> InferAsync(
        string csv,
        int maxDataRecords,
        CsvReaderOptions? readerOptions = null,
        CsvSchemaInferenceOptions? schemaOptions = null)
    {
        readerOptions ??= new CsvReaderOptions();
        CsvSourceSnapshot snapshot = await CsvSourceSnapshot.CreateAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(csv)),
            cancellationToken: Cancellation);
        try
        {
            CsvFormatInspection inspection = await CsvFormatInspector.InspectAsync(
                snapshot,
                readerOptions,
                new CsvInspectionOptions { DelimiterCandidates = [readerOptions.Delimiter] },
                Cancellation);
            CsvSourceBinding binding = await CsvSourceBinding.CreateAsync(
                snapshot,
                inspection,
                cancellationToken: Cancellation);
            CsvSchemaInferenceResult schema = await CsvSchemaInferer.InferAsync(
                binding,
                snapshot,
                maxDataRecords,
                schemaOptions,
                Cancellation);
            return (snapshot, schema);
        }
        catch
        {
            await snapshot.DisposeAsync();
            throw;
        }
    }

    private static async Task<MigrationRowRejectedException> ReadFirstBatchThenFailureAsync(
        CsvMigrationDataSource source,
        MigrationReadRequest request)
    {
        await using IAsyncEnumerator<MigrationDataBatch> records = source
            .ReadAsync(request, Cancellation)
            .GetAsyncEnumerator(Cancellation);
        Assert.True(await records.MoveNextAsync());
        MigrationDataBatch first = records.Current;
        Assert.Equal(0, first.BatchOrdinal);
        Assert.Equal(["1", "2"], first.Rows.Select(row => row.Values[0].CanonicalText));
        return await Assert.ThrowsAsync<MigrationRowRejectedException>(
            async () => await records.MoveNextAsync());
    }

    private static IEnumerable<string?> TextValues(MigrationDataRow row) =>
        row.Values.Select(value => value.CanonicalText);

    private static void AssertBatchEqual(MigrationDataBatch expected, MigrationDataBatch actual)
    {
        Assert.Equal(expected.SourceObjectId, actual.SourceObjectId);
        Assert.Equal(expected.SnapshotIdentity, actual.SnapshotIdentity);
        Assert.Equal(expected.ColumnObjectIds, actual.ColumnObjectIds);
        Assert.Equal(expected.BatchOrdinal, actual.BatchOrdinal);
        Assert.Equal(expected.StartCursor, actual.StartCursor);
        Assert.Equal(expected.NextCursor, actual.NextCursor);
        Assert.Equal(expected.Rows.Count, actual.Rows.Count);
        for (int rowIndex = 0; rowIndex < expected.Rows.Count; rowIndex++)
        {
            MigrationDataRow expectedRow = expected.Rows[rowIndex];
            MigrationDataRow actualRow = actual.Rows[rowIndex];
            Assert.Equal(expectedRow.StableKey, actualRow.StableKey);
            Assert.Equal(expectedRow.Values.Count, actualRow.Values.Count);
            for (int valueIndex = 0; valueIndex < expectedRow.Values.Count; valueIndex++)
            {
                MigrationSourceValue expectedValue = expectedRow.Values[valueIndex];
                MigrationSourceValue actualValue = actualRow.Values[valueIndex];
                Assert.Equal(expectedValue.Kind, actualValue.Kind);
                Assert.Equal(expectedValue.CanonicalText, actualValue.CanonicalText);
                Assert.Equal(expectedValue.BinaryValue.ToArray(), actualValue.BinaryValue.ToArray());
            }
        }
    }

    private static async Task<List<T>> CollectAsync<T>(IAsyncEnumerable<T> values)
    {
        var result = new List<T>();
        await foreach (T value in values.WithCancellation(Cancellation))
            result.Add(value);
        return result;
    }

    private static async Task<List<T>> CollectWithTokenAsync<T>(
        IAsyncEnumerable<T> values,
        CancellationToken cancellationToken)
    {
        var result = new List<T>();
        await foreach (T value in values.WithCancellation(cancellationToken))
            result.Add(value);
        return result;
    }
}
