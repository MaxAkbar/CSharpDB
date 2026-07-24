using CSharpDB.Migration;
using CSharpDB.Migration.Sqlite;

namespace CSharpDB.Migration.Sqlite.Tests;

public sealed class SqliteMigrationDataSourceTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task StreamsExactScalarsInSignedRowIdOrder()
    {
        using var temporary = new SqliteTestDirectory();
        await using OpenedSource source = await OpenedSource.CreateAsync(
            temporary,
            "scalars",
            """
            CREATE TABLE scalar_rows (
                id INTEGER PRIMARY KEY,
                nullable_text TEXT,
                integer_value INTEGER NOT NULL,
                real_value REAL NOT NULL,
                text_value TEXT NOT NULL,
                blob_value BLOB NOT NULL
            );
            INSERT INTO scalar_rows(
                id,
                nullable_text,
                integer_value,
                real_value,
                text_value,
                blob_value)
            VALUES
                (
                    -9,
                    NULL,
                    -9223372036854775808,
                    1.25,
                    'left' || char(0) || '雪😀',
                    X'0001FEFF'
                ),
                (2, 'present', 0, -2.5, 'middle', X'10'),
                (999, 'last', 9223372036854775807, 3.5, 'last', X'20');
            """,
            includeProfile: true,
            profileSampleSize: 100);

        MigrationCatalogObject table = source.Table("scalar_rows");
        string[] projection =
        [
            source.Column(table, "id").ObjectId,
            source.Column(table, "nullable_text").ObjectId,
            source.Column(table, "integer_value").ObjectId,
            source.Column(table, "real_value").ObjectId,
            source.Column(table, "text_value").ObjectId,
            source.Column(table, "blob_value").ObjectId,
        ];
        MigrationReadRequest request = source.Request(table, projection, batchSize: 2);

        IReadOnlyList<MigrationDataBatch> batches =
            await ReadAllAsync(source.DataSource, request, Ct);
        MigrationDataRow[] rows = batches.SelectMany(static batch => batch.Rows).ToArray();

        Assert.Equal(3, rows.Length);
        Assert.Equal(["-9", "2", "999"], rows.Select(row => row.Values[0].CanonicalText));
        Assert.All(rows, static row => Assert.Null(row.StableKey));

        MigrationDataRow first = rows[0];
        AssertValue(first.Values[0], MigrationSourceValueKind.SignedInteger, "-9");
        AssertValue(first.Values[1], MigrationSourceValueKind.Null, expectedText: null);
        AssertValue(
            first.Values[2],
            MigrationSourceValueKind.SignedInteger,
            "-9223372036854775808");
        AssertValue(first.Values[3], MigrationSourceValueKind.FloatingPoint, "1.25");
        AssertValue(first.Values[4], MigrationSourceValueKind.Text, "left\0雪😀");
        Assert.Equal(MigrationSourceValueKind.Binary, first.Values[5].Kind);
        Assert.Null(first.Values[5].CanonicalText);
        Assert.Equal([0x00, 0x01, 0xFE, 0xFF], first.Values[5].BinaryValue.ToArray());

        Assert.Equal([0L, 1L], batches.Select(static batch => batch.BatchOrdinal));
        Assert.Null(batches[0].StartCursor);
        Assert.Equal(batches[0].NextCursor, batches[1].StartCursor);
        Assert.Null(batches[1].NextCursor);
    }

    [Theory]
    [InlineData(2, 1_024, 128)]
    [InlineData(10, 20, 20)]
    public async Task SplitsAtRowAndCanonicalByteBounds(
        int batchSize,
        long maxBatchBytes,
        int maxValueBytes)
    {
        using var temporary = new SqliteTestDirectory();
        await using OpenedSource source = await OpenedSource.CreateAsync(
            temporary,
            "batch-bounds",
            """
            CREATE TABLE batch_rows (
                id INTEGER PRIMARY KEY,
                value TEXT NOT NULL
            );
            INSERT INTO batch_rows(id, value) VALUES
                (1, 'aaaaa'),
                (2, 'bbbbb'),
                (3, 'ccccc'),
                (4, 'ddddd'),
                (5, 'eeeee');
            """);
        MigrationCatalogObject table = source.Table("batch_rows");
        MigrationReadRequest request = source.Request(
            table,
            [source.Column(table, "value").ObjectId],
            batchSize,
            maxBatchBytes,
            maxValueBytes);

        IReadOnlyList<MigrationDataBatch> batches =
            await ReadAllAsync(source.DataSource, request, Ct);

        Assert.Equal([2, 2, 1], batches.Select(static batch => batch.Rows.Count));
        Assert.Equal([0L, 1L, 2L], batches.Select(static batch => batch.BatchOrdinal));
        Assert.Null(batches[0].StartCursor);
        Assert.Equal(batches[0].NextCursor, batches[1].StartCursor);
        Assert.Equal(batches[1].NextCursor, batches[2].StartCursor);
        Assert.Null(batches[2].NextCursor);
    }

    [Fact]
    public async Task SplitsWideRowsAtScalarObjectBoundAndResumesExactly()
    {
        const int projectedColumnCount = 128;
        const int scalarObjectBound = 65_536;
        int rowsPerBatch = scalarObjectBound / projectedColumnCount;
        string[] nullableColumnNames = Enumerable.Range(
                1,
                projectedColumnCount - 1)
            .Select(static index => $"value_{index:D3}")
            .ToArray();
        string columnDefinitions = string.Join(
            "," + Environment.NewLine,
            nullableColumnNames.Select(static name => $"    {name} TEXT"));
        string rowValues = string.Join(
            ", ",
            Enumerable.Range(1, rowsPerBatch + 1)
                .Select(static value => $"({value})"));

        using var temporary = new SqliteTestDirectory();
        await using OpenedSource source = await OpenedSource.CreateAsync(
            temporary,
            "scalar-object-bound",
            $"""
            CREATE TABLE wide_rows (
                id INTEGER PRIMARY KEY,
            {columnDefinitions}
            );
            INSERT INTO wide_rows(id) VALUES {rowValues};
            """);
        MigrationCatalogObject table = source.Table("wide_rows");
        string[] projection =
        [
            source.Column(table, "id").ObjectId,
            .. nullableColumnNames.Select(
                name => source.Column(table, name).ObjectId),
        ];
        MigrationReadRequest request = source.Request(
            table,
            projection,
            batchSize: rowsPerBatch + 1);

        IReadOnlyList<MigrationDataBatch> batches =
            await ReadAllAsync(source.DataSource, request, Ct);

        Assert.Equal([rowsPerBatch, 1], batches.Select(static batch => batch.Rows.Count));
        Assert.All(
            batches.SelectMany(static batch => batch.Rows),
            row => Assert.Equal(projectedColumnCount, row.Values.Count));
        Assert.Equal(
            Enumerable.Range(1, rowsPerBatch + 1).Select(static value => value.ToString()),
            batches
                .SelectMany(static batch => batch.Rows)
                .Select(static row => row.Values[0].CanonicalText));

        string cursor = Assert.IsType<string>(batches[0].NextCursor);
        IReadOnlyList<MigrationDataBatch> resumed = await ReadAllAsync(
            source.DataSource,
            request with { ResumeCursor = cursor },
            Ct);

        MigrationDataBatch resumedBatch = Assert.Single(resumed);
        Assert.Equal(1, resumedBatch.BatchOrdinal);
        Assert.Equal(cursor, resumedBatch.StartCursor);
        Assert.Null(resumedBatch.NextCursor);
        MigrationDataRow resumedRow = Assert.Single(resumedBatch.Rows);
        Assert.Equal((rowsPerBatch + 1).ToString(), resumedRow.Values[0].CanonicalText);
        Assert.Equal(projectedColumnCount, resumedRow.Values.Count);
    }

    [Fact]
    public async Task EveryEmittedCursorResumesWithoutDuplicatesOrMissingRows()
    {
        using var temporary = new SqliteTestDirectory();
        await using OpenedSource source = await OpenedSource.CreateAsync(
            temporary,
            "resume",
            """
            CREATE TABLE resume_rows (
                id INTEGER PRIMARY KEY,
                value TEXT NOT NULL
            );
            INSERT INTO resume_rows(id, value) VALUES
                (-10, 'a'),
                (-2, 'b'),
                (1, 'c'),
                (7, 'd'),
                (40, 'e'),
                (400, 'f'),
                (9000, 'g');
            """);
        MigrationCatalogObject table = source.Table("resume_rows");
        MigrationReadRequest request = source.Request(
            table,
            [source.Column(table, "id").ObjectId],
            batchSize: 2);
        IReadOnlyList<MigrationDataBatch> original =
            await ReadAllAsync(source.DataSource, request, Ct);
        string[] expected = original
            .SelectMany(static batch => batch.Rows)
            .Select(static row => Assert.IsType<string>(row.Values[0].CanonicalText))
            .ToArray();

        Assert.Equal(["-10", "-2", "1", "7", "40", "400", "9000"], expected);
        for (int boundary = 0; boundary < original.Count - 1; boundary++)
        {
            string cursor = Assert.IsType<string>(original[boundary].NextCursor);
            MigrationReadRequest resumedRequest = request with
            {
                ResumeCursor = cursor,
            };
            IReadOnlyList<MigrationDataBatch> resumed =
                await ReadAllAsync(source.DataSource, resumedRequest, Ct);
            string[] actual = resumed
                .SelectMany(static batch => batch.Rows)
                .Select(static row => Assert.IsType<string>(row.Values[0].CanonicalText))
                .ToArray();
            int rowsBeforeBoundary = original
                .Take(boundary + 1)
                .Sum(static batch => batch.Rows.Count);

            Assert.Equal(expected[rowsBeforeBoundary..], actual);
            Assert.Equal(boundary + 1, resumed[0].BatchOrdinal);
            Assert.Equal(cursor, resumed[0].StartCursor);
        }
    }

    [Fact]
    public async Task RejectsTamperedCrossTableCrossSnapshotCursorsAndWrongSnapshotToken()
    {
        using var temporary = new SqliteTestDirectory();
        const string schema =
            """
            CREATE TABLE first_table (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE second_table (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO first_table(id, value) VALUES (1, 'a'), (2, 'b');
            INSERT INTO second_table(id, value) VALUES (1, 'x'), (2, 'y');
            """;
        await using OpenedSource firstSource = await OpenedSource.CreateAsync(
            temporary,
            "cursor-first",
            schema);
        await using OpenedSource secondSource = await OpenedSource.CreateAsync(
            temporary,
            "cursor-second",
            schema + Environment.NewLine +
            "INSERT INTO first_table(id, value) VALUES (3, 'different snapshot');");

        MigrationCatalogObject firstTable = firstSource.Table("first_table");
        MigrationReadRequest firstRequest = firstSource.Request(
            firstTable,
            [firstSource.Column(firstTable, "id").ObjectId],
            batchSize: 1);
        IReadOnlyList<MigrationDataBatch> firstBatches =
            await ReadAllAsync(firstSource.DataSource, firstRequest, Ct);
        string cursor = Assert.IsType<string>(firstBatches[0].NextCursor);
        string tampered = cursor[..^1] + (cursor[^1] == 'a' ? 'b' : 'a');

        Assert.Throws<InvalidDataException>(() =>
            firstSource.DataSource.ReadAsync(firstRequest with
            {
                ResumeCursor = tampered,
            }, Ct));

        MigrationCatalogObject otherTable = firstSource.Table("second_table");
        MigrationReadRequest otherTableRequest = firstSource.Request(
            otherTable,
            [firstSource.Column(otherTable, "id").ObjectId],
            batchSize: 1);
        Assert.Throws<InvalidDataException>(() =>
            firstSource.DataSource.ReadAsync(otherTableRequest with
            {
                ResumeCursor = cursor,
            }, Ct));

        Assert.Throws<InvalidDataException>(() =>
            firstSource.DataSource.ReadAsync(firstRequest with
            {
                SnapshotToken = secondSource.Snapshot.SnapshotIdentity,
            }, Ct));

        MigrationCatalogObject secondSnapshotTable = secondSource.Table("first_table");
        MigrationReadRequest secondSnapshotRequest = secondSource.Request(
            secondSnapshotTable,
            [secondSource.Column(secondSnapshotTable, "id").ObjectId],
            batchSize: 1);
        Assert.Throws<InvalidDataException>(() =>
            secondSource.DataSource.ReadAsync(secondSnapshotRequest with
            {
                ResumeCursor = cursor,
            }, Ct));
    }

    [Fact]
    public async Task RejectsRecomputedCursorThatWasNotAnEmittedBatchBoundary()
    {
        using var temporary = new SqliteTestDirectory();
        await using OpenedSource source = await OpenedSource.CreateAsync(
            temporary,
            "forged-boundary",
            """
            CREATE TABLE rows_table (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO rows_table(id, value) VALUES
                (1, 'one'),
                (2, 'two'),
                (3, 'three');
            """);
        MigrationCatalogObject table = source.Table("rows_table");
        MigrationReadRequest request = source.Request(
            table,
            [source.Column(table, "id").ObjectId],
            batchSize: 2);
        string scope = SqliteCursorCodec.ComputeScope(
            source.DataSource.Source.Fingerprint,
            source.DataSource.SnapshotIdentity,
            source.DataSource.CatalogDigest,
            table.ObjectId,
            request.ColumnObjectIds,
            request.BatchSize,
            request.MaxBatchBytes,
            request.MaxValueBytes);
        string forged = SqliteCursorCodec.Encode(
            lastRowId: 1,
            nextBatchOrdinal: 1,
            nextSourceRowOrdinal: 1,
            scopeDigest: scope);

        await Assert.ThrowsAsync<InvalidDataException>(
            async () =>
            {
                _ = await ReadAllAsync(
                    source.DataSource,
                    request with { ResumeCursor = forged },
                    Ct);
            });
    }

    [Fact]
    public async Task RejectsCatalogFromAnotherRetainedSnapshot()
    {
        using var temporary = new SqliteTestDirectory();
        await using OpenedSource first = await OpenedSource.CreateAsync(
            temporary,
            "binding-first",
            """
            CREATE TABLE rows_table (id INTEGER PRIMARY KEY, value TEXT);
            INSERT INTO rows_table(id, value) VALUES (1, 'first');
            """);
        await using OpenedSource second = await OpenedSource.CreateAsync(
            temporary,
            "binding-second",
            """
            CREATE TABLE rows_table (id INTEGER PRIMARY KEY, value TEXT);
            INSERT INTO rows_table(id, value) VALUES (1, 'second');
            """);

        await Assert.ThrowsAsync<ArgumentException>(
            async () =>
            {
                _ = await SqliteMigrationDataSource.CreateAsync(
                    first.Snapshot,
                    second.Catalog,
                    Ct);
            });
    }

    [Fact]
    public async Task RejectsOversizedTextAndBlobAndNullAgainstNonNullableCatalog()
    {
        using var temporary = new SqliteTestDirectory();
        await using OpenedSource source = await OpenedSource.CreateAsync(
            temporary,
            "value-policy",
            """
            CREATE TABLE payloads (
                id INTEGER PRIMARY KEY,
                text_value TEXT,
                blob_value BLOB,
                nullable_value TEXT
            );
            INSERT INTO payloads(id, text_value, blob_value, nullable_value)
                VALUES (1, '12345', X'0102030405', NULL);
            """,
            includeProfile: true,
            profileSampleSize: 10);
        MigrationCatalogObject table = source.Table("payloads");
        MigrationCatalogObject textColumn = source.Column(table, "text_value");
        MigrationCatalogObject blobColumn = source.Column(table, "blob_value");
        MigrationCatalogObject nullableColumn = source.Column(table, "nullable_value");

        MigrationRowRejectedException textError = await Assert.ThrowsAsync<
            MigrationRowRejectedException>(
            async () =>
            {
                _ = await ReadAllAsync(
                    source.DataSource,
                    source.Request(
                        table,
                        [textColumn.ObjectId],
                        batchSize: 10,
                        maxBatchBytes: 100,
                        maxValueBytes: 9),
                    Ct);
            });
        AssertRejection(
            textError,
            SqliteMigrationDataRules.ValueSizeExceeded,
            table,
            textColumn,
            sourceRowOrdinal: 0);

        MigrationRowRejectedException blobError = await Assert.ThrowsAsync<
            MigrationRowRejectedException>(
            async () =>
            {
                _ = await ReadAllAsync(
                    source.DataSource,
                    source.Request(
                        table,
                        [blobColumn.ObjectId],
                        batchSize: 10,
                        maxBatchBytes: 100,
                        maxValueBytes: 9),
                    Ct);
            });
        AssertRejection(
            blobError,
            SqliteMigrationDataRules.ValueSizeExceeded,
            table,
            blobColumn,
            sourceRowOrdinal: 0);

        MigrationCatalog nonNullableCatalog = ReplaceFacet(
            source.Catalog,
            nullableColumn.ObjectId,
            "nullable",
            "false");
        await using SqliteMigrationDataSource nonNullable =
            await SqliteMigrationDataSource.CreateAsync(
                source.Snapshot,
                nonNullableCatalog,
                Ct);
        MigrationRowRejectedException nullError = await Assert.ThrowsAsync<
            MigrationRowRejectedException>(
            async () =>
            {
                _ = await ReadAllAsync(
                    nonNullable,
                    new MigrationReadRequest
                    {
                        SourceObjectId = table.ObjectId,
                        ColumnObjectIds = [nullableColumn.ObjectId],
                        BatchSize = 10,
                        MaxBatchBytes = 100,
                        MaxValueBytes = 100,
                        SnapshotToken = source.Snapshot.SnapshotIdentity,
                    },
                    Ct);
            });
        AssertRejection(
            nullError,
            SqliteMigrationDataRules.NullNotAllowed,
            table,
            nullableColumn,
            sourceRowOrdinal: 0);
    }

    [Fact]
    public async Task EnforcesUtf8ByteBoundsAndRejectsMalformedSqliteText()
    {
        using var temporary = new SqliteTestDirectory();
        await using OpenedSource source = await OpenedSource.CreateAsync(
            temporary,
            "utf8-policy",
            """
            CREATE TABLE text_rows (
                id INTEGER PRIMARY KEY,
                value TEXT
            );
            INSERT INTO text_rows(id, value) VALUES
                (1, '😀'),
                (2, CAST(X'80' AS TEXT));
            """,
            includeProfile: true,
            profileSampleSize: 10);
        MigrationCatalogObject table = source.Table("text_rows");
        MigrationCatalogObject column = source.Column(table, "value");

        MigrationRowRejectedException sizeError = await Assert.ThrowsAsync<
            MigrationRowRejectedException>(
            async () =>
            {
                _ = await ReadAllAsync(
                    source.DataSource,
                    source.Request(
                        table,
                        [column.ObjectId],
                        batchSize: 10,
                        maxBatchBytes: 100,
                        maxValueBytes: 8),
                    Ct);
            });
        AssertRejection(
            sizeError,
            SqliteMigrationDataRules.ValueSizeExceeded,
            table,
            column,
            sourceRowOrdinal: 0);

        MigrationRowRejectedException encodingError =
            await Assert.ThrowsAsync<MigrationRowRejectedException>(
                async () =>
                {
                    _ = await ReadAllAsync(
                        source.DataSource,
                        source.Request(
                            table,
                            [column.ObjectId],
                            batchSize: 10,
                            maxBatchBytes: 100,
                            maxValueBytes: 100),
                        Ct);
                });
        AssertRejection(
            encodingError,
            SqliteMigrationDataRules.InvalidTextEncoding,
            table,
            column,
            sourceRowOrdinal: 1);
    }

    [Fact]
    public async Task LateStorageClassConflictFailsDuringStreaming()
    {
        using var temporary = new SqliteTestDirectory();
        await using OpenedSource source = await OpenedSource.CreateAsync(
            temporary,
            "late-conflict",
            """
            CREATE TABLE late_values (
                id INTEGER PRIMARY KEY,
                value INTEGER
            );
            INSERT INTO late_values(id, value) VALUES
                (1, 10),
                (2, 20),
                (3, 'late text');
            """,
            includeProfile: true,
            profileSampleSize: 2);
        MigrationCatalogObject table = source.Table("late_values");
        MigrationCatalogObject column = source.Column(table, "value");
        AssertFacet(column, "profileKind", MigrationCoverageKind.Sample.ToString());
        AssertFacet(column, "sqliteStorageClasses", "integer");

        MigrationRowRejectedException error = await Assert.ThrowsAsync<
            MigrationRowRejectedException>(
            async () =>
            {
                _ = await ReadAllAsync(
                    source.DataSource,
                    source.Request(table, [column.ObjectId], batchSize: 10),
                    Ct);
            });

        AssertRejection(
            error,
            SqliteMigrationDataRules.StorageClassMismatch,
            table,
            column,
            sourceRowOrdinal: 2);
    }

    [Fact]
    public async Task ResumePreservesGlobalSourceRowOrdinalForLateFailure()
    {
        using var temporary = new SqliteTestDirectory();
        await using OpenedSource source = await OpenedSource.CreateAsync(
            temporary,
            "resume-late-conflict",
            """
            CREATE TABLE late_values (
                id INTEGER PRIMARY KEY,
                value INTEGER
            );
            INSERT INTO late_values(id, value) VALUES
                (1, 10),
                (2, 20),
                (3, 'late text');
            """,
            includeProfile: true,
            profileSampleSize: 2);
        MigrationCatalogObject table = source.Table("late_values");
        MigrationCatalogObject column = source.Column(table, "value");
        MigrationReadRequest request = source.Request(
            table,
            [column.ObjectId],
            batchSize: 1);
        IAsyncEnumerator<MigrationDataBatch> initial = source.DataSource
            .ReadAsync(request, Ct)
            .GetAsyncEnumerator(Ct);
        string cursor;
        try
        {
            Assert.True(await initial.MoveNextAsync());
            cursor = Assert.IsType<string>(initial.Current.NextCursor);
        }
        finally
        {
            await initial.DisposeAsync();
        }

        MigrationRowRejectedException error = await Assert.ThrowsAsync<
            MigrationRowRejectedException>(
            async () =>
            {
                _ = await ReadAllAsync(
                    source.DataSource,
                    request with { ResumeCursor = cursor },
                    Ct);
            });

        AssertRejection(
            error,
            SqliteMigrationDataRules.StorageClassMismatch,
            table,
            column,
            sourceRowOrdinal: 2,
            batchOrdinal: 1);
    }

    [Fact]
    public async Task RejectsDeterministicRejectContractBeforeStreaming()
    {
        using var temporary = new SqliteTestDirectory();
        await using OpenedSource source = await OpenedSource.CreateAsync(
            temporary,
            "reject-contract",
            """
            CREATE TABLE rows_table (id INTEGER PRIMARY KEY, value TEXT);
            INSERT INTO rows_table(id, value) VALUES (1, 'one');
            """);
        MigrationCatalogObject table = source.Table("rows_table");
        MigrationReadRequest request = source.Request(
            table,
            [source.Column(table, "value").ObjectId],
            batchSize: 2) with
        {
            RejectContractVersion = MigrationRejectContract.DeterministicRejectsV1,
            RejectPolicy = new MigrationDeterministicRejectPolicy
            {
                ContractVersion = MigrationRejectContract.DeterministicRejectsV1,
                AllowedRuleIds = [SqliteMigrationDataRules.StorageClassMismatch],
                MaxRejectedRowsPerBatch = 1,
                MaxRejectedRowsPerRun = 10,
                MaxRawValueBytes = 1_024,
                MaxRawValueBytesPerBatch = 4_096,
                MaxRawValueBytesPerRun = 8_192,
                MaxArtifactBytes = 1024 * 1024,
            },
        };

        Assert.Throws<NotSupportedException>(() =>
            source.DataSource.ReadAsync(request, Ct));
    }

    [Fact]
    public async Task EmptyTableEmitsNoBatchesAndCancellationIsObserved()
    {
        using var temporary = new SqliteTestDirectory();
        await using OpenedSource source = await OpenedSource.CreateAsync(
            temporary,
            "empty-cancellation",
            """
            CREATE TABLE empty_rows (id INTEGER PRIMARY KEY, value TEXT);
            CREATE TABLE populated_rows (id INTEGER PRIMARY KEY, value TEXT);
            INSERT INTO populated_rows(id, value) VALUES (1, 'one');
            """);

        MigrationCatalogObject empty = source.Table("empty_rows");
        IReadOnlyList<MigrationDataBatch> emptyBatches = await ReadAllAsync(
            source.DataSource,
            source.Request(empty, [source.Column(empty, "id").ObjectId]),
            Ct);
        Assert.Empty(emptyBatches);

        MigrationCatalogObject populated = source.Table("populated_rows");
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () =>
            {
                _ = await ReadAllAsync(
                    source.DataSource,
                    source.Request(
                        populated,
                        [source.Column(populated, "id").ObjectId]),
                    cancellation.Token);
            });
    }

    [Fact]
    public async Task DisposeWaitsForActiveReaderAndPreventsNewReaders()
    {
        using var temporary = new SqliteTestDirectory();
        await using OpenedSource source = await OpenedSource.CreateAsync(
            temporary,
            "dispose-reader",
            """
            CREATE TABLE rows_table (id INTEGER PRIMARY KEY, value TEXT);
            INSERT INTO rows_table(id, value) VALUES
                (1, 'one'),
                (2, 'two');
            """);
        MigrationCatalogObject table = source.Table("rows_table");
        MigrationReadRequest request = source.Request(
            table,
            [source.Column(table, "id").ObjectId],
            batchSize: 1);
        IAsyncEnumerator<MigrationDataBatch> reader = source.DataSource
            .ReadAsync(request, Ct)
            .GetAsyncEnumerator(Ct);
        Assert.True(await reader.MoveNextAsync());

        Task disposal = source.DataSource.DisposeAsync().AsTask();
        Assert.False(disposal.IsCompleted);

        await reader.DisposeAsync();
        await disposal;
        Assert.Throws<ObjectDisposedException>(
            () => source.DataSource.ReadAsync(request, Ct));
    }

    [Fact]
    public async Task StreamsOneHundredThousandRowsInFixedSmallBatches()
    {
        using var temporary = new SqliteTestDirectory();
        await using OpenedSource source = await OpenedSource.CreateAsync(
            temporary,
            "large-stream",
            """
            CREATE TABLE large_rows (
                id INTEGER PRIMARY KEY,
                value INTEGER NOT NULL
            );
            WITH digits(d) AS (
                VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9)
            )
            INSERT INTO large_rows(id, value)
            SELECT
                a.d * 10000 + b.d * 1000 + c.d * 100 + d.d * 10 + e.d + 1,
                a.d * 10000 + b.d * 1000 + c.d * 100 + d.d * 10 + e.d + 1
            FROM digits AS a
            CROSS JOIN digits AS b
            CROSS JOIN digits AS c
            CROSS JOIN digits AS d
            CROSS JOIN digits AS e;
            """);
        MigrationCatalogObject table = source.Table("large_rows");
        MigrationReadRequest request = source.Request(
            table,
            [source.Column(table, "value").ObjectId],
            batchSize: 127,
            maxBatchBytes: 127 * 9,
            maxValueBytes: 9);

        long rowCount = 0;
        long batchCount = 0;
        string? first = null;
        string? last = null;
        await foreach (MigrationDataBatch batch in source.DataSource
                           .ReadAsync(request, Ct)
                           .WithCancellation(Ct))
        {
            Assert.InRange(batch.Rows.Count, 1, 127);
            foreach (MigrationDataRow row in batch.Rows)
            {
                string value = Assert.IsType<string>(row.Values[0].CanonicalText);
                first ??= value;
                last = value;
                rowCount++;
            }
            batchCount++;
        }

        Assert.Equal(100_000, rowCount);
        Assert.Equal((100_000L + 126) / 127, batchCount);
        Assert.Equal("1", first);
        Assert.Equal("100000", last);
    }

    private static async Task<IReadOnlyList<MigrationDataBatch>> ReadAllAsync(
        SqliteMigrationDataSource source,
        MigrationReadRequest request,
        CancellationToken cancellationToken)
    {
        var batches = new List<MigrationDataBatch>();
        await foreach (MigrationDataBatch batch in source
                           .ReadAsync(request, cancellationToken)
                           .WithCancellation(cancellationToken))
        {
            batches.Add(batch);
        }
        return batches;
    }

    private static MigrationCatalog ReplaceFacet(
        MigrationCatalog catalog,
        string objectId,
        string facetName,
        string value) =>
        catalog with
        {
            Objects = catalog.Objects.Select(candidate =>
                candidate.ObjectId == objectId
                    ? candidate with
                    {
                        Facets = candidate.Facets.Select(facet =>
                                facet.Name == facetName
                                    ? facet with { Value = value }
                                    : facet)
                            .ToArray(),
                    }
                    : candidate)
                .ToArray(),
        };

    private static void AssertRejection(
        MigrationRowRejectedException error,
        string expectedCode,
        MigrationCatalogObject table,
        MigrationCatalogObject column,
        long sourceRowOrdinal,
        long batchOrdinal = 0)
    {
        Assert.Equal(MigrationRejectContract.DeterministicFailFastV1, error.ContractVersion);
        Assert.Equal(expectedCode, error.Code);
        Assert.Equal(table.ObjectId, error.SourceObjectId);
        Assert.Equal(column.ObjectId, error.ColumnObjectId);
        Assert.Equal(batchOrdinal, error.BatchOrdinal);
        Assert.Equal(sourceRowOrdinal, error.SourceRowOrdinal);
    }

    private static void AssertValue(
        MigrationSourceValue value,
        MigrationSourceValueKind expectedKind,
        string? expectedText)
    {
        Assert.Equal(expectedKind, value.Kind);
        Assert.Equal(expectedText, value.CanonicalText);
        Assert.True(value.BinaryValue.IsEmpty);
    }

    private static void AssertFacet(
        MigrationCatalogObject schemaObject,
        string name,
        string expectedValue)
    {
        MigrationCatalogFacet facet = Assert.Single(
            schemaObject.Facets,
            candidate => candidate.Name == name);
        Assert.Equal(expectedValue, facet.Value);
    }

    private sealed class OpenedSource : IAsyncDisposable
    {
        private OpenedSource(
            SqliteBackupSnapshot snapshot,
            MigrationCatalog catalog,
            SqliteMigrationDataSource dataSource)
        {
            Snapshot = snapshot;
            Catalog = catalog;
            DataSource = dataSource;
        }

        internal SqliteBackupSnapshot Snapshot { get; }

        internal MigrationCatalog Catalog { get; }

        internal SqliteMigrationDataSource DataSource { get; }

        internal static async ValueTask<OpenedSource> CreateAsync(
            SqliteTestDirectory temporary,
            string prefix,
            string sql,
            bool includeProfile = false,
            int profileSampleSize = 1_000)
        {
            string sourcePath = temporary.PathFor(prefix + "-source.sqlite");
            string snapshotPath = temporary.PathFor(prefix + "-snapshot.sqlite");
            await SqliteTestDatabase.CreateAsync(sourcePath, sql, Ct);
            SqliteBackupSnapshot snapshot = await SqliteBackupSnapshot.CreateAsync(
                sourcePath,
                snapshotPath,
                Ct);
            var inspector = new SqliteMigrationSourceInspector(snapshot);
            MigrationCatalog catalog = await inspector.InspectAsync(
                new MigrationInspectionRequest
                {
                    TargetCSharpDbVersion =
                        CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                    IncludeProfile = includeProfile,
                    ProfileSampleSize = profileSampleSize,
                },
                Ct);
            SqliteMigrationDataSource dataSource =
                await SqliteMigrationDataSource.CreateAsync(snapshot, catalog, Ct);
            return new OpenedSource(snapshot, catalog, dataSource);
        }

        internal MigrationCatalogObject Table(string sourceName) =>
            Assert.Single(
                Catalog.Objects,
                candidate =>
                    candidate.Kind == MigrationObjectKind.Table &&
                    candidate.SourceName == sourceName);

        internal MigrationCatalogObject Column(
            MigrationCatalogObject table,
            string sourceName) =>
            Assert.Single(
                Catalog.Objects,
                candidate =>
                    candidate.Kind == MigrationObjectKind.Column &&
                    candidate.ParentObjectId == table.ObjectId &&
                    candidate.SourceName == sourceName);

        internal MigrationReadRequest Request(
            MigrationCatalogObject table,
            IReadOnlyList<string> columnObjectIds,
            int batchSize = 1_000,
            long maxBatchBytes = 1024 * 1024,
            int maxValueBytes = 64 * 1024) => new()
            {
                SourceObjectId = table.ObjectId,
                ColumnObjectIds = columnObjectIds,
                BatchSize = batchSize,
                MaxBatchBytes = maxBatchBytes,
                MaxValueBytes = maxValueBytes,
                SnapshotToken = Snapshot.SnapshotIdentity,
            };

        public async ValueTask DisposeAsync()
        {
            await DataSource.DisposeAsync();
            await SqliteTestDatabase.DisposeIfSupportedAsync(Snapshot);
        }
    }
}
