using System.Globalization;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Migration.Canonicalization;
using CSharpDB.Migration.Files.Json;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Files.Tests;

public sealed class JsonStreamingExporterTests
{
    private static CancellationToken Cancellation =>
        TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(JsonExportFraming.RootArray, "[]\n")]
    [InlineData(JsonExportFraming.Ndjson, "")]
    public async Task EmptyExportHasExactFramingAndProofs(
        JsonExportFraming framing,
        string expected)
    {
        TableSchema schema = Schema(
            "empty",
            Column("id", DbType.Integer));

        (JsonStreamingExportResult result, byte[] bytes) =
            await ExportAsync(schema, [], framing);

        Assert.Equal(expected, Encoding.UTF8.GetString(bytes));
        Assert.Equal(0, result.Manifest.Content.RowCount);
        Assert.Equal(bytes.LongLength, result.Manifest.Content.DataByteLength);
        Assert.Equal(
            PhysicalDigest(bytes),
            result.Manifest.Content.DataDigest.Value);
        Assert.Equal(
            result.Manifest.Content.SourceLogicalDigest,
            result.Manifest.Content.ExportedLogicalDigest);
        Assert.Equal(
            JsonExportManifestSerializer.Serialize(result.Manifest),
            result.CanonicalManifestBytes);
        Assert.Equal(
            JsonExportManifestSerializer.ComputeManifestDigest(
                result.Manifest),
            result.ManifestDigest);
        Assert.Empty(await ReadAsync(bytes, framing));
    }

    [Theory]
    [InlineData(
        JsonExportFraming.RootArray,
        "[{\"id\":1,\"name\":\"one\"}]\n")]
    [InlineData(
        JsonExportFraming.Ndjson,
        "{\"id\":1,\"name\":\"one\"}\n")]
    public async Task OneRowHasExactBytesAndReaderRoundTrips(
        JsonExportFraming framing,
        string expected)
    {
        TableSchema schema = Schema(
            "one",
            Column("id", DbType.Integer, nullable: false),
            Column("name", DbType.Text, nullable: false));

        (JsonStreamingExportResult result, byte[] bytes) =
            await ExportAsync(
                schema,
                [
                    Row(
                        1,
                        DbValue.FromInteger(1),
                        DbValue.FromText("one")),
                ],
                framing);

        Assert.Equal(expected, Encoding.UTF8.GetString(bytes));
        Assert.Equal(1, result.Manifest.Content.RowCount);
        JsonLogicalValue value =
            Assert.Single(await ReadAsync(bytes, framing)).Value;
        Assert.Equal(JsonLogicalValueKind.Object, value.Kind);
        Assert.Equal(
            ["id", "name"],
            value.Properties.Select(
                static property => property.Name));
        Assert.Equal(
            "1",
            value.Properties[0].Value.NumberLexeme);
        Assert.Equal(
            "one",
            value.Properties[1].Value.StringValue);
    }

    [Theory]
    [InlineData(
        JsonExportFraming.RootArray,
        "[{\"id\":-9,\"score\":-9.5,\"note\":null,\"payload\":\"AAEC\"},{\"id\":4,\"score\":4.25,\"note\":\"{\\\"nested\\\":true}\",\"payload\":\"\"}]\n")]
    [InlineData(
        JsonExportFraming.Ndjson,
        "{\"id\":-9,\"score\":-9.5,\"note\":null,\"payload\":\"AAEC\"}\n{\"id\":4,\"score\":4.25,\"note\":\"{\\\"nested\\\":true}\",\"payload\":\"\"}\n")]
    public async Task MixedRowsHaveExactLosslessBytesAndTypedEvidence(
        JsonExportFraming framing,
        string expected)
    {
        TableSchema schema = Schema(
            "mixed",
            Column("id", DbType.Integer, nullable: false),
            Column("score", DbType.Real, nullable: false),
            Column("note", DbType.Text),
            Column("payload", DbType.Blob, nullable: false));
        JsonExportRow[] rows =
        [
            Row(
                -9,
                DbValue.FromInteger(-9),
                DbValue.FromReal(-9.5),
                DbValue.Null,
                DbValue.FromBlob([0, 1, 2])),
            Row(
                4,
                DbValue.FromInteger(4),
                DbValue.FromReal(4.25),
                DbValue.FromText("{\"nested\":true}"),
                DbValue.FromBlob([])),
        ];

        (JsonStreamingExportResult result, byte[] bytes) =
            await ExportAsync(
                schema,
                rows,
                framing,
                maximumDecodedBlobBytes: 3);

        Assert.Equal(expected, Encoding.UTF8.GetString(bytes));
        Assert.Equal(
            [
                JsonExportDatabaseType.Integer,
                JsonExportDatabaseType.Real,
                JsonExportDatabaseType.Text,
                JsonExportDatabaseType.Blob,
            ],
            result.Manifest.Table.Columns.Select(
                static column => column.DatabaseType));
        Assert.Equal(
            ["id", "score", "note", "payload"],
            result.Manifest.Table.Columns.Select(
                static column => column.PropertyName));
        Assert.Equal(
            result.Manifest.Content.SourceLogicalDigest,
            result.Manifest.Content.ExportedLogicalDigest);
        Assert.Equal(
            OrderedDigest(
                [
                    CanonicalValue.Int64(-9),
                    CanonicalValue.Binary64(-9.5),
                    CanonicalValue.Null(CanonicalType.Text),
                    CanonicalValue.Blob(
                        new byte[] { 0, 1, 2 }),
                ],
                [
                    CanonicalValue.Int64(4),
                    CanonicalValue.Binary64(4.25),
                    CanonicalValue.Text("{\"nested\":true}"),
                    CanonicalValue.Blob(
                        Array.Empty<byte>()),
                ]),
            result.Manifest.Content.SourceLogicalDigest);

        List<JsonLogicalRecord> parsed =
            await ReadAsync(bytes, framing);
        Assert.Equal(2, parsed.Count);
        Assert.Equal(
            "{\"nested\":true}",
            parsed[1].Value.Properties[2].Value.StringValue);
    }

    [Theory]
    [InlineData(JsonExportFraming.RootArray)]
    [InlineData(JsonExportFraming.Ndjson)]
    public async Task SchemaOrderAndMinimalUnicodeEscapingAreStable(
        JsonExportFraming framing)
    {
        const string hostileName = "z\"\\\n\U0001F642";
        const string hostileText =
            "a\"\\\b\t\n\f\r\0\U0001F642";
        TableSchema schema = Schema(
            "unicode",
            Column(hostileName, DbType.Text, nullable: false),
            Column("a", DbType.Integer, nullable: false));

        (_, byte[] bytes) = await ExportAsync(
            schema,
            [
                Row(
                    1,
                    DbValue.FromText(hostileText),
                    DbValue.FromInteger(2)),
            ],
            framing);

        string objectText =
            "{\"z\\\"\\\\\\n🙂\":\"a\\\"\\\\\\b\\t\\n\\f\\r\\u0000🙂\",\"a\":2}";
        string expected = framing == JsonExportFraming.RootArray
            ? $"[{objectText}]\n"
            : objectText + "\n";
        Assert.Equal(expected, Encoding.UTF8.GetString(bytes));

        JsonLogicalValue parsed =
            Assert.Single(await ReadAsync(bytes, framing)).Value;
        Assert.Equal(
            [hostileName, "a"],
            parsed.Properties.Select(
                static property => property.Name));
        Assert.Equal(
            hostileText,
            parsed.Properties[0].Value.StringValue);
    }

    [Fact]
    public async Task NullEmptyAndJsonLookingTextRemainDistinctStrings()
    {
        TableSchema schema = Schema(
            "text",
            Column("value", DbType.Text));
        JsonExportRow[] rows =
        [
            Row(1, DbValue.Null),
            Row(2, DbValue.FromText(string.Empty)),
            Row(3, DbValue.FromText("null")),
            Row(4, DbValue.FromText("[1,2]")),
        ];

        (_, byte[] bytes) = await ExportAsync(
            schema,
            rows,
            JsonExportFraming.Ndjson);

        Assert.Equal(
            "{\"value\":null}\n" +
            "{\"value\":\"\"}\n" +
            "{\"value\":\"null\"}\n" +
            "{\"value\":\"[1,2]\"}\n",
            Encoding.UTF8.GetString(bytes));
        List<JsonLogicalRecord> parsed =
            await ReadAsync(bytes, JsonExportFraming.Ndjson);
        Assert.Equal(JsonLogicalValueKind.Null, Cell(parsed[0]).Kind);
        Assert.Equal(string.Empty, Cell(parsed[1]).StringValue);
        Assert.Equal("null", Cell(parsed[2]).StringValue);
        Assert.Equal("[1,2]", Cell(parsed[3]).StringValue);
    }

    [Theory]
    [InlineData(JsonExportFraming.RootArray)]
    [InlineData(JsonExportFraming.Ndjson)]
    public async Task Int64EndpointsAreInvariantJsonNumbers(
        JsonExportFraming framing)
    {
        TableSchema schema = Schema(
            "integers",
            Column("value", DbType.Integer, nullable: false));

        (_, byte[] bytes) = await ExportAsync(
            schema,
            [
                Row(long.MinValue, DbValue.FromInteger(long.MinValue)),
                Row(long.MaxValue, DbValue.FromInteger(long.MaxValue)),
            ],
            framing);

        string objects =
            "{\"value\":-9223372036854775808}," +
            "{\"value\":9223372036854775807}";
        Assert.Equal(
            framing == JsonExportFraming.RootArray
                ? $"[{objects}]\n"
                : objects.Replace("},{", "}\n{", StringComparison.Ordinal) +
                  "\n",
            Encoding.UTF8.GetString(bytes));
    }

    [Fact]
    public async Task FiniteRealLexemesPreserveNegativeZeroAndSubnormal()
    {
        TableSchema schema = Schema(
            "reals",
            Column("value", DbType.Real, nullable: false));
        double negativeZero =
            BitConverter.Int64BitsToDouble(
                unchecked((long)0x8000000000000000UL));
        JsonExportRow[] rows =
        [
            Row(1, DbValue.FromReal(negativeZero)),
            Row(2, DbValue.FromReal(double.Epsilon)),
            Row(3, DbValue.FromReal(0.1)),
            Row(4, DbValue.FromReal(double.MaxValue)),
        ];

        (_, byte[] bytes) = await ExportAsync(
            schema,
            rows,
            JsonExportFraming.Ndjson);

        Assert.Equal(
            "{\"value\":-0}\n" +
            "{\"value\":5E-324}\n" +
            "{\"value\":0.1}\n" +
            "{\"value\":1.7976931348623157E+308}\n",
            Encoding.UTF8.GetString(bytes));
        Assert.Equal(
            ["-0", "5E-324", "0.1", "1.7976931348623157E+308"],
            (await ReadAsync(bytes, JsonExportFraming.Ndjson))
                .Select(static record =>
                    Cell(record).NumberLexeme));
    }

    [Theory]
    [InlineData(double.NaN)]
    [InlineData(double.PositiveInfinity)]
    [InlineData(double.NegativeInfinity)]
    public async Task NonFiniteRealIsRejectedBeforeCurrentRow(
        double value)
    {
        TableSchema schema = Schema(
            "reals",
            Column("value", DbType.Real, nullable: false));

        foreach (JsonExportFraming framing in
                 Enum.GetValues<JsonExportFraming>())
        {
            await using var destination = new MemoryStream();
            InvalidDataException error =
                await Assert.ThrowsAsync<InvalidDataException>(
                    () => new JsonStreamingExporter()
                        .WriteAsync(
                            destination,
                            Request(
                                schema,
                                [
                                    Row(
                                        1,
                                        DbValue.FromReal(value)),
                                ],
                                framing),
                            Cancellation)
                        .AsTask());

            Assert.Contains(
                "non-finite",
                error.Message,
                StringComparison.OrdinalIgnoreCase);
            Assert.Equal(
                framing == JsonExportFraming.RootArray
                    ? "["
                    : string.Empty,
                Encoding.UTF8.GetString(destination.ToArray()));
        }
    }

    [Fact]
    public async Task EmptyPaddedAndConfiguredCeilingBlobsRoundTrip()
    {
        TableSchema schema = Schema(
            "blobs",
            Column("payload", DbType.Blob, nullable: false));
        JsonExportRow[] rows =
        [
            Row(1, DbValue.FromBlob([])),
            Row(2, DbValue.FromBlob([0])),
            Row(3, DbValue.FromBlob([0, 1])),
            Row(4, DbValue.FromBlob([0, 1, 2])),
        ];

        (_, byte[] bytes) = await ExportAsync(
            schema,
            rows,
            JsonExportFraming.Ndjson,
            maximumDecodedBlobBytes: 3);

        Assert.Equal(
            "{\"payload\":\"\"}\n" +
            "{\"payload\":\"AA==\"}\n" +
            "{\"payload\":\"AAE=\"}\n" +
            "{\"payload\":\"AAEC\"}\n",
            Encoding.UTF8.GetString(bytes));

        await using var rejected = new MemoryStream();
        await Assert.ThrowsAsync<InvalidDataException>(
            () => new JsonStreamingExporter()
                .WriteAsync(
                    rejected,
                    Request(
                        schema,
                        [
                            Row(
                                1,
                                DbValue.FromBlob([0, 1, 2])),
                        ],
                        JsonExportFraming.Ndjson,
                        maximumDecodedBlobBytes: 2),
                    Cancellation)
                .AsTask());
        Assert.Empty(rejected.ToArray());
    }

    [Fact]
    public async Task AbsoluteMaximumBlobIsAcceptedAtItsExactCeiling()
    {
        TableSchema schema = Schema(
            "maximum-blob",
            Column("payload", DbType.Blob, nullable: false));
        byte[] blob = new byte[
            JsonExportContracts.MaximumSupportedDecodedBlobBytes];
        blob[^1] = 0xff;

        (JsonStreamingExportResult result, byte[] bytes) =
            await ExportAsync(
                schema,
                [Row(1, DbValue.FromBlob(blob))],
                JsonExportFraming.Ndjson,
                maxDataBytes:
                    JsonInputContracts.MaximumValueBytes + 1L,
                maximumDecodedBlobBytes: blob.Length);

        Assert.Equal(1, result.Manifest.Content.RowCount);
        Assert.Equal(bytes.LongLength, result.Manifest.Content.DataByteLength);
        JsonLogicalValue value =
            Cell(Assert.Single(
                await ReadAsync(bytes, JsonExportFraming.Ndjson)));
        byte[] decoded = Convert.FromBase64String(value.StringValue);
        Assert.Equal(blob.Length, decoded.Length);
        Assert.Equal(0xff, decoded[^1]);
    }

    [Fact]
    public async Task RowWidthOrderRuntimeTypeAndNullabilityFailAtBoundary()
    {
        TableSchema schema = Schema(
            "shape",
            Column("id", DbType.Integer, nullable: false),
            Column("text", DbType.Text, nullable: false));
        JsonExportRow[][] invalidCases =
        [
            [
                Row(1, DbValue.FromInteger(1)),
            ],
            [
                Row(
                    1,
                    DbValue.FromText("wrong"),
                    DbValue.FromText("value")),
            ],
            [
                Row(1, DbValue.FromInteger(1), DbValue.Null),
            ],
            [
                Row(
                    1,
                    DbValue.FromInteger(1),
                    DbValue.FromText("one")),
                Row(
                    1,
                    DbValue.FromInteger(2),
                    DbValue.FromText("two")),
            ],
        ];

        foreach (JsonExportRow[] rows in invalidCases)
        {
            foreach (JsonExportFraming framing in
                     Enum.GetValues<JsonExportFraming>())
            {
                await using var destination =
                    new MemoryStream();
                await Assert.ThrowsAsync<InvalidDataException>(
                    () => new JsonStreamingExporter()
                        .WriteAsync(
                            destination,
                            Request(schema, rows, framing),
                            Cancellation)
                        .AsTask());

                string actual =
                    Encoding.UTF8.GetString(
                        destination.ToArray());
                if (rows.Length == 1)
                {
                    Assert.Equal(
                        framing ==
                            JsonExportFraming.RootArray
                            ? "["
                            : string.Empty,
                        actual);
                }
                else
                {
                    Assert.Equal(
                        framing ==
                            JsonExportFraming.RootArray
                            ? "[{\"id\":1,\"text\":\"one\"}"
                            : "{\"id\":1,\"text\":\"one\"}\n",
                        actual);
                }
            }
        }
    }

    [Fact]
    public async Task InvalidUtf16FailsBeforeCurrentRow()
    {
        TableSchema schema = Schema(
            "text",
            Column("value", DbType.Text, nullable: false));
        await using var destination = new MemoryStream();

        InvalidDataException error =
            await Assert.ThrowsAsync<InvalidDataException>(
                () => new JsonStreamingExporter()
                    .WriteAsync(
                        destination,
                        Request(
                            schema,
                            [
                                Row(
                                    1,
                                    DbValue.FromText(
                                        "invalid-\ud800")),
                            ],
                            JsonExportFraming.RootArray),
                        Cancellation)
                    .AsTask());

        Assert.Contains(
            "invalid",
            error.Message,
            StringComparison.OrdinalIgnoreCase);
        Assert.Equal(
            "[",
            Encoding.UTF8.GetString(destination.ToArray()));
    }

    [Theory]
    [InlineData(JsonExportFraming.RootArray)]
    [InlineData(JsonExportFraming.Ndjson)]
    public async Task ExactDataCeilingSucceedsAndOneUnderWritesNoPartialRow(
        JsonExportFraming framing)
    {
        TableSchema schema = Schema(
            "limit",
            Column("id", DbType.Integer, nullable: false));
        JsonExportRow[] rows =
        [
            Row(1, DbValue.FromInteger(1)),
        ];
        (_, byte[] exactBytes) =
            await ExportAsync(schema, rows, framing);

        (JsonStreamingExportResult exact, byte[] repeated) =
            await ExportAsync(
                schema,
                rows,
                framing,
                maxDataBytes: exactBytes.LongLength);
        Assert.Equal(exactBytes, repeated);
        Assert.Equal(
            exactBytes.LongLength,
            exact.Manifest.Content.DataByteLength);

        await using var oneUnder = new MemoryStream();
        await Assert.ThrowsAsync<InvalidDataException>(
            () => new JsonStreamingExporter()
                .WriteAsync(
                    oneUnder,
                    Request(
                        schema,
                        rows,
                        framing,
                        maxDataBytes:
                            exactBytes.LongLength - 1),
                    Cancellation)
                .AsTask());
        Assert.Equal(
            framing == JsonExportFraming.RootArray
                ? "["
                : string.Empty,
            Encoding.UTF8.GetString(oneUnder.ToArray()));
    }

    [Fact]
    public async Task FailureAfterOneRowLeavesPriorCompleteBoundaryOnly()
    {
        TableSchema schema = Schema(
            "boundary",
            Column("id", DbType.Integer, nullable: false));
        JsonExportRow[] rows =
        [
            Row(1, DbValue.FromInteger(1)),
            Row(2, DbValue.FromText("wrong")),
        ];

        foreach (JsonExportFraming framing in
                 Enum.GetValues<JsonExportFraming>())
        {
            await using var destination = new MemoryStream();
            await Assert.ThrowsAsync<InvalidDataException>(
                () => new JsonStreamingExporter()
                    .WriteAsync(
                        destination,
                        Request(schema, rows, framing),
                        Cancellation)
                    .AsTask());

            Assert.Equal(
                framing == JsonExportFraming.RootArray
                    ? "[{\"id\":1}"
                    : "{\"id\":1}\n",
                Encoding.UTF8.GetString(destination.ToArray()));
        }
    }

    [Fact]
    public async Task PreCanceledExportWritesNothingAndLeavesDestinationOpen()
    {
        TableSchema schema = Schema(
            "cancel",
            Column("id", DbType.Integer));
        await using var destination = new MemoryStream();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => new JsonStreamingExporter()
                .WriteAsync(
                    destination,
                    Request(
                        schema,
                        [
                            Row(1, DbValue.FromInteger(1)),
                        ],
                        JsonExportFraming.RootArray),
                    cancellation.Token)
                .AsTask());

        Assert.Empty(destination.ToArray());
        destination.WriteByte(0x7f);
        Assert.Equal(1, destination.Length);
    }

    [Theory]
    [InlineData(JsonExportFraming.RootArray)]
    [InlineData(JsonExportFraming.Ndjson)]
    public async Task CancellationDuringEnumerationStopsAtCompleteBoundary(
        JsonExportFraming framing)
    {
        TableSchema schema = Schema(
            "cancel-boundary",
            Column("id", DbType.Integer, nullable: false));
        await using var destination = new MemoryStream();
        using var cancellation = new CancellationTokenSource();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => new JsonStreamingExporter()
                .WriteAsync(
                    destination,
                    Request(
                        schema,
                        RowThenCancel(
                            cancellation,
                            Cancellation),
                        framing),
                    cancellation.Token)
                .AsTask());

        Assert.Equal(
            framing == JsonExportFraming.RootArray
                ? "[{\"id\":1}"
                : "{\"id\":1}\n",
            Encoding.UTF8.GetString(destination.ToArray()));
        long boundary = destination.Length;
        destination.WriteByte(0x7f);
        Assert.Equal(boundary + 1, destination.Length);
    }

    [Fact]
    public async Task LargeTextAndBlobUseBoundedWritesAndLeaveDestinationOpen()
    {
        TableSchema schema = Schema(
            "large",
            Column("text", DbType.Text, nullable: false),
            Column("blob", DbType.Blob, nullable: false));
        string text = new('é', 100_000);
        byte[] blob = Enumerable
            .Range(0, 100_000)
            .Select(static value => unchecked((byte)value))
            .ToArray();
        await using var destination = new ChunkTrackingStream();

        JsonStreamingExportResult result =
            await new JsonStreamingExporter().WriteAsync(
                destination,
                Request(
                    schema,
                    [
                        Row(
                            1,
                            DbValue.FromText(text),
                            DbValue.FromBlob(blob)),
                    ],
                    JsonExportFraming.RootArray,
                    maximumDecodedBlobBytes: blob.Length),
                Cancellation);

        Assert.Equal(1, result.Manifest.Content.RowCount);
        Assert.InRange(
            destination.MaximumWriteSize,
            1,
            16 * 1024);
        Assert.True(destination.FlushObserved);
        long completedLength = destination.Length;
        destination.WriteByte(0x7f);
        Assert.Equal(completedLength + 1, destination.Length);
    }

    [Theory]
    [InlineData(JsonExportFraming.RootArray)]
    [InlineData(JsonExportFraming.Ndjson)]
    public async Task FiftyThousandRowsStreamWithDeterministicProofs(
        JsonExportFraming framing)
    {
        const int rowCount = 50_000;
        TableSchema schema = Schema(
            "large-stream",
            Column("id", DbType.Integer, nullable: false),
            Column("value", DbType.Text, nullable: false));
        await using var destination = new ChunkTrackingStream();

        JsonStreamingExportResult first =
            await new JsonStreamingExporter().WriteAsync(
                destination,
                Request(
                    schema,
                    GeneratedRows(
                        rowCount,
                        Cancellation),
                    framing),
                Cancellation);

        Assert.Equal(rowCount, first.Manifest.Content.RowCount);
        Assert.Equal(
            destination.Length,
            first.Manifest.Content.DataByteLength);
        Assert.InRange(
            destination.MaximumWriteSize,
            1,
            16 * 1024);

        await using var replay = new MemoryStream();
        JsonStreamingExportResult second =
            await new JsonStreamingExporter().WriteAsync(
                replay,
                Request(
                    schema,
                    GeneratedRows(
                        rowCount,
                        Cancellation),
                    framing),
                Cancellation);

        Assert.Equal(
            first.Manifest.Content.DataDigest,
            second.Manifest.Content.DataDigest);
        Assert.Equal(
            first.Manifest.Content.SourceLogicalDigest,
            second.Manifest.Content.SourceLogicalDigest);
        Assert.Equal(
            first.CanonicalManifestBytes,
            second.CanonicalManifestBytes);
        Assert.Equal(
            first.ManifestDigest,
            second.ManifestDigest);
    }

    [Fact]
    public async Task DestinationMustBeWritableSeekableEmptyAndAtZero()
    {
        TableSchema schema = Schema(
            "destination",
            Column("id", DbType.Integer));
        JsonStreamingExportRequest request = Request(
            schema,
            [],
            JsonExportFraming.RootArray);

        await using var nonempty =
            new MemoryStream([0x01], writable: true);
        nonempty.Position = 0;
        await Assert.ThrowsAsync<ArgumentException>(
            () => new JsonStreamingExporter()
                .WriteAsync(nonempty, request, Cancellation)
                .AsTask());

        await using var positioned = new MemoryStream();
        positioned.WriteByte(0x01);
        positioned.SetLength(0);
        positioned.Position = 1;
        Assert.Equal(0, positioned.Length);
        Assert.Equal(1, positioned.Position);
        await Assert.ThrowsAsync<ArgumentException>(
            () => new JsonStreamingExporter()
                .WriteAsync(positioned, request, Cancellation)
                .AsTask());

        await using var nonseekable =
            new NonSeekableWriteStream();
        await Assert.ThrowsAsync<ArgumentException>(
            () => new JsonStreamingExporter()
                .WriteAsync(nonseekable, request, Cancellation)
                .AsTask());
    }

    private static async Task<(
        JsonStreamingExportResult Result,
        byte[] Bytes)> ExportAsync(
        TableSchema schema,
        IReadOnlyList<JsonExportRow> rows,
        JsonExportFraming framing,
        long maxDataBytes = 1L << 30,
        int maximumDecodedBlobBytes =
            JsonExportContracts
                .MaximumSupportedDecodedBlobBytes)
    {
        await using var destination = new MemoryStream();
        JsonStreamingExportResult result =
            await new JsonStreamingExporter().WriteAsync(
                destination,
                Request(
                    schema,
                    rows,
                    framing,
                    maxDataBytes,
                    maximumDecodedBlobBytes),
                Cancellation);
        return (result, destination.ToArray());
    }

    private static JsonStreamingExportRequest Request(
        TableSchema schema,
        IReadOnlyList<JsonExportRow> rows,
        JsonExportFraming framing,
        long maxDataBytes = 1L << 30,
        int maximumDecodedBlobBytes =
            JsonExportContracts
                .MaximumSupportedDecodedBlobBytes) =>
        Request(
            schema,
            Rows(rows),
            framing,
            maxDataBytes,
            maximumDecodedBlobBytes);

    private static JsonStreamingExportRequest Request(
        TableSchema schema,
        IAsyncEnumerable<JsonExportRow> rows,
        JsonExportFraming framing,
        long maxDataBytes = 1L << 30,
        int maximumDecodedBlobBytes =
            JsonExportContracts
                .MaximumSupportedDecodedBlobBytes) =>
        new()
        {
            Profile = JsonExportProfile.LosslessV1,
            Framing = framing,
            Source = new JsonExportSourceManifest
            {
                Kind = JsonExportContracts.SourceKind,
                Version = "4.3.0",
                SnapshotByteLength = 4_096,
                SnapshotDigest = Hash('a'),
            },
            Table = schema,
            Rows = rows,
            MaxDataBytes = maxDataBytes,
            MaximumDecodedBlobBytes =
                maximumDecodedBlobBytes,
        };

    private static async IAsyncEnumerable<JsonExportRow> Rows(
        IReadOnlyList<JsonExportRow> rows)
    {
        foreach (JsonExportRow row in rows)
        {
            yield return row;
            await Task.Yield();
        }
    }

    private static async IAsyncEnumerable<JsonExportRow>
        GeneratedRows(
        int count,
        [EnumeratorCancellation]
        CancellationToken cancellationToken = default)
    {
        for (int index = 0; index < count; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return Row(
                index,
                DbValue.FromInteger(index),
                DbValue.FromText(
                    index.ToString(
                        CultureInfo.InvariantCulture)));
            if ((index & 1023) == 0)
                await Task.Yield();
        }
    }

    private static async IAsyncEnumerable<JsonExportRow>
        RowThenCancel(
        CancellationTokenSource cancellation,
        [EnumeratorCancellation]
        CancellationToken cancellationToken = default)
    {
        yield return Row(
            1,
            DbValue.FromInteger(1));
        cancellation.Cancel();
        await Task.Yield();
        cancellationToken.ThrowIfCancellationRequested();
    }

    private static async Task<List<JsonLogicalRecord>> ReadAsync(
        byte[] bytes,
        JsonExportFraming framing)
    {
        await using var stream =
            new MemoryStream(bytes, writable: false);
        await using JsonStreamingReader reader =
            await JsonStreamingReader.OpenAsync(
                stream,
                new JsonStreamingReaderOptions
                {
                    Framing =
                        framing ==
                        JsonExportFraming.RootArray
                            ? JsonInputFraming.RootArray
                            : JsonInputFraming
                                .MultipleValues,
                    MaxValueBytes =
                        JsonInputContracts.MaximumValueBytes,
                    MaxStringBytes =
                        JsonInputContracts.MaximumStringBytes,
                    LeaveOpen = true,
                },
                Cancellation);
        var records = new List<JsonLogicalRecord>();
        await foreach (JsonLogicalRecord record in
                       reader.ReadValuesAsync(Cancellation))
        {
            records.Add(record);
        }

        return records;
    }

    private static JsonLogicalValue Cell(
        JsonLogicalRecord record) =>
        Assert.Single(record.Value.Properties).Value;

    private static TableSchema Schema(
        string name,
        params ColumnDefinition[] columns) =>
        new()
        {
            TableName = name,
            Columns = columns,
        };

    private static ColumnDefinition Column(
        string name,
        DbType type,
        bool nullable = true) =>
        new()
        {
            Name = name,
            Type = type,
            Nullable = nullable,
        };

    private static JsonExportRow Row(
        long rowId,
        params DbValue[] values) =>
        new(rowId, values);

    private static JsonExportHashManifest Hash(char character) =>
        new()
        {
            Algorithm =
                JsonExportHashManifest.Sha256Algorithm,
            Value = new string(character, 64),
        };

    private static string PhysicalDigest(
        ReadOnlySpan<byte> bytes) =>
        Convert.ToHexString(SHA256.HashData(bytes))
            .ToLowerInvariant();

    private static JsonExportHashManifest OrderedDigest(
        params CanonicalValue[][] rows)
    {
        using var digest =
            new JsonExportOrderedContentDigest();
        foreach (CanonicalValue[] row in rows)
            digest.AppendRow(row);
        return digest.Complete();
    }

    private sealed class ChunkTrackingStream : MemoryStream
    {
        public int MaximumWriteSize { get; private set; }

        public bool FlushObserved { get; private set; }

        public override void Write(
            byte[] buffer,
            int offset,
            int count)
        {
            MaximumWriteSize =
                Math.Max(MaximumWriteSize, count);
            base.Write(buffer, offset, count);
        }

        public override void Write(
            ReadOnlySpan<byte> buffer)
        {
            MaximumWriteSize =
                Math.Max(
                    MaximumWriteSize,
                    buffer.Length);
            base.Write(buffer);
        }

        public override Task WriteAsync(
            byte[] buffer,
            int offset,
            int count,
            CancellationToken cancellationToken)
        {
            MaximumWriteSize =
                Math.Max(MaximumWriteSize, count);
            return base.WriteAsync(
                buffer,
                offset,
                count,
                cancellationToken);
        }

        public override ValueTask WriteAsync(
            ReadOnlyMemory<byte> buffer,
            CancellationToken cancellationToken = default)
        {
            MaximumWriteSize =
                Math.Max(
                    MaximumWriteSize,
                    buffer.Length);
            return base.WriteAsync(
                buffer,
                cancellationToken);
        }

        public override void Flush()
        {
            FlushObserved = true;
            base.Flush();
        }

        public override Task FlushAsync(
            CancellationToken cancellationToken)
        {
            FlushObserved = true;
            return base.FlushAsync(cancellationToken);
        }
    }

    private sealed class NonSeekableWriteStream : Stream
    {
        public override bool CanRead => false;

        public override bool CanSeek => false;

        public override bool CanWrite => true;

        public override long Length =>
            throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override void Flush()
        {
        }

        public override int Read(
            byte[] buffer,
            int offset,
            int count) =>
            throw new NotSupportedException();

        public override long Seek(
            long offset,
            SeekOrigin origin) =>
            throw new NotSupportedException();

        public override void SetLength(long value) =>
            throw new NotSupportedException();

        public override void Write(
            byte[] buffer,
            int offset,
            int count)
        {
        }
    }
}
