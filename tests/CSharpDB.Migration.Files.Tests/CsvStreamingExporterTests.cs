using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Migration.Canonicalization;
using CSharpDB.Migration.Files.Csv;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Files.Tests;

public sealed class CsvStreamingExporterTests
{
    private static readonly CancellationToken Cancellation =
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task LosslessMixedValues_HaveExactBytes_ReaderRoundTrip_AndProofs()
    {
        TableSchema schema = Schema(
            "mixed",
            Column("id", DbType.Integer, nullable: false),
            Column("note", DbType.Text),
            Column("amount", DbType.Real),
            Column("payload", DbType.Blob));
        CsvExportRow[] rows =
        [
            Row(
                -2,
                DbValue.FromInteger(7),
                DbValue.FromText("plain"),
                DbValue.FromReal(1.5),
                DbValue.FromBlob([0x00, 0x01, 0x02])),
            Row(
                4,
                DbValue.FromInteger(9),
                DbValue.FromText("a,\"b\"\r\nc"),
                DbValue.Null,
                DbValue.FromBlob([])),
            Row(
                7,
                DbValue.FromInteger(12),
                DbValue.FromText("\\N"),
                DbValue.FromReal(BitConverter.Int64BitsToDouble(
                    unchecked((long)0x8000000000000000))),
                DbValue.Null),
        ];
        const string expected =
            "id,note,amount,payload\r\n" +
            "7,plain,1.5,AAEC\r\n" +
            "9,\"a,\"\"b\"\"\r\nc\",\\N,\r\n" +
            "12,\"\\N\",-0,\\N\r\n";

        (CsvStreamingExportResult result, byte[] bytes) =
            await ExportAsync(schema, rows);

        Assert.Equal(expected, Encoding.UTF8.GetString(bytes));
        Assert.False(bytes.AsSpan().StartsWith(Encoding.UTF8.Preamble));
        Assert.Equal(bytes.LongLength, result.Manifest.Content.DataByteLength);
        Assert.Equal(3, result.Manifest.Content.RowCount);
        Assert.Equal(Sha256(bytes), result.Manifest.Content.DataDigest.Value);
        Assert.Equal(
            CsvExportManifestSerializer.Serialize(result.Manifest),
            result.CanonicalManifestBytes);
        Assert.Equal(
            CsvExportManifestSerializer.ComputeManifestDigest(result.Manifest),
            result.ManifestDigest);

        CsvExportHashManifest expectedLogical = OrderedDigest(
            [
                CanonicalValue.Int64(7),
                CanonicalValue.Text("plain"),
                CanonicalValue.Binary64(1.5),
                CanonicalValue.Blob(new byte[] { 0x00, 0x01, 0x02 }),
            ],
            [
                CanonicalValue.Int64(9),
                CanonicalValue.Text("a,\"b\"\r\nc"),
                CanonicalValue.Null(CanonicalType.Binary64),
                CanonicalValue.Blob(ReadOnlyMemory<byte>.Empty),
            ],
            [
                CanonicalValue.Int64(12),
                CanonicalValue.Text("\\N"),
                CanonicalValue.Binary64(-0D),
                CanonicalValue.Null(CanonicalType.Blob),
            ]);
        Assert.Equal(
            expectedLogical.Value,
            result.Manifest.Content.SourceLogicalDigest.Value);
        Assert.Equal(
            expectedLogical.Value,
            result.Manifest.Content.ExportedLogicalDigest.Value);

        (CsvHeader header, List<CsvLogicalRecord> parsed) = await ReadAsync(bytes);
        Assert.Equal(["id", "note", "amount", "payload"], header.Fields);
        Assert.Equal(3, parsed.Count);
        Assert.Equal("plain", parsed[0].Fields[1].Value);
        Assert.Equal("AAEC", parsed[0].Fields[3].Value);
        Assert.Equal("a,\"b\"\r\nc", parsed[1].Fields[1].Value);
        Assert.Equal(CsvFieldKind.Null, parsed[1].Fields[2].Kind);
        Assert.Equal(CsvFieldKind.Empty, parsed[1].Fields[3].Kind);
        Assert.Equal("\\N", parsed[2].Fields[1].Value);
        Assert.True(parsed[2].Fields[1].WasQuoted);
        Assert.Equal(CsvFieldKind.Null, parsed[2].Fields[3].Kind);
        Assert.Equal(
            unchecked((long)0x8000000000000000),
            BitConverter.DoubleToInt64Bits(double.Parse(
                parsed[2].Fields[2].Value!,
                NumberStyles.Float,
                CultureInfo.InvariantCulture)));
    }

    [Fact]
    public async Task EmptyTable_WritesOnlyHeaderAndEmptyLogicalVector()
    {
        TableSchema schema = Schema(
            "empty",
            Column("id", DbType.Integer, nullable: false));
        await using var destination = new MemoryStream();

        CsvStreamingExportResult result = await new CsvStreamingExporter().WriteAsync(
            destination,
            Request(schema, []),
            Cancellation);

        Assert.Equal("id\r\n", Encoding.UTF8.GetString(destination.ToArray()));
        Assert.Equal(0, result.Manifest.Content.RowCount);
        Assert.Equal(4, result.Manifest.Content.DataByteLength);
        Assert.Equal(
            "879a6d96f9dbe682b05f572f0f462ca37a21893f7aa626a93ab8f06acea14550",
            result.Manifest.Content.SourceLogicalDigest.Value);
        Assert.Equal(
            result.Manifest.Content.SourceLogicalDigest.Value,
            result.Manifest.Content.ExportedLogicalDigest.Value);
        Assert.True(destination.CanWrite);
    }

    [Fact]
    public async Task SignedGappedRowIds_AreOrderingOnly_AndDuplicateValuesRemainDuplicated()
    {
        TableSchema schema = Schema("ordered", Column("value", DbType.Text));
        CsvExportRow[] rows =
        [
            Row(-10, DbValue.FromText("same")),
            Row(-1, DbValue.FromText("same")),
            Row(15, DbValue.FromText("last")),
        ];

        (CsvStreamingExportResult result, byte[] bytes) =
            await ExportAsync(schema, rows);

        Assert.Equal("value\r\nsame\r\nsame\r\nlast\r\n", Encoding.UTF8.GetString(bytes));
        Assert.Equal(3, result.Manifest.Content.RowCount);
        Assert.Equal(
            OrderedDigest(
                [CanonicalValue.Text("same")],
                [CanonicalValue.Text("same")],
                [CanonicalValue.Text("last")]).Value,
            result.Manifest.Content.SourceLogicalDigest.Value);
    }

    [Theory]
    [InlineData(5, 5)]
    [InlineData(5, 4)]
    public async Task DuplicateOrDecreasingRowId_IsRejected(long first, long second)
    {
        TableSchema schema = Schema("ordered", Column("value", DbType.Integer));
        await using var destination = new MemoryStream();
        CsvStreamingExportRequest request = Request(
            schema,
            [
                Row(first, DbValue.FromInteger(1)),
                Row(second, DbValue.FromInteger(2)),
            ]);

        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(
            () => new CsvStreamingExporter()
                .WriteAsync(destination, request, Cancellation)
                .AsTask());

        Assert.Contains("strictly increasing", error.Message, StringComparison.Ordinal);
        Assert.Equal("value\r\n1\r\n", Encoding.UTF8.GetString(destination.ToArray()));
    }

    [Fact]
    public async Task IntegerAndFiniteRealLexicals_AreInvariantAndPreserveNegativeZero()
    {
        TableSchema schema = Schema(
            "numbers",
            Column("integer_value", DbType.Integer, nullable: false),
            Column("real_value", DbType.Real, nullable: false));
        double negativeZero = BitConverter.Int64BitsToDouble(
            unchecked((long)0x8000000000000000));
        double smallestNormal = BitConverter.Int64BitsToDouble(0x0010000000000000);
        CsvExportRow[] rows =
        [
            Row(1, DbValue.FromInteger(long.MinValue), DbValue.FromReal(negativeZero)),
            Row(2, DbValue.FromInteger(long.MaxValue), DbValue.FromReal(double.Epsilon)),
            Row(3, DbValue.FromInteger(0), DbValue.FromReal(double.MaxValue)),
            Row(4, DbValue.FromInteger(-1), DbValue.FromReal(smallestNormal)),
        ];
        const string expected =
            "integer_value,real_value\r\n" +
            "-9223372036854775808,-0\r\n" +
            "9223372036854775807,5E-324\r\n" +
            "0,1.7976931348623157E+308\r\n" +
            "-1,2.2250738585072014E-308\r\n";
        CultureInfo original = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("ar-SA");
            (CsvStreamingExportResult _, byte[] bytes) =
                await ExportAsync(schema, rows);
            Assert.Equal(expected, Encoding.UTF8.GetString(bytes));

            string negativeZeroText = expected.Split("\r\n")[1].Split(',')[1];
            Assert.Equal(
                BitConverter.DoubleToInt64Bits(negativeZero),
                BitConverter.DoubleToInt64Bits(double.Parse(
                    negativeZeroText,
                    NumberStyles.Float,
                    CultureInfo.InvariantCulture)));
        }
        finally
        {
            CultureInfo.CurrentCulture = original;
        }
    }

    [Theory]
    [InlineData(double.NaN)]
    [InlineData(double.PositiveInfinity)]
    [InlineData(double.NegativeInfinity)]
    public async Task NonFiniteReal_IsRejectedBeforeItsRecord(double value)
    {
        TableSchema schema = Schema(
            "numbers",
            Column("value", DbType.Real, nullable: false));
        await using var destination = new MemoryStream();

        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(
            () => new CsvStreamingExporter().WriteAsync(
                    destination,
                    Request(schema, [Row(1, DbValue.FromReal(value))]),
                    Cancellation)
                .AsTask());

        Assert.Contains("non-finite", error.Message, StringComparison.Ordinal);
        Assert.Equal("value\r\n", Encoding.UTF8.GetString(destination.ToArray()));
    }

    [Fact]
    public async Task NullEmptyLiteralNullTokenQuotesAndMultiline_AreDistinct()
    {
        TableSchema schema = Schema("text", Column("value", DbType.Text));
        CsvExportRow[] rows =
        [
            Row(1, DbValue.Null),
            Row(2, DbValue.FromText(string.Empty)),
            Row(3, DbValue.FromText("\\N")),
            Row(4, DbValue.FromText("a,\"b\"\r\nc")),
        ];
        const string expected =
            "value\r\n" +
            "\\N\r\n" +
            "\r\n" +
            "\"\\N\"\r\n" +
            "\"a,\"\"b\"\"\r\nc\"\r\n";

        (CsvStreamingExportResult _, byte[] bytes) =
            await ExportAsync(schema, rows);

        Assert.Equal(expected, Encoding.UTF8.GetString(bytes));
        (_, List<CsvLogicalRecord> parsed) = await ReadAsync(bytes);
        Assert.Equal(CsvFieldKind.Null, parsed[0].Fields[0].Kind);
        Assert.Equal(CsvFieldKind.Empty, parsed[1].Fields[0].Kind);
        Assert.Equal(CsvFieldKind.Text, parsed[2].Fields[0].Kind);
        Assert.Equal("\\N", parsed[2].Fields[0].Value);
        Assert.True(parsed[2].Fields[0].WasQuoted);
        Assert.Equal("a,\"b\"\r\nc", parsed[3].Fields[0].Value);
    }

    [Fact]
    public async Task InvalidUtf16Text_IsRejectedBeforeItsRecord()
    {
        TableSchema schema = Schema("text", Column("value", DbType.Text));
        await using var destination = new MemoryStream();

        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(
            () => new CsvStreamingExporter().WriteAsync(
                    destination,
                    Request(schema, [Row(1, DbValue.FromText("invalid-\ud800"))]),
                    Cancellation)
                .AsTask());

        Assert.Contains("UTF-16", error.Message, StringComparison.Ordinal);
        Assert.Equal("value\r\n", Encoding.UTF8.GetString(destination.ToArray()));
    }

    [Fact]
    public async Task StrictUtf8Text_PreservesNulAndSupplementaryCharactersAcrossChunks()
    {
        TableSchema schema = Schema("text", Column("value", DbType.Text));
        string text = new string('a', 4095) + "\U0001F642\0end";

        (CsvStreamingExportResult _, byte[] bytes) =
            await ExportAsync(schema, [Row(1, DbValue.FromText(text))]);

        Assert.Equal("value\r\n" + text + "\r\n", Encoding.UTF8.GetString(bytes));
        (_, List<CsvLogicalRecord> parsed) = await ReadAsync(bytes);
        Assert.Equal(text, Assert.Single(parsed).Fields[0].Value);
    }

    [Fact]
    public async Task BlobPaddingAndPerColumnBound_AreEnforced()
    {
        TableSchema schema = Schema("blobs", Column("payload", DbType.Blob));
        CsvExportRow[] rows =
        [
            Row(1, DbValue.FromBlob([])),
            Row(2, DbValue.FromBlob([0x00])),
            Row(3, DbValue.FromBlob([0x00, 0x01])),
            Row(4, DbValue.FromBlob([0x00, 0x01, 0x02])),
        ];

        (CsvStreamingExportResult _, byte[] bytes) =
            await ExportAsync(schema, rows, maximumDecodedBlobBytes: 3);

        Assert.Equal(
            "payload\r\n\r\nAA==\r\nAAE=\r\nAAEC\r\n",
            Encoding.UTF8.GetString(bytes));

        await using var rejectedDestination = new MemoryStream();
        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(
            () => new CsvStreamingExporter().WriteAsync(
                    rejectedDestination,
                    Request(
                        schema,
                        [Row(1, DbValue.FromBlob([0x00, 0x01, 0x02]))],
                        maximumDecodedBlobBytes: 2),
                    Cancellation)
                .AsTask());
        Assert.Contains("2-byte", error.Message, StringComparison.Ordinal);
        Assert.Equal("payload\r\n", Encoding.UTF8.GetString(rejectedDestination.ToArray()));
    }

    [Fact]
    public async Task BlobInput_IsSnapshottedBeforeEmissionAndLogicalHashing()
    {
        TableSchema schema = Schema(
            "blob-snapshot",
            Column("payload", DbType.Blob, nullable: false));
        byte[] sourceBlob = [0x01, 0x02, 0x03];
        await using var destination = new MutateBlobOnThirdWriteStream(sourceBlob);

        CsvStreamingExportResult result = await new CsvStreamingExporter().WriteAsync(
            destination,
            Request(
                schema,
                [Row(1, DbValue.FromBlob(sourceBlob))],
                maximumDecodedBlobBytes: sourceBlob.Length),
            Cancellation);

        Assert.Equal("payload\r\nAQID\r\n", Encoding.UTF8.GetString(destination.ToArray()));
        Assert.Equal(
            OrderedDigest([CanonicalValue.Blob(new byte[] { 0x01, 0x02, 0x03 })]).Value,
            result.Manifest.Content.SourceLogicalDigest.Value);
        Assert.Equal(
            result.Manifest.Content.SourceLogicalDigest.Value,
            result.Manifest.Content.ExportedLogicalDigest.Value);
        Assert.Equal(new byte[] { 0xff, 0xff, 0xff }, sourceBlob);
    }

    [Fact]
    public async Task SpreadsheetProfile_TransformsHeadersAndTextCells_WithExactEvidence()
    {
        TableSchema schema = Schema(
            "spreadsheet",
            Column("=name", DbType.Text),
            Column("amount", DbType.Integer, nullable: false),
            Column("note", DbType.Text));
        CsvExportRow[] rows =
        [
            Row(
                1,
                DbValue.FromText("=SUM(A1:A2)"),
                DbValue.FromInteger(-1),
                DbValue.FromText("ordinary")),
            Row(
                2,
                DbValue.FromText("+value"),
                DbValue.FromInteger(2),
                DbValue.FromText("@other")),
            Row(
                3,
                DbValue.FromText("'already"),
                DbValue.FromInteger(3),
                DbValue.FromText(string.Empty)),
        ];

        (CsvStreamingExportResult result, byte[] bytes) = await ExportAsync(
            schema,
            rows,
            CsvExportProfile.SpreadsheetSafeLossyV1);

        Assert.Equal(
            "'=name,amount,note\r\n" +
            "'=SUM(A1:A2),-1,ordinary\r\n" +
            "'+value,2,'@other\r\n" +
            "'already,3,\r\n",
            Encoding.UTF8.GetString(bytes));
        CsvExportLossyTransformManifest loss = Assert.IsType<CsvExportLossyTransformManifest>(
            result.Manifest.LossyTransform);
        Assert.Equal(1, loss.TransformedHeaderCount);
        Assert.Equal(2, loss.TransformedRowCount);
        Assert.Equal(3, loss.TransformedCellCount);
        Assert.NotEqual(
            result.Manifest.Content.SourceLogicalDigest.Value,
            result.Manifest.Content.ExportedLogicalDigest.Value);
        Assert.Equal(
            OrderedDigest(
                [
                    CanonicalValue.Text("'=SUM(A1:A2)"),
                    CanonicalValue.Int64(-1),
                    CanonicalValue.Text("ordinary"),
                ],
                [
                    CanonicalValue.Text("'+value"),
                    CanonicalValue.Int64(2),
                    CanonicalValue.Text("'@other"),
                ],
                [
                    CanonicalValue.Text("'already"),
                    CanonicalValue.Int64(3),
                    CanonicalValue.Text(string.Empty),
                ]).Value,
            result.Manifest.Content.ExportedLogicalDigest.Value);
    }

    [Fact]
    public async Task SpreadsheetHeaderOnlyTransform_KeepsLogicalDigestsEqual()
    {
        TableSchema schema = Schema("spreadsheet", Column("@header", DbType.Text));

        (CsvStreamingExportResult result, byte[] bytes) = await ExportAsync(
            schema,
            [],
            CsvExportProfile.SpreadsheetSafeLossyV1);

        Assert.Equal("'@header\r\n", Encoding.UTF8.GetString(bytes));
        CsvExportLossyTransformManifest loss = Assert.IsType<CsvExportLossyTransformManifest>(
            result.Manifest.LossyTransform);
        Assert.Equal(1, loss.TransformedHeaderCount);
        Assert.Equal(0, loss.TransformedRowCount);
        Assert.Equal(0, loss.TransformedCellCount);
        Assert.Equal(
            result.Manifest.Content.SourceLogicalDigest.Value,
            result.Manifest.Content.ExportedLogicalDigest.Value);
    }

    [Fact]
    public async Task SpreadsheetBlob_IsRejectedBeforeAnyBytes()
    {
        TableSchema schema = Schema("spreadsheet", Column("payload", DbType.Blob));
        await using var destination = new MemoryStream();

        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(
            () => new CsvStreamingExporter().WriteAsync(
                    destination,
                    Request(
                        schema,
                        [],
                        CsvExportProfile.SpreadsheetSafeLossyV1),
                    Cancellation)
                .AsTask());

        Assert.Contains("BLOB", error.Message, StringComparison.Ordinal);
        Assert.Empty(destination.ToArray());
    }

    [Fact]
    public async Task SourceAndTransformedHeaderCollisions_AreRejectedBeforeAnyBytes()
    {
        (TableSchema Schema, CsvExportProfile Profile)[] cases =
        [
            (
                Schema(
                    "duplicate-source",
                    Column("Name", DbType.Text),
                    Column("name", DbType.Text)),
                CsvExportProfile.LosslessV1),
            (
                Schema(
                    "transformed-collision",
                    Column("=name", DbType.Text),
                    Column("'=name", DbType.Text)),
                CsvExportProfile.SpreadsheetSafeLossyV1),
        ];

        foreach ((TableSchema schema, CsvExportProfile profile) in cases)
        {
            await using var destination = new MemoryStream();
            await Assert.ThrowsAsync<InvalidDataException>(
                () => new CsvStreamingExporter().WriteAsync(
                        destination,
                        Request(schema, [], profile),
                        Cancellation)
                    .AsTask());
            Assert.Empty(destination.ToArray());
        }
    }

    [Fact]
    public async Task RowWidthRuntimeTypeAndNullability_AreRejectedBeforeRecordBytes()
    {
        TableSchema schema = Schema(
            "shape",
            Column("id", DbType.Integer, nullable: false),
            Column("text", DbType.Text, nullable: false));
        CsvExportRow[] invalidRows =
        [
            Row(1, DbValue.FromInteger(1)),
            Row(1, DbValue.FromText("wrong"), DbValue.FromText("value")),
            Row(1, DbValue.FromInteger(1), DbValue.Null),
        ];

        foreach (CsvExportRow invalid in invalidRows)
        {
            await using var destination = new MemoryStream();
            await Assert.ThrowsAsync<InvalidDataException>(
                () => new CsvStreamingExporter().WriteAsync(
                        destination,
                        Request(schema, [invalid]),
                        Cancellation)
                    .AsTask());
            Assert.Equal("id,text\r\n", Encoding.UTF8.GetString(destination.ToArray()));
        }
    }

    [Fact]
    public async Task PreCanceledExport_WritesZeroBytes_AndLeavesDestinationOpen()
    {
        TableSchema schema = Schema("cancel", Column("id", DbType.Integer));
        await using var destination = new MemoryStream();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => new CsvStreamingExporter().WriteAsync(
                    destination,
                    Request(schema, [Row(1, DbValue.FromInteger(1))]),
                    cancellation.Token)
                .AsTask());

        Assert.Equal(0, destination.Length);
        destination.WriteByte(0x7f);
        Assert.Equal(1, destination.Length);
    }

    [Fact]
    public async Task MaximumDataBytes_IsCheckedBeforeHeaderAndWholeRow()
    {
        TableSchema schema = Schema("limit", Column("id", DbType.Integer));

        await using (var tooSmallForHeader = new MemoryStream())
        {
            await Assert.ThrowsAsync<InvalidDataException>(
                () => new CsvStreamingExporter().WriteAsync(
                        tooSmallForHeader,
                        Request(schema, [], maxDataBytes: 3),
                        Cancellation)
                    .AsTask());
            Assert.Equal(0, tooSmallForHeader.Length);
        }

        await using (var exactHeader = new MemoryStream())
        {
            CsvStreamingExportResult result = await new CsvStreamingExporter().WriteAsync(
                exactHeader,
                Request(schema, [], maxDataBytes: 4),
                Cancellation);
            Assert.Equal(4, result.Manifest.Content.DataByteLength);
        }

        await using (var tooSmallForRow = new MemoryStream())
        {
            await Assert.ThrowsAsync<InvalidDataException>(
                () => new CsvStreamingExporter().WriteAsync(
                        tooSmallForRow,
                        Request(
                            schema,
                            [Row(1, DbValue.FromInteger(1))],
                            maxDataBytes: 6),
                        Cancellation)
                    .AsTask());
            Assert.Equal("id\r\n", Encoding.UTF8.GetString(tooSmallForRow.ToArray()));
        }
    }

    [Fact]
    public async Task ReaderFieldAndRecordCeilings_AreEnforcedBeforeRecordBytes()
    {
        string maximumField = new(
            'x',
            CsvReaderOptions.MaximumSupportedFieldCharacters);
        TableSchema recordSchema = Schema(
            "record-limit",
            Column("a", DbType.Text),
            Column("b", DbType.Text),
            Column("c", DbType.Text),
            Column("d", DbType.Text));
        await using (var recordDestination = new MemoryStream())
        {
            await Assert.ThrowsAsync<InvalidDataException>(
                () => new CsvStreamingExporter().WriteAsync(
                        recordDestination,
                        Request(
                            recordSchema,
                            [
                                Row(
                                    1,
                                    DbValue.FromText(maximumField),
                                    DbValue.FromText(maximumField),
                                    DbValue.FromText(maximumField),
                                    DbValue.FromText(maximumField)),
                            ]),
                        Cancellation)
                    .AsTask());
            Assert.Equal(
                "a,b,c,d\r\n",
                Encoding.UTF8.GetString(recordDestination.ToArray()));
        }

        string oversizedField = maximumField + "x";
        TableSchema fieldSchema = Schema("field-limit", Column("value", DbType.Text));
        await using var fieldDestination = new MemoryStream();
        await Assert.ThrowsAsync<InvalidDataException>(
            () => new CsvStreamingExporter().WriteAsync(
                    fieldDestination,
                    Request(
                        fieldSchema,
                        [Row(1, DbValue.FromText(oversizedField))]),
                    Cancellation)
                .AsTask());
        Assert.Equal(
            "value\r\n",
            Encoding.UTF8.GetString(fieldDestination.ToArray()));
    }

    [Fact]
    public async Task TooManyColumns_AreRejectedBeforeAnyBytes()
    {
        ColumnDefinition[] columns = Enumerable
            .Range(0, CsvReaderOptions.MaximumSupportedFieldsPerRecord + 1)
            .Select(static index => Column($"c{index}", DbType.Integer))
            .ToArray();
        TableSchema schema = Schema("wide", columns);
        await using var destination = new MemoryStream();

        await Assert.ThrowsAsync<InvalidDataException>(
            () => new CsvStreamingExporter().WriteAsync(
                    destination,
                    Request(schema, []),
                    Cancellation)
                .AsTask());

        Assert.Empty(destination.ToArray());
    }

    [Fact]
    public async Task CancellationAfterSuccessfulFinalFlush_DoesNotReplaceSuccess()
    {
        TableSchema schema = Schema("late-cancel", Column("id", DbType.Integer));
        using var cancellation = new CancellationTokenSource();
        await using var destination = new CancelAfterFlushStream(cancellation);

        CsvStreamingExportResult result = await new CsvStreamingExporter().WriteAsync(
            destination,
            Request(schema, [Row(1, DbValue.FromInteger(1))]),
            cancellation.Token);

        Assert.True(cancellation.IsCancellationRequested);
        Assert.Equal(1, result.Manifest.Content.RowCount);
        Assert.Equal("id\r\n1\r\n", Encoding.UTF8.GetString(destination.ToArray()));
    }

    [Fact]
    public async Task LargeTextAndBlob_AreWrittenInBoundedChunks_AndDestinationStaysOpen()
    {
        TableSchema schema = Schema(
            "large",
            Column("text", DbType.Text, nullable: false),
            Column("blob", DbType.Blob, nullable: false));
        string text = new('é', 100_000);
        byte[] blob = Enumerable.Range(0, 100_000)
            .Select(static value => unchecked((byte)value))
            .ToArray();
        await using var destination = new ChunkTrackingStream();

        CsvStreamingExportResult result = await new CsvStreamingExporter().WriteAsync(
            destination,
            Request(
                schema,
                [Row(1, DbValue.FromText(text), DbValue.FromBlob(blob))],
                maximumDecodedBlobBytes: blob.Length),
            Cancellation);

        Assert.Equal(1, result.Manifest.Content.RowCount);
        Assert.InRange(destination.MaximumWriteSize, 1, 16 * 1024);
        Assert.True(destination.FlushObserved);
        long completedLength = destination.Length;
        destination.WriteByte(0x7f);
        Assert.Equal(completedLength + 1, destination.Length);
    }

    private static async Task<(CsvStreamingExportResult Result, byte[] Bytes)> ExportAsync(
        TableSchema schema,
        IReadOnlyList<CsvExportRow> rows,
        CsvExportProfile profile = CsvExportProfile.LosslessV1,
        int maximumDecodedBlobBytes =
            CsvExportContracts.MaximumSupportedDecodedBlobBytes)
    {
        await using var destination = new MemoryStream();
        CsvStreamingExportResult result = await new CsvStreamingExporter().WriteAsync(
            destination,
            Request(
                schema,
                rows,
                profile,
                maximumDecodedBlobBytes: maximumDecodedBlobBytes),
            Cancellation);
        return (result, destination.ToArray());
    }

    private static CsvStreamingExportRequest Request(
        TableSchema schema,
        IReadOnlyList<CsvExportRow> rows,
        CsvExportProfile profile = CsvExportProfile.LosslessV1,
        long maxDataBytes = 1L << 30,
        int maximumDecodedBlobBytes =
            CsvExportContracts.MaximumSupportedDecodedBlobBytes) => new()
            {
                Profile = profile,
                Source = new CsvExportSourceManifest
                {
                    Kind = CsvExportContracts.SourceKind,
                    Version = "4.3.0",
                    SnapshotByteLength = 4096,
                    SnapshotDigest = Hash('a'),
                },
                Table = schema,
                Rows = Rows(rows),
                MaxDataBytes = maxDataBytes,
                MaximumDecodedBlobBytes = maximumDecodedBlobBytes,
            };

    private static async IAsyncEnumerable<CsvExportRow> Rows(
        IReadOnlyList<CsvExportRow> rows)
    {
        foreach (CsvExportRow row in rows)
        {
            yield return row;
            await Task.Yield();
        }
    }

    private static CsvExportRow Row(long rowId, params DbValue[] values) =>
        new(rowId, values);

    private static TableSchema Schema(
        string tableName,
        params ColumnDefinition[] columns) => new()
        {
            TableName = tableName,
            Columns = columns,
        };

    private static ColumnDefinition Column(
        string name,
        DbType type,
        bool nullable = true) => new()
        {
            Name = name,
            Type = type,
            Nullable = nullable,
        };

    private static CsvExportHashManifest OrderedDigest(
        params CanonicalValue[][] rows)
    {
        using var digest = new CsvExportOrderedContentDigest();
        foreach (CanonicalValue[] row in rows)
            digest.AppendRow(row);
        return digest.Complete();
    }

    private static CsvExportHashManifest Hash(char value) => new()
    {
        Algorithm = CsvExportHashManifest.Sha256Algorithm,
        Value = new string(value, 64),
    };

    private static string Sha256(ReadOnlySpan<byte> bytes) =>
        Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();

    private static async Task<(CsvHeader Header, List<CsvLogicalRecord> Records)> ReadAsync(
        byte[] bytes)
    {
        await using var stream = new MemoryStream(bytes);
        await using CsvStreamingReader reader = await CsvStreamingReader.OpenAsync(
            stream,
            new CsvReaderOptions
            {
                NullToken = CsvExportContracts.NullToken,
                NullTokenMatchesQuotedFields = false,
            },
            Cancellation);
        var records = new List<CsvLogicalRecord>();
        await foreach (CsvLogicalRecord record in reader.ReadRecordsAsync(Cancellation))
            records.Add(record);
        return (Assert.IsType<CsvHeader>(reader.Header), records);
    }

    private sealed class ChunkTrackingStream : MemoryStream
    {
        public int MaximumWriteSize { get; private set; }

        public bool FlushObserved { get; private set; }

        public override ValueTask WriteAsync(
            ReadOnlyMemory<byte> buffer,
            CancellationToken cancellationToken = default)
        {
            MaximumWriteSize = Math.Max(MaximumWriteSize, buffer.Length);
            return base.WriteAsync(buffer, cancellationToken);
        }

        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            FlushObserved = true;
            return base.FlushAsync(cancellationToken);
        }
    }

    private sealed class CancelAfterFlushStream(
        CancellationTokenSource cancellation) : MemoryStream
    {
        public override async Task FlushAsync(CancellationToken cancellationToken)
        {
            await base.FlushAsync(cancellationToken);
            cancellation.Cancel();
        }
    }

    private sealed class MutateBlobOnThirdWriteStream(byte[] sourceBlob) : MemoryStream
    {
        private int writeCount;

        public override ValueTask WriteAsync(
            ReadOnlyMemory<byte> buffer,
            CancellationToken cancellationToken = default)
        {
            writeCount++;
            if (writeCount == 3)
                sourceBlob.AsSpan().Fill(0xff);
            return base.WriteAsync(buffer, cancellationToken);
        }
    }
}
