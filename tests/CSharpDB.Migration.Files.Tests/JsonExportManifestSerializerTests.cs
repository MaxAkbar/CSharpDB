using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CSharpDB.Migration.Canonicalization;
using CSharpDB.Migration.Files.Json;

namespace CSharpDB.Migration.Files.Tests;

public sealed class JsonExportManifestSerializerTests
{
    [Fact]
    public void LosslessManifestHasStableCanonicalBytesAndRoundTrips()
    {
        JsonExportManifest manifest = CreateManifest();

        byte[] first = JsonExportManifestSerializer.Serialize(manifest);
        byte[] repeated = JsonExportManifestSerializer.Serialize(manifest);
        JsonExportManifest reopened =
            JsonExportManifestSerializer.Deserialize(first);

        Assert.Equal(first, repeated);
        Assert.False(first.AsSpan().StartsWith(Encoding.UTF8.Preamble));
        Assert.Equal(
            JsonExportManifestSerializer.ComputeManifestDigest(manifest),
            JsonExportManifestSerializer.ComputeManifestDigest(reopened));
        Assert.Equal(JsonExportProfile.LosslessV1, reopened.Profile);
        Assert.Equal(JsonExportFraming.RootArray, reopened.Json.Framing);
        Assert.Equal("orders", reopened.Table.Name);
        Assert.Equal(
            ["id", "note", "payload"],
            reopened.Table.Columns.Select(
                static column => column.PropertyName));
        Assert.Equal(2, reopened.Content.RowCount);
    }

    [Fact]
    public void SchemaAndOrderedLogicalDigestsAreDeterministicAndOrderSensitive()
    {
        JsonExportColumnManifest[] columns = CreateColumns();

        JsonExportHashManifest first =
            JsonExportManifestSerializer.ComputeSchemaDigest(columns);
        JsonExportHashManifest repeated =
            JsonExportManifestSerializer.ComputeSchemaDigest(columns);
        JsonExportHashManifest reordered =
            JsonExportManifestSerializer.ComputeSchemaDigest(
            [
                Reordinal(columns[1], 0),
                Reordinal(columns[0], 1),
                columns[2],
            ]);

        Assert.Equal(
            JsonExportHashManifest.Sha256Algorithm,
            first.Algorithm);
        Assert.Equal(first, repeated);
        Assert.NotEqual(first.Value, reordered.Value);

        using var empty = new JsonExportOrderedContentDigest();
        JsonExportHashManifest emptyDigest = empty.Complete();

        using var rows = new JsonExportOrderedContentDigest();
        rows.AppendRow(
        [
            CanonicalValue.Int64(7),
            CanonicalValue.Text("alpha"),
        ]);
        rows.AppendRow(
        [
            CanonicalValue.Int64(7),
            CanonicalValue.Text("alpha"),
        ]);
        JsonExportHashManifest rowsDigest = rows.Complete();

        using var reversed = new JsonExportOrderedContentDigest();
        reversed.AppendRow(
        [
            CanonicalValue.Text("alpha"),
            CanonicalValue.Int64(7),
        ]);
        reversed.AppendRow(
        [
            CanonicalValue.Text("alpha"),
            CanonicalValue.Int64(7),
        ]);
        JsonExportHashManifest reversedDigest = reversed.Complete();

        Assert.Equal(2, rows.RowCount);
        Assert.NotEqual(emptyDigest.Value, rowsDigest.Value);
        Assert.NotEqual(rowsDigest.Value, reversedDigest.Value);
    }

    [Fact]
    public void OrderedLogicalDigestRejectsInvalidOrCompletedUse()
    {
        using var digest = new JsonExportOrderedContentDigest();

        Assert.Throws<ArgumentException>(
            () => digest.AppendRowHash(new byte[31]));
        _ = digest.GetCurrentPrefixDigest();
        _ = digest.Complete();
        Assert.Throws<InvalidOperationException>(
            () => digest.AppendRowHash(new byte[32]));
        Assert.Throws<InvalidOperationException>(
            () => digest.GetCurrentPrefixDigest());
        Assert.Throws<InvalidOperationException>(
            () => digest.Complete());
    }

    [Fact]
    public void TamperedCanonicalPayloadIsRejected()
    {
        string json = CanonicalText(CreateManifest());
        string tampered = json.Replace(
            "\"rowCount\":2",
            "\"rowCount\":3",
            StringComparison.Ordinal);

        InvalidDataException error =
            Assert.Throws<InvalidDataException>(
                () => JsonExportManifestSerializer.Deserialize(
                    Encoding.UTF8.GetBytes(tampered)));

        Assert.Contains(
            "digest",
            error.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void NoncanonicalBomWhitespaceAndPropertyOrderAreRejected()
    {
        byte[] canonical =
            JsonExportManifestSerializer.Serialize(CreateManifest());
        byte[] withBom = [.. Encoding.UTF8.Preamble, .. canonical];
        byte[] withWhitespace = [.. canonical, (byte)'\n'];
        byte[] reordered = ReorderEnvelope(canonical);

        InvalidDataException bomError =
            Assert.Throws<InvalidDataException>(
                () => JsonExportManifestSerializer.Deserialize(withBom));
        InvalidDataException whitespaceError =
            Assert.Throws<InvalidDataException>(
                () => JsonExportManifestSerializer.Deserialize(
                    withWhitespace));
        InvalidDataException orderError =
            Assert.Throws<InvalidDataException>(
                () => JsonExportManifestSerializer.Deserialize(reordered));

        Assert.Contains(
            "BOM",
            bomError.Message,
            StringComparison.OrdinalIgnoreCase);
        Assert.Contains(
            "canonical",
            whitespaceError.Message,
            StringComparison.OrdinalIgnoreCase);
        Assert.Contains(
            "canonical",
            orderError.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void DuplicateUnknownMisCasedNullAndNumericEnumMembersAreRejected()
    {
        string canonical = CanonicalText(CreateManifest());
        string duplicate = canonical.Replace(
            "{\"format\":",
            $"{{\"format\":\"{JsonExportContracts.ManifestFormat}\",\"format\":",
            StringComparison.Ordinal);
        string unknown = canonical.Replace(
            "\"profile\":",
            "\"unexpected\":true,\"profile\":",
            StringComparison.Ordinal);
        string misCased = canonical.Replace(
            "\"profile\":",
            "\"Profile\":",
            StringComparison.Ordinal);
        string requiredNull = canonical.Replace(
            $"\"kind\":\"{JsonExportContracts.SourceKind}\"",
            "\"kind\":null",
            StringComparison.Ordinal);
        string numericEnum = canonical.Replace(
            "\"framing\":\"rootArray\"",
            "\"framing\":0",
            StringComparison.Ordinal);

        Assert.Throws<InvalidDataException>(
            () => DeserializeText(duplicate));
        Assert.Throws<InvalidDataException>(
            () => DeserializeText(unknown));
        Assert.Throws<InvalidDataException>(
            () => DeserializeText(misCased));
        Assert.Throws<InvalidDataException>(
            () => DeserializeText(requiredNull));
        Assert.Throws<InvalidDataException>(
            () => DeserializeText(numericEnum));
    }

    [Fact]
    public void AttackerControlledManifestNamesAndValuesDoNotLeak()
    {
        const string marker = "LEAK_MARKER_7f3a";
        string canonical = CanonicalText(CreateManifest());
        string wrongFormat = canonical.Replace(
            JsonExportContracts.ManifestFormat,
            marker,
            StringComparison.Ordinal);
        string unknown = canonical.Replace(
            "\"profile\":",
            $"\"{marker}\":true,\"profile\":",
            StringComparison.Ordinal);
        string duplicate = canonical.Replace(
            "\"profile\":",
            $"\"{marker}\":true,\"{marker}\":false,\"profile\":",
            StringComparison.Ordinal);

        foreach (string candidate in
                 new[] { wrongFormat, unknown, duplicate })
        {
            InvalidDataException error =
                Assert.Throws<InvalidDataException>(
                    () => DeserializeText(candidate));
            Assert.DoesNotContain(
                marker,
                error.ToString(),
                StringComparison.Ordinal);
        }
    }

    [Fact]
    public void ManifestBytesAreCultureIndependent()
    {
        CultureInfo originalCulture = CultureInfo.CurrentCulture;
        CultureInfo originalUiCulture = CultureInfo.CurrentUICulture;
        try
        {
            CultureInfo.CurrentCulture =
                CultureInfo.GetCultureInfo("ar-SA");
            CultureInfo.CurrentUICulture =
                CultureInfo.GetCultureInfo("ar-SA");
            byte[] first =
                JsonExportManifestSerializer.Serialize(CreateManifest());

            CultureInfo.CurrentCulture =
                CultureInfo.GetCultureInfo("de-DE");
            CultureInfo.CurrentUICulture =
                CultureInfo.GetCultureInfo("de-DE");
            byte[] second =
                JsonExportManifestSerializer.Serialize(CreateManifest());

            Assert.Equal(first, second);
        }
        finally
        {
            CultureInfo.CurrentCulture = originalCulture;
            CultureInfo.CurrentUICulture = originalUiCulture;
        }
    }

    [Fact]
    public void HashesMustBeCanonicalAndLogicalProofsMustMatch()
    {
        JsonExportManifest manifest = CreateManifest();
        JsonExportManifest uppercaseHash = manifest with
        {
            Source = manifest.Source with
            {
                SnapshotDigest = manifest.Source.SnapshotDigest with
                {
                    Value = manifest.Source.SnapshotDigest.Value
                        .ToUpperInvariant(),
                },
            },
        };
        JsonExportManifest changedLogicalContent = manifest with
        {
            Content = manifest.Content with
            {
                ExportedLogicalDigest = Hash('f'),
            },
        };

        Assert.Throws<InvalidDataException>(
            () => JsonExportManifestSerializer.Serialize(
                uppercaseHash));
        Assert.Throws<InvalidDataException>(
            () => JsonExportManifestSerializer.Serialize(
                changedLogicalContent));
    }

    [Fact]
    public void SchemaDigestNamesOrdinalsTypesAndEncodingsAreCrossChecked()
    {
        JsonExportColumnManifest[] columns = CreateColumns();
        JsonExportManifest wrongDigest = CreateManifest() with
        {
            Table = CreateManifest().Table with
            {
                SchemaDigest = Hash('f'),
            },
        };
        JsonExportColumnManifest[] skippedOrdinal =
        [
            columns[0],
            Reordinal(columns[1], 2),
        ];
        JsonExportColumnManifest[] changedPropertyName =
        [
            columns[0] with { PropertyName = "different" },
            columns[1],
            columns[2],
        ];
        JsonExportColumnManifest[] wrongEncoding =
        [
            columns[0] with
            {
                ValueEncoding =
                    JsonExportContracts.TextValueEncoding,
            },
            columns[1],
            columns[2],
        ];

        Assert.Throws<InvalidDataException>(
            () => JsonExportManifestSerializer.Serialize(
                wrongDigest));
        Assert.Throws<InvalidDataException>(
            () => JsonExportManifestSerializer.ComputeSchemaDigest(
                skippedOrdinal));
        Assert.Throws<InvalidDataException>(
            () => JsonExportManifestSerializer.ComputeSchemaDigest(
                changedPropertyName));
        Assert.Throws<InvalidDataException>(
            () => JsonExportManifestSerializer.ComputeSchemaDigest(
                wrongEncoding));
    }

    [Fact]
    public void BlobBoundsAndFormatResourcePolicyAreCrossChecked()
    {
        JsonExportColumnManifest blob = Column(
            0,
            "payload",
            JsonExportDatabaseType.Blob);
        JsonExportColumnManifest text = Column(
            0,
            "text",
            JsonExportDatabaseType.Text);

        Assert.Throws<InvalidDataException>(
            () => JsonExportManifestSerializer.ComputeSchemaDigest(
            [
                blob with { MaximumDecodedBytes = 0 },
            ]));
        Assert.Throws<InvalidDataException>(
            () => JsonExportManifestSerializer.ComputeSchemaDigest(
            [
                blob with
                {
                    MaximumDecodedBytes =
                        JsonExportContracts
                            .MaximumSupportedDecodedBlobBytes + 1,
                },
            ]));
        Assert.Throws<InvalidDataException>(
            () => JsonExportManifestSerializer.ComputeSchemaDigest(
            [
                text with { MaximumDecodedBytes = 1 },
            ]));

        JsonExportManifest wrongBlobLimit = CreateManifest() with
        {
            Json = CreateManifest().Json with
            {
                MaximumDecodedBlobBytes =
                    JsonExportContracts
                        .MaximumSupportedDecodedBlobBytes - 1,
            },
        };
        JsonExportManifest wrongReaderLimit = CreateManifest() with
        {
            Json = CreateManifest().Json with
            {
                MaximumValueBytes =
                    JsonInputContracts.MaximumValueBytes - 1,
            },
        };

        Assert.Throws<InvalidDataException>(
            () => JsonExportManifestSerializer.Serialize(
                wrongBlobLimit));
        Assert.Throws<InvalidDataException>(
            () => JsonExportManifestSerializer.Serialize(
                wrongReaderLimit));
    }

    [Fact]
    public void FramingAndFixedCodecFieldsCannotDrift()
    {
        JsonExportManifest manifest = CreateManifest();
        JsonExportManifest[] invalid =
        [
            manifest with
            {
                Profile = (JsonExportProfile)999,
            },
            manifest with
            {
                Json = manifest.Json with
                {
                    Framing = (JsonExportFraming)999,
                },
            },
            manifest with
            {
                Json = manifest.Json with
                {
                    Encoding = "utf-16",
                },
            },
            manifest with
            {
                Json = manifest.Json with
                {
                    HasByteOrderMark = true,
                },
            },
            manifest with
            {
                Json = manifest.Json with
                {
                    Compact = false,
                },
            },
            manifest with
            {
                Json = manifest.Json with
                {
                    Newline = "crlf",
                },
            },
            manifest with
            {
                Content = manifest.Content with
                {
                    Aggregation = "other",
                },
            },
        ];

        foreach (JsonExportManifest item in invalid)
        {
            Assert.Throws<InvalidDataException>(
                () => JsonExportManifestSerializer.Serialize(item));
        }
    }

    [Fact]
    public void InvalidUnicodeAndOversizedInputsAreRejected()
    {
        JsonExportColumnManifest invalidName = Column(
            0,
            "invalid-\ud800",
            JsonExportDatabaseType.Text);

        InvalidDataException unicodeError =
            Assert.Throws<InvalidDataException>(
                () => JsonExportManifestSerializer.ComputeSchemaDigest(
                    [invalidName]));
        Assert.Contains(
            "UTF",
            unicodeError.Message,
            StringComparison.OrdinalIgnoreCase);

        byte[] oversized =
            new byte[
                JsonExportManifestSerializer
                    .MaximumManifestBytes + 1];
        Assert.Throws<InvalidDataException>(
            () => JsonExportManifestSerializer.Deserialize(oversized));
    }

    [Fact]
    public void ColumnCountAndAggregateTextBudgetsAreEnforced()
    {
        Assert.Throws<InvalidDataException>(
            () => JsonExportManifestSerializer.ComputeSchemaDigest([]));

        JsonExportColumnManifest[] tooManyColumns = Enumerable
            .Range(
                0,
                JsonExportManifestSerializer.MaximumColumns + 1)
            .Select(static index => Column(
                index,
                $"c{index}",
                JsonExportDatabaseType.Integer))
            .ToArray();
        Assert.Throws<InvalidDataException>(
            () => JsonExportManifestSerializer.ComputeSchemaDigest(
                tooManyColumns));

        JsonExportManifest oversizedText = CreateManifest() with
        {
            Table = CreateManifest().Table with
            {
                Name = new string(
                    'x',
                    checked((int)
                        JsonExportManifestSerializer
                            .MaximumTextCharacters + 1)),
            },
        };
        Assert.Throws<InvalidDataException>(
            () => JsonExportManifestSerializer.Serialize(
                oversizedText));
    }

    [Fact]
    public void SourceAndContentRangesAreEnforced()
    {
        JsonExportManifest manifest = CreateManifest();
        JsonExportManifest[] invalid =
        [
            manifest with
            {
                Source = manifest.Source with
                {
                    SnapshotByteLength = 0,
                },
            },
            manifest with
            {
                Content = manifest.Content with
                {
                    RowCount = -1,
                },
            },
            manifest with
            {
                Content = manifest.Content with
                {
                    DataByteLength =
                        manifest.Json.MaxDataBytes + 1,
                },
            },
            manifest with
            {
                Content = manifest.Content with
                {
                    DataByteLength = 0,
                },
            },
        ];

        foreach (JsonExportManifest item in invalid)
        {
            Assert.Throws<InvalidDataException>(
                () => JsonExportManifestSerializer.Serialize(item));
        }
    }

    [Fact]
    public void EmptyContentBindsExactFramingBytesAndLogicalDigest()
    {
        JsonExportManifest manifest = CreateManifest();
        using var logical =
            new JsonExportOrderedContentDigest();
        JsonExportHashManifest emptyLogical =
            logical.Complete();

        JsonExportManifest rootArray = manifest with
        {
            Content = manifest.Content with
            {
                RowCount = 0,
                DataByteLength = 3,
                DataDigest = PhysicalHash("[]\n"u8),
                SourceLogicalDigest = emptyLogical,
                ExportedLogicalDigest =
                    emptyLogical with { },
            },
        };
        JsonExportManifest ndjson = rootArray with
        {
            Json = rootArray.Json with
            {
                Framing = JsonExportFraming.Ndjson,
            },
            Content = rootArray.Content with
            {
                DataByteLength = 0,
                DataDigest =
                    PhysicalHash(
                        ReadOnlySpan<byte>.Empty),
            },
        };

        _ = JsonExportManifestSerializer.Serialize(
            rootArray);
        _ = JsonExportManifestSerializer.Serialize(
            ndjson);

        JsonExportManifest[] impossible =
        [
            rootArray with
            {
                Content = rootArray.Content with
                {
                    DataByteLength = 1,
                },
            },
            ndjson with
            {
                Content = ndjson.Content with
                {
                    DataByteLength = 1,
                },
            },
            rootArray with
            {
                Content = rootArray.Content with
                {
                    DataDigest = Hash('f'),
                },
            },
            rootArray with
            {
                Content = rootArray.Content with
                {
                    SourceLogicalDigest = Hash('e'),
                    ExportedLogicalDigest = Hash('e'),
                },
            },
            manifest with
            {
                Content = manifest.Content with
                {
                    RowCount = long.MaxValue,
                    DataByteLength = 1,
                },
            },
            manifest with
            {
                Content = manifest.Content with
                {
                    RowCount = 1,
                    DataByteLength =
                        manifest.Json.MaxDataBytes,
                },
            },
        ];

        foreach (JsonExportManifest item in
                 impossible)
        {
            Assert.Throws<InvalidDataException>(
                () =>
                    JsonExportManifestSerializer
                        .Serialize(item));
        }
    }

    [Fact]
    public void AggregateColumnTextFailsBeforeSchemaSerialization()
    {
        const int columnCount = 1_024;
        string prefix = new(
            'x',
            checked(
                (int)(
                    JsonExportManifestSerializer
                        .MaximumTextCharacters /
                    columnCount)));
        JsonExportColumnManifest[] columns =
            Enumerable.Range(0, columnCount)
                .Select(index =>
                    Column(
                        index,
                        prefix +
                        index.ToString(
                            CultureInfo.InvariantCulture),
                        JsonExportDatabaseType.Text))
                .ToArray();

        Assert.Throws<InvalidDataException>(
            () =>
                JsonExportManifestSerializer
                    .ComputeSchemaDigest(columns));
    }

    private static JsonExportManifest CreateManifest()
    {
        JsonExportColumnManifest[] columns = CreateColumns();
        JsonExportHashManifest logical = Hash('c');
        return new JsonExportManifest
        {
            Profile = JsonExportProfile.LosslessV1,
            Source = new JsonExportSourceManifest
            {
                Kind = JsonExportContracts.SourceKind,
                Version = "4.3.0",
                SnapshotByteLength = 4_096,
                SnapshotDigest = Hash('a'),
            },
            Table = new JsonExportTableManifest
            {
                Name = "orders",
                SchemaContract = JsonExportContracts.Schema,
                SchemaDigest =
                    JsonExportManifestSerializer.ComputeSchemaDigest(
                        columns),
                RowOrder = JsonExportContracts.RowOrder,
                Columns = columns,
            },
            Json = new JsonExportFormatManifest
            {
                Encoding = JsonExportContracts.Encoding,
                HasByteOrderMark = false,
                Culture = JsonExportContracts.Culture,
                Framing = JsonExportFraming.RootArray,
                Compact = true,
                PropertyOrder = JsonExportContracts.PropertyOrder,
                Newline = JsonExportContracts.Newline,
                HasFinalNewline = true,
                NullEncoding = JsonExportContracts.NullEncoding,
                TextEscape = JsonExportContracts.TextEscape,
                MaxDataBytes = 1L << 30,
                MaximumDecodedBlobBytes =
                    JsonExportContracts
                        .MaximumSupportedDecodedBlobBytes,
                MaximumValueBytes =
                    JsonInputContracts.MaximumValueBytes,
                MaximumStringBytes =
                    JsonInputContracts.MaximumStringBytes,
                MaximumPropertyNameBytes =
                    JsonInputContracts.MaximumPropertyNameBytes,
                MaximumPropertiesPerObject =
                    JsonInputContracts.MaximumPropertiesPerObject,
            },
            Content = new JsonExportContentManifest
            {
                RowCount = 2,
                DataByteLength = 128,
                DataDigest = Hash('b'),
                Canonicalization =
                    JsonExportContracts.Canonicalization,
                CanonicalizationContractDigest =
                    JsonExportContracts
                        .CanonicalizationContractDigest,
                Aggregation =
                    JsonExportContracts.OrderedContentDigest,
                SourceLogicalDigest = logical,
                ExportedLogicalDigest = logical with { },
            },
        };
    }

    private static JsonExportColumnManifest[] CreateColumns() =>
    [
        Column(
            0,
            "id",
            JsonExportDatabaseType.Integer,
            nullable: false),
        Column(
            1,
            "note",
            JsonExportDatabaseType.Text),
        Column(
            2,
            "payload",
            JsonExportDatabaseType.Blob),
    ];

    private static JsonExportColumnManifest Column(
        int ordinal,
        string name,
        JsonExportDatabaseType databaseType,
        bool nullable = true) =>
        new()
        {
            Ordinal = ordinal,
            SourceName = name,
            PropertyName = name,
            DatabaseType = databaseType,
            Nullable = nullable,
            ValueEncoding = databaseType switch
            {
                JsonExportDatabaseType.Integer =>
                    JsonExportContracts.IntegerValueEncoding,
                JsonExportDatabaseType.Real =>
                    JsonExportContracts.RealValueEncoding,
                JsonExportDatabaseType.Text =>
                    JsonExportContracts.TextValueEncoding,
                JsonExportDatabaseType.Blob =>
                    JsonExportContracts.BlobValueEncoding,
                _ => throw new ArgumentOutOfRangeException(
                    nameof(databaseType)),
            },
            MaximumDecodedBytes =
                databaseType == JsonExportDatabaseType.Blob
                    ? JsonExportContracts
                        .MaximumSupportedDecodedBlobBytes
                    : 0,
        };

    private static JsonExportColumnManifest Reordinal(
        JsonExportColumnManifest column,
        int ordinal) =>
        column with { Ordinal = ordinal };

    private static JsonExportHashManifest Hash(char character) =>
        new()
        {
            Algorithm = JsonExportHashManifest.Sha256Algorithm,
            Value = new string(character, 64),
        };

    private static JsonExportHashManifest PhysicalHash(
        ReadOnlySpan<byte> bytes) =>
        new()
        {
            Algorithm =
                JsonExportHashManifest.Sha256Algorithm,
            Value = Convert.ToHexString(
                    SHA256.HashData(bytes))
                .ToLowerInvariant(),
        };

    private static string CanonicalText(
        JsonExportManifest manifest) =>
        Encoding.UTF8.GetString(
            JsonExportManifestSerializer.Serialize(manifest));

    private static void DeserializeText(string value) =>
        _ = JsonExportManifestSerializer.Deserialize(
            Encoding.UTF8.GetBytes(value));

    private static byte[] ReorderEnvelope(
        ReadOnlySpan<byte> canonical)
    {
        using JsonDocument document =
            JsonDocument.Parse(canonical.ToArray());
        JsonElement root = document.RootElement;
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream))
        {
            writer.WriteStartObject();
            writer.WritePropertyName("digestAlgorithm");
            root.GetProperty("digestAlgorithm")
                .WriteTo(writer);
            writer.WritePropertyName("format");
            root.GetProperty("format")
                .WriteTo(writer);
            writer.WritePropertyName("digest");
            root.GetProperty("digest")
                .WriteTo(writer);
            writer.WritePropertyName("payload");
            root.GetProperty("payload")
                .WriteTo(writer);
            writer.WriteEndObject();
        }

        return stream.ToArray();
    }
}
