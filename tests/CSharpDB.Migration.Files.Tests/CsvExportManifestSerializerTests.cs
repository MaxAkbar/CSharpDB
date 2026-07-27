using System.Security.Cryptography;
using System.Text;
using System.Globalization;
using CSharpDB.Migration.Canonicalization;
using CSharpDB.Migration.Files.Csv;

namespace CSharpDB.Migration.Files.Tests;

public sealed class CsvExportManifestSerializerTests
{
    [Fact]
    public void LosslessManifestHasStableCanonicalBytesAndRoundTrips()
    {
        CsvExportManifest manifest = CreateLosslessManifest();

        byte[] first = CsvExportManifestSerializer.Serialize(manifest);
        byte[] repeated = CsvExportManifestSerializer.Serialize(manifest);
        CsvExportManifest reopened = CsvExportManifestSerializer.Deserialize(first);

        Assert.Equal(first, repeated);
        Assert.False(first.AsSpan().StartsWith(Encoding.UTF8.Preamble));
        Assert.Equal(
            "7bcbc1d715d7c69c06936b3ffb2344b8b42982faff2588dd8bacf7cce2e653d5",
            CsvExportManifestSerializer.ComputeManifestDigest(manifest));
        Assert.Equal(
            "1ed8f5f1b3f2d45d5481710d40fd7c44b9be5c77a842aa82f492f99db0a1aeaf",
            Convert.ToHexString(SHA256.HashData(first)).ToLowerInvariant());
        Assert.Equal(CsvExportProfile.LosslessV1, reopened.Profile);
        Assert.Equal("orders", reopened.Table.Name);
        Assert.Equal(2, reopened.Table.Columns.Count);
        Assert.Equal(2, reopened.Content.RowCount);
        Assert.Null(reopened.LossyTransform);
    }

    [Fact]
    public void SchemaDigestHasStableCanonicalVector()
    {
        CsvExportHashManifest digest = CsvExportManifestSerializer.ComputeSchemaDigest(
            CreateLosslessColumns());

        Assert.Equal(CsvExportHashManifest.Sha256Algorithm, digest.Algorithm);
        Assert.Equal(
            "324011ae6d14ca429d36929841883ed357dd4ccfd7922ddfefe09e0331ed5586",
            digest.Value);
    }

    [Fact]
    public void OrderedLogicalDigestHasStableEmptyAndDuplicatePreservingVectors()
    {
        using var empty = new CsvExportOrderedContentDigest();
        CsvExportHashManifest emptyDigest = empty.Complete();

        using var rows = new CsvExportOrderedContentDigest();
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
        CsvExportHashManifest rowsDigest = rows.Complete();

        Assert.Equal(
            "879a6d96f9dbe682b05f572f0f462ca37a21893f7aa626a93ab8f06acea14550",
            emptyDigest.Value);
        Assert.Equal(
            "954c6576d615bfc871f31d0385bb8ebbeee99a85dbdf80140511a0a75ce39961",
            rowsDigest.Value);
        Assert.Equal(2, rows.RowCount);
        Assert.NotEqual(emptyDigest.Value, rowsDigest.Value);
    }

    [Fact]
    public void OrderedLogicalDigestRejectsInvalidUse()
    {
        using var digest = new CsvExportOrderedContentDigest();

        Assert.Throws<ArgumentException>(() => digest.AppendRowHash(new byte[31]));
        _ = digest.Complete();
        Assert.Throws<InvalidOperationException>(() => digest.AppendRowHash(new byte[32]));
        Assert.Throws<InvalidOperationException>(() => digest.Complete());
    }

    [Fact]
    public void SpreadsheetProfileBindsRenderedHeadersAndAggregateLossEvidence()
    {
        CsvExportColumnManifest[] columns =
        [
            Column(0, "=formula", CsvExportDatabaseType.Text) with
            {
                Header = CsvSpreadsheetFormulaPolicy.Transform("=formula"),
            },
            Column(1, "amount", CsvExportDatabaseType.Real),
        ];
        CsvExportManifest manifest = CreateManifest(
            CsvExportProfile.SpreadsheetSafeLossyV1,
            columns,
            sourceLogicalDigest: Hash('c'),
            exportedLogicalDigest: Hash('d'),
            lossyTransform: new CsvExportLossyTransformManifest
            {
                RuleId = CsvExportContracts.SpreadsheetFormulaRuleId,
                Algorithm = CsvExportContracts.SpreadsheetFormulaTransform,
                TransformedHeaderCount = 1,
                TransformedRowCount = 1,
                TransformedCellCount = 1,
            });

        byte[] bytes = CsvExportManifestSerializer.Serialize(manifest);
        CsvExportManifest reopened = CsvExportManifestSerializer.Deserialize(bytes);

        Assert.Equal(CsvExportProfile.SpreadsheetSafeLossyV1, reopened.Profile);
        Assert.Equal("'=formula", reopened.Table.Columns[0].Header);
        Assert.Equal(1, reopened.LossyTransform!.TransformedHeaderCount);
        Assert.NotEqual(
            reopened.Content.SourceLogicalDigest.Value,
            reopened.Content.ExportedLogicalDigest.Value);
    }

    [Fact]
    public void TamperedCanonicalPayloadIsRejected()
    {
        string json = Encoding.UTF8.GetString(
            CsvExportManifestSerializer.Serialize(CreateLosslessManifest()));
        string tampered = json.Replace(
            "\"rowCount\":2",
            "\"rowCount\":3",
            StringComparison.Ordinal);

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => CsvExportManifestSerializer.Deserialize(Encoding.UTF8.GetBytes(tampered)));

        Assert.Contains("digest", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void NoncanonicalJsonIsRejectedEvenWhenItsPayloadIsValid()
    {
        byte[] canonical = CsvExportManifestSerializer.Serialize(CreateLosslessManifest());
        byte[] withTrailingWhitespace = [.. canonical, (byte)'\n'];

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => CsvExportManifestSerializer.Deserialize(withTrailingWhitespace));

        Assert.Contains("canonical", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Utf8BomAndDuplicateEnvelopePropertiesAreRejected()
    {
        byte[] canonical = CsvExportManifestSerializer.Serialize(CreateLosslessManifest());
        byte[] withBom = [.. Encoding.UTF8.Preamble, .. canonical];
        string json = Encoding.UTF8.GetString(canonical);
        string duplicate = json.Replace(
            "{\"format\":",
            "{\"format\":\"csharpdb-csv-export-manifest/v1\",\"format\":",
            StringComparison.Ordinal);

        InvalidDataException bomError = Assert.Throws<InvalidDataException>(
            () => CsvExportManifestSerializer.Deserialize(withBom));
        InvalidDataException duplicateError = Assert.Throws<InvalidDataException>(
            () => CsvExportManifestSerializer.Deserialize(Encoding.UTF8.GetBytes(duplicate)));

        Assert.Contains("BOM", bomError.Message, StringComparison.Ordinal);
        Assert.Contains("duplicate", duplicateError.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void UnknownManifestMembersAreRejected()
    {
        string json = Encoding.UTF8.GetString(
            CsvExportManifestSerializer.Serialize(CreateLosslessManifest()));
        string unknown = json.Replace(
            "\"profile\":",
            "\"unexpected\":true,\"profile\":",
            StringComparison.Ordinal);

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => CsvExportManifestSerializer.Deserialize(Encoding.UTF8.GetBytes(unknown)));

        Assert.Contains("payload", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void NumericEnumsAndUppercaseHashesAreRejected()
    {
        CsvExportManifest manifest = CreateLosslessManifest();
        string json = Encoding.UTF8.GetString(CsvExportManifestSerializer.Serialize(manifest));
        string numericProfile = json.Replace(
            "\"profile\":\"losslessV1\"",
            "\"profile\":0",
            StringComparison.Ordinal);
        CsvExportManifest uppercaseHash = manifest with
        {
            Source = manifest.Source with
            {
                SnapshotDigest = manifest.Source.SnapshotDigest with
                {
                    Value = manifest.Source.SnapshotDigest.Value.ToUpperInvariant(),
                },
            },
        };

        Assert.Throws<InvalidDataException>(
            () => CsvExportManifestSerializer.Deserialize(Encoding.UTF8.GetBytes(numericProfile)));
        Assert.Throws<InvalidDataException>(
            () => CsvExportManifestSerializer.Serialize(uppercaseHash));
    }

    [Fact]
    public void ManifestBytesAreIndependentOfCurrentCulture()
    {
        CultureInfo originalCulture = CultureInfo.CurrentCulture;
        CultureInfo originalUiCulture = CultureInfo.CurrentUICulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("ar-SA");
            CultureInfo.CurrentUICulture = CultureInfo.GetCultureInfo("ar-SA");
            byte[] first = CsvExportManifestSerializer.Serialize(CreateLosslessManifest());

            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("de-DE");
            CultureInfo.CurrentUICulture = CultureInfo.GetCultureInfo("de-DE");
            byte[] second = CsvExportManifestSerializer.Serialize(CreateLosslessManifest());

            Assert.Equal(first, second);
        }
        finally
        {
            CultureInfo.CurrentCulture = originalCulture;
            CultureInfo.CurrentUICulture = originalUiCulture;
        }
    }

    [Fact]
    public void LosslessProfileRejectsChangedLogicalContentOrRenderedHeaders()
    {
        CsvExportManifest changedContent = CreateLosslessManifest() with
        {
            Content = CreateLosslessManifest().Content with
            {
                ExportedLogicalDigest = Hash('d'),
            },
        };
        CsvExportColumnManifest[] changedColumns =
        [
            CreateLosslessColumns()[0] with { Header = "'id" },
            CreateLosslessColumns()[1],
        ];
        CsvExportManifest changedHeader = CreateLosslessManifest() with
        {
            Table = CreateLosslessManifest().Table with
            {
                Columns = changedColumns,
                SchemaDigest = CsvExportManifestSerializer.ComputeSchemaDigest(changedColumns),
            },
        };

        Assert.Throws<InvalidDataException>(
            () => CsvExportManifestSerializer.Serialize(changedContent));
        Assert.Throws<InvalidDataException>(
            () => CsvExportManifestSerializer.Serialize(changedHeader));
    }

    [Fact]
    public void SpreadsheetProfileRequiresExactTransformEvidence()
    {
        CsvExportManifest lossless = CreateLosslessManifest();
        CsvExportManifest missingEvidence = lossless with
        {
            Profile = CsvExportProfile.SpreadsheetSafeLossyV1,
        };
        CsvExportManifest falseCounts = CreateManifest(
            CsvExportProfile.SpreadsheetSafeLossyV1,
            CreateLosslessColumns(),
            Hash('c'),
            Hash('c'),
            new CsvExportLossyTransformManifest
            {
                RuleId = CsvExportContracts.SpreadsheetFormulaRuleId,
                Algorithm = CsvExportContracts.SpreadsheetFormulaTransform,
                TransformedHeaderCount = 1,
                TransformedRowCount = 0,
                TransformedCellCount = 0,
            });

        Assert.Throws<InvalidDataException>(
            () => CsvExportManifestSerializer.Serialize(missingEvidence));
        Assert.Throws<InvalidDataException>(
            () => CsvExportManifestSerializer.Serialize(falseCounts));
    }

    [Fact]
    public void SpreadsheetCellEvidenceIsBoundedByTransformedRowsAndTextColumns()
    {
        CsvExportManifest tooManyCells = CreateManifest(
            CsvExportProfile.SpreadsheetSafeLossyV1,
            CreateLosslessColumns(),
            Hash('c'),
            Hash('d'),
            new CsvExportLossyTransformManifest
            {
                RuleId = CsvExportContracts.SpreadsheetFormulaRuleId,
                Algorithm = CsvExportContracts.SpreadsheetFormulaTransform,
                TransformedHeaderCount = 0,
                TransformedRowCount = 1,
                TransformedCellCount = 2,
            });
        CsvExportManifest unexplainedDigestChange = CreateManifest(
            CsvExportProfile.SpreadsheetSafeLossyV1,
            CreateLosslessColumns(),
            Hash('c'),
            Hash('d'),
            new CsvExportLossyTransformManifest
            {
                RuleId = CsvExportContracts.SpreadsheetFormulaRuleId,
                Algorithm = CsvExportContracts.SpreadsheetFormulaTransform,
                TransformedHeaderCount = 0,
                TransformedRowCount = 0,
                TransformedCellCount = 0,
            });

        Assert.Throws<InvalidDataException>(
            () => CsvExportManifestSerializer.Serialize(tooManyCells));
        Assert.Throws<InvalidDataException>(
            () => CsvExportManifestSerializer.Serialize(unexplainedDigestChange));
    }

    [Fact]
    public void SchemaDigestAndTypeEncodingAreEnforced()
    {
        CsvExportManifest manifest = CreateLosslessManifest();
        CsvExportManifest wrongDigest = manifest with
        {
            Table = manifest.Table with { SchemaDigest = Hash('f') },
        };
        CsvExportColumnManifest[] wrongEncodingColumns =
        [
            CreateLosslessColumns()[0] with
            {
                ValueEncoding = CsvExportContracts.TextValueEncoding,
            },
            CreateLosslessColumns()[1],
        ];

        Assert.Throws<InvalidDataException>(
            () => CsvExportManifestSerializer.Serialize(wrongDigest));
        Assert.Throws<InvalidDataException>(
            () => CsvExportManifestSerializer.ComputeSchemaDigest(wrongEncodingColumns));
    }

    [Fact]
    public void ColumnsMustBeContiguousAndNamesMustRemainUnambiguous()
    {
        CsvExportColumnManifest[] skippedOrdinal =
        [
            Column(1, "id", CsvExportDatabaseType.Integer),
        ];
        CsvExportColumnManifest[] duplicateNames =
        [
            Column(0, "Name", CsvExportDatabaseType.Text),
            Column(1, "name", CsvExportDatabaseType.Text),
        ];

        Assert.Throws<InvalidDataException>(
            () => CsvExportManifestSerializer.ComputeSchemaDigest(skippedOrdinal));
        Assert.Throws<InvalidDataException>(
            () => CsvExportManifestSerializer.ComputeSchemaDigest(duplicateNames));
    }

    [Fact]
    public void BlobColumnsRequireAnExplicitSupportedDecodedSizeCeiling()
    {
        CsvExportColumnManifest validBlob = Column(
            0,
            "payload",
            CsvExportDatabaseType.Blob);
        CsvExportColumnManifest missingBound = validBlob with { MaximumDecodedBytes = 0 };
        CsvExportColumnManifest excessiveBound = validBlob with
        {
            MaximumDecodedBytes = checked(
                CsvExportContracts.MaximumSupportedDecodedBlobBytes + 1),
        };
        CsvExportColumnManifest boundOnText = Column(
            0,
            "text",
            CsvExportDatabaseType.Text) with
        {
            MaximumDecodedBytes = 1,
        };

        CsvExportHashManifest digest = CsvExportManifestSerializer.ComputeSchemaDigest([validBlob]);

        Assert.Equal(CsvExportHashManifest.Sha256Algorithm, digest.Algorithm);
        Assert.Throws<InvalidDataException>(
            () => CsvExportManifestSerializer.ComputeSchemaDigest([missingBound]));
        Assert.Throws<InvalidDataException>(
            () => CsvExportManifestSerializer.ComputeSchemaDigest([excessiveBound]));
        Assert.Throws<InvalidDataException>(
            () => CsvExportManifestSerializer.ComputeSchemaDigest([boundOnText]));
    }

    [Fact]
    public void SpreadsheetProfileRejectsFormulaLookingBase64BlobCells()
    {
        CsvExportColumnManifest[] columns =
        [
            Column(0, "payload", CsvExportDatabaseType.Blob),
        ];
        CsvExportManifest manifest = CreateManifest(
            CsvExportProfile.SpreadsheetSafeLossyV1,
            columns,
            Hash('c'),
            Hash('c'),
            new CsvExportLossyTransformManifest
            {
                RuleId = CsvExportContracts.SpreadsheetFormulaRuleId,
                Algorithm = CsvExportContracts.SpreadsheetFormulaTransform,
                TransformedHeaderCount = 0,
                TransformedRowCount = 0,
                TransformedCellCount = 0,
            });

        Assert.True(CsvSpreadsheetFormulaPolicy.RequiresTransform("+123"));
        Assert.Throws<InvalidDataException>(
            () => CsvExportManifestSerializer.Serialize(manifest));
    }

    [Fact]
    public void InvalidUtf16IsRejectedBeforeSerialization()
    {
        CsvExportColumnManifest[] columns =
        [
            Column(0, "invalid-\ud800", CsvExportDatabaseType.Text),
        ];

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => CsvExportManifestSerializer.ComputeSchemaDigest(columns));

        Assert.Contains("UTF-16", error.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("=SUM(A1:A2)", true)]
    [InlineData("+1", true)]
    [InlineData("-1", true)]
    [InlineData("@name", true)]
    [InlineData(" value", true)]
    [InlineData("\tvalue", true)]
    [InlineData("\rvalue", true)]
    [InlineData("\nvalue", true)]
    [InlineData("ordinary", false)]
    [InlineData("'already-prefixed", false)]
    [InlineData("", false)]
    public void SpreadsheetFormulaPolicyHasAFrozenTriggerRegistry(
        string value,
        bool expected)
    {
        Assert.Equal(expected, CsvSpreadsheetFormulaPolicy.RequiresTransform(value));
        Assert.Equal(expected ? "'" + value : value, CsvSpreadsheetFormulaPolicy.Transform(value));
    }

    private static CsvExportManifest CreateLosslessManifest() => CreateManifest(
        CsvExportProfile.LosslessV1,
        CreateLosslessColumns(),
        sourceLogicalDigest: Hash('c'),
        exportedLogicalDigest: Hash('c'),
        lossyTransform: null);

    private static CsvExportManifest CreateManifest(
        CsvExportProfile profile,
        IReadOnlyList<CsvExportColumnManifest> columns,
        CsvExportHashManifest sourceLogicalDigest,
        CsvExportHashManifest exportedLogicalDigest,
        CsvExportLossyTransformManifest? lossyTransform)
    {
        return new CsvExportManifest
        {
            Profile = profile,
            Source = new CsvExportSourceManifest
            {
                Kind = CsvExportContracts.SourceKind,
                Version = "4.3.0",
                SnapshotByteLength = 4096,
                SnapshotDigest = Hash('a'),
            },
            Table = new CsvExportTableManifest
            {
                Name = "orders",
                SchemaContract = CsvExportContracts.Schema,
                SchemaDigest = CsvExportManifestSerializer.ComputeSchemaDigest(columns),
                RowOrder = CsvExportContracts.RowOrder,
                Columns = columns,
            },
            Csv = new CsvExportFormatManifest
            {
                Encoding = CsvExportContracts.Encoding,
                HasByteOrderMark = false,
                Culture = CsvExportContracts.Culture,
                Delimiter = ",",
                Quote = '"',
                Newline = CsvExportContracts.Newline,
                HasHeaderRecord = true,
                HasFinalNewline = true,
                NullToken = CsvExportContracts.NullToken,
                NullTokenMatchesQuotedFields = false,
                TextEscape = CsvExportContracts.TextEscape,
            },
            Content = new CsvExportContentManifest
            {
                RowCount = 2,
                DataByteLength = 128,
                DataDigest = Hash('b'),
                Canonicalization = CsvExportContracts.Canonicalization,
                CanonicalizationContractDigest =
                    CsvExportContracts.CanonicalizationContractDigest,
                Aggregation = CsvExportContracts.OrderedContentDigest,
                SourceLogicalDigest = sourceLogicalDigest,
                ExportedLogicalDigest = exportedLogicalDigest,
            },
            LossyTransform = lossyTransform,
        };
    }

    private static CsvExportColumnManifest[] CreateLosslessColumns() =>
    [
        Column(0, "id", CsvExportDatabaseType.Integer, nullable: false),
        Column(1, "note", CsvExportDatabaseType.Text),
    ];

    private static CsvExportColumnManifest Column(
        int ordinal,
        string name,
        CsvExportDatabaseType databaseType,
        bool nullable = true) => new()
        {
            Ordinal = ordinal,
            SourceName = name,
            Header = name,
            DatabaseType = databaseType,
            Nullable = nullable,
            ValueEncoding = databaseType switch
            {
                CsvExportDatabaseType.Integer => CsvExportContracts.IntegerValueEncoding,
                CsvExportDatabaseType.Real => CsvExportContracts.RealValueEncoding,
                CsvExportDatabaseType.Text => CsvExportContracts.TextValueEncoding,
                CsvExportDatabaseType.Blob => CsvExportContracts.BlobValueEncoding,
                _ => throw new ArgumentOutOfRangeException(nameof(databaseType)),
            },
            MaximumDecodedBytes = databaseType == CsvExportDatabaseType.Blob
            ? CsvExportContracts.MaximumSupportedDecodedBlobBytes
            : 0,
        };

    private static CsvExportHashManifest Hash(char value) => new()
    {
        Algorithm = CsvExportHashManifest.Sha256Algorithm,
        Value = new string(value, 64),
    };
}
