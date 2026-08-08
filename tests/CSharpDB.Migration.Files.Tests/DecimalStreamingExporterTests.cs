using System.Text;
using CSharpDB.Migration.Canonicalization;
using CSharpDB.Migration.Files.Csv;
using CSharpDB.Migration.Files.Json;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Files.Tests;

public sealed class DecimalStreamingExporterTests
{
    private static readonly CancellationToken Cancellation =
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task Csv_EmitsNormalizedExactDecimalsAndCanonicalDecimalDigest()
    {
        DbValue first = DbValue.FromDecimalParts(123_456_789_012_345_678, 18);
        DbValue second = DbValue.FromDecimalParts(-900_000_000_000_000_001, 4);
        TableSchema schema = Schema("amounts", Column("amount", DbType.Decimal));

        await using var destination = new MemoryStream();
        CsvStreamingExportResult result = await new CsvStreamingExporter().WriteAsync(
            destination,
            new CsvStreamingExportRequest
            {
                Profile = CsvExportProfile.LosslessV1,
                Source = CsvSource(),
                Table = schema,
                Rows = CsvRows(
                [
                    new CsvExportRow(1, new[] { first }),
                    new CsvExportRow(2, new[] { second }),
                    new CsvExportRow(3, new[] { DbValue.FromDecimalParts(0, 18) }),
                    new CsvExportRow(4, new[] { DbValue.Null }),
                ]),
            },
            Cancellation);

        Assert.Equal(
            "amount\r\n0.123456789012345678\r\n-90000000000000.0001\r\n0\r\n\\N\r\n",
            Encoding.UTF8.GetString(destination.ToArray()));
        CsvExportColumnManifest column = Assert.Single(result.Manifest.Table.Columns);
        Assert.Equal(CsvExportDatabaseType.Decimal, column.DatabaseType);
        Assert.Equal(CsvExportContracts.DecimalValueEncoding, column.ValueEncoding);
        Assert.Equal(CsvExportContracts.Schema, result.Manifest.Table.SchemaContract);
        string manifestText = Encoding.UTF8.GetString(result.CanonicalManifestBytes);
        Assert.Contains(
            $"\"format\":\"{CsvExportContracts.ManifestFormat}\"",
            manifestText,
            StringComparison.Ordinal);
        Assert.Contains(
            $"\"databaseType\":\"decimal\",\"nullable\":true,\"valueEncoding\":\"{CsvExportContracts.DecimalValueEncoding}\"",
            manifestText,
            StringComparison.Ordinal);

        using var digest = new CsvExportOrderedContentDigest();
        digest.AppendRow([CanonicalValue.Decimal(first.AsDecimal)]);
        digest.AppendRow([CanonicalValue.Decimal(second.AsDecimal)]);
        digest.AppendRow([CanonicalValue.Decimal(decimal.Zero)]);
        digest.AppendRow([CanonicalValue.Null(CanonicalType.Decimal)]);
        CsvExportHashManifest expected = digest.Complete();
        Assert.Equal(expected, result.Manifest.Content.SourceLogicalDigest);
        Assert.Equal(expected, result.Manifest.Content.ExportedLogicalDigest);
        CsvExportManifest reopened =
            CsvExportManifestSerializer.Deserialize(result.CanonicalManifestBytes);
        Assert.Equal(CsvExportDatabaseType.Decimal, reopened.Table.Columns[0].DatabaseType);
        Assert.Equal(
            result.CanonicalManifestBytes,
            CsvExportManifestSerializer.Serialize(reopened));
    }

    [Fact]
    public async Task Json_EmitsNormalizedExactNumberAndVerifiesCanonicalDecimalDigest()
    {
        DbValue first = DbValue.FromDecimalParts(123_456_789_012_345_678, 18);
        DbValue second = DbValue.FromDecimalParts(-900_000_000_000_000_001, 4);
        TableSchema schema = Schema("amounts", Column("amount", DbType.Decimal));

        await using var destination = new MemoryStream();
        JsonStreamingExportResult result = await new JsonStreamingExporter().WriteAsync(
            destination,
            new JsonStreamingExportRequest
            {
                Profile = JsonExportProfile.LosslessV1,
                Framing = JsonExportFraming.RootArray,
                Source = JsonSource(),
                Table = schema,
                Rows = JsonRows(
                [
                    new JsonExportRow(1, new[] { first }),
                    new JsonExportRow(2, new[] { second }),
                    new JsonExportRow(3, new[] { DbValue.FromDecimalParts(0, 18) }),
                    new JsonExportRow(4, new[] { DbValue.Null }),
                ]),
            },
            Cancellation);

        Assert.Equal(
            "[{\"amount\":0.123456789012345678},{\"amount\":-90000000000000.0001},{\"amount\":0},{\"amount\":null}]\n",
            Encoding.UTF8.GetString(destination.ToArray()));
        JsonExportColumnManifest column = Assert.Single(result.Manifest.Table.Columns);
        Assert.Equal(JsonExportDatabaseType.Decimal, column.DatabaseType);
        Assert.Equal(JsonExportContracts.DecimalValueEncoding, column.ValueEncoding);
        Assert.Equal(JsonExportContracts.Schema, result.Manifest.Table.SchemaContract);
        string manifestText = Encoding.UTF8.GetString(result.CanonicalManifestBytes);
        Assert.Contains(
            $"\"format\":\"{JsonExportContracts.ManifestFormat}\"",
            manifestText,
            StringComparison.Ordinal);
        Assert.Contains(
            $"\"databaseType\":\"decimal\",\"nullable\":true,\"valueEncoding\":\"{JsonExportContracts.DecimalValueEncoding}\"",
            manifestText,
            StringComparison.Ordinal);

        using var digest = new JsonExportOrderedContentDigest();
        digest.AppendRow([CanonicalValue.Decimal(first.AsDecimal)]);
        digest.AppendRow([CanonicalValue.Decimal(second.AsDecimal)]);
        digest.AppendRow([CanonicalValue.Decimal(decimal.Zero)]);
        digest.AppendRow([CanonicalValue.Null(CanonicalType.Decimal)]);
        JsonExportHashManifest expected = digest.Complete();
        Assert.Equal(expected, result.Manifest.Content.SourceLogicalDigest);
        Assert.Equal(expected, result.Manifest.Content.ExportedLogicalDigest);
        JsonExportManifest reopened =
            JsonExportManifestSerializer.Deserialize(result.CanonicalManifestBytes);
        Assert.Equal(JsonExportDatabaseType.Decimal, reopened.Table.Columns[0].DatabaseType);
        Assert.Equal(
            result.CanonicalManifestBytes,
            JsonExportManifestSerializer.Serialize(reopened));
    }

    [Fact]
    public void DecimalManifestEncoding_IsBoundToItsDatabaseType()
    {
        var csvColumn = new CsvExportColumnManifest
        {
            Ordinal = 0,
            SourceName = "amount",
            Header = "amount",
            DatabaseType = CsvExportDatabaseType.Decimal,
            Nullable = false,
            ValueEncoding = CsvExportContracts.RealValueEncoding,
            MaximumDecodedBytes = 0,
        };
        var jsonColumn = new JsonExportColumnManifest
        {
            Ordinal = 0,
            SourceName = "amount",
            PropertyName = "amount",
            DatabaseType = JsonExportDatabaseType.Decimal,
            Nullable = false,
            ValueEncoding = JsonExportContracts.RealValueEncoding,
            MaximumDecodedBytes = 0,
        };

        Assert.Throws<InvalidDataException>(
            () => CsvExportManifestSerializer.ComputeSchemaDigest([csvColumn]));
        Assert.Throws<InvalidDataException>(
            () => JsonExportManifestSerializer.ComputeSchemaDigest([jsonColumn]));
    }

    private static TableSchema Schema(string name, params ColumnDefinition[] columns) =>
        new()
        {
            TableName = name,
            Columns = columns,
        };

    private static ColumnDefinition Column(string name, DbType type) =>
        new()
        {
            Name = name,
            Type = type,
            Nullable = true,
        };

    private static CsvExportSourceManifest CsvSource() =>
        new()
        {
            Kind = CsvExportContracts.SourceKind,
            Version = "4.5.0",
            SnapshotByteLength = 1,
            SnapshotDigest = new CsvExportHashManifest
            {
                Algorithm = CsvExportHashManifest.Sha256Algorithm,
                Value = new string('a', 64),
            },
        };

    private static JsonExportSourceManifest JsonSource() =>
        new()
        {
            Kind = JsonExportContracts.SourceKind,
            Version = "4.5.0",
            SnapshotByteLength = 1,
            SnapshotDigest = new JsonExportHashManifest
            {
                Algorithm = JsonExportHashManifest.Sha256Algorithm,
                Value = new string('a', 64),
            },
        };

    private static async IAsyncEnumerable<CsvExportRow> CsvRows(
        IReadOnlyList<CsvExportRow> rows)
    {
        foreach (CsvExportRow row in rows)
        {
            yield return row;
            await Task.Yield();
        }
    }

    private static async IAsyncEnumerable<JsonExportRow> JsonRows(
        IReadOnlyList<JsonExportRow> rows)
    {
        foreach (JsonExportRow row in rows)
        {
            yield return row;
            await Task.Yield();
        }
    }
}
