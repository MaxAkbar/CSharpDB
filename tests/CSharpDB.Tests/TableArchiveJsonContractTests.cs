using System.Buffers.Binary;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Nodes;
using CSharpDB.ImportExport.Models;
using CSharpDB.ImportExport.TableArchives;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class TableArchiveJsonContractTests
{
    [Fact]
    public async Task GeneratedMetadata_PreservesExistingSchemaAndManifestJsonBytes()
    {
        var ct = TestContext.Current.CancellationToken;
        TableSchema schema = CreateSchema();
        await using var stream = new MemoryStream();
        TableArchiveManifest written = await TableArchiveWriter.WriteAsync(
            stream, schema, TableArchiveWriter.ToAsyncRows([], ct), ct);
        byte[] archive = stream.ToArray();
        // This is the pre-source-generation archive contract, kept independent
        // of the product's generated context and converter registration.
        var legacyOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = true,
            Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase, allowIntegerValues: false) },
        };
        Assert.Equal(JsonSerializer.SerializeToUtf8Bytes(TableArchiveSchema.FromTableSchema(schema), legacyOptions),
            Section(archive, offsetField: 12, lengthField: 20));
        Assert.Equal(JsonSerializer.SerializeToUtf8Bytes(written, legacyOptions),
            Section(archive, offsetField: 24, lengthField: 32));

        stream.Position = 0;
        var (readSchema, readManifest) = await TableArchiveReader.ReadMetadataAsync(stream, ct);
        Assert.Equal(SqlTypeDescriptor.Create(SqlTypeKind.VarChar, length: 32), readSchema.Columns[1].DeclaredType);
        Assert.Equal(SqlTypeKind.BigInt, readSchema.Columns[0].DeclaredType!.Kind);
        Assert.Equal(ForeignKeyOnDeleteAction.Cascade, Assert.Single(readSchema.ForeignKeys).OnDelete);
        Assert.Equal(KeyConstraintKind.PrimaryKey, Assert.Single(readSchema.KeyConstraints).Kind);
        Assert.Equal(written.Digests!.Schema, readManifest.Digests!.Schema);
        Assert.Equal(Section(archive, offsetField: 12, lengthField: 20),
            JsonSerializer.SerializeToUtf8Bytes(readSchema, legacyOptions));
        Assert.Equal(Section(archive, offsetField: 24, lengthField: 32),
            JsonSerializer.SerializeToUtf8Bytes(readManifest, legacyOptions));
    }

    [Theory]
    [InlineData("type", "integer", (int)DbType.Integer)]
    [InlineData("kind", "bigInt", (int)SqlTypeKind.BigInt)]
    [InlineData("onDelete", "cascade", (int)ForeignKeyOnDeleteAction.Cascade)]
    [InlineData("kind", "primaryKey", (int)KeyConstraintKind.PrimaryKey)]
    public async Task GeneratedMetadata_StillRejectsNumericEnums(string property, string name, int numeric)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await TableArchiveWriter.WriteAsync(stream, CreateSchema(), TableArchiveWriter.ToAsyncRows([], ct), ct);
        byte[] archive = stream.ToArray();
        byte[] original = Encoding.UTF8.GetBytes($"\"{property}\": \"{name}\"");
        byte[] replacement = Encoding.UTF8.GetBytes($"\"{property}\": {numeric}".PadRight(original.Length));
        int offset = archive.AsSpan().IndexOf(original);
        Assert.True(offset >= 76);
        Assert.Equal(original.Length, replacement.Length);
        replacement.CopyTo(archive, offset);
        await using var modified = new MemoryStream(archive);
        // Deserialization precedes digest validation. A converter that accepts
        // numeric enums would reach the digest failure instead of JsonException.
        await Assert.ThrowsAsync<JsonException>(async () => await TableArchiveReader.ReadMetadataAsync(modified, ct));
    }

    private static byte[] Section(byte[] archive, int offsetField, int lengthField)
    {
        int offset = checked((int)BinaryPrimitives.ReadInt64LittleEndian(archive.AsSpan(offsetField)));
        int length = BinaryPrimitives.ReadInt32LittleEndian(archive.AsSpan(lengthField));
        return archive.AsSpan(offset, length).ToArray();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task MissingManifestFields_KeepDefaultsWithoutAcceptingExplicitNull(bool indexed)
    {
        var ct = TestContext.Current.CancellationToken;
        var schema = new TableSchema
        {
            TableName = "manifest_defaults",
            Columns = [new ColumnDefinition { Name = "id", Type = DbType.Integer, DeclaredType = SqlTypeDescriptor.Create(SqlTypeKind.BigInt), IsPrimaryKey = indexed, Nullable = false }],
        };
        await using var stream = new MemoryStream();
        await TableArchiveWriter.WriteAsync(stream, schema,
            TableArchiveWriter.ToAsyncRows([new[] { DbValue.FromInteger(1) }], ct), ct);
        byte[] archive = stream.ToArray();
        JsonObject manifest = JsonNode.Parse(Section(archive, 24, 32))!.AsObject();
        foreach (string name in new[] { "formatVersion", "createdUtc", "schemaEntry", "rowsEntry" }) manifest.Remove(name);
        if (indexed)
            foreach (JsonNode? index in manifest["indexes"]!.AsArray()) index!.AsObject().Remove("sectionEntry");
        else manifest.Remove("indexes");
        manifest["digests"]!.AsObject().Remove("algorithm");
        manifest["digests"]!.AsObject().Remove("encoding");

        byte[] RewriteManifest()
        {
            int offset = checked((int)BinaryPrimitives.ReadInt64LittleEndian(archive.AsSpan(24)));
            byte[] json = JsonSerializer.SerializeToUtf8Bytes(manifest);
            byte[] result = new byte[offset + json.Length];
            archive.AsSpan(0, offset).CopyTo(result);
            json.CopyTo(result, offset);
            BinaryPrimitives.WriteInt32LittleEndian(result.AsSpan(32), json.Length);
            return result;
        }
        DateTimeOffset before = DateTimeOffset.UtcNow;
        await using var omitted = new MemoryStream(RewriteManifest());
        var (_, restored) = await TableArchiveReader.ReadMetadataAsync(omitted, ct);
        Assert.Equal(TableArchiveManifest.LatestFormatVersion, restored.FormatVersion);
        Assert.InRange(restored.CreatedUtc, before, DateTimeOffset.UtcNow);
        Assert.Equal("native:schema", restored.SchemaEntry);
        Assert.Equal("native:rows", restored.RowsEntry);
        Assert.Equal(TableArchiveSectionDigests.Sha256Algorithm, restored.Digests!.Algorithm);
        Assert.Equal(TableArchiveSectionDigests.LowercaseHexEncoding, restored.Digests.Encoding);
        if (indexed) Assert.Equal("native:index:primary-key", Assert.Single(restored.Indexes).SectionEntry);
        else Assert.Empty(restored.Indexes);

        manifest["digests"]!["algorithm"] = null;
        await using var explicitNull = new MemoryStream(RewriteManifest());
        await Assert.ThrowsAsync<InvalidDataException>(async () => await TableArchiveReader.ReadMetadataAsync(explicitNull, ct));
    }

    private static TableSchema CreateSchema() => new()
    {
        TableName = "json_contract",
        Columns =
        [
            new ColumnDefinition { Name = "id", Type = DbType.Integer, DeclaredType = SqlTypeDescriptor.Create(SqlTypeKind.BigInt), IsPrimaryKey = true, Nullable = false },
            new ColumnDefinition { Name = "name", Type = DbType.Text, DeclaredType = SqlTypeDescriptor.Create(SqlTypeKind.VarChar, length: 32), Nullable = true },
        ],
        ForeignKeys =
        [
            new ForeignKeyDefinition { ConstraintName = "fk_contract", ColumnName = "id", ReferencedTableName = "parent", ReferencedColumnName = "id", SupportingIndexName = "fk_contract_index", OnDelete = ForeignKeyOnDeleteAction.Cascade },
        ],
        KeyConstraints = [new KeyConstraintDefinition { Kind = KeyConstraintKind.PrimaryKey, Columns = ["id"] }],
    };
}
