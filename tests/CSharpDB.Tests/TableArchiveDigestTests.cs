using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using CSharpDB.ImportExport.Models;
using CSharpDB.ImportExport.TableArchives;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class TableArchiveDigestTests
{
    [Fact]
    public async Task Archive_V5WritesCanonicalDigestsForEveryDataSection()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(Path.GetTempPath(), $"archive_digests_{Guid.NewGuid():N}.csdbtable");
        TableSchema schema = CreateIndexedSchema();
        DbValue[][] rows =
        [
            [DbValue.FromInteger(1), DbValue.FromText("one")],
            [DbValue.FromInteger(2), DbValue.FromText("two")],
        ];

        try
        {
            TableArchiveManifest manifest = await TableArchiveWriter.WriteAsync(
                path,
                schema,
                TableArchiveWriter.ToAsyncRows(rows, ct),
                ct);

            Assert.Equal(TableArchiveManifest.LatestFormatVersion, manifest.FormatVersion);
            TableArchiveSectionDigests digests = Assert.IsType<TableArchiveSectionDigests>(manifest.Digests);
            Assert.Equal(TableArchiveSectionDigests.Sha256Algorithm, digests.Algorithm);
            Assert.Equal(TableArchiveSectionDigests.LowercaseHexEncoding, digests.Encoding);
            Assert.All(
                new[] { digests.Schema, digests.Rows, digests.PhysicalIndex },
                digest =>
                {
                    Assert.Equal(SHA256.HashSizeInBytes * 2, digest.Length);
                    Assert.Equal(digest.ToLowerInvariant(), digest);
                });

            byte[] archive = await File.ReadAllBytesAsync(path, ct);
            Assert.Equal(digests.Schema, HashSection(
                archive,
                BinaryPrimitives.ReadInt64LittleEndian(archive.AsSpan(12)),
                BinaryPrimitives.ReadInt32LittleEndian(archive.AsSpan(20))));
            Assert.Equal(digests.Rows, HashSection(
                archive,
                BinaryPrimitives.ReadInt64LittleEndian(archive.AsSpan(36)),
                BinaryPrimitives.ReadInt64LittleEndian(archive.AsSpan(44))));
            Assert.Equal(digests.PhysicalIndex, HashSection(
                archive,
                BinaryPrimitives.ReadInt64LittleEndian(archive.AsSpan(60)),
                BinaryPrimitives.ReadInt64LittleEndian(archive.AsSpan(68))));

            (TableArchiveSchema _, TableArchiveManifest readManifest) =
                await TableArchiveReader.ReadMetadataAsync(path, ct);
            Assert.Equal(digests.Schema, readManifest.Digests!.Schema);

            var restored = new List<DbValue[]>();
            await foreach (DbValue[] row in TableArchiveReader.ReadRowsAsync(path, ct))
                restored.Add(row);
            Assert.Equal(2, restored.Count);
            Assert.Equal("two", restored[1][1].AsText);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task Archive_V5RejectsCorruptionInSchemaRowsAndPhysicalIndexSections()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(Path.GetTempPath(), $"archive_corruption_{Guid.NewGuid():N}.csdbtable");
        TableSchema schema = CreateIndexedSchema();
        DbValue[][] rows =
        [
            [DbValue.FromInteger(1), DbValue.FromText("one")],
            [DbValue.FromInteger(2), DbValue.FromText("two")],
        ];

        try
        {
            await TableArchiveWriter.WriteAsync(
                path,
                schema,
                TableArchiveWriter.ToAsyncRows(rows, ct),
                ct);
            byte[] original = await File.ReadAllBytesAsync(path, ct);

            foreach (string section in new[] { "schema", "rows", "physical index" })
            {
                byte[] corrupted = original.ToArray();
                CorruptSectionWithoutChangingItsLength(corrupted, section);
                await File.WriteAllBytesAsync(path, corrupted, ct);

                InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(
                    async () => await TableArchiveReader.ReadMetadataAsync(path, ct));
                Assert.Contains($"{section} section digest", error.Message, StringComparison.Ordinal);
            }
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task Archive_V5RequiresDigestsAndRejectsUppercaseHex()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(Path.GetTempPath(), $"archive_digest_contract_{Guid.NewGuid():N}.csdbtable");
        var schema = new TableSchema
        {
            TableName = "digest_contract_items",
            Columns =
            [
                new ColumnDefinition { Name = "value", Type = DbType.Text, Nullable = false },
            ],
        };

        try
        {
            TableArchiveManifest manifest = await TableArchiveWriter.WriteAsync(
                path,
                schema,
                TableArchiveWriter.ToAsyncRows(Array.Empty<DbValue[]>(), ct),
                ct);
            byte[] original = await File.ReadAllBytesAsync(path, ct);

            byte[] missing = RewriteEmptyUnindexedManifest(
                original,
                TableArchiveManifest.LatestFormatVersion,
                json => json.Remove("digests"));
            await File.WriteAllBytesAsync(path, missing, ct);
            InvalidDataException missingError = await Assert.ThrowsAsync<InvalidDataException>(
                async () => await TableArchiveReader.ReadMetadataAsync(path, ct));
            Assert.Contains("requires section digests", missingError.Message, StringComparison.Ordinal);

            byte[] uppercase = original.ToArray();
            string schemaDigest = manifest.Digests!.Schema;
            int digestOffset = uppercase.AsSpan().IndexOf(Encoding.ASCII.GetBytes(schemaDigest));
            Assert.True(digestOffset >= 0);
            int letterOffset = schemaDigest.IndexOfAny(['a', 'b', 'c', 'd', 'e', 'f']);
            Assert.True(letterOffset >= 0);
            uppercase[digestOffset + letterOffset] = (byte)char.ToUpperInvariant(schemaDigest[letterOffset]);
            await File.WriteAllBytesAsync(path, uppercase, ct);
            InvalidDataException uppercaseError = await Assert.ThrowsAsync<InvalidDataException>(
                async () => await TableArchiveReader.ReadMetadataAsync(path, ct));
            Assert.Contains("lowercase hexadecimal", uppercaseError.Message, StringComparison.Ordinal);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task Archive_V4WithoutDigestsRemainsReadable()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(Path.GetTempPath(), $"archive_v4_compat_{Guid.NewGuid():N}.csdbtable");
        var schema = new TableSchema
        {
            TableName = "legacy_rowversion_items",
            Columns =
            [
                new ColumnDefinition
                {
                    Name = "version",
                    Type = DbType.Blob,
                    Nullable = false,
                    IsRowVersion = true,
                },
            ],
        };

        try
        {
            await TableArchiveWriter.WriteAsync(
                path,
                schema,
                TableArchiveWriter.ToAsyncRows(Array.Empty<DbValue[]>(), ct),
                ct);
            byte[] current = await File.ReadAllBytesAsync(path, ct);
            byte[] legacyV4 = RewriteEmptyUnindexedManifest(
                current,
                TableArchiveManifest.RowVersionFormatVersion,
                json => json.Remove("digests"));
            await File.WriteAllBytesAsync(path, legacyV4, ct);

            (TableArchiveSchema archivedSchema, TableArchiveManifest manifest) =
                await TableArchiveReader.ReadMetadataAsync(path, ct);
            Assert.Equal(TableArchiveManifest.RowVersionFormatVersion, manifest.FormatVersion);
            Assert.Null(manifest.Digests);
            Assert.True(Assert.Single(archivedSchema.Columns).IsRowVersion);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    private static TableSchema CreateIndexedSchema()
        => new()
        {
            TableName = "digest_items",
            Columns =
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false, IsPrimaryKey = true },
                new ColumnDefinition { Name = "name", Type = DbType.Text, Nullable = false },
            ],
        };

    private static string HashSection(byte[] archive, long offset, long length)
    {
        ReadOnlySpan<byte> section = archive.AsSpan(checked((int)offset), checked((int)length));
        return Convert.ToHexString(SHA256.HashData(section)).ToLowerInvariant();
    }

    private static void CorruptSectionWithoutChangingItsLength(byte[] archive, string section)
    {
        long offset;
        long length;
        switch (section)
        {
            case "schema":
                offset = BinaryPrimitives.ReadInt64LittleEndian(archive.AsSpan(12));
                length = BinaryPrimitives.ReadInt32LittleEndian(archive.AsSpan(20));
                int whitespaceOffset = archive.AsSpan(checked((int)offset), checked((int)length)).IndexOf((byte)' ');
                Assert.True(whitespaceOffset >= 0);
                archive[checked((int)offset) + whitespaceOffset] = (byte)'\t';
                return;
            case "rows":
                offset = BinaryPrimitives.ReadInt64LittleEndian(archive.AsSpan(36));
                length = BinaryPrimitives.ReadInt64LittleEndian(archive.AsSpan(44));
                break;
            case "physical index":
                offset = BinaryPrimitives.ReadInt64LittleEndian(archive.AsSpan(60));
                length = BinaryPrimitives.ReadInt64LittleEndian(archive.AsSpan(68));
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(section));
        }

        Assert.True(length > 0);
        archive[checked((int)(offset + length - 1))] ^= 0x01;
    }

    private static byte[] RewriteEmptyUnindexedManifest(
        byte[] archive,
        int formatVersion,
        Action<JsonObject> rewrite)
    {
        const int headerSize = 76;
        long schemaOffset = BinaryPrimitives.ReadInt64LittleEndian(archive.AsSpan(12));
        int schemaLength = BinaryPrimitives.ReadInt32LittleEndian(archive.AsSpan(20));
        long manifestOffset = BinaryPrimitives.ReadInt64LittleEndian(archive.AsSpan(24));
        int manifestLength = BinaryPrimitives.ReadInt32LittleEndian(archive.AsSpan(32));
        Assert.Equal(0, BinaryPrimitives.ReadInt64LittleEndian(archive.AsSpan(44)));
        Assert.Equal(0, BinaryPrimitives.ReadInt64LittleEndian(archive.AsSpan(52)));
        Assert.Equal(0, BinaryPrimitives.ReadInt64LittleEndian(archive.AsSpan(60)));
        Assert.Equal(0, BinaryPrimitives.ReadInt64LittleEndian(archive.AsSpan(68)));

        byte[] schemaBytes = archive.AsSpan(checked((int)schemaOffset), schemaLength).ToArray();
        string manifestJson = Encoding.UTF8.GetString(
            archive,
            checked((int)manifestOffset),
            manifestLength);
        JsonObject manifest = JsonNode.Parse(manifestJson)!.AsObject();
        manifest["formatVersion"] = formatVersion;
        rewrite(manifest);
        byte[] manifestBytes = Encoding.UTF8.GetBytes(manifest.ToJsonString(new JsonSerializerOptions
        {
            WriteIndented = true,
        }));

        long rowsOffset = headerSize + schemaBytes.Length;
        long rewrittenManifestOffset = rowsOffset;
        var result = new byte[checked(headerSize + schemaBytes.Length + manifestBytes.Length)];
        archive.AsSpan(0, headerSize).CopyTo(result);
        BinaryPrimitives.WriteInt32LittleEndian(result.AsSpan(8), formatVersion);
        BinaryPrimitives.WriteInt64LittleEndian(result.AsSpan(12), headerSize);
        BinaryPrimitives.WriteInt32LittleEndian(result.AsSpan(20), schemaBytes.Length);
        BinaryPrimitives.WriteInt64LittleEndian(result.AsSpan(24), rewrittenManifestOffset);
        BinaryPrimitives.WriteInt32LittleEndian(result.AsSpan(32), manifestBytes.Length);
        BinaryPrimitives.WriteInt64LittleEndian(result.AsSpan(36), rowsOffset);
        schemaBytes.CopyTo(result.AsSpan(headerSize));
        manifestBytes.CopyTo(result.AsSpan(checked((int)rewrittenManifestOffset)));
        return result;
    }
}
