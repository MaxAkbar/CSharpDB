using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CSharpDB.ImportExport.TableArchives;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class TableArchivePrimaryKeyLookupDigestTests
{
    [Theory]
    [InlineData("schema")]
    [InlineData("rows")]
    [InlineData("physical index")]
    public async Task ReusableLookupReader_ValidatesEverySectionBeforeOpening(string section)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = CreateTemporaryPath();

        try
        {
            await WriteArchiveAsync(path, ct);
            byte[] archive = await File.ReadAllBytesAsync(path, ct);
            CorruptSectionWithoutChangingItsLength(archive, section);
            await File.WriteAllBytesAsync(path, archive, ct);

            InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            {
                TableArchivePrimaryKeyLookupReader? reader =
                    await TableArchivePrimaryKeyLookupReader.TryOpenAsync(path, ct);
                if (reader is not null)
                    await reader.DisposeAsync();
            });
            Assert.Contains($"{section} section digest", error.Message, StringComparison.Ordinal);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task ReusableLookupReader_RejectsIntegrityValidIndexPointingToDifferentKey()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = CreateTemporaryPath();

        try
        {
            await WriteArchiveAsync(path, ct);
            byte[] archive = await File.ReadAllBytesAsync(path, ct);
            (int firstEntryOffset, _, long secondRowOffset) = ReadFirstIndexEntries(archive);
            BinaryPrimitives.WriteInt64LittleEndian(
                archive.AsSpan(firstEntryOffset + sizeof(long)),
                secondRowOffset);
            RefreshSectionDigest(archive, "physicalIndex");
            await File.WriteAllBytesAsync(path, archive, ct);

            await using TableArchivePrimaryKeyLookupReader reader =
                (await TableArchivePrimaryKeyLookupReader.TryOpenAsync(path, ct))!;
            InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(
                async () => await reader.LookupAsync(1, ct));
            Assert.Contains("different primary key", error.Message, StringComparison.Ordinal);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task ReusableLookupReader_RejectsIntegrityValidRowThatViolatesSchema()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = CreateTemporaryPath();

        try
        {
            await WriteArchiveAsync(path, ct);
            byte[] archive = await File.ReadAllBytesAsync(path, ct);
            (_, long firstRowOffset, _) = ReadFirstIndexEntries(archive);
            int recordOffset = checked((int)firstRowOffset + sizeof(int));
            const int secondColumnTypeOffset = 10;
            Assert.Equal((byte)DbType.Text, archive[recordOffset + secondColumnTypeOffset]);
            archive[recordOffset + secondColumnTypeOffset] = (byte)DbType.Null;
            RefreshSectionDigest(archive, "rows");
            await File.WriteAllBytesAsync(path, archive, ct);

            await using TableArchivePrimaryKeyLookupReader reader =
                (await TableArchivePrimaryKeyLookupReader.TryOpenAsync(path, ct))!;
            InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(
                async () => await reader.LookupAsync(1, ct));
            Assert.Contains("cannot be NULL", error.Message, StringComparison.Ordinal);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    private static string CreateTemporaryPath()
        => Path.Combine(Path.GetTempPath(), $"archive_lookup_digest_{Guid.NewGuid():N}.csdbtable");

    private static async Task WriteArchiveAsync(string path, CancellationToken ct)
    {
        var schema = new TableSchema
        {
            TableName = "lookup_digest_items",
            Columns =
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false, IsPrimaryKey = true },
                new ColumnDefinition { Name = "name", Type = DbType.Text, Nullable = false },
            ],
        };
        DbValue[][] rows =
        [
            [DbValue.FromInteger(1), DbValue.FromText("one")],
            [DbValue.FromInteger(2), DbValue.FromText("two")],
        ];

        await TableArchiveWriter.WriteAsync(
            path,
            schema,
            TableArchiveWriter.ToAsyncRows(rows, ct),
            ct);
    }

    private static (int FirstEntryOffset, long FirstRowOffset, long SecondRowOffset) ReadFirstIndexEntries(
        byte[] archive)
    {
        long indexOffset = BinaryPrimitives.ReadInt64LittleEndian(archive.AsSpan(60));
        long rootPageOffset = BinaryPrimitives.ReadInt64LittleEndian(
            archive.AsSpan(checked((int)indexOffset + 24)));
        int firstEntryOffset = checked((int)(indexOffset + rootPageOffset + 24));
        long firstRowOffset = BinaryPrimitives.ReadInt64LittleEndian(
            archive.AsSpan(firstEntryOffset + sizeof(long)));
        long secondRowOffset = BinaryPrimitives.ReadInt64LittleEndian(
            archive.AsSpan(firstEntryOffset + 16 + sizeof(long)));
        return (firstEntryOffset, firstRowOffset, secondRowOffset);
    }

    private static void CorruptSectionWithoutChangingItsLength(byte[] archive, string section)
    {
        (long offset, long length) = ReadSectionRange(archive, section switch
        {
            "schema" => "schema",
            "rows" => "rows",
            "physical index" => "physicalIndex",
            _ => throw new ArgumentOutOfRangeException(nameof(section)),
        });

        if (section == "schema")
        {
            int whitespaceOffset = archive.AsSpan(checked((int)offset), checked((int)length)).IndexOf((byte)' ');
            Assert.True(whitespaceOffset >= 0);
            archive[checked((int)offset) + whitespaceOffset] = (byte)'\t';
            return;
        }

        Assert.True(length > 0);
        archive[checked((int)(offset + length - 1))] ^= 0x01;
    }

    private static void RefreshSectionDigest(byte[] archive, string digestProperty)
    {
        (long sectionOffset, long sectionLength) = ReadSectionRange(archive, digestProperty);
        string newDigest = Convert.ToHexString(SHA256.HashData(
            archive.AsSpan(checked((int)sectionOffset), checked((int)sectionLength)))).ToLowerInvariant();

        long manifestOffset = BinaryPrimitives.ReadInt64LittleEndian(archive.AsSpan(24));
        int manifestLength = BinaryPrimitives.ReadInt32LittleEndian(archive.AsSpan(32));
        using JsonDocument manifest = JsonDocument.Parse(
            archive.AsMemory(checked((int)manifestOffset), manifestLength));
        string oldDigest = manifest.RootElement
            .GetProperty("digests")
            .GetProperty(digestProperty)
            .GetString()!;
        Span<byte> manifestBytes = archive.AsSpan(checked((int)manifestOffset), manifestLength);
        int digestOffset = manifestBytes.IndexOf(Encoding.ASCII.GetBytes(oldDigest));
        Assert.True(digestOffset >= 0);
        Encoding.ASCII.GetBytes(newDigest).CopyTo(manifestBytes[digestOffset..]);
    }

    private static (long Offset, long Length) ReadSectionRange(byte[] archive, string section)
        => section switch
        {
            "schema" => (
                BinaryPrimitives.ReadInt64LittleEndian(archive.AsSpan(12)),
                BinaryPrimitives.ReadInt32LittleEndian(archive.AsSpan(20))),
            "rows" => (
                BinaryPrimitives.ReadInt64LittleEndian(archive.AsSpan(36)),
                BinaryPrimitives.ReadInt64LittleEndian(archive.AsSpan(44))),
            "physicalIndex" => (
                BinaryPrimitives.ReadInt64LittleEndian(archive.AsSpan(60)),
                BinaryPrimitives.ReadInt64LittleEndian(archive.AsSpan(68))),
            _ => throw new ArgumentOutOfRangeException(nameof(section)),
        };
}
