using System.Buffers;
using System.Buffers.Binary;
using System.Collections.ObjectModel;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Migration;

namespace CSharpDB.Migration.Retained;

internal static class RetainedMigrationBinaryCodec
{
    public const int HeaderBytes = 12;

    public const int RowHeaderBytes = 16;

    public const int DigestBytes = 32;

    public const int MaximumIdentifierBytes = 64 * 1024;

    private static ReadOnlySpan<byte> PackageMagic =>
        "CSDBRMP1"u8;

    private static ReadOnlySpan<byte> RowMagic =>
        "ROW1"u8;

    private static UTF8Encoding StrictUtf8 { get; } =
        new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    public static byte[] BuildManifest(
        MigrationCatalog catalog,
        string catalogJson,
        string catalogDigest,
        string snapshotIdentity,
        string contentDigest,
        IReadOnlyList<RetainedPackageTableBinding> tables,
        int maximumBytes)
    {
        using var stream = new BoundedMemoryStream(
            maximumBytes,
            "The retained package manifest exceeds its configured byte bound.");
        WriteString(stream, RetainedMigrationPackageContract.Format);
        WriteString(stream, catalogDigest);
        WriteInt32(stream, (int)catalog.Source.Kind);
        WriteString(stream, catalog.Source.Identity);
        WriteString(stream, catalog.Source.Fingerprint);
        WriteString(stream, snapshotIdentity);
        WriteString(stream, contentDigest);
        WriteString(stream, catalogJson);
        WriteInt32(stream, tables.Count);
        foreach (RetainedPackageTableBinding table in tables)
        {
            WriteString(stream, table.Descriptor.SourceObjectId);
            WriteStringList(stream, table.Descriptor.ColumnObjectIds);
            WriteStringList(
                stream,
                table.Descriptor.OrderingKeyColumnObjectIds);
            WriteInt64(stream, table.RowCount);
            WriteInt64(stream, table.RelativeOffset);
            WriteInt64(stream, table.SectionLength);
            byte[] digest = ParseDigest(table.SectionDigest);
            stream.Write(digest);
            CryptographicOperations.ZeroMemory(digest);
        }

        return stream.ToArray();
    }

    public static void WriteHeader(
        Stream destination,
        int manifestLength)
    {
        destination.Write(PackageMagic);
        WriteInt32(destination, manifestLength);
    }

    public static ParsedRetainedPackage ReadAndValidateManifest(
        Stream stream,
        long packageLength,
        RetainedMigrationPackageOpenOptions options)
    {
        Span<byte> header = stackalloc byte[HeaderBytes];
        ReadExactly(stream, header);
        if (!header[..PackageMagic.Length].SequenceEqual(PackageMagic))
            throw InvalidPackage("The retained package header is invalid.");

        int manifestLength =
            BinaryPrimitives.ReadInt32BigEndian(header[8..]);
        if (manifestLength <= 0 ||
            manifestLength > options.MaxManifestBytes)
        {
            throw LimitExceeded(
                "The retained package manifest exceeds its configured bound.");
        }
        long bodyOffset = checked((long)HeaderBytes + manifestLength);
        if (bodyOffset > packageLength)
            throw InvalidPackage("The retained package manifest is truncated.");

        byte[] manifestBytes = new byte[manifestLength];
        ReadExactly(stream, manifestBytes);
        using var manifest = new MemoryStream(
            manifestBytes,
            writable: false);
        string format = ReadString(
            manifest,
            256,
            "package format");
        if (!string.Equals(
                format,
                RetainedMigrationPackageContract.Format,
                StringComparison.Ordinal))
        {
            throw InvalidPackage(
                "The retained package format is not supported.");
        }

        string catalogDigest = ReadBareDigestString(
            manifest,
            "catalog digest");
        int sourceKindValue = ReadInt32(
            manifest,
            "source kind");
        if (!Enum.IsDefined(
                typeof(MigrationSourceKind),
                sourceKindValue))
        {
            throw InvalidPackage(
                "The retained package source kind is invalid.");
        }
        var sourceKind = (MigrationSourceKind)sourceKindValue;
        string sourceIdentity = ReadString(
            manifest,
            MaximumIdentifierBytes,
            "source identity");
        string sourceFingerprint = ReadString(
            manifest,
            MaximumIdentifierBytes,
            "source fingerprint");
        string snapshotIdentity = ReadString(
            manifest,
            MaximumIdentifierBytes,
            "snapshot identity");
        string contentDigest = ReadDigestString(
            manifest,
            "content digest");
        string catalogJson = ReadString(
            manifest,
            options.MaxCatalogBytes,
            "catalog");
        int tableCount = ReadInt32(
            manifest,
            "table count");
        if (tableCount < 0 ||
            tableCount > options.MaxTables)
        {
            throw LimitExceeded(
                "The retained package table count exceeds its configured bound.");
        }

        var tables =
            new List<RetainedPackageTableBinding>(
                tableCount);
        var objectIds =
            new HashSet<string>(StringComparer.Ordinal);
        long expectedRelativeOffset = 0;
        byte[] digestBytes =
            new byte[DigestBytes];
        for (int index = 0; index < tableCount; index++)
        {
            string sourceObjectId = ReadString(
                manifest,
                MaximumIdentifierBytes,
                "source object id");
            if (!objectIds.Add(sourceObjectId))
            {
                throw InvalidPackage(
                    "The retained package repeats a table source object id.");
            }

            IReadOnlyList<string> columns =
                ReadStringList(
                    manifest,
                    options.MaxColumnsPerTable,
                    "column object ids");
            IReadOnlyList<string> orderingKeys =
                ReadStringList(
                    manifest,
                    options.MaxColumnsPerTable,
                    "ordering key column object ids");
            long rowCount = ReadInt64(
                manifest,
                "row count");
            long relativeOffset = ReadInt64(
                manifest,
                "section offset");
            long sectionLength = ReadInt64(
                manifest,
                "section length");
            ReadExactly(manifest, digestBytes);

            if (rowCount < 0 ||
                rowCount > options.MaxRowsPerTable ||
                relativeOffset != expectedRelativeOffset ||
                sectionLength < 0)
            {
                throw InvalidPackage(
                    "A retained package table section has invalid bounds.");
            }
            if (rowCount == 0 &&
                sectionLength != 0)
            {
                throw InvalidPackage(
                    "An empty retained table has a nonempty row section.");
            }
            if (rowCount > 0 &&
                rowCount >
                    sectionLength /
                    RowHeaderBytes)
            {
                throw InvalidPackage(
                    "A retained table row section is too short for its row count.");
            }
            if (sectionLength >
                long.MaxValue -
                relativeOffset)
            {
                throw InvalidPackage(
                    "A retained package table section overflows its bounds.");
            }
            expectedRelativeOffset =
                relativeOffset + sectionLength;
            if (expectedRelativeOffset >
                packageLength - bodyOffset)
            {
                throw InvalidPackage(
                    "A retained package table section extends beyond the package.");
            }

            var descriptor =
                new RetainedMigrationTableDescriptor
                {
                    SourceObjectId = sourceObjectId,
                    ColumnObjectIds = columns,
                    OrderingKeyColumnObjectIds =
                        orderingKeys,
                };
            tables.Add(
                new RetainedPackageTableBinding(
                    descriptor,
                    rowCount,
                    relativeOffset,
                    sectionLength,
                    FormatDigest(digestBytes)));
        }

        if (manifest.Position != manifest.Length)
        {
            throw InvalidPackage(
                "The retained package manifest contains trailing data.");
        }
        if (expectedRelativeOffset !=
            packageLength - bodyOffset)
        {
            throw InvalidPackage(
                "The retained package contains unbound trailing data.");
        }

        return new ParsedRetainedPackage(
            catalogDigest,
            sourceKind,
            sourceIdentity,
            sourceFingerprint,
            snapshotIdentity,
            contentDigest,
            catalogJson,
            bodyOffset,
            tables.AsReadOnly());
    }

    public static long MeasureRowPayload(
        MigrationDataRow row,
        int expectedValueCount,
        int maxValueBytes,
        int maxStableKeyBytes,
        int maxRowBytes)
    {
        ArgumentNullException.ThrowIfNull(row);
        if (row.Values is null ||
            row.Values.Count != expectedValueCount)
        {
            throw new ArgumentException(
                "A retained row does not match its table column count.",
                nameof(row));
        }

        long length = 1 + sizeof(int);
        if (row.StableKey is not null)
        {
            int stableKeyBytes = GetUtf8ByteCount(
                row.StableKey,
                "stable key");
            if (stableKeyBytes > maxStableKeyBytes)
            {
                throw LimitExceeded(
                    "A retained row stable key exceeds its configured bound.");
            }
            length = checked(
                length + sizeof(int) +
                stableKeyBytes);
        }

        foreach (MigrationSourceValue value in
                 row.Values)
        {
            if (value is null)
            {
                throw new ArgumentException(
                    "A retained row cannot contain a null value object.",
                    nameof(row));
            }

            int valueLength = MeasureValue(
                value,
                maxValueBytes);
            length = checked(
                length + 1 + sizeof(int) +
                valueLength);
            if (length > maxRowBytes)
            {
                throw LimitExceeded(
                    "A retained row exceeds its configured byte bound.");
            }
        }

        return length;
    }

    public static void WriteRowRecord(
        Stream destination,
        IncrementalHash sectionHash,
        long rowOrdinal,
        MigrationDataRow row,
        int payloadLength)
    {
        var sink = new HashedStreamWriter(
            destination,
            sectionHash);
        sink.Write(RowMagic);
        sink.WriteInt64(rowOrdinal);
        sink.WriteInt32(payloadLength);
        sink.WriteByte(
            row.StableKey is null
                ? (byte)0
                : (byte)1);
        if (row.StableKey is not null)
            sink.WriteString(row.StableKey);
        sink.WriteInt32(row.Values.Count);
        foreach (MigrationSourceValue value in row.Values)
        {
            sink.WriteByte((byte)value.Kind);
            switch (value.Kind)
            {
                case MigrationSourceValueKind.Null:
                    sink.WriteInt32(0);
                    break;

                case MigrationSourceValueKind.Binary:
                    sink.WriteInt32(
                        value.BinaryValue.Length);
                    sink.Write(value.BinaryValue.Span);
                    break;

                default:
                    string text = value.CanonicalText ??
                        throw new InvalidOperationException(
                            "A measured retained scalar lost its canonical text.");
                    sink.WriteString(text);
                    break;
            }
        }
    }

    public static DecodedRetainedRow ReadRow(
        Stream stream,
        long expectedRowOrdinal,
        int expectedValueCount,
        int maxValueBytes,
        int maxStableKeyBytes,
        int maxRowBytes,
        IncrementalHash? sectionHash = null)
    {
        var reader = new HashedStreamReader(
            stream,
            sectionHash);
        Span<byte> magic = stackalloc byte[4];
        reader.ReadExactly(
            magic,
            "row boundary");
        if (!magic.SequenceEqual(RowMagic))
            throw InvalidPackage("A retained row boundary is invalid.");
        long rowOrdinal = reader.ReadInt64("row ordinal");
        if (rowOrdinal != expectedRowOrdinal)
        {
            throw InvalidPackage(
                "A retained row ordinal is not contiguous.");
        }
        int payloadLength =
            reader.ReadInt32("row payload length");
        if (payloadLength <
                1 + sizeof(int) ||
            payloadLength > maxRowBytes)
        {
            throw LimitExceeded(
                "A retained row payload exceeds its configured bound.");
        }
        long payloadStart = stream.Position;
        long payloadEnd = checked(
            payloadStart + payloadLength);
        reader.LimitTo(payloadEnd);

        byte stableKeyMarker =
            reader.ReadByte("stable key marker");
        string? stableKey = stableKeyMarker switch
        {
            0 => null,
            1 => reader.ReadString(
                maxStableKeyBytes,
                "stable key"),
            _ => throw InvalidPackage(
                "A retained row stable-key marker is invalid."),
        };
        int valueCount =
            reader.ReadInt32("row value count");
        if (valueCount != expectedValueCount)
        {
            throw InvalidPackage(
                "A retained row does not match its table column count.");
        }

        var values =
            new MigrationSourceValue[valueCount];
        for (int index = 0;
             index < valueCount;
             index++)
        {
            byte kindValue =
                reader.ReadByte("value kind");
            if (!Enum.IsDefined(
                    typeof(MigrationSourceValueKind),
                    (int)kindValue))
            {
                throw InvalidPackage(
                    "A retained row contains an unknown scalar kind.");
            }
            var kind =
                (MigrationSourceValueKind)kindValue;
            int valueLength =
                reader.ReadInt32("value length");
            if (valueLength < 0 ||
                valueLength > maxValueBytes)
            {
                throw LimitExceeded(
                    "A retained scalar exceeds its configured bound.");
            }

            if (kind == MigrationSourceValueKind.Null)
            {
                if (valueLength != 0)
                {
                    throw InvalidPackage(
                        "A retained null scalar carries a payload.");
                }
                values[index] =
                    new MigrationSourceValue
                    {
                        Kind = kind,
                    };
            }
            else if (kind ==
                MigrationSourceValueKind.Binary)
            {
                byte[] bytes =
                    reader.ReadBytes(
                        valueLength,
                        "binary scalar");
                values[index] =
                    new MigrationSourceValue
                    {
                        Kind = kind,
                        BinaryValue = bytes,
                    };
            }
            else
            {
                string text =
                    reader.ReadUtf8(
                        valueLength,
                        "canonical scalar");
                values[index] =
                    new MigrationSourceValue
                    {
                        Kind = kind,
                        CanonicalText = text,
                    };
            }
        }

        long actualPayloadLength =
            stream.Position - payloadStart;
        if (actualPayloadLength != payloadLength)
        {
            throw InvalidPackage(
                "A retained row payload length is inconsistent.");
        }

        return new DecodedRetainedRow(
            new MigrationDataRow
            {
                StableKey = stableKey,
                Values =
                    Array.AsReadOnly(values),
            },
            checked(RowHeaderBytes +
                (long)payloadLength));
    }

    public static IncrementalHash CreateSectionHash()
    {
        IncrementalHash hash =
            IncrementalHash.CreateHash(
                HashAlgorithmName.SHA256);
        AppendDomain(
            hash,
            RetainedMigrationPackageContract
                .TableSectionDigestAlgorithm);
        return hash;
    }

    public static string ComputeContentDigest(
        IReadOnlyList<RetainedPackageTableBinding> tables)
    {
        using IncrementalHash hash =
            IncrementalHash.CreateHash(
                HashAlgorithmName.SHA256);
        AppendDomain(
            hash,
            RetainedMigrationPackageContract
                .ContentDigestAlgorithm);
        AppendInt32(hash, tables.Count);
        foreach (RetainedPackageTableBinding table in tables)
        {
            AppendString(
                hash,
                table.Descriptor.SourceObjectId);
            AppendStringList(
                hash,
                table.Descriptor.ColumnObjectIds);
            AppendStringList(
                hash,
                table.Descriptor
                    .OrderingKeyColumnObjectIds);
            AppendInt64(hash, table.RowCount);
            byte[] digest =
                ParseDigest(table.SectionDigest);
            hash.AppendData(digest);
            CryptographicOperations.ZeroMemory(digest);
        }
        return FormatDigest(
            hash.GetHashAndReset());
    }

    public static string FinishDigest(
        IncrementalHash hash) =>
        FormatDigest(hash.GetHashAndReset());

    public static string FormatDigest(
        ReadOnlySpan<byte> bytes) =>
        "sha256:" +
        Convert.ToHexString(bytes)
            .ToLowerInvariant();

    public static bool IsCanonicalDigest(
        string? digest) =>
        digest is not null &&
        digest.Length == 71 &&
        digest.StartsWith(
            "sha256:",
            StringComparison.Ordinal) &&
        !digest.AsSpan(7).ContainsAnyExcept(
            "0123456789abcdef".AsSpan());

    public static bool IsCanonicalBareDigest(
        string? digest) =>
        digest is not null &&
        digest.Length == 64 &&
        !digest.AsSpan().ContainsAnyExcept(
            "0123456789abcdef".AsSpan());

    public static byte[] ParseDigest(string digest)
    {
        if (!IsCanonicalDigest(digest))
        {
            throw InvalidPackage(
                "A retained package digest is not canonical lowercase SHA-256.");
        }
        return Convert.FromHexString(digest.AsSpan(7));
    }

    public static int GetUtf8ByteCount(
        string text,
        string fieldName)
    {
        try
        {
            return StrictUtf8.GetByteCount(text);
        }
        catch (EncoderFallbackException exception)
        {
            throw new ArgumentException(
                $"The retained {fieldName} is not valid Unicode.",
                exception);
        }
    }

    public static void ValidateSafeManifestText(
        string text,
        string fieldName)
    {
        if (string.IsNullOrWhiteSpace(text) ||
            GetUtf8ByteCount(text, fieldName) >
                MaximumIdentifierBytes ||
            text.Any(static character =>
                char.IsControl(character)))
        {
            throw new ArgumentException(
                $"The retained {fieldName} is blank, too long, or contains control characters.");
        }
    }

    public static bool FixedTimeDigestEquals(
        string left,
        string right)
    {
        if (!IsCanonicalDigest(left) ||
            !IsCanonicalDigest(right))
        {
            return false;
        }
        byte[] leftBytes = ParseDigest(left);
        byte[] rightBytes = ParseDigest(right);
        try
        {
            return CryptographicOperations
                .FixedTimeEquals(
                    leftBytes,
                    rightBytes);
        }
        finally
        {
            CryptographicOperations
                .ZeroMemory(leftBytes);
            CryptographicOperations
                .ZeroMemory(rightBytes);
        }
    }

    public static bool FixedTimeBareDigestEquals(
        string left,
        string right)
    {
        if (!IsCanonicalBareDigest(left) ||
            !IsCanonicalBareDigest(right))
        {
            return false;
        }
        byte[] leftBytes =
            Convert.FromHexString(left);
        byte[] rightBytes =
            Convert.FromHexString(right);
        try
        {
            return CryptographicOperations
                .FixedTimeEquals(
                    leftBytes,
                    rightBytes);
        }
        finally
        {
            CryptographicOperations
                .ZeroMemory(leftBytes);
            CryptographicOperations
                .ZeroMemory(rightBytes);
        }
    }

    private static int MeasureValue(
        MigrationSourceValue value,
        int maxValueBytes)
    {
        if (!Enum.IsDefined(value.Kind))
        {
            throw new ArgumentException(
                "A retained scalar kind is invalid.",
                nameof(value));
        }

        int length;
        switch (value.Kind)
        {
            case MigrationSourceValueKind.Null:
                if (value.CanonicalText is not null ||
                    !value.BinaryValue.IsEmpty)
                {
                    throw new ArgumentException(
                        "A retained null scalar cannot carry a payload.",
                        nameof(value));
                }
                length = 0;
                break;

            case MigrationSourceValueKind.Binary:
                if (value.CanonicalText is not null)
                {
                    throw new ArgumentException(
                        "A retained binary scalar cannot carry canonical text.",
                        nameof(value));
                }
                length = value.BinaryValue.Length;
                break;

            default:
                if (value.CanonicalText is null ||
                    !value.BinaryValue.IsEmpty)
                {
                    throw new ArgumentException(
                        "A retained text-encoded scalar must carry only canonical text.",
                        nameof(value));
                }
                length = GetUtf8ByteCount(
                    value.CanonicalText,
                    "canonical scalar");
                break;
        }

        if (length > maxValueBytes)
        {
            throw LimitExceeded(
                "A retained scalar exceeds its configured byte bound.");
        }
        return length;
    }

    private static void WriteStringList(
        Stream stream,
        IReadOnlyList<string> values)
    {
        WriteInt32(stream, values.Count);
        foreach (string value in values)
            WriteString(stream, value);
    }

    private static void WriteString(
        Stream stream,
        string value)
    {
        int byteCount =
            GetUtf8ByteCount(value, "text");
        WriteInt32(stream, byteCount);
        WriteUtf8(stream, value, byteCount);
    }

    private static void WriteUtf8(
        Stream stream,
        string value,
        int byteCount)
    {
        if (byteCount == 0)
            return;
        byte[] buffer =
            ArrayPool<byte>.Shared.Rent(
                Math.Min(byteCount, 64 * 1024));
        try
        {
            Encoder encoder =
                StrictUtf8.GetEncoder();
            ReadOnlySpan<char> remaining =
                value.AsSpan();
            while (!remaining.IsEmpty)
            {
                encoder.Convert(
                    remaining,
                    buffer,
                    flush: true,
                    out int charsUsed,
                    out int bytesUsed,
                    out _);
                stream.Write(
                    buffer,
                    0,
                    bytesUsed);
                remaining = remaining[charsUsed..];
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(
                buffer.AsSpan(
                    0,
                    Math.Min(byteCount, buffer.Length)));
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static string ReadDigestString(
        Stream stream,
        string fieldName)
    {
        string value = ReadString(
            stream,
            128,
            fieldName);
        if (!IsCanonicalDigest(value))
        {
            throw InvalidPackage(
                $"The retained package {fieldName} is invalid.");
        }
        return value;
    }

    private static string ReadBareDigestString(
        Stream stream,
        string fieldName)
    {
        string value = ReadString(
            stream,
            128,
            fieldName);
        if (!IsCanonicalBareDigest(value))
        {
            throw InvalidPackage(
                $"The retained package {fieldName} is invalid.");
        }
        return value;
    }

    private static IReadOnlyList<string>
        ReadStringList(
        Stream stream,
        int maximumCount,
        string fieldName)
    {
        int count = ReadInt32(
            stream,
            fieldName + " count");
        if (count < 0 ||
            count > maximumCount)
        {
            throw LimitExceeded(
                $"The retained package {fieldName} count exceeds its configured bound.");
        }
        var seen =
            new HashSet<string>(
                StringComparer.Ordinal);
        var values = new string[count];
        for (int index = 0;
             index < count;
             index++)
        {
            string value = ReadString(
                stream,
                MaximumIdentifierBytes,
                fieldName);
            if (!seen.Add(value))
            {
                throw InvalidPackage(
                    $"The retained package repeats a value in {fieldName}.");
            }
            values[index] = value;
        }
        return Array.AsReadOnly(values);
    }

    private static string ReadString(
        Stream stream,
        int maximumBytes,
        string fieldName)
    {
        int length = ReadInt32(
            stream,
            fieldName + " length");
        if (length < 0 ||
            length > maximumBytes)
        {
            throw LimitExceeded(
                $"The retained package {fieldName} exceeds its configured bound.");
        }
        byte[] bytes = new byte[length];
        ReadExactly(stream, bytes);
        try
        {
            return StrictUtf8.GetString(bytes);
        }
        catch (DecoderFallbackException exception)
        {
            throw InvalidPackage(
                $"The retained package {fieldName} is not valid UTF-8.",
                exception);
        }
    }

    private static void WriteInt32(
        Stream stream,
        int value)
    {
        Span<byte> buffer =
            stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(
            buffer,
            value);
        stream.Write(buffer);
    }

    private static void WriteInt64(
        Stream stream,
        long value)
    {
        Span<byte> buffer =
            stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(
            buffer,
            value);
        stream.Write(buffer);
    }

    private static int ReadInt32(
        Stream stream,
        string fieldName)
    {
        Span<byte> buffer =
            stackalloc byte[sizeof(int)];
        try
        {
            ReadExactly(stream, buffer);
        }
        catch (EndOfStreamException exception)
        {
            throw InvalidPackage(
                $"The retained package {fieldName} is truncated.",
                exception);
        }
        return BinaryPrimitives
            .ReadInt32BigEndian(buffer);
    }

    private static long ReadInt64(
        Stream stream,
        string fieldName)
    {
        Span<byte> buffer =
            stackalloc byte[sizeof(long)];
        try
        {
            ReadExactly(stream, buffer);
        }
        catch (EndOfStreamException exception)
        {
            throw InvalidPackage(
                $"The retained package {fieldName} is truncated.",
                exception);
        }
        return BinaryPrimitives
            .ReadInt64BigEndian(buffer);
    }

    private static void ReadExactly(
        Stream stream,
        Span<byte> destination)
    {
        int read = 0;
        while (read < destination.Length)
        {
            int current =
                stream.Read(destination[read..]);
            if (current == 0)
                throw new EndOfStreamException();
            read += current;
        }
    }

    private static void ReadExactly(
        Stream stream,
        byte[] destination) =>
        ReadExactly(
            stream,
            destination.AsSpan());

    private static void AppendDomain(
        IncrementalHash hash,
        string domain)
    {
        AppendString(hash, domain);
        Span<byte> terminator =
            stackalloc byte[1];
        hash.AppendData(terminator);
    }

    private static void AppendStringList(
        IncrementalHash hash,
        IReadOnlyList<string> values)
    {
        AppendInt32(hash, values.Count);
        foreach (string value in values)
            AppendString(hash, value);
    }

    private static void AppendString(
        IncrementalHash hash,
        string value)
    {
        int byteCount =
            GetUtf8ByteCount(value, "digest text");
        AppendInt32(hash, byteCount);
        byte[] bytes = StrictUtf8.GetBytes(value);
        try
        {
            hash.AppendData(bytes);
        }
        finally
        {
            CryptographicOperations
                .ZeroMemory(bytes);
        }
    }

    private static void AppendInt32(
        IncrementalHash hash,
        int value)
    {
        Span<byte> buffer =
            stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(
            buffer,
            value);
        hash.AppendData(buffer);
    }

    private static void AppendInt64(
        IncrementalHash hash,
        long value)
    {
        Span<byte> buffer =
            stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(
            buffer,
            value);
        hash.AppendData(buffer);
    }

    private static RetainedMigrationPackageException
        InvalidPackage(string message) =>
        new(message);

    private static RetainedMigrationPackageLimitException
        LimitExceeded(string message) =>
        new(message);

    private static RetainedMigrationPackageException
        InvalidPackage(
        string message,
        Exception innerException) =>
        new(message, innerException);

    private sealed class HashedStreamWriter(
        Stream stream,
        IncrementalHash hash)
    {
        public void WriteByte(byte value)
        {
            Span<byte> buffer =
                stackalloc byte[1];
            buffer[0] = value;
            Write(buffer);
        }

        public void WriteInt32(int value)
        {
            Span<byte> buffer =
                stackalloc byte[sizeof(int)];
            BinaryPrimitives.WriteInt32BigEndian(
                buffer,
                value);
            Write(buffer);
        }

        public void WriteInt64(long value)
        {
            Span<byte> buffer =
                stackalloc byte[sizeof(long)];
            BinaryPrimitives.WriteInt64BigEndian(
                buffer,
                value);
            Write(buffer);
        }

        public void WriteString(string value)
        {
            int byteCount =
                GetUtf8ByteCount(
                    value,
                    "row text");
            WriteInt32(byteCount);
            byte[] bytes =
                StrictUtf8.GetBytes(value);
            try
            {
                Write(bytes);
            }
            finally
            {
                CryptographicOperations
                    .ZeroMemory(bytes);
            }
        }

        public void Write(ReadOnlySpan<byte> bytes)
        {
            stream.Write(bytes);
            hash.AppendData(bytes);
        }
    }

    private sealed class HashedStreamReader(
        Stream stream,
        IncrementalHash? hash)
    {
        private long? endPosition;

        public void LimitTo(long value)
        {
            if (value < stream.Position ||
                endPosition is not null)
            {
                throw InvalidPackage(
                    "A retained row payload boundary is invalid.");
            }
            endPosition = value;
        }

        public byte ReadByte(string fieldName)
        {
            Span<byte> buffer =
                stackalloc byte[1];
            ReadExactly(buffer, fieldName);
            return buffer[0];
        }

        public int ReadInt32(string fieldName)
        {
            Span<byte> buffer =
                stackalloc byte[sizeof(int)];
            ReadExactly(buffer, fieldName);
            return BinaryPrimitives
                .ReadInt32BigEndian(buffer);
        }

        public long ReadInt64(string fieldName)
        {
            Span<byte> buffer =
                stackalloc byte[sizeof(long)];
            ReadExactly(buffer, fieldName);
            return BinaryPrimitives
                .ReadInt64BigEndian(buffer);
        }

        public string ReadString(
            int maximumBytes,
            string fieldName)
        {
            int length =
                ReadInt32(fieldName + " length");
            if (length < 0 ||
                length > maximumBytes)
            {
                throw LimitExceeded(
                    $"A retained row {fieldName} exceeds its configured bound.");
            }
            return ReadUtf8(length, fieldName);
        }

        public string ReadUtf8(
            int length,
            string fieldName)
        {
            byte[] bytes =
                ReadBytes(length, fieldName);
            try
            {
                return StrictUtf8
                    .GetString(bytes);
            }
            catch (DecoderFallbackException exception)
            {
                throw InvalidPackage(
                    $"A retained row {fieldName} is not valid UTF-8.",
                    exception);
            }
        }

        public byte[] ReadBytes(
            int length,
            string fieldName)
        {
            EnsureAvailable(
                length,
                fieldName);
            byte[] bytes = new byte[length];
            ReadExactly(bytes, fieldName);
            return bytes;
        }

        public void ReadExactly(
            Span<byte> destination,
            string fieldName)
        {
            EnsureAvailable(
                destination.Length,
                fieldName);
            try
            {
                RetainedMigrationBinaryCodec
                    .ReadExactly(
                        stream,
                        destination);
            }
            catch (EndOfStreamException exception)
            {
                throw InvalidPackage(
                    $"A retained row {fieldName} is truncated.",
                    exception);
            }
            hash?.AppendData(destination);
        }

        private void EnsureAvailable(
            int length,
            string fieldName)
        {
            if (length < 0)
            {
                throw InvalidPackage(
                    $"A retained row {fieldName} length is invalid.");
            }
            if (endPosition is long end &&
                (stream.Position > end ||
                 length > end - stream.Position))
            {
                throw InvalidPackage(
                    $"A retained row {fieldName} extends beyond its declared payload.");
            }
        }
    }

    private sealed class BoundedMemoryStream(
        int maximumBytes,
        string limitMessage) : MemoryStream
    {
        public override void Write(
            byte[] buffer,
            int offset,
            int count)
        {
            EnsureCapacityFor(count);
            base.Write(buffer, offset, count);
        }

        public override void Write(
            ReadOnlySpan<byte> buffer)
        {
            EnsureCapacityFor(buffer.Length);
            base.Write(buffer);
        }

        public override void WriteByte(byte value)
        {
            EnsureCapacityFor(1);
            base.WriteByte(value);
        }

        private void EnsureCapacityFor(int count)
        {
            long required = checked(
                Position + count);
            if (count < 0 ||
                required > maximumBytes)
            {
                throw LimitExceeded(limitMessage);
            }
            if (required <= Capacity)
                return;

            long doubled = Math.Max(
                256L,
                (long)Capacity * 2);
            Capacity = checked((int)Math.Min(
                maximumBytes,
                Math.Max(required, doubled)));
        }
    }
}

internal sealed record RetainedPackageTableBinding(
    RetainedMigrationTableDescriptor Descriptor,
    long RowCount,
    long RelativeOffset,
    long SectionLength,
    string SectionDigest);

internal sealed record ParsedRetainedPackage(
    string CatalogDigest,
    MigrationSourceKind SourceKind,
    string SourceIdentity,
    string SourceFingerprint,
    string SnapshotIdentity,
    string ContentDigest,
    string CatalogJson,
    long BodyOffset,
    ReadOnlyCollection<RetainedPackageTableBinding> Tables);

internal sealed record DecodedRetainedRow(
    MigrationDataRow Row,
    long EncodedBytes);
