using System.Buffers.Binary;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace CSharpDB.Migration.Retained;

internal static class RetainedMigrationCursorCodec
{
    // Cursor hashes detect corruption and bind a position to one package and
    // read policy. They are intentionally not authentication tokens; callers
    // must keep persisted checkpoints in trusted storage.
    public const string AlgorithmId =
        "csharpdb-retained-row-cursor-v1";

    private const int MaximumCharacters = 256;

    private static UTF8Encoding StrictUtf8 { get; } =
        new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    public static string ComputeScope(
        string packageDigest,
        string catalogDigest,
        string sourceIdentity,
        string sourceFingerprint,
        string snapshotIdentity,
        string sourceObjectId,
        IReadOnlyList<string> columnObjectIds,
        int batchSize,
        long maxBatchBytes,
        int maxValueBytes,
        string rejectContractVersion)
    {
        using IncrementalHash hash =
            IncrementalHash.CreateHash(
                HashAlgorithmName.SHA256);
        Append(hash, AlgorithmId + "/scope");
        Append(hash, packageDigest);
        Append(hash, catalogDigest);
        Append(hash, sourceIdentity);
        Append(hash, sourceFingerprint);
        Append(hash, snapshotIdentity);
        Append(hash, sourceObjectId);
        Append(hash, batchSize.ToString(
            CultureInfo.InvariantCulture));
        Append(hash, maxBatchBytes.ToString(
            CultureInfo.InvariantCulture));
        Append(hash, maxValueBytes.ToString(
            CultureInfo.InvariantCulture));
        Append(hash, rejectContractVersion);
        Append(hash, columnObjectIds.Count.ToString(
            CultureInfo.InvariantCulture));
        foreach (string columnObjectId in
                 columnObjectIds)
        {
            Append(hash, columnObjectId);
        }
        return RetainedMigrationBinaryCodec
            .FormatDigest(hash.GetHashAndReset());
    }

    public static string Encode(
        long nextRowOrdinal,
        long nextRelativeOffset,
        long nextBatchOrdinal,
        string scopeDigest)
    {
        string token = Token(
            nextRowOrdinal,
            nextRelativeOffset,
            nextBatchOrdinal,
            scopeDigest);
        return string.Join(
            '/',
            AlgorithmId,
            nextRowOrdinal.ToString(
                CultureInfo.InvariantCulture),
            nextRelativeOffset.ToString(
                CultureInfo.InvariantCulture),
            nextBatchOrdinal.ToString(
                CultureInfo.InvariantCulture),
            token.AsSpan("sha256:".Length)
                .ToString());
    }

    public static Position Parse(
        string cursor,
        string expectedScopeDigest)
    {
        if (string.IsNullOrEmpty(cursor) ||
            cursor.Length > MaximumCharacters)
        {
            throw InvalidCursor();
        }
        string[] parts = cursor.Split('/');
        if (parts.Length != 5 ||
            !string.Equals(
                parts[0],
                AlgorithmId,
                StringComparison.Ordinal) ||
            !TryParseCanonicalNonnegative(
                parts[1],
                out long rowOrdinal) ||
            !TryParseCanonicalNonnegative(
                parts[2],
                out long relativeOffset) ||
            !TryParseCanonicalNonnegative(
                parts[3],
                out long batchOrdinal) ||
            rowOrdinal <= 0 ||
            relativeOffset <= 0 ||
            batchOrdinal <= 0 ||
            parts[4].Length != 64 ||
            parts[4].AsSpan().ContainsAnyExcept(
                "0123456789abcdef".AsSpan()))
        {
            throw InvalidCursor();
        }

        string expected = Token(
            rowOrdinal,
            relativeOffset,
            batchOrdinal,
            expectedScopeDigest);
        byte[] expectedBytes =
            Convert.FromHexString(
                expected.AsSpan(7));
        byte[] actualBytes;
        try
        {
            actualBytes =
                Convert.FromHexString(parts[4]);
        }
        catch (FormatException)
        {
            CryptographicOperations
                .ZeroMemory(expectedBytes);
            throw InvalidCursor();
        }
        try
        {
            if (!CryptographicOperations
                    .FixedTimeEquals(
                        expectedBytes,
                        actualBytes))
            {
                throw InvalidCursor();
            }
        }
        finally
        {
            CryptographicOperations
                .ZeroMemory(expectedBytes);
            CryptographicOperations
                .ZeroMemory(actualBytes);
        }

        return new Position(
            cursor,
            rowOrdinal,
            relativeOffset,
            batchOrdinal);
    }

    private static string Token(
        long rowOrdinal,
        long relativeOffset,
        long batchOrdinal,
        string scopeDigest)
    {
        if (!RetainedMigrationBinaryCodec
                .IsCanonicalDigest(
                    scopeDigest))
        {
            throw new InvalidDataException(
                "The retained cursor scope is invalid.");
        }
        using IncrementalHash hash =
            IncrementalHash.CreateHash(
                HashAlgorithmName.SHA256);
        Append(hash, AlgorithmId + "/token");
        Append(hash, scopeDigest);
        Append(
            hash,
            rowOrdinal.ToString(
                CultureInfo.InvariantCulture));
        Append(
            hash,
            relativeOffset.ToString(
                CultureInfo.InvariantCulture));
        Append(
            hash,
            batchOrdinal.ToString(
                CultureInfo.InvariantCulture));
        return RetainedMigrationBinaryCodec
            .FormatDigest(hash.GetHashAndReset());
    }

    private static void Append(
        IncrementalHash hash,
        string value)
    {
        byte[] bytes =
            StrictUtf8.GetBytes(value);
        Span<byte> length =
            stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(
            length,
            bytes.Length);
        hash.AppendData(length);
        hash.AppendData(bytes);
        CryptographicOperations
            .ZeroMemory(bytes);
    }

    private static bool
        TryParseCanonicalNonnegative(
        string text,
        out long value)
    {
        value = 0;
        if (string.IsNullOrEmpty(text) ||
            text[0] is '+' or '-' ||
            (text.Length > 1 &&
             text[0] == '0'))
        {
            return false;
        }
        return long.TryParse(
            text,
            NumberStyles.None,
            CultureInfo.InvariantCulture,
            out value);
    }

    private static InvalidDataException
        InvalidCursor() =>
        new(
            "The retained resume cursor is malformed, tampered, stale, or bound to a different package/read policy.");

    internal sealed record Position(
        string Original,
        long RowOrdinal,
        long RelativeOffset,
        long BatchOrdinal);
}
