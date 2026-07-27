using System.Globalization;
using CSharpDB.Migration;

namespace CSharpDB.Migration.Sqlite;

internal static class SqliteCursorCodec
{
    public const string AlgorithmId = "csharpdb-sqlite-rowid-cursor-v1";

    private const int MaximumCharacters = 192;

    public static string ComputeScope(
        string sourceFingerprint,
        string snapshotIdentity,
        string catalogDigest,
        string tableObjectId,
        IReadOnlyList<string> columnObjectIds,
        int batchSize,
        long maxBatchBytes,
        int maxValueBytes)
    {
        var components = new string?[9 + columnObjectIds.Count];
        components[0] = sourceFingerprint;
        components[1] = snapshotIdentity;
        components[2] = catalogDigest;
        components[3] = tableObjectId;
        components[4] = batchSize.ToString(CultureInfo.InvariantCulture);
        components[5] = maxBatchBytes.ToString(CultureInfo.InvariantCulture);
        components[6] = maxValueBytes.ToString(CultureInfo.InvariantCulture);
        components[7] = MigrationRejectContract.DeterministicFailFastV1;
        components[8] = columnObjectIds.Count.ToString(CultureInfo.InvariantCulture);
        for (int index = 0; index < columnObjectIds.Count; index++)
            components[9 + index] = columnObjectIds[index];
        return "sha256:" + SqliteStableDigest.Text(AlgorithmId, components);
    }

    public static string Encode(
        long lastRowId,
        long nextBatchOrdinal,
        long nextSourceRowOrdinal,
        string scopeDigest)
    {
        string token = Token(
            lastRowId,
            nextBatchOrdinal,
            nextSourceRowOrdinal,
            scopeDigest);
        return string.Join(
            '/',
            AlgorithmId,
            lastRowId.ToString(CultureInfo.InvariantCulture),
            nextBatchOrdinal.ToString(CultureInfo.InvariantCulture),
            nextSourceRowOrdinal.ToString(CultureInfo.InvariantCulture),
            token);
    }

    public static Position Parse(string cursor, string expectedScopeDigest)
    {
        if (string.IsNullOrEmpty(cursor) || cursor.Length > MaximumCharacters)
            throw InvalidCursor();

        string[] parts = cursor.Split('/');
        if (parts.Length != 5 ||
            !string.Equals(parts[0], AlgorithmId, StringComparison.Ordinal) ||
            !TryParseCanonicalInt64(parts[1], allowNegative: true, out long lastRowId) ||
            !TryParseCanonicalInt64(parts[2], allowNegative: false, out long batchOrdinal) ||
            !TryParseCanonicalInt64(
                parts[3],
                allowNegative: false,
                out long sourceRowOrdinal) ||
            batchOrdinal <= 0 ||
            sourceRowOrdinal <= 0 ||
            parts[4].Length != 64 ||
            parts[4].Any(character =>
                character is not (>= '0' and <= '9') and not (>= 'a' and <= 'f')) ||
            !string.Equals(
                parts[4],
                Token(
                    lastRowId,
                    batchOrdinal,
                    sourceRowOrdinal,
                    expectedScopeDigest),
                StringComparison.Ordinal))
        {
            throw InvalidCursor();
        }

        return new Position(
            cursor,
            lastRowId,
            batchOrdinal,
            sourceRowOrdinal);
    }

    private static string Token(
        long lastRowId,
        long nextBatchOrdinal,
        long nextSourceRowOrdinal,
        string scopeDigest)
    {
        if (scopeDigest.Length != 71 ||
            !scopeDigest.StartsWith("sha256:", StringComparison.Ordinal))
        {
            throw new InvalidDataException("The SQLite cursor scope is invalid.");
        }

        return SqliteStableDigest.Text(
            AlgorithmId + "/token",
            scopeDigest,
            lastRowId.ToString(CultureInfo.InvariantCulture),
            nextBatchOrdinal.ToString(CultureInfo.InvariantCulture),
            nextSourceRowOrdinal.ToString(CultureInfo.InvariantCulture));
    }

    private static bool TryParseCanonicalInt64(
        string text,
        bool allowNegative,
        out long value)
    {
        value = 0;
        if (string.IsNullOrEmpty(text) ||
            text[0] == '+' ||
            (!allowNegative && text[0] == '-') ||
            (text.Length > 1 && text[0] == '0') ||
            (text.StartsWith("-0", StringComparison.Ordinal)))
        {
            return false;
        }

        return long.TryParse(
            text,
            NumberStyles.AllowLeadingSign,
            CultureInfo.InvariantCulture,
            out value);
    }

    private static InvalidDataException InvalidCursor() => new(
        "The SQLite resume cursor is malformed or does not match this snapshot and read policy.");

    internal sealed record Position(
        string Original,
        long LastRowId,
        long BatchOrdinal,
        long SourceRowOrdinal);
}
