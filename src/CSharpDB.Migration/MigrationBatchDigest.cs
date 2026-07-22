using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Primitives;

namespace CSharpDB.Migration;

/// <summary>
/// Canonical digest for a converted migration batch. The digest binds the
/// execution identities, cursor chain, ordered target columns, stable row
/// identities, and exact target value tags/payloads.
/// </summary>
public static class MigrationBatchDigest
{
    public const string Format = "csharpdb-migration-batch/v1";

    public static string Compute(MigrationTargetBatch batch)
    {
        ArgumentNullException.ThrowIfNull(batch);
        using IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);

        AppendString(hash, Format);
        AppendString(hash, batch.PlanDigest);
        AppendString(hash, batch.CatalogDigest);
        AppendString(hash, batch.SourceFingerprint);
        AppendString(hash, batch.SourceSnapshotIdentity);
        AppendString(hash, batch.SourceObjectId);
        AppendInt64(hash, batch.BatchOrdinal);
        AppendNullableString(hash, batch.StartCursor);
        AppendNullableString(hash, batch.NextCursor);

        IReadOnlyList<string> columns = batch.ColumnObjectIds ??
            throw new InvalidDataException("Migration target batch columns cannot be null.");
        AppendInt32(hash, columns.Count);
        foreach (string column in columns)
            AppendString(hash, column);

        IReadOnlyList<MigrationTargetRow> rows = batch.Rows ??
            throw new InvalidDataException("Migration target batch rows cannot be null.");
        AppendInt32(hash, rows.Count);
        foreach (MigrationTargetRow row in rows)
        {
            if (row is null)
                throw new InvalidDataException("Migration target batch rows cannot contain null values.");

            AppendInt64(hash, row.SourceRowOrdinal);
            AppendNullableString(hash, row.StableKey);
            IReadOnlyList<DbValue> values = row.Values ??
                throw new InvalidDataException("Migration target row values cannot be null.");
            AppendInt32(hash, values.Count);
            foreach (DbValue value in values)
                AppendValue(hash, value);
        }

        return Convert.ToHexString(hash.GetHashAndReset()).ToLowerInvariant();
    }

    private static void AppendValue(IncrementalHash hash, DbValue value)
    {
        AppendInt32(hash, (int)value.Type);
        switch (value.Type)
        {
            case DbType.Null:
                return;
            case DbType.Integer:
                AppendInt64(hash, value.AsInteger);
                return;
            case DbType.Real:
                AppendInt64(hash, BitConverter.DoubleToInt64Bits(value.AsReal));
                return;
            case DbType.Text:
                AppendString(hash, value.AsText);
                return;
            case DbType.Blob:
                AppendBytes(hash, value.AsBlob);
                return;
            default:
                throw new InvalidDataException($"Unsupported target value tag '{value.Type}'.");
        }
    }

    private static void AppendNullableString(IncrementalHash hash, string? value)
    {
        if (value is null)
        {
            AppendInt32(hash, -1);
            return;
        }

        AppendString(hash, value);
    }

    private static void AppendString(IncrementalHash hash, string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        AppendBytes(hash, Encoding.UTF8.GetBytes(value));
    }

    private static void AppendBytes(IncrementalHash hash, ReadOnlySpan<byte> value)
    {
        AppendInt32(hash, value.Length);
        hash.AppendData(value);
    }

    private static void AppendInt32(IncrementalHash hash, int value)
    {
        Span<byte> bytes = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(bytes, value);
        hash.AppendData(bytes);
    }

    private static void AppendInt64(IncrementalHash hash, long value)
    {
        Span<byte> bytes = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(bytes, value);
        hash.AppendData(bytes);
    }
}
