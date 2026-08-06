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
    private static readonly UTF8Encoding StrictUtf8 = new(
        encoderShouldEmitUTF8Identifier: false,
        throwOnInvalidBytes: true);

    public const string LegacyFormat = "csharpdb-migration-batch/v1";

    public const string Format = "csharpdb-migration-batch/v2";

    public static string Compute(MigrationTargetBatch batch) => Compute(batch, Format);

    public static string Compute(MigrationTargetBatch batch, string format)
    {
        ArgumentNullException.ThrowIfNull(batch);
        return format switch
        {
            LegacyFormat => ComputeV1(batch),
            Format => ComputeV2(batch),
            _ => throw new InvalidDataException("Migration batch digest format is unsupported."),
        };
    }

    private static string ComputeV2(MigrationTargetBatch batch)
    {
        IReadOnlyList<MigrationTargetRow> rows = batch.Rows ??
            throw new InvalidDataException("Migration target batch rows cannot be null.");
        IReadOnlyList<MigrationRejectedRow> rejectedRows = batch.RejectedRows ??
            throw new InvalidDataException("Migration target batch rejects cannot be null.");
        long expectedFirstSourceRowOrdinal = FirstSourceRowOrdinal(rows, rejectedRows);
        MigrationBatchOutcomeValidator.Validate(
            batch,
            expectedFirstSourceRowOrdinal,
            int.MaxValue);

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
        AppendString(hash, batch.RejectContractVersion);
        AppendString(hash, batch.RejectDigest);

        IReadOnlyList<string> columns = batch.ColumnObjectIds ??
            throw new InvalidDataException("Migration target batch columns cannot be null.");
        AppendInt32(hash, columns.Count);
        foreach (string column in columns)
            AppendString(hash, column);

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

        AppendInt32(hash, checked(rows.Count + rejectedRows.Count));
        int acceptedIndex = 0;
        int rejectedIndex = 0;
        while (acceptedIndex < rows.Count || rejectedIndex < rejectedRows.Count)
        {
            long acceptedOrdinal = acceptedIndex < rows.Count
                ? rows[acceptedIndex].SourceRowOrdinal
                : long.MaxValue;
            long rejectedOrdinal = rejectedIndex < rejectedRows.Count
                ? rejectedRows[rejectedIndex].SourceRowOrdinal
                : long.MaxValue;
            if (acceptedOrdinal < rejectedOrdinal)
            {
                AppendInt32(hash, 0);
                AppendInt64(hash, acceptedOrdinal);
                acceptedIndex++;
            }
            else
            {
                AppendInt32(hash, 1);
                AppendInt64(hash, rejectedOrdinal);
                rejectedIndex++;
            }
        }

        return Convert.ToHexString(hash.GetHashAndReset()).ToLowerInvariant();
    }

    private static string ComputeV1(MigrationTargetBatch batch)
    {
        if (!string.Equals(
                batch.RejectContractVersion,
                MigrationRejectContract.DeterministicFailFastV1,
                StringComparison.Ordinal) ||
            batch.RejectedRows is null ||
            batch.RejectedRows.Count != 0)
        {
            throw new InvalidDataException(
                "The legacy migration batch digest supports only fail-fast batches.");
        }

        using IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        AppendStringV1(hash, LegacyFormat);
        AppendStringV1(hash, batch.PlanDigest);
        AppendStringV1(hash, batch.CatalogDigest);
        AppendStringV1(hash, batch.SourceFingerprint);
        AppendStringV1(hash, batch.SourceSnapshotIdentity);
        AppendStringV1(hash, batch.SourceObjectId);
        AppendInt64(hash, batch.BatchOrdinal);
        AppendNullableStringV1(hash, batch.StartCursor);
        AppendNullableStringV1(hash, batch.NextCursor);

        IReadOnlyList<string> columns = batch.ColumnObjectIds ??
            throw new InvalidDataException("Migration target batch columns cannot be null.");
        AppendInt32(hash, columns.Count);
        foreach (string column in columns)
            AppendStringV1(hash, column);

        IReadOnlyList<MigrationTargetRow> rows = batch.Rows ??
            throw new InvalidDataException("Migration target batch rows cannot be null.");
        AppendInt32(hash, rows.Count);
        foreach (MigrationTargetRow row in rows)
        {
            if (row is null)
                throw new InvalidDataException("Migration target batch rows cannot contain null values.");

            AppendInt64(hash, row.SourceRowOrdinal);
            AppendNullableStringV1(hash, row.StableKey);
            IReadOnlyList<DbValue> values = row.Values ??
                throw new InvalidDataException("Migration target row values cannot be null.");
            AppendInt32(hash, values.Count);
            foreach (DbValue value in values)
                AppendValueV1(hash, value);
        }

        return Convert.ToHexString(hash.GetHashAndReset()).ToLowerInvariant();
    }

    private static long FirstSourceRowOrdinal(
        IReadOnlyList<MigrationTargetRow> rows,
        IReadOnlyList<MigrationRejectedRow> rejectedRows)
    {
        MigrationTargetRow? firstAccepted = rows.Count == 0 ? null : rows[0];
        MigrationRejectedRow? firstRejected = rejectedRows.Count == 0 ? null : rejectedRows[0];
        if (rows.Count != 0 && firstAccepted is null)
            throw new InvalidDataException("Migration target batch rows cannot contain null values.");
        if (rejectedRows.Count != 0 && firstRejected is null)
            throw new InvalidDataException("Migration reject rows cannot contain null values.");

        long accepted = firstAccepted?.SourceRowOrdinal ?? long.MaxValue;
        long rejected = firstRejected?.SourceRowOrdinal ?? long.MaxValue;
        long first = Math.Min(accepted, rejected);
        if (first == long.MaxValue)
            throw new InvalidDataException("Migration target batches must contain at least one outcome.");
        return first;
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
            case DbType.Decimal:
                AppendInt64(hash, value.DecimalCoefficient);
                AppendInt32(hash, value.DecimalScale);
                return;
            default:
                throw new InvalidDataException($"Unsupported target value tag '{value.Type}'.");
        }
    }

    private static void AppendValueV1(IncrementalHash hash, DbValue value)
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
                AppendStringV1(hash, value.AsText);
                return;
            case DbType.Blob:
                AppendBytes(hash, value.AsBlob);
                return;
            case DbType.Decimal:
                AppendInt64(hash, value.DecimalCoefficient);
                AppendInt32(hash, value.DecimalScale);
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

    private static void AppendNullableStringV1(IncrementalHash hash, string? value)
    {
        if (value is null)
        {
            AppendInt32(hash, -1);
            return;
        }

        AppendStringV1(hash, value);
    }

    private static void AppendString(IncrementalHash hash, string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        try
        {
            AppendBytes(hash, StrictUtf8.GetBytes(value));
        }
        catch (EncoderFallbackException error)
        {
            throw new InvalidDataException(
                "Migration batch digest input must contain valid Unicode scalar data.",
                error);
        }
    }

    private static void AppendStringV1(IncrementalHash hash, string value)
    {
        // Valid scalar text hashes byte-for-byte like the historical v1
        // algorithm. Invalid UTF-16 now fails closed because replacement
        // fallback made distinct replay payloads collide.
        AppendString(hash, value);
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
