using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace CSharpDB.Migration;

/// <summary>
/// Canonical digest for the ordered, bounded reject set attached to one
/// attempted source batch. The digest contains no exception messages.
/// </summary>
public static class MigrationRejectDigest
{
    private static readonly UTF8Encoding StrictUtf8 = new(
        encoderShouldEmitUTF8Identifier: false,
        throwOnInvalidBytes: true);

    public static string Compute(MigrationTargetBatch batch)
    {
        ArgumentNullException.ThrowIfNull(batch);
        IReadOnlyList<MigrationRejectedRow> rejectedRows = batch.RejectedRows ??
            throw new InvalidDataException("Migration target batch rejects cannot be null.");
        ValidateRejectedRows(rejectedRows);

        using IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        AppendString(hash, MigrationRejectContract.RejectSetV1);
        AppendString(hash, batch.RejectContractVersion);
        AppendString(hash, batch.PlanDigest);
        AppendString(hash, batch.CatalogDigest);
        AppendString(hash, batch.SourceFingerprint);
        AppendString(hash, batch.SourceSnapshotIdentity);
        AppendString(hash, batch.SourceObjectId);
        AppendInt64(hash, batch.BatchOrdinal);
        AppendNullableString(hash, batch.StartCursor);
        AppendNullableString(hash, batch.NextCursor);
        AppendInt32(hash, rejectedRows.Count);

        foreach (MigrationRejectedRow rejectedRow in rejectedRows)
        {
            AppendInt64(hash, rejectedRow.SourceRowOrdinal);
            AppendString(hash, rejectedRow.RuleId);
            AppendNullableString(hash, rejectedRow.ColumnObjectId);
            AppendInt32(hash, rejectedRow.Evidence.Count);
            foreach (MigrationRejectEvidence evidence in rejectedRow.Evidence)
            {
                AppendString(hash, evidence.Name);
                AppendNullableString(hash, evidence.Value);
            }
        }

        return Convert.ToHexString(hash.GetHashAndReset()).ToLowerInvariant();
    }

    internal static void ValidateRejectedRows(
        IReadOnlyList<MigrationRejectedRow> rejectedRows)
    {
        if (rejectedRows.Count > MigrationRejectContract.MaximumRejectedRowsPerBatch)
            throw new InvalidDataException("Migration reject count exceeds the contract ceiling.");

        long previousRowOrdinal = -1;
        long batchEvidenceBytes = 0;
        foreach (MigrationRejectedRow? rejectedRow in rejectedRows)
        {
            if (rejectedRow is null)
                throw new InvalidDataException("Migration reject rows cannot contain null values.");
            if (rejectedRow.SourceRowOrdinal < 0 ||
                rejectedRow.SourceRowOrdinal == long.MaxValue ||
                rejectedRow.SourceRowOrdinal <= previousRowOrdinal)
            {
                throw new InvalidDataException(
                    "Migration reject rows must use strictly increasing source ordinals.");
            }
            if (string.IsNullOrWhiteSpace(rejectedRow.RuleId) ||
                !MigrationRejectContract.IsBoundedRuleId(rejectedRow.RuleId))
            {
                throw new InvalidDataException("Migration reject rule ID is invalid.");
            }
            if (rejectedRow.ColumnObjectId is not null &&
                (string.IsNullOrWhiteSpace(rejectedRow.ColumnObjectId) ||
                 !MigrationRejectContract.IsBoundedIdentifier(rejectedRow.ColumnObjectId)))
            {
                throw new InvalidDataException("Migration reject column object ID is invalid.");
            }

            IReadOnlyList<MigrationRejectEvidence> evidenceItems = rejectedRow.Evidence ??
                throw new InvalidDataException("Migration reject evidence cannot be null.");
            if (evidenceItems.Count > MigrationRejectContract.MaximumEvidenceEntriesPerRow)
                throw new InvalidDataException("Migration reject evidence count exceeds the contract ceiling.");

            int evidenceBytes = 0;
            string? previousName = null;
            foreach (MigrationRejectEvidence? evidence in evidenceItems)
            {
                if (evidence is null)
                    throw new InvalidDataException("Migration reject evidence cannot contain null values.");
                if (!IsEvidenceName(evidence.Name) ||
                    (previousName is not null &&
                     string.CompareOrdinal(previousName, evidence.Name) >= 0))
                {
                    throw new InvalidDataException(
                        "Migration reject evidence names must be valid, unique, and ordinally ordered.");
                }

                int nameBytes = StrictByteCount(evidence.Name);
                int valueBytes = evidence.Value is null ? 0 : StrictByteCount(evidence.Value);
                if (valueBytes > MigrationRejectContract.MaximumEvidenceValueBytes)
                {
                    throw new InvalidDataException(
                        "Migration reject evidence value exceeds the contract ceiling.");
                }

                evidenceBytes = checked(evidenceBytes + nameBytes + valueBytes);
                if (evidenceBytes > MigrationRejectContract.MaximumEvidenceBytesPerRow)
                {
                    throw new InvalidDataException(
                        "Migration reject evidence exceeds the per-row contract ceiling.");
                }
                previousName = evidence.Name;
            }

            batchEvidenceBytes = checked(batchEvidenceBytes + evidenceBytes);
            if (batchEvidenceBytes > MigrationRejectContract.MaximumEvidenceBytesPerBatch)
            {
                throw new InvalidDataException(
                    "Migration reject evidence exceeds the per-batch contract ceiling.");
            }

            previousRowOrdinal = rejectedRow.SourceRowOrdinal;
        }
    }

    private static bool IsEvidenceName(string? value)
    {
        if (string.IsNullOrEmpty(value) ||
            value.Length > MigrationRejectContract.MaximumEvidenceNameCharacters ||
            value[0] is not (>= 'a' and <= 'z'))
        {
            return false;
        }

        return value.All(character =>
            character is >= 'a' and <= 'z' or
                >= 'A' and <= 'Z' or
                >= '0' and <= '9' or
                '_');
    }

    private static int StrictByteCount(string value)
    {
        try
        {
            return StrictUtf8.GetByteCount(value);
        }
        catch (EncoderFallbackException error)
        {
            throw new InvalidDataException(
                "Migration reject evidence must contain valid Unicode scalar data.",
                error);
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
        byte[] bytes;
        try
        {
            bytes = StrictUtf8.GetBytes(value);
        }
        catch (EncoderFallbackException error)
        {
            throw new InvalidDataException(
                "Migration reject digest input must contain valid Unicode scalar data.",
                error);
        }
        AppendBytes(hash, bytes);
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

/// <summary>
/// Validates the one-outcome-per-source-row invariant independently of a
/// provider's storage implementation.
/// </summary>
public static class MigrationBatchOutcomeValidator
{
    public static void Validate(
        MigrationTargetBatch batch,
        long expectedFirstSourceRowOrdinal,
        int maximumAttemptedRows)
    {
        ArgumentNullException.ThrowIfNull(batch);
        ArgumentOutOfRangeException.ThrowIfNegative(expectedFirstSourceRowOrdinal);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maximumAttemptedRows);

        IReadOnlyList<MigrationTargetRow> rows = batch.Rows ??
            throw new InvalidDataException("Migration target batch rows cannot be null.");
        IReadOnlyList<MigrationRejectedRow> rejectedRows = batch.RejectedRows ??
            throw new InvalidDataException("Migration target batch rejects cannot be null.");
        long attemptedRows = (long)rows.Count + rejectedRows.Count;
        if (attemptedRows == 0 || attemptedRows > maximumAttemptedRows)
        {
            throw new InvalidDataException(
                "Migration target batch attempted-row count is outside the execution bounds.");
        }

        if (batch.RejectContractVersion is not
            (MigrationRejectContract.DeterministicFailFastV1 or
             MigrationRejectContract.DeterministicRejectsV1))
        {
            throw new InvalidDataException("Migration target batch reject contract is unsupported.");
        }
        if (string.Equals(
                batch.RejectContractVersion,
                MigrationRejectContract.DeterministicFailFastV1,
                StringComparison.Ordinal) &&
            rejectedRows.Count != 0)
        {
            throw new InvalidDataException("Fail-fast batches cannot contain durable rejects.");
        }

        string expectedRejectDigest = MigrationRejectDigest.Compute(batch);
        if (!FixedTimeSha256Equals(expectedRejectDigest, batch.RejectDigest))
            throw new InvalidDataException("Migration target batch reject digest is invalid.");

        long previousAcceptedOrdinal = -1;
        foreach (MigrationTargetRow? row in rows)
        {
            if (row is null)
                throw new InvalidDataException("Migration target batch rows cannot contain null values.");
            if (row.SourceRowOrdinal < 0 ||
                row.SourceRowOrdinal == long.MaxValue ||
                row.SourceRowOrdinal <= previousAcceptedOrdinal)
            {
                throw new InvalidDataException(
                    "Accepted migration rows must use strictly increasing source ordinals.");
            }
            previousAcceptedOrdinal = row.SourceRowOrdinal;
        }

        int acceptedIndex = 0;
        int rejectedIndex = 0;
        long expectedOrdinal = expectedFirstSourceRowOrdinal;
        while (acceptedIndex < rows.Count || rejectedIndex < rejectedRows.Count)
        {
            long acceptedOrdinal = acceptedIndex < rows.Count
                ? rows[acceptedIndex].SourceRowOrdinal
                : long.MaxValue;
            long rejectedOrdinal = rejectedIndex < rejectedRows.Count
                ? rejectedRows[rejectedIndex].SourceRowOrdinal
                : long.MaxValue;
            if (acceptedOrdinal == rejectedOrdinal ||
                Math.Min(acceptedOrdinal, rejectedOrdinal) != expectedOrdinal)
            {
                throw new InvalidDataException(
                    "Migration batch outcomes must cover one contiguous source-row interval exactly once.");
            }

            if (acceptedOrdinal < rejectedOrdinal)
                acceptedIndex++;
            else
                rejectedIndex++;
            expectedOrdinal = checked(expectedOrdinal + 1);
        }
    }

    internal static bool FixedTimeSha256Equals(string expected, string? actual)
    {
        if (!IsLowerSha256(expected) || !IsLowerSha256(actual))
            return false;
        byte[] expectedBytes = Convert.FromHexString(expected);
        byte[] actualBytes = Convert.FromHexString(actual!);
        return CryptographicOperations.FixedTimeEquals(expectedBytes, actualBytes);
    }

    private static bool IsLowerSha256(string? value) =>
        value is { Length: 64 } &&
        value.All(character =>
            character is >= '0' and <= '9' or
                >= 'a' and <= 'f');
}
