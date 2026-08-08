using System.Runtime.InteropServices;
using System.Security.Cryptography;

namespace CSharpDB.Migration.Files.Json;

/// <summary>
/// Bounded byte geometry for one durable JSON export prefix.
/// </summary>
internal readonly record struct JsonExportCheckpointPrefixGeometry(
    long MinimumObjectByteLength,
    long MinimumDataPrefixByteLength,
    long MaximumDataPrefixByteLength,
    int CompletionTailByteLength);

/// <summary>
/// Platform-neutral validation for durable JSON and NDJSON prefix boundaries.
/// This type deliberately performs no file access or JSON parsing.
/// </summary>
internal static class JsonExportCheckpointFraming
{
    private static ReadOnlySpan<byte> RootArrayWritingEmpty =>
        "["u8;

    private static ReadOnlySpan<byte> RootArrayCompleteEmpty =>
        "[]\n"u8;

    private static ReadOnlySpan<byte> RootArrayWritingRowEnd =>
        "}"u8;

    private static ReadOnlySpan<byte> RootArrayCompleteRowEnd =>
        "}]\n"u8;

    private static ReadOnlySpan<byte> NdjsonRowEnd =>
        "}\n"u8;

    /// <summary>
    /// Validates row-boundary metadata and returns its exact bounded byte
    /// geometry. A root-array Writing prefix always reserves the two bytes
    /// needed for <c>]\n</c>.
    /// </summary>
    internal static JsonExportCheckpointPrefixGeometry ValidateGeometry(
        JsonExportCheckpointBinding binding,
        JsonExportCheckpointPhase phase,
        JsonExportCheckpointProgress progress)
    {
        ArgumentNullException.ThrowIfNull(binding);
        ArgumentNullException.ThrowIfNull(progress);

        JsonExportFormatManifest json =
            binding.Json ??
            throw Invalid(
                "JSON export checkpoint format evidence is missing.");
        JsonExportTableManifest table =
            binding.Table ??
            throw Invalid(
                "JSON export checkpoint table evidence is missing.");
        IReadOnlyList<JsonExportColumnManifest> columns =
            table.Columns ??
            throw Invalid(
                "JSON export checkpoint columns are missing.");

        if (!Enum.IsDefined(phase))
        {
            throw Invalid(
                "JSON export checkpoint phase is unsupported.");
        }
        if (!Enum.IsDefined(json.Framing))
        {
            throw Invalid(
                "JSON export checkpoint framing is unsupported.");
        }
        if (progress.CompletedRowCount < 0)
        {
            throw Invalid(
                "JSON export checkpoint row count cannot be negative.");
        }
        if (progress.DataPrefixByteLength < 0)
        {
            throw Invalid(
                "JSON export checkpoint data-prefix length cannot be negative.");
        }
        if ((progress.CompletedRowCount == 0) !=
            (progress.LastCompletedRowId is null))
        {
            throw Invalid(
                "JSON export checkpoint row-id evidence must be present if and only if at least one row is complete.");
        }
        if (json.MaxDataBytes <= 0)
        {
            throw Invalid(
                "JSON export checkpoint data-byte ceiling must be positive.");
        }
        if (json.MaximumValueBytes <= 0)
        {
            throw Invalid(
                "JSON export checkpoint value-byte ceiling must be positive.");
        }

        long minimumObjectByteLength =
            GetMinimumObjectByteLength(columns);
        if (minimumObjectByteLength >
            json.MaximumValueBytes)
        {
            throw Invalid(
                "JSON export checkpoint schema exceeds its value-byte ceiling.");
        }

        (
            long minimumPrefixByteLength,
            long rowGeometryMaximum,
            int completionTailByteLength) =
            GetPrefixGeometry(
                phase,
                json.Framing,
                progress.CompletedRowCount,
                minimumObjectByteLength,
                json.MaximumValueBytes);

        long ceilingMaximum;
        try
        {
            ceilingMaximum =
                completionTailByteLength == 0
                    ? json.MaxDataBytes
                    : checked(
                        json.MaxDataBytes -
                        completionTailByteLength);
        }
        catch (OverflowException)
        {
            throw Invalid(
                "JSON export checkpoint byte ceiling exceeds bounded prefix geometry.");
        }

        long maximumPrefixByteLength =
            Math.Min(
                rowGeometryMaximum,
                ceilingMaximum);
        if (maximumPrefixByteLength <
            minimumPrefixByteLength)
        {
            throw Invalid(
                "JSON export checkpoint byte ceiling cannot contain the represented complete-row prefix.");
        }
        if (progress.DataPrefixByteLength <
                minimumPrefixByteLength ||
            progress.DataPrefixByteLength >
                maximumPrefixByteLength)
        {
            throw Invalid(
                "JSON export checkpoint data-prefix length is outside its row-count, schema, framing, and resource geometry.");
        }

        return new JsonExportCheckpointPrefixGeometry(
            minimumObjectByteLength,
            minimumPrefixByteLength,
            maximumPrefixByteLength,
            completionTailByteLength);
    }

    /// <summary>
    /// Validates the observed first byte and exact trailing boundary window
    /// without requiring the prepared output to be loaded into memory.
    /// </summary>
    internal static JsonExportCheckpointPrefixGeometry
        ValidateObservedBoundary(
        JsonExportCheckpointBinding binding,
        JsonExportCheckpointPhase phase,
        JsonExportCheckpointProgress progress,
        byte? firstByte,
        ReadOnlySpan<byte> trailingBoundary)
    {
        JsonExportCheckpointPrefixGeometry geometry =
            ValidateGeometry(
                binding,
                phase,
                progress);
        JsonExportFraming framing =
            binding.Json.Framing;
        bool hasRows =
            progress.CompletedRowCount > 0;

        byte? expectedFirstByte;
        ReadOnlySpan<byte> expectedTrailingBoundary;
        switch (framing)
        {
            case JsonExportFraming.RootArray:
                expectedFirstByte = (byte)'[';
                expectedTrailingBoundary =
                    (phase, hasRows) switch
                    {
                        (
                            JsonExportCheckpointPhase.Writing,
                            false) =>
                            RootArrayWritingEmpty,
                        (
                            JsonExportCheckpointPhase.Writing,
                            true) =>
                            RootArrayWritingRowEnd,
                        (
                            JsonExportCheckpointPhase.DataComplete,
                            false) =>
                            RootArrayCompleteEmpty,
                        (
                            JsonExportCheckpointPhase.DataComplete,
                            true) =>
                            RootArrayCompleteRowEnd,
                        _ => throw Invalid(
                            "JSON export checkpoint boundary state is unsupported."),
                    };
                break;

            case JsonExportFraming.Ndjson:
                expectedFirstByte =
                    hasRows
                        ? (byte)'{'
                        : null;
                expectedTrailingBoundary =
                    hasRows
                        ? NdjsonRowEnd
                        : ReadOnlySpan<byte>.Empty;
                break;

            default:
                throw Invalid(
                    "JSON export checkpoint framing is unsupported.");
        }

        if (firstByte != expectedFirstByte ||
            !trailingBoundary.SequenceEqual(
                expectedTrailingBoundary))
        {
            throw Invalid(
                "Prepared JSON export bytes do not end at the checkpoint's complete-row boundary.");
        }

        return geometry;
    }

    /// <summary>
    /// Validates an idempotent checkpoint or one exact next-generation
    /// transition. Generation zero admission is intentionally handled by the
    /// checkpoint owner.
    /// </summary>
    internal static void ValidateTransition(
        JsonExportCheckpoint current,
        JsonExportCheckpoint next)
    {
        ArgumentNullException.ThrowIfNull(current);
        ArgumentNullException.ThrowIfNull(next);

        ValidateCheckpointShape(current);
        ValidateCheckpointShape(next);

        if (current.Generation == next.Generation)
        {
            if (!CheckpointEquals(
                    current,
                    next))
            {
                throw Invalid(
                    "An equal-generation JSON export checkpoint must be exactly idempotent.");
            }

            return;
        }

        if (current.Generation == long.MaxValue ||
            next.Generation !=
                current.Generation + 1L)
        {
            throw Invalid(
                "JSON export checkpoint generations must advance by exactly one.");
        }
        if (!BindingEquals(
                current.Binding,
                next.Binding) ||
            !HashEquals(
                current.BindingDigest,
                next.BindingDigest))
        {
            throw Invalid(
                "JSON export checkpoint binding cannot change between generations.");
        }

        if (current.Phase ==
            JsonExportCheckpointPhase.DataComplete)
        {
            throw Invalid(
                "A data-complete JSON export checkpoint is terminal.");
        }

        switch (next.Phase)
        {
            case JsonExportCheckpointPhase.Writing:
                ValidateWritingAdvance(
                    current.Progress,
                    next.Progress);
                break;

            case JsonExportCheckpointPhase.DataComplete:
                ValidateCompletionTransition(
                    current.Binding.Json.Framing,
                    current.Progress,
                    next.Progress);
                break;

            default:
                throw Invalid(
                    "JSON export checkpoint phase is unsupported.");
        }
    }

    private static void ValidateCheckpointShape(
        JsonExportCheckpoint checkpoint)
    {
        if (checkpoint.Generation < 0)
        {
            throw Invalid(
                "JSON export checkpoint generation cannot be negative.");
        }
        if (checkpoint.Binding is null ||
            checkpoint.Progress is null ||
            checkpoint.BindingDigest is null)
        {
            throw Invalid(
                "JSON export checkpoint transition evidence is incomplete.");
        }

        _ = ValidateGeometry(
            checkpoint.Binding,
            checkpoint.Phase,
            checkpoint.Progress);

        if ((checkpoint.Phase ==
                JsonExportCheckpointPhase.Writing) !=
            (checkpoint.Completion is null))
        {
            throw Invalid(
                "JSON export checkpoint completion evidence must be present only for the data-complete phase.");
        }
    }

    private static void ValidateWritingAdvance(
        JsonExportCheckpointProgress current,
        JsonExportCheckpointProgress next)
    {
        if (next.CompletedRowCount <=
                current.CompletedRowCount ||
            next.DataPrefixByteLength <=
                current.DataPrefixByteLength ||
            next.LastCompletedRowId is null ||
            (current.LastCompletedRowId is long currentRowId &&
             next.LastCompletedRowId.Value <=
                currentRowId))
        {
            throw Invalid(
                "A writing JSON export checkpoint must advance rows, row id, and complete-prefix bytes.");
        }
        if (!string.Equals(
                current.LogicalPrefixAggregation,
                next.LogicalPrefixAggregation,
                StringComparison.Ordinal) ||
            HashEquals(
                current.DataPrefixDigest,
                next.DataPrefixDigest) ||
            HashEquals(
                current.SourceLogicalRowHashPrefixDigest,
                next.SourceLogicalRowHashPrefixDigest) ||
            HashEquals(
                current.ExportedLogicalRowHashPrefixDigest,
                next.ExportedLogicalRowHashPrefixDigest))
        {
            throw Invalid(
                "A writing JSON export checkpoint must advance all physical and logical prefix evidence without changing its aggregation contract.");
        }
    }

    private static void ValidateCompletionTransition(
        JsonExportFraming framing,
        JsonExportCheckpointProgress current,
        JsonExportCheckpointProgress next)
    {
        if (!string.Equals(
                current.LogicalPrefixAggregation,
                next.LogicalPrefixAggregation,
                StringComparison.Ordinal))
        {
            throw Invalid(
                "Completing a JSON export cannot change its logical-prefix aggregation contract.");
        }

        bool rowsAdvanced =
            next.CompletedRowCount >
            current.CompletedRowCount;
        if (next.CompletedRowCount <
                current.CompletedRowCount ||
            (rowsAdvanced &&
             (next.LastCompletedRowId is null ||
              (current.LastCompletedRowId is long currentRowId &&
               next.LastCompletedRowId.Value <=
                    currentRowId))) ||
            (!rowsAdvanced &&
             next.LastCompletedRowId !=
                current.LastCompletedRowId))
        {
            throw Invalid(
                "Completing a JSON export cannot regress rows, and any terminal rows must advance the signed row id.");
        }

        bool sourceLogicalChanged =
            !HashEquals(
                current.SourceLogicalRowHashPrefixDigest,
                next.SourceLogicalRowHashPrefixDigest);
        bool exportedLogicalChanged =
            !HashEquals(
                current.ExportedLogicalRowHashPrefixDigest,
                next.ExportedLogicalRowHashPrefixDigest);
        if (sourceLogicalChanged != rowsAdvanced ||
            exportedLogicalChanged != rowsAdvanced)
        {
            throw Invalid(
                "Completing a JSON export must advance both logical prefix hashes if and only if terminal rows were added.");
        }

        switch (framing)
        {
            case JsonExportFraming.RootArray:
                long expectedLength;
                try
                {
                    expectedLength =
                        checked(
                            current.DataPrefixByteLength +
                            2L);
                }
                catch (OverflowException)
                {
                    throw Invalid(
                        "The root-array completion tail exceeds bounded prefix geometry.");
                }

                bool rootPhysicalChanged =
                    !HashEquals(
                        current.DataPrefixDigest,
                        next.DataPrefixDigest);
                if ((!rowsAdvanced &&
                     next.DataPrefixByteLength !=
                        expectedLength) ||
                    (rowsAdvanced &&
                     next.DataPrefixByteLength <=
                        expectedLength) ||
                    !rootPhysicalChanged)
                {
                    throw Invalid(
                        "Completing a root-array export must add its two-byte closing tail, any terminal rows, and the corresponding physical prefix evidence.");
                }
                break;

            case JsonExportFraming.Ndjson:
                bool ndjsonPhysicalChanged =
                    !HashEquals(
                        current.DataPrefixDigest,
                        next.DataPrefixDigest);
                if ((!rowsAdvanced &&
                     (next.DataPrefixByteLength !=
                        current.DataPrefixByteLength ||
                      ndjsonPhysicalChanged)) ||
                    (rowsAdvanced &&
                     (next.DataPrefixByteLength <=
                        current.DataPrefixByteLength ||
                      !ndjsonPhysicalChanged)))
                {
                    throw Invalid(
                        "Completing an NDJSON export is phase-only without terminal rows and must advance physical prefix evidence with terminal rows.");
                }
                break;

            default:
                throw Invalid(
                    "JSON export checkpoint framing is unsupported.");
        }
    }

    private static (
        long MinimumPrefixByteLength,
        long MaximumPrefixByteLength,
        int CompletionTailByteLength)
        GetPrefixGeometry(
        JsonExportCheckpointPhase phase,
        JsonExportFraming framing,
        long rowCount,
        long minimumObjectByteLength,
        long maximumObjectByteLength)
    {
        try
        {
            return framing switch
            {
                JsonExportFraming.RootArray =>
                    GetRootArrayGeometry(
                        phase,
                        rowCount,
                        minimumObjectByteLength,
                        maximumObjectByteLength),
                JsonExportFraming.Ndjson =>
                    GetNdjsonGeometry(
                        rowCount,
                        minimumObjectByteLength,
                        maximumObjectByteLength),
                _ => throw Invalid(
                    "JSON export checkpoint framing is unsupported."),
            };
        }
        catch (OverflowException)
        {
            throw Invalid(
                "JSON export checkpoint row count and schema exceed bounded prefix geometry.");
        }
    }

    private static (
        long MinimumPrefixByteLength,
        long MaximumPrefixByteLength,
        int CompletionTailByteLength)
        GetRootArrayGeometry(
        JsonExportCheckpointPhase phase,
        long rowCount,
        long minimumObjectByteLength,
        long maximumObjectByteLength)
    {
        int completionTailByteLength =
            phase ==
                JsonExportCheckpointPhase.Writing
                ? 2
                : 0;
        if (rowCount == 0)
        {
            long exactLength =
                phase ==
                    JsonExportCheckpointPhase.Writing
                    ? 1L
                    : 3L;
            return (
                exactLength,
                exactLength,
                completionTailByteLength);
        }

        long minimumWritingLength =
            checked(
                checked(
                    rowCount *
                    checked(
                        minimumObjectByteLength +
                        1L)));
        long maximumWritingLength =
            checked(
                rowCount *
                checked(
                    maximumObjectByteLength +
                    1L));
        long phaseTail =
            phase ==
                JsonExportCheckpointPhase.DataComplete
                ? 2L
                : 0L;
        return (
            checked(
                minimumWritingLength +
                phaseTail),
            checked(
                maximumWritingLength +
                phaseTail),
            completionTailByteLength);
    }

    private static (
        long MinimumPrefixByteLength,
        long MaximumPrefixByteLength,
        int CompletionTailByteLength)
        GetNdjsonGeometry(
        long rowCount,
        long minimumObjectByteLength,
        long maximumObjectByteLength)
    {
        if (rowCount == 0)
            return (0L, 0L, 0);

        return (
            checked(
                rowCount *
                checked(
                    minimumObjectByteLength +
                    1L)),
            checked(
                rowCount *
                checked(
                    maximumObjectByteLength +
                    1L)),
            0);
    }

    private static long GetMinimumObjectByteLength(
        IReadOnlyList<JsonExportColumnManifest> columns)
    {
        if (columns.Count is
            < 1 or >
            JsonInputContracts.MaximumPropertiesPerObject)
        {
            throw Invalid(
                "JSON export checkpoint schema has an unsupported column count.");
        }

        try
        {
            long length =
                checked(
                    2L +
                    columns.Count -
                    1L);
            for (int index = 0;
                 index < columns.Count;
                 index++)
            {
                JsonExportColumnManifest column =
                    columns[index] ??
                    throw Invalid(
                        $"JSON export checkpoint schema column {index} is missing.");
                string propertyName =
                    column.PropertyName ??
                    throw Invalid(
                        $"JSON export checkpoint schema column {index} has no property name.");
                length =
                    checked(
                        length +
                        GetJsonStringLiteralByteLength(
                            propertyName,
                            index) +
                        1L +
                        GetMinimumValueByteLength(
                            column.DatabaseType));
            }

            return length;
        }
        catch (OverflowException)
        {
            throw Invalid(
                "JSON export checkpoint schema exceeds bounded object geometry.");
        }
    }

    private static int GetMinimumValueByteLength(
        JsonExportDatabaseType databaseType) =>
        databaseType switch
        {
            JsonExportDatabaseType.Integer => 1,
            JsonExportDatabaseType.Real => 1,
            JsonExportDatabaseType.Decimal => 1,
            JsonExportDatabaseType.Text => 2,
            JsonExportDatabaseType.Blob => 2,
            _ => throw Invalid(
                "JSON export checkpoint column type is unsupported."),
        };

    private static long GetJsonStringLiteralByteLength(
        string value,
        int columnIndex)
    {
        long decodedBytes = 0;
        long escapedBytes = 2;
        for (int index = 0;
             index < value.Length;
             index++)
        {
            char character = value[index];
            if (char.IsHighSurrogate(character))
            {
                if (index + 1 >= value.Length ||
                    !char.IsLowSurrogate(
                        value[index + 1]))
                {
                    throw Invalid(
                        $"JSON export checkpoint schema column {columnIndex} contains invalid Unicode.");
                }

                decodedBytes =
                    checked(
                        decodedBytes +
                        4L);
                escapedBytes =
                    checked(
                        escapedBytes +
                        4L);
                index++;
            }
            else if (char.IsLowSurrogate(
                         character))
            {
                throw Invalid(
                    $"JSON export checkpoint schema column {columnIndex} contains invalid Unicode.");
            }
            else
            {
                int utf8Bytes =
                    character switch
                    {
                        <= '\u007f' => 1,
                        <= '\u07ff' => 2,
                        _ => 3,
                    };
                decodedBytes =
                    checked(
                        decodedBytes +
                        utf8Bytes);
                escapedBytes =
                    checked(
                        escapedBytes +
                        (character switch
                        {
                            '"' or '\\' => 2,
                            < '\u0020' =>
                                character is
                                    '\b' or '\t' or
                                    '\n' or '\f' or '\r'
                                    ? 2
                                    : 6,
                            _ => utf8Bytes,
                        }));
            }

            if (decodedBytes >
                JsonInputContracts
                    .MaximumPropertyNameBytes)
            {
                throw Invalid(
                    $"JSON export checkpoint schema column {columnIndex} exceeds the property-name byte ceiling.");
            }
        }

        return escapedBytes;
    }

    private static bool CheckpointEquals(
        JsonExportCheckpoint left,
        JsonExportCheckpoint right) =>
        left.Phase == right.Phase &&
        BindingEquals(
            left.Binding,
            right.Binding) &&
        HashEquals(
            left.BindingDigest,
            right.BindingDigest) &&
        ProgressEquals(
            left.Progress,
            right.Progress) &&
        CompletionEquals(
            left.Completion,
            right.Completion);

    private static bool BindingEquals(
        JsonExportCheckpointBinding left,
        JsonExportCheckpointBinding right)
    {
        if (left.Profile != right.Profile ||
            !string.Equals(
                left.SourceSnapshotIdentity,
                right.SourceSnapshotIdentity,
                StringComparison.Ordinal) ||
            !SourceEquals(
                left.Source,
                right.Source) ||
            !TableEquals(
                left.Table,
                right.Table) ||
            !FormatEquals(
                left.Json,
                right.Json))
        {
            return false;
        }

        return true;
    }

    private static bool SourceEquals(
        JsonExportSourceManifest left,
        JsonExportSourceManifest right) =>
        left is not null &&
        right is not null &&
        string.Equals(
            left.Kind,
            right.Kind,
            StringComparison.Ordinal) &&
        string.Equals(
            left.Version,
            right.Version,
            StringComparison.Ordinal) &&
        left.SnapshotByteLength ==
            right.SnapshotByteLength &&
        HashEquals(
            left.SnapshotDigest,
            right.SnapshotDigest);

    private static bool TableEquals(
        JsonExportTableManifest left,
        JsonExportTableManifest right)
    {
        if (left is null ||
            right is null ||
            !string.Equals(
                left.Name,
                right.Name,
                StringComparison.Ordinal) ||
            !string.Equals(
                left.SchemaContract,
                right.SchemaContract,
                StringComparison.Ordinal) ||
            !HashEquals(
                left.SchemaDigest,
                right.SchemaDigest) ||
            !string.Equals(
                left.RowOrder,
                right.RowOrder,
                StringComparison.Ordinal) ||
            left.Columns is null ||
            right.Columns is null ||
            left.Columns.Count !=
                right.Columns.Count)
        {
            return false;
        }

        for (int index = 0;
             index < left.Columns.Count;
             index++)
        {
            JsonExportColumnManifest leftColumn =
                left.Columns[index];
            JsonExportColumnManifest rightColumn =
                right.Columns[index];
            if (leftColumn is null ||
                rightColumn is null ||
                leftColumn.Ordinal !=
                    rightColumn.Ordinal ||
                !string.Equals(
                    leftColumn.SourceName,
                    rightColumn.SourceName,
                    StringComparison.Ordinal) ||
                !string.Equals(
                    leftColumn.PropertyName,
                    rightColumn.PropertyName,
                    StringComparison.Ordinal) ||
                leftColumn.DatabaseType !=
                    rightColumn.DatabaseType ||
                leftColumn.Nullable !=
                    rightColumn.Nullable ||
                !string.Equals(
                    leftColumn.ValueEncoding,
                    rightColumn.ValueEncoding,
                    StringComparison.Ordinal) ||
                leftColumn.MaximumDecodedBytes !=
                    rightColumn.MaximumDecodedBytes)
            {
                return false;
            }
        }

        return true;
    }

    private static bool FormatEquals(
        JsonExportFormatManifest left,
        JsonExportFormatManifest right) =>
        left is not null &&
        right is not null &&
        string.Equals(
            left.Encoding,
            right.Encoding,
            StringComparison.Ordinal) &&
        left.HasByteOrderMark ==
            right.HasByteOrderMark &&
        string.Equals(
            left.Culture,
            right.Culture,
            StringComparison.Ordinal) &&
        left.Framing ==
            right.Framing &&
        left.Compact ==
            right.Compact &&
        string.Equals(
            left.PropertyOrder,
            right.PropertyOrder,
            StringComparison.Ordinal) &&
        string.Equals(
            left.Newline,
            right.Newline,
            StringComparison.Ordinal) &&
        left.HasFinalNewline ==
            right.HasFinalNewline &&
        string.Equals(
            left.NullEncoding,
            right.NullEncoding,
            StringComparison.Ordinal) &&
        string.Equals(
            left.TextEscape,
            right.TextEscape,
            StringComparison.Ordinal) &&
        left.MaxDataBytes ==
            right.MaxDataBytes &&
        left.MaximumDecodedBlobBytes ==
            right.MaximumDecodedBlobBytes &&
        left.MaximumValueBytes ==
            right.MaximumValueBytes &&
        left.MaximumStringBytes ==
            right.MaximumStringBytes &&
        left.MaximumPropertyNameBytes ==
            right.MaximumPropertyNameBytes &&
        left.MaximumPropertiesPerObject ==
            right.MaximumPropertiesPerObject;

    private static bool ProgressEquals(
        JsonExportCheckpointProgress left,
        JsonExportCheckpointProgress right) =>
        left.CompletedRowCount ==
            right.CompletedRowCount &&
        left.LastCompletedRowId ==
            right.LastCompletedRowId &&
        left.DataPrefixByteLength ==
            right.DataPrefixByteLength &&
        HashEquals(
            left.DataPrefixDigest,
            right.DataPrefixDigest) &&
        string.Equals(
            left.LogicalPrefixAggregation,
            right.LogicalPrefixAggregation,
            StringComparison.Ordinal) &&
        HashEquals(
            left.SourceLogicalRowHashPrefixDigest,
            right.SourceLogicalRowHashPrefixDigest) &&
        HashEquals(
            left.ExportedLogicalRowHashPrefixDigest,
            right.ExportedLogicalRowHashPrefixDigest);

    private static bool CompletionEquals(
        JsonExportCheckpointCompletion? left,
        JsonExportCheckpointCompletion? right)
    {
        if (left is null ||
            right is null)
        {
            return left is null &&
                right is null;
        }

        return HashEquals(
                left.SourceLogicalDigest,
                right.SourceLogicalDigest) &&
            HashEquals(
                left.ExportedLogicalDigest,
                right.ExportedLogicalDigest) &&
            FixedTimeTextEquals(
                left.ManifestDigest,
                right.ManifestDigest);
    }

    private static bool HashEquals(
        JsonExportHashManifest? left,
        JsonExportHashManifest? right) =>
        left is not null &&
        right is not null &&
        string.Equals(
            left.Algorithm,
            right.Algorithm,
            StringComparison.Ordinal) &&
        FixedTimeTextEquals(
            left.Value,
            right.Value);

    private static bool FixedTimeTextEquals(
        string? left,
        string? right)
    {
        if (left is null ||
            right is null ||
            left.Length != right.Length)
        {
            return left is null &&
                right is null;
        }

        return CryptographicOperations.FixedTimeEquals(
            MemoryMarshal.AsBytes(
                left.AsSpan()),
            MemoryMarshal.AsBytes(
                right.AsSpan()));
    }

    private static InvalidDataException Invalid(
        string message) =>
        new(message);
}
