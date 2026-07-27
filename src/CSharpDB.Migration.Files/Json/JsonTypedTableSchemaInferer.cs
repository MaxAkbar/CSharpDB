using System.Security.Cryptography;

namespace CSharpDB.Migration.Files.Json;

/// <summary>
/// Resolves a source-bound typed-intent manifest against the complete JSON
/// object-table shape while reusing the unchanged v1 representation
/// discovery and profiling contract.
/// </summary>
public static class JsonTypedTableSchemaInferer
{
    public static ValueTask<
        JsonTypedTableSchemaInferenceResult> InferAsync(
        JsonSourceBinding binding,
        JsonSourceSnapshot snapshot,
        JsonTypedIntentManifest intentManifest,
        int maxProfileRecords,
        JsonTableSchemaInferenceOptions? options = null,
        CancellationToken cancellationToken = default) =>
        InferCoreAsync(
            binding,
            snapshot,
            intentManifest,
            maxProfileRecords,
            options ?? new JsonTableSchemaInferenceOptions(),
            collectProfile: true,
            cancellationToken);

    public static ValueTask<
        JsonTypedTableSchemaInferenceResult> DiscoverAsync(
        JsonSourceBinding binding,
        JsonSourceSnapshot snapshot,
        JsonTypedIntentManifest intentManifest,
        JsonTableSchemaInferenceOptions? options = null,
        CancellationToken cancellationToken = default) =>
        InferCoreAsync(
            binding,
            snapshot,
            intentManifest,
            maxProfileRecords: 1,
            options ?? new JsonTableSchemaInferenceOptions(),
            collectProfile: false,
            cancellationToken);

    private static async ValueTask<
        JsonTypedTableSchemaInferenceResult> InferCoreAsync(
        JsonSourceBinding binding,
        JsonSourceSnapshot snapshot,
        JsonTypedIntentManifest intentManifest,
        int maxProfileRecords,
        JsonTableSchemaInferenceOptions options,
        bool collectProfile,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(binding);
        ArgumentNullException.ThrowIfNull(snapshot);
        ArgumentNullException.ThrowIfNull(intentManifest);
        ArgumentNullException.ThrowIfNull(options);

        cancellationToken.ThrowIfCancellationRequested();
        byte[] canonicalIntentBytes =
            intentManifest.ToCanonicalUtf8Bytes();
        JsonTypedIntentManifest canonicalIntent;
        try
        {
            canonicalIntent =
                JsonTypedIntentSidecar.Parse(
                    canonicalIntentBytes,
                    binding,
                    intentManifest.ManifestDigest);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(
                canonicalIntentBytes);
        }
        JsonTableSchemaInferenceOptions effectiveOptions =
            CreateEffectiveOptions(options, canonicalIntent);

        JsonTableSchemaInferenceResult representation;
        try
        {
            representation = collectProfile
                ? await JsonTableSchemaInferer.InferAsync(
                        binding,
                        snapshot,
                        maxProfileRecords,
                        effectiveOptions,
                        cancellationToken)
                    .ConfigureAwait(false)
                : await JsonTableSchemaInferer.DiscoverAsync(
                        binding,
                        snapshot,
                        effectiveOptions,
                        cancellationToken)
                    .ConfigureAwait(false);
        }
        catch (ArgumentException exception) when (
            IsDiscoveredShapeMismatch(exception))
        {
            throw new JsonTypedTableSchemaException(
                JsonTypedTableSchemaRules.ColumnMismatch,
                "A typed JSON intent declaration does not match the discovered column ordinal and exact decoded property name.",
                exception);
        }

        ValidateDiscoveredShape(
            representation,
            canonicalIntent);
        return new JsonTypedTableSchemaInferenceResult(
            representation,
            canonicalIntent);
    }

    private static JsonTableSchemaInferenceOptions
        CreateEffectiveOptions(
            JsonTableSchemaInferenceOptions options,
            JsonTypedIntentManifest intentManifest)
    {
        if (options.ColumnOverrides is null)
        {
            throw new ArgumentException(
                "JSON column overrides cannot be null.",
                nameof(options));
        }

        var overrides =
            new List<JsonTableColumnSchemaOverride>(
                checked(
                    options.ColumnOverrides.Count +
                    intentManifest.Columns.Count));
        var occupiedOrdinals = new HashSet<int>();
        foreach (JsonTableColumnSchemaOverride item in
                 options.ColumnOverrides)
        {
            if (item is null)
            {
                throw new ArgumentException(
                    "JSON column overrides cannot contain null values.",
                    nameof(options));
            }
            if (!occupiedOrdinals.Add(item.ColumnIndex))
            {
                throw new ArgumentException(
                    "A JSON column ordinal cannot have more than one schema override.",
                    nameof(options));
            }

            overrides.Add(CloneOverride(item));
        }

        foreach (JsonTypedColumnIntent intent in
                 intentManifest.Columns)
        {
            if (!occupiedOrdinals.Add(intent.ColumnIndex))
            {
                throw new ArgumentException(
                    "A typed JSON intent declaration cannot overlap an ordinary schema override.",
                    nameof(options));
            }
            if (intent.ColumnIndex >= options.MaxColumns)
            {
                throw new ArgumentException(
                    "A typed JSON intent declaration exceeds the configured distinct-column limit.",
                    nameof(options));
            }

            overrides.Add(
                new JsonTableColumnSchemaOverride
                {
                    ColumnIndex = intent.ColumnIndex,
                    ExpectedPropertyName =
                        intent.ExpectedPropertyName,
                    LogicalType =
                        RepresentationType(intent.Codec),
                    Nullable = intent.Nullable,
                    MissingPolicy = intent.MissingPolicy,
                });
        }

        return new JsonTableSchemaInferenceOptions
        {
            TableName = options.TableName,
            ColumnOverrides = overrides
                .OrderBy(
                    item => item.ColumnIndex)
                .ToArray(),
            MaxColumns = options.MaxColumns,
            MaxTotalColumnNameBytes =
                options.MaxTotalColumnNameBytes,
            MaxProfileBytes = options.MaxProfileBytes,
        };
    }

    private static JsonTableColumnSchemaOverride CloneOverride(
        JsonTableColumnSchemaOverride item) =>
        new()
        {
            ColumnIndex = item.ColumnIndex,
            ExpectedPropertyName =
                item.ExpectedPropertyName,
            LogicalType = item.LogicalType,
            Nullable = item.Nullable,
            MissingPolicy = item.MissingPolicy,
        };

    private static JsonTableColumnLogicalType
        RepresentationType(JsonTypedValueCodec codec) =>
        codec switch
        {
            JsonTypedValueCodec.DecimalNumber =>
                JsonTableColumnLogicalType.Decimal,
            JsonTypedValueCodec.BinaryBase64 or
            JsonTypedValueCodec.DecimalString or
            JsonTypedValueCodec.GuidD or
            JsonTypedValueCodec.DateCSharpDbText or
            JsonTypedValueCodec.TimeCSharpDbText or
            JsonTypedValueCodec.DateTimeCSharpDbText or
            JsonTypedValueCodec.DateTimeOffsetCSharpDbText or
            JsonTypedValueCodec.Int64String or
            JsonTypedValueCodec.UInt64String =>
                JsonTableColumnLogicalType.Text,
            _ => throw new ArgumentOutOfRangeException(
                nameof(codec)),
        };

    private static void ValidateDiscoveredShape(
        JsonTableSchemaInferenceResult representation,
        JsonTypedIntentManifest intentManifest)
    {
        foreach (JsonTypedColumnIntent intent in
                 intentManifest.Columns)
        {
            if ((uint)intent.ColumnIndex >=
                    (uint)representation.Columns.Count ||
                !string.Equals(
                    representation
                        .Columns[intent.ColumnIndex]
                        .OriginalPropertyName,
                    intent.ExpectedPropertyName,
                    StringComparison.Ordinal))
            {
                throw new JsonTypedTableSchemaException(
                    JsonTypedTableSchemaRules.ColumnMismatch,
                    "A typed JSON intent declaration does not match the discovered column ordinal and exact decoded property name.");
            }
        }
    }

    private static bool IsDiscoveredShapeMismatch(
        ArgumentException exception) =>
        exception.ParamName == "options" &&
        (exception.Message.StartsWith(
             "JSON schema override column ",
             StringComparison.Ordinal) ||
         exception.Message.StartsWith(
             "JSON column ",
             StringComparison.Ordinal));
}
