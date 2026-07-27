using System.Globalization;
using System.Text;
using CSharpDB.Migration;

namespace CSharpDB.Migration.Files.Json;

public static class JsonTableSchemaDiagnosticRules
{
    public const string NonObjectRow = "MIG-JSON-SCHEMA-ROW-001";
    public const string EmptyTableShape = "MIG-JSON-SCHEMA-EMPTY-001";
    public const string MissingProperty = "MIG-JSON-SCHEMA-MISSING-001";
    public const string MissingAsNull = "MIG-JSON-SCHEMA-MISSING-AS-NULL-001";
    public const string JsonFallback = "MIG-JSON-SCHEMA-JSON-001";
    public const string OverrideMismatch = "MIG-JSON-SCHEMA-OVERRIDE-001";
    public const string SampledType = "MIG-JSON-SCHEMA-SAMPLE-001";
    public const string PropertyName = "MIG-JSON-SCHEMA-NAME-001";
    public const string ColumnLimit = "MIG-JSON-SCHEMA-LIMIT-COLUMNS-001";
    public const string ColumnNameBytesLimit = "MIG-JSON-SCHEMA-LIMIT-NAMES-001";
}

/// <summary>
/// Discovers the complete top-level object shape while retaining only bounded
/// per-column counters and an independently bounded prefix of type evidence.
/// </summary>
public static class JsonTableSchemaInferer
{
    private const string TableObjectId = "json:table:0";
    private const JsonTableScalarCandidate AllCandidates =
        JsonTableScalarCandidate.Text |
        JsonTableScalarCandidate.Boolean |
        JsonTableScalarCandidate.SignedInteger |
        JsonTableScalarCandidate.UnsignedInteger |
        JsonTableScalarCandidate.Decimal |
        JsonTableScalarCandidate.Json;

    public static ValueTask<JsonTableSchemaInferenceResult> InferAsync(
        JsonSourceBinding binding,
        JsonSourceSnapshot snapshot,
        int maxProfileRecords,
        JsonTableSchemaInferenceOptions? options = null,
        CancellationToken cancellationToken = default) =>
        InferCoreAsync(
            binding,
            snapshot,
            maxProfileRecords,
            options ?? new JsonTableSchemaInferenceOptions(),
            collectProfile: true,
            cancellationToken);

    public static ValueTask<JsonTableSchemaInferenceResult> DiscoverAsync(
        JsonSourceBinding binding,
        JsonSourceSnapshot snapshot,
        JsonTableSchemaInferenceOptions? options = null,
        CancellationToken cancellationToken = default) =>
        InferCoreAsync(
            binding,
            snapshot,
            maxProfileRecords: 1,
            options ?? new JsonTableSchemaInferenceOptions(),
            collectProfile: false,
            cancellationToken);

    internal static ValueTask<JsonTableSchemaInferenceResult> ReplayAsync(
        JsonSourceBinding binding,
        JsonSourceSnapshot snapshot,
        JsonTableSchemaInferenceRecipe recipe,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(recipe);
        return InferCoreAsync(
            binding,
            snapshot,
            recipe.MaxProfileRecords,
            recipe.ToOptions(),
            recipe.CollectProfile,
            cancellationToken);
    }

    private static async ValueTask<JsonTableSchemaInferenceResult> InferCoreAsync(
        JsonSourceBinding binding,
        JsonSourceSnapshot snapshot,
        int maxProfileRecords,
        JsonTableSchemaInferenceOptions options,
        bool collectProfile,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(binding);
        ArgumentNullException.ThrowIfNull(snapshot);
        ArgumentNullException.ThrowIfNull(options);
        ValidateOptions(binding, maxProfileRecords, options);
        cancellationToken.ThrowIfCancellationRequested();

        Dictionary<int, JsonTableColumnSchemaOverride> validatedOverrides =
            ValidateOverrides(binding, options);
        var recipe = new JsonTableSchemaInferenceRecipe(
            collectProfile,
            maxProfileRecords,
            options,
            validatedOverrides.Values);
        Dictionary<int, JsonTableColumnSchemaOverride> overrides = recipe.ColumnOverrides
            .ToDictionary(item => item.ColumnIndex);

        var columns = new List<ColumnAccumulator>();
        var columnsByName = new Dictionary<string, ColumnAccumulator>(StringComparer.Ordinal);
        long totalRecords = 0;
        long eligibleRecords = 0;
        long ineligibleRecords = 0;
        long totalNameBytes = 0;
        long profileRecords = 0;
        long profileBytes = 0;
        bool profileRecordLimitReached = false;
        bool profileByteLimitReached = false;
        bool profileOpen = collectProfile;
        JsonRecordLocation? firstIneligible = null;

        await using JsonStreamingReader reader = await binding
            .OpenReaderAsync(snapshot, cancellationToken)
            .ConfigureAwait(false);
        await foreach (JsonLogicalRecord record in reader
                           .ReadValuesAsync(cancellationToken)
                           .ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            totalRecords = checked(totalRecords + 1);
            if (record.Value.Kind != JsonLogicalValueKind.Object)
            {
                ineligibleRecords = checked(ineligibleRecords + 1);
                firstIneligible ??= JsonRecordLocation.From(record);
                continue;
            }

            eligibleRecords = checked(eligibleRecords + 1);
            foreach (JsonLogicalProperty property in record.Value.Properties)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (!columnsByName.TryGetValue(property.Name, out ColumnAccumulator? column))
                {
                    if (columns.Count == recipe.MaxColumns)
                    {
                        throw LimitExceeded(
                            JsonTableSchemaDiagnosticRules.ColumnLimit,
                            "JSON table discovery exceeds the configured distinct-column limit.",
                            recipe.MaxColumns,
                            checked((long)columns.Count + 1));
                    }

                    int nameBytes = StrictUtf8ByteCount(property.Name);
                    if (nameBytes > recipe.MaxTotalColumnNameBytes - totalNameBytes)
                    {
                        throw LimitExceeded(
                            JsonTableSchemaDiagnosticRules.ColumnNameBytesLimit,
                            "JSON table discovery exceeds the configured cumulative property-name byte limit.",
                            recipe.MaxTotalColumnNameBytes,
                            checked(totalNameBytes + nameBytes));
                    }

                    int columnIndex = columns.Count;
                    overrides.TryGetValue(
                        columnIndex,
                        out JsonTableColumnSchemaOverride? schemaOverride);
                    if (schemaOverride is not null &&
                        !string.Equals(
                            schemaOverride.ExpectedPropertyName,
                            property.Name,
                            StringComparison.Ordinal))
                    {
                        throw new ArgumentException(
                            $"JSON schema override column {columnIndex} does not match its exact expected property name.",
                            nameof(options));
                    }

                    string sourceName = string.IsNullOrWhiteSpace(property.Name)
                        ? $"column_{columnIndex + 1}"
                        : property.Name;
                    column = new ColumnAccumulator(
                        columnIndex,
                        sourceName,
                        property.Name,
                        record.RecordOrdinal,
                        property.Ordinal,
                        schemaOverride);
                    columns.Add(column);
                    columnsByName.Add(property.Name, column);
                    totalNameBytes = checked(totalNameBytes + nameBytes);
                }

                column.ObservePresence(property.Value, record.RecordOrdinal);
            }

            if (!profileOpen)
                continue;
            if (profileRecords == recipe.MaxProfileRecords)
            {
                profileRecordLimitReached = true;
                profileOpen = false;
                continue;
            }

            long recordProfileBytes = CountRecordProfileBytes(
                record.Value,
                cancellationToken);
            if (recordProfileBytes > recipe.MaxProfileBytes - profileBytes)
            {
                profileByteLimitReached = true;
                profileOpen = false;
                continue;
            }

            foreach (JsonLogicalProperty property in record.Value.Properties)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (property.Value.Kind != JsonLogicalValueKind.Null)
                {
                    columnsByName[property.Name].ObserveProfile(
                        property.Value,
                        record.RecordOrdinal,
                        cancellationToken);
                }
            }

            profileBytes = checked(profileBytes + recordProfileBytes);
            profileRecords = checked(profileRecords + 1);
        }

        if (overrides.Keys.Any(index => index >= columns.Count))
        {
            int invalid = overrides.Keys.Where(index => index >= columns.Count).Min();
            throw new ArgumentOutOfRangeException(
                nameof(options),
                $"JSON schema override column {invalid} is outside the discovered width {columns.Count}.");
        }

        var structuralCoverage = new MigrationProfileCoverage
        {
            Kind = MigrationCoverageKind.Full,
            ValuesExamined = totalRecords,
            TotalValues = totalRecords,
            RequiresFullStreamValidation = false,
        };
        MigrationProfileCoverage typeCoverage = CreateTypeCoverage(
            collectProfile,
            profileRecords,
            eligibleRecords);
        var diagnostics = new List<MigrationDiagnostic>();
        if (ineligibleRecords > 0)
        {
            diagnostics.Add(CreateDiagnostic(
                binding,
                JsonTableSchemaDiagnosticRules.NonObjectRow,
                TableObjectId,
                "non-object-row",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Conditional,
                "A top-level JSON value is not eligible for relational table projection.",
                "Table projection accepts only top-level objects; a fail-fast or deterministic reject outcome is required.",
                "Select a collection target or an explicit deterministic row-reject policy.",
                firstIneligible));
        }

        JsonTableColumnSchema[] resolvedColumns = columns
            .Select(column => column.Resolve(
                eligibleRecords,
                typeCoverage,
                binding,
                diagnostics))
            .ToArray();
        if (resolvedColumns.Length == 0)
        {
            diagnostics.Add(CreateDiagnostic(
                binding,
                JsonTableSchemaDiagnosticRules.EmptyTableShape,
                TableObjectId,
                "empty-table-shape",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Conditional,
                "The JSON snapshot does not discover a relational table shape.",
                "No eligible object property exists from which to create an ordered table schema.",
                "Provide a typed schema sidecar or select a collection target.",
                location: null));
        }

        return new JsonTableSchemaInferenceResult(
            binding,
            recipe,
            totalRecords,
            eligibleRecords,
            ineligibleRecords,
            totalNameBytes,
            profileRecords,
            profileBytes,
            profileRecordLimitReached,
            profileByteLimitReached,
            structuralCoverage,
            typeCoverage,
            resolvedColumns,
            diagnostics.OrderBy(item => item.DiagnosticId, StringComparer.Ordinal).ToArray());
    }

    private static void ValidateOptions(
        JsonSourceBinding binding,
        int maxProfileRecords,
        JsonTableSchemaInferenceOptions options)
    {
        if (maxProfileRecords is < 1 or >
            JsonTableSchemaInferenceOptions.MaximumSupportedProfileRecords)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxProfileRecords),
                $"The JSON type-profile record limit must be between 1 and {JsonTableSchemaInferenceOptions.MaximumSupportedProfileRecords}.");
        }
        if (string.IsNullOrWhiteSpace(options.TableName) || options.TableName.Length > 1024)
        {
            throw new ArgumentException(
                "The JSON table name must be nonblank and at most 1024 characters.",
                nameof(options));
        }
        JsonLogicalText.RequireValidUnicode(options.TableName, "JSON table name");
        if (options.ColumnOverrides is null)
            throw new ArgumentException("JSON column overrides cannot be null.", nameof(options));
        if (options.MaxColumns is < 1 or >
            JsonTableSchemaInferenceOptions.MaximumSupportedColumns)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                $"The JSON distinct-column limit must be between 1 and {JsonTableSchemaInferenceOptions.MaximumSupportedColumns}.");
        }
        if (options.MaxTotalColumnNameBytes is < 1 or >
            JsonTableSchemaInferenceOptions.MaximumSupportedTotalColumnNameBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                $"The JSON cumulative property-name byte limit must be between 1 and {JsonTableSchemaInferenceOptions.MaximumSupportedTotalColumnNameBytes}.");
        }
        if (options.MaxProfileBytes is < 1 or >
            JsonTableSchemaInferenceOptions.MaximumSupportedProfileBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                $"The JSON type-profile byte limit must be between 1 and {JsonTableSchemaInferenceOptions.MaximumSupportedProfileBytes}.");
        }
        if (options.ColumnOverrides.Count > options.MaxColumns)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                "JSON schema overrides cannot exceed the configured distinct-column limit.");
        }

        _ = binding.ReaderOptions;
    }

    private static Dictionary<int, JsonTableColumnSchemaOverride> ValidateOverrides(
        JsonSourceBinding binding,
        JsonTableSchemaInferenceOptions options)
    {
        var result = new Dictionary<int, JsonTableColumnSchemaOverride>();
        int maximumNameBytes = binding.ReaderOptions.MaxPropertyNameBytes;
        foreach (JsonTableColumnSchemaOverride schemaOverride in options.ColumnOverrides)
        {
            if (schemaOverride is null)
            {
                throw new ArgumentException(
                    "JSON column overrides cannot contain null values.",
                    nameof(options));
            }
            if (schemaOverride.ColumnIndex < 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(options),
                    "JSON override indexes cannot be negative.");
            }
            if (schemaOverride.ColumnIndex >= options.MaxColumns)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(options),
                    "A JSON override index exceeds the configured distinct-column limit.");
            }
            if (schemaOverride.ExpectedPropertyName is null)
            {
                throw new ArgumentException(
                    "A JSON override exact property-name guard is required.",
                    nameof(options));
            }
            JsonLogicalText.RequireValidUnicode(
                schemaOverride.ExpectedPropertyName,
                "JSON override property-name guard");
            if (StrictUtf8ByteCount(schemaOverride.ExpectedPropertyName) > maximumNameBytes)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(options),
                    "A JSON override property-name guard exceeds the bound reader limit.");
            }
            if (!Enum.IsDefined(schemaOverride.LogicalType))
            {
                throw new ArgumentOutOfRangeException(
                    nameof(options),
                    "A JSON override logical type is invalid.");
            }
            if (!Enum.IsDefined(schemaOverride.MissingPolicy))
            {
                throw new ArgumentOutOfRangeException(
                    nameof(options),
                    "A JSON override missing-property policy is invalid.");
            }
            if (schemaOverride.MissingPolicy == JsonMissingPropertyPolicy.AsNull &&
                schemaOverride.Nullable == false)
            {
                throw new ArgumentException(
                    "A JSON MissingAsNull override cannot declare the column non-nullable.",
                    nameof(options));
            }
            if (!result.TryAdd(schemaOverride.ColumnIndex, schemaOverride))
            {
                throw new ArgumentException(
                    $"JSON column {schemaOverride.ColumnIndex} has more than one schema override.",
                    nameof(options));
            }
        }

        return result;
    }

    private static MigrationProfileCoverage CreateTypeCoverage(
        bool collectProfile,
        long profileRecords,
        long eligibleRecords)
    {
        if (!collectProfile)
        {
            return new MigrationProfileCoverage
            {
                Kind = MigrationCoverageKind.None,
                ValuesExamined = 0,
                TotalValues = eligibleRecords,
                RequiresFullStreamValidation = true,
            };
        }

        bool full = profileRecords == eligibleRecords;
        return new MigrationProfileCoverage
        {
            Kind = full ? MigrationCoverageKind.Full : MigrationCoverageKind.Sample,
            ValuesExamined = profileRecords,
            TotalValues = eligibleRecords,
            RequiresFullStreamValidation = !full,
        };
    }

    private static long CountRecordProfileBytes(
        JsonLogicalValue value,
        CancellationToken cancellationToken)
    {
        long count = 0;
        foreach (JsonLogicalProperty property in value.Properties)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (property.Value.Kind != JsonLogicalValueKind.Null)
            {
                count = checked(
                    count +
                    JsonTableScalarPolicy.GetCanonicalUtf8ByteCount(
                        property.Value,
                        cancellationToken));
            }
        }

        return count;
    }

    private static int StrictUtf8ByteCount(string value)
    {
        try
        {
            return new UTF8Encoding(false, true).GetByteCount(value);
        }
        catch (EncoderFallbackException error)
        {
            throw new ArgumentException("Text contains invalid Unicode.", nameof(value), error);
        }
    }

    private static JsonTableSchemaInferenceException LimitExceeded(
        string ruleId,
        string message,
        long limit,
        long observed) =>
        new(ruleId, message, limit, observed);

    private static MigrationDiagnostic CreateDiagnostic(
        JsonSourceBinding binding,
        string ruleId,
        string objectId,
        string reason,
        MigrationDiagnosticSeverity severity,
        MigrationCompatibilityStatus status,
        string summary,
        string explanation,
        string? remediation,
        JsonRecordLocation? location)
    {
        string digest = JsonStableDigest.Compute(
            "csharpdb-json-table-schema-diagnostic-v1",
            ruleId,
            binding.Source.Fingerprint,
            objectId,
            reason);
        string shortDigest = digest["sha256:".Length..][..16];
        return new MigrationDiagnostic
        {
            DiagnosticId = $"diag:{ruleId.ToLowerInvariant()}:{shortDigest}",
            RuleId = ruleId,
            Severity = severity,
            Status = status,
            Evidence = MigrationEvidenceLevel.Parsed,
            Summary = summary,
            Explanation = explanation,
            ObjectId = objectId,
            SourceSpan = CreateSourceSpan(binding, location),
            Remediation = remediation,
            CanOverride = false,
        };
    }

    private static MigrationSourceSpan? CreateSourceSpan(
        JsonSourceBinding binding,
        JsonRecordLocation? location)
    {
        if (location is not JsonRecordLocation actual ||
            actual.LineNumber is < 1 or > int.MaxValue ||
            actual.BytePositionInLine >= int.MaxValue)
        {
            return null;
        }

        return new MigrationSourceSpan
        {
            SourceId = binding.Source.Identity,
            Line = (int)actual.LineNumber,
            Column = checked((int)actual.BytePositionInLine + 1),
        };
    }

    private sealed class ColumnAccumulator
    {
        private JsonTableScalarCandidate intersection = AllCandidates;
        private JsonValueKindEvidence profiledKinds;
        private int maximumIntegralDigits;
        private int maximumScale;
        private bool overrideCompatible = true;

        internal ColumnAccumulator(
            int columnIndex,
            string sourceName,
            string originalPropertyName,
            long firstSeenRecordOrdinal,
            int firstSeenPropertyOrdinal,
            JsonTableColumnSchemaOverride? schemaOverride)
        {
            ColumnIndex = columnIndex;
            SourceName = sourceName;
            OriginalPropertyName = originalPropertyName;
            FirstSeenRecordOrdinal = firstSeenRecordOrdinal;
            FirstSeenPropertyOrdinal = firstSeenPropertyOrdinal;
            Override = schemaOverride;
        }

        internal int ColumnIndex { get; }

        internal string SourceName { get; }

        internal string OriginalPropertyName { get; }

        internal long FirstSeenRecordOrdinal { get; }

        internal int FirstSeenPropertyOrdinal { get; }

        internal JsonTableColumnSchemaOverride? Override { get; }

        internal long PresentCount { get; private set; }

        internal long NullCount { get; private set; }

        internal long ProfiledNonNullCount { get; private set; }

        internal long ProfiledStringCount { get; private set; }

        internal long ProfiledBooleanCount { get; private set; }

        internal long ProfiledNumberCount { get; private set; }

        internal long ProfiledObjectCount { get; private set; }

        internal long ProfiledArrayCount { get; private set; }

        internal long ProfiledLexemePreservationCount { get; private set; }

        internal long ObservedMaxCanonicalValueBytes { get; private set; }

        internal long? FirstOverrideMismatchRecordOrdinal { get; private set; }

        internal void ObservePresence(JsonLogicalValue value, long recordOrdinal)
        {
            PresentCount = checked(PresentCount + 1);
            if (value.Kind == JsonLogicalValueKind.Null)
            {
                NullCount = checked(NullCount + 1);
                if (Override?.Nullable == false)
                    MarkOverrideMismatch(recordOrdinal);
            }
        }

        internal void ObserveProfile(
            JsonLogicalValue value,
            long recordOrdinal,
            CancellationToken cancellationToken)
        {
            JsonTableScalarClassification classification =
                JsonTableScalarPolicy.Classify(
                    value,
                    cancellationToken);
            ProfiledNonNullCount = checked(ProfiledNonNullCount + 1);
            intersection &= classification.Candidates;
            if (classification.RequiresJsonLexemePreservation)
            {
                ProfiledLexemePreservationCount =
                    checked(ProfiledLexemePreservationCount + 1);
            }
            ObservedMaxCanonicalValueBytes = Math.Max(
                ObservedMaxCanonicalValueBytes,
                JsonTableScalarPolicy.GetCanonicalUtf8ByteCount(
                    value,
                    cancellationToken));

            switch (value.Kind)
            {
                case JsonLogicalValueKind.String:
                    ProfiledStringCount = checked(ProfiledStringCount + 1);
                    profiledKinds |= JsonValueKindEvidence.String;
                    break;
                case JsonLogicalValueKind.Boolean:
                    ProfiledBooleanCount = checked(ProfiledBooleanCount + 1);
                    profiledKinds |= JsonValueKindEvidence.Boolean;
                    break;
                case JsonLogicalValueKind.Number:
                    ProfiledNumberCount = checked(ProfiledNumberCount + 1);
                    profiledKinds |= JsonValueKindEvidence.Number;
                    if ((classification.Candidates & JsonTableScalarCandidate.Decimal) != 0)
                    {
                        maximumIntegralDigits = Math.Max(
                            maximumIntegralDigits,
                            classification.IntegralDigits);
                        maximumScale = Math.Max(maximumScale, classification.Scale);
                    }
                    break;
                case JsonLogicalValueKind.Object:
                    ProfiledObjectCount = checked(ProfiledObjectCount + 1);
                    profiledKinds |= JsonValueKindEvidence.Object;
                    break;
                case JsonLogicalValueKind.Array:
                    ProfiledArrayCount = checked(ProfiledArrayCount + 1);
                    profiledKinds |= JsonValueKindEvidence.Array;
                    break;
                default:
                    throw new InvalidDataException(
                        "JSON null cannot contribute type evidence.");
            }

            if (Override is not null &&
                !JsonTableScalarPolicy.IsCompatible(
                    value,
                    Override.LogicalType,
                    cancellationToken))
            {
                MarkOverrideMismatch(recordOrdinal);
            }
        }

        internal JsonTableColumnSchema Resolve(
            long eligibleRecords,
            MigrationProfileCoverage coverage,
            JsonSourceBinding binding,
            ICollection<MigrationDiagnostic> diagnostics)
        {
            long missingCount = checked(eligibleRecords - PresentCount);
            JsonTableColumnLogicalType logicalType;
            JsonTableColumnSchemaResolution resolution;
            JsonTableColumnInferenceReason reason;
            JsonTableInferenceConfidence confidence;

            if (Override is not null)
            {
                logicalType = Override.LogicalType;
                resolution = JsonTableColumnSchemaResolution.ExplicitOverride;
                reason = JsonTableColumnInferenceReason.ExplicitOverride;
                confidence = JsonTableInferenceConfidence.Explicit;
            }
            else if (ProfiledNonNullCount == 0)
            {
                logicalType = JsonTableColumnLogicalType.Json;
                resolution = JsonTableColumnSchemaResolution.DefaultedToJson;
                reason = JsonTableColumnInferenceReason.InsufficientEvidence;
                confidence = JsonTableInferenceConfidence.None;
            }
            else
            {
                logicalType = SelectCandidate(intersection);
                reason = logicalType == JsonTableColumnLogicalType.Json
                    ? ProfiledLexemePreservationCount > 0
                        ? JsonTableColumnInferenceReason.LexicalPreservation
                        : HasMultipleKinds(profiledKinds)
                            ? JsonTableColumnInferenceReason.MixedKinds
                            : JsonTableColumnInferenceReason.ExactEvidence
                    : JsonTableColumnInferenceReason.ExactEvidence;
                resolution = logicalType == JsonTableColumnLogicalType.Json &&
                             reason != JsonTableColumnInferenceReason.ExactEvidence
                    ? JsonTableColumnSchemaResolution.WidenedToJson
                    : JsonTableColumnSchemaResolution.Inferred;
                confidence = coverage.Kind == MigrationCoverageKind.Full
                    ? JsonTableInferenceConfidence.High
                    : ProfiledNonNullCount == 1
                        ? JsonTableInferenceConfidence.Low
                        : JsonTableInferenceConfidence.Medium;
            }

            JsonMissingPropertyPolicy missingPolicy =
                Override?.MissingPolicy ?? JsonMissingPropertyPolicy.Reject;
            bool nullable = Override?.Nullable ??
                (NullCount > 0 ||
                 missingPolicy == JsonMissingPropertyPolicy.AsNull);
            JsonTableOverrideValidationStatus overrideValidation = Override is null
                ? JsonTableOverrideValidationStatus.NotApplicable
                : !overrideCompatible
                    ? JsonTableOverrideValidationStatus.Incompatible
                    : ProfiledNonNullCount == 0
                        ? JsonTableOverrideValidationStatus.NotProfiled
                        : coverage.Kind == MigrationCoverageKind.Full
                            ? JsonTableOverrideValidationStatus.FullCompatible
                            : JsonTableOverrideValidationStatus.SampleCompatible;

            AddColumnDiagnostics(
                binding,
                diagnostics,
                logicalType,
                resolution,
                reason,
                coverage,
                missingPolicy,
                missingCount,
                overrideValidation);

            int? observedPrecision = logicalType == JsonTableColumnLogicalType.Decimal &&
                                     maximumIntegralDigits > 0
                ? checked(maximumIntegralDigits + maximumScale)
                : null;
            int? observedScale = observedPrecision is null ? null : maximumScale;
            return new JsonTableColumnSchema(
                ColumnIndex,
                SourceName,
                OriginalPropertyName,
                FirstSeenRecordOrdinal,
                FirstSeenPropertyOrdinal,
                logicalType,
                resolution,
                reason,
                confidence,
                nullable,
                missingPolicy,
                overrideValidation,
                PresentCount,
                NullCount,
                missingCount,
                ProfiledNonNullCount,
                ProfiledStringCount,
                ProfiledBooleanCount,
                ProfiledNumberCount,
                ProfiledObjectCount,
                ProfiledArrayCount,
                ProfiledLexemePreservationCount,
                ObservedMaxCanonicalValueBytes,
                observedPrecision,
                observedScale,
                FirstOverrideMismatchRecordOrdinal);
        }

        private void AddColumnDiagnostics(
            JsonSourceBinding binding,
            ICollection<MigrationDiagnostic> diagnostics,
            JsonTableColumnLogicalType logicalType,
            JsonTableColumnSchemaResolution resolution,
            JsonTableColumnInferenceReason reason,
            MigrationProfileCoverage coverage,
            JsonMissingPropertyPolicy missingPolicy,
            long missingCount,
            JsonTableOverrideValidationStatus overrideValidation)
        {
            string objectId = $"json:column:{ColumnIndex}";
            if (missingCount > 0)
            {
                bool asNull = missingPolicy == JsonMissingPropertyPolicy.AsNull;
                diagnostics.Add(CreateDiagnostic(
                    binding,
                    asNull
                        ? JsonTableSchemaDiagnosticRules.MissingAsNull
                        : JsonTableSchemaDiagnosticRules.MissingProperty,
                    objectId,
                    asNull ? "missing-as-null" : "missing-reject",
                    asNull
                        ? MigrationDiagnosticSeverity.Warning
                        : MigrationDiagnosticSeverity.Error,
                    asNull
                        ? MigrationCompatibilityStatus.CompatibleWithRewrite
                        : MigrationCompatibilityStatus.Conditional,
                    asNull
                        ? "Missing JSON properties will be represented as database null."
                        : "A JSON object is missing a discovered table property.",
                    asNull
                        ? "The explicit column policy changes property absence into database null."
                        : "Property absence remains distinct from explicit JSON null and requires a row outcome policy.",
                    asNull
                        ? "Retain the explicit rewrite only if absence and null are equivalent for this column."
                        : "Select deterministic rejection or an explicit nullable MissingAsNull override.",
                    location: null));
            }

            if (resolution is JsonTableColumnSchemaResolution.WidenedToJson or
                JsonTableColumnSchemaResolution.DefaultedToJson)
            {
                diagnostics.Add(CreateDiagnostic(
                    binding,
                    JsonTableSchemaDiagnosticRules.JsonFallback,
                    objectId,
                    reason.ToString(),
                    MigrationDiagnosticSeverity.Information,
                    MigrationCompatibilityStatus.CompatibleWithRewrite,
                    "JSON table inference selected ordered canonical JSON text.",
                    "Nested, mixed, lexically significant, or insufficient evidence is preserved through the versioned JSON value representation.",
                    "Keep the lossless JSON representation or add a compatible exact schema override.",
                    location: null));
            }

            if (overrideValidation == JsonTableOverrideValidationStatus.Incompatible)
            {
                diagnostics.Add(CreateDiagnostic(
                    binding,
                    JsonTableSchemaDiagnosticRules.OverrideMismatch,
                    objectId,
                    "override-mismatch",
                    MigrationDiagnosticSeverity.Error,
                    MigrationCompatibilityStatus.Conditional,
                    "A JSON value contradicts the explicit table-schema override.",
                    "The override remains visible, but its declared kind or nullability is incompatible with observed evidence.",
                    "Correct or remove the override before apply.",
                    location: null));
            }

            if (coverage.Kind == MigrationCoverageKind.Sample &&
                logicalType != JsonTableColumnLogicalType.Json)
            {
                diagnostics.Add(CreateDiagnostic(
                    binding,
                    JsonTableSchemaDiagnosticRules.SampledType,
                    objectId,
                    logicalType.ToString(),
                    MigrationDiagnosticSeverity.Information,
                    MigrationCompatibilityStatus.Conditional,
                    "The JSON column type is derived from bounded evidence.",
                    "Every streamed value must be checked by the same scalar policy before a target batch commits.",
                    "Retain full-stream validation during apply.",
                    location: null));
            }

            if (string.IsNullOrWhiteSpace(OriginalPropertyName))
            {
                diagnostics.Add(CreateDiagnostic(
                    binding,
                    JsonTableSchemaDiagnosticRules.PropertyName,
                    objectId,
                    "blank",
                    MigrationDiagnosticSeverity.Information,
                    MigrationCompatibilityStatus.Compatible,
                    "A JSON property requires a deterministic nonblank catalog name.",
                    "The exact blank or whitespace-only property name remains preserved separately from its ordinal fallback.",
                    "Review the deterministic target name during planning.",
                    location: null));
            }
        }

        private void MarkOverrideMismatch(long recordOrdinal)
        {
            overrideCompatible = false;
            FirstOverrideMismatchRecordOrdinal ??= recordOrdinal;
        }

        private static JsonTableColumnLogicalType SelectCandidate(
            JsonTableScalarCandidate candidates)
        {
            (JsonTableScalarCandidate Candidate, JsonTableColumnLogicalType Type)[] precedence =
            [
                (JsonTableScalarCandidate.Text, JsonTableColumnLogicalType.Text),
                (JsonTableScalarCandidate.Boolean, JsonTableColumnLogicalType.Boolean),
                (JsonTableScalarCandidate.SignedInteger, JsonTableColumnLogicalType.SignedInteger),
                (JsonTableScalarCandidate.UnsignedInteger, JsonTableColumnLogicalType.UnsignedInteger),
                (JsonTableScalarCandidate.Decimal, JsonTableColumnLogicalType.Decimal),
                (JsonTableScalarCandidate.Json, JsonTableColumnLogicalType.Json),
            ];
            foreach ((JsonTableScalarCandidate candidate, JsonTableColumnLogicalType type) in
                     precedence)
            {
                if ((candidates & candidate) != 0)
                    return type;
            }

            throw new InvalidDataException("JSON scalar candidate intersection is empty.");
        }
    }

    [Flags]
    private enum JsonValueKindEvidence
    {
        None = 0,
        String = 1,
        Boolean = 2,
        Number = 4,
        Object = 8,
        Array = 16,
    }

    private static bool HasMultipleKinds(JsonValueKindEvidence kinds)
    {
        int value = (int)kinds;
        return value != 0 && (value & (value - 1)) != 0;
    }

    private readonly record struct JsonRecordLocation(
        long LineNumber,
        long BytePositionInLine)
    {
        internal static JsonRecordLocation From(JsonLogicalRecord record) =>
            new(record.StartLineNumber, record.StartBytePositionInLine);
    }
}
