using System.Collections.ObjectModel;
using CSharpDB.Migration;

namespace CSharpDB.Migration.Files.Json;

public enum JsonTableColumnLogicalType
{
    Text,
    Boolean,
    SignedInteger,
    UnsignedInteger,
    Decimal,
    Json,
}

public enum JsonMissingPropertyPolicy
{
    Reject,
    AsNull,
}

public enum JsonTableColumnSchemaResolution
{
    Inferred,
    WidenedToJson,
    DefaultedToJson,
    ExplicitOverride,
}

public enum JsonTableColumnInferenceReason
{
    ExactEvidence,
    MixedKinds,
    LexicalPreservation,
    InsufficientEvidence,
    ExplicitOverride,
}

public enum JsonTableInferenceConfidence
{
    None,
    Low,
    Medium,
    High,
    Explicit,
}

public enum JsonTableOverrideValidationStatus
{
    NotApplicable,
    NotProfiled,
    SampleCompatible,
    FullCompatible,
    Incompatible,
}

/// <summary>
/// An ordinal-addressed declaration for one discovered top-level JSON
/// property. The exact decoded-name guard prevents a late discovery-order
/// change from silently retargeting an override.
/// </summary>
public sealed record JsonTableColumnSchemaOverride
{
    public required int ColumnIndex { get; init; }

    public required string ExpectedPropertyName { get; init; }

    public required JsonTableColumnLogicalType LogicalType { get; init; }

    public bool? Nullable { get; init; }

    public JsonMissingPropertyPolicy MissingPolicy { get; init; } =
        JsonMissingPropertyPolicy.Reject;
}

public sealed record JsonTableSchemaInferenceOptions
{
    public const int DefaultMaximumColumns = 4_096;

    public const int MaximumSupportedColumns = 16_384;

    public const long DefaultMaximumTotalColumnNameBytes = 4L * 1024 * 1024;

    public const long MaximumSupportedTotalColumnNameBytes = 64L * 1024 * 1024;

    public const long MaximumSupportedProfileBytes = 64L * 1024 * 1024;

    public const int MaximumSupportedProfileRecords = 1_000_000;

    public string TableName { get; init; } = "json_data";

    public IReadOnlyList<JsonTableColumnSchemaOverride> ColumnOverrides { get; init; } = [];

    public int MaxColumns { get; init; } = DefaultMaximumColumns;

    public long MaxTotalColumnNameBytes { get; init; } =
        DefaultMaximumTotalColumnNameBytes;

    /// <summary>
    /// Maximum canonical UTF-8 bytes from present, non-null property values
    /// that may contribute type evidence.
    /// </summary>
    public long MaxProfileBytes { get; init; } = MaximumSupportedProfileBytes;
}

internal sealed class JsonTableSchemaInferenceRecipe
{
    internal JsonTableSchemaInferenceRecipe(
        bool collectProfile,
        int maxProfileRecords,
        JsonTableSchemaInferenceOptions options,
        IEnumerable<JsonTableColumnSchemaOverride> overrides)
    {
        CollectProfile = collectProfile;
        MaxProfileRecords = maxProfileRecords;
        TableName = options.TableName;
        MaxColumns = options.MaxColumns;
        MaxTotalColumnNameBytes = options.MaxTotalColumnNameBytes;
        MaxProfileBytes = options.MaxProfileBytes;
        ColumnOverrides = Array.AsReadOnly(overrides
            .Select(CloneOverride)
            .OrderBy(item => item.ColumnIndex)
            .ToArray());
    }

    internal bool CollectProfile { get; }

    internal int MaxProfileRecords { get; }

    internal string TableName { get; }

    internal int MaxColumns { get; }

    internal long MaxTotalColumnNameBytes { get; }

    internal long MaxProfileBytes { get; }

    internal ReadOnlyCollection<JsonTableColumnSchemaOverride> ColumnOverrides { get; }

    internal JsonTableSchemaInferenceOptions ToOptions() => new()
    {
        TableName = TableName,
        MaxColumns = MaxColumns,
        MaxTotalColumnNameBytes = MaxTotalColumnNameBytes,
        MaxProfileBytes = MaxProfileBytes,
        ColumnOverrides = ColumnOverrides.Select(CloneOverride).ToArray(),
    };

    private static JsonTableColumnSchemaOverride CloneOverride(
        JsonTableColumnSchemaOverride item) => new()
    {
        ColumnIndex = item.ColumnIndex,
        ExpectedPropertyName = item.ExpectedPropertyName,
        LogicalType = item.LogicalType,
        Nullable = item.Nullable,
        MissingPolicy = item.MissingPolicy,
    };
}

public sealed class JsonTableColumnSchema
{
    internal JsonTableColumnSchema(
        int columnIndex,
        string sourceName,
        string originalPropertyName,
        long firstSeenRecordOrdinal,
        int firstSeenPropertyOrdinal,
        JsonTableColumnLogicalType logicalType,
        JsonTableColumnSchemaResolution resolution,
        JsonTableColumnInferenceReason reason,
        JsonTableInferenceConfidence confidence,
        bool nullable,
        JsonMissingPropertyPolicy missingPolicy,
        JsonTableOverrideValidationStatus overrideValidation,
        long presentCount,
        long nullCount,
        long missingCount,
        long profiledNonNullCount,
        long profiledStringCount,
        long profiledBooleanCount,
        long profiledNumberCount,
        long profiledObjectCount,
        long profiledArrayCount,
        long profiledLexemePreservationCount,
        long observedMaxCanonicalValueBytes,
        int? observedPrecision,
        int? observedScale,
        long? firstOverrideMismatchRecordOrdinal)
    {
        ColumnIndex = columnIndex;
        SourceName = sourceName;
        OriginalPropertyName = originalPropertyName;
        FirstSeenRecordOrdinal = firstSeenRecordOrdinal;
        FirstSeenPropertyOrdinal = firstSeenPropertyOrdinal;
        LogicalType = logicalType;
        Resolution = resolution;
        Reason = reason;
        Confidence = confidence;
        Nullable = nullable;
        MissingPolicy = missingPolicy;
        OverrideValidation = overrideValidation;
        PresentCount = presentCount;
        NullCount = nullCount;
        MissingCount = missingCount;
        ProfiledNonNullCount = profiledNonNullCount;
        ProfiledStringCount = profiledStringCount;
        ProfiledBooleanCount = profiledBooleanCount;
        ProfiledNumberCount = profiledNumberCount;
        ProfiledObjectCount = profiledObjectCount;
        ProfiledArrayCount = profiledArrayCount;
        ProfiledLexemePreservationCount = profiledLexemePreservationCount;
        ObservedMaxCanonicalValueBytes = observedMaxCanonicalValueBytes;
        ObservedPrecision = observedPrecision;
        ObservedScale = observedScale;
        FirstOverrideMismatchRecordOrdinal = firstOverrideMismatchRecordOrdinal;
    }

    public int ColumnIndex { get; }

    /// <summary>Exact property name, or a deterministic nonblank fallback.</summary>
    public string SourceName { get; }

    /// <summary>The exact decoded JSON property name, including blank names.</summary>
    public string OriginalPropertyName { get; }

    public long FirstSeenRecordOrdinal { get; }

    public int FirstSeenPropertyOrdinal { get; }

    public JsonTableColumnLogicalType LogicalType { get; }

    public JsonTableColumnSchemaResolution Resolution { get; }

    public JsonTableColumnInferenceReason Reason { get; }

    public JsonTableInferenceConfidence Confidence { get; }

    public bool Nullable { get; }

    public JsonMissingPropertyPolicy MissingPolicy { get; }

    public JsonTableOverrideValidationStatus OverrideValidation { get; }

    public long PresentCount { get; }

    public long NullCount { get; }

    public long MissingCount { get; }

    public long ProfiledNonNullCount { get; }

    public long ProfiledStringCount { get; }

    public long ProfiledBooleanCount { get; }

    public long ProfiledNumberCount { get; }

    public long ProfiledObjectCount { get; }

    public long ProfiledArrayCount { get; }

    public long ProfiledLexemePreservationCount { get; }

    public long ObservedMaxCanonicalValueBytes { get; }

    public int? ObservedPrecision { get; }

    public int? ObservedScale { get; }

    public long? FirstOverrideMismatchRecordOrdinal { get; }
}

public sealed class JsonTableSchemaInferenceResult
{
    internal JsonTableSchemaInferenceResult(
        JsonSourceBinding binding,
        JsonTableSchemaInferenceRecipe recipe,
        long totalRecords,
        long eligibleObjectRecords,
        long ineligibleRecords,
        long totalColumnNameBytes,
        long profileRecordsExamined,
        long profileBytesExamined,
        bool profileRecordLimitReached,
        bool profileByteLimitReached,
        MigrationProfileCoverage structuralCoverage,
        MigrationProfileCoverage typeProfileCoverage,
        JsonTableColumnSchema[] columns,
        MigrationDiagnostic[] diagnostics)
    {
        Binding = binding;
        Recipe = recipe;
        Source = binding.Source;
        SnapshotIdentity = binding.SnapshotIdentity;
        ContentDigest = binding.ContentDigest;
        ContentLength = binding.ContentLength;
        TableName = recipe.TableName;
        TotalRecords = totalRecords;
        EligibleObjectRecords = eligibleObjectRecords;
        IneligibleRecords = ineligibleRecords;
        TotalColumnNameBytes = totalColumnNameBytes;
        ProfileRecordsExamined = profileRecordsExamined;
        ProfileBytesExamined = profileBytesExamined;
        ProfileRecordLimitReached = profileRecordLimitReached;
        ProfileByteLimitReached = profileByteLimitReached;
        StructuralCoverage = structuralCoverage;
        TypeProfileCoverage = typeProfileCoverage;
        Columns = Array.AsReadOnly(columns);
        Diagnostics = Array.AsReadOnly(diagnostics);
    }

    public const string AlgorithmId = "csharpdb-json-table-schema-v1";

    public const string ScalarPolicyId = JsonTableScalarPolicy.AlgorithmId;

    public MigrationSourceIdentity Source { get; }

    public string SnapshotIdentity { get; }

    public string ContentDigest { get; }

    public long ContentLength { get; }

    public string TableName { get; }

    public long TotalRecords { get; }

    public long EligibleObjectRecords { get; }

    public long IneligibleRecords { get; }

    public long TotalColumnNameBytes { get; }

    public long ProfileRecordsExamined { get; }

    public long ProfileBytesExamined { get; }

    public bool ProfileRecordLimitReached { get; }

    public bool ProfileByteLimitReached { get; }

    public MigrationProfileCoverage StructuralCoverage { get; }

    public MigrationProfileCoverage TypeProfileCoverage { get; }

    public ReadOnlyCollection<JsonTableColumnSchema> Columns { get; }

    public ReadOnlyCollection<MigrationDiagnostic> Diagnostics { get; }

    public MigrationCatalog CreateCatalog(string targetCSharpDbVersion) =>
        JsonMigrationCatalogBuilder.Build(this, targetCSharpDbVersion);

    internal JsonSourceBinding Binding { get; }

    internal JsonTableSchemaInferenceRecipe Recipe { get; }
}

/// <summary>Stable fatal failures owned by table-schema inference.</summary>
public sealed class JsonTableSchemaInferenceException : Exception
{
    internal JsonTableSchemaInferenceException(
        string ruleId,
        string message,
        long limit,
        long observed)
        : base(message)
    {
        RuleId = ruleId;
        Limit = limit;
        Observed = observed;
    }

    public string RuleId { get; }

    public long Limit { get; }

    public long Observed { get; }
}
