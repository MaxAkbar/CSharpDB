using System.Collections.ObjectModel;
using CSharpDB.Migration;

namespace CSharpDB.Migration.Files.Csv;

public enum CsvColumnLogicalType
{
    Text,
    Boolean,
    SignedInteger,
    UnsignedInteger,
    Decimal,
    FloatingPoint,
    Guid,
    Date,
    Time,
    DateTime,
    DateTimeOffset,
}

public enum CsvColumnSchemaResolution
{
    Inferred,
    DefaultedToText,
    ExplicitOverride,
}

public enum CsvColumnInferenceReason
{
    ExactEvidence,
    InsufficientEvidence,
    MixedKinds,
    EmptyValue,
    LexicalPreservation,
    ExplicitOverride,
}

public enum CsvInferenceConfidence
{
    None,
    Low,
    Medium,
    High,
    Explicit,
}

public enum CsvOverrideValidationStatus
{
    NotApplicable,
    NotProfiled,
    SampleCompatible,
    FullCompatible,
    Incompatible,
}

/// <summary>
/// An ordinal-addressed source-schema declaration. It is intentionally
/// separate from migration target-type overrides: this contract describes
/// how decoded CSV scalars are interpreted before target mapping begins.
/// </summary>
public sealed record CsvColumnSchemaOverride
{
    /// <summary>Zero-based CSV column index.</summary>
    public required int ColumnIndex { get; init; }

    /// <summary>
    /// Optional exact header guard. A guard cannot match a headerless source.
    /// </summary>
    public string? ExpectedHeader { get; init; }

    public required CsvColumnLogicalType LogicalType { get; init; }

    /// <summary>
    /// Optional declared nullability. A false declaration is checked against
    /// every profiled value and must be checked again over the full stream.
    /// </summary>
    public bool? Nullable { get; init; }
}

public sealed record CsvSchemaInferenceOptions
{
    public string TableName { get; init; } = "csv_data";

    public IReadOnlyList<CsvColumnSchemaOverride> ColumnOverrides { get; init; } = [];

    /// <summary>
    /// Maximum decoded scalar characters that may contribute profile
    /// evidence. At most one additionally bounded logical record is parsed to
    /// identify the limit or exact EOF.
    /// </summary>
    public long MaxProfileCharacters { get; init; } = 64L * 1024 * 1024;
}

public sealed class CsvColumnSchema
{
    internal CsvColumnSchema(
        int columnIndex,
        string sourceName,
        string? originalHeader,
        CsvColumnLogicalType logicalType,
        CsvColumnLogicalType? suggestedLogicalType,
        CsvColumnSchemaResolution resolution,
        CsvColumnInferenceReason reason,
        CsvInferenceConfidence confidence,
        bool nullable,
        CsvOverrideValidationStatus overrideValidation,
        MigrationProfileCoverage coverage,
        long substantiveValueCount,
        long nullCount,
        long emptyCount,
        long missingCount,
        long quotedCount,
        long nonCanonicalNumericCount,
        int observedMaxLength,
        int? observedPrecision,
        int? observedScale,
        long? firstMissingDataRecordNumber,
        long? firstOverrideMismatchDataRecordNumber)
    {
        ColumnIndex = columnIndex;
        SourceName = sourceName;
        OriginalHeader = originalHeader;
        LogicalType = logicalType;
        SuggestedLogicalType = suggestedLogicalType;
        Resolution = resolution;
        Reason = reason;
        Confidence = confidence;
        Nullable = nullable;
        OverrideValidation = overrideValidation;
        Coverage = coverage;
        SubstantiveValueCount = substantiveValueCount;
        NullCount = nullCount;
        EmptyCount = emptyCount;
        MissingCount = missingCount;
        QuotedCount = quotedCount;
        NonCanonicalNumericCount = nonCanonicalNumericCount;
        ObservedMaxLength = observedMaxLength;
        ObservedPrecision = observedPrecision;
        ObservedScale = observedScale;
        FirstMissingDataRecordNumber = firstMissingDataRecordNumber;
        FirstOverrideMismatchDataRecordNumber = firstOverrideMismatchDataRecordNumber;
    }

    public int ColumnIndex { get; }

    /// <summary>Exact header, or a deterministic nonblank ordinal fallback.</summary>
    public string SourceName { get; }

    /// <summary>Exact decoded header; null for a headerless source.</summary>
    public string? OriginalHeader { get; }

    public CsvColumnLogicalType LogicalType { get; }

    /// <summary>A low-evidence hint that was not safe to activate automatically.</summary>
    public CsvColumnLogicalType? SuggestedLogicalType { get; }

    public CsvColumnSchemaResolution Resolution { get; }

    public CsvColumnInferenceReason Reason { get; }

    public CsvInferenceConfidence Confidence { get; }

    public bool Nullable { get; }

    public CsvOverrideValidationStatus OverrideValidation { get; }

    public MigrationProfileCoverage Coverage { get; }

    public long SubstantiveValueCount { get; }

    public long NullCount { get; }

    public long EmptyCount { get; }

    public long MissingCount { get; }

    public long QuotedCount { get; }

    public long NonCanonicalNumericCount { get; }

    public int ObservedMaxLength { get; }

    public int? ObservedPrecision { get; }

    public int? ObservedScale { get; }

    public long? FirstMissingDataRecordNumber { get; }

    public long? FirstOverrideMismatchDataRecordNumber { get; }
}

public sealed class CsvSchemaInferenceResult
{
    private readonly CsvSourceBinding binding;

    internal CsvSchemaInferenceResult(
        CsvSourceBinding binding,
        string tableName,
        long recordsExamined,
        long profileCharactersExamined,
        bool profileCharacterLimitReached,
        bool reachedEndOfSource,
        MigrationProfileCoverage coverage,
        CsvColumnSchema[] columns,
        MigrationDiagnostic[] diagnostics)
    {
        this.binding = binding;
        TableName = tableName;
        RecordsExamined = recordsExamined;
        ProfileCharactersExamined = profileCharactersExamined;
        ProfileCharacterLimitReached = profileCharacterLimitReached;
        ReachedEndOfSource = reachedEndOfSource;
        Coverage = coverage;
        Columns = Array.AsReadOnly(columns);
        Diagnostics = Array.AsReadOnly(diagnostics);
    }

    public const string AlgorithmId = "csharpdb-csv-schema-v1";

    public const string ScalarPolicyId = CsvScalarLexicalPolicy.AlgorithmId;

    public MigrationSourceIdentity Source => binding.Source;

    public string SnapshotIdentity => binding.SnapshotIdentity;

    public string TableName { get; }

    public long RecordsExamined { get; }

    public long ProfileCharactersExamined { get; }

    public bool ProfileCharacterLimitReached { get; }

    public bool ReachedEndOfSource { get; }

    public MigrationProfileCoverage Coverage { get; }

    public ReadOnlyCollection<CsvColumnSchema> Columns { get; }

    public ReadOnlyCollection<MigrationDiagnostic> Diagnostics { get; }

    internal CsvSourceBinding Binding => binding;

    public MigrationCatalog CreateCatalog(string targetCSharpDbVersion) =>
        CsvMigrationCatalogBuilder.Build(this, targetCSharpDbVersion);

    /// <summary>
    /// Validates and converts a decoded scalar to the invariant source text
    /// consumed by the migration mapping layer. Explicit schema overrides use
    /// their declared normalization grammar; inferred types remain strict.
    /// </summary>
    public bool TryNormalizeScalar(
        int columnIndex,
        string text,
        out string? canonicalText)
    {
        ArgumentNullException.ThrowIfNull(text);
        if ((uint)columnIndex >= (uint)Columns.Count)
            throw new ArgumentOutOfRangeException(nameof(columnIndex));

        CsvColumnSchema column = Columns[columnIndex];
        return CsvScalarLexicalPolicy.TryNormalize(
            text,
            column.LogicalType,
            binding.Culture,
            allowLexicalNormalization:
                column.Resolution == CsvColumnSchemaResolution.ExplicitOverride,
            out canonicalText);
    }
}
