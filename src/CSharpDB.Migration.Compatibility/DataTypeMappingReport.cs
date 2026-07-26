using CSharpDB.Primitives;

namespace CSharpDB.Migration.Compatibility;

public static class DataTypeMappingReportFormats
{
    public const string V1 = "csharpdb-data-type-mapping-report/v1";
}

public sealed record DataTypeMappingReportOptions
{
    public MigrationMappingProfile Profile { get; init; } =
        MigrationMappingProfile.Preserve;

    public IReadOnlyDictionary<string, DbType> CustomTargetTypes { get; init; } =
        new Dictionary<string, DbType>(StringComparer.Ordinal);
}

public sealed record DataTypeMappingReport
{
    public string Format { get; init; } = DataTypeMappingReportFormats.V1;

    public required string TargetCSharpDbVersion { get; init; }

    public required MigrationSourceKind SourceKind { get; init; }

    public required string CatalogDigest { get; init; }

    public required string MappingPolicyId { get; init; }

    public required int MappingPolicyVersion { get; init; }

    public required MigrationMappingProfile Profile { get; init; }

    public required DataTypeMappingReportSummary Summary { get; init; }

    public IReadOnlyList<DataTypeMappingReportEntry> Entries { get; init; } = [];
}

public sealed record DataTypeMappingReportSummary
{
    public int Total { get; init; }

    public int Exact { get; init; }

    public int LosslessReencoded { get; init; }

    public int Lossy { get; init; }

    public int Unsupported { get; init; }

    public int RequiresFullStreamValidation { get; init; }
}

public sealed record DataTypeMappingReportEntry
{
    public required string SourceObjectId { get; init; }

    public required MigrationObjectKind SourceObjectKind { get; init; }

    public string? ParentObjectId { get; init; }

    public string? SourceNamespace { get; init; }

    public required string SourceName { get; init; }

    public required string SourceNativeType { get; init; }

    public required string SourceLogicalType { get; init; }

    public DbType? TargetType { get; init; }

    public DbType? RequestedTargetType { get; init; }

    public required MigrationMappingClassification Classification { get; init; }

    public required MigrationMappingProfile Profile { get; init; }

    public required MigrationProfileCoverage Coverage { get; init; }

    public MigrationConversionDescriptor? Conversion { get; init; }

    public DataTypeMappingReportDiagnostic? Diagnostic { get; init; }
}

public sealed record DataTypeMappingReportDiagnostic
{
    public required string DiagnosticId { get; init; }

    public required string RuleId { get; init; }

    public required MigrationDiagnosticSeverity Severity { get; init; }

    public required MigrationCompatibilityStatus Status { get; init; }

    public required string Summary { get; init; }

    public required string Explanation { get; init; }

    public string? Remediation { get; init; }

    public bool CanOverride { get; init; }
}
