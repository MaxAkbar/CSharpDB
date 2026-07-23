using CSharpDB.Primitives;

namespace CSharpDB.Migration;

public enum MigrationSourceKind
{
    Synthetic,
    Csv,
    Json,
    Sqlite,
    LiteDb,
    Access,
    SqlServer,
    MySql,
    CSharpDb,
}

public enum MigrationObjectKind
{
    Database,
    Namespace,
    Table,
    Collection,
    Column,
    Key,
    ForeignKey,
    CheckConstraint,
    Index,
    Sequence,
    View,
    Trigger,
    Routine,
    Other,
}

public enum MigrationCompatibilityStatus
{
    Compatible,
    CompatibleWithRewrite,
    Conditional,
    Unsupported,
    Unknown,
}

public enum MigrationEvidenceLevel
{
    Parsed = 1,
    CapabilityMatched = 2,
    Bound = 3,
    ScratchExecuted = 4,
    DifferentiallyValidated = 5,
}

public enum MigrationDiagnosticSeverity
{
    Information,
    Warning,
    Error,
}

public enum MigrationMappingClassification
{
    Exact,
    LosslessReencoded,
    Lossy,
    Unsupported,
}

public enum MigrationMappingProfile
{
    Preserve,
    Queryable,
    Custom,
}

public enum MigrationCoverageKind
{
    None,
    Sample,
    Full,
}

public enum MigrationConsistencyKind
{
    Immutable,
    Snapshot,
    Backup,
    Transaction,
    Watermark,
    BestEffort,
    Unavailable,
}

public enum MigrationResumeMode
{
    TransactionalReceipts,
    StableKeyUpsert,
    Disabled,
}

public enum MigrationRejectMode
{
    FailFast,
    DeterministicRejects,
}

public sealed record MigrationConsistencyStrategy
{
    public required MigrationConsistencyKind Kind { get; init; }

    public required string Description { get; init; }

    public string? Watermark { get; init; }
}

public sealed record MigrationSourceIdentity
{
    public required MigrationSourceKind Kind { get; init; }

    /// <summary>
    /// A stable non-secret identity. Credentials and raw connection strings are prohibited.
    /// </summary>
    public required string Identity { get; init; }

    public required string Fingerprint { get; init; }

    public string? ProviderVersion { get; init; }

    public string? SourceVersion { get; init; }

    public required MigrationConsistencyStrategy Consistency { get; init; }
}

public sealed record MigrationSourceSpan
{
    public string? SourceId { get; init; }

    public int? Start { get; init; }

    public int? Length { get; init; }

    public int? Line { get; init; }

    public int? Column { get; init; }
}

public sealed record MigrationDiagnostic
{
    /// <summary>
    /// Stable identifier for this diagnostic occurrence. Overrides bind to this value,
    /// not to the broader rule id.
    /// </summary>
    public required string DiagnosticId { get; init; }

    public required string RuleId { get; init; }

    public required MigrationDiagnosticSeverity Severity { get; init; }

    public required MigrationCompatibilityStatus Status { get; init; }

    public required MigrationEvidenceLevel Evidence { get; init; }

    public required string Summary { get; init; }

    public required string Explanation { get; init; }

    public string? ObjectId { get; init; }

    public MigrationSourceSpan? SourceSpan { get; init; }

    public string? Remediation { get; init; }

    public bool CanOverride { get; init; }
}

public sealed record MigrationCatalogFacet
{
    public required string Name { get; init; }

    public string? Value { get; init; }
}

/// <summary>
/// An ordered, role-qualified structural reference. Unlike <c>DependsOn</c>,
/// member order is semantic and is preserved in migration artifacts.
/// </summary>
public sealed record MigrationObjectReference
{
    public required string ObjectId { get; init; }

    public required string Role { get; init; }

    public int Ordinal { get; init; }
}

public static class MigrationObjectReferenceRoles
{
    public const string Column = "column";

    public const string SourceColumn = "sourceColumn";

    public const string ReferencedKey = "referencedKey";
}

public sealed record MigrationCatalogObject
{
    public required string ObjectId { get; init; }

    public required MigrationObjectKind Kind { get; init; }

    /// <summary>
    /// Structural owner, kept separate from execution-order dependencies.
    /// </summary>
    public string? ParentObjectId { get; init; }

    public string? SourceNamespace { get; init; }

    public required string SourceName { get; init; }

    public string? NativeType { get; init; }

    public MigrationSourceSpan? SourceSpan { get; init; }

    public IReadOnlyList<MigrationCatalogFacet> Facets { get; init; } = [];

    /// <summary>
    /// Ordered structural members such as key/index columns and foreign-key
    /// source columns. Dependency order must never be used for this purpose.
    /// </summary>
    public IReadOnlyList<MigrationObjectReference> Members { get; init; } = [];

    public IReadOnlyList<string> DependsOn { get; init; } = [];
}

public sealed record MigrationCatalog
{
    public required string TargetCSharpDbVersion { get; init; }

    public required MigrationSourceIdentity Source { get; init; }

    public IReadOnlyList<MigrationCatalogObject> Objects { get; init; } = [];

    public IReadOnlyList<MigrationDiagnostic> Diagnostics { get; init; } = [];
}

public sealed record MigrationProfileCoverage
{
    public required MigrationCoverageKind Kind { get; init; }

    public long ValuesExamined { get; init; }

    public long? TotalValues { get; init; }

    /// <summary>
    /// True when apply must verify every value against the planned mapping.
    /// Sample-derived mappings must set this to true.
    /// </summary>
    public bool RequiresFullStreamValidation { get; init; }
}

public sealed record MigrationTypeMapping
{
    public required string SourceObjectId { get; init; }

    public required string SourceNativeType { get; init; }

    public DbType? TargetType { get; init; }

    /// <summary>
    /// The explicit target requested by a custom profile. This remains present
    /// even when that request is rejected and <see cref="TargetType"/> is null.
    /// </summary>
    public DbType? RequestedTargetType { get; init; }

    public required MigrationMappingClassification Classification { get; init; }

    public required MigrationMappingProfile Profile { get; init; }

    public required MigrationProfileCoverage Coverage { get; init; }

    public MigrationConversionDescriptor? Conversion { get; init; }

    public string? DiagnosticId { get; init; }
}

public sealed record MigrationConversionDescriptor
{
    public required string ConversionId { get; init; }

    public required int Version { get; init; }

    public IReadOnlyList<MigrationCatalogFacet> Parameters { get; init; } = [];
}

public sealed record MigrationPlanObject
{
    public required string SourceObjectId { get; init; }

    public string? TargetParentObjectId { get; init; }

    public bool Included { get; init; } = true;

    public string? ExclusionReason { get; init; }

    public string? TargetName { get; init; }

    public IReadOnlyList<MigrationTypeMapping> TypeMappings { get; init; } = [];

    public IReadOnlyList<string> DependsOn { get; init; } = [];
}

/// <summary>
/// Plan-bound limits and rule registry for durable deterministic row rejects.
/// This policy is absent for fail-fast plans so existing plan-v1 artifacts
/// retain their canonical JSON shape and digest.
/// </summary>
public sealed record MigrationDeterministicRejectPolicy
{
    public required string ContractVersion { get; init; }

    public IReadOnlyList<string> AllowedRuleIds { get; init; } = [];

    public required int MaxRejectedRowsPerBatch { get; init; }

    public required long MaxRejectedRowsPerRun { get; init; }

    public required int MaxRawValueBytes { get; init; }

    public required long MaxRawValueBytesPerBatch { get; init; }

    public required long MaxRawValueBytesPerRun { get; init; }

    public required long MaxArtifactBytes { get; init; }
}

public sealed record MigrationLoadPolicy
{
    public int BatchSize { get; init; } = 1_000;

    /// <summary>
    /// Maximum combined canonical payload retained for a single batch.
    /// Sources must split earlier when this limit would be exceeded.
    /// </summary>
    public long MaxBatchBytes { get; init; } = 64L * 1024 * 1024;

    /// <summary>
    /// Maximum canonical size of one scalar value, including BLOBs.
    /// </summary>
    public int MaxValueBytes { get; init; } = 16 * 1024 * 1024;

    public MigrationResumeMode ResumeMode { get; init; } = MigrationResumeMode.TransactionalReceipts;

    public MigrationRejectMode RejectMode { get; init; } = MigrationRejectMode.FailFast;

    public MigrationDeterministicRejectPolicy? RejectPolicy { get; init; }

    public bool CreateStagedTarget { get; init; } = true;
}

public sealed record MigrationValidationPolicy
{
    public bool ValidateSchema { get; init; } = true;

    public bool ValidateCounts { get; init; } = true;

    public bool ValidateChecksums { get; init; } = true;

    public string CanonicalizationVersion { get; init; } = "csharpdb-canon-v1";
}

public sealed record MigrationPlan
{
    public required string TargetCSharpDbVersion { get; init; }

    public required MigrationSourceIdentity Source { get; init; }

    public required string CatalogDigest { get; init; }

    public required string CapabilityDigest { get; init; }

    public required string NamingAlgorithmVersion { get; init; }

    public required string MappingPolicyId { get; init; }

    public required int MappingPolicyVersion { get; init; }

    public required MigrationMappingProfile MappingProfile { get; init; }

    public IReadOnlyList<MigrationPlanObject> Objects { get; init; } = [];

    public MigrationLoadPolicy Load { get; init; } = new();

    public MigrationValidationPolicy Validation { get; init; } = new();

    public IReadOnlyList<MigrationDiagnostic> Diagnostics { get; init; } = [];

    public IReadOnlyList<string> AcceptedDiagnosticIds { get; init; } = [];

    /// <summary>
    /// Explicit acknowledgement that these catalog objects will be omitted.
    /// This is distinct from accepting an overrideable diagnostic: excluded
    /// unsupported source facts are never silently re-enabled.
    /// </summary>
    public IReadOnlyList<string> AcceptedExclusionObjectIds { get; init; } = [];

    public string? GeneratedDdlDigest { get; init; }
}
