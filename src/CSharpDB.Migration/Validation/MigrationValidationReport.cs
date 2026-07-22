namespace CSharpDB.Migration;

public enum MigrationSnapshotConsistencyStatus
{
    Established,
    NotEstablished,
    Unavailable,
}

public enum MigrationSchemaDifferenceKind
{
    MissingFromSource,
    MissingFromTarget,
    DefinitionMismatch,
}

public enum MigrationValidationMismatchKind
{
    SourceOnly,
    TargetOnly,
    Changed,
}

/// <summary>
/// Deterministic evidence produced by a validation run. The report deliberately
/// carries only identities, counts, digests, and stable diagnostic references;
/// timestamps, host details, temporary paths, and raw row values are not part of
/// the artifact contract.
/// </summary>
public sealed record MigrationValidationReport
{
    public required MigrationValidationBinding Binding { get; init; }

    public required MigrationValidationLevel Level { get; init; }

    public required MigrationValidationStatus Outcome { get; init; }

    public required MigrationSnapshotConsistencyEvidence SnapshotConsistency { get; init; }

    public required MigrationSchemaValidationEvidence Schema { get; init; }

    public IReadOnlyList<MigrationObjectValidationEvidence> Objects { get; init; } = [];

    public IReadOnlyList<MigrationValidationDiagnosticEvidence> Diagnostics { get; init; } = [];
}

public sealed record MigrationValidationBinding
{
    public required string TargetCSharpDbVersion { get; init; }

    public required string PlanDigest { get; init; }

    public required string CatalogDigest { get; init; }

    public required string CapabilityDigest { get; init; }

    public required string SourceIdentity { get; init; }

    public required string SourceFingerprint { get; init; }

    public required string TargetIdentity { get; init; }

    public required string SourceSnapshotIdentity { get; init; }

    public required string TargetSnapshotIdentity { get; init; }

    public required string CanonicalizationVersion { get; init; }

    public required string CanonicalizationContractDigest { get; init; }
}

public sealed record MigrationSnapshotConsistencyEvidence
{
    public required MigrationSnapshotConsistencyStatus Status { get; init; }
}

public sealed record MigrationSchemaValidationEvidence
{
    public required MigrationValidationStatus Status { get; init; }

    public required string SourceSchemaDigest { get; init; }

    public required string TargetSchemaDigest { get; init; }

    public IReadOnlyList<MigrationSchemaDifferenceEvidence> Differences { get; init; } = [];
}

public sealed record MigrationSchemaDifferenceEvidence
{
    public required string ObjectId { get; init; }

    public required MigrationSchemaDifferenceKind Kind { get; init; }

    public string? SourceDefinitionDigest { get; init; }

    public string? TargetDefinitionDigest { get; init; }
}

public sealed record MigrationObjectValidationEvidence
{
    public required string SourceObjectId { get; init; }

    public required string TargetObjectId { get; init; }

    public required MigrationValidationStatus Status { get; init; }

    public required string CanonicalTypeContractDigest { get; init; }

    public required string ObjectContractDigest { get; init; }

    public long? SourceRowCount { get; init; }

    public long? TargetRowCount { get; init; }

    public string? SourceChecksum { get; init; }

    public string? TargetChecksum { get; init; }

    public IReadOnlyList<MigrationValidationPartitionEvidence> Partitions { get; init; } = [];
}

public sealed record MigrationValidationPartitionEvidence
{
    public int PartitionId { get; init; }

    public required MigrationValidationStatus Status { get; init; }

    public long SourceRowCount { get; init; }

    public long TargetRowCount { get; init; }

    public required string SourceDigest { get; init; }

    public required string TargetDigest { get; init; }

    public IReadOnlyList<MigrationValidationMismatchEvidence> Mismatches { get; init; } = [];
}

public sealed record MigrationValidationMismatchEvidence
{
    public required MigrationValidationMismatchKind Kind { get; init; }

    public string? KeyHash { get; init; }

    public string? SourceRowHash { get; init; }

    public string? TargetRowHash { get; init; }

    public long SourceMultiplicity { get; init; }

    public long TargetMultiplicity { get; init; }
}

public sealed record MigrationValidationDiagnosticEvidence
{
    public required string DiagnosticId { get; init; }

    public required string RuleId { get; init; }

    public required MigrationDiagnosticSeverity Severity { get; init; }

    public required MigrationValidationStatus Status { get; init; }

    public required MigrationEvidenceLevel Evidence { get; init; }

    public string? ObjectId { get; init; }

    public int? PartitionId { get; init; }
}
