namespace CSharpDB.Migration;

public enum MigrationArtifactKind
{
    Catalog,
    Plan,
    ValidationReport,
}

public static class MigrationArtifactFormats
{
    public const string CatalogV1 = "csharpdb-migration-catalog/v1";
    public const string PlanV1 = "csharpdb-migration-plan/v1";
    public const string ValidationReportV1 = "csharpdb-migration-validation/v1";
    public const string DigestAlgorithm = "sha256";

    public static string For(MigrationArtifactKind kind) => kind switch
    {
        MigrationArtifactKind.Catalog => CatalogV1,
        MigrationArtifactKind.Plan => PlanV1,
        MigrationArtifactKind.ValidationReport => ValidationReportV1,
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unknown migration artifact kind."),
    };
}

internal sealed record MigrationArtifactEnvelope<TPayload>
{
    public required string Format { get; init; }

    public required string DigestAlgorithm { get; init; }

    public required string Digest { get; init; }

    public required TPayload Payload { get; init; }
}
