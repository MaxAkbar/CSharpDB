namespace CSharpDB.Client.Models;

/// <summary>One persisted definition, or a consecutive fragment of a large definition.</summary>
public sealed record DefinitionCatalogRecord
{
    public required string Id { get; init; }
    public required string Kind { get; init; }
    public required string Name { get; init; }
    public string? OwnerName { get; init; }
    public required string Source { get; init; }
    public string Format { get; init; } = "sql";
    public string? MetadataJson { get; init; }
    public string SourceHash { get; init; } = "";
    public bool IsEnabled { get; init; } = true;
    public DefinitionDocumentation? Documentation { get; init; }
    public int PartIndex { get; init; }
    public int PartCount { get; init; } = 1;
}

/// <summary>Optional documentation metadata. A null override with a revision is a retained deletion.</summary>
public sealed record DefinitionDocumentation
{
    public string? NativeDescription { get; init; }
    public string? Description { get; init; }
    public long Revision { get; init; }
    public string DefinitionFingerprint { get; init; } = "";
    public bool HasStableIdentity { get; init; }
    public bool NeedsReview { get; init; }
}

public sealed record DefinitionCatalogDiagnostic(string Source, string Message);

public sealed record DefinitionCatalogPage
{
    public int DocumentationVersion { get; init; }
    public string CatalogVersion { get; init; } = "";
    public DateTimeOffset CapturedUtc { get; init; }
    public IReadOnlyList<DefinitionCatalogRecord> Records { get; init; } = [];
    public IReadOnlyList<DefinitionCatalogDiagnostic> Diagnostics { get; init; } = [];
    public string? ContinuationToken { get; init; }
}
