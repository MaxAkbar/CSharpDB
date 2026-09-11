using CSharpDB.Client.Models;
using CSharpDB.Sql;

namespace CSharpDB.DevOps;

public sealed record DefinitionCatalogSnapshot(string Version, DateTimeOffset CapturedUtc,
    IReadOnlyList<DefinitionCatalogRecord> Definitions, IReadOnlyList<DefinitionCatalogDiagnostic> Diagnostics,
    int DocumentationVersion = 0);
public enum DependencyConfidence { Confirmed, Possible }
public enum DependencyRelationshipKind { Usage, ModelMembership, ProposedChange, ExternalArchive }
public sealed record DefinitionDependency(string SourceId, string TargetId, string? TargetColumn,
    string? OutputColumn, string Usage, string Location, SqlSourceSpan? Span = null,
    DependencyConfidence Confidence = DependencyConfidence.Confirmed,
    DependencyRelationshipKind Relationship = DependencyRelationshipKind.Usage);
public sealed record DefinitionAnalysisDiagnostic(string ObjectId, string Location, string Message);
public sealed record DefinitionAnalysis(DefinitionCatalogSnapshot Catalog,
    IReadOnlyList<DefinitionDependency> Dependencies, IReadOnlyList<DefinitionAnalysisDiagnostic> Diagnostics,
    IReadOnlyDictionary<string, IReadOnlyList<string>> OutputColumns);
public sealed record DefinitionTextMatch(int Start, int Length, int Line, int Column);
public sealed record DefinitionSearchResult(DefinitionCatalogRecord Definition, IReadOnlyList<DefinitionTextMatch> Matches, bool NameMatched);
public enum ColumnChangeKind { Rename, Drop, Type, Nullability }
public enum ColumnImpactCategory { ConfirmedUsage, IndirectEffects, EngineRestriction, NeedsReview, ModelMembership, ProposedChange, ExternalArchive }
public sealed record ColumnImpactFinding(string ObjectId, ColumnImpactCategory Category, string Reason,
    IReadOnlyList<DefinitionDependency> Path, string? Location = null);
public sealed record ColumnImpactReport(string Table, string Column, ColumnChangeKind Change,
    IReadOnlyList<ColumnImpactFinding> Findings, bool CoverageComplete);

/// <summary>Extension point for static language inspection; the inspector must never execute stored code.</summary>
public interface IDefinitionModuleAnalyzer
{
    ModuleDefinitionInspection Inspect(string source, CancellationToken ct = default);
}
public sealed record ModuleDefinitionReference(string Kind, string Text, int Line, bool Confirmed = false);
public sealed record ModuleDefinitionInspection(IReadOnlyList<ModuleDefinitionReference> References, IReadOnlyList<string> Diagnostics);
