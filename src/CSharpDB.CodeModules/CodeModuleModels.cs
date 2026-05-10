using System.Collections.ObjectModel;

namespace CSharpDB.CodeModules;

public enum CodeModuleKind
{
    Form,
    Standard,
    Class,
}

public enum CodeModuleBuildStatus
{
    Succeeded,
    Failed,
}

public enum CodeModuleDiagnosticSeverity
{
    Hidden,
    Info,
    Warning,
    Error,
}

public sealed record CodeModuleHandler(
    string ModuleId,
    string TypeName,
    string MethodName);

public sealed record CodeModuleDefinition(
    string ModuleId,
    string Name,
    CodeModuleKind Kind,
    string Source,
    string? OwnerKind = null,
    string? OwnerId = null,
    string? TypeName = null,
    IReadOnlyDictionary<string, string>? Metadata = null,
    string Language = "csharp",
    string? SourceHash = null,
    DateTimeOffset? CreatedUtc = null,
    DateTimeOffset? UpdatedUtc = null)
{
    public IReadOnlyDictionary<string, string> SafeMetadata =>
        Metadata is null
            ? new ReadOnlyDictionary<string, string>(new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase))
            : new ReadOnlyDictionary<string, string>(new Dictionary<string, string>(Metadata, StringComparer.OrdinalIgnoreCase));
}

public sealed record CodeModuleSummary(
    string ModuleId,
    string Name,
    CodeModuleKind Kind,
    string? OwnerKind,
    string? OwnerId,
    string? TypeName,
    string SourceHash,
    DateTimeOffset CreatedUtc,
    DateTimeOffset UpdatedUtc);

public sealed record CodeModuleDiagnostic(
    string? ModuleId,
    string? Path,
    int Line,
    int Column,
    CodeModuleDiagnosticSeverity Severity,
    string Code,
    string Message);

public sealed record CodeModuleBuildResult(
    string ModuleSetHash,
    CodeModuleBuildStatus Status,
    IReadOnlyList<CodeModuleDiagnostic> Diagnostics,
    DateTimeOffset BuiltUtc,
    byte[]? AssemblyBytes = null)
{
    public bool Succeeded => Status == CodeModuleBuildStatus.Succeeded;
}

public sealed record CodeModuleTrustState(
    string DatabasePath,
    string ModuleSetHash,
    bool IsTrusted,
    DateTimeOffset? TrustedUtc = null);

public sealed record CodeModuleExportResult(
    string WorkspaceDirectory,
    string ManifestPath,
    int ModuleCount,
    string ModuleSetHash);

public enum CodeModuleImportChangeKind
{
    Added,
    Updated,
    Unchanged,
    Skipped,
    Conflict,
}

public sealed record CodeModuleImportChange(
    string ModuleId,
    string Path,
    CodeModuleImportChangeKind Kind,
    string? Message = null);

public sealed record CodeModuleImportResult(
    string WorkspaceDirectory,
    IReadOnlyList<CodeModuleImportChange> Changes)
{
    public bool HasConflicts => Changes.Any(change => change.Kind == CodeModuleImportChangeKind.Conflict);
}

public sealed record CodeModuleRuntimeOptions
{
    public bool EnableInProcessExecution { get; set; }
}
