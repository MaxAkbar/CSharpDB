using CSharpDB.DevOps;

namespace CSharpDB.Admin.Models;

public enum CompareDeployMode
{
    Schema,
    Data,
    Drift,
    History,
}

public enum CompareDeployScriptKind
{
    None,
    Schema,
    Data,
}

public enum CompareDeployScriptEndpoint
{
    Source,
    Target,
}

public enum CompareDeployEndpointKind
{
    CurrentDatabase,
    DatabaseFile,
    TableArchive,
}

public sealed record CompareDeploySeed(
    CompareDeployMode Mode = CompareDeployMode.Schema,
    string? TableName = null,
    string? SourcePath = null,
    CompareDeployEndpointKind SourceKind = CompareDeployEndpointKind.DatabaseFile,
    string? TargetPath = null,
    CompareDeployEndpointKind TargetKind = CompareDeployEndpointKind.CurrentDatabase,
    CompareDeployScriptEndpoint? ScriptEndpoint = null,
    SchemaObjectKind? ScriptObjectKind = null,
    string? ScriptObjectName = null,
    bool? ScriptIncludeIndexes = null,
    bool? ScriptIncludeTriggers = null,
    bool? ScriptIncludeRelatedViews = null,
    bool? ScriptIncludeRelatedProcedures = null,
    bool ScriptOnOpen = false)
{
    public static CompareDeploySeed ForTableScript(string tableName) => new(
        Mode: CompareDeployMode.Schema,
        TableName: tableName,
        SourceKind: CompareDeployEndpointKind.CurrentDatabase,
        TargetKind: CompareDeployEndpointKind.CurrentDatabase,
        ScriptEndpoint: CompareDeployScriptEndpoint.Target,
        ScriptObjectKind: SchemaObjectKind.Table,
        ScriptObjectName: tableName,
        ScriptIncludeIndexes: true,
        ScriptIncludeTriggers: true,
        ScriptOnOpen: true);
}

public sealed record CompareDeployEndpointSpec(
    CompareDeployEndpointKind Kind,
    string? Path = null)
{
    public static readonly CompareDeployEndpointSpec CurrentDatabase = new(CompareDeployEndpointKind.CurrentDatabase);
}

public sealed record CompareDeployRunResult<T>(
    T Report,
    TimeSpan Elapsed);

public sealed record CompareDeploySchemaObjectOption(
    SchemaObjectKind Kind,
    string Name,
    string? ParentName = null);

public sealed record CompareDeployApplyResult(
    TimeSpan Elapsed,
    int RowsAffected);

public sealed record CompareDeployHistoryEntry(
    DateTime Timestamp,
    string Operation,
    string Target,
    string Summary,
    TimeSpan? Elapsed,
    bool IsError = false);
