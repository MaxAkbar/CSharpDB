using CSharpDB.Primitives;

namespace CSharpDB.Pipelines.Models;

public sealed class PipelinePackageDefinition
{
    public string Name { get; init; } = string.Empty;
    public string Version { get; init; } = string.Empty;
    public string? Description { get; init; }
    public PipelineSourceDefinition Source { get; init; } = new();
    public IReadOnlyList<PipelineTransformDefinition> Transforms { get; init; } = [];
    public PipelineDestinationDefinition Destination { get; init; } = new();
    public PipelineExecutionOptions Options { get; init; } = new();
    public PipelineIncrementalOptions? Incremental { get; init; }
    public IReadOnlyList<PipelineCommandHookDefinition> Hooks { get; init; } = [];
    public DbAutomationMetadata? Automation { get; init; }
}

public enum PipelineCommandHookEvent
{
    OnRunStarted,
    OnBatchCompleted,
    OnRunSucceeded,
    OnRunFailed,
}

public sealed class PipelineCommandHookDefinition
{
    public PipelineCommandHookEvent Event { get; init; }
    public string CommandName { get; init; } = string.Empty;
    public IReadOnlyDictionary<string, object?>? Arguments { get; init; }
    public bool StopOnFailure { get; init; } = true;
}

public enum PipelineSourceKind
{
    CsvFile,
    JsonFile,
    CSharpDbTable,
    SqlQuery,
}

public sealed class PipelineSourceDefinition
{
    public PipelineSourceKind Kind { get; init; }
    public string? Path { get; init; }
    public string? ConnectionString { get; init; }
    public string? TableName { get; init; }
    public string? QueryText { get; init; }
    public bool HasHeaderRow { get; init; } = true;
}

public enum PipelineDestinationKind
{
    CSharpDbTable,
    CsvFile,
    JsonFile,
}

public sealed class PipelineDestinationDefinition
{
    public PipelineDestinationKind Kind { get; init; }
    public string? Path { get; init; }
    public string? ConnectionString { get; init; }
    public string? TableName { get; init; }
    public bool Overwrite { get; init; }
}

public enum PipelineTransformKind
{
    Select,
    Rename,
    Cast,
    Filter,
    Derive,
    Deduplicate,
}

public sealed class PipelineTransformDefinition
{
    public PipelineTransformKind Kind { get; init; }
    public IReadOnlyList<string>? SelectColumns { get; init; }
    public IReadOnlyList<PipelineRenameMapping>? RenameMappings { get; init; }
    public IReadOnlyList<PipelineCastMapping>? CastMappings { get; init; }
    public string? FilterExpression { get; init; }
    public IReadOnlyList<PipelineDerivedColumn>? DerivedColumns { get; init; }
    public IReadOnlyList<string>? DeduplicateKeys { get; init; }
}

public sealed class PipelineRenameMapping
{
    public string Source { get; init; } = string.Empty;
    public string Target { get; init; } = string.Empty;
}

public sealed class PipelineCastMapping
{
    public string Column { get; init; } = string.Empty;
    public DbType TargetType { get; init; }
}

public sealed class PipelineDerivedColumn
{
    public string Name { get; init; } = string.Empty;
    public string Expression { get; init; } = string.Empty;
}

public enum PipelineErrorMode
{
    FailFast,
    SkipBadRows,
}

public sealed class PipelineExecutionOptions
{
    public int BatchSize { get; init; } = 1000;
    public PipelineErrorMode ErrorMode { get; init; } = PipelineErrorMode.FailFast;
    public int CheckpointInterval { get; init; } = 1000;
    public int MaxRejects { get; init; }
}

public sealed class PipelineIncrementalOptions
{
    public string WatermarkColumn { get; init; } = string.Empty;
    public string? LastProcessedValue { get; init; }
}
