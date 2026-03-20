namespace CSharpDB.Pipelines.Models;

public enum PipelineExecutionMode
{
    Validate,
    DryRun,
    Run,
    Resume,
}

public enum PipelineRunStatus
{
    Pending,
    Validating,
    Running,
    Succeeded,
    Failed,
    Canceled,
}

public sealed class PipelineRunRequest
{
    public required PipelinePackageDefinition Package { get; init; }
    public PipelineExecutionMode Mode { get; init; } = PipelineExecutionMode.Run;
    public string? ExistingRunId { get; init; }
}

public sealed class PipelineRunResult
{
    public required string RunId { get; init; }
    public required string PipelineName { get; init; }
    public required PipelineExecutionMode Mode { get; init; }
    public required PipelineRunStatus Status { get; init; }
    public DateTimeOffset StartedUtc { get; init; }
    public DateTimeOffset? CompletedUtc { get; init; }
    public PipelineRunMetrics Metrics { get; init; } = new();
    public PipelineCheckpointState? Checkpoint { get; init; }
    public string? ErrorSummary { get; init; }
}

public sealed class PipelineRunMetrics
{
    public long RowsRead { get; init; }
    public long RowsWritten { get; init; }
    public long RowsRejected { get; init; }
    public int BatchesCompleted { get; init; }
}

public sealed class PipelineCheckpointState
{
    public string? StepName { get; init; }
    public long BatchNumber { get; init; }
    public string? OffsetToken { get; init; }
    public DateTimeOffset UpdatedUtc { get; init; }
}

public sealed class PipelineRejectRecord
{
    public long RowNumber { get; init; }
    public required string Reason { get; init; }
    public string? PayloadJson { get; init; }
}

public sealed class PipelineDefinitionSummary
{
    public required string Name { get; init; }
    public string Version { get; init; } = string.Empty;
    public string? Description { get; init; }
    public int Revision { get; init; }
    public DateTimeOffset UpdatedUtc { get; init; }
}

public sealed class PipelineRevisionSummary
{
    public required string Name { get; init; }
    public int Revision { get; init; }
    public string Version { get; init; } = string.Empty;
    public string? Description { get; init; }
    public DateTimeOffset CreatedUtc { get; init; }
}

public sealed class PipelineExecutionContext
{
    public required string RunId { get; init; }
    public required PipelinePackageDefinition Package { get; init; }
    public required PipelineExecutionMode Mode { get; init; }
    public PipelineCheckpointState? Checkpoint { get; init; }
}

public sealed class PipelineRowBatch
{
    public required IReadOnlyList<Dictionary<string, object?>> Rows { get; init; }
    public long BatchNumber { get; init; }
    public long StartingRowNumber { get; init; }
}
