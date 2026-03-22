using CSharpDB.Pipelines.Models;

namespace CSharpDB.Pipelines.Runtime;

public sealed class NullPipelineCheckpointStore : IPipelineCheckpointStore
{
    public Task<PipelineCheckpointState?> LoadAsync(string runId, CancellationToken ct = default)
        => Task.FromResult<PipelineCheckpointState?>(null);

    public Task SaveAsync(string runId, PipelineCheckpointState checkpoint, CancellationToken ct = default)
        => Task.CompletedTask;
}

public sealed class NullPipelineRunLogger : IPipelineRunLogger
{
    public Task RunStartedAsync(PipelineExecutionContext context, CancellationToken ct = default)
        => Task.CompletedTask;

    public Task StatusChangedAsync(string runId, PipelineRunStatus status, PipelineRunMetrics metrics, CancellationToken ct = default)
        => Task.CompletedTask;

    public Task RejectsCapturedAsync(string runId, IReadOnlyList<PipelineRejectRecord> rejects, CancellationToken ct = default)
        => Task.CompletedTask;

    public Task RunCompletedAsync(PipelineRunResult result, CancellationToken ct = default)
        => Task.CompletedTask;
}
