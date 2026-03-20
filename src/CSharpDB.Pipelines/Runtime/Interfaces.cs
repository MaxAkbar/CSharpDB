using CSharpDB.Pipelines.Models;

namespace CSharpDB.Pipelines.Runtime;

public interface IPipelineSource
{
    Task OpenAsync(PipelineExecutionContext context, CancellationToken ct = default);
    IAsyncEnumerable<PipelineRowBatch> ReadBatchesAsync(PipelineExecutionContext context, CancellationToken ct = default);
}

public interface IPipelineDestination
{
    Task InitializeAsync(PipelineExecutionContext context, CancellationToken ct = default);
    Task WriteBatchAsync(PipelineRowBatch batch, PipelineExecutionContext context, CancellationToken ct = default);
    Task CompleteAsync(PipelineExecutionContext context, CancellationToken ct = default);
}

public interface IPipelineTransform
{
    string Name { get; }

    ValueTask<PipelineRowBatch> TransformAsync(
        PipelineRowBatch batch,
        PipelineExecutionContext context,
        CancellationToken ct = default);
}

public interface IPipelineCheckpointStore
{
    Task<PipelineCheckpointState?> LoadAsync(string runId, CancellationToken ct = default);
    Task SaveAsync(string runId, PipelineCheckpointState checkpoint, CancellationToken ct = default);
}

public interface IPipelineRunLogger
{
    Task RunStartedAsync(PipelineExecutionContext context, CancellationToken ct = default);
    Task StatusChangedAsync(string runId, PipelineRunStatus status, PipelineRunMetrics metrics, CancellationToken ct = default);
    Task RejectsCapturedAsync(string runId, IReadOnlyList<PipelineRejectRecord> rejects, CancellationToken ct = default);
    Task RunCompletedAsync(PipelineRunResult result, CancellationToken ct = default);
}

public interface IPipelineComponentFactory
{
    IPipelineSource CreateSource(PipelineSourceDefinition definition);
    IReadOnlyList<IPipelineTransform> CreateTransforms(IReadOnlyList<PipelineTransformDefinition> definitions);
    IPipelineDestination CreateDestination(PipelineDestinationDefinition definition);
}

public interface IPipelineOrchestrator
{
    Task<PipelineRunResult> ExecuteAsync(PipelineRunRequest request, CancellationToken ct = default);
}
