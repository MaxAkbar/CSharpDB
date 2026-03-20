using CSharpDB.Pipelines.Models;
using CSharpDB.Pipelines.Validation;

namespace CSharpDB.Pipelines.Runtime;

public sealed class PipelineOrchestrator : IPipelineOrchestrator
{
    private readonly IPipelineComponentFactory _componentFactory;
    private readonly IPipelineCheckpointStore _checkpointStore;
    private readonly IPipelineRunLogger _runLogger;

    public PipelineOrchestrator(
        IPipelineComponentFactory componentFactory,
        IPipelineCheckpointStore checkpointStore,
        IPipelineRunLogger runLogger)
    {
        _componentFactory = componentFactory;
        _checkpointStore = checkpointStore;
        _runLogger = runLogger;
    }

    public async Task<PipelineRunResult> ExecuteAsync(PipelineRunRequest request, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.Package);

        string runId = string.IsNullOrWhiteSpace(request.ExistingRunId)
            ? Guid.NewGuid().ToString("N")
            : request.ExistingRunId;

        DateTimeOffset startedUtc = DateTimeOffset.UtcNow;
        PipelineValidationResult validation = PipelinePackageValidator.Validate(request.Package);
        if (!validation.IsValid)
        {
            return new PipelineRunResult
            {
                RunId = runId,
                PipelineName = request.Package.Name,
                Mode = request.Mode,
                Status = PipelineRunStatus.Failed,
                StartedUtc = startedUtc,
                CompletedUtc = DateTimeOffset.UtcNow,
                ErrorSummary = string.Join(Environment.NewLine, validation.Errors.Select(e => $"{e.Path}: {e.Message}")),
            };
        }

        PipelineCheckpointState? checkpoint = request.Mode == PipelineExecutionMode.Resume
            ? await _checkpointStore.LoadAsync(runId, ct)
            : null;

        var context = new PipelineExecutionContext
        {
            RunId = runId,
            Package = request.Package,
            Mode = request.Mode,
            Checkpoint = checkpoint,
        };

        await _runLogger.RunStartedAsync(context, ct);

        if (request.Mode == PipelineExecutionMode.Validate)
        {
            var validateResult = new PipelineRunResult
            {
                RunId = runId,
                PipelineName = request.Package.Name,
                Mode = request.Mode,
                Status = PipelineRunStatus.Succeeded,
                StartedUtc = startedUtc,
                CompletedUtc = DateTimeOffset.UtcNow,
            };

            await _runLogger.RunCompletedAsync(validateResult, ct);
            return validateResult;
        }

        var source = _componentFactory.CreateSource(request.Package.Source);
        var transforms = _componentFactory.CreateTransforms(request.Package.Transforms);
        var destination = _componentFactory.CreateDestination(request.Package.Destination);

        await source.OpenAsync(context, ct);

        if (request.Mode is PipelineExecutionMode.Run or PipelineExecutionMode.Resume)
        {
            await destination.InitializeAsync(context, ct);
        }

        var metrics = new PipelineRunMetrics();
        PipelineCheckpointState? latestCheckpoint = checkpoint;

        await _runLogger.StatusChangedAsync(runId, PipelineRunStatus.Running, metrics, ct);

        await foreach (var sourceBatch in source.ReadBatchesAsync(context, ct))
        {
            PipelineRowBatch batch = sourceBatch;
            foreach (var transform in transforms)
            {
                batch = await transform.TransformAsync(batch, context, ct);
            }

            metrics = new PipelineRunMetrics
            {
                RowsRead = metrics.RowsRead + sourceBatch.Rows.Count,
                RowsWritten = metrics.RowsWritten + (request.Mode == PipelineExecutionMode.DryRun ? 0 : batch.Rows.Count),
                RowsRejected = metrics.RowsRejected,
                BatchesCompleted = metrics.BatchesCompleted + 1,
            };

            if (request.Mode is PipelineExecutionMode.Run or PipelineExecutionMode.Resume)
            {
                await destination.WriteBatchAsync(batch, context, ct);
            }

            if (request.Mode is PipelineExecutionMode.Run or PipelineExecutionMode.Resume)
            {
                latestCheckpoint = new PipelineCheckpointState
                {
                    BatchNumber = batch.BatchNumber,
                    StepName = "destination-write",
                    UpdatedUtc = DateTimeOffset.UtcNow,
                };

                await _checkpointStore.SaveAsync(runId, latestCheckpoint, ct);
            }

            await _runLogger.StatusChangedAsync(runId, PipelineRunStatus.Running, metrics, ct);
        }

        if (request.Mode is PipelineExecutionMode.Run or PipelineExecutionMode.Resume)
        {
            await destination.CompleteAsync(context, ct);
        }

        var result = new PipelineRunResult
        {
            RunId = runId,
            PipelineName = request.Package.Name,
            Mode = request.Mode,
            Status = PipelineRunStatus.Succeeded,
            StartedUtc = startedUtc,
            CompletedUtc = DateTimeOffset.UtcNow,
            Metrics = metrics,
            Checkpoint = latestCheckpoint,
        };

        await _runLogger.RunCompletedAsync(result, ct);
        return result;
    }
}
