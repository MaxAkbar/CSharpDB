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

        var metrics = new PipelineRunMetrics();
        PipelineCheckpointState? latestCheckpoint = checkpoint;
        string currentStep = "starting";
        string? currentComponent = null;
        PipelineRowBatch? currentBatch = null;

        try
        {
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

            currentStep = "source-open";
            currentComponent = GetSourceLabel(request.Package.Source);
            await source.OpenAsync(context, ct);

            if (request.Mode is PipelineExecutionMode.Run or PipelineExecutionMode.Resume)
            {
                currentStep = "destination-initialize";
                currentComponent = GetDestinationLabel(request.Package.Destination);
                await destination.InitializeAsync(context, ct);
            }

            currentStep = "source-read";
            currentComponent = GetSourceLabel(request.Package.Source);
            await _runLogger.StatusChangedAsync(runId, PipelineRunStatus.Running, metrics, ct);

            await foreach (var sourceBatch in source.ReadBatchesAsync(context, ct))
            {
                currentBatch = sourceBatch;
                PipelineRowBatch batch = sourceBatch;
                foreach (var transform in transforms)
                {
                    currentStep = "transform";
                    currentComponent = transform.Name;
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
                    currentStep = "destination-write";
                    currentComponent = GetDestinationLabel(request.Package.Destination);
                    currentBatch = batch;
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
                currentStep = "destination-complete";
                currentComponent = GetDestinationLabel(request.Package.Destination);
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
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            var failedResult = new PipelineRunResult
            {
                RunId = runId,
                PipelineName = request.Package.Name,
                Mode = request.Mode,
                Status = PipelineRunStatus.Failed,
                StartedUtc = startedUtc,
                CompletedUtc = DateTimeOffset.UtcNow,
                Metrics = metrics,
                Checkpoint = latestCheckpoint,
                ErrorSummary = FormatErrorSummary(currentStep, currentComponent, currentBatch, ex),
            };

            await _runLogger.RunCompletedAsync(failedResult, ct);
            return failedResult;
        }
    }

    private static string FormatErrorSummary(string step, string? component, PipelineRowBatch? batch, Exception exception)
    {
        var segments = new List<string>
        {
            $"Step: {step}",
        };

        if (!string.IsNullOrWhiteSpace(component))
        {
            segments.Add($"Component: {component}");
        }

        if (batch is not null)
        {
            segments.Add($"Batch: {batch.BatchNumber}");
            segments.Add($"Starting row: {batch.StartingRowNumber}");
        }

        segments.Add($"Error: {exception.Message}");
        return string.Join(Environment.NewLine, segments);
    }

    private static string GetSourceLabel(PipelineSourceDefinition definition) => definition.Kind switch
    {
        PipelineSourceKind.CsvFile => "csv-source",
        PipelineSourceKind.JsonFile => "json-source",
        PipelineSourceKind.CSharpDbTable => $"table-source:{definition.TableName}",
        PipelineSourceKind.SqlQuery => "sql-query-source",
        _ => definition.Kind.ToString(),
    };

    private static string GetDestinationLabel(PipelineDestinationDefinition definition) => definition.Kind switch
    {
        PipelineDestinationKind.CsvFile => "csv-destination",
        PipelineDestinationKind.JsonFile => "json-destination",
        PipelineDestinationKind.CSharpDbTable => $"table-destination:{definition.TableName}",
        _ => definition.Kind.ToString(),
    };
}
