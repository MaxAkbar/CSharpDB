using CSharpDB.Pipelines.Models;
using CSharpDB.Pipelines.Validation;
using System.Text.Json;

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
        bool skipBadRows = request.Package.Options.ErrorMode == PipelineErrorMode.SkipBadRows;
        int maxRejects = request.Package.Options.MaxRejects <= 0
            ? int.MaxValue
            : request.Package.Options.MaxRejects;

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
                long rowsWritten = 0;
                var rejects = new List<PipelineRejectRecord>();

                try
                {
                    PipelineRowBatch batch = sourceBatch;
                    foreach (var transform in transforms)
                    {
                        currentStep = "transform";
                        currentComponent = transform.Name;
                        batch = await transform.TransformAsync(batch, context, ct);
                    }

                    if (request.Mode is PipelineExecutionMode.Run or PipelineExecutionMode.Resume)
                    {
                        currentStep = "destination-write";
                        currentComponent = GetDestinationLabel(request.Package.Destination);
                        currentBatch = batch;
                        await destination.WriteBatchAsync(batch, context, ct);
                        rowsWritten = batch.Rows.Count;
                    }
                }
                catch (Exception ex) when (skipBadRows && sourceBatch.Rows.Count > 0)
                {
                    if (sourceBatch.Rows.Count == 1)
                    {
                        rejects.Add(CreateReject(sourceBatch.StartingRowNumber, sourceBatch.Rows[0], ex));
                    }
                    else
                    {
                        for (int i = 0; i < sourceBatch.Rows.Count; i++)
                        {
                            var singleRowBatch = new PipelineRowBatch
                            {
                                BatchNumber = sourceBatch.BatchNumber,
                                StartingRowNumber = sourceBatch.StartingRowNumber + i,
                                Rows = [sourceBatch.Rows[i]],
                            };

                            currentBatch = singleRowBatch;

                            try
                            {
                                PipelineRowBatch transformedBatch = singleRowBatch;
                                foreach (var transform in transforms)
                                {
                                    currentStep = "transform";
                                    currentComponent = transform.Name;
                                    transformedBatch = await transform.TransformAsync(transformedBatch, context, ct);
                                }

                                if (request.Mode is PipelineExecutionMode.Run or PipelineExecutionMode.Resume)
                                {
                                    currentStep = "destination-write";
                                    currentComponent = GetDestinationLabel(request.Package.Destination);
                                    currentBatch = transformedBatch;
                                    await destination.WriteBatchAsync(transformedBatch, context, ct);
                                    rowsWritten += transformedBatch.Rows.Count;
                                }
                            }
                            catch (Exception rowEx)
                            {
                                rejects.Add(CreateReject(singleRowBatch.StartingRowNumber, singleRowBatch.Rows[0], rowEx));
                            }
                        }
                    }
                }

                metrics = new PipelineRunMetrics
                {
                    RowsRead = metrics.RowsRead + sourceBatch.Rows.Count,
                    RowsWritten = metrics.RowsWritten + rowsWritten,
                    RowsRejected = metrics.RowsRejected + rejects.Count,
                    BatchesCompleted = metrics.BatchesCompleted + 1,
                };

                if (rejects.Count > 0)
                {
                    await _runLogger.RejectsCapturedAsync(runId, rejects, ct);
                }

                if (request.Mode is PipelineExecutionMode.Run or PipelineExecutionMode.Resume)
                {
                    latestCheckpoint = new PipelineCheckpointState
                    {
                        BatchNumber = sourceBatch.BatchNumber,
                        StepName = "destination-write",
                        UpdatedUtc = DateTimeOffset.UtcNow,
                    };

                    await _checkpointStore.SaveAsync(runId, latestCheckpoint, ct);
                }

                if (metrics.RowsRejected > maxRejects)
                {
                    throw new InvalidOperationException(
                        $"Pipeline rejected {metrics.RowsRejected} row(s), exceeding MaxRejects={request.Package.Options.MaxRejects}.");
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

    private static PipelineRejectRecord CreateReject(
        long rowNumber,
        IReadOnlyDictionary<string, object?> row,
        Exception exception)
    {
        return new PipelineRejectRecord
        {
            RowNumber = rowNumber,
            Reason = exception.Message,
            PayloadJson = JsonSerializer.Serialize(row),
        };
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
