using System.Runtime.CompilerServices;
using CSharpDB.Pipelines.Models;
using CSharpDB.Pipelines.Runtime;

namespace CSharpDB.Pipelines.Tests;

public sealed class PipelineOrchestratorTests
{
    [Fact]
    public async Task ExecuteAsync_ValidateMode_SucceedsWithoutCreatingComponents()
    {
        var factory = new FakeComponentFactory();
        var logger = new RecordingRunLogger();
        var orchestrator = new PipelineOrchestrator(factory, new RecordingCheckpointStore(), logger);

        PipelineRunResult result = await orchestrator.ExecuteAsync(new PipelineRunRequest
        {
            Package = CreatePackage(),
            Mode = PipelineExecutionMode.Validate,
        });

        Assert.Equal(PipelineRunStatus.Succeeded, result.Status);
        Assert.Equal(0, factory.SourceCreateCount);
        Assert.Single(logger.CompletedRuns);
    }

    [Fact]
    public async Task ExecuteAsync_DryRun_ReadsAndTransformsWithoutWriting()
    {
        var source = new FakeSource(
        [
            CreateBatch(1, 1, 2),
            CreateBatch(2, 3),
        ]);
        var destination = new FakeDestination();
        var transform = new RecordingTransform();
        var factory = new FakeComponentFactory(source, destination, [transform]);
        var checkpointStore = new RecordingCheckpointStore();
        var orchestrator = new PipelineOrchestrator(factory, checkpointStore, new RecordingRunLogger());

        PipelineRunResult result = await orchestrator.ExecuteAsync(new PipelineRunRequest
        {
            Package = CreatePackage(),
            Mode = PipelineExecutionMode.DryRun,
        });

        Assert.Equal(PipelineRunStatus.Succeeded, result.Status);
        Assert.Equal(3, result.Metrics.RowsRead);
        Assert.Equal(0, result.Metrics.RowsWritten);
        Assert.Equal(2, result.Metrics.BatchesCompleted);
        Assert.Equal(0, destination.InitializeCount);
        Assert.Equal(0, destination.WriteCount);
        Assert.Equal(0, destination.CompleteCount);
        Assert.Equal(2, transform.CallCount);
        Assert.Empty(checkpointStore.Saved);
    }

    [Fact]
    public async Task ExecuteAsync_RunMode_WritesBatchesAndCompletesDestination()
    {
        var source = new FakeSource([CreateBatch(1, 1, 2)]);
        var destination = new FakeDestination();
        var factory = new FakeComponentFactory(source, destination, []);
        var checkpointStore = new RecordingCheckpointStore();
        var orchestrator = new PipelineOrchestrator(factory, checkpointStore, new RecordingRunLogger());

        PipelineRunResult result = await orchestrator.ExecuteAsync(new PipelineRunRequest
        {
            Package = CreatePackage(errorMode: PipelineErrorMode.FailFast),
            Mode = PipelineExecutionMode.Run,
        });

        Assert.Equal(PipelineRunStatus.Succeeded, result.Status);
        Assert.Equal(2, result.Metrics.RowsRead);
        Assert.Equal(2, result.Metrics.RowsWritten);
        Assert.Equal(1, destination.InitializeCount);
        Assert.Equal(1, destination.WriteCount);
        Assert.Equal(1, destination.CompleteCount);
        Assert.Single(checkpointStore.Saved);
    }

    [Fact]
    public async Task ExecuteAsync_ResumeMode_LoadsExistingCheckpoint()
    {
        var runId = "resume-run-1";
        var source = new FakeSource([CreateBatch(2, 3, 4)]);
        var destination = new FakeDestination();
        var checkpointStore = new RecordingCheckpointStore
        {
            Existing = new PipelineCheckpointState
            {
                BatchNumber = 1,
                StepName = "destination-write",
                OffsetToken = "offset-1",
                UpdatedUtc = DateTimeOffset.UtcNow,
            },
        };
        var logger = new RecordingRunLogger();
        var factory = new FakeComponentFactory(source, destination, []);
        var orchestrator = new PipelineOrchestrator(factory, checkpointStore, logger);

        PipelineRunResult result = await orchestrator.ExecuteAsync(new PipelineRunRequest
        {
            Package = CreatePackage(),
            Mode = PipelineExecutionMode.Resume,
            ExistingRunId = runId,
        });

        Assert.Equal(PipelineRunStatus.Succeeded, result.Status);
        Assert.Equal(runId, checkpointStore.LoadedRunId);
        Assert.NotNull(logger.StartedContexts.Single().Checkpoint);
        Assert.Equal("offset-1", logger.StartedContexts.Single().Checkpoint?.OffsetToken);
    }

    [Fact]
    public async Task ExecuteAsync_InvalidPackage_ReturnsFailedResult()
    {
        var orchestrator = new PipelineOrchestrator(
            new FakeComponentFactory(),
            new RecordingCheckpointStore(),
            new RecordingRunLogger());

        PipelineRunResult result = await orchestrator.ExecuteAsync(new PipelineRunRequest
        {
            Package = new PipelinePackageDefinition(),
            Mode = PipelineExecutionMode.Run,
        });

        Assert.Equal(PipelineRunStatus.Failed, result.Status);
        Assert.Contains("Pipeline name is required.", result.ErrorSummary);
    }

    [Fact]
    public async Task ExecuteAsync_TransformFailure_ReturnsStepAndBatchContext()
    {
        var source = new FakeSource([CreateBatch(3, 41, 42)]);
        var transform = new ThrowingTransform("cast", "Cannot parse integer.");
        var orchestrator = new PipelineOrchestrator(
            new FakeComponentFactory(source, new FakeDestination(), [transform]),
            new RecordingCheckpointStore(),
            new RecordingRunLogger());

        PipelineRunResult result = await orchestrator.ExecuteAsync(new PipelineRunRequest
        {
            Package = CreatePackage(errorMode: PipelineErrorMode.FailFast),
            Mode = PipelineExecutionMode.Run,
        });

        Assert.Equal(PipelineRunStatus.Failed, result.Status);
        Assert.Contains("Step: transform", result.ErrorSummary);
        Assert.Contains("Component: cast", result.ErrorSummary);
        Assert.Contains("Batch: 3", result.ErrorSummary);
        Assert.Contains("Starting row: 41", result.ErrorSummary);
        Assert.Contains("Cannot parse integer.", result.ErrorSummary);
    }

    [Fact]
    public async Task ExecuteAsync_DestinationFailure_ReturnsStepAndComponentContext()
    {
        var source = new FakeSource([CreateBatch(1, 7)]);
        var destination = new ThrowingDestination("Cannot read Text as Integer.");
        var orchestrator = new PipelineOrchestrator(
            new FakeComponentFactory(source, destination, []),
            new RecordingCheckpointStore(),
            new RecordingRunLogger());

        PipelineRunResult result = await orchestrator.ExecuteAsync(new PipelineRunRequest
        {
            Package = CreatePackage(errorMode: PipelineErrorMode.FailFast),
            Mode = PipelineExecutionMode.Run,
        });

        Assert.Equal(PipelineRunStatus.Failed, result.Status);
        Assert.Contains("Step: destination-write", result.ErrorSummary);
        Assert.Contains("Component: table-destination:customers", result.ErrorSummary);
        Assert.Contains("Batch: 1", result.ErrorSummary);
        Assert.Contains("Starting row: 7", result.ErrorSummary);
        Assert.Contains("Cannot read Text as Integer.", result.ErrorSummary);
    }

    [Fact]
    public async Task ExecuteAsync_SkipBadRows_RecordsRejectsAndContinues()
    {
        var source = new FakeSource([CreateBatch(1, 1, 2)]);
        var destination = new RejectingDestination(row => Convert.ToInt32(row["id"]) == 2, "Duplicate key.");
        var logger = new RecordingRunLogger();
        var orchestrator = new PipelineOrchestrator(
            new FakeComponentFactory(source, destination, []),
            new RecordingCheckpointStore(),
            logger);

        PipelineRunResult result = await orchestrator.ExecuteAsync(new PipelineRunRequest
        {
            Package = CreatePackage(),
            Mode = PipelineExecutionMode.Run,
        });

        Assert.Equal(PipelineRunStatus.Succeeded, result.Status);
        Assert.Equal(2, result.Metrics.RowsRead);
        Assert.Equal(1, result.Metrics.RowsWritten);
        Assert.Equal(1, result.Metrics.RowsRejected);
        Assert.Equal(1, result.Metrics.BatchesCompleted);
        Assert.Single(logger.CapturedRejects);
        Assert.Single(logger.CapturedRejects[0]);
        Assert.Equal(2, logger.CapturedRejects[0][0].RowNumber);
        Assert.Contains("Duplicate key.", logger.CapturedRejects[0][0].Reason);
    }

    [Fact]
    public async Task ExecuteAsync_SkipBadRows_RespectsMaxRejects()
    {
        var source = new FakeSource([CreateBatch(1, 1, 2)]);
        var destination = new RejectingDestination(_ => true, "Bad row.");
        var checkpointStore = new RecordingCheckpointStore();
        var orchestrator = new PipelineOrchestrator(
            new FakeComponentFactory(source, destination, []),
            checkpointStore,
            new RecordingRunLogger());

        PipelineRunResult result = await orchestrator.ExecuteAsync(new PipelineRunRequest
        {
            Package = CreatePackage(maxRejects: 1),
            Mode = PipelineExecutionMode.Run,
        });

        Assert.Equal(PipelineRunStatus.Failed, result.Status);
        Assert.Contains("exceeding MaxRejects=1", result.ErrorSummary);
        Assert.Single(checkpointStore.Saved);
        Assert.Equal(1, checkpointStore.Saved[0].BatchNumber);
    }

    private static PipelinePackageDefinition CreatePackage(
        PipelineErrorMode errorMode = PipelineErrorMode.SkipBadRows,
        int maxRejects = 10) => new()
    {
        Name = "customers-import",
        Version = "1.0.0",
        Source = new PipelineSourceDefinition
        {
            Kind = PipelineSourceKind.CsvFile,
            Path = "customers.csv",
        },
        Destination = new PipelineDestinationDefinition
        {
            Kind = PipelineDestinationKind.CSharpDbTable,
            TableName = "customers",
        },
        Options = new PipelineExecutionOptions
        {
            BatchSize = 100,
            CheckpointInterval = 1,
            ErrorMode = errorMode,
            MaxRejects = errorMode == PipelineErrorMode.SkipBadRows ? maxRejects : 0,
        },
    };

    private static PipelineRowBatch CreateBatch(long batchNumber, params int[] ids) => new()
    {
        BatchNumber = batchNumber,
        StartingRowNumber = ids[0],
        Rows = ids.Select(id => new Dictionary<string, object?> { ["id"] = id }).ToArray(),
    };

    private sealed class FakeComponentFactory : IPipelineComponentFactory
    {
        private readonly IPipelineSource _source;
        private readonly IPipelineDestination _destination;
        private readonly IReadOnlyList<IPipelineTransform> _transforms;

        public int SourceCreateCount { get; private set; }

        public FakeComponentFactory()
            : this(new FakeSource([]), new FakeDestination(), [])
        {
        }

        public FakeComponentFactory(IPipelineSource source, IPipelineDestination destination, IReadOnlyList<IPipelineTransform> transforms)
        {
            _source = source;
            _destination = destination;
            _transforms = transforms;
        }

        public IPipelineSource CreateSource(PipelineSourceDefinition definition)
        {
            SourceCreateCount++;
            return _source;
        }

        public IReadOnlyList<IPipelineTransform> CreateTransforms(IReadOnlyList<PipelineTransformDefinition> definitions)
            => _transforms;

        public IPipelineDestination CreateDestination(PipelineDestinationDefinition definition)
            => _destination;
    }

    private sealed class FakeSource : IPipelineSource
    {
        private readonly IReadOnlyList<PipelineRowBatch> _batches;

        public FakeSource(IReadOnlyList<PipelineRowBatch> batches)
        {
            _batches = batches;
        }

        public Task OpenAsync(PipelineExecutionContext context, CancellationToken ct = default)
            => Task.CompletedTask;

        public async IAsyncEnumerable<PipelineRowBatch> ReadBatchesAsync(
            PipelineExecutionContext context,
            [EnumeratorCancellation] CancellationToken ct = default)
        {
            foreach (var batch in _batches)
            {
                yield return batch;
                await Task.Yield();
            }
        }
    }

    private sealed class FakeDestination : IPipelineDestination
    {
        public int InitializeCount { get; private set; }
        public int WriteCount { get; private set; }
        public int CompleteCount { get; private set; }

        public Task InitializeAsync(PipelineExecutionContext context, CancellationToken ct = default)
        {
            InitializeCount++;
            return Task.CompletedTask;
        }

        public Task WriteBatchAsync(PipelineRowBatch batch, PipelineExecutionContext context, CancellationToken ct = default)
        {
            WriteCount++;
            return Task.CompletedTask;
        }

        public Task CompleteAsync(PipelineExecutionContext context, CancellationToken ct = default)
        {
            CompleteCount++;
            return Task.CompletedTask;
        }
    }

    private sealed class RecordingTransform : IPipelineTransform
    {
        public string Name => "recording";
        public int CallCount { get; private set; }

        public ValueTask<PipelineRowBatch> TransformAsync(PipelineRowBatch batch, PipelineExecutionContext context, CancellationToken ct = default)
        {
            CallCount++;
            return ValueTask.FromResult(batch);
        }
    }

    private sealed class ThrowingTransform : IPipelineTransform
    {
        private readonly string _message;

        public ThrowingTransform(string name, string message)
        {
            Name = name;
            _message = message;
        }

        public string Name { get; }

        public ValueTask<PipelineRowBatch> TransformAsync(PipelineRowBatch batch, PipelineExecutionContext context, CancellationToken ct = default)
            => ValueTask.FromException<PipelineRowBatch>(new InvalidOperationException(_message));
    }

    private sealed class RecordingCheckpointStore : IPipelineCheckpointStore
    {
        public string? LoadedRunId { get; private set; }
        public PipelineCheckpointState? Existing { get; set; }
        public List<PipelineCheckpointState> Saved { get; } = [];

        public Task<PipelineCheckpointState?> LoadAsync(string runId, CancellationToken ct = default)
        {
            LoadedRunId = runId;
            return Task.FromResult(Existing);
        }

        public Task SaveAsync(string runId, PipelineCheckpointState checkpoint, CancellationToken ct = default)
        {
            Saved.Add(checkpoint);
            return Task.CompletedTask;
        }
    }

    private sealed class ThrowingDestination : IPipelineDestination
    {
        private readonly string _message;

        public ThrowingDestination(string message)
        {
            _message = message;
        }

        public Task InitializeAsync(PipelineExecutionContext context, CancellationToken ct = default)
            => Task.CompletedTask;

        public Task WriteBatchAsync(PipelineRowBatch batch, PipelineExecutionContext context, CancellationToken ct = default)
            => Task.FromException(new InvalidOperationException(_message));

        public Task CompleteAsync(PipelineExecutionContext context, CancellationToken ct = default)
            => Task.CompletedTask;
    }

    private sealed class RecordingRunLogger : IPipelineRunLogger
    {
        public List<PipelineExecutionContext> StartedContexts { get; } = [];
        public List<PipelineRunResult> CompletedRuns { get; } = [];
        public List<IReadOnlyList<PipelineRejectRecord>> CapturedRejects { get; } = [];

        public Task RunStartedAsync(PipelineExecutionContext context, CancellationToken ct = default)
        {
            StartedContexts.Add(context);
            return Task.CompletedTask;
        }

        public Task StatusChangedAsync(string runId, PipelineRunStatus status, PipelineRunMetrics metrics, CancellationToken ct = default)
            => Task.CompletedTask;

        public Task RejectsCapturedAsync(string runId, IReadOnlyList<PipelineRejectRecord> rejects, CancellationToken ct = default)
        {
            CapturedRejects.Add(rejects);
            return Task.CompletedTask;
        }

        public Task RunCompletedAsync(PipelineRunResult result, CancellationToken ct = default)
        {
            CompletedRuns.Add(result);
            return Task.CompletedTask;
        }
    }

    private sealed class RejectingDestination : IPipelineDestination
    {
        private readonly Func<IReadOnlyDictionary<string, object?>, bool> _shouldReject;
        private readonly string _message;

        public RejectingDestination(Func<IReadOnlyDictionary<string, object?>, bool> shouldReject, string message)
        {
            _shouldReject = shouldReject;
            _message = message;
        }

        public Task InitializeAsync(PipelineExecutionContext context, CancellationToken ct = default)
            => Task.CompletedTask;

        public Task WriteBatchAsync(PipelineRowBatch batch, PipelineExecutionContext context, CancellationToken ct = default)
        {
            if (batch.Rows.Any(_shouldReject))
                return Task.FromException(new InvalidOperationException(_message));

            return Task.CompletedTask;
        }

        public Task CompleteAsync(PipelineExecutionContext context, CancellationToken ct = default)
            => Task.CompletedTask;
    }
}
