using CSharpDB.Client.Internal;
using CSharpDB.Observability;
using CSharpDB.Pipelines.Models;
using CSharpDB.Pipelines.Runtime;
using CSharpDB.Pipelines.Serialization;
using CSharpDB.Primitives;

namespace CSharpDB.Client.Pipelines;

public sealed class CSharpDbPipelineRunner
{
    private readonly IPipelineOrchestrator _orchestrator;
    private readonly ICSharpDbClient? _client;

    public CSharpDbPipelineRunner(
        ICSharpDbClient client,
        DbFunctionRegistry? functions = null,
        DbCommandRegistry? commands = null,
        DbExtensionPolicy? callbackPolicy = null)
        : this(new PipelineOrchestrator(
            new CSharpDbPipelineComponentFactory(client, functions, callbackPolicy),
            new CSharpDbPipelineCheckpointStore(client),
            new CSharpDbPipelineRunLogger(client),
            commands,
            callbackPolicy), client)
    {
    }

    public CSharpDbPipelineRunner(IPipelineOrchestrator orchestrator)
        : this(orchestrator, client: null)
    {
    }

    private CSharpDbPipelineRunner(
        IPipelineOrchestrator orchestrator,
        ICSharpDbClient? client)
    {
        _orchestrator = orchestrator ?? throw new ArgumentNullException(nameof(orchestrator));
        _client = client;
    }

    public async Task<PipelineRunResult> RunAsync(PipelineRunRequest request, CancellationToken ct = default)
    {
        ClientOperationObservation? observation = _client is null
            ? null
            : ClientOperationObservation.StartRequest(
                _client,
                CSharpDbOperationClass.Pipeline);
        using IDisposable? scope = observation?.EnterScope();

        try
        {
            PipelineRunResult result = await _orchestrator.ExecuteAsync(request, ct);
            if (result.Status == PipelineRunStatus.Failed)
            {
                observation?.Fail(
                    SafeErrorKind.DatabaseOperation,
                    result.Metrics.RowsRead,
                    result.Metrics.RowsWritten);
            }
            else
            {
                observation?.Succeed(
                    result.Metrics.RowsRead,
                    result.Metrics.RowsWritten);
            }

            return result;
        }
        catch (Exception exception)
        {
            observation?.Fail(exception);
            throw;
        }
    }

    public Task<PipelineRunResult> RunPackageAsync(PipelinePackageDefinition package, PipelineExecutionMode mode = PipelineExecutionMode.Run, CancellationToken ct = default)
        => RunAsync(new PipelineRunRequest
        {
            Package = package,
            Mode = mode,
        }, ct);

    public async Task<PipelineRunResult> RunPackageFileAsync(string path, PipelineExecutionMode mode = PipelineExecutionMode.Run, CancellationToken ct = default)
    {
        PipelinePackageDefinition package = await PipelinePackageSerializer.LoadFromFileAsync(path, ct);
        return await RunPackageAsync(package, mode, ct);
    }
}
