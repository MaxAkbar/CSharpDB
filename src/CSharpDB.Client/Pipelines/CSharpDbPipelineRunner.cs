using CSharpDB.Pipelines.Models;
using CSharpDB.Pipelines.Runtime;
using CSharpDB.Pipelines.Serialization;
using CSharpDB.Primitives;

namespace CSharpDB.Client.Pipelines;

public sealed class CSharpDbPipelineRunner
{
    private readonly IPipelineOrchestrator _orchestrator;

    public CSharpDbPipelineRunner(
        ICSharpDbClient client,
        DbFunctionRegistry? functions = null,
        DbCommandRegistry? commands = null)
        : this(new PipelineOrchestrator(
            new CSharpDbPipelineComponentFactory(client, functions),
            new CSharpDbPipelineCheckpointStore(client),
            new CSharpDbPipelineRunLogger(client),
            commands))
    {
    }

    public CSharpDbPipelineRunner(IPipelineOrchestrator orchestrator)
    {
        _orchestrator = orchestrator;
    }

    public Task<PipelineRunResult> RunAsync(PipelineRunRequest request, CancellationToken ct = default)
        => _orchestrator.ExecuteAsync(request, ct);

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
