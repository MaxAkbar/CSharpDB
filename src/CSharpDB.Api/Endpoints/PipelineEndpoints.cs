using CSharpDB.Api.Dtos;
using CSharpDB.Client;
using CSharpDB.Client.Pipelines;
using CSharpDB.Pipelines.Models;

namespace CSharpDB.Api.Endpoints;

public static class PipelineEndpoints
{
    public static RouteGroupBuilder MapPipelineEndpoints(this RouteGroupBuilder group)
    {
        var pipelines = group.MapGroup("/etl");
        pipelines.MapGet("/pipelines", ListPipelines);
        pipelines.MapGet("/pipelines/{name}", GetPipeline);
        pipelines.MapGet("/pipelines/{name}/revisions", ListPipelineRevisions);
        pipelines.MapGet("/pipelines/{name}/revisions/{revision:int}", GetPipelineRevision);
        pipelines.MapPut("/pipelines/{name}", SavePipeline);
        pipelines.MapDelete("/pipelines/{name}", DeletePipeline);
        pipelines.MapPost("/pipelines/{name}/run", RunStoredPipeline);
        pipelines.MapPost("/validate", Validate);
        pipelines.MapPost("/run", Run);
        pipelines.MapGet("/runs", ListRuns);
        pipelines.MapGet("/runs/{runId}", GetRun);
        pipelines.MapGet("/runs/{runId}/package", GetRunPackage);
        pipelines.MapGet("/runs/{runId}/rejects", GetRejects);
        pipelines.MapPost("/runs/{runId}/resume", Resume);
        return group;
    }

    private static async Task<IResult> Validate(ICSharpDbClient db, ExecutePipelineRequest request)
    {
        var runner = new CSharpDbPipelineRunner(db);
        var result = await runner.RunPackageAsync(request.Package, PipelineExecutionMode.Validate);
        return Results.Ok(result);
    }

    private static async Task<IResult> Run(ICSharpDbClient db, ExecutePipelineRequest request)
    {
        var runner = new CSharpDbPipelineRunner(db);
        PipelineExecutionMode mode = ParseMode(request.Mode);
        var result = await runner.RunPackageAsync(request.Package, mode);
        return Results.Ok(result);
    }

    private static async Task<IResult> ListPipelines(ICSharpDbClient db, int limit = 100)
    {
        var catalog = new CSharpDbPipelineCatalogClient(db);
        return Results.Ok(await catalog.ListPipelinesAsync(limit));
    }

    private static async Task<IResult> GetPipeline(ICSharpDbClient db, string name)
    {
        var catalog = new CSharpDbPipelineCatalogClient(db);
        var pipeline = await catalog.GetPipelineAsync(name);
        return pipeline is null
            ? Results.NotFound(new { error = $"Pipeline '{name}' not found." })
            : Results.Ok(pipeline);
    }

    private static async Task<IResult> SavePipeline(ICSharpDbClient db, string name, SavePipelineRequest request)
    {
        var catalog = new CSharpDbPipelineCatalogClient(db);
        return Results.Ok(await catalog.SavePipelineAsync(request.Package, string.IsNullOrWhiteSpace(request.Name) ? name : request.Name));
    }

    private static async Task<IResult> ListPipelineRevisions(ICSharpDbClient db, string name, int limit = 25)
    {
        var catalog = new CSharpDbPipelineCatalogClient(db);
        return Results.Ok(await catalog.ListPipelineRevisionsAsync(name, limit));
    }

    private static async Task<IResult> GetPipelineRevision(ICSharpDbClient db, string name, int revision)
    {
        var catalog = new CSharpDbPipelineCatalogClient(db);
        var pipeline = await catalog.GetPipelineRevisionAsync(name, revision);
        return pipeline is null
            ? Results.NotFound(new { error = $"Pipeline '{name}' revision {revision} not found." })
            : Results.Ok(pipeline);
    }

    private static async Task<IResult> DeletePipeline(ICSharpDbClient db, string name)
    {
        var catalog = new CSharpDbPipelineCatalogClient(db);
        await catalog.DeletePipelineAsync(name);
        return Results.NoContent();
    }

    private static async Task<IResult> RunStoredPipeline(ICSharpDbClient db, string name, string? mode = null)
    {
        var catalog = new CSharpDbPipelineCatalogClient(db);
        return Results.Ok(await catalog.RunStoredPipelineAsync(name, ParseMode(mode)));
    }

    private static async Task<IResult> ListRuns(ICSharpDbClient db, int limit = 50)
    {
        var catalog = new CSharpDbPipelineCatalogClient(db);
        var runs = await catalog.ListRunsAsync(limit);
        return Results.Ok(runs);
    }

    private static async Task<IResult> GetRun(ICSharpDbClient db, string runId)
    {
        var catalog = new CSharpDbPipelineCatalogClient(db);
        var run = await catalog.GetRunAsync(runId);
        return run is null
            ? Results.NotFound(new { error = $"Pipeline run '{runId}' not found." })
            : Results.Ok(run);
    }

    private static async Task<IResult> GetRunPackage(ICSharpDbClient db, string runId)
    {
        var catalog = new CSharpDbPipelineCatalogClient(db);
        var package = await catalog.GetRunPackageAsync(runId);
        return package is null
            ? Results.NotFound(new { error = $"Pipeline run '{runId}' does not have a stored package definition." })
            : Results.Ok(package);
    }

    private static async Task<IResult> GetRejects(ICSharpDbClient db, string runId)
    {
        var catalog = new CSharpDbPipelineCatalogClient(db);
        return Results.Ok(await catalog.GetRejectsAsync(runId));
    }

    private static async Task<IResult> Resume(ICSharpDbClient db, string runId)
    {
        var catalog = new CSharpDbPipelineCatalogClient(db);
        return Results.Ok(await catalog.ResumeAsync(runId));
    }

    private static PipelineExecutionMode ParseMode(string? mode)
    {
        if (string.IsNullOrWhiteSpace(mode))
            return PipelineExecutionMode.Run;

        if (Enum.TryParse<PipelineExecutionMode>(mode, ignoreCase: true, out var parsed))
            return parsed;

        throw new ArgumentException($"Unsupported pipeline execution mode '{mode}'.");
    }
}
