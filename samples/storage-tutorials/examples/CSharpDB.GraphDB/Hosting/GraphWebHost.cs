using CSharpDB.GraphDB;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

internal static class GraphWebHost
{
    public static async Task RunAsync(string[] args, string databasePath)
    {
        var builder = WebApplication.CreateBuilder(args);
        builder.Services.AddSingleton(new GraphApiService(databasePath));

        var app = builder.Build();
        app.UseDefaultFiles();
        app.UseStaticFiles();

        // ── Nodes ──────────────────────────────────────────────

        app.MapPost("/api/graph/nodes", async Task<IResult> (AddNodeRequest request, GraphApiService service, CancellationToken ct) =>
        {
            try
            {
                var node = await service.AddNodeAsync(request.Label, request.Type, request.Properties, ct);
                return Results.Ok(node);
            }
            catch (Exception ex) { return Results.BadRequest(ex.Message); }
        });

        app.MapGet("/api/graph/nodes", async Task<IResult> (int? maxResults, GraphApiService service, CancellationToken ct) =>
        {
            try
            {
                var nodes = await service.GetAllNodesAsync(maxResults ?? 1000, ct);
                return Results.Ok(nodes);
            }
            catch (Exception ex) { return Results.BadRequest(ex.Message); }
        });

        app.MapGet("/api/graph/nodes/count", async (GraphApiService service, CancellationToken ct) =>
            Results.Ok(await service.CountNodesAsync(ct)));

        app.MapGet("/api/graph/nodes/{nodeId}", async Task<IResult> (long nodeId, GraphApiService service, CancellationToken ct) =>
        {
            try
            {
                var node = await service.GetNodeAsync(nodeId, ct);
                return node is null ? Results.NotFound() : Results.Ok(node);
            }
            catch (Exception ex) { return Results.BadRequest(ex.Message); }
        });

        app.MapDelete("/api/graph/nodes/{nodeId}", async Task<IResult> (long nodeId, GraphApiService service, CancellationToken ct) =>
        {
            try
            {
                await service.DeleteNodeAsync(nodeId, ct);
                return Results.Ok();
            }
            catch (Exception ex) { return Results.BadRequest(ex.Message); }
        });

        // ── Edges ──────────────────────────────────────────────

        app.MapPost("/api/graph/edges", async Task<IResult> (AddEdgeRequest request, GraphApiService service, CancellationToken ct) =>
        {
            try
            {
                var edge = await service.AddEdgeAsync(request.SourceId, request.TargetId, request.Label, request.Weight, request.Properties, ct);
                return Results.Ok(edge);
            }
            catch (Exception ex) { return Results.BadRequest(ex.Message); }
        });

        app.MapGet("/api/graph/edges", async Task<IResult> (long sourceId, long targetId, GraphApiService service, CancellationToken ct) =>
        {
            try
            {
                var edge = await service.GetEdgeAsync(sourceId, targetId, ct);
                return edge is null ? Results.NotFound() : Results.Ok(edge);
            }
            catch (Exception ex) { return Results.BadRequest(ex.Message); }
        });

        app.MapDelete("/api/graph/edges", async Task<IResult> (long sourceId, long targetId, GraphApiService service, CancellationToken ct) =>
        {
            try
            {
                await service.DeleteEdgeAsync(sourceId, targetId, ct);
                return Results.Ok();
            }
            catch (Exception ex) { return Results.BadRequest(ex.Message); }
        });

        app.MapGet("/api/graph/edges/count", async (GraphApiService service, CancellationToken ct) =>
            Results.Ok(await service.CountEdgesAsync(ct)));

        // ── Edge traversal (cursor range scans) ────────────────

        app.MapGet("/api/graph/edges/outgoing/{nodeId}", async Task<IResult> (long nodeId, string? label, GraphApiService service, CancellationToken ct) =>
        {
            try
            {
                var (edges, scanned) = await service.GetOutgoingEdgesAsync(nodeId, label, ct);
                return Results.Ok(new { edges, scanned });
            }
            catch (Exception ex) { return Results.BadRequest(ex.Message); }
        });

        app.MapGet("/api/graph/edges/incoming/{nodeId}", async Task<IResult> (long nodeId, string? label, GraphApiService service, CancellationToken ct) =>
        {
            try
            {
                var (edges, scanned) = await service.GetIncomingEdgesAsync(nodeId, label, ct);
                return Results.Ok(new { edges, scanned });
            }
            catch (Exception ex) { return Results.BadRequest(ex.Message); }
        });

        // ── Graph traversal ────────────────────────────────────

        app.MapGet("/api/graph/traverse/bfs", async Task<IResult> (
            long startNodeId, int? maxDepth, string? edgeLabel, string? direction,
            GraphApiService service, CancellationToken ct) =>
        {
            try
            {
                var result = await service.TraverseBfsAsync(startNodeId, maxDepth ?? 3, edgeLabel, direction ?? "outgoing", ct);
                return Results.Ok(result);
            }
            catch (Exception ex) { return Results.BadRequest(ex.Message); }
        });

        app.MapGet("/api/graph/traverse/shortest-path", async Task<IResult> (
            long sourceId, long targetId, int? maxDepth, string? edgeLabel,
            GraphApiService service, CancellationToken ct) =>
        {
            try
            {
                var result = await service.ShortestPathAsync(sourceId, targetId, maxDepth ?? 10, edgeLabel, ct);
                return Results.Ok(result);
            }
            catch (Exception ex) { return Results.BadRequest(ex.Message); }
        });

        // ── Reset ──────────────────────────────────────────────

        app.MapPost("/api/graph/reset", async (GraphApiService service, CancellationToken ct) =>
        {
            await service.ResetAsync(ct);
            return Results.Ok();
        });

        await app.RunAsync();
    }
}
