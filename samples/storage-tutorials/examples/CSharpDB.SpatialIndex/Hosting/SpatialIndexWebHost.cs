using CSharpDB.SpatialIndex;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

internal static class SpatialIndexWebHost
{
    public static async Task RunAsync(string[] args, string databasePath)
    {
        var builder = WebApplication.CreateBuilder(args);
        builder.Services.AddSingleton(new SpatialIndexApiService(databasePath));

        var app = builder.Build();
        app.UseDefaultFiles();
        app.UseStaticFiles();

        // ── Add a point ──────────────────────────────────────
        app.MapPost("/api/spatial/points", async Task<IResult> (AddPointRequest request, SpatialIndexApiService service, CancellationToken ct) =>
        {
            try
            {
                var point = await service.AddAsync(
                    request.Latitude, request.Longitude, request.Name,
                    request.Category, request.Description, request.Tags, ct);
                return Results.Ok(point);
            }
            catch (Exception ex)
            {
                return Results.BadRequest(ex.Message);
            }
        });

        // ── Nearby query ─────────────────────────────────────
        app.MapGet("/api/spatial/nearby", async Task<IResult> (
            double lat, double lon, double radiusKm, string? category, int? maxResults,
            SpatialIndexApiService service, CancellationToken ct) =>
        {
            try
            {
                var result = await service.QueryNearbyAsync(lat, lon, radiusKm, category, maxResults ?? 100, ct);
                return Results.Ok(result);
            }
            catch (Exception ex)
            {
                return Results.BadRequest(ex.Message);
            }
        });

        // ── Bounding box query ───────────────────────────────
        app.MapGet("/api/spatial/bbox", async Task<IResult> (
            double minLat, double minLon, double maxLat, double maxLon,
            string? category, int? maxResults,
            SpatialIndexApiService service, CancellationToken ct) =>
        {
            try
            {
                var result = await service.QueryBoundingBoxAsync(
                    minLat, minLon, maxLat, maxLon, category, maxResults ?? 10_000, ct);
                return Results.Ok(result);
            }
            catch (Exception ex)
            {
                return Results.BadRequest(ex.Message);
            }
        });

        // ── Get single point ─────────────────────────────────
        app.MapGet("/api/spatial/points/{hilbertKey}", async Task<IResult> (long hilbertKey, SpatialIndexApiService service, CancellationToken ct) =>
        {
            try
            {
                var point = await service.GetAsync(hilbertKey, ct);
                return point is null ? Results.NotFound() : Results.Ok(point);
            }
            catch (Exception ex)
            {
                return Results.BadRequest(ex.Message);
            }
        });

        // ── Delete a point ───────────────────────────────────
        app.MapDelete("/api/spatial/points/{hilbertKey}", async Task<IResult> (long hilbertKey, SpatialIndexApiService service, CancellationToken ct) =>
        {
            try
            {
                await service.DeleteAsync(hilbertKey, ct);
                return Results.Ok();
            }
            catch (Exception ex)
            {
                return Results.BadRequest(ex.Message);
            }
        });

        // ── Count ────────────────────────────────────────────
        app.MapGet("/api/spatial/count", async (SpatialIndexApiService service, CancellationToken ct) =>
            Results.Ok(await service.CountAsync(ct)));

        // ── Reset ────────────────────────────────────────────
        app.MapPost("/api/spatial/reset", async (SpatialIndexApiService service, CancellationToken ct) =>
        {
            await service.ResetAsync(ct);
            return Results.Ok();
        });

        await app.RunAsync();
    }
}
