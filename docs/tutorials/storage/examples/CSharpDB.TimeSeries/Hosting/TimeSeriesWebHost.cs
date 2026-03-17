using CSharpDB.TimeSeries;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

internal static class TimeSeriesWebHost
{
    public static async Task RunAsync(string[] args, string databasePath)
    {
        var builder = WebApplication.CreateBuilder(args);
        builder.Services.AddSingleton(new TimeSeriesApiService(databasePath));

        var app = builder.Build();
        app.UseDefaultFiles();
        app.UseStaticFiles();

        // ── Record a data point ──────────────────────────────
        app.MapPost("/api/timeseries/points", async Task<IResult> (RecordPointRequest request, TimeSeriesApiService service, CancellationToken ct) =>
        {
            try
            {
                var point = await service.RecordAsync(
                    request.Metric, request.Value, request.Unit,
                    request.Tags, request.TimestampUtc, ct);
                return Results.Ok(point);
            }
            catch (Exception ex)
            {
                return Results.BadRequest(ex.Message);
            }
        });

        // ── Query a time range ───────────────────────────────
        app.MapGet("/api/timeseries/query", async Task<IResult> (
            DateTime from, DateTime to, string? metric, int? maxResults,
            TimeSeriesApiService service, CancellationToken ct) =>
        {
            try
            {
                var result = await service.QueryAsync(from, to, metric, maxResults ?? 10_000, ct);
                return Results.Ok(result);
            }
            catch (Exception ex)
            {
                return Results.BadRequest(ex.Message);
            }
        });

        // ── Get a single point by ticks ──────────────────────
        app.MapGet("/api/timeseries/points/{ticks}", async Task<IResult> (long ticks, TimeSeriesApiService service, CancellationToken ct) =>
        {
            try
            {
                var point = await service.GetPointAsync(ticks, ct);
                return point is null ? Results.NotFound() : Results.Ok(point);
            }
            catch (Exception ex)
            {
                return Results.BadRequest(ex.Message);
            }
        });

        // ── Delete a point by ticks ──────────────────────────
        app.MapDelete("/api/timeseries/points/{ticks}", async Task<IResult> (long ticks, TimeSeriesApiService service, CancellationToken ct) =>
        {
            try
            {
                await service.DeleteAsync(ticks, ct);
                return Results.Ok();
            }
            catch (Exception ex)
            {
                return Results.BadRequest(ex.Message);
            }
        });

        // ── Get latest point ─────────────────────────────────
        app.MapGet("/api/timeseries/latest", async Task<IResult> (TimeSeriesApiService service, CancellationToken ct) =>
        {
            try
            {
                var point = await service.GetLatestAsync(ct);
                return point is null ? Results.NotFound() : Results.Ok(point);
            }
            catch (Exception ex)
            {
                return Results.BadRequest(ex.Message);
            }
        });

        // ── Count points ─────────────────────────────────────
        app.MapGet("/api/timeseries/count", async (TimeSeriesApiService service, CancellationToken ct) =>
            Results.Ok(await service.CountAsync(ct)));

        // ── Reset database ───────────────────────────────────
        app.MapPost("/api/timeseries/reset", async (TimeSeriesApiService service, CancellationToken ct) =>
        {
            await service.ResetAsync(ct);
            return Results.Ok();
        });

        await app.RunAsync();
    }
}
