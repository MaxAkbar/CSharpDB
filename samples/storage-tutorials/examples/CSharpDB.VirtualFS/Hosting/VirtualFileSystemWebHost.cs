using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

internal static class VirtualFileSystemWebHost
{
    public static async Task RunAsync(string[] args, string databasePath)
    {
        var builder = WebApplication.CreateBuilder(args);
        builder.Services.AddSingleton(new VirtualFileSystemApiService(databasePath));

        var app = builder.Build();
        app.UseDefaultFiles();
        app.UseStaticFiles();

        app.MapGet("/api/filesystem/tree", async (string? path, VirtualFileSystemApiService service, CancellationToken ct) =>
            Results.Ok(await service.RenderTreeAsync(NormalizePath(path), ct)));

        app.MapGet("/api/filesystem/entries", async (string? path, VirtualFileSystemApiService service, CancellationToken ct) =>
            Results.Ok(await service.ListDirectoryAsync(NormalizePath(path), ct)));

        app.MapGet("/api/filesystem/entry", async Task<IResult> (string path, VirtualFileSystemApiService service, CancellationToken ct) =>
        {
            try
            {
                return Results.Ok(await service.GetEntryInfoAsync(NormalizePath(path), ct));
            }
            catch (Exception ex)
            {
                return Results.BadRequest(ex.Message);
            }
        });

        app.MapGet("/api/filesystem/files/content", async Task<IResult> (string path, VirtualFileSystemApiService service, CancellationToken ct) =>
        {
            try
            {
                return Results.Ok(await service.ReadFileAsync(NormalizePath(path), ct));
            }
            catch (Exception ex)
            {
                return Results.BadRequest(ex.Message);
            }
        });

        app.MapPost("/api/filesystem/directories", async Task<IResult> (PathRequest request, VirtualFileSystemApiService service, CancellationToken ct) =>
        {
            try
            {
                await service.CreateDirectoryAsync(NormalizePath(request.Path), ct);
                return Results.Ok();
            }
            catch (Exception ex)
            {
                return Results.BadRequest(ex.Message);
            }
        });

        app.MapPost("/api/filesystem/files", async Task<IResult> (VirtualFileWriteRequest request, VirtualFileSystemApiService service, CancellationToken ct) =>
        {
            try
            {
                await service.WriteFileAsync(NormalizePath(request.Path), request.Content, ct);
                return Results.Ok();
            }
            catch (Exception ex)
            {
                return Results.BadRequest(ex.Message);
            }
        });

        app.MapPost("/api/filesystem/shortcuts", async Task<IResult> (VirtualFileShortcutRequest request, VirtualFileSystemApiService service, CancellationToken ct) =>
        {
            try
            {
                await service.CreateShortcutAsync(NormalizePath(request.ShortcutPath), NormalizePath(request.TargetPath), ct);
                return Results.Ok();
            }
            catch (Exception ex)
            {
                return Results.BadRequest(ex.Message);
            }
        });

        app.MapPost("/api/filesystem/reset", async (VirtualFileSystemApiService service, CancellationToken ct) =>
        {
            await service.ResetAsync(ct);
            return Results.Ok();
        });

        app.MapDelete("/api/filesystem/entry", async Task<IResult> (string path, VirtualFileSystemApiService service, CancellationToken ct) =>
        {
            try
            {
                await service.DeleteAsync(NormalizePath(path), ct);
                return Results.Ok();
            }
            catch (Exception ex)
            {
                return Results.BadRequest(ex.Message);
            }
        });

        await app.RunAsync();
    }

    private static string NormalizePath(string? path)
    {
        return string.IsNullOrWhiteSpace(path) ? "/" : path;
    }

    private sealed record PathRequest(string Path);
}
