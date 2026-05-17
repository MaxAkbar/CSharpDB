using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;

namespace CSharpDB.Admin.ImportExport.Services;

public static class ImportExportServiceCollectionExtensions
{
    public static IServiceCollection AddCSharpDbAdminImportExport(this IServiceCollection services)
    {
        services.AddSingleton<ITableArchiveDownloadStore, TableArchiveDownloadStore>();
        services.AddScoped<ITableImportExportService, TableImportExportService>();
        return services;
    }

    public static IEndpointRouteBuilder MapCSharpDbAdminImportExport(this IEndpointRouteBuilder endpoints)
    {
        endpoints.MapGet("/admin/import-export/download/{token}", (
            string token,
            HttpContext httpContext,
            ITableArchiveDownloadStore downloads) =>
        {
            if (!downloads.TryTake(token, out var download) || !File.Exists(download.Path))
                return Results.NotFound();

            httpContext.Response.OnCompleted(() =>
            {
                TableArchiveDownloadStore.TryDelete(download.Path);
                return Task.CompletedTask;
            });

            return Results.File(
                download.Path,
                "application/octet-stream",
                download.FileName,
                enableRangeProcessing: false);
        });

        return endpoints;
    }
}
