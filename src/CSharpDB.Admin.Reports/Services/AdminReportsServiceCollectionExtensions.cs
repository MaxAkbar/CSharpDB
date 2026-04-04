using CSharpDB.Admin.Reports.Contracts;
using Microsoft.Extensions.DependencyInjection;

namespace CSharpDB.Admin.Reports.Services;

public static class AdminReportsServiceCollectionExtensions
{
    public static IServiceCollection AddCSharpDbAdminReports(this IServiceCollection services)
    {
        services.AddScoped<IReportRepository, DbReportRepository>();
        services.AddScoped<IReportSourceProvider, DbReportSourceProvider>();
        services.AddScoped<IReportGenerator, DefaultReportGenerator>();
        services.AddScoped<IReportPreviewService, DefaultReportPreviewService>();
        return services;
    }
}
