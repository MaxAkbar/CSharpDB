using CSharpDB.Admin.Reports.Contracts;
using CSharpDB.Primitives;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace CSharpDB.Admin.Reports.Services;

public static class AdminReportsServiceCollectionExtensions
{
    public static IServiceCollection AddCSharpDbAdminReports(this IServiceCollection services)
    {
        services.TryAddSingleton(DbCommandRegistry.Empty);
        services.AddScoped<IReportRepository, DbReportRepository>();
        services.AddScoped<IReportSourceProvider, DbReportSourceProvider>();
        services.AddScoped<IReportGenerator, DefaultReportGenerator>();
        services.AddScoped<IReportEventDispatcher, DefaultReportEventDispatcher>();
        services.AddScoped<IReportPreviewService, DefaultReportPreviewService>();
        return services;
    }

    public static IServiceCollection AddCSharpDbAdminReports(
        this IServiceCollection services,
        Action<DbCommandRegistryBuilder> configureCommands)
    {
        ArgumentNullException.ThrowIfNull(configureCommands);

        services.AddSingleton(DbCommandRegistry.Create(configureCommands));
        return services.AddCSharpDbAdminReports();
    }
}
