using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Primitives;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace CSharpDB.Admin.Forms.Services;

public static class AdminFormsServiceCollectionExtensions
{
    public static IServiceCollection AddCSharpDbAdminForms(this IServiceCollection services)
    {
        services.TryAddSingleton(DbCommandRegistry.Empty);
        services.TryAddSingleton<IFormActionRuntime>(NullFormActionRuntime.Instance);
        services.AddScoped<IFormRepository, DbFormRepository>();
        services.AddScoped<ISchemaProvider, DbSchemaProvider>();
        services.AddScoped<IFormRecordService, DbFormRecordService>();
        services.AddScoped<IFormEventDispatcher, DefaultFormEventDispatcher>();
        services.AddScoped<IFormGenerator, DefaultFormGenerator>();
        services.AddScoped<IValidationInferenceService, DefaultValidationInferenceService>();
        return services;
    }

    public static IServiceCollection AddCSharpDbAdminForms(
        this IServiceCollection services,
        Action<DbCommandRegistryBuilder> configureCommands)
    {
        ArgumentNullException.ThrowIfNull(configureCommands);

        services.AddSingleton(DbCommandRegistry.Create(configureCommands));
        return services.AddCSharpDbAdminForms();
    }
}
