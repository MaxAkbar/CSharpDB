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
        services.TryAddFormControlRegistry();
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

    public static IServiceCollection AddCSharpDbAdminFormControls(
        this IServiceCollection services,
        Action<FormControlRegistryBuilder> configureControls)
    {
        ArgumentNullException.ThrowIfNull(configureControls);

        services.AddSingleton<IFormControlRegistryConfiguration>(
            new DelegateFormControlRegistryConfiguration(configureControls));
        services.TryAddFormControlRegistry();
        return services;
    }

    private static IServiceCollection TryAddFormControlRegistry(this IServiceCollection services)
    {
        services.TryAddSingleton<IFormControlRegistry>(sp =>
        {
            var builder = new FormControlRegistryBuilder();
            BuiltInFormControlDescriptors.AddTo(builder);

            foreach (IFormControlRegistryConfiguration configuration in sp.GetServices<IFormControlRegistryConfiguration>())
                configuration.Configure(builder);

            return builder.Build();
        });

        return services;
    }
}
