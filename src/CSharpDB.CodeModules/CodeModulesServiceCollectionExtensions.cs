using CSharpDB.CodeModules.Trust;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace CSharpDB.CodeModules;

public static class CodeModulesServiceCollectionExtensions
{
    public static IServiceCollection AddCSharpDbCodeModules(
        this IServiceCollection services,
        Action<CodeModuleRuntimeOptions>? configureRuntime = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        var options = new CodeModuleRuntimeOptions();
        configureRuntime?.Invoke(options);

        services.TryAddSingleton(options);
        services.TryAddSingleton<ICodeModuleTrustStore, FileCodeModuleTrustStore>();
        services.TryAddScoped<CSharpDbCodeModuleClient>();
        services.TryAddScoped<ICodeModuleFormEventDispatcher, CodeModuleFormEventDispatcher>();
        return services;
    }
}
