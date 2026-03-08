using Microsoft.Extensions.DependencyInjection;

namespace CSharpDB.Client;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddCSharpDbClient(
        this IServiceCollection services,
        CSharpDbClientOptions options)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);

        services.AddSingleton(options);
        services.AddSingleton<ICSharpDbClient>(_ => CSharpDbClient.Create(options));
        return services;
    }

    public static IServiceCollection AddCSharpDbClient(
        this IServiceCollection services,
        Func<IServiceProvider, CSharpDbClientOptions> optionsFactory)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(optionsFactory);

        services.AddSingleton<ICSharpDbClient>(sp => CSharpDbClient.Create(optionsFactory(sp)));
        return services;
    }
}
