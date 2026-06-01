using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

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

    public static IServiceCollection AddCSharpDbShardedClient(
        this IServiceCollection services,
        CSharpDbShardingOptions options)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);

        services.TryAddSingleton<ICSharpDbRouteContextAccessor, CSharpDbRouteContextAccessor>();
        services.AddSingleton(options);
        services.AddSingleton(sp => CSharpDbShardedClient.Create(
            options,
            sp.GetService<ICSharpDbRouteContextAccessor>()));
        services.AddSingleton<ICSharpDbClient>(sp => sp.GetRequiredService<CSharpDbShardedClient>());
        return services;
    }

    public static IServiceCollection AddCSharpDbShardedClient(
        this IServiceCollection services,
        Func<IServiceProvider, CSharpDbShardingOptions> optionsFactory)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(optionsFactory);

        services.TryAddSingleton<ICSharpDbRouteContextAccessor, CSharpDbRouteContextAccessor>();
        services.AddSingleton(sp => CSharpDbShardedClient.Create(
            optionsFactory(sp),
            sp.GetService<ICSharpDbRouteContextAccessor>()));
        services.AddSingleton<ICSharpDbClient>(sp => sp.GetRequiredService<CSharpDbShardedClient>());
        return services;
    }
}
