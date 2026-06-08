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

    public static IServiceCollection AddCSharpDbClientFromMasterCatalog(
        this IServiceCollection services,
        CSharpDbClientOptions options)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);

        services.TryAddSingleton<ICSharpDbRouteContextAccessor, CSharpDbRouteContextAccessor>();
        services.AddSingleton(options);
        services.AddSingleton<ICSharpDbClient>(sp =>
            CSharpDbShardedClient.TryCreateFromMasterCatalog(
                options,
                sp.GetService<ICSharpDbRouteContextAccessor>())
            ?? CSharpDbClient.Create(options));
        return services;
    }

    public static IServiceCollection AddCSharpDbClientFromMasterCatalog(
        this IServiceCollection services,
        Func<IServiceProvider, CSharpDbClientOptions> optionsFactory)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(optionsFactory);

        services.TryAddSingleton<ICSharpDbRouteContextAccessor, CSharpDbRouteContextAccessor>();
        services.AddSingleton<ICSharpDbClient>(sp =>
        {
            CSharpDbClientOptions options = optionsFactory(sp);
            return CSharpDbShardedClient.TryCreateFromMasterCatalog(
                       options,
                       sp.GetService<ICSharpDbRouteContextAccessor>())
                   ?? CSharpDbClient.Create(options);
        });
        return services;
    }

    public static IServiceCollection AddCSharpDbShardAdminClient(
        this IServiceCollection services,
        CSharpDbClientOptions options)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);

        services.AddSingleton(options);
        services.AddSingleton<ICSharpDbShardAdminClient>(_ => CSharpDbClient.CreateShardAdmin(options));
        return services;
    }

    public static IServiceCollection AddCSharpDbShardAdminClient(
        this IServiceCollection services,
        Func<IServiceProvider, CSharpDbClientOptions> optionsFactory)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(optionsFactory);

        services.AddSingleton<ICSharpDbShardAdminClient>(sp => CSharpDbClient.CreateShardAdmin(optionsFactory(sp)));
        return services;
    }

    public static IServiceCollection AddCSharpDbShardDirectoryClient(
        this IServiceCollection services,
        CSharpDbClientOptions options)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);

        services.AddSingleton(options);
        services.AddSingleton<ICSharpDbShardDirectoryClient>(_ => CSharpDbClient.CreateShardDirectoryClient(options));
        return services;
    }

    public static IServiceCollection AddCSharpDbShardDirectoryClient(
        this IServiceCollection services,
        Func<IServiceProvider, CSharpDbClientOptions> optionsFactory)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(optionsFactory);

        services.AddSingleton<ICSharpDbShardDirectoryClient>(sp => CSharpDbClient.CreateShardDirectoryClient(optionsFactory(sp)));
        return services;
    }
}
