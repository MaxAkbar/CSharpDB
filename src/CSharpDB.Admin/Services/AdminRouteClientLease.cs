using CSharpDB.Admin.Models;
using CSharpDB.Client;

namespace CSharpDB.Admin.Services;

public sealed class AdminRouteClientLease(DatabaseClientHolder dbHolder) : IAsyncDisposable
{
    private ICSharpDbClient? _routeClient;
    private string? _loadedRouteKeyspace;
    private string? _loadedRouteKey;

    public ICSharpDbClient? RouteClient => _routeClient;

    public ICSharpDbClient GetActiveClient(ICSharpDbClient fallback)
        => _routeClient ?? fallback;

    public bool IsRouteChanged(TabDescriptor? tab)
        => !string.Equals(_loadedRouteKeyspace, tab?.RouteKeyspace, StringComparison.Ordinal)
           || !string.Equals(_loadedRouteKey, tab?.RouteKey, StringComparison.Ordinal);

    public async Task UpdateAsync(TabDescriptor? tab)
    {
        CSharpDbRouteContext? route = tab?.RouteContext;
        if (route is null)
        {
            await DisposeRouteClientAsync();
            _loadedRouteKeyspace = tab?.RouteKeyspace;
            _loadedRouteKey = tab?.RouteKey;
            return;
        }

        if (_routeClient is not null && !IsRouteChanged(tab))
            return;

        await DisposeRouteClientAsync();
        _routeClient = dbHolder.CreateRouteBoundClient(route);
        _loadedRouteKeyspace = tab?.RouteKeyspace;
        _loadedRouteKey = tab?.RouteKey;
    }

    public async ValueTask DisposeAsync()
    {
        await DisposeRouteClientAsync();
    }

    private async Task DisposeRouteClientAsync()
    {
        if (_routeClient is null)
            return;

        ICSharpDbClient routeClient = _routeClient;
        _routeClient = null;
        await routeClient.DisposeAsync();
    }
}
