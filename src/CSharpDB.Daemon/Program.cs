using CSharpDB.Api;
using CSharpDB.Api.Security;
using CSharpDB.Client;
using CSharpDB.Daemon.Configuration;
using CSharpDB.Daemon.Grpc;
using CSharpDB.Observability;
using ObservabilityTransport = CSharpDB.Observability.CSharpDbTransport;

var builder = WebApplication.CreateBuilder(args);
bool enableRestApi = builder.Configuration.GetValue("CSharpDB:Daemon:EnableRestApi", true);

builder.Host.UseWindowsService(options =>
{
    options.ServiceName = "CSharpDB Daemon";
});
builder.Host.UseSystemd();

builder.Services.AddCSharpDbObservability(builder.Configuration);
builder.Services.AddSingleton(sp =>
    DaemonClientOptionsBuilder.BindHostDatabaseOptions(sp.GetRequiredService<IConfiguration>()));
builder.Services.AddSingleton(sp =>
    DaemonClientOptionsBuilder.Build(
        sp.GetRequiredService<IConfiguration>(),
        sp.GetRequiredService<DaemonHostDatabaseOptions>(),
        sp.GetRequiredService<CSharpDbObservabilityOptions>()));
builder.Services.AddSingleton<ICSharpDbRouteContextAccessor, CSharpDbRouteContextAccessor>();
builder.Services.AddSingleton<ICSharpDbClient>(sp =>
{
    CSharpDbClientOptions options = sp.GetRequiredService<CSharpDbClientOptions>();
    return CSharpDbShardedClient.TryCreateFromMasterCatalog(
               options,
               sp.GetRequiredService<ICSharpDbRouteContextAccessor>())
           ?? CSharpDbClient.Create(options);
});
builder.Services.Configure<CSharpDbApiSecurityOptions>(
    builder.Configuration.GetSection("CSharpDB:Daemon:Security"));

if (enableRestApi)
{
    builder.Services.AddCSharpDbRestApi();
}

builder.Services.AddGrpc(options =>
{
    options.Interceptors.Add<CSharpDbOperationScopeGrpcInterceptor>();
    options.Interceptors.Add<CSharpDbApiKeyGrpcInterceptor>();
    options.Interceptors.Add<CSharpDbRouteContextGrpcInterceptor>();
});

var app = builder.Build();
app.UseCSharpDbObservability(ObservabilityTransport.Direct);

await using (var scope = app.Services.CreateAsyncScope())
{
    var dbClient = scope.ServiceProvider.GetRequiredService<ICSharpDbClient>();
    _ = await dbClient.GetInfoAsync();
}

if (app.Configuration.GetValue("CSharpDB:Daemon:EnableRestApi", true))
{
    app.MapCSharpDbRestApi(options =>
    {
        options.OpenApiTitle = "CSharpDB Daemon API";
        options.ApplyMiddlewareToApiOnly = true;
    });
}

app.UseGrpcWeb();
app.MapGrpcService<CSharpDbRpcService>().EnableGrpcWeb();

app.Run();

public partial class Program;
