using CSharpDB.Api;
using CSharpDB.Client;
using CSharpDB.Daemon.Configuration;
using CSharpDB.Daemon.Grpc;

var builder = WebApplication.CreateBuilder(args);
bool enableRestApi = builder.Configuration.GetValue("CSharpDB:Daemon:EnableRestApi", true);

builder.Host.UseWindowsService(options =>
{
    options.ServiceName = "CSharpDB Daemon";
});
builder.Host.UseSystemd();

builder.Services.AddSingleton(sp =>
    DaemonClientOptionsBuilder.BindHostDatabaseOptions(sp.GetRequiredService<IConfiguration>()));

builder.Services.AddSingleton(sp =>
    DaemonClientOptionsBuilder.Build(
        sp.GetRequiredService<IConfiguration>(),
        sp.GetRequiredService<DaemonHostDatabaseOptions>()));

builder.Services.AddCSharpDbClient(sp => sp.GetRequiredService<CSharpDbClientOptions>());

if (enableRestApi)
{
    builder.Services.AddCSharpDbRestApi();
}

builder.Services.AddGrpc();

var app = builder.Build();

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
