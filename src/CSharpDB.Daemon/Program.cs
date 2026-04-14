using CSharpDB.Client;
using CSharpDB.Daemon.Configuration;
using CSharpDB.Daemon.Grpc;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton(sp =>
    DaemonClientOptionsBuilder.BindHostDatabaseOptions(sp.GetRequiredService<IConfiguration>()));

builder.Services.AddSingleton(sp =>
    DaemonClientOptionsBuilder.Build(
        sp.GetRequiredService<IConfiguration>(),
        sp.GetRequiredService<DaemonHostDatabaseOptions>()));

builder.Services.AddCSharpDbClient(sp => sp.GetRequiredService<CSharpDbClientOptions>());

builder.Services.AddGrpc();

var app = builder.Build();

await using (var scope = app.Services.CreateAsyncScope())
{
    var dbClient = scope.ServiceProvider.GetRequiredService<ICSharpDbClient>();
    _ = await dbClient.GetInfoAsync();
}

app.MapGrpcService<CSharpDbRpcService>();

app.Run();

public partial class Program;
