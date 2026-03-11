using CSharpDB.Client;
using CSharpDB.Daemon.Grpc;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddCSharpDbClient(sp => new CSharpDbClientOptions
{
    ConnectionString = sp.GetRequiredService<IConfiguration>().GetConnectionString("CSharpDB")
        ?? "Data Source=csharpdb.db",
});

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
