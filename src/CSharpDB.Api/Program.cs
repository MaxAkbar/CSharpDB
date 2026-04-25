using CSharpDB.Api;
using CSharpDB.Client;

var builder = WebApplication.CreateBuilder(args);

// ─── Services ───────────────────────────────────────────────

builder.Services.AddCSharpDbClient(sp => new CSharpDbClientOptions
{
    ConnectionString = sp.GetRequiredService<IConfiguration>().GetConnectionString("CSharpDB")
        ?? "Data Source=csharpdb.db",
});

builder.Services.AddCSharpDbRestApi();

var app = builder.Build();

// ─── Initialize database ────────────────────────────────────

await using (var scope = app.Services.CreateAsyncScope())
{
    var dbClient = scope.ServiceProvider.GetRequiredService<ICSharpDbClient>();
    _ = await dbClient.GetInfoAsync();
}

// ─── Middleware pipeline and endpoints ──────────────────────

app.MapCSharpDbRestApi();

app.Run();

public partial class Program;
