using CSharpDB.Api;
using CSharpDB.Client;

var builder = WebApplication.CreateBuilder(args);

// ─── Services ───────────────────────────────────────────────

builder.Services.AddSingleton<ICSharpDbRouteContextAccessor, CSharpDbRouteContextAccessor>();
builder.Services.AddSingleton(sp =>
{
    return new CSharpDbClientOptions
    {
        ConnectionString = sp.GetRequiredService<IConfiguration>().GetConnectionString("CSharpDB")
            ?? "Data Source=csharpdb.db",
    };
});
builder.Services.AddSingleton<ICSharpDbClient>(sp =>
{
    CSharpDbClientOptions options = sp.GetRequiredService<CSharpDbClientOptions>();
    return CSharpDbShardedClient.TryCreateFromMasterCatalog(
               options,
               sp.GetRequiredService<ICSharpDbRouteContextAccessor>())
           ?? CSharpDbClient.Create(options);
});

builder.Services.AddCSharpDbRestApi(builder.Configuration.GetSection("CSharpDB:Api:Security"));

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
