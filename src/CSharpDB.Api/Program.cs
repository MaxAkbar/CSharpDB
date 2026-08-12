using CSharpDB.Api;
using CSharpDB.Client;
using CSharpDB.Engine;
using CSharpDB.Observability;
using ClientTransport = CSharpDB.Client.CSharpDbTransport;
using ObservabilityTransport = CSharpDB.Observability.CSharpDbTransport;

var builder = WebApplication.CreateBuilder(args);

// ─── Services ───────────────────────────────────────────────

builder.Services.AddSingleton<ICSharpDbRouteContextAccessor, CSharpDbRouteContextAccessor>();
builder.Services.AddCSharpDbObservability(
    builder.Configuration,
    defaultServiceName: "CSharpDB.Api",
    defaultDeploymentEnvironment: builder.Environment.EnvironmentName);
builder.Services.AddSingleton(sp =>
{
    CSharpDbObservabilityOptions observabilityOptions =
        sp.GetRequiredService<CSharpDbObservabilityOptions>();
    return new CSharpDbClientOptions
    {
        Transport = ClientTransport.Direct,
        ConnectionString = sp.GetRequiredService<IConfiguration>().GetConnectionString("CSharpDB")
            ?? "Data Source=csharpdb.db",
        DirectDatabaseOptions = new DatabaseOptions
        {
            ObservabilityOptions = observabilityOptions,
        },
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
app.UseCSharpDbObservability(ObservabilityTransport.Direct);

// ─── Middleware pipeline and endpoints ──────────────────────

app.MapCSharpDbRestApi();
app.MapCSharpDbPrometheusEndpoint();

// ─── Initialize database ────────────────────────────────────

await using (var scope = app.Services.CreateAsyncScope())
{
    var dbClient = scope.ServiceProvider.GetRequiredService<ICSharpDbClient>();
    _ = await dbClient.GetInfoAsync();
}

app.Run();

public partial class Program;
