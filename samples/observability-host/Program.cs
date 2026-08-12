using CSharpDB.Api;
using CSharpDB.Api.Security;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using CSharpDB.Engine;
using CSharpDB.Observability;
using ClientTransport = CSharpDB.Client.CSharpDbTransport;
using ObservabilityTransport = CSharpDB.Observability.CSharpDbTransport;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

builder.Services.AddCSharpDbObservability(builder.Configuration);
builder.Services.Configure<CSharpDbApiSecurityOptions>(
    builder.Configuration.GetSection("CSharpDB:Api:Security"));
builder.Services.AddSingleton(serviceProvider =>
{
    CSharpDbObservabilityOptions observability = serviceProvider
        .GetRequiredService<CSharpDbObservabilityOptions>();
    string connectionString = serviceProvider
        .GetRequiredService<IConfiguration>()
        .GetConnectionString("CSharpDB")
        ?? "Data Source=:memory:";

    return new CSharpDbClientOptions
    {
        Transport = ClientTransport.Direct,
        ConnectionString = connectionString,
        DirectDatabaseOptions = new DatabaseOptions
        {
            ObservabilityOptions = observability,
        },
    };
});
builder.Services.AddSingleton<ICSharpDbClient>(serviceProvider =>
    CSharpDbClient.Create(
        serviceProvider.GetRequiredService<CSharpDbClientOptions>()));
builder.Services.AddCSharpDbHealth(DiagnosticsSource.Api);

WebApplication app = builder.Build();
app.UseCSharpDbObservability(ObservabilityTransport.Direct);

app.MapGet("/", () => Results.Ok(new
{
    sample = "CSharpDB observability host",
    workload = "GET /work",
    liveness = "/health/live",
    readiness = "/health/ready",
    prometheus = "/metrics",
}));

app.MapGet(
    "/work",
    async Task<IResult> (
        ICSharpDbClient client,
        ILoggerFactory loggerFactory,
        CancellationToken cancellationToken) =>
    {
        ILogger logger = loggerFactory.CreateLogger(
            "ObservabilityHostSample.Workload");
        logger.LogInformation(
            "Starting the safe sample database workload.");

        SqlExecutionResult result = await client.ExecuteSqlAsync(
            "SELECT 42 AS answer",
            cancellationToken);

        logger.LogInformation(
            "Completed the safe sample database workload in {ElapsedMilliseconds} ms.",
            result.Elapsed.TotalMilliseconds);

        return Results.Ok(new
        {
            status = result.ErrorCode is null ? "completed" : "failed",
            rowCount = result.Rows?.Count ?? 0,
            elapsedMilliseconds = result.Elapsed.TotalMilliseconds,
        });
    });

app.MapCSharpDbHealthEndpoints();
app.MapCSharpDbPrometheusEndpoint();

await app.RunAsync();

public partial class Program;
