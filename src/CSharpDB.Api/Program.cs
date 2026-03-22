using System.Text.Json;
using System.Text.Json.Serialization;
using CSharpDB.Client;
using CSharpDB.Api.Endpoints;
using CSharpDB.Api.Middleware;
using Scalar.AspNetCore;

var builder = WebApplication.CreateBuilder(args);

// ─── Services ───────────────────────────────────────────────

builder.Services.AddCSharpDbClient(sp => new CSharpDbClientOptions
{
    ConnectionString = sp.GetRequiredService<IConfiguration>().GetConnectionString("CSharpDB")
        ?? "Data Source=csharpdb.db",
});

builder.Services.AddOpenApi();

builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(policy =>
    {
        policy.AllowAnyOrigin()
              .AllowAnyMethod()
              .AllowAnyHeader();
    });
});

builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
    options.SerializerOptions.DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull;
    options.SerializerOptions.Converters.Add(new JsonStringEnumConverter(JsonNamingPolicy.CamelCase));
});

var app = builder.Build();

// ─── Initialize database ────────────────────────────────────

await using (var scope = app.Services.CreateAsyncScope())
{
    var dbClient = scope.ServiceProvider.GetRequiredService<ICSharpDbClient>();
    _ = await dbClient.GetInfoAsync();
}

// ─── Middleware pipeline ────────────────────────────────────

app.UseCors();
app.UseMiddleware<ExceptionHandlingMiddleware>();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
    app.MapScalarApiReference(options =>
    {
        options.WithTitle("CSharpDB API");
        options.WithDefaultHttpClient(ScalarTarget.CSharp, ScalarClient.HttpClient);
    });
}

// ─── Endpoints ──────────────────────────────────────────────

var api = app.MapGroup("/api");

api.MapTableEndpoints();
api.MapRowEndpoints();
api.MapIndexEndpoints();
api.MapViewEndpoints();
api.MapTriggerEndpoints();
api.MapProcedureEndpoints();
api.MapSavedQueryEndpoints();
api.MapSqlEndpoints();
api.MapPipelineEndpoints();
api.MapTransactionEndpoints();
api.MapCollectionEndpoints();
api.MapSchemaEndpoints();
api.MapInspectEndpoints();
api.MapMaintenanceEndpoints();

app.Run();

public partial class Program;
