using System.Text.Json;
using System.Text.Json.Serialization;
using CSharpDB.Api.Endpoints;
using CSharpDB.Api.Middleware;
using CSharpDB.Service;
using Scalar.AspNetCore;

var builder = WebApplication.CreateBuilder(args);

// ─── Services ───────────────────────────────────────────────

builder.Services.AddSingleton<CSharpDbService>();

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

var dbService = app.Services.GetRequiredService<CSharpDbService>();
await dbService.InitializeAsync();

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
api.MapSqlEndpoints();
api.MapSchemaEndpoints();

app.Run();
