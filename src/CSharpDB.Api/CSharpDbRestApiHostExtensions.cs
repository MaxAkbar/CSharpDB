using System.Text.Json;
using System.Text.Json.Serialization;
using CSharpDB.Api.Endpoints;
using CSharpDB.Api.Middleware;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Scalar.AspNetCore;

namespace CSharpDB.Api;

public sealed class CSharpDbRestApiHostOptions
{
    public string RoutePrefix { get; set; } = "/api";

    public string OpenApiTitle { get; set; } = "CSharpDB API";

    public bool MapDevelopmentOpenApi { get; set; } = true;

    public bool ApplyMiddlewareToApiOnly { get; set; }
}

public static class CSharpDbRestApiHostExtensions
{
    public static IServiceCollection AddCSharpDbRestApi(this IServiceCollection services)
    {
        services.AddOpenApi();

        services.AddCors(options =>
        {
            options.AddDefaultPolicy(policy =>
            {
                policy.AllowAnyOrigin()
                      .AllowAnyMethod()
                      .AllowAnyHeader();
            });
        });

        services.ConfigureHttpJsonOptions(options =>
        {
            options.SerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
            options.SerializerOptions.DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull;
            options.SerializerOptions.Converters.Add(new JsonStringEnumConverter(JsonNamingPolicy.CamelCase));
        });

        return services;
    }

    public static WebApplication MapCSharpDbRestApi(this WebApplication app)
        => app.MapCSharpDbRestApi(configure: null);

    public static WebApplication MapCSharpDbRestApi(
        this WebApplication app,
        Action<CSharpDbRestApiHostOptions>? configure)
    {
        var options = new CSharpDbRestApiHostOptions();
        configure?.Invoke(options);

        string routePrefix = NormalizeRoutePrefix(options.RoutePrefix);
        var apiPath = new PathString(routePrefix);

        if (options.ApplyMiddlewareToApiOnly)
        {
            app.UseWhen(
                context => context.Request.Path.StartsWithSegments(apiPath),
                branch =>
                {
                    branch.UseCors();
                    branch.UseMiddleware<ExceptionHandlingMiddleware>();
                });
        }
        else
        {
            app.UseCors();
            app.UseMiddleware<ExceptionHandlingMiddleware>();
        }

        if (options.MapDevelopmentOpenApi && app.Environment.IsDevelopment())
        {
            app.MapOpenApi();
            app.MapScalarApiReference(scalar =>
            {
                scalar.WithTitle(options.OpenApiTitle);
                scalar.WithDefaultHttpClient(ScalarTarget.CSharp, ScalarClient.HttpClient);
            });
        }

        var api = app.MapGroup(routePrefix);

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

        return app;
    }

    private static string NormalizeRoutePrefix(string routePrefix)
    {
        if (string.IsNullOrWhiteSpace(routePrefix))
            return "/api";

        routePrefix = routePrefix.Trim();
        return routePrefix.StartsWith("/", StringComparison.Ordinal)
            ? routePrefix
            : "/" + routePrefix;
    }
}
