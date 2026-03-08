using CSharpDB.Api.Dtos;
using CSharpDB.Api.Helpers;
using CSharpDB.Client;
using CSharpDB.Client.Models;

namespace CSharpDB.Api.Endpoints;

public static class ProcedureEndpoints
{
    public static RouteGroupBuilder MapProcedureEndpoints(this RouteGroupBuilder group)
    {
        group.MapGet("/procedures", GetProcedures);
        group.MapGet("/procedures/{name}", GetProcedure);
        group.MapPost("/procedures", CreateProcedure);
        group.MapPut("/procedures/{name}", UpdateProcedure);
        group.MapDelete("/procedures/{name}", DeleteProcedure);
        group.MapPost("/procedures/{name}/execute", ExecuteProcedure);

        return group;
    }

    private static async Task<IResult> GetProcedures(ICSharpDbClient db, bool includeDisabled = true)
    {
        var procedures = await db.GetProceduresAsync(includeDisabled);
        var response = procedures.Select(MapSummary).ToList();
        return Results.Ok(response);
    }

    private static async Task<IResult> GetProcedure(string name, ICSharpDbClient db)
    {
        var procedure = await db.GetProcedureAsync(name);
        if (procedure is null)
            return Results.NotFound(new { error = $"Procedure '{name}' not found." });

        return Results.Ok(MapDetail(procedure));
    }

    private static async Task<IResult> CreateProcedure(CreateProcedureRequest req, ICSharpDbClient db)
    {
        var definition = MapDefinition(req.Name, req.BodySql, req.Parameters, req.Description, req.IsEnabled);
        await db.CreateProcedureAsync(definition);

        var created = await db.GetProcedureAsync(req.Name);
        return Results.Created($"/api/procedures/{req.Name}", created is null ? MapDetail(definition) : MapDetail(created));
    }

    private static async Task<IResult> UpdateProcedure(string name, UpdateProcedureRequest req, ICSharpDbClient db)
    {
        var definition = MapDefinition(req.NewName, req.BodySql, req.Parameters, req.Description, req.IsEnabled);
        await db.UpdateProcedureAsync(name, definition);

        var updated = await db.GetProcedureAsync(req.NewName);
        return Results.Ok(updated is null ? MapDetail(definition) : MapDetail(updated));
    }

    private static async Task<IResult> DeleteProcedure(string name, ICSharpDbClient db)
    {
        await db.DeleteProcedureAsync(name);
        return Results.NoContent();
    }

    private static async Task<IResult> ExecuteProcedure(string name, ExecuteProcedureRequest req, ICSharpDbClient db)
    {
        var procedure = await db.GetProcedureAsync(name);
        if (procedure is null)
            return Results.NotFound(new { error = $"Procedure '{name}' not found." });

        var args = req.Args is null
            ? new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
            : JsonHelper.CoerceDictionary(req.Args);

        var result = await db.ExecuteProcedureAsync(name, args);
        var response = MapExecution(result);
        return result.Succeeded
            ? Results.Ok(response)
            : Results.BadRequest(response);
    }

    private static ProcedureDefinition MapDefinition(
        string name,
        string bodySql,
        IReadOnlyList<ProcedureParameterRequest>? parameters,
        string? description,
        bool isEnabled)
    {
        var mappedParameters = new List<ProcedureParameterDefinition>();
        if (parameters is not null)
        {
            foreach (var p in parameters)
            {
                if (!Enum.TryParse<DbType>(p.Type, ignoreCase: true, out var type))
                    throw new ArgumentException($"Invalid procedure parameter type '{p.Type}'.");

                mappedParameters.Add(new ProcedureParameterDefinition
                {
                    Name = p.Name,
                    Type = type,
                    Required = p.Required,
                    Default = p.Default,
                    Description = p.Description,
                });
            }
        }

        return new ProcedureDefinition
        {
            Name = name,
            BodySql = bodySql,
            Parameters = mappedParameters,
            Description = description,
            IsEnabled = isEnabled,
            CreatedUtc = DateTime.UtcNow,
            UpdatedUtc = DateTime.UtcNow,
        };
    }

    private static ProcedureSummaryResponse MapSummary(ProcedureDefinition procedure) => new(
        procedure.Name,
        procedure.Description,
        procedure.IsEnabled,
        procedure.CreatedUtc,
        procedure.UpdatedUtc);

    private static ProcedureDetailResponse MapDetail(ProcedureDefinition procedure) => new(
        procedure.Name,
        procedure.BodySql,
        procedure.Parameters.Select(p => new ProcedureParameterResponse(
            p.Name,
            p.Type.ToString().ToUpperInvariant(),
            p.Required,
            p.Default,
            p.Description)).ToList(),
        procedure.Description,
        procedure.IsEnabled,
        procedure.CreatedUtc,
        procedure.UpdatedUtc);

    private static ProcedureExecutionResponse MapExecution(ProcedureExecutionResult result) => new(
        result.ProcedureName,
        result.Succeeded,
        result.Statements.Select(statement => new ProcedureStatementResultResponse(
            statement.StatementIndex,
            statement.StatementText,
            statement.IsQuery,
            statement.ColumnNames,
            statement.IsQuery && statement.ColumnNames is not null && statement.Rows is not null
                ? JsonHelper.RowsToNamedDictionaries(statement.ColumnNames, statement.Rows)
                : null,
            statement.RowsAffected,
            statement.Elapsed.TotalMilliseconds)).ToList(),
        result.Error,
        result.FailedStatementIndex,
        result.Elapsed.TotalMilliseconds);
}
