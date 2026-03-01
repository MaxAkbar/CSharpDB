using CSharpDB.Api.Dtos;
using CSharpDB.Core;
using CSharpDB.Service;

namespace CSharpDB.Api.Endpoints;

public static class TriggerEndpoints
{
    public static RouteGroupBuilder MapTriggerEndpoints(this RouteGroupBuilder group)
    {
        group.MapGet("/triggers", GetAllTriggers);
        group.MapPost("/triggers", CreateTrigger);
        group.MapPut("/triggers/{name}", UpdateTrigger);
        group.MapDelete("/triggers/{name}", DropTrigger);

        return group;
    }

    private static async Task<IResult> GetAllTriggers(CSharpDbService db)
    {
        var triggers = await db.GetTriggersAsync();
        var response = triggers.Select(t => new TriggerResponse(
            t.TriggerName, t.TableName,
            t.Timing.ToString(), t.Event.ToString(),
            t.BodySql)).ToList();
        return Results.Ok(response);
    }

    private static async Task<IResult> CreateTrigger(CreateTriggerRequest req, CSharpDbService db)
    {
        if (!Enum.TryParse<TriggerTiming>(req.Timing, ignoreCase: true, out var timing))
            return Results.BadRequest(new { error = $"Invalid timing '{req.Timing}'. Valid values: Before, After." });
        if (!Enum.TryParse<TriggerEvent>(req.Event, ignoreCase: true, out var triggerEvent))
            return Results.BadRequest(new { error = $"Invalid event '{req.Event}'. Valid values: Insert, Update, Delete." });

        await db.CreateTriggerAsync(req.TriggerName, req.TableName, timing, triggerEvent, req.BodySql);
        return Results.Created($"/api/triggers/{req.TriggerName}", new TriggerResponse(
            req.TriggerName, req.TableName, timing.ToString(), triggerEvent.ToString(), req.BodySql));
    }

    private static async Task<IResult> UpdateTrigger(string name, UpdateTriggerRequest req, CSharpDbService db)
    {
        if (!Enum.TryParse<TriggerTiming>(req.Timing, ignoreCase: true, out var timing))
            return Results.BadRequest(new { error = $"Invalid timing '{req.Timing}'. Valid values: Before, After." });
        if (!Enum.TryParse<TriggerEvent>(req.Event, ignoreCase: true, out var triggerEvent))
            return Results.BadRequest(new { error = $"Invalid event '{req.Event}'. Valid values: Insert, Update, Delete." });

        await db.UpdateTriggerAsync(name, req.NewTriggerName, req.TableName, timing, triggerEvent, req.BodySql);
        return Results.Ok(new TriggerResponse(
            req.NewTriggerName, req.TableName, timing.ToString(), triggerEvent.ToString(), req.BodySql));
    }

    private static async Task<IResult> DropTrigger(string name, CSharpDbService db)
    {
        await db.DropTriggerAsync(name);
        return Results.NoContent();
    }
}
