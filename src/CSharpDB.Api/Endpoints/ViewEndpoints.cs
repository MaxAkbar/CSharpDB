using CSharpDB.Api.Dtos;
using CSharpDB.Api.Helpers;
using CSharpDB.Client;

namespace CSharpDB.Api.Endpoints;

public static class ViewEndpoints
{
    public static RouteGroupBuilder MapViewEndpoints(this RouteGroupBuilder group)
    {
        group.MapGet("/views", GetAllViews);
        group.MapGet("/views/{name}", GetView);
        group.MapGet("/views/{name}/rows", BrowseViewRows);
        group.MapPost("/views", CreateView);
        group.MapPut("/views/{name}", UpdateView);
        group.MapDelete("/views/{name}", DropView);

        return group;
    }

    private static async Task<IResult> GetAllViews(ICSharpDbClient db)
    {
        var views = await db.GetViewsAsync();
        var response = views.Select(v => new ViewResponse(v.Name, v.Sql)).ToList();
        return Results.Ok(response);
    }

    private static async Task<IResult> GetView(string name, ICSharpDbClient db)
    {
        var sql = await db.GetViewSqlAsync(name);
        return sql is null
            ? Results.NotFound(new { error = $"View '{name}' not found." })
            : Results.Ok(new ViewResponse(name, sql));
    }

    private static async Task<IResult> BrowseViewRows(
        string name, ICSharpDbClient db, int page = 1, int pageSize = 50)
    {
        if (page < 1) page = 1;
        if (pageSize < 1) pageSize = 50;
        if (pageSize > 1000) pageSize = 1000;

        var result = await db.BrowseViewAsync(name, page, pageSize);
        var rows = JsonHelper.RowsToNamedDictionaries(result.ColumnNames, result.Rows);

        return Results.Ok(new BrowseResponse(
            result.ColumnNames, rows, result.TotalRows, result.Page, result.PageSize, result.TotalPages));
    }

    private static async Task<IResult> CreateView(CreateViewRequest req, ICSharpDbClient db)
    {
        await db.CreateViewAsync(req.ViewName, req.SelectSql);
        return Results.Created($"/api/views/{req.ViewName}", new ViewResponse(req.ViewName, req.SelectSql));
    }

    private static async Task<IResult> UpdateView(string name, UpdateViewRequest req, ICSharpDbClient db)
    {
        await db.UpdateViewAsync(name, req.NewViewName, req.SelectSql);
        return Results.Ok(new ViewResponse(req.NewViewName, req.SelectSql));
    }

    private static async Task<IResult> DropView(string name, ICSharpDbClient db)
    {
        await db.DropViewAsync(name);
        return Results.NoContent();
    }
}
