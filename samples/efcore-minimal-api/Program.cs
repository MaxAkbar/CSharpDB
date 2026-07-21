using CSharpDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

namespace EfCoreMinimalApiSample;

public sealed class Program
{
    public static async Task Main(string[] args)
    {
        WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

        builder.Services.AddDbContext<TodoDbContext>((services, options) =>
        {
            IConfiguration configuration = services.GetRequiredService<IConfiguration>();
            options.UseCSharpDb(GetConnectionString(configuration));
        });

        WebApplication app = builder.Build();

        CreateDatabaseDirectory(app.Configuration);

        await using (AsyncServiceScope scope = app.Services.CreateAsyncScope())
        {
            TodoDbContext db = scope.ServiceProvider.GetRequiredService<TodoDbContext>();
            await db.Database.EnsureCreatedAsync();
        }

        app.MapGet("/", () => Results.Ok(new
        {
            message = "CSharpDB EF Core minimal API sample",
            endpoints = new[]
            {
                "GET    /todos",
                "GET    /todos/{id}",
                "POST   /todos",
                "PUT    /todos/{id}",
                "DELETE /todos/{id}",
            },
        }));

        app.MapGet("/todos", async (TodoDbContext db, CancellationToken ct) =>
            await db.Todos
                .AsNoTracking()
                .OrderBy(todo => todo.Id)
                .ToListAsync(ct));

        app.MapGet("/todos/{id:int}", async Task<IResult> (
            int id,
            TodoDbContext db,
            CancellationToken ct) =>
        {
            TodoItem? todo = await db.Todos
                .AsNoTracking()
                .SingleOrDefaultAsync(item => item.Id == id, ct);

            return todo is null
                ? Results.NotFound()
                : Results.Ok(todo);
        });

        app.MapPost("/todos", async Task<IResult> (
            CreateTodoRequest request,
            TodoDbContext db,
            CancellationToken ct) =>
        {
            if (string.IsNullOrWhiteSpace(request.Title))
            {
                return Results.ValidationProblem(new Dictionary<string, string[]>
                {
                    [nameof(request.Title)] = ["A title is required."],
                });
            }

            var todo = new TodoItem
            {
                Title = request.Title.Trim(),
            };

            db.Todos.Add(todo);
            await db.SaveChangesAsync(ct);

            return Results.Created($"/todos/{todo.Id}", todo);
        });

        app.MapPut("/todos/{id:int}", async Task<IResult> (
            int id,
            UpdateTodoRequest request,
            TodoDbContext db,
            CancellationToken ct) =>
        {
            if (string.IsNullOrWhiteSpace(request.Title))
            {
                return Results.ValidationProblem(new Dictionary<string, string[]>
                {
                    [nameof(request.Title)] = ["A title is required."],
                });
            }

            TodoItem? todo = await db.Todos
                .SingleOrDefaultAsync(item => item.Id == id, ct);

            if (todo is null)
                return Results.NotFound();

            todo.Title = request.Title.Trim();
            todo.IsComplete = request.IsComplete;
            await db.SaveChangesAsync(ct);

            return Results.Ok(todo);
        });

        app.MapDelete("/todos/{id:int}", async Task<IResult> (
            int id,
            TodoDbContext db,
            CancellationToken ct) =>
        {
            TodoItem? todo = await db.Todos
                .SingleOrDefaultAsync(item => item.Id == id, ct);

            if (todo is null)
                return Results.NotFound();

            db.Todos.Remove(todo);
            await db.SaveChangesAsync(ct);
            return Results.NoContent();
        });

        await app.RunAsync();
    }

    private static string GetConnectionString(IConfiguration configuration)
    {
        string? databasePath = configuration["database-path"];
        if (!string.IsNullOrWhiteSpace(databasePath))
            return $"Data Source={Path.GetFullPath(databasePath)}";

        return configuration.GetConnectionString("CSharpDB")
            ?? throw new InvalidOperationException(
                "Configure ConnectionStrings:CSharpDB or pass --database-path <path>.");
    }

    private static void CreateDatabaseDirectory(IConfiguration configuration)
    {
        string? databasePath = configuration["database-path"];
        if (string.IsNullOrWhiteSpace(databasePath))
            return;

        string? directory = Path.GetDirectoryName(Path.GetFullPath(databasePath));
        if (!string.IsNullOrWhiteSpace(directory))
            Directory.CreateDirectory(directory);
    }
}

public sealed class TodoDbContext(DbContextOptions<TodoDbContext> options)
    : DbContext(options)
{
    public DbSet<TodoItem> Todos => Set<TodoItem>();
}

public sealed class TodoItem
{
    public int Id { get; set; }

    public string Title { get; set; } = string.Empty;

    public bool IsComplete { get; set; }
}

public sealed record CreateTodoRequest(string Title);

public sealed record UpdateTodoRequest(string Title, bool IsComplete);
