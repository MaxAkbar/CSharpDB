# EF Core Minimal API Sample

This ASP.NET Core 10 sample shows the CSharpDB EF Core provider in a small,
file-backed HTTP API. It demonstrates:

- registering a scoped `DbContext` with `AddDbContext(...)` and
  `UseCSharpDb(...)`
- creating the database during application startup with
  `EnsureCreatedAsync()`
- cancellation-aware EF Core queries and tracked writes
- generated integer keys
- create, read, update, and delete endpoints
- persistence across application restarts

## Run the sample

From the repository root:

```bash
dotnet run --project samples/efcore-minimal-api/EfCoreMinimalApiSample.csproj
```

The launch profile listens on `http://localhost:5291` and creates `sample.db`
in the current working directory. To use another file:

```bash
dotnet run --project samples/efcore-minimal-api/EfCoreMinimalApiSample.csproj -- --database-path artifacts/efcore-minimal-api.db
```

Open `sample.http` in Visual Studio or VS Code and run its requests in order,
or use any HTTP client:

```bash
curl -X POST http://localhost:5291/todos \
  -H "Content-Type: application/json" \
  -d '{"title":"Ship the next CSharpDB release"}'

curl http://localhost:5291/todos
```

## Endpoints

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/` | List the available endpoints |
| `GET` | `/todos` | Return todos ordered by generated ID |
| `GET` | `/todos/{id}` | Return one todo or `404` |
| `POST` | `/todos` | Create a todo |
| `PUT` | `/todos/{id}` | Replace its title and completion state |
| `DELETE` | `/todos/{id}` | Delete a todo |

The connection string defaults to `Data Source=sample.db` in
`appsettings.json`. Standard ASP.NET Core configuration overrides work, so a
deployment can set `ConnectionStrings__CSharpDB`. The sample-specific
`--database-path` argument takes precedence when supplied.

`EnsureCreatedAsync()` keeps the first-run experience concise. Applications
that evolve an existing production schema should use EF Core migrations; see
the sibling [provider sample](../efcore-provider/README.md) for its
design-time context and migration commands.
