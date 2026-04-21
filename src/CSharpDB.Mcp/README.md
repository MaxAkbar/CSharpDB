# CSharpDB.Mcp

Model Context Protocol server for CSharpDB.

`CSharpDB.Mcp` exposes database schema, data, mutation, and SQL operations as
MCP tools over stdio. It is intended for local agent/tooling integrations that
need controlled access to a CSharpDB database through `CSharpDB.Client`.

## Running Locally

Default local database:

```powershell
dotnet run --project src/CSharpDB.Mcp/CSharpDB.Mcp.csproj
```

Explicit database path:

```powershell
dotnet run --project src/CSharpDB.Mcp/CSharpDB.Mcp.csproj -- --database C:\data\app.db
```

Remote endpoint:

```powershell
dotnet run --project src/CSharpDB.Mcp/CSharpDB.Mcp.csproj -- --transport grpc --endpoint http://localhost:5820
```

## Configuration

Target selection priority:

- `--endpoint` / `-e`
- `CSHARPDB_ENDPOINT`
- `--database` / `-d`
- `CSHARPDB_DATABASE`
- `ConnectionStrings:CSharpDB` from `appsettings.json`
- `Data Source=csharpdb.db`

Transport selection priority:

- `--transport` / `-t`
- `CSHARPDB_TRANSPORT`
- inferred/default client behavior

Supported transport values are `direct`, `http`, `grpc`, and the parsed but
not implemented `namedpipes` aliases.

Default `appsettings.json`:

```json
{
  "ConnectionStrings": {
    "CSharpDB": "Data Source=csharpdb.db"
  }
}
```

The host initializes the database before serving MCP requests by calling
`ICSharpDbClient.GetInfoAsync()`.

## Tools

Schema tools:

- `GetDatabaseInfo`
- `ListTables`
- `DescribeTable`
- `ListIndexes`
- `ListViews`
- `ListTriggers`

Data tools:

- `BrowseTable`
- `BrowseView`
- `GetRowByPk`
- `GetRowCount`

Mutation tools:

- `InsertRow`
- `UpdateRow`
- `DeleteRow`

SQL tools:

- `ExecuteSql`
- `GetSqlReference`

## Project Layout

- `Program.cs` - host setup, configuration parsing, and MCP server registration
- `Tools/SchemaTools.cs` - database and schema metadata tools
- `Tools/DataTools.cs` - table, view, and row browsing tools
- `Tools/MutationTools.cs` - row insert, update, and delete tools
- `Tools/SqlTools.cs` - SQL execution and SQL reference tools
- `Helpers/JsonHelper.cs` - JSON serialization and value coercion
- `Helpers/SqlReference.cs` - SQL reference text returned by the MCP tool

## Build

```powershell
dotnet build src/CSharpDB.Mcp/CSharpDB.Mcp.csproj
```

## Dependencies

- `CSharpDB.Client`
- `ModelContextProtocol`
- `Microsoft.Extensions.Hosting`
