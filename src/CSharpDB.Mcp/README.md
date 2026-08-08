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

## Type Metadata and JSON Values

`DescribeTable` and `BrowseTable` return both `type` and `storageType` for each
column. `type` is the canonical declared SQL spelling, such as `BOOLEAN`,
`INTEGER`, `DECIMAL(18,4)`, or `DATETIMEOFFSET(7)`. `storageType` is the compact
physical carrier (`Integer`, `Real`, `Decimal`, `Text`, or `Blob`). Logical
types can share a carrier without sharing their SQL semantics; for example,
`BOOLEAN`, `INTEGER`, and `BIGINT` all use `Integer` storage. Rowversion columns
are reported as `ROWVERSION` and also carry `isRowVersion: true`. See the
[complete SQL data type reference](https://csharpdb.com/docs/sql-reference.html#data-types).

The mutation tools intentionally accept a small JSON value surface. JSON
integers become signed 64-bit values, other JSON numbers become binary64,
Booleans become integer `0`/`1`, strings remain strings, and `null` remains
SQL `NULL`; arrays and objects are passed as their JSON text. The target column
still applies its declared-type validation and coercion. For exact decimal,
ordinary binary, exact-length bit-string, or other type-specific input, use
`ExecuteSql` or a typed `CSharpDB.Client` API rather than relying on generic MCP
JSON coercion.

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
