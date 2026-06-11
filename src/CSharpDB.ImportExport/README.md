# CSharpDB.ImportExport

Native table archive import/export support for CSharpDB.

`CSharpDB.ImportExport` provides the shared table archive format used by the
execution engine, client API, admin import/export tools, and DevOps comparison
features. It is a low-level package for tools that need to read or write
CSharpDB table archives directly.

## Features

- native CSharpDB table archive reader and writer
- schema and manifest metadata models
- row streaming through `IAsyncEnumerable<DbValue[]>`
- primary-key index metadata and indexed integer primary-key lookup
- conversion between archive schema models and `CSharpDB.Primitives.TableSchema`

## Reading Archives

```csharp
using CSharpDB.ImportExport.TableArchives;

var metadata = await TableArchiveReader.ReadMetadataAsync("customers.cdbtable");
Console.WriteLine($"{metadata.Manifest.SourceTableName}: {metadata.Manifest.RowCount} rows");

await foreach (var row in TableArchiveReader.ReadRowsAsync("customers.cdbtable"))
{
    Console.WriteLine(row[0]);
}
```

## Writing Archives

```csharp
using CSharpDB.ImportExport.TableArchives;
using CSharpDB.Primitives;

TableSchema schema = new()
{
    TableName = "customers",
    Columns =
    [
        new ColumnDefinition { Name = "id", Type = DbType.Integer, IsPrimaryKey = true },
        new ColumnDefinition { Name = "name", Type = DbType.Text },
    ],
};

DbValue[][] rows =
[
    [DbValue.Integer(1), DbValue.Text("Ada")],
    [DbValue.Integer(2), DbValue.Text("Grace")],
];

await TableArchiveWriter.WriteAsync(
    "customers.cdbtable",
    schema,
    TableArchiveWriter.ToAsyncRows(rows));
```

## Dependencies

- `CSharpDB.Primitives` - shared type system and schema contracts
- `CSharpDB.Storage` - record serialization used by native table archives

## Related Packages

| Package | Description |
|---------|-------------|
| [CSharpDB.Execution](https://www.nuget.org/packages/CSharpDB.Execution) | Query planner and operators that consume archive readers for external tables |
| [CSharpDB.Client](https://www.nuget.org/packages/CSharpDB.Client) | Public client API that exposes table archive import/export workflows |
| [CSharpDB](https://www.nuget.org/packages/CSharpDB) | Recommended all-in-one package |

## License

MIT - see [LICENSE](https://github.com/MaxAkbar/CSharpDB/blob/main/LICENSE) for details.
