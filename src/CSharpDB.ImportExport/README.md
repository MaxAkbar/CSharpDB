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

The integer primary-key lookup index is an optional acceleration structure.
Writers retain at most 65,536 index entries in memory. Archives with more rows
are written without the physical lookup index; their schema and rows are still
complete, and external-table point lookups use the streaming scan fallback.

Format v5 archives carry required SHA-256 digests for their schema, rows, and
optional physical index. Every reader path verifies those section digests
before exposing archive data; v3 and v4 remain readable for compatibility.

The Admin restore workflow adds an independent post-load check before a staged
table becomes visible. It compares archive rows with the loaded table using
the `csharpdb-canon-v1` logical encoding and an order-independent,
duplicate-preserving 256-partition checksum. Regenerated rowversion values are
explicitly excluded. The target scan and final rename share one transaction,
and metadata plus rows are read from one immutable private archive snapshot.
A mismatch, validation error, or cancellation before the activation boundary
leaves no activated table. Once validation passes, the short rename/commit
sequence is intentionally non-cancelable and its result is reconciled if
commit reporting fails through a durable activation receipt written in the
same transaction. The immutable snapshot and memory-bounded checksum
spill workspace each have a configurable 4-GiB default safety limit through
`CSharpDB.Admin.ImportExport.Services.TableArchiveRestoreOptions`; exceeding
either limit fails the restore safely.
This activation contract currently requires a direct/local CSharpDB transport.

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
