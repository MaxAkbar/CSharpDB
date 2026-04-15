# CSV Bulk Import Sample

This sample is a runnable C# console app that demonstrates the current best-practice bulk-ingest pattern for SQL tables in CSharpDB.

## Why This Sample Is API-First

CSharpDB does not currently expose a first-class table-import CLI command or SQL `BULK INSERT` / `COPY` surface. The fastest supported relational ingest path today is still the public engine API:

- `Database.OpenAsync(...)`
- `UseWriteOptimizedPreset()`
- `PrepareInsertBatch(...)`
- explicit transaction batching
- secondary indexes created after the load

That is exactly what this sample shows.

## Files

- `CsvBulkImportSample.csproj` - sample project
- `Program.cs` - creates the table, streams CSV rows, batches inserts, and creates indexes after the load
- `events.csv` - bundled fixed-schema input file

## Run

```bash
dotnet run --project samples/csv-bulk-import/CsvBulkImportSample.csproj
```

Optional flags:

```bash
dotnet run --project samples/csv-bulk-import/CsvBulkImportSample.csproj -- \
  --csv-path samples/csv-bulk-import/events.csv \
  --database-path artifacts/samples/csv-import-demo.db \
  --batch-size 1000
```

The sample validates the CSV header case-insensitively, converts each row into `DbValue[]`, flushes each batch inside an explicit transaction, then creates secondary indexes after the data is loaded.

## Inspect with the CLI

The sample prints the generated database path on success. Open it with `CSharpDB.Cli`:

```bash
dotnet run --project src/CSharpDB.Cli -- artifacts/samples/csv-import-demo.db
```

Example queries:

```sql
SELECT COUNT(*) FROM events;
SELECT severity, COUNT(*) FROM events GROUP BY severity ORDER BY severity;
SELECT source, AVG(ingest_ms) FROM events GROUP BY source ORDER BY source;
```

## Related Docs

- [CSV Bulk Import Tutorial](https://csharpdb.com/docs/tutorials/csv-bulk-import.html)
- [Samples Overview](../README.md)
