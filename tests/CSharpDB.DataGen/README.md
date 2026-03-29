# CSharpDB.DataGen

Synthetic dataset generator for CSharpDB. It produces realistic, repeatable test data that exercises the storage engine, indexes, and collection paths under controlled conditions.

Everything is spec-driven: dataset shape, field generation rules, row counts, output filenames, and indexes are all declared in JSON spec files under `Specs/`. No code changes are needed to reshape a dataset.

## When do you need this?

Any time you want to test CSharpDB behavior beyond trivial hand-written rows. Typical situations:

- You changed the B-tree or page layout and want to bulk-load a million rows to see if insert throughput regressed.
- You are working on collection-path indexing and need thousands of nested JSON documents with sparse fields and variable sizes.
- You want to validate that recovery works correctly after a crash mid-insert, and you need a deterministic dataset you can reproduce exactly.
- You are comparing "load data then build indexes" vs "build indexes then load data" and need the same dataset for both runs.

## Four dataset tracks

The generator ships three built-in dataset specs plus a schema-inference mode that reads an existing CSharpDB database. Each one targets a different storage-engine concern.

### Relational

Models a typical order-entry system: customers, addresses, products, orders, line items, and payments. This is the dataset to reach for when you are testing B-tree insert/lookup performance, secondary index overhead, or SQL query correctness.

The relational spec produces referentially consistent data across six tables. Foreign keys point at real parent rows, monetary values are internally consistent (line totals sum to order subtotals), and statuses follow realistic distributions. When you load this into CSharpDB with `--build-indexes`, you get a database that is ready for point lookups, range scans, and join queries without any post-processing.

### Documents

Models a multi-tenant event store where each document has a fixed header (`id`, `tenantId`, `type`, `createdUtc`) plus optional nested sections like `profile`, `preferences`, `addresses`, `tags`, and a variable-size `payload`. This is the dataset to reach for when you are testing collection storage, JSON serialization costs, or path-index selectivity.

The document spec controls sparsity (how often optional sections are null), average payload size, and tenant cardinality. Increasing `--null-rate` produces documents that stress sparse-field handling. Increasing `--avg-size` produces documents that stress large-value storage and page fragmentation.

### Time-series

Models an IoT sensor network: devices emit readings (temperature, pressure, humidity, throughput, latency) with timestamps. This is the dataset to reach for when you are testing append-heavy write paths, recent-time-window queries, or hot-partition behavior.

The time-series spec skews both device selection and timestamps toward the recent window, which mimics real telemetry workloads where a small number of devices generate most of the traffic and most queries target the last few hours or days.

### From Database

Reads the schema of an existing CSharpDB database file -- tables, columns, types, indexes -- and generates synthetic data that matches the schema. This is the mode to reach for when you already have a database with a real schema and you want to fill it with test data without writing a JSON spec by hand.

The generator uses column-name heuristics to pick realistic generation rules. A column named `Email` gets fake email addresses via Bogus, a column named `CreatedUtc` gets skewed timestamps, a column ending in `Id` that looks like a foreign key gets random integers in the row-count range, and so on. Columns that don't match any heuristic fall back to type-appropriate random values (random integers, random doubles, or lorem-ipsum text).

## Walkthrough: your first relational benchmark

Suppose you just changed the batch-insert code path and want to know if it got faster or slower. Here is how you would set up a before/after comparison.

**Step 1 -- generate CSV files to inspect the data.**

```bash
dotnet run --project tests/CSharpDB.DataGen/CSharpDB.DataGen.csproj -- relational --rows 100000 --seed 42
```

This writes `schema.sql`, one CSV per table, and `summary.json` into `artifacts/data-gen/relational/`. Open the CSV files to spot-check that the generated values look right. Open `summary.json` to confirm the resolved row counts and the seed that was used.

Because the seed is fixed, running this command again produces byte-identical output. That means you can diff two runs if you suspect a spec change introduced a regression in the generated data.

**Step 2 -- bulk-load into CSharpDB and build indexes.**

```bash
dotnet run --project tests/CSharpDB.DataGen/CSharpDB.DataGen.csproj -- relational --rows 250000 --load-direct --database-path artifacts/data-gen/relational.db --overwrite-db --build-indexes
```

This skips the "generate files then import" step and writes rows directly into the storage engine using the batch-insert API. The `--build-indexes` flag tells the loader to create secondary indexes after the bulk load finishes, which is typically faster than maintaining indexes during insertion.

After this completes you have a ready-to-query `.db` file you can point benchmarks or integration tests at.

**Step 3 -- compare.**

Run your benchmark suite against the database from step 2, record the numbers, then switch to the new code path and repeat. Because the seed and row count are identical, the dataset is the same in both runs, so any difference in throughput or latency is attributable to your code change.

## Walkthrough: stress-testing document storage

Suppose you want to find out how CSharpDB handles large, sparse JSON documents with path indexes on nested fields. Here is a scenario that exercises those code paths.

```bash
dotnet run --project tests/CSharpDB.DataGen/CSharpDB.DataGen.csproj -- docs --rows 250000 --avg-size 4096 --null-rate 0.15 --recent-rate 0.95 --load-direct --database-path artifacts/data-gen/documents.db --overwrite-db --build-indexes
```

What each knob does in this scenario:

- `--avg-size 4096` pushes the payload generator toward 4 KB documents. This means the storage engine has to deal with values that span multiple pages, which is where fragmentation bugs tend to hide.
- `--null-rate 0.15` makes 15% of optional document sections null. This tests whether path indexes handle missing fields correctly and whether serialization skips absent sections efficiently.
- `--recent-rate 0.95` concentrates 95% of `createdUtc` timestamps in the recent window. This is realistic for event-style workloads and tests whether time-range queries on the collection path index perform well when most data clusters at one end.

After loading, the database has collection-path indexes on `tenantId`, `createdUtc`, and `tags[]`, so you can immediately run filtered lookups and range scans without additional setup.

## Walkthrough: simulating IoT ingestion pressure

Suppose you want to test how the append path and WAL behave under sustained sequential writes with skewed device access. Here is a scenario that models that.

```bash
dotnet run --project tests/CSharpDB.DataGen/CSharpDB.DataGen.csproj -- timeseries --rows 2000000 --device-count 50000 --hot-key-rate 0.30 --recent-rate 0.90 --load-direct --database-path artifacts/data-gen/timeseries.db --overwrite-db --build-indexes
```

What each knob does in this scenario:

- `--device-count 50000` creates a population of 50K distinct device IDs. This controls the cardinality of the `deviceId` index.
- `--hot-key-rate 0.30` means 30% of inserts target a narrow "hot band" of devices. This models real-world situations where a handful of sensors report much more frequently than the rest, which stresses page-level contention and cache behavior.
- `--recent-rate 0.90` makes 90% of timestamps fall in the recent window. This means the B-tree index on `timestampUtc` grows almost entirely at the right edge, which is a common pattern for time-series databases and a good test for append-optimized page splits.

After loading, you can run "latest N readings for device X" and "readings in time range for device X" queries to validate that the indexes and scan paths work correctly under skewed data.

## Walkthrough: populating an existing database schema with test data

Suppose you have a CSharpDB database with tables already created -- maybe from running your application's migration scripts -- but the tables are empty. You want to fill them with realistic test data for manual exploration or integration testing, and you don't want to write a JSON spec.

**Step 1 -- generate CSV files from the schema.**

```bash
dotnet run --project tests/CSharpDB.DataGen/CSharpDB.DataGen.csproj -- fromdb --source-database myapp.db --rows 50000 --seed 42
```

The generator opens `myapp.db`, reads every table's column definitions and indexes, and builds generation rules on the fly. For example, if your database has a `Users` table with columns `Id INTEGER PRIMARY KEY`, `Email TEXT NOT NULL`, `FirstName TEXT`, `CreatedUtc TEXT`, the generator will produce auto-increment IDs, fake email addresses, fake first names, and skewed timestamps -- all without any configuration.

The output goes to `artifacts/data-gen/fromdb/` and includes one CSV per table, a `schema.sql` reflecting the discovered schema, and a `summary.json`.

**Step 2 -- load directly into a new database for benchmarking.**

```bash
dotnet run --project tests/CSharpDB.DataGen/CSharpDB.DataGen.csproj -- fromdb --source-database myapp.db --rows 250000 --load-direct --database-path artifacts/data-gen/populated.db --overwrite-db --build-indexes
```

This creates a fresh database with the same schema as `myapp.db`, fills it with 250K rows per table, and builds all the secondary indexes that existed in the source. You now have a ready-to-query test database that matches your real schema.

This is useful when:

- you want to load-test your application's queries against realistic table shapes without maintaining a separate spec file
- you are developing a new feature that adds tables, and you want quick test data that matches the current schema
- you need to hand a populated database to another team member who doesn't know the spec DSL

**How the heuristics work.** The generator looks at each column's name, type, and constraints to pick a generation rule:

- Integer primary keys get auto-increment row indexes
- Columns named like `Email`, `FirstName`, `Phone`, `City`, `Country` get Bogus-generated fake values
- Columns ending in `Id` (that aren't the primary key) get random foreign-key-style integers
- Columns with date/time-related names get skewed timestamps
- Columns with monetary names (`Price`, `Total`, `Amount`) get random decimal values
- Boolean-style columns (`IsActive`, `HasVerified`, `Enabled`) get true/false with 70/30 probability
- Status/type/category columns get picked from a small set of labels
- Everything else falls back to type-appropriate random values

If a column is nullable, the generator wraps the rule in a 5% null-chance layer.

## Using a custom spec

All three built-in datasets are defined by JSON files under `Specs/`. If you need a different schema -- say you want to add a table, rename a column, or change a generation rule -- you can copy a spec, edit it, and pass it with `--spec-path`:

```bash
dotnet run --project tests/CSharpDB.DataGen/CSharpDB.DataGen.csproj -- relational --rows 50000 --spec-path my-custom-spec.dataset.json
```

This is useful when you want to:

- test a specific schema shape in isolation (e.g., a table with 50 columns to stress wide-row handling)
- change output filenames for a particular benchmark run
- experiment with different generation rules without touching the defaults

The spec format is documented in the [data generation plan](../../docs/CSharpDB-Data-Generation.md).

## Output details

**File output** (default): writes to `artifacts/data-gen/<dataset>/`. Override with `--output-path`. Relational datasets produce CSV files and a `schema.sql`; document datasets produce JSONL files. Both write a `summary.json` with the resolved row counts, seed, and knob values for that run.

**Direct load** (`--load-direct`): writes rows or documents directly into a CSharpDB database file. This bypasses CSV/JSONL parsing entirely, which is useful when you want to isolate storage-engine performance from import overhead.

**Skip files** (`--no-files`): when combined with `--load-direct`, skips file output entirely so you only get the database.

## Quick reference: all CLI options

| Option | Default | Description |
|---|---|---|
| `--rows <n>` | 100K (relational/docs), 1M (timeseries) | Top-level row or document count |
| `--seed <n>` | 42 | Deterministic generation seed |
| `--batch-size <n>` | 1000 | Batch size for direct database loading |
| `--output-path <path>` | `artifacts/data-gen/<dataset>/` | Where CSV/JSONL files are written |
| `--spec-path <path>` | built-in spec | Path to a custom dataset spec |
| `--load-direct` | off | Load data directly into CSharpDB |
| `--database-path <path>` | -- | Database file path for direct load |
| `--overwrite-db` | off | Replace an existing database file |
| `--build-indexes` | off | Build secondary indexes after load |
| `--no-files` | off | Skip CSV/JSONL file output |
| `--null-rate <0..1>` | 0.05 | Fraction of optional fields that are null |
| `--hot-key-rate <0..1>` | 0.20 | Fraction of traffic directed at hot keys |
| `--recent-rate <0..1>` | 0.80 | Fraction of timestamps in the recent window |
| `--orders-per-customer <n>` | 5 | Relational: average orders per customer |
| `--items-per-order <n>` | 4 | Relational: average items per order |
| `--tenant-count <n>` | 250 | Relational/docs: number of distinct tenants |
| `--avg-size <bytes>` | 1024 | Docs: target average document size |
| `--source-database <path>` | -- | From-database: path to the existing CSharpDB to read schema from |
| `--device-count <n>` | 100000 | Time-series: number of distinct devices |
