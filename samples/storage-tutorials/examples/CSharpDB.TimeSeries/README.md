# CSharpDB.TimeSeries

A **time-series database** built on a single [CSharpDB.Storage](../CSharpDB.Storage) B+tree.
It uses `DateTime.UtcNow.Ticks` (a monotonically increasing `long`) as the tree key, which means
data is always stored in chronological order and time-range queries come for free via
`BTreeCursor.SeekAsync`.

This sample demonstrates the simplest possible B+tree use case: **a single tree where the natural
key order matches the query pattern**.

---

## What This Sample Demonstrates

| CSharpDB.Storage concept | How this sample uses it |
|---|---|
| **Natural key ordering** | `DateTime.Ticks` increases monotonically, so inserts are always append-like at the rightmost leaf |
| **BTreeCursor.SeekAsync** | Range queries seek directly to the start tick and scan forward — no index needed |
| **BTreeCursor.MoveNextAsync** | Sequential scan with automatic leaf-to-leaf traversal and speculative prefetch |
| **Single B+tree** | The entire database is one tree: key = ticks, value = JSON payload |
| **Transactions** | Every insert/delete is wrapped in `BeginTransactionAsync` / `CommitAsync` / `RollbackAsync` |
| **Superblock pattern** | The tree's root page ID is persisted at reserved key 0 |
| **Crash safety** | WAL ensures durability; incomplete writes are rolled back on recovery |

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│  TimeSeriesDatabase (high-level API)                     │
│    RecordAsync, QueryAsync, GetLatestAsync, DeleteAsync   │
├──────────────────────────────────────────────────────────┤
│  TimeSeriesStore (B+tree operations)                     │
│    _data : DateTime.Ticks (long) → JSON(TimeSeriesPoint) │
│                                                          │
│    Insert: tree.InsertAsync(point.TimestampTicks, json)  │
│    Query:  cursor.SeekAsync(startTicks)                  │
│            while cursor.CurrentKey <= endTicks            │
│              → cursor.MoveNextAsync()                    │
├──────────────────────────────────────────────────────────┤
│  CSharpDB.Storage                                        │
│    Pager → WAL → FileStorageDevice → .cdb file           │
└──────────────────────────────────────────────────────────┘
```

### Why Ticks as the B+tree Key?

B+trees store entries sorted by key. When the key is `DateTime.UtcNow.Ticks`:

1. **Inserts are always at the rightmost leaf** — new data is always the latest timestamp,
   so the tree never has to split interior pages to insert in the middle. This gives
   near-optimal write performance.

2. **Range queries are a cursor scan** — to find all points between 10:00 and 10:30,
   `SeekAsync(startTicks)` jumps directly to the first relevant leaf page, then
   `MoveNextAsync()` walks forward through consecutive leaf pages until the end tick.
   No secondary index required.

3. **Aggregation is a single pass** — min, max, sum, average, first, and last are computed
   while scanning the range. No separate materialised view needed.

### Data Model

```csharp
public sealed class TimeSeriesPoint
{
    public long      TimestampTicks { get; set; }  // B+tree key
    public DateTime  TimestampUtc   { get; set; }  // Derived from ticks
    public string    Metric         { get; set; }  // e.g. "temperature_c"
    public double    Value          { get; set; }  // e.g. 23.7
    public string?   Unit           { get; set; }  // e.g. "°C", "%", "USD"
    public Dictionary<string, string>? Tags { get; set; }  // e.g. {"sensor":"s01"}
}

public sealed class TimeSeriesAggregation
{
    public int    Count   { get; set; }
    public double Min     { get; set; }
    public double Max     { get; set; }
    public double Sum     { get; set; }
    public double Average => Count > 0 ? Sum / Count : 0;
    public double First   { get; set; }
    public double Last    { get; set; }
}
```

## Getting Started

### Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download) or later

### Clone and Build

```bash
dotnet build samples/storage-tutorials/examples/CSharpDB.TimeSeries/CSharpDB.TimeSeries.csproj
```

### Run the CLI REPL

```bash
dotnet run --project samples/storage-tutorials/examples/CSharpDB.TimeSeries/CSharpDB.TimeSeries.csproj
```

You'll see an interactive shell:

```
╔═══════════════════════════════════════════╗
║  CSharpDB Time-Series Database REPL       ║
║  Type 'help' for commands, 'exit' to quit ║
╚═══════════════════════════════════════════╝
• Database file: timeseries.cdb

tsdb> _
```

Run the `sample` command to generate demo data (IoT temperature, CPU metrics, stock prices)
and see queries and ASCII charts in action.

### Run the Web UI

```bash
dotnet run serve
```

Then open [https://localhost:62388](https://localhost:62388) in your browser.

## CLI Commands

| Command | Description | Example |
|---|---|---|
| `sample` | Reset DB and run full demo (IoT + CPU + stock data) | `sample` |
| `record <metric> <value> [unit] [tag:k=v]` | Record a data point | `record cpu_percent 73.5 % tag:host=web01` |
| `query <from> <to> [metric] [max:N]` | Query a time range | `query -1h now cpu_percent` |
| `get <ticks>` | Retrieve a single point by exact ticks | `get 638700000000000000` |
| `delete <ticks>` | Delete a point by exact ticks | `delete 638700000000000000` |
| `latest` | Show the most recently recorded point | `latest` |
| `count` | Count total stored data points | `count` |
| `chart <from> <to> [metric]` | Render an ASCII chart | `chart -1h now temperature_c` |
| `reset` | Delete database and start fresh | `reset` |

### Time Format

The `<from>` and `<to>` parameters accept multiple formats:

| Format | Example | Meaning |
|---|---|---|
| ISO 8601 | `2024-01-15T10:30:00Z` | Absolute UTC timestamp |
| Date only | `2024-01-15` | Midnight UTC on that date |
| Relative | `-1h`, `-30m`, `-7d`, `-60s` | Relative to now (hours, minutes, days, seconds) |
| Keywords | `now`, `today`, `yesterday` | Convenience aliases |

## REST API

When running in `serve` mode, the following endpoints are available:

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/timeseries/points` | Record a point (`{ "metric": "...", "value": 42, "unit": "°C", "tags": {...} }`) |
| `GET` | `/api/timeseries/query?from=...&to=...&metric=...&maxResults=1000` | Time-range query with aggregation |
| `GET` | `/api/timeseries/points/{ticks}` | Get single point by ticks |
| `DELETE` | `/api/timeseries/points/{ticks}` | Delete a point |
| `GET` | `/api/timeseries/latest` | Most recent point |
| `GET` | `/api/timeseries/count` | Total point count |
| `POST` | `/api/timeseries/reset` | Reset entire database |

### Example: Record and Query

```bash
# Record a temperature reading
curl -X POST https://localhost:62388/api/timeseries/points \
  -H "Content-Type: application/json" \
  -d '{"metric":"temperature_c","value":23.7,"unit":"°C","tags":{"sensor":"s01"}}'

# Query the last hour
curl "https://localhost:62388/api/timeseries/query?from=2024-01-15T09:00:00Z&to=2024-01-15T10:00:00Z&metric=temperature_c"
```

Response includes both the data points and pre-computed aggregation:

```json
{
  "points": [
    { "timestampTicks": 638398..., "metric": "temperature_c", "value": 23.7, "unit": "°C", ... },
    ...
  ],
  "aggregation": {
    "count": 60,
    "min": 21.2,
    "max": 25.8,
    "sum": 1398.6,
    "average": 23.31,
    "first": 22.1,
    "last": 24.9
  }
}
```

## Web Dashboard

The web dashboard provides a visual interface for recording, querying, and visualizing time-series data:

- **Stat cards** — total points, latest value/metric, latest timestamp, query result count
- **Canvas chart** — interactive line chart with area fill, grid lines, and axis labels (no external libraries)
- **Aggregation panel** — min, max, average, sum, first, last computed over the query range
- **Data table** — sortable rows with metric badges, unit badges, and tag pills; click any row to inspect
- **Quick range buttons** — 1h, 6h, 24h, 7d presets for fast time-window selection
- **Sample data generator** — generate IoT temperature, CPU utilization, or stock price data with one click
- **Record form** — manual data point entry with metric, value, unit, and JSON tags

## Key Code Walkthrough

### The core B+tree concept: ticks as keys

```csharp
// TimeSeriesStore — a single B+tree
// Key:   DateTime.UtcNow.Ticks (long) — monotonically increasing
// Value: JSON-serialised TimeSeriesPoint

// Insert: always appends to the rightmost leaf
await _data.InsertAsync(point.TimestampTicks, json, ct);

// Query: BTreeCursor seeks to the start time, scans forward
var cursor = _data.CreateCursor();
await cursor.SeekAsync(startTicks, ct);

do
{
    if (cursor.CurrentKey > endTicks) break;
    var point = JsonSerializer.Deserialize<TimeSeriesPoint>(cursor.CurrentValue.Span);
    results.Add(point);
}
while (await cursor.MoveNextAsync(ct));
```

### Opening or creating the database

```csharp
// TimeSeriesDatabase.OpenAsync()
var options = new StorageEngineOptionsBuilder()
    .UsePagerOptions(new PagerOptions { MaxCachedPages = 2048 })
    .UseBTreeIndexes()
    .Build();

var factory = new DefaultStorageEngineFactory();
var context = await factory.OpenAsync(filePath, options, ct);

var store = new TimeSeriesStore(context.Pager);
```

### Recording a data point (transaction-wrapped)

```csharp
// TimeSeriesStore.InsertPointAsync()
await Pager.BeginTransactionAsync(ct);
try
{
    await _data.DeleteAsync(point.TimestampTicks, ct);  // Upsert: remove if exists
    await _data.InsertAsync(point.TimestampTicks, json, ct);
    await PersistSuperblockAsync(ct);
    await Pager.CommitAsync(ct);
}
catch
{
    await Pager.RollbackAsync(ct);
    throw;
}
```

### Aggregation computed during the scan

```csharp
// TimeSeriesAggregation.Compute()
public static TimeSeriesAggregation Compute(IReadOnlyList<TimeSeriesPoint> points)
{
    var min = double.MaxValue;
    var max = double.MinValue;
    var sum = 0.0;

    foreach (var point in points)
    {
        if (point.Value < min) min = point.Value;
        if (point.Value > max) max = point.Value;
        sum += point.Value;
    }

    return new TimeSeriesAggregation
    {
        Count = points.Count,
        Min = min, Max = max, Sum = sum,
        First = points[0].Value,
        Last = points[^1].Value,
    };
}
```

## Use Cases

This pattern (ticks as B+tree keys) maps directly to real-world scenarios:

| Scenario | Metric examples | Tags |
|---|---|---|
| **IoT telemetry** | `temperature_c`, `humidity_pct`, `pressure_hpa` | `sensor=s01`, `location=warehouse-A` |
| **Application metrics** | `cpu_percent`, `memory_mb`, `request_latency_ms` | `host=web-01`, `region=us-east` |
| **Stock price history** | `stock_price`, `volume`, `market_cap` | `symbol=ACME`, `exchange=NYSE` |
| **Energy monitoring** | `power_watts`, `energy_kwh` | `meter=main`, `building=HQ` |
| **Health data** | `heart_rate_bpm`, `blood_oxygen_pct` | `device=watch-01`, `user=alice` |

## Project Structure

```
CSharpDB.TimeSeries/
├── Core/
│   ├── TimeSeriesPoint.cs             # Data model: ticks, metric, value, unit, tags
│   ├── TimeSeriesQueryResult.cs       # Query result + aggregation computation
│   ├── TimeSeriesDatabase.cs          # High-level API (factory + record/query/delete)
│   └── TimeSeriesStore.cs             # B+tree operations (insert, seek, scan)
├── Api/
│   ├── ITimeSeriesApi.cs              # Service contract + request DTOs
│   ├── TimeSeriesApiService.cs        # Thread-safe service wrapper
│   └── Clients/
│       ├── InProcessTimeSeriesApiClient.cs
│       └── HttpTimeSeriesApiClient.cs
├── Hosting/
│   └── TimeSeriesWebHost.cs           # ASP.NET Core REST endpoints
├── Infrastructure/
│   └── TimeSeriesDatabaseUtility.cs
├── Cli/
│   ├── AnsiConsoleWriter.cs           # Colored terminal output
│   ├── TimeSeriesConsolePresenter.cs  # Tables, aggregation, ASCII charts
│   ├── TimeSeriesSampleRunner.cs      # End-to-end demo data generator
│   ├── Commands/                      # REPL command implementations
│   └── Repl/
│       └── ReplHost.cs                # Interactive command loop
├── Program.cs                         # Entry point (CLI or serve mode)
└── CSharpDB.TimeSeries.csproj
```

## Database File

All data is stored in a single file: `timeseries.cdb` (with a companion `.wal` file during writes).
Delete both files to start fresh, or use the `reset` command.

## Comparison with VirtualFS Sample

| Aspect | VirtualFS | TimeSeries |
|---|---|---|
| Number of B+trees | 4 (entries, content, path index, children) | 1 (data) |
| Key type | Entry IDs, hash keys, composite keys | `DateTime.Ticks` (monotonic long) |
| Key design | Application-generated IDs + hash-based lookups | Natural timestamp ordering |
| Primary query | Path traversal + directory listing | Time-range scan |
| Cursor usage | Range scan for directory children | Range scan for time windows |
| Write pattern | Random inserts across 4 trees | Append-only to rightmost leaf |
| Complexity | Multi-tree coordination, path resolution | Single tree, simple key-value |
