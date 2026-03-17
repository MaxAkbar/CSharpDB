# Storage Examples

This page is the runnable example index for the `CSharpDB.Storage` tutorial track.

There are two distinct example tracks:

1. **Study examples** — the shared `StorageStudyExamples.*` REPL-based walkthroughs for guided exploration
2. **Advanced standalone examples** — larger sample applications that extend `CSharpDB.Storage` into domain-specific engines

## Choose a track

| Track | Best for | Shape |
|-------|----------|-------|
| **Study examples** | Learning the storage surface in guided slices | Shared REPL host with multiple small examples |
| **Advanced standalone examples** | Seeing how to build a full storage-backed engine | Separate app per domain with its own CLI/API/UI |

Start with the study examples if you are learning `Pager`, WAL, B+tree, and configurability. Start with the advanced standalone examples if you want bigger, self-contained applications that demonstrate how to extend `CSharpDB.Storage` directly.

## Advanced standalone examples

These four apps are the official advanced examples for extending `CSharpDB.Storage` into domain-specific engines. Each sample keeps its own README as the authoritative deep dive.

| Example | Scenario | Storage pattern | Run | README | Key concepts |
|---------|----------|-----------------|-----|--------|--------------|
| **CSharpDB.GraphDB** | Graph database | Multi-tree graph adjacency with reverse-edge index | `dotnet run --project docs/tutorials/storage/examples/CSharpDB.GraphDB/CSharpDB.GraphDB.csproj` | [GraphDB](./CSharpDB.GraphDB/README.md) | Multiple B+trees, composite edge keys, forward/reverse traversal |
| **CSharpDB.SpatialIndex** | Spatial proximity search | Single-tree Hilbert-key locality index | `dotnet run --project docs/tutorials/storage/examples/CSharpDB.SpatialIndex/CSharpDB.SpatialIndex.csproj` | [SpatialIndex](./CSharpDB.SpatialIndex/README.md) | Space-filling curves, range scans, post-filtered nearest queries |
| **CSharpDB.TimeSeries** | Time-series engine | Monotonic-key append and range scan | `dotnet run --project docs/tutorials/storage/examples/CSharpDB.TimeSeries/CSharpDB.TimeSeries.csproj` | [TimeSeries](./CSharpDB.TimeSeries/README.md) | Natural key ordering, right-edge inserts, cursor-based time windows |
| **CSharpDB.VirtualFS** | Virtual file system | Multi-tree metadata/content/path/children layout | `dotnet run --project docs/tutorials/storage/examples/CSharpDB.VirtualFS/CSharpDB.VirtualFS.csproj` | [VirtualFS](./CSharpDB.VirtualFS/README.md) | Multiple B+trees, composite child keys, hashed path lookup, transactional file content |

### What each advanced sample demonstrates

- **GraphDB**: a graph database built on multiple B+trees, with separate outgoing and incoming edge indexes for efficient directed traversal
- **SpatialIndex**: a geographic index that maps 2D points onto a Hilbert curve so a single B+tree can support nearby and bounding-box queries
- **TimeSeries**: a minimal time-series design where `DateTime.UtcNow.Ticks` becomes the natural B+tree key for append-heavy writes and range scans
- **VirtualFS**: a virtual file system split across metadata, content, path, and child trees to support directory listings, file content, and path-based lookup

## Study examples

The study examples are an interactive REPL with two categories of examples:

1. **Application patterns** — real-world use cases built entirely on the `Database` API, proving the storage engine can back much more than traditional SQL tables. Each example provides domain-specific commands for natural interaction (file system commands for virtual-drive, graph queries for graph-store, etc.).
2. **Storage extensibility** — custom caches, checkpoint policies, interceptors, checksum providers, and more. These demonstrate how to plug into the storage engine internals.

## Architecture

The examples follow SOLID principles with a multi-project structure:

| Project | Description |
|---------|-------------|
| **StorageStudyExamples.Core** | Shared interfaces (`IExample`, `IInteractiveExample`), abstract `DataStoreBase`, `CommandInfo` record |
| **StorageStudyExamples.Repl** | Interactive REPL host — two-state flow (main menu / example mode) |
| **StorageStudyExamples.VirtualDrive** | Virtual file system with folders, files, and shortcuts |
| **StorageStudyExamples.ConfigStore** | Hierarchical config store with versioning and change history |
| **StorageStudyExamples.EventLog** | Append-only event log with batch inserts and analytics |
| **StorageStudyExamples.TaskQueue** | Persistent job queue with state machine and retry logic |
| **StorageStudyExamples.GraphStore** | Social network graph with traversal queries |
| **StorageStudyExamples.StorageInternals** | Storage engine configuration examples (9 demos) |

## Run

Start the interactive REPL:

```bash
dotnet run --project docs/tutorials/storage/examples/StorageStudyExamples.Repl/StorageStudyExamples.Repl.csproj
```

### REPL flow

The REPL has two states:

**Main menu** — lists all available examples grouped by type. Type `load <name>` to enter an example.

```
  Main menu:
    list                    List all available examples
    load <name>             Load an example
    help                    Show help
    clear                   Clear the screen
    quit                    Exit the REPL
```

**Example mode** — once loaded, only the example's domain-specific commands are shown. Type `back` to return to the main menu.

```
  Common commands (available in all examples):
    demo                    Run the scripted demo
    sql <query>             Execute raw SQL
    back                    Return to main menu
    help                    Show commands for this example
    clear                   Clear the screen
    quit                    Exit the REPL
```

The REPL creates a temp working directory for each loaded example so no `.cdb` or `.wal` files are left in the repository.

## Interface design

All application-pattern examples implement `IInteractiveExample`, which extends `IExample`:

```csharp
public interface IExample : IAsyncDisposable
{
    string Name { get; }
    string CommandName { get; }
    string Description { get; }
    Task InitializeAsync(string workingDirectory);
    Task RunDemoAsync(TextWriter output);
}

public sealed record CommandInfo(string Name, string Usage, string Description);

public interface IInteractiveExample : IExample
{
    IReadOnlyList<CommandInfo> GetCommands();
    Task<bool> ExecuteCommandAsync(string commandName, string args, TextWriter output);
}
```

Each example defines its own domain-specific commands via `GetCommands()` and handles them in `ExecuteCommandAsync()`. The abstract `DataStoreBase` class provides database lifecycle management (`InitializeAsync`, `DisposeAsync`), schema/seed hooks, and a raw SQL helper. Storage internals examples implement only `IExample` (Interface Segregation Principle).

## Storage extensibility examples

These show how to customize the storage engine internals via `PagerOptions` and `StorageEngineOptionsBuilder`.

| Command | What it demonstrates |
|---------|---------------------|
| `default-config` | Open a database with all default settings |
| `production-config` | Bounded LRU cache, CRC32 checksums, caching indexes |
| `debug-config` | Verbose interceptor logging on every page operation |
| `batch-import` | Disable auto-checkpoint for high-throughput bulk writes |
| `metrics-cache` | Instrument the page cache with hit/miss/eviction stats |
| `multiple-interceptors` | Combine a logger and latency tracker in one pipeline |
| `crash-recovery-test` | Fault-inject a write failure and verify WAL recovery |
| `checkpoint-policy-test` | Deterministic test of `TimeIntervalCheckpointPolicy` with a fake clock |
| `wal-size-policy-test` | Deterministic test of `WalSizeCheckpointPolicy` thresholds |

**Suggested starting points:** `default-config`, `debug-config`, `metrics-cache`

## Application pattern examples

These show the storage engine used as a general-purpose embedded data store for non-traditional workloads. Each is self-contained: it creates a schema, populates sample data, and provides domain-specific commands for interactive exploration. A scripted `demo` is also available in each example.

### `virtual-drive` — Virtual file system

Stores folders, files, and shortcuts in a single `.cdb` file. Supports path-based navigation with a current directory, recursive tree walking, and shortcut (symlink) following.

| Command | Usage | Description |
|---------|-------|-------------|
| `tree` | `tree` | Show the full directory tree |
| `ls` | `ls [path]` | List contents of a directory |
| `cd` | `cd <path>` | Change current directory |
| `pwd` | `pwd` | Print current directory path |
| `cat` | `cat <filename>` | Print file contents |
| `mkdir` | `mkdir <name>` | Create a folder |
| `touch` | `touch <name> [content...]` | Create a file |
| `ln` | `ln <name> <target-path>` | Create a shortcut |
| `rm` | `rm <name>` | Remove a file or folder (recursive) |
| `mv` | `mv <name> <new-name>` | Rename an entry |
| `info` | `info <name>` | Show entry details (type, size, created) |
| `stats` | `stats` | Drive statistics (counts by type, total size) |

**CSharpDB features used:** `CREATE INDEX`, `INSERT` with `PrepareInsertBatch` (for BLOB content), `SELECT WHERE parent_id = ?`, `GROUP BY`, `SUM`, `UPDATE` (rename via `SET name`, move via `SET parent_id`, retarget via `SET target_path`), `DELETE` (single row and recursive cascade), transactional read-modify-write for BLOB updates.

### `config-store` — Key-value configuration store

A persistent hierarchical config system similar to etcd or Windows Registry. Supports namespaced keys, typed values, versioning (auto-incremented on update), and a full change history log.

| Command | Usage | Description |
|---------|-------|-------------|
| `list` | `list [namespace]` | List config entries (all or filtered) |
| `get` | `get <namespace> <key>` | Get a config value |
| `set` | `set <namespace> <key> <value> [type]` | Set a config value (default type: string) |
| `delete` | `delete <namespace> <key>` | Delete a config entry |
| `history` | `history [namespace]` | Show change history |
| `namespaces` | `namespaces` | List all namespaces with counts |
| `rename-ns` | `rename-ns <old> <new>` | Rename a namespace |
| `drop-ns` | `drop-ns <namespace>` | Delete all entries in a namespace |

**CSharpDB features used:** Transactions (`BEGIN`/`COMMIT`/`ROLLBACK`) for atomic read-then-write, two-table design (entries + history), `CREATE INDEX`, `GROUP BY`, `ORDER BY`, batch `UPDATE` (rename namespace across multiple rows), bulk `DELETE` with history logging.

### `event-log` — Append-only event log

An audit trail / event store with 34 events from five services (AuthService, PaymentGateway, Scheduler, Monitor, ApiGateway). Queries by time range, severity threshold, and source.

| Command | Usage | Description |
|---------|-------|-------------|
| `tail` | `tail [count]` | Show most recent events (default: 20) |
| `log` | `log <source> <severity> <message>` | Append a new event |
| `filter` | `filter <source\|severity\|category> <value>` | Filter events by field |
| `stats` | `stats` | Severity histogram and error rates by source |
| `reclassify` | `reclassify <id> <new-severity>` | Change event severity |
| `purge` | `purge <count>` | Delete the oldest N events |

**CSharpDB features used:** `PrepareInsertBatch` for bulk writes, `CREATE INDEX` on timestamp and severity, `GROUP BY` with `COUNT(*)`, multiple aggregate queries, `UPDATE` (reclassify severity on indexed column), `DELETE` (range-based log rotation).

### `task-queue` — Persistent job queue

A job scheduler with state machine transitions (pending -> running -> completed/failed), priority-based dequeue, and retry logic.

| Command | Usage | Description |
|---------|-------|-------------|
| `list` | `list [queue]` | List jobs (all or by queue) |
| `enqueue` | `enqueue <queue> <priority> <payload...>` | Add a job |
| `claim` | `claim <queue>` | Claim highest-priority pending job |
| `complete` | `complete <id>` | Mark job as completed |
| `fail` | `fail <id> <error...>` | Mark job as failed |
| `retry` | `retry <id>` | Re-queue a failed job |
| `cancel` | `cancel <id>` | Cancel a pending job |
| `dashboard` | `dashboard` | Status summary per queue |
| `reprioritize` | `reprioritize <id> <priority>` | Change job priority |
| `move` | `move <id> <queue>` | Move job to different queue |
| `purge` | `purge` | Delete all completed jobs |

**CSharpDB features used:** Transactions for atomic state transitions, `ORDER BY priority DESC LIMIT 1` for priority dequeue, `GROUP BY queue, status` for dashboard, `UPDATE` with computed values, `UPDATE` (reprioritize, move between queues via `SET queue`), conditional `DELETE` (cancel pending), bulk `DELETE` (purge completed).

### `graph-store` — Social network graph

Nodes and edges stored relationally, with traversal queries over a 10-person social network. All commands accept person names instead of IDs.

| Command | Usage | Description |
|---------|-------|-------------|
| `nodes` | `nodes` | List all people |
| `graph` | `graph` | Full adjacency list |
| `follows` | `follows <name>` | Who this person follows |
| `followers` | `followers <name>` | Who follows this person |
| `fof` | `fof <name>` | Friends-of-friends (2-hop) |
| `mutual` | `mutual <name1> <name2>` | People both follow |
| `reciprocal` | `reciprocal` | All mutual follow-back pairs |
| `follow` | `follow <source> <target>` | Add a follow edge |
| `unfollow` | `unfollow <source> <target>` | Remove a follow edge |
| `add-person` | `add-person <name>` | Add a new person |
| `rename` | `rename <old-name> <new-name>` | Rename a person |
| `remove` | `remove <name>` | Remove person and all edges |
| `stats` | `stats` | Connection counts per person |

**CSharpDB features used:** `PrepareInsertBatch`, `CREATE INDEX` on `source_id` and `target_id`, `JOIN` (including 3-table JOINs and self-JOINs), `GROUP BY` with `COUNT(*)`, `DISTINCT`, post-query filtering in C# for set operations, `UPDATE` (rename node label), `DELETE` (unfollow edge, cascading user removal across tables in a transaction).
