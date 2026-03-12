# Pub/Sub Change Events

Engine-level change event system for CSharpDB that hooks directly into the DML write path, using `System.Threading.Channels` as the delivery backbone. Events are only emitted for **committed** changes, ensuring consumers never see rolled-back data.

**Status:** Planned
**Roadmap link:** [Replication / change feed](../roadmap.md)

---

## Motivation

CSharpDB currently has no way for consumers to react to data changes in real time. Earlier host layers exposed basic `Action` delegates (`TablesChanged`, `SchemaChanged`) that carried no payload and were used only for UI refresh. Real-time change events unlock:

- **Reactive UIs** that refresh when data changes without polling
- **Audit logging** that captures every committed mutation
- **Cache invalidation** driven by actual writes
- **Event-driven architectures** where downstream systems react to database changes
- **Replication foundations** for future read replicas and change feeds

---

## Design Overview

```
   DML Execution                     Dispatch                     Consumers
┌──────────────────┐          ┌──────────────────────┐     ┌───────────────────────┐
│  QueryPlanner    │          │ ChangeEventDispatcher│     │  In-process           │
│  ─────────────   │          │  (IChangeEventSink)  │     │  (ChannelReader)      │
│  INSERT ──►buffer│          │                      │     ├───────────────────────┤
│  UPDATE ──►buffer│──commit──►  fan-out via         ├────►│  SSE endpoint         │
│  DELETE ──►buffer│          │  System.Threading    │     │  (/api/events/stream) │
│                  │          │  .Channels           │     ├───────────────────────┤
│  Collection API  │          │                      │     │  Client SDK           │
│  Put ────►buffer │          │  Bounded channels    ├────►│  (StreamChangesAsync) │
│  Delete ─►buffer │          │  w/ DropOldest       │     ├───────────────────────┤
└──────────────────┘          └──────────────────────┘     │  Future: SignalR,     │
         │                              ▲                  │  gRPC streaming       │
         │ rollback → discard           │                  └───────────────────────┘
         └──────────────────────────────┘
```

### Key Properties

- **Zero cost when disabled**: A null check on `_changeEventSink` is the only overhead per row when no subscribers exist
- **Committed changes only**: Events are buffered during the transaction and dispatched after `Pager.CommitAsync` succeeds; rolled-back transactions discard the buffer
- **Trigger cascades included**: Instrumentation sits after `FireTriggersAsync`, so any nested DML from triggers appends to the same buffer — consumers see the full picture
- **Single-writer safety**: All changes come from the single writer thread, so no synchronization is needed on the producer side
- **AOT-compatible**: Uses `System.Threading.Channels` (built-in .NET, no reflection)

---

## Approach Comparison

| Approach | Verdict |
|----------|---------|
| **Engine-level + Channels** (this plan) | Row-level data already available at trigger fire points, zero-cost when off, built-in .NET primitives, AOT-compatible |
| **WAL-based CDC** | Page-level data requires complex decode back to rows; cannot map page IDs to table names without catalog traversal; not practical for row-level events |
| **IObservable / Rx** | External dependency (`System.Reactive`), `OnNext` is synchronous on writer thread, not AOT-friendly; can be layered on top later as an extension |
| **Architectural overlay** (message broker, Redis pub/sub) | Adds external infrastructure dependency; out of scope but consumers can bridge from our channel-based events |

---

## Event Model

### Core Types (in `CSharpDB.Core`)

```csharp
public enum ChangeOperation { Insert, Update, Delete }

public sealed class ChangeEvent
{
    /// <summary>Table or collection name where the change occurred.</summary>
    public required string TableName { get; init; }

    /// <summary>Type of mutation.</summary>
    public required ChangeOperation Operation { get; init; }

    /// <summary>Physical row ID (B+tree key) of the affected row.</summary>
    public required long RowId { get; init; }

    /// <summary>Column definitions for interpreting OldValues/NewValues.</summary>
    public required IReadOnlyList<ColumnDefinition> Columns { get; init; }

    /// <summary>Previous row values. Null for INSERT.</summary>
    public DbValue[]? OldValues { get; init; }

    /// <summary>New row values. Null for DELETE.</summary>
    public DbValue[]? NewValues { get; init; }

    /// <summary>Monotonically increasing sequence number within this database instance.</summary>
    public long SequenceNumber { get; init; }
}
```

```csharp
public sealed class ChangeEventBatch
{
    /// <summary>All change events from a single committed transaction.</summary>
    public required IReadOnlyList<ChangeEvent> Events { get; init; }

    /// <summary>Monotonically increasing transaction sequence number.</summary>
    public required long TransactionSequence { get; init; }

    /// <summary>UTC timestamp when the transaction was committed.</summary>
    public required DateTimeOffset CommitTimestamp { get; init; }
}
```

```csharp
/// <summary>
/// Receives committed change events. Implemented by ChangeEventDispatcher.
/// Called on the writer thread after commit — implementations MUST NOT block.
/// </summary>
public interface IChangeEventSink
{
    void OnCommitted(ChangeEventBatch batch);
}
```

### Subscription Options

```csharp
public sealed class ChangeSubscriptionOptions
{
    /// <summary>Filter to specific table(s). Null = all tables.</summary>
    public string? TableName { get; init; }

    /// <summary>Filter to specific operation(s). Null = all operations.</summary>
    public ChangeOperation? Operation { get; init; }

    /// <summary>Include old/new row values. False = lightweight notifications only.</summary>
    public bool IncludeRowData { get; init; } = true;

    /// <summary>Channel capacity for this subscriber. Null = unbounded.</summary>
    public int? ChannelCapacity { get; init; } = 1000;
}
```

---

## Engine Integration Points

### 1. QueryPlanner — Buffer Changes Alongside Triggers

**File:** `src/CSharpDB.Execution/QueryPlanner.cs`

Add fields to `QueryPlanner`:
- `List<ChangeEvent> _pendingChanges` — accumulates events during a transaction
- `IChangeEventSink? _changeEventSink` — set by `Database` when events are enabled

Instrument at three points (all immediately after AFTER triggers fire):

| Operation | Location | Old Values | New Values | Notes |
|-----------|----------|-----------|-----------|-------|
| **INSERT** | After `FireTriggersAsync(AFTER INSERT)` (~line 5593) | `null` | Clone `row` | Row is the resolved insert values |
| **DELETE** | After `FireTriggersAsync(AFTER DELETE)` (~line 2137) | `row` (already cloned at line 2123) | `null` | |
| **UPDATE** | After `FireTriggersAsync(AFTER UPDATE)` (~line 2207) | `oldRow` (cloned at line 2168) | Clone `newRow` | Both old and new provided |

Add drain/discard methods:

```csharp
internal IReadOnlyList<ChangeEvent> DrainPendingChanges()
{
    if (_pendingChanges.Count == 0) return Array.Empty<ChangeEvent>();
    var result = _pendingChanges.ToArray();
    _pendingChanges.Clear();
    return result;
}

internal void DiscardPendingChanges() => _pendingChanges.Clear();
```

### 2. Database — Dispatch on Commit, Discard on Rollback

**File:** `src/CSharpDB.Engine/Database.cs`

**CommitWithCatalogSyncAsync** (line 496) — after successful commit:
```
1. Drain pending changes from _planner
2. await _catalog.PersistAllRootPageChangesAsync(ct)   // existing
3. await _pager.CommitAsync(ct)                        // existing
4. If changes exist and sink is set → _changeEventSink.OnCommitted(batch)
```

**RollbackAsync** (line 282) and error catch blocks (lines 224, 250):
- Call `_planner.DiscardPendingChanges()` before rollback

**New public API:**
```csharp
/// <summary>
/// Enable change event notifications. Returns the dispatcher for subscribing.
/// Call once after opening the database. Thread-safe.
/// </summary>
public ChangeEventDispatcher EnableChangeEvents(int? channelCapacity = 1000);
```

### 3. Collection — Instrument NoSQL Path

**File:** `src/CSharpDB.Engine/Collection.cs`

The `Collection<T>` class bypasses `QueryPlanner` entirely, operating directly on the B+tree. It needs separate instrumentation:

- Pass `IChangeEventSink` to `Collection<T>` constructor (from `Database`)
- In `PutAsync`: after successful `_tree.InsertAsync`, buffer a `ChangeEvent` with `Operation = Insert` (or `Update` if overwriting an existing key)
- In `DeleteAsync`: after successful `_tree.DeleteAsync`, buffer a `ChangeEvent` with `Operation = Delete`
- Changes are flushed/discarded via the same `AutoCommitAsync` → `CommitWithCatalogSyncAsync` path

---

## Dispatcher

### `ChangeEventDispatcher` (in `CSharpDB.Engine`)

Implements `IChangeEventSink` and `IAsyncDisposable`. Uses `System.Threading.Channels` for fan-out.

```csharp
public sealed class ChangeEventDispatcher : IChangeEventSink, IAsyncDisposable
{
    // Called on writer thread — must not block.
    // Iterates all subscriber channels and calls TryWrite (non-blocking).
    public void OnCommitted(ChangeEventBatch batch);

    // Create a subscription. Returns a ChannelReader the consumer reads from.
    public ChannelReader<ChangeEventBatch> Subscribe(ChangeSubscriptionOptions? options = null);

    // Clean up: completes all channels.
    public ValueTask DisposeAsync();
}
```

**Backpressure strategy:**
- Default: Bounded channel (capacity 1000) with `BoundedChannelFullMode.DropOldest`
- If a subscriber falls behind, oldest batches are silently dropped (at-most-once delivery)
- Consumers that need guaranteed delivery should use unbounded channels (`ChannelCapacity = null`) and ensure they process fast enough

**Filtering:**
- When `ChangeSubscriptionOptions` specifies `TableName` or `Operation`, the dispatcher runs a lightweight background task that reads from the raw channel and forwards only matching events to a filtered channel

**Thread safety:**
- Subscriber list protected by a lock (held briefly for `TryWrite` loop)
- `TryWrite` on `ChannelWriter<T>` is thread-safe and non-blocking (~20-50ns per subscriber)

---

## Consumer Usage

### Embedded (In-Process)

```csharp
await using var db = await Database.OpenAsync("mydb.db");
var dispatcher = db.EnableChangeEvents();

// Subscribe to all changes on the "orders" table
var reader = dispatcher.Subscribe(new ChangeSubscriptionOptions
{
    TableName = "orders"
});

// Consume in a background task
_ = Task.Run(async () =>
{
    await foreach (var batch in reader.ReadAllAsync())
    {
        foreach (var change in batch.Events)
        {
            Console.WriteLine($"{change.Operation} on {change.TableName}, " +
                              $"rowId={change.RowId}");

            if (change.NewValues is { } newVals)
                Console.WriteLine($"  New: [{string.Join(", ", newVals)}]");
            if (change.OldValues is { } oldVals)
                Console.WriteLine($"  Old: [{string.Join(", ", oldVals)}]");
        }
    }
});

// DML operations fire events after commit
await db.ExecuteAsync("INSERT INTO orders VALUES (1, 'widget', 9.99)");
await db.ExecuteAsync("UPDATE orders SET price = 12.99 WHERE id = 1");
await db.ExecuteAsync("DELETE FROM orders WHERE id = 1");
```

### Lightweight Notifications (No Row Data)

```csharp
var reader = dispatcher.Subscribe(new ChangeSubscriptionOptions
{
    IncludeRowData = false  // Only table name, operation, rowId — no values cloned
});
```

---

## Network Delivery

### Server-Sent Events (SSE)

**File:** `src/CSharpDB.Api/Endpoints/EventEndpoints.cs`

| Endpoint | Description |
|----------|-------------|
| `GET /api/events/stream` | Stream all committed changes |
| `GET /api/events/stream?table=orders` | Filter by table name |
| `GET /api/events/stream?table=orders&op=Insert` | Filter by table and operation |

Response format: `text/event-stream` with JSON-serialized `ChangeEventBatch` per SSE `data:` frame.

```
data: {"events":[{"tableName":"orders","operation":"Insert","rowId":1,...}],"transactionSequence":1,"commitTimestamp":"2026-03-08T12:00:00Z"}

data: {"events":[{"tableName":"orders","operation":"Update","rowId":1,...}],"transactionSequence":2,"commitTimestamp":"2026-03-08T12:00:01Z"}

```

### Client SDK

**File:** `src/CSharpDB.Client/ICSharpDbClient.cs`

```csharp
IAsyncEnumerable<ChangeEventBatch> StreamChangesAsync(
    string? tableName = null,
    ChangeOperation? operation = null,
    CancellationToken ct = default);
```

- **Direct transport**: reads from local `ChannelReader` (in-process)
- **HTTP transport** (future): connects to SSE endpoint

### Future: SignalR / gRPC Streaming

These can be added later as additional delivery mechanisms. A `BackgroundService` would bridge from the `ChannelReader` to SignalR groups or gRPC server streams, following the same fan-out pattern.

---

## Files Summary

### New Files

| File | Project | Purpose |
|------|---------|---------|
| `src/CSharpDB.Core/ChangeEvent.cs` | Core | `ChangeEvent`, `ChangeEventBatch`, `ChangeOperation` enum |
| `src/CSharpDB.Core/IChangeEventSink.cs` | Core | Sink interface for engine integration |
| `src/CSharpDB.Core/ChangeSubscriptionOptions.cs` | Core | Subscription filter options |
| `src/CSharpDB.Engine/ChangeEventDispatcher.cs` | Engine | Channel-based fan-out dispatcher |
| `src/CSharpDB.Api/Endpoints/EventEndpoints.cs` | Api | SSE streaming endpoint |
| `tests/CSharpDB.Tests/ChangeEventTests.cs` | Tests | Unit + integration tests |

### Modified Files

| File | Change |
|------|--------|
| `src/CSharpDB.Execution/QueryPlanner.cs` | Add pending changes buffer, instrument INSERT/UPDATE/DELETE after triggers |
| `src/CSharpDB.Engine/Database.cs` | Dispatch on commit, discard on rollback, `EnableChangeEvents()` API |
| `src/CSharpDB.Engine/Collection.cs` | Pass sink, instrument `PutAsync`/`DeleteAsync` |
| `src/CSharpDB.Client/ICSharpDbClient.cs` | Add `StreamChangesAsync` method |
| `src/CSharpDB.Client/CSharpDbClient.cs` | Delegate `StreamChangesAsync` to transport |
| `src/CSharpDB.Client/Internal/EngineTransportClient.cs` | Implement for Direct transport |
| `src/CSharpDB.Api/Program.cs` | Register event endpoints |

---

## Implementation Phases

### Phase 1: Core Infrastructure
1. Create `ChangeEvent`, `ChangeEventBatch`, `IChangeEventSink`, `ChangeSubscriptionOptions` in Core
2. Create `ChangeEventDispatcher` in Engine
3. Instrument `QueryPlanner` INSERT/UPDATE/DELETE paths (buffer after AFTER triggers)
4. Wire `Database.CommitWithCatalogSyncAsync` (flush) and rollback paths (discard)
5. Add `Database.EnableChangeEvents()` public API

### Phase 2: Collection API
1. Pass `IChangeEventSink` to `Collection<T>` constructor
2. Instrument `PutAsync`/`DeleteAsync` within `AutoCommitAsync`

### Phase 3: Client SDK Integration
1. Add `StreamChangesAsync` to `ICSharpDbClient`
2. Implement in `EngineTransportClient` (Direct transport)
3. Delegate in `CSharpDbClient`

### Phase 4: Network Delivery (SSE)
1. Add `EventEndpoints` to REST API
2. Register in `Program.cs`

### Phase 5: Tests
1. Unit + integration tests for all phases

---

## Event Ordering & Delivery Guarantees

| Property | Guarantee |
|----------|-----------|
| **Within a transaction** | Events ordered by execution order (same as trigger firing order) |
| **Across transactions** | Batches dispatched in commit order (single writer) |
| **Sequence numbers** | Monotonically increasing per database instance; not persisted across restarts |
| **Delivery** | At-most-once by default (bounded channel with `DropOldest`); configurable to best-effort-all with unbounded channels |
| **Trigger cascades** | Included — nested DML from triggers appends to the same buffer |

---

## Performance Considerations

| Aspect | Impact |
|--------|--------|
| **No subscribers** | Single null check per row (~0ns) |
| **Per-row overhead** | One `DbValue[].Clone()` for new/old values + `ChangeEvent` allocation |
| **Per-commit overhead** | `TryWrite` to each subscriber channel (~20-50ns each) |
| **Memory** | Events buffered for transaction duration; 10K-row batch = 10K `ChangeEvent` objects until commit |
| **Writer thread** | `OnCommitted` is non-blocking (all `TryWrite`); writer is never stalled by slow consumers |

---

## See Also

- [Roadmap](../roadmap.md) — Long-term: Replication / change feed
- [Architecture Guide](../architecture.md) — How the engine is structured
- [Storage Engine Guide](../storage/README.md) — Pager, WAL, B+tree internals
