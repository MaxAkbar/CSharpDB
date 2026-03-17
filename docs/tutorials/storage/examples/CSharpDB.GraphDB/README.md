# CSharpDB.GraphDB

A **graph database** built on three CSharpDB.Storage B+trees.
Nodes and edges are stored in separate trees, with a **reverse-edge index** for efficient incoming-edge traversal. `BTreeCursor.SeekAsync` + `MoveNextAsync` range scans list all edges from/to a node in a single seek.

---

## Why Three B+trees?

A graph has two primitives: **nodes** and **edges**. To traverse edges in both directions efficiently, we need three trees:

| B+tree | Key | Value | Purpose |
|---|---|---|---|
| `_nodes` | `nodeId` (long) | `JSON(GraphNode)` | Stores all nodes |
| `_outEdges` | `(sourceId << 32) \| targetId` | `JSON(GraphEdge)` | Outgoing edges — "who does this node point to?" |
| `_inEdges` | `(targetId << 32) \| sourceId` | `JSON(GraphEdge)` | Incoming edges — "who points to this node?" |

**Key 0** in the `_nodes` tree is reserved as a **superblock** that stores the root page IDs of all three trees (12 bytes: 3 × uint32).

### The Edge Key Encoding Trick

By packing two 32-bit node IDs into a single 64-bit key, all edges from/to a node share the same **upper 32 bits**. This means:

```
Outgoing edge key:   sourceId (32 bits)  |  targetId (32 bits)
                     ├── same prefix ──┤

SeekAsync(sourceId << 32)     → first outgoing edge
MoveNextAsync                 → next outgoing edge
until (key >> 32) != sourceId → done, all edges listed
```

The incoming (reverse) tree uses the same encoding with swapped IDs, enabling identical cursor scans for incoming edges.

```
_outEdges B+tree                          _inEdges B+tree (reverse)

key = (src << 32) | tgt                  key = (tgt << 32) | src
┌──────────────────┐                     ┌──────────────────┐
│ (1<<32)|2  edge  │  Alice→Bob          │ (2<<32)|1  edge  │  Bob←Alice
│ (1<<32)|3  edge  │  Alice→Carol        │ (3<<32)|1  edge  │  Carol←Alice
│ (2<<32)|4  edge  │  Bob→Dave           │ (4<<32)|2  edge  │  Dave←Bob
│ (2<<32)|5  edge  │  Bob→Eve            │ (5<<32)|2  edge  │  Eve←Bob
│ ...              │                     │ ...              │
└──────────────────┘                     └──────────────────┘

SeekAsync(1 << 32) on _outEdges:         SeekAsync(2 << 32) on _inEdges:
→ Lists Alice's outgoing edges           → Lists Bob's incoming edges
```

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                        Program.cs                            │
│                  "dotnet run" → CLI REPL                     │
│                  "dotnet run serve" → Web server             │
└──────────┬──────────────────────────────┬────────────────────┘
           │                              │
    ┌──────▼──────┐              ┌────────▼────────┐
    │  CLI / REPL │              │  ASP.NET Minimal│
    │   Commands  │              │    API Host     │
    └──────┬──────┘              └────────┬────────┘
           │                              │
    ┌──────▼──────────────────────────────▼──────┐
    │           IGraphApi  (interface)            │
    │  ┌──────────────┐   ┌────────────────────┐ │
    │  │ InProcess     │   │ HttpGraphApi       │ │
    │  │ ApiClient     │   │ Client             │ │
    │  └──────┬───────┘   └────────┬───────────┘ │
    └─────────┼────────────────────┼─────────────┘
              │                    │
    ┌─────────▼────────────────────▼─────────────┐
    │            GraphApiService                  │
    │        (SemaphoreSlim gate, lazy init)      │
    └─────────────────┬──────────────────────────┘
                      │
    ┌─────────────────▼──────────────────────────┐
    │             GraphDatabase                   │
    │   AddNode / AddEdge / BFS / ShortestPath    │
    └─────────────────┬──────────────────────────┘
                      │
    ┌─────────────────▼──────────────────────────┐
    │              GraphStore                     │
    │   Three B+trees: _nodes, _outEdges, _inEdges│
    │   ScanOutgoing / ScanIncoming (cursor scans)│
    └─────────────────┬──────────────────────────┘
                      │
    ┌─────────────────▼──────────────────────────┐
    │          CSharpDB.Storage                   │
    │      BTree · BTreeCursor · Pager            │
    └────────────────────────────────────────────┘
```

### Project Structure

```
CSharpDB.GraphDB/
├── Core/
│   ├── GraphNode.cs              # Node data model (id, label, type, properties)
│   ├── GraphEdge.cs              # Edge data model (sourceId, targetId, label, weight)
│   ├── GraphKeyEncoding.cs       # Edge key packing: (id1 << 32) | id2
│   ├── GraphTraversalResult.cs   # BFS / shortest path results + statistics
│   ├── GraphStore.cs             # Three B+trees — CRUD + cursor range scans
│   └── GraphDatabase.cs          # High-level API (BFS, shortest path, edge scans)
├── Api/
│   ├── IGraphApi.cs              # Interface + request DTOs
│   ├── GraphApiService.cs        # Thread-safe service wrapper
│   └── Clients/
│       ├── InProcessGraphApiClient.cs
│       └── HttpGraphApiClient.cs
├── Hosting/
│   └── GraphWebHost.cs           # ASP.NET Core Minimal API endpoints
├── Infrastructure/
│   └── GraphDatabaseUtility.cs
├── Cli/
│   ├── AnsiConsoleWriter.cs      # ANSI-coloured terminal output
│   ├── GraphConsolePresenter.cs  # Tables, traversal results, ASCII path display
│   ├── GraphSampleRunner.cs      # Social network sample (people, cities, companies)
│   ├── Commands/
│   │   ├── IReplCommand.cs
│   │   ├── GraphCommand.cs
│   │   ├── SampleCommand.cs
│   │   └── PrefixedReplCommand.cs
│   └── Repl/
│       └── ReplHost.cs
├── wwwroot/
│   └── index.html                # Web dashboard (force-directed graph, traversal UI)
└── Program.cs                    # Entry point
```

---

## Core Cursor Showcase: `ScanEdgeRangeAsync`

This is the heart of the sample — a single cursor range scan lists all edges from/to a node:

```csharp
// GraphStore.cs — the B+tree cursor showcase
private static async Task<(List<GraphEdge> Edges, int Scanned)> ScanEdgeRangeAsync(
    BTree tree, long nodeId, string? labelFilter, int maxResults, CancellationToken ct)
{
    var results = new List<GraphEdge>();
    var scanned = 0;
    var startKey = GraphKeyEncoding.RangeStart(nodeId);  // nodeId << 32
    var endKey = GraphKeyEncoding.RangeEnd(nodeId);      // (nodeId << 32) | 0xFFFFFFFF

    var cursor = tree.CreateCursor();
    if (!await cursor.SeekAsync(startKey, ct))
        return (results, scanned);

    do
    {
        if (cursor.CurrentKey > endKey) break;   // past our node's edges
        scanned++;

        var edge = JsonSerializer.Deserialize<GraphEdge>(cursor.CurrentValue.Span);
        if (labelFilter is not null &&
            !string.Equals(edge.Label, labelFilter, StringComparison.OrdinalIgnoreCase))
            continue;

        results.Add(edge);
        if (results.Count >= maxResults) break;
    }
    while (await cursor.MoveNextAsync(ct));

    return (results, scanned);
}
```

The same method is called for **both** directions — the only difference is which tree is passed:

```csharp
// Outgoing: scan _outEdges where high bits = sourceId
public Task<...> ScanOutgoingEdgesAsync(long nodeId, ...)
    => ScanEdgeRangeAsync(_outEdges, nodeId, ...);

// Incoming: scan _inEdges where high bits = targetId
public Task<...> ScanIncomingEdgesAsync(long nodeId, ...)
    => ScanEdgeRangeAsync(_inEdges, nodeId, ...);
```

---

## Graph Traversal Algorithms

### BFS (Breadth-First Search)

```
TraverseBfsAsync(startNodeId, maxDepth, direction)
   │
   ├─ Queue start node at depth 0
   │
   └─ While queue is not empty:
       ├─ Dequeue (nodeId, depth)
       ├─ If depth >= maxDepth → skip
       ├─ ScanOutgoingEdgesAsync(nodeId) → cursor range scan
       │     SeekAsync(nodeId << 32) → MoveNextAsync loop
       └─ For each discovered edge:
           ├─ If target not visited → enqueue at depth+1
           └─ Record edge in results
```

Supports three directions:
- **Outgoing**: follow outgoing edges only
- **Incoming**: follow incoming edges only (uses reverse tree)
- **Both**: follow edges in both directions

### Shortest Path (BFS-based)

```
ShortestPathAsync(sourceId, targetId)
   │
   ├─ BFS from source with parent tracking
   ├─ Stop as soon as targetId is dequeued
   └─ Reconstruct path by following parent chain backwards
```

Returns the exact path (nodes + edges) and statistics about cursor operations.

### Traversal Statistics

| Metric | Meaning |
|---|---|
| `TotalNodes` | Nodes discovered in the traversal |
| `TotalEdges` | Edges traversed |
| `MaxDepth` | Deepest BFS level reached |
| `CursorSeeks` | Number of `SeekAsync` calls (one per node visited) |
| `EntriesScanned` | Total B+tree entries scanned across all seeks |

---

## Running the Sample

### Prerequisites

- .NET 10 SDK
- CSharpDB.Storage project (referenced in the solution)

### CLI Mode (Interactive REPL)

```bash
dotnet run --project docs/tutorials/storage/examples/CSharpDB.GraphDB/CSharpDB.GraphDB.csproj
```

```
graphdb> sample                              # Load social network + cities + companies
graphdb> nodes                               # List all nodes
graphdb> add-node "Alice" type:person        # Add a node
graphdb> add-edge 1 2 KNOWS weight:0.9       # Add a directed edge
graphdb> out 1                               # List outgoing edges from node 1
graphdb> out 1 label:KNOWS                   # Filter by edge label
graphdb> in 15                               # List incoming edges to node 15
graphdb> bfs 1 depth:2 dir:outgoing          # BFS from node 1
graphdb> bfs 1 depth:3 label:KNOWS           # BFS filtered to KNOWS edges
graphdb> path 1 7                            # Shortest path from 1 to 7
graphdb> get-node 1                          # Lookup a node
graphdb> get-edge 1 2                        # Lookup an edge
graphdb> delete-edge 1 2                     # Delete an edge
graphdb> delete-node 3                       # Delete a node + all its edges
graphdb> count                               # Count nodes and edges
graphdb> reset                               # Wipe database
graphdb> help                                # Show all commands
graphdb> exit                                # Quit
```

### Web Mode (REST API + Dashboard)

```bash
dotnet run --project docs/tutorials/storage/examples/CSharpDB.GraphDB/CSharpDB.GraphDB.csproj -- serve
```

Open **http://localhost:62501** for the interactive dashboard with:
- Force-directed graph visualization with node/edge rendering
- Color-coded nodes by type (person, city, company)
- Color-coded edges by label (KNOWS, LIVES_IN, WORKS_AT, ROAD_TO)
- Arrow heads showing edge direction
- Highlighted subgraphs for query results
- Edge scan, BFS traversal, and shortest path query forms
- Quick demo presets for common queries
- Traversal statistics (cursor seeks, entries scanned, max depth)
- Results table with path visualization

### REST API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/graph/nodes` | Add a node |
| `GET` | `/api/graph/nodes` | List all nodes |
| `GET` | `/api/graph/nodes/{nodeId}` | Get a node by ID |
| `DELETE` | `/api/graph/nodes/{nodeId}` | Delete a node + all edges |
| `GET` | `/api/graph/nodes/count` | Count nodes |
| `POST` | `/api/graph/edges` | Add a directed edge |
| `GET` | `/api/graph/edges?sourceId=…&targetId=…` | Get a specific edge |
| `DELETE` | `/api/graph/edges?sourceId=…&targetId=…` | Delete an edge |
| `GET` | `/api/graph/edges/outgoing/{nodeId}` | Scan outgoing edges (cursor range scan) |
| `GET` | `/api/graph/edges/incoming/{nodeId}` | Scan incoming edges (reverse cursor scan) |
| `GET` | `/api/graph/edges/count` | Count edges |
| `GET` | `/api/graph/traverse/bfs?startNodeId=…&maxDepth=…` | BFS traversal |
| `GET` | `/api/graph/traverse/shortest-path?sourceId=…&targetId=…` | Shortest path |
| `POST` | `/api/graph/reset` | Reset the database |

#### Add a node

```bash
curl -X POST http://localhost:62501/api/graph/nodes \
  -H "Content-Type: application/json" \
  -d '{"label":"Alice","type":"person"}'
```

#### Add an edge

```bash
curl -X POST http://localhost:62501/api/graph/edges \
  -H "Content-Type: application/json" \
  -d '{"sourceId":1,"targetId":2,"label":"KNOWS","weight":0.9}'
```

#### Scan outgoing edges

```bash
curl "http://localhost:62501/api/graph/edges/outgoing/1"
curl "http://localhost:62501/api/graph/edges/outgoing/1?label=KNOWS"
```

#### BFS traversal

```bash
curl "http://localhost:62501/api/graph/traverse/bfs?startNodeId=1&maxDepth=2&direction=outgoing&edgeLabel=KNOWS"
```

#### Shortest path

```bash
curl "http://localhost:62501/api/graph/traverse/shortest-path?sourceId=1&targetId=7&edgeLabel=KNOWS"
```

---

## Sample Graph

The `SampleCommand` loads a social network with 17 nodes and 34 edges:

### Nodes

| Type | Count | Examples |
|---|---|---|
| **Person** | 8 | Alice, Bob, Carol, Dave, Eve, Frank, Grace, Heidi |
| **City** | 6 | New York, London, Paris, Berlin, Tokyo, San Francisco |
| **Company** | 3 | Acme Corp, Globex, Initech |

### Edges

| Label | Count | Description |
|---|---|---|
| **KNOWS** | 11 | Social connections between people (weighted by strength) |
| **LIVES_IN** | 8 | Person → City residence |
| **WORKS_AT** | 8 | Person → Company employment |
| **ROAD_TO** | 7 | City → City routes (weighted by distance in km) |

### Sample Demos

1. **Outgoing edges from Alice** — lists KNOWS, LIVES_IN, WORKS_AT edges
2. **Incoming edges to Acme Corp** — shows who works there (reverse-edge scan)
3. **KNOWS-only from Alice** — label-filtered edge scan
4. **BFS from Alice (depth 2, KNOWS)** — discovers friends of friends
5. **Shortest path Alice → Grace** — finds the shortest KNOWS chain
6. **City routes from NYC** — BFS on ROAD_TO edges with weighted distances

---

## Key Takeaways

- **Three B+trees for a graph** — nodes, outgoing edges, incoming (reverse) edges
- **Edge key packing** — `(sourceId << 32) | targetId` groups all edges from a node
- **Reverse-edge tree** — same encoding with swapped IDs enables incoming-edge traversal
- **Cursor range scans list edges** — `SeekAsync(nodeId << 32)` + `MoveNextAsync` is the core pattern
- **BFS uses cursor scans** — each BFS level issues one `SeekAsync` per node visited
- **Statistics track efficiency** — cursor seeks and entries scanned show B+tree utilisation
- **32-bit node ID limit** — each node ID must fit in 32 bits (~4 billion nodes)
