# CSharpDB Service Daemon — Roadmap & Design

A persistent background service that keeps CSharpDB loaded in memory, serves multiple clients concurrently, and persists changes to disk automatically. Runs on Windows, Linux, and macOS.

---

## Problem

Today, every client connection opens the database file from scratch — parsing the file header, loading the schema catalog, and building the page cache from cold. The existing `CSharpDbService` serializes all requests behind a single `SemaphoreSlim` lock, meaning only one query runs at a time. There is no way to keep the database warm between client sessions or serve concurrent reads.

## Goal

A long-running service process that:

1. **Opens the database once** at startup and keeps it loaded
2. **Serves concurrent reads** via snapshot-isolated reader sessions (already built in the engine)
3. **Persists writes immediately** through the WAL, with periodic checkpoint to the main `.db` file
4. **Manages memory intelligently** — only the working set (hot pages) lives in RAM, cold pages are read from disk on demand and evicted under memory pressure
5. **Runs cross-platform** — systemd on Linux, Windows Service on Windows, launchd on macOS
6. **Exposes multiple protocols** — HTTP/REST for broad compatibility, plus fast local transports (Unix sockets, named pipes)

---

## Design Decisions

- **Single project, multi-platform** — One `CSharpDB.Daemon` project using .NET Generic Host. Platform-specific service integration via NuGet packages (`Microsoft.Extensions.Hosting.WindowsServices`, `Microsoft.Extensions.Hosting.Systemd`).
- **Database-per-service instance** — Each daemon process manages one database file. Run multiple instances for multiple databases (different ports).
- **Reuse existing engine** — The WAL, page cache, checkpoint policies, reader sessions, and statement cache are production-ready. The daemon is an orchestration layer, not a rewrite.
- **Concurrent readers, single writer** — Matches the existing engine model. Reads scale horizontally via reader sessions; writes are serialized by the engine's writer lock.
- **Graceful degradation** — If the service crashes, WAL recovery replays uncommitted frames on next start. No data loss for committed transactions.

---

## Architecture

```
                    ┌─────────────────────────────────────────────────┐
                    │              CSharpDB.Daemon                    │
                    │                                                 │
  HTTP Client ────► │  ┌──────────────┐    ┌──────────────────────┐   │
  (any language)    │  │  Kestrel     │    │  DatabaseHost        │   │
                    │  │  REST API    │───►│                      │   │
  gRPC Client ────► │  │  + gRPC      │    │  ┌────────────────┐  │   │
  (high perf)       │  │  + WebSocket │    │  │ Database       │  │   │
                    │  └──────────────┘    │  │ (single inst.) │  │   │
  Unix Socket ────► │                      │  └───────┬────────┘  │   │
  (local fast)      │  ┌─────────────┐     │          │           │   │
                    │  │  Health     │     │  ┌───────▼────────┐  │   │
  Named Pipe ─────► │  │  Monitor    │     │  │ Reader Session │  │   │
  (Windows local)   │  └─────────────┘     │  │ Pool           │  │   │
                    │                      │  │ (concurrent    │  │   │
                    │  ┌──────────────┐    │  │  snapshots)    │  │   │
                    │  │  Checkpoint  │    │  └───────┬────────┘  │   │
                    │  │  Scheduler   │    │          │           │   │
                    │  └──────────────┘    │  ┌───────▼────────┐  │   │
                    │                      │  │ LRU Page Cache │  │   │
                    │  ┌──────────────┐    │  │ (hot pages     │  │   │
                    │  │  Metrics     │    │  │  in memory)    │  │   │
                    │  │  Collector   │    │  └───────┬────────┘  │   │
                    │  └──────────────┘    │          │           │   │
                    │                      │  ┌───────▼────────┐  │   │
                    │                      │  │ WAL + .db file │  │   │
                    │                      │  │ (durable)      │  │   │
                    │                      │  └────────────────┘  │   │
                    │                      └──────────────────────┘   │
                    └─────────────────────────────────────────────────┘
```

### Memory Model

```
┌───────────────────────────────────────────────────────┐
│                    Database File (e.g. 500 MB)        │
│  ┌───────────────────────────────────────────────┐    │
│  │  Page 1  │  Page 2  │  ...  │  Page 125,000   │    │
│  └───────────────────────────────────────────────┘    │
│       ▲           ▲                                   │
│       │ cached    │ cached                            │
│  ┌────┴───────────┴───────────────┐                   │
│  │    LRU Page Cache (e.g. 50 MB) │  ◄── hot pages    │
│  │    ~12,500 pages in memory     │      only         │
│  │    evicts cold pages on demand │                   │
│  └────────────────────────────────┘                   │
│                                                       │
│  Pages NOT in cache are read from disk when needed    │
│  and cached for subsequent access.                    │
└───────────────────────────────────────────────────────┘
```

### Write Path

```
Client: INSERT INTO users VALUES (...)
   │
   ▼
1. Engine acquires writer lock (single writer)
2. Statement cache hit? Use cached plan : parse SQL
3. Execute plan → modify B+tree pages in memory
4. Write dirty pages to WAL (.wal file) + fsync
5. Release writer lock → client gets response
   │
   ▼  (async, periodic)
6. Checkpoint scheduler triggers
7. If no active readers: copy WAL frames → main .db file
8. Truncate WAL
```

### Read Path (Concurrent)

```
Client A: SELECT * FROM users WHERE id = 42
Client B: SELECT COUNT(*) FROM orders
Client C: SELECT * FROM products WHERE price > 10
   │            │            │
   ▼            ▼            ▼
   Reader       Reader       Reader
   Session 1    Session 2    Session 3
   (snapshot)   (snapshot)   (snapshot)
   │            │            │
   ▼            ▼            ▼
   LRU Page Cache (shared, read-only snapshots)
   │
   ▼
   Disk (for pages not in cache)
```

---

## Implementation Phases

### Phase 1: Core Daemon — DatabaseHost + Kestrel REST

**Goal:** A single-binary service that keeps the database open and serves the existing REST API endpoints concurrently.

**New project:** `src/CSharpDB.Daemon/`

#### 1A. DatabaseHost (IHostedService)

Manages the `Database` lifecycle as a .NET hosted service.

**File:** `src/CSharpDB.Daemon/DatabaseHost.cs`

Responsibilities:
- `StartAsync()`: Open database with configured `PagerOptions`, warm the page cache by loading the schema catalog
- `StopAsync()`: Checkpoint, flush WAL, dispose database gracefully
- Expose the `Database` instance to the DI container as a singleton
- Configure `LruPageCache` with `MaxCachedPages` from settings
- Configure checkpoint policy (combine time interval + frame count via `AnyCheckpointPolicy`)

```csharp
// Pseudo-code — lifecycle management
public class DatabaseHost : IHostedService, IAsyncDisposable
{
    private Database? _database;

    public async Task StartAsync(CancellationToken ct)
    {
        _database = await Database.OpenAsync(_options.DatabasePath, new PagerOptions
        {
            MaxCachedPages = _options.MaxCachedPages,      // e.g. 12500 (~50 MB)
            CheckpointPolicy = new AnyCheckpointPolicy(
                new FrameCountCheckpointPolicy(1000),
                new TimeIntervalCheckpointPolicy(TimeSpan.FromSeconds(30))
            ),
            WriterLockTimeout = TimeSpan.FromSeconds(30),
        });
    }

    public async Task StopAsync(CancellationToken ct)
    {
        if (_database != null)
            await _database.DisposeAsync();   // checkpoints + closes
    }
}
```

Configuration model:

```json
{
  "CSharpDB": {
    "DatabasePath": "./data/myapp.db",
    "MaxCachedPages": 12500,
    "CheckpointIntervalSeconds": 30,
    "CheckpointFrameThreshold": 1000,
    "WriterLockTimeoutSeconds": 30,
    "ListenUrl": "http://0.0.0.0:5820"
  }
}
```

#### 1B. Concurrent Request Handler

Replace the single-lock `CSharpDbService` with a handler that uses reader sessions for SELECTs and the writer path for mutations.

**File:** `src/CSharpDB.Daemon/ConcurrentDbService.cs`

Routing logic:
- `SELECT` statements → `Database.CreateReaderSession()` → execute → dispose session
- `INSERT/UPDATE/DELETE/DDL` → `Database.ExecuteAsync()` (writer lock handled by engine)
- Each HTTP request gets its own reader session — no global lock for reads

```csharp
// Pseudo-code — concurrent read handling
public async Task<QueryResult> ExecuteAsync(string sql)
{
    if (IsReadOnly(sql))
    {
        using var reader = _database.CreateReaderSession();
        return await reader.ExecuteReadAsync(sql);
    }
    else
    {
        return await _database.ExecuteAsync(sql);
    }
}
```

#### 1C. Wire Up Existing REST Endpoints

Reuse the existing endpoint extension methods (`MapTableEndpoints`, `MapRowEndpoints`, etc.) from `CSharpDB.Api`, pointing them at `ConcurrentDbService` instead of the old `CSharpDbService`.

#### 1D. Cross-Platform Service Registration

Add NuGet references for platform-specific hosting:

```xml
<PackageReference Include="Microsoft.Extensions.Hosting.WindowsServices" />
<PackageReference Include="Microsoft.Extensions.Hosting.Systemd" />
```

Host builder:

```csharp
var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddWindowsService();        // Windows Service support
builder.Services.AddSystemd();               // systemd support (Linux)
// macOS: launchd launches a normal process — no special package needed
```

**Deliverables:**
- [ ] `CSharpDB.Daemon` project with `DatabaseHost`
- [ ] `ConcurrentDbService` replacing single-lock pattern
- [ ] REST API endpoints wired to concurrent service
- [ ] `appsettings.json` with all configuration options
- [ ] Build as self-contained binary for win-x64, linux-x64, osx-arm64

---

### Phase 2: Platform Service Installation

**Goal:** Install and manage the daemon as a native OS service.

#### 2A. systemd (Linux / Ubuntu)

**File:** `deploy/linux/csharpdb.service`

```ini
[Unit]
Description=CSharpDB Database Service
After=network.target

[Service]
Type=notify
ExecStart=/opt/csharpdb/csharpdb-daemon --database /var/lib/csharpdb/data.db
Restart=on-failure
RestartSec=5
WorkingDirectory=/opt/csharpdb
User=csharpdb
Group=csharpdb
LimitNOFILE=65536

# Graceful shutdown — gives time for checkpoint
TimeoutStopSec=30
KillSignal=SIGTERM

[Install]
WantedBy=multi-user.target
```

Usage:
```bash
sudo systemctl enable csharpdb
sudo systemctl start csharpdb
sudo systemctl status csharpdb
journalctl -u csharpdb -f          # live logs
```

#### 2B. Windows Service

No additional files needed — `AddWindowsService()` handles registration. Install via `sc.exe`:

```powershell
sc.exe create CSharpDB binPath="C:\Program Files\CSharpDB\csharpdb-daemon.exe"
sc.exe start CSharpDB
sc.exe query CSharpDB
```

Or via PowerShell:
```powershell
New-Service -Name "CSharpDB" -BinaryPathName "C:\Program Files\CSharpDB\csharpdb-daemon.exe" -StartupType Automatic
Start-Service CSharpDB
```

#### 2C. launchd (macOS)

**File:** `deploy/macos/com.csharpdb.daemon.plist`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.csharpdb.daemon</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/local/bin/csharpdb-daemon</string>
        <string>--database</string>
        <string>/usr/local/var/csharpdb/data.db</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/usr/local/var/log/csharpdb/stdout.log</string>
    <key>StandardErrorPath</key>
    <string>/usr/local/var/log/csharpdb/stderr.log</string>
</dict>
</plist>
```

Usage:
```bash
sudo cp deploy/macos/com.csharpdb.daemon.plist /Library/LaunchDaemons/
sudo launchctl load /Library/LaunchDaemons/com.csharpdb.daemon.plist
sudo launchctl list | grep csharpdb
```

**Deliverables:**
- [ ] `deploy/linux/csharpdb.service` — systemd unit file
- [ ] `deploy/macos/com.csharpdb.daemon.plist` — launchd plist
- [ ] `deploy/windows/install.ps1` — PowerShell install script
- [ ] Installation guide in docs

---

### Phase 3: Local Transport — Unix Sockets & Named Pipes

**Goal:** Add fast local-only transports for same-machine clients (Python, Node.js, etc.).

#### 3A. Unix Domain Socket (Linux / macOS)

Kestrel natively supports Unix sockets:

```csharp
builder.WebHost.ConfigureKestrel(options =>
{
    // TCP for remote clients
    options.ListenAnyIP(5820);

    // Unix socket for local clients (Linux / macOS)
    if (!OperatingSystem.IsWindows())
    {
        options.ListenUnixSocket("/var/run/csharpdb/csharpdb.sock");
    }
});
```

Python client connects via:
```python
import urllib3
http = urllib3.HTTPConnectionPool("localhost", scheme="http+unix", host="/var/run/csharpdb/csharpdb.sock")
response = http.request("POST", "/api/sql", body='{"sql": "SELECT 1"}')
```

Node.js client connects via:
```javascript
const response = await fetch("http://unix:/var/run/csharpdb/csharpdb.sock:/api/sql", {
  method: "POST",
  body: JSON.stringify({ sql: "SELECT 1" }),
});
```

#### 3B. Named Pipes (Windows)

Kestrel also supports named pipes:

```csharp
if (OperatingSystem.IsWindows())
{
    options.ListenNamedPipe("csharpdb");
}
```

.NET client connects via:
```csharp
var handler = new SocketsHttpHandler { ConnectCallback = NamedPipeConnectAsync };
var client = new HttpClient(handler) { BaseAddress = new Uri("http://csharpdb.pipe") };
```

#### 3C. Auto-Discovery

Clients should auto-detect the fastest available transport:

```
1. Check for Unix socket / named pipe → use if available (fastest)
2. Check for localhost TCP → use if available
3. Fall back to remote TCP
```

**Deliverables:**
- [ ] Unix socket listener in Kestrel configuration
- [ ] Named pipe listener for Windows
- [ ] Python client helper for socket connection
- [ ] Node.js client helper for socket connection
- [ ] Auto-discovery logic in client libraries

---

### Phase 4: Operational Features

**Goal:** Health monitoring, metrics, and admin controls for production use.

#### 4A. Health Check Endpoint

```
GET /health

{
  "status": "healthy",
  "uptime": "2d 14h 32m",
  "database": {
    "path": "/var/lib/csharpdb/data.db",
    "sizeBytes": 524288000,
    "walSizeBytes": 1048576,
    "cachedPages": 8432,
    "maxCachedPages": 12500,
    "activeReaders": 3,
    "totalQueries": 1458923,
    "totalWrites": 23456
  }
}
```

#### 4B. Metrics Endpoint (Prometheus-Compatible)

```
GET /metrics

# HELP csharpdb_queries_total Total queries executed
# TYPE csharpdb_queries_total counter
csharpdb_queries_total{type="read"} 1458923
csharpdb_queries_total{type="write"} 23456

# HELP csharpdb_cache_hit_ratio Page cache hit ratio
# TYPE csharpdb_cache_hit_ratio gauge
csharpdb_cache_hit_ratio 0.94

# HELP csharpdb_active_readers Current active reader sessions
# TYPE csharpdb_active_readers gauge
csharpdb_active_readers 3

# HELP csharpdb_wal_frames_pending WAL frames awaiting checkpoint
# TYPE csharpdb_wal_frames_pending gauge
csharpdb_wal_frames_pending 42

# HELP csharpdb_checkpoint_duration_seconds Time spent on last checkpoint
# TYPE csharpdb_checkpoint_duration_seconds gauge
csharpdb_checkpoint_duration_seconds 0.12
```

#### 4C. Admin Control Endpoints

```
POST /admin/checkpoint          # Force immediate checkpoint
POST /admin/compact             # Checkpoint + VACUUM
POST /admin/cache/clear         # Evict all cached pages
GET  /admin/cache/stats         # Cache hit/miss/eviction counters
GET  /admin/wal/status          # WAL frame count, size, last checkpoint time
POST /admin/shutdown            # Graceful shutdown (checkpoint + close)
```

#### 4D. Structured Logging

Use `ILogger` with structured output for observability:

```
[2026-03-08 14:23:01.123 INF] Database opened path=/var/lib/csharpdb/data.db pages=125000 cache_capacity=12500
[2026-03-08 14:23:05.456 INF] Checkpoint completed frames=847 duration_ms=120 wal_truncated=true
[2026-03-08 14:25:12.789 WRN] Writer lock contention waited_ms=1200 sql="INSERT INTO..."
[2026-03-08 14:30:00.000 INF] Health check uptime=6m58s cached_pages=4231 active_readers=2
```

**Deliverables:**
- [ ] `/health` endpoint with database stats
- [ ] `/metrics` Prometheus-compatible endpoint
- [ ] `/admin/*` control endpoints (authenticated)
- [ ] Structured logging throughout daemon lifecycle

---

### Phase 5: Client SDK Updates

**Goal:** Update the Python, Node.js, and TypeScript clients to connect to the daemon via HTTP (or local socket) instead of loading the native library directly.

#### 5A. Python Client (HTTP Mode)

```python
from csharpdb import CSharpDB

# Option 1: Direct FFI (current — loads native library)
db = CSharpDB(lib_path="./CSharpDB.Native.dll")
db.open("mydata.db")

# Option 2: HTTP client (new — connects to daemon)
db = CSharpDB.connect("http://localhost:5820")
# or auto-discover local socket:
db = CSharpDB.connect()  # finds /var/run/csharpdb/csharpdb.sock or localhost:5820
```

Same `db.query()`, `db.execute()`, `db.transaction()` API — the transport is transparent.

#### 5B. Node.js Client (HTTP Mode)

```javascript
import { Database } from 'csharpdb';

// Option 1: Direct FFI (current)
const db = new Database('mydata.db');

// Option 2: HTTP client (new)
const db = Database.connect('http://localhost:5820');
// or auto-discover:
const db = Database.connect();
```

#### 5C. .NET Client SDK

A thin HTTP client package for .NET apps that want to connect to a remote daemon:

```csharp
// Instead of opening the file directly:
await using var db = await CSharpDbClient.ConnectAsync("http://localhost:5820");
var result = await db.ExecuteAsync("SELECT * FROM users");
```

**Deliverables:**
- [ ] Python `CSharpDB.connect()` HTTP/socket client mode
- [ ] Node.js `Database.connect()` HTTP/socket client mode
- [ ] `CSharpDB.Client` NuGet package for .NET HTTP client
- [ ] Connection string parsing (`csharpdb://localhost:5820/mydb`)

---

### Phase 6: Advanced Features (Future)

#### 6A. WebSocket Streaming

Long-lived connections with server-push for:
- Live query subscriptions (`SUBSCRIBE SELECT * FROM orders WHERE status = 'new'`)
- Change notifications (table X was modified)
- Streaming large result sets without buffering

#### 6B. Connection Authentication

- API key authentication for remote connections
- mTLS for encrypted transport
- Role-based access control (read-only vs read-write)

#### 6C. Multi-Database Support

Single daemon instance managing multiple database files:

```
GET  /databases                           # List managed databases
POST /databases  { "name": "orders" }     # Create/open new database
POST /databases/orders/sql                # Execute against specific DB
```

#### 6D. Hot Backup

Non-blocking backup while the service is running:

```
POST /admin/backup?dest=/backups/mydb-2026-03-08.db
```

Uses WAL snapshot to create a consistent copy without stopping writes.

#### 6E. Replication (Read Replicas)

Stream committed WAL frames to read-only replicas:

```
Primary ──WAL stream──► Replica 1 (read-only)
                    ──► Replica 2 (read-only)
```

---

## Configuration Reference

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `DatabasePath` | string | `./csharpdb.db` | Path to the database file |
| `ListenUrl` | string | `http://0.0.0.0:5820` | HTTP listen address |
| `UnixSocketPath` | string | `/var/run/csharpdb/csharpdb.sock` | Unix socket path (Linux/macOS) |
| `NamedPipeName` | string | `csharpdb` | Named pipe name (Windows) |
| `MaxCachedPages` | int | `12500` | Max pages in LRU cache (~50 MB at 4 KB/page) |
| `CheckpointIntervalSeconds` | int | `30` | Time-based checkpoint interval |
| `CheckpointFrameThreshold` | int | `1000` | Frame-count checkpoint trigger |
| `WriterLockTimeoutSeconds` | int | `30` | Max wait for write lock |
| `MaxConcurrentReaders` | int | `64` | Reader session pool size |
| `EnableMetrics` | bool | `true` | Expose /metrics endpoint |
| `LogLevel` | string | `Information` | Minimum log level |

---

## File Layout

```
src/CSharpDB.Daemon/
  CSharpDB.Daemon.csproj
  Program.cs                    # Host builder, Kestrel config, DI setup
  DatabaseHost.cs               # IHostedService — database lifecycle
  ConcurrentDbService.cs        # Reader session routing (read vs write)
  Configuration/
    DaemonOptions.cs            # Strongly-typed settings
  Health/
    HealthCheckEndpoints.cs     # /health, /metrics
  Admin/
    AdminEndpoints.cs           # /admin/* control endpoints
  appsettings.json              # Default configuration

deploy/
  linux/
    csharpdb.service            # systemd unit file
    install.sh                  # Linux install script
  macos/
    com.csharpdb.daemon.plist   # launchd plist
    install.sh                  # macOS install script
  windows/
    install.ps1                 # Windows Service install script
  docker/
    Dockerfile                  # Container image
    docker-compose.yml          # Compose example
```

---

## Priority Order

| Phase | Effort | Impact | Priority |
|-------|--------|--------|----------|
| **Phase 1**: Core Daemon | Medium | High — unlocks persistent service model | **P0** |
| **Phase 2**: Platform Installation | Small | High — makes it usable in production | **P0** |
| **Phase 3**: Local Transports | Small | Medium — improves local client perf | **P1** |
| **Phase 4**: Operational Features | Medium | Medium — production monitoring | **P1** |
| **Phase 5**: Client SDK Updates | Medium | Medium — seamless client experience | **P2** |
| **Phase 6**: Advanced Features | Large | Variable — future capabilities | **P3** |

---

## See Also

- [Architecture Guide](../architecture.md) — How the engine is structured
- [Deployment & Installation Plan](../deployment/README.md) — Cross-platform distribution
- [Storage Engine Guide](../storage/README.md) — WAL, page cache, checkpoint internals
- [Native FFI Tutorials](../tutorials/native-ffi/README.md) — Python and Node.js client examples
- [NativeAOT Library](../../src/CSharpDB.Native/README.md) — C-compatible shared library for FFI clients
