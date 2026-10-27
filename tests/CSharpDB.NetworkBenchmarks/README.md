# Network socket experiments

The gRPC micro-optimization phase is closed. See the
[closeout and evidence index](CLOSEOUT.md) for the retained client/session
foundation, rejected candidates, validation boundary, and external archive
receipts. Persistent streaming remains experimental and opt-in; unary calls
remain the default.

This directory contains distinct historical workloads. The initial pilot below
does not describe the topology or pooling behavior of every later mode.

| Mode | Workload documentation |
| --- | --- |
| `native` (initial pilot) | [Original socket pilot](#original-socket-pilot) |
| `pooling` | [Remote gRPC pooling pilot](#remote-grpc-pooling-pilot) |
| `shared` | [Shared SDK and ADO.NET comparison](#shared-sdk-and-adonet-comparison) |
| `session` | [Persistent command session experiment](#persistent-command-session-experiment) |
| `session-isolated` | [Separate client and daemon processes](#separate-client-and-daemon-processes) |
| `session-c8` | [Eight concurrent pooled sessions](#eight-concurrent-pooled-sessions) |
| `pooled-lifecycle-c8` | [Complete pooled request lifecycle](#complete-pooled-request-lifecycle) |
| `pooled-allocation-profile` | [Warm pooled allocation profile](#warm-pooled-allocation-profile) |

The final paired qualifications additionally used an external frozen
`pooled-unary-guard` adapter. That mode is not a command implemented by this
checkout; reproduction requires the archived manifests and helper described in
the closeout.

## Original socket pilot

This bounded pilot compares the existing plain-HTTP gRPC-Web/HTTP/1.1 client
with opt-in native gRPC/HTTP/2. Both listeners run inside the actual daemon,
share one warm database, and execute identical unary SQL requests. The
server records the observed HTTP version, content type, method, and count;
each measured block must match its expected wire protocol and request counts.

The workloads are one-row point reads through a reused SDK client, a reused
ADO.NET connection, and ADO.NET open/read/close cycles. The last workload
includes the current `GetInfo` validation request on every opening. It does
not implement or simulate connection pooling, prepared handles, batching,
or a custom TCP protocol.

Build once, then run the frozen executable from the repository root:

```powershell
dotnet build tests/CSharpDB.NetworkBenchmarks/CSharpDB.NetworkBenchmarks.csproj -c Release
dotnet tests/CSharpDB.NetworkBenchmarks/bin/Release/net10.0/CSharpDB.NetworkBenchmarks.dll --output C:/benchmarks/network-pilot-new --iterations 2000 --open-iterations 300 --rounds 4 --settle-seconds 10
```

Choose a fresh output directory outside the repository for every run. The
harness refuses an existing directory. Protocol version 2 settles each mode
for ten seconds before measuring each workload, then warms each individual
block with 100 operations, or 25 for open/read/close. Version 1 used only the
per-block warmup and showed substantial throughput drift; its evidence must
remain separate from version 2.
Four rounds alternate Auto/Native then Native/Auto, with identical counts and
no retries. The entire run has a three-minute cancellation deadline.

Output includes the protocol frozen before measurement, source and binary
hashes, the working diff and new source files, every latency sample, per-block
CPU/allocation totals, server request counts, and medians across blocks. Raw
samples are written outside the timed request loop. A failed block retains
its completed samples; failures are recorded and the run stops. Timing results
have no speed-based promotion gate in this initial pilot, and all blocks must
be reported. A separate stability screen requires maximum/minimum block
throughput to differ by at most 25% for every scenario/mode. A failed screen
leaves the result descriptive and does not trigger a retry.

These are cleartext loopback measurements with one outstanding operation and
client and server in the same process using server GC. Process allocation and
CPU metrics include both sides. They are not separate-machine, TLS, maximum
throughput, or release qualification measurements. Keep generated evidence
outside the repository and compare all rounds before drawing conclusions.

## Remote gRPC pooling pilot

Protocol version 3 compares three arms on the existing Auto
gRPC-Web/HTTP/1.1 path: unpooled open/read/close (U), pooled open/read/close (P),
and an already-open connection (R). The pooled arm uses `Max Pool Size=1` and
performs its first physical `GetInfo` check before settling. Subsequent pooled
opens must send zero `GetInfo` RPCs; unpooled opens must send one each. All
arms use the same one-row query, host, and fixed iteration count.

```powershell
dotnet tests/CSharpDB.NetworkBenchmarks/bin/Release/net10.0/CSharpDB.NetworkBenchmarks.dll --experiment pooling --output C:/benchmarks/pooling-pilot-new --iterations 1000 --rounds 6 --settle-seconds 10
```

Each arm settles for ten seconds before the six rounds. Round orders are
UPR, PRU, RUP, RPU, PUR, URP, balancing every arm's position. Each block warms
with 25 open/read/close operations or 100 already-open reads, followed by
1,000 measured operations. The pooling pilot uses `--iterations` for all
arms; `--open-iterations` applies only to the native comparison.

The predeclared pilot gate requires all three arms to pass the 25% stability
screen, at least 25% higher median throughput for pooling than unpooled,
and pooled median block p99 no worse than unpooled. Failed gates cause no
retry. Preserve and report every block, including failures. Pooling is
opt-in; this pilot does not qualify concurrency, TLS, separate-host latency,
or remote prepared statements. The same evidence and three-minute deadline
rules apply.

## Shared SDK and ADO.NET comparison

Protocol version 5 measures both APIs in one process, with six lifetimes:
SDK reused, ADO reused, SDK create/read/dispose, ADO open/read/close, SDK
rent/read/dispose, and ADO pooled open/read/close. Each runs over both Auto
gRPC-Web/HTTP/1.1 and Native gRPC/HTTP/2, giving 12 cases. Pooled cases use
the same SDK-owned pool with `Max Pool Size=1` for each endpoint and mode.

```powershell
dotnet tests/CSharpDB.NetworkBenchmarks/bin/Release/net10.0/CSharpDB.NetworkBenchmarks.dll --experiment shared --output C:/benchmarks/shared-pool-new --iterations 1000 --open-iterations 100 --rounds 12 --settle-seconds 10
```

Reused/pooled cases settle for ten seconds; fresh-client cases settle with
100 operations. Twelve measured rounds follow a Williams
design: base case indices `0,1,11,2,10,3,9,4,8,5,7,6`, shifted by round modulo
12. Each case occupies every position exactly once; immediate predecessors
are balanced. Reused/pooled blocks measure 1,000 identical one-row reads;
fresh blocks measure 100. Each block follows 100 reused-client warmups or 25
client-lifecycle warmups, for 144 blocks and 100,800 samples. The whole run
has a five-minute cancellation deadline. Fresh cases therefore have fewer
latency samples; median block p99 is a pilot estimate.

Version 4 used unlimited ten-second settling of fresh clients and exhausted
Windows socket addresses (10048) before completing any measured block.
Keep its failed evidence. Version 5 caps fresh settling and measured counts,
using approximately 6,400 fresh connections plus reference-client setup,
below the observed Windows dynamic range of 16,384 ports. Do not change OS
network settings or retry failed blocks.

SDK `Create` makes no validation call; ADO fresh opens make one `GetInfo`
per operation. Shared rentals perform one physical validation per endpoint
and mode before settling, then zero validation RPCs during measured blocks.
All protocol and request counts are checked against the actual daemon.

The predeclared gate requires all 12 cases to pass the 25% stability screen.
For both APIs and both transports, pooling must improve median throughput by
at least 25% over fresh clients and have median block p99 no worse than fresh
clients. Failures cause no retry; retain and report all evidence. Reused
clients remain reference cases rather than pooling speedup claims. Earlier
pilot versions stay separate; do not compare their absolute numbers as a
causal improvement.

## Persistent command session experiment

```powershell
dotnet tests/CSharpDB.NetworkBenchmarks/bin/Release/net10.0/CSharpDB.NetworkBenchmarks.dll --experiment session --output C:/benchmarks/session-new
```

This fixed C1 experiment compares pooled, reused SDK and ADO.NET calls over
gRPC-Web unary, native HTTP/2 unary, and a persistent native gRPC stream.
All six arms share the actual daemon and warm one-row query; SDK and ADO.NET
use the same pool per wire mode with capacity one. Rental/open and warmup
occur outside timing. The stream allows one outstanding command.

Each arm settles for three seconds. Six Williams rounds use base indices
`0,1,5,2,4,3`, shifted by round modulo six, so each arm occupies each position
once. Every block has 256 warmups and 8,192 measured operations: 36 blocks
and 294,912 raw samples. Counts, order, gates, and the five-minute deadline
are fixed. There are no retries or exclusions.

The harness validates every scalar, the HTTP request shape, and decoded
stream command/response counts. Timed unary blocks must have exactly 8,192
SQL RPCs and no validation RPCs; stream blocks must have exactly 8,192 SQL
frames and responses and zero new HTTP RPCs. The extra test interceptor
counts unary calls and stream messages. Source snapshots, source/binary
hashes, raw QPC ticks, CPU/allocations, wire counts, and failures are saved
outside the repository. Source/binary changes invalidate the run.

For each API, both baselines and the stream must pass the 25% max/min
throughput stability screen. Streaming must beat each baseline's median
throughput by at least 5%, win at least five of six paired rounds against
each, and have median block p99 no worse than each. Failed gates leave
descriptive results. This qualifies only one outstanding cleartext loopback
read at a time; TLS, remote hosts, concurrent sessions, writes, batching,
and prepared handles require separate experiments.

The session protocol manifest version 1 stopped during ADO.NET settling,
before any timed block, because its scalar check incorrectly required
`Int64` for an `INTEGER` value exposed by ADO.NET as `Int32`. Version 2 accepts
either integer representation of 42. Counts, order, and gates are unchanged;
retain version 1 evidence alongside the corrected harness run.

### Runtime diagnosis

Use `--experiment session-diagnostic` with a fresh output directory to add
client/server stage timings and a dedicated-thread sampler of ThreadPool
dispatch delay. The `CSharpDB-Network-Diagnostic` EventSource emits QPC clock
and block markers for joining an external EventPipe GC/threading trace.
This mode adds overhead and never qualifies transport speed. It keeps the
same fixed SQL, arm order, operation counts, wire checks, and deadline.
`*.stages.csv` records every command's client start/end, server arrival,
backend execution, and response-write boundaries; `threadpool-dispatch.csv`
retains queued/completed timestamps and runtime counters. A stream write may
finish after its client has received the response, so its end is not treated
as an additive component of client latency.

### Separate client and daemon processes

```powershell
dotnet tests/CSharpDB.NetworkBenchmarks/bin/Release/net10.0/CSharpDB.NetworkBenchmarks.dll --experiment session-isolated --output C:/benchmarks/session-isolated-new
```

This is a separate deployment experiment: the parent runs SDK/ADO.NET and
one owned child hosts the actual daemon through the same socket fixture.
The two processes use separate heaps and ThreadPools. It installs no stage
probes or profiler. All six arms use the same child/database and the existing
shared client pool. Counts, order, warmups, settling, gates, and deadline
match the original C1 experiment. This remains cleartext loopback on one
machine; it does not qualify TLS, separate hosts, concurrent sessions, or
writes, and does not replace a failed same-process result.

The child sends endpoint/runtime/process identity metadata and snapshots of
HTTP and decoded command counts over redirected control pipes. Control IPC
is outside timing; it carries no SQL. CPU and allocation totals include both
processes and bounded snapshot overhead. The parent requests graceful stop
and waits for child exit; shutdown failure kills only that owned child tree.
Client latency samples survive a failed counter snapshot. `host-info.json`
records the separate process IDs and server runtime.

### Eight concurrent pooled sessions

```powershell
dotnet tests/CSharpDB.NetworkBenchmarks/bin/Release/net10.0/CSharpDB.NetworkBenchmarks.dll --experiment session-c8 --output C:/benchmarks/session-c8-new
```

This fixed C8 experiment runs eight workers against the same separate-process
daemon. Each worker holds its own SDK rental or ADO.NET connection and command;
each pool family has capacity eight. SDK and ADO.NET reuse the same physical
owners for matching wire options. All six arms remain gRPC-Web unary, native
unary, and native streaming through both APIs. Ordinary SQL queues on the
daemon backend's existing client lock, so this measures transport concurrency
and server queueing together.

Every arm settles for three seconds. Each block warms 256 reads per worker,
then all workers signal readiness before one asynchronous start barrier.
Each worker measures 4,096 reads: 32,768 per block, 36 balanced blocks, and
1,179,648 total. The six-round Williams ordering and five-minute deadline are
fixed. Each scalar must equal 42. There are no retries or excluded rounds.
On a worker failure, siblings are canceled and all workers drain before
snapshot/disposal; each worker's completed samples and error are retained.

Raw CSV records worker/sample identity and absolute QPC start/end timestamps.
Aggregate throughput uses elapsed wall time from barrier release through all
workers finishing. Latency quantiles pool all samples; the maximum of the
eight worker p99 values is reported separately. Readiness/completion times,
CPU/allocations, HTTP and decoded command counts, and source/binary hashes
are retained. Every timed block requires exactly 32,768 SQL commands and
responses with no validation/handshake RPCs. Before and after the run, the
daemon must show exactly eight physical owners per wire-mode pool, with no
additional owners when ADO.NET follows SDK.

For each API, all three arms must pass the 25% max/min throughput stability
screen. Streaming must beat each baseline's median throughput by at least
5%, win at least five of six paired rounds, and have both median pooled p99
and median worst-worker p99 no worse than each baseline. Both comparisons
must pass to qualify an API. Failed gates remain descriptive. This does not
qualify 32 workers, TLS, separate hosts, writes, or transaction throughput.

Use `--experiment session-c8-timing` for the same fixed C8 workload and gates
with timing and CPU accounting only. This mode never requests or reports
process allocation counters. It records finite, nonnegative, monotonic CPU
snapshots and preserves raw evidence if validation fails. Historical modes
remain available for reproduction; their allocation fields require separate
validation and must not be treated as exact memory savings.

### Complete pooled request lifecycle

```powershell
dotnet tests/CSharpDB.NetworkBenchmarks/bin/Release/net10.0/CSharpDB.NetworkBenchmarks.dll --experiment pooled-lifecycle-c8 --output C:/benchmarks/pooled-lifecycle-c8-new
```

This diagnostic compares a held rental with a complete short-lived pooled
request through both the SDK and ADO.NET. It explicitly selects native HTTP/2
streaming and a shared pool of eight physical sessions. Each lifecycle sample
includes renting or opening, one validated scalar read, and awaited disposal
or close; held rentals and their commands are created outside measurement.
SDK options and the ADO connection string are reused between requests.

Eight workers each complete 4,096 requests per block. Four arms run in eight
balanced rounds: 32 blocks and 1,048,576 measured reads. Each block warms 256
requests per worker, after an initial three-second settling period per arm.
Prewarming holds all eight sessions concurrently and checks that switching
from SDK to ADO.NET creates no additional physical session. Timed blocks must
contain only the expected SQL frames and responses, with no new HTTP calls,
validation, or handshake frames.

Client and daemon run in separate processes at Normal priority. The mode
collects raw per-worker QPC samples and timing/CPU snapshots, without allocation
counters, a profiler, startup hooks, or priority changes. It preserves partial
evidence and stops on failure, with no retries or excluded blocks. The fixed
throughput spread screen is 25% for each arm. Ratios of lifecycle to held
throughput describe this workload; they do not qualify a product optimization
or a comparison with historical benchmark results. The workload is warm
cleartext loopback and does not establish TLS, remote-host, or write performance.

### Warm pooled allocation profile

`--experiment pooled-allocation-profile --output <new directory>` isolates
warm checkout and return without executing SQL or creating commands. Eight
workers run 32,768 cycles each in four fixed blocks: SDK, ADO.NET, ADO.NET,
SDK. Both APIs share eight prewarmed native HTTP/2 streaming owners. SDK
options and the exact ADO.NET connection-string instance are reused. Each
worker waits behind an asynchronous release barrier before its loop.

Boundary counters report process-wide managed allocation, including client
harness and background work. Optional client EventPipe allocation ticks
provide sampled stack attribution; their weights are not exact per-type
bytes or object counts. The separate daemon must remain at Normal priority,
and every measured block must create zero RPC frames or HTTP calls. This
mode permits launch-attached EventPipe profiling and always records
`QualificationEligible=false`. It cannot qualify throughput improvements.
