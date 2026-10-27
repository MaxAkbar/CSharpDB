# Network experiment closeout

The gRPC micro-optimization phase is closed. The retained work is a reusable
client and session foundation; the recent isolated optimization candidates did
not establish the required performance improvement. The current gRPC transport
remains available, and persistent streaming remains experimental and opt-in.
Unary calls remain the default.

This checkpoint does not add a custom TCP transport or remote prepared-statement
handles. A native transport is a separate proposed experiment with its own
protocol, correctness checks, and performance decision.

## Foundation retained

| Area | Retained behavior | Boundary for a future transport |
| --- | --- | --- |
| Shared SDK and ADO.NET access | ADO.NET rents the same SDK client and physical pool for matching options. | Preserve the public client contract and one shared access path. |
| Pool and logical leases | Capacity limits, cancelable waiting, credential/route isolation, retirement, stale-rental rejection, transaction cleanup, and draining before return. | Pool and lease classes currently contain gRPC dependencies; extract their ownership rules when another transport needs them. |
| Command sessions | Version/capability negotiation, consecutive request IDs, one command in flight, SQL and transaction commands, and disconnect rollback. | Existing Protobuf payloads can be reused initially; stream I/O and server dispatch still depend on gRPC. |
| Results | Logical type mapping, private schema reuse, and compact scalar projection through the existing rental. | Keep type and ownership semantics independent of a future frame encoding. |
| Correctness tests | Pool isolation, transaction ownership, cancellation, disposal/draining, malformed responses, schema ownership, and no replay. | Reuse portable behavior tests and provide transport-specific fixtures. |
| Benchmark harness | Held-client and rent/query/return workloads, fixed schedules, raw timings, CPU/wire checks, and preserved failures. | A new transport requires its own frozen wire validation and comparison protocol. |

The main source boundaries are [the SDK rental API](../../src/CSharpDB.Client/CSharpDbClient.cs),
[pool](../../src/CSharpDB.Client/Internal/GrpcClientPool.cs),
[lease](../../src/CSharpDB.Client/Internal/GrpcClientLease.cs),
[client session](../../src/CSharpDB.Client/Internal/GrpcCommandSession.cs),
[server session](../../src/CSharpDB.Daemon/Grpc/CSharpDbRpcService.Session.cs),
[wire messages](../../src/CSharpDB.Client/Protos/csharpdb_rpc.proto), and
[ADO.NET integration](../../src/CSharpDB.Data/CSharpDbConnection.cs).

The engine/transaction ownership work belongs to the dependency review for this
foundation. Sharing a database owner must preserve private temporary-table
state, transaction bindings, cleanup, and runtime-diagnostics ownership. These
changes are not a transport speedup claim. Separate storage and benchmark
commits from the research branch remain outside this foundation checkpoint.

## Final retained evidence

These are historical studies of frozen external runtimes, not validation counts
for a newly rebuilt foundation checkout.

| Study | Scope | Outcome |
| --- | --- | --- |
| Streaming client allocation profile | Two fresh baseline processes, 64 blocks, 2,097,152 measured reads, and four traces. Both client and server used three GC heaps with dynamic adaptation disabled. | Diagnostic integrity passed with zero trace loss. Process-wide allocation counters include diagnostics/background activity; sampled type weights are approximate. No performance qualification or candidate installation. |
| Private `ExchangeAsync` state pooling | Eight fresh processes in B/C/C/B order per streaming/control stage, 200 blocks, and 5,373,952 measured requests. Default GC; no tracing or allocation probes. | Raw-data integrity passed; performance qualification failed. All 2,058 correctness cases passed across both variants: 434 daemon and 595 Data cases per variant, including nine common lifetime cases. The candidate remained isolated. |

The private-exchange candidate changed only its private async builder and
`Task<ExchangeReply>` return type to pooled `ValueTask<ExchangeReply>`. Its method
body and all three direct-await callers were unchanged. Both client assemblies
were rebuilt with the same toolchain; the other 387 runtime files were retained.

| Rent/query/return path | First mirrored comparison: throughput change | Second mirrored comparison: throughput change |
| --- | ---: | ---: |
| SDK | +0.01% | -1.11% |
| ADO.NET | -3.18% | +0.76% |

Both paths missed the predeclared 5% improvement requirement. Several tail and
control gates also failed; the original stability gates passed. Latency gates
used the median of fixed-round block p99 values and median block worst-worker
p99, rather than a single p99 pooled over every request. No capture was retried
or excluded to improve the decision. A postcapture audit-reader type error was
repaired without changing the frozen captures, gates, or raw arithmetic; the
failed audit and repair receipt were retained.

The recent qualifications also used an external, frozen `pooled-unary-guard`
helper and entrypoint adaptation. That mode is **not** implemented by this
checkout's [benchmark program](NetworkBenchmarkProgram.cs). Reproducing those
studies requires their retained source/runtime manifests and adapters; invoking
that flag on a newly built checkout does not reproduce them.

The earlier server-wrapper and other rejected micro-optimization candidates
retain their original evidence and decisions. They are not included in the
foundation merely because they passed correctness tests.

## Interpretation and next boundary

The measured workload was a warm, one-row read over cleartext localhost with
eight workers. Client and server were separate processes on the same Windows
host. It does not establish separate-host or TLS performance, write throughput,
maximum capacity, or a performance ceiling for CSharpDB. Initial CPU headroom
and an observational host monitor did not prove scheduling or thermal isolation.

Preserve the gRPC baseline and archived failures. A future TCP study should
first isolate replacing gRPC/HTTP/2 framing while retaining the query executor,
session semantics, and payload representation. Changing row encoding, prepared
queries, or multiplexing at the same time would require separate attribution.
Any new adoption threshold must be declared before its measurements and must
not alter the decisions of the closed studies.

## Foundation checkpoint validation

Actual-source validation on Windows x64 with SDK **10.0.403**, following the
repository's unchanged SDK policy:

- Solution restore and Release build passed with **zero warnings and errors**.
- The solution-wide Microsoft.Testing.Platform run covered all **27 test
  modules**, one module at a time. Final validated results are **8,985 passed,
  zero failed, and four skipped**.
- The four skips require optional trusted Access fixtures or live MySQL/SQL
  Server administrator connections; those environments were not configured.
- Core engine/storage tests passed all **3,521** cases, daemon tests all **434**,
  and Data tests all **595**. This includes nine imported public-task lifetime
  cases, four new Admin route-mode cases, and one new commit-preparation failure
  disposal case.
- The first complete run exposed one existing observability test race: a
  process-global listener count assertion overlapped two health events from
  another test. `OperationScopeTests` now uses the existing nonparallel telemetry
  collection. Its assertions are unchanged; rebuilding and rerunning the full
  **134-case** observability suite passed. The initial failed run, overlapping
  event evidence, and corrected run are retained in the closeout archive.
- Documentation, published-package dependency closure, and EF Core version
  consistency checks passed. Source and dependency review confirmed that the
  rejected private-exchange and server-wrapper candidates are absent.

The source checkpoint also fixes Admin route cloning to preserve streaming mode,
while keeping Unary as the default. The original experiment's working files,
index, and three storage/benchmark commits remain preserved separately.

This is Windows source/correctness validation. Cross-platform CI, NativeAOT
publishing, optional live-provider qualifications, and new network performance
qualification were not performed as part of this closeout. The historical
performance decisions above remain unchanged.

## External archive references

Raw evidence is intentionally outside this source tree. In the retained research
archive, resolve these directory names under `Documents/Codex/2026-10-26/`.
The directory date is the local work date; receipt timestamps are UTC.

| Archive directory | Final receipt | SHA-256 of receipt |
| --- | --- | --- |
| `csharpdb-streaming-client-allocations-20261026` | `report-receipt-seal.json` | `193F31A44D970C02A9AAE01B58B6E13AD5C6E7142CC1044AC3B2CFD6A2DD34FF` |
| `csharpdb-private-exchange-pooling-20261026` | `report-receipt-seal.json` | `EA2906AA7AC9CDEADD708AFC73B913C38C3FBD217F7B476093F9CECF41FD4CE9` |

The receipts index manifests, raw captures, exact binaries, source inventories,
test outputs, analyses, failures, and ownership/cleanup evidence. They are
external research records, not downloadable release artifacts or public
quick-start requirements.
