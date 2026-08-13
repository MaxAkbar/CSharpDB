# What's New

## CSharpDB 4.6.0

CSharpDB 4.6.0 adds an end-to-end observability and diagnostics platform for
embedded databases, API and daemon hosts, sharded clients, and CSharpDB Admin.
The release introduces safe structured events, bounded runtime diagnostics,
OpenTelemetry traces and metrics, Prometheus export, health and readiness
signals, and a dedicated Admin workspace while keeping exporter dependencies
out of the database engine.

### Observability Contracts and Safe Defaults

- Added the BCL-only `CSharpDB.Observability` package and included it in the
  all-in-one `CSharpDB` package without adding OpenTelemetry, ASP.NET Core, or
  exporter dependencies to embedded applications.
- Added one validated `CSharpDbObservabilityOptions` configuration model for
  logging, bounded history, OpenTelemetry, OTLP, console export, Prometheus,
  health, resource identity, retention, and capacity limits.
- Added stable operation contexts, opaque correlation identifiers, schema and
  instrumentation versions, safe error projections, and explicit
  `Available`, `Disabled`, `Unsupported`, `Denied`, and `Unavailable` states.
- SQL text capture remains disabled by default. Ordinary snapshots, metrics,
  traces, and events exclude parameter and row values, credentials, connection
  strings, file paths, raw exceptions, and exception messages.
- Added tokenizer-based normalized SQL and versioned fingerprints. Raw SQL is
  an explicit sensitive-data opt-in and produces a startup warning.

### Structured Events and Query Lifecycle

- Added stable `ILogger` categories, event ids, names, message templates, and
  typed payloads for host, query, transaction, storage, maintenance, health,
  and API activity.
- Instrumented queries, scripts, procedures, triggers, pipelines, transactions,
  checkpoints, recovery, backup, restore, reindex, vacuum, and maintenance with
  exact request/statement ownership and once-only terminal outcomes.
- Streaming queries remain active until exhaustion, disposal, cancellation, or
  failure and report total duration, time to first result, queue time, rows,
  outcome, and safe error fields without leaving stale ambient correlation.
- Added independently configurable query, slow-query, and long-running-query
  events. Logging-provider and diagnostic-listener failures cannot change a
  database operation's result.

### Runtime Diagnostics

- Added the optional `ICSharpDbObservabilityClient` capability without changing
  `ICSharpDbClient`. Direct, ADO.NET, HTTP, gRPC, and sharded clients expose the
  same immutable diagnostics contracts.
- Added bounded active and recent query views, safe plan summaries, separately
  authorized query detail, connection and session state, transaction state,
  collection capacity, retention, dropped-record, and truncation metadata.
- Added aggregate and capped per-shard views that preserve each shard's safe
  alias, availability, server instance, and counter epoch without combining
  incompatible lifetimes.
- Diagnostics polling suppresses its own observation and never automatically
  executes `EXPLAIN ANALYZE`, replays SQL, terminates sessions, or fabricates
  zero values for unavailable producers.

### Storage, WAL, and Maintenance Visibility

- Added live database-size, page, cache, logical and physical I/O, WAL size and
  publication, commit-path, checkpoint, recovery, and durability snapshots.
- Added bounded active and recent maintenance views for backup, restore,
  checkpoint, reindex, vacuum, foreign-key migration, and generic maintenance,
  including phase, elapsed time, progress, outcome, warnings, and safe errors.
- Restore and exclusive maintenance now participate in readiness and retain
  accurate recovery/reopen state. Backup and validation remain available when
  they do not require exclusive ownership.
- Deep page, index, WAL, and storage inspection remains an explicit operator
  action rather than a normal telemetry hot path.

### OpenTelemetry and Prometheus

- Added the `CSharpDB` `ActivitySource` and `Meter` with versioned,
  low-cardinality span names, attributes, counters, histograms, gauges, units,
  and closed tag vocabularies.
- Added parent-aware tracing for direct, REST, gRPC, ADO.NET, and sharded work.
  Lazy query spans detach outside active work and finish exactly once.
- Added physical startup-recovery and automatic checkpoint spans with captured
  timing, explicit root semantics, and suppression of duplicate logical and
  physical checkpoint spans.
- Added API and daemon host integration for parent-based sampling, resource
  identity, console export, OTLP export, ASP.NET Core instrumentation, and
  explicit histogram views. Collector outages do not prevent host startup or
  database work.
- Added optional Prometheus export on the normal Kestrel listener. Metrics can
  run without tracing, use bounded labels, disable exemplars, support custom
  paths, and remain available when the daemon's ordinary REST API is disabled.

### Health, Readiness, and Liveness

- Added cached, bounded `/health/live` and `/health/ready` endpoints with
  minimal status-only JSON and independent configuration. Probes never open,
  query, checkpoint, or acquire the main database execution lock.
- Added startup, recovery, failure, restore, exclusive-maintenance, reopen, and
  shutdown readiness transitions plus a low-cardinality health gauge and
  transition event.
- Added standard daemon gRPC Health `Check` and `Watch` support for overall and
  database readiness, including operation when the daemon REST API is disabled.
- Health routes can be disabled as one unit. Exact gRPC health methods may be
  anonymous, while ordinary RPC, diagnostics, and Prometheus access retain
  their configured security requirements.

### Admin Observability Workspace

- Added an Observability workspace reachable from Admin navigation and the
  command palette in direct, HTTP, gRPC, and sharded modes.
- Added bounded overview sampling, health, query rate and latency, active and
  recent queries, sessions, storage/WAL, maintenance, and aggregate/per-shard
  panels with explicit stale, truncated, denied, unsupported, and unavailable
  states.
- Polling is serialized and active-tab-only. Samples reset across database,
  server-instance, counter-epoch, scope, or monotonic-counter changes.
- Query plans and sensitive query detail are loaded only on explicit request.
  Revealed text is cleared when the tab hides, the scope or database changes,
  or the workspace is disposed. Shell overlays and data-source identity are
  suppressed while the workspace is active.
- Improved tab keyboard behavior, deterministic ARIA relationships, responsive
  layouts, and status text for charts and partial diagnostics failures.

### Security and Access Policy

- Added authenticated REST and gRPC diagnostics operations with transport-neutral
  access-denied and capability-not-supported behavior.
- API-key mode protects diagnostics and Prometheus. Security mode `None` permits
  only the actual loopback peer; forwarded-address headers cannot grant access.
- Remote unauthenticated access requires an explicit insecure override and
  emits a warning. Sensitive query detail requires a separate host opt-in.
- Prometheus paths, diagnostics routes, health routes, OpenAPI, Scalar, REST,
  and gRPC reservations are validated for collisions before startup.

### Documentation, Samples, and Qualification Tooling

- Added a public observability guide covering embedded, API, daemon, Admin, and
  sharded deployments; configuration; event, trace, and metric schemas;
  security; retention; compatibility; overhead; and troubleshooting.
- Added a supported ASP.NET Core observability-host sample with `ILogger`,
  OpenTelemetry console output, optional OTLP, loopback Prometheus, liveness,
  readiness, and a small database workload.
- Added redaction and metric-schema goldens, cardinality and concurrency stress
  tests, transport parity tests, host publish and package-graph qualification,
  trim/source-generation smoke coverage, and a fail-closed performance
  attestation workflow.
- Corrected the durable SQL batching benchmark to use a `BIGINT` primary key so
  its full randomized key permutation remains valid under strict SQL integer
  semantics.

### Compatibility and Upgrade Notes

- Observability, OpenTelemetry, OTLP, console export, and Prometheus remain
  opt-in. Health endpoints are configured independently and are enabled by
  default in the supported API and daemon hosts.
- `History.Enabled` defaults to `true` for compatibility and can be disabled
  independently for metrics- or tracing-focused deployments.
- Runtime snapshots use schema `1.1` and continue to accept supported `1.0`
  payloads. Metric schema `1.0` and instrumentation version `1.0.0` define the
  initial stable telemetry vocabulary.
- Existing custom clients remain valid when they do not implement the optional
  diagnostics interface; callers receive an explicit unsupported capability.
- Ownerless path-only static restore validation, restore, reindex, vacuum, and
  foreign-key migration APIs do not have a runtime telemetry identity. Use
  database- or client-owned operations when correlation is required.
