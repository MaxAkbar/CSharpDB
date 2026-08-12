# CSharpDB Observability Host Sample

This supported ASP.NET Core sample shows one safe operational setup for a
direct CSharpDB database:

- CSharpDB typed events bridged through standard `ILogger` categories;
- OpenTelemetry traces and metrics with local console export;
- optional OTLP trace and metric export through standard environment variables;
- a Prometheus scrape restricted to the actual loopback peer;
- separate cached liveness and readiness routes; and
- a small `/work` endpoint that generates real query telemetry.

The sample uses a named in-memory database so running it does not leave a
database or WAL file behind. SQL capture is `None`, histories are bounded, and
no endpoint returns SQL, values, credentials, connection strings, file paths,
raw exceptions, or exception messages.

## Run

From the repository root:

```powershell
dotnet run --project samples/observability-host/ObservabilityHostSample.csproj
```

The sample listens at `http://localhost:5099`. Use `sample.http` or curl:

```text
curl http://localhost:5099/work
curl http://localhost:5099/health/live
curl http://localhost:5099/health/ready
curl http://localhost:5099/metrics
```

Calling `/work` runs `SELECT 42 AS answer`. The statement is intentionally not
included in logs or traces. The console shows the sample's `ILogger` messages,
the stable `CSharpDB.Query` event, and OpenTelemetry output. The Prometheus
scrape includes CSharpDB instruments after the workload executes.

## Enable OTLP

Console export is enabled and OTLP is disabled by default, so the sample runs
without a collector. To send traces and metrics to a collector, keep endpoints
and credentials out of `appsettings.json`:

```powershell
$env:CSharpDB__Observability__OpenTelemetry__Otlp__Enabled = "true"
$env:OTEL_EXPORTER_OTLP_ENDPOINT = "http://localhost:4317"
$env:OTEL_EXPORTER_OTLP_PROTOCOL = "grpc"
dotnet run --project samples/observability-host/ObservabilityHostSample.csproj
```

For a protected collector, set `OTEL_EXPORTER_OTLP_HEADERS` through a secret
provider and use `OTEL_EXPORTER_OTLP_TIMEOUT` when the deployment needs a
different timeout. An unavailable collector does not prevent the host or
database from starting, but export may be delayed or dropped. Monitor the
exporter/collector separately.

## Understand the wiring

`Program.cs` follows the same public host sequence as the standalone API:

1. `AddCSharpDbObservability(...)` binds and validates one options instance.
2. That exact instance is placed on the direct `DatabaseOptions`.
3. `AddCSharpDbHealth(...)` starts cached database readiness before probes are
   served.
4. `UseCSharpDbObservability(...)` starts the logger bridge and configured
   telemetry providers before database warmup.
5. `MapCSharpDbHealthEndpoints()` and `MapCSharpDbPrometheusEndpoint()` map the
   exact configured routes.

`CSharpDB.Observability` itself remains BCL-only. The ASP.NET Core adapter and
exporter dependencies live in `CSharpDB.Api`, at the application boundary.

## Production changes

The local sample uses security mode `None`, so Prometheus accepts only an
actual loopback peer. For a remote scrape, configure API-key mode and combine it
with TLS termination and private networking. Do not set
`Prometheus:AllowInsecureRemoteAccess=true` as a shortcut for those controls.

Keep `Logging:SqlText=None` unless a data-handling review explicitly approves
normalized or raw capture. Raw mode may expose literals and emits a startup
warning; sensitive query detail still requires separate host authorization.

See the public
[Observability and Diagnostics guide](https://csharpdb.com/docs/observability.html)
for the complete configuration, event, span, metric, runtime-diagnostics,
privacy, capacity, compatibility, and troubleshooting contracts.
