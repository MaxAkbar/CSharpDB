using CSharpDB.Api;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace CSharpDB.Daemon.Grpc;

internal sealed class CSharpDbDaemonReadinessHealthCheck(
    CSharpDbHostReadinessCoordinator readiness) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
        => Task.FromResult(
            readiness.IsReady
                ? HealthCheckResult.Healthy()
                : HealthCheckResult.Unhealthy());
}
