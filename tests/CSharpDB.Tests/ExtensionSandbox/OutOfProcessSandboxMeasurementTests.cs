using System.Diagnostics;

namespace CSharpDB.Tests;

public sealed class OutOfProcessSandboxMeasurementTests
{
    [Fact]
    public async Task InvokeCommandAsync_CapturesColdWarmAndBatchedLatencyMeasurements()
    {
        var ct = TestContext.Current.CancellationToken;
        OutOfProcessCommandSandboxPrototype sandbox = await OutOfProcessCommandSandboxPrototype.CreateAsync(ct);

        SandboxInvocationResult cold = await sandbox.InvokeCommandAsync(
            "Echo",
            new Dictionary<string, object?> { ["message"] = "cold" },
            TimeSpan.FromSeconds(5),
            ct);

        await using OutOfProcessCommandSandboxSession session = sandbox.StartSession();
        var warmResults = new List<SandboxInvocationResult>();
        Stopwatch batchStopwatch = Stopwatch.StartNew();
        for (int i = 0; i < 8; i++)
        {
            SandboxInvocationResult warm = await session.InvokeCommandAsync(
                "Echo",
                new Dictionary<string, object?> { ["message"] = $"warm-{i}" },
                TimeSpan.FromSeconds(5),
                ct);

            warmResults.Add(warm);
        }

        batchStopwatch.Stop();
        SandboxLatencyMeasurements measurements = SandboxLatencyMeasurements.Create(
            cold,
            warmResults,
            batchStopwatch.Elapsed);

        Assert.True(cold.Succeeded);
        Assert.All(warmResults, static result => Assert.True(result.Succeeded));
        Assert.Equal(8, measurements.WarmInvocationCount);
        Assert.True(measurements.ColdInvocationElapsed > TimeSpan.Zero);
        Assert.True(measurements.WarmBatchElapsed > TimeSpan.Zero);
        Assert.True(measurements.WarmMedianInvocationElapsed > TimeSpan.Zero);
        Assert.True(measurements.WarmBatchElapsed < TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task InvokeCommandAsync_KillsWorkerWhenSoftWorkingSetLimitIsExceeded()
    {
        var ct = TestContext.Current.CancellationToken;
        OutOfProcessCommandSandboxPrototype sandbox = await OutOfProcessCommandSandboxPrototype.CreateAsync(ct);

        SandboxInvocationResult result = await sandbox.InvokeCommandAsync(
            "AllocateMemory",
            new Dictionary<string, object?>
            {
                ["megabytes"] = 256,
                ["holdMs"] = 5_000,
            },
            TimeSpan.FromSeconds(10),
            ct,
            new SandboxResourceLimits(
                MaxWorkingSetBytes: 128L * 1024 * 1024,
                PollInterval: TimeSpan.FromMilliseconds(10)));

        Assert.False(result.Succeeded);
        Assert.False(result.TimedOut);
        Assert.False(result.Crashed);
        Assert.True(result.ResourceLimitExceeded);
        Assert.Equal("ResourceLimitExceeded", result.ErrorCode);
        Assert.True(result.ObservedWorkingSetBytes > 128L * 1024 * 1024);
        Assert.True(result.Elapsed < TimeSpan.FromSeconds(5));
    }
}
