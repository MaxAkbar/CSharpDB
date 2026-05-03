namespace CSharpDB.Tests;

public sealed class OutOfProcessSandboxPrototypeTests
{
    [Fact]
    public async Task InvokeCommandAsync_ReturnsJsonResponse()
    {
        var ct = TestContext.Current.CancellationToken;
        OutOfProcessCommandSandboxPrototype sandbox = await OutOfProcessCommandSandboxPrototype.CreateAsync(ct);

        SandboxInvocationResult result = await sandbox.InvokeCommandAsync(
            "Echo",
            new Dictionary<string, object?> { ["message"] = "approved" },
            TimeSpan.FromSeconds(5),
            ct);

        Assert.True(result.Succeeded);
        Assert.False(result.TimedOut);
        Assert.False(result.Crashed);
        Assert.False(result.ResourceLimitExceeded);
        Assert.Equal("Echo completed.", result.Message);
        Assert.Equal("approved", result.Value?.GetString());
        Assert.True(result.Elapsed > TimeSpan.Zero);
    }

    [Fact]
    public async Task InvokeCommandAsync_KillsWorkerOnTimeout()
    {
        var ct = TestContext.Current.CancellationToken;
        OutOfProcessCommandSandboxPrototype sandbox = await OutOfProcessCommandSandboxPrototype.CreateAsync(ct);

        SandboxInvocationResult result = await sandbox.InvokeCommandAsync(
            "Sleep",
            new Dictionary<string, object?> { ["delayMs"] = 5_000 },
            TimeSpan.FromMilliseconds(150),
            ct);

        Assert.False(result.Succeeded);
        Assert.True(result.TimedOut);
        Assert.False(result.Crashed);
        Assert.False(result.ResourceLimitExceeded);
        Assert.Equal("Timeout", result.ErrorCode);
        Assert.True(result.Elapsed < TimeSpan.FromSeconds(3));
    }

    [Fact]
    public async Task InvokeCommandAsync_KillsWorkerOnCancellation()
    {
        var ct = TestContext.Current.CancellationToken;
        OutOfProcessCommandSandboxPrototype sandbox = await OutOfProcessCommandSandboxPrototype.CreateAsync(ct);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromMilliseconds(150));

        await Assert.ThrowsAsync<OperationCanceledException>(
            async () => await sandbox.InvokeCommandAsync(
                "Sleep",
                new Dictionary<string, object?> { ["delayMs"] = 5_000 },
                TimeSpan.FromSeconds(5),
                cts.Token));
    }

    [Fact]
    public async Task InvokeCommandAsync_ReportsWorkerCrashWithoutCrashingHost()
    {
        var ct = TestContext.Current.CancellationToken;
        OutOfProcessCommandSandboxPrototype sandbox = await OutOfProcessCommandSandboxPrototype.CreateAsync(ct);

        SandboxInvocationResult result = await sandbox.InvokeCommandAsync(
            "Crash",
            arguments: null,
            TimeSpan.FromSeconds(5),
            ct);

        Assert.False(result.Succeeded);
        Assert.False(result.TimedOut);
        Assert.True(result.Crashed);
        Assert.False(result.ResourceLimitExceeded);
        Assert.Equal(42, result.ExitCode);
        Assert.Equal("WorkerCrash", result.ErrorCode);
    }
}
