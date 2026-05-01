using System.Diagnostics;
using System.Text.Json;

namespace CSharpDB.Tests;

internal sealed class OutOfProcessCommandSandboxPrototype
{
    private static readonly SemaphoreSlim s_buildLock = new(1, 1);
    private readonly string _workerAssemblyPath;

    private OutOfProcessCommandSandboxPrototype(string workerAssemblyPath)
    {
        _workerAssemblyPath = workerAssemblyPath;
    }

    public static async Task<OutOfProcessCommandSandboxPrototype> CreateAsync(CancellationToken ct)
    {
        string workerAssemblyPath = GetWorkerAssemblyPath();
        if (!File.Exists(workerAssemblyPath))
            await BuildWorkerAsync(ct);

        return new OutOfProcessCommandSandboxPrototype(workerAssemblyPath);
    }

    public async Task<SandboxInvocationResult> InvokeCommandAsync(
        string name,
        IReadOnlyDictionary<string, object?>? arguments,
        TimeSpan timeout,
        CancellationToken ct,
        SandboxResourceLimits? resourceLimits = null)
    {
        await using OutOfProcessCommandSandboxSession session = StartSession();
        return await session.InvokeCommandAsync(name, arguments, timeout, ct, resourceLimits);
    }

    public OutOfProcessCommandSandboxSession StartSession()
        => new(_workerAssemblyPath);

    private static async Task BuildWorkerAsync(CancellationToken ct)
    {
        await s_buildLock.WaitAsync(ct);
        try
        {
            string workerAssemblyPath = GetWorkerAssemblyPath();
            if (File.Exists(workerAssemblyPath))
                return;

            var startInfo = new ProcessStartInfo
            {
                FileName = GetDotNetHostPath(),
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            };
            startInfo.ArgumentList.Add("build");
            startInfo.ArgumentList.Add(GetWorkerProjectPath());
            startInfo.ArgumentList.Add("--configuration");
            startInfo.ArgumentList.Add(GetConfiguration());
            startInfo.ArgumentList.Add("--nologo");

            using var process = new Process { StartInfo = startInfo };
            process.Start();
            await process.WaitForExitAsync(ct);
            if (process.ExitCode != 0)
            {
                string stdout = await process.StandardOutput.ReadToEndAsync(ct);
                string stderr = await process.StandardError.ReadToEndAsync(ct);
                throw new InvalidOperationException(
                    $"Failed to build extension sandbox worker. Exit code {process.ExitCode}.{Environment.NewLine}{stdout}{Environment.NewLine}{stderr}");
            }
        }
        finally
        {
            s_buildLock.Release();
        }
    }

    private static string GetWorkerAssemblyPath()
        => Path.Combine(
            GetRepositoryRoot(),
            "tests",
            "CSharpDB.ExtensionSandbox.Worker",
            "bin",
            GetConfiguration(),
            "net10.0",
            "CSharpDB.ExtensionSandbox.Worker.dll");

    private static string GetWorkerProjectPath()
        => Path.Combine(
            GetRepositoryRoot(),
            "tests",
            "CSharpDB.ExtensionSandbox.Worker",
            "CSharpDB.ExtensionSandbox.Worker.csproj");

    private static string GetRepositoryRoot()
    {
        DirectoryInfo? current = new(AppContext.BaseDirectory);
        while (current is not null)
        {
            if (File.Exists(Path.Combine(current.FullName, "CSharpDB.slnx")))
                return current.FullName;

            current = current.Parent;
        }

        throw new InvalidOperationException("Could not find CSharpDB repository root.");
    }

    private static string GetConfiguration()
        => AppContext.BaseDirectory.Contains(
            $"{Path.DirectorySeparatorChar}Release{Path.DirectorySeparatorChar}",
            StringComparison.OrdinalIgnoreCase)
                ? "Release"
                : "Debug";

    private static string GetDotNetHostPath()
        => Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet";
}

internal sealed class OutOfProcessCommandSandboxSession : IAsyncDisposable
{
    private readonly Process _process;
    private bool _disposed;

    public OutOfProcessCommandSandboxSession(string workerAssemblyPath)
    {
        var startInfo = new ProcessStartInfo
        {
            FileName = GetDotNetHostPath(),
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
        startInfo.ArgumentList.Add(workerAssemblyPath);

        _process = new Process { StartInfo = startInfo };
        Stopwatch stopwatch = Stopwatch.StartNew();
        _process.Start();
        StartupElapsed = stopwatch.Elapsed;
    }

    public TimeSpan StartupElapsed { get; }

    public async Task<SandboxInvocationResult> InvokeCommandAsync(
        string name,
        IReadOnlyDictionary<string, object?>? arguments,
        TimeSpan timeout,
        CancellationToken ct,
        SandboxResourceLimits? resourceLimits = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        if (timeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(timeout), timeout, "Timeout must be greater than zero.");

        var request = new SandboxCommandRequest(
            Kind: "command",
            Name: name,
            Arguments: arguments,
            Metadata: new Dictionary<string, string>
            {
                ["surface"] = "Phase9Prototype",
                ["correlationId"] = Guid.NewGuid().ToString("N"),
            });

        Stopwatch stopwatch = Stopwatch.StartNew();
        using var timeoutCts = new CancellationTokenSource(timeout);
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutCts.Token);
        using var watchdogCts = new CancellationTokenSource();
        Task<ResourceLimitViolation?>? resourceTask = StartResourceWatchdog(resourceLimits, watchdogCts.Token);

        try
        {
            string requestJson = JsonSerializer.Serialize(request, SandboxJson.Options);
            await _process.StandardInput.WriteLineAsync(requestJson.AsMemory(), linkedCts.Token);
            await _process.StandardInput.FlushAsync(linkedCts.Token);

            Task<string?> responseTask = _process.StandardOutput.ReadLineAsync(linkedCts.Token).AsTask();
            Task completedTask = resourceTask is null
                ? responseTask
                : await Task.WhenAny(responseTask, resourceTask);

            if (resourceTask is not null && completedTask == resourceTask)
            {
                ResourceLimitViolation? violation = await resourceTask;
                if (violation is not null)
                {
                    await KillWorkerAsync(_process);
                    return new SandboxInvocationResult(
                        Succeeded: false,
                        TimedOut: false,
                        Crashed: false,
                        ResourceLimitExceeded: true,
                        ExitCode: GetExitCodeOrNull(_process),
                        Elapsed: stopwatch.Elapsed,
                        Message: $"Worker exceeded the soft working set limit of {violation.LimitBytes} bytes.",
                        Value: null,
                        ErrorCode: "ResourceLimitExceeded",
                        ObservedWorkingSetBytes: violation.ObservedBytes);
                }
            }

            string? responseLine = await responseTask;
            if (responseLine is null)
            {
                await _process.WaitForExitAsync(CancellationToken.None);
                string stderr = await _process.StandardError.ReadToEndAsync();
                return new SandboxInvocationResult(
                    Succeeded: false,
                    TimedOut: false,
                    Crashed: _process.ExitCode != 0,
                    ResourceLimitExceeded: false,
                    ExitCode: _process.ExitCode,
                    Elapsed: stopwatch.Elapsed,
                    Message: stderr,
                    Value: null,
                    ErrorCode: _process.ExitCode == 0 ? "ProtocolError" : "WorkerCrash");
            }

            SandboxCommandResponse? response = JsonSerializer.Deserialize<SandboxCommandResponse>(
                responseLine,
                SandboxJson.Options);

            if (response is null)
            {
                return new SandboxInvocationResult(
                    Succeeded: false,
                    TimedOut: false,
                    Crashed: false,
                    ResourceLimitExceeded: false,
                    ExitCode: GetExitCodeOrNull(_process),
                    Elapsed: stopwatch.Elapsed,
                    Message: "Worker returned an empty response.",
                    Value: null,
                    ErrorCode: "ProtocolError");
            }

            return new SandboxInvocationResult(
                response.Succeeded,
                TimedOut: false,
                Crashed: false,
                ResourceLimitExceeded: false,
                ExitCode: GetExitCodeOrNull(_process),
                Elapsed: stopwatch.Elapsed,
                Message: response.Message,
                Value: response.Value,
                ErrorCode: response.ErrorCode);
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !ct.IsCancellationRequested)
        {
            await KillWorkerAsync(_process);
            return new SandboxInvocationResult(
                Succeeded: false,
                TimedOut: true,
                Crashed: false,
                ResourceLimitExceeded: false,
                ExitCode: GetExitCodeOrNull(_process),
                Elapsed: stopwatch.Elapsed,
                Message: $"Command '{name}' timed out after {timeout.TotalMilliseconds:0.###}ms.",
                Value: null,
                ErrorCode: "Timeout");
        }
        catch (OperationCanceledException)
        {
            await KillWorkerAsync(_process);
            throw;
        }
        catch (IOException ex) when (_process.HasExited)
        {
            return new SandboxInvocationResult(
                Succeeded: false,
                TimedOut: false,
                Crashed: _process.ExitCode != 0,
                ResourceLimitExceeded: false,
                ExitCode: _process.ExitCode,
                Elapsed: stopwatch.Elapsed,
                Message: ex.Message,
                Value: null,
                ErrorCode: _process.ExitCode == 0 ? "ProtocolError" : "WorkerCrash");
        }
        finally
        {
            await watchdogCts.CancelAsync();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        try
        {
            if (!_process.HasExited)
            {
                _process.StandardInput.Close();
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
                await _process.WaitForExitAsync(cts.Token);
            }
        }
        catch (OperationCanceledException)
        {
            await KillWorkerAsync(_process);
        }
        catch (InvalidOperationException)
        {
        }
        catch (IOException)
        {
        }
        finally
        {
            _process.Dispose();
        }
    }

    private Task<ResourceLimitViolation?>? StartResourceWatchdog(
        SandboxResourceLimits? resourceLimits,
        CancellationToken ct)
    {
        if (resourceLimits?.MaxWorkingSetBytes is not { } maxWorkingSetBytes)
            return null;

        TimeSpan pollInterval = resourceLimits.PollInterval ?? TimeSpan.FromMilliseconds(25);
        return WatchWorkingSetAsync(_process, maxWorkingSetBytes, pollInterval, ct);
    }

    private static async Task<ResourceLimitViolation?> WatchWorkingSetAsync(
        Process process,
        long maxWorkingSetBytes,
        TimeSpan pollInterval,
        CancellationToken ct)
    {
        long maxObservedBytes = 0;
        try
        {
            while (!ct.IsCancellationRequested && !process.HasExited)
            {
                process.Refresh();
                maxObservedBytes = Math.Max(maxObservedBytes, process.WorkingSet64);
                if (maxObservedBytes > maxWorkingSetBytes)
                    return new ResourceLimitViolation(maxObservedBytes, maxWorkingSetBytes);

                await Task.Delay(pollInterval, ct);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (InvalidOperationException)
        {
        }

        return null;
    }

    private static async Task KillWorkerAsync(Process process)
    {
        try
        {
            if (!process.HasExited)
                process.Kill(entireProcessTree: true);
        }
        catch (InvalidOperationException)
        {
        }

        try
        {
            if (!process.HasExited)
                await process.WaitForExitAsync(CancellationToken.None);
        }
        catch (InvalidOperationException)
        {
        }
    }

    private static int? GetExitCodeOrNull(Process process)
    {
        try
        {
            return process.HasExited ? process.ExitCode : null;
        }
        catch (InvalidOperationException)
        {
            return null;
        }
    }

    private static string GetDotNetHostPath()
        => Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet";
}

internal sealed record SandboxResourceLimits(
    long? MaxWorkingSetBytes = null,
    TimeSpan? PollInterval = null);

internal sealed record SandboxLatencyMeasurements(
    TimeSpan ColdInvocationElapsed,
    TimeSpan WarmBatchElapsed,
    TimeSpan WarmMedianInvocationElapsed,
    int WarmInvocationCount)
{
    public static SandboxLatencyMeasurements Create(
        SandboxInvocationResult coldInvocation,
        IReadOnlyList<SandboxInvocationResult> warmInvocations,
        TimeSpan warmBatchElapsed)
    {
        if (warmInvocations.Count == 0)
            throw new ArgumentException("At least one warm invocation is required.", nameof(warmInvocations));

        TimeSpan[] sorted = warmInvocations
            .Select(static invocation => invocation.Elapsed)
            .Order()
            .ToArray();

        return new SandboxLatencyMeasurements(
            coldInvocation.Elapsed,
            warmBatchElapsed,
            sorted[sorted.Length / 2],
            warmInvocations.Count);
    }
}

internal sealed record SandboxInvocationResult(
    bool Succeeded,
    bool TimedOut,
    bool Crashed,
    bool ResourceLimitExceeded,
    int? ExitCode,
    TimeSpan Elapsed,
    string? Message,
    JsonElement? Value,
    string? ErrorCode,
    long? ObservedWorkingSetBytes = null);

internal sealed record SandboxCommandRequest(
    string Kind,
    string Name,
    IReadOnlyDictionary<string, object?>? Arguments,
    IReadOnlyDictionary<string, string> Metadata);

internal sealed record SandboxCommandResponse(
    bool Succeeded,
    string? Message = null,
    JsonElement? Value = null,
    string? ErrorCode = null);

internal sealed record ResourceLimitViolation(
    long ObservedBytes,
    long LimitBytes);

internal static class SandboxJson
{
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);
}
