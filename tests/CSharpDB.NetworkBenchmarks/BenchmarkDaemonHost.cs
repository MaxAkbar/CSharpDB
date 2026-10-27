using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text.Json;
using CSharpDB.Client;
using CSharpDB.Testing;
using Microsoft.Extensions.DependencyInjection;

namespace CSharpDB.NetworkBenchmarks;

/// <summary>Owns exactly one real daemon, optionally in a separate child process.</summary>
internal sealed class BenchmarkDaemonHost : IAsyncDisposable
{
    private readonly GrpcSocketDaemonFixture? _fixture;
    private readonly GrpcSessionBenchmark.WireCounters? _counters;
    private readonly Process? _child;
    private readonly Task<string>? _stderr;
    public HostInfo Info { get; }
    public Uri CompatibilityEndpoint => new(Info.CompatibilityEndpoint);
    public Uri NativeEndpoint => new(Info.NativeEndpoint);
    public Uri EndpointFor(CSharpDbGrpcMode mode) => mode == CSharpDbGrpcMode.Native ? NativeEndpoint : CompatibilityEndpoint;

    private BenchmarkDaemonHost(GrpcSocketDaemonFixture fixture, GrpcSessionBenchmark.WireCounters counters)
    {
        _fixture = fixture; _counters = counters;
        Info = new(Environment.ProcessId, Environment.ProcessId, RuntimeInformation.FrameworkDescription,
            System.Runtime.GCSettings.IsServerGC, fixture.CompatibilityEndpoint.AbsoluteUri, fixture.NativeEndpoint.AbsoluteUri);
    }
    private BenchmarkDaemonHost(Process child, Task<string> stderr, HostInfo info)
    { _child = child; _stderr = stderr; Info = info; }

    public static async Task<BenchmarkDaemonHost> StartAsync(bool isolated, SessionDiagnosticProbe? probe, CancellationToken ct)
    {
        if (!isolated)
        {
            var counters = new GrpcSessionBenchmark.WireCounters();
            var fixture = new GrpcSocketDaemonFixture(configureServices: services =>
            {
                services.AddSingleton(counters);
                services.AddGrpc(options => options.Interceptors.Add<GrpcSessionBenchmark.WireCounters>());
                probe?.Configure(services);
            });
            return new(fixture, counters);
        }
        if (probe is not null) throw new InvalidOperationException("Isolated qualification must not install diagnostic probes.");
        var start = new ProcessStartInfo("dotnet")
        { RedirectStandardInput = true, RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false, CreateNoWindow = true };
        start.ArgumentList.Add(typeof(BenchmarkDaemonHost).Assembly.Location);
        start.ArgumentList.Add("--session-daemon");
        start.Environment["CSHARPDB_BENCHMARK_PARENT"] = Environment.ProcessId.ToString(System.Globalization.CultureInfo.InvariantCulture);
        // A launch-attached client trace must not suspend the separate server on the same diagnostic port.
        start.Environment.Remove("DOTNET_DiagnosticPorts");
        start.Environment.Remove("DOTNET_DefaultDiagnosticPortSuspend");
        var child = Process.Start(start) ?? throw new InvalidOperationException("Could not start owned benchmark daemon.");
        Task<string> stderr = child.StandardError.ReadToEndAsync();
        try
        {
            string? line = await child.StandardOutput.ReadLineAsync(ct).AsTask().WaitAsync(TimeSpan.FromSeconds(30), ct);
            HostInfo? info;
            try { info = line is null ? null : JsonSerializer.Deserialize<HostInfo>(line); }
            catch (JsonException error) { throw new InvalidOperationException($"Invalid child handshake: {line}", error); }
            if (info is null || info.ServerProcessId != child.Id || info.ParentProcessId != Environment.ProcessId)
                throw new InvalidOperationException($"Invalid child handshake: {line}");
            return new(child, stderr, info);
        }
        catch
        {
            if (!child.HasExited) child.Kill(entireProcessTree: true);
            await child.WaitForExitAsync(CancellationToken.None);
            string error = await stderr;
            child.Dispose();
            if (error.Length > 0) Console.Error.WriteLine(error);
            throw;
        }
    }
    public async Task<Snapshot> CaptureAsync(CancellationToken ct)
    {
        if (_fixture is not null) return Capture(_fixture, _counters!, false);
        await _child!.StandardInput.WriteLineAsync("snapshot".AsMemory(), ct);
        await _child.StandardInput.FlushAsync(ct);
        string? line = await _child.StandardOutput.ReadLineAsync(ct).AsTask().WaitAsync(TimeSpan.FromSeconds(10), ct);
        return line is null ? throw new IOException("Owned daemon closed its control pipe.")
            : JsonSerializer.Deserialize<Snapshot>(line) ?? throw new IOException("Invalid daemon snapshot.");
    }

    public async Task<TimingSnapshot> CaptureTimingAsync(CancellationToken ct)
    {
        if (_fixture is not null) return CaptureTiming(_fixture, _counters!);
        await _child!.StandardInput.WriteLineAsync("snapshot-timing".AsMemory(), ct);
        await _child.StandardInput.FlushAsync(ct);
        string? line = await _child.StandardOutput.ReadLineAsync(ct).AsTask().WaitAsync(TimeSpan.FromSeconds(10), ct);
        return line is null ? throw new IOException("Owned daemon closed its control pipe.")
            : JsonSerializer.Deserialize<TimingSnapshot>(line) ?? throw new IOException("Invalid daemon timing snapshot.");
    }

    public async Task<TimelineClock> CaptureTimelineClockAsync(int id, CancellationToken ct)
    {
        if (id is not (1 or 2)) throw new ArgumentOutOfRangeException(nameof(id));
        if (_fixture is not null) return CaptureTimelineClock(id);
        await _child!.StandardInput.WriteLineAsync($"timeline-clock:{id}".AsMemory(), ct);
        await _child.StandardInput.FlushAsync(ct);
        string? line = await _child.StandardOutput.ReadLineAsync(ct).AsTask().WaitAsync(TimeSpan.FromSeconds(10), ct);
        return line is null ? throw new IOException("Owned daemon closed its timeline clock pipe.")
            : JsonSerializer.Deserialize<TimelineClock>(line) ?? throw new IOException("Invalid daemon timeline clock.");
    }

    private static TimelineClock CaptureTimelineClock(int id)
    {
        long qpc = Stopwatch.GetTimestamp();
        TimelineEventSource.Log.Clock(id, qpc, Stopwatch.Frequency);
        return new(id, qpc, Stopwatch.Frequency);
    }

    private static TimingSnapshot CaptureTiming(GrpcSocketDaemonFixture fixture, GrpcSessionBenchmark.WireCounters counters)
    {
        using var process = Process.GetCurrentProcess();
        return new(fixture.Snapshot().Select(x => new HttpCount(x.Key.Protocol, x.Key.ContentType, x.Key.Method, x.Value)).ToArray(),
            counters.Snapshot(), process.TotalProcessorTime.TotalMilliseconds);
    }

    private static Snapshot Capture(GrpcSocketDaemonFixture fixture, GrpcSessionBenchmark.WireCounters counters, bool separate)
    {
        using var process = separate ? Process.GetCurrentProcess() : null;
        return new(fixture.Snapshot().Select(x => new HttpCount(x.Key.Protocol, x.Key.ContentType, x.Key.Method, x.Value)).ToArray(),
            counters.Snapshot(), separate ? GC.GetTotalAllocatedBytes(true) : 0, process?.TotalProcessorTime.TotalMilliseconds ?? 0);
    }
    public static async Task<int> RunChildAsync()
    {
        try
        {
            var counters = new GrpcSessionBenchmark.WireCounters();
            await using var fixture = new GrpcSocketDaemonFixture(configureServices: services =>
            {
                services.AddSingleton(counters);
                services.AddGrpc(options => options.Interceptors.Add<GrpcSessionBenchmark.WireCounters>());
            });
            int parent = int.Parse(Environment.GetEnvironmentVariable("CSHARPDB_BENCHMARK_PARENT")!);
            Console.WriteLine(JsonSerializer.Serialize(new HostInfo(parent, Environment.ProcessId, RuntimeInformation.FrameworkDescription,
                System.Runtime.GCSettings.IsServerGC, fixture.CompatibilityEndpoint.AbsoluteUri, fixture.NativeEndpoint.AbsoluteUri)));
            string? command;
            while ((command = await Console.In.ReadLineAsync()) is not null)
            {
                if (command == "stop") return 0;
                if (command == "snapshot-timing")
                {
                    Console.WriteLine(JsonSerializer.Serialize(CaptureTiming(fixture, counters)));
                    continue;
                }
                if (command.StartsWith("timeline-clock:", StringComparison.Ordinal))
                {
                    if (!int.TryParse(command.AsSpan("timeline-clock:".Length),
                        System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out int id)
                        || id is not (1 or 2)) throw new InvalidOperationException("Invalid timeline clock anchor.");
                    Console.WriteLine(JsonSerializer.Serialize(CaptureTimelineClock(id)));
                    continue;
                }
                if (command != "snapshot") throw new InvalidOperationException("Unknown benchmark control command.");
                Console.WriteLine(JsonSerializer.Serialize(Capture(fixture, counters, true)));
            }
            return 0;
        }
        catch (Exception error) { Console.Error.WriteLine(error); return 1; }
    }
    public async ValueTask DisposeAsync()
    {
        if (_fixture is not null) { await _fixture.DisposeAsync(); return; }
        try
        {
            if (!_child!.HasExited)
            {
                await _child.StandardInput.WriteLineAsync("stop");
                await _child.StandardInput.FlushAsync();
                await _child.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
            }
            if (_child.ExitCode != 0) throw new InvalidOperationException($"Owned daemon exited {_child.ExitCode}: {await _stderr!}");
        }
        finally
        {
            if (!_child!.HasExited) { _child.Kill(entireProcessTree: true); await _child.WaitForExitAsync(); }
            _child.Dispose();
        }
    }
    internal sealed record HostInfo(int ParentProcessId, int ServerProcessId, string Runtime, bool IsServerGC, string CompatibilityEndpoint, string NativeEndpoint);
    internal sealed record HttpCount(string Protocol, string ContentType, string Method, long Value);
    internal sealed record Snapshot(HttpCount[] Http, GrpcSessionBenchmark.Counts Wire, long ServerAllocatedBytes, double ServerCpuMs);
    internal sealed record TimingSnapshot(HttpCount[] Http, GrpcSessionBenchmark.Counts Wire, double ServerCpuMs);
    internal sealed record TimelineClock(int Id, long Qpc, long Frequency);
}
