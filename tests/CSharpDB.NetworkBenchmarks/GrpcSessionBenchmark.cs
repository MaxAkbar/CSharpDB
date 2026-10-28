using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text.Json;
using CSharpDB.Client;
using CSharpDB.Client.Grpc;
using CSharpDB.Data;
using CSharpDB.Testing;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.DependencyInjection;

namespace CSharpDB.NetworkBenchmarks;

/// <summary>A fixed, evidence-preserving C1 experiment; never retries a failed block.</summary>
internal static class GrpcSessionBenchmark
{
    private const int Iterations = 8192, Rounds = 6, Warmup = 256;
    private const string Query = "SELECT value FROM network_read WHERE id = 1";
    private static readonly JsonSerializerOptions Json = new() { WriteIndented = true };
    private static readonly Arm[] Arms = [new("SDK", "Web"), new("SDK", "Native"), new("SDK", "Stream"),
        new("ADO", "Web"), new("ADO", "Native"), new("ADO", "Stream")];

    public static async Task<int> RunAsync(string[] args, bool diagnostics = false, bool isolated = false)
    {
        string? output = null;
        bool ownsOutput = false;
        try
        {
            if (args.Length != 4 || Array.IndexOf(args, "--output") is not (0 or 2) ||
                Array.IndexOf(args, "--experiment") is not (0 or 2))
                throw new ArgumentException("Use --experiment session --output <new directory>; counts and gates are fixed.");
            output = Path.GetFullPath(args[Array.IndexOf(args, "--output") + 1]);
            string root = Git("rev-parse", "--show-toplevel");
            if (output.Equals(root, StringComparison.OrdinalIgnoreCase) || output.StartsWith(root + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase))
                throw new ArgumentException("Keep generated evidence outside the repository.");
            if (Directory.Exists(output)) throw new ArgumentException("The evidence directory must be new.");
            Directory.CreateDirectory(output);
            ownsOutput = true;
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(5));
            await RunCoreAsync(root, output, diagnostics, isolated, timeout.Token);
            return 0;
        }
        catch (Exception error)
        {
            Console.Error.WriteLine(error);
            if (ownsOutput && output is not null)
            {
                File.AppendAllText(Path.Combine(output, "failures.jsonl"), JsonSerializer.Serialize(new { Utc = DateTimeOffset.UtcNow, Error = error.ToString() }) + Environment.NewLine);
                Write(output, "status.json", new { State = "failed", FinishedUtc = DateTimeOffset.UtcNow });
            }
            return 1;
        }
    }

    private static async Task RunCoreAsync(string root, string output, bool diagnostics, bool isolated, CancellationToken ct)
    {
        var sources = SourceHashes(root);
        var binaries = BinaryHashes();
        string status = Git("status", "--porcelain=v1");
        Write(output, "protocol.json", new
        {
            Version = isolated ? 4 : diagnostics ? 3 : 2, Experiment = isolated ? "persistent-command-session-isolated-C1" : diagnostics ? "persistent-command-session-diagnostic" : "persistent-command-session-C1", Iterations, Rounds, Warmup,
            DiagnosticScope = diagnostics ? "DIAGNOSTIC ONLY: extra stage timing, dedicated-thread ThreadPool dispatch sampling, EventPipe GC/threading trace. No performance qualification or promotion. Original failed qualification retained unchanged. No thread/GC/OS tuning." : null,
            SeparateProcesses = isolated,
            HarnessRevision = "Version 1 stopped in ADO settling with zero timed blocks: it incorrectly required Int64 for an INTEGER scalar mapped by ADO to Int32. Version 2 accepts Int32/Int64 value 42; fixed counts, order, and gates are unchanged. Preserve version 1 evidence.",
            SettlingSecondsPerArm = 3, Query, Arms,
            Order = "Six Williams rounds, base indices 0,1,5,2,4,3 shifted by round modulo six. Every arm occupies every position once. No retries or exclusions.",
            Scope = $"Actual daemon, {(isolated ? "separate client/server processes" : "same process")}, cleartext loopback, warm one-row point lookup, one outstanding command. SDK and ADO.NET rent from the same pool per wire mode, Max Pool Size=1. Rental/open and warmup are outside timing. No TLS, remote-host, concurrency, pipelining, batching, or prepared-handle claim.",
            IsolationProtocol = isolated ? "New deployment boundary, not a retry of same-process qualification: one owned child hosts the actual daemon, client remains in parent. Separate heaps and ThreadPools. No profiler or stage probes. Every arm uses the same child. Snapshot IPC is outside timing. Allocations/CPU include both processes and bounded snapshot overhead. Preserve prior failed same-process qualification unchanged." : null,
            Gate = "Per API: stream and both baselines must each pass max/min block throughput spread <=25%; stream median throughput >=105% of each baseline, wins >=5 of six paired rounds against each, median block p99 <= each baseline. Otherwise descriptive only; no retry.",
            Validation = "Every result equals 42. Per timed block: exactly 8192 SQL requests/responses, zero other commands, unary Native uses HTTP/2 application/grpc, Web uses HTTP/1.1 application/grpc-web; stream has zero new HTTP RPCs and exactly 8192 decoded ExecuteSql frames and responses. All raw QPC samples retained.",
            Runtime = RuntimeInformation.FrameworkDescription, OS = RuntimeInformation.OSDescription,
            Environment.MachineName, Environment.ProcessorCount, Architecture = RuntimeInformation.ProcessArchitecture.ToString(),
            System.Runtime.GCSettings.IsServerGC, Stopwatch.Frequency,
            GitHead = Git("rev-parse", "HEAD"), GitStatus = status, Sources = sources, Binaries = binaries,
        });
        File.WriteAllText(Path.Combine(output, "source.patch"), Git("diff", "--binary"));
        foreach (var file in sources)
        {
            string copy = Path.Combine(output, "source", file.Name);
            Directory.CreateDirectory(Path.GetDirectoryName(copy)!);
            File.Copy(Path.Combine(root, file.Name), copy);
        }
        Write(output, "status.json", new { State = "running", StartedUtc = DateTimeOffset.UtcNow });
        var blocks = new List<Block>();
        using var probe = diagnostics ? new SessionDiagnosticProbe() : null;
        await using var host = await BenchmarkDaemonHost.StartAsync(isolated, probe, ct);
        Write(output, "host-info.json", host.Info);
        try
        {
            await using (var seed = CSharpDbClient.Create(new() { Transport = CSharpDbTransport.Grpc, GrpcMode = CSharpDbGrpcMode.Native, Endpoint = host.NativeEndpoint.AbsoluteUri }))
            {
                var result = await seed.ExecuteSqlAsync("CREATE TABLE network_read (id INTEGER PRIMARY KEY, value INTEGER); INSERT INTO network_read VALUES (1, 42)", ct);
                if (result.Error is not null) throw new InvalidOperationException(result.Error);
            }
            var settling = new List<object>();
            foreach (var arm in Arms)
            {
                await using var runner = await Runner.OpenAsync(host, arm, ct);
                var timer = Stopwatch.StartNew();
                int completed = 0;
                do { await runner.ReadAsync(ct); completed++; } while (timer.Elapsed < TimeSpan.FromSeconds(3));
                settling.Add(new { arm.Api, arm.Wire, Completed = completed, timer.Elapsed.TotalSeconds });
                Write(output, "settling.json", settling);
            }
            int[] order = [0, 1, 5, 2, 4, 3];
            for (int round = 0; round < Rounds; round++)
                foreach (int position in order)
                {
                    var block = await MeasureAsync(host, probe, Arms[(position + round) % Arms.Length], round + 1, output, ct);
                    blocks.Add(block);
                    Write(output, "blocks.json", blocks);
                    Console.WriteLine($"{block.Api,-3} {block.Wire,-6} round {block.Round}: {block.OpsPerSecond:N0} ops/sec; p99 {block.P99Us:N1} us");
                }
            if (!sources.SequenceEqual(SourceHashes(root)) || !binaries.SequenceEqual(BinaryHashes()) || status != Git("status", "--porcelain=v1"))
                throw new InvalidOperationException("Source, binary, or working-tree state changed during the run.");
            var summaries = blocks.GroupBy(x => new Arm(x.Api, x.Wire)).Select(g => new Summary(g.Key.Api, g.Key.Wire,
                Median(g.Select(x => x.OpsPerSecond)), Median(g.Select(x => x.P99Us)),
                g.Max(x => x.OpsPerSecond) / g.Min(x => x.OpsPerSecond) - 1,
                Median(g.Select(x => x.AllocatedBytes / (double)Iterations)))).ToArray();
            var gates = new List<object>();
            foreach (string api in new[] { "SDK", "ADO" })
            {
                Summary stream = summaries.Single(x => x.Api == api && x.Wire == "Stream");
                foreach (string baseline in new[] { "Web", "Native" })
                {
                    Summary other = summaries.Single(x => x.Api == api && x.Wire == baseline);
                    int wins = blocks.Where(x => x.Api == api && x.Wire == "Stream")
                        .Count(x => x.OpsPerSecond > blocks.Single(b => b.Api == api && b.Wire == baseline && b.Round == x.Round).OpsPerSecond);
                    bool stable = stream.Spread <= .25 && other.Spread <= .25;
                    double change = stream.MedianOpsPerSecond / other.MedianOpsPerSecond - 1;
                    bool passed = !diagnostics && stable && change >= .05 && wins >= 5 && stream.MedianP99Us <= other.MedianP99Us;
                    gates.Add(new { Api = api, Baseline = baseline, ThroughputChange = change, PairedWins = wins,
                        Stable = stable, P99Ratio = stream.MedianP99Us / other.MedianP99Us, Passed = passed });
                }
            }
            Write(output, "summary.json", new { Summaries = summaries, Gates = gates, TotalOperations = blocks.Sum(x => x.Completed),
                SourcesAndBinariesUnchanged = true, WireCountsValidated = true, DiagnosticOnly = diagnostics });
            probe?.Save(output);
            Write(output, "status.json", new { State = "complete", FinishedUtc = DateTimeOffset.UtcNow, Blocks = blocks.Count,
                TotalOperations = blocks.Sum(x => x.Completed) });
        }
        finally
        {
            await CSharpDbClient.ClearPoolAsync(host.CompatibilityEndpoint.AbsoluteUri);
            await CSharpDbClient.ClearPoolAsync(host.NativeEndpoint.AbsoluteUri);
        }
    }

    private static async Task<Block> MeasureAsync(BenchmarkDaemonHost host, SessionDiagnosticProbe? probe, Arm arm, int round, string output, CancellationToken ct)
    {
        await using var runner = await Runner.OpenAsync(host, arm, ct);
        for (int i = 0; i < Warmup; i++) await runner.ReadAsync(ct);
        var before = await host.CaptureAsync(ct);
        var beforeHttp = before.Http;
        var beforeWire = before.Wire;
        var ticks = new long[Iterations];
        using var process = Process.GetCurrentProcess();
        long allocations = GC.GetTotalAllocatedBytes(true) + before.ServerAllocatedBytes;
        double cpu = process.TotalProcessorTime.TotalMilliseconds + before.ServerCpuMs;
        BenchmarkDaemonHost.Snapshot? after = null;
        int completed = 0;
        Exception? commandFailure = null;
        probe?.BeginBlock(arm.Api, arm.Wire, round, Iterations);
        long start = Stopwatch.GetTimestamp(), end = start;
        try
        {
            for (; completed < Iterations; completed++)
            {
                long sample = Stopwatch.GetTimestamp();
                probe?.BeginSample(completed, sample);
                await runner.ReadAsync(ct);
                long sampleEnd = Stopwatch.GetTimestamp();
                ticks[completed] = sampleEnd - sample;
                probe?.EndSample(completed, sampleEnd);
            }
        }
        catch (Exception error) { commandFailure = error; throw; }
        finally
        {
            end = Stopwatch.GetTimestamp();
            probe?.EndBlock();
            Exception? captureFailure = null;
            try { after = await host.CaptureAsync(ct); }
            catch (Exception error) { captureFailure = error; }
            allocations = GC.GetTotalAllocatedBytes(true) + (after?.ServerAllocatedBytes ?? before.ServerAllocatedBytes) - allocations;
            cpu = process.TotalProcessorTime.TotalMilliseconds + (after?.ServerCpuMs ?? before.ServerCpuMs) - cpu;
            string stem = $"{arm.Api}-{arm.Wire}-{round}";
            File.WriteAllLines(Path.Combine(output, stem + ".csv"), new[] { "Sample,ElapsedTicks" }
                .Concat(ticks.Take(completed).Select((value, index) => $"{index + 1},{value}")));
            Write(output, stem + ".wire.json", new { Before = beforeWire, After = after?.Wire,
                HttpBefore = beforeHttp, HttpAfter = after?.Http, Completed = completed });
            Write(output, stem + ".metrics.json", new { Completed = completed, Requested = Iterations, ElapsedTicks = end - start,
                AllocatedBytes = allocations, CpuMs = cpu, CommandFailure = commandFailure?.ToString(), CaptureFailure = captureFailure?.ToString() });
            if (probe is not null) await probe.SaveBlockAsync(output, stem, completed, ct);
            if (captureFailure is not null && commandFailure is null)
                throw new IOException("Could not capture daemon counters; raw samples retained.", captureFailure);
        }
        ValidateWire(beforeHttp, after!.Http, after.Wire - beforeWire, arm);
        Array.Sort(ticks);
        return new Block(arm.Api, arm.Wire, round, completed, completed * (double)Stopwatch.Frequency / (end - start),
            Percentile(ticks, .50), Percentile(ticks, .95), Percentile(ticks, .99), allocations, cpu);
    }

    internal static void ValidateWire(BenchmarkDaemonHost.HttpCount[] before, BenchmarkDaemonHost.HttpCount[] after, Counts delta, Arm arm, int count = Iterations)
    {
        long http = 0;
        foreach (var pair in after)
        {
            long change = pair.Value - (before.SingleOrDefault(x => x.Method == pair.Method && x.Protocol == pair.Protocol && x.ContentType == pair.ContentType)?.Value ?? 0);
            if (change == 0) continue;
            if (arm.Wire == "Stream" || pair.Protocol != (arm.Wire == "Web" ? "HTTP/1.1" : "HTTP/2") ||
                !pair.ContentType.StartsWith(arm.Wire == "Web" ? "application/grpc-web" : "application/grpc", StringComparison.Ordinal) ||
                (arm.Wire != "Web" && pair.ContentType.Contains("grpc-web", StringComparison.Ordinal)) ||
                !pair.Method.EndsWith("/ExecuteSql", StringComparison.Ordinal))
                throw new InvalidOperationException($"Unexpected timed HTTP request: {pair}");
            http += change;
        }
        var expected = arm.Wire == "Stream" ? new Counts(0, 0, count, 0, count) : new Counts(count, 0, 0, 0, 0);
        if (delta != expected || http != (arm.Wire == "Stream" ? 0 : count))
            throw new InvalidOperationException($"Unexpected command counts: {delta}; expected {expected}; HTTP {http}.");
    }

    internal sealed class Runner : IAsyncDisposable
    {
        private ICSharpDbClient? _client;
        private CSharpDbConnection? _connection;
        private CSharpDbCommand? _command;
        public static async Task<Runner> OpenAsync(BenchmarkDaemonHost host, Arm arm, CancellationToken ct, int maxPoolSize = 1)
        {
            var runner = new Runner();
            var mode = arm.Wire == "Web" ? CSharpDbGrpcMode.Auto : CSharpDbGrpcMode.Native;
            var calls = arm.Wire == "Stream" ? CSharpDbGrpcCallMode.Streaming : CSharpDbGrpcCallMode.Unary;
            try
            {
                if (arm.Api == "SDK")
                    runner._client = await CSharpDbClient.RentAsync(new() { Transport = CSharpDbTransport.Grpc, GrpcMode = mode,
                        GrpcCallMode = calls, Endpoint = host.EndpointFor(mode).AbsoluteUri }, maxPoolSize, ct);
                else
                {
                    runner._connection = new($"Transport=Grpc;Grpc Mode={mode};Grpc Call Mode={calls};Endpoint={host.EndpointFor(mode)};Pooling=true;Max Pool Size={maxPoolSize}");
                    await runner._connection.OpenAsync(ct);
                    runner._command = runner._connection.CreateCommand();
                    runner._command.CommandText = Query;
                }
                return runner;
            }
            catch { await runner.DisposeAsync(); throw; }
        }
        public async Task ReadAsync(CancellationToken ct)
        {
            if (_client is not null)
            {
                var result = await _client.ExecuteSqlAsync(Query, ct);
                if (result.Error is not null || result.Rows is not { Count: 1 } || result.Rows[0].Length != 1 || result.Rows[0][0] is not long value || value != 42)
                    throw new InvalidOperationException("The SDK query returned the wrong result.");
            }
            else
            {
                object? scalar = await _command!.ExecuteScalarAsync(ct);
                if (scalar is not (int or long) || Convert.ToInt64(scalar, CultureInfo.InvariantCulture) != 42)
                    throw new InvalidOperationException($"The ADO.NET query returned the wrong result: {scalar} ({scalar?.GetType().Name}).");
            }
        }
        public async ValueTask DisposeAsync()
        {
            _command?.Dispose();
            if (_connection is not null) await _connection.DisposeAsync();
            if (_client is not null) await _client.DisposeAsync();
        }
    }

    internal sealed class WireCounters : Interceptor
    {
        private long _unarySql, _unaryOther, _streamSql, _streamOther, _responses;
        public Counts Snapshot() => new(Interlocked.Read(ref _unarySql), Interlocked.Read(ref _unaryOther),
            Interlocked.Read(ref _streamSql), Interlocked.Read(ref _streamOther), Interlocked.Read(ref _responses));
        public override Task<TResponse> UnaryServerHandler<TRequest, TResponse>(TRequest request, ServerCallContext context, UnaryServerMethod<TRequest, TResponse> continuation)
        {
            if (context.Method.EndsWith("/ExecuteSql", StringComparison.Ordinal)) Interlocked.Increment(ref _unarySql);
            else Interlocked.Increment(ref _unaryOther);
            return continuation(request, context);
        }
        public override Task DuplexStreamingServerHandler<TRequest, TResponse>(IAsyncStreamReader<TRequest> reader,
            IServerStreamWriter<TResponse> writer, ServerCallContext context, DuplexStreamingServerMethod<TRequest, TResponse> continuation)
            => continuation(new Reader<TRequest>(reader, this), new Writer<TResponse>(writer, this), context);
        private sealed class Reader<T>(IAsyncStreamReader<T> inner, WireCounters owner) : IAsyncStreamReader<T>
        {
            public T Current => inner.Current;
            public async Task<bool> MoveNext(CancellationToken ct)
            {
                if (!await inner.MoveNext(ct)) return false;
                if (Current is CommandSessionRequest { ExecuteSql: not null }) Interlocked.Increment(ref owner._streamSql);
                else Interlocked.Increment(ref owner._streamOther);
                return true;
            }
        }
        private sealed class Writer<T>(IServerStreamWriter<T> inner, WireCounters owner) : IServerStreamWriter<T>
        {
            public WriteOptions? WriteOptions { get => inner.WriteOptions; set => inner.WriteOptions = value; }
            public Task WriteAsync(T message) { Interlocked.Increment(ref owner._responses); return inner.WriteAsync(message); }
        }
    }
    internal sealed record Counts(long UnarySql, long UnaryOther, long StreamSql, long StreamOther, long StreamResponses)
    {
        public static Counts operator -(Counts a, Counts b) => new(a.UnarySql - b.UnarySql, a.UnaryOther - b.UnaryOther,
            a.StreamSql - b.StreamSql, a.StreamOther - b.StreamOther, a.StreamResponses - b.StreamResponses);
    }
    internal static Hash[] SourceHashes(string root) => Git("ls-files", "-z", "--cached", "--others", "--exclude-standard",
            "src", "tests", "Directory.Build.props", "Directory.Build.targets", "global.json")
        .Split('\0', StringSplitOptions.RemoveEmptyEntries).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal)
        .Where(path => File.Exists(Path.Combine(root, path))).Select(path => new Hash(path,
            Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(Path.Combine(root, path)))))).ToArray();
    internal static Hash[] BinaryHashes() => Directory.EnumerateFiles(AppContext.BaseDirectory).Where(path =>
            path.EndsWith(".dll", StringComparison.OrdinalIgnoreCase) || path.EndsWith(".deps.json", StringComparison.OrdinalIgnoreCase) ||
            path.EndsWith(".runtimeconfig.json", StringComparison.OrdinalIgnoreCase)).Order(StringComparer.Ordinal)
        .Select(path => new Hash(Path.GetFileName(path), Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(path))))).ToArray();
    internal static string Git(params string[] args)
    {
        var info = new ProcessStartInfo("git") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        foreach (string argument in args) info.ArgumentList.Add(argument);
        using var process = Process.Start(info)!;
        string output = process.StandardOutput.ReadToEnd(), error = process.StandardError.ReadToEnd();
        process.WaitForExit();
        if (process.ExitCode != 0) throw new InvalidOperationException(error);
        return output.TrimEnd();
    }
    internal static void Write(string output, string name, object value) => File.WriteAllText(Path.Combine(output, name), JsonSerializer.Serialize(value, Json));
    internal static double Percentile(long[] values, double quantile) => values[(int)Math.Ceiling(values.Length * quantile) - 1] * 1_000_000.0 / Stopwatch.Frequency;
    internal static double Median(IEnumerable<double> values)
    {
        double[] sorted = values.Order().ToArray();
        return (sorted[(sorted.Length - 1) / 2] + sorted[sorted.Length / 2]) / 2;
    }
    internal sealed record Arm(string Api, string Wire);
    internal sealed record Hash(string Name, string Sha256);
    private sealed record Summary(string Api, string Wire, double MedianOpsPerSecond, double MedianP99Us, double Spread, double BytesPerOp);
    private sealed record Block(string Api, string Wire, int Round, int Completed, double OpsPerSecond, double P50Us, double P95Us, double P99Us, long AllocatedBytes, double CpuMs);
}
