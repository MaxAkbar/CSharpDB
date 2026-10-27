using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text.Json;
using CSharpDB.Client;
using CSharpDB.Data;
using CSharpDB.Testing;

namespace CSharpDB.NetworkBenchmarks;

internal static class NetworkBenchmarkProgram
{
    private const string Query = "SELECT value FROM network_read WHERE id = 1";
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };

    public static async Task<int> Main(string[] args)
    {
        int experimentIndex = Array.IndexOf(args, "--experiment");
        if (args is ["--session-daemon"]) return await BenchmarkDaemonHost.RunChildAsync();
        if (experimentIndex >= 0 && experimentIndex + 1 < args.Length && args[experimentIndex + 1] == "pooled-allocation-profile")
            return await GrpcPooledAllocationProfile.RunAsync(args);
        if (experimentIndex >= 0 && experimentIndex + 1 < args.Length && args[experimentIndex + 1] == "pooled-lifecycle-c8")
            return await GrpcPooledLifecycleBenchmark.RunAsync(args);
        if (experimentIndex >= 0 && experimentIndex + 1 < args.Length && args[experimentIndex + 1] == "session-c8-baseline-timeline")
            return await GrpcConcurrentSessionBenchmark.RunBaselineTimelineAsync(args);
        if (experimentIndex >= 0 && experimentIndex + 1 < args.Length && args[experimentIndex + 1] == "session-c8-timing")
            return await GrpcConcurrentSessionBenchmark.RunTimingAsync(args);
        if (experimentIndex >= 0 && experimentIndex + 1 < args.Length && args[experimentIndex + 1] == "session-c8-timing-profile")
            return await GrpcConcurrentSessionBenchmark.RunTimingAsync(args, true);
        if (experimentIndex >= 0 && experimentIndex + 1 < args.Length && args[experimentIndex + 1] == "session-c8-paired")
            return await GrpcConcurrentSessionBenchmark.RunPairedAsync(args);
        if (experimentIndex >= 0 && experimentIndex + 1 < args.Length && args[experimentIndex + 1] == "session-c8-paired-timing")
            return await GrpcConcurrentSessionBenchmark.RunPairedTimingAsync(args);
        if (experimentIndex >= 0 && experimentIndex + 1 < args.Length && args[experimentIndex + 1] == "session-c8-paired-long-timing")
            return await GrpcConcurrentSessionBenchmark.RunLongPairedTimingAsync(args);
        if (experimentIndex >= 0 && experimentIndex + 1 < args.Length && args[experimentIndex + 1] is "session-c8" or "session-c8-profile")
            return await GrpcConcurrentSessionBenchmark.RunAsync(args, args[experimentIndex + 1] == "session-c8-profile");
        if (experimentIndex >= 0 && experimentIndex + 1 < args.Length && args[experimentIndex + 1] is "session" or "session-diagnostic" or "session-isolated")
            return await GrpcSessionBenchmark.RunAsync(args, args[experimentIndex + 1] == "session-diagnostic", args[experimentIndex + 1] == "session-isolated");
        if (args.Length == 0 || args.Contains("--help"))
        {
            Console.WriteLine("Usage: --output <new directory> [--experiment native|pooling|shared] [--iterations 2000] [--open-iterations 300] [--rounds 4] [--settle-seconds 10]. Shared compares SDK/ADO, Auto/Native, fresh/pooled/reused with 1000 iterations and twelve balanced rounds. --experiment session --output <new directory> runs the fixed six-arm persistent-session experiment. --experiment session-c8-timing --output <new directory> runs fixed C8 timing/CPU qualification without allocation-counter collection.");
            return args.Contains("--help") ? 0 : 1;
        }
        string? output = null;
        bool ownsOutput = false;
        try
        {
            var options = Parse(args);
            output = Path.GetFullPath(options.Output);
            string root = Path.GetFullPath(Git("rev-parse", "--show-toplevel"));
            if (output.Equals(root, StringComparison.OrdinalIgnoreCase) ||
                output.StartsWith(root + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException("Keep generated evidence outside the repository.");
            if (Directory.Exists(output))
                throw new InvalidOperationException("The output directory must be new; existing evidence is never overwritten.");
            Directory.CreateDirectory(output);
            ownsOutput = true;
            await RunAsync(options, output);
            return 0;
        }
        catch (Exception error)
        {
            if (ownsOutput && output is not null)
            {
                await File.AppendAllTextAsync(Path.Combine(output, "failures.jsonl"),
                    JsonSerializer.Serialize(new { Utc = DateTimeOffset.UtcNow, Error = error.ToString() }) + Environment.NewLine);
                WriteJson(output, "status.json", new { State = "failed", FinishedUtc = DateTimeOffset.UtcNow });
            }
            Console.Error.WriteLine(error);
            return 1;
        }
    }

    private static async Task RunAsync(Options options, string output)
    {
        bool shared = options.Experiment == "shared";
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(shared ? 5 : 3));
        var ct = timeout.Token;
        var binaries = HashBinaries();
        var sources = HashSources();
        bool pooling = options.Experiment == "pooling";
        WriteJson(output, "protocol.json", new
        {
            Version = shared ? 5 : pooling ? 3 : 2, options.Experiment, options.Iterations, options.OpenIterations, options.Rounds, options.SettleSeconds,
            Warmup = 100, OpenWarmup = 25,
            Order = shared
                ? "12 cases: SDK reused, ADO reused, SDK fresh, ADO fresh, SDK pooled, ADO pooled; Auto then Native for each. Twelve Williams-design rounds use base case indices 0,1,11,2,10,3,9,4,8,5,7,6 shifted by round modulo 12. Every case occupies every position once. No retries."
                : pooling
                ? "Six balanced permutations of unpooled(U), pooled(P), reused(R): UPR, PRU, RUP, RPU, PUR, URP. All use Auto and the same iteration count. No retries."
                : "Even rounds Auto then Native; odd rounds Native then Auto. No retries.",
            Query, GitHead = Git("rev-parse", "HEAD"), GitStatus = Git("status", "--porcelain=v1"),
            Runtime = RuntimeInformation.FrameworkDescription, OS = RuntimeInformation.OSDescription,
            Environment.MachineName, Architecture = RuntimeInformation.ProcessArchitecture.ToString(),
            System.Runtime.GCSettings.IsServerGC,
            Environment.ProcessorCount, Stopwatch.Frequency, Binaries = binaries, Sources = sources,
            Scope = shared
                ? "Pilot: SDK and ADO.NET, Auto gRPC-Web/HTTP1 and Native gRPC/HTTP2, reused/fresh/pooled lifetimes. Same daemon, cleartext loopback, warm database, same process, one outstanding operation. Shared pool Max Pool Size=1. No TLS, prepared handles, separate-host or saturation claim."
                : pooling
                ? "Pilot: existing Auto gRPC-Web/HTTP1, cleartext loopback, actual daemon, shared warm database, one outstanding request, same-process client/server. Compare unpooled open/read/close, pooled open/read/close (Max Pool Size=1), and already-open. No prepared handles. Not TLS, separate-host, or saturation."
                : "Pilot: cleartext loopback, actual daemon, shared warm database, one outstanding request, client and server in the same process. No pooling or prepared handles. Not a TLS, separate-host, or saturation result.",
            Metrics = "Per-call client latency, sequential operations/sec, process CPU and allocations including client and server. Raw samples retained.",
            StabilityScreen = "Every scenario/mode must have (maximum block throughput / minimum block throughput - 1) <= 25%. Failure leaves the result descriptive; there are no retries.",
            PoolingGate = pooling ? "All three arms pass stability; pooled median throughput >= 1.25 times unpooled; pooled median block p99 <= unpooled. This is a pilot gate, not production qualification." : null,
            SharedPoolGate = shared ? "All 12 cases pass the 25% stability screen. For both APIs and both modes, pooled median throughput >= 1.25 times fresh and pooled median block p99 <= fresh. No retries or promotion from failed gates." : null,
            DeadlineSeconds = shared ? 300 : 180,
            FreshClientBudget = shared ? "Fresh SDK/ADO cases use 100 settling operations and OpenIterations per block (default 100); reused/pooled cases retain timed settling and Iterations per block. Fresh per-block warmup is 25. Bound total fresh connections below the Windows 16384-port range. Version 4 exhausted ports during unlimited fresh settling before any timed block; preserve it separately." : null,
        });
        await File.WriteAllTextAsync(Path.Combine(output, "source.patch"), Git("diff", "--binary"), ct);
        string repoRoot = Git("rev-parse", "--show-toplevel");
        foreach (string path in Git("ls-files", "-z", "--others", "--exclude-standard", "src/CSharpDB.Client", "src/CSharpDB.Data", "tests/CSharpDB.Data.Tests", "tests/CSharpDB.Daemon.Tests", "tests/Shared", "tests/CSharpDB.NetworkBenchmarks")
                     .Split('\0', StringSplitOptions.RemoveEmptyEntries))
        {
            string destination = Path.Combine(output, "new-source", path);
            Directory.CreateDirectory(Path.GetDirectoryName(destination)!);
            File.Copy(Path.Combine(repoRoot, path), destination);
        }
        WriteJson(output, "status.json", new { State = "running", StartedUtc = DateTimeOffset.UtcNow });

        await using var host = new GrpcSocketDaemonFixture();
        await using (var seed = host.CreateClient(CSharpDbGrpcMode.Native))
        {
            Check(await seed.ExecuteSqlAsync(
                "CREATE TABLE network_read (id INTEGER PRIMARY KEY, value INTEGER); INSERT INTO network_read VALUES (1, 42)", ct));
        }

        var rows = new List<BlockResult>();
        var settling = new List<SettlingResult>();
        try
        {
            if (shared)
            {
                var cases = new[] { "sdk-reused", "ado-reused", "sdk-create-read-dispose", "ado-open-read-close", "sdk-pooled-read-dispose", "ado-pooled-open-read-close" }
                    .SelectMany(scenario => new[] { CSharpDbGrpcMode.Auto, CSharpDbGrpcMode.Native }.Select(mode => new BenchmarkCase(scenario, mode))).ToArray();
                foreach (var benchmark in cases)
                {
                    settling.Add(await SettleAsync(host, benchmark.Scenario, benchmark.Mode,
                        IsFresh(benchmark.Scenario) ? 0 : options.SettleSeconds, ct));
                    WriteJson(output, "settling.json", settling);
                }
                int[] baseOrder = [0, 1, 11, 2, 10, 3, 9, 4, 8, 5, 7, 6];
                for (int round = 0; round < 12; round++)
                    foreach (int index in baseOrder)
                    {
                        var benchmark = cases[(index + round) % cases.Length];
                        int count = IsFresh(benchmark.Scenario) ? options.OpenIterations : options.Iterations;
                        var row = await MeasureAsync(host, benchmark.Scenario, benchmark.Mode, round + 1, count, output, ct);
                        rows.Add(row);
                        WriteJson(output, "blocks.json", rows);
                        Console.WriteLine($"{row.Scenario,-27} {row.Mode,-6} round {row.Round}: {row.OperationsPerSecond:N0} ops/sec; p99 {row.P99Us:N1} us");
                    }
            }
            else if (pooling)
            {
                foreach (string scenario in new[] { "ado-open-read-close", "ado-pooled-open-read-close", "ado-reused" })
                {
                    settling.Add(await SettleAsync(host, scenario, CSharpDbGrpcMode.Auto, options.SettleSeconds, ct));
                    WriteJson(output, "settling.json", settling);
                }
                string[][] orders =
                [
                    ["ado-open-read-close", "ado-pooled-open-read-close", "ado-reused"],
                    ["ado-pooled-open-read-close", "ado-reused", "ado-open-read-close"],
                    ["ado-reused", "ado-open-read-close", "ado-pooled-open-read-close"],
                    ["ado-reused", "ado-pooled-open-read-close", "ado-open-read-close"],
                    ["ado-pooled-open-read-close", "ado-open-read-close", "ado-reused"],
                    ["ado-open-read-close", "ado-reused", "ado-pooled-open-read-close"],
                ];
                for (int round = 0; round < orders.Length; round++)
                    foreach (string scenario in orders[round])
                    {
                        var row = await MeasureAsync(host, scenario, CSharpDbGrpcMode.Auto, round + 1, options.Iterations, output, ct);
                        rows.Add(row);
                        WriteJson(output, "blocks.json", rows);
                        Console.WriteLine($"{scenario,-27} round {round + 1}: {row.OperationsPerSecond:N0} ops/sec; p50 {row.P50Us:N1} us; p99 {row.P99Us:N1} us");
                    }
            }
            else foreach (string scenario in new[] { "sdk-reused", "ado-reused", "ado-open-read-close" })
            {
                foreach (var mode in new[] { CSharpDbGrpcMode.Auto, CSharpDbGrpcMode.Native })
                {
                    settling.Add(await SettleAsync(host, scenario, mode, options.SettleSeconds, ct));
                    WriteJson(output, "settling.json", settling);
                }
                int iterations = scenario == "ado-open-read-close" ? options.OpenIterations : options.Iterations;
                for (int round = 0; round < options.Rounds; round++)
                {
                    CSharpDbGrpcMode[] modes = round % 2 == 0
                        ? [CSharpDbGrpcMode.Auto, CSharpDbGrpcMode.Native]
                        : [CSharpDbGrpcMode.Native, CSharpDbGrpcMode.Auto];
                    foreach (var mode in modes)
                    {
                        var row = await MeasureAsync(host, scenario, mode, round + 1, iterations, output, ct);
                        rows.Add(row);
                        // Persist each finished block before attempting the next one.
                        WriteJson(output, "blocks.json", rows);
                        Console.WriteLine($"{scenario,-20} {mode,-6} round {round + 1}: {row.OperationsPerSecond:N0} ops/sec; p50 {row.P50Us:N1} us; p99 {row.P99Us:N1} us");
                    }
                }
            }

            if (!binaries.SequenceEqual(HashBinaries()))
                throw new InvalidOperationException("A measured binary changed during the run.");
            if (!sources.SequenceEqual(HashSources()))
                throw new InvalidOperationException("A source file changed during the run.");
            var summary = rows.GroupBy(row => new { row.Scenario, row.Mode }).Select(group => new
            {
                group.Key.Scenario, group.Key.Mode,
                MedianOperationsPerSecond = Median(group.Select(row => row.OperationsPerSecond)),
                MedianP50Us = Median(group.Select(row => row.P50Us)),
                MedianP95Us = Median(group.Select(row => row.P95Us)),
                MedianP99Us = Median(group.Select(row => row.P99Us)),
                ThroughputSpreadPercent = 100 * (group.Max(row => row.OperationsPerSecond) / group.Min(row => row.OperationsPerSecond) - 1),
            }).ToArray();
            WriteJson(output, "summary.json", summary);
            WriteJson(output, "requests.json", host.Snapshot().Select(pair => new { pair.Key, Count = pair.Value }));
            var unpooled = summary.SingleOrDefault(row => row.Scenario == "ado-open-read-close" && row.Mode == "Auto");
            var pooled = summary.SingleOrDefault(row => row.Scenario == "ado-pooled-open-read-close" && row.Mode == "Auto");
            var comparisons = shared ? new[] { "sdk", "ado" }.SelectMany(api => new[] { "Auto", "Native" }.Select(mode =>
            {
                var fresh = summary.Single(row => row.Mode == mode && row.Scenario == (api == "sdk" ? "sdk-create-read-dispose" : "ado-open-read-close"));
                var rented = summary.Single(row => row.Mode == mode && row.Scenario == (api == "sdk" ? "sdk-pooled-read-dispose" : "ado-pooled-open-read-close"));
                return new
                {
                    Api = api, Mode = mode,
                    ThroughputRatio = rented.MedianOperationsPerSecond / fresh.MedianOperationsPerSecond,
                    PooledP99NoWorse = rented.MedianP99Us <= fresh.MedianP99Us,
                };
            })).ToArray() : null;
            WriteJson(output, "assessment.json", new
            {
                StabilityPassed = summary.All(row => row.ThroughputSpreadPercent <= 25),
                PoolingGatePassed = pooling ? (bool?)(summary.All(row => row.ThroughputSpreadPercent <= 25) &&
                    pooled!.MedianOperationsPerSecond >= 1.25 * unpooled!.MedianOperationsPerSecond &&
                    pooled.MedianP99Us <= unpooled.MedianP99Us) : null,
                PooledThroughputChangePercent = pooled is not null ? (double?)(100 * (pooled.MedianOperationsPerSecond / unpooled!.MedianOperationsPerSecond - 1)) : null,
                SharedPoolingComparisons = comparisons,
                SharedPoolGatePassed = shared ? (bool?)(summary.All(row => row.ThroughputSpreadPercent <= 25) && comparisons!.All(row => row.ThroughputRatio >= 1.25 && row.PooledP99NoWorse)) : null,
                Scope = "Loopback sequential pilot; passing stability does not establish separate-host or TLS performance.",
            });
            WriteJson(output, "status.json", new { State = "complete", FinishedUtc = DateTimeOffset.UtcNow, Blocks = rows.Count });
            Console.WriteLine($"Evidence: {output}");
        }
        finally
        {
            if (pooling || shared)
            {
                await CSharpDbClient.ClearPoolAsync(host.CompatibilityEndpoint.AbsoluteUri);
                await CSharpDbClient.ClearPoolAsync(host.NativeEndpoint.AbsoluteUri);
            }
        }
    }

    private static async Task<SettlingResult> SettleAsync(GrpcSocketDaemonFixture host,
        string scenario, CSharpDbGrpcMode mode, int seconds, CancellationToken ct)
    {
        string connectionString = ConnectionString(host, scenario, mode);
        ICSharpDbClient? client = null;
        CSharpDbConnection? connection = null;
        CSharpDbCommand? command = null;
        try
        {
            if (scenario == "sdk-reused") client = host.CreateClient(mode);
            else if (scenario == "ado-reused")
            {
                connection = new CSharpDbConnection(connectionString);
                await connection.OpenAsync(ct);
                command = connection.CreateCommand();
                command.CommandText = Query;
            }
            else if (scenario == "sdk-pooled-read-dispose")
            {
                await using var first = await CSharpDbClient.RentAsync(ClientOptions(host, mode), 1, ct);
            }
            else if (scenario == "ado-pooled-open-read-close")
            {
                // The first physical validation is outside settling and timing;
                // all following logical opens must perform zero GetInfo calls.
                await using var first = new CSharpDbConnection(connectionString);
                await first.OpenAsync(ct);
            }
            var before = host.Snapshot();
            long start = Stopwatch.GetTimestamp();
            int completed = 0;
            do
            {
                if (client is not null)
                {
                    ValidateSdkResult(await client.ExecuteSqlAsync(Query, ct));
                }
                else if (command is not null) ValidateScalar(await command.ExecuteScalarAsync(ct));
                else if (scenario.StartsWith("sdk-", StringComparison.Ordinal))
                {
                    await using var opened = scenario == "sdk-pooled-read-dispose"
                        ? await CSharpDbClient.RentAsync(ClientOptions(host, mode), 1, ct) : host.CreateClient(mode);
                    ValidateSdkResult(await opened.ExecuteSqlAsync(Query, ct));
                }
                else
                {
                    await using var opened = new CSharpDbConnection(connectionString);
                    await opened.OpenAsync(ct);
                    using var query = opened.CreateCommand();
                    query.CommandText = Query;
                    ValidateScalar(await query.ExecuteScalarAsync(ct));
                }
                completed++;
            } while (seconds == 0 ? completed < 100 : Stopwatch.GetElapsedTime(start).TotalSeconds < seconds);
            double elapsed = Stopwatch.GetElapsedTime(start).TotalSeconds;
            ValidateRequests(before, host.Snapshot(), mode, scenario, completed);
            Console.WriteLine($"Settled {scenario} {mode}: {completed} operations in {elapsed:N1}s");
            return new SettlingResult(scenario, mode.ToString(), completed, elapsed);
        }
        finally
        {
            command?.Dispose();
            if (connection is not null) await connection.DisposeAsync();
            if (client is not null) await client.DisposeAsync();
        }
    }

    private static async Task<BlockResult> MeasureAsync(GrpcSocketDaemonFixture host, string scenario,
        CSharpDbGrpcMode mode, int round, int count, string output, CancellationToken ct)
    {
        ICSharpDbClient? client = null;
        CSharpDbConnection? connection = null;
        CSharpDbCommand? command = null;
        BlockResult? blockResult = null;
        string connectionString = ConnectionString(host, scenario, mode);
        try
        {
            if (scenario == "sdk-reused")
                client = host.CreateClient(mode);
            else if (scenario == "ado-reused")
            {
                connection = new CSharpDbConnection(connectionString);
                await connection.OpenAsync(ct);
                command = connection.CreateCommand();
                command.CommandText = Query;
            }
            async Task ExecuteAsync()
            {
                if (client is not null)
                {
                    ValidateSdkResult(await client.ExecuteSqlAsync(Query, ct));
                }
                else if (command is not null)
                    ValidateScalar(await command.ExecuteScalarAsync(ct));
                else if (scenario.StartsWith("sdk-", StringComparison.Ordinal))
                {
                    await using var opened = scenario == "sdk-pooled-read-dispose"
                        ? await CSharpDbClient.RentAsync(ClientOptions(host, mode), 1, ct) : host.CreateClient(mode);
                    ValidateSdkResult(await opened.ExecuteSqlAsync(Query, ct));
                }
                else
                {
                    await using var opened = new CSharpDbConnection(connectionString);
                    await opened.OpenAsync(ct);
                    using var query = opened.CreateCommand();
                    query.CommandText = Query;
                    ValidateScalar(await query.ExecuteScalarAsync(ct));
                }
            }
            int warmup = scenario.EndsWith("reused", StringComparison.Ordinal) ? 100 : 25;
            for (int i = 0; i < warmup; i++)
                await ExecuteAsync();

            var before = host.Snapshot();
            var ticks = new long[count];
            using var process = Process.GetCurrentProcess();
            var cpuBefore = process.TotalProcessorTime;
            long allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
            long blockStart = Stopwatch.GetTimestamp();
            int completed = 0;
            try
            {
                for (int i = 0; i < count; i++)
                {
                    long start = Stopwatch.GetTimestamp();
                    await ExecuteAsync();
                    ticks[i] = Stopwatch.GetTimestamp() - start;
                    completed++;
                }
            }
            catch (Exception error)
            {
                WriteJson(output, $"{scenario}-{mode}-round-{round}.failure.json", new
                {
                    Scenario = scenario, Mode = mode.ToString(), Round = round,
                    FailedSample = completed + 1, Error = error.ToString(),
                });
                throw;
            }
            finally
            {
                // Preserve successful samples even if the next operation fails.
                // These files are written after timing, not in the request loop.
                long end = Stopwatch.GetTimestamp();
                long allocatedAfter = GC.GetTotalAllocatedBytes(precise: true);
                var cpuAfter = process.TotalProcessorTime;
                string stem = $"{scenario}-{mode}-round-{round}";
                await File.WriteAllLinesAsync(Path.Combine(output, stem + ".csv"),
                    new[] { "Sample,ElapsedTicks,LatencyUs" }.Concat(ticks.Take(completed).Select((tick, index) =>
                        $"{index + 1},{tick},{(tick * 1_000_000.0 / Stopwatch.Frequency).ToString("R", CultureInfo.InvariantCulture)}")));
                WriteJson(output, stem + ".metrics.json", new
                {
                    Completed = completed, Requested = count, ElapsedTicks = end - blockStart,
                    AllocatedBytes = allocatedAfter - allocatedBefore, CpuMs = (cpuAfter - cpuBefore).TotalMilliseconds,
                });
                if (completed == count)
                {
                    ValidateRequests(before, host.Snapshot(), mode, scenario, count);
                    double elapsedSeconds = (end - blockStart) / (double)Stopwatch.Frequency;
                    Array.Sort(ticks);
                    blockResult = new BlockResult(scenario, mode.ToString(), round, count,
                        count / elapsedSeconds, Percentile(ticks, 0.50), Percentile(ticks, 0.95), Percentile(ticks, 0.99),
                        allocatedAfter - allocatedBefore, (cpuAfter - cpuBefore).TotalMilliseconds);
                }
            }
            return blockResult ?? throw new InvalidOperationException("The measurement block did not finish.");
        }
        finally
        {
            command?.Dispose();
            if (connection is not null) await connection.DisposeAsync();
            if (client is not null) await client.DisposeAsync();
        }
    }

    private static void ValidateRequests(IReadOnlyDictionary<GrpcSocketDaemonFixture.RequestShape, long> before,
        IReadOnlyDictionary<GrpcSocketDaemonFixture.RequestShape, long> after, CSharpDbGrpcMode mode, string scenario, int count)
    {
        string expectedProtocol = mode == CSharpDbGrpcMode.Native ? "HTTP/2" : "HTTP/1.1";
        string expectedContentType = mode == CSharpDbGrpcMode.Native ? "application/grpc" : "application/grpc-web";
        long queries = 0, opens = 0;
        foreach (var pair in after)
        {
            before.TryGetValue(pair.Key, out long previous);
            long delta = pair.Value - previous;
            if (delta == 0) continue;
            if (pair.Key.Protocol != expectedProtocol || !pair.Key.ContentType.StartsWith(expectedContentType, StringComparison.Ordinal)
                || (mode == CSharpDbGrpcMode.Native && pair.Key.ContentType.Contains("grpc-web", StringComparison.Ordinal)))
                throw new InvalidOperationException($"Unexpected wire protocol: {pair.Key}");
            if (pair.Key.Method.EndsWith("/ExecuteSql", StringComparison.Ordinal)) queries += delta;
            else if (pair.Key.Method.EndsWith("/GetInfo", StringComparison.Ordinal)) opens += delta;
            else throw new InvalidOperationException($"Unexpected request during measurement: {pair.Key.Method}");
        }
        if (queries != count || opens != (scenario == "ado-open-read-close" ? count : 0))
            throw new InvalidOperationException($"Unexpected request counts: {queries} queries and {opens} opens.");
    }

    private static double Percentile(long[] sorted, double percentile)
        => sorted[Math.Clamp((int)Math.Ceiling(sorted.Length * percentile) - 1, 0, sorted.Length - 1)] * 1_000_000.0 / Stopwatch.Frequency;
    private static double Median(IEnumerable<double> values)
    {
        double[] sorted = values.Order().ToArray();
        return sorted.Length % 2 == 1 ? sorted[sorted.Length / 2] : (sorted[sorted.Length / 2 - 1] + sorted[sorted.Length / 2]) / 2;
    }
    private static string ConnectionString(GrpcSocketDaemonFixture host, string scenario, CSharpDbGrpcMode mode)
        => $"Transport=Grpc;Grpc Mode={mode};Endpoint={host.EndpointFor(mode)}" +
           (scenario == "ado-pooled-open-read-close" ? ";Pooling=true;Max Pool Size=1" : "");
    private static CSharpDbClientOptions ClientOptions(GrpcSocketDaemonFixture host, CSharpDbGrpcMode mode) => new()
    {
        Transport = CSharpDbTransport.Grpc, GrpcMode = mode, Endpoint = host.EndpointFor(mode).AbsoluteUri,
    };
    private static bool IsFresh(string scenario) => scenario is "sdk-create-read-dispose" or "ado-open-read-close";
    private static void ValidateSdkResult(CSharpDB.Client.Models.SqlExecutionResult result)
    {
        Check(result);
        if (result.Rows is not { Count: 1 } || result.Rows[0].Length != 1 || result.Rows[0][0] is not long value || value != 42)
            throw new InvalidOperationException("SDK query returned the wrong result.");
    }
    private static void Check(CSharpDB.Client.Models.SqlExecutionResult result)
    {
        if (result.Error is not null) throw new InvalidOperationException(result.Error);
    }
    private static void ValidateScalar(object? value)
    {
        if (value is null || Convert.ToInt64(value, CultureInfo.InvariantCulture) != 42)
            throw new InvalidOperationException("ADO.NET query returned the wrong result.");
    }
    private static void WriteJson(string output, string name, object value)
        => File.WriteAllText(Path.Combine(output, name), JsonSerializer.Serialize(value, JsonOptions));
    private static BinaryHash[] HashBinaries()
        => Directory.EnumerateFiles(AppContext.BaseDirectory).Where(path => path.EndsWith(".dll", StringComparison.OrdinalIgnoreCase)
            || path.EndsWith(".runtimeconfig.json", StringComparison.OrdinalIgnoreCase)
            || path.EndsWith(".deps.json", StringComparison.OrdinalIgnoreCase)).Order(StringComparer.Ordinal).Select(path =>
            new BinaryHash(Path.GetFileName(path), Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(path))))).ToArray();
    private static BinaryHash[] HashSources()
    {
        string root = Git("rev-parse", "--show-toplevel");
        return Git("ls-files", "-z", "--cached", "--others", "--exclude-standard", "src", "tests/Shared",
                "tests/CSharpDB.NetworkBenchmarks", "Directory.Build.props", "Directory.Build.targets", "global.json")
            .Split('\0', StringSplitOptions.RemoveEmptyEntries).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal)
            .Where(path => File.Exists(Path.Combine(root, path)))
            .Select(path => new BinaryHash(path, Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(Path.Combine(root, path)))))).ToArray();
    }
    private static string Git(params string[] arguments)
    {
        var info = new ProcessStartInfo("git") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        foreach (var argument in arguments) info.ArgumentList.Add(argument);
        using var process = Process.Start(info)!;
        string result = process.StandardOutput.ReadToEnd();
        string error = process.StandardError.ReadToEnd();
        process.WaitForExit();
        if (process.ExitCode != 0) throw new InvalidOperationException(error);
        return result.TrimEnd();
    }
    private static Options Parse(string[] args)
    {
        string? output = null;
        string experiment = "native";
        int iterations = 2000, opens = 300, rounds = 4, settleSeconds = 10;
        for (int i = 0; i < args.Length; i += 2)
        {
            if (i + 1 >= args.Length) throw new ArgumentException($"Missing value for {args[i]}.");
            switch (args[i])
            {
                case "--output": output = args[i + 1]; break;
                case "--experiment": experiment = args[i + 1]; break;
                case "--iterations": iterations = int.Parse(args[i + 1], CultureInfo.InvariantCulture); break;
                case "--open-iterations": opens = int.Parse(args[i + 1], CultureInfo.InvariantCulture); break;
                case "--rounds": rounds = int.Parse(args[i + 1], CultureInfo.InvariantCulture); break;
                case "--settle-seconds": settleSeconds = int.Parse(args[i + 1], CultureInfo.InvariantCulture); break;
                default: throw new ArgumentException($"Unknown option {args[i]}.");
            }
        }
        if (experiment is not ("native" or "pooling" or "shared")) throw new ArgumentException("Experiment must be native, pooling, or shared.");
        if (experiment == "pooling")
        {
            if (!args.Contains("--rounds")) rounds = 6;
            if (!args.Contains("--iterations")) iterations = 1000;
            if (rounds != 6) throw new ArgumentException("The pooling experiment requires six balanced rounds.");
        }
        if (experiment == "shared")
        {
            if (!args.Contains("--rounds")) rounds = 12;
            if (!args.Contains("--iterations")) iterations = 1000;
            if (!args.Contains("--open-iterations")) opens = 100;
            if (rounds != 12) throw new ArgumentException("The shared experiment requires twelve balanced rounds.");
            if (opens > 100) throw new ArgumentException("Shared fresh-client blocks are capped at 100 operations to stay within the socket budget.");
        }
        if (string.IsNullOrWhiteSpace(output) || iterations is < 1 or > 100000 || opens is < 1 or > 10000 || rounds < 2 || rounds > (experiment == "shared" ? 12 : 10) || rounds % 2 != 0 || settleSeconds is < 1 or > 15)
            throw new ArgumentException("Supply a new output directory, 1..100000 iterations, 1..10000 open iterations, an even round count from 2..10, and 1..15 settling seconds.");
        return new Options(output, iterations, opens, rounds, settleSeconds, experiment);
    }
    private sealed record Options(string Output, int Iterations, int OpenIterations, int Rounds, int SettleSeconds, string Experiment);
    private sealed record SettlingResult(string Scenario, string Mode, int Completed, double ElapsedSeconds);
    private sealed record BenchmarkCase(string Scenario, CSharpDbGrpcMode Mode);
    private sealed record BinaryHash(string Name, string Sha256);
    private sealed record BlockResult(string Scenario, string Mode, int Round, int Count, double OperationsPerSecond,
        double P50Us, double P95Us, double P99Us, long AllocatedBytes, double CpuMs);
}
