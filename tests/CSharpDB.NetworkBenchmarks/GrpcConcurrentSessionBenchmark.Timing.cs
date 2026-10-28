using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text.Json;
using CSharpDB.Client;
using static CSharpDB.NetworkBenchmarks.GrpcSessionBenchmark;

namespace CSharpDB.NetworkBenchmarks;

internal static partial class GrpcConcurrentSessionBenchmark
{
    public static async Task<int> RunTimingAsync(string[] args, bool profileOnly = false)
    {
        string? output = null;
        bool ownsOutput = false;
        try
        {
            if (args.Length != 4 || Array.IndexOf(args, "--output") is not (0 or 2)
                || Array.IndexOf(args, "--experiment") is not (0 or 2)
                || args[Array.IndexOf(args, "--experiment") + 1] != (profileOnly ? "session-c8-timing-profile" : "session-c8-timing"))
                throw new ArgumentException("Use --experiment session-c8-timing or session-c8-timing-profile --output <new directory>; all workload settings are fixed.");
            output = Path.GetFullPath(args[Array.IndexOf(args, "--output") + 1]);
            string root = Git("rev-parse", "--show-toplevel");
            if (output.Equals(root, StringComparison.OrdinalIgnoreCase)
                || output.StartsWith(root + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase))
                throw new ArgumentException("Keep generated evidence outside the repository.");
            if (Directory.Exists(output)) throw new ArgumentException("Evidence directory must be new.");
            Directory.CreateDirectory(output);
            ownsOutput = true;
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(5));
            await RunTimingCoreAsync(root, output, profileOnly, timeout.Token);
            return 0;
        }
        catch (Exception error)
        {
            if (ownsOutput && output is not null)
            {
                File.AppendAllText(Path.Combine(output, "failures.jsonl"), JsonSerializer.Serialize(new { Utc = DateTimeOffset.UtcNow, Error = error.ToString() }) + Environment.NewLine);
                Write(output, "status.json", new { State = "failed", AllocationAccounting = "NotCollected", FinishedUtc = DateTimeOffset.UtcNow });
            }
            Console.Error.WriteLine(error);
            return 1;
        }
    }

    private static async Task RunTimingCoreAsync(string root, string output, bool profileOnly, CancellationToken ct)
    {
        var sources = SourceHashes(root);
        var binaries = BinaryHashes();
        string status = Git("status", "--porcelain=v1");
        Write(output, "protocol.json", new
        {
            Version = 3,
            Experiment = profileOnly ? "persistent-command-session-isolated-C8-timing-profile" : "persistent-command-session-isolated-C8-timing",
            ProfileOnly = profileOnly,
            DiagnosticBoundary = profileOnly ? "Diagnostic trace only. Throughput gates cannot qualify or replace a failed run." : null,
            AllocationAccounting = "NotCollected",
            Workers, PerWorker, Count, Rounds, Warmup,
            SettlingSecondsPerArm = 3,
            PoolCapacity = Workers,
            Query = "SELECT value FROM network_read WHERE id = 1",
            Arms,
            Order = "Six Williams rounds, base indices0,1,5,2,4,3 shifted by round modulo6. Every arm occupies every position once. No retries/exclusions.",
            Scope = "Separate client/server processes, actual daemon, cleartext loopback, warm point reads. Eight held rentals/commands, one command at a time per worker; shared SDK-owned pool capacity8 per mode. No TLS, remote-host, writes, pipelining or prepared-handle qualification.",
            Timing = "All8 workers warm then signal readiness; asynchronous start barrier. Wall time is release through completion of all workers. Latency spans every validated scalar42 read. Every worker/sample QPC start/end is retained. Cancel siblings on failure, drain workers, preserve partial samples, stop.",
            Gate = "Per API all3 arms must pass max/min block throughput spread<=25%. Against EACH baseline stream median throughput>=105%, paired wins>=5/6, median pooled blockp99<=baseline AND median worst-worker blockp99<=baseline. Require finite nonnegative monotonic client/server CPU snapshots, exact command/wire counts and zero errors. An API qualifies only if both baseline comparisons pass. No retry after failed gate.",
            Validation = "Each of8 workers completes4096 reads. Every timed block has32768 ExecuteSql requests/responses; no GetInfo/hello or extra HTTP calls. Streaming has32768 decoded SQL frames/responses and zero new HTTP RPCs. Before/after pool proof requires Web GetInfo8, native GetInfo8 and CommandSession8 shared by SDK/ADO. Source/binary hashes and working-tree state must remain unchanged.",
            Accounting = profileOnly
                ? "Diagnostic trace attached; no throughput qualification. Allocation counters are not collected in either client or daemon; no legacy allocation snapshot. CPU snapshots include bounded control-pipe work outside latency windows. No forced GC or ThreadPool/GC/OS/runtime tuning."
                : "Allocation counters are not collected in either client or daemon. Timing mode never invokes the legacy allocation snapshot. Process CPU snapshots include bounded control-pipe/counter overhead outside the latency window; require monotonic snapshots within and across blocks. No profiler, forced GC, stage probes or ThreadPool/GC/OS/runtime tuning.",
            Runtime = RuntimeInformation.FrameworkDescription,
            OS = RuntimeInformation.OSDescription,
            Environment.MachineName,
            Environment.ProcessorCount,
            System.Runtime.GCSettings.IsServerGC,
            Stopwatch.Frequency,
            GitHead = Git("rev-parse", "HEAD"),
            GitStatus = status,
            Sources = sources,
            Binaries = binaries,
        });
        File.WriteAllText(Path.Combine(output, "source.patch"), Git("diff", "--binary"));
        foreach (var file in sources)
        {
            string destination = Path.Combine(output, "source", file.Name);
            Directory.CreateDirectory(Path.GetDirectoryName(destination)!);
            File.Copy(Path.Combine(root, file.Name), destination);
        }
        Write(output, "status.json", new { State = "running", AllocationAccounting = "NotCollected", StartedUtc = DateTimeOffset.UtcNow });
        await using var host = await BenchmarkDaemonHost.StartAsync(true, null, ct);
        Write(output, "host-info.json", host.Info);
        var blocks = new List<TimingBlock>();
        var settling = new List<object>();
        var cpu = new TimingCpuLedger();
        try
        {
            await using (var seed = CSharpDbClient.Create(new()
            {
                Transport = CSharpDbTransport.Grpc,
                GrpcMode = CSharpDbGrpcMode.Native,
                Endpoint = host.NativeEndpoint.AbsoluteUri,
            }))
            {
                var result = await seed.ExecuteSqlAsync("CREATE TABLE network_read (id INTEGER PRIMARY KEY, value INTEGER); INSERT INTO network_read VALUES (1, 42)", ct);
                if (result.Error is not null) throw new InvalidOperationException(result.Error);
            }
            foreach (var arm in Arms)
            {
                await using var group = await Group.OpenAsync(host, arm, ct);
                var timer = Stopwatch.StartNew();
                var completed = new int[Workers];
                try
                {
                    await Task.WhenAll(group.Runners.Select(async (runner, worker) =>
                    {
                        do { await runner.ReadAsync(ct); completed[worker]++; }
                        while (timer.Elapsed < TimeSpan.FromSeconds(3));
                    }));
                }
                finally
                {
                    settling.Add(new { arm.Api, arm.Wire, Completed = completed, ElapsedSeconds = timer.Elapsed.TotalSeconds });
                    Write(output, "settling.json", settling);
                }
            }
            var settled = await host.CaptureTimingAsync(ct);
            ValidateTimingFamilies(settled);
            cpu.ObserveServer(settled.ServerCpuMs);
            Write(output, "pool-proof-before.json", settled);
            int[] order = [0, 1, 5, 2, 4, 3];
            for (int round = 0; round < Rounds; round++)
            {
                foreach (int position in order)
                {
                    TimingBlock block = await MeasureTimingAsync(host, Arms[(position + round) % Arms.Length], round + 1, output, cpu, ct);
                    blocks.Add(block);
                    Write(output, "blocks.json", blocks);
                    Console.WriteLine($"{block.Api,-3} {block.Wire,-6} round{block.Round}: {block.OpsPerSecond:N0} ops/sec; p99 {block.P99Us:N1} us; worst workerp99 {block.WorstWorkerP99Us:N1} us");
                }
            }
            var final = await host.CaptureTimingAsync(ct);
            ValidateTimingFamilies(final);
            cpu.ObserveServer(final.ServerCpuMs);
            Write(output, "pool-proof-after.json", final);
            if (!sources.SequenceEqual(SourceHashes(root)) || !binaries.SequenceEqual(BinaryHashes())
                || status != Git("status", "--porcelain=v1"))
                throw new InvalidOperationException("Source, binary, or working-tree state changed during the run.");
            if (blocks.Count != Rounds * Arms.Length || blocks.Any(x => x.Completed != Count || !x.CpuAccountingValid))
                throw new InvalidDataException("Timing qualification requires every fixed block and valid CPU accounting.");
            var summaries = blocks.GroupBy(x => new Arm(x.Api, x.Wire)).Select(g => new TimingSummary(g.Key.Api, g.Key.Wire,
                Median(g.Select(x => x.OpsPerSecond)), Median(g.Select(x => x.P99Us)), Median(g.Select(x => x.WorstWorkerP99Us)),
                g.Max(x => x.OpsPerSecond) / g.Min(x => x.OpsPerSecond) - 1,
                Median(g.Select(x => x.ClientCpuMs)), Median(g.Select(x => x.ServerCpuMs)), g.All(x => x.CpuAccountingValid))).ToArray();
            var gates = new List<TimingGate>();
            foreach (string api in new[] { "SDK", "ADO" })
            {
                var stream = summaries.Single(x => x.Api == api && x.Wire == "Stream");
                bool stable = summaries.Where(x => x.Api == api).All(x => x.Spread <= .25);
                bool validCpu = summaries.Where(x => x.Api == api).All(x => x.CpuAccountingValid);
                foreach (string baseline in new[] { "Web", "Native" })
                {
                    var other = summaries.Single(x => x.Api == api && x.Wire == baseline);
                    int wins = blocks.Where(x => x.Api == api && x.Wire == "Stream").Count(x => x.OpsPerSecond
                        > blocks.Single(b => b.Api == api && b.Wire == baseline && b.Round == x.Round).OpsPerSecond);
                    double gain = stream.MedianOpsPerSecond / other.MedianOpsPerSecond - 1;
                    bool passed = !profileOnly && stable && validCpu && gain >= .05 && wins >= 5
                        && stream.MedianP99Us <= other.MedianP99Us && stream.MedianWorstWorkerP99Us <= other.MedianWorstWorkerP99Us;
                    gates.Add(new(api, baseline, gain, wins, stable, stream.MedianP99Us / other.MedianP99Us,
                        stream.MedianWorstWorkerP99Us / other.MedianWorstWorkerP99Us, validCpu, passed));
                }
            }
            Write(output, "summary.json", new
            {
                Summaries = summaries, Gates = gates,
                ApiOutcomes = new[] { "SDK", "ADO" }.Select(api => new { Api = api, Qualified = gates.Where(x => x.Api == api).All(x => x.Passed) }),
                TotalOperations = blocks.Sum(x => x.Completed),
                SourcesAndBinariesUnchanged = true,
                WireCountsValidated = true,
                ProfileOnly = profileOnly,
                AllocationAccounting = "NotCollected",
                CpuAccountingValid = true,
            });
            Write(output, "status.json", new { State = "complete", FinishedUtc = DateTimeOffset.UtcNow, Blocks = blocks.Count,
                TotalOperations = blocks.Sum(x => x.Completed), AllocationAccounting = "NotCollected", CpuAccountingValid = true });
        }
        finally
        {
            await CSharpDbClient.ClearPoolAsync(host.CompatibilityEndpoint.AbsoluteUri);
            await CSharpDbClient.ClearPoolAsync(host.NativeEndpoint.AbsoluteUri);
        }
    }

    private static async Task<TimingBlock> MeasureTimingAsync(BenchmarkDaemonHost host, Arm arm, int round,
        string output, TimingCpuLedger cpu, CancellationToken ct)
    {
        await using var group = await Group.OpenAsync(host, arm, ct);
        await Task.WhenAll(group.Runners.Select(async runner =>
        {
            for (int i = 0; i < Warmup; i++) await runner.ReadAsync(ct);
        }));
        long[][] starts = Enumerable.Range(0, Workers).Select(_ => new long[PerWorker]).ToArray();
        long[][] ends = Enumerable.Range(0, Workers).Select(_ => new long[PerWorker]).ToArray();
        var completed = new int[Workers];
        var readyTicks = new long[Workers];
        var doneTicks = new long[Workers];
        var failures = new string?[Workers];
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ready = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int readyCount = 0;
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(ct);
        async Task Worker(int worker)
        {
            try
            {
                readyTicks[worker] = Stopwatch.GetTimestamp();
                if (Interlocked.Increment(ref readyCount) == Workers) ready.SetResult();
                await release.Task.WaitAsync(cancel.Token);
                for (int n = 0; n < PerWorker; n++)
                {
                    starts[worker][n] = Stopwatch.GetTimestamp();
                    await group.Runners[worker].ReadAsync(cancel.Token);
                    ends[worker][n] = Stopwatch.GetTimestamp();
                    completed[worker]++;
                }
            }
            catch (Exception error) { failures[worker] = error.ToString(); cancel.Cancel(); throw; }
            finally { doneTicks[worker] = Stopwatch.GetTimestamp(); }
        }
        Task[] workers = Enumerable.Range(0, Workers).Select(Worker).ToArray();
        using var process = Process.GetCurrentProcess();
        BenchmarkDaemonHost.TimingSnapshot? before = null, after = null;
        double? clientCpuBefore = null, clientCpuAfter = null;
        long blockStart = 0, blockEnd = 0;
        Exception? executionFailure = null, captureFailure = null, cpuFailure = null;
        try
        {
            await ready.Task.WaitAsync(ct);
            before = await host.CaptureTimingAsync(ct);
            clientCpuBefore = process.TotalProcessorTime.TotalMilliseconds;
            try { cpu.Observe(clientCpuBefore.Value, before.ServerCpuMs); }
            catch (Exception error) { cpuFailure = error; throw; }
            blockStart = Stopwatch.GetTimestamp();
            release.SetResult();
            await Task.WhenAll(workers);
            blockEnd = Stopwatch.GetTimestamp();
        }
        catch (Exception error)
        {
            executionFailure = error;
            cancel.Cancel();
            try { await Task.WhenAll(workers); } catch { /* Preserve the first failure and every worker's raw status. */ }
        }
        finally
        {
            if (blockStart != 0 && blockEnd == 0) blockEnd = Stopwatch.GetTimestamp();
            try
            {
                after = await host.CaptureTimingAsync(ct);
                clientCpuAfter = process.TotalProcessorTime.TotalMilliseconds;
            }
            catch (Exception error) { captureFailure = error; }
            try
            {
                if (before is null || after is null || clientCpuBefore is null || clientCpuAfter is null)
                    throw new InvalidDataException("Timing CPU snapshot is missing.");
                ValidateTimingCpu(clientCpuBefore.Value, clientCpuAfter.Value, before.ServerCpuMs, after.ServerCpuMs);
                cpu.Observe(clientCpuAfter.Value, after.ServerCpuMs);
            }
            catch (Exception error) { cpuFailure ??= error; }
            bool validCpu = cpuFailure is null && captureFailure is null;
            string stem = $"{arm.Api}-{arm.Wire}-{round}";
            File.WriteAllLines(Path.Combine(output, stem + ".csv"), new[] { "Worker,Sample,StartTicks,EndTicks,ElapsedTicks" }.Concat(
                Enumerable.Range(0, Workers).SelectMany(w => Enumerable.Range(0, completed[w])
                    .Select(n => $"{w + 1},{n + 1},{starts[w][n]},{ends[w][n]},{ends[w][n] - starts[w][n]}"))));
            Write(output, stem + ".metrics.json", new
            {
                Completed = completed.Sum(), Requested = Count, PerWorker = completed,
                ReadyTicks = readyTicks, DoneTicks = doneTicks, BlockStart = blockStart, BlockEnd = blockEnd,
                ElapsedTicks = blockEnd - blockStart,
                AllocationAccounting = "NotCollected",
                ClientCpuBeforeMs = FiniteTimingCpu(clientCpuBefore), ClientCpuAfterMs = FiniteTimingCpu(clientCpuAfter),
                ServerCpuBeforeMs = FiniteTimingCpu(before?.ServerCpuMs), ServerCpuAfterMs = FiniteTimingCpu(after?.ServerCpuMs),
                ClientCpuMs = validCpu ? clientCpuAfter - clientCpuBefore : null,
                ServerCpuMs = validCpu ? after!.ServerCpuMs - before!.ServerCpuMs : (double?)null,
                CpuMs = validCpu ? clientCpuAfter - clientCpuBefore + after!.ServerCpuMs - before!.ServerCpuMs : null,
                CpuAccountingValid = validCpu,
                CpuSnapshotsText = new
                {
                    ClientBefore = clientCpuBefore?.ToString("R", CultureInfo.InvariantCulture),
                    ClientAfter = clientCpuAfter?.ToString("R", CultureInfo.InvariantCulture),
                    ServerBefore = before?.ServerCpuMs.ToString("R", CultureInfo.InvariantCulture),
                    ServerAfter = after?.ServerCpuMs.ToString("R", CultureInfo.InvariantCulture),
                },
                WorkerFailures = failures, ExecutionFailure = executionFailure?.ToString(),
                CaptureFailure = captureFailure?.ToString(), CpuFailure = cpuFailure?.ToString(),
            });
            Write(output, stem + ".wire.json", new { Before = before?.Wire, After = after?.Wire, HttpBefore = before?.Http, HttpAfter = after?.Http });
        }
        if (executionFailure is not null) throw new InvalidOperationException("Timing block failed; partial raw samples retained without retry.", executionFailure);
        if (captureFailure is not null) throw new IOException("Timing counter snapshot failed; partial raw samples retained.", captureFailure);
        if (cpuFailure is not null) throw new InvalidDataException("CPU counters decreased or were invalid; raw snapshots retained. Qualification stopped without retry.", cpuFailure);
        if (completed.Any(n => n != PerWorker) || readyTicks.Any(t => t == 0 || t > blockStart)
            || doneTicks.Any(t => t < blockStart || t > blockEnd) || blockEnd <= blockStart)
            throw new InvalidOperationException("Worker readiness, timing or operation count mismatch.");
        for (int worker = 0; worker < Workers; worker++)
        {
            for (int n = 0; n < PerWorker; n++)
            {
                if (starts[worker][n] < blockStart || ends[worker][n] < starts[worker][n]
                    || ends[worker][n] > blockEnd || (n > 0 && starts[worker][n] < ends[worker][n - 1]))
                    throw new InvalidDataException("A raw command latency is outside its ordered timing block.");
            }
        }
        ValidateWire(before!.Http, after!.Http, after.Wire - before.Wire, arm, Count);
        var workerQuantiles = Enumerable.Range(0, Workers).Select(w =>
        {
            long[] ticks = Enumerable.Range(0, PerWorker).Select(n => ends[w][n] - starts[w][n]).Order().ToArray();
            return Percentile(ticks, .99);
        }).ToArray();
        long[] all = Enumerable.Range(0, Workers).SelectMany(w => Enumerable.Range(0, PerWorker)
            .Select(n => ends[w][n] - starts[w][n])).Order().ToArray();
        double clientCpu = clientCpuAfter!.Value - clientCpuBefore!.Value;
        double serverCpu = after.ServerCpuMs - before.ServerCpuMs;
        return new(arm.Api, arm.Wire, round, Count, Count * (double)Stopwatch.Frequency / (blockEnd - blockStart),
            Percentile(all, .50), Percentile(all, .95), Percentile(all, .99), workerQuantiles.Max(), clientCpu, serverCpu, clientCpu + serverCpu, true);
    }

    private static void ValidateTimingFamilies(BenchmarkDaemonHost.TimingSnapshot snapshot)
    {
        long web = snapshot.Http.Where(x => x.Method.EndsWith("/GetInfo", StringComparison.Ordinal) && x.Protocol == "HTTP/1.1").Sum(x => x.Value);
        long native = snapshot.Http.Where(x => x.Method.EndsWith("/GetInfo", StringComparison.Ordinal) && x.Protocol == "HTTP/2").Sum(x => x.Value);
        long streams = snapshot.Http.Where(x => x.Method.EndsWith("/CommandSession", StringComparison.Ordinal)).Sum(x => x.Value);
        if (web != Workers || native != Workers || streams != Workers
            || snapshot.Wire.UnaryOther != 2 * Workers || snapshot.Wire.StreamOther != 2 * Workers)
            throw new InvalidOperationException($"Expected exactly8 retained physical owners per shared pool family: Web{web}, Native{native}, Stream{streams}.");
    }

    private static void ValidateTimingCpu(double clientBefore, double clientAfter, double serverBefore, double serverAfter)
    {
        if (!double.IsFinite(clientBefore) || clientBefore < 0 || !double.IsFinite(clientAfter) || clientAfter < clientBefore
            || !double.IsFinite(serverBefore) || serverBefore < 0 || !double.IsFinite(serverAfter) || serverAfter < serverBefore)
            throw new InvalidDataException("CPU snapshots must be finite, nonnegative and monotonic.");
    }

    private static double? FiniteTimingCpu(double? value)
        => value.HasValue && double.IsFinite(value.Value) ? value : null;

    private sealed class TimingCpuLedger
    {
        private double _client, _server;
        internal void Observe(double client, double server)
        {
            ValidateTimingCpu(_client, client, _server, server);
            _client = client;
            _server = server;
        }
        internal void ObserveServer(double server)
        {
            ValidateTimingCpu(_client, _client, _server, server);
            _server = server;
        }
    }

    private sealed record TimingBlock(string Api, string Wire, int Round, int Completed, double OpsPerSecond,
        double P50Us, double P95Us, double P99Us, double WorstWorkerP99Us,
        double ClientCpuMs, double ServerCpuMs, double CpuMs, bool CpuAccountingValid)
    {
        public string AllocationAccounting => "NotCollected";
    }
    private sealed record TimingSummary(string Api, string Wire, double MedianOpsPerSecond, double MedianP99Us,
        double MedianWorstWorkerP99Us, double Spread, double MedianClientCpuMs, double MedianServerCpuMs, bool CpuAccountingValid)
    {
        public string AllocationAccounting => "NotCollected";
    }
    private sealed record TimingGate(string Api, string Baseline, double ThroughputChange, int PairedWins,
        bool AllApiArmsStable, double P99Ratio, double WorstWorkerP99Ratio, bool CpuAccountingValid, bool Passed);
}
