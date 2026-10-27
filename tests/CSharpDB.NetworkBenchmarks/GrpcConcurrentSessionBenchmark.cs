using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text.Json;
using CSharpDB.Client;
using static CSharpDB.NetworkBenchmarks.GrpcSessionBenchmark;

namespace CSharpDB.NetworkBenchmarks;

/// <summary>Fixed C8 qualification: one held rental and command per worker, no replay.</summary>
internal static partial class GrpcConcurrentSessionBenchmark
{
    private const int Workers = 8, PerWorker = 4096, Count = Workers * PerWorker, Rounds = 6, Warmup = 256;
    private static readonly Arm[] Arms = [new("SDK", "Web"), new("SDK", "Native"), new("SDK", "Stream"),
        new("ADO", "Web"), new("ADO", "Native"), new("ADO", "Stream")];

    public static async Task<int> RunAsync(string[] args, bool profileOnly = false)
    {
        string? output = null;
        bool ownsOutput = false;
        try
        {
            if (args.Length != 4 || Array.IndexOf(args, "--output") is not (0 or 2) || Array.IndexOf(args, "--experiment") is not (0 or 2))
                throw new ArgumentException("Use --experiment session-c8 --output <new directory>; all workload settings are fixed.");
            output = Path.GetFullPath(args[Array.IndexOf(args, "--output") + 1]);
            string root = Git("rev-parse", "--show-toplevel");
            if (output.Equals(root, StringComparison.OrdinalIgnoreCase) || output.StartsWith(root + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase))
                throw new ArgumentException("Keep generated evidence outside the repository.");
            if (Directory.Exists(output)) throw new ArgumentException("Evidence directory must be new.");
            Directory.CreateDirectory(output); ownsOutput = true;
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(5));
            await RunCoreAsync(root, output, profileOnly, timeout.Token);
            return 0;
        }
        catch (Exception error)
        {
            if (ownsOutput && output is not null)
            {
                File.AppendAllText(Path.Combine(output, "failures.jsonl"), JsonSerializer.Serialize(new { Utc = DateTimeOffset.UtcNow, Error = error.ToString() }) + Environment.NewLine);
                Write(output, "status.json", new { State = "failed", FinishedUtc = DateTimeOffset.UtcNow });
            }
            Console.Error.WriteLine(error); return 1;
        }
    }

    private static async Task RunCoreAsync(string root, string output, bool profileOnly, CancellationToken ct)
    {
        var sources = SourceHashes(root); var binaries = BinaryHashes(); string status = Git("status", "--porcelain=v1");
        Write(output, "protocol.json", new
        {
            Version = profileOnly ? 2 : 1,
            Experiment = profileOnly ? "persistent-command-session-isolated-C8-profile" : "persistent-command-session-isolated-C8",
            ProfileOnly = profileOnly,
            ProfileScope = profileOnly ? "DIAGNOSTIC ONLY. Parent client sampled managed/external stacks and allocation ticks with custom measured-block markers. No performance qualification or promotion. External samples include waiting; managed samples are not actual CPU time. Exact process CPU and allocation deltas recorded separately for client/server. Preserve prior C8 failure unchanged." : null,
            Workers,
            PerWorker,
            Count,
            Rounds,
            Warmup,
            SettlingSecondsPerArm = 3,
            PoolCapacity = Workers,
            Query = "SELECT value FROM network_read WHERE id = 1",
            Arms,
            Order = "Six Williams rounds, base indices0,1,5,2,4,3 shifted by round modulo6. Every arm occupies every position once. No retries/exclusions.",
            Scope = "Separate client/server processes, actual daemon, cleartext loopback, warm point reads. Eight simultaneous held SDK rentals or ADO connections/commands; each sends one command at a time. Shared SDK-owned pool capacity8 per mode. Rental and warmup outside timing. Backend serializes ordinary SQL on its shared client lock, so this includes server queueing. No TLS, remote-host, write, pipelining or prepared-handle qualification.",
            Timing = "All8 workers warm then signal readiness; asynchronous start barrier. Wall time is release through completion of all workers. Latency spans each validated read. Store every worker/sample QPC start/end; never sum latencies for throughput. Cancel siblings on failure, drain every worker, preserve partial samples, stop.",
            Gate = "Per API all3 arms must pass max/min block throughput spread<=25%. Against EACH baseline stream median throughput>=105%, paired wins>=5/6, median pooled blockp99<=baseline AND median worst-worker blockp99<=baseline. All results/counts correct, zero errors. An API qualifies only if both baseline comparisons pass. No rerun after failed gate.",
            Validation = "Each of8 workers completes4096 reads, every scalar42. Every timed block has32768 ExecuteSql requests/responses; no GetInfo/hello or extra HTTP calls. Streaming has32768 decoded SQL frames and responses, zero new HTTP RPCs. All3 pool families retain8 physical owners; SDK and ADO reuse the same families. Verify GetInfo Web8 Native8, CommandSession8 before and after all timed blocks.",
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
            Accounting = "CPU/allocations include client and server and bounded counter-snapshot/control-pipe overhead; snapshots outside latency window. "
                + (profileOnly ? "Client profiler enabled; diagnostic results cannot qualify a performance change. " : "No profiler. ")
                + "No stage probe or ThreadPool/GC/OS tuning. All prior C1 and diagnostic evidence remains separate.",
        });
        File.WriteAllText(Path.Combine(output, "source.patch"), Git("diff", "--binary"));
        foreach (var file in sources)
        {
            string destination = Path.Combine(output, "source", file.Name); Directory.CreateDirectory(Path.GetDirectoryName(destination)!);
            File.Copy(Path.Combine(root, file.Name), destination);
        }
        Write(output, "status.json", new { State = "running", StartedUtc = DateTimeOffset.UtcNow });
        if (profileOnly) DiagnosticEvents.Log.Clock(Stopwatch.GetTimestamp(), Stopwatch.Frequency);
        await using var host = await BenchmarkDaemonHost.StartAsync(true, null, ct);
        Write(output, "host-info.json", host.Info);
        var blocks = new List<Block>(); var settling = new List<object>();
        try
        {
            await using (var seed = CSharpDbClient.Create(new() { Transport = CSharpDbTransport.Grpc, GrpcMode = CSharpDbGrpcMode.Native, Endpoint = host.NativeEndpoint.AbsoluteUri }))
            {
                var result = await seed.ExecuteSqlAsync("CREATE TABLE network_read (id INTEGER PRIMARY KEY, value INTEGER); INSERT INTO network_read VALUES (1, 42)", ct);
                if (result.Error is not null) throw new InvalidOperationException(result.Error);
            }
            foreach (var arm in Arms)
            {
                await using var group = await Group.OpenAsync(host, arm, ct);
                var timer = Stopwatch.StartNew(); var completed = new int[Workers];
                try { await Task.WhenAll(group.Runners.Select(async (runner, worker) =>
                {
                    do { await runner.ReadAsync(ct); completed[worker]++; } while (timer.Elapsed < TimeSpan.FromSeconds(3));
                })); }
                finally
                {
                    settling.Add(new { arm.Api, arm.Wire, Completed = completed, ElapsedSeconds = timer.Elapsed.TotalSeconds });
                    Write(output, "settling.json", settling);
                }
            }
            var settled = await host.CaptureAsync(ct); ValidateFamilies(settled);
            Write(output, "pool-proof-before.json", settled);
            int[] order = [0, 1, 5, 2, 4, 3];
            for (int round = 0; round < Rounds; round++) foreach (int position in order)
            {
                Block block = await MeasureAsync(host, Arms[(position + round) % 6], round + 1, output, profileOnly, ct);
                blocks.Add(block); Write(output, "blocks.json", blocks);
                Console.WriteLine($"{block.Api,-3} {block.Wire,-6} round{block.Round}: {block.OpsPerSecond:N0} ops/sec; p99 {block.P99Us:N1} us; worst workerp99 {block.WorstWorkerP99Us:N1} us");
            }
            var final = await host.CaptureAsync(ct); ValidateFamilies(final); Write(output, "pool-proof-after.json", final);
            if (!sources.SequenceEqual(SourceHashes(root)) || !binaries.SequenceEqual(BinaryHashes()) || status != Git("status", "--porcelain=v1"))
                throw new InvalidOperationException("Source, binary, or working-tree state changed during the run.");
            var summaries = blocks.GroupBy(x => new Arm(x.Api, x.Wire)).Select(g => new Summary(g.Key.Api, g.Key.Wire,
                Median(g.Select(x => x.OpsPerSecond)), Median(g.Select(x => x.P99Us)), Median(g.Select(x => x.WorstWorkerP99Us)),
                g.Max(x => x.OpsPerSecond) / g.Min(x => x.OpsPerSecond) - 1,
                g.All(x => x.AccountingValid) ? Median(g.Select(x => x.AllocatedBytes / (double)Count)) : null,
                g.All(x => x.AccountingValid))).ToArray();
            var gates = new List<Gate>();
            foreach (string api in new[] { "SDK", "ADO" })
            {
                var stream = summaries.Single(x => x.Api == api && x.Wire == "Stream");
                bool stable = summaries.Where(x => x.Api == api).All(x => x.Spread <= .25);
                foreach (string baseline in new[] { "Web", "Native" })
                {
                    var other = summaries.Single(x => x.Api == api && x.Wire == baseline);
                    int wins = blocks.Where(x => x.Api == api && x.Wire == "Stream").Count(x => x.OpsPerSecond > blocks.Single(b => b.Api == api && b.Wire == baseline && b.Round == x.Round).OpsPerSecond);
                    double gain = stream.MedianOpsPerSecond / other.MedianOpsPerSecond - 1;
                    bool passed = !profileOnly && stable && gain >= .05 && wins >= 5 && stream.MedianP99Us <= other.MedianP99Us && stream.MedianWorstWorkerP99Us <= other.MedianWorstWorkerP99Us;
                    gates.Add(new(api, baseline, gain, wins, stable, stream.MedianP99Us / other.MedianP99Us, stream.MedianWorstWorkerP99Us / other.MedianWorstWorkerP99Us, passed));
                }
            }
            Write(output, "summary.json", new { Summaries = summaries, Gates = gates, ApiOutcomes = new[] { "SDK", "ADO" }.Select(api => new { Api = api, Qualified = gates.Where(x => x.Api == api).All(x => x.Passed) }), TotalOperations = blocks.Sum(x => x.Completed), SourcesAndBinariesUnchanged = true, WireCountsValidated = true, ProfileOnly = profileOnly });
            Write(output, "status.json", new { State = "complete", FinishedUtc = DateTimeOffset.UtcNow, Blocks = blocks.Count, TotalOperations = blocks.Sum(x => x.Completed) });
        }
        finally
        {
            await CSharpDbClient.ClearPoolAsync(host.CompatibilityEndpoint.AbsoluteUri);
            await CSharpDbClient.ClearPoolAsync(host.NativeEndpoint.AbsoluteUri);
        }
    }

    private static async Task<Block> MeasureAsync(BenchmarkDaemonHost host, Arm arm, int round, string output, bool profileOnly, CancellationToken ct)
    {
        await using var group = await Group.OpenAsync(host, arm, ct);
        await Task.WhenAll(group.Runners.Select(async runner => { for (int i = 0; i < Warmup; i++) await runner.ReadAsync(ct); }));
        long[][] starts = Enumerable.Range(0, Workers).Select(_ => new long[PerWorker]).ToArray();
        long[][] ends = Enumerable.Range(0, Workers).Select(_ => new long[PerWorker]).ToArray();
        var completed = new int[Workers]; var readyTicks = new long[Workers]; var doneTicks = new long[Workers]; var failures = new string?[Workers];
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ready = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously); int readyCount = 0;
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(ct);
        async Task Worker(int worker)
        {
            try
            {
                readyTicks[worker] = Stopwatch.GetTimestamp(); if (Interlocked.Increment(ref readyCount) == Workers) ready.SetResult();
                await release.Task.WaitAsync(cancel.Token);
                for (int n = 0; n < PerWorker; n++)
                {
                    starts[worker][n] = Stopwatch.GetTimestamp(); await group.Runners[worker].ReadAsync(cancel.Token);
                    ends[worker][n] = Stopwatch.GetTimestamp(); completed[worker]++;
                }
            }
            catch (Exception error) { failures[worker] = error.ToString(); cancel.Cancel(); throw; }
            finally { doneTicks[worker] = Stopwatch.GetTimestamp(); }
        }
        Task[] workers = Enumerable.Range(0, Workers).Select(Worker).ToArray();
        await ready.Task.WaitAsync(ct);
        using var process = Process.GetCurrentProcess();
        BenchmarkDaemonHost.Snapshot before;
        try { before = await host.CaptureAsync(ct); } catch { cancel.Cancel(); try { await Task.WhenAll(workers); } catch { } throw; }
        long clientAllocatedBefore = GC.GetTotalAllocatedBytes(true);
        double clientCpuBefore = process.TotalProcessorTime.TotalMilliseconds;
        long allocated = clientAllocatedBefore + before.ServerAllocatedBytes;
        double cpu = clientCpuBefore + before.ServerCpuMs;
        if (profileOnly) DiagnosticEvents.Log.Block(arm.Api, arm.Wire, round, 1, Stopwatch.GetTimestamp());
        long blockStart = Stopwatch.GetTimestamp(), blockEnd = blockStart;
        BenchmarkDaemonHost.Snapshot? after = null; Exception? workerFailure = null; Exception? captureFailure = null;
        bool accountingValid = false;
        try { release.SetResult(); await Task.WhenAll(workers); }
        catch (Exception error) { workerFailure = error; throw; }
        finally
        {
            blockEnd = Stopwatch.GetTimestamp();
            if (profileOnly) DiagnosticEvents.Log.Block(arm.Api, arm.Wire, round, 0, Stopwatch.GetTimestamp());
            try { after = await host.CaptureAsync(ct); } catch (Exception error) { captureFailure = error; }
            long clientAllocatedAfter = GC.GetTotalAllocatedBytes(true);
            double clientCpuAfter = process.TotalProcessorTime.TotalMilliseconds;
            long clientAllocated = clientAllocatedAfter - clientAllocatedBefore;
            long serverAllocated = (after?.ServerAllocatedBytes ?? before.ServerAllocatedBytes) - before.ServerAllocatedBytes;
            double clientCpu = clientCpuAfter - clientCpuBefore;
            double serverCpu = (after?.ServerCpuMs ?? before.ServerCpuMs) - before.ServerCpuMs;
            accountingValid = after is not null && clientAllocated >= 0 && serverAllocated >= 0
                && double.IsFinite(clientCpu) && clientCpu >= 0 && double.IsFinite(serverCpu) && serverCpu >= 0;
            allocated = clientAllocated + serverAllocated;
            cpu = clientCpu + serverCpu;
            string stem = $"{arm.Api}-{arm.Wire}-{round}";
            File.WriteAllLines(Path.Combine(output, stem + ".csv"), new[] { "Worker,Sample,StartTicks,EndTicks,ElapsedTicks" }.Concat(
                Enumerable.Range(0, Workers).SelectMany(w => Enumerable.Range(0, completed[w]).Select(n => $"{w + 1},{n + 1},{starts[w][n]},{ends[w][n]},{ends[w][n] - starts[w][n]}"))));
            Write(output, stem + ".metrics.json", new { Completed = completed.Sum(), Requested = Count, PerWorker = completed, ReadyTicks = readyTicks, DoneTicks = doneTicks, BlockStart = blockStart, BlockEnd = blockEnd, ElapsedTicks = blockEnd - blockStart, AllocatedBytes = allocated, CpuMs = cpu, ClientAllocatedBytes = clientAllocated, ServerAllocatedBytes = serverAllocated, ClientCpuMs = clientCpu, ServerCpuMs = serverCpu, ClientAllocatedBefore = clientAllocatedBefore, ClientAllocatedAfter = clientAllocatedAfter, ServerAllocatedBefore = before.ServerAllocatedBytes, ServerAllocatedAfter = after?.ServerAllocatedBytes, ClientCpuBeforeMs = clientCpuBefore, ClientCpuAfterMs = clientCpuAfter, ServerCpuBeforeMs = before.ServerCpuMs, ServerCpuAfterMs = after?.ServerCpuMs, AccountingValid = accountingValid, WorkerFailures = failures, CaptureFailure = captureFailure?.ToString() });
            Write(output, stem + ".wire.json", new { Before = before.Wire, After = after?.Wire, HttpBefore = before.Http, HttpAfter = after?.Http });
            if (captureFailure is not null && workerFailure is null) throw new IOException("Counter snapshot failed; partial raw samples retained.", captureFailure);
            if (!profileOnly && !accountingValid && workerFailure is null)
                throw new InvalidDataException("Resource counters decreased or were invalid; raw counters retained. Qualification stopped without retry.");
        }
        if (completed.Any(n => n != PerWorker) || readyTicks.Any(t => t > blockStart)) throw new InvalidOperationException("Worker readiness or operation count mismatch.");
        ValidateWire(before.Http, after!.Http, after.Wire - before.Wire, arm, Count);
        var workerQuantiles = Enumerable.Range(0, Workers).Select(w => { long[] ticks = Enumerable.Range(0, PerWorker).Select(n => ends[w][n] - starts[w][n]).Order().ToArray(); return Percentile(ticks, .99); }).ToArray();
        long[] all = Enumerable.Range(0, Workers).SelectMany(w => Enumerable.Range(0, PerWorker).Select(n => ends[w][n] - starts[w][n])).Order().ToArray();
        return new(arm.Api, arm.Wire, round, Count, Count * (double)Stopwatch.Frequency / (blockEnd - blockStart), Percentile(all, .50), Percentile(all, .95), Percentile(all, .99), workerQuantiles.Max(), allocated, cpu, accountingValid);
    }

    private static void ValidateFamilies(BenchmarkDaemonHost.Snapshot snapshot)
    {
        long web = snapshot.Http.Where(x => x.Method.EndsWith("/GetInfo", StringComparison.Ordinal) && x.Protocol == "HTTP/1.1").Sum(x => x.Value);
        long native = snapshot.Http.Where(x => x.Method.EndsWith("/GetInfo", StringComparison.Ordinal) && x.Protocol == "HTTP/2").Sum(x => x.Value);
        long streams = snapshot.Http.Where(x => x.Method.EndsWith("/CommandSession", StringComparison.Ordinal)).Sum(x => x.Value);
        if (web != Workers || native != Workers || streams != Workers || snapshot.Wire.UnaryOther != 2 * Workers || snapshot.Wire.StreamOther != 2 * Workers)
            throw new InvalidOperationException($"Expected exactly8 retained physical owners per pool family shared by SDK/ADO: Web{web}, Native{native}, Stream{streams}.");
    }
    private sealed class Group : IAsyncDisposable
    {
        public Runner[] Runners { get; private init; } = [];
        public static async Task<Group> OpenAsync(BenchmarkDaemonHost host, Arm arm, CancellationToken ct)
        {
            Task<Runner>[] opens = Enumerable.Range(0, Workers).Select(_ => Runner.OpenAsync(host, arm, ct, Workers)).ToArray();
            try { return new() { Runners = await Task.WhenAll(opens) }; }
            catch
            {
                foreach (var open in opens)
                    if (open.IsCompletedSuccessfully)
                        try { await open.Result.DisposeAsync(); } catch { /* Preserve the opening error; retire all successful rentals. */ }
                throw;
            }
        }
        public async ValueTask DisposeAsync()
        {
            List<Exception>? errors = null;
            foreach (var runner in Runners)
            {
                try { await runner.DisposeAsync(); }
                catch (Exception error) { (errors ??= []).Add(error); }
            }
            if (errors is not null) throw new AggregateException("Failed to return benchmark rentals.", errors);
        }
    }
    private sealed record Summary(string Api, string Wire, double MedianOpsPerSecond, double MedianP99Us, double MedianWorstWorkerP99Us, double Spread, double? BytesPerOp, bool AccountingValid);
    private sealed record Gate(string Api, string Baseline, double ThroughputChange, int PairedWins, bool AllApiArmsStable, double P99Ratio, double WorstWorkerP99Ratio, bool Passed);
    private sealed record Block(string Api, string Wire, int Round, int Completed, double OpsPerSecond, double P50Us, double P95Us, double P99Us, double WorstWorkerP99Us, long AllocatedBytes, double CpuMs, bool AccountingValid);
}
