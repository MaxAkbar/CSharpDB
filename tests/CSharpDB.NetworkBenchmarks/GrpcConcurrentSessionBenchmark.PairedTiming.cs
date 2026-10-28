using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text.Json;
using CSharpDB.Client;
using static CSharpDB.NetworkBenchmarks.GrpcSessionBenchmark;

namespace CSharpDB.NetworkBenchmarks;

internal static partial class GrpcConcurrentSessionBenchmark
{
    // One process measures one member of a predeclared old/new pair. The external
    // six-pair schedule and validator decide causality; this mode never retries.
    public static async Task<int> RunPairedTimingAsync(string[] args)
    {
        string? output = null;
        bool ownsOutput = false;
        try
        {
            string[] flags = ["--experiment", "--variant", "--round", "--output"];
            if (args.Length != 8 || args.Where((_, i) => i % 2 == 0).Distinct().Count() != flags.Length
                || !flags.All(flag => args.Where((_, i) => i % 2 == 0).Contains(flag))
                || args[Array.IndexOf(args, "--experiment") + 1] != "session-c8-paired-timing")
                throw new ArgumentException("Use --experiment session-c8-paired-timing --variant baseline|candidate --round 1..6 --output <new directory>; all workload settings are fixed.");

            string variant = args[Array.IndexOf(args, "--variant") + 1];
            if (variant is not ("baseline" or "candidate")
                || !int.TryParse(args[Array.IndexOf(args, "--round") + 1], NumberStyles.None, CultureInfo.InvariantCulture, out int round)
                || round is < 1 or > 6)
                throw new ArgumentException("Invalid variant or round.");

            string root = Git("rev-parse", "--show-toplevel");
            output = Path.GetFullPath(args[Array.IndexOf(args, "--output") + 1]);
            if (output.Equals(root, StringComparison.OrdinalIgnoreCase)
                || output.StartsWith(root + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase))
                throw new ArgumentException("Evidence must be outside the repository.");
            if (Directory.Exists(output)) throw new ArgumentException("Evidence directory must be new.");
            Directory.CreateDirectory(output);
            ownsOutput = true;
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
            await RunPairedTimingCoreAsync(root, output, variant, round, timeout.Token);
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

    private static async Task RunPairedTimingCoreAsync(string root, string output, string variant, int round, CancellationToken ct)
    {
        string frozenRoot = Directory.GetParent(Path.TrimEndingDirectorySeparator(AppContext.BaseDirectory))!.FullName;
        string manifestPath = Path.Combine(frozenRoot, "snapshot.json");
        byte[] manifestBytes = File.ReadAllBytes(manifestPath);
        string manifestSha256 = Convert.ToHexString(SHA256.HashData(manifestBytes));
        var snapshot = JsonSerializer.Deserialize<FrozenSnapshot>(manifestBytes)
            ?? throw new InvalidDataException("Missing frozen build manifest.");
        if (snapshot.Variant != variant || snapshot.Sources.Length == 0 || snapshot.Binaries.Length == 0)
            throw new InvalidDataException("Wrong frozen variant manifest.");
        VerifyFrozenSources(frozenRoot, snapshot.Sources);
        var binaries = BinaryHashes();
        if (!snapshot.Binaries.SequenceEqual(binaries)) throw new InvalidDataException("Frozen binary hash mismatch.");
        var liveSources = SourceHashes(root);
        string liveStatus = Git("status", "--porcelain=v1");
        string liveHead = Git("rev-parse", "HEAD");
        string[] apis = round is 2 or 3 or 6 ? ["ADO", "SDK"] : ["SDK", "ADO"];
        Write(output, "protocol.json", new
        {
            Version = 1,
            Experiment = "matched-C8-timing",
            Variant = variant,
            Round = round,
            ProfileOnly = false,
            AllocationAccounting = "NotCollected",
            Workers, PerWorker, Count, Rounds, Warmup,
            SettlingSecondsPerArm = 3,
            PoolCapacity = Workers,
            ApiOrder = apis,
            Query = "SELECT value FROM network_read WHERE id = 1",
            Scope = "Causal comparison of frozen baseline/candidate streaming builds. Separate client/server processes, actual daemon, warm cleartext loopback, one held rental/command per worker, one command at a time, same shared SDK-owned pool. No profiler, replay, exclusions or retries. No TLS, remote-host, writes, pipelining or prepared-handle qualification.",
            Order = "External six-pair schedule: baseline first in rounds1,4,5; candidate first in rounds2,3,6. Within each member ADO first in rounds2,3,6, SDK first otherwise. This process measures one member only.",
            Timing = "All8 workers warm256 reads then signal readiness; asynchronous start barrier. Every block retains32768 validated scalar42 reads with per-worker/sample QPC start/end. Wall time spans release through completion of all workers. Cancel siblings on failure, drain workers, preserve partial samples, stop.",
            Gate = "External six-pair validator per API: each variant max/min throughput spread<=25%, candidate median throughput>=105% baseline, >=5/6 paired wins, median pooled blockp99 and median worst-worker blockp99 no worse. Require finite nonnegative monotonic CPU, exact command/wire counts, unchanged source/binary provenance and zero errors. This member alone does not qualify a change. No retries after a failed gate.",
            Validation = "Each of8 workers completes4096 reads per block. Two streaming blocks per member,65536 total reads. Every timed block has32768 SQL frames/responses and zero new HTTP RPCs. Before/after pool proof requires exactly8 retained CommandSession owners shared by SDK/ADO, zero GetInfo and16 other stream frames for hello/reset. Frozen sources, manifest, binaries and live workspace must remain unchanged.",
            Accounting = "Allocation counters are not collected in either client or daemon. Only timing CPU snapshots are used; the legacy allocation snapshot is never invoked. Bounded control-pipe/counter work is outside latency windows. No forced GC, stage probes, ThreadPool/GC/OS/runtime tuning or profiling.",
            Provenance = "Frozen source and binary hashes identify this variant. LiveWorkspace is only a stability check and does not identify the baseline/candidate build sources. The exact manifest and frozen sources are copied alongside the samples.",
            FrozenRoot = frozenRoot,
            ManifestSha256 = manifestSha256,
            Snapshot = snapshot,
            Binaries = binaries,
            LiveWorkspace = new { Root = root, GitHead = liveHead, GitStatus = liveStatus, Sources = liveSources },
            Stopwatch.Frequency,
            Runtime = RuntimeInformation.FrameworkDescription,
            OS = RuntimeInformation.OSDescription,
            Environment.MachineName,
            Environment.ProcessorCount,
            System.Runtime.GCSettings.IsServerGC,
        });
        File.WriteAllBytes(Path.Combine(output, "variant-manifest.json"), manifestBytes);
        foreach (var file in snapshot.Sources)
        {
            string destination = Path.Combine(output, "source", file.Name);
            Directory.CreateDirectory(Path.GetDirectoryName(destination)!);
            File.Copy(Path.Combine(frozenRoot, file.Name), destination);
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
            foreach (string api in apis)
            {
                await using var group = await Group.OpenAsync(host, new(api, "Stream"), ct);
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
                    settling.Add(new { Api = api, Wire = "Stream", Completed = completed, ElapsedSeconds = timer.Elapsed.TotalSeconds });
                    Write(output, "settling.json", settling);
                }
            }
            var before = await host.CaptureTimingAsync(ct);
            ValidatePairedTimingStreamFamily(before);
            cpu.ObserveServer(before.ServerCpuMs);
            Write(output, "pool-proof-before.json", before);
            foreach (string api in apis)
            {
                TimingBlock block = await MeasureTimingAsync(host, new(api, "Stream"), round, output, cpu, ct);
                blocks.Add(block);
                Write(output, "blocks.json", blocks);
                Console.WriteLine($"{variant} {api} round{round}: {block.OpsPerSecond:N0} ops/sec; p99 {block.P99Us:N1} us; worst workerp99 {block.WorstWorkerP99Us:N1} us");
            }
            var after = await host.CaptureTimingAsync(ct);
            ValidatePairedTimingStreamFamily(after);
            cpu.ObserveServer(after.ServerCpuMs);
            Write(output, "pool-proof-after.json", after);
            if (!liveSources.SequenceEqual(SourceHashes(root)) || liveStatus != Git("status", "--porcelain=v1")
                || liveHead != Git("rev-parse", "HEAD") || !binaries.SequenceEqual(BinaryHashes())
                || manifestSha256 != Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(manifestPath))))
                throw new InvalidDataException("Live workspace, frozen manifest or frozen binaries changed during run.");
            VerifyFrozenSources(frozenRoot, snapshot.Sources);
            if (blocks.Count != apis.Length || blocks.Any(x => x.Completed != Count || !x.CpuAccountingValid))
                throw new InvalidDataException("Paired timing requires both fixed blocks and valid CPU accounting.");
            Write(output, "summary.json", new
            {
                Variant = variant,
                Round = round,
                Blocks = blocks,
                TotalOperations = blocks.Sum(x => x.Completed),
                SourcesAndBinariesUnchanged = true,
                WireCountsValidated = true,
                ProfileOnly = false,
                AllocationAccounting = "NotCollected",
                CpuAccountingValid = true,
            });
            Write(output, "status.json", new
            {
                State = "complete",
                FinishedUtc = DateTimeOffset.UtcNow,
                Blocks = blocks.Count,
                TotalOperations = blocks.Sum(x => x.Completed),
                AllocationAccounting = "NotCollected",
                CpuAccountingValid = true,
            });
        }
        finally
        {
            await CSharpDbClient.ClearPoolAsync(host.CompatibilityEndpoint.AbsoluteUri);
            await CSharpDbClient.ClearPoolAsync(host.NativeEndpoint.AbsoluteUri);
        }
    }

    private static void ValidatePairedTimingStreamFamily(BenchmarkDaemonHost.TimingSnapshot snapshot)
    {
        long streams = snapshot.Http.Where(x => x.Method.EndsWith("/CommandSession", StringComparison.Ordinal)).Sum(x => x.Value);
        long infos = snapshot.Http.Where(x => x.Method.EndsWith("/GetInfo", StringComparison.Ordinal)).Sum(x => x.Value);
        if (streams != Workers || infos != 0 || snapshot.Wire.UnaryOther != 0 || snapshot.Wire.StreamOther != 2 * Workers)
            throw new InvalidDataException("Expected exactly eight retained streaming owners shared by SDK/ADO, without unary GetInfo.");
    }
}
