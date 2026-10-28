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
    private const int LongPerWorker = 32768, LongCount = Workers * LongPerWorker, LongTimingChunkLength = 4096;

    // One fixed member of an identical-build A/A pair. The external validator
    // assesses repeatability; this mode never retries or qualifies a product change.
    public static async Task<int> RunLongPairedTimingAsync(string[] args)
    {
        string? output = null;
        bool ownsOutput = false;
        try
        {
            string[] flags = ["--experiment", "--variant", "--round", "--output"];
            if (args.Length != 8 || args.Where((_, i) => i % 2 == 0).Distinct().Count() != flags.Length
                || !flags.All(flag => args.Where((_, i) => i % 2 == 0).Contains(flag))
                || args[Array.IndexOf(args, "--experiment") + 1] != "session-c8-paired-long-timing")
                throw new ArgumentException("Use --experiment session-c8-paired-long-timing --variant baseline|candidate --round 1..6 --output <new directory>; all workload settings are fixed.");

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
            if (Directory.Exists(output) || File.Exists(output)) throw new ArgumentException("Evidence directory must be new.");
            Directory.CreateDirectory(output);
            ownsOutput = true;
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
            await RunLongPairedTimingCoreAsync(root, output, variant, round, timeout.Token);
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

    private static async Task RunLongPairedTimingCoreAsync(string root, string output, string variant, int round, CancellationToken ct)
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
            Experiment = "matched-C8-long-timing",
            Variant = variant,
            Round = round,
            ProfileOnly = false,
            QualificationEligible = false,
            AllocationAccounting = "NotCollected",
            Workers, PerWorker = LongPerWorker, Count = LongCount, Rounds, Warmup,
            TimingChunkLength = LongTimingChunkLength,
            DeadlineSeconds = 180,
            SettlingSecondsPerArm = 3,
            PoolCapacity = Workers,
            ApiOrder = apis,
            Query = "SELECT value FROM network_read WHERE id = 1",
            Scope = "Fixed longer-block identical-build A/A repeatability control. Baseline/candidate are labels for the same frozen sources and runtime. Separate client/server processes, actual daemon, warm cleartext loopback, one held rental/command per worker, one command at a time, same shared SDK-owned pool. No profiler, replay, exclusions or retries. No product improvement, TLS, remote-host, writes, pipelining or prepared-handle qualification.",
            Order = "External six-pair schedule: baseline first in rounds1,4,5; candidate first in rounds2,3,6. Within each member ADO first in rounds2,3,6, SDK first otherwise. This process measures one member only.",
            Timing = "All8 workers warm256 reads then signal readiness; asynchronous start barrier. Every block retains262144 validated scalar42 reads,32768 per worker, with actual per-read QPC start/end. Timing buffers use4096-element chunks. Wall time spans release through completion of all workers. Cancel siblings on failure, drain workers, preserve partial samples, stop. CSV writes, ordered-sample checks and bounded-merge nearest-rank quantiles occur after the window.",
            Gate = "The external fixed six-pair A/A validator assesses each API: each label max/min throughput spread<=25%, absolute candidate/baseline median throughput change<=5%, absolute median pooled blockp99 change<=10%, absolute median worst-worker blockp99 change<=10%. Paired wins are diagnostic only. All raw counts, finite nonnegative monotonic CPU, wire families and provenance must validate. No optimization or product promotion is evaluated, and previous failed studies remain failed. No retries or exclusions.",
            Validation = "Each of8 workers completes32768 reads per block. Two streaming blocks per member,524288 total reads;24 blocks across12 members,6291456 total. Every timed block has262144 SQL frames/responses and zero new HTTP RPCs. Before/after pool proof requires exactly8 retained CommandSession owners shared by SDK/ADO, zero GetInfo and16 other stream frames. Pool delta includes two2048-read warmups plus524288 timed reads. Frozen sources, manifest, binaries and live workspace must remain unchanged.",
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
        long settledOperations = 0;
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
                using var settleCancel = CancellationTokenSource.CreateLinkedTokenSource(ct);
                try
                {
                    await Task.WhenAll(group.Runners.Select(async (runner, worker) =>
                    {
                        try
                        {
                            do { await runner.ReadAsync(settleCancel.Token); completed[worker]++; }
                            while (timer.Elapsed < TimeSpan.FromSeconds(3));
                        }
                        catch { settleCancel.Cancel(); throw; }
                    }));
                }
                finally
                {
                    settledOperations += completed.Sum();
                    settling.Add(new { Api = api, Wire = "Stream", Completed = completed, ElapsedSeconds = timer.Elapsed.TotalSeconds });
                    Write(output, "settling.json", settling);
                }
            }
            var before = await host.CaptureTimingAsync(ct);
            ValidateLongPairedTimingStreamFamily(before);
            if (before.Wire.StreamSql != settledOperations) throw new InvalidDataException("Unexpected reads outside settling.");
            cpu.ObserveServer(before.ServerCpuMs);
            Write(output, "pool-proof-before.json", before);
            foreach (string api in apis)
            {
                TimingBlock block = await MeasureLongTimingAsync(host, new(api, "Stream"), round, output, cpu, ct);
                blocks.Add(block);
                Write(output, "blocks.json", blocks);
                Console.WriteLine($"{variant} {api} round{round}: {block.OpsPerSecond:N0} ops/sec; p99 {block.P99Us:N1} us; worst workerp99 {block.WorstWorkerP99Us:N1} us");
            }
            var after = await host.CaptureTimingAsync(ct);
            ValidateLongPairedTimingStreamFamily(after);
            cpu.ObserveServer(after.ServerCpuMs);
            Write(output, "pool-proof-after.json", after);
            ValidateWire(before.Http, after.Http, after.Wire - before.Wire, new("SDK", "Stream"), 2 * (LongCount + Workers * Warmup));
            if (!liveSources.SequenceEqual(SourceHashes(root)) || liveStatus != Git("status", "--porcelain=v1")
                || liveHead != Git("rev-parse", "HEAD") || !binaries.SequenceEqual(BinaryHashes())
                || manifestSha256 != Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(manifestPath))))
                throw new InvalidDataException("Live workspace, frozen manifest or frozen binaries changed during run.");
            VerifyFrozenSources(frozenRoot, snapshot.Sources);
            if (blocks.Count != apis.Length || blocks.Any(x => x.Completed != LongCount || !x.CpuAccountingValid))
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
                QualificationEligible = false,
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

    private static async Task<TimingBlock> MeasureLongTimingAsync(BenchmarkDaemonHost host, Arm arm, int round,
        string output, TimingCpuLedger cpu, CancellationToken ct)
    {
        await using var group = await Group.OpenAsync(host, arm, ct);
        await Task.WhenAll(group.Runners.Select(async runner =>
        {
            for (int i = 0; i < Warmup; i++) await runner.ReadAsync(ct);
        }));
        static long[][][] Buffers() => Enumerable.Range(0, Workers).Select(_ =>
            Enumerable.Range(0, LongPerWorker / LongTimingChunkLength).Select(_ => new long[LongTimingChunkLength]).ToArray()).ToArray();
        long[][][] starts = Buffers(), ends = Buffers();
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
                for (int n = 0; n < LongPerWorker; n++)
                {
                    int chunk = n / LongTimingChunkLength, offset = n % LongTimingChunkLength;
                    starts[worker][chunk][offset] = Stopwatch.GetTimestamp();
                    await group.Runners[worker].ReadAsync(cancel.Token);
                    ends[worker][chunk][offset] = Stopwatch.GetTimestamp();
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
            ValidateLongPairedTimingStreamFamily(before);
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
            try { await Task.WhenAll(workers); } catch { /* Preserve first failure and every worker's raw status. */ }
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
                    throw new InvalidDataException("Long timing CPU snapshot is missing.");
                ValidateTimingCpu(clientCpuBefore.Value, clientCpuAfter.Value, before.ServerCpuMs, after.ServerCpuMs);
                cpu.Observe(clientCpuAfter.Value, after.ServerCpuMs);
            }
            catch (Exception error) { cpuFailure ??= error; }
            bool validCpu = cpuFailure is null && captureFailure is null;
            string stem = $"{arm.Api}-{arm.Wire}-{round}";
            using (var csv = new StreamWriter(Path.Combine(output, stem + ".csv")))
            {
                csv.WriteLine("Worker,Sample,StartTicks,EndTicks,ElapsedTicks");
                for (int worker = 0; worker < Workers; worker++)
                    for (int n = 0; n < completed[worker]; n++)
                    {
                        long start = starts[worker][n / LongTimingChunkLength][n % LongTimingChunkLength];
                        long end = ends[worker][n / LongTimingChunkLength][n % LongTimingChunkLength];
                        csv.WriteLine(FormattableString.Invariant($"{worker + 1},{n + 1},{start},{end},{end - start}"));
                    }
            }
            Write(output, stem + ".metrics.json", new
            {
                Completed = completed.Sum(), Requested = LongCount, PerWorker = completed,
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
        if (executionFailure is not null) throw new InvalidOperationException("Long timing block failed; partial raw samples retained without retry.", executionFailure);
        if (captureFailure is not null) throw new IOException("Long timing counter snapshot failed; partial raw samples retained.", captureFailure);
        if (cpuFailure is not null) throw new InvalidDataException("Long timing CPU counters were invalid; raw snapshots retained without retry.", cpuFailure);
        if (completed.Any(n => n != LongPerWorker) || readyTicks.Any(t => t == 0 || t > blockStart)
            || doneTicks.Any(t => t < blockStart || t > blockEnd) || blockEnd <= blockStart)
            throw new InvalidOperationException("Long timing worker readiness, timing or operation count mismatch.");
        for (int worker = 0; worker < Workers; worker++)
        {
            long previousEnd = blockStart;
            for (int n = 0; n < LongPerWorker; n++)
            {
                long start = starts[worker][n / LongTimingChunkLength][n % LongTimingChunkLength];
                long end = ends[worker][n / LongTimingChunkLength][n % LongTimingChunkLength];
                if (start < previousEnd || end < start || end > doneTicks[worker])
                    throw new InvalidDataException("A long timing raw latency is outside its ordered worker envelope.");
                previousEnd = end;
            }
        }
        ValidateLongPairedTimingStreamFamily(after!);
        ValidateWire(before!.Http, after!.Http, after.Wire - before.Wire, arm, LongCount);
        // Raw CSV and envelope checks are complete. Reuse the start chunks as
        // latency scratch; all sorts/merges run after the measured CPU window.
        for (int worker = 0; worker < Workers; worker++)
            for (int chunk = 0; chunk < starts[worker].Length; chunk++)
            {
                for (int n = 0; n < LongTimingChunkLength; n++) starts[worker][chunk][n] = ends[worker][chunk][n] - starts[worker][chunk][n];
                Array.Sort(starts[worker][chunk]);
            }
        var workerQuantiles = starts.Select(chunks => LongTimingQuantiles(chunks, LongPerWorker).P99).ToArray();
        var pooled = LongTimingQuantiles(starts.SelectMany(chunks => chunks).ToArray(), LongCount);
        double clientCpu = clientCpuAfter!.Value - clientCpuBefore!.Value;
        double serverCpu = after.ServerCpuMs - before.ServerCpuMs;
        return new(arm.Api, arm.Wire, round, LongCount, LongCount * (double)Stopwatch.Frequency / (blockEnd - blockStart),
            pooled.P50, pooled.P95, pooled.P99, workerQuantiles.Max(), clientCpu, serverCpu, clientCpu + serverCpu, true);
    }

    private static LongTimingPercentiles LongTimingQuantiles(IReadOnlyList<long[]> chunks, int count)
    {
        if (count <= 0 || chunks.Count == 0 || chunks.Any(x => x.Length == 0) || chunks.Sum(x => x.Length) != count)
            throw new InvalidDataException("Invalid long timing quantile chunk counts.");
        var queue = new PriorityQueue<(int Chunk, int Offset), long>(chunks.Count);
        for (int chunk = 0; chunk < chunks.Count; chunk++) queue.Enqueue((chunk, 0), chunks[chunk][0]);
        int rank50 = (int)Math.Ceiling(count * .50), rank95 = (int)Math.Ceiling(count * .95), rank99 = (int)Math.Ceiling(count * .99);
        long p50 = 0, p95 = 0, p99 = 0;
        for (int rank = 1; rank <= rank99; rank++)
        {
            if (!queue.TryDequeue(out var item, out long ticks)) throw new InvalidDataException("Truncated long timing quantile merge.");
            if (rank == rank50) p50 = ticks;
            if (rank == rank95) p95 = ticks;
            if (rank == rank99) p99 = ticks;
            int next = item.Offset + 1;
            if (next < chunks[item.Chunk].Length) queue.Enqueue((item.Chunk, next), chunks[item.Chunk][next]);
        }
        return new(p50 * 1_000_000.0 / Stopwatch.Frequency, p95 * 1_000_000.0 / Stopwatch.Frequency, p99 * 1_000_000.0 / Stopwatch.Frequency);
    }

    // Optional local reflection probe; never called by a measurement process.
    public static object CheckLongTimingPercentileMerge()
    {
        long[][] vectors =
        [
            [0],
            Enumerable.Repeat(17L, 131).ToArray(),
            Enumerable.Range(0, 257).Select(x => (long)(256 - x)).ToArray(),
            Enumerable.Range(0, 391).Select(x => x % 9 == 0 ? 0L : x % 7 == 0 ? 123456789012L : x % 13).ToArray(),
        ];
        var results = vectors.Select(vector =>
        {
            var chunks = vector.Chunk(11).Select(chunk => chunk.Order().ToArray()).ToArray();
            var actual = LongTimingQuantiles(chunks, vector.Length);
            var sorted = vector.Order().ToArray();
            var expected = new LongTimingPercentiles(Percentile(sorted, .50), Percentile(sorted, .95), Percentile(sorted, .99));
            if (actual != expected) throw new InvalidDataException("Long timing nearest-rank merge differs from the legacy percentile helper.");
            return new { Samples = vector.Length, Chunks = chunks.Length, Actual = actual, Expected = expected, Passed = true };
        }).ToArray();
        return new { Passed = true, NearestRank = "Ceiling(count * quantile) - 1", Cases = results, NetworkOperations = 0 };
    }

    private readonly record struct LongTimingPercentiles(double P50, double P95, double P99);

    private static void ValidateLongPairedTimingStreamFamily(BenchmarkDaemonHost.TimingSnapshot snapshot)
    {
        long streams = snapshot.Http.Where(x => x.Method.EndsWith("/CommandSession", StringComparison.Ordinal)).Sum(x => x.Value);
        long infos = snapshot.Http.Where(x => x.Method.EndsWith("/GetInfo", StringComparison.Ordinal)).Sum(x => x.Value);
        long seed = snapshot.Http.Where(x => x.Method.EndsWith("/ExecuteSql", StringComparison.Ordinal)).Sum(x => x.Value);
        if (snapshot.Http.Length != 2 || snapshot.Http.Any(x => x.Protocol != "HTTP/2" || x.Value < 0
                || !x.ContentType.StartsWith("application/grpc", StringComparison.Ordinal) || x.ContentType.Contains("grpc-web", StringComparison.Ordinal))
            || streams != Workers || seed != 1 || infos != 0 || snapshot.Wire.UnarySql != 1 || snapshot.Wire.UnaryOther != 0
            || snapshot.Wire.StreamOther != 2 * Workers || snapshot.Wire.StreamSql < 0
            || snapshot.Wire.StreamResponses != snapshot.Wire.StreamSql + 2 * Workers)
            throw new InvalidDataException("Expected exactly eight retained streaming owners shared by SDK/ADO, without unary GetInfo.");
    }
}
