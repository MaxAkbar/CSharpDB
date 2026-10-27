using System.Diagnostics;
using System.Diagnostics.Tracing;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text.Json;
using CSharpDB.Client;
using static CSharpDB.NetworkBenchmarks.GrpcSessionBenchmark;

namespace CSharpDB.NetworkBenchmarks;

internal static partial class GrpcConcurrentSessionBenchmark
{
    private const int TimelinePerWorker = 32768;
    private const int TimelineCount = Workers * TimelinePerWorker;
    private const int TimelineChunkLength = 4096;
    private static readonly string[] TimelineOrder = ["SDK", "ADO", "ADO", "SDK", "SDK", "ADO", "ADO", "SDK"];

    public static async Task<int> RunBaselineTimelineAsync(string[] args)
    {
        string? output = null;
        bool ownsOutput = false;
        try
        {
            if (args.Length != 4 || Array.IndexOf(args, "--experiment") is not (0 or 2)
                || Array.IndexOf(args, "--output") is not (0 or 2)
                || args[Array.IndexOf(args, "--experiment") + 1] != "session-c8-baseline-timeline")
                throw new ArgumentException("Use --experiment session-c8-baseline-timeline --output <new external directory>; all diagnostic workload settings are fixed.");
            string root = Git("rev-parse", "--show-toplevel");
            output = Path.GetFullPath(args[Array.IndexOf(args, "--output") + 1]);
            if (output.Equals(root, StringComparison.OrdinalIgnoreCase)
                || output.StartsWith(root + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase))
                throw new ArgumentException("Timeline evidence must be outside the repository.");
            if (Directory.Exists(output)) throw new ArgumentException("Timeline evidence directory must be new.");
            Directory.CreateDirectory(output);
            ownsOutput = true;
            using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(3));
            using var workload = CancellationTokenSource.CreateLinkedTokenSource(deadline.Token);
            workload.CancelAfter(TimeSpan.FromSeconds(150));
            await RunBaselineTimelineCoreAsync(root, output, workload.Token, deadline.Token);
            return 0;
        }
        catch (Exception error)
        {
            if (ownsOutput && output is not null)
            {
                File.AppendAllText(Path.Combine(output, "failures.jsonl"), JsonSerializer.Serialize(new
                    { Utc = DateTimeOffset.UtcNow, Error = error.ToString() }) + Environment.NewLine);
                Write(output, "status.json", new { State = "failed", FinishedUtc = DateTimeOffset.UtcNow,
                    ProfileOnly = true, QualificationEligible = false, AllocationAccounting = "NotCollected" });
            }
            Console.Error.WriteLine(error);
            return 1;
        }
    }

    private static async Task RunBaselineTimelineCoreAsync(string root, string output,
        CancellationToken ct, CancellationToken shutdownCt)
    {
        string frozenRoot = Directory.GetParent(Path.TrimEndingDirectorySeparator(AppContext.BaseDirectory))!.FullName;
        string manifestPath = Path.Combine(frozenRoot, "snapshot.json");
        byte[] manifestBytes = File.ReadAllBytes(manifestPath);
        string manifestHash = Convert.ToHexString(SHA256.HashData(manifestBytes));
        var snapshot = JsonSerializer.Deserialize<FrozenSnapshot>(manifestBytes)
            ?? throw new InvalidDataException("Missing frozen timeline baseline manifest.");
        if (snapshot.Variant != "baseline" || snapshot.Sources.Length == 0 || snapshot.Binaries.Length == 0)
            throw new InvalidDataException("Timeline requires a frozen baseline build.");
        VerifyFrozenSources(frozenRoot, snapshot.Sources);
        var binaries = BinaryHashes();
        if (!snapshot.Binaries.SequenceEqual(binaries)) throw new InvalidDataException("Timeline frozen runtime hash mismatch.");
        var liveSources = SourceHashes(root);
        string liveHead = Git("rev-parse", "HEAD"), liveStatus = Git("status", "--porcelain=v1");
        Write(output, "protocol.json", new
        {
            Version = 1, Experiment = "baseline-stream-timeline-diagnostic", Variant = "baseline",
            ProfileOnly = true, QualificationEligible = false, AllocationAccounting = "NotCollected",
            Workers, PerWorker = TimelinePerWorker, CountPerPhase = TimelineCount, Phases = TimelineOrder.Length,
            TotalTimedOperations = TimelineCount * TimelineOrder.Length, WarmupPerWorker = Warmup,
            SettlingSecondsPerArm = 3, SettlingOrder = new[] { "SDK", "ADO" }, PhaseOrder = TimelineOrder,
            TimingChunkLength = TimelineChunkLength, PoolCapacity = Workers, DeadlineSeconds = 180,
            WorkloadDeadlineSeconds = 150, CleanupReserveSeconds = 30,
            CollectorHandshakeSeconds = 30, Query = "SELECT value FROM network_read WHERE id = 1",
            EventSource = "CSharpDB-Baseline-Timeline",
            PhaseEvent = "1:Phase(int phase,string api,string kind,string edge,long qpc,long completed)",
            ClockEvent = "2:Clock(int id,long qpc,long frequency)",
            Scope = "Baseline-only diagnostic timeline in separate real client/daemon processes. Eight held streaming rentals/commands, one outstanding read per worker, shared SDK pool. Every read validates scalar42. ProfileOnly=true; this run cannot qualify a product or performance improvement.",
            Timing = "Eight fixed phases SDK,ADO,ADO,SDK,SDK,ADO,ADO,SDK. Each phase warms256 reads/worker then retains32768 QPC start/end pairs per worker. One readiness/start barrier; cancel siblings and drain on failure. Raw timings use4096-element chunks, avoiding new large timing arrays. CSV/JSON writes occur after measured-end markers, outside timing windows.",
            Accounting = "Timing CPU/control-pipe snapshots only. No allocation counters, forced GC, runtime/ThreadPool/OS tuning, retries or exclusions. EventSource emits only phase boundaries and two QPC anchors per process. Sampled trace events are diagnostic observations, not exact allocations.",
            Lifecycle = "Host starts without inherited launch diagnostic ports. attach-ready precedes collectors-ready; no seed or settling occurs before collector acknowledgment. workload-complete precedes collectors-stopped, and the daemon remains alive until acknowledgment or bounded timeout for trace rundown.",
            FrozenRoot = frozenRoot, ManifestSha256 = manifestHash, Snapshot = snapshot, Binaries = binaries,
            LiveWorkspace = new { Root = root, GitHead = liveHead, GitStatus = liveStatus, Sources = liveSources },
            Stopwatch.Frequency, Runtime = RuntimeInformation.FrameworkDescription, OS = RuntimeInformation.OSDescription,
            Environment.MachineName, Environment.ProcessorCount, System.Runtime.GCSettings.IsServerGC,
        });
        File.WriteAllBytes(Path.Combine(output, "variant-manifest.json"), manifestBytes);
        foreach (var source in snapshot.Sources)
        {
            string destination = Path.Combine(output, "source", source.Name);
            Directory.CreateDirectory(Path.GetDirectoryName(destination)!);
            File.Copy(Path.Combine(frozenRoot, source.Name), destination);
        }
        Write(output, "status.json", new { State = "running", StartedUtc = DateTimeOffset.UtcNow,
            ProfileOnly = true, QualificationEligible = false, AllocationAccounting = "NotCollected" });
        _ = TimelineEventSource.Log;
        await using var host = await BenchmarkDaemonHost.StartAsync(true, null, ct);
        Write(output, "host-info.json", host.Info);
        Write(output, "attach-ready.json", new { ClientProcessId = Environment.ProcessId,
            ServerProcessId = host.Info.ServerProcessId, Qpc = Stopwatch.GetTimestamp(), QpcFrequency = Stopwatch.Frequency });
        var settling = new List<TimelineUntimed>();
        var phases = new List<TimelinePhase>();
        var clocks = new List<TimelineClockSync>();
        var cpu = new TimingCpuLedger();
        bool collectorsReady = false, stopWaitStarted = false, workloadCompleteWritten = false;
        try
        {
            await WaitTimelineCollectorsAsync(output, "collectors-ready.json", host, ct);
            collectorsReady = true;
            clocks.Add(await SynchronizeTimelineClockAsync(host, 1, ct));
            Write(output, "clock-sync.json", clocks);
            await using (var seed = CSharpDbClient.Create(new()
            {
                Transport = CSharpDbTransport.Grpc, GrpcMode = CSharpDbGrpcMode.Native,
                Endpoint = host.NativeEndpoint.AbsoluteUri,
            }))
            {
                var seeded = await seed.ExecuteSqlAsync("CREATE TABLE network_read (id INTEGER PRIMARY KEY, value INTEGER); INSERT INTO network_read VALUES (1, 42)", ct);
                if (seeded.Error is not null) throw new InvalidOperationException(seeded.Error);
            }
            foreach (string api in new[] { "SDK", "ADO" })
            {
                await using var group = await Group.OpenAsync(host, new(api, "Stream"), ct);
                settling.Add(await RunTimelineUntimedAsync(host, group, 0, api, "settling", output, cpu, ct));
                Write(output, "settling.json", settling);
            }
            var poolBefore = await host.CaptureTimingAsync(ct);
            ValidateTimelineFamily(poolBefore);
            cpu.ObserveServer(poolBefore.ServerCpuMs);
            Write(output, "pool-proof-before.json", poolBefore);
            for (int index = 0; index < TimelineOrder.Length; index++)
            {
                int phase = index + 1;
                string api = TimelineOrder[index];
                await using var group = await Group.OpenAsync(host, new(api, "Stream"), ct);
                await RunTimelineUntimedAsync(host, group, phase, api, "warmup", output, cpu, ct);
                phases.Add(await MeasureTimelineAsync(host, group, phase, api, output, cpu, ct));
                Write(output, "phases.json", phases);
                Console.WriteLine($"Timeline phase{phase} {api}: {phases[^1].Completed} reads; {phases[^1].OpsPerSecond:N0} ops/sec (diagnostic only)");
            }
            var poolAfter = await host.CaptureTimingAsync(ct);
            ValidateTimelineFamily(poolAfter);
            cpu.ObserveServer(poolAfter.ServerCpuMs);
            Write(output, "pool-proof-after.json", poolAfter);
            int timed = phases.Sum(x => x.Completed), warmup = Warmup * Workers * TimelineOrder.Length;
            int settled = settling.Sum(x => x.Completed);
            if (phases.Count != 8 || timed != 2097152 || phases.Any(x => x.Completed != TimelineCount))
                throw new InvalidDataException("Timeline fixed phase/read counts are incomplete.");
            ValidateWire(poolBefore.Http, poolAfter.Http, poolAfter.Wire - poolBefore.Wire, new("SDK", "Stream"), timed + warmup);
            if (poolAfter.Wire.StreamSql != settled + timed + warmup
                || poolAfter.Wire.StreamResponses != settled + timed + warmup + 16)
                throw new InvalidDataException("Timeline settling/warmup/measured wire totals differ.");
            if (!liveSources.SequenceEqual(SourceHashes(root)) || liveHead != Git("rev-parse", "HEAD")
                || liveStatus != Git("status", "--porcelain=v1") || !binaries.SequenceEqual(BinaryHashes())
                || manifestHash != Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(manifestPath))))
                throw new InvalidDataException("Timeline source, workspace, manifest or runtime changed.");
            VerifyFrozenSources(frozenRoot, snapshot.Sources);
            clocks.Add(await SynchronizeTimelineClockAsync(host, 2, ct));
            Write(output, "clock-sync.json", clocks);
            Write(output, "summary.json", new
            {
                ProfileOnly = true, QualificationEligible = false, AllocationAccounting = "NotCollected",
                Phases = phases, TotalTimedOperations = timed, TotalWarmupOperations = warmup, SettlingOperations = settled,
                SourcesAndBinariesUnchanged = true, WireCountsValidated = true, CpuAccountingValid = true,
            });
            Write(output, "workload-complete.json", new { State = "complete", Qpc = Stopwatch.GetTimestamp(),
                ClientProcessId = Environment.ProcessId, ServerProcessId = host.Info.ServerProcessId,
                Phases = phases.Count, TotalTimedOperations = timed, TotalWarmupOperations = warmup,
                ProfileOnly = true, QualificationEligible = false, DiagnosticIntegrityValid = true });
            workloadCompleteWritten = true;
            stopWaitStarted = true;
            await WaitTimelineCollectorsAsync(output, "collectors-stopped.json", host, shutdownCt);
            Write(output, "status.json", new { State = "complete", FinishedUtc = DateTimeOffset.UtcNow,
                Phases = phases.Count, TotalTimedOperations = timed, ProfileOnly = true, QualificationEligible = false,
                AllocationAccounting = "NotCollected", CpuAccountingValid = true, CollectorsStopped = true });
        }
        catch (Exception error)
        {
            if (!workloadCompleteWritten)
                Write(output, "workload-complete.json", new { State = "failed", Qpc = Stopwatch.GetTimestamp(),
                    ClientProcessId = Environment.ProcessId, ServerProcessId = host.Info.ServerProcessId,
                    ProfileOnly = true, QualificationEligible = false, DiagnosticIntegrityValid = false,
                    CollectorsReady = collectorsReady, Error = error.ToString() });
            if (!stopWaitStarted)
            {
                stopWaitStarted = true;
                try { await WaitTimelineCollectorsAsync(output, "collectors-stopped.json", host, shutdownCt); }
                catch (Exception stopError) { throw new AggregateException("Timeline workload and collector shutdown failed.", error, stopError); }
            }
            throw;
        }
        finally
        {
            await CSharpDbClient.ClearPoolAsync(host.CompatibilityEndpoint.AbsoluteUri);
            await CSharpDbClient.ClearPoolAsync(host.NativeEndpoint.AbsoluteUri);
        }
    }

    private static async Task<TimelineClockSync> SynchronizeTimelineClockAsync(BenchmarkDaemonHost host, int id, CancellationToken ct)
    {
        long before = Stopwatch.GetTimestamp();
        var server = await host.CaptureTimelineClockAsync(id, ct);
        long after = Stopwatch.GetTimestamp();
        TimelineEventSource.Log.Clock(id, after, Stopwatch.Frequency);
        if (server.Id != id || server.Frequency != Stopwatch.Frequency || server.Qpc < before || server.Qpc > after)
            throw new InvalidDataException("Timeline cross-process QPC anchor is inconsistent.");
        return new(id, before, after, after, server.Qpc, Stopwatch.Frequency, Environment.ProcessId, host.Info.ServerProcessId);
    }

    private static async Task WaitTimelineCollectorsAsync(string output, string name, BenchmarkDaemonHost host, CancellationToken ct)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(ct);
        deadline.CancelAfter(TimeSpan.FromSeconds(30));
        string path = Path.Combine(output, name);
        try
        {
            while (true)
            {
                deadline.Token.ThrowIfCancellationRequested();
                if (File.Exists(path))
                {
                    TimelineCollectors? signal = null;
                    try { signal = JsonSerializer.Deserialize<TimelineCollectors>(File.ReadAllText(path)); }
                    catch (IOException) { /* External writer may still be replacing its handshake file. */ }
                    catch (JsonException) { /* Do not accept a partially written handshake. */ }
                    if (signal is not null)
                    {
                        if (signal.ClientProcessId != Environment.ProcessId || signal.ServerProcessId != host.Info.ServerProcessId)
                            throw new InvalidDataException($"Wrong process identities in {name}.");
                        string errors = string.Join("; ", (signal.Errors ?? []).Take(8).Select(error =>
                            error is { Length: > 2048 } ? error[..2048] : error));
                        bool valid = name == "collectors-ready.json"
                            ? signal.State == "ready" && signal.Succeeded is not false
                            : signal.State == "stopped" && signal.Succeeded == true && signal.Outcome == "complete";
                        if (!valid || signal.Errors is { Length: > 0 })
                            throw new InvalidDataException($"Collector handshake {name} failed: State={signal.State}, Succeeded={signal.Succeeded}, Outcome={signal.Outcome}. {errors}");
                        return;
                    }
                }
                await Task.Delay(50, deadline.Token);
            }
        }
        catch (OperationCanceledException error) when (!ct.IsCancellationRequested)
        { throw new TimeoutException($"Timeline collector handshake {name} did not arrive within30 seconds.", error); }
    }

    private static async Task<TimelineUntimed> RunTimelineUntimedAsync(BenchmarkDaemonHost host, Group group,
        int phase, string api, string kind, string output, TimingCpuLedger cpu, CancellationToken ct)
    {
        var before = await host.CaptureTimingAsync(ct);
        ValidateTimelineFamily(before);
        cpu.ObserveServer(before.ServerCpuMs);
        var completed = new int[Workers];
        var failures = new string?[Workers];
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(ct);
        long start = Stopwatch.GetTimestamp(), end = 0;
        TimelineEventSource.Log.Phase(phase, api, kind, "start", start, 0);
        async Task Worker(int worker)
        {
            try
            {
                if (kind == "warmup")
                    for (int n = 0; n < Warmup; n++) { await group.Runners[worker].ReadAsync(cancel.Token); completed[worker]++; }
                else
                    do { await group.Runners[worker].ReadAsync(cancel.Token); completed[worker]++; }
                    while (Stopwatch.GetTimestamp() - start < 3 * Stopwatch.Frequency);
            }
            catch (Exception error) { failures[worker] = error.ToString(); cancel.Cancel(); throw; }
        }
        Exception? executionFailure = null, captureFailure = null;
        BenchmarkDaemonHost.TimingSnapshot? after = null;
        Task[] workers = Enumerable.Range(0, Workers).Select(Worker).ToArray();
        try { await Task.WhenAll(workers); }
        catch (Exception error)
        {
            executionFailure = error;
            cancel.Cancel();
            try { await Task.WhenAll(workers); } catch { /* Preserve all per-worker failures. */ }
        }
        finally
        {
            end = Stopwatch.GetTimestamp();
            TimelineEventSource.Log.Phase(phase, api, kind, "end", end, completed.Sum());
            try { after = await host.CaptureTimingAsync(ct); cpu.ObserveServer(after.ServerCpuMs); }
            catch (Exception error) { captureFailure = error; }
        }
        var result = new TimelineUntimed(phase, api, "Stream", kind, Workers, kind == "warmup" ? Warmup : null,
            kind == "settling" ? 3 : null, start, end, completed.Sum(), completed, failures, before, after,
            executionFailure?.ToString(), captureFailure?.ToString());
        Write(output, kind == "warmup" ? $"phase-{phase:D2}-{api}.warmup.json" : $"settling-{api}.json", result);
        if (executionFailure is not null) throw new InvalidOperationException($"Timeline {kind} failed without retry.", executionFailure);
        if (captureFailure is not null) throw new IOException($"Timeline {kind} snapshot failed.", captureFailure);
        if (completed.Any(x => kind == "warmup" ? x != Warmup : x <= 0) || end <= start
            || (kind == "settling" && end - start < 3 * Stopwatch.Frequency))
            throw new InvalidDataException($"Timeline {kind} operation counts/timestamps differ.");
        ValidateTimelineFamily(after!);
        ValidateWire(before.Http, after!.Http, after.Wire - before.Wire, new(api, "Stream"), completed.Sum());
        return result;
    }

    private static async Task<TimelinePhase> MeasureTimelineAsync(BenchmarkDaemonHost host, Group group,
        int phase, string api, string output, TimingCpuLedger cpu, CancellationToken ct)
    {
        static long[][][] Buffers() => Enumerable.Range(0, Workers).Select(_ =>
            Enumerable.Range(0, TimelinePerWorker / TimelineChunkLength).Select(_ => new long[TimelineChunkLength]).ToArray()).ToArray();
        long[][][] starts = Buffers(), ends = Buffers();
        var completed = new int[Workers];
        var readyTicks = new long[Workers];
        var doneTicks = new long[Workers];
        var failures = new string?[Workers];
        var ready = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int readyCount = 0;
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(ct);
        async Task Worker(int worker)
        {
            try
            {
                readyTicks[worker] = Stopwatch.GetTimestamp();
                if (Interlocked.Increment(ref readyCount) == Workers) ready.SetResult();
                await release.Task.WaitAsync(cancel.Token);
                for (int n = 0; n < TimelinePerWorker; n++)
                {
                    int chunk = n / TimelineChunkLength, offset = n % TimelineChunkLength;
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
        double? clientBefore = null, clientAfter = null;
        long blockStart = 0, blockEnd = 0;
        Exception? executionFailure = null, captureFailure = null, cpuFailure = null;
        try
        {
            await ready.Task.WaitAsync(ct);
            before = await host.CaptureTimingAsync(ct);
            ValidateTimelineFamily(before);
            clientBefore = process.TotalProcessorTime.TotalMilliseconds;
            cpu.Observe(clientBefore.Value, before.ServerCpuMs);
            blockStart = Stopwatch.GetTimestamp();
            TimelineEventSource.Log.Phase(phase, api, "measured", "start", blockStart, 0);
            release.SetResult();
            await Task.WhenAll(workers);
            blockEnd = Stopwatch.GetTimestamp();
        }
        catch (Exception error)
        {
            executionFailure = error;
            cancel.Cancel();
            try { await Task.WhenAll(workers); } catch { /* Keep the first error and every partial sample. */ }
        }
        finally
        {
            if (blockStart != 0 && blockEnd == 0) blockEnd = Stopwatch.GetTimestamp();
            if (blockStart != 0) TimelineEventSource.Log.Phase(phase, api, "measured", "end", blockEnd, completed.Sum());
            try { after = await host.CaptureTimingAsync(ct); clientAfter = process.TotalProcessorTime.TotalMilliseconds; }
            catch (Exception error) { captureFailure = error; }
            try
            {
                if (before is null || after is null || clientBefore is null || clientAfter is null)
                    throw new InvalidDataException("Timeline CPU snapshot missing.");
                ValidateTimingCpu(clientBefore.Value, clientAfter.Value, before.ServerCpuMs, after.ServerCpuMs);
                cpu.Observe(clientAfter.Value, after.ServerCpuMs);
            }
            catch (Exception error) { cpuFailure = error; }
            bool validCpu = cpuFailure is null && captureFailure is null;
            string stem = $"phase-{phase:D2}-{api}";
            using (var csv = new StreamWriter(Path.Combine(output, stem + ".csv")))
            {
                csv.WriteLine("Worker,Sample,StartTicks,EndTicks,ElapsedTicks");
                for (int worker = 0; worker < Workers; worker++)
                    for (int n = 0; n < completed[worker]; n++)
                    {
                        long start = starts[worker][n / TimelineChunkLength][n % TimelineChunkLength];
                        long end = ends[worker][n / TimelineChunkLength][n % TimelineChunkLength];
                        csv.WriteLine(FormattableString.Invariant($"{worker + 1},{n + 1},{start},{end},{end - start}"));
                    }
            }
            Write(output, stem + ".metrics.json", new
            {
                Phase = phase, Api = api, Wire = "Stream", Kind = "measured", Frequency = Stopwatch.Frequency,
                Workers, PerWorker = TimelinePerWorker, Requested = TimelineCount, Completed = completed.Sum(),
                PerWorkerCompleted = completed, ReadyTicks = readyTicks, DoneTicks = doneTicks,
                BlockStart = blockStart, BlockEnd = blockEnd, ElapsedTicks = blockEnd - blockStart,
                Before = before, After = after, AllocationAccounting = "NotCollected",
                ClientCpuBeforeMs = FiniteTimingCpu(clientBefore), ClientCpuAfterMs = FiniteTimingCpu(clientAfter),
                ServerCpuBeforeMs = FiniteTimingCpu(before?.ServerCpuMs), ServerCpuAfterMs = FiniteTimingCpu(after?.ServerCpuMs),
                ClientCpuMs = validCpu ? clientAfter - clientBefore : null,
                ServerCpuMs = validCpu ? after!.ServerCpuMs - before!.ServerCpuMs : (double?)null,
                CpuMs = validCpu ? clientAfter - clientBefore + after!.ServerCpuMs - before!.ServerCpuMs : null,
                CpuAccountingValid = validCpu, CpuSnapshotsText = new
                {
                    ClientBefore = clientBefore?.ToString("R", CultureInfo.InvariantCulture), ClientAfter = clientAfter?.ToString("R", CultureInfo.InvariantCulture),
                    ServerBefore = before?.ServerCpuMs.ToString("R", CultureInfo.InvariantCulture), ServerAfter = after?.ServerCpuMs.ToString("R", CultureInfo.InvariantCulture),
                },
                WorkerFailures = failures, ExecutionFailure = executionFailure?.ToString(),
                CaptureFailure = captureFailure?.ToString(), CpuFailure = cpuFailure?.ToString(),
            });
        }
        if (executionFailure is not null) throw new InvalidOperationException("Timeline phase failed; partial raw samples retained without retry.", executionFailure);
        if (captureFailure is not null) throw new IOException("Timeline phase snapshot failed; partial evidence retained.", captureFailure);
        if (cpuFailure is not null) throw new InvalidDataException("Timeline CPU accounting invalid; evidence retained.", cpuFailure);
        if (completed.Any(x => x != TimelinePerWorker) || readyTicks.Any(x => x == 0 || x > blockStart)
            || doneTicks.Any(x => x < blockStart || x > blockEnd) || blockEnd <= blockStart)
            throw new InvalidDataException("Timeline worker counts/readiness/completion differ.");
        for (int worker = 0; worker < Workers; worker++)
        {
            long previous = blockStart;
            for (int n = 0; n < TimelinePerWorker; n++)
            {
                long start = starts[worker][n / TimelineChunkLength][n % TimelineChunkLength];
                long end = ends[worker][n / TimelineChunkLength][n % TimelineChunkLength];
                if (start < previous || end <= start || end > doneTicks[worker]) throw new InvalidDataException("Timeline sample outside ordered worker envelope.");
                previous = end;
            }
        }
        ValidateTimelineFamily(after!);
        ValidateWire(before!.Http, after!.Http, after.Wire - before.Wire, new(api, "Stream"), TimelineCount);
        double clientCpu = clientAfter!.Value - clientBefore!.Value, serverCpu = after.ServerCpuMs - before.ServerCpuMs;
        return new(phase, api, "Stream", TimelineCount, blockStart, blockEnd,
            TimelineCount * (double)Stopwatch.Frequency / (blockEnd - blockStart), clientCpu, serverCpu, clientCpu + serverCpu);
    }

    private static void ValidateTimelineFamily(BenchmarkDaemonHost.TimingSnapshot snapshot)
    {
        if (!double.IsFinite(snapshot.ServerCpuMs) || snapshot.ServerCpuMs < 0) throw new InvalidDataException("Invalid timeline server CPU snapshot.");
        long sessions = 0, seed = 0;
        foreach (var row in snapshot.Http)
        {
            if (row.Value < 0 || row.Protocol != "HTTP/2" || !row.ContentType.StartsWith("application/grpc", StringComparison.Ordinal)
                || row.ContentType.Contains("grpc-web", StringComparison.Ordinal)) throw new InvalidDataException("Unexpected timeline HTTP family.");
            if (row.Method.EndsWith("/CommandSession", StringComparison.Ordinal)) sessions += row.Value;
            else if (row.Method.EndsWith("/ExecuteSql", StringComparison.Ordinal)) seed += row.Value;
            else throw new InvalidDataException("Unexpected timeline HTTP method.");
        }
        if (snapshot.Http.Length != 2 || sessions != 8 || seed != 1 || snapshot.Wire.UnarySql != 1
            || snapshot.Wire.UnaryOther != 0 || snapshot.Wire.StreamOther != 16
            || snapshot.Wire.StreamSql < 0 || snapshot.Wire.StreamResponses != snapshot.Wire.StreamSql + 16)
            throw new InvalidDataException("Timeline requires exactly8 shared stream owners and one seed RPC.");
    }

    private sealed record TimelineCollectors(int ClientProcessId, int ServerProcessId, string? State,
        bool? Succeeded, string? Outcome, string[]? Errors);
    private sealed record TimelineClockSync(int Id, long ClientBeforeQpc, long ClientAfterQpc, long ClientClockQpc,
        long ServerQpc, long Frequency, int ClientProcessId, int ServerProcessId);
    private sealed record TimelineUntimed(int Phase, string Api, string Wire, string Kind, int Workers, int? PerWorker,
        int? SettlingSeconds, long StartQpc, long EndQpc, int Completed, int[] PerWorkerCompleted, string?[] WorkerFailures,
        BenchmarkDaemonHost.TimingSnapshot Before, BenchmarkDaemonHost.TimingSnapshot? After, string? ExecutionFailure, string? CaptureFailure);
    private sealed record TimelinePhase(int Phase, string Api, string Wire, int Completed, long BlockStart, long BlockEnd,
        double OpsPerSecond, double ClientCpuMs, double ServerCpuMs, double CpuMs);
}

[EventSource(Name = "CSharpDB-Baseline-Timeline")]
internal sealed class TimelineEventSource : EventSource
{
    internal static readonly TimelineEventSource Log = new();
    private TimelineEventSource() { }

    [Event(1, Level = EventLevel.Informational)]
    public void Phase(int phase, string api, string kind, string edge, long qpc, long completed)
    {
        if (IsEnabled()) WriteEvent(1, phase, api, kind, edge, qpc, completed);
    }

    [Event(2, Level = EventLevel.Informational)]
    public void Clock(int id, long qpc, long frequency)
    {
        if (IsEnabled()) WriteEvent(2, new object[] { id, qpc, frequency });
    }
}
