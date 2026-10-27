using System.Diagnostics;
using System.Globalization;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using System.Text.Json;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using CSharpDB.Data;
using static CSharpDB.NetworkBenchmarks.GrpcSessionBenchmark;

namespace CSharpDB.NetworkBenchmarks;

/// <summary>Describes the cost of a complete pooled request relative to a held rental.</summary>
internal static class GrpcPooledLifecycleBenchmark
{
    private const int Workers = 8, PerWorker = 4096, Count = Workers * PerWorker, Warmup = 256;
    private const string Query = "SELECT value FROM network_read WHERE id = 1";
    private static readonly ArmDefinition[] Arms =
        [new("SDK-Held", "SDK", false), new("SDK-Lifecycle", "SDK", true),
         new("ADO-Held", "ADO", false), new("ADO-Lifecycle", "ADO", true)];
    private static readonly int[][] Schedule =
        [[0, 1, 3, 2], [1, 2, 0, 3], [2, 3, 1, 0], [3, 0, 2, 1],
         [3, 0, 2, 1], [2, 3, 1, 0], [1, 2, 0, 3], [0, 1, 3, 2]];

    internal static async Task<int> RunAsync(string[] args)
    {
        string? output = null;
        bool ownsOutput = false;
        try
        {
            int outputIndex = Array.IndexOf(args, "--output"), experimentIndex = Array.IndexOf(args, "--experiment");
            if (args.Length != 4 || outputIndex is not (0 or 2) || experimentIndex is not (0 or 2)
                || outputIndex == experimentIndex || args[experimentIndex + 1] != "pooled-lifecycle-c8")
                throw new ArgumentException("Use --experiment pooled-lifecycle-c8 --output <new directory>; workload settings are fixed.");
            string root = Path.GetFullPath(Git("rev-parse", "--show-toplevel"));
            output = Path.GetFullPath(args[outputIndex + 1]);
            if (output.Equals(root, StringComparison.OrdinalIgnoreCase)
                || output.StartsWith(root + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase))
                throw new ArgumentException("Keep evidence outside the repository.");
            if (Directory.Exists(output) || File.Exists(output)) throw new ArgumentException("Evidence path must be new.");
            CheckEnvironment();
            Directory.CreateDirectory(output);
            ownsOutput = true;
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(300));
            await RunCoreAsync(root, output, deadline.Token);
            return 0;
        }
        catch (Exception error)
        {
            if (ownsOutput && output is not null)
            {
                File.AppendAllText(Path.Combine(output, "failures.jsonl"),
                    JsonSerializer.Serialize(new { Utc = DateTimeOffset.UtcNow, Error = error.ToString() }) + Environment.NewLine);
                Write(output, "status.json", new { State = "failed", DiagnosticOnly = true, QualificationEligible = false,
                    AllocationAccounting = "NotCollected", FinishedUtc = DateTimeOffset.UtcNow });
            }
            Console.Error.WriteLine(error);
            return 1;
        }
    }

    private static async Task RunCoreAsync(string root, string output, CancellationToken ct)
    {
        Hash[] sources = SourceHashes(root), binaries = BinaryHashes();
        string head = Git("rev-parse", "HEAD"), status = Git("status", "--porcelain=v1");
        Write(output, "protocol.json", new
        {
            Version = 1, Experiment = "pooled-lifecycle-c8", DiagnosticOnly = true, QualificationEligible = false,
            AllocationAccounting = "NotCollected", Workers, PerWorker, Count, Rounds = Schedule.Length, Warmup,
            SettlingSecondsPerArm = 3, PoolCapacity = Workers, TotalOperations = Schedule.Length * Arms.Length * Count,
            Query, Arms, Schedule, WorkingDirectory = Environment.CurrentDirectory,
            Scope = "Separate actual daemon, native HTTP/2 streaming, one outstanding read per worker. SDK and ADO share eight physical owners. Cleartext loopback only.",
            Timing = "Lifecycle samples include rent/open, one validated read, and awaited dispose/close. Held rental and command creation are outside timing. Throughput is barrier release through completion of all eight workers; nearest-rank pooled and worst-worker p99.",
            Prewarm = "Concurrently rent and hold eight SDK owners, read once each, then release. Concurrently open and hold eight ADO connections from the same pool, read once each, then release. Sixteen prewarm SQL reads are outside measurement.",
            WarmupMeaning = "256 complete operations per worker before each block; settle each of four arms for three seconds once before all measured blocks.",
            Order = "Four Williams rows from base [0,1,3,2] shifted modulo4; repeat those rows in reverse row order. Every arm occupies each position twice. No retries or exclusions.",
            Conditions = "Require client and exact owned daemon Normal priority before every measured block and after all blocks; read-only checks, no priority changes. Reject inherited hooks, benchmark-parent, diagnostic ports and enabled CLR profiling. No allocation counters, profiler, forced GC or runtime/OS tuning.",
            Gates = "Descriptive only: each arm has fixed max/min block throughput spread <=25%. Report lifecycle/held ratios within each API. No product improvement or cross-historical qualification.",
            Validation = "Every timed block has32768 SQL frames and responses, zero other frames and zero new HTTP calls. Cumulative HTTP is one unary seed plus eight CommandSession calls; stream hello/GetInfo frames16. Stop on any SQL/value/count/CPU/source/runtime/condition failure.",
            DeadlineSeconds = 300, WorkerDrainSeconds = 15, CleanupSeconds = 20,
            Runtime = RuntimeInformation.FrameworkDescription, OS = RuntimeInformation.OSDescription,
            Environment.MachineName, Environment.ProcessorCount, System.Runtime.GCSettings.IsServerGC,
            Stopwatch.Frequency, GitHead = head, GitStatus = status, Sources = sources, Binaries = binaries,
        });
        File.WriteAllText(Path.Combine(output, "source.patch"), Git("diff", "--binary"));
        foreach (Hash source in sources)
        {
            string destination = Path.Combine(output, "source", source.Name);
            Directory.CreateDirectory(Path.GetDirectoryName(destination)!);
            File.Copy(Path.Combine(root, source.Name), destination, false);
        }
        var checks = new List<object>();
        var blocks = new List<Block>();
        BenchmarkDaemonHost? host = null;
        DateTime? daemonCreationUtc = null;
        Exception? failure = null;
        try
        {
            RecordConditions(output, checks, "ClientBeforeDaemon", null);
            host = await BenchmarkDaemonHost.StartAsync(true, null, ct);
            using (var daemon = Process.GetProcessById(host.Info.ServerProcessId)) daemonCreationUtc = daemon.StartTime.ToUniversalTime();
            Write(output, "host-info.json", host.Info);
            RecordConditions(output, checks, "AfterDaemonStart", host, daemonCreationUtc);
            var options = new CSharpDbClientOptions { Transport = CSharpDbTransport.Grpc, GrpcMode = CSharpDbGrpcMode.Native,
                GrpcCallMode = CSharpDbGrpcCallMode.Streaming, Endpoint = host.NativeEndpoint.AbsoluteUri };
            string connectionString = $"Transport=Grpc;Grpc Mode=Native;Grpc Call Mode=Streaming;Endpoint={host.NativeEndpoint};Pooling=true;Max Pool Size={Workers}";
            await using (var seed = CSharpDbClient.Create(new() { Transport = CSharpDbTransport.Grpc,
                GrpcMode = CSharpDbGrpcMode.Native, Endpoint = host.NativeEndpoint.AbsoluteUri }))
            {
                SqlExecutionResult result = await seed.ExecuteSqlAsync(
                    "CREATE TABLE network_read (id INTEGER PRIMARY KEY, value INTEGER); INSERT INTO network_read VALUES (1, 42)", ct);
                if (result.Error is not null) throw new InvalidDataException(result.Error);
            }
            var seedSnapshot = await host.CaptureTimingAsync(ct);
            await using (var group = await OpenGroupAsync(Arms[0], options, connectionString, ct))
                await RunParallelAsync(async (worker, token) => await group[worker].ReadAsync(token), ct);
            var sdkPrewarm = await host.CaptureTimingAsync(ct);
            ValidateOwners(sdkPrewarm);
            await using (var group = await OpenGroupAsync(Arms[2], options, connectionString, ct))
                await RunParallelAsync(async (worker, token) => await group[worker].ReadAsync(token), ct);
            var adoPrewarm = await host.CaptureTimingAsync(ct);
            ValidateOwners(adoPrewarm);
            ValidateWire(sdkPrewarm.Http, adoPrewarm.Http, adoPrewarm.Wire - sdkPrewarm.Wire, new("ADO", "Stream"), Workers);
            Write(output, "prewarm.json", new { Seed = seedSnapshot, Sdk = sdkPrewarm, Ado = adoPrewarm, SqlReads = 2 * Workers, SharedOwners = Workers });
            if (seedSnapshot.Wire != new Counts(1, 0, 0, 0, 0) || sdkPrewarm.Wire.StreamSql != Workers
                || adoPrewarm.Wire.StreamSql != 2 * Workers) throw new InvalidDataException("Prewarm SQL/control counts differ.");
            var settling = new List<object>();
            foreach (ArmDefinition arm in Arms)
            {
                await using var group = await OpenGroupAsync(arm, options, connectionString, ct);
                var completed = new int[Workers];
                var errors = new string?[Workers];
                var timer = Stopwatch.StartNew();
                try
                {
                    await RunParallelAsync(async (worker, token) =>
                    {
                        try { do { await group[worker].ReadAsync(token); completed[worker]++; } while (timer.Elapsed.TotalSeconds < 3); }
                        catch (Exception error) { errors[worker] = error.ToString(); throw; }
                    }, ct);
                }
                finally
                {
                    settling.Add(new { Arm = arm.Id, arm.Api, arm.Lifecycle, Completed = completed,
                        ElapsedSeconds = timer.Elapsed.TotalSeconds, WorkerFailures = errors });
                    Write(output, "settling.json", settling);
                }
            }
            var before = await host.CaptureTimingAsync(ct);
            ValidateOwners(before);
            Write(output, "pool-proof-before.json", before);
            var cpu = new CpuLedger();
            cpu.ObserveServer(before.ServerCpuMs);
            for (int round = 0; round < Schedule.Length; round++)
                foreach (int index in Schedule[round])
                {
                    ArmDefinition arm = Arms[index];
                    Block block = await MeasureAsync(host, daemonCreationUtc.Value, arm, round + 1, options, connectionString, output, checks, cpu, ct);
                    blocks.Add(block);
                    Write(output, "blocks.json", blocks);
                    Console.WriteLine($"{arm.Id,-14} round{round + 1}: {block.OpsPerSecond:N0} ops/sec; p99 {block.P99Us:N1} us; worst worker p99 {block.WorstWorkerP99Us:N1} us");
                }
            var after = await host.CaptureTimingAsync(ct);
            ValidateOwners(after);
            cpu.ObserveServer(after.ServerCpuMs);
            Write(output, "pool-proof-after.json", after);
            RecordConditions(output, checks, "AfterAllBlocks", host, daemonCreationUtc);
            if (blocks.Count != Schedule.Length * Arms.Length || blocks.Any(x => x.Completed != Count || !x.CpuAccountingValid))
                throw new InvalidDataException("Every fixed block and CPU snapshot is required.");
            if (!sources.SequenceEqual(SourceHashes(root)) || !binaries.SequenceEqual(BinaryHashes())
                || head != Git("rev-parse", "HEAD") || status != Git("status", "--porcelain=v1"))
                throw new InvalidDataException("Source, runtime, HEAD or working-tree state changed.");
            var summaries = Arms.Select(arm =>
            {
                Block[] measured = blocks.Where(x => x.Arm == arm.Id).ToArray();
                double spread = measured.Max(x => x.OpsPerSecond) / measured.Min(x => x.OpsPerSecond) - 1;
                return new { Arm = arm.Id, arm.Api, arm.Lifecycle, MedianOpsPerSecond = Median(measured.Select(x => x.OpsPerSecond)),
                    MedianP99Us = Median(measured.Select(x => x.P99Us)), MedianWorstWorkerP99Us = Median(measured.Select(x => x.WorstWorkerP99Us)),
                    Spread = spread, Stable = spread <= .25 };
            }).ToArray();
            var ratios = new[] { "SDK", "ADO" }.Select(api =>
            {
                var held = summaries.Single(x => x.Api == api && !x.Lifecycle);
                var lifecycle = summaries.Single(x => x.Api == api && x.Lifecycle);
                return new { Api = api, LifecycleToHeldOpsPerSecond = lifecycle.MedianOpsPerSecond / held.MedianOpsPerSecond,
                    LifecycleToHeldP99 = lifecycle.MedianP99Us / held.MedianP99Us,
                    LifecycleToHeldWorstWorkerP99 = lifecycle.MedianWorstWorkerP99Us / held.MedianWorstWorkerP99Us };
            }).ToArray();
            Write(output, "summary.json", new { Arms = summaries, Ratios = ratios, DiagnosticOnly = true, QualificationEligible = false,
                AllocationAccounting = "NotCollected", TotalOperations = blocks.Sum(x => x.Completed),
                AllArmsStable = summaries.All(x => x.Stable), SourceAndRuntimeUnchanged = true,
                WireCountsValidated = true, CpuAccountingValid = true });
        }
        catch (Exception error) { failure = error; }
        finally
        {
            var cleanupErrors = new List<string>();
            if (host is not null)
            {
                try { await CSharpDbClient.ClearPoolAsync(host.NativeEndpoint.AbsoluteUri).AsTask().WaitAsync(TimeSpan.FromSeconds(20)); }
                catch (Exception error) { cleanupErrors.Add(error.ToString()); }
                try { await host.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(20)); }
                catch (Exception error) { cleanupErrors.Add(error.ToString()); }
                // The legacy host owns graceful shutdown. Bound a failure backstop
                // to the exact daemon creation identity, without touching other processes.
                if (cleanupErrors.Count != 0 && daemonCreationUtc is { } created)
                {
                    try
                    {
                        using var daemon = Process.GetProcessById(host.Info.ServerProcessId);
                        if (daemon.StartTime.ToUniversalTime() == created && !daemon.HasExited)
                        {
                            daemon.Kill(entireProcessTree: true);
                            await daemon.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
                        }
                    }
                    catch (ArgumentException) { /* The owned daemon already exited. */ }
                    catch (Exception error) { cleanupErrors.Add(error.ToString()); }
                }
            }
            bool same = sources.SequenceEqual(SourceHashes(root)) && binaries.SequenceEqual(BinaryHashes())
                && head == Git("rev-parse", "HEAD") && status == Git("status", "--porcelain=v1");
            Write(output, "cleanup.json", new { HostStarted = host is not null, Host = host?.Info, CleanupErrors = cleanupErrors,
                OwnedDaemonExited = host is not null && cleanupErrors.Count == 0, SourceAndRuntimeUnchanged = same,
                CompletedBlocks = blocks.Count, CompletedOperations = blocks.Sum(x => x.Completed), PrimaryFailure = failure?.ToString() });
            if (!same) failure ??= new InvalidDataException("Source, runtime, HEAD or working-tree state changed.");
            if (cleanupErrors.Count != 0) failure ??= new IOException("Owned pool/daemon cleanup failed; retained in cleanup.json.");
        }
        if (failure is not null) ExceptionDispatchInfo.Capture(failure).Throw();
        Write(output, "status.json", new { State = "complete", DiagnosticOnly = true, QualificationEligible = false,
            AllocationAccounting = "NotCollected", FinishedUtc = DateTimeOffset.UtcNow, Blocks = blocks.Count,
            TotalOperations = blocks.Sum(x => x.Completed), CpuAccountingValid = true, SourceAndRuntimeUnchanged = true,
            OwnedDaemonExited = true });
    }

    private static async Task<Block> MeasureAsync(BenchmarkDaemonHost host, DateTime daemonCreationUtc, ArmDefinition arm, int round,
        CSharpDbClientOptions options, string connectionString, string output, List<object> checks, CpuLedger cpu, CancellationToken ct)
    {
        await using var group = await OpenGroupAsync(arm, options, connectionString, ct);
        var warmed = new int[Workers];
        try
        {
            await RunParallelAsync(async (worker, token) =>
            { for (int n = 0; n < Warmup; n++) { await group[worker].ReadAsync(token); warmed[worker]++; } }, ct);
        }
        finally { Write(output, $"{arm.Id}-Round{round}.warmup.json", new { PerWorker = warmed, ExpectedPerWorker = Warmup }); }
        RecordConditions(output, checks, $"Before-Round{round}-{arm.Id}", host, daemonCreationUtc);
        long[][] starts = Enumerable.Range(0, Workers).Select(_ => new long[PerWorker]).ToArray();
        long[][] ends = Enumerable.Range(0, Workers).Select(_ => new long[PerWorker]).ToArray();
        var completed = new int[Workers];
        var readyTicks = new long[Workers];
        var doneTicks = new long[Workers];
        var errors = new string?[Workers];
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ready = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int readyCount = 0;
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(ct);
        async Task Worker(int worker)
        {
            try
            {
                readyTicks[worker] = Stopwatch.GetTimestamp();
                if (Interlocked.Increment(ref readyCount) == Workers) ready.TrySetResult();
                await release.Task.WaitAsync(cancel.Token);
                for (int n = 0; n < PerWorker; n++)
                {
                    starts[worker][n] = Stopwatch.GetTimestamp();
                    await group[worker].ReadAsync(cancel.Token);
                    ends[worker][n] = Stopwatch.GetTimestamp();
                    completed[worker]++;
                }
            }
            catch (Exception error) { errors[worker] = error.ToString(); cancel.Cancel(); throw; }
            finally { doneTicks[worker] = Stopwatch.GetTimestamp(); }
        }
        Task all = Task.WhenAll(Enumerable.Range(0, Workers).Select(Worker));
        using var process = Process.GetCurrentProcess();
        BenchmarkDaemonHost.TimingSnapshot? before = null, after = null;
        double? clientBefore = null, clientAfter = null;
        long blockStart = 0, blockEnd = 0;
        Exception? executionFailure = null, captureFailure = null, cpuFailure = null, drainFailure = null;
        try
        {
            await ready.Task.WaitAsync(ct);
            before = await host.CaptureTimingAsync(ct);
            clientBefore = process.TotalProcessorTime.TotalMilliseconds;
            cpu.Observe(clientBefore.Value, before.ServerCpuMs);
            blockStart = Stopwatch.GetTimestamp();
            release.TrySetResult();
            await all.WaitAsync(ct);
            blockEnd = Stopwatch.GetTimestamp();
        }
        catch (Exception error)
        {
            executionFailure = error;
            cancel.Cancel();
            try { await all.WaitAsync(TimeSpan.FromSeconds(15)); }
            catch (Exception drain) { if (!all.IsCompleted) drainFailure = drain; }
        }
        finally
        {
            if (blockStart != 0 && blockEnd == 0) blockEnd = Stopwatch.GetTimestamp();
            try
            {
                using var snapshotDeadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                after = await host.CaptureTimingAsync(snapshotDeadline.Token);
                clientAfter = process.TotalProcessorTime.TotalMilliseconds;
            }
            catch (Exception error) { captureFailure = error; }
            try
            {
                if (before is null || after is null || clientBefore is null || clientAfter is null)
                    throw new InvalidDataException("CPU snapshot missing.");
                ValidateCpu(clientBefore.Value, clientAfter.Value, before.ServerCpuMs, after.ServerCpuMs);
                cpu.Observe(clientAfter.Value, after.ServerCpuMs);
            }
            catch (Exception error) { cpuFailure = error; }
            bool validCpu = cpuFailure is null && captureFailure is null;
            string stem = $"{arm.Id}-Round{round}";
            File.WriteAllLines(Path.Combine(output, stem + ".csv"), new[] { "Worker,Sample,StartTicks,EndTicks,ElapsedTicks" }.Concat(
                Enumerable.Range(0, Workers).SelectMany(w => Enumerable.Range(0, completed[w])
                    .Select(n => $"{w + 1},{n + 1},{starts[w][n]},{ends[w][n]},{ends[w][n] - starts[w][n]}"))));
            Write(output, stem + ".metrics.json", new
            {
                Arm = arm.Id, arm.Api, arm.Lifecycle, Round = round, Completed = completed.Sum(), Requested = Count, PerWorker = completed,
                ReadyTicks = readyTicks, DoneTicks = doneTicks, BlockStart = blockStart, BlockEnd = blockEnd, ElapsedTicks = blockEnd - blockStart,
                AllocationAccounting = "NotCollected", ClientCpuBeforeMs = Finite(clientBefore), ClientCpuAfterMs = Finite(clientAfter),
                ServerCpuBeforeMs = Finite(before?.ServerCpuMs), ServerCpuAfterMs = Finite(after?.ServerCpuMs),
                ClientCpuMs = validCpu ? clientAfter - clientBefore : null,
                ServerCpuMs = validCpu ? after!.ServerCpuMs - before!.ServerCpuMs : (double?)null,
                CpuMs = validCpu ? clientAfter - clientBefore + after!.ServerCpuMs - before!.ServerCpuMs : null,
                CpuAccountingValid = validCpu,
                CpuSnapshotsText = new { ClientBefore = clientBefore?.ToString("R", CultureInfo.InvariantCulture),
                    ClientAfter = clientAfter?.ToString("R", CultureInfo.InvariantCulture), ServerBefore = before?.ServerCpuMs.ToString("R", CultureInfo.InvariantCulture),
                    ServerAfter = after?.ServerCpuMs.ToString("R", CultureInfo.InvariantCulture) },
                WorkerFailures = errors, ExecutionFailure = executionFailure?.ToString(), CaptureFailure = captureFailure?.ToString(),
                CpuFailure = cpuFailure?.ToString(), DrainFailure = drainFailure?.ToString(), WorkersDrained = all.IsCompleted,
                PartialAttempts = Enumerable.Range(0, Workers).Where(w => completed[w] < PerWorker && starts[w][completed[w]] != 0)
                    .Select(w => new { Worker = w + 1, Sample = completed[w] + 1, StartTicks = starts[w][completed[w]], EndTicks = ends[w][completed[w]] }),
            });
            Write(output, stem + ".wire.json", new { Before = before?.Wire, After = after?.Wire, HttpBefore = before?.Http, HttpAfter = after?.Http });
        }
        if (executionFailure is not null) throw new InvalidOperationException("Block failed; partial results retained, no retry.", executionFailure);
        if (captureFailure is not null) throw new IOException("Counter capture failed; samples retained.", captureFailure);
        if (cpuFailure is not null) throw new InvalidDataException("CPU accounting failed; raw snapshots retained.", cpuFailure);
        if (completed.Any(x => x != PerWorker) || blockEnd <= blockStart || readyTicks.Any(x => x == 0 || x > blockStart)
            || doneTicks.Any(x => x < blockStart || x > blockEnd)) throw new InvalidDataException("Worker count/barrier bounds differ.");
        for (int w = 0; w < Workers; w++)
            for (int n = 0; n < PerWorker; n++)
                if (starts[w][n] < blockStart || ends[w][n] <= starts[w][n] || ends[w][n] > doneTicks[w]
                    || (n > 0 && starts[w][n] < ends[w][n - 1])) throw new InvalidDataException("Sample QPC bounds/order differ.");
        ValidateWire(before!.Http, after!.Http, after.Wire - before.Wire, new(arm.Api, "Stream"), Count);
        ValidateOwners(after);
        long[] samples = Enumerable.Range(0, Workers).SelectMany(w => Enumerable.Range(0, PerWorker)
            .Select(n => ends[w][n] - starts[w][n])).Order().ToArray();
        double worst = Enumerable.Range(0, Workers).Max(w => Percentile(Enumerable.Range(0, PerWorker)
            .Select(n => ends[w][n] - starts[w][n]).Order().ToArray(), .99));
        double clientCpu = clientAfter!.Value - clientBefore!.Value, serverCpu = after.ServerCpuMs - before.ServerCpuMs;
        return new(arm.Id, arm.Api, arm.Lifecycle, round, Count, Count * (double)Stopwatch.Frequency / (blockEnd - blockStart),
            Percentile(samples, .5), Percentile(samples, .95), Percentile(samples, .99), worst, clientCpu, serverCpu, clientCpu + serverCpu, true);
    }

    private static async Task<RunnerGroup> OpenGroupAsync(ArmDefinition arm, CSharpDbClientOptions options, string connectionString, CancellationToken ct)
    {
        var runners = new Runner?[Workers];
        try
        {
            await RunParallelAsync(async (worker, token) => { runners[worker] = await Runner.OpenAsync(arm, options, connectionString, token); }, ct);
            return new(runners.Select(x => x!).ToArray());
        }
        catch
        {
            foreach (Runner? runner in runners)
                if (runner is not null) try { await runner.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(20)); } catch { }
            throw;
        }
    }

    private static async Task RunParallelAsync(Func<int, CancellationToken, Task> operation, CancellationToken ct)
    {
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(ct);
        async Task Run(int worker)
        {
            try { await operation(worker, cancel.Token); }
            catch { cancel.Cancel(); throw; }
        }
        Task all = Task.WhenAll(Enumerable.Range(0, Workers).Select(Run));
        try { await all.WaitAsync(ct); }
        catch (Exception primary)
        {
            cancel.Cancel();
            try { await all.WaitAsync(TimeSpan.FromSeconds(15)); }
            catch (Exception drain)
            {
                if (!all.IsCompleted) throw new AggregateException("Sibling workers did not drain within15 seconds.", primary, drain);
            }
            throw;
        }
    }

    private sealed class Runner(ArmDefinition arm, CSharpDbClientOptions options, string connectionString) : IAsyncDisposable
    {
        private ICSharpDbClient? _client;
        private CSharpDbConnection? _connection;
        private CSharpDbCommand? _command;
        internal static async Task<Runner> OpenAsync(ArmDefinition arm, CSharpDbClientOptions options, string connectionString, CancellationToken ct)
        {
            var runner = new Runner(arm, options, connectionString);
            try
            {
                if (!arm.Lifecycle)
                {
                    if (arm.Api == "SDK") runner._client = await CSharpDbClient.RentAsync(options, Workers, ct);
                    else
                    {
                        runner._connection = new(connectionString);
                        await runner._connection.OpenAsync(ct);
                        runner._command = runner._connection.CreateCommand();
                        runner._command.CommandText = Query;
                    }
                }
                return runner;
            }
            catch { await runner.DisposeAsync(); throw; }
        }
        internal async Task ReadAsync(CancellationToken ct)
        {
            if (arm.Lifecycle)
            {
                if (arm.Api == "SDK")
                {
                    await using var client = await CSharpDbClient.RentAsync(options, Workers, ct);
                    ValidateSdk(await client.ExecuteSqlAsync(Query, ct));
                }
                else
                {
                    await using var connection = new CSharpDbConnection(connectionString);
                    await connection.OpenAsync(ct);
                    using var command = connection.CreateCommand();
                    command.CommandText = Query;
                    ValidateScalar(await command.ExecuteScalarAsync(ct));
                }
            }
            else if (_client is not null) ValidateSdk(await _client.ExecuteSqlAsync(Query, ct));
            else ValidateScalar(await _command!.ExecuteScalarAsync(ct));
        }
        public async ValueTask DisposeAsync()
        {
            _command?.Dispose();
            if (_connection is not null) await _connection.DisposeAsync();
            if (_client is not null) await _client.DisposeAsync();
        }
    }

    private sealed class RunnerGroup(Runner[] runners) : IAsyncDisposable
    {
        internal Runner this[int worker] => runners[worker];
        public async ValueTask DisposeAsync()
        {
            await Task.WhenAll(runners.Select(runner => runner.DisposeAsync().AsTask())).WaitAsync(TimeSpan.FromSeconds(20));
        }
    }

    private static void ValidateSdk(SqlExecutionResult result)
    {
        if (result.Error is not null || !result.IsQuery || result.Rows is not { Count: 1 } || result.Rows[0] is not { Length: 1 }
            || result.Rows[0][0] is not long value || value != 42) throw new InvalidDataException("SDK scalar result differs.");
    }
    private static void ValidateScalar(object? value)
    {
        if (value is not int scalar || scalar != 42) throw new InvalidDataException("ADO INTEGER scalar must be Int32 value42.");
    }
    private static void ValidateOwners(BenchmarkDaemonHost.TimingSnapshot snapshot)
    {
        ValidateCpu(0, 0, 0, snapshot.ServerCpuMs);
        long streams = snapshot.Http.Where(x => x.Method.EndsWith("/CommandSession", StringComparison.Ordinal)
            && x.Protocol == "HTTP/2" && x.ContentType.StartsWith("application/grpc", StringComparison.Ordinal)
            && !x.ContentType.Contains("grpc-web", StringComparison.Ordinal)).Sum(x => x.Value);
        if (streams != Workers || snapshot.Http.Sum(x => x.Value) != Workers + 1 || snapshot.Wire.UnarySql != 1
            || snapshot.Wire.UnaryOther != 0 || snapshot.Wire.StreamOther != 2 * Workers
            || snapshot.Wire.StreamResponses != snapshot.Wire.StreamSql + 2 * Workers)
            throw new InvalidDataException("Expected eight shared streaming owners, one unary seed and no extra control calls.");
    }
    private static void CheckEnvironment()
    {
        foreach (string name in new[] { "DOTNET_STARTUP_HOOKS", "CSHARPDB_BENCHMARK_PARENT", "DOTNET_DiagnosticPorts", "DOTNET_DefaultDiagnosticPortSuspend" })
            if (Environment.GetEnvironmentVariable(name) is not null) throw new InvalidOperationException($"Unexpected inherited environment: {name}.");
        foreach (string name in new[] { "COR_ENABLE_PROFILING", "CORECLR_ENABLE_PROFILING" })
            if (Environment.GetEnvironmentVariable(name) is { } value && value != "0") throw new InvalidOperationException($"Enabled CLR profiling: {name}.");
        if (Debugger.IsAttached) throw new InvalidOperationException("Diagnostic mode must run without an attached debugger.");
    }
    private static void RecordConditions(string output, List<object> checks, string stage, BenchmarkDaemonHost? host, DateTime? daemonCreationUtc = null)
    {
        CheckEnvironment();
        using var client = Process.GetCurrentProcess();
        using Process? server = host is null ? null : Process.GetProcessById(host.Info.ServerProcessId);
        client.Refresh(); server?.Refresh();
        var record = new { Stage = stage, Utc = DateTimeOffset.UtcNow, Qpc = Stopwatch.GetTimestamp(),
            Client = new { Pid = client.Id, CreationUtc = client.StartTime.ToUniversalTime(), Priority = client.PriorityClass.ToString() },
            Server = server is null ? null : new { Pid = server.Id, CreationUtc = server.StartTime.ToUniversalTime(), Priority = server.PriorityClass.ToString() },
            HookEnvironmentAbsent = true, ProfilingEnvironmentDisabled = true };
        checks.Add(record);
        Write(output, "priority-checks.json", checks);
        if (server is not null && server.StartTime.ToUniversalTime() != daemonCreationUtc)
            throw new InvalidOperationException("Owned daemon creation identity changed.");
        if (client.PriorityClass != ProcessPriorityClass.Normal || server is not null && server.PriorityClass != ProcessPriorityClass.Normal)
            throw new InvalidOperationException("Client and owned daemon must both have Normal process priority.");
    }
    private static double? Finite(double? value) => value.HasValue && double.IsFinite(value.Value) ? value : null;
    private static void ValidateCpu(double clientBefore, double clientAfter, double serverBefore, double serverAfter)
    {
        if (!double.IsFinite(clientBefore) || clientBefore < 0 || !double.IsFinite(clientAfter) || clientAfter < clientBefore
            || !double.IsFinite(serverBefore) || serverBefore < 0 || !double.IsFinite(serverAfter) || serverAfter < serverBefore)
            throw new InvalidDataException("CPU snapshots must be finite, nonnegative and monotonic.");
    }
    private sealed class CpuLedger
    {
        private double _client, _server;
        internal void Observe(double client, double server) { ValidateCpu(_client, client, _server, server); _client = client; _server = server; }
        internal void ObserveServer(double server) { ValidateCpu(_client, _client, _server, server); _server = server; }
    }
    private sealed record ArmDefinition(string Id, string Api, bool Lifecycle);
    private sealed record Block(string Arm, string Api, bool Lifecycle, int Round, int Completed, double OpsPerSecond,
        double P50Us, double P95Us, double P99Us, double WorstWorkerP99Us, double ClientCpuMs, double ServerCpuMs, double CpuMs, bool CpuAccountingValid)
    { public string AllocationAccounting => "NotCollected"; }
}
