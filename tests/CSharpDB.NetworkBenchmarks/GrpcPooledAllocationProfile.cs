using System.Diagnostics;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using System.Text.Json;
using CSharpDB.Client;
using CSharpDB.Data;
using static CSharpDB.NetworkBenchmarks.GrpcSessionBenchmark;

namespace CSharpDB.NetworkBenchmarks;

/// <summary>Profiles warmed pool checkout/return without executing SQL.</summary>
internal static class GrpcPooledAllocationProfile
{
    private const int Workers = 8, PerWorker = 32768, Count = Workers * PerWorker, Warmup = 256;
    private static readonly string[] BlockOrder = ["SDK", "ADO", "ADO", "SDK"];

    internal static async Task<int> RunAsync(string[] args)
    {
        string? output = null;
        bool ownsOutput = false;
        try
        {
            int oi = Array.IndexOf(args, "--output"), ei = Array.IndexOf(args, "--experiment");
            if (args.Length != 4 || oi is not (0 or 2) || ei is not (0 or 2) || oi == ei
                || args[ei + 1] != "pooled-allocation-profile")
                throw new ArgumentException("Use --experiment pooled-allocation-profile --output <new directory>; all settings are fixed.");
            string root = Path.GetFullPath(Git("rev-parse", "--show-toplevel"));
            output = Path.GetFullPath(args[oi + 1]);
            if (output.Equals(root, StringComparison.OrdinalIgnoreCase)
                || output.StartsWith(root + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase))
                throw new ArgumentException("Keep evidence outside the repository.");
            if (Directory.Exists(output) || File.Exists(output)) throw new ArgumentException("Evidence path must be new.");
            CheckEnvironment();
            Directory.CreateDirectory(output);
            ownsOutput = true;
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(120));
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
                    AllocationAccounting = "ClientProcessManagedPrecise", FinishedUtc = DateTimeOffset.UtcNow });
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
            Version = 1, Experiment = "pooled-allocation-profile", DiagnosticOnly = true, QualificationEligible = false,
            AllocationAccounting = "ClientProcessManagedPrecise", Workers, PerWorker, Count, BlockOrder, TotalCycles = Count * BlockOrder.Length,
            Warmup, PoolCapacity = Workers, DeadlineSeconds = 120, WorkerDrainSeconds = 15, CleanupSeconds = 20,
            SqlCommands = 0, ClientProcessId = Environment.ProcessId, WorkingDirectory = Environment.CurrentDirectory,
            Scope = "Actual isolated daemon; native HTTP/2 streaming; shared pool8. Prewarm eight simultaneously held SDK rentals then eight ADO opens. Reuse options and the same connection-string object. Warm256 zero-SQL cycles per worker/API once. Four measured blocks SDK,ADO,ADO,SDK.",
            Counter = "GC.GetTotalAllocatedBytes(precise:true) in the client process, before QPC start/release and after all workers/QPC end, before output or control IPC. Exact process-managed allocation includes remaining harness, scheduling, pool-timer and background activity; bytes/cycle are not exact product-only allocation or exact per-type counts. Server allocation is not collected.",
            WorkersMeaning = "All8 workers await an unreleased RunContinuationsAsynchronously barrier before any cycle. SDK cycle awaits RentAsync then DisposeAsync; ADO cycle constructs, opens and asynchronously disposes a connection. No SQL, commands, per-cycle timing, delegates, CSV, markers or forced asynchronous hops.",
            Trace = "Expected client DOTNET_DiagnosticPorts launch tracing is allowed. The owned-daemon helper removes diagnostic ports. AllocationTick stacks are sampled attribution, not exact allocation totals by type. No throughput/product qualification.",
            ClockAnchors = "Two CSharpDB-Baseline-Timeline event2 Clock anchors, IDs1/2, before first and after fourth allocation windows; payload QPC/frequency retained in clock-anchors.json. No per-cycle events.",
            Conditions = "Client and exact owned daemon must remain Normal; read-only before each block and after all blocks. Reject inherited startup hooks, benchmark parent and enabled CLR profiling. No GC/ThreadPool/OS changes.",
            Validation = "After prewarm exactly8 native CommandSession HTTP calls and wire(0,0,0,16,16): eight hello and eight GetInfo responses. All warmup and measured blocks must leave every HTTP/wire count unchanged. No retries/exclusions.",
            Runtime = RuntimeInformation.FrameworkDescription, OS = RuntimeInformation.OSDescription, Environment.ProcessorCount,
            System.Runtime.GCSettings.IsServerGC, Stopwatch.Frequency,
            DiagnosticPorts = Environment.GetEnvironmentVariable("DOTNET_DiagnosticPorts"),
            DefaultDiagnosticPortSuspend = Environment.GetEnvironmentVariable("DOTNET_DefaultDiagnosticPortSuspend"),
            GitHead = head, GitStatus = status, Sources = sources, Binaries = binaries,
        });
        File.WriteAllText(Path.Combine(output, "source.patch"), Git("diff", "--binary"));
        foreach (Hash source in sources)
        {
            string destination = Path.Combine(output, "source", source.Name);
            Directory.CreateDirectory(Path.GetDirectoryName(destination)!);
            File.Copy(Path.Combine(root, source.Name), destination, false);
        }
        var blocks = new List<ProfileBlock>();
        var conditions = new List<object>();
        var anchors = new List<object>();
        BenchmarkDaemonHost? host = null;
        DateTime? creation = null;
        Exception? failure = null;
        try
        {
            RecordConditions(output, conditions, "ClientBeforeDaemon", null, null);
            host = await BenchmarkDaemonHost.StartAsync(true, null, ct);
            using (var process = Process.GetProcessById(host.Info.ServerProcessId)) creation = process.StartTime.ToUniversalTime();
            Write(output, "host-info.json", host.Info);
            RecordConditions(output, conditions, "AfterDaemonStart", host, creation);
            var options = new CSharpDbClientOptions { Transport = CSharpDbTransport.Grpc, GrpcMode = CSharpDbGrpcMode.Native,
                GrpcCallMode = CSharpDbGrpcCallMode.Streaming, Endpoint = host.NativeEndpoint.AbsoluteUri };
            string connectionString = $"Transport=Grpc;Grpc Mode=Native;Grpc Call Mode=Streaming;Endpoint={host.NativeEndpoint};Pooling=true;Max Pool Size={Workers}";
            var initial = await host.CaptureTimingAsync(ct);
            if (initial.Http.Length != 0 || initial.Wire != new Counts(0, 0, 0, 0, 0))
                throw new InvalidDataException("Unexpected daemon traffic before pool prewarm.");
            await PrewarmAsync("SDK", options, connectionString, ct);
            var sdk = await host.CaptureTimingAsync(ct);
            ValidateOwners(sdk);
            await PrewarmAsync("ADO", options, connectionString, ct);
            var ado = await host.CaptureTimingAsync(ct);
            ValidateOwners(ado);
            ValidateUnchanged(sdk, ado);
            Write(output, "prewarm.json", new { Before = initial, Sdk = sdk, Ado = ado, SharedOwners = Workers, SqlCommands = 0 });
            var warmups = new List<ProfileBlock>();
            foreach (string api in new[] { "SDK", "ADO" })
            {
                ProfileBlock warm = await RunCyclesAsync(api, 0, Warmup, false, options, connectionString, ct);
                warmups.Add(warm);
                Write(output, "warmup.json", warmups);
                ThrowIfFailed(warm);
                ValidateUnchanged(ado, await host.CaptureTimingAsync(ct));
            }
            var before = await host.CaptureTimingAsync(ct);
            ValidateOwners(before);
            Write(output, "pool-proof-before.json", before);
            RecordClock(output, anchors, 1);
            for (int index = 0; index < BlockOrder.Length; index++)
            {
                string api = BlockOrder[index];
                RecordConditions(output, conditions, $"BeforeBlock{index + 1}-{api}", host, creation);
                var wireBefore = await host.CaptureTimingAsync(ct);
                ValidateUnchanged(before, wireBefore);
                ProfileBlock block = await RunCyclesAsync(api, index + 1, PerWorker, true, options, connectionString, ct);
                blocks.Add(block);
                string stem = $"{index + 1:D2}-{api}";
                Write(output, stem + ".metrics.json", block);
                Write(output, "blocks.json", blocks);
                ThrowIfFailed(block);
                var wireAfter = await host.CaptureTimingAsync(ct);
                Write(output, stem + ".wire.json", new { Before = wireBefore, After = wireAfter });
                ValidateUnchanged(wireBefore, wireAfter);
                Console.WriteLine($"Block{index + 1} {api}: {block.Completed} cycles; client managed bytes/cycle {block.ClientBytesPerCycle:N1} (diagnostic process total)");
            }
            RecordClock(output, anchors, 2);
            var after = await host.CaptureTimingAsync(ct);
            Write(output, "pool-proof-after.json", after);
            ValidateUnchanged(before, after);
            RecordConditions(output, conditions, "AfterAllBlocks", host, creation);
            if (blocks.Count != 4 || blocks.Sum(x => x.Completed) != 1048576 || blocks.Any(x => !x.AllocationAccountingValid))
                throw new InvalidDataException("Every fixed allocation block is required.");
        }
        catch (Exception error) { failure = error; }
        finally
        {
            var errors = new List<string>();
            if (host is not null)
            {
                try { await CSharpDbClient.ClearPoolAsync(host.NativeEndpoint.AbsoluteUri).AsTask().WaitAsync(TimeSpan.FromSeconds(20)); }
                catch (Exception error) { errors.Add(error.ToString()); }
                try { await host.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(20)); }
                catch (Exception error) { errors.Add(error.ToString()); }
                if (errors.Count != 0 && creation is { } expected)
                {
                    try
                    {
                        using var process = Process.GetProcessById(host.Info.ServerProcessId);
                        if (process.StartTime.ToUniversalTime() == expected && !process.HasExited)
                        {
                            process.Kill(entireProcessTree: true);
                            await process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
                        }
                    }
                    catch (ArgumentException) { }
                    catch (Exception error) { errors.Add(error.ToString()); }
                }
            }
            bool same = sources.SequenceEqual(SourceHashes(root)) && binaries.SequenceEqual(BinaryHashes())
                && head == Git("rev-parse", "HEAD") && status == Git("status", "--porcelain=v1");
            Write(output, "cleanup.json", new { HostStarted = host is not null, Host = host?.Info, CleanupErrors = errors,
                OwnedDaemonExited = host is not null && errors.Count == 0, SourceAndRuntimeUnchanged = same,
                CompletedBlocks = blocks.Count, CompletedCycles = blocks.Sum(x => x.Completed), PrimaryFailure = failure?.ToString() });
            if (!same) failure ??= new InvalidDataException("Source, runtime, HEAD or working-tree state changed.");
            if (errors.Count != 0) failure ??= new IOException("Owned pool/daemon cleanup failed; evidence retained.");
        }
        if (failure is not null) ExceptionDispatchInfo.Capture(failure).Throw();
        Write(output, "status.json", new { State = "complete", DiagnosticOnly = true, QualificationEligible = false,
            AllocationAccounting = "ClientProcessManagedPrecise", FinishedUtc = DateTimeOffset.UtcNow,
            Blocks = blocks.Count, TotalCycles = blocks.Sum(x => x.Completed), SqlCommands = 0,
            SourceAndRuntimeUnchanged = true, OwnedDaemonExited = true });
    }

    private static async Task<ProfileBlock> RunCyclesAsync(string api, int block, int perWorker, bool measured,
        CSharpDbClientOptions options, string connectionString, CancellationToken ct)
    {
        var completed = new int[Workers];
        var readyTicks = new long[Workers];
        var starts = new long[Workers];
        var done = new long[Workers];
        var errors = new string?[Workers];
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ready = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int readyCount = 0;
        async Task Worker(int worker)
        {
            try
            {
                readyTicks[worker] = Stopwatch.GetTimestamp();
                if (Interlocked.Increment(ref readyCount) == Workers) ready.TrySetResult();
                await release.Task.WaitAsync(cancel.Token);
                starts[worker] = Stopwatch.GetTimestamp();
                if (api == "SDK")
                {
                    for (int n = 0; n < perWorker; n++)
                    {
                        { await using var rental = await CSharpDbClient.RentAsync(options, Workers, cancel.Token); }
                        completed[worker]++;
                    }
                }
                else
                {
                    for (int n = 0; n < perWorker; n++)
                    {
                        {
                            await using var connection = new CSharpDbConnection(connectionString);
                            await connection.OpenAsync(cancel.Token);
                        }
                        completed[worker]++;
                    }
                }
            }
            catch (Exception error) { errors[worker] = error.ToString(); cancel.Cancel(); throw; }
            finally { done[worker] = Stopwatch.GetTimestamp(); }
        }
        Task all = Task.WhenAll(Enumerable.Range(0, Workers).Select(Worker));
        Task bounded = all.WaitAsync(ct); // Establish the block-level cancellation wrapper before the allocation counter.
        long start = 0, end = 0;
        long? allocatedBefore = null, allocatedAfter = null;
        Exception? executionFailure = null, drainFailure = null;
        try
        {
            await ready.Task.WaitAsync(ct);
            if (measured) allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
            start = Stopwatch.GetTimestamp();
            release.TrySetResult();
            await bounded;
            end = Stopwatch.GetTimestamp();
            if (measured) allocatedAfter = GC.GetTotalAllocatedBytes(precise: true);
        }
        catch (Exception error)
        {
            executionFailure = error;
            cancel.Cancel();
            try { await all.WaitAsync(TimeSpan.FromSeconds(15)); }
            catch (Exception drain) { if (!all.IsCompleted) drainFailure = drain; }
            end = Stopwatch.GetTimestamp();
            if (measured && allocatedBefore.HasValue) allocatedAfter = GC.GetTotalAllocatedBytes(precise: true);
        }
        bool allocationValid = measured && allocatedBefore.HasValue && allocatedAfter >= allocatedBefore;
        long? allocated = allocationValid ? allocatedAfter - allocatedBefore : null;
        return new(block, api, perWorker * Workers, completed.Sum(), completed, readyTicks, starts, done, errors,
            start, end, end - start, allocatedBefore, allocatedAfter, allocated,
            allocationValid && completed.Sum() != 0 ? allocated / (double)completed.Sum() : null,
            allocationValid, all.IsCompleted, executionFailure?.ToString(), drainFailure?.ToString());
    }

    private static void ThrowIfFailed(ProfileBlock block)
    {
        if (block.ExecutionFailure is not null || block.DrainFailure is not null || !block.WorkersDrained
            || block.Completed != block.Requested || block.PerWorker.Any(n => n != block.Requested / Workers)
            || block.WorkerFailures.Any(x => x is not null)) throw new InvalidOperationException("Pool cycle phase failed; partial counts retained without retry.");
        if (block.BlockStart <= 0 || block.BlockEnd <= block.BlockStart || block.ReadyTicks.Any(t => t <= 0 || t > block.BlockStart)
            || block.WorkerStartTicks.Any(t => t < block.BlockStart || t > block.BlockEnd)
            || block.DoneTicks.Where((t, worker) => t < block.WorkerStartTicks[worker] || t > block.BlockEnd).Any())
            throw new InvalidDataException("Pool cycle worker/QPC bounds differ.");
        if (block.Block != 0 && !block.AllocationAccountingValid) throw new InvalidDataException("Managed allocation counters decreased or are missing.");
    }

    private static async Task PrewarmAsync(string api, CSharpDbClientOptions options, string connectionString, CancellationToken ct)
    {
        var rentals = new IAsyncDisposable?[Workers];
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(ct);
        async Task Open(int worker)
        {
            try
            {
                if (api == "SDK") rentals[worker] = await CSharpDbClient.RentAsync(options, Workers, cancel.Token);
                else
                {
                    var connection = new CSharpDbConnection(connectionString);
                    rentals[worker] = connection;
                    await connection.OpenAsync(cancel.Token);
                }
            }
            catch { cancel.Cancel(); throw; }
        }
        Task all = Task.WhenAll(Enumerable.Range(0, Workers).Select(Open));
        try { await all.WaitAsync(ct); }
        catch
        {
            cancel.Cancel();
            try { await all.WaitAsync(TimeSpan.FromSeconds(15)); } catch { }
            throw;
        }
        finally
        {
            await Task.WhenAll(rentals.Where(x => x is not null).Select(x => x!.DisposeAsync().AsTask()))
                .WaitAsync(TimeSpan.FromSeconds(20));
        }
    }

    private static void ValidateOwners(BenchmarkDaemonHost.TimingSnapshot snapshot)
    {
        if (!double.IsFinite(snapshot.ServerCpuMs) || snapshot.ServerCpuMs < 0
            || snapshot.Wire != new Counts(0, 0, 0, 2 * Workers, 2 * Workers)
            || snapshot.Http.Length != 1 || snapshot.Http[0].Value != Workers
            || !snapshot.Http[0].Method.EndsWith("/CommandSession", StringComparison.Ordinal)
            || snapshot.Http[0].Protocol != "HTTP/2" || !snapshot.Http[0].ContentType.StartsWith("application/grpc", StringComparison.Ordinal)
            || snapshot.Http[0].ContentType.Contains("grpc-web", StringComparison.Ordinal))
            throw new InvalidDataException("Zero-SQL profile requires exactly eight retained stream owners and16 hello/GetInfo replies.");
    }
    private static void ValidateUnchanged(BenchmarkDaemonHost.TimingSnapshot before, BenchmarkDaemonHost.TimingSnapshot after)
    {
        ValidateOwners(before); ValidateOwners(after);
        if (!before.Http.SequenceEqual(after.Http) || before.Wire != after.Wire || after.ServerCpuMs < before.ServerCpuMs)
            throw new InvalidDataException("Checkout/return caused traffic, owner replacement or invalid daemon CPU accounting.");
    }
    private static void RecordClock(string output, List<object> anchors, int id)
    {
        long qpc = Stopwatch.GetTimestamp();
        TimelineEventSource.Log.Clock(id, qpc, Stopwatch.Frequency);
        anchors.Add(new { Id = id, Qpc = qpc, Frequency = Stopwatch.Frequency, ClientProcessId = Environment.ProcessId });
        Write(output, "clock-anchors.json", anchors);
    }
    private static void CheckEnvironment()
    {
        foreach (string name in new[] { "DOTNET_STARTUP_HOOKS", "CSHARPDB_BENCHMARK_PARENT" })
            if (Environment.GetEnvironmentVariable(name) is not null) throw new InvalidOperationException($"Unexpected inherited environment: {name}.");
        foreach (string name in new[] { "COR_ENABLE_PROFILING", "CORECLR_ENABLE_PROFILING" })
            if (Environment.GetEnvironmentVariable(name) is { } value && value != "0") throw new InvalidOperationException($"Enabled CLR profiling: {name}.");
        if (Debugger.IsAttached) throw new InvalidOperationException("Do not attach a debugger to allocation profiling.");
    }
    private static void RecordConditions(string output, List<object> checks, string stage, BenchmarkDaemonHost? host, DateTime? creation)
    {
        CheckEnvironment();
        using var client = Process.GetCurrentProcess();
        using Process? server = host is null ? null : Process.GetProcessById(host.Info.ServerProcessId);
        client.Refresh(); server?.Refresh();
        checks.Add(new { Stage = stage, Utc = DateTimeOffset.UtcNow, Qpc = Stopwatch.GetTimestamp(),
            Client = new { Pid = client.Id, CreationUtc = client.StartTime.ToUniversalTime(), Priority = client.PriorityClass.ToString() },
            Server = server is null ? null : new { Pid = server.Id, CreationUtc = server.StartTime.ToUniversalTime(), Priority = server.PriorityClass.ToString() },
            HookEnvironmentAbsent = true, ClrProfilingEnvironmentDisabled = true, DiagnosticPortAllowed = true });
        Write(output, "priority-checks.json", checks);
        if (client.PriorityClass != ProcessPriorityClass.Normal || server is not null
            && (server.PriorityClass != ProcessPriorityClass.Normal || server.StartTime.ToUniversalTime() != creation))
            throw new InvalidOperationException("Client and exact owned daemon must have Normal priority.");
    }

    private sealed record ProfileBlock(int Block, string Api, int Requested, int Completed, int[] PerWorker,
        long[] ReadyTicks, long[] WorkerStartTicks, long[] DoneTicks, string?[] WorkerFailures,
        long BlockStart, long BlockEnd, long ElapsedTicks, long? ClientAllocatedBefore, long? ClientAllocatedAfter,
        long? ClientAllocatedBytes, double? ClientBytesPerCycle, bool AllocationAccountingValid, bool WorkersDrained,
        string? ExecutionFailure, string? DrainFailure)
    { public string AllocationAccounting => "ClientProcessManagedPrecise"; }
}
