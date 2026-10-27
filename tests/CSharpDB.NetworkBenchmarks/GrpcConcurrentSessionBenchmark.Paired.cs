using System.Diagnostics;
using System.Security.Cryptography;
using System.Text.Json;
using CSharpDB.Client;
using static CSharpDB.NetworkBenchmarks.GrpcSessionBenchmark;

namespace CSharpDB.NetworkBenchmarks;

internal static partial class GrpcConcurrentSessionBenchmark
{
    // Each process measures one predeclared old/new pair member. The external schedule
    // alternates variant order; source provenance comes from its frozen build, not the live checkout.
    public static async Task<int> RunPairedAsync(string[] args)
    {
        string? output = null; bool ownsOutput = false;
        try
        {
            if (args.Length != 8 || args.Where((_, i) => i % 2 == 0).Distinct().Count() != 4
                || !new[] { "--experiment", "--variant", "--round", "--output" }.All(args.Contains))
                throw new ArgumentException("Use --experiment session-c8-paired --variant baseline|candidate --round 1..6 --output <new directory>.");
            string variant = args[Array.IndexOf(args, "--variant") + 1];
            int round = int.Parse(args[Array.IndexOf(args, "--round") + 1]);
            if (variant is not ("baseline" or "candidate") || round is < 1 or > 6) throw new ArgumentException("Invalid variant or round.");
            string root = Git("rev-parse", "--show-toplevel");
            output = Path.GetFullPath(args[Array.IndexOf(args, "--output") + 1]);
            if (output.Equals(root, StringComparison.OrdinalIgnoreCase) || output.StartsWith(root + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase))
                throw new ArgumentException("Evidence must be outside the repository.");
            if (Directory.Exists(output)) throw new ArgumentException("Evidence directory must be new.");
            Directory.CreateDirectory(output); ownsOutput = true;
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
            await RunPairedCoreAsync(root, output, variant, round, timeout.Token);
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

    private static async Task RunPairedCoreAsync(string root, string output, string variant, int round, CancellationToken ct)
    {
        string frozenRoot = Directory.GetParent(Path.TrimEndingDirectorySeparator(AppContext.BaseDirectory))!.FullName;
        string manifestPath = Path.Combine(frozenRoot, "snapshot.json");
        var snapshot = JsonSerializer.Deserialize<FrozenSnapshot>(File.ReadAllText(manifestPath))
            ?? throw new InvalidDataException("Missing frozen build manifest.");
        if (snapshot.Variant != variant || snapshot.Sources.Length == 0 || snapshot.Binaries.Length == 0)
            throw new InvalidDataException("Wrong frozen variant manifest.");
        VerifyFrozenSources(frozenRoot, snapshot.Sources);
        var binaries = BinaryHashes();
        if (!snapshot.Binaries.SequenceEqual(binaries)) throw new InvalidDataException("Frozen binary hash mismatch.");
        var liveSources = SourceHashes(root); string liveStatus = Git("status", "--porcelain=v1");
        string[] apis = round is 2 or 3 or 6 ? ["ADO", "SDK"] : ["SDK", "ADO"];
        Write(output, "protocol.json", new
        {
            Version = 1, Experiment = "direct-dispatch-matched-C8", Variant = variant, Round = round,
            Workers, PerWorker, Count, Warmup, SettlingSecondsPerArm = 3, PoolCapacity = Workers,
            ApiOrder = apis, Query = "SELECT value FROM network_read WHERE id = 1",
            Scope = "Diagnostic causal comparison of frozen old/new streaming builds. Separate client/server, warm cleartext loopback, one held rental/command per worker, same shared pool. No profiler, replay, exclusions or retries.",
            Gate = "Global six-pair validator per API: both variants spread<=25%, candidate median throughput>=105% baseline,>=5/6 paired wins, median pooled p99 and median worst-worker p99 no worse. Require all reads/counts/accounting valid. Separate six-arm transport qualification is not a causal before/after comparison.",
            Provenance = "Frozen sources and binary hashes identify this build; LiveWorkspace is only a stability check and does not identify baseline build sources.",
            FrozenRoot = frozenRoot, ManifestSha256 = Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(manifestPath))),
            Snapshot = snapshot, Binaries = binaries,
            LiveWorkspace = new { Root = root, GitHead = Git("rev-parse", "HEAD"), GitStatus = liveStatus, Sources = liveSources },
            Stopwatch.Frequency, Runtime = System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription,
            System.Runtime.GCSettings.IsServerGC, Environment.ProcessorCount,
        });
        Write(output, "status.json", new { State = "running", StartedUtc = DateTimeOffset.UtcNow });
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
            foreach (string api in apis)
            {
                await using var group = await Group.OpenAsync(host, new(api, "Stream"), ct);
                var timer = Stopwatch.StartNew(); var completed = new int[Workers];
                await Task.WhenAll(group.Runners.Select(async (runner, worker) =>
                {
                    do { await runner.ReadAsync(ct); completed[worker]++; } while (timer.Elapsed < TimeSpan.FromSeconds(3));
                }));
                settling.Add(new { Api = api, Completed = completed, ElapsedSeconds = timer.Elapsed.TotalSeconds });
                Write(output, "settling.json", settling);
            }
            var before = await host.CaptureAsync(ct); ValidateStreamFamily(before); Write(output, "pool-proof-before.json", before);
            foreach (string api in apis)
            {
                var block = await MeasureAsync(host, new(api, "Stream"), round, output, false, ct);
                blocks.Add(block); Write(output, "blocks.json", blocks);
                Console.WriteLine($"{variant} {api} round{round}: {block.OpsPerSecond:N0} ops/sec; p99 {block.P99Us:N1} us");
            }
            var after = await host.CaptureAsync(ct); ValidateStreamFamily(after); Write(output, "pool-proof-after.json", after);
            if (!liveSources.SequenceEqual(SourceHashes(root)) || liveStatus != Git("status", "--porcelain=v1")
                || !binaries.SequenceEqual(BinaryHashes())) throw new InvalidDataException("Workspace or frozen binaries changed during run.");
            VerifyFrozenSources(frozenRoot, snapshot.Sources);
            Write(output, "summary.json", new { Variant = variant, Round = round, Blocks = blocks, TotalOperations = blocks.Sum(x => x.Completed), SourcesAndBinariesUnchanged = true, WireCountsValidated = true, ProfileOnly = false });
            Write(output, "status.json", new { State = "complete", FinishedUtc = DateTimeOffset.UtcNow, Blocks = blocks.Count, TotalOperations = blocks.Sum(x => x.Completed) });
        }
        finally
        {
            await CSharpDbClient.ClearPoolAsync(host.CompatibilityEndpoint.AbsoluteUri);
            await CSharpDbClient.ClearPoolAsync(host.NativeEndpoint.AbsoluteUri);
        }
    }

    private static void ValidateStreamFamily(BenchmarkDaemonHost.Snapshot snapshot)
    {
        long streams = snapshot.Http.Where(x => x.Method.EndsWith("/CommandSession", StringComparison.Ordinal)).Sum(x => x.Value);
        long infos = snapshot.Http.Where(x => x.Method.EndsWith("/GetInfo", StringComparison.Ordinal)).Sum(x => x.Value);
        if (streams != Workers || infos != 0 || snapshot.Wire.UnaryOther != 0 || snapshot.Wire.StreamOther != 2 * Workers)
            throw new InvalidDataException("Expected exactly eight retained streaming owners shared by SDK/ADO, without unary GetInfo.");
    }

    private static void VerifyFrozenSources(string root, Hash[] hashes)
    {
        foreach (var hash in hashes)
            if (Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(Path.Combine(root, hash.Name)))) != hash.Sha256)
                throw new InvalidDataException($"Frozen source changed: {hash.Name}");
    }

    private sealed record FrozenSnapshot(string Variant, string GitHead, string GitStatus, Hash[] Sources, Hash[] Binaries);
}
