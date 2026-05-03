using System.Diagnostics;
using System.IO.Compression;
using System.Text;
using CSharpDB.Engine;
using CSharpDB.Primitives;
using CSharpDB.Samples.CompressionSdk.Infrastructure;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Samples.CompressionSdk.Benchmarks;

public static class CompressionEndToEndBenchmark
{
    private const int NormalRowCount = 2_000;
    private const int NormalReadCount = 2_000;
    private const int NormalPayloadBytes = 2 * 1024;
    private const int QuickRowCount = 200;
    private const int QuickReadCount = 400;
    private const int QuickPayloadBytes = 1024;
    private const int BatchSize = 100;

    private static readonly Scenario[] s_scenarios =
    [
        new("SqlRecordText", Surface.SqlRecordText, PayloadKind.RecordText, CompressionCodec.None),
        new("SqlRecordText", Surface.SqlRecordText, PayloadKind.RecordText, CompressionCodec.GZip),
        new("SqlRecordText", Surface.SqlRecordText, PayloadKind.RecordText, CompressionCodec.Brotli),
        new("SqlPageLikeBlob", Surface.SqlPageLikeBlob, PayloadKind.PageLike, CompressionCodec.None),
        new("SqlPageLikeBlob", Surface.SqlPageLikeBlob, PayloadKind.PageLike, CompressionCodec.GZip),
        new("SqlPageLikeBlob", Surface.SqlPageLikeBlob, PayloadKind.PageLike, CompressionCodec.Brotli),
        new("CollectionJsonDocument", Surface.CollectionJsonDocument, PayloadKind.CollectionJson, CompressionCodec.None),
        new("CollectionJsonDocument", Surface.CollectionJsonDocument, PayloadKind.CollectionJson, CompressionCodec.GZip),
        new("CollectionJsonDocument", Surface.CollectionJsonDocument, PayloadKind.CollectionJson, CompressionCodec.Brotli),
    ];

    public static Task<List<BenchmarkResult>> RunAsync(bool quick = false)
    {
        var profile = quick
            ? new Profile(QuickRowCount, QuickReadCount, QuickPayloadBytes)
            : new Profile(NormalRowCount, NormalReadCount, NormalPayloadBytes);

        return RunAsync(profile);
    }

    private static async Task<List<BenchmarkResult>> RunAsync(Profile profile)
    {
        var outcomes = new List<ScenarioOutcome>(s_scenarios.Length);
        var baselines = new Dictionary<Surface, ScenarioOutcome>();

        foreach (Scenario scenario in s_scenarios)
        {
            ScenarioOutcome outcome = await RunScenarioAsync(scenario, profile);
            outcomes.Add(outcome);

            if (scenario.Codec == CompressionCodec.None)
                baselines[scenario.Surface] = outcome;
        }

        var results = new List<BenchmarkResult>(outcomes.Count * 2);
        foreach (ScenarioOutcome outcome in outcomes)
        {
            ScenarioOutcome baseline = baselines[outcome.Scenario.Surface];
            results.Add(CreateWriteResult(outcome, baseline, profile));
            results.Add(CreateReadResult(outcome, baseline, profile));
        }

        return results;
    }

    private static async Task<ScenarioOutcome> RunScenarioAsync(Scenario scenario, Profile profile)
    {
        string root = Path.Combine(Path.GetTempPath(), $"csharpdb_compression_e2e_{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);
        string dbPath = Path.Combine(root, "bench.db");

        try
        {
            var payloads = CreatePayloads(scenario.PayloadKind, profile.RowCount, profile.PayloadBytes);
            var randomReadIds = CreateReadIds(profile.ReadCount, profile.RowCount);
            await using var db = await Database.OpenAsync(dbPath, CreateOptions());

            if (scenario.Surface is Surface.SqlRecordText or Surface.SqlPageLikeBlob)
                await CreateSqlSchemaAsync(db, scenario);

            long originalPayloadBytes;
            long storedPayloadBytes;
            TimedOperation write;

            if (scenario.Surface == Surface.CollectionJsonDocument)
            {
                (write, originalPayloadBytes, storedPayloadBytes) = await WriteCollectionAsync(db, scenario, payloads);
            }
            else
            {
                (write, originalPayloadBytes, storedPayloadBytes) = await WriteSqlAsync(db, scenario, payloads);
            }

            var preCheckpointSize = GetDatabaseSize(dbPath);
            await db.CheckpointAsync();
            var postCheckpointSize = GetDatabaseSize(dbPath);

            TimedOperation read = scenario.Surface == Surface.CollectionJsonDocument
                ? await ReadCollectionAsync(db, scenario, randomReadIds)
                : await ReadSqlAsync(db, scenario, randomReadIds);

            return new ScenarioOutcome(
                scenario,
                write,
                read,
                originalPayloadBytes,
                storedPayloadBytes,
                preCheckpointSize,
                postCheckpointSize);
        }
        finally
        {
            TryDeleteDirectory(root);
        }
    }

    private static async Task CreateSqlSchemaAsync(Database db, Scenario scenario)
    {
        string payloadType = scenario.Codec == CompressionCodec.None && scenario.Surface == Surface.SqlRecordText
            ? "TEXT"
            : "BLOB";

        await ExecuteCommandAsync(db, $"CREATE TABLE bench (id INTEGER PRIMARY KEY, payload {payloadType});");
    }

    private static async Task<(TimedOperation Write, long OriginalPayloadBytes, long StoredPayloadBytes)> WriteSqlAsync(
        Database db,
        Scenario scenario,
        IReadOnlyList<byte[]> payloads)
    {
        var batch = db.PrepareInsertBatch("bench", initialCapacity: BatchSize);
        var row = new DbValue[2];
        var histogram = new LatencyHistogram();
        long originalPayloadBytes = 0;
        long storedPayloadBytes = 0;
        var total = Stopwatch.StartNew();

        for (int offset = 0; offset < payloads.Count; offset += BatchSize)
        {
            int take = Math.Min(BatchSize, payloads.Count - offset);
            var batchSw = Stopwatch.StartNew();
            await db.BeginTransactionAsync();
            try
            {
                for (int i = 0; i < take; i++)
                {
                    int id = offset + i + 1;
                    byte[] original = payloads[offset + i];
                    originalPayloadBytes += original.Length;
                    row[0] = DbValue.FromInteger(id);

                    if (scenario.Codec == CompressionCodec.None && scenario.Surface == Surface.SqlRecordText)
                    {
                        row[1] = DbValue.FromText(Encoding.UTF8.GetString(original));
                        storedPayloadBytes += original.Length;
                    }
                    else
                    {
                        byte[] stored = scenario.Codec == CompressionCodec.None
                            ? original
                            : Compress(original, scenario.Codec);
                        row[1] = DbValue.FromBlob(stored);
                        storedPayloadBytes += stored.Length;
                    }

                    batch.AddRow(row);
                }

                int rowsAffected = await batch.ExecuteAsync();
                if (rowsAffected != take)
                    throw new InvalidOperationException($"Expected {take} inserted rows, observed {rowsAffected}.");

                await db.CommitAsync();
            }
            catch
            {
                await db.RollbackAsync();
                throw;
            }

            batchSw.Stop();
            histogram.Record(batchSw.Elapsed.TotalMilliseconds);
        }

        total.Stop();
        return (new TimedOperation(payloads.Count, total.Elapsed.TotalMilliseconds, histogram), originalPayloadBytes, storedPayloadBytes);
    }

    private static async Task<(TimedOperation Write, long OriginalPayloadBytes, long StoredPayloadBytes)> WriteCollectionAsync(
        Database db,
        Scenario scenario,
        IReadOnlyList<byte[]> payloads)
    {
        var histogram = new LatencyHistogram();
        long originalPayloadBytes = 0;
        long storedPayloadBytes = 0;
        var total = Stopwatch.StartNew();

        if (scenario.Codec == CompressionCodec.None)
        {
            var collection = await db.GetCollectionAsync<UncompressedCollectionDocument>("compression_docs");
            for (int offset = 0; offset < payloads.Count; offset += BatchSize)
            {
                int take = Math.Min(BatchSize, payloads.Count - offset);
                var batchSw = Stopwatch.StartNew();
                await db.BeginTransactionAsync();
                try
                {
                    for (int i = 0; i < take; i++)
                    {
                        int id = offset + i + 1;
                        byte[] payload = payloads[offset + i];
                        originalPayloadBytes += payload.Length;
                        storedPayloadBytes += payload.Length;
                        await collection.PutAsync(
                            $"doc:{id}",
                            new UncompressedCollectionDocument
                            {
                                Id = id,
                                Tenant = $"tenant-{id % 16}",
                                Payload = Encoding.UTF8.GetString(payload),
                            });
                    }

                    await db.CommitAsync();
                }
                catch
                {
                    await db.RollbackAsync();
                    throw;
                }

                batchSw.Stop();
                histogram.Record(batchSw.Elapsed.TotalMilliseconds);
            }
        }
        else
        {
            var collection = await db.GetCollectionAsync<CompressedCollectionDocument>("compression_docs");
            for (int offset = 0; offset < payloads.Count; offset += BatchSize)
            {
                int take = Math.Min(BatchSize, payloads.Count - offset);
                var batchSw = Stopwatch.StartNew();
                await db.BeginTransactionAsync();
                try
                {
                    for (int i = 0; i < take; i++)
                    {
                        int id = offset + i + 1;
                        byte[] original = payloads[offset + i];
                        byte[] stored = Compress(original, scenario.Codec);
                        originalPayloadBytes += original.Length;
                        storedPayloadBytes += stored.Length;
                        await collection.PutAsync(
                            $"doc:{id}",
                            new CompressedCollectionDocument
                            {
                                Id = id,
                                Tenant = $"tenant-{id % 16}",
                                Codec = scenario.Codec.ToString(),
                                Payload = stored,
                            });
                    }

                    await db.CommitAsync();
                }
                catch
                {
                    await db.RollbackAsync();
                    throw;
                }

                batchSw.Stop();
                histogram.Record(batchSw.Elapsed.TotalMilliseconds);
            }
        }

        total.Stop();
        return (new TimedOperation(payloads.Count, total.Elapsed.TotalMilliseconds, histogram), originalPayloadBytes, storedPayloadBytes);
    }

    private static async Task<TimedOperation> ReadSqlAsync(Database db, Scenario scenario, IReadOnlyList<int> ids)
    {
        var histogram = new LatencyHistogram();
        var total = Stopwatch.StartNew();

        foreach (int id in ids)
        {
            var sw = Stopwatch.StartNew();
            await using var result = await db.ExecuteAsync($"SELECT payload FROM bench WHERE id = {id};");
            if (!await result.MoveNextAsync())
                throw new InvalidOperationException($"Row {id} was not found.");

            if (scenario.Codec == CompressionCodec.None && scenario.Surface == Surface.SqlRecordText)
            {
                string value = result.Current[0].AsText;
                if (value.Length == 0)
                    throw new InvalidOperationException($"Row {id} returned an empty payload.");
            }
            else
            {
                byte[] stored = result.Current[0].AsBlob;
                byte[] payload = scenario.Codec == CompressionCodec.None
                    ? stored
                    : Decompress(stored, scenario.Codec);
                if (payload.Length == 0)
                    throw new InvalidOperationException($"Row {id} returned an empty payload.");
            }

            sw.Stop();
            histogram.Record(sw.Elapsed.TotalMilliseconds);
        }

        total.Stop();
        return new TimedOperation(ids.Count, total.Elapsed.TotalMilliseconds, histogram);
    }

    private static async Task<TimedOperation> ReadCollectionAsync(Database db, Scenario scenario, IReadOnlyList<int> ids)
    {
        var histogram = new LatencyHistogram();
        var total = Stopwatch.StartNew();

        if (scenario.Codec == CompressionCodec.None)
        {
            var collection = await db.GetCollectionAsync<UncompressedCollectionDocument>("compression_docs");
            foreach (int id in ids)
            {
                var sw = Stopwatch.StartNew();
                UncompressedCollectionDocument? document = await collection.GetAsync($"doc:{id}");
                if (document is null || string.IsNullOrEmpty(document.Payload))
                    throw new InvalidOperationException($"Document {id} was not found or had no payload.");

                sw.Stop();
                histogram.Record(sw.Elapsed.TotalMilliseconds);
            }
        }
        else
        {
            var collection = await db.GetCollectionAsync<CompressedCollectionDocument>("compression_docs");
            foreach (int id in ids)
            {
                var sw = Stopwatch.StartNew();
                CompressedCollectionDocument? document = await collection.GetAsync($"doc:{id}");
                if (document is null)
                    throw new InvalidOperationException($"Document {id} was not found.");

                byte[] payload = Decompress(document.Payload, scenario.Codec);
                if (payload.Length == 0)
                    throw new InvalidOperationException($"Document {id} returned an empty payload.");

                sw.Stop();
                histogram.Record(sw.Elapsed.TotalMilliseconds);
            }
        }

        total.Stop();
        return new TimedOperation(ids.Count, total.Elapsed.TotalMilliseconds, histogram);
    }

    private static BenchmarkResult CreateWriteResult(ScenarioOutcome outcome, ScenarioOutcome baseline, Profile profile)
    {
        double throughputChange = PercentThroughputChange(outcome.Write.OpsPerSecond, baseline.Write.OpsPerSecond);
        return CreateResult(
            $"{outcome.Scenario.Name}_{outcome.Scenario.Codec}_WriteRows",
            outcome.Write,
            CreateExtraInfo(outcome, baseline, profile, "write", throughputChange));
    }

    private static BenchmarkResult CreateReadResult(ScenarioOutcome outcome, ScenarioOutcome baseline, Profile profile)
    {
        double throughputChange = PercentThroughputChange(outcome.Read.OpsPerSecond, baseline.Read.OpsPerSecond);
        return CreateResult(
            $"{outcome.Scenario.Name}_{outcome.Scenario.Codec}_HotPointRead",
            outcome.Read,
            CreateExtraInfo(outcome, baseline, profile, "read", throughputChange));
    }

    private static BenchmarkResult CreateResult(string name, TimedOperation operation, string extraInfo)
    {
        LatencyHistogram histogram = operation.Histogram;
        return new BenchmarkResult
        {
            Name = name,
            TotalOps = operation.TotalOps,
            ElapsedMs = operation.ElapsedMs,
            P50Ms = histogram.Percentile(0.50),
            P90Ms = histogram.Percentile(0.90),
            P95Ms = histogram.Percentile(0.95),
            P99Ms = histogram.Percentile(0.99),
            P999Ms = histogram.Percentile(0.999),
            MinMs = histogram.Min,
            MaxMs = histogram.Max,
            MeanMs = histogram.Mean,
            StdDevMs = histogram.StdDev,
            ExtraInfo = extraInfo,
        };
    }

    private static string CreateExtraInfo(
        ScenarioOutcome outcome,
        ScenarioOutcome baseline,
        Profile profile,
        string operation,
        double throughputChangePercent)
    {
        double payloadRatio = Ratio(outcome.StoredPayloadBytes, outcome.OriginalPayloadBytes);
        double finalSizeRatio = Ratio(outcome.PostCheckpointSize.TotalBytes, baseline.PostCheckpointSize.TotalBytes);
        double sizeSavingsPercent = 100.0 * (1.0 - finalSizeRatio);

        return
            $"operation={operation}, surface={outcome.Scenario.Surface}, payloadKind={outcome.Scenario.PayloadKind}, codec={outcome.Scenario.Codec}, " +
            $"rows={profile.RowCount}, reads={profile.ReadCount}, payloadBytes={profile.PayloadBytes}, batchSize={BatchSize}, " +
            $"originalPayloadBytes={outcome.OriginalPayloadBytes}, storedPayloadBytes={outcome.StoredPayloadBytes}, storedPayloadRatio={payloadRatio:F4}, " +
            $"preCheckpointDbBytes={outcome.PreCheckpointSize.DatabaseBytes}, preCheckpointWalBytes={outcome.PreCheckpointSize.WalBytes}, preCheckpointTotalBytes={outcome.PreCheckpointSize.TotalBytes}, " +
            $"postCheckpointDbBytes={outcome.PostCheckpointSize.DatabaseBytes}, postCheckpointWalBytes={outcome.PostCheckpointSize.WalBytes}, postCheckpointTotalBytes={outcome.PostCheckpointSize.TotalBytes}, " +
            $"finalSizeRatioVsUncompressed={finalSizeRatio:F4}, finalSizeSavingsVsUncompressedPct={sizeSavingsPercent:F2}, throughputChangeVsUncompressedPct={throughputChangePercent:F2}, " +
            "note=payload-level candidate using real CSharpDB files; not transparent page-level compression";
    }

    private static IReadOnlyList<byte[]> CreatePayloads(PayloadKind kind, int count, int payloadBytes)
    {
        var payloads = new byte[count][];
        for (int i = 0; i < count; i++)
            payloads[i] = Encoding.UTF8.GetBytes(CreatePayload(kind, i + 1, payloadBytes));

        return payloads;
    }

    private static IReadOnlyList<int> CreateReadIds(int count, int maxId)
    {
        var rng = new Random(42);
        var ids = new int[count];
        for (int i = 0; i < ids.Length; i++)
            ids[i] = rng.Next(1, maxId + 1);

        return ids;
    }

    private static string CreatePayload(PayloadKind kind, int id, int targetBytes)
    {
        var builder = new StringBuilder(targetBytes + 256);
        switch (kind)
        {
            case PayloadKind.RecordText:
                while (builder.Length < targetBytes)
                {
                    builder.Append("id=").Append(id)
                        .Append(";name=user-").Append(id % 1024)
                        .Append(";region=").Append(id % 8)
                        .Append(";status=active;notes=repeatable storage row payload;score=")
                        .Append(id % 100)
                        .Append('\n');
                }

                break;

            case PayloadKind.CollectionJson:
                builder.Append("{\"id\":").Append(id)
                    .Append(",\"tenant\":\"tenant-").Append(id % 16)
                    .Append("\",\"tags\":[\"alpha\",\"beta\",\"search\"],\"profile\":{\"active\":true,\"tier\":")
                    .Append(id % 4)
                    .Append("},\"items\":[");

                for (int i = 0; builder.Length < targetBytes; i++)
                {
                    if (i > 0)
                        builder.Append(',');
                    builder.Append("{\"sku\":\"sku-").Append(i % 256)
                        .Append("\",\"qty\":").Append((i % 9) + 1)
                        .Append(",\"description\":\"collection payload compression candidate\"}");
                }

                builder.Append("]}");
                break;

            case PayloadKind.PageLike:
                builder.Append("CSDB").Append('\0', 96);
                while (builder.Length < targetBytes)
                {
                    builder.Append("cell:").Append(id)
                        .Append("|ptr=").Append(builder.Length % 4096)
                        .Append("|free=").Append((4096 - builder.Length) & 0x0FFF)
                        .Append("|payload=page-local repeated value block")
                        .Append('\0', 4);
                }

                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, null);
        }

        string value = builder.ToString();
        if (Encoding.UTF8.GetByteCount(value) <= targetBytes)
            return value;

        return value[..targetBytes];
    }

    private static byte[] Compress(byte[] payload, CompressionCodec codec)
    {
        using var output = new MemoryStream();
        Stream compressor = codec switch
        {
            CompressionCodec.GZip => new GZipStream(output, CompressionLevel.Fastest, leaveOpen: true),
            CompressionCodec.Brotli => new BrotliStream(output, CompressionLevel.Fastest, leaveOpen: true),
            CompressionCodec.None => throw new ArgumentOutOfRangeException(nameof(codec), codec, null),
            _ => throw new ArgumentOutOfRangeException(nameof(codec), codec, null),
        };

        using (compressor)
            compressor.Write(payload);

        return output.ToArray();
    }

    private static byte[] Decompress(byte[] payload, CompressionCodec codec)
    {
        using var input = new MemoryStream(payload);
        Stream decompressor = codec switch
        {
            CompressionCodec.GZip => new GZipStream(input, CompressionMode.Decompress),
            CompressionCodec.Brotli => new BrotliStream(input, CompressionMode.Decompress),
            CompressionCodec.None => throw new ArgumentOutOfRangeException(nameof(codec), codec, null),
            _ => throw new ArgumentOutOfRangeException(nameof(codec), codec, null),
        };

        using (decompressor)
        using (var output = new MemoryStream())
        {
            decompressor.CopyTo(output);
            return output.ToArray();
        }
    }

    private static DatabaseSize GetDatabaseSize(string dbPath)
    {
        string walPath = dbPath + ".wal";
        long dbBytes = File.Exists(dbPath) ? new FileInfo(dbPath).Length : 0;
        long walBytes = File.Exists(walPath) ? new FileInfo(walPath).Length : 0;
        return new DatabaseSize(dbBytes, walBytes);
    }

    private static DatabaseOptions CreateOptions()
        => new DatabaseOptions().ConfigureStorageEngine(builder => builder.UseWriteOptimizedPreset());

    private static async Task ExecuteCommandAsync(Database db, string sql)
    {
        await using var result = await db.ExecuteAsync(sql);
        _ = result.RowsAffected;
    }

    private static double Ratio(long value, long baseline)
        => baseline == 0 ? 0 : value / (double)baseline;

    private static double PercentThroughputChange(double value, double baseline)
    {
        if (baseline <= 0)
            return 0;

        return 100.0 * (value - baseline) / baseline;
    }

    private static void TryDeleteDirectory(string path)
    {
        try
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            // Benchmark temp cleanup should not hide the measured result.
        }
    }

    private sealed class UncompressedCollectionDocument
    {
        public int Id { get; set; }
        public string Tenant { get; set; } = "";
        public string Payload { get; set; } = "";
    }

    private sealed class CompressedCollectionDocument
    {
        public int Id { get; set; }
        public string Tenant { get; set; } = "";
        public string Codec { get; set; } = "";
        public byte[] Payload { get; set; } = [];
    }

    private sealed record Profile(int RowCount, int ReadCount, int PayloadBytes);

    private sealed record Scenario(string Name, Surface Surface, PayloadKind PayloadKind, CompressionCodec Codec);

    private sealed record TimedOperation(int TotalOps, double ElapsedMs, LatencyHistogram Histogram)
    {
        public double OpsPerSecond => TotalOps > 0 && ElapsedMs > 0
            ? TotalOps / (ElapsedMs / 1000.0)
            : 0;
    }

    private sealed record DatabaseSize(long DatabaseBytes, long WalBytes)
    {
        public long TotalBytes => DatabaseBytes + WalBytes;
    }

    private sealed record ScenarioOutcome(
        Scenario Scenario,
        TimedOperation Write,
        TimedOperation Read,
        long OriginalPayloadBytes,
        long StoredPayloadBytes,
        DatabaseSize PreCheckpointSize,
        DatabaseSize PostCheckpointSize);

    private enum Surface
    {
        SqlRecordText,
        SqlPageLikeBlob,
        CollectionJsonDocument,
    }

    private enum PayloadKind
    {
        RecordText,
        CollectionJson,
        PageLike,
    }

    private enum CompressionCodec
    {
        None,
        GZip,
        Brotli,
    }
}
