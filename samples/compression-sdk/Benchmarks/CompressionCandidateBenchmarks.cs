using System.IO.Compression;
using System.Text;
using BenchmarkDotNet.Attributes;

namespace CSharpDB.Samples.CompressionSdk.Benchmarks;

[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class CompressionCandidateBenchmarks
{
    private byte[] _payload = [];
    private byte[] _gzipCompressed = [];
    private byte[] _brotliCompressed = [];

    [Params(CompressionPayloadKind.RecordPayload, CompressionPayloadKind.CollectionPayload, CompressionPayloadKind.PagePayload)]
    public CompressionPayloadKind PayloadKind { get; set; }

    [Params(4 * 1024, 32 * 1024)]
    public int PayloadBytes { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        _payload = CreatePayload(PayloadKind, PayloadBytes);
        _gzipCompressed = CompressWithGZip(_payload);
        _brotliCompressed = CompressWithBrotli(_payload);
    }

    [Benchmark(Description = "GZip Fastest compress")]
    public byte[] GZip_Compress_Fastest()
        => CompressWithGZip(_payload);

    [Benchmark(Description = "GZip Fastest decompress")]
    public byte[] GZip_Decompress_Fastest()
        => DecompressWithGZip(_gzipCompressed);

    [Benchmark(Description = "Brotli Fastest compress")]
    public byte[] Brotli_Compress_Fastest()
        => CompressWithBrotli(_payload);

    [Benchmark(Description = "Brotli Fastest decompress")]
    public byte[] Brotli_Decompress_Fastest()
        => DecompressWithBrotli(_brotliCompressed);

    [Benchmark(Description = "Uncompressed copy baseline", Baseline = true)]
    public byte[] Uncompressed_CopyBaseline()
    {
        byte[] copy = new byte[_payload.Length];
        _payload.AsSpan().CopyTo(copy);
        return copy;
    }

    private static byte[] CompressWithGZip(byte[] payload)
    {
        using var output = new MemoryStream();
        using (var compressor = new GZipStream(output, CompressionLevel.Fastest, leaveOpen: true))
            compressor.Write(payload);

        return output.ToArray();
    }

    private static byte[] DecompressWithGZip(byte[] payload)
    {
        using var input = new MemoryStream(payload);
        using var decompressor = new GZipStream(input, CompressionMode.Decompress);
        using var output = new MemoryStream();
        decompressor.CopyTo(output);
        return output.ToArray();
    }

    private static byte[] CompressWithBrotli(byte[] payload)
    {
        using var output = new MemoryStream();
        using (var compressor = new BrotliStream(output, CompressionLevel.Fastest, leaveOpen: true))
            compressor.Write(payload);

        return output.ToArray();
    }

    private static byte[] DecompressWithBrotli(byte[] payload)
    {
        using var input = new MemoryStream(payload);
        using var decompressor = new BrotliStream(input, CompressionMode.Decompress);
        using var output = new MemoryStream();
        decompressor.CopyTo(output);
        return output.ToArray();
    }

    private static byte[] CreatePayload(CompressionPayloadKind kind, int length)
    {
        byte[] source = Encoding.UTF8.GetBytes(kind switch
        {
            CompressionPayloadKind.RecordPayload => CreateRecordPayload(length),
            CompressionPayloadKind.CollectionPayload => CreateCollectionPayload(length),
            CompressionPayloadKind.PagePayload => CreatePageLikePayload(length),
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null),
        });

        if (source.Length == length)
            return source;

        byte[] result = new byte[length];
        int copied = 0;
        while (copied < result.Length)
        {
            int take = Math.Min(source.Length, result.Length - copied);
            source.AsSpan(0, take).CopyTo(result.AsSpan(copied));
            copied += take;
        }

        return result;
    }

    private static string CreateRecordPayload(int length)
    {
        var builder = new StringBuilder(length + 256);
        for (int i = 0; builder.Length < length; i++)
        {
            builder.Append("id=").Append(i)
                .Append(";name=user-").Append(i % 1024)
                .Append(";region=").Append(i % 8)
                .Append(";status=active;notes=repeatable storage row payload;score=")
                .Append(i % 100)
                .Append('\n');
        }

        return builder.ToString();
    }

    private static string CreateCollectionPayload(int length)
    {
        var builder = new StringBuilder(length + 256);
        builder.Append('[');
        for (int i = 0; builder.Length < length; i++)
        {
            if (i > 0)
                builder.Append(',');

            builder.Append("{\"key\":\"doc-").Append(i)
                .Append("\",\"tenant\":\"tenant-").Append(i % 16)
                .Append("\",\"tags\":[\"alpha\",\"beta\",\"search\"],\"profile\":{\"active\":true,\"tier\":")
                .Append(i % 4)
                .Append("},\"description\":\"collection payload compression candidate\"}");
        }

        builder.Append(']');
        return builder.ToString();
    }

    private static string CreatePageLikePayload(int length)
    {
        var builder = new StringBuilder(length + 256);
        builder.Append("CSDB").Append('\0', 96);
        for (int i = 0; builder.Length < length; i++)
        {
            builder.Append("cell:").Append(i)
                .Append("|ptr=").Append(i % 4096)
                .Append("|free=").Append((4096 - i) & 0x0FFF)
                .Append("|payload=page-local repeated value block")
                .Append('\0', 4);
        }

        return builder.ToString();
    }
}

public enum CompressionPayloadKind
{
    RecordPayload,
    CollectionPayload,
    PagePayload,
}
