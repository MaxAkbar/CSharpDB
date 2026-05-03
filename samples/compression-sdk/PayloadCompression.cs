using System.IO.Compression;
using System.Text;

namespace CSharpDB.Samples.CompressionSdk;

public enum CompressionCodec
{
    None,
    GZip,
    Brotli,
}

public readonly record struct CompressedPayload(CompressionCodec Codec, byte[] Bytes, int OriginalByteCount);

public static class PayloadCompression
{
    public static CompressedPayload CompressText(
        string value,
        CompressionCodec codec = CompressionCodec.GZip,
        int minimumBytes = 1024)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(value);
        return CompressBytes(bytes, codec, minimumBytes);
    }

    public static string DecompressText(CompressedPayload payload)
        => Encoding.UTF8.GetString(DecompressBytes(payload));

    public static CompressedPayload CompressBytes(
        byte[] value,
        CompressionCodec codec = CompressionCodec.GZip,
        int minimumBytes = 1024)
    {
        if (codec == CompressionCodec.None || value.Length < minimumBytes)
            return new CompressedPayload(CompressionCodec.None, value, value.Length);

        return new CompressedPayload(codec, Compress(value, codec), value.Length);
    }

    public static byte[] DecompressBytes(CompressedPayload payload)
        => payload.Codec == CompressionCodec.None
            ? payload.Bytes
            : Decompress(payload.Bytes, payload.Codec);

    private static byte[] Compress(byte[] payload, CompressionCodec codec)
    {
        using var output = new MemoryStream();
        Stream compressor = codec switch
        {
            CompressionCodec.GZip => new GZipStream(output, CompressionLevel.Fastest, leaveOpen: true),
            CompressionCodec.Brotli => new BrotliStream(output, CompressionLevel.Fastest, leaveOpen: true),
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
            _ => throw new ArgumentOutOfRangeException(nameof(codec), codec, null),
        };

        using (decompressor)
        using (var output = new MemoryStream())
        {
            decompressor.CopyTo(output);
            return output.ToArray();
        }
    }
}
