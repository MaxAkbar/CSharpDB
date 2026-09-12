using System.Buffers.Binary;
using System.Globalization;
using System.Security.Cryptography;
using System.Text.Json;

namespace CSharpDB.DataGeneration;

/// <summary>Version 1: SHA-256 over typed JSON seed parts, then SplitMix64. No process hash state.</summary>
public sealed class StableRandom : Random
{
    public const string Version = "sha256-splitmix64-v1/bogus-35.6.5/net10";
    private ulong _state;
    public StableRandom(ulong seed) => _state = seed;

    public static StableRandom Create(params object?[] parts)
    {
        byte[] bytes = JsonSerializer.SerializeToUtf8Bytes(parts.Select(CanonicalPart).ToArray());
        return new StableRandom(BinaryPrimitives.ReadUInt64LittleEndian(SHA256.HashData(bytes)));
    }

    private static object? CanonicalPart(object? part) => part switch
    {
        DateTime date => new object[] { "date", date.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture) },
        DateTimeOffset date => new object[] { "offset", date.ToString("O", CultureInfo.InvariantCulture) },
        byte[] bytes => new object[] { "bytes", Convert.ToHexString(bytes) },
        System.Collections.IEnumerable values when part is not string => values.Cast<object?>().Select(CanonicalPart).ToArray(),
        null => null,
        _ => new object?[] { part.GetType().FullName, part },
    };

    private ulong NextBits()
    {
        ulong z = unchecked(_state += 0x9E3779B97F4A7C15UL);
        z = unchecked((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL);
        z = unchecked((z ^ (z >> 27)) * 0x94D049BB133111EBUL);
        return z ^ (z >> 31);
    }

    private ulong Bounded(ulong range)
    {
        if (range == 0) return NextBits();
        ulong threshold = unchecked(0UL - range) % range;
        ulong value;
        do value = NextBits(); while (value < threshold);
        return value % range;
    }

    public override double NextDouble() => (NextBits() >> 11) * (1.0 / (1UL << 53));
    public override float NextSingle() => (float)((NextBits() >> 40) * (1.0 / (1UL << 24)));
    protected override double Sample() => NextDouble();
    public override int Next() => (int)Bounded(int.MaxValue);
    public override int Next(int maxValue) => Next(0, maxValue);
    public override int Next(int minValue, int maxValue)
    {
        if (minValue > maxValue) throw new ArgumentOutOfRangeException(nameof(maxValue));
        return minValue == maxValue ? minValue : (int)(minValue + (long)Bounded((ulong)((long)maxValue - minValue)));
    }
    public override long NextInt64() => (long)Bounded(long.MaxValue);
    public override long NextInt64(long maxValue) => NextInt64(0, maxValue);
    public override long NextInt64(long minValue, long maxValue)
    {
        if (minValue > maxValue) throw new ArgumentOutOfRangeException(nameof(maxValue));
        return minValue == maxValue ? minValue : unchecked((long)((ulong)minValue + Bounded(unchecked((ulong)maxValue - (ulong)minValue))));
    }
    public long NextInclusive(long minValue, long maxValue)
    {
        if (minValue > maxValue) throw new ArgumentOutOfRangeException(nameof(maxValue));
        return unchecked((long)((ulong)minValue + Bounded(unchecked((ulong)maxValue - (ulong)minValue + 1))));
    }
    public override void NextBytes(byte[] buffer) => NextBytes(buffer.AsSpan());
    public override void NextBytes(Span<byte> buffer)
    {
        for (int i = 0; i < buffer.Length; i++) buffer[i] = (byte)NextBits();
    }
}
