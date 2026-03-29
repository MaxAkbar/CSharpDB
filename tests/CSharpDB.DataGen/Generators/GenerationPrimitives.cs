using System.Globalization;
using System.Text;

namespace CSharpDB.DataGen.Generators;

internal static class GenerationPrimitives
{
    private static readonly string[] s_textFragments =
    [
        "csharpdb",
        "benchmark",
        "storage",
        "durable",
        "hotset",
        "btree",
        "journal",
        "cache",
        "range",
        "collection",
        "cafe",
        "café",
        "naive",
        "naïve",
        "uber",
        "über",
        "smorgasbord",
        "smörgåsbord",
        "munchen",
        "München",
        "sao",
        "São",
        "東京",
        "delta",
    ];

    internal static readonly DateTime AnchorUtc = new(2026, 3, 28, 0, 0, 0, DateTimeKind.Utc);

    public static Random CreateRandom(int seed, long entityId, int salt = 0)
        => new(unchecked(HashCode.Combine(seed, salt, entityId)));

    public static long PickSkewedId(Random rng, long maxId, double hotKeyRate)
    {
        if (maxId <= 1)
            return 1;

        hotKeyRate = ClampRate(hotKeyRate);
        long hotBandUpper = Math.Max(1, maxId / 5);
        return rng.NextDouble() < hotKeyRate
            ? rng.NextInt64(1, hotBandUpper + 1)
            : rng.NextInt64(1, maxId + 1);
    }

    public static DateTime PickSkewedTimestamp(
        Random rng,
        DateTime anchorUtc,
        int recentWindowDays,
        int fullWindowDays,
        double recentRate)
    {
        recentWindowDays = Math.Max(1, recentWindowDays);
        fullWindowDays = Math.Max(recentWindowDays, fullWindowDays);
        recentRate = ClampRate(recentRate);

        int chosenWindowDays = rng.NextDouble() < recentRate ? recentWindowDays : fullWindowDays;
        int seconds = rng.Next(0, checked(chosenWindowDays * 24 * 60 * 60));
        return anchorUtc.AddSeconds(-seconds);
    }

    public static double NextMoney(Random rng, double minInclusive, double maxInclusive)
        => Math.Round(minInclusive + (rng.NextDouble() * (maxInclusive - minInclusive)), 2);

    public static string BuildSizedText(Random rng, int targetLength)
    {
        targetLength = Math.Max(16, targetLength);

        var builder = new StringBuilder(targetLength + 32);
        while (builder.Length < targetLength)
        {
            if (builder.Length > 0)
                builder.Append(' ');

            builder.Append(s_textFragments[rng.Next(s_textFragments.Length)]);
        }

        return builder.ToString(0, targetLength);
    }

    public static string ToIsoString(DateTime value)
        => value.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);

    public static Guid NextGuid(Random rng)
    {
        Span<byte> bytes = stackalloc byte[16];
        rng.NextBytes(bytes);
        return new Guid(bytes);
    }

    public static T PickOne<T>(Random rng, IReadOnlyList<T> values)
        => values[rng.Next(values.Count)];

    public static double ClampRate(double value) => Math.Clamp(value, 0d, 1d);
}
