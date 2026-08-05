using System.Globalization;

namespace CSharpDB.Benchmarks.Infrastructure;

internal sealed record ReleaseQualificationSettings(
    TimeSpan WarmupDuration,
    TimeSpan MinimumMeasuredDuration,
    int MinimumLatencySamples,
    TimeSpan MaximumMeasuredDuration)
{
    internal static ReleaseQualificationSettings DurableWrite { get; } = new(
        WarmupDuration: TimeSpan.FromSeconds(2),
        MinimumMeasuredDuration: TimeSpan.FromSeconds(30),
        MinimumLatencySamples: 10_000,
        MaximumMeasuredDuration: TimeSpan.FromSeconds(120));

    internal void Validate()
    {
        if (WarmupDuration < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(WarmupDuration));
        if (MinimumMeasuredDuration <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(MinimumMeasuredDuration));
        if (MinimumLatencySamples <= 0)
            throw new ArgumentOutOfRangeException(nameof(MinimumLatencySamples));
        if (MaximumMeasuredDuration < MinimumMeasuredDuration)
            throw new ArgumentOutOfRangeException(nameof(MaximumMeasuredDuration));
    }

    internal bool HasMetMeasurementTarget(TimeSpan elapsed, int retainedLatencySamples)
        => elapsed >= MinimumMeasuredDuration &&
           retainedLatencySamples >= MinimumLatencySamples;

    internal string CreateExtraInfo(
        DateTimeOffset measurementStartedUtc,
        DateTimeOffset measurementEndedUtc)
    {
        Validate();
        if (measurementStartedUtc.Offset != TimeSpan.Zero)
            throw new ArgumentException("Measurement start must be UTC.", nameof(measurementStartedUtc));
        if (measurementEndedUtc.Offset != TimeSpan.Zero)
            throw new ArgumentException("Measurement end must be UTC.", nameof(measurementEndedUtc));
        if (measurementEndedUtc < measurementStartedUtc)
            throw new ArgumentOutOfRangeException(
                nameof(measurementEndedUtc),
                "Measurement end cannot precede measurement start.");

        return $"qualification=true; " +
               $"unrecorded-warmup-seconds={FormatSeconds(WarmupDuration)}; " +
               $"minimum-measured-seconds={FormatSeconds(MinimumMeasuredDuration)}; " +
               $"minimum-retained-latency-samples={MinimumLatencySamples.ToString(CultureInfo.InvariantCulture)}; " +
               $"measurement-cap-seconds={FormatSeconds(MaximumMeasuredDuration)}; " +
               $"measurement-begin-utc={measurementStartedUtc:O}; " +
               $"measurement-end-utc={measurementEndedUtc:O}";
    }

    internal (DateTimeOffset BeginUtc, DateTimeOffset EndUtc) ParseAndValidateExtraInfo(
        string? extraInfo)
    {
        Validate();
        if (string.IsNullOrWhiteSpace(extraInfo))
            throw new InvalidOperationException("Qualification metadata is missing.");

        var metadata = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (string tokenText in extraInfo.Split(';'))
        {
            string token = tokenText.Trim();
            int separator = token.IndexOf('=');
            if (separator <= 0)
                continue;

            string name = token[..separator].Trim();
            string value = token[(separator + 1)..].Trim();
            if (!metadata.TryAdd(name, value))
            {
                throw new InvalidOperationException(
                    $"Qualification metadata contains duplicate key '{name}'.");
            }
        }

        var required = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["qualification"] = "true",
            ["unrecorded-warmup-seconds"] = FormatSeconds(WarmupDuration),
            ["minimum-measured-seconds"] = FormatSeconds(MinimumMeasuredDuration),
            ["minimum-retained-latency-samples"] =
                MinimumLatencySamples.ToString(CultureInfo.InvariantCulture),
            ["measurement-cap-seconds"] = FormatSeconds(MaximumMeasuredDuration),
        };
        foreach ((string name, string expectedValue) in required)
        {
            if (!metadata.TryGetValue(name, out string? actualValue) ||
                !string.Equals(actualValue, expectedValue, StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"Qualification metadata must declare '{name}={expectedValue}'.");
            }
        }

        DateTimeOffset beginUtc = ParseUtcTimestamp(metadata, "measurement-begin-utc");
        DateTimeOffset endUtc = ParseUtcTimestamp(metadata, "measurement-end-utc");
        TimeSpan measuredDuration = endUtc - beginUtc;
        if (measuredDuration < MinimumMeasuredDuration ||
            measuredDuration > MaximumMeasuredDuration)
        {
            throw new InvalidOperationException(
                $"Qualification metadata declares a measured interval of " +
                $"{measuredDuration.TotalMilliseconds:F3} ms; expected between " +
                $"{MinimumMeasuredDuration.TotalMilliseconds:F0} and " +
                $"{MaximumMeasuredDuration.TotalMilliseconds:F0} ms.");
        }

        return (beginUtc, endUtc);
    }

    private static DateTimeOffset ParseUtcTimestamp(
        IReadOnlyDictionary<string, string> metadata,
        string name)
    {
        if (!metadata.TryGetValue(name, out string? value) ||
            !DateTimeOffset.TryParseExact(
                value,
                "O",
                CultureInfo.InvariantCulture,
                DateTimeStyles.RoundtripKind,
                out DateTimeOffset timestamp) ||
            timestamp.Offset != TimeSpan.Zero)
        {
            throw new InvalidOperationException(
                $"Qualification metadata must declare a round-trip UTC '{name}'.");
        }

        return timestamp;
    }

    private static string FormatSeconds(TimeSpan duration)
        => duration.TotalSeconds.ToString("F0", CultureInfo.InvariantCulture);
}
