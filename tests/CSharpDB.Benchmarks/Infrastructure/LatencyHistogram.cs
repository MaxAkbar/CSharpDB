namespace CSharpDB.Benchmarks.Infrastructure;

/// <summary>
/// Simple latency histogram that collects individual latency samples
/// and computes percentiles, min, max, mean, and standard deviation.
/// </summary>
public sealed class LatencyHistogram
{
    private readonly List<double> _samples = new();
    private bool _sorted;

    public int Count => _samples.Count;

    /// <summary>
    /// Record a latency sample in milliseconds.
    /// </summary>
    public void Record(double latencyMs)
    {
        _samples.Add(latencyMs);
        _sorted = false;
    }

    /// <summary>
    /// Compute a percentile (0.0 to 1.0). e.g., 0.5 = P50, 0.99 = P99.
    /// </summary>
    public double Percentile(double p)
    {
        if (_samples.Count == 0)
            return 0;

        EnsureSorted();

        double index = p * (_samples.Count - 1);
        int lower = (int)Math.Floor(index);
        int upper = (int)Math.Ceiling(index);
        if (lower == upper)
            return _samples[lower];

        double weight = index - lower;
        return _samples[lower] * (1 - weight) + _samples[upper] * weight;
    }

    public double Min => _samples.Count > 0 ? _samples.Min() : 0;
    public double Max => _samples.Count > 0 ? _samples.Max() : 0;

    public double Mean
    {
        get
        {
            if (_samples.Count == 0) return 0;
            return _samples.Average();
        }
    }

    public double StdDev
    {
        get
        {
            if (_samples.Count < 2) return 0;
            double mean = Mean;
            double sumSq = _samples.Sum(x => (x - mean) * (x - mean));
            return Math.Sqrt(sumSq / (_samples.Count - 1));
        }
    }

    private void EnsureSorted()
    {
        if (!_sorted)
        {
            _samples.Sort();
            _sorted = true;
        }
    }
}
