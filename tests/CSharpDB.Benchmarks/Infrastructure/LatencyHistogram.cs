namespace CSharpDB.Benchmarks.Infrastructure;

/// <summary>
/// Simple latency histogram that tracks total operations and optionally samples
/// latency values at a fixed stride for percentile and summary statistics.
/// </summary>
public sealed class LatencyHistogram
{
    private readonly List<double> _samples = new();
    private readonly int _sampleEvery;
    private readonly int _sampleMask;
    private readonly bool _sampleEveryIsPowerOfTwo;
    private bool _sorted;
    private int _count;

    public LatencyHistogram(int sampleEvery = 1)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(sampleEvery, 1);

        _sampleEvery = sampleEvery;
        _sampleMask = sampleEvery - 1;
        _sampleEveryIsPowerOfTwo = (sampleEvery & (sampleEvery - 1)) == 0;
    }

    /// <summary>
    /// Total operations recorded, including unsampled ones.
    /// </summary>
    public int Count => _count;

    /// <summary>
    /// Number of latency samples retained for percentile and summary calculations.
    /// </summary>
    public int SampleCount => _samples.Count;

    /// <summary>
    /// Keep every Nth latency sample. A value of 1 stores every sample.
    /// </summary>
    public int SampleEvery => _sampleEvery;

    /// <summary>
    /// Returns true when the next operation should capture a latency sample.
    /// </summary>
    public bool ShouldSampleNext()
        => _sampleEvery == 1 ||
           (_sampleEveryIsPowerOfTwo
               ? (_count & _sampleMask) == 0
               : _count % _sampleEvery == 0);

    /// <summary>
    /// Record an operation without retaining a latency sample.
    /// </summary>
    public void RecordUnsampled()
    {
        _count = checked(_count + 1);
    }

    /// <summary>
    /// Record a latency sample in milliseconds.
    /// </summary>
    public void Record(double latencyMs)
    {
        int zeroBasedIndex = _count;
        _count = checked(_count + 1);

        if (_sampleEvery == 1 ||
            (_sampleEveryIsPowerOfTwo
                ? (zeroBasedIndex & _sampleMask) == 0
                : zeroBasedIndex % _sampleEvery == 0))
        {
            _samples.Add(latencyMs);
            _sorted = false;
        }
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
