namespace CSharpDB.Samples.CompressionSdk.Infrastructure;

public sealed class LatencyHistogram
{
    private readonly List<double> _samples = new();
    private bool _sorted;

    public int Count => _samples.Count;
    public double Min => _samples.Count > 0 ? _samples.Min() : 0;
    public double Max => _samples.Count > 0 ? _samples.Max() : 0;
    public double Mean => _samples.Count > 0 ? _samples.Average() : 0;

    public double StdDev
    {
        get
        {
            if (_samples.Count < 2)
                return 0;

            double mean = Mean;
            double sumSq = _samples.Sum(value => (value - mean) * (value - mean));
            return Math.Sqrt(sumSq / (_samples.Count - 1));
        }
    }

    public void Record(double latencyMs)
    {
        _samples.Add(latencyMs);
        _sorted = false;
    }

    public double Percentile(double percentile)
    {
        if (_samples.Count == 0)
            return 0;

        EnsureSorted();
        double index = percentile * (_samples.Count - 1);
        int lower = (int)Math.Floor(index);
        int upper = (int)Math.Ceiling(index);
        if (lower == upper)
            return _samples[lower];

        double weight = index - lower;
        return _samples[lower] * (1 - weight) + _samples[upper] * weight;
    }

    private void EnsureSorted()
    {
        if (_sorted)
            return;

        _samples.Sort();
        _sorted = true;
    }
}
