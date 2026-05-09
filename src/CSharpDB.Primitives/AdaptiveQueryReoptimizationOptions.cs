namespace CSharpDB.Primitives;

/// <summary>
/// Controls opt-in adaptive query re-optimization for SELECT joins.
/// </summary>
public sealed class AdaptiveQueryReoptimizationOptions
{
    public bool Enabled { get; init; }
    public int DivergenceFactor { get; init; } = 8;
    public int MinimumObservedRows { get; init; } = 4096;
    public int MaxBufferedRows { get; init; } = 65536;
    public int MaxReoptimizationsPerQuery { get; init; } = 1;
}
