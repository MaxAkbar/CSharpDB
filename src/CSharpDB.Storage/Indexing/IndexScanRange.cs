namespace CSharpDB.Storage.Indexing;

/// <summary>
/// Inclusive/exclusive bounds for ordered index scans.
/// </summary>
public readonly record struct IndexScanRange(
    long? LowerBound,
    bool LowerInclusive,
    long? UpperBound,
    bool UpperInclusive)
{
    public static IndexScanRange All { get; } = new(null, true, null, true);

    public static IndexScanRange At(long key) => new(key, true, key, true);
}
