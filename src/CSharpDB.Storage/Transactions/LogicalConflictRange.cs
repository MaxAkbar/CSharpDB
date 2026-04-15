using CSharpDB.Storage.Indexing;

namespace CSharpDB.Storage.Transactions;

internal readonly struct LogicalConflictRange : IEquatable<LogicalConflictRange>
{
    public LogicalConflictRange(
        string resourceName,
        long? lowerBound,
        bool lowerInclusive,
        long? upperBound,
        bool upperInclusive)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(resourceName);

        ResourceName = resourceName;
        LowerBound = lowerBound;
        LowerInclusive = lowerInclusive;
        UpperBound = upperBound;
        UpperInclusive = upperInclusive;
    }

    public LogicalConflictRange(string resourceName, IndexScanRange range)
        : this(resourceName, range.LowerBound, range.LowerInclusive, range.UpperBound, range.UpperInclusive)
    {
    }

    public string ResourceName { get; }

    public long? LowerBound { get; }

    public bool LowerInclusive { get; }

    public long? UpperBound { get; }

    public bool UpperInclusive { get; }

    public bool Contains(LogicalConflictKey key)
    {
        if (!StringComparer.OrdinalIgnoreCase.Equals(ResourceName, key.ResourceName))
            return false;

        if (LowerBound.HasValue)
        {
            if (LowerInclusive)
            {
                if (key.Key < LowerBound.Value)
                    return false;
            }
            else if (key.Key <= LowerBound.Value)
            {
                return false;
            }
        }

        if (UpperBound.HasValue)
        {
            if (UpperInclusive)
            {
                if (key.Key > UpperBound.Value)
                    return false;
            }
            else if (key.Key >= UpperBound.Value)
            {
                return false;
            }
        }

        return true;
    }

    public bool Equals(LogicalConflictRange other)
        => StringComparer.OrdinalIgnoreCase.Equals(ResourceName, other.ResourceName) &&
           LowerBound == other.LowerBound &&
           LowerInclusive == other.LowerInclusive &&
           UpperBound == other.UpperBound &&
           UpperInclusive == other.UpperInclusive;

    public override bool Equals(object? obj)
        => obj is LogicalConflictRange other && Equals(other);

    public override int GetHashCode()
        => HashCode.Combine(
            StringComparer.OrdinalIgnoreCase.GetHashCode(ResourceName),
            LowerBound,
            LowerInclusive,
            UpperBound,
            UpperInclusive);

    public override string ToString()
    {
        string lower = LowerBound?.ToString() ?? "-inf";
        string upper = UpperBound?.ToString() ?? "+inf";
        char lowerBracket = LowerInclusive ? '[' : '(';
        char upperBracket = UpperInclusive ? ']' : ')';
        return $"{ResourceName}{lowerBracket}{lower}, {upper}{upperBracket}";
    }
}
