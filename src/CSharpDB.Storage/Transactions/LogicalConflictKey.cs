namespace CSharpDB.Storage.Transactions;

internal readonly struct LogicalConflictKey : IEquatable<LogicalConflictKey>
{
    public LogicalConflictKey(string resourceName, long key)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(resourceName);

        ResourceName = resourceName;
        Key = key;
    }

    public string ResourceName { get; }

    public long Key { get; }

    public bool Equals(LogicalConflictKey other)
        => Key == other.Key &&
           StringComparer.OrdinalIgnoreCase.Equals(ResourceName, other.ResourceName);

    public override bool Equals(object? obj)
        => obj is LogicalConflictKey other && Equals(other);

    public override int GetHashCode()
        => HashCode.Combine(StringComparer.OrdinalIgnoreCase.GetHashCode(ResourceName), Key);

    public override string ToString()
        => $"{ResourceName}[{Key}]";
}
