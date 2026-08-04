namespace CSharpDB.Primitives;

/// <summary>
/// Controls bounded in-memory execution for SQL window functions.
/// </summary>
public sealed class WindowExecutionOptions
{
    /// <summary>
    /// Maximum number of rows in one logical window partition.
    /// </summary>
    public int MaxPartitionRows { get; init; } = 65536;

    /// <summary>
    /// Maximum number of input rows that one in-memory window stage may buffer.
    /// Queries that exceed the limit fail with <see cref="ErrorCode.ResourceLimitExceeded"/>.
    /// </summary>
    public int MaxBufferedRows { get; init; } = 262144;
}
