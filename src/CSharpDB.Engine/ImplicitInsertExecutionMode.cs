namespace CSharpDB.Engine;

/// <summary>
/// Controls how shared auto-commit INSERT statements execute on a Database handle.
/// </summary>
public enum ImplicitInsertExecutionMode
{
    /// <summary>
    /// Preserve the legacy shared write-gate path so implicit inserts are serialized on the Database handle.
    /// This remains the default because hot right-edge insert workloads can perform better when serialized.
    /// </summary>
    Serialized = 0,

    /// <summary>
    /// Route each implicit insert through an isolated WriteTransaction so disjoint-key insert workloads can
    /// overlap and batch durable commits. This can regress hot right-edge insert patterns.
    /// </summary>
    ConcurrentWriteTransactions = 1,
}
