using CSharpDB.Client.Models;

namespace CSharpDB.Client;

/// <summary>
/// Optional direct-transport capability for reading schema and rows from an
/// existing client-managed transaction.
/// </summary>
public interface ICSharpDbTransactionalSnapshotReader
{
    /// <summary>
    /// Gets whether transaction-bound snapshot reads are available.
    /// </summary>
    bool SupportsTransactionalSnapshotReads { get; }

    /// <summary>
    /// Reads a table's schema and secondary indexes while holding the specified
    /// transaction's operation gate.
    /// </summary>
    ValueTask<TransactionTableSnapshot?> ReadTableSnapshotAsync(
        string transactionId,
        string tableName,
        CancellationToken ct = default);

    /// <summary>
    /// Opens a forward-only SQL query cursor in the specified transaction. The
    /// transaction remains reserved until the returned cursor is disposed.
    /// </summary>
    ValueTask<ForwardOnlyQueryCursor?> TryOpenForwardOnlyQueryCursorAsync(
        string transactionId,
        string sql,
        CancellationToken ct = default);
}

/// <summary>
/// Transaction-consistent schema metadata for one table.
/// </summary>
public sealed record TransactionTableSnapshot
{
    public required TableSchema Schema { get; init; }
    public IReadOnlyList<IndexSchema> Indexes { get; init; } = Array.Empty<IndexSchema>();
}
