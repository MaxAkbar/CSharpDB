using CSharpDB.Client.Models;

namespace CSharpDB.Client;

/// <summary>
/// Optional direct-transport capability used by exact recovery workflows to
/// apply trusted archived schema identities inside an existing transaction.
/// </summary>
public interface ICSharpDbTransactionalSchemaIdentityWriter
{
    /// <summary>
    /// Gets whether transaction-bound schema identity writes are available.
    /// </summary>
    bool SupportsTransactionalSchemaIdentityWrites { get; }

    /// <summary>
    /// Applies the identities from <paramref name="identitySource"/> to an
    /// existing structurally equivalent table.
    /// </summary>
    ValueTask ApplyTableSchemaIdentitiesAsync(
        string transactionId,
        string tableName,
        TableSchema identitySource,
        CancellationToken ct = default);
}
