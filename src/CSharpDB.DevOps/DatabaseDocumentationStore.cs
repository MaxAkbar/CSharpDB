using System.Globalization;
using CSharpDB.Client;
using CSharpDB.Client.Models;

namespace CSharpDB.DevOps;

/// <summary>Persists documentation overrides using the caller's already bound database client.</summary>
public sealed class DatabaseDocumentationStore
{
    public const int MaximumDescriptionLength = 4000;
    private const string Table = "__documentation_annotations";

    /// <summary>A null description removes the override while retaining its concurrency revision.</summary>
    public async Task SaveAsync(ICSharpDbClient client, string objectId, string fingerprint,
        long expectedRevision, string? description, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        if (expectedRevision < 0) throw new ArgumentOutOfRangeException(nameof(expectedRevision));
        if (description?.Length > MaximumDescriptionLength)
            throw new ArgumentException($"Descriptions can contain at most {MaximumDescriptionLength} characters.", nameof(description));
        description = string.IsNullOrWhiteSpace(description) ? null : description;
        var catalog = await DefinitionCatalogService.ReadAsync(client, ct: ct);
        if (catalog.DocumentationVersion < 1)
            throw new NotSupportedException("This server does not support database descriptions. Upgrade the server.");
        if (catalog.Diagnostics.Any(d => d.Source == Table || (d.Source == "Documentation" && d.Message.StartsWith("An invalid", StringComparison.Ordinal))))
            throw new InvalidOperationException("The documentation catalog could not be read. Repair it before saving descriptions.");
        var definition = catalog.Definitions.SingleOrDefault(d => d.Id == objectId);
        if (definition is null || !DatabaseDocumenterService.IsDocumentedKind(definition.Kind))
            throw new InvalidOperationException("This object is no longer available. Refresh the dictionary.");
        if (definition.Documentation is not { } documentation || documentation.DefinitionFingerprint != fingerprint)
            throw new InvalidOperationException("The definition changed. Refresh and review your description before saving.");
        if (documentation.Revision != expectedRevision)
            throw Conflict();
        long nextRevision = checked(expectedRevision + 1);
        var transaction = await client.BeginTransactionAsync(ct);
        try
        {
            Check(await client.ExecuteInTransactionAsync(transaction.TransactionId, $"""
                CREATE TABLE IF NOT EXISTS {Table} (
                    object_id TEXT PRIMARY KEY,
                    description TEXT,
                    definition_hash TEXT NOT NULL,
                    revision BIGINT NOT NULL,
                    updated_utc TEXT NOT NULL
                );
                """, ct));
            string values = $"description = {Literal(description)}, definition_hash = {Literal(fingerprint)}, revision = {nextRevision.ToString(CultureInfo.InvariantCulture)}, updated_utc = {Literal(DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture))}";
            SqlExecutionResult result;
            if (expectedRevision == 0)
            {
                result = await client.ExecuteInTransactionAsync(transaction.TransactionId, $"""
                    INSERT INTO {Table} (object_id, description, definition_hash, revision, updated_utc)
                    VALUES ({Literal(objectId)}, {Literal(description)}, {Literal(fingerprint)}, 1, {Literal(DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture))});
                    """, ct);
            }
            else
            {
                result = await client.ExecuteInTransactionAsync(transaction.TransactionId,
                    $"UPDATE {Table} SET {values} WHERE object_id = {Literal(objectId)} AND revision = {expectedRevision.ToString(CultureInfo.InvariantCulture)};", ct);
            }
            Check(result);
            if (result.RowsAffected != 1) throw Conflict();
            ct.ThrowIfCancellationRequested();
            await client.CommitTransactionAsync(transaction.TransactionId, ct);
        }
        catch
        {
            // Cleanup must still run when the UI cancels or switches databases.
            try { await client.RollbackTransactionAsync(transaction.TransactionId, CancellationToken.None); }
            catch { /* Preserve the save failure; the client owns transaction cleanup on disposal. */ }
            throw;
        }
    }

    private static InvalidOperationException Conflict() => new("Another user changed this description. Refresh before saving; your draft has been retained.");
    private static string Literal(string? value) => value is null ? "NULL" : "'" + value.Replace("'", "''", StringComparison.Ordinal) + "'";
    private static void Check(SqlExecutionResult result)
    {
        if (!string.IsNullOrWhiteSpace(result.Error)) throw new InvalidOperationException(result.Error);
    }
}
