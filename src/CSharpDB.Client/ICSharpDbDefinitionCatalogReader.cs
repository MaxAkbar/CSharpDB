using CSharpDB.Client.Models;

namespace CSharpDB.Client;

/// <summary>Optional, non-initializing access to persisted definitions. Never executes their contents.</summary>
public interface ICSharpDbDefinitionCatalogReader
{
    Task<DefinitionCatalogPage> ReadDefinitionCatalogAsync(
        string? continuationToken = null, int pageSize = 64, CancellationToken ct = default);
}
