
namespace CSharpDB.Engine;

/// <summary>
/// Composition options for Database.OpenAsync.
/// </summary>
public sealed class DatabaseOptions
{
    /// <summary>
    /// Storage-engine component options.
    /// </summary>
    public StorageEngineOptions StorageEngineOptions { get; init; } = new();

    /// <summary>
    /// Factory used to compose storage engine components.
    /// </summary>
    public IStorageEngineFactory StorageEngineFactory { get; init; } = new DefaultStorageEngineFactory();
}
