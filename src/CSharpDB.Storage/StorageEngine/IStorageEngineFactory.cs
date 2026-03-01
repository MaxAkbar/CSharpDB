namespace CSharpDB.Storage.StorageEngine;

/// <summary>
/// Factory for constructing storage engine components.
/// </summary>
public interface IStorageEngineFactory
{
    ValueTask<StorageEngineContext> OpenAsync(
        string filePath,
        StorageEngineOptions options,
        CancellationToken ct = default);
}
