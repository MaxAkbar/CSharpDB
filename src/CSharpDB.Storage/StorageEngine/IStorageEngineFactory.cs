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

    /// <summary>
    /// Creates a new storage engine, atomically refusing to open or replace an existing database file.
    /// </summary>
    /// <remarks>
    /// Implementations that support this operation must use create-new semantics at the storage-device
    /// boundary. The default implementation fails safely so custom factories cannot accidentally fall
    /// back to open-or-create behavior.
    /// </remarks>
    ValueTask<StorageEngineContext> CreateNewAsync(
        string filePath,
        StorageEngineOptions options,
        CancellationToken ct = default)
        => throw new NotSupportedException(
            $"Storage engine factory '{GetType().FullName}' does not support atomic create-new semantics.");
}
