using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Engine;

public static class DatabaseOptionsExtensions
{
    /// <summary>
    /// Applies storage-engine provider registrations and returns a new DatabaseOptions instance.
    /// </summary>
    public static DatabaseOptions ConfigureStorageEngine(
        this DatabaseOptions options,
        Action<StorageEngineOptionsBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(configure);

        return new DatabaseOptions
        {
            StorageEngineFactory = options.StorageEngineFactory,
            StorageEngineOptions = options.StorageEngineOptions.Configure(configure),
        };
    }
}
