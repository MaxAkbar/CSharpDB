namespace CSharpDB.Storage.StorageEngine;

public static class StorageEngineOptionsExtensions
{
    /// <summary>
    /// Applies provider registrations using a builder and returns a new options instance.
    /// </summary>
    public static StorageEngineOptions Configure(
        this StorageEngineOptions options,
        Action<StorageEngineOptionsBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(configure);

        var builder = new StorageEngineOptionsBuilder(options);
        configure(builder);
        return builder.Build();
    }
}
