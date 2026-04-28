using CSharpDB.Primitives;
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
            Functions = options.Functions,
            ImplicitInsertExecutionMode = options.ImplicitInsertExecutionMode,
            StorageEngineFactory = options.StorageEngineFactory,
            StorageEngineOptions = options.StorageEngineOptions.Configure(configure),
        };
    }

    /// <summary>
    /// Registers trusted in-process scalar functions and returns a new DatabaseOptions instance.
    /// </summary>
    public static DatabaseOptions ConfigureFunctions(
        this DatabaseOptions options,
        Action<DbFunctionRegistryBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(configure);

        return new DatabaseOptions
        {
            Functions = DbFunctionRegistry.Create(configure),
            ImplicitInsertExecutionMode = options.ImplicitInsertExecutionMode,
            StorageEngineFactory = options.StorageEngineFactory,
            StorageEngineOptions = options.StorageEngineOptions,
        };
    }
}
