using System.Text.Json;
using CSharpDB.Engine;
using CSharpDB.Observability;

namespace CSharpDB.Data;

internal static class DataObservabilityOptionsSnapshot
{
    internal static DatabaseOptions Freeze(DatabaseOptions configured)
    {
        ArgumentNullException.ThrowIfNull(configured);

        return new DatabaseOptions
        {
            AdaptiveQueryReoptimization = configured.AdaptiveQueryReoptimization,
            Functions = configured.Functions,
            ImplicitInsertExecutionMode = configured.ImplicitInsertExecutionMode,
            ObservabilityOptions = Create(configured.ObservabilityOptions),
            StorageEngineFactory = configured.StorageEngineFactory,
            StorageEngineOptions = configured.StorageEngineOptions,
            WindowExecution = configured.WindowExecution,
        };
    }

    internal static CSharpDbObservabilityOptions? Create(
        CSharpDbObservabilityOptions? configured)
    {
        if (configured?.Enabled != true)
            return null;

        byte[] json = JsonSerializer.SerializeToUtf8Bytes(
            configured,
            CSharpDbObservabilityJsonContext.Default.CSharpDbObservabilityOptions);
        return JsonSerializer.Deserialize(
                json,
                CSharpDbObservabilityJsonContext.Default.CSharpDbObservabilityOptions)
            ?? throw new InvalidOperationException(
                "The observability options snapshot could not be created.");
    }
}
