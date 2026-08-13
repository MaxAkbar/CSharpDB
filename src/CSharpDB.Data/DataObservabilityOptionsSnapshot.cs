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
            RuntimeDiagnosticsState = configured.RuntimeDiagnosticsState,
            StorageEngineFactory = configured.StorageEngineFactory,
            StorageEngineOptions = configured.StorageEngineOptions,
            WindowExecution = configured.WindowExecution,
        };
    }

    internal static DatabaseOptions WithRuntimeDiagnosticsState(
        DatabaseOptions configured,
        CSharpDbRuntimeDiagnosticsState runtimeDiagnosticsState)
    {
        ArgumentNullException.ThrowIfNull(configured);
        ArgumentNullException.ThrowIfNull(runtimeDiagnosticsState);

        return new DatabaseOptions
        {
            AdaptiveQueryReoptimization = configured.AdaptiveQueryReoptimization,
            Functions = configured.Functions,
            ImplicitInsertExecutionMode = configured.ImplicitInsertExecutionMode,
            ObservabilityOptions = configured.ObservabilityOptions,
            RuntimeDiagnosticsState = runtimeDiagnosticsState,
            StorageEngineFactory = configured.StorageEngineFactory,
            StorageEngineOptions = configured.StorageEngineOptions,
            WindowExecution = configured.WindowExecution,
        };
    }

    internal static DatabaseOptions WithRuntimeDiagnosticsStateForTest(
        DatabaseOptions configured,
        object runtimeDiagnosticsState)
        => WithRuntimeDiagnosticsState(
            configured,
            runtimeDiagnosticsState as CSharpDbRuntimeDiagnosticsState
            ?? throw new ArgumentException(
                "The value is not a runtime diagnostics state.",
                nameof(runtimeDiagnosticsState)));

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
