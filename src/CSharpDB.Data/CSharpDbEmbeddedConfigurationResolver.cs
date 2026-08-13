using CSharpDB.Engine;
using CSharpDB.Observability;

namespace CSharpDB.Data;

internal static class CSharpDbEmbeddedConfigurationResolver
{
    internal static TimeProvider? RuntimeDiagnosticsTimeProviderForTest { get; set; }

    internal static bool HasRequestedTuning(
        CSharpDbConnectionStringBuilder builder,
        DatabaseOptions? directDatabaseOptions,
        HybridDatabaseOptions? hybridDatabaseOptions)
    {
        ArgumentNullException.ThrowIfNull(builder);

        return directDatabaseOptions is not null
            || hybridDatabaseOptions is not null
            || builder.StoragePreset is not null
            || builder.EmbeddedOpenMode is not null
            || builder.AdaptiveQueryReoptimization;
    }

    internal static ResolvedEmbeddedConfiguration Resolve(
        CSharpDbConnectionStringBuilder builder,
        DatabaseOptions? directDatabaseOptions,
        HybridDatabaseOptions? hybridDatabaseOptions,
        CSharpDbStoragePreset? storagePresetOverride = null,
        CSharpDbEmbeddedOpenMode? embeddedOpenModeOverride = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        CSharpDbStoragePreset? requestedStoragePreset = storagePresetOverride ?? builder.StoragePreset;
        CSharpDbEmbeddedOpenMode? requestedOpenMode = embeddedOpenModeOverride ?? builder.EmbeddedOpenMode;

        DatabaseOptions effectiveDirectDatabaseOptions = directDatabaseOptions
            ?? CreateDirectDatabaseOptions(requestedStoragePreset, builder.AdaptiveQueryReoptimization);
        DatabaseOptions runtimeDirectDatabaseOptions = directDatabaseOptions is null
            ? effectiveDirectDatabaseOptions
            : DataObservabilityOptionsSnapshot.Freeze(effectiveDirectDatabaseOptions);
        DataRuntimeDiagnosticsStateOwner? runtimeDiagnosticsStateOwner = null;
        if (runtimeDirectDatabaseOptions.ObservabilityOptions?.Enabled == true &&
            runtimeDirectDatabaseOptions.RuntimeDiagnosticsState is null)
        {
            var runtimeDiagnosticsState = new CSharpDbRuntimeDiagnosticsState(
                runtimeDirectDatabaseOptions.ObservabilityOptions,
                RuntimeDiagnosticsTimeProviderForTest);
            runtimeDiagnosticsStateOwner = new DataRuntimeDiagnosticsStateOwner(
                runtimeDiagnosticsState);
            runtimeDirectDatabaseOptions =
                DataObservabilityOptionsSnapshot.WithRuntimeDiagnosticsState(
                    runtimeDirectDatabaseOptions,
                    runtimeDiagnosticsState);
        }

        HybridDatabaseOptions? effectiveHybridDatabaseOptions = hybridDatabaseOptions
            ?? CreateHybridDatabaseOptions(requestedOpenMode);

        return new ResolvedEmbeddedConfiguration(
            effectiveDirectDatabaseOptions,
            effectiveHybridDatabaseOptions,
            effectiveHybridDatabaseOptions is null
                ? CSharpDbEmbeddedOpenMode.Direct
                : GetEffectiveOpenMode(effectiveHybridDatabaseOptions),
            directDatabaseOptions is null ? requestedStoragePreset : null,
            directDatabaseOptions,
            hybridDatabaseOptions,
            directDatabaseOptions is not null
                || hybridDatabaseOptions is not null
                || requestedStoragePreset is not null
                || requestedOpenMode is not null
                || builder.AdaptiveQueryReoptimization,
            builder.AdaptiveQueryReoptimization && directDatabaseOptions is null,
            runtimeDirectDatabaseOptions,
            runtimeDiagnosticsStateOwner);
    }

    internal static CSharpDbEmbeddedOpenMode GetEffectiveOpenMode(HybridDatabaseOptions hybridDatabaseOptions)
    {
        ArgumentNullException.ThrowIfNull(hybridDatabaseOptions);

        return hybridDatabaseOptions.PersistenceMode switch
        {
            HybridPersistenceMode.IncrementalDurable => CSharpDbEmbeddedOpenMode.HybridIncrementalDurable,
            HybridPersistenceMode.Snapshot => CSharpDbEmbeddedOpenMode.HybridSnapshot,
            _ => throw new InvalidOperationException(
                $"Unsupported hybrid persistence mode '{hybridDatabaseOptions.PersistenceMode}'."),
        };
    }

    private static DatabaseOptions CreateDirectDatabaseOptions(
        CSharpDbStoragePreset? storagePreset,
        bool adaptiveQueryReoptimization)
    {
        DatabaseOptions options = adaptiveQueryReoptimization
            ? new DatabaseOptions().EnableAdaptiveQueryReoptimization()
            : new DatabaseOptions();

        if (storagePreset is null)
            return options;

        return options.ConfigureStorageEngine(builder =>
        {
            switch (storagePreset.Value)
            {
                case CSharpDbStoragePreset.DirectLookupOptimized:
                    builder.UseDirectLookupOptimizedPreset();
                    break;
                case CSharpDbStoragePreset.DirectColdFileLookup:
                    builder.UseDirectColdFileLookupPreset();
                    break;
                case CSharpDbStoragePreset.HybridFileCache:
                    builder.UseHybridFileCachePreset();
                    break;
                case CSharpDbStoragePreset.WriteOptimized:
                    builder.UseWriteOptimizedPreset();
                    break;
                case CSharpDbStoragePreset.LowLatencyDurableWrite:
                    builder.UseLowLatencyDurableWritePreset();
                    break;
                default:
                    throw new InvalidOperationException($"Unsupported storage preset '{storagePreset.Value}'.");
            }
        });
    }

    private static HybridDatabaseOptions? CreateHybridDatabaseOptions(CSharpDbEmbeddedOpenMode? embeddedOpenMode)
    {
        return embeddedOpenMode switch
        {
            null or CSharpDbEmbeddedOpenMode.Direct => null,
            CSharpDbEmbeddedOpenMode.HybridIncrementalDurable => new HybridDatabaseOptions
            {
                PersistenceMode = HybridPersistenceMode.IncrementalDurable,
            },
            CSharpDbEmbeddedOpenMode.HybridSnapshot => new HybridDatabaseOptions
            {
                PersistenceMode = HybridPersistenceMode.Snapshot,
            },
            _ => throw new InvalidOperationException($"Unsupported embedded open mode '{embeddedOpenMode}'."),
        };
    }
}

internal readonly record struct ResolvedEmbeddedConfiguration(
    DatabaseOptions EffectiveDirectDatabaseOptions,
    HybridDatabaseOptions? EffectiveHybridDatabaseOptions,
    CSharpDbEmbeddedOpenMode EffectiveOpenMode,
    CSharpDbStoragePreset? EffectiveStoragePreset,
    DatabaseOptions? ExplicitDirectDatabaseOptions,
    HybridDatabaseOptions? ExplicitHybridDatabaseOptions,
    bool HasRequestedTuning,
    bool EffectiveAdaptiveQueryReoptimization,
    DatabaseOptions RuntimeDirectDatabaseOptions,
    DataRuntimeDiagnosticsStateOwner? RuntimeDiagnosticsStateOwner)
{
    internal bool HasRuntimeDiagnosticsStateForTest =>
        RuntimeDirectDatabaseOptions.RuntimeDiagnosticsState is not null;

    internal object? RuntimeDiagnosticsStateForTest =>
        RuntimeDirectDatabaseOptions.RuntimeDiagnosticsState;
}

/// <summary>
/// Owns a resolver-created diagnostics state until the physical embedded
/// family that adopted it reaches final retirement. The wrapper also lets
/// cached open plans recognize that a retired family must be resolved again.
/// A direct Database is one physical family; a pool or named-memory host keeps
/// its family alive across logical connection closes until explicit retirement.
/// </summary>
internal sealed class DataRuntimeDiagnosticsStateOwner : IDisposable
{
    private CSharpDbRuntimeDiagnosticsState? _state;

    internal DataRuntimeDiagnosticsStateOwner(
        CSharpDbRuntimeDiagnosticsState state)
    {
        _state = state ?? throw new ArgumentNullException(nameof(state));
    }

    internal CSharpDbRuntimeDiagnosticsState State =>
        Volatile.Read(ref _state) ??
        throw new ObjectDisposedException(GetType().FullName);

    internal bool IsDisposed => Volatile.Read(ref _state) is null;

    public void Dispose()
        => Interlocked.Exchange(ref _state, null)?.Dispose();
}
