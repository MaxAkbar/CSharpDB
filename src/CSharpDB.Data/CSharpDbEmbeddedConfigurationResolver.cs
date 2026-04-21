using CSharpDB.Engine;

namespace CSharpDB.Data;

internal static class CSharpDbEmbeddedConfigurationResolver
{
    internal static bool HasRequestedTuning(
        CSharpDbConnectionStringBuilder builder,
        DatabaseOptions? directDatabaseOptions,
        HybridDatabaseOptions? hybridDatabaseOptions)
    {
        ArgumentNullException.ThrowIfNull(builder);

        return directDatabaseOptions is not null
            || hybridDatabaseOptions is not null
            || builder.StoragePreset is not null
            || builder.EmbeddedOpenMode is not null;
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
            ?? CreateDirectDatabaseOptions(requestedStoragePreset);

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
                || requestedOpenMode is not null);
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

    private static DatabaseOptions CreateDirectDatabaseOptions(CSharpDbStoragePreset? storagePreset)
    {
        if (storagePreset is null)
            return new DatabaseOptions();

        return new DatabaseOptions().ConfigureStorageEngine(builder =>
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
    bool HasRequestedTuning);
