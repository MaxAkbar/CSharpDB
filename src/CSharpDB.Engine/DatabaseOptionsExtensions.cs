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
            AdaptiveQueryReoptimization = options.AdaptiveQueryReoptimization,
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
            AdaptiveQueryReoptimization = options.AdaptiveQueryReoptimization,
            Functions = DbFunctionRegistry.Create(configure),
            ImplicitInsertExecutionMode = options.ImplicitInsertExecutionMode,
            StorageEngineFactory = options.StorageEngineFactory,
            StorageEngineOptions = options.StorageEngineOptions,
        };
    }

    /// <summary>
    /// Enables opt-in adaptive join re-optimization and returns a new DatabaseOptions instance.
    /// </summary>
    public static DatabaseOptions EnableAdaptiveQueryReoptimization(
        this DatabaseOptions options,
        Action<AdaptiveQueryReoptimizationOptionsBuilder>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(options);

        var builder = new AdaptiveQueryReoptimizationOptionsBuilder();
        configure?.Invoke(builder);

        return new DatabaseOptions
        {
            AdaptiveQueryReoptimization = builder.Build(enabled: true),
            Functions = options.Functions,
            ImplicitInsertExecutionMode = options.ImplicitInsertExecutionMode,
            StorageEngineFactory = options.StorageEngineFactory,
            StorageEngineOptions = options.StorageEngineOptions,
        };
    }
}

public sealed class AdaptiveQueryReoptimizationOptionsBuilder
{
    private int _divergenceFactor = 8;
    private int _minimumObservedRows = 4096;
    private int _maxBufferedRows = 65536;
    private int _maxReoptimizationsPerQuery = 1;

    public AdaptiveQueryReoptimizationOptionsBuilder WithDivergenceFactor(int value)
    {
        if (value < 2)
            throw new ArgumentOutOfRangeException(nameof(value), "Divergence factor must be at least 2.");

        _divergenceFactor = value;
        return this;
    }

    public AdaptiveQueryReoptimizationOptionsBuilder WithMinimumObservedRows(int value)
    {
        if (value < 1)
            throw new ArgumentOutOfRangeException(nameof(value), "Minimum observed rows must be greater than 0.");

        _minimumObservedRows = value;
        return this;
    }

    public AdaptiveQueryReoptimizationOptionsBuilder WithMaxBufferedRows(int value)
    {
        if (value < 1)
            throw new ArgumentOutOfRangeException(nameof(value), "Max buffered rows must be greater than 0.");

        _maxBufferedRows = value;
        return this;
    }

    public AdaptiveQueryReoptimizationOptionsBuilder WithMaxReoptimizationsPerQuery(int value)
    {
        if (value < 0)
            throw new ArgumentOutOfRangeException(nameof(value), "Max reoptimizations per query cannot be negative.");

        _maxReoptimizationsPerQuery = value;
        return this;
    }

    internal AdaptiveQueryReoptimizationOptions Build(bool enabled)
        => new()
        {
            Enabled = enabled,
            DivergenceFactor = _divergenceFactor,
            MinimumObservedRows = _minimumObservedRows,
            MaxBufferedRows = _maxBufferedRows,
            MaxReoptimizationsPerQuery = _maxReoptimizationsPerQuery,
        };
}
