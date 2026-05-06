
using CSharpDB.Primitives;

namespace CSharpDB.Engine;

/// <summary>
/// Composition options for Database.OpenAsync.
/// </summary>
public sealed class DatabaseOptions
{
    /// <summary>
    /// Storage-engine component options.
    /// </summary>
    public StorageEngineOptions StorageEngineOptions { get; init; } = new();

    /// <summary>
    /// Controls how shared auto-commit INSERT statements execute. Defaults to the legacy serialized path.
    /// </summary>
    public ImplicitInsertExecutionMode ImplicitInsertExecutionMode { get; init; } = ImplicitInsertExecutionMode.Serialized;

    /// <summary>
    /// Opt-in adaptive join re-optimization for SELECT queries. Disabled by default.
    /// </summary>
    public AdaptiveQueryReoptimizationOptions AdaptiveQueryReoptimization { get; init; } = new();

    /// <summary>
    /// Trusted in-process scalar functions available to SQL and embedded expression surfaces.
    /// </summary>
    public DbFunctionRegistry Functions { get; init; } = DbFunctionRegistry.Empty;

    /// <summary>
    /// Factory used to compose storage engine components.
    /// </summary>
    public IStorageEngineFactory StorageEngineFactory { get; init; } = new DefaultStorageEngineFactory();
}
