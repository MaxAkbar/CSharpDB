using StorageStudyExamples.Core;

namespace StorageStudyExamples.StorageInternals;

/// <summary>
/// Adapter that wraps the storage extensibility examples as <see cref="IExample"/> instances.
/// These are demo-only examples that demonstrate storage engine configuration
/// (custom page caches, checkpoint policies, interceptors, etc.).
/// They do not support interactive CRUD operations.
/// </summary>
public sealed class StorageInternalsExample : IExample
{
    private readonly string _name;
    private readonly string _commandName;
    private readonly string _description;
    private readonly Func<Task> _runAsync;

    private StorageInternalsExample(string name, string commandName, string description, Func<Task> runAsync)
    {
        _name = name;
        _commandName = commandName;
        _description = description;
        _runAsync = runAsync;
    }

    public string Name => _name;
    public string CommandName => _commandName;
    public string Description => _description;

    public Task InitializeAsync(string workingDirectory)
    {
        // Storage internals examples manage their own files.
        // Set the working directory so any .cdb files are created there.
        Directory.CreateDirectory(workingDirectory);
        Environment.CurrentDirectory = workingDirectory;
        return Task.CompletedTask;
    }

    public Task RunDemoAsync(TextWriter output)
    {
        // These examples write directly to Console (legacy behavior).
        // Since they're demo-only, this is acceptable.
        return _runAsync();
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    /// <summary>
    /// Create all 9 storage internals examples as <see cref="IExample"/> instances.
    /// </summary>
    public static IReadOnlyList<IExample> CreateAll() =>
    [
        new StorageInternalsExample(
            "Default Config",
            "default-config",
            "Open the database with default storage settings.",
            ConfigurationExamples.DefaultConfigurationAsync),

        new StorageInternalsExample(
            "Production Config",
            "production-config",
            "Open the database with bounded cache, caching indexes, and custom checksums.",
            ConfigurationExamples.ProductionConfigurationAsync),

        new StorageInternalsExample(
            "Debug Config",
            "debug-config",
            "Run a verbose example with interceptor logging.",
            ConfigurationExamples.DebugConfigurationAsync),

        new StorageInternalsExample(
            "Batch Import",
            "batch-import",
            "Run the heavier bulk-import example with auto-checkpoint disabled.",
            ConfigurationExamples.BatchImportConfigurationAsync),

        new StorageInternalsExample(
            "Metrics Cache",
            "metrics-cache",
            "Run a cache-instrumented workload and print cache stats.",
            ConfigurationExamples.MetricsCacheConfigurationAsync),

        new StorageInternalsExample(
            "Multiple Interceptors",
            "multiple-interceptors",
            "Run an example with more than one page interceptor.",
            ConfigurationExamples.MultipleInterceptorsAsync),

        new StorageInternalsExample(
            "Crash Recovery Test",
            "crash-recovery-test",
            "Simulate a write failure and reopen the database.",
            TestingExamples.CrashRecoveryTestAsync),

        new StorageInternalsExample(
            "Checkpoint Policy Test",
            "checkpoint-policy-test",
            "Print deterministic output for the time-based checkpoint policy.",
            () => { TestingExamples.CheckpointPolicyUnitTest(); return Task.CompletedTask; }),

        new StorageInternalsExample(
            "WAL Size Policy Test",
            "wal-size-policy-test",
            "Print deterministic output for the WAL-size checkpoint policy.",
            () => { TestingExamples.WalSizePolicyTest(); return Task.CompletedTask; }),
    ];
}
