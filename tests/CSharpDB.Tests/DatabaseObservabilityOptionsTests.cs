using CSharpDB.Engine;
using CSharpDB.Observability;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Tests;

public sealed class DatabaseObservabilityOptionsTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task DefaultOptions_KeepObservabilityDisabledWithoutRuntimeConfiguration()
    {
        var options = new DatabaseOptions();

        Assert.Null(options.ObservabilityOptions);

        await using Database database = await Database.OpenInMemoryAsync(options, Ct);

        Assert.False(database.IsObservabilityEnabled);
        Assert.Null(database.ObservabilityDatabaseAlias);
        Assert.Null(database.ObservabilitySlowQueryThreshold);

        await using var result = await database.ExecuteAsync("SELECT 1", Ct);
        Assert.Single(await result.ToListAsync(Ct));
    }

    [Fact]
    public async Task EnabledInvalidOptions_FailBeforeStorageIsOpened()
    {
        var storageFactory = new CountingStorageEngineFactory();
        var options = new DatabaseOptions
        {
            ObservabilityOptions = new CSharpDbObservabilityOptions
            {
                Enabled = true,
                DatabaseAlias = @"C:\private\database.db",
            },
            StorageEngineFactory = storageFactory,
        };
        string databasePath = Path.Combine(Path.GetTempPath(), $"csharpdb_observability_{Guid.NewGuid():N}.db");

        await Assert.ThrowsAsync<CSharpDbObservabilityOptionsValidationException>(
            () => Database.OpenAsync(databasePath, options, Ct).AsTask());

        Assert.Equal(0, storageFactory.OpenCount);
    }

    [Fact]
    public async Task EnabledOptions_AreDeepSnapshottedBeforeDatabaseOpen()
    {
        var configured = new CSharpDbObservabilityOptions
        {
            Enabled = true,
            DatabaseAlias = "primary",
            Logging = new CSharpDbLoggingOptions
            {
                SlowQueryThreshold = TimeSpan.FromSeconds(2),
            },
        };
        var options = new DatabaseOptions { ObservabilityOptions = configured };

        await using Database database = await Database.OpenInMemoryAsync(options, Ct);

        configured.DatabaseAlias = "changed";
        configured.Logging.SlowQueryThreshold = TimeSpan.FromSeconds(9);

        Assert.True(database.IsObservabilityEnabled);
        Assert.Equal("primary", database.ObservabilityDatabaseAlias);
        Assert.Equal(TimeSpan.FromSeconds(2), database.ObservabilitySlowQueryThreshold);
    }

    [Fact]
    public void DatabaseOptionsCopies_PreserveObservabilityConfiguration()
    {
        var configured = new CSharpDbObservabilityOptions
        {
            Enabled = true,
            DatabaseAlias = "primary",
        };
        var source = new DatabaseOptions { ObservabilityOptions = configured };

        DatabaseOptions[] copies =
        [
            source.ConfigureStorageEngine(_ => { }),
            source.ConfigureFunctions(_ => { }),
            source.EnableAdaptiveQueryReoptimization(),
            RetainedDatabaseSnapshot.CreateBoundedDatabaseOptions(
                source,
                new RetainedDatabaseSnapshotOptions()),
        ];

        Assert.All(copies, copy => Assert.Same(configured, copy.ObservabilityOptions));
    }

    private sealed class CountingStorageEngineFactory : IStorageEngineFactory
    {
        public int OpenCount { get; private set; }

        public ValueTask<StorageEngineContext> OpenAsync(
            string filePath,
            StorageEngineOptions options,
            CancellationToken ct = default)
        {
            OpenCount++;
            throw new InvalidOperationException("Storage should not be opened for invalid observability options.");
        }
    }
}
