using CSharpDB.Client;
using CSharpDB.Engine;

namespace CSharpDB.Tests;

public sealed class ClientHybridDatabaseOptionsTests
{
    [Fact]
    public void HybridDatabaseOptions_RejectsGrpcTransport()
    {
        var ex = Assert.Throws<CSharpDbClientConfigurationException>(() => CSharpDbClient.Create(new CSharpDbClientOptions
        {
            Transport = CSharpDbTransport.Grpc,
            Endpoint = "http://localhost:5820",
            HybridDatabaseOptions = new HybridDatabaseOptions(),
        }));

        Assert.Contains("does not support HybridDatabaseOptions", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task HybridDatabaseOptions_PersistCommittedStateToBackingFile()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempDbPath();
        string crashImagePath = NewTempDbPath();

        try
        {
            await using var client = Assert.IsType<CSharpDbClient>(CSharpDbClient.Create(new CSharpDbClientOptions
            {
                DataSource = dbPath,
                HybridDatabaseOptions = new HybridDatabaseOptions(),
            }));

            Assert.Null((await client.ExecuteSqlAsync("CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER);", ct)).Error);
            Assert.Null((await client.ExecuteSqlAsync("INSERT INTO bench VALUES (1, 10);", ct)).Error);

            CopyDatabaseFiles(dbPath, crashImagePath);

            await using var reopened = await Database.OpenAsync(crashImagePath, ct);
            await using var result = await reopened.ExecuteAsync("SELECT value FROM bench WHERE id = 1;", ct);
            var rows = await result.ToListAsync(ct);

            var row = Assert.Single(rows);
            Assert.Equal(10L, row[0].AsInteger);
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
            DeleteIfExists(crashImagePath);
            DeleteIfExists(crashImagePath + ".wal");
        }
    }

    [Fact]
    public async Task HybridDatabaseOptions_AreUsedAfterReleaseCachedDatabaseAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempDbPath();

        try
        {
            await using var client = Assert.IsType<CSharpDbClient>(CSharpDbClient.Create(new CSharpDbClientOptions
            {
                DataSource = dbPath,
                HybridDatabaseOptions = new HybridDatabaseOptions
                {
                    PersistenceMode = HybridPersistenceMode.Snapshot,
                    PersistenceTriggers = HybridPersistenceTriggers.Dispose,
                },
            }));

            Assert.Null((await client.ExecuteSqlAsync("CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER);", ct)).Error);
            Assert.Null((await client.ExecuteSqlAsync("INSERT INTO bench VALUES (1, 10);", ct)).Error);

            await client.ReleaseCachedDatabaseAsync(ct);

            var query = await client.ExecuteSqlAsync("SELECT value FROM bench WHERE id = 1;", ct);
            Assert.Null(query.Error);
            Assert.NotNull(query.Rows);
            Assert.Equal(10L, query.Rows![0][0]);

            await using var reopened = await Database.OpenAsync(dbPath, ct);
            await using var result = await reopened.ExecuteAsync("SELECT value FROM bench WHERE id = 1;", ct);
            var rows = await result.ToListAsync(ct);

            var row = Assert.Single(rows);
            Assert.Equal(10L, row[0].AsInteger);
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    private static string NewTempDbPath()
        => Path.Combine(Path.GetTempPath(), $"csharpdb_client_hybrid_{Guid.NewGuid():N}.db");

    private static void CopyDatabaseFiles(string sourcePath, string destinationPath)
    {
        File.WriteAllBytes(destinationPath, File.ReadAllBytes(sourcePath));

        string sourceWalPath = sourcePath + ".wal";
        if (File.Exists(sourceWalPath))
            File.WriteAllBytes(destinationPath + ".wal", File.ReadAllBytes(sourceWalPath));
    }

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }
}
