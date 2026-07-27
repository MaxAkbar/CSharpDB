using CSharpDB.Engine;

namespace CSharpDB.Tests;

public sealed class DatabaseCreateNewTests : IDisposable
{
    private readonly List<string> _paths = [];
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task CreateNewAsync_ExistingFile_RefusesWithoutMutatingDatabaseOrWal()
    {
        string filePath = NewTempDbPath();
        byte[] databaseBytes = [0x43, 0x53, 0x44, 0x42, 0x01, 0x02, 0x03];
        byte[] walBytes = [0x57, 0x41, 0x4c, 0x09, 0x08, 0x07];
        await File.WriteAllBytesAsync(filePath, databaseBytes, Ct);
        await File.WriteAllBytesAsync(filePath + ".wal", walBytes, Ct);

        await Assert.ThrowsAsync<IOException>(
            () => Database.CreateNewAsync(filePath, Ct).AsTask());

        Assert.Equal(databaseBytes, await File.ReadAllBytesAsync(filePath, Ct));
        Assert.Equal(walBytes, await File.ReadAllBytesAsync(filePath + ".wal", Ct));
    }

    [Fact]
    public async Task CreateNewAsync_NewFile_CreatesDatabaseThatCanBeReopened()
    {
        string filePath = NewTempDbPath();

        await using (var database = await Database.CreateNewAsync(filePath, Ct))
        {
            await database.ExecuteAsync(
                "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
                Ct);
            await database.ExecuteAsync("INSERT INTO items VALUES (1, 'created-new')", Ct);
        }

        await using var reopened = await Database.OpenAsync(filePath, Ct);
        await using var result = await reopened.ExecuteAsync(
            "SELECT name FROM items WHERE id = 1",
            Ct);
        var rows = await result.ToListAsync(Ct);

        Assert.Equal("created-new", Assert.Single(rows)[0].AsText);
    }

    public void Dispose()
    {
        foreach (string path in _paths)
        {
            DeleteIfExists(path);
            DeleteIfExists(path + ".wal");
        }
    }

    private string NewTempDbPath()
    {
        string path = Path.Combine(Path.GetTempPath(), $"csharpdb_create_new_{Guid.NewGuid():N}.db");
        _paths.Add(path);
        return path;
    }

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }
}
