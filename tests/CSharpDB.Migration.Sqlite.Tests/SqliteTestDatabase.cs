using System.Security.Cryptography;
using Microsoft.Data.Sqlite;

namespace CSharpDB.Migration.Sqlite.Tests;

internal sealed class SqliteTestDirectory : IDisposable
{
    internal SqliteTestDirectory()
    {
        Root = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb-migration-sqlite-tests-{Guid.NewGuid():N}");
        Directory.CreateDirectory(Root);
    }

    internal string Root { get; }

    internal string PathFor(string fileName) => Path.Combine(Root, fileName);

    public void Dispose()
    {
        if (!Directory.Exists(Root))
            return;

        SqliteConnection.ClearAllPools();
        Directory.Delete(Root, recursive: true);
    }
}

internal static class SqliteTestDatabase
{
    internal static async ValueTask CreateAsync(
        string path,
        string sql,
        CancellationToken cancellationToken)
    {
        var connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = path,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Cache = SqliteCacheMode.Private,
            Pooling = false,
        }.ToString();

        await using var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText =
            """
            PRAGMA journal_mode=DELETE;
            PRAGMA foreign_keys=ON;
            """ +
            Environment.NewLine +
            sql;
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    internal static async ValueTask<byte[]> ReadBytesAsync(
        string path,
        CancellationToken cancellationToken) =>
        await File.ReadAllBytesAsync(path, cancellationToken);

    internal static string Digest(byte[] bytes) =>
        "sha256:" + Convert.ToHexStringLower(SHA256.HashData(bytes));

    internal static IReadOnlyDictionary<string, byte[]> CaptureSidecars(string databasePath)
    {
        string[] suffixes = ["-journal", "-wal", "-shm"];
        return suffixes
            .Select(suffix => databasePath + suffix)
            .Where(File.Exists)
            .ToDictionary(
                static path => path,
                static path => File.ReadAllBytes(path),
                StringComparer.Ordinal);
    }

    internal static async ValueTask DisposeIfSupportedAsync(object? value)
    {
        switch (value)
        {
            case IAsyncDisposable asyncDisposable:
                await asyncDisposable.DisposeAsync();
                break;
            case IDisposable disposable:
                disposable.Dispose();
                break;
        }
    }
}
