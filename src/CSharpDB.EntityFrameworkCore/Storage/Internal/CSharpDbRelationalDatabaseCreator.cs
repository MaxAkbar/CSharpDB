using System.Data;
using CSharpDB.Data;
using CSharpDB.EntityFrameworkCore.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore.Storage;

namespace CSharpDB.EntityFrameworkCore.Storage.Internal;

public sealed class CSharpDbRelationalDatabaseCreator : RelationalDatabaseCreator
{
    public CSharpDbRelationalDatabaseCreator(RelationalDatabaseCreatorDependencies dependencies)
        : base(dependencies)
    {
    }

    public override bool Exists()
    {
        if (TryGetPrivateMemoryConnection(out _))
            return true;

        string? filePath = TryGetFilePath();
        return filePath is not null && File.Exists(filePath);
    }

    public override Task<bool> ExistsAsync(CancellationToken cancellationToken = default)
        => Task.FromResult(Exists());

    public override void Create()
        => CreateAsync().GetAwaiter().GetResult();

    public override async Task CreateAsync(CancellationToken cancellationToken = default)
    {
        string? filePath = TryGetFilePath();
        if (filePath is not null)
        {
            string? directory = Path.GetDirectoryName(filePath);
            if (!string.IsNullOrWhiteSpace(directory))
                Directory.CreateDirectory(directory);
        }

        await using var connection = CreateStandaloneConnection();
        await connection.OpenAsync(cancellationToken);
    }

    public override void Delete()
        => DeleteAsync().GetAwaiter().GetResult();

    public override Task DeleteAsync(CancellationToken cancellationToken = default)
    {
        string? filePath = TryGetFilePath();
        if (filePath is null)
            return Task.CompletedTask;

        if (File.Exists(filePath))
            File.Delete(filePath);

        return Task.CompletedTask;
    }

    public override bool HasTables()
        => HasTablesAsync().GetAwaiter().GetResult();

    public override async Task<bool> HasTablesAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = CreateStandaloneConnection();
        await connection.OpenAsync(cancellationToken);
        return connection.GetTableNames().Count > 0;
    }

    private bool TryGetPrivateMemoryConnection(out CSharpDbConnection connection)
    {
        connection = CreateStandaloneConnection();
        var builder = new CSharpDbConnectionStringBuilder(connection.ConnectionString);
        return CSharpDbProviderValidation.IsPrivateMemory(builder.DataSource);
    }

    private string? TryGetFilePath()
    {
        var connection = CreateStandaloneConnection();
        var builder = new CSharpDbConnectionStringBuilder(connection.ConnectionString);
        if (string.IsNullOrWhiteSpace(builder.DataSource) || CSharpDbProviderValidation.IsPrivateMemory(builder.DataSource))
            return null;

        return CSharpDbProviderValidation.IsNamedSharedMemory(builder.DataSource)
            ? null
            : Path.GetFullPath(builder.DataSource);
    }

    private CSharpDbConnection CreateStandaloneConnection()
    {
        if (Dependencies.Connection.DbConnection is CSharpDbConnection existing)
            return new CSharpDbConnection(existing.ConnectionString);

        return new CSharpDbConnection(Dependencies.Connection.ConnectionString ?? string.Empty);
    }
}
