using System.Data.Common;
using CSharpDB.EntityFrameworkCore.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace CSharpDB.EntityFrameworkCore;

public static class CSharpDbDbContextOptionsBuilderExtensions
{
    public static DbContextOptionsBuilder UseCSharpDb(
        this DbContextOptionsBuilder optionsBuilder,
        string connectionString,
        Action<CSharpDbDbContextOptionsBuilder>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        var extension = GetOrCreateExtension(optionsBuilder).WithConnectionString(connectionString);
        ConfigureProvider(optionsBuilder, extension, configure);
        return optionsBuilder;
    }

    public static DbContextOptionsBuilder UseCSharpDb(
        this DbContextOptionsBuilder optionsBuilder,
        DbConnection connection,
        Action<CSharpDbDbContextOptionsBuilder>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);
        ArgumentNullException.ThrowIfNull(connection);

        var extension = GetOrCreateExtension(optionsBuilder).WithConnection(connection);
        ConfigureProvider(optionsBuilder, extension, configure);
        return optionsBuilder;
    }

    public static DbContextOptionsBuilder<TContext> UseCSharpDb<TContext>(
        this DbContextOptionsBuilder<TContext> optionsBuilder,
        string connectionString,
        Action<CSharpDbDbContextOptionsBuilder>? configure = null)
        where TContext : DbContext
        => (DbContextOptionsBuilder<TContext>)UseCSharpDb((DbContextOptionsBuilder)optionsBuilder, connectionString, configure);

    public static DbContextOptionsBuilder<TContext> UseCSharpDb<TContext>(
        this DbContextOptionsBuilder<TContext> optionsBuilder,
        DbConnection connection,
        Action<CSharpDbDbContextOptionsBuilder>? configure = null)
        where TContext : DbContext
        => (DbContextOptionsBuilder<TContext>)UseCSharpDb((DbContextOptionsBuilder)optionsBuilder, connection, configure);

    private static CSharpDbOptionsExtension GetOrCreateExtension(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.Options.FindExtension<CSharpDbOptionsExtension>()
            ?? new CSharpDbOptionsExtension();

    private static void ConfigureProvider(
        DbContextOptionsBuilder optionsBuilder,
        CSharpDbOptionsExtension extension,
        Action<CSharpDbDbContextOptionsBuilder>? configure)
    {
        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);
        configure?.Invoke(new CSharpDbDbContextOptionsBuilder(optionsBuilder));
    }
}
