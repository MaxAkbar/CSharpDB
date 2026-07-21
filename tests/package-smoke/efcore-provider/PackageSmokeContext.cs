using CSharpDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace CSharpDB.EntityFrameworkCore.PackageSmoke;

public sealed class PackageSmokeContext : DbContext
{
    private readonly string? _databasePath;

    public PackageSmokeContext()
    {
    }

    public PackageSmokeContext(string databasePath)
    {
        _databasePath = Path.GetFullPath(databasePath);
    }

    public DbSet<PackageSmokeItem> Items => Set<PackageSmokeItem>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        if (optionsBuilder.IsConfigured)
            return;

        string databasePath = _databasePath
            ?? Environment.GetEnvironmentVariable("CSHARPDB_EF_PACKAGE_SMOKE_DATABASE")
            ?? throw new InvalidOperationException(
                "Set CSHARPDB_EF_PACKAGE_SMOKE_DATABASE for design-time commands.");

        optionsBuilder.UseCSharpDb($"Data Source={Path.GetFullPath(databasePath)}");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<PackageSmokeItem>(item =>
        {
            item.Property(value => value.Name)
                .HasMaxLength(200)
                .IsRequired();
        });
    }
}

public sealed class PackageSmokeContextFactory
    : IDesignTimeDbContextFactory<PackageSmokeContext>
{
    public PackageSmokeContext CreateDbContext(string[] args)
    {
        string databasePath =
            Environment.GetEnvironmentVariable("CSHARPDB_EF_PACKAGE_SMOKE_DATABASE")
            ?? throw new InvalidOperationException(
                "Set CSHARPDB_EF_PACKAGE_SMOKE_DATABASE before running dotnet ef.");

        return new PackageSmokeContext(databasePath);
    }
}

public sealed class PackageSmokeItem
{
    public int Id { get; set; }

    public string Name { get; set; } = string.Empty;
}
