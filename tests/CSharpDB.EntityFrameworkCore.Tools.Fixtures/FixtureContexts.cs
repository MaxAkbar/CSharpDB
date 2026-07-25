using CSharpDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace CSharpDB.EntityFrameworkCore.Tools.Fixtures;

public sealed class FixtureContext(
    DbContextOptions<FixtureContext> options)
    : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<FixtureWidget>(entity =>
        {
            entity.ToTable("widgets");
            entity.HasKey(widget => widget.Id);
            entity.Property(widget => widget.Name)
                .HasColumnType("TEXT")
                .IsRequired();
            entity.Property(widget => widget.Tag)
                .HasColumnType("TEXT");
            entity.HasIndex(widget => widget.Name)
                .HasDatabaseName("ix_widgets_name");
        });
    }
}

public sealed class FixtureContextFactory
    : IDesignTimeDbContextFactory<FixtureContext>
{
    public FixtureContext CreateDbContext(string[] args)
    {
        Console.Out.WriteLine("TOP-SECRET-EF-FIXTURE");
        Console.Error.WriteLine("\u001b[31mTOP-SECRET-EF-FIXTURE\u001b[0m");

        var options = new DbContextOptionsBuilder<FixtureContext>()
            .UseCSharpDb("Data Source=:memory:")
            .Options;
        return new FixtureContext(options);
    }
}

public sealed class UnsupportedFixtureContext(
    DbContextOptions<UnsupportedFixtureContext> options)
    : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<UnsupportedFixtureRow>()
            .HasKey(row => row.Id);
    }
}

public sealed class UnsupportedFixtureContextFactory
    : IDesignTimeDbContextFactory<UnsupportedFixtureContext>
{
    public UnsupportedFixtureContext CreateDbContext(string[] args)
    {
        var options =
            new DbContextOptionsBuilder<UnsupportedFixtureContext>()
                .UseCSharpDb("Data Source=:memory:")
                .Options;
        return new UnsupportedFixtureContext(options);
    }
}

public sealed class RawSqlFixtureContext(
    DbContextOptions<RawSqlFixtureContext> options)
    : DbContext(options);

public sealed class RawSqlFixtureContextFactory
    : IDesignTimeDbContextFactory<RawSqlFixtureContext>
{
    public RawSqlFixtureContext CreateDbContext(string[] args)
    {
        var options =
            new DbContextOptionsBuilder<RawSqlFixtureContext>()
                .UseCSharpDb("Data Source=:memory:")
                .Options;
        return new RawSqlFixtureContext(options);
    }
}

public sealed class TargetModelFixtureContext(
    DbContextOptions<TargetModelFixtureContext> options)
    : DbContext(options)
{
    protected override void OnModelCreating(
        ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<TargetModelFixtureRow>(
            entity =>
            {
                entity.ToTable("target_model_rows");
                entity.HasKey(row => row.Id);
                entity.Property(row => row.Id)
                    .HasColumnType("INTEGER");
            });
    }
}

public sealed class TargetModelFixtureContextFactory
    : IDesignTimeDbContextFactory<TargetModelFixtureContext>
{
    public TargetModelFixtureContext CreateDbContext(
        string[] args)
    {
        var options =
            new DbContextOptionsBuilder<
                TargetModelFixtureContext>()
                .UseCSharpDb("Data Source=:memory:")
                .Options;
        return new TargetModelFixtureContext(options);
    }
}

public sealed class FixtureWidget
{
    public long Id { get; set; }

    public required string Name { get; set; }

    public string? Tag { get; set; }
}

public sealed class UnsupportedFixtureRow
{
    public long Id { get; set; }
}

public sealed class TargetModelFixtureRow
{
    public long Id { get; set; }
}
