using CSharpDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace CSharpDB.EntityFrameworkCore.Tools.Fixtures;

public static class Program
{
    public static void Main(string[] args)
    {
    }

    public static IHostBuilder CreateHostBuilder(string[] args) =>
        Host.CreateDefaultBuilder(args)
            .ConfigureServices(services =>
            {
                services.AddDbContext<HostedFixtureContext>(
                    options => options.UseCSharpDb(
                        "Data Source=:memory:"));
            });
}

public sealed class HostedFixtureContext(
    DbContextOptions<HostedFixtureContext> options)
    : DbContext(options)
{
    protected override void OnModelCreating(
        ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<HostedFixtureRow>()
            .HasKey(row => row.Id);
    }
}

public sealed class HostedFixtureRow
{
    public long Id { get; set; }
}
