using Microsoft.EntityFrameworkCore.Design;

namespace EfCoreProviderSample;

public sealed class BloggingContextFactory : IDesignTimeDbContextFactory<BloggingContext>
{
    public BloggingContext CreateDbContext(string[] args)
        => new(SamplePaths.GetDatabasePath(args, Directory.GetCurrentDirectory()));
}
