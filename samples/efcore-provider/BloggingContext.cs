using CSharpDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

namespace EfCoreProviderSample;

public sealed class BloggingContext(string databasePath) : DbContext
{
    private readonly string _databasePath = Path.GetFullPath(databasePath);

    public DbSet<Blog> Blogs => Set<Blog>();

    public DbSet<Post> Posts => Set<Post>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseCSharpDb($"Data Source={_databasePath}");

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Blog>(blog =>
        {
            blog.Property(item => item.Name).IsRequired();
            blog.HasMany(item => item.Posts)
                .WithOne(item => item.Blog)
                .HasForeignKey(item => item.BlogId)
                .OnDelete(DeleteBehavior.Cascade);
        });

        modelBuilder.Entity<Post>(post =>
        {
            post.Property(item => item.Title).IsRequired();
        });
    }
}

public sealed class Blog
{
    public int Id { get; set; }

    public string Name { get; set; } = string.Empty;

    public List<Post> Posts { get; set; } = [];
}

public sealed class Post
{
    public int Id { get; set; }

    public int BlogId { get; set; }

    public string Title { get; set; } = string.Empty;

    public Blog Blog { get; set; } = null!;
}
