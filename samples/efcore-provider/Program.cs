using EfCoreProviderSample;
using Microsoft.EntityFrameworkCore;

string databasePath = SamplePaths.GetDatabasePath(args);
string? databaseDirectory = Path.GetDirectoryName(databasePath);
if (!string.IsNullOrWhiteSpace(databaseDirectory))
    Directory.CreateDirectory(databaseDirectory);

await using var db = new BloggingContext(databasePath);
await db.Database.EnsureCreatedAsync();

if (!await db.Blogs.AnyAsync())
{
    db.Blogs.AddRange(
        new Blog
        {
            Name = "Engineering",
            Posts =
            [
                new Post { Title = "Release checklist" },
                new Post { Title = "Migration notes" },
            ],
        },
        new Blog
        {
            Name = "Operations",
            Posts =
            [
                new Post { Title = "Backup runbook" },
            ],
        });

    await db.SaveChangesAsync();
}

List<Blog> blogs = await db.Blogs
    .OrderBy(blog => blog.Name)
    .Include(blog => blog.Posts)
    .ToListAsync();

Console.WriteLine($"Database: {Path.GetFullPath(databasePath)}");
Console.WriteLine($"Blogs: {blogs.Count}");
Console.WriteLine($"Posts: {await db.Posts.CountAsync()}");

foreach (Blog blog in blogs)
    Console.WriteLine($"{blog.Name}|{blog.Posts.Count}");
