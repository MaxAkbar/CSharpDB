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
            MonthlyBudget = 1250.50m,
            Posts =
            [
                new Post { Title = "Release checklist" },
                new Post { Title = "Migration notes" },
            ],
        },
        new Blog
        {
            Name = "Operations",
            MonthlyBudget = 800.00m,
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
var joinedPosts = await db.Blogs
    .Join(
        db.Posts,
        blog => blog.Id,
        post => post.BlogId,
        (blog, post) => new
        {
            BlogName = blog.Name,
            post.Title,
        })
    .OrderBy(item => item.BlogName)
    .ThenBy(item => item.Title)
    .ToListAsync();

Blog engineering = blogs.Single(blog => blog.Name == "Engineering");
byte[] rowVersionBeforeRawSql = engineering.RowVersion.ToArray();
await db.Database.ExecuteSqlRawAsync(
    "UPDATE \"Blogs\" SET \"Name\" = \"Name\" WHERE \"Id\" = {0}",
    engineering.Id);
byte[] rowVersionAfterRawSql = await db.Blogs
    .AsNoTracking()
    .Where(blog => blog.Id == engineering.Id)
    .Select(blog => blog.RowVersion)
    .SingleAsync();

Console.WriteLine($"Database: {Path.GetFullPath(databasePath)}");
Console.WriteLine($"Blogs: {blogs.Count}");
Console.WriteLine($"Posts: {await db.Posts.CountAsync()}");
Console.WriteLine($"JoinedPosts: {joinedPosts.Count}");
Console.WriteLine($"RowVersionBytes: {rowVersionAfterRawSql.Length}");
Console.WriteLine(
    $"RowVersionAdvancedAfterRawSql: {!rowVersionBeforeRawSql.SequenceEqual(rowVersionAfterRawSql)}");

foreach (Blog blog in blogs)
    Console.WriteLine($"{blog.Name}|{blog.Posts.Count}");
