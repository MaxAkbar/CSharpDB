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
        },
        new Blog
        {
            Name = "Research",
            MonthlyBudget = 500.00m,
        });

    await db.SaveChangesAsync();
}

List<Blog> blogs = await db.Blogs
    .OrderBy(blog => blog.Name)
    .Include(blog => blog.Posts)
    .ToListAsync();
var stringPredicateMatches = await db.Blogs
    .Where(blog =>
        blog.Name.Contains("search")
        || blog.Name.StartsWith("Eng", StringComparison.Ordinal)
        || blog.Name.EndsWith("tions", StringComparison.Ordinal))
    .OrderBy(blog => blog.Name)
    .Select(blog => blog.Name)
    .ToListAsync();
string likePattern = "%AT_ONS";
var likePredicateMatches = await db.Blogs
    .Where(blog =>
        EF.Functions.Like(
            blog.Name,
            likePattern))
    .Select(blog => blog.Name)
    .ToListAsync();
var terminalExceptBlogIds = await db.Blogs
    .Select(blog => blog.Id)
    .Except(
        db.Posts.Select(post =>
            post.BlogId))
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
var leftJoinedRows = await db.Blogs
    .LeftJoin(
        db.Posts,
        blog => blog.Id,
        post => post.BlogId,
        (blog, post) => new
        {
            BlogName = blog.Name,
            PostId = (int?)post!.Id,
            PostTitle = post!.Title,
        })
    .OrderBy(item => item.BlogName)
    .ThenBy(item => item.PostTitle)
    .ToListAsync();
int blogsWithoutPosts = leftJoinedRows
    .Count(item => item.PostId is null);

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
Console.WriteLine($"LeftJoinedRows: {leftJoinedRows.Count}");
Console.WriteLine($"BlogsWithoutPosts: {blogsWithoutPosts}");
Console.WriteLine($"StringPredicateMatches: {stringPredicateMatches.Count}");
Console.WriteLine($"LikePredicateMatches: {likePredicateMatches.Count}");
Console.WriteLine(
    $"TerminalExceptBlogsWithoutPosts: {terminalExceptBlogIds.Count}");
Console.WriteLine($"RowVersionBytes: {rowVersionAfterRawSql.Length}");
Console.WriteLine(
    $"RowVersionAdvancedAfterRawSql: {!rowVersionBeforeRawSql.SequenceEqual(rowVersionAfterRawSql)}");

foreach (Blog blog in blogs)
    Console.WriteLine($"{blog.Name}|{blog.Posts.Count}");
