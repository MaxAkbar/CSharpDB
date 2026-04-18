using System.Data.Common;
using CSharpDB.Data;
using Microsoft.EntityFrameworkCore;

namespace CSharpDB.EntityFrameworkCore.Tests;

[Collection("ConnectionPoolState")]
public sealed class CSharpDbRuntimeTests : IAsyncLifetime
{
    private readonly string _workspace = Path.Combine(Path.GetTempPath(), $"csharpdb_efcore_runtime_{Guid.NewGuid():N}");

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        Directory.CreateDirectory(_workspace);
        await CSharpDbConnection.ClearAllPoolsAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await CSharpDbConnection.ClearAllPoolsAsync();
        DeleteDirectoryIfExists(_workspace);
    }

    [Fact]
    public async Task EnsureCreated_FileBackedCrudAndTypeRoundTrip_Succeeds()
    {
        string dbPath = GetDbPath("runtime");

        await using var db = new ProviderRuntimeContext($"Data Source={dbPath}");
        await db.Database.EnsureCreatedAsync(Ct);

        var person = new PersonRecord
        {
            Name = "Ada",
            Active = true,
            Score = 12.5,
            Visits = 9,
            Status = PersonStatus.Active,
            GuidValue = Guid.Parse("11111111-1111-1111-1111-111111111111"),
            CreatedAt = new DateTime(2026, 4, 16, 12, 34, 56, DateTimeKind.Utc),
            ObservedAt = new DateTimeOffset(2026, 4, 16, 5, 34, 56, TimeSpan.FromHours(-7)),
            Birthday = new DateOnly(2000, 1, 2),
            Alarm = new TimeOnly(7, 8, 9),
            Payload = [0x01, 0x02, 0x03, 0xFE],
        };

        db.People.Add(person);
        db.Widgets.Add(new ManualWidget
        {
            Id = 42,
            Name = "manually-assigned",
        });

        await db.SaveChangesAsync(Ct);

        Assert.True(person.Id > 0);

        PersonRecord loaded = await db.People.AsNoTracking().SingleAsync(p => p.Id == person.Id, Ct);
        ManualWidget widget = await db.Widgets.AsNoTracking().SingleAsync(w => w.Id == 42, Ct);

        Assert.Equal("Ada", loaded.Name);
        Assert.True(loaded.Active);
        Assert.Equal(12.5, loaded.Score);
        Assert.Equal(9L, loaded.Visits);
        Assert.Equal(PersonStatus.Active, loaded.Status);
        Assert.Equal(person.GuidValue, loaded.GuidValue);
        Assert.Equal(person.CreatedAt, loaded.CreatedAt);
        Assert.Equal(person.ObservedAt, loaded.ObservedAt);
        Assert.Equal(person.Birthday, loaded.Birthday);
        Assert.Equal(person.Alarm, loaded.Alarm);
        Assert.Equal(person.Payload, loaded.Payload);
        Assert.Equal("manually-assigned", widget.Name);

        person.Name = "Grace";
        await db.SaveChangesAsync(Ct);

        Assert.Equal(1, await db.People.CountAsync(p => p.Name == "Grace", Ct));

        db.Remove(person);
        await db.SaveChangesAsync(Ct);

        Assert.Equal(0, await db.People.CountAsync(Ct));
    }

    [Fact]
    public async Task Queries_IncludePaginationAndContainsOverConstantsAndParameters_Succeed()
    {
        string dbPath = GetDbPath("queries");

        await using var db = new ProviderRuntimeContext($"Data Source={dbPath}");
        await db.Database.EnsureCreatedAsync(Ct);

        db.Blogs.AddRange(
            new Blog
            {
                Name = "Alpha",
                Posts =
                [
                    new Post { Title = "Welcome" },
                    new Post { Title = "GettingStarted" },
                ],
            },
            new Blog
            {
                Name = "Beta",
                Posts =
                [
                    new Post { Title = "Roadmap" },
                ],
            },
            new Blog
            {
                Name = "Gamma",
            });

        await db.SaveChangesAsync(Ct);

        string alphaName = await db.Blogs
            .Where(blog => blog.Name == "Alpha")
            .Select(blog => blog.Name)
            .SingleAsync(Ct);

        List<Blog> blogsWithPosts = await db.Blogs
            .OrderBy(blog => blog.Name)
            .Include(blog => blog.Posts)
            .ToListAsync(Ct);

        List<string> pagedNames = await db.Blogs
            .OrderBy(blog => blog.Name)
            .Skip(1)
            .Take(1)
            .Select(blog => blog.Name)
            .ToListAsync(Ct);

        string[] allowedNames = ["Alpha", "Gamma"];

        List<string> parameterContains = await db.Blogs
            .Where(blog => allowedNames.Contains(blog.Name))
            .OrderBy(blog => blog.Name)
            .Select(blog => blog.Name)
            .ToListAsync(Ct);

        List<string> constantContains = await db.Blogs
            .Where(blog => new[] { "Alpha", "Gamma" }.Contains(blog.Name))
            .OrderBy(blog => blog.Name)
            .Select(blog => blog.Name)
            .ToListAsync(Ct);

        Blog alpha = Assert.Single(blogsWithPosts, blog => blog.Name == "Alpha");

        Assert.Equal("Alpha", alphaName);
        Assert.Equal(2, alpha.Posts.Count);
        Assert.Equal(["Beta"], pagedNames);
        Assert.Equal(["Alpha", "Gamma"], parameterContains);
        Assert.Equal(["Alpha", "Gamma"], constantContains);
        Assert.True(await db.Posts.AnyAsync(post => post.Title == "Welcome", Ct));
        Assert.Equal(3, await db.Posts.CountAsync(Ct));
    }

    [Fact]
    public async Task SaveChanges_WithConcurrencyToken_ThrowsWhenRowWasModified()
    {
        string dbPath = GetDbPath("concurrency");

        await using (var seed = new ProviderRuntimeContext($"Data Source={dbPath}"))
        {
            await seed.Database.EnsureCreatedAsync(Ct);
            seed.Tickets.Add(new Ticket
            {
                Name = "alpha",
                Version = 1,
            });

            await seed.SaveChangesAsync(Ct);
        }

        await using var first = new ProviderRuntimeContext($"Data Source={dbPath}");
        await using var second = new ProviderRuntimeContext($"Data Source={dbPath}");

        Ticket firstTicket = await first.Tickets.SingleAsync(Ct);
        Ticket secondTicket = await second.Tickets.SingleAsync(Ct);

        firstTicket.Name = "alpha-v2";
        firstTicket.Version = 2;
        await first.SaveChangesAsync(Ct);

        secondTicket.Name = "alpha-stale";
        secondTicket.Version = 2;

        var error = await Assert.ThrowsAsync<DbUpdateConcurrencyException>(() => second.SaveChangesAsync(Ct));
        Assert.Contains("expected to affect 1 row", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task UseCSharpDb_WithPrivateMemoryConnection_SupportsRuntimeOperations()
    {
        await using var connection = new CSharpDbConnection("Data Source=:memory:");
        await connection.OpenAsync(Ct);

        await using var db = new ProviderRuntimeContext(connection);
        await db.Database.EnsureCreatedAsync(Ct);

        db.People.Add(new PersonRecord
        {
            Name = "Transient",
            Active = true,
            Score = 1.25,
            Visits = 1,
            Status = PersonStatus.Active,
            GuidValue = Guid.NewGuid(),
            CreatedAt = new DateTime(2026, 4, 16, 0, 0, 0, DateTimeKind.Utc),
            ObservedAt = new DateTimeOffset(2026, 4, 16, 0, 0, 0, TimeSpan.Zero),
            Birthday = new DateOnly(2001, 1, 1),
            Alarm = new TimeOnly(8, 0, 0),
            Payload = [0xCA, 0xFE],
        });

        await db.SaveChangesAsync(Ct);

        Assert.Equal("Transient", await db.People.Select(person => person.Name).SingleAsync(Ct));
    }

    [Fact]
    public async Task ModelValidation_RejectsDecimalWithoutConverter()
    {
        string dbPath = GetDbPath("decimal");

        await using var db = new DecimalModelContext($"Data Source={dbPath}");
        var error = await Assert.ThrowsAsync<NotSupportedException>(() => db.Database.EnsureCreatedAsync(Ct));

        Assert.Contains("decimal without an explicit value converter", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ModelValidation_RejectsSchemas()
    {
        string dbPath = GetDbPath("schema");

        await using var db = new SchemaModelContext($"Data Source={dbPath}");
        var error = await Assert.ThrowsAsync<NotSupportedException>(() => db.Database.EnsureCreatedAsync(Ct));

        Assert.Contains("schemas are not supported", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ModelValidation_RejectsDefaultsAndCheckConstraints()
    {
        await using var defaultValueDb = new DefaultValueModelContext($"Data Source={GetDbPath("defaults")}");
        var defaultError = await Assert.ThrowsAsync<NotSupportedException>(() => defaultValueDb.Database.EnsureCreatedAsync(Ct));
        Assert.Contains("defaultvalue", defaultError.Message, StringComparison.OrdinalIgnoreCase);

        await using var checkConstraintDb = new CheckConstraintModelContext($"Data Source={GetDbPath("checks")}");
        var checkError = await Assert.ThrowsAsync<NotSupportedException>(() => checkConstraintDb.Database.EnsureCreatedAsync(Ct));
        Assert.Contains("check constraints", checkError.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("Data Source=provider.db;Pooling=true", "pooled connections")]
    [InlineData("Data Source=:memory:shared", "named shared-memory")]
    [InlineData("Data Source=provider.db;Transport=Http", "direct embedded transports")]
    [InlineData("Endpoint=http://localhost:5123;Transport=Http", "Endpoint connections")]
    public async Task ProviderValidation_RejectsUnsupportedConnectionConfigurations(string connectionString, string expectedMessage)
    {
        await using var db = new ProviderRuntimeContext(connectionString);

        var error = await Assert.ThrowsAsync<NotSupportedException>(() => db.Database.EnsureCreatedAsync(Ct));
        Assert.Contains(expectedMessage, error.Message, StringComparison.OrdinalIgnoreCase);
    }

    private string GetDbPath(string name)
        => Path.Combine(_workspace, $"{name}.db");

    private static void DeleteDirectoryIfExists(string path)
    {
        if (Directory.Exists(path))
            Directory.Delete(path, recursive: true);
    }

    private abstract class TestDbContext : DbContext
    {
        private readonly string? _connectionString;
        private readonly DbConnection? _connection;

        protected TestDbContext(string connectionString)
            => _connectionString = connectionString;

        protected TestDbContext(DbConnection connection)
            => _connection = connection;

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (_connection is not null)
                optionsBuilder.UseCSharpDb(_connection);
            else
                optionsBuilder.UseCSharpDb(_connectionString!);
        }
    }

    private sealed class ProviderRuntimeContext : TestDbContext
    {
        public ProviderRuntimeContext(string connectionString)
            : base(connectionString)
        {
        }

        public ProviderRuntimeContext(DbConnection connection)
            : base(connection)
        {
        }

        public DbSet<PersonRecord> People => Set<PersonRecord>();

        public DbSet<ManualWidget> Widgets => Set<ManualWidget>();

        public DbSet<Blog> Blogs => Set<Blog>();

        public DbSet<Post> Posts => Set<Post>();

        public DbSet<Ticket> Tickets => Set<Ticket>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<ManualWidget>()
                .Property(widget => widget.Id)
                .ValueGeneratedNever();

            modelBuilder.Entity<Blog>()
                .HasMany(blog => blog.Posts)
                .WithOne(post => post.Blog)
                .HasForeignKey(post => post.BlogId)
                .OnDelete(DeleteBehavior.Cascade);

            modelBuilder.Entity<Ticket>()
                .Property(ticket => ticket.Version)
                .IsConcurrencyToken();
        }
    }

    private sealed class DecimalModelContext(string connectionString) : TestDbContext(connectionString)
    {
        public DbSet<DecimalEntity> Items => Set<DecimalEntity>();
    }

    private sealed class SchemaModelContext(string connectionString) : TestDbContext(connectionString)
    {
        public DbSet<PersonRecord> People => Set<PersonRecord>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<PersonRecord>().ToTable("People", "app");
    }

    private sealed class DefaultValueModelContext(string connectionString) : TestDbContext(connectionString)
    {
        public DbSet<DefaultValueEntity> Items => Set<DefaultValueEntity>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<DefaultValueEntity>()
                .Property(item => item.Name)
                .HasDefaultValue("pending");
    }

    private sealed class CheckConstraintModelContext(string connectionString) : TestDbContext(connectionString)
    {
        public DbSet<CheckConstraintEntity> Items => Set<CheckConstraintEntity>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<CheckConstraintEntity>()
                .ToTable(tableBuilder => tableBuilder.HasCheckConstraint("CK_CheckConstraintEntity_Value", "Value > 0"));
    }

    private sealed class PersonRecord
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        public bool Active { get; set; }

        public double Score { get; set; }

        public long Visits { get; set; }

        public PersonStatus Status { get; set; }

        public Guid GuidValue { get; set; }

        public DateTime CreatedAt { get; set; }

        public DateTimeOffset ObservedAt { get; set; }

        public DateOnly Birthday { get; set; }

        public TimeOnly Alarm { get; set; }

        public byte[] Payload { get; set; } = [];
    }

    private sealed class ManualWidget
    {
        public long Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }

    private sealed class Blog
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        public List<Post> Posts { get; set; } = [];
    }

    private sealed class Post
    {
        public int Id { get; set; }

        public int BlogId { get; set; }

        public string Title { get; set; } = string.Empty;

        public Blog Blog { get; set; } = null!;
    }

    private sealed class Ticket
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        public int Version { get; set; }
    }

    private sealed class DecimalEntity
    {
        public int Id { get; set; }

        public decimal Amount { get; set; }
    }

    private sealed class DefaultValueEntity
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }

    private sealed class CheckConstraintEntity
    {
        public int Id { get; set; }

        public int Value { get; set; }
    }

    private enum PersonStatus
    {
        Unknown = 0,
        Active = 1,
        Suspended = 2,
    }
}
