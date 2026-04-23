using CSharpDB.EntityFrameworkCore;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;

namespace CSharpDB.EntityFrameworkCore.Tests;

[Collection("ConnectionPoolState")]
public sealed class ComparativeEfCoreSmokeTests : IAsyncLifetime
{
    private readonly string _workspace = Path.Combine(Path.GetTempPath(), $"csharpdb_ef_compare_{Guid.NewGuid():N}");

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        Directory.CreateDirectory(_workspace);
        await Data.CSharpDbConnection.ClearAllPoolsAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await Data.CSharpDbConnection.ClearAllPoolsAsync();

        if (Directory.Exists(_workspace))
            Directory.Delete(_workspace, recursive: true);
    }

    [Theory]
    [InlineData(ProviderKind.CSharpDb)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task EnsureCreated_CrudRoundTrip_Succeeds(ProviderKind provider)
    {
        await using var db = CreateContext(provider, GetDbPath(provider, "crud"));
        await db.Database.EnsureCreatedAsync(Ct);

        db.Rows.Add(new BenchEntity
        {
            Id = 1,
            Value = 7,
            TextCol = "durable",
            Category = "Alpha",
        });

        Assert.Equal(1, await db.SaveChangesAsync(Ct));
        db.ChangeTracker.Clear();

        BenchEntity loaded = await db.Rows.SingleAsync(row => row.Id == 1, Ct);
        Assert.Equal(7, loaded.Value);
        Assert.Equal("durable", loaded.TextCol);
        Assert.Equal("Alpha", loaded.Category);

        loaded.Category = "Beta";
        Assert.Equal(1, await db.SaveChangesAsync(Ct));
        db.ChangeTracker.Clear();

        Assert.Equal(1, await db.Rows.CountAsync(row => row.Category == "Beta", Ct));

        BenchEntity remove = await db.Rows.SingleAsync(row => row.Id == 1, Ct);
        db.Remove(remove);
        Assert.Equal(1, await db.SaveChangesAsync(Ct));

        Assert.Equal(0, await db.Rows.CountAsync(Ct));
    }

    [Theory]
    [InlineData(ProviderKind.CSharpDb)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task BatchInsert_CountMatches(ProviderKind provider)
    {
        await using var db = CreateContext(provider, GetDbPath(provider, "batch"));
        await db.Database.EnsureCreatedAsync(Ct);

        db.Rows.AddRange(
            Enumerable.Range(1, 8).Select(id => new BenchEntity
            {
                Id = id,
                Value = id * 10,
                TextCol = $"row-{id}",
                Category = (id % 2 == 0) ? "Even" : "Odd",
            }));

        Assert.Equal(8, await db.SaveChangesAsync(Ct));
        db.ChangeTracker.Clear();

        Assert.Equal(8, await db.Rows.CountAsync(Ct));
        Assert.Equal(4, await db.Rows.CountAsync(row => row.Category == "Even", Ct));
    }

    private ComparisonDbContext CreateContext(ProviderKind provider, string dbPath)
        => new(provider, provider switch
        {
            ProviderKind.CSharpDb => $"Data Source={dbPath}",
            ProviderKind.Sqlite => new SqliteConnectionStringBuilder
            {
                DataSource = dbPath,
                Mode = SqliteOpenMode.ReadWriteCreate,
                Cache = SqliteCacheMode.Private,
                Pooling = false,
                DefaultTimeout = 30,
            }.ToString(),
            _ => throw new ArgumentOutOfRangeException(nameof(provider), provider, null),
        });

    private string GetDbPath(ProviderKind provider, string suffix)
        => Path.Combine(_workspace, $"{provider.ToString().ToLowerInvariant()}-{suffix}.db");

    public enum ProviderKind
    {
        CSharpDb,
        Sqlite,
    }

    private sealed class ComparisonDbContext : DbContext
    {
        private readonly ProviderKind _provider;
        private readonly string _connectionString;

        public ComparisonDbContext(ProviderKind provider, string connectionString)
        {
            _provider = provider;
            _connectionString = connectionString;
        }

        public DbSet<BenchEntity> Rows => Set<BenchEntity>();

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (_provider == ProviderKind.CSharpDb)
                optionsBuilder.UseCSharpDb(_connectionString);
            else
                optionsBuilder.UseSqlite(_connectionString);
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<BenchEntity>(entity =>
            {
                entity.ToTable("bench");
                entity.HasKey(row => row.Id);
                entity.Property(row => row.Id).ValueGeneratedNever();
                entity.Property(row => row.Value);
                entity.Property(row => row.TextCol);
                entity.Property(row => row.Category);
            });
        }
    }

    private sealed class BenchEntity
    {
        public int Id { get; set; }

        public int Value { get; set; }

        public string TextCol { get; set; } = string.Empty;

        public string Category { get; set; } = string.Empty;
    }
}
