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

    [Theory]
    [InlineData(ProviderKind.CSharpDb)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task DoubleMathFunctions_MatchForFiniteNonMidpointValues(ProviderKind provider)
    {
        await using var db = CreateContext(provider, GetDbPath(provider, "double-math"));
        await db.Database.EnsureCreatedAsync(Ct);
        db.Rows.AddRange(
            new BenchEntity
            {
                Id = 1,
                Value = 1,
                Number = -12.55,
                OptionalNumber = null,
                TextCol = "negative",
                Category = "Math",
            },
            new BenchEntity
            {
                Id = 2,
                Value = 2,
                Number = 3.25,
                OptionalNumber = -3.25,
                TextCol = "positive",
                Category = "Math",
            });
        await db.SaveChangesAsync(Ct);

        var result = await db.Rows
            .Where(row => Math.Abs(row.Number) > 10)
            .Select(row => new
            {
                Absolute = Math.Abs(row.Number),
                Rounded = Math.Round(Math.Abs(row.Number)),
                Floor = Math.Floor(row.Number),
                Ceiling = Math.Ceiling(row.Number),
                Truncated = Math.Truncate(row.Number),
                Sign = Math.Sign(row.Number),
            })
            .SingleAsync(Ct);

        Assert.Equal(12.55, result.Absolute, precision: 10);
        Assert.Equal(13, result.Rounded);
        Assert.Equal(-13, result.Floor);
        Assert.Equal(-12, result.Ceiling);
        Assert.Equal(-12, result.Truncated);
        Assert.Equal(-1, result.Sign);

        Assert.Equal(
            "positive",
            await db.Rows
                .Where(row => Math.Abs(row.OptionalNumber!.Value) > 0)
                .Select(row => row.TextCol)
                .SingleAsync(Ct));
    }

    [Theory]
    [InlineData(ProviderKind.CSharpDb)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScalarNumericAggregates_MatchForBoundedValuesAndNulls(ProviderKind provider)
    {
        await using var db = CreateContext(provider, GetDbPath(provider, "aggregates"));
        await db.Database.EnsureCreatedAsync(Ct);
        db.Rows.AddRange(
            new BenchEntity
            {
                Id = 1,
                Value = 2,
                Number = 2.5,
                OptionalNumber = 2.5,
                TextCol = "active-one",
                Category = "Active",
            },
            new BenchEntity
            {
                Id = 2,
                Value = 4,
                Number = 7.5,
                OptionalNumber = null,
                TextCol = "active-two",
                Category = "Active",
            },
            new BenchEntity
            {
                Id = 3,
                Value = 8,
                Number = -3,
                OptionalNumber = null,
                TextCol = "inactive",
                Category = "Inactive",
            });
        await db.SaveChangesAsync(Ct);

        IQueryable<BenchEntity> active =
            db.Rows.Where(row => row.Category == "Active");
        IQueryable<BenchEntity> empty =
            db.Rows.Where(row => row.Number > 100);
        IQueryable<BenchEntity> allNull =
            db.Rows.Where(row => row.Category == "Inactive");

        Assert.Equal(2, await active.CountAsync(Ct));
        Assert.Equal(2L, await active.LongCountAsync(Ct));
        Assert.True(await active.OrderBy(row => row.Id).Take(1).AnyAsync(Ct));
        Assert.False(await empty.AnyAsync(Ct));
        Assert.Equal(6, await active.SumAsync(row => row.Value, Ct));
        Assert.Equal(10, await active.SumAsync(row => row.Number, Ct));
        Assert.Equal(5, await active.AverageAsync(row => row.Number, Ct));
        Assert.Equal(2, await active.MinAsync(row => row.Value, Ct));
        Assert.Equal(4, await active.MaxAsync(row => row.Value, Ct));
        Assert.Equal(2.5, await active.MinAsync(row => row.Number, Ct));
        Assert.Equal(7.5, await active.MaxAsync(row => row.Number, Ct));
        Assert.Equal(0, await empty.SumAsync(row => row.Value, Ct));
        Assert.Equal(0, await empty.SumAsync(row => row.Number, Ct));
        Assert.Equal(2.5, await active.SumAsync(row => row.OptionalNumber, Ct));
        Assert.Equal(2.5, await active.AverageAsync(row => row.OptionalNumber, Ct));
        Assert.Equal(0, await empty.CountAsync(Ct));
        Assert.Equal(0L, await empty.LongCountAsync(Ct));
        Assert.Equal(0, await allNull.SumAsync(row => row.OptionalNumber, Ct));
        Assert.Equal(0, await empty.SumAsync(row => row.OptionalNumber, Ct));
        Assert.Null(await empty.AverageAsync(row => row.OptionalNumber, Ct));
        Assert.Null(await empty.MinAsync(row => row.OptionalNumber, Ct));
        Assert.Null(await empty.MaxAsync(row => row.OptionalNumber, Ct));
        InvalidOperationException emptyAverageException =
            await Assert.ThrowsAsync<InvalidOperationException>(
            () => empty.AverageAsync(row => row.Number, Ct));
        Assert.Contains(
            "Nullable object",
            emptyAverageException.Message,
            StringComparison.OrdinalIgnoreCase);

        InvalidOperationException emptyMinException =
            await Assert.ThrowsAsync<InvalidOperationException>(
            () => empty.MinAsync(row => row.Value, Ct));
        Assert.Contains(
            "Nullable object",
            emptyMinException.Message,
            StringComparison.OrdinalIgnoreCase);

        InvalidOperationException emptyMaxException =
            await Assert.ThrowsAsync<InvalidOperationException>(
            () => empty.MaxAsync(row => row.Number, Ct));
        Assert.Contains(
            "Nullable object",
            emptyMaxException.Message,
            StringComparison.OrdinalIgnoreCase);
        Assert.Null(await allNull.AverageAsync(row => row.OptionalNumber, Ct));
        Assert.Null(await allNull.MinAsync(row => row.OptionalNumber, Ct));
        Assert.Null(await allNull.MaxAsync(row => row.OptionalNumber, Ct));
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
                entity.Property(row => row.Number);
                entity.Property(row => row.OptionalNumber);
                entity.Property(row => row.TextCol);
                entity.Property(row => row.Category);
            });
        }
    }

    private sealed class BenchEntity
    {
        public int Id { get; set; }

        public int Value { get; set; }

        public double Number { get; set; }

        public double? OptionalNumber { get; set; }

        public string TextCol { get; set; } = string.Empty;

        public string Category { get; set; } = string.Empty;
    }
}
