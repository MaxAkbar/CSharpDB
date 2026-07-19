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

    [Theory]
    [InlineData(ProviderKind.CSharpDb)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task GroupedAndDistinctAggregates_MatchForBoundedValues(
        ProviderKind provider)
    {
        await using var db = CreateContext(
            provider,
            GetDbPath(provider, "grouped-distinct-aggregates"));
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
                Value = 4,
                Number = 7.5,
                OptionalNumber = 2.5,
                TextCol = "active-duplicate",
                Category = "Active",
            },
            new BenchEntity
            {
                Id = 4,
                Value = 8,
                Number = -3,
                OptionalNumber = 1,
                TextCol = "inactive-one",
                Category = "Inactive",
            },
            new BenchEntity
            {
                Id = 5,
                Value = 8,
                Number = -3,
                OptionalNumber = null,
                TextCol = "inactive-duplicate",
                Category = "Inactive",
            });
        await db.SaveChangesAsync(Ct);

        var grouped = await db.Rows
            .GroupBy(row => row.Category)
            .Where(group => group.Count() >= 2)
            .Select(group => new
            {
                Category = group.Key,
                Count = group.Count(),
                IntegerSum = group.Sum(row => row.Value),
                IntegerMin = group.Min(row => row.Value),
                IntegerMax = group.Max(row => row.Value),
                Sum = group.Sum(row => row.Number),
                Average = group.Average(row => row.Number),
                Min = group.Min(row => row.Number),
                Max = group.Max(row => row.Number),
                DistinctIntegerCount = group
                    .Select(row => row.Value)
                    .Distinct()
                    .Count(),
                DistinctIntegerSum = group
                    .Select(row => row.Value)
                    .Distinct()
                    .Sum(),
                DistinctIntegerMin = group
                    .Select(row => row.Value)
                    .Distinct()
                    .Min(),
                DistinctIntegerMax = group
                    .Select(row => row.Value)
                    .Distinct()
                    .Max(),
            })
            .OrderByDescending(row => row.Count)
            .ThenBy(row => row.Category)
            .ToListAsync(Ct);

        Assert.Equal(2, grouped.Count);
        Assert.Equal("Active", grouped[0].Category);
        Assert.Equal(3, grouped[0].Count);
        Assert.Equal(10, grouped[0].IntegerSum);
        Assert.Equal(2, grouped[0].IntegerMin);
        Assert.Equal(4, grouped[0].IntegerMax);
        Assert.Equal(17.5, grouped[0].Sum);
        Assert.Equal(17.5 / 3, grouped[0].Average, precision: 10);
        Assert.Equal(2.5, grouped[0].Min);
        Assert.Equal(7.5, grouped[0].Max);
        Assert.Equal(2, grouped[0].DistinctIntegerCount);
        Assert.Equal(6, grouped[0].DistinctIntegerSum);
        Assert.Equal(2, grouped[0].DistinctIntegerMin);
        Assert.Equal(4, grouped[0].DistinctIntegerMax);
        Assert.Equal("Inactive", grouped[1].Category);
        Assert.Equal(2, grouped[1].Count);
        Assert.Equal(16, grouped[1].IntegerSum);
        Assert.Equal(8, grouped[1].IntegerMin);
        Assert.Equal(8, grouped[1].IntegerMax);
        Assert.Equal(-6, grouped[1].Sum);
        Assert.Equal(-3, grouped[1].Average);
        Assert.Equal(-3, grouped[1].Min);
        Assert.Equal(-3, grouped[1].Max);
        Assert.Equal(1, grouped[1].DistinctIntegerCount);
        Assert.Equal(8, grouped[1].DistinctIntegerSum);
        Assert.Equal(8, grouped[1].DistinctIntegerMin);
        Assert.Equal(8, grouped[1].DistinctIntegerMax);

        IQueryable<int> distinctValues =
            db.Rows.Select(row => row.Value).Distinct();

        Assert.Equal(3, await distinctValues.CountAsync(Ct));
        Assert.Equal(3L, await distinctValues.LongCountAsync(Ct));
        Assert.Equal(14, await distinctValues.SumAsync(Ct));
        Assert.Equal(2, await distinctValues.MinAsync(Ct));
        Assert.Equal(8, await distinctValues.MaxAsync(Ct));
    }

    [Theory]
    [InlineData(ProviderKind.CSharpDb)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task DirectInnerJoin_MatchesForBoundedIntegerKeys(
        ProviderKind provider)
    {
        await using var db = CreateContext(
            provider,
            GetDbPath(provider, "direct-inner-join"));
        await db.Database.EnsureCreatedAsync(Ct);
        db.Rows.AddRange(
            new BenchEntity
            {
                Id = 1,
                Value = 2,
                TextCol = "outer-one",
                Category = "Outer",
            },
            new BenchEntity
            {
                Id = 2,
                Value = 3,
                TextCol = "lookup-two",
                Category = "Lookup",
            },
            new BenchEntity
            {
                Id = 3,
                Value = 99,
                TextCol = "lookup-three",
                Category = "Lookup",
            },
            new BenchEntity
            {
                Id = 4,
                Value = 2,
                TextCol = "outer-four",
                Category = "Outer",
            },
            new BenchEntity
            {
                Id = 5,
                Value = 88,
                TextCol = "unmatched",
                Category = "Outer",
            });
        await db.SaveChangesAsync(Ct);

        int minimumInnerId = 2;
        var results = await db.Rows
            .Where(outer =>
                outer.Category == "Outer")
            .Join(
                db.Rows,
                outer => outer.Value,
                inner => inner.Id,
                (outer, inner) => new
                {
                    OuterId = outer.Id,
                    InnerId = inner.Id,
                    InnerText = inner.TextCol,
                })
            .Where(result =>
                result.InnerId >= minimumInnerId)
            .OrderBy(result => result.OuterId)
            .Skip(0)
            .Take(10)
            .ToListAsync(Ct);

        Assert.Equal(
            [1, 4],
            results.Select(result =>
                result.OuterId));
        Assert.All(
            results,
            result =>
            {
                Assert.Equal(2, result.InnerId);
                Assert.Equal(
                    "lookup-two",
                    result.InnerText);
            });
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
