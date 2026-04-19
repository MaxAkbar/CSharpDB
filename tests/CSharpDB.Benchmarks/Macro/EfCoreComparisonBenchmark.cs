using System.Data.Common;
using System.Reflection;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.EntityFrameworkCore;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;

namespace CSharpDB.Benchmarks.Macro;

public static class EfCoreComparisonBenchmark
{
    private const int BatchSize = 100;
    private static readonly TimeSpan WarmupDuration = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan MeasuredDuration = TimeSpan.FromSeconds(5);

    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        var results = new List<BenchmarkResult>(capacity: 4);

        foreach (ProviderKind provider in Enum.GetValues<ProviderKind>())
        {
            results.Add(await RunSingleInsertAsync(provider));
            results.Add(await RunBatchInsertAsync(provider));
        }

        return results;
    }

    private static async Task<BenchmarkResult> RunSingleInsertAsync(ProviderKind provider)
    {
        await using var context = await ComparisonContext.CreateAsync(provider, $"{provider}-ef-single");
        await using var db = context.OpenDbContext();

        int nextId = 1_000_000;
        BenchmarkResult result = await MacroBenchmarkRunner.RunForDurationAsync(
            GetSingleInsertName(provider),
            WarmupDuration,
            MeasuredDuration,
            async () =>
            {
                int id = nextId++;
                db.Rows.Add(new BenchRow
                {
                    Id = id,
                    Value = id * 10,
                    TextCol = "durable",
                    Category = GetCategory(id),
                });

                int rowsAffected = await db.SaveChangesAsync().ConfigureAwait(false);
                if (rowsAffected != 1)
                    throw new InvalidOperationException($"Expected one inserted row for id={id}, observed {rowsAffected}.");

                db.ChangeTracker.Clear();
            }).ConfigureAwait(false);

        return CloneResult(result, extraInfo: context.BuildExtraInfo("workload=single-row SaveChanges"));
    }

    private static async Task<BenchmarkResult> RunBatchInsertAsync(ProviderKind provider)
    {
        await using var context = await ComparisonContext.CreateAsync(provider, $"{provider}-ef-batch");
        await using var db = context.OpenDbContext();

        int nextId = 2_000_000;
        BenchmarkResult result = await MacroBenchmarkRunner.RunForDurationAsync(
            GetBatchInsertName(provider),
            WarmupDuration,
            MeasuredDuration,
            async () =>
            {
                for (int i = 0; i < BatchSize; i++)
                {
                    int id = nextId++;
                    db.Rows.Add(new BenchRow
                    {
                        Id = id,
                        Value = id * 10,
                        TextCol = "durable",
                        Category = GetCategory(id),
                    });
                }

                int rowsAffected = await db.SaveChangesAsync().ConfigureAwait(false);
                if (rowsAffected != BatchSize)
                    throw new InvalidOperationException($"Expected {BatchSize} inserted rows, observed {rowsAffected}.");

                db.ChangeTracker.Clear();
            }).ConfigureAwait(false);

        return CloneResult(
            result,
            totalOps: result.TotalOps * BatchSize,
            extraInfo: context.BuildExtraInfo(
                $"batch-size={BatchSize}",
                "throughput-unit=rows/sec from 100-row SaveChanges batches"));
    }

    private static BenchmarkResult CloneResult(
        BenchmarkResult source,
        int? totalOps = null,
        string? extraInfo = null)
    {
        return new BenchmarkResult
        {
            Name = source.Name,
            TotalOps = totalOps ?? source.TotalOps,
            ElapsedMs = source.ElapsedMs,
            P50Ms = source.P50Ms,
            P90Ms = source.P90Ms,
            P95Ms = source.P95Ms,
            P99Ms = source.P99Ms,
            P999Ms = source.P999Ms,
            MinMs = source.MinMs,
            MaxMs = source.MaxMs,
            MeanMs = source.MeanMs,
            StdDevMs = source.StdDevMs,
            ExtraInfo = extraInfo ?? source.ExtraInfo
        };
    }

    private static string GetCategory(int id)
        => (id % 4) switch
        {
            0 => "Alpha",
            1 => "Beta",
            2 => "Gamma",
            _ => "Delta",
        };

    private static string GetSingleInsertName(ProviderKind provider)
        => provider switch
        {
            ProviderKind.CSharpDb => "EfCompare_CSharpDB_SingleInsert_5s",
            ProviderKind.Sqlite => "EfCompare_SQLite_SingleInsert_5s",
            _ => throw new ArgumentOutOfRangeException(nameof(provider), provider, null),
        };

    private static string GetBatchInsertName(ProviderKind provider)
        => provider switch
        {
            ProviderKind.CSharpDb => "EfCompare_CSharpDB_Batch100_5s",
            ProviderKind.Sqlite => "EfCompare_SQLite_Batch100_5s",
            _ => throw new ArgumentOutOfRangeException(nameof(provider), provider, null),
        };

    private enum ProviderKind
    {
        CSharpDb,
        Sqlite,
    }

    private sealed class ComparisonContext : IAsyncDisposable
    {
        private ComparisonContext(ProviderKind provider, string filePath)
        {
            Provider = provider;
            FilePath = filePath;
        }

        private ProviderKind Provider { get; }

        internal string FilePath { get; }

        internal static async Task<ComparisonContext> CreateAsync(ProviderKind provider, string prefix)
        {
            string filePath = Path.Combine(Path.GetTempPath(), $"{prefix}_{Guid.NewGuid():N}.db");
            var context = new ComparisonContext(provider, filePath);
            await using BenchmarkDbContext db = context.OpenDbContext();
            await db.Database.EnsureCreatedAsync().ConfigureAwait(false);
            db.ChangeTracker.Clear();
            return context;
        }

        internal BenchmarkDbContext OpenDbContext()
            => new(Provider, FilePath);

        internal string BuildExtraInfo(params string[] notes)
        {
            string baseInfo = Provider switch
            {
                ProviderKind.CSharpDb => $"provider=CSharpDB.EntityFrameworkCore/{GetCSharpDbProviderVersion()}, connectionPooling=false, surface=DbContext.SaveChangesAsync",
                ProviderKind.Sqlite => $"provider=Microsoft.EntityFrameworkCore.Sqlite/{GetSqliteEfCoreProviderVersion()}, cache=private, pooling=false, journal_mode=wal, synchronous=full, busyTimeoutMs=1000, surface=DbContext.SaveChangesAsync",
                _ => throw new ArgumentOutOfRangeException(),
            };

            return notes.Length == 0
                ? baseInfo
                : $"{baseInfo}, {string.Join(", ", notes)}";
        }

        public ValueTask DisposeAsync()
        {
            DeleteFiles(Provider, FilePath);
            return ValueTask.CompletedTask;
        }

        private static void DeleteFiles(ProviderKind provider, string filePath)
        {
            try { if (File.Exists(filePath)) File.Delete(filePath); } catch { }

            switch (provider)
            {
                case ProviderKind.CSharpDb:
                    try { if (File.Exists(filePath + ".wal")) File.Delete(filePath + ".wal"); } catch { }
                    break;
                case ProviderKind.Sqlite:
                    try { if (File.Exists(filePath + "-wal")) File.Delete(filePath + "-wal"); } catch { }
                    try { if (File.Exists(filePath + "-shm")) File.Delete(filePath + "-shm"); } catch { }
                    break;
            }
        }
    }

    private sealed class BenchmarkDbContext : DbContext
    {
        private readonly ProviderKind _provider;
        private readonly string _connectionString;

        internal BenchmarkDbContext(ProviderKind provider, string filePath)
        {
            _provider = provider;
            _connectionString = provider switch
            {
                ProviderKind.CSharpDb => $"Data Source={filePath}",
                ProviderKind.Sqlite => new SqliteConnectionStringBuilder
                {
                    DataSource = filePath,
                    Mode = SqliteOpenMode.ReadWriteCreate,
                    Cache = SqliteCacheMode.Private,
                    Pooling = false,
                    DefaultTimeout = 30,
                }.ToString(),
                _ => throw new ArgumentOutOfRangeException(nameof(provider), provider, null),
            };
        }

        public DbSet<BenchRow> Rows => Set<BenchRow>();

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (_provider == ProviderKind.CSharpDb)
            {
                optionsBuilder.UseCSharpDb(_connectionString);
            }
            else
            {
                optionsBuilder
                    .UseSqlite(_connectionString)
                    .AddInterceptors(SqlitePragmaConnectionInterceptor.Instance);
            }
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<BenchRow>(entity =>
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

    private sealed class BenchRow
    {
        public int Id { get; set; }

        public long Value { get; set; }

        public string TextCol { get; set; } = string.Empty;

        public string Category { get; set; } = string.Empty;
    }

    private sealed class SqlitePragmaConnectionInterceptor : DbConnectionInterceptor
    {
        internal static readonly SqlitePragmaConnectionInterceptor Instance = new();

        public override async Task ConnectionOpenedAsync(
            DbConnection connection,
            ConnectionEndEventData eventData,
            CancellationToken cancellationToken = default)
        {
            if (connection is not SqliteConnection sqliteConnection)
                return;

            await using var busyTimeout = sqliteConnection.CreateCommand();
            busyTimeout.CommandText = "PRAGMA busy_timeout=1000;";
            await busyTimeout.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            await using var journal = sqliteConnection.CreateCommand();
            journal.CommandText = "PRAGMA journal_mode=WAL;";
            string journalMode = (Convert.ToString(await journal.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false)) ?? string.Empty).Trim();
            if (!journalMode.Equals("wal", StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException($"Expected SQLite journal_mode=wal, observed '{journalMode}'.");

            await using var sync = sqliteConnection.CreateCommand();
            sync.CommandText = "PRAGMA synchronous=FULL;";
            await sync.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private static string GetCSharpDbProviderVersion()
    {
        Assembly assembly = typeof(CSharpDbDbContextOptionsBuilderExtensions).Assembly;
        string? informationalVersion = assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?
            .InformationalVersion;

        if (!string.IsNullOrWhiteSpace(informationalVersion))
            return informationalVersion.Split('+', 2)[0];

        return assembly.GetName().Version?.ToString() ?? "unknown";
    }

    private static string GetSqliteEfCoreProviderVersion()
    {
        Assembly assembly = typeof(SqliteDbContextOptionsBuilderExtensions).Assembly;
        string? informationalVersion = assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?
            .InformationalVersion;

        if (!string.IsNullOrWhiteSpace(informationalVersion))
            return informationalVersion.Split('+', 2)[0];

        return assembly.GetName().Version?.ToString() ?? "unknown";
    }
}
