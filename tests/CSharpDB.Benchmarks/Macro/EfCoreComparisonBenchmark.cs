using System.Data.Common;
using System.Reflection;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Data;
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

    public static Task<List<BenchmarkResult>> RunAsync()
        => RunAsync(ConnectionLifetimeMode.OpenOncePerRun);

    public static async Task<List<BenchmarkResult>> RunAsync(ConnectionLifetimeMode connectionLifetimeMode)
    {
        var results = new List<BenchmarkResult>(capacity: 4);

        foreach (ProviderKind provider in Enum.GetValues<ProviderKind>())
        {
            results.Add(await RunSingleInsertAsync(provider, connectionLifetimeMode));
            results.Add(await RunBatchInsertAsync(provider, connectionLifetimeMode));
        }

        return results;
    }

    private static async Task<BenchmarkResult> RunSingleInsertAsync(
        ProviderKind provider,
        ConnectionLifetimeMode connectionLifetimeMode)
    {
        await using var context = await ComparisonContext.CreateAsync(
            provider,
            $"{provider}-ef-single");

        int nextId = 1_000_000;
        BenchmarkResult result;

        if (connectionLifetimeMode == ConnectionLifetimeMode.HybridSharedConnectionPerRun)
        {
            result = await MacroBenchmarkRunner.RunForDurationAsync(
                GetSingleInsertName(provider, connectionLifetimeMode),
                WarmupDuration,
                MeasuredDuration,
                async () =>
                {
                    await using BenchmarkDbContext db = await context.OpenDbContextAsync(connectionLifetimeMode).ConfigureAwait(false);
                    await RunSingleInsertIterationAsync(db, nextId++, clearTrackedState: false).ConfigureAwait(false);
                }).ConfigureAwait(false);
        }
        else
        {
            await using BenchmarkDbContext db = await context.OpenDbContextAsync(connectionLifetimeMode).ConfigureAwait(false);
            await OpenConnectionIfRequestedAsync(db, connectionLifetimeMode).ConfigureAwait(false);

            result = await MacroBenchmarkRunner.RunForDurationAsync(
                GetSingleInsertName(provider, connectionLifetimeMode),
                WarmupDuration,
                MeasuredDuration,
                async () =>
                {
                    await RunSingleInsertIterationAsync(db, nextId++, clearTrackedState: true).ConfigureAwait(false);
                }).ConfigureAwait(false);
        }

        return CloneResult(
            result,
            extraInfo: context.BuildExtraInfo(
                connectionLifetimeMode,
                "workload=single-row SaveChanges"));
    }

    private static async Task<BenchmarkResult> RunBatchInsertAsync(
        ProviderKind provider,
        ConnectionLifetimeMode connectionLifetimeMode)
    {
        await using var context = await ComparisonContext.CreateAsync(
            provider,
            $"{provider}-ef-batch");

        int nextId = 2_000_000;
        BenchmarkResult result;

        if (connectionLifetimeMode == ConnectionLifetimeMode.HybridSharedConnectionPerRun)
        {
            result = await MacroBenchmarkRunner.RunForDurationAsync(
                GetBatchInsertName(provider, connectionLifetimeMode),
                WarmupDuration,
                MeasuredDuration,
                async () =>
                {
                    await using BenchmarkDbContext db = await context.OpenDbContextAsync(connectionLifetimeMode).ConfigureAwait(false);
                    nextId = await RunBatchInsertIterationAsync(db, nextId, clearTrackedState: false).ConfigureAwait(false);
                }).ConfigureAwait(false);
        }
        else
        {
            await using BenchmarkDbContext db = await context.OpenDbContextAsync(connectionLifetimeMode).ConfigureAwait(false);
            await OpenConnectionIfRequestedAsync(db, connectionLifetimeMode).ConfigureAwait(false);

            result = await MacroBenchmarkRunner.RunForDurationAsync(
                GetBatchInsertName(provider, connectionLifetimeMode),
                WarmupDuration,
                MeasuredDuration,
                async () =>
                {
                    nextId = await RunBatchInsertIterationAsync(db, nextId, clearTrackedState: true).ConfigureAwait(false);
                }).ConfigureAwait(false);
        }

        return CloneResult(
            result,
            totalOps: result.TotalOps * BatchSize,
            extraInfo: context.BuildExtraInfo(
                connectionLifetimeMode,
                $"batch-size={BatchSize}",
                "throughput-unit=rows/sec from 100-row SaveChanges batches"));
    }

    private static Task OpenConnectionIfRequestedAsync(
        BenchmarkDbContext db,
        ConnectionLifetimeMode connectionLifetimeMode)
        => connectionLifetimeMode == ConnectionLifetimeMode.OpenOncePerRun
            ? db.Database.OpenConnectionAsync()
            : Task.CompletedTask;

    private static async Task RunSingleInsertIterationAsync(
        BenchmarkDbContext db,
        int id,
        bool clearTrackedState)
    {
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

        if (clearTrackedState)
            db.ChangeTracker.Clear();
    }

    private static async Task<int> RunBatchInsertIterationAsync(
        BenchmarkDbContext db,
        int nextId,
        bool clearTrackedState)
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

        if (clearTrackedState)
            db.ChangeTracker.Clear();

        return nextId;
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

    private static string GetSingleInsertName(ProviderKind provider, ConnectionLifetimeMode connectionLifetimeMode)
        => provider switch
        {
            ProviderKind.CSharpDb => $"EfCompare_CSharpDB_WriteOptimized_{GetConnectionLifetimeName(connectionLifetimeMode)}_SingleInsert_5s",
            ProviderKind.Sqlite => $"EfCompare_SQLite_{GetConnectionLifetimeName(connectionLifetimeMode)}_SingleInsert_5s",
            _ => throw new ArgumentOutOfRangeException(nameof(provider), provider, null),
        };

    private static string GetBatchInsertName(ProviderKind provider, ConnectionLifetimeMode connectionLifetimeMode)
        => provider switch
        {
            ProviderKind.CSharpDb => $"EfCompare_CSharpDB_WriteOptimized_{GetConnectionLifetimeName(connectionLifetimeMode)}_Batch100_5s",
            ProviderKind.Sqlite => $"EfCompare_SQLite_{GetConnectionLifetimeName(connectionLifetimeMode)}_Batch100_5s",
            _ => throw new ArgumentOutOfRangeException(nameof(provider), provider, null),
        };

    private static string GetConnectionLifetimeName(ConnectionLifetimeMode connectionLifetimeMode)
        => connectionLifetimeMode switch
        {
            ConnectionLifetimeMode.OpenOncePerRun => "OpenOnce",
            ConnectionLifetimeMode.HybridSharedConnectionPerRun => "HybridSharedConnection",
            ConnectionLifetimeMode.AutoOpenClosePerSaveChanges => "AutoOpenClose",
            _ => throw new ArgumentOutOfRangeException(nameof(connectionLifetimeMode), connectionLifetimeMode, null),
        };

    private enum ProviderKind
    {
        CSharpDb,
        Sqlite,
    }

    public enum ConnectionLifetimeMode
    {
        OpenOncePerRun,
        HybridSharedConnectionPerRun,
        AutoOpenClosePerSaveChanges,
    }

    private sealed class ComparisonContext : IAsyncDisposable
    {
        private DbConnection? _sharedConnection;

        private ComparisonContext(ProviderKind provider, string filePath)
        {
            Provider = provider;
            FilePath = filePath;
        }

        private ProviderKind Provider { get; }

        internal string FilePath { get; }

        internal static async Task<ComparisonContext> CreateAsync(
            ProviderKind provider,
            string prefix)
        {
            string filePath = Path.Combine(Path.GetTempPath(), $"{prefix}_{Guid.NewGuid():N}.db");
            var context = new ComparisonContext(provider, filePath);
            await using BenchmarkDbContext db = new(provider, filePath);
            await db.Database.EnsureCreatedAsync().ConfigureAwait(false);
            db.ChangeTracker.Clear();
            return context;
        }

        internal async Task<BenchmarkDbContext> OpenDbContextAsync(ConnectionLifetimeMode connectionLifetimeMode)
        {
            if (connectionLifetimeMode != ConnectionLifetimeMode.HybridSharedConnectionPerRun)
                return new BenchmarkDbContext(Provider, FilePath);

            DbConnection sharedConnection = await GetOrCreateSharedConnectionAsync().ConfigureAwait(false);
            return new BenchmarkDbContext(Provider, sharedConnection);
        }

        internal string BuildExtraInfo(
            ConnectionLifetimeMode connectionLifetimeMode,
            params string[] notes)
        {
            string connectionLifetime = connectionLifetimeMode switch
            {
                ConnectionLifetimeMode.OpenOncePerRun => "open-once-per-run",
                ConnectionLifetimeMode.HybridSharedConnectionPerRun => "externally-owned-open-connection",
                ConnectionLifetimeMode.AutoOpenClosePerSaveChanges => "ef-managed-auto-open-close",
                _ => throw new ArgumentOutOfRangeException(nameof(connectionLifetimeMode), connectionLifetimeMode, null),
            };

            string dbContextLifetime = connectionLifetimeMode switch
            {
                ConnectionLifetimeMode.OpenOncePerRun => "single-context-per-run",
                ConnectionLifetimeMode.HybridSharedConnectionPerRun => "short-lived-context-per-save",
                ConnectionLifetimeMode.AutoOpenClosePerSaveChanges => "single-context-per-run",
                _ => throw new ArgumentOutOfRangeException(nameof(connectionLifetimeMode), connectionLifetimeMode, null),
            };

            string baseInfo = Provider switch
            {
                ProviderKind.CSharpDb =>
                    $"provider=CSharpDB.EntityFrameworkCore/{GetCSharpDbProviderVersion()}, connectionPooling=false, connection-lifetime={connectionLifetime}, dbcontext-lifetime={dbContextLifetime}, surface=DbContext.SaveChangesAsync, storage-preset=WriteOptimized, embedded-open-mode=direct",
                ProviderKind.Sqlite => $"provider=Microsoft.EntityFrameworkCore.Sqlite/{GetSqliteEfCoreProviderVersion()}, cache=private, pooling=false, connection-lifetime={connectionLifetime}, dbcontext-lifetime={dbContextLifetime}, journal_mode=wal, synchronous=full, busyTimeoutMs=1000, surface=DbContext.SaveChangesAsync",
                _ => throw new ArgumentOutOfRangeException(),
            };

            return notes.Length == 0
                ? baseInfo
                : $"{baseInfo}, {string.Join(", ", notes)}";
        }

        public async ValueTask DisposeAsync()
        {
            if (_sharedConnection is IAsyncDisposable asyncDisposable)
                await asyncDisposable.DisposeAsync().ConfigureAwait(false);
            else
                _sharedConnection?.Dispose();

            DeleteFiles(Provider, FilePath);
        }

        private async Task<DbConnection> GetOrCreateSharedConnectionAsync()
        {
            if (_sharedConnection is not null)
                return _sharedConnection;

            _sharedConnection = Provider switch
            {
                ProviderKind.CSharpDb => new CSharpDbConnection(GetCSharpDbConnectionString(FilePath)),
                ProviderKind.Sqlite => new SqliteConnection(GetSqliteConnectionString(FilePath)),
                _ => throw new ArgumentOutOfRangeException(),
            };

            await _sharedConnection.OpenAsync().ConfigureAwait(false);

            if (_sharedConnection is SqliteConnection sqliteConnection)
                await ConfigureSqliteConnectionAsync(sqliteConnection).ConfigureAwait(false);

            return _sharedConnection;
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
        private readonly DbConnection? _connection;
        private readonly string? _connectionString;

        internal BenchmarkDbContext(ProviderKind provider, string filePath)
        {
            _provider = provider;
            _connectionString = provider switch
            {
                ProviderKind.CSharpDb => GetCSharpDbConnectionString(filePath),
                ProviderKind.Sqlite => GetSqliteConnectionString(filePath),
                _ => throw new ArgumentOutOfRangeException(nameof(provider), provider, null),
            };
        }

        internal BenchmarkDbContext(ProviderKind provider, DbConnection connection)
        {
            _provider = provider;
            _connection = connection;
            _connectionString = connection.ConnectionString;
        }

        public DbSet<BenchRow> Rows => Set<BenchRow>();

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (_provider == ProviderKind.CSharpDb)
            {
                if (_connection is not null)
                {
                    optionsBuilder.UseCSharpDb(_connection);
                }
                else
                {
                    optionsBuilder.UseCSharpDb(
                        _connectionString!,
                        csharpdb =>
                        {
                            csharpdb.UseStoragePreset(CSharpDbStoragePreset.WriteOptimized);
                            csharpdb.UseEmbeddedOpenMode(CSharpDbEmbeddedOpenMode.Direct);
                        });
                }
            }
            else
            {
                if (_connection is SqliteConnection sqliteConnection)
                {
                    optionsBuilder.UseSqlite(sqliteConnection);
                }
                else
                {
                    optionsBuilder.UseSqlite(_connectionString!);
                }

                optionsBuilder.AddInterceptors(SqlitePragmaConnectionInterceptor.Instance);
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

            await ConfigureSqliteConnectionAsync(sqliteConnection, cancellationToken).ConfigureAwait(false);
        }
    }

    private static string GetCSharpDbConnectionString(string filePath)
        => $"Data Source={filePath};Pooling=false;Storage Preset=WriteOptimized;Embedded Open Mode=Direct";

    private static string GetSqliteConnectionString(string filePath)
        => new SqliteConnectionStringBuilder
        {
            DataSource = filePath,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Cache = SqliteCacheMode.Private,
            Pooling = false,
            DefaultTimeout = 30,
        }.ToString();

    private static async Task ConfigureSqliteConnectionAsync(
        SqliteConnection sqliteConnection,
        CancellationToken cancellationToken = default)
    {
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
