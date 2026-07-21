using CSharpDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

namespace CSharpDB.EntityFrameworkCore.Tests;

[Collection("ConnectionPoolState")]
public sealed class CSharpDbTransactionCompatibilityTests : IAsyncLifetime
{
    private readonly string _workspace =
        Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_efcore_transactions_{Guid.NewGuid():N}");

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

    [Fact]
    public async Task ExplicitTransaction_SaveChangesCanCommitAndRollback()
    {
        string dbPath = GetDbPath("commit-rollback");

        await using (var db = CreateContext(dbPath))
        {
            await db.Database.EnsureCreatedAsync(Ct);

            await using (var transaction =
                await db.Database.BeginTransactionAsync(Ct))
            {
                Assert.False(transaction.SupportsSavepoints);
                db.Rows.Add(new TransactionRow { Name = "committed" });
                Assert.Equal(1, await db.SaveChangesAsync(Ct));
                await transaction.CommitAsync(Ct);
            }

            await using (var transaction =
                await db.Database.BeginTransactionAsync(Ct))
            {
                db.Rows.Add(new TransactionRow { Name = "rolled-back" });
                Assert.Equal(1, await db.SaveChangesAsync(Ct));
                await transaction.RollbackAsync(Ct);
            }
        }

        await using (var verify = CreateContext(dbPath))
        {
            Assert.Equal(
                ["committed"],
                await verify.Rows
                    .OrderBy(row => row.Id)
                    .Select(row => row.Name)
                    .ToListAsync(Ct));
        }
    }

    [Fact]
    public async Task ExplicitSavepointApis_ReportUnsupportedCapability()
    {
        string dbPath = GetDbPath("savepoint-boundary");

        await using var db = CreateContext(dbPath);
        await db.Database.EnsureCreatedAsync(Ct);
        await using var transaction =
            await db.Database.BeginTransactionAsync(Ct);

        Assert.False(transaction.SupportsSavepoints);

        NotSupportedException syncError =
            Assert.Throws<NotSupportedException>(
                () => transaction.CreateSavepoint("manual"));
        Assert.Equal(
            "CSharpDB does not support transaction savepoints.",
            syncError.Message);

        NotSupportedException asyncError =
            await Assert.ThrowsAsync<NotSupportedException>(
                () => transaction.CreateSavepointAsync("manual", Ct));
        Assert.Equal(syncError.Message, asyncError.Message);

        await transaction.RollbackAsync(Ct);
    }

    private TransactionContext CreateContext(string dbPath)
    {
        var options = new DbContextOptionsBuilder<TransactionContext>()
            .UseCSharpDb($"Data Source={dbPath}")
            .Options;

        return new TransactionContext(options);
    }

    private string GetDbPath(string name) =>
        Path.Combine(_workspace, $"{name}.cdb");

    private sealed class TransactionContext(
        DbContextOptions<TransactionContext> options)
        : DbContext(options)
    {
        public DbSet<TransactionRow> Rows => Set<TransactionRow>();
    }

    private sealed class TransactionRow
    {
        public int Id { get; set; }

        public required string Name { get; set; }
    }
}
