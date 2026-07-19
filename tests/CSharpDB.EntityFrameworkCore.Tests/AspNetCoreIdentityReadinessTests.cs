using System.Security.Claims;
using CSharpDB.EntityFrameworkCore;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Identity.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

namespace CSharpDB.EntityFrameworkCore.Tests;

[Collection("ConnectionPoolState")]
public sealed class AspNetCoreIdentityReadinessTests : IAsyncLifetime
{
    private readonly string _workspace =
        Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_efcore_identity_{Guid.NewGuid():N}");

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
    public async Task IdentitySchemaV1_WithIntegerKeys_StoreWorkflowPersistsAndReopens()
    {
        string dbPath = GetDbPath("store-workflow");

        await using (var db = CreateContext(dbPath))
        {
            Assert.True(await db.Database.EnsureCreatedAsync(Ct));

            string[] tables = db.Model
                .GetEntityTypes()
                .Select(entityType => entityType.GetTableName())
                .Where(table => table is not null)
                .Cast<string>()
                .OrderBy(table => table, StringComparer.Ordinal)
                .ToArray();

            Assert.Equal(
                [
                    "AspNetRoleClaims",
                    "AspNetRoles",
                    "AspNetUserClaims",
                    "AspNetUserLogins",
                    "AspNetUserRoles",
                    "AspNetUserTokens",
                    "AspNetUsers",
                ],
                tables);

            using var roleStore =
                new RoleStore<QualifiedIdentityRole, QualifiedIdentityContext, int>(
                    db,
                    new IdentityErrorDescriber());
            using var userStore =
                new UserStore<QualifiedIdentityUser, QualifiedIdentityRole, QualifiedIdentityContext, int>(
                    db,
                    new IdentityErrorDescriber());

            var role = new QualifiedIdentityRole
            {
                Name = "Administrator",
                NormalizedName = "ADMINISTRATOR",
                ConcurrencyStamp = Guid.NewGuid().ToString("N"),
            };
            AssertIdentitySuccess(await roleStore.CreateAsync(role, Ct));
            Assert.True(role.Id > 0);

            var user = new QualifiedIdentityUser
            {
                UserName = "alice",
                NormalizedUserName = "ALICE",
                Email = "alice@example.test",
                NormalizedEmail = "ALICE@EXAMPLE.TEST",
                EmailConfirmed = true,
                PasswordHash = "qualified-password-hash",
                SecurityStamp = Guid.NewGuid().ToString("N"),
                ConcurrencyStamp = Guid.NewGuid().ToString("N"),
            };
            AssertIdentitySuccess(await userStore.CreateAsync(user, Ct));
            Assert.True(user.Id > 0);

            await userStore.AddToRoleAsync(user, role.NormalizedName!, Ct);
            await userStore.AddClaimsAsync(
                user,
                [new Claim("permission", "manage")],
                Ct);
            await userStore.AddLoginAsync(
                user,
                new UserLoginInfo("qualified-provider", "alice-key", "Qualified provider"),
                Ct);
            await userStore.SetTokenAsync(
                user,
                "qualified-provider",
                "refresh",
                "token-value",
                Ct);
            await db.SaveChangesAsync(Ct);
        }

        await using (var db = CreateContext(dbPath))
        {
            Assert.False(await db.Database.EnsureCreatedAsync(Ct));

            using var userStore =
                new UserStore<QualifiedIdentityUser, QualifiedIdentityRole, QualifiedIdentityContext, int>(
                    db,
                    new IdentityErrorDescriber());

            QualifiedIdentityUser? user =
                await userStore.FindByNameAsync("ALICE", Ct);
            Assert.NotNull(user);
            Assert.Equal("alice@example.test", user.Email);

            Assert.Equal(
                ["Administrator"],
                await userStore.GetRolesAsync(user, Ct));

            Claim claim = Assert.Single(await userStore.GetClaimsAsync(user, Ct));
            Assert.Equal("permission", claim.Type);
            Assert.Equal("manage", claim.Value);

            UserLoginInfo login =
                Assert.Single(await userStore.GetLoginsAsync(user, Ct));
            Assert.Equal("qualified-provider", login.LoginProvider);
            Assert.Equal("alice-key", login.ProviderKey);

            Assert.Equal(
                "token-value",
                await userStore.GetTokenAsync(
                    user,
                    "qualified-provider",
                    "refresh",
                    Ct));

            QualifiedIdentityUser? byLogin =
                await userStore.FindByLoginAsync(
                    "qualified-provider",
                    "alice-key",
                    Ct);
            Assert.Equal(user.Id, byLogin?.Id);

            AssertIdentitySuccess(await userStore.DeleteAsync(user, Ct));
        }

        await using (var db = CreateContext(dbPath))
        {
            Assert.Empty(await db.Users.ToListAsync(Ct));
            Assert.Empty(await db.UserClaims.ToListAsync(Ct));
            Assert.Empty(await db.UserLogins.ToListAsync(Ct));
            Assert.Empty(await db.UserRoles.ToListAsync(Ct));
            Assert.Empty(await db.UserTokens.ToListAsync(Ct));
            Assert.Single(await db.Roles.ToListAsync(Ct));
        }
    }

    [Fact]
    public async Task IdentitySchemaV1_WithIntegerKeys_ConcurrencyStampRejectsStaleUpdate()
    {
        string dbPath = GetDbPath("concurrency");

        await using (var seed = CreateContext(dbPath))
        {
            await seed.Database.EnsureCreatedAsync(Ct);
            using var store =
                new UserStore<QualifiedIdentityUser, QualifiedIdentityRole, QualifiedIdentityContext, int>(
                    seed,
                    new IdentityErrorDescriber());

            AssertIdentitySuccess(
                await store.CreateAsync(CreateQualifiedUser("concurrent"), Ct));
        }

        await using var first = CreateContext(dbPath);
        await using var second = CreateContext(dbPath);
        using var firstStore =
            new UserStore<QualifiedIdentityUser, QualifiedIdentityRole, QualifiedIdentityContext, int>(
                first,
                new IdentityErrorDescriber());
        using var secondStore =
            new UserStore<QualifiedIdentityUser, QualifiedIdentityRole, QualifiedIdentityContext, int>(
                second,
                new IdentityErrorDescriber());

        QualifiedIdentityUser firstUser =
            Assert.IsType<QualifiedIdentityUser>(
                await firstStore.FindByNameAsync("CONCURRENT", Ct));
        QualifiedIdentityUser staleUser =
            Assert.IsType<QualifiedIdentityUser>(
                await secondStore.FindByNameAsync("CONCURRENT", Ct));

        firstUser.PhoneNumber = "111-1111";
        AssertIdentitySuccess(await firstStore.UpdateAsync(firstUser, Ct));

        staleUser.PhoneNumber = "222-2222";
        IdentityResult staleResult =
            await secondStore.UpdateAsync(staleUser, Ct);

        Assert.False(staleResult.Succeeded);
        IdentityError concurrencyError = Assert.Single(staleResult.Errors);
        Assert.Equal("ConcurrencyFailure", concurrencyError.Code);

        await using var verify = CreateContext(dbPath);
        Assert.Equal(
            "111-1111",
            (await verify.Users.SingleAsync(Ct)).PhoneNumber);
    }

    [Fact]
    public async Task IdentitySchemaV1_WithIntegerKeys_StoreWritesHonorTransactionRollback()
    {
        string dbPath = GetDbPath("transaction-rollback");

        await using (var db = CreateContext(dbPath))
        {
            await db.Database.EnsureCreatedAsync(Ct);
            await using var transaction =
                await db.Database.BeginTransactionAsync(Ct);
            using var roleStore =
                new RoleStore<QualifiedIdentityRole, QualifiedIdentityContext, int>(
                    db,
                    new IdentityErrorDescriber());
            using var userStore =
                new UserStore<QualifiedIdentityUser, QualifiedIdentityRole, QualifiedIdentityContext, int>(
                    db,
                    new IdentityErrorDescriber());

            AssertIdentitySuccess(
                await roleStore.CreateAsync(
                    new QualifiedIdentityRole
                    {
                        Name = "Transient",
                        NormalizedName = "TRANSIENT",
                        ConcurrencyStamp = Guid.NewGuid().ToString("N"),
                    },
                    Ct));
            AssertIdentitySuccess(
                await userStore.CreateAsync(
                    CreateQualifiedUser("transient"),
                    Ct));

            await transaction.RollbackAsync(Ct);
        }

        await using (var verify = CreateContext(dbPath))
        {
            Assert.Empty(await verify.Users.ToListAsync(Ct));
            Assert.Empty(await verify.Roles.ToListAsync(Ct));
        }
    }

    [Fact]
    public async Task IdentitySchemaV1_WithIntegerKeys_PreCanceledStoreReadIsCanceled()
    {
        string dbPath = GetDbPath("cancellation");

        await using var db = CreateContext(dbPath);
        await db.Database.EnsureCreatedAsync(Ct);
        using var store =
            new UserStore<QualifiedIdentityUser, QualifiedIdentityRole, QualifiedIdentityContext, int>(
                db,
                new IdentityErrorDescriber());
        AssertIdentitySuccess(
            await store.CreateAsync(CreateQualifiedUser("cancel-me"), Ct));

        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => store.FindByNameAsync(
                "CANCEL-ME",
                cancellation.Token));
    }

    [Fact]
    public async Task IdentitySchemaV1_WithDefaultStringKeys_RoleJoinReportsProviderDiagnostic()
    {
        string dbPath = GetDbPath("default-string-key-boundary");
        var options = new DbContextOptionsBuilder<StringKeyIdentityContext>()
            .UseCSharpDb($"Data Source={dbPath}")
            .Options;

        await using var db = new StringKeyIdentityContext(options);
        await db.Database.EnsureCreatedAsync(Ct);
        using var roleStore =
            new RoleStore<IdentityRole, StringKeyIdentityContext, string>(
                db,
                new IdentityErrorDescriber());
        using var userStore =
            new UserStore<IdentityUser, IdentityRole, StringKeyIdentityContext, string>(
                db,
                new IdentityErrorDescriber());

        var role = new IdentityRole
        {
            Name = "Member",
            NormalizedName = "MEMBER",
            ConcurrencyStamp = Guid.NewGuid().ToString("N"),
        };
        var user = new IdentityUser
        {
            UserName = "string-user",
            NormalizedUserName = "STRING-USER",
            SecurityStamp = Guid.NewGuid().ToString("N"),
            ConcurrencyStamp = Guid.NewGuid().ToString("N"),
        };

        AssertIdentitySuccess(await roleStore.CreateAsync(role, Ct));
        AssertIdentitySuccess(await userStore.CreateAsync(user, Ct));
        await userStore.AddToRoleAsync(user, role.NormalizedName!, Ct);
        await db.SaveChangesAsync(Ct);

        InvalidOperationException error =
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => userStore.GetRolesAsync(user, Ct));
        Assert.Contains("CDBEF1007", error.Message, StringComparison.Ordinal);
        Assert.Contains(
            "INTEGER-backed",
            error.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    private QualifiedIdentityContext CreateContext(string dbPath)
    {
        var options = new DbContextOptionsBuilder<QualifiedIdentityContext>()
            .UseCSharpDb($"Data Source={dbPath}")
            .Options;

        return new QualifiedIdentityContext(options);
    }

    private string GetDbPath(string name) =>
        Path.Combine(_workspace, $"{name}.cdb");

    private static QualifiedIdentityUser CreateQualifiedUser(string userName)
    {
        string normalized = userName.ToUpperInvariant();
        return new QualifiedIdentityUser
        {
            UserName = userName,
            NormalizedUserName = normalized,
            Email = $"{userName}@example.test",
            NormalizedEmail = $"{normalized}@EXAMPLE.TEST",
            SecurityStamp = Guid.NewGuid().ToString("N"),
            ConcurrencyStamp = Guid.NewGuid().ToString("N"),
        };
    }

    private static void AssertIdentitySuccess(IdentityResult result)
    {
        Assert.True(
            result.Succeeded,
            string.Join(
                Environment.NewLine,
                result.Errors.Select(error => $"{error.Code}: {error.Description}")));
    }

    private sealed class QualifiedIdentityContext(
        DbContextOptions<QualifiedIdentityContext> options)
        : IdentityDbContext<
            QualifiedIdentityUser,
            QualifiedIdentityRole,
            int>(options)
    {
        protected override Version SchemaVersion => new(1, 0);
    }

    private sealed class QualifiedIdentityUser : IdentityUser<int>;

    private sealed class QualifiedIdentityRole : IdentityRole<int>;

    private sealed class StringKeyIdentityContext(
        DbContextOptions<StringKeyIdentityContext> options)
        : IdentityDbContext<IdentityUser, IdentityRole, string>(options)
    {
        protected override Version SchemaVersion => new(1, 0);
    }
}
