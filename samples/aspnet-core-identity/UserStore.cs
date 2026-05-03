using System.Data.Common;
using CSharpDB.Data;
using Microsoft.AspNetCore.Identity;

namespace AspNetCoreIdentitySample;

public sealed class UserStore
{
    private readonly string _connectionString;
    private readonly IPasswordHasher<AppUser> _hasher;

    public UserStore(string connectionString, IPasswordHasher<AppUser> hasher)
    {
        _connectionString = connectionString;
        _hasher = hasher;
    }

    public async Task EnsureSchemaAsync()
    {
        await using CSharpDbConnection conn = await OpenAsync();

        foreach (string ddl in SchemaScript)
        {
            await using CSharpDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = ddl;
            await cmd.ExecuteNonQueryAsync();
        }
    }

    public async Task<AppUser?> FindByEmailAsync(string email)
    {
        string normalized = NormalizeEmail(email);
        await using CSharpDbConnection conn = await OpenAsync();

        await using CSharpDbCommand cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT Id, Email, DisplayName, PasswordHash, LockoutEnd, AccessFailedCount, CreatedUtc
            FROM Users
            WHERE NormalizedEmail = @email
            """;
        cmd.Parameters.Add(new CSharpDbParameter("@email", normalized));

        await using DbDataReader reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
            return null;

        AppUser user = ReadUser(reader);
        await PopulateRolesAndClaimsAsync(conn, user);
        return user;
    }

    public async Task<AppUser?> FindByIdAsync(string id)
    {
        await using CSharpDbConnection conn = await OpenAsync();

        await using CSharpDbCommand cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT Id, Email, DisplayName, PasswordHash, LockoutEnd, AccessFailedCount, CreatedUtc
            FROM Users
            WHERE Id = @id
            """;
        cmd.Parameters.Add(new CSharpDbParameter("@id", id));

        await using DbDataReader reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
            return null;

        AppUser user = ReadUser(reader);
        await PopulateRolesAndClaimsAsync(conn, user);
        return user;
    }

    public async Task<CreateResult> CreateAsync(string email, string? displayName, string password)
    {
        string normalized = NormalizeEmail(email);

        await using CSharpDbConnection conn = await OpenAsync();
        if (await EmailExistsAsync(conn, normalized))
            return CreateResult.Failed("DuplicateEmail", "Email is already registered.");

        AppUser user = new()
        {
            Email = email,
            DisplayName = displayName,
            PasswordHash = string.Empty,
        };
        user.PasswordHash = _hasher.HashPassword(user, password);

        await using CSharpDbTransaction tx = (CSharpDbTransaction)await conn.BeginTransactionAsync();

        await using (CSharpDbCommand cmd = conn.CreateCommand())
        {
            cmd.Transaction = tx;
            cmd.CommandText = """
                INSERT INTO Users (Id, Email, NormalizedEmail, DisplayName, PasswordHash, LockoutEnd, AccessFailedCount, CreatedUtc)
                VALUES (@id, @email, @normalizedEmail, @displayName, @hash, NULL, 0, @createdUtc)
                """;
            cmd.Parameters.Add(new CSharpDbParameter("@id", user.Id));
            cmd.Parameters.Add(new CSharpDbParameter("@email", user.Email));
            cmd.Parameters.Add(new CSharpDbParameter("@normalizedEmail", normalized));
            cmd.Parameters.Add(new CSharpDbParameter("@displayName", (object?)user.DisplayName ?? DBNull.Value));
            cmd.Parameters.Add(new CSharpDbParameter("@hash", user.PasswordHash));
            cmd.Parameters.Add(new CSharpDbParameter("@createdUtc", user.CreatedUtc.ToString("O")));
            await cmd.ExecuteNonQueryAsync();
        }

        await tx.CommitAsync(default);
        return CreateResult.Ok(user);
    }

    public async Task<VerifyResult> VerifyPasswordAsync(string email, string password)
    {
        AppUser? user = await FindByEmailAsync(email);
        if (user is null)
            return VerifyResult.NotFound;

        if (user.LockoutEnd is { } until && until > DateTimeOffset.UtcNow)
            return VerifyResult.LockedOut(user, until);

        PasswordVerificationResult check = _hasher.VerifyHashedPassword(user, user.PasswordHash, password);
        if (check == PasswordVerificationResult.Failed)
        {
            await RecordFailedAttemptAsync(user.Id);
            return VerifyResult.WrongPassword;
        }

        if (check == PasswordVerificationResult.SuccessRehashNeeded)
        {
            user.PasswordHash = _hasher.HashPassword(user, password);
            await UpdatePasswordHashAsync(user.Id, user.PasswordHash);
        }

        if (user.AccessFailedCount > 0)
            await ResetFailedAttemptsAsync(user.Id);

        return VerifyResult.Ok(user);
    }

    public async Task EnsureRoleAsync(string roleName)
    {
        await using CSharpDbConnection conn = await OpenAsync();

        await using CSharpDbCommand check = conn.CreateCommand();
        check.CommandText = "SELECT 1 FROM Roles WHERE Name = @name";
        check.Parameters.Add(new CSharpDbParameter("@name", roleName));
        object? existing = await check.ExecuteScalarAsync();
        if (existing is not null)
            return;

        await using CSharpDbCommand insert = conn.CreateCommand();
        insert.CommandText = "INSERT INTO Roles (Id, Name) VALUES (@id, @name)";
        insert.Parameters.Add(new CSharpDbParameter("@id", Guid.NewGuid().ToString("N")));
        insert.Parameters.Add(new CSharpDbParameter("@name", roleName));
        await insert.ExecuteNonQueryAsync();
    }

    public async Task AddToRoleAsync(string userId, string roleName)
    {
        await using CSharpDbConnection conn = await OpenAsync();

        await using CSharpDbCommand exists = conn.CreateCommand();
        exists.CommandText = "SELECT 1 FROM UserRoles WHERE UserId = @uid AND RoleName = @role";
        exists.Parameters.Add(new CSharpDbParameter("@uid", userId));
        exists.Parameters.Add(new CSharpDbParameter("@role", roleName));
        if (await exists.ExecuteScalarAsync() is not null)
            return;

        await using CSharpDbCommand insert = conn.CreateCommand();
        insert.CommandText = "INSERT INTO UserRoles (UserId, RoleName) VALUES (@uid, @role)";
        insert.Parameters.Add(new CSharpDbParameter("@uid", userId));
        insert.Parameters.Add(new CSharpDbParameter("@role", roleName));
        await insert.ExecuteNonQueryAsync();
    }

    public async Task AddClaimAsync(string userId, string type, string value)
    {
        await using CSharpDbConnection conn = await OpenAsync();

        await using CSharpDbCommand exists = conn.CreateCommand();
        exists.CommandText = "SELECT 1 FROM UserClaims WHERE UserId = @uid AND ClaimType = @type AND ClaimValue = @value";
        exists.Parameters.Add(new CSharpDbParameter("@uid", userId));
        exists.Parameters.Add(new CSharpDbParameter("@type", type));
        exists.Parameters.Add(new CSharpDbParameter("@value", value));
        if (await exists.ExecuteScalarAsync() is not null)
            return;

        await using CSharpDbCommand insert = conn.CreateCommand();
        insert.CommandText = "INSERT INTO UserClaims (UserId, ClaimType, ClaimValue) VALUES (@uid, @type, @value)";
        insert.Parameters.Add(new CSharpDbParameter("@uid", userId));
        insert.Parameters.Add(new CSharpDbParameter("@type", type));
        insert.Parameters.Add(new CSharpDbParameter("@value", value));
        await insert.ExecuteNonQueryAsync();
    }

    public async Task LockAsync(string userId, DateTimeOffset until)
    {
        await using CSharpDbConnection conn = await OpenAsync();
        await using CSharpDbCommand cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE Users SET LockoutEnd = @until WHERE Id = @id";
        cmd.Parameters.Add(new CSharpDbParameter("@until", until.ToString("O")));
        cmd.Parameters.Add(new CSharpDbParameter("@id", userId));
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task RecordFailedAttemptAsync(string userId)
    {
        await using CSharpDbConnection conn = await OpenAsync();
        await using CSharpDbCommand cmd = conn.CreateCommand();
        cmd.CommandText = """
            UPDATE Users
            SET AccessFailedCount = AccessFailedCount + 1,
                LockoutEnd = CASE WHEN AccessFailedCount + 1 >= 5 THEN @lockout ELSE LockoutEnd END
            WHERE Id = @id
            """;
        cmd.Parameters.Add(new CSharpDbParameter("@lockout", DateTimeOffset.UtcNow.AddMinutes(15).ToString("O")));
        cmd.Parameters.Add(new CSharpDbParameter("@id", userId));
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task ResetFailedAttemptsAsync(string userId)
    {
        await using CSharpDbConnection conn = await OpenAsync();
        await using CSharpDbCommand cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE Users SET AccessFailedCount = 0, LockoutEnd = NULL WHERE Id = @id";
        cmd.Parameters.Add(new CSharpDbParameter("@id", userId));
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task UpdatePasswordHashAsync(string userId, string hash)
    {
        await using CSharpDbConnection conn = await OpenAsync();
        await using CSharpDbCommand cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE Users SET PasswordHash = @hash WHERE Id = @id";
        cmd.Parameters.Add(new CSharpDbParameter("@hash", hash));
        cmd.Parameters.Add(new CSharpDbParameter("@id", userId));
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<bool> EmailExistsAsync(CSharpDbConnection conn, string normalizedEmail)
    {
        await using CSharpDbCommand cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1 FROM Users WHERE NormalizedEmail = @email";
        cmd.Parameters.Add(new CSharpDbParameter("@email", normalizedEmail));
        return await cmd.ExecuteScalarAsync() is not null;
    }

    private static async Task PopulateRolesAndClaimsAsync(CSharpDbConnection conn, AppUser user)
    {
        await using (CSharpDbCommand rolesCmd = conn.CreateCommand())
        {
            rolesCmd.CommandText = "SELECT RoleName FROM UserRoles WHERE UserId = @uid";
            rolesCmd.Parameters.Add(new CSharpDbParameter("@uid", user.Id));
            await using DbDataReader reader = await rolesCmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
                user.Roles.Add(reader.GetString(0));
        }

        await using CSharpDbCommand claimsCmd = conn.CreateCommand();
        claimsCmd.CommandText = "SELECT ClaimType, ClaimValue FROM UserClaims WHERE UserId = @uid";
        claimsCmd.Parameters.Add(new CSharpDbParameter("@uid", user.Id));
        await using DbDataReader claimsReader = await claimsCmd.ExecuteReaderAsync();
        while (await claimsReader.ReadAsync())
            user.Claims.Add(new UserClaim(claimsReader.GetString(0), claimsReader.GetString(1)));
    }

    private static AppUser ReadUser(DbDataReader reader)
    {
        DateTimeOffset? lockoutEnd = reader.IsDBNull(4)
            ? null
            : DateTimeOffset.Parse(reader.GetString(4), null, System.Globalization.DateTimeStyles.RoundtripKind);

        return new AppUser
        {
            Id = reader.GetString(0),
            Email = reader.GetString(1),
            DisplayName = reader.IsDBNull(2) ? null : reader.GetString(2),
            PasswordHash = reader.GetString(3),
            LockoutEnd = lockoutEnd,
            AccessFailedCount = reader.GetInt32(5),
            CreatedUtc = DateTime.Parse(reader.GetString(6), null, System.Globalization.DateTimeStyles.RoundtripKind),
        };
    }

    private async Task<CSharpDbConnection> OpenAsync()
    {
        CSharpDbConnection conn = new(_connectionString);
        await conn.OpenAsync();
        return conn;
    }

    private static string NormalizeEmail(string email) => email.Trim().ToUpperInvariant();

    private static readonly string[] SchemaScript =
    [
        """
        CREATE TABLE IF NOT EXISTS Users (
            Id                TEXT    PRIMARY KEY,
            Email             TEXT    NOT NULL,
            NormalizedEmail   TEXT    NOT NULL,
            DisplayName       TEXT,
            PasswordHash      TEXT    NOT NULL,
            LockoutEnd        TEXT,
            AccessFailedCount INTEGER NOT NULL,
            CreatedUtc        TEXT    NOT NULL
        )
        """,
        "CREATE UNIQUE INDEX IF NOT EXISTS IX_Users_NormalizedEmail ON Users(NormalizedEmail)",
        """
        CREATE TABLE IF NOT EXISTS Roles (
            Id   TEXT PRIMARY KEY,
            Name TEXT NOT NULL
        )
        """,
        "CREATE UNIQUE INDEX IF NOT EXISTS IX_Roles_Name ON Roles(Name)",
        """
        CREATE TABLE IF NOT EXISTS UserRoles (
            Id       INTEGER PRIMARY KEY,
            UserId   TEXT    NOT NULL,
            RoleName TEXT    NOT NULL
        )
        """,
        "CREATE INDEX IF NOT EXISTS IX_UserRoles_UserId ON UserRoles(UserId)",
        """
        CREATE TABLE IF NOT EXISTS UserClaims (
            Id         INTEGER PRIMARY KEY,
            UserId     TEXT    NOT NULL,
            ClaimType  TEXT    NOT NULL,
            ClaimValue TEXT    NOT NULL
        )
        """,
        "CREATE INDEX IF NOT EXISTS IX_UserClaims_UserId ON UserClaims(UserId)",
    ];
}

public sealed record CreateResult(bool Succeeded, AppUser? User, string? ErrorCode, string? ErrorDescription)
{
    public static CreateResult Ok(AppUser user) => new(true, user, null, null);

    public static CreateResult Failed(string code, string description) => new(false, null, code, description);
}

public abstract record VerifyResult
{
    public static readonly VerifyResult NotFound = new NotFoundResult();
    public static readonly VerifyResult WrongPassword = new WrongPasswordResult();

    public static VerifyResult Ok(AppUser user) => new SuccessResult(user);

    public static VerifyResult LockedOut(AppUser user, DateTimeOffset until) => new LockedOutResult(user, until);

    public sealed record SuccessResult(AppUser User) : VerifyResult;
    public sealed record NotFoundResult : VerifyResult;
    public sealed record WrongPasswordResult : VerifyResult;
    public sealed record LockedOutResult(AppUser User, DateTimeOffset Until) : VerifyResult;
}
