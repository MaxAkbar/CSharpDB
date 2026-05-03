namespace AspNetCoreIdentitySample;

public sealed class AppUser
{
    public string Id { get; init; } = Guid.NewGuid().ToString("N");

    public required string Email { get; init; }

    public string? DisplayName { get; init; }

    public required string PasswordHash { get; set; }

    public DateTimeOffset? LockoutEnd { get; set; }

    public int AccessFailedCount { get; set; }

    public DateTime CreatedUtc { get; init; } = DateTime.UtcNow;

    public List<string> Roles { get; init; } = [];

    public List<UserClaim> Claims { get; init; } = [];
}

public sealed record UserClaim(string Type, string Value);
