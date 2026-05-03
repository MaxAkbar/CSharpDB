namespace AspNetCoreIdentitySample;

internal static class Seed
{
    public const string AdminEmail = "admin@example.com";

    public const string AdminPassword = "ChangeMe!2026";

    private static readonly string[] DefaultRoles = ["Admin", "Editor", "Viewer"];

    public static async Task EnsureRolesAndAdminAsync(UserStore users)
    {
        foreach (string role in DefaultRoles)
            await users.EnsureRoleAsync(role);

        AppUser? admin = await users.FindByEmailAsync(AdminEmail);
        if (admin is not null)
            return;

        CreateResult create = await users.CreateAsync(AdminEmail, "Local Admin", AdminPassword);
        if (!create.Succeeded || create.User is null)
            throw new InvalidOperationException($"Seed failed: {create.ErrorDescription ?? "unknown error"}");

        await users.AddToRoleAsync(create.User.Id, "Admin");
        await users.AddClaimAsync(create.User.Id, "perm", "users.manage");
    }
}
