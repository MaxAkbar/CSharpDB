using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;
using AspNetCoreIdentitySample;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Identity;
using Microsoft.IdentityModel.Tokens;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

string databasePath = SamplePaths.GetDatabasePath(args);
string? databaseDirectory = Path.GetDirectoryName(databasePath);
if (!string.IsNullOrWhiteSpace(databaseDirectory))
    Directory.CreateDirectory(databaseDirectory);

string connectionString = $"Data Source={Path.GetFullPath(databasePath)}";

builder.Services.AddSingleton<IPasswordHasher<AppUser>, PasswordHasher<AppUser>>();
builder.Services.AddSingleton(sp => new UserStore(connectionString, sp.GetRequiredService<IPasswordHasher<AppUser>>()));

const string CookieScheme = CookieAuthenticationDefaults.AuthenticationScheme;
const string JwtScheme = JwtBearerDefaults.AuthenticationScheme;

string jwtKey = builder.Configuration["Jwt:Key"]
    ?? throw new InvalidOperationException("Missing Jwt:Key configuration value.");
string jwtIssuer = builder.Configuration["Jwt:Issuer"]
    ?? throw new InvalidOperationException("Missing Jwt:Issuer configuration value.");

builder.Services
    .AddAuthentication(opt =>
    {
        opt.DefaultScheme = CookieScheme;
        opt.DefaultAuthenticateScheme = CookieScheme;
        opt.DefaultChallengeScheme = CookieScheme;
    })
    .AddCookie(CookieScheme, opt =>
    {
        opt.Cookie.Name = "csharpdb.identity";
        opt.SlidingExpiration = true;
        opt.ExpireTimeSpan = TimeSpan.FromHours(1);
        opt.Events.OnRedirectToLogin = ctx =>
        {
            ctx.Response.StatusCode = StatusCodes.Status401Unauthorized;
            return Task.CompletedTask;
        };
        opt.Events.OnRedirectToAccessDenied = ctx =>
        {
            ctx.Response.StatusCode = StatusCodes.Status403Forbidden;
            return Task.CompletedTask;
        };
    })
    .AddJwtBearer(JwtScheme, opt =>
    {
        opt.TokenValidationParameters = new TokenValidationParameters
        {
            ValidateIssuer = true,
            ValidateAudience = true,
            ValidateLifetime = true,
            ValidateIssuerSigningKey = true,
            ValidIssuer = jwtIssuer,
            ValidAudience = jwtIssuer,
            IssuerSigningKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(jwtKey)),
            ClockSkew = TimeSpan.FromSeconds(30),
        };
    });

builder.Services.AddAuthorization(opt =>
{
    opt.AddPolicy("AdminOnly", p => p
        .AddAuthenticationSchemes(CookieScheme, JwtScheme)
        .RequireAuthenticatedUser()
        .RequireRole("Admin"));

    opt.AddPolicy("CanManageUsers", p => p
        .AddAuthenticationSchemes(CookieScheme, JwtScheme)
        .RequireAuthenticatedUser()
        .RequireClaim("perm", "users.manage"));
});

WebApplication app = builder.Build();

await using (AsyncServiceScope scope = app.Services.CreateAsyncScope())
{
    UserStore store = scope.ServiceProvider.GetRequiredService<UserStore>();
    await store.EnsureSchemaAsync();
    await Seed.EnsureRolesAndAdminAsync(store);
}

app.UseAuthentication();
app.UseAuthorization();

// ----- Public endpoints -----

app.MapGet("/", () => Results.Ok(new
{
    message = "CSharpDB ASP.NET Core Identity Sample",
    seededAdmin = new { email = Seed.AdminEmail, password = Seed.AdminPassword },
    endpoints = new[]
    {
        "POST /auth/register",
        "POST /auth/login   (cookie)",
        "POST /auth/logout  (cookie)",
        "POST /auth/token   (JWT)",
        "GET  /me/cookie",
        "GET  /me/jwt",
        "GET  /admin/ping             (role: Admin)",
        "POST /admin/users/{id}/lock  (policy: CanManageUsers)",
    },
}));

app.MapPost("/auth/register", async (RegisterRequest dto, UserStore store) =>
{
    CreateResult result = await store.CreateAsync(dto.Email, dto.DisplayName, dto.Password);
    if (!result.Succeeded || result.User is null)
    {
        return Results.ValidationProblem(new Dictionary<string, string[]>
        {
            [result.ErrorCode ?? "Error"] = [result.ErrorDescription ?? "Failed to create user."],
        });
    }

    return Results.Created($"/users/{result.User.Id}", new
    {
        id = result.User.Id,
        email = result.User.Email,
        displayName = result.User.DisplayName,
    });
});

app.MapPost("/auth/login", async (LoginRequest dto, UserStore store, HttpContext http) =>
{
    VerifyResult verify = await store.VerifyPasswordAsync(dto.Email, dto.Password);
    return verify switch
    {
        VerifyResult.SuccessResult success => await SignInWithCookieAsync(http, success.User, dto.RememberMe),
        VerifyResult.LockedOutResult locked => Results.Problem(
            $"Account is locked until {locked.Until:O}.",
            statusCode: StatusCodes.Status423Locked),
        _ => Results.Unauthorized(),
    };
});

app.MapPost("/auth/logout", async (HttpContext http) =>
{
    await http.SignOutAsync(CookieScheme);
    return Results.Ok(new { signedOut = true });
}).RequireAuthorization(new AuthorizeAttribute { AuthenticationSchemes = CookieScheme });

app.MapPost("/auth/token", async (LoginRequest dto, UserStore store, IConfiguration config) =>
{
    VerifyResult verify = await store.VerifyPasswordAsync(dto.Email, dto.Password);
    return verify switch
    {
        VerifyResult.SuccessResult success => Results.Ok(IssueJwt(success.User, config)),
        VerifyResult.LockedOutResult locked => Results.Problem(
            $"Account is locked until {locked.Until:O}.",
            statusCode: StatusCodes.Status423Locked),
        _ => Results.Unauthorized(),
    };
});

// ----- Protected endpoints -----

static object Whoami(ClaimsPrincipal user) => new
{
    name = user.Identity?.Name,
    authenticationType = user.Identity?.AuthenticationType,
    roles = user.FindAll(ClaimTypes.Role).Select(c => c.Value),
    claims = user.Claims.Select(c => new { type = c.Type, value = c.Value }),
};

app.MapGet("/me/cookie", (ClaimsPrincipal user) => Results.Ok(Whoami(user)))
    .RequireAuthorization(new AuthorizeAttribute { AuthenticationSchemes = CookieScheme });

app.MapGet("/me/jwt", (ClaimsPrincipal user) => Results.Ok(Whoami(user)))
    .RequireAuthorization(new AuthorizeAttribute { AuthenticationSchemes = JwtScheme });

app.MapGet("/admin/ping", () => Results.Ok(new { ok = true, area = "admin" }))
    .RequireAuthorization("AdminOnly");

app.MapPost("/admin/users/{id}/lock", async (string id, UserStore store) =>
{
    AppUser? target = await store.FindByIdAsync(id);
    if (target is null)
        return Results.NotFound();

    DateTimeOffset until = DateTimeOffset.UtcNow.AddHours(1);
    await store.LockAsync(target.Id, until);
    return Results.Ok(new { id = target.Id, lockedUntil = until });
}).RequireAuthorization("CanManageUsers");

Console.WriteLine();
Console.WriteLine("CSharpDB ASP.NET Core Identity Sample");
Console.WriteLine($"  Database:     {Path.GetFullPath(databasePath)}");
Console.WriteLine($"  Seeded admin: {Seed.AdminEmail} / {Seed.AdminPassword}");
Console.WriteLine("  Try:          GET / for the endpoint list");
Console.WriteLine();

app.Run();

static async Task<IResult> SignInWithCookieAsync(HttpContext http, AppUser user, bool rememberMe)
{
    ClaimsIdentity identity = new(BuildClaims(user), CookieScheme);
    ClaimsPrincipal principal = new(identity);
    AuthenticationProperties properties = new()
    {
        IsPersistent = rememberMe,
        ExpiresUtc = rememberMe ? DateTimeOffset.UtcNow.AddDays(14) : null,
    };
    await http.SignInAsync(CookieScheme, principal, properties);
    return Results.Ok(new { signedIn = true, userId = user.Id });
}

static object IssueJwt(AppUser user, IConfiguration config)
{
    SymmetricSecurityKey key = new(Encoding.UTF8.GetBytes(config["Jwt:Key"]!));
    JwtSecurityToken token = new(
        issuer: config["Jwt:Issuer"],
        audience: config["Jwt:Issuer"],
        claims: BuildClaims(user),
        notBefore: DateTime.UtcNow,
        expires: DateTime.UtcNow.AddMinutes(15),
        signingCredentials: new SigningCredentials(key, SecurityAlgorithms.HmacSha256));

    return new
    {
        accessToken = new JwtSecurityTokenHandler().WriteToken(token),
        tokenType = "Bearer",
        expiresAt = token.ValidTo,
    };
}

static IEnumerable<Claim> BuildClaims(AppUser user)
{
    yield return new Claim(JwtRegisteredClaimNames.Sub, user.Id);
    yield return new Claim(ClaimTypes.NameIdentifier, user.Id);
    yield return new Claim(ClaimTypes.Name, user.Email);
    yield return new Claim(JwtRegisteredClaimNames.Email, user.Email);
    yield return new Claim(JwtRegisteredClaimNames.Jti, Guid.NewGuid().ToString("N"));

    foreach (string role in user.Roles)
        yield return new Claim(ClaimTypes.Role, role);

    foreach (UserClaim claim in user.Claims)
        yield return new Claim(claim.Type, claim.Value);
}

internal sealed record RegisterRequest(string Email, string DisplayName, string Password);

internal sealed record LoginRequest(string Email, string Password, bool RememberMe = false);
