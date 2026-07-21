# ASP.NET Core Authentication & Authorization Sample

This sample shows ASP.NET Core 10 authentication and authorization wired to a single-file CSharpDB database. It covers:

- A custom user/role/claim store backed by the `CSharpDB.Data` ADO.NET provider
- Password hashing with `PasswordHasher<AppUser>` from `Microsoft.AspNetCore.Identity`
- Cookie authentication for browser flows
- JWT bearer authentication for API/SPA flows
- Role-based authorization (`Admin`)
- Claim/policy-based authorization (`CanManageUsers`)
- Lockout on repeated failed sign-ins
- An idempotent seed for default roles and a first admin

Everything &mdash; users, roles, role assignments, claims, lockout state &mdash; lives in a single `.db` file beside the binaries.

## Why Does This Sample Use a Custom Store?

Provider integration tests cover a bounded EF-backed Identity configuration:
Identity schema v1 with integer user and role keys. This sample
intentionally remains a small custom `CSharpDB.Data` store because it
demonstrates the broader cookie, JWT, role, policy, and lockout pipeline
independently of that EF configuration. The default string-key
`IdentityDbContext<TUser>`, schema versions 2 and 3, and passkeys remain
unsupported.

This sample takes the runs-today path: a small custom user store over `CSharpDB.Data`. The auth and authorization pipeline is the standard ASP.NET Core surface (`AddAuthentication`, `AddCookie`, `AddJwtBearer`, `AddAuthorization`) &mdash; only the user store is custom, and it is small enough to read in one sitting ([UserStore.cs](UserStore.cs)).

## Run The Sample

```bash
dotnet run --project samples/aspnet-core-identity/AspNetCoreIdentitySample.csproj
```

On first start the app creates `sample.db`, seeds the `Admin`, `Editor`, and `Viewer` roles, and creates a seeded admin:

```text
admin@example.com / ChangeMe!2026
```

The console banner prints the database path and seed credentials. The default URL is `http://localhost:5290` (see `Properties/launchSettings.json`).

To pick a different database file:

```bash
dotnet run --project samples/aspnet-core-identity/AspNetCoreIdentitySample.csproj -- --database-path artifacts/aspnet-core-identity.db
```

## Try the Endpoints

Open `sample.http` in VS Code (with the REST Client extension) or Visual Studio and run the requests in order. The flow demonstrates both cookie and JWT auth:

1. `POST /auth/login` &mdash; signs in with cookies
2. `GET /me/cookie` &mdash; reads the cookie identity
3. `POST /auth/token` &mdash; exchanges credentials for a JWT
4. `GET /me/jwt` &mdash; reads the JWT identity
5. `GET /admin/ping` &mdash; role-based authorization (`Admin`)
6. `POST /auth/register` &mdash; creates a new user
7. `POST /admin/users/{id}/lock` &mdash; policy-based authorization (`CanManageUsers`)

Or use `curl` directly:

```bash
# Cookie login (saves cookie jar)
curl -c cookies.txt -X POST http://localhost:5290/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"admin@example.com","password":"ChangeMe!2026"}'

# Whoami via cookie
curl -b cookies.txt http://localhost:5290/me/cookie

# Get a JWT
TOKEN=$(curl -s -X POST http://localhost:5290/auth/token \
  -H "Content-Type: application/json" \
  -d '{"email":"admin@example.com","password":"ChangeMe!2026"}' \
  | jq -r .accessToken)

# Whoami via JWT
curl -H "Authorization: Bearer $TOKEN" http://localhost:5290/me/jwt

# Admin-only check (role)
curl -H "Authorization: Bearer $TOKEN" http://localhost:5290/admin/ping
```

## Inspect the Database

The whole identity store lives in `sample.db`. Open it with the CSharpDB CLI to look around:

```bash
dotnet run --project src/CSharpDB.Cli -- samples/aspnet-core-identity/sample.db

csdb> .tables
csdb> SELECT Id, Email, DisplayName, AccessFailedCount, LockoutEnd FROM Users;
csdb> SELECT * FROM UserRoles;
csdb> SELECT * FROM UserClaims;
```

You can also point CSharpDB Admin at the same file for a UI-driven view of users, roles, and claims.

## What to Look At in the Code

| File | Purpose |
|------|---------|
| [Program.cs](Program.cs) | Host wiring: cookie + JWT schemes, authorization policies, minimal-API endpoints, manual `SignInAsync` and JWT issuance |
| [UserStore.cs](UserStore.cs) | ADO.NET-backed user/role/claim store, schema bootstrap, password hashing, lockout, transactional create |
| [AppUser.cs](AppUser.cs) | The user record plus `UserClaim` |
| [Seed.cs](Seed.cs) | Idempotent role + first-admin seed |
| [appsettings.json](appsettings.json) | Dev-only `Jwt:Key` and `Jwt:Issuer` &mdash; replace before any non-local use |

## Production Notes

- **Rotate the JWT key.** The `Jwt:Key` value in `appsettings.json` is for local development only. Move it to user-secrets or environment variables before deploying.
- **Persist Data Protection keys.** By default the auth cookie key ring lives in a profile directory and resets on container redeploys. For a single-node deployment, persist it back into CSharpDB with a custom `IXmlRepository`.
- **Storage tuning.** For login-heavy workloads, configure the ADO.NET connection string with `EmbeddedOpenMode=HybridIncrementalDurable` or use `DatabaseOptions.ConfigureStorageEngine(b => b.UseWriteOptimizedPreset())` directly to speed up lockout-counter and token writes.

## See Also

- [EF Core Provider sample](../efcore-provider/README.md)
- [EF Core Provider guide](https://csharpdb.com/docs/entity-framework-core.html)
