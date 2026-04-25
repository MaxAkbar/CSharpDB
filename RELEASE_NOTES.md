# What's New

## v3.4.0

v3.4.0 focuses on daemon packaging, cross-platform deployment, and remote host
consolidation without changing SQL, storage, WAL, query planning, or the gRPC
client contract.

### Remote Host Consolidation

- `CSharpDB.Daemon` now hosts both the existing REST `/api` surface and gRPC
  from one long-running process.
- REST and gRPC requests share the same warm daemon-hosted database client, so
  remote users no longer need separate REST and gRPC host processes for the
  same database.
- Plain `http://` daemon endpoints support `Transport = Grpc` through
  gRPC-Web compatibility so gRPC and REST can share the default service URL.
  HTTPS and custom test clients can continue using native gRPC.
- REST is enabled in the daemon by default and can be disabled with
  `CSharpDB__Daemon__EnableRestApi=false`.
- The standalone `CSharpDB.Api` REST host remains supported for REST-only
  deployments.
- Release archive smoke validation now calls the daemon REST `/api/info`
  endpoint and a gRPC `GetInfoAsync` client after extracting and starting each
  daemon binary.

### Admin Warm Local Database Hosting

- `CSharpDB.Admin` direct local mode keeps a warm in-process database instance
  and now opens it through hybrid incremental-durable database options by
  default.
- Admin startup and database switching both use the same host database option
  builder, so opening a different database from the UI keeps the same warm
  local database behavior.
- Admin remote mode still uses `CSharpDB:Transport` plus `CSharpDB:Endpoint`
  without attaching local direct/hybrid options.
- Set `CSharpDB__HostDatabase__OpenMode=Direct` to opt back into the older
  plain direct open path for local Admin runs.
- Admin table data filters now support contains, starts-with, and ends-with
  `LIKE` placement plus exact `=` match mode per column.
- The Admin SQL query editor now has homegrown guided completions for SQL
  keywords, table/view selection, select-list columns, qualified alias columns,
  and stored procedure names without adding a third-party editor dependency.

### Daemon Service Packaging

- Added Windows Service and systemd host integration to `CSharpDB.Daemon`.
  These hooks are no-ops for normal console and `dotnet run` execution.
- Added Windows service install/uninstall scripts with defaults for
  `CSharpDBDaemon`, `C:\Program Files\CSharpDB\Daemon`,
  `C:\ProgramData\CSharpDB`, and `http://127.0.0.1:5820`.
- Added Linux systemd service template plus install/uninstall scripts with
  defaults for `/opt/csharpdb-daemon`, `/var/lib/csharpdb`, service user
  `csharpdb`, and `http://127.0.0.1:5820`.
- Added macOS launchd plist template plus install/uninstall scripts with
  defaults for `/usr/local/lib/csharpdb-daemon`,
  `/usr/local/var/csharpdb`, and `http://127.0.0.1:5820`.

### Release Archives

- Added `scripts/Publish-CSharpDbDaemonRelease.ps1` for self-contained,
  single-file, non-trimmed daemon release archives.
- Added release archive coverage for `win-x64`, `linux-x64`, and `osx-arm64`.
- Added checksum generation through `SHA256SUMS.txt`.
- Updated the GitHub Release workflow to build daemon archives on native
  Windows, Linux, and macOS runners, verify each archive, smoke-start the
  extracted daemon, and attach the archives plus combined checksums to the
  GitHub Release.

### Docs

- Updated the daemon README with archive installation, service installation,
  upgrade, uninstall, and configuration override guidance.
- Updated the Admin README with warm in-process local database behavior, hybrid
  local hosting defaults, and the direct open-mode opt-out.
- Updated the scripts README with daemon packaging and service installer
  references.
- Updated the roadmap to mark daemon service packaging done and scoped
  cross-platform daemon archive deployment in progress.
- Added a new blog post covering the C# launcher pattern for
  `CSharpDB.Admin`, including syntax-highlighted examples for the launcher
  executable flow.
- Migrated the remaining source-heavy markdown docs into companion reference
  pages under `www` for architecture, getting started, performance, SQL query
  execution pipeline, SQL reference, storage engine, roadmap, and the
  CSharpDB-versus-SQLite benchmarking article so the full original content now
  stays published on the website.
- Updated the curated docs/blog pages and sitemap to point at the new source
  reference routes when users need the full original long-form content.
- Removed the duplicated markdown copies of the CLI, REST API, MCP server,
  internals, and storage inspector guides after their website versions were
  audited and verified.

### Validation

- PowerShell parser validation passed for the daemon release publisher and
  Windows install/uninstall scripts.
- `bash -n` passed for Linux and macOS service install/uninstall scripts.
- `dotnet restore CSharpDB.slnx` completed successfully.
- `dotnet build CSharpDB.slnx -c Release` completed successfully.
- `dotnet build CSharpDB.slnx -c Release --no-restore` completed successfully.
- `dotnet test tests\CSharpDB.Api.Tests\CSharpDB.Api.Tests.csproj -c Release --no-build`
  passed with `15` tests.
- `dotnet test tests\CSharpDB.Daemon.Tests\CSharpDB.Daemon.Tests.csproj -c Release --no-build`
  passed with `18` tests.
- `dotnet test tests\CSharpDB.Admin.Forms.Tests\CSharpDB.Admin.Forms.Tests.csproj -c Release --no-build`
  passed with `211` tests.
- Content audit confirmed the new source-reference pages preserve the migrated
  markdown coverage (100% heading coverage and 99.7-100% token overlap across
  the migrated set).
- `python -c "import xml.etree.ElementTree as ET; ET.parse('www/sitemap.xml')"`
  passed for the updated site map.
- Repo scan found no remaining references to the deleted duplicated markdown
  docs.
- `.\scripts\Publish-CSharpDbDaemonRelease.ps1 -Version 3.4.0 -Runtime win-x64 -OutputRoot artifacts\daemon-release-local`
  created `csharpdb-daemon-v3.4.0-win-x64.zip` and `SHA256SUMS.txt`.
- The extracted `win-x64` daemon archive smoke-started successfully, served
  `/api/info`, and accepted a gRPC `GetInfoAsync` client call on the same base
  URL with a temporary database.
- `dotnet run --project src\CSharpDB.Api\CSharpDB.Api.csproj --configuration Release --no-build --no-launch-profile`
  smoke-started successfully and served `/api/info` with a temporary database.
- `dotnet run --project src\CSharpDB.Daemon\CSharpDB.Daemon.csproj --configuration Release --no-build --no-launch-profile`
  smoke-started successfully, served `/api/info`, and accepted a gRPC
  `GetInfoAsync` client call on the same base URL with a temporary database.
- `dotnet run --project src\CSharpDB.Admin\CSharpDB.Admin.csproj --configuration Release --no-build --no-launch-profile`
  smoke-started successfully in direct hybrid incremental-durable mode with a
  temporary database.
