# CSharpDB Cross-Platform Deployment & Installation Plan

A phased plan to distribute CSharpDB across Windows, macOS, and Linux via multiple installation channels — from `dotnet tool install` to self-contained binaries, Docker, Homebrew, and winget.

---

## Design Decisions

- **Unified CLI + separate tools** — A single `csharpdb` binary with subcommands (`serve`, `inspect`, `version`) is the primary distribution. The MCP server is also available as a separate `csharpdb-mcp` dotnet tool.
- **Both audiences from day one** — Phase 1 ships both `dotnet tool install` (for .NET developers) and self-contained binaries (for anyone, no .NET required).
- **No mobile** — Focus on Windows, macOS, Linux, and Docker. A database server/CLI does not translate to mobile.

---

## Install Experience by Platform

| Platform | dotnet tool | Self-contained | Package manager | Docker |
|----------|------------|----------------|-----------------|--------|
| **Windows** | `dotnet tool install -g CSharpDB.Cli` | `irm .../install.ps1 \| iex` | `winget install CSharpDB` | `docker run csharpdb/server` |
| **macOS** | `dotnet tool install -g CSharpDB.Cli` | `curl .../install.sh \| sh` | `brew install csharpdb` | `docker run csharpdb/server` |
| **Linux** | `dotnet tool install -g CSharpDB.Cli` | `curl .../install.sh \| sh` | `brew install csharpdb` | `docker run csharpdb/server` |
| **NuGet** | `dotnet add package CSharpDB.Engine` | — | — | — |

---

## Phase 1: Packaging Foundation

Ships both dotnet tool and self-contained binaries via GitHub Actions CI/CD.

### 1A. Make CLI a dotnet global tool

**File:** `src/CSharpDB.Cli/CSharpDB.Cli.csproj`

Change `IsPackable` from `false` to `true` and add tool properties:

```xml
<IsPackable>true</IsPackable>
<PackAsTool>true</PackAsTool>
<ToolCommandName>csharpdb</ToolCommandName>
<PackageId>CSharpDB.Cli</PackageId>
<Description>Interactive SQL shell and diagnostics CLI for CSharpDB.</Description>
```

Users install with:
```bash
dotnet tool install -g CSharpDB.Cli
csharpdb mydata.db
```

### 1B. Make MCP server a dotnet tool

**File:** `src/CSharpDB.Mcp/CSharpDB.Mcp.csproj`

```xml
<IsPackable>true</IsPackable>
<PackAsTool>true</PackAsTool>
<ToolCommandName>csharpdb-mcp</ToolCommandName>
<PackageId>CSharpDB.Mcp</PackageId>
<Description>MCP server for CSharpDB. Connect AI assistants to your database.</Description>
```

Users install with:
```bash
dotnet tool install -g CSharpDB.Mcp
csharpdb-mcp --database mydata.db
```

### 1C. Fix NuGet package metadata

**File:** `src/Directory.Build.props`

Add `PackageReadmeFile` so all library packages include the repo README:

```xml
<PackageReadmeFile>README.md</PackageReadmeFile>
```

Add ItemGroup to include the README in packages:

```xml
<ItemGroup>
  <None Include="$(MSBuildThisFileDirectory)../README.md" Pack="true" PackagePath="/" />
</ItemGroup>
```

### 1D. Add `version` subcommand to CLI

**File:** `src/CSharpDB.Cli/Program.cs`

Handle `version`, `--version`, `-v` args that print the assembly version (derived from `Directory.Build.props` `<Version>`).

### 1E. GitHub Actions CI workflow

**File:** `.github/workflows/ci.yml` (CREATE)

| Trigger | Job | Details |
|---------|-----|---------|
| Push to `main`, PRs to `main` | `build-and-test` | Matrix: ubuntu, windows, macos; restore, build Release, test |
| Push to `main` only | `pack` | `dotnet pack -c Release`, upload `.nupkg` artifacts |

### 1F. GitHub Actions Release workflow

**File:** `.github/workflows/release.yml` (CREATE)

Triggered on tag push (`v*`).

| Job | Details |
|-----|---------|
| `build-and-test` | Gate: matrix across 3 OS runners |
| `publish-nuget` | `dotnet pack` then `dotnet nuget push` to NuGet.org |
| `publish-binaries` | Matrix of 7 RIDs (see below), publish CLI + API + MCP as self-contained single-file trimmed binaries |
| `create-release` | Download all artifacts, create GitHub Release with generated notes |

**Target Runtime Identifiers:**

| RID | Runner | Binary |
|-----|--------|--------|
| `win-x64` | windows-latest | `.zip` |
| `win-arm64` | windows-latest | `.zip` |
| `linux-x64` | ubuntu-latest | `.tar.gz` |
| `linux-arm64` | ubuntu-latest | `.tar.gz` |
| `linux-musl-x64` | ubuntu-latest | `.tar.gz` |
| `osx-x64` | macos-latest | `.tar.gz` |
| `osx-arm64` | macos-latest | `.tar.gz` |

Publish command per binary:
```bash
dotnet publish -c Release -r <RID> \
  --self-contained true \
  -p:PublishSingleFile=true \
  -p:PublishTrimmed=true \
  -p:IncludeNativeLibrariesForSelfExtract=true
```

### 1G. .dockerignore

**File:** `.dockerignore` (CREATE)

Exclude build artifacts, tests, docs, database files.

---

## Phase 2: Unified CLI with `serve` Subcommand

Extends the CLI to host the API server + Admin dashboard as a subcommand, making `csharpdb` the single entry point for everything.

### 2A. New CLI command structure

```
csharpdb [database.db]                          # REPL mode (existing)
csharpdb serve [--database] [--port] [--host]   # API server + Admin
csharpdb inspect <dbfile>                       # Existing inspector
csharpdb inspect-page <dbfile> <pageId>         # Existing inspector
csharpdb check-wal <dbfile>                     # Existing inspector
csharpdb check-indexes <dbfile>                 # Existing inspector
csharpdb version                                # Version info
```

### 2B. Project changes

**File:** `src/CSharpDB.Cli/CSharpDB.Cli.csproj`

Add ASP.NET Core framework reference and Service project reference:

```xml
<FrameworkReference Include="Microsoft.AspNetCore.App" />
<ProjectReference Include="..\CSharpDB.Service\CSharpDB.Service.csproj" />
```

### 2C. Extract API endpoint registration

**File:** `src/CSharpDB.Api/EndpointRegistration.cs` (CREATE)

Extract endpoint mapping from `Program.cs` into a static extension method `app.MapCSharpDbEndpoints()` that both the standalone API project and the CLI `serve` command can call.

### 2D. Serve command implementation

**File:** `src/CSharpDB.Cli/Commands/ServeCommand.cs` (CREATE)

Builds a `WebApplication`, registers CSharpDB service, maps API endpoints via `MapCSharpDbEndpoints()`, starts Kestrel. Accepts `--database`, `--port`, `--host` arguments.

---

## Phase 3: Docker + Install Scripts

### 3A. Dockerfile

**File:** `Dockerfile` (CREATE)

Multi-stage build:

```
Build stage:  mcr.microsoft.com/dotnet/sdk:10.0-preview
              Copy csproj files first (layer caching), restore, build, publish API

Runtime stage: mcr.microsoft.com/dotnet/aspnet:10.0-preview
               Copy published output, expose 61818, volume /data
```

Usage:
```bash
docker build -t csharpdb/server .
docker run -p 61818:61818 -v ./data:/data csharpdb/server
```

### 3B. Docker Hub publishing

Add Docker build/push job to `.github/workflows/release.yml`:
- Multi-arch: `linux/amd64` + `linux/arm64`
- Tags: `latest` + version tag
- Requires `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` secrets

### 3C. Install script (Unix)

**File:** `install.sh` (CREATE)

POSIX shell script:

1. Detect OS (`uname -s`) and architecture (`uname -m`)
2. Map to RID: `linux-x64`, `linux-arm64`, `linux-musl-x64`, `osx-x64`, `osx-arm64`
3. Fetch latest release tag from GitHub API (or honor `CSHARPDB_VERSION` env var)
4. Download `cli-<rid>-<version>.tar.gz` from GitHub Releases
5. Extract to `~/.csharpdb/bin/` (or `CSHARPDB_INSTALL_DIR`)
6. Print PATH instructions

Usage:
```bash
curl -sSL https://raw.githubusercontent.com/MaxAkbar/CSharpDB/main/install.sh | sh
```

### 3D. Install script (Windows)

**File:** `install.ps1` (CREATE)

PowerShell script:

1. Detect architecture (x64 vs ARM64)
2. Download `cli-win-<arch>-<version>.zip` from GitHub Releases
3. Extract to `%USERPROFILE%\.csharpdb\bin\`
4. Add to user PATH

Usage:
```powershell
irm https://raw.githubusercontent.com/MaxAkbar/CSharpDB/main/install.ps1 | iex
```

---

## Phase 4: Platform Package Managers

### 4A. Homebrew tap

**Separate repo:** `MaxAkbar/homebrew-csharpdb`

**File:** `Formula/csharpdb.rb`

Ruby formula with platform-specific blocks (`on_macos`/`on_linux`, `on_arm`/`on_intel`). Downloads self-contained binary from GitHub Releases.

Usage:
```bash
brew tap MaxAkbar/csharpdb
brew install csharpdb
```

The release workflow computes SHA256 hashes and opens a PR against the tap repo to update the formula automatically.

### 4B. winget manifest

Submit to `microsoft/winget-pkgs`:

```
manifests/m/MaxAkbar/CSharpDB/<version>/
  MaxAkbar.CSharpDB.yaml               # Version
  MaxAkbar.CSharpDB.locale.en-US.yaml  # Metadata
  MaxAkbar.CSharpDB.installer.yaml     # x64 + arm64 portable zip
```

Usage:
```
winget install MaxAkbar.CSharpDB
```

---

## Files to Create/Modify

| File | Action | Phase |
|------|--------|-------|
| `src/CSharpDB.Cli/CSharpDB.Cli.csproj` | EDIT | 1, 2 |
| `src/CSharpDB.Mcp/CSharpDB.Mcp.csproj` | EDIT | 1 |
| `src/Directory.Build.props` | EDIT | 1 |
| `src/CSharpDB.Cli/Program.cs` | EDIT | 1, 2 |
| `src/CSharpDB.Cli/Commands/ServeCommand.cs` | CREATE | 2 |
| `src/CSharpDB.Api/EndpointRegistration.cs` | CREATE | 2 |
| `src/CSharpDB.Api/Program.cs` | EDIT | 2 |
| `.github/workflows/ci.yml` | CREATE | 1 |
| `.github/workflows/release.yml` | CREATE | 1, 3 |
| `.dockerignore` | CREATE | 1 |
| `Dockerfile` | CREATE | 3 |
| `install.sh` | CREATE | 3 |
| `install.ps1` | CREATE | 3 |

---

## Verification Checklist

### Phase 1
- [ ] `dotnet pack src/CSharpDB.Cli -c Release` produces `.nupkg` with tool metadata
- [ ] `dotnet tool install -g --add-source ./artifacts CSharpDB.Cli` installs `csharpdb` command
- [ ] `csharpdb version` prints version
- [ ] `dotnet tool install -g --add-source ./artifacts CSharpDB.Mcp` installs `csharpdb-mcp` command
- [ ] `dotnet publish src/CSharpDB.Cli -r win-x64 --self-contained -p:PublishSingleFile=true` produces single EXE
- [ ] CI workflow passes on all 3 OS runners

### Phase 2
- [ ] `csharpdb serve --database test.db --port 61818` starts API server
- [ ] `curl http://localhost:61818/api/info` returns database info
- [ ] Standalone `CSharpDB.Api` project still works independently

### Phase 3
- [ ] `docker build -t csharpdb/server .` succeeds
- [ ] `docker run -p 61818:61818 csharpdb/server` serves API
- [ ] `install.sh` downloads and installs correct binary on Linux/macOS
- [ ] `install.ps1` downloads and installs correct binary on Windows

### Phase 4
- [ ] `brew tap MaxAkbar/csharpdb && brew install csharpdb` installs on macOS
- [ ] `winget install MaxAkbar.CSharpDB` installs on Windows

---

## See Also

- [Roadmap](../roadmap.md) — Project-wide feature roadmap
- [CLI Reference](../cli.md) — CLI meta-commands and usage
- [REST API](../rest-api.md) — API endpoint reference
- [MCP Server](../mcp-server.md) — MCP server setup
