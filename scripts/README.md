# Scripts

These scripts are developer, operator, and release helpers for local source
runs, repository maintenance, and CSharpDB daemon release packaging.

The local start scripts launch `dotnet run` processes from the repo and update
the admin app config so the web UI starts in the expected transport mode.
Daemon service install scripts live under [`deploy/daemon`](../deploy/daemon)
and are included in daemon release archives.

## How The Scripts Fit The Release Cycle

The release and local workflow notes are grouped by audience:

- Release maintainers use `scripts/Publish-ReleaseTag.ps1` to qualify the exact
  merged `main` commit and publish its release tag. They can use
  `scripts/Publish-CSharpDbDaemonRelease.ps1` directly for local packaging
  checks. The GitHub Release workflow also uses the daemon publisher after the
  guarded `v*` tag is pushed.
- Store release maintainers use
  `scripts/Publish-CSharpDbAdminStorePackage.ps1` on Windows to produce the
  CSharpDB Studio MSIX and `.msixupload` artifacts for Partner Center.
- Mac/Linux Studio direct-download packaging remains planned future work. It
  uses a native launcher plus the user's default browser instead of an embedded
  WebView shell.
- Operators use the service scripts after a release is published. These scripts
  are included inside each daemon archive under `service/`.
- Developers use `Start-CSharpDbAdminAndDaemon.ps1` and
  `Start-CSharpDbAdminDirect.ps1` for local source runs. Developers can also
  use `Start-CSharpDbAdminFormsWeb.ps1` when they only need the runtime form
  host. These are not release packaging scripts.

## Release Maintainer Walkthrough

Use this path before tagging or when validating release packaging locally.

1. Prepare the release state before creating a tag.

- Update `src/Directory.Build.props` to the release version.
- Update the public documentation and release notes.
- Validate EF Core package alignment and public documentation.

```powershell
.\scripts\Test-EfCoreVersionConsistency.ps1
.\scripts\Test-Documentation.ps1
```

Commit implementation, version, documentation, roadmap, and release-note
changes together. The release tag is the immutable record of that source state.

2. Run the complete local release qualification from a clean checkout.

```powershell
$QualificationOutput = Join-Path `
  ([IO.Path]::GetTempPath()) `
  "csharpdb-sql-release-$([Guid]::NewGuid().ToString('N'))"

.\scripts\Test-SqlReleaseQualification.ps1 `
  -OutputPath $QualificationOutput `
  -Configuration Release
```

The script validates documentation and package boundaries, restores and builds
the solution, runs every test project in `CSharpDB.slnx`, runs the
cross-platform migration-isolation checks, and adds the Windows-only Access
isolation check on Windows. Logs, TRX results, temporary package state, and the
Markdown result are written beneath the caller-selected output path outside
the repository. The source tree must be clean before and after the run.

The authoritative GitHub `SQL Release Qualification` workflow executes this
same functional check in two independent clean jobs on each of Windows, Linux,
and macOS. It can be started manually for a release candidate and is also
required automatically by the tag release workflow before any publishing job
starts. Before the workflow has first reached the default branch, pushing a
non-release `qualification-*` tag runs it from that exact candidate commit
without invoking the `v*` publishing workflow. Once registered on the default
branch, normal manual dispatch can target any release-candidate branch. The
workflow also keeps the 18 persistent-read and in-memory master-table rows
blocking in two balanced paired Windows jobs. Only the ten disk-sensitive
durable writes are excluded from hosted performance qualification.

The durable-write comparison is run against the final release commit immediately
before tagging in step 5 below.

For a same-artifact exact-row A/A diagnostic, use an absent or empty output
directory outside the checkout:

```powershell
.\tests\CSharpDB.Benchmarks\scripts\Test-PreviousReleasePerformance.ps1 `
  -PreviousRef HEAD -CandidateRef HEAD `
  -OutputPath (Join-Path ([IO.Path]::GetTempPath()) "csharpdb-hybrid-aa-$([Guid]::NewGuid().ToString('N'))") `
  -QualificationPass 1 -Paired -RepeatCount 5 `
  -AllowSameRevision -ShareSameRevisionArtifact `
  -HybridStorageScenarioName 'Storage_HybridIncrementalDurable_Sql_SingleInsert_5s' `
  -PostBuildQuiescenceSeconds 30
```

Five repeats mean five pairs per order, ten pairs total. The exact,
case-sensitive scenario replaces the seven-suite plan and takes precedence over
`-SuiteName`. Each logical invocation performs one internal two-second warmup,
then records until it has both 30 measured seconds and 10,000 retained latency
samples; failure to reach both by the 120-second measured-phase cap fails
closed. All paired comparisons use exact direct DLL execution and pre/post
verification of the complete runnable closure. `-AllowSameRevision` alone
permits A/A but still uses two builds; `-ShareSameRevisionArtifact` additionally
requires equal commits and maps one candidate DLL and closure to both labels.
Both logical identities, the shared execution-time path, and all relative
closure file hashes are recorded.

These controls diagnose the harness only and cannot replace either balanced
paired cross-version qualification pass or promote a baseline.
`-PostBuildQuiescenceSeconds` is an opt-in build-server shutdown plus fixed wait,
not a machine-idleness guarantee, and it can affect concurrent .NET builds.
Preflight metadata, hash manifests, raw and aggregate CSV evidence, logs, and
reports must remain in the runner-owned external directory. Never copy or
commit them into source paths. Diagnostic runs do not update the curated
`release-core-manifest.json` and do not introduce generated diagnostic JSON.

3. Publish one local archive for a fast packaging check.

```powershell
$Version = (Read-Host 'Release version without the v prefix').Trim()
if ($Version -notmatch '^[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$') {
  throw 'Enter a semantic release version in major.minor.patch form.'
}
.\scripts\Publish-CSharpDbDaemonRelease.ps1 `
  -Version $Version `
  -Runtime win-x64 `
  -OutputRoot artifacts\daemon-release-local
```

4. Confirm the archive and checksum exist.

```powershell
Get-ChildItem artifacts\daemon-release-local\archives
Get-Content artifacts\daemon-release-local\archives\SHA256SUMS.txt
```

5. After the pull request is merged, update local `main`, then run the canonical
   release-tag publisher on the dedicated fixed-SSD Windows machine.

```powershell
git switch main
git pull --ff-only
$Version = (Read-Host 'Release version without the v prefix').Trim()
if ($Version -notmatch '^[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$') {
  throw 'Enter a semantic release version in major.minor.patch form.'
}

.\scripts\Publish-ReleaseTag.ps1 `
  -Version $Version `
  -ConfirmDedicatedFixedSsd
```

`Publish-ReleaseTag.ps1` is the only supported way to create a release tag. Do
not create release tags in the GitHub UI or with raw `git tag` / `git push`
commands; those paths bypass the preflight and cause the fail-closed Release
workflow to reject the tag. The publisher requires a clean, checked-out `main`,
fetches `origin/main`, and requires local `HEAD` to equal that remote commit. It
also validates the requested version and requires a canonical
`csharpdb/local-durable-performance` success status from the configured attestor
on that exact SHA before it creates or pushes the tag.

When that exact commit already has a valid status, the publisher reuses it and
does not rerun the benchmarks. Otherwise `-ConfirmDedicatedFixedSsd` authorizes
the publisher to run the local durable wrapper. That wrapper forces durable mode
and runs the ten durable SQL/collection single and batch write rows in two
sequential balanced paired passes. On an idle fixed-SSD machine this normally
takes 75–100 minutes total. It pins the candidate and previous commits, retains
hash-verified raw evidence and a Markdown summary outside the repository,
creates no repository JSON, and publishes the status only after both passes
succeed. Without a reusable status or the confirmation switch, the publisher
stops before creating a tag and explains how to run the required qualification.

The wrapper's Windows quiescence preflight refuses to start while a Windows
Installer transaction is active or Component-Based Servicing (CBS) or Windows
Update reports that a restart is required. It classifies
`PendingFileRenameOperations` separately: a stable, well-formed deletion-only
set may be fingerprinted and accepted as the
run baseline, while malformed entries and replacement or rename operations
block qualification. An active installer normally means wait for it to finish
and run the preflight again; restart Windows when CBS or Windows Update requires
it, or when blocking file operations remain after installers and updates have
settled.

After each pass, the wrapper audits Windows Installer transactions and compares
the pending-file-operation state with the recorded baseline. Any MSI transaction
during a pass or any baseline change contaminates the timing evidence, prevents
qualification, and stops the run before another pass begins. Start a new clean
run instead of reusing contaminated evidence.

The command is idempotent for the same version and exact commit: it safely
reuses a valid status and an existing local or remote tag that already points to
that commit. It rejects a same-named tag that points elsewhere. Optional
`-GitHubRepository owner/name` and `-ExpectedStatusCreator login` arguments are
available when repository discovery or the configured attestor must be supplied
explicitly.

The Release workflow remains a fail-closed backstop. It independently requires
the matching-commit status, completes both clean functional passes on Windows,
Linux, and macOS, and runs the 18 persistent-read and in-memory performance rows
on hosted Windows runners before publishing. It then publishes the daemon
archives for `win-x64`, `linux-x64`, and `osx-arm64`, smoke-starts each extracted
binary,
calls the daemon REST `/api/info` endpoint, verifies a gRPC `GetInfoAsync`
client call, combines checksums, and attaches everything to the GitHub Release.

`-NoGitHubStatus` is available only for diagnostics and wrapper tests. A run with
that switch does not satisfy the release workflow's matching-commit check.
The official status is available only with automatic previous-release discovery
and the canonical `durable-v2` repeat, quiescence, and regression settings. That
policy blocks on throughput and P95 while retaining P99 as diagnostic evidence;
selecting P99 or any other override requires `-NoGitHubStatus`. A truly blocking
P99 qualification would require a separately designed longer experiment with
enough tail observations and repeatability. The workflow accepts the status
only from the login named by the `LOCAL_DURABLE_ATTESTOR` repository variable,
falling back to the repository owner when the variable is unset.

## Operator Walkthrough

Use this path after a GitHub Release is published.

1. Download the daemon archive for the target OS:
   `csharpdb-daemon-v{version}-win-x64.zip`,
   `csharpdb-daemon-v{version}-linux-x64.tar.gz`, or
   `csharpdb-daemon-v{version}-osx-arm64.tar.gz`.
2. Verify the archive with the published `SHA256SUMS.txt`.
3. Extract the archive on the target machine.
4. Run the matching service installer from inside the extracted archive.
5. Connect REST clients to `http://127.0.0.1:5820/api/...` or gRPC clients to
   the same base URL. The installed daemon exposes both transports by default.

Windows, from an elevated PowerShell session:

```powershell
.\service\windows\install-csharpdb-daemon.ps1 -Start
```

Linux:

```bash
sudo ./service/linux/install-csharpdb-daemon.sh --start
```

macOS:

```bash
sudo ./service/macos/install-csharpdb-daemon.sh --start
```

To upgrade, extract the newer archive and rerun the same installer with the
same data directory plus `-Force` on Windows or `--force` on Linux/macOS. The
installers replace daemon files but do not delete the database directory.

To disable the daemon REST surface while keeping gRPC enabled, set
`CSharpDB__Daemon__EnableRestApi=false` in the service environment or generated
`appsettings.Production.json`, then restart the service.

## Scripts

### `Clear-GitHubWorkflowRuns.ps1`

Use this when the GitHub Actions run history has become noisy and you want to
delete older completed runs.

What it does:

- resolves the GitHub repository from the local `origin` remote by default
- enumerates workflow runs through the GitHub CLI
- deletes only completed runs older than the configured cutoff
- supports `-WhatIf` and `-Confirm` for safe dry runs

Example:

```powershell
& .\scripts\Clear-GitHubWorkflowRuns.ps1 -OlderThanDays 7
& .\scripts\Clear-GitHubWorkflowRuns.ps1 -OlderThanDays 30 -WhatIf
```

### `Start-CSharpDbAdminAndDaemon.ps1`

Use this when the admin site should talk to the gRPC daemon.

What it does:

- reads the daemon launch URL from
  [`src/CSharpDB.Daemon/Properties/launchSettings.json`](../src/CSharpDB.Daemon/Properties/launchSettings.json)
- reads the daemon database connection string from
  [`src/CSharpDB.Daemon/appsettings.json`](../src/CSharpDB.Daemon/appsettings.json)
- updates [`src/CSharpDB.Admin/appsettings.json`](../src/CSharpDB.Admin/appsettings.json) to:
  - set `CSharpDB.Transport = "grpc"`
  - set `CSharpDB.Endpoint` to the daemon base URL
  - copy the daemon connection string into `ConnectionStrings:CSharpDB`
- starts `CSharpDB.Daemon`
- waits for the daemon port to come up
- starts `CSharpDB.Admin`

The daemon started by this script also serves the REST `/api` surface on the
same base URL unless `CSharpDB:Daemon:EnableRestApi` is disabled.

### `Start-CSharpDbAdminDirect.ps1`

Use this when the admin site should open a local database directly without the
daemon.

What it does:

- updates [`src/CSharpDB.Admin/appsettings.json`](../src/CSharpDB.Admin/appsettings.json) to:
  - set `CSharpDB.Transport = "direct"`
  - remove `CSharpDB.Endpoint`
  - keep or set `ConnectionStrings:CSharpDB`
- starts only `CSharpDB.Admin`

The default Admin direct configuration uses
`CSharpDB:HostDatabase:OpenMode = "HybridIncrementalDurable"`, so direct mode
opens through the hybrid incremental-durable local host path. The Admin host
warms one in-process database instance at startup and keeps it alive until the
Admin app shuts down or the user switches databases. Set
`CSharpDB:HostDatabase:OpenMode = "Direct"` if you need the older plain direct
open path for a local run.

### `Start-CSharpDbAdminFormsWeb.ps1`

Use this when you want to run stored forms through the forms-only runtime host
without opening the full Admin studio.

What it does:

- reads the default target database path from
  [`src/CSharpDB.Admin.Forms.Web/appsettings.json`](../src/CSharpDB.Admin.Forms.Web/appsettings.json)
  unless you pass `-DataSource`
- starts `src/CSharpDB.Admin.Forms.Web`
- passes the resolved `CSharpDB:DataSource` and `--urls` values through
  command-line configuration overrides
- waits for the forms host port to accept TCP connections
- optionally opens the root runtime page in the default browser

Use a sample database that already contains seeded forms, such as the
Fulfillment Hub sample database, when you want the runtime root page to list
real forms immediately.

### `Publish-CSharpDbDaemonRelease.ps1`

Use this when preparing self-contained daemon release archives.

What it does:

- publishes `src/CSharpDB.Daemon` for one or more runtime identifiers
- uses Release, self-contained, single-file, non-trimmed publish settings
- stages the daemon with service assets from `deploy/daemon`
- creates `csharpdb-daemon-v{version}-{rid}.zip` for Windows
- creates `csharpdb-daemon-v{version}-{rid}.tar.gz` for Linux/macOS
- writes `SHA256SUMS.txt`

Examples:

```powershell
$Version = (Read-Host 'Release version without the v prefix').Trim()
if ($Version -notmatch '^[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$') {
  throw 'Enter a semantic release version in major.minor.patch form.'
}
.\scripts\Publish-CSharpDbDaemonRelease.ps1 -Version $Version -Runtime win-x64
.\scripts\Publish-CSharpDbDaemonRelease.ps1 -Version $Version -Runtime linux-x64,osx-arm64
```

Default runtimes:

- `win-x64`
- `linux-x64`
- `osx-arm64`

Outputs are written under `artifacts\daemon-release` unless `-OutputRoot` is
provided.

### `Publish-CSharpDbMigrationRelease.ps1`

Use this to create the installable combined migration CLI archives for a
CSharpDB release. It composes the reviewed SQL Server and MySQL bundle publishers
for every runtime and the reviewed Microsoft Access bundle publisher for
`win-x64`, instead of rebuilding any provider layout itself.

What it does:

- publishes the SQL Server and MySQL audited framework-dependent bundles for
  every selected RID, plus the Windows-only Access bundle for `win-x64`
- verifies every applicable bundle's base CLI root has identical file sets,
  lengths, and SHA-256 digests before merging anything
- preserves the fixed workers and all provider notices/licenses beneath
  `adapters/sqlserver`, `adapters/mysql`, and, on Windows only,
  `adapters/access`
- rejects Access assemblies, `System.Data.OleDb`, and the Access adapter
  directory from Linux and macOS stages
- verifies the CLI and workers target `Microsoft.NETCore.App` 10.x and rejects
  accidental self-contained runtime files
- adds the safe Windows and POSIX installers from `deploy/migration-tool`
- creates `csharpdb-migration-tool-v{version}-{rid}.zip` for Windows
- creates `csharpdb-migration-tool-v{version}-{rid}.tar.gz` for Linux/macOS
- writes `SHA256SUMS.txt`

The default RIDs are `win-x64`, `linux-x64`, and `osx-arm64`:

```powershell
$Version = (Read-Host 'Release version without the v prefix').Trim()
.\scripts\Publish-CSharpDbMigrationRelease.ps1 `
  -Version $Version
```

The publisher requires PowerShell 7.4 or later.

For a single local packaging check:

```powershell
$Version = (Read-Host 'Release version without the v prefix').Trim()
.\scripts\Publish-CSharpDbMigrationRelease.ps1 `
  -Version $Version `
  -Runtime win-x64 `
  -OutputRoot artifacts\migration-release-local
```

Outputs are written under `artifacts\migration-release` by default. Managed
publish, stage, and archive directories must be empty unless `-Force` is
explicitly supplied. The script never removes `OutputRoot` itself and rejects
links or reparse points in the output path, its existing ancestors, and
directories it would replace.

These releases are framework-dependent and require the Microsoft .NET 10
runtime. A RID-specific archive is not a claim of runtime, authentication,
TLS, or live Access/SQL Server/MySQL qualification. Each extracted archive
includes `README.md`, `LICENSE`, and installers under `install/windows` and
`install/posix`. The Windows installer requires the complete Access adapter in
the `win-x64` archive. The POSIX installer has no Access dependency. Both copy
into a caller-selected directory, do not overwrite a nonempty destination
without explicit force, do not create a service or require administrator
access by default, and print optional `PATH` setup instructions without
changing `PATH`.

### `Publish-CSharpDbAccessMigrationBundle.ps1`

Use this to produce the non-packable, Windows-only Microsoft Access capture
distribution. The generic `csharpdb` host remains provider-free at the bundle
root. `System.Data.OleDb` and the reviewed support closure are published only
beneath `adapters/access`, alongside the fixed companion worker and
`THIRD-PARTY-NOTICES.md`.

The destination must be absent or empty. Output is framework-dependent and
restricted to `win-x64`. The Microsoft Access Database Engine (ACE) is an
external, bitness-sensitive prerequisite that CSharpDB does not redistribute
or install. Publishing the adapter does not complete its deferred live
qualification matrix.

```powershell
.\scripts\Publish-CSharpDbAccessMigrationBundle.ps1 `
  -OutputPath artifacts\access-migration-local `
  -Configuration Release
```

### `Test-SqlReleaseQualification.ps1`

Use this from a clean checkout to run the source-level release gate. It
requires an empty output directory outside the repository and records its
logs, TRX files, temporary NuGet state, and Markdown summary there. It does not
create a feature-coverage file or any other generated source artifact.

The check validates public documentation, NuGet package closure, EF Core
version consistency, the full solution restore/build/test sequence, SQL Server
and MySQL provider isolation, the Windows-only Access isolation boundary, and
the packaged EF migration tool. Supplying both `-ReleaseVersion` and
`-ReleaseCommit` additionally validates an existing release tag.

```powershell
$OutputPath = Join-Path `
  ([IO.Path]::GetTempPath()) `
  "csharpdb-sql-release-$([Guid]::NewGuid().ToString('N'))"

.\scripts\Test-SqlReleaseQualification.ps1 `
  -OutputPath $OutputPath `
  -Configuration Release `
  -QualificationPass 1
```

The GitHub workflow runs qualification passes 1 and 2 in separate clean hosted
jobs for each supported operating system. The pass number identifies evidence;
it does not weaken or filter the checks.

### `Test-AccessMigrationIsolation.ps1`

Use this Windows-only provider-boundary check after restore. It publishes the
base CLI and optional Access bundle separately, proves the base dependency
graph contains no Access adapter or OLE DB assets, proves the provider closure
stays beneath `adapters/access`, and exercises stable worker-absent and
provider/capture failure paths without requiring a valid Access database.

```powershell
.\scripts\Test-AccessMigrationIsolation.ps1 `
  -Configuration Release
```

### `Publish-CSharpDbMySqlMigrationBundle.ps1`

Use this to produce the non-packable MySQL schema-analysis distribution. The
generic `csharpdb` host is published at the bundle root. MySqlConnector and its
supporting dependencies are published only beneath `adapters/mysql`, alongside
the fixed companion worker, the resolved dependency inventory, and the
applicable third-party license notices.

The destination must be absent or empty. Output is intentionally
framework-dependent; self-contained output is excluded until its runtime
license and notice closure is audited. A runtime identifier may be selected
for a qualification-only framework-dependent build, but doing so does not
qualify that runtime or a live MySQL version.

```powershell
.\scripts\Publish-CSharpDbMySqlMigrationBundle.ps1 `
  -OutputPath artifacts\mysql-migration-local `
  -Configuration Release
```

### `Test-MySqlMigrationIsolation.ps1`

Use this provider-absent packaging check after restore. It publishes the base
CLI and optional MySQL bundle separately, proves the base dependency graph
contains no MySQL adapter or provider assets, proves the provider closure stays
under the worker directory, and exercises stable adapter-absent and
connection-absent failures without contacting a server. The bundled failure
also confirms that the fixed-path worker and
`csharpdb-mysql-worker/v1` protocol are usable.

```powershell
.\scripts\Test-MySqlMigrationIsolation.ps1 `
  -Configuration Release
```

### `Publish-CSharpDbSqlServerMigrationBundle.ps1`

Use this to produce the non-packable SQL Server schema-analysis distribution.
The generic `csharpdb` host is published at the bundle root. SqlClient,
ScriptDom, SNI, and authentication dependencies are published only beneath
`adapters/sqlserver`, alongside the fixed companion worker, the resolved
dependency inventory, and the applicable third-party license notices.

The destination must be absent or empty. Output is intentionally
framework-dependent; self-contained output is excluded until its runtime
license and notice closure is audited. A runtime identifier may be selected
for a qualification-only framework-dependent build, but doing so does not
qualify that runtime or a live SQL Server version.

The worker keeps the reviewed SNI package private from consumers while
explicitly including its applicable native binary in publish output. The
publisher requires that binary for Windows RID builds (and all three reviewed
Windows native assets for a portable build), alongside the exact dependency
inventory and the hashed SNI license. Linux and macOS RID closures exclude the
Windows-only SNI package while retaining its notice and license in the common
worker distribution. This explicit publish contract is required by the .NET
10 SDK for packages marked `PrivateAssets="all"`.

```powershell
.\scripts\Publish-CSharpDbSqlServerMigrationBundle.ps1 `
  -OutputPath artifacts\sqlserver-migration-local `
  -Configuration Release
```

### `Test-SqlServerMigrationIsolation.ps1`

Use this provider-absent packaging check after restore. It publishes the base
CLI and the optional SQL Server bundle separately, proves the base dependency
graph contains no SQL Server adapter or provider assets, proves the provider
closure stays under the worker directory, and exercises stable adapter-absent
and connection-absent failures without contacting a server.

```powershell
.\scripts\Test-SqlServerMigrationIsolation.ps1 `
  -Configuration Release
```

### `Test-EfCoreMigrationTool.ps1`

Use this after restore to qualify the packaged `csharpdb-ef` tool boundary. It
packs and installs the tool into a validated repository-local temporary
workspace, analyzes the compiled valid and unsupported fixtures, checks the
unchanged generation-only exit/status contract, and proves target-controlled
console output does not escape into the JSON report. A separate `--scratch`
lane executes the valid two-prefix chain, requires
`Compatible`/`ScratchExecuted` evidence, verifies apply/down/reapply and two
analyzer-owned guarded replays built from retained `Up` command payloads, and
checks that no configured, sample, temporary database, WAL, journal, or lock
artifact is left behind. It also checks the Web SDK minimal sample without
creating `sample.db`, then publishes the base `csharpdb` CLI and verifies that
the separate analyzer and EF Core design-time dependencies did not enter that
dependency graph. A Raw SQL scratch request separately pins the blocked
envelope, zero execution evidence, and `Conditional` exit code `1`.

The scratch lane proves only an empty private-memory database. It does not
qualify existing-row conversions, file/WAL persistence, configured
`IMigrator`, `IMigrator.GenerateScript`, EF-generated idempotent scripts,
migration-history behavior, migration locks, or interceptors.

```powershell
.\scripts\Test-EfCoreMigrationTool.ps1 `
  -Configuration Release `
  -NoRestore
```

To qualify the exact tool package produced by a release pack, supply both its
local feed and version. This mode does not repack the project; it verifies the
selected nupkg identity and hash, installs only from that feed, and runs the
same checks.

```powershell
$Version = (Read-Host 'Release version without the v prefix').Trim()
.\scripts\Test-EfCoreMigrationTool.ps1 `
  -FeedPath artifacts/nuget `
  -Version $Version
```

### `Publish-CSharpDbAdminStorePackage.ps1`

Use this on Windows when preparing the Microsoft Store package for CSharpDB
Studio.

What it does:

- publishes `src/CSharpDB.Admin` self-contained for `win-x64`
- publishes the WPF/WebView2 desktop shell from `src/CSharpDB.Admin.Desktop`
- copies the Admin host into `artifacts\admin-store\publish\desktop\admin`
  so `CSharpDB.Admin.Desktop.exe` can be smoke-tested before packaging
- stages the Admin host under the desktop shell's private `admin` folder
- creates app visual assets from the existing Admin icon
- signs the MSIX with a local test certificate by default and exports the
  public `.cer` beside the package
- emits a local `.msix` and Store `.msixupload` under `artifacts\admin-store`

Example:

```powershell
$Version = (Read-Host 'Release version without the v prefix').Trim()
.\scripts\Publish-CSharpDbAdminStorePackage.ps1 -Version $Version
```

For local App Installer testing, import the exported test certificate from an
elevated PowerShell session before double-clicking the `.msix`:

```powershell
$Version = (Read-Host 'Release version without the v prefix').Trim()
Import-Certificate `
  -FilePath "artifacts\admin-store\packages\csharpdb-studio-v$Version-win-x64-local-test.cer" `
  -CertStoreLocation Cert:\LocalMachine\TrustedPeople
```

You can also run the packaging script from an elevated PowerShell session with
`-TrustLocalTestCertificate` to perform that import automatically. Use
`-SkipSigning` only for packaging diagnostics; unsigned MSIX files cannot be
installed directly with App Installer.

The package identity starts as `MaxAkbar.CSharpDBStudio` with publisher
`CN=MaxAkbar`; associate the package with Partner Center before submission so
the final Store identity and publisher are applied.

### `Publish-CSharpDbAdminRelease.ps1`

Use this to create the portable Windows Admin desktop archive published with a
GitHub Release. It publishes `CSharpDB.Admin.Desktop.exe` and the self-contained
`CSharpDB.Admin` host, places the host under the shell's required `admin/`
folder, and creates:

`csharpdb-admin-desktop-v{version}-win-x64.zip`

The archive can be extracted anywhere and launched with
`CSharpDB.Admin.Desktop.exe`. A matching entry is written to
`ADMIN-SHA256SUMS.txt`.

```powershell
$Version = (Read-Host 'Release version without the v prefix').Trim()
if ($Version -notmatch '^[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$') {
  throw 'Enter a semantic release version in major.minor.patch form.'
}
.\scripts\Publish-CSharpDbAdminRelease.ps1 -Version $Version
```

## Daemon Service Installers

Release archives include OS service assets under `service/`:

- Windows: `service/windows/install-csharpdb-daemon.ps1`
- Windows: `service/windows/uninstall-csharpdb-daemon.ps1`
- Linux: `service/linux/csharpdb-daemon.service`
- Linux: `service/linux/install-csharpdb-daemon.sh`
- Linux: `service/linux/uninstall-csharpdb-daemon.sh`
- macOS: `service/macos/com.csharpdb.daemon.plist`
- macOS: `service/macos/install-csharpdb-daemon.sh`
- macOS: `service/macos/uninstall-csharpdb-daemon.sh`

Default service settings:

| Platform | Service | Install directory | Data directory | URL |
|----------|---------|-------------------|----------------|-----|
| Windows | `CSharpDBDaemon` | `C:\Program Files\CSharpDB\Daemon` | `C:\ProgramData\CSharpDB` | `http://127.0.0.1:5820` |
| Linux | `csharpdb-daemon` | `/opt/csharpdb-daemon` | `/var/lib/csharpdb` | `http://127.0.0.1:5820` |
| macOS | `com.csharpdb.daemon` | `/usr/local/lib/csharpdb-daemon` | `/usr/local/var/csharpdb` | `http://127.0.0.1:5820` |

Installers accept service name, install directory, data directory, bind URL,
and force/overwrite options. Windows scripts support `-WhatIf`; Linux and macOS
scripts require `sudo` and fail early when not run as root.

The generated production config enables REST by default through
`CSharpDB:Daemon:EnableRestApi=true`. Service-level environment variables can
override this with `CSharpDB__Daemon__EnableRestApi=false`.

## Quick Start

From the repo root in PowerShell:

```powershell
& .\scripts\Start-CSharpDbAdminAndDaemon.ps1
```

Start the admin in direct mode:

```powershell
& .\scripts\Start-CSharpDbAdminDirect.ps1
```

Start the forms-only runtime host against the Fulfillment Hub sample database:

```powershell
& .\scripts\Start-CSharpDbAdminFormsWeb.ps1 `
  -DataSource samples\fulfillment-hub\bin\Debug\net10.0\fulfillment-hub-demo.db `
  -OpenBrowser
```

Open the admin site automatically after startup:

```powershell
& .\scripts\Start-CSharpDbAdminAndDaemon.ps1 -OpenAdmin
```

Use a specific direct-mode database:

```powershell
& .\scripts\Start-CSharpDbAdminDirect.ps1 -ConnectionString "Data Source=C:\data\demo.db"
```

## What Gets Changed

Both scripts rewrite
[`src/CSharpDB.Admin/appsettings.json`](../src/CSharpDB.Admin/appsettings.json).

That means:

- the current admin transport mode persists after the script exits
- if you switch between gRPC mode and direct mode, the last script you ran wins
- if you have local edits in `src/CSharpDB.Admin/appsettings.json`, the script
  may overwrite those transport-related settings

The daemon script does not modify
[`src/CSharpDB.Daemon/appsettings.json`](../src/CSharpDB.Daemon/appsettings.json).
It only reads from it.

## Start And Stop Workflow

### Recommended: capture the process IDs

Use `-PassThru` so PowerShell returns the host PIDs:

```powershell
$session = & .\scripts\Start-CSharpDbAdminAndDaemon.ps1 -PassThru
$session
```

Example output object:

```text
DaemonEndpoint : https://localhost:49995
AdminUrl       : https://localhost:61816
DaemonPid      : 12345
AdminPid       : 23456
```

Stop both hosts:

```powershell
Stop-Process -Id $session.AdminPid, $session.DaemonPid
```

For direct mode:

```powershell
$session = & .\scripts\Start-CSharpDbAdminDirect.ps1 -PassThru
Stop-Process -Id $session.AdminPid
```

### If you already started them without `-PassThru`

Find the running host processes by command line:

```powershell
Get-CimInstance Win32_Process |
  Where-Object {
    $_.Name -eq 'dotnet.exe' -and
    $_.CommandLine -match 'CSharpDB.Admin|CSharpDB.Daemon'
  } |
  Select-Object ProcessId, CommandLine
```

Stop them:

```powershell
Get-CimInstance Win32_Process |
  Where-Object {
    $_.Name -eq 'dotnet.exe' -and
    $_.CommandLine -match 'CSharpDB.Admin|CSharpDB.Daemon'
  } |
  ForEach-Object { Stop-Process -Id $_.ProcessId }
```

## Common Options

### `-NoLaunch`

Update config only without starting any process:

```powershell
& .\scripts\Start-CSharpDbAdminAndDaemon.ps1 -NoLaunch
& .\scripts\Start-CSharpDbAdminDirect.ps1 -NoLaunch
```

This is useful when you want to inspect the config change first.

### `-OpenAdmin`

Open the admin URL in the default browser after startup succeeds:

```powershell
& .\scripts\Start-CSharpDbAdminAndDaemon.ps1 -OpenAdmin
```

### `-PassThru`

Return the resolved URLs and PIDs:

```powershell
& .\scripts\Start-CSharpDbAdminAndDaemon.ps1 -PassThru
& .\scripts\Start-CSharpDbAdminDirect.ps1 -PassThru
```

### Startup timeout overrides

If your machine is slow or the first build is cold, increase the wait time:

```powershell
& .\scripts\Start-CSharpDbAdminAndDaemon.ps1 `
  -DaemonStartupTimeoutSeconds 60 `
  -AdminStartupTimeoutSeconds 60
```

## Config Sources

The admin-and-daemon startup script resolves values from the repo files below:

- daemon URL:
  [`src/CSharpDB.Daemon/Properties/launchSettings.json`](../src/CSharpDB.Daemon/Properties/launchSettings.json)
- daemon database connection string:
  [`src/CSharpDB.Daemon/appsettings.json`](../src/CSharpDB.Daemon/appsettings.json)
- admin URL:
  [`src/CSharpDB.Admin/Properties/launchSettings.json`](../src/CSharpDB.Admin/Properties/launchSettings.json)

The direct-mode script resolves values from:

- admin URL:
  [`src/CSharpDB.Admin/Properties/launchSettings.json`](../src/CSharpDB.Admin/Properties/launchSettings.json)
- admin database connection string:
  [`src/CSharpDB.Admin/appsettings.json`](../src/CSharpDB.Admin/appsettings.json), unless you pass `-ConnectionString`

The forms-runtime script resolves values from:

- forms host database path:
  [`src/CSharpDB.Admin.Forms.Web/appsettings.json`](../src/CSharpDB.Admin.Forms.Web/appsettings.json), unless you pass `-DataSource`
- forms host URL:
  the script `-Url` parameter, defaulting to `http://127.0.0.1:5095`

## Use `Get-Help`

Both scripts now include comment-based help:

```powershell
Get-Help .\scripts\Start-CSharpDbAdminAndDaemon.ps1 -Detailed
Get-Help .\scripts\Start-CSharpDbAdminDirect.ps1 -Detailed
Get-Help .\scripts\Start-CSharpDbAdminFormsWeb.ps1 -Detailed
```

## Troubleshooting

### The admin starts in the wrong mode

Check the current values in
[`src/CSharpDB.Admin/appsettings.json`](../src/CSharpDB.Admin/appsettings.json):

- `CSharpDB.Transport`
- `CSharpDB.Endpoint`
- `ConnectionStrings:CSharpDB`

Run the appropriate script again with `-NoLaunch` if you want to confirm the
config update without starting the hosts.

### The daemon script times out

Check:

- the daemon launch URL in
  [`src/CSharpDB.Daemon/Properties/launchSettings.json`](../src/CSharpDB.Daemon/Properties/launchSettings.json)
- whether another process is already using that port
- whether HTTPS development certificates or local firewall rules are blocking
  the host startup

### Closing the shell did not stop the hosts

That is expected. The scripts use `Start-Process`, so the launched `dotnet`
processes keep running until you stop them explicitly.
