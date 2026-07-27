# CSharpDB Migration CLI

This archive contains the generic `csharpdb` migration CLI plus the fixed SQL
Server and MySQL workers. The `win-x64` archive also contains the Windows-only
Microsoft Access worker:

```text
adapters/
  sqlserver/
  mysql/
  access/       # win-x64 only
```

Keep that layout intact. Each adapter directory contains its own
`THIRD-PARTY-NOTICES.md` and applicable `licenses/` files; those notices and
licenses must remain with redistributed or installed copies.

Linux and macOS archives do not contain `adapters/access`, Access assemblies,
or `System.Data.OleDb`. Their POSIX installer does not require or install the
Access adapter.

## Runtime requirement

This is a framework-dependent release, not a self-contained application.
Install the Microsoft .NET 10 runtime for this archive's operating system and
architecture before running the CLI:

```text
Microsoft.NETCore.App 10.x
```

Packaging a runtime-specific app host does not establish runtime,
authentication, TLS, or live-provider qualification.

## Verify the download

Before extraction, compare the archive's SHA-256 digest with its entry in the
GitHub release asset `MIGRATION-SHA256SUMS.txt`. Keep migration package digests
in an independently trusted record as described by the migration commands; an
archive checksum is not a substitute for a source-package digest.

## Install on Windows

Extract the zip, then run from the extracted archive:

```powershell
.\install\windows\install-csharpdb-migration-tool.ps1 `
  -InstallDirectory "$env:LOCALAPPDATA\CSharpDB\MigrationTool"
```

The destination must be absent or empty. Add `-Force` only when you intend to
overwrite colliding files in an existing directory. The script does not
request administrator access, create a service, or change `PATH`. The
extracted release and destination paths must not pass through links or
reparse points.

The Access adapter requires a separately installed,
process-bitness-compatible Microsoft Access Database Engine (ACE). CSharpDB
does not redistribute or install ACE. Access capture accepts supported
unencrypted local `.mdb` and `.accdb` files, but remains evaluation-only until
its declared Windows, ACE, file-format, and process-bitness qualification
matrix is complete.

## Install on Linux or macOS

Extract the tarball, then run from the extracted archive:

```sh
sh install/posix/install-csharpdb-migration-tool.sh \
  --install-dir "$HOME/.local/lib/csharpdb-migration-tool"
```

Add `--force` only when you intend to overwrite colliding files. The script
does not use `sudo`, create a service, or change `PATH`. It prints the `export
PATH=...` command you can use for the current shell or add to your shell
profile. Choose a directory your current user can write; the filesystem root
is never accepted as an install destination.

You may also run `csharpdb` directly from the extracted archive without
installing it.

## Source credentials and retained data

Pass database credentials only through the environment variable named by
`--connection-env`; never put a connection string directly in command text.
Retained `.csdbsqlserver`, `.csdbmysql`, and `.csdbaccess` packages contain
plaintext-sensitive source data. Protect them with source-equivalent access,
retention, and deletion controls, and store each expected package digest
separately from the package.

The retained MySQL v1 path uses a dedicated read-only account with direct
schema-level `SELECT` on the selected database's ordinary base tables. It does
not require `TRIGGER`, `EXECUTE`, or `SHOW VIEW`. Programmable objects remain
outside retained v1.

These archives provide the reviewed package and process boundaries. They do
not claim broad live Access, SQL Server, or MySQL qualification. Consult the
migration catalog diagnostics and complete the applicable live qualification
matrix before treating a provider path as shipping-qualified.
