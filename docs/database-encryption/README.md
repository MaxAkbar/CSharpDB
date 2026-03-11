# CSharpDB Database Encryption — Roadmap & Design

> **Status (March 2026):** Research. This document captures the recommended direction for at-rest encryption in CSharpDB. The first implementation milestone should cover managed local surfaces only; Native/FFI support is a follow-up phase.

CSharpDB currently stores database and WAL bytes in plaintext on disk. This document outlines how to add full at-rest encryption without weakening crash recovery, snapshot readers, checkpointing, or export flows.

---

## Problem

Today:

- the main `.db` file is plaintext
- the companion `.wal` file is plaintext
- `SaveToFileAsync` and `LoadIntoMemoryAsync` operate on plaintext bytes
- storage diagnostics read raw bytes assuming the current plaintext format

A simple `IStorageDevice` wrapper is not enough for first-class encryption because:

- WAL I/O is implemented separately and does not flow through `IStorageDevice`
- page and WAL frame sizes are hard-coded around plaintext layouts
- diagnostics and inspection tools read file bytes directly
- recovery and checkpoint logic assume the current on-disk header/frame structure

If encryption is added as an ad hoc wrapper in only one layer, the feature will be incomplete and the format will be brittle.

---

## Goal

Add **full at-rest encryption** for managed local surfaces:

1. Encrypt the main database file
2. Encrypt the WAL file
3. Preserve crash recovery and checkpoint correctness
4. Support both plaintext and encrypted files
5. Require explicit migration/export instead of silently rewriting plaintext files on open
6. Expose configuration through code options and ADO.NET connection strings

For v1, the scope is:

- `CSharpDB.Engine`
- `CSharpDB.Storage`
- `CSharpDB.Data`
- direct `CSharpDB.Client` usage
- API/MCP local configuration
- CLI local/direct usage
- storage diagnostics behavior for encrypted files

Out of scope for v1:

- TLS or remote transport security
- OS secret-store integration
- Native/FFI and downstream language wrapper changes

---

## Recommended Design

### 1. Use a new encrypted storage format

Encryption should be implemented as a **new encrypted storage format version**, not as a raw byte wrapper around the existing plaintext file format.

That format layer should own:

- file header layout
- logical page to physical page encoding
- WAL header and frame encoding
- authenticated metadata handling
- format detection at open time

The storage stack should support at least:

- plaintext format v1: current format, unchanged
- encrypted format v2: new format with encrypted pages and WAL frames

### 2. Encrypt page and WAL payloads, not the entire bootstrap header

The encrypted format should keep a small clear bootstrap header so the engine can:

- detect encrypted vs plaintext format
- identify page size / physical layout
- read KDF parameters
- read the wrapped database master key

Page and frame payloads should then be encrypted and authenticated.

Recommended approach:

- clear file header for bootstrap metadata
- encrypted logical page payloads
- clear WAL header for bootstrap metadata
- encrypted WAL frame payloads

### 3. Crypto and key hierarchy

Recommended baseline:

- `AesGcm` from `System.Security.Cryptography`
- random database master key generated at database creation time
- passphrase-derived KEK using PBKDF2-HMAC-SHA256
- wrapped/encrypted master key stored in the bootstrap header
- separate derived subkeys for DB pages and WAL frames

This keeps passphrase handling simple while allowing future extension to stronger key-management sources.

### 4. Authentication replaces plaintext-style integrity checks

Encrypted files should rely on AEAD authentication tags for integrity of encrypted payloads.

Implication:

- plaintext WAL checksum behavior remains for plaintext format
- encrypted format uses authenticated encryption instead of additive-style checksum validation for encrypted frames/pages

---

## Public Surface Changes

### Engine and storage options

Add a small public configuration surface for encryption:

- `DatabaseEncryptionOptions`
- `StorageEngineOptions.Encryption`
- `StorageEngineOptionsBuilder.UseEncryption(...)`

Recommended fields:

- `RequireEncrypted`
- `Passphrase`
- `KdfIterations`

### Export and migration

Keep the current `SaveToFileAsync(string path)` behavior as the default preserve-current-format path.

Add explicit save/export options for format transitions:

- `DatabaseSaveOptions`
- `Database.SaveToFileAsync(string path, DatabaseSaveOptions options, ...)`
- `CSharpDbConnection.SaveToFileAsync(string path, DatabaseSaveOptions options, ...)`

That API becomes the migration path for:

- plaintext -> encrypted
- encrypted -> plaintext
- encrypted -> encrypted with a new passphrase

### ADO.NET connection strings

Extend `CSharpDbConnectionStringBuilder` with:

- `Encrypt=true|false`
- `Password=<passphrase>`
- `Kdf Iterations=<int>` optional

Recommended semantics:

- `Encrypt=true` + new file creates encrypted storage
- `Encrypt=true` + encrypted file opens encrypted storage
- `Encrypt=true` + plaintext file fails with migration-required behavior
- `Encrypt=false` or unset opens plaintext only
- `Password` without `Encrypt=true` is invalid configuration

### CLI local/direct usage

The CLI currently treats local access as a path/endpoint decision. For encrypted local usage, it should support explicit configuration without placing secrets in the positional path.

Recommended additions:

- `--connection-string`
- `--password-env <NAME>`

Interactive prompting is not required for v1.

### Connection pooling and direct client behavior

Pooled `Database` instances must not be shared across different encryption settings for the same file path.

Pool identity should include:

- normalized file path
- encryption enabled/disabled
- non-secret fingerprint of the effective encryption configuration

Direct client open behavior should also carry full direct-open configuration, not only the normalized file path.

---

## Compatibility and Migration Policy

### Supported coexistence

The engine should support both:

- existing plaintext databases
- new encrypted databases

### Required behaviors

- plaintext v1 opens unchanged when encryption is not requested
- encrypted v2 requires a passphrase and fails fast when the passphrase is missing or wrong
- `Encrypt=true` against an existing plaintext database does **not** auto-upgrade the file
- the user must explicitly export/migrate the database into encrypted storage

### Why explicit migration

Automatic rewrite-on-open is too risky because it combines:

- format conversion
- secret handling
- WAL/recovery interaction
- file replacement

into a single open path. Export/migration keeps the risky transformation explicit and testable.

---

## Managed Local Rollout Phases

### Phase 1: Storage format and crypto plumbing

- introduce format detection and runtime format descriptors
- add encrypted file/WAL header and payload encoding
- integrate authenticated page/frame reads and writes into pager and WAL paths
- keep plaintext v1 behavior untouched

### Phase 2: Managed local public surfaces

- add engine/storage encryption options
- add ADO.NET connection string support
- update direct client resolution/open behavior
- update API/MCP local configuration
- update CLI local/direct configuration

### Phase 3: Migration and diagnostics

- add explicit save/export migration options
- support plaintext<->encrypted and rekey export flows
- update storage inspectors to recognize encrypted files
- return “encrypted database, key required” instead of false corruption reports when appropriate

### Phase 4: Native/FFI follow-up

- extend native open APIs with encryption settings
- update language wrappers after the managed format is stable

---

## Validation and Testing

Minimum validation matrix:

- create encrypted DB, write data, reopen, and verify reads
- verify encrypted collections round-trip the same way as SQL tables
- verify encrypted WAL recovery after crash-style reopen
- verify checkpointing from encrypted WAL to encrypted DB file
- verify wrong password and missing password failures
- verify plaintext v1 files still open and behave unchanged
- verify plaintext -> encrypted export
- verify encrypted -> plaintext export
- verify encrypted -> encrypted export with a new passphrase
- verify `LoadIntoMemoryAsync` from encrypted files with correct settings
- verify connection pooling separation by encryption settings
- verify diagnostics behavior with and without key material

Performance regression checks should cover:

- encrypted point reads
- encrypted commit path
- encrypted checkpoint path
- encrypted reopen/recovery

---

## Non-Goals

This roadmap item does not cover:

- network encryption
- authentication/authorization
- multi-tenant secret management
- platform-specific secret vault integration
- Native/FFI support in the first milestone

---

## Recommended Positioning in the Project

This should be treated as a **Long-Term roadmap item in Research status** until the storage-format work is ready to begin.

The key design choice is:

- build a shared storage-format layer used by pager, WAL, export, and diagnostics

Not:

- add independent encryption wrappers in isolated subsystems

That shared format layer is what keeps at-rest encryption coherent across the project.

---

## See Also

- [Roadmap](../roadmap.md)
- [Storage Engine Guide](../storage/README.md)
- [Storage Architecture](../tutorials/storage/architecture.md)
