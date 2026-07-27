# Retained migration packages

`CSharpDB.Migration.Retained` stores a provider-neutral catalog and its
deterministically ordered source rows in one replayable package. Provider
adapters capture the source; the normal migration planner, apply runner, and
validator consume the verified catalog and `RetainedMigrationDataSource`.

## Safe lifecycle

1. Write the package with `RetainedMigrationPackageWriter`.
2. Record the returned whole-package SHA-256 digest in trusted storage separate
   from the package.
3. Open it with `RetainedMigrationPackageSession.OpenAsync`, supplying that
   trusted digest and resource limits appropriate for the migration.
4. Use `session.Catalog` and `session.DataSource` together. Do not mix either
   one with another package.
5. Dispose the session and treat any cleanup exception as actionable.

Opening copies the package into a per-session workspace, verifies the trusted
whole-package digest, validates the embedded catalog and bindings, and checks
every table section before exposing rows.

## Sensitive plaintext

Packages and their temporary/session copies contain source row values in
plaintext. They may contain customer data, credentials stored as ordinary
database values, or other regulated material. Keep output and workspace
directories access-controlled, use encrypted storage where required, avoid
shared temporary directories, and securely remove retained artifacts according
to the applicable data-retention policy. The format provides integrity checks,
not encryption.

## Resume cursors

Resume cursors detect accidental modification and are bound to the package
digest, catalog, source identity, snapshot, table, requested projection, and
batch policy. A cursor is accepted only at a batch boundary produced by that
same read policy.

Cursor hashes are not keyed and do not authenticate who created a checkpoint.
Store checkpoints in trusted, access-controlled target state. Do not accept a
cursor supplied or modified by an untrusted party.

## Bounds

Both writing and opening require explicit ceilings for package, manifest,
catalog, table, column, row, value, stable-key, and buffer sizes. Defaults are
upper compatibility limits, not a sizing recommendation. Set tighter values
when the expected source size is known.
