# Migration JSON Retained Package

## Status And Scope

This document freezes the fourth Track 4B contract: a portable, immutable
snapshot package that can be reopened without the caller's original JSON or
NDJSON path.

It builds on
[`migration-json-reader-foundation.md`](migration-json-reader-foundation.md),
[`migration-json-table-schema.md`](migration-json-table-schema.md), and
[`migration-json-data-source.md`](migration-json-data-source.md). Later slices
have added typed sidecars and package v2, export, and fail-fast or
deterministic-reject CLI routing without changing this package-v1 contract.
Collection projection remains outside its scope.

## Public Contract

`JsonSnapshotPackage.WriteAsync` publishes one `.csdbjson` file from a verified
`JsonSourceSnapshot`, its exact `JsonTableSchemaInferenceResult`, and a target
CSharpDB version. It does not take ownership of the caller's snapshot and never
overwrites an existing destination.

`JsonSnapshotPackage.OpenAsync` verifies the retained package and returns a
`JsonSnapshotPackageSession`. The session exposes the reconstructed manifest,
schema, catalog, and `JsonMigrationDataSource` while privately owning a fresh
snapshot copy. Disposing a session never removes or changes the durable
package.

An open can be pinned with an independently retained
`ExpectedManifestDigest`. The pin is a canonical lowercase SHA-256 identifier
and is checked against the header before manifest allocation or workspace
creation. This detects whole-package replacement only when the expected digest
is kept outside the package.

## Single-File Format

Package format `csharpdb-json-snapshot-package/v1` has three contiguous
sections:

1. a fixed 64-byte big-endian header;
2. a bounded canonical manifest; and
3. the exact immutable source bytes.

The header contains the eight-byte magic `CSDBJSN1`, version `1`, header size
`64`, manifest length, zero flags, raw-source length, and SHA-256 of the exact
manifest bytes. The physical file length must exactly equal the sum of the
three sections. Unsupported fields, arithmetic overflow, gaps, truncation, or
trailing bytes fail closed.

The manifest is strict UTF-8 JSON without a BOM. Its self-digesting envelope
uses fixed property order and carries format, digest algorithm, digest, and a
typed payload. The canonical codec fixes literal Unicode, control escaping,
lowercase hexadecimal, number spelling, enum spelling, and property order.
Reopen rejects duplicate, unknown, mis-cased, reordered, explicitly null, or
noncanonical properties; comments; trailing commas; NUL; invalid UTF-8;
integer enums; excessive depth or size; and credential-shaped retained text.

The outer and inner SHA-256 values detect corruption and component mixing.
They are not signatures: a party able to replace the whole package can
calculate new unkeyed hashes.

## Retained Semantic Recipe

The manifest stores only the information needed to reproduce interpretation:

- snapshot byte length, SHA-256, and `json-snapshot-v1` identity;
- safe source identity, source fingerprint, and reader-options digest;
- framing plus every reader resource ceiling;
- the JSON source-binding, schema, scalar, and ordered-canonical-value policy
  identifiers;
- profile collection choice and bounds;
- table name and normalized ordinal overrides, including expected property
  name, logical type, nullability, and missing-value policy;
- target CSharpDB version; and
- canonical migration-catalog digest.

The original source path and an unhashed logical identity are never retained.
Inferred columns, sampled values, diagnostics, cursors, plans, receipts, and
catalog bytes are not trusted conclusions in the manifest.

Reopen streams the raw section into a fresh private `JsonSourceSnapshot`,
recomputes the source binding, replays schema inference from the retained
bytes, rebuilds the catalog, and compares every retained identity and digest.
Changing framing, a reader ceiling, inference sampling, override order or
policy, target version, or catalog policy therefore fails before the source
can be used.

## Publication And Reopen Safety

Writing validates the snapshot, source binding, inferred schema, and catalog
relationship before publication. It creates a unique sibling temporary file
without overwrite, writes and rehashes every section, flushes the file through
the operating system, and atomically renames it to the absent destination.
Cancellation or failure before the rename removes only the temporary file
created by that operation. After a successful rename, the complete package is
preserved even if cancellation acknowledgement would otherwise be ambiguous.

This provides absent-or-complete publication on filesystems that implement an
atomic same-directory rename. It does not claim parent-directory `fsync`
durability or identical network-filesystem semantics.

Reopen accepts only a regular final path using operating-system no-follow
semantics. It validates the fixed header, section arithmetic, optional trusted
pin, and source-size policy before allocating the manifest or creating a
workspace. The raw section is copied through a fixed-size buffer into a newly
owned snapshot and hashed while copying. Every failure removes only
package-owned temporary state and preserves the durable package.

A caller-supplied workspace parent is a trust boundary and must remain under
the caller's control while the session is alive. Each successful open owns an
independent private snapshot. Once open returns, deleting, renaming, or
changing the durable package cannot affect that session's reads, batching,
reject outcomes, or cursor replay.

## Stable Failures

Package failures use JSON-specific stable rules:

- `MIG-JSON-PACKAGE-FORMAT-001` for unsupported or noncanonical structure;
- `MIG-JSON-PACKAGE-INTEGRITY-001` for digest, identity, or trusted-pin
  mismatch;
- `MIG-JSON-PACKAGE-POLICY-001` for semantic recipe or replay drift;
- `MIG-JSON-PACKAGE-LIMIT-001` for bounded-size failures; and
- `MIG-JSON-PACKAGE-PATH-001` for unsafe or unsupported package paths.

Exception text contains no raw source values. Digest comparisons use
fixed-time comparison.

## Qualification

The merge gate covers both root-array and multiple-value/NDJSON framing:

- deterministic package bytes and manifests;
- reopen after the original input changes or is deleted;
- complete inference sampling, override, missing/null, reader-limit, source,
  schema, and catalog replay;
- independent sessions, batch boundaries, cursors, and cross-session suffix
  resume;
- trusted-pin validation before workspace creation;
- header, manifest, raw-byte, truncation, and trailing-byte tamper;
- exact source-size limits and fixed copy buffers;
- caller-snapshot and durable-package ownership;
- cancellation, cleanup, and idempotent disposal; and
- strict canonical-manifest and privacy checks.

Fresh-process qualification writes in one process, deletes the original
input, pin-opens and reads in a second process, then independently resumes an
earlier cursor in a third process for both framing modes. CLI qualification now
also covers fail-fast inspect, plan, apply, an independent resume command, and
validation from independently pinned package-v1 manifests for root-array JSON
and NDJSON. The deterministic CLI route additionally covers bounded late-tail
type rejects, canonical artifact publication/reuse, resume, and pre-activation
validation for both framings. Large generated streams and broader
header/manifest mutation fuzzing remain release qualification.

## Deferred Work

Version 1 itself does not add overwrite, repair, in-place upgrade, signatures,
encryption, compression, deduplication, direct reads from the durable package,
remote/object storage, automatic retention cleanup, embedded plans or
receipts, typed sidecars, collection projection, or typed-v2 reject CLI
policy. Typed intent and fail-fast package-v2 CLI routing use the separate,
explicitly selected v2 contract.
