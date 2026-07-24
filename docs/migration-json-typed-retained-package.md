# Migration JSON Typed Retained Package

## Status And Scope

This document freezes the seventh Track 4B contract: a portable, immutable
typed JSON snapshot package that carries the exact source-bound intent needed
to reconstruct typed object-table migration after the original source and
sidecar are unavailable.

It builds on
[`migration-json-retained-package.md`](migration-json-retained-package.md),
[`migration-json-typed-intent.md`](migration-json-typed-intent.md), and
[`migration-json-typed-table.md`](migration-json-typed-table.md). Package v1,
untyped schema and cursor contracts, collection projection, export, and CLI
routing remain unchanged.

## Public Contract

`JsonTypedSnapshotPackage.WriteAsync` publishes one `.csdbjson` file from a
verified `JsonSourceSnapshot`, its exact
`JsonTypedTableSchemaInferenceResult`, and a target CSharpDB version. It does
not accept an untyped schema and does not take ownership of the caller's
snapshot or typed result.

`JsonTypedSnapshotPackage.OpenAsync` returns a
`JsonTypedSnapshotPackageSession`. The session exposes:

- the package manifest and exact intent-manifest digest;
- the reparsed `JsonTypedIntentManifest`;
- the replayed typed schema and catalog; and
- a typed `JsonMigrationDataSource` backed by a private snapshot copy.

The typed session and schema are distinct from package v1 types. They cannot
be passed through the untyped retained-package API or silently lose intent.
Both package versions use `.csdbjson`; callers select an explicit API, and the
header magic rejects the other version.

`JsonSnapshotPackageOpenOptions.ExpectedManifestDigest` remains the external
trust pin. It is the lowercase SHA-256 of the exact canonical v2 package
manifest and is checked from the header before manifest or intent allocation
and before workspace creation. Hashes provide integrity and replacement
detection only when the pin is retained independently; they are not
signatures.

## Four-Section Format

Format `csharpdb-json-snapshot-package/v2` has four contiguous sections:

1. a fixed 112-byte big-endian header;
2. a bounded canonical v2 package manifest;
3. the exact canonical `csharpdb-json-table-intent/v1` envelope bytes; and
4. the exact immutable source bytes.

The header layout is:

| Offset | Size | Meaning |
| ---: | ---: | --- |
| 0 | 8 | ASCII magic `CSDBJSN2` |
| 8 | 4 | version `2` |
| 12 | 4 | header size `112` |
| 16 | 4 | package-manifest byte length |
| 20 | 4 | typed-intent byte length |
| 24 | 4 | flags, fixed at zero |
| 28 | 4 | reserved, fixed at zero |
| 32 | 8 | raw-source byte length |
| 40 | 32 | SHA-256 of exact package-manifest bytes |
| 72 | 32 | SHA-256 of exact typed-intent bytes |
| 104 | 8 | reserved, fixed at zero |

Section arithmetic is checked for overflow, and the physical file length must
equal the exact sum. Gaps, overlap, truncation, trailing data, nonzero reserved
fields, unsupported flags, and either version's wrong magic fail closed.

The package manifest retains the source snapshot and safe source identity,
every reader ceiling, the representation and typed schema/scalar contracts,
the canonical nested-value contract, the sidecar/typed-value/text-codec
contracts, inference policy, typed-intent byte length/digest/limits/count,
target version, and exact typed catalog digest.

Only caller-supplied ordinary schema overrides are retained in the inference
recipe. Synthetic overrides derived from typed declarations are verified
during write, removed from the retained ordinary list, and re-synthesized
exactly once during reopen. Ordinary and typed ordinals cannot overlap.

## Publication And Reopen

Write validates the typed schema, exact intent bytes, snapshot binding, typed
catalog, synthetic overrides, and section limits before publication. It
writes a unique sibling temporary file, rehashes the raw snapshot while
copying, flushes the complete file through the operating system, and performs
one atomic no-overwrite rename. Failure before rename removes only the
operation-owned temporary file; successful publication is preserved.
The destination parent is an operator-owned publication boundary and must
remain under the caller's exclusive control until the operation and any
cleanup finish. This excludes hostile pathname replacement inside that
directory from the package threat model.

Open uses a no-follow regular-file handle and validates header geometry,
resource bounds, exact physical length, the optional external manifest pin,
the canonical package manifest, and the canonical intent section before it
creates a workspace. It then copies the raw section into a fresh private
snapshot, restores the reader/source binding, reparses the embedded intent
against that exact binding and digest, replays typed inference, rebuilds the
typed catalog, and compares the retained digest before constructing the
session.

Each successful open owns an independent private snapshot. Deleting, renaming,
or changing the durable package after open cannot affect that session.
Disposal removes only session-owned temporary state and never changes the
durable package.

## Bounds And Stable Failures

The package manifest remains bounded to 16 MiB and depth 64. The exact intent
section must be between one byte and the sidecar's 4 MiB maximum and retains
the sidecar's one-million-character, 16,384-column, binary, and decimal safety
ceilings. Raw source size and copy-buffer bounds come from
`JsonSnapshotPackageOpenOptions`.

Package v2 reuses the stable package rules:

- `MIG-JSON-PACKAGE-FORMAT-001` for unsupported or noncanonical structure;
- `MIG-JSON-PACKAGE-INTEGRITY-001` for section digest, identity, or trusted-pin
  mismatch;
- `MIG-JSON-PACKAGE-POLICY-001` for retained contract or replay drift;
- `MIG-JSON-PACKAGE-LIMIT-001` for bounded-size failures; and
- `MIG-JSON-PACKAGE-PATH-001` for unsafe or unsupported paths.

Exception text retains no raw source values. Digest comparisons use fixed-time
comparison, and temporary byte buffers containing manifests or intent are
cleared before release.

## Qualification

The merge gate covers both root-array and multiple-value/NDJSON framing:

- deterministic package bytes and exact package/intent digests;
- reopen after original source and sidecar mutation or deletion;
- ordinary-override replay with typed overrides synthesized once;
- all typed codecs, undeclared columns, planning, apply conversion, rejects,
  batches, and v2 cursor suffix replay across independent sessions;
- private-copy ownership after durable package deletion;
- trusted-pin validation before workspace creation;
- header, manifest, intent, raw-byte, semantic-replay, truncation, and trailing
  data tamper;
- exact and one-under source limits plus bounded manifest and intent section
  geometry;
- cancellation, path safety, cleanup, and idempotent disposal;
- v1/v2 header, API, schema, and cursor isolation; and
- fresh-process write, pin-open, and independent cursor resume.

## Deferred Work

Version 2 does not add overwrite, repair, in-place upgrade, signatures,
encryption, compression, direct reads from the durable package, automatic
sidecar discovery, embedded plans or receipts, collection projection, export,
or CLI routing.
