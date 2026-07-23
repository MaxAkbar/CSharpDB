# Migration CSV Retained Package

This note records the fifth Phase 4A implementation slice. It separates CSV
inspection from a later apply process without reopening the caller's original
path or weakening the snapshot, parser, schema, and catalog bindings described
in the earlier CSV notes.

## Single-File Trust Boundary

`CsvSnapshotPackage.WriteAsync` publishes one immutable `.csdbcsv` file. The
package contains a fixed versioned header, a bounded canonical manifest, and
the exact raw snapshot bytes. It never stores the original CSV path or an
unhashed logical source name. A manifest cannot select another file, so reopen
has no relative-path traversal, sidecar replacement, or manifest/payload
generation-mixing surface.

The 64-byte big-endian header contains magic, version, header size, a zero
flags field, manifest length, snapshot length, and SHA-256 of the exact
manifest bytes. Section offsets are derived from those lengths. A reader
requires the physical file length to equal the header, manifest, and snapshot
sum exactly; gaps, truncation, and trailing bytes fail closed.

The manifest uses the same compact-envelope pattern as the shared migration
artifacts. It has a second SHA-256 over its format, digest algorithm, and typed
payload. Strict UTF-8 and JSON parsing reject a BOM, NUL, invalid encoding,
comments, trailing commas, duplicate or unknown properties, integer enums,
noncanonical property order or spelling, excessive depth or size, and
credential-shaped content. SHA-256 comparisons use fixed-time comparison.
Canonical bytes are produced by the package codec itself rather than a
runtime HTML or Unicode encoder table: property order is explicit, control
escapes and lowercase hexadecimal are fixed, and all other valid Unicode is
written as strict UTF-8. The same bytes are required again on reopen.

These hashes detect corruption and mismatched components; they are not a
signature. A party able to replace the entire package can calculate new
unkeyed hashes. Plans, catalogs, receipts, and the trusted package location
remain the execution anchors.

Callers that retain the writer's `ManifestDigest` can pass it back as
`ExpectedManifestDigest`. Reopen compares that trusted value in fixed time
against the header before allocating the manifest or copying raw bytes. This
turns whole-package replacement into an early failure as long as the expected
digest is stored independently of the package.

## Atomic Publication

Writing first validates the exact snapshot/schema/catalog relationship. A
unique sibling temporary file is created without overwrite, with user-only
mode on Unix. On Windows the package inherits its parent directory's ACL, so
the caller must select a trusted, access-controlled destination directory.
That publication directory must also remain under the caller's control while
the sibling temporary file is written and renamed.
Header, manifest, and raw snapshot are written while the source bytes are
rehashed, then the file is flushed through the operating system and moved to
the requested name without overwrite. Cancellation or failure before the move
removes only a temporary file whose `CreateNew` open succeeded. After the
move, the complete package survives even if cancellation acknowledgement would
otherwise be ambiguous.

This gives an absent-or-complete final path on filesystems that provide atomic
same-directory rename. It does not claim parent-directory `fsync` durability
or identical network-filesystem semantics on every platform.

## Verified Reopen

`CsvSnapshotPackage.OpenAsync` opens one regular package handle with
operating-system no-follow semantics for the final path component. Unix opens
are initially nonblocking so
a FIFO or device cannot stall before same-handle type validation; Windows
opens the reparse point itself so links and junctions can be rejected.
After the handle is accepted, reopen validates all header arithmetic before
allocation, verifies and parses the manifest, then streams exactly the raw
section into a new private `CsvSourceSnapshot`. Its workspace directory is
claimed with one operating-system exclusive-create operation, uses user-only
mode on Unix and a protected current-user ACL on Windows, and has a random
ownership marker. A caller-supplied workspace parent is a trust boundary: it
must remain controlled by the caller and cannot be writable, renamed, or
replaced by an untrusted principal while the session is alive. Cleanup
rechecks the marker and removes only registered regular immediate children
with nonrecursive operations; an unowned child, directory, device, or link is
preserved and reported. The package handle is closed before a session is
returned.
Later migration reads use only that fresh private copy, so renaming, deleting,
or changing the retained package cannot affect an already opened session.

Reopen then reconstructs and verifies every semantic layer:

- raw length, SHA-256, and `csv-snapshot-v1` identity;
- delimiter, quote, header, null, width, parser limits, configured encoding,
  resolved BOM/encoding, and complete culture-policy digest;
- safe source identity, format/options digest, and source fingerprint;
- the normalized inference recipe, including profile bounds and ordinal
  overrides;
- replayed schema inference and the regenerated migration catalog digest.

Package v1 accepts only named cultures reproducible with user overrides
disabled. An open-time culture override is useful only for platform drift and
must reproduce the exact retained culture-policy digest. Inference is replayed
over the retained bytes instead of trusting serialized type conclusions, so a
sampled schema, explicit normalization rule, late-row rejection, and cursor
scope remain identical across processes.

`CsvSnapshotPackageSession` owns the fresh private snapshot and its
`CsvMigrationDataSource`. Disposing it prevents new reads and deletes only the
ephemeral workspace after active readers close. It never deletes the durable
package. Independent sessions receive independent snapshots and readers.

## CLI Boundary

`csharpdb migrate inspect --source csv` snapshots, inspects, infers, and
publishes the retained package before writing the ordinary migration catalog.
The status line returns the writer's exact `manifestDigest`; it is not placed
in a sidecar or overloaded into the semantic catalog. Existing package paths
are never overwritten. Once package publication succeeds the CLI never
deletes that pathname: if catalog publication then fails, it reports and
preserves the complete package so a concurrent replacement cannot be mistaken
for an owned rollback artifact.

CSV apply, resume, and validation require `--source-package` together with an
independently retained `--expected-manifest-digest`. Reopen checks that exact
container pin before manifest allocation, then the CLI checks the reconstructed
catalog digest, target version, source identity, and adapter policy against the
supplied catalog and plan before creating or opening the staged target. The
package path is also prohibited from colliding with the plan, catalog, target,
target companions, or report. The CLI requires the package parent and optional
workspace to preexist and rejects either trust-boundary directory when it is a
link, reparse point, or device. Ancestor aliases are resolved before role
comparison so they cannot bypass those collision checks. Synthetic migration
syntax remains unchanged.

## Deferred Work

This slice does not add overwrite, repair, in-place upgrade, automatic
retention cleanup, deduplication, signatures, encryption, direct reads from the
durable package, remote/object storage, or package-embedded plans and receipts.
A future plan-artifact version may carry a generic source-container digest so
operators do not need to pass the independently trusted pin again. Strict
large-stream and resource bounds are qualified in
[`migration-csv-performance.md`](migration-csv-performance.md). Tolerant
rejects, typed CSV binary declarations, and export manifests remain separate
work.
