# Migration JSON Typed Intent Sidecar

## Status And Scope

This document freezes the fifth Track 4B contract: a canonical, source-bound
sidecar for JSON scalar intent that native JSON cannot express by itself.

The sidecar declares how selected object-table columns are represented as
binary, decimal, GUID, date/time, or full-width integer values. It is a
standalone artifact bound to the exact immutable source and reader policy. It
does not change those behaviors merely by existing and is never discovered
implicitly. Later typed-schema and package-v2 contracts explicitly bind it
into inference, row projection, cursors, retained migration, and the fail-fast
CLI route.

The existing `csharpdb-json-snapshot-package/v1` format remains byte- and
behavior-compatible. Applying typed intent uses the separate typed schema,
scalar, cursor, and retained-package-v2 contracts.

## Public Contract

`JsonTypedIntentSidecar.WriteAsync` publishes one
`.csdbjson-intent.json` file from a verified `JsonSourceBinding` and normalized
`JsonTypedIntentOptions`. Publication never overwrites an existing
destination.

`JsonTypedIntentSidecar.OpenAsync` accepts only a regular final path, verifies
the canonical artifact, and requires the supplied source binding to match
every retained source fact. An optional independently retained
`ExpectedManifestDigest` pins the exact canonical sidecar bytes and detects
whole-artifact replacement.

`JsonTypedIntentSidecar.Parse` applies the same canonical, digest, policy, and
source checks to caller-owned bytes. `JsonTypedIntentManifest` exposes the
safe source facts, normalized limits and declarations, its exact-byte digest,
and a defensive copy of the canonical bytes.

## Canonical Format

Format `csharpdb-json-table-intent/v1` is strict UTF-8 JSON without a BOM. Its
fixed-order envelope contains `format`, `digestAlgorithm`, `digest`, and
`payload`. The inner lowercase SHA-256 is calculated over the canonical
`format`, `digestAlgorithm`, and `payload` object. `ManifestDigest` is a
separate `sha256:<hex>` identifier over the exact canonical envelope bytes.

The payload contains, in fixed order:

- versioned source-binding, reader-options, property-name, typed-value, and
  text-codec contracts;
- the source snapshot identity, content digest and length, safe source
  identity, fingerprint, and reader-options digest;
- decoded binary and decimal-digit limits; and
- normalized column declarations in strictly increasing ordinal order.

Canonical parsing rejects a BOM, NUL, invalid UTF-8, whitespace, comments,
trailing commas, excessive depth or size, duplicate or unknown properties,
mis-cased or reordered properties, explicit nulls, integer enum values,
noncanonical digest text, credential-shaped retained text, and any
semantically invalid declaration.

The sidecar never retains a source path, unhashed logical identity, source
values, samples, defaults, target details, plans, cursors, or credentials.
Its hashes detect accidental or local tampering; they are not signatures.

## Column Declarations

Each declaration binds a zero-based source column ordinal to the exact decoded
property name using Unicode ordinal comparison. Ordinals are unique and
strictly increasing. A declaration carries one representation codec, optional
nullability intent, and a missing-property policy. Decimal declarations also
carry required precision and scale.

Version 1 codecs are:

| Codec | Required JSON representation | Typed meaning |
| --- | --- | --- |
| `BinaryBase64` | Exact padded RFC 4648 base64 string | Raw binary |
| `DecimalString` | Canonical fixed-point string | Exact decimal |
| `DecimalNumber` | Canonical fixed-point JSON number | Exact decimal |
| `GuidD` | Exact lowercase `D` string | GUID |
| `DateCSharpDbText` | Exact versioned text-codec string | Date |
| `TimeCSharpDbText` | Exact versioned text-codec string | Time |
| `DateTimeCSharpDbText` | Exact versioned text-codec string | Wall-clock date/time |
| `DateTimeOffsetCSharpDbText` | Exact versioned text-codec string | Date/time with numeric offset |
| `Int64String` | Exact invariant signed integer string | Signed 64-bit integer |
| `UInt64String` | Exact invariant unsigned integer string | Unsigned 64-bit integer |

All string codecs validate by parsing and formatting back to the exact original
text. They do not accept alternate spellings, whitespace, URL-safe base64,
unpadded base64, GUID case or shape variants, exponent notation, `Z` in place
of a numeric offset, or otherwise equivalent but noncanonical values.

Decimal precision is at least one, scale is between zero and precision, and
both are forbidden for non-decimal codecs. Binary and decimal safety ceilings
are retained as part of the manifest so future interpretation cannot silently
change resource policy.

## Source Binding And Trust

The manifest binds:

- exact raw content length and SHA-256;
- immutable snapshot identity;
- safe content-derived or hashed logical source identity;
- safe source fingerprint;
- normalized reader-options digest; and
- versioned JSON source and property-name semantics.

Opening the same sidecar against different bytes, framing, reader ceilings, or
logical source identity fails before the declarations can be used. A sidecar
may describe only the binding supplied at publication; it cannot act as a
portable type dictionary detached from a source.

An unpinned artifact can protect its own internal consistency but cannot
distinguish a coherently replaced sidecar. Callers that cross a trust boundary
must keep and supply `ExpectedManifestDigest` independently.

## Publication And File Safety

The sidecar is bounded to 4 MiB, JSON depth 64, 16,384 declarations, and
1 MiB of retained decoded text. Default decoded-binary and decimal-digit
limits are part of the public options and remain subject to absolute bounds.

Writing validates and canonicalizes the complete artifact before creating a
unique sibling temporary file. It writes, flushes, and atomically renames that
file to an absent destination. Cancellation or failure before publication
removes only the temporary file owned by that operation.

Opening uses operating-system no-follow semantics and accepts only a regular
final file. Links, directories, devices, named pipes, oversized files, unsafe
paths, and unsupported secure-open platforms fail closed.

## Stable Failures

Sidecar failures use JSON-specific stable rules:

- `MIG-JSON-INTENT-FORMAT-001` for unsupported or noncanonical structure;
- `MIG-JSON-INTENT-INTEGRITY-001` for digest or trusted-pin mismatch;
- `MIG-JSON-INTENT-SOURCE-001` for a different source binding;
- `MIG-JSON-INTENT-POLICY-001` for an invalid declaration or contract;
- `MIG-JSON-INTENT-LIMIT-001` for bounded-resource failures; and
- `MIG-JSON-INTENT-PATH-001` for unsafe or unsupported paths.

Exception text does not include property names, source values, or credentials.
Digest comparisons use fixed-time comparison.

## Integration And Deferred Work

Version 1 of the standalone sidecar does not inspect whether a declaration
matches a discovered column or decode values. The completed typed integration
layers:

- bind the manifest digest into typed schema, scalar, catalog, cursor, plan,
  and reject contracts with new version identifiers;
- require each declared ordinal and exact name to match the full discovered
  object-table shape;
- decode typed values with deterministic row-local failures and fatal resource
  limits;
- add a retained package v2 that embeds the exact canonical intent artifact;
  and
- consume the explicitly supplied, independently pinned sidecar during typed
  CLI inspect, then replay its embedded bytes from the independently pinned
  package during fail-fast apply/resume and validation.

Package v1 will never infer, embed, or silently apply typed intent.
Typed export-intent generation, CLI sidecar authoring or discovery,
collection projection, and typed deterministic-reject CLI qualification
remain deferred.
