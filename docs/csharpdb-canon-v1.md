# `csharpdb-canon-v1`

`csharpdb-canon-v1` is the byte-level logical row contract used by migration
validation. Its purpose is to give independent implementations exactly one
cross-platform representation to hash. It does not depend on database page
layout, source-provider storage, the current culture, or host endianness.

## Row and key domains

Every integer below is unsigned big-endian unless its description says it is
signed. A canonical row is:

```text
ASCII("CSDBCAN1")                                      8 bytes
SHA256(UTF8("csharpdb-canon-v1"))                    32 bytes
field-count                                             u32
field-0 ... field-N
```

The raw contract hash is
`8a323b42ac39d6faa2a8609c88143f5e78f613fb2b73cb2947ac50bf35ee616a`.
The row hash is the lowercase hexadecimal SHA-256 digest of the complete row
above.

A key hash has a separate domain:

```text
SHA256(ASCII("CSDBKEY1") || canonical-key-row)
```

`canonical-key-row` is a complete canonical row envelope whose fields are the
key fields in declared key order. A key hash can therefore never equal the row
hash of the same fields by accident.

## Field frame and states

Each field is framed independently:

```text
type-tag        u8
state           u8
payload-length  u64
payload         payload-length bytes
```

The registered states are:

| State | Byte | Payload |
| --- | ---: | --- |
| value | `00` | The canonical logical payload for the type. |
| null | `01` | Empty. The payload length must be zero. The type tag remains significant. |
| excluded | `02` | Exactly one registered exclusion-reason byte. |

The sole v1 exclusion is reason `01`, `regenerated-rowversion`. It is valid
only with the BLOB tag (`08`). The encoder must emit only that reason byte and
must not read or hash the regenerated rowversion value. Unknown reasons and
exclusions on any other type are invalid.

## Value tags and payloads

| Tag | Logical type | Canonical payload |
| ---: | --- | --- |
| `01` | Boolean | Exactly one byte, `00` or `01`. |
| `02` | Int64 | Exactly 8 bytes, signed two's-complement big-endian. |
| `03` | UInt64 | Exactly 8 bytes, unsigned big-endian. |
| `04` | Decimal | Scale as u32, then the minimal signed two's-complement big-endian coefficient. |
| `05` | Binary32 | Exactly 4 bytes, IEEE 754 binary32 in network byte order. |
| `06` | Binary64 | Exactly 8 bytes, IEEE 754 binary64 in network byte order. |
| `07` | Text | Strict UTF-8 bytes, with no Unicode normalization. |
| `08` | BLOB | Exact bytes. |
| `09` | GUID | Exactly 16 RFC/network-order bytes. |
| `0a` | Date | Signed int32 days from `1970-01-01`. |
| `0b` | Time | UInt64 nanoseconds since midnight. |
| `0c` | Wall date-time | Date payload followed by time payload (4 + 8 bytes). |
| `0d` | UTC instant | Signed int64 Unix seconds followed by u32 fractional nanoseconds (8 + 4 bytes). |
| `0e` | Offset date-time | Local date, local time, then signed int16 offset minutes (4 + 8 + 2 bytes). |

Decimal represents `coefficient * 10^-scale`. While scale is greater than
zero, trailing decimal zeroes are removed from the coefficient and scale
together. Every zero is encoded with scale zero and the one-byte coefficient
`00`. A nonzero coefficient has no redundant leading `00` or `ff` sign
extension. For example, `123.4500` becomes scale 2 and coefficient 12345.

Binary32 and binary64 accept finite values only. Both negative-zero bit
patterns normalize to positive zero; all other finite IEEE bit patterns are
preserved at their declared width. NaN and infinities are invalid.

Text must be a valid Unicode scalar sequence encodable by strict UTF-8.
Ill-formed surrogate sequences are invalid. Canonicalization does not perform
NFC, NFD, case folding, trimming, newline conversion, or collation processing.

GUID bytes use the RFC field order, not the mixed-endian byte order returned
by legacy `Guid.ToByteArray()` APIs.

Temporal values use the proleptic Gregorian calendar, allow at most nine
fractional-second digits, and do not admit leap-second notation. Time is in
the range 0 through `86,400,000,000,000 - 1` nanoseconds. A wall date-time has
no implicit zone. A UTC instant uses floor division for pre-epoch values, so
`1969-12-31T23:59:59.5Z` is seconds `-1` plus 500,000,000 nanoseconds. An
offset date-time preserves its local civil date/time and signed offset rather
than normalizing to UTC.

## Planned logical projection

Canonical rows are ordered by the included column object IDs used by the
reviewed migration plan. A primary-key row uses the key's declared member
order. The per-object contract digest binds that field order, every source and
target column identity, the stored CSharpDB type, the logical canonical tag,
the registered conversion and its parameters, exclusions, and key ordinals.
Changing any of those facts therefore changes the object checksum domain.

Validation hashes planned logical values, not merely the target's physical
storage tags. The v1 projections are:

- BOOLEAN stored as INTEGER is restored to tag `01` and must be exactly 0 or 1.
- Signed and unsigned integers use tags `02` and `03`; invariant TEXT used for
  a preserved UInt64 is parsed back to UInt64 before hashing.
- Scaled INTEGER and canonical invariant TEXT decimal encodings are restored
  to the arbitrary-precision tag `04` representation.
- A deliberately lossy numeric-to-REAL mapping is bound as binary64 (`06`),
  so validation proves the planned lossy result rather than claiming an exact
  source decimal or UInt64 comparison.
- GUID, date, time, wall date-time, UTC-instant, and offset-date-time TEXT
  codecs are parsed back to tags `09` through `0e`.
- JSON or XML is tag `07` only when the plan explicitly maps it to TEXT. There
  is no implicit JSON normalization. Unresolved native/vendor values have no
  v1 tag and validation rejects them.
- A planned regenerated rowversion emits the registered excluded BLOB frame;
  its physical bytes are never read into the canonical hash.

Typed NULL retains the field's planned logical tag. Projection rejects a
stored tag, value domain, decimal syntax, text codec, temporal precision, or
non-finite real that contradicts the object contract.

## Native CSharpDB row-layout digest

Native archive restore validation uses a separate SHA-256 aggregate contract
domain, `ASCII("CSDBNAT1")`. The digest binds the raw `csharpdb-canon-v1`
contract hash, the ordered field count, and each field's name, stored type,
canonical type, and optional exclusion reason. It also binds the primary-key
field ordinals in their declared order. Changing a hash-relevant column name,
field order, type, exclusion, or key order therefore changes the digest.

`CSDBNAT1` deliberately omits the table name and source/target object identity.
An archive table and the temporary staging table created for it consequently
share a digest even though staging has a generated name. This rename stability
does not make the digest a row encoding, an archive-integrity checksum, or a
replacement for full schema validation. Nullability, defaults, identity state,
indexes, and other schema facts are checked separately during restore.

## Streaming requirement

Hashing implementations must stream the exact envelope and payload bytes into
SHA-256. They must not require a row-sized encoded buffer. The CSharpDB
implementation uses one `IBufferWriter<byte>` serialization core for both the
streaming `IncrementalHash` path and the test/diagnostic `EncodeRow` path, with
a fixed 4 KiB scratch buffer for production hashing.

The normative cross-platform vectors are in
`tests/CSharpDB.Migration.Tests/Fixtures/csharpdb-canon-v1.golden.json`. They
cover every registered v1 logical type tag, typed NULL, the registered
rowversion exclusion, an empty row, and representative normalization edges.
