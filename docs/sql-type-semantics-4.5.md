# SQL type semantics in 4.5

CSharpDB 4.5 separates a column's declared SQL type from its compact physical
record representation. This is a logical-semantics change; database pages and
record encodings are unchanged.

## Complete declared type catalog

CSharpDB supports 25 logical SQL type kinds plus the special generated
`ROWVERSION` column declaration. Canonical names are what schema and typed
metadata render; accepted aliases parse to the same logical descriptor.

| Canonical declaration | Accepted aliases | Semantics | ADO.NET value |
|---|---|---|---|
| `BOOLEAN` | `BOOL`, bare `BIT` | Logical Boolean, stored canonically as 0 or 1 | `bool` |
| `TINYINT` | — | Unsigned 8-bit integer, 0 through 255 | `byte` |
| `SMALLINT` | — | Signed 16-bit integer | `short` |
| `INTEGER` | `INT` | Signed 32-bit integer | `int` |
| `BIGINT` | — | Signed 64-bit integer | `long` |
| `REAL` | — | Finite floating point using the stable binary64 engine carrier | `double` (EF Core maps `float`) |
| `DOUBLE PRECISION` | `DOUBLE`, `FLOAT` | Finite IEEE 754 binary64 floating point | `double` |
| `DECIMAL`, `DECIMAL(p)`, `DECIMAL(p,s)` | Corresponding `NUMERIC` forms | Exact fixed-scale decimal; precision 1–18 and scale 0–precision | `decimal` |
| `CHAR`, `CHAR(n)` | `CHARACTER`, `NCHAR` | Unicode text; the faceted form is fixed length and space-padded | `string` |
| `VARCHAR`, `VARCHAR(n)` | `CHARACTER VARYING`, `NVARCHAR` | Unicode text with an optional maximum character count | `string` |
| `TEXT` | `CLOB` | Unfaceted Unicode text | `string` |
| `BINARY`, `BINARY(n)` | — | Bytes; the faceted form is fixed length and zero-padded | `byte[]` |
| `VARBINARY`, `VARBINARY(n)` | — | Bytes with an optional maximum length | `byte[]` |
| `BLOB` | — | Unfaceted binary data | `byte[]` |
| `UUID` | `GUID`, `UNIQUEIDENTIFIER` | Canonical UUID stored as 16 bytes | `Guid` |
| `DATE` | — | Calendar date | `DateOnly` |
| `TIME`, `TIME(p)` | — | Time of day with optional fractional-seconds precision | `TimeOnly` |
| `DATETIME2`, `DATETIME2(p)` | `DATETIME` (without a facet) | Date and time without an offset | `DateTime` |
| `DATETIMEOFFSET`, `DATETIMEOFFSET(p)` | `TIMESTAMP WITH TIME ZONE`, `TIMESTAMP(p) WITH TIME ZONE` | Date, time, and offset; normalized to UTC | `DateTimeOffset` |
| `INTERVAL YEAR TO MONTH` | — | Calendar interval represented canonically as signed years and months | `string` |
| `INTERVAL DAY TO SECOND`, `INTERVAL DAY TO SECOND(p)` | — | Duration with optional fractional-seconds precision | `TimeSpan` |
| `JSON` | — | Validated, compact canonical JSON text | `string` |
| `XML` | — | Validated, canonical XML text | `string` |
| `BIT(n)` | — | Fixed-length bit string; `n` is required | `SqlBitString` |
| `BIT VARYING`, `BIT VARYING(n)` | `VARBIT`, `VARBIT(n)` | Variable-length bit string with an optional maximum length | `SqlBitString` |
| `ROWVERSION` | bare `TIMESTAMP` | Generated, non-nullable eight-byte concurrency token | `byte[]` |

`NULL` is a value and runtime type tag, not a declarable SQL column type. All
ordinary declared types may be nullable unless constrained with `NOT NULL`;
`ROWVERSION` is always generated and non-nullable.

## Facets and canonical values

- Length `n` must be positive. Character lengths count Unicode scalar values;
  binary lengths count bytes; bit-string lengths count bits.
- `DECIMAL` without facets means `DECIMAL(18,2)`, while `DECIMAL(p)` means
  `DECIMAL(p,0)`. Assignment is exact and never rounds excess fractional
  digits.
- Fractional-seconds precision `p` is optional and ranges from 0 through 7 for
  `TIME`, `DATETIME2`, `DATETIMEOFFSET`, and `INTERVAL DAY TO SECOND`.
  `DATETIME(p)` is not accepted; use `DATETIME2(p)`.
- `TIMESTAMP(p) WITH TIME ZONE` is temporal. Bare `TIMESTAMP` is a rowversion;
  `TIMESTAMP(p)` without `WITH TIME ZONE` is rejected.

## Logical types and physical carriers

The record format retains six compact `DbType` tags: `Null`, `Integer`,
`Real`, `Decimal`, `Text`, and `Blob`. Logical metadata supplies the rules and
typed materialization:

- Boolean and all integer widths share the `Integer` carrier.
- `REAL` and `DOUBLE PRECISION` share the `Real` carrier but remain distinct
  logical types.
- Character, temporal, interval, JSON, and XML values share the `Text`
  carrier.
- Binary, UUID, bit-string, and rowversion values share the `Blob` carrier.
- Exact `DECIMAL` values use the dedicated `Decimal` carrier.

## 4.5 integer and Boolean behavior

The internal integer value carrier remains signed 64-bit. A declared
`INTEGER`, however, is range-checked everywhere values enter or are produced
for that type. `INTEGER` arithmetic is checked and stays `INTEGER`; an
operation involving `BIGINT` produces `BIGINT`. Arithmetic on `TINYINT` and
`SMALLINT` widens to `BIGINT`. Integer literals within the signed 32-bit range
are inferred as `INTEGER`, while larger literals are `BIGINT`. Row IDs, row
counts, ranks, and other potentially large counters remain 64-bit.

Booleans are stored canonically as integer `0` or `1`, but are not ordinary
numeric operands. Numeric conversion follows SQL Server `BIT` rules: zero is
false and any finite nonzero value is true. Character assignment accepts
quoted or bound `TRUE`, `FALSE`, `1`, or `0`. `BIT(n)` remains a separate bit
string type and is never materialized as a Boolean.

## Rowversion and compatibility

`ROWVERSION` and bare `TIMESTAMP` are generated column declarations, not cast
or `ALTER COLUMN TYPE` targets. A table may contain one rowversion. It cannot
be assigned, nullable, defaulted, collated, used as an identity, or included in
a key, foreign key, or index. It lowers internally to an eight-byte BLOB plus
a rowversion flag and uses one database-wide allocator. Every insert or update
of a row containing a rowversion consumes a new token, including no-op and
trigger-issued updates. The legacy declaration `BLOB ROWVERSION NOT NULL`
remains accepted.

Metadata version 11 marks these semantics. Descriptor-less integer columns
from older databases continue to mean `BIGINT`. Metadata version 10 was the
4.5 preview format; its declared `INTEGER` columns are exposed as `BIGINT` so
existing 64-bit values are never silently narrowed. The persisted temporal
enum value named `Timestamp` remains readable but is rendered as `DATETIME2`.
