# SQL type semantics in 4.5

CSharpDB 4.5 separates a column's declared SQL type from its compact physical
record representation. This is a logical-semantics change; database pages and
record encodings are unchanged.

| Declaration | Semantics |
|---|---|
| `TINYINT` | Unsigned 8-bit integer |
| `SMALLINT` | Signed 16-bit integer |
| `INT`, `INTEGER` | Signed 32-bit integer |
| `BIGINT` | Signed 64-bit integer |
| `BOOLEAN`, `BOOL`, bare `BIT` | Logical Boolean, materialized as `bool` by ADO.NET and transports |
| `BIT(n)` | Fixed-length bit string |
| `VARBIT(n)`, `BIT VARYING(n)` | Variable-length bit string |
| `DATETIME`, `DATETIME2(p)` | Date and time; `DATETIME2` is the canonical schema spelling |
| `DATETIMEOFFSET(p)` | Date, time, and UTC offset |
| `TIMESTAMP ... WITH TIME ZONE` | Portable input alias for `DATETIMEOFFSET` |
| `ROWVERSION`, bare `TIMESTAMP` | Database-generated, non-nullable eight-byte concurrency token |

The internal integer value carrier remains signed 64-bit. A declared
`INTEGER`, however, is range-checked everywhere values enter or are produced
for that type. `INTEGER` arithmetic is checked and stays `INTEGER`; an
operation involving `BIGINT` produces `BIGINT`. Integer literals within the
signed 32-bit range are inferred as `INTEGER`, while larger literals are
`BIGINT`. Row IDs, row counts, ranks, and other potentially large counters
remain 64-bit.

Booleans are stored canonically as integer `0` or `1`, but are not ordinary
numeric operands. Numeric conversion follows SQL Server `BIT` rules: zero is
false and any finite nonzero value is true. `BIT(n)` remains a separate bit
string type and is never materialized as a Boolean.

`ROWVERSION` and bare `TIMESTAMP` are generated column declarations, not cast
targets. They lower to the existing eight-byte BLOB representation, are always
non-nullable and read-only, and use one database-wide allocator. Every insert
or update of a row containing a rowversion consumes a new token, including
no-op and trigger-issued updates. The legacy declaration
`BLOB ROWVERSION NOT NULL` remains accepted.

Metadata version 11 marks these semantics. Descriptor-less integer columns
from older databases continue to mean `BIGINT`. Metadata version 10 was the
4.5 preview format; its declared `INTEGER` columns are exposed as `BIGINT` so
existing 64-bit values are never silently narrowed. The persisted temporal
enum value named `Timestamp` remains readable but is rendered as `DATETIME2`.
