# Migration CSV Schema Inference

This note records the third Phase 4A implementation slice. It adds bounded,
confidence-bearing CSV schema inference, ordinal column overrides, and a shared
migration catalog inspector on top of the immutable source binding described in
[`migration-csv-inspection-and-source-binding.md`](migration-csv-inspection-and-source-binding.md).

## Trust Boundary

`CsvSchemaInferer` reads only from the exact `CsvSourceSnapshot` named by a
`CsvSourceBinding`. It retains fixed-size counters and candidate flags for each
bounded column; sampled field values are never retained in the result or
diagnostics. A caller supplies positive logical data-record and cumulative
decoded-character limits. The inferrer may parse one additional individually
bounded logical record solely to distinguish a limit from exact end-of-file,
but that record never contributes type evidence.

The result records two versioned contracts:

- `csharpdb-csv-schema-v1` defines inference, confidence, evidence, and catalog
  behavior.
- `csharpdb-csv-scalar-v1` defines the exact scalar grammar that a later CSV
  data source must reuse while validating every streamed row.

Target conversion codecs are deliberately not used as the source grammar.
Some target codecs accept permissive GUID, date, or numeric forms that would
make a sampled inference unsafe if a later row used a different spelling.

## Conservative Type Policy

Automatic inference requires at least two substantive, compatible values.
One compatible value produces only a low-confidence suggestion and activates
`Text` in the migration catalog.

| Logical type | Automatic evidence |
| --- | --- |
| `Boolean` | Exact lowercase `true` and `false`. One-sided evidence remains low confidence. |
| `SignedInteger` | Canonical invariant base-10 within `Int64`. |
| `UnsignedInteger` | Canonical nonnegative base-10 values that require `UInt64`. |
| `Decimal` | Arbitrary-precision base-10 syntax with the bound culture's exact decimal separator, no grouping or exponent. |
| `Guid` | Exact `D` form. |
| `Date`, `Time`, `DateTime`, `DateTimeOffset` | Exact unambiguous ISO forms defined by scalar policy v1. |
| `FloatingPoint` | Never automatic; binary64 intent requires an explicit override and finite values. |
| `Text` | Exact ordinary text or the safe fallback for ambiguity. |

Leading-zero identifiers and other lexically significant numeric-looking
values such as `001`, `-01`, `+1`, `-0`, and scientific notation default to
`Text`. Empty strings also force `Text`; they are not nulls. Mixed unrelated
kinds and unsupported syntax default to `Text` without row-level dynamic
typing.

Decimal evidence tracks maximum integral digits and maximum scale separately.
The required precision is their sum, so values `999` and `0.99` require
precision 5 and scale 2. Sampled maxima are published only as `observed*`
facets. Mapper-active `precision`, `scale`, and `maxLength` facets are emitted
only after full coverage.

## Coverage And Nullability

Coverage counts one examined slot for every profiled data row and column,
including null, empty, and missing slots:

- EOF at or before the bound produces `Full` coverage with an exact total and
  no sample claim.
- A bounded prefix produces `Sample` coverage with `TotalValues = null` and
  `RequiresFullStreamValidation = true`.
- Structure-only inspection produces `None` coverage and defaults columns to
  `Text` unless explicitly declared.

The migration planner accepts an omitted `profileTotalValues` facet for
sampled coverage. It still requires the exact total for full coverage.

Observed null tokens do not constrain type candidates but make a fully
profiled column nullable. A sampled column defaults to nullable because an
unseen tail may contain nulls. A fully scanned column becomes non-nullable only
when at least one present value was observed; a header-only file remains
nullable rather than claiming a constraint from no evidence. A missing field
is a structural row defect, not a null: it receives a stable blocking
diagnostic until a future strict/reject policy is selected.

## Overrides And Source Names

`CsvColumnSchemaOverride` addresses a column by zero-based ordinal and can add
an exact expected-header guard, a logical source type, and declared
nullability. Duplicate, negative, out-of-range, and header-mismatched overrides
are rejected. Profiled values that contradict a declaration produce a stable,
non-overrideable error diagnostic; inference never silently falls back on a
row-by-row basis.

An explicit numeric override is also the declaration that numeric lexemes may
be normalized. For example, an explicitly declared signed integer accepts
`001` and exposes invariant canonical text `1`; automatic inference still keeps
`001` as `Text`. `CsvSchemaInferenceResult.TryNormalizeScalar` is the one
versioned normalization entry point for both culture-specific numeric syntax
and strict date/GUID forms. Override validation is coverage-qualified as
`NotProfiled`, `SampleCompatible`, `FullCompatible`, or `Incompatible`; a
sample-only success is never labeled fully compatible.

Overrides change the migration catalog digest, not the bound source
fingerprint. The latter continues to describe only immutable bytes and parsing
semantics.

Blank and whitespace-only headers receive deterministic names such as
`column_1` because catalog source names must be nonblank. Their exact decoded
header remains in `csvOriginalHeader`. Duplicate and case-colliding headers
remain source facts and use ordinal object IDs; the shared deterministic name
mapper resolves target collisions.

## Migration Catalog

`CsvMigrationSourceInspector` implements `IMigrationSourceInspector` while the
caller retains ownership of the snapshot. It emits one default namespace, one
table, and ordinal columns with stable `CSV_*` native types and shared
`logicalType` facets. Coverage, confidence, resolution, value-free counts,
algorithm versions, original-header facts, and override compatibility are all
bound into the catalog digest. `MigrationContractValidator.ValidateCatalog`
runs before the catalog is returned.

This slice does not implement `IMigrationDataSource`, target writes, resume, or
reject files. Those next steps must reuse scalar policy v1 against every value
from the same snapshot before a batch commits. Until that adapter exists, a
sample is preview evidence only, not proof of the unseen tail.

Binary intent is also deferred: base64- or hex-looking text is never inferred
as a BLOB, and v1 does not expose a binary override without an explicit
encoding and decoded-size bound. The typed manifest/data-source slice must add
that declaration before CSV binary import can be enabled safely.
