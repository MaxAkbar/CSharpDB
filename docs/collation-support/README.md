# Multilingual Text Support Plan

> **Status (March 2026):** In progress. `BINARY`, `NOCASE`, and `NOCASE_AI` are now implemented across SQL DDL and query semantics, schema/catalog metadata, client and API surfaces, Admin tooling, and collection path indexes. Remaining work is mainly locale-aware `ICU:<locale>` collation, ordered SQL text index optimization, and benchmark hardening.

CSharpDB stores all text as UTF-8 and supports the full Unicode range, meaning any language can be stored and retrieved correctly. Default text comparison and sorting still remain ordinal unless users opt into collation explicitly, but `BINARY`, `NOCASE`, and `NOCASE_AI` collations are now supported in SQL schema definitions, query expressions, and collection path indexes.

---

## Remaining Gaps

The first multilingual-text slice is in place, but three important limitations still remain:

1. Default text semantics are still ordinal. Users must opt into `COLLATE NOCASE` on columns, indexes, collection indexes, or query expressions.
2. Locale-aware `ICU:<locale>` collation remains future work.
3. SQL text `ORDER BY` and range semantics are now collation-correct, but the planner does not yet have a dedicated ordered SQL text index path for those plans.

The current implementation centers collation in these paths:

- `CollationSupport` for metadata normalization, text normalization, and runtime comparison semantics
- `ExpressionEvaluator`, `ExpressionCompiler`, and sort operators for SQL execution behavior
- `IndexMaintenanceHelper` and `QueryPlanner` for SQL index writes, validation, and lookup planning
- `Collection` and `CollectionIndexBinding` for collection index key generation and path query behavior

---

## Design Principles

1. **Ordinal stays the default.** No performance regression for users who do not need collation.
2. **Opt-in per column or per index.** Collation is declared explicitly, not applied globally.
3. **Pre-compute collation keys at write time.** Store normalized sort keys in indexes so reads stay fast — pay the cost once on write, not on every comparison.
4. **Case-insensitive as a fast path.** Simple invariant-case normalization covers the majority of real-world needs and is much cheaper than full ICU locale-aware collation.

---

## Performance Analysis

| Area | Ordinal (current) | With collation (opt-in) | Strategy |
|------|-------------------|------------------------|----------|
| Text comparison in WHERE/JOIN | Raw byte compare | `CompareInfo.Compare` or normalized key compare | Pre-normalized keys avoid per-comparison cost |
| ORDER BY sorting | Code-point order | Culture-aware sort key comparison | Pre-computed sort keys stored alongside data |
| Index equality lookup | UTF-8 byte match | Normalized key match | Normalize at write time, lookup stays O(1) |
| Index range scan | UTF-8 byte order | Collation sort-key order | Store collation sort keys as B+tree keys |
| INSERT/UPDATE | Raw bytes | Generate collation key on write | One-time cost per write |
| Users not using collation | Zero overhead | Zero overhead | Opt-in only |

---

## Proposed Collation Model

### Built-In Collations

| Name | Behavior | Implementation |
|------|----------|----------------|
| `BINARY` | Current ordinal behavior (default) | `StringComparison.Ordinal` — no change |
| `NOCASE` | Case-insensitive, locale-independent | `TextInfo.ToLowerInvariant()` normalization on write; ordinal compare on normalized keys |
| `NOCASE_AI` | Case-insensitive, accent-insensitive | Unicode decomposition + combining-mark stripping + invariant-case normalization |
| `ICU:<locale>` | Full locale-aware collation | `CompareInfo.GetSortKey()` from `System.Globalization` for the specified culture |

`NOCASE` and `NOCASE_AI` cover the common case-insensitive text scenarios today, while `ICU:<locale>` remains the path for future locale-specific linguistic behavior.

### SQL Syntax

#### Column-level collation

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT COLLATE NOCASE,
    email TEXT COLLATE NOCASE
);
```

#### Index-level collation

```sql
CREATE INDEX idx_users_name ON users (name COLLATE NOCASE);
```

#### Query-level collation override

```sql
SELECT * FROM users WHERE name = 'alice' COLLATE NOCASE;
SELECT * FROM users ORDER BY name COLLATE NOCASE;
```

---

## Implementation Plan

### Phase 1: NOCASE Collation

**Goal:** Ship `COLLATE NOCASE` for column definitions, indexes, and query expressions.

Parser changes:
- Add `COLLATE` as a recognized token and keyword
- Support `COLLATE <name>` after column type in `CREATE TABLE`
- Support `COLLATE <name>` after column reference in `CREATE INDEX`
- Support `COLLATE <name>` as a postfix expression modifier in WHERE, ORDER BY, and HAVING

Schema changes:
- Add optional `Collation` property to `ColumnDefinition`
- Add optional `Collation` property to `IndexSchema` column entries
- Persist collation metadata in the schema catalog

Storage changes:
- When a column or index declares `COLLATE NOCASE`, index keys are stored as `ToLowerInvariant()` normalized UTF-8 bytes
- The B+tree key order reflects the normalized form
- Equality comparisons on collated columns use normalized form
- Non-collated columns continue to use raw ordinal bytes (zero overhead)

Execution changes:
- `ExpressionEvaluator` checks column collation metadata for comparison and LIKE operations
- `QueryPlanner` propagates collation from column/index schema to operator construction
- `FilterOperator` and `ProjectionOperator` apply collation when evaluating text expressions
- LIKE on `NOCASE` columns normalizes both pattern and value before matching

Collection changes:
- `EnsureIndexAsync` gains an optional collation parameter for ordered text indexes
- `FindByPathAsync` and `FindByPathRangeAsync` respect index collation

### Phase 2: ICU Locale-Aware Collation

**Goal:** Support `COLLATE ICU:<locale>` for full linguistic sorting.

- Use `CultureInfo.GetCultureInfo(locale).CompareInfo.GetSortKey(text)` to generate binary sort keys
- Store sort keys as the B+tree index key bytes
- Sort keys are larger than raw UTF-8 (typically 2–3x) — this is the main storage cost
- Equality uses sort-key comparison, not original text comparison
- Original text is still stored in the table row; sort keys are index-only

### Phase 3: NOCASE_AI (Accent-Insensitive)

**Status:** Implemented for normalization-based equality, ordering, and index semantics.

- Use Unicode canonical decomposition (NFD) + accent stripping, or `CompareOptions.IgnoreNonSpace`
- Pre-compute normalized keys at write time
- Useful for French, German, Spanish, Portuguese, and other languages with diacritics

---

## Affected Objects and Rollout Checklist

This section maps the feature to the concrete engine objects that will change. The goal is to keep implementation scope explicit and avoid accidental regressions in unrelated text paths.

### Architectural guardrails

- **Default stays fast.** `BINARY` / ordinal comparison remains the default path for users who do not opt into collation.
- **Do not make `DbValue.Compare()` globally locale-aware.** Keep collation as an explicit comparison context so existing non-collated code paths retain current semantics and performance.
- **Prefer pre-computed keys over runtime compare cost.** Locale-aware and case-insensitive behavior should be expressed through normalized or sort-key bytes stored in indexes when possible.
- **Keep pager / WAL / B+tree generic.** The low-level storage engine should not need collation-specific branches if higher layers provide already-normalized key bytes.

### Phase 0: Shared metadata and serialization

- [x] Add collation metadata to `src/CSharpDB.Primitives/Schema.cs`.
- [x] Persist and reload that metadata in `src/CSharpDB.Storage/Serialization/SchemaSerializer.cs`.
- [x] Maintain backward compatibility for existing schema payloads that do not contain collation data.
- [x] Keep SQL index metadata in the current `Columns` + `ColumnCollations` representation for the initial rollout.

### Phase 1: SQL tokenizer, AST, and parser

- [x] Add `COLLATE` to the SQL tokenizer/parser surface.
- [x] Extend AST objects in `src/CSharpDB.Sql/Ast.cs` so column, index, order-by, and expression nodes can carry collation metadata.
- [x] Update `src/CSharpDB.Sql/Parser.cs` to parse `COLLATE` in `CREATE TABLE`, `CREATE INDEX`, and postfix expression contexts such as `WHERE`, `ORDER BY`, and `HAVING`.

### Phase 2: Expression and sort semantics

- [x] Introduce explicit collation helpers in `src/CSharpDB.Execution/CollationSupport.cs` rather than making `DbValue.Compare` globally locale-aware.
- [x] Keep ordinal behavior as the default path while routing collated equality and ordering through explicit helpers.
- [x] Update `src/CSharpDB.Execution/ExpressionEvaluator.cs` and `src/CSharpDB.Execution/ExpressionCompiler.cs` for `=`, `<>`, range comparisons, `IN`, `BETWEEN`, and `LIKE`.
- [x] Update sort operators in `src/CSharpDB.Execution/Operators.cs` so collated ordering does not fall back to ordinal compare semantics.

### Phase 3: SQL schema and planner integration

- [x] Update `src/CSharpDB.Execution/QueryPlanner.cs` DDL paths to persist column and index collation metadata.
- [x] Propagate collation metadata from schema into filter, projection, and sort execution.
- [x] Extend `sys.columns` and `sys.indexes` to expose collation metadata.
- [x] Update `src/CSharpDB.Data/CSharpDbSchemaProvider.cs` so ADO.NET `GetSchema()` surfaces collation metadata.

### Phase 4: SQL index maintenance and lookup behavior

- [x] Update `src/CSharpDB.Execution/IndexMaintenanceHelper.cs` so collated text index components normalize through the active collation while integer fast paths remain intact.
- [x] Ensure unique SQL index enforcement uses collation-aware equality for collated text keys.
- [x] Update `src/CSharpDB.Execution/QueryPlanner.cs` fast lookup logic with collation-aware validation and conservative guardrails for incompatible lookup plans.
- [x] Preserve non-collated SQL index behavior unchanged for existing databases and workloads.

### Phase 5: Ordered SQL text index path

Current SQL text indexes are hash-based, which is enough for equality but not for true locale-aware ordering. Collection ordered text indexes already have a separate ordered-key path.

- [x] Initial strategy: keep collated SQL text `ORDER BY` and range semantics correct even without a dedicated ordered SQL text index path.
- [ ] If ordered SQL text indexes are introduced, add the new path alongside current hashed SQL indexes rather than mutating the existing hash format in place.
- [x] Update planner selection rules in `src/CSharpDB.Execution/QueryPlanner.cs` so collated text range/order plans only use compatible index structures.

### Phase 6: Collection path indexes

- [x] Add optional collation parameters to collection index creation in `src/CSharpDB.Engine/Collection.cs`.
- [x] Make `FindByPathAsync(...)` and `FindByPathRangeAsync(...)` respect collection index collation metadata.
- [x] Extend `src/CSharpDB.Engine/CollectionIndexBinding.cs` to store collation metadata, build normalized text keys, and use collation-aware equality/range comparisons.
- [x] Reuse the existing ordered text codec format with normalized text payloads for the `NOCASE` rollout.
- [x] Preserve existing integer and non-collated text collection index behavior.

### Phase 7: Public contracts and tooling

- [x] Update client-facing schema models in `src/CSharpDB.Client/Models/SchemaModels.cs`.
- [x] Update gRPC contracts in `src/CSharpDB.Client/Protos/csharpdb_rpc.proto`.
- [x] Update gRPC mapping in `src/CSharpDB.Client/Grpc/GrpcModelMapper.cs`.
- [x] Update REST DTOs and endpoints so column/index collation metadata round-trips through HTTP.
- [x] Update Admin SQL editor helpers so `COLLATE` is recognized and rendered correctly.

### Phase 8: AST rewriting, binding, and cloning safety

Several engine paths rebuild AST objects. Any new collation fields must be preserved in those clone/rewrite paths or the feature will behave inconsistently.

- [x] Update parameter binding in `src/CSharpDB.Data/PreparedStatementTemplate.cs`.
- [x] Update planner rewrite/binding paths in `src/CSharpDB.Execution/QueryPlanner.cs` so explicit `COLLATE` metadata is preserved.
- [x] Carry collation metadata through the known AST clone and rewrite paths touched by the NOCASE rollout.

### Phase 9: Testing and benchmark gates

- [x] Parser tests for `COLLATE` in table definitions, index definitions, and query expressions.
- [x] Schema serialization compatibility tests for collated and legacy non-collated schemas.
- [x] SQL behavior tests for equality, `LIKE`, `ORDER BY`, query-level overrides, and unique index enforcement under `NOCASE`.
- [x] Collection tests for collated `EnsureIndexAsync`, `FindByPathAsync`, and `FindByPathRangeAsync`.
- [ ] Benchmark non-collated workloads to verify default `BINARY` paths retain current performance.
- [ ] Benchmark collated write cost, lookup cost, range-scan cost, and index size growth.

### Suggested remaining delivery order

1. SQL ordered text collation/index strategy
2. ICU locale-aware collation
3. Benchmark and compatibility hardening

---

## Non-Goals

For the first implementation:

- Per-database default collation (keep `BINARY` as the global default)
- Runtime-registered custom collation functions
- Collation changes on existing data without rebuild (require `REINDEX` after collation change)
- Automatic migration of existing non-collated indexes

---

## Risks

- **Index size increase:** Locale-aware sort keys are larger than raw UTF-8. For `NOCASE` the overhead is minimal (same byte length, different case). For ICU collations, sort keys can be 2–3x larger.
- **Write overhead:** Generating collation keys on every INSERT/UPDATE adds CPU cost. Pre-computation means this is per-write, not per-read.
- **REINDEX requirement:** Changing a column's collation requires rebuilding all indexes that reference it. This should be enforced, not silent.
- **NativeAOT compatibility:** `System.Globalization` and ICU are available in NativeAOT but require ICU data files to be bundled. `NOCASE` using `ToLowerInvariant()` has no ICU dependency.

---

## Test Plan

- `COLLATE NOCASE` column: case-insensitive equality, LIKE, and ORDER BY
- `COLLATE NOCASE` index: index-backed lookup matches regardless of case
- Mixed collation: collated and non-collated columns in the same table
- Query-level `COLLATE` override on a `BINARY` column
- Round-trip: store mixed-case text, retrieve original casing, sort case-insensitively
- Multi-language text: Chinese, Arabic, Japanese, Emoji with `BINARY` and `NOCASE`
- ICU collation: German `ä` sorts near `a`, not after `z`
- Performance: benchmark collated vs non-collated index writes, lookups, and range scans
- REINDEX after collation change
- Collection path index with `NOCASE` collation

---

## See Also

- [Roadmap](../roadmap.md)
- [Architecture Guide](../architecture.md)
- [Storage Engine Guide](../storage/README.md)
