# Collation Support Plan

> **Status (March 2026):** Planned. This document captures the design direction for adding collation support to CSharpDB — enabling case-insensitive comparisons, locale-aware sorting, and a `COLLATE` clause for queries and index definitions.

CSharpDB currently stores all text as UTF-8 and supports the full Unicode range, meaning any language can be stored and retrieved correctly. However, all text comparison and sorting uses `StringComparison.Ordinal` (raw byte/code-point order), which means there is no case-insensitive matching, no locale-aware sort ordering, and no `COLLATE` clause.

---

## Problem

Today, the text handling has three constraints:

1. `WHERE name = 'Alice'` does not match `'alice'` or `'ALICE'` — there is no case-insensitive equality.
2. `ORDER BY name` sorts by Unicode code point, not by linguistic rules — German `ä` sorts after `z` instead of near `a`, and uppercase letters sort before all lowercase letters.
3. `LIKE 'a%'` is case-sensitive — there is no `COLLATE NOCASE` modifier.

These are the specific code paths where ordinal comparison is hardcoded:

- `DbValue.CompareTo` — `string.Compare(a.AsText, b.AsText, StringComparison.Ordinal)`
- `DbValue.Equals` — `AsText == other.AsText` (ordinal equality)
- `ExpressionEvaluator` LIKE evaluation — ordinal character matching
- B+tree index key encoding — raw UTF-8 bytes determine key order
- Collection ordered text indexes — UTF-8 prefix keys with ordinal ordering

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
| `NOCASE_AI` | Case-insensitive, accent-insensitive | `string.Compare(..., CompareOptions.IgnoreCase \| IgnoreNonSpace)` or Unicode normalization + folding |
| `ICU:<locale>` | Full locale-aware collation | `CompareInfo.GetSortKey()` from `System.Globalization` for the specified culture |

`NOCASE` should ship first — it covers the vast majority of use cases (case-insensitive lookups and sorts) with minimal cost.

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

**Goal:** Support accent-insensitive matching for European languages.

- Use Unicode canonical decomposition (NFD) + accent stripping, or `CompareOptions.IgnoreNonSpace`
- Pre-compute normalized keys at write time
- Useful for French, German, Spanish, Portuguese, and other languages with diacritics

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
