# Full-Text Search Plan

> **Status (March 2026):** Planned. This document reviews the existing full-text research for CSharpDB and turns it into a repo-specific implementation plan.

The external research is directionally correct: CSharpDB should implement full-text search as a custom inverted index stored inside the existing catalog-managed B+tree/index system, not as a separate sidecar store. That keeps full-text data inside the same pager, WAL, snapshot, and root-page persistence model as the rest of the engine.

The main adjustments are specific to this repo:

1. Internal FTS structures cannot be treated as ordinary SQL indexes by the current generic index-maintenance paths.
2. The safest delivery path is storage/query infrastructure first, then a programmatic API, and only then new SQL grammar.

---

## Review Outcome

The research aligns well with the current codebase in these areas:

- `CatalogService` already persists multiple index roots per table and updates them through `PersistRootPageChangesAsync(...)`.
- `IIndexStore` already gives the right primitive shape for `term -> payload` and `docId -> payload` trees.
- Existing codecs (`RowIdPayloadCodec`, `OrderedTextIndexPayloadCodec`) prove that payload rewrite on update is already an accepted pattern in CSharpDB.
- The execution layer already has a clean place for a future `FullTextSearchOperator`.

The research needs these repo-specific corrections:

- `QueryPlanner` currently loops over `GetIndexesForTable(table)` for generic SQL index maintenance on INSERT/UPDATE/DELETE. Hidden FTS trees would be touched incorrectly unless we add index-kind metadata or explicitly filter them out.
- `IndexSchema` only stores `IndexName`, `TableName`, `Columns`, and `IsUnique`. That is not enough for tokenizer settings, stopwords, state, ranking mode, or hidden-store ownership.
- `OrderedTextIndexKeyCodec` is optimized for ordered prefix buckets, not for a high-cardinality term dictionary. FTS terms should use a stable full-token hash key and store the normalized token in the payload for collision resolution.
- `ScalarFunctionEvaluator` currently evaluates functions row-by-row. That is not sufficient for full-text search; the planner must route matching queries to a dedicated operator.

---

## Scope Recommendation

Initial scope should be intentionally narrow:

- SQL tables only
- Explicit `TEXT` columns only
- One tokenizer pipeline per full-text index
- Exact term queries first
- Transactional maintenance required before calling the feature complete

Defer these until the storage and planner pieces are stable:

- Collection/document full-text indexing
- Phrase and proximity queries
- BM25 ranking
- Fuzzy / wildcard expansion
- Lucene.NET index sidecar integration

Lucene.NET analyzers may still be worth evaluating later as an optional tokenization pipeline, but CSharpDB should own the on-disk index format so the feature remains single-file, WAL-backed, and snapshot-safe.

---

## Core Design

### User-visible metadata

CSharpDB needs one user-visible full-text definition plus a small family of hidden storage indexes.

Recommended shape:

- Extend `IndexSchema` with optional trailing metadata:
  - `Kind` (`Sql`, `Collection`, `FullText`, `FullTextInternal`)
  - `OptionsPayload` or equivalent serialized settings
  - `OwnerIndexName` for hidden internal trees
  - `State` (`Building`, `Ready`)

This is preferable to inventing a separate FTS catalog immediately because:

- the catalog already knows how to persist index roots,
- the serializer already uses forward-compatible trailing metadata patterns,
- the planner already resolves indexes through the catalog.

### Hidden storage trees

Each logical full-text index should own these internal stores:

- `fts_<name>__meta`
- `fts_<name>__terms`
- `fts_<name>__postings`
- `fts_<name>__docstats`
- optional `fts_<name>__kgrams`

Suggested responsibilities:

- `__meta`: document count, total token count, average document length inputs, index state, tokenizer/version info
- `__terms`: normalized token -> document frequency and optional cached metadata
- `__postings`: normalized token -> postings list
- `__docstats`: `docId -> doc length` and any future field-length stats
- `__kgrams`: optional fuzzy/wildcard support later

### Keying

Do not key FTS terms with `OrderedTextIndexKeyCodec`.

Recommended approach:

- add `FullTextTermKeyCodec` using a stable full-token hash,
- store the normalized token string in the payload bucket to resolve collisions,
- keep direct integer keys for `docId`-based stores such as `__docstats`.

That avoids the hot-prefix collision pattern that would come from packing only the first few UTF-8 bytes.

### Payload codecs

Add dedicated codecs under `src/CSharpDB.Storage/Indexing`:

- `FullTextMetaPayloadCodec`
- `FullTextTermStatsPayloadCodec`
- `FullTextPostingsPayloadCodec`
- `FullTextDocStatsPayloadCodec`

Implementation rules:

- use the existing `Varint` helper for compact integer encoding,
- delta-encode sorted `docId` and position lists,
- include magic bytes and defensive decode validation,
- design for full payload rewrite on update and delete.

---

## Query Surface

### Phase 1 API surface

Do not start with new SQL grammar.

Add a programmatic API first:

```csharp
await db.EnsureFullTextIndexAsync(
    tableName: "docs",
    indexName: "fts_docs",
    columns: ["title", "body"],
    options: new FullTextIndexOptions());

await foreach (var match in db.SearchAsync("fts_docs", "distributed systems"))
{
    // row id + optional score
}
```

This lets the storage model, codecs, and query executor stabilize without immediately committing to SQL syntax.

### Phase 2 SQL surface

After the engine path is proven, add:

```sql
CREATE FULLTEXT INDEX fts_docs ON docs (title, body);
SELECT * FROM docs WHERE FTS_MATCH('fts_docs', 'distributed systems');
```

`FTS_MATCH(...)` should not be evaluated as a normal scalar function. The planner must recognize it and replace the table scan with an FTS-backed candidate source plus optional residual filtering.

---

## Implementation Sequence

### Phase 1: Metadata and storage primitives

Goal: make full-text indexes representable and persistable without corrupting generic SQL index behavior.

Work:

- extend `IndexSchema` and `SchemaSerializer.SerializeIndex/DeserializeIndex`
- teach `CatalogService` to create, enumerate, and open full-text internal indexes
- filter `FullTextInternal` indexes out of generic SQL planner selection and generic DML index maintenance
- add `FullTextIndexOptions`
- add `FullTextTermKeyCodec` and the four FTS payload codecs
- add codec tests and serializer compatibility tests

Primary files:

- `src/CSharpDB.Primitives/Schema.cs`
- `src/CSharpDB.Storage/Serialization/SchemaSerializer.cs`
- `src/CSharpDB.Storage/Catalog/CatalogService.cs`
- `src/CSharpDB.Execution/QueryPlanner.cs`
- `src/CSharpDB.Storage/Indexing/*`

### Phase 2: Tokenization and backfill

Goal: build a correct inverted index for existing table rows.

Work:

- add `FullTextTokenizer` with normalization + lowercase + positional token output
- add `FullTextIndexWriter`
- implement backfill scan over a base table, similar to existing SQL index creation
- store `df`, document length, and corpus-level counters needed for later ranking
- add focused tests for Unicode normalization, surrogate pairs, empty/null text, and multi-column concatenation rules

Primary files:

- `src/CSharpDB.Execution` or `src/CSharpDB.Engine` for writer orchestration
- `src/CSharpDB.Storage/Indexing/*`
- `tests/CSharpDB.Tests/*`

### Phase 3: Read path and search executor

Goal: make the index queryable before full SQL syntax lands.

Work:

- add `FullTextIndexReader`
- add `FullTextQueryParser` for a minimal term/AND/OR/NOT grammar
- add `FullTextQueryExecutor` returning `rowId` candidates
- expose a programmatic `SearchAsync(...)` API
- add benchmarks for backfill throughput, single-term lookup, and boolean intersections

Keep ranking out of this phase unless it comes almost for free.

### Phase 4: Transactional DML maintenance

Goal: keep full-text indexes correct under INSERT/UPDATE/DELETE.

Work:

- hook FTS maintenance into table writes inside the same transaction path as base-row changes
- on UPDATE, tokenize old and new values and rewrite affected postings
- keep `__meta`, `__terms`, and `__docstats` consistent with document mutations
- add reopen/recovery tests so WAL replay and snapshot readers see a consistent state

This is the phase that makes the feature durable in the same sense as existing secondary indexes.

### Phase 5: SQL planner integration

Goal: expose full-text search through SQL without per-row function evaluation.

Work:

- add tokenizer/parser/AST support for `FULLTEXT` and for the chosen SQL search surface
- add a planner rule that recognizes the full-text predicate and builds a dedicated operator
- add `FullTextSearchOperator`
- support residual predicates and projection on top of the candidate set

### Phase 6: Ranking, phrase, and fuzzy

Add in this order:

1. BM25 scoring using `__meta`, `df`, and `docstats`
2. phrase/proximity using positional postings
3. optional k-gram vocabulary expansion for fuzzy/wildcard

Do not start with fuzzy search. It adds a lot of surface area for the least immediate value.

---

## Testing Plan

Minimum required tests before shipping:

- schema/index serialization compatibility for new FTS metadata
- codec roundtrips for postings, docstats, and term stats
- tokenizer normalization and surrogate-pair handling
- backfill correctness on existing rows
- transactional INSERT/UPDATE/DELETE maintenance
- WAL recovery and reopened-database consistency
- SQL/planner tests once the SQL surface exists
- microbenchmarks for build throughput and term lookup latency

The existing benchmark project already has a natural place for this work; add dedicated FTS cases instead of bolting the feature onto the current text equality benchmark.

---

## Non-Goals for v1

- Collection/document full-text indexing
- Stemming and lemmatization
- Locale-aware analyzers
- Snippet/highlight generation
- Lucene-compatible query syntax
- External sidecar index files

---

## Recommended First PR Slice

The best first PR is not query syntax. It is:

1. `IndexSchema` metadata extension for FTS kinds/options
2. hidden internal-index filtering in `QueryPlanner`
3. FTS codecs + tests
4. `FullTextTokenizer`
5. catalog helper that creates a logical FTS index plus its hidden stores

That slice de-risks the rest of the feature and gives the repo a stable foundation for later search execution work.

---

## See Also

- [Roadmap](../roadmap.md)
- [Architecture Guide](../architecture.md)
- [Storage Engine Guide](../storage/README.md)
- [Collection Indexing Guide](../collection-indexing/README.md)
