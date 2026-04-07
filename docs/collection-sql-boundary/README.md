# Collection And SQL Boundary

This note keeps only the changes we would actually make and why we support
them.

## Changes We Would Make

### 1. Split SQL And Collection Orchestration More Cleanly

Move collection-specific setup and runtime orchestration out of `Database` into
a dedicated component such as:

- `CollectionManager`
- `CollectionRuntime`
- `DocumentStore`

That component should own:

- collection creation and lookup
- collection cache management
- generated-model validation
- collection-specific metadata and index orchestration

`Database` should continue owning the shared transaction, commit, and durability
infrastructure.

### 2. Remove Collection-Specific Logic From The Query Planner

Stop keying planner behavior off `_col_` table names.

Instead, route collection-backed reads through:

- schema metadata
- a serializer/read-model boundary
- or a collection-read adapter above the planner

The planner should not need to know about collection storage naming
conventions.

### 3. Keep One Shared Engine Core

Do not split collections and SQL into separate storage engines.

Keep these parts shared:

- pager and WAL
- B-tree and index maintenance
- catalog and schema metadata
- transaction and commit behavior
- recovery and durability rules
- row-count and advisory-stat plumbing

### 4. Keep `_col_*` Backing Tables

Collections should keep using internal `_col_*` backing tables for now.

## Why We Support This Direction

### Shared Engine Code Is The Right Kind Of Sharing

The lower layers are the hardest code in the system to keep correct and fast.
Sharing them avoids duplicating:

- recovery logic
- durability behavior
- transaction rules
- index maintenance
- catalog synchronization

That keeps correctness and performance work in one place.

### The Current Performance Findings Support A Shared Core

The recent collection-performance investigation showed that the remaining
allocation overhead after the collection-specific fixes was mostly in the shared
auto-commit and catalog path, not in the collection API itself.

That is a strong signal that the storage and commit layers are shared engine
concerns, not something that should be forked into a separate collection engine.

### The Real Problem Is Boundary Clarity At The Top

Collections already behave like a separate API surface. The messy part is that
some collection concerns still leak into `Database` and `QueryPlanner`.

Cleaning up those boundaries gives us:

- clearer ownership
- less cross-layer coupling
- easier maintenance
- lower risk for future performance work

### Keeping `_col_*` Backing Tables Preserves Useful Properties

Keeping collections on internal backing tables preserves:

- one on-disk model
- one recovery model
- one durability model
- one catalog model

That is simpler than introducing a second persistence model just to make the
surface area look more separate.

## Decision

The direction we support is:

- separate SQL and collection orchestration more cleanly
- keep the storage and commit core shared
- remove collection-specific planner leakage
- keep `_col_*` backing tables
