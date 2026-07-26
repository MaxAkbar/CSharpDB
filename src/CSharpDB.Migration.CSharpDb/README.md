# CSharpDB.Migration.CSharpDb

Target-specific staged execution adapter for `CSharpDB.Migration`.

The adapter creates a new database with atomic create-new semantics and holds
an exclusive same-directory migration lease for its lifetime. Its internal
tables bind the target to the plan/catalog/capability digests, source identity
and fingerprint, actual source snapshot, and a stable random target ID.
Staged files use the engine's write-optimized checkpoint policy with an
explicit 2,048-page LRU cache, so target residency is bounded independently of
the total source size.

Schema actions are applied in ordered, transactionally receipted stages. Each
prepared data batch is converted and digested by the provider-neutral runner;
the target then validates the binding, ordered columns, value tags,
nullability, finite REAL policy, and digest again. Accepted rows, canonical
reject-ledger entries, and their full v2 receipt commit in one explicit
transaction. Resume accepts only exact existing stage and batch receipts and
rejects changed identities, cursor chains, outcome order, or payloads.

`CSharpDbDdlPreviewBuilder` exposes the same ordered target-schema renderer as
an explicit, no-write preview. It reports plan readiness, separates SQL from
document-collection creation actions, and includes empty stages so the output
order is unambiguous. Its versioned digest covers only migrated schema actions,
not internal migration bookkeeping tables, and is independent of the plan
digest so it remains stable after attachment. A plan stores only the digest;
when present, the staged target recomputes and verifies it before creating or
resuming a target. `BuildAndAttachGeneratedDdlDigestBounded` performs one
authoritative bounded render and returns the sealed plan without retaining the
SQL. Legacy externally authored plans without a preview digest remain accepted
for compatibility, while newly CLI-authored plans are always sealed.
Operator-facing preview callers use `BuildBounded`; both bounded entry points
enforce configurable action, per-action SQL, and aggregate UTF-8 limits that
cannot exceed production ceilings. The renderer enforces those limits
incrementally before retaining each action and reports a stable, sanitized
limit kind without publishing SQL or names. Those are render-action limits,
not plan/catalog acquisition or preprocessing limits; callers must bound and
validate those inputs separately.

`CSharpDbDdlScratchValidator` verifies that reviewed preview with bounded
aggregate and parser limits, parses each SQL action once with `CSharpDB.Sql`,
and executes the same parsed statements in one committed, in-memory
transaction. It then reads the resulting catalog and compares it with the
plan's normalized expected schema. Its versioned report contains only digests,
stable stage/action/rule identifiers, readiness, and evidence level; it never
publishes rendered SQL, object names, ASTs, or parser/engine messages. A pass
is scratch-execution and schema-shape evidence only. It is not source semantic
equivalence and does not claim that view or trigger bodies were bound.

## Phase 6B: CSharpDB-ready DDL proof

`CSharpDbDdlCompatibilityAnalyzer` accepts one strict, bounded CSharpDB SQL
script. Its initial explicit allowlist is persistent `CREATE TABLE` plus
simple `CREATE INDEX`. Unsupported statements and unproven table or index
features produce stable, span-based diagnostics and stop before scratch
execution; supported statements are never extracted from an otherwise
unsupported script and proven in isolation.

For a completely supported script, the analyzer lowers the schema into the
migration catalog, evaluates the versioned target capability rules, renders
candidate CSharpDB DDL, parses it through `CSharpDB.Sql`, executes it only in a
new in-memory scratch database, and compares the resulting normalized schema
with the intended model. The deterministic report exposes only sanitized
digests, counts, source spans, stable rule identifiers, status, evidence level,
and `SourceGrammar`. Native CSharpDB input reports
`csharpdb-sql/v1`; callers that reuse the target proof for a different source
grammar must supply its fixed identifier. The report does not expose SQL,
paths, object names, ASTs, or raw parser and engine messages.

This analyzer has no existing-target or auto-apply path. It does not create a
database file, modify a migration plan, or install a generated rewrite.

Phase 6B.2 reuses this target proof from the optional
`CSharpDB.Migration.SqlServer` adapter. That route parses one standalone script
with the fixed `TSql160Parser`, `QUOTED_IDENTIFIER` on, and
`SqlEngineType.Standalone`, then fail-closed lowers only ordinary two-part
`dbo` `CREATE TABLE` statements and later simple `CREATE INDEX` statements.
Its source grammar is `tsql160`; the CLI exposes no grammar or server-version
override.

The T-SQL hard ceilings are 4,194,304 UTF-16 code units, 16 MiB of UTF-8,
4,096 statements, 1,048,576 code units per statement, 250,000 tokens, nesting
depth 128, 250,000 AST nodes, 64 lexer errors, and 64 parser errors. Any lexer
or parser error rejects the script; exceeding either error ceiling returns
`Unknown`. The existing target proof independently caps candidate actions at
4,096, aggregate candidate SQL at 16 MiB, and, by default, each action at
1,048,576 UTF-16 code units and 4 MiB of UTF-8. Every statement must fit the
allowlist and resolve in source order before any candidate DDL is executed;
unsupported statements, defaults, identity, computed/generated columns,
rowversion, checks, non-`dbo` names, unsupported types or physical index
features, duplicate objects, and unresolved or forward references stop the
whole proof.

Because T-SQL is lowered into CSharpDB DDL, a successful result is never
`Compatible`; it is at best `CompatibleWithRewrite`. Text columns retain the
stable unresolved-collation diagnostic, so their report remains `Conditional`
even when scratch execution succeeds and expected and actual normalized schema
digests match; `ProvenStatementCount` remains zero. `ScratchExecuted` describes
the attained evidence, not a claim that SQL Server collation or other source
semantics are equivalent. MySQL, SQLite, and other standalone source-DDL
grammars remain fail-closed deferrals.

Apply stops at `awaiting-validation`. Activation accepts only a permit derived
from a coherent, published, passing validation report and persists its receipt
atomically. One immutable validation reader snapshot exposes the complete
ordered receipt and reject-ledger streams alongside schema, counts, and rows;
the SDK runner requires exact source-outcome agreement before it can publish a
deterministic-reject report. The project has no overwrite, merge, or replace
API. The provider-neutral SDK artifact writer projects those immutable streams
into canonical JSONL and publishes without overwriting a different file.

The adapter implements both deterministic fail-fast and the opt-in deterministic
reject contract. Conversion visits canonical source objects, rows, and columns
in order, while tolerant batches bind every accepted/rejected outcome to the
receipt and ledger. A cancellation observed before the final commit check rolls
back rows, rejects, and receipt; after that check commit is deliberately
non-cancellable and resume verifies whether it completed. Fresh-process fault
qualification covers accepted-only, mixed, and all-reject batches at every
transaction boundary.

The adapter also recognizes only the exact versioned JSON document-collection
contract. It creates a real Engine collection, stores generated ordinal keys
and already-canonical JSON through a migration-only direct-payload seam, and
refuses duplicate keys rather than updating them. The document writes and
batch receipt share one transaction. Physical `_col_` names are resolved for
resume, schema capture, counts, and validation while normalized evidence keeps
the logical `Collection` shape. Root-array and NDJSON qualification covers
exact bytes, every transaction fault cutoff, reopen/resume, checksums,
activation, and collection API access. The public
[database migration guide](https://csharpdb.com/docs/database-migration.html)
documents the operator-facing retained JSON routes.

The bounded CLI can now request exact DDL or a separate sanitized scratch
report, but it cannot apply that DDL or promote plan readiness. Identity,
rowversion, and default lowering plus archive restoration remain explicit
follow-up work. SDK reject-artifact publication is qualified independently of
the CLI.
