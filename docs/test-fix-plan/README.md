# Test Failure Recovery Plan

## Summary
Stabilize the remaining test failures in ordered workstreams that favor narrow correctness fixes and test-helper repairs over hot-path changes. Performance-sensitive paths should keep their current asymptotic behavior: no new table scans in steady-state lookups, no extra catalog reloads on normal reads, and no storage-handle redesign that would force more frequent open/close churn.

## Progress
- Completed: collection payload header compatibility fix for wrapped serializers.
- Completed: collection statistics/root persistence fixes for auto-commit and vacuum copy.
- Completed: WAL open contract fixes so transaction entry does not hit a closed WAL.
- Completed: crash-image test helpers now copy with `FileShare.ReadWrite | FileShare.Delete` instead of `File.ReadAllBytes`, which avoids forcing a production storage change on Windows.
- Completed: ordered-text array index dedupe test now decodes the matching row-id slice before counting row ids.
- In progress: hybrid cache-retention and WAL-cache read regressions.
- In progress: batch-plan and index-ordered planner regressions.

## Workstreams
### 1. Collection payload compatibility
- Keep direct binary collection payload detection limited to serializers that actually emit that format.
- Keep binary payload traversal fixes narrow to the collection codec; do not add a generic JSON fallback on indexed reads.
- Validate nested array/object path extraction with targeted codec and collection index tests.

### 2. Collection persistence, rollback, and vacuum correctness
- Persist dirty table statistics before collection commits and vacuum destination commits.
- Persist collection catalog root changes when statistics updates dirty the catalog table even if the collection root itself does not move.
- Keep rollback repair focused on refreshing stale in-memory references after catalog reload rather than changing commit behavior.

### 3. WAL lifecycle and hybrid crash-image behavior
- Ensure the WAL is open before transaction entry and database initialization paths that rely on it.
- Keep long-lived file handles for database performance; fix crash-image tests with share-compatible reads instead of redesigning core handle ownership.
- Re-verify hybrid cache warming after lifecycle fixes.

### 4. Batch-plan and interceptor regressions
- Investigate `FilterProjectionOperator_BatchPlanPath_UsesSpecializedPlan` as an execution/planner regression, not a test loosen-up.
- Investigate missing `WalCache` interceptor reads together with hybrid retained-page failures because both touch page residency after WAL reads and checkpoints.

### 5. Planner expectation drift
- Restore an index-ordered plan for `ORDER BY` over indexed scans if the current `SortOperator` is unintended.
- If the fallback sort is intentional, document the reason and update the assertion to verify the intended property precisely, not with a broad null check.

## Guardrails
- Do not trade hot-path lookup performance for test convenience.
- Prefer test-helper fixes when a Windows file-sharing mismatch is the real issue.
- Prefer targeted metadata persistence over full catalog rewrites.
- Prefer cache-preservation fixes over extra warming passes or unconditional rescans.

## Verification
- Targeted suites:
  - `CollectionBinaryDocumentCodecTests`, `CollectionTests`, `CollectionIndexTests`, `InMemoryDatabaseTests`, `DatabaseMaintenanceTests`
  - `PageReadBufferTests`, `StorageEngineExtensibilityTests`, `HybridDatabaseTests`, `ClientHybridDatabaseOptionsTests`
  - `BatchEvaluationContractTests`, targeted `IntegrationTests`
- Full suite:
  - `.\tests\CSharpDB.Tests\bin\Debug\net10.0\CSharpDB.Tests.exe -reporter quiet -noLogo`

## Remaining Acceptance Criteria
- No collection persistence test loses committed rows after reopen or rollback.
- No pager test throws `WAL not open`.
- Hybrid crash-image tests pass without changing the production file-handle strategy.
- Hybrid hot-set reads stay cache-backed across checkpoints.
- Planner test updates, if any, remain behavior-specific and performance-aware.
