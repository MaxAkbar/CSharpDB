# What's New

## CSharpDB 4.6.5

CSharpDB 4.6.5 adds a Privacy & Retention workflow in Studio, opens the
Storage Inspector without an initial full-file scan, and improves SQL editor
completion and storage write paths. These notes cover changes since v4.6.4.

### Privacy & Retention

- Create and save reusable policies that select records with typed conditions,
  date rules, and explicitly mapped related records. Search, duplicate, import,
  and export policies in **Data Hygiene → Privacy & Retention**.
- Choose fields to erase, replace with a constant or stable anonymous value, or
  partially mask. Preview eligible records, changed counts, preserved fields,
  warnings, and bounded before/after samples before applying a policy.
- Apply a prepared policy in one transaction with bounded batches and
  cancellation before commit. Review committed run receipts and reconcile an
  uncertain commit outcome before retrying.
- Protect keys and relationship columns, reject unsupported constraints or
  mutation callbacks during preparation, and keep records and their declared
  relationships intact.

### Studio and storage improvements

- Open Storage Inspector with a persisted summary. Full page and WAL analysis
  now starts only when **Analyze storage** is requested, with cancellation.
- Restore context-aware SQL editor suggestions for tables, columns, and
  keywords, and expand the supported keyword completion set.
- Reuse WAL snapshot segments and share immutable snapshot maps to reduce
  copying during concurrent reads. Streamline simple INSERT execution and
  checkpoint storage paths.
- Refresh the .NET SDK and selected runtime, tooling, and test dependencies.

### Compatibility and scope

- No database file-format revision is introduced by these changes.
- The initial Privacy & Retention workflow supports a single standard
  file-backed direct database, including durable hybrid storage. Remote,
  sharded, memory-only, and snapshot-only hybrid execution are outside its
  scope. It changes selected live fields; it does not delete records, discover
  unselected personal-data copies, or establish that a person is unidentifiable.
- Privacy runs require an exclusive writable file session. Other writable
  handles can prevent preparation or apply from starting. Review the policy,
  preview, and run outcome before retrying a failed or uncertain operation.
- Storage analysis remains an explicit potentially long-running operation.
  The initial summary does not include results of a full integrity scan.
