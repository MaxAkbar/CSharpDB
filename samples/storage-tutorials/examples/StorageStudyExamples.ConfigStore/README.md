# StorageStudyExamples.ConfigStore

Hierarchical key-value configuration store with namespaces, versioning, and change history.

## Load in the REPL

```
> load config-store
```

## Commands

| Command | Usage | Description |
|---------|-------|-------------|
| `list` | `list [namespace]` | List config entries (all or filtered) |
| `get` | `get <namespace> <key>` | Get a config value |
| `set` | `set <namespace> <key> <value> [type]` | Set a config value (default type: string) |
| `delete` | `delete <namespace> <key>` | Delete a config entry |
| `history` | `history [namespace]` | Show change history |
| `namespaces` | `namespaces` | List all namespaces with counts |
| `rename-ns` | `rename-ns <old> <new>` | Rename a namespace |
| `drop-ns` | `drop-ns <namespace>` | Delete all entries in a namespace |

## Schema

```sql
CREATE TABLE config_entries (
    id INTEGER PRIMARY KEY,
    namespace TEXT,
    config_key TEXT,
    value TEXT,
    value_type TEXT,
    version INTEGER,
    updated_at INTEGER
)
CREATE TABLE config_history (
    id INTEGER PRIMARY KEY,
    namespace TEXT,
    config_key TEXT,
    old_value TEXT,
    new_value TEXT,
    changed_at INTEGER
)
CREATE INDEX idx_config_ns ON config_entries(namespace)
```

## Seed Data

12 config entries across 3 namespaces: `app/display`, `app/network`, and `system/logging`.

## CSharpDB Features Used

Transactions (`BEGIN`/`COMMIT`/`ROLLBACK`) for atomic read-then-write, two-table design (entries + history), `CREATE INDEX`, `GROUP BY`, `ORDER BY`, batch `UPDATE` (rename namespace), bulk `DELETE` (drop namespace with history logging).

See the [root README](../README.md) for full documentation.
