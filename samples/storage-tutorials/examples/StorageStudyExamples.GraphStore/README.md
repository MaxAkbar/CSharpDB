# StorageStudyExamples.GraphStore

Social network graph with nodes, directed edges, and traversal queries.

## Load in the REPL

```
> load graph-store
```

## Commands

| Command | Usage | Description |
|---------|-------|-------------|
| `nodes` | `nodes` | List all people |
| `graph` | `graph` | Full adjacency list |
| `follows` | `follows <name>` | Who this person follows |
| `followers` | `followers <name>` | Who follows this person |
| `fof` | `fof <name>` | Friends-of-friends (2-hop) |
| `mutual` | `mutual <name1> <name2>` | People both follow |
| `reciprocal` | `reciprocal` | All mutual follow-back pairs |
| `follow` | `follow <source> <target>` | Add a follow edge |
| `unfollow` | `unfollow <source> <target>` | Remove a follow edge |
| `add-person` | `add-person <name>` | Add a new person |
| `rename` | `rename <old-name> <new-name>` | Rename a person |
| `remove` | `remove <name>` | Remove person and all edges |
| `stats` | `stats` | Connection counts per person |

All commands accept person names instead of IDs.

## Schema

```sql
CREATE TABLE nodes (
    id INTEGER PRIMARY KEY,
    label TEXT,
    node_type TEXT
)
CREATE TABLE edges (
    id INTEGER PRIMARY KEY,
    source_id INTEGER,
    target_id INTEGER,
    edge_type TEXT
)
CREATE INDEX idx_edges_source ON edges(source_id)
CREATE INDEX idx_edges_target ON edges(target_id)
```

## Seed Data

10-person social network (Alice, Bob, Charlie, Diana, Eve, Frank, Grace, Hank, Ivy, Jack) with 23 directed "follows" edges.

## CSharpDB Features Used

`PrepareInsertBatch`, `CREATE INDEX` on `source_id` and `target_id`, `JOIN` (including 3-table JOINs and self-JOINs), `GROUP BY` with `COUNT(*)`, `DISTINCT`, post-query filtering in C# for set operations, `UPDATE` (rename node label), `DELETE` (unfollow edge, cascading user removal across tables in a transaction).

See the [root README](../README.md) for full documentation.
