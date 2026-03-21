# StorageStudyExamples.VirtualDrive

Virtual file system with folders, files, and shortcuts stored in a single `.cdb` file.

## Load in the REPL

```
> load virtual-drive
```

## Commands

| Command | Usage | Description |
|---------|-------|-------------|
| `tree` | `tree` | Show the full directory tree |
| `ls` | `ls [path]` | List contents of a directory |
| `cd` | `cd <path>` | Change current directory |
| `pwd` | `pwd` | Print current directory path |
| `cat` | `cat <filename>` | Print file contents |
| `mkdir` | `mkdir <name>` | Create a folder |
| `touch` | `touch <name> [content...]` | Create a file |
| `ln` | `ln <name> <target-path>` | Create a shortcut |
| `rm` | `rm <name>` | Remove a file or folder (recursive) |
| `mv` | `mv <name> <new-name>` | Rename an entry |
| `info` | `info <name>` | Show entry details (type, size, created) |
| `stats` | `stats` | Drive statistics (counts by type, total size) |

## Schema

```sql
CREATE TABLE fs_entries (
    id INTEGER PRIMARY KEY,
    parent_id INTEGER,
    name TEXT,
    entry_type INTEGER,   -- 0=Folder, 1=File, 2=Shortcut
    size INTEGER,
    created_at INTEGER,
    content BLOB,
    target_path TEXT
)
CREATE INDEX idx_fs_parent ON fs_entries(parent_id)
```

## Seed Data

8 folders, 8 files, and 3 desktop shortcuts forming a realistic directory tree with Documents, Pictures, Desktop, and Downloads.

## CSharpDB Features Used

`CREATE INDEX`, `PrepareInsertBatch` (BLOB content), `SELECT WHERE parent_id = ?`, `GROUP BY`, `SUM`, `UPDATE` (rename, move, retarget), `DELETE` (single row and recursive cascade), transactional read-modify-write for BLOB updates.

See the [root README](../README.md) for full documentation.
