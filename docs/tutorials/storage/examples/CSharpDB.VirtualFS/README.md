# CSharpDB.VirtualFS

A **virtual file system** built entirely on top of [CSharpDB.Storage](../CSharpDB.Storage) B+trees.
It stores directories, files, and shortcuts inside a single `.cdb` database file, with crash-safe
transactions and a web UI for browsing.

This sample demonstrates how to compose **multiple B+trees** within a single Pager to build a
non-trivial, production-style storage application.

---

## What This Sample Demonstrates

| CSharpDB.Storage concept | How this sample uses it |
|---|---|
| **Multiple B+trees in one file** | Four trees (Entries, Content, PathIndex, Children) share a single Pager and WAL |
| **Composite keys** | `MakeChildKey(parentId, childId)` encodes two IDs into one `long` for directory listing |
| **Hash-based lookup** | `ComputePathHash(parentId, name)` uses SHA-256 to map path segments to tree keys |
| **BTreeCursor range scans** | `ListChildrenAsync` seeks to a lower bound and scans forward to an upper bound |
| **Transactions** | Every mutating operation is wrapped in `BeginTransactionAsync` / `CommitAsync` / `RollbackAsync` |
| **Superblock pattern** | Tree root page IDs and the next-ID counter are persisted at key 0 in the entries tree |
| **Crash safety** | WAL ensures atomicity; partial writes are rolled back on recovery |

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│  VirtualFileSystem (high-level API)                      │
│    CreateDirectoryAsync, WriteFileAsync, ReadFileAsync,  │
│    ListDirectoryAsync, DeleteAsync, CreateShortcutAsync  │
├──────────────────────────────────────────────────────────┤
│  VirtualFileSystemStore (B+tree operations)              │
│    _entries   : id → JSON(FsEntry)     metadata          │
│    _content   : id → byte[]            file content      │
│    _pathIndex : hash(parent,name) → id fast name lookup  │
│    _children  : (parent<<N|child) → id range scan        │
├──────────────────────────────────────────────────────────┤
│  CSharpDB.Storage                                        │
│    Pager → WAL → FileStorageDevice → .cdb file           │
└──────────────────────────────────────────────────────────┘
```

### Four-Tree Design

| B+tree | Key | Value | Purpose |
|---|---|---|---|
| **Entries** | `entryId` (long) | JSON-serialised `FsEntry` | Master metadata for every directory, file, and shortcut |
| **Content** | `entryId` (long) | Raw `byte[]` | File content, stored separately so large files don't bloat metadata pages |
| **PathIndex** | `SHA256(parentId + name)` | `entryId` bytes | O(1) child lookup by name without scanning the parent |
| **Children** | `parentId * 1B + childId` | `entryId` bytes | Directory listing via cursor range scan `[parent*1B+0 .. parent*1B+999M]` |

### Data Model

```csharp
public enum EntryKind : byte
{
    Directory = 1,
    File      = 2,
    Shortcut  = 3,
}

public sealed class FsEntry
{
    public long       Id          { get; set; }
    public long       ParentId    { get; set; }
    public string     Name        { get; set; }
    public EntryKind  Kind        { get; set; }
    public long       Size        { get; set; }
    public DateTime   CreatedUtc  { get; set; }
    public DateTime   ModifiedUtc { get; set; }
    public long?      TargetId    { get; set; }  // shortcuts only
    public Dictionary<string, string>? Attributes { get; set; }
}
```

## Getting Started

### Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download) or later

### Clone and Build

```bash
dotnet build docs/tutorials/storage/examples/CSharpDB.VirtualFS/CSharpDB.VirtualFS.csproj
```

### Run the CLI REPL

```bash
dotnet run --project docs/tutorials/storage/examples/CSharpDB.VirtualFS/CSharpDB.VirtualFS.csproj
```

You'll see an interactive shell:

```
╔══════════════════════════════════════════╗
║  CSharpDB Virtual File System REPL       ║
║  Type 'help' for commands, 'exit' to quit║
╚══════════════════════════════════════════╝
• Database file: virtual_drive.cdb
• Current directory: /

vfs:/> _
```

### Run the Web UI

```bash
dotnet run serve
```

Then open [https://localhost:62288](https://localhost:62288) in your browser.

## CLI Commands

| Command | Description | Example |
|---|---|---|
| `sample` | Reset DB and run the full demo workflow | `sample` |
| `tree [path]` | Render a recursive tree view | `tree /documents` |
| `ls [path]` | List directory contents | `ls` |
| `mkdir <path>` | Create a directory | `mkdir /notes` |
| `write <path> <text>` | Write UTF-8 text to a file | `write /notes/todo.txt "ship it"` |
| `read <path>` | Read and display a file | `read /notes/todo.txt` |
| `info <path>` | Show entry metadata | `info /notes/todo.txt` |
| `shortcut <path> <target>` | Create a shortcut (symlink) | `shortcut /latest /notes/todo.txt` |
| `rm <path>` | Delete a file or empty directory | `rm /notes/todo.txt` |
| `cd <path>` | Change working directory | `cd /documents` |
| `pwd` | Print working directory | `pwd` |
| `reset` | Delete database and start fresh | `reset` |

Path resolution supports relative paths (`.`, `..`) from the current directory.

## REST API

When running in `serve` mode, the following endpoints are available:

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/filesystem/tree?path=/` | Recursive tree view |
| `GET` | `/api/filesystem/entries?path=/` | List directory children |
| `GET` | `/api/filesystem/entry?path=/notes/todo.txt` | Entry metadata |
| `GET` | `/api/filesystem/files/content?path=/notes/todo.txt` | Read file content |
| `POST` | `/api/filesystem/directories` | Create directory (`{ "path": "/notes" }`) |
| `POST` | `/api/filesystem/files` | Write file (`{ "path": "...", "content": "<base64>" }`) |
| `POST` | `/api/filesystem/shortcuts` | Create shortcut (`{ "shortcutPath": "...", "targetPath": "..." }`) |
| `DELETE` | `/api/filesystem/entry?path=/notes/todo.txt` | Delete entry |
| `POST` | `/api/filesystem/reset` | Reset entire database |

## Web UI

The web dashboard provides a visual interface for all operations:

- **Sidebar** with collapsible panels for creating directories, writing files, creating shortcuts, and deleting entries
- **Tree view** showing the full directory hierarchy
- **Directory listing** with type badges (directory, file, shortcut) and clickable navigation
- **File content viewer** with path input
- **Entry inspector** showing JSON metadata
- **Breadcrumb navigation** for quick directory traversal

## Key Code Walkthrough

### Opening or creating the database

```csharp
// VirtualFileSystem.OpenAsync()
var options = new StorageEngineOptionsBuilder()
    .UsePagerOptions(new PagerOptions { MaxCachedPages = 2048 })
    .UseBTreeIndexes()
    .Build();

var factory = new DefaultStorageEngineFactory();
var context = await factory.OpenAsync(filePath, options, ct);

var store = new VirtualFileSystemStore(context.Pager);
```

### Creating four B+trees on first run

```csharp
// VirtualFileSystemStore.InitializeNewAsync()
var entriesRoot  = await BTree.CreateNewAsync(Pager, ct);
var contentRoot  = await BTree.CreateNewAsync(Pager, ct);
var pathIdxRoot  = await BTree.CreateNewAsync(Pager, ct);
var childrenRoot = await BTree.CreateNewAsync(Pager, ct);

_entries  = new BTree(Pager, entriesRoot);
_content  = new BTree(Pager, contentRoot);
_pathIndex = new BTree(Pager, pathIdxRoot);
_children = new BTree(Pager, childrenRoot);
```

### Transaction-wrapped directory creation

```csharp
// VirtualFileSystem.CreateDirectoryAsync()
await _store.Pager.BeginTransactionAsync(ct);
try
{
    await _store.WriteEntryAsync(entry, ct);     // _entries tree
    await _store.IndexEntryAsync(entry, ct);     // _pathIndex + _children trees
    await _store.PersistNextIdAsync(_nextId, ct); // superblock at key 0
    await _store.Pager.CommitAsync(ct);
}
catch
{
    await _store.Pager.RollbackAsync(ct);
    _nextId--;
    throw;
}
```

### Directory listing via cursor range scan

```csharp
// VirtualFileSystemStore.ListChildrenAsync()
var lowerBound = MakeChildKey(parentId, 0);           // parentId * 1_000_000_000 + 0
var upperBound = MakeChildKey(parentId, 999_999_999); // parentId * 1_000_000_000 + 999M

var cursor = _children.CreateCursor();
if (!await cursor.SeekAsync(lowerBound, ct))
    return result;

do
{
    if (cursor.CurrentKey > upperBound) break;
    var childId = BinaryPrimitives.ReadInt64LittleEndian(cursor.CurrentValue.Span);
    result.Add(await GetEntryAsync(childId, ct));
}
while (await cursor.MoveNextAsync(ct));
```

## Project Structure

```
CSharpDB.VirtualFS/
├── Core/
│   ├── EntryKind.cs                       # Directory | File | Shortcut enum
│   ├── FsEntry.cs                         # Entry metadata model
│   ├── VirtualFileSystem.cs               # High-level API (factory + CRUD)
│   ├── VirtualFileSystemStore.cs          # B+tree storage operations
│   ├── VirtualFileSystemPathUtility.cs    # Path hashing, child key encoding
│   └── VirtualFileSystemTreeRenderer.cs   # Recursive tree display
├── Api/
│   ├── IVirtualFileSystemApi.cs           # Service contract + DTOs
│   ├── VirtualFileSystemApiService.cs     # Thread-safe service wrapper
│   └── Clients/
│       ├── InProcessVirtualFileSystemApiClient.cs
│       └── HttpVirtualFileSystemApiClient.cs
├── Hosting/
│   └── VirtualFileSystemWebHost.cs        # ASP.NET Core REST endpoints
├── Infrastructure/
│   └── VirtualFileSystemDatabaseUtility.cs
├── Cli/
│   ├── AnsiConsoleWriter.cs               # Colored terminal output
│   ├── VirtualFileSystemConsolePresenter.cs
│   ├── VirtualFileSystemSampleRunner.cs   # End-to-end demo
│   ├── Commands/                          # REPL command implementations
│   └── Repl/
│       └── ReplHost.cs                    # Interactive command loop
├── wwwroot/
│   └── index.html                         # Single-page web dashboard
├── Program.cs                             # Entry point (CLI or serve mode)
└── CSharpDB.VirtualFS.csproj
```

## Database File

All data is stored in a single file: `virtual_drive.cdb` (with a companion `.wal` file during writes).
Delete both files to start fresh, or use the `reset` command.
