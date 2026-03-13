// ============================================================================
// Virtual Drive Example (Folders, Files, Shortcuts)
// ============================================================================
//
// Demonstrates using CSharpDB as a virtual file system stored in a single
// .cdb file. Supports folders, files with BLOB content, and shortcuts
// (symlink-like entries pointing to other paths).
// Shows: hierarchical data via parent_id, BLOB storage, recursive tree
// walking in C#, path resolution, CREATE INDEX, UPDATE (rename, move,
// retarget), DELETE (single row, recursive cascade), transactional
// read-modify-write for BLOB content, copy via SELECT+INSERT.
// ============================================================================

using System.Text;
using CSharpDB.Primitives;
using StorageStudyExamples.Core;

namespace StorageStudyExamples.VirtualDrive;

public sealed class VirtualDriveStore : DataStoreBase
{
    // Entry types
    private const int Folder = 0;
    private const int File = 1;
    private const int Shortcut = 2;

    // IDs assigned during seeding, used by the demo
    private int _nextId = 1;
    private int _root, _docs, _projects, _personal, _pictures, _vacation, _desktop, _downloads;

    // Current directory for interactive commands
    private int _currentDirId;

    public override string Name => "Virtual Drive";
    public override string CommandName => "virtual-drive";
    public override string Description => "Virtual file system with folders, files, and shortcuts.";

    private static string TypeIcon(long entryType) => entryType switch
    {
        Folder => "[DIR]",
        File => "[FILE]",
        Shortcut => "[LNK]",
        _ => "[?]"
    };

    // ── Commands ───────────────────────────────────────────────────────────

    public override IReadOnlyList<CommandInfo> GetCommands() =>
    [
        new("tree",  "tree",                        "Show the full directory tree"),
        new("ls",    "ls [path]",                   "List contents of a directory (default: current)"),
        new("cd",    "cd <path>",                   "Change current directory"),
        new("cat",   "cat <filename>",              "Print file contents"),
        new("mkdir", "mkdir <name>",                "Create a folder in current directory"),
        new("touch", "touch <name> [content...]",   "Create a file in current directory"),
        new("ln",    "ln <name> <target-path>",     "Create a shortcut"),
        new("rm",    "rm <name>",                   "Remove a file or folder (recursive)"),
        new("mv",    "mv <name> <new-name>",        "Rename an entry"),
        new("info",  "info <name>",                 "Show entry details (type, size, created)"),
        new("pwd",   "pwd",                         "Print current directory path"),
        new("stats", "stats",                       "Drive statistics (counts by type, total size)"),
    ];

    public override async Task<bool> ExecuteCommandAsync(string commandName, string args, TextWriter output)
    {
        switch (commandName)
        {
            case "tree":
                await PrintTreeAsync(output, _root, "");
                return true;

            case "ls":
            {
                var dirId = _currentDirId;
                if (!string.IsNullOrWhiteSpace(args))
                {
                    var resolved = await ResolveDirId(args);
                    if (resolved < 0)
                    {
                        output.WriteLine($"Directory not found: {args}");
                        return true;
                    }
                    dirId = resolved;
                }
                await ListDirectoryAsync(output, dirId);
                return true;
            }

            case "cd":
            {
                if (string.IsNullOrWhiteSpace(args))
                {
                    output.WriteLine("Usage: cd <path>");
                    return true;
                }

                if (args == "/")
                {
                    _currentDirId = _root;
                    output.WriteLine(await GetPathForId(_root));
                    return true;
                }

                if (args == "..")
                {
                    // Go to parent
                    await using var result = await Db.ExecuteAsync(
                        $"SELECT parent_id FROM fs_entries WHERE id = {_currentDirId}");
                    var rows = await result.ToListAsync();
                    if (rows.Count > 0 && !rows[0][0].IsNull && rows[0][0].AsInteger >= 0)
                    {
                        var parentId = (int)rows[0][0].AsInteger;
                        // Don't go above root (parent_id = -1)
                        await using var parentCheck = await Db.ExecuteAsync(
                            $"SELECT id FROM fs_entries WHERE id = {parentId}");
                        var parentRows = await parentCheck.ToListAsync();
                        if (parentRows.Count > 0)
                            _currentDirId = parentId;
                    }
                    output.WriteLine(await GetPathForId(_currentDirId));
                    return true;
                }

                var resolved = await ResolveDirId(args);
                if (resolved < 0)
                {
                    output.WriteLine($"Directory not found: {args}");
                    return true;
                }
                _currentDirId = resolved;
                output.WriteLine(await GetPathForId(_currentDirId));
                return true;
            }

            case "cat":
            {
                if (string.IsNullOrWhiteSpace(args))
                {
                    output.WriteLine("Usage: cat <filename>");
                    return true;
                }
                await ReadFileAsync(output, _currentDirId, args);
                return true;
            }

            case "mkdir":
            {
                if (string.IsNullOrWhiteSpace(args))
                {
                    output.WriteLine("Usage: mkdir <name>");
                    return true;
                }
                long now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                var id = await MkdirAsync(_currentDirId, args, now);
                output.WriteLine($"Created directory: {args} (id={id})");
                return true;
            }

            case "touch":
            {
                if (string.IsNullOrWhiteSpace(args))
                {
                    output.WriteLine("Usage: touch <name> [content...]");
                    return true;
                }
                var parts = args.Split(' ', 2);
                var name = parts[0];
                var content = parts.Length > 1 ? parts[1] : "";
                long now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                var id = await WriteFileAsync(_currentDirId, name, content, now);
                output.WriteLine($"Created file: {name} (id={id}, {Encoding.UTF8.GetByteCount(content)} bytes)");
                return true;
            }

            case "ln":
            {
                if (string.IsNullOrWhiteSpace(args))
                {
                    output.WriteLine("Usage: ln <name> <target-path>");
                    return true;
                }
                var parts = args.Split(' ', 2);
                if (parts.Length < 2)
                {
                    output.WriteLine("Usage: ln <name> <target-path>");
                    return true;
                }
                var name = parts[0];
                var targetPath = parts[1];
                long now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                var id = await CreateShortcutAsync(_currentDirId, name, targetPath, now);
                output.WriteLine($"Created shortcut: {name} -> {targetPath} (id={id})");
                return true;
            }

            case "rm":
            {
                if (string.IsNullOrWhiteSpace(args))
                {
                    output.WriteLine("Usage: rm <name>");
                    return true;
                }
                var entry = await FindEntryByName(args);
                if (entry == null)
                {
                    output.WriteLine($"Not found: {args}");
                    return true;
                }
                await DeleteRecursiveAsync(entry.Value.Id);
                output.WriteLine($"Removed: {args}");
                return true;
            }

            case "mv":
            {
                if (string.IsNullOrWhiteSpace(args))
                {
                    output.WriteLine("Usage: mv <name> <new-name>");
                    return true;
                }
                var parts = args.Split(' ', 2);
                if (parts.Length < 2)
                {
                    output.WriteLine("Usage: mv <name> <new-name>");
                    return true;
                }
                var oldName = parts[0];
                var newName = parts[1];
                var entry = await FindEntryByName(oldName);
                if (entry == null)
                {
                    output.WriteLine($"Not found: {oldName}");
                    return true;
                }
                await Db.ExecuteAsync(
                    $"UPDATE fs_entries SET name = '{Esc(newName)}' WHERE id = {entry.Value.Id}");
                output.WriteLine($"Renamed: {oldName} -> {newName}");
                return true;
            }

            case "info":
            {
                if (string.IsNullOrWhiteSpace(args))
                {
                    output.WriteLine("Usage: info <name>");
                    return true;
                }
                await using var result = await Db.ExecuteAsync(
                    $"SELECT id, name, entry_type, size, created_at, target_path FROM fs_entries WHERE parent_id = {_currentDirId} AND name = '{Esc(args)}'");
                var rows = await result.ToListAsync();
                if (rows.Count == 0)
                {
                    output.WriteLine($"Not found: {args}");
                    return true;
                }
                var row = rows[0];
                var entryType = row[2].AsInteger;
                output.WriteLine($"  Name:    {row[1].AsText}");
                output.WriteLine($"  Type:    {TypeIcon(entryType)} ({entryType switch { Folder => "Folder", File => "File", Shortcut => "Shortcut", _ => "Unknown" }})");
                output.WriteLine($"  Size:    {row[3].AsInteger} bytes");
                output.WriteLine($"  Created: {row[4].AsInteger}");
                output.WriteLine($"  ID:      {row[0].AsInteger}");
                if (entryType == Shortcut && !row[5].IsNull)
                    output.WriteLine($"  Target:  {row[5].AsText}");
                return true;
            }

            case "pwd":
            {
                output.WriteLine(await GetPathForId(_currentDirId));
                return true;
            }

            case "stats":
            {
                await using (var result = await Db.ExecuteAsync(
                    "SELECT entry_type, COUNT(*) FROM fs_entries GROUP BY entry_type ORDER BY entry_type"))
                {
                    await foreach (var row in result.GetRowsAsync())
                    {
                        var label = row[0].AsInteger switch
                        {
                            Folder => "Folders",
                            File => "Files",
                            Shortcut => "Shortcuts",
                            _ => "Unknown"
                        };
                        output.WriteLine($"  {label,-12} {row[1].AsInteger}");
                    }
                }

                await using (var result = await Db.ExecuteAsync(
                    $"SELECT COUNT(*), SUM(size) FROM fs_entries WHERE entry_type = {File}"))
                {
                    var rows = await result.ToListAsync();
                    if (rows.Count > 0 && !rows[0][1].IsNull)
                    {
                        var totalSize = rows[0][1].AsInteger;
                        output.WriteLine($"  Total size:  {totalSize:N0} bytes");
                    }
                }
                return true;
            }

            default:
                return false;
        }
    }

    // ── Schema ─────────────────────────────────────────────────────────────

    protected override async Task CreateSchemaAsync()
    {
        await Db.ExecuteAsync("""
            CREATE TABLE fs_entries (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER,
                name TEXT,
                entry_type INTEGER,
                size INTEGER,
                created_at INTEGER,
                content BLOB,
                target_path TEXT
            )
            """);
        await Db.ExecuteAsync("CREATE INDEX idx_fs_parent ON fs_entries(parent_id)");
    }

    // ── Seed data ──────────────────────────────────────────────────────────

    protected override async Task SeedDataAsync()
    {
        long now = 1700000000;

        // Folders
        _root = await MkdirAsync(-1, "/", now);
        _docs = await MkdirAsync(_root, "Documents", now);
        _projects = await MkdirAsync(_docs, "Projects", now);
        _personal = await MkdirAsync(_docs, "Personal", now);
        _pictures = await MkdirAsync(_root, "Pictures", now);
        _vacation = await MkdirAsync(_pictures, "Vacation", now);
        _desktop = await MkdirAsync(_root, "Desktop", now);
        _downloads = await MkdirAsync(_root, "Downloads", now);

        // Files
        await WriteFileAsync(_root, "readme.txt",
            "Welcome to the CSharpDB Virtual Drive!\nThis file system is stored entirely in a single .cdb database file.", now);
        await WriteFileAsync(_docs, "notes.md",
            "# Meeting Notes\n- Discussed Q4 roadmap\n- Action item: update storage benchmarks\n- Next meeting: Friday 3pm", now);
        await WriteFileAsync(_projects, "hello.cs",
            "using System;\n\nclass Program\n{\n    static void Main()\n    {\n        Console.WriteLine(\"Hello from the virtual drive!\");\n    }\n}", now);
        await WriteFileAsync(_projects, "todo.txt",
            "1. Implement B+tree compaction\n2. Add WAL compression\n3. Write integration tests", now);
        await WriteFileAsync(_personal, "budget.csv",
            "Category,Amount\nRent,1200\nGroceries,400\nUtilities,150\nSavings,500", now);
        await WriteFileAsync(_vacation, "day1.txt",
            "Arrived at the coast. Weather is perfect.\nVisited the lighthouse and had seafood for dinner.", now);
        await WriteFileAsync(_downloads, "report-q4.pdf",
            "(binary PDF content would go here — this is a placeholder)", now);
        await WriteFileAsync(_downloads, "setup-v2.1.exe",
            "(binary installer content would go here — this is a placeholder)", now);

        // Shortcuts
        await CreateShortcutAsync(_desktop, "Projects", "/Documents/Projects", now);
        await CreateShortcutAsync(_desktop, "notes.md", "/Documents/notes.md", now);
        await CreateShortcutAsync(_desktop, "Q4 Report", "/Downloads/report-q4.pdf", now);

        // Set initial current directory to root
        _currentDirId = _root;
    }

    // ── Scripted demo ──────────────────────────────────────────────────────

    public override async Task RunDemoAsync(TextWriter output)
    {
        long now = 1700000000;

        output.WriteLine("--- Building virtual drive ---");
        output.WriteLine("  Created 8 folders.");
        output.WriteLine("  Created 8 files.");
        output.WriteLine("  Created 3 shortcuts on Desktop.");
        output.WriteLine();

        // ── Print full tree
        output.WriteLine("--- Virtual Drive Tree ---");
        await PrintTreeAsync(output, _root, "");
        output.WriteLine();

        // ── List directories
        output.WriteLine("--- ls /Documents ---");
        await ListDirectoryAsync(output, _docs);
        output.WriteLine();

        output.WriteLine("--- ls /Desktop ---");
        await ListDirectoryAsync(output, _desktop);
        output.WriteLine();

        // ── Read a file
        output.WriteLine("--- cat /Documents/Projects/hello.cs ---");
        await ReadFileAsync(output, _projects, "hello.cs");
        output.WriteLine();

        // ── Resolve shortcut
        output.WriteLine("--- Resolve shortcut: /Desktop/notes.md ---");
        await ResolveShortcutAsync(output, _desktop, "notes.md");
        output.WriteLine();

        // ── File system operations (Update, Delete, Copy)
        output.WriteLine("--- File system operations ---");

        // Rename: /Documents/notes.md -> /Documents/meeting-notes.md (id=10)
        await Db.ExecuteAsync("UPDATE fs_entries SET name = 'meeting-notes.md' WHERE id = 10");
        output.WriteLine("  Renamed: /Documents/notes.md -> /Documents/meeting-notes.md");

        // Move: /Downloads/report-q4.pdf -> /Documents/Projects/ (id=15)
        await Db.ExecuteAsync($"UPDATE fs_entries SET parent_id = {_projects} WHERE id = 15");
        output.WriteLine("  Moved:   /Downloads/report-q4.pdf -> /Documents/Projects/");

        // Update file content: append a line to todo.txt (id=12)
        await Db.BeginTransactionAsync();
        try
        {
            string existingContent;
            await using (var readResult = await Db.ExecuteAsync("SELECT content FROM fs_entries WHERE id = 12"))
            {
                var readRows = await readResult.ToListAsync();
                existingContent = Encoding.UTF8.GetString(readRows[0][0].AsBlob);
            }

            var updatedContent = existingContent + "\n4. Benchmark page cache under contention";
            var updatedBytes = Encoding.UTF8.GetBytes(updatedContent);

            await Db.ExecuteAsync("DELETE FROM fs_entries WHERE id = 12");
            var updateBatch = Db.PrepareInsertBatch("fs_entries");
            updateBatch.AddRow(
                DbValue.FromInteger(12),
                DbValue.FromInteger(_projects),
                DbValue.FromText("todo.txt"),
                DbValue.FromInteger(File),
                DbValue.FromInteger(updatedBytes.Length),
                DbValue.FromInteger(now),
                DbValue.FromBlob(updatedBytes),
                DbValue.Null);
            await updateBatch.ExecuteAsync();
            await Db.CommitAsync();
        }
        catch
        {
            await Db.RollbackAsync();
            throw;
        }
        output.WriteLine("  Updated: /Documents/Projects/todo.txt (appended line)");

        // Copy: hello.cs (id=11) -> /Desktop/hello.cs
        byte[] sourceContent;
        long sourceSize;
        await using (var copyResult = await Db.ExecuteAsync("SELECT content, size FROM fs_entries WHERE id = 11"))
        {
            var copyRows = await copyResult.ToListAsync();
            sourceContent = copyRows[0][0].AsBlob;
            sourceSize = copyRows[0][1].AsInteger;
        }
        var copyId = _nextId++;
        var copyBatch = Db.PrepareInsertBatch("fs_entries");
        copyBatch.AddRow(
            DbValue.FromInteger(copyId),
            DbValue.FromInteger(_desktop),
            DbValue.FromText("hello.cs"),
            DbValue.FromInteger(File),
            DbValue.FromInteger(sourceSize),
            DbValue.FromInteger(now),
            DbValue.FromBlob(sourceContent),
            DbValue.Null);
        await copyBatch.ExecuteAsync();
        output.WriteLine("  Copied:  /Documents/Projects/hello.cs -> /Desktop/hello.cs");

        // Retarget shortcuts
        await Db.ExecuteAsync("UPDATE fs_entries SET target_path = '/Documents/meeting-notes.md' WHERE id = 18");
        output.WriteLine("  Retarget: /Desktop/notes.md shortcut -> /Documents/meeting-notes.md");

        await Db.ExecuteAsync("UPDATE fs_entries SET target_path = '/Documents/Projects/report-q4.pdf' WHERE id = 19");
        output.WriteLine("  Retarget: /Desktop/Q4 Report shortcut -> /Documents/Projects/report-q4.pdf");

        // Delete file
        await Db.ExecuteAsync("DELETE FROM fs_entries WHERE id = 16");
        output.WriteLine("  Deleted: /Downloads/setup-v2.1.exe");

        // Delete folder recursively
        await DeleteRecursiveAsync(_pictures);
        output.WriteLine("  Deleted: /Pictures (recursive — 1 folder, 1 subfolder, 1 file)");
        output.WriteLine();

        // ── Re-print tree after mutations
        output.WriteLine("--- Virtual Drive Tree (after mutations) ---");
        await PrintTreeAsync(output, _root, "");
        output.WriteLine();

        // ── Verify updated file content
        output.WriteLine("--- cat /Documents/Projects/todo.txt (after update) ---");
        await ReadFileAsync(output, _projects, "todo.txt");
        output.WriteLine();

        // ── Verify retargeted shortcut
        output.WriteLine("--- Resolve shortcut: /Desktop/notes.md (after retarget) ---");
        await ResolveShortcutAsync(output, _desktop, "notes.md");
        output.WriteLine();

        // ── Drive statistics
        output.WriteLine("--- Drive statistics ---");

        await using (var result = await Db.ExecuteAsync(
            "SELECT entry_type, COUNT(*) FROM fs_entries GROUP BY entry_type ORDER BY entry_type"))
        {
            await foreach (var row in result.GetRowsAsync())
            {
                var label = row[0].AsInteger switch
                {
                    Folder => "Folders",
                    File => "Files",
                    Shortcut => "Shortcuts",
                    _ => "Unknown"
                };
                output.WriteLine($"  {label,-12} {row[1].AsInteger}");
            }
        }

        await using (var result = await Db.ExecuteAsync(
            $"SELECT COUNT(*), SUM(size) FROM fs_entries WHERE entry_type = {File}"))
        {
            var rows = await result.ToListAsync();
            if (rows.Count > 0 && !rows[0][1].IsNull)
            {
                var totalSize = rows[0][1].AsInteger;
                output.WriteLine($"  Total size:  {totalSize:N0} bytes");
            }
        }
    }

    // ── Interactive helpers ────────────────────────────────────────────────

    /// <summary>Find a child folder by name in the current directory.</summary>
    private async Task<int> ResolveDirId(string name)
    {
        await using var result = await Db.ExecuteAsync(
            $"SELECT id, entry_type FROM fs_entries WHERE parent_id = {_currentDirId} AND name = '{Esc(name)}'");
        var rows = await result.ToListAsync();
        if (rows.Count == 0) return -1;
        if (rows[0][1].AsInteger != Folder) return -1;
        return (int)rows[0][0].AsInteger;
    }

    /// <summary>Find a child entry by name in the current directory.</summary>
    private async Task<(int Id, int EntryType)?> FindEntryByName(string name)
    {
        await using var result = await Db.ExecuteAsync(
            $"SELECT id, entry_type FROM fs_entries WHERE parent_id = {_currentDirId} AND name = '{Esc(name)}'");
        var rows = await result.ToListAsync();
        if (rows.Count == 0) return null;
        return ((int)rows[0][0].AsInteger, (int)rows[0][1].AsInteger);
    }

    /// <summary>Build the full path string by walking parent_id up to root.</summary>
    private async Task<string> GetPathForId(int id)
    {
        var segments = new List<string>();
        var currentId = id;

        while (currentId >= 0)
        {
            await using var result = await Db.ExecuteAsync(
                $"SELECT name, parent_id FROM fs_entries WHERE id = {currentId}");
            var rows = await result.ToListAsync();
            if (rows.Count == 0) break;

            var name = rows[0][0].AsText;
            var parentId = rows[0][1].IsNull ? -1 : (int)rows[0][1].AsInteger;

            if (parentId < 0)
            {
                // This is the root node
                break;
            }

            segments.Add(name);
            currentId = parentId;
        }

        segments.Reverse();
        return "/" + string.Join("/", segments);
    }

    /// <summary>Get the next available ID from the database.</summary>
    private async Task<int> GetNextId()
    {
        await using var result = await Db.ExecuteAsync("SELECT MAX(id) FROM fs_entries");
        var rows = await result.ToListAsync();
        if (rows.Count == 0 || rows[0][0].IsNull) return 1;
        return (int)rows[0][0].AsInteger + 1;
    }

    // ── Data helpers ───────────────────────────────────────────────────────

    private async Task<int> MkdirAsync(int parentId, string name, long createdAt)
    {
        var id = _nextId++;
        await Db.ExecuteAsync(
            $"INSERT INTO fs_entries VALUES ({id}, {parentId}, '{Esc(name)}', {Folder}, 0, {createdAt}, NULL, NULL)");
        return id;
    }

    private async Task<int> WriteFileAsync(int parentId, string name, string textContent, long createdAt)
    {
        var id = _nextId++;
        var bytes = Encoding.UTF8.GetBytes(textContent);
        var batch = Db.PrepareInsertBatch("fs_entries");
        batch.AddRow(
            DbValue.FromInteger(id),
            DbValue.FromInteger(parentId),
            DbValue.FromText(name),
            DbValue.FromInteger(File),
            DbValue.FromInteger(bytes.Length),
            DbValue.FromInteger(createdAt),
            DbValue.FromBlob(bytes),
            DbValue.Null);
        await batch.ExecuteAsync();
        return id;
    }

    private async Task<int> CreateShortcutAsync(int parentId, string name, string targetPath, long createdAt)
    {
        var id = _nextId++;
        await Db.ExecuteAsync(
            $"INSERT INTO fs_entries VALUES ({id}, {parentId}, '{Esc(name)}', {Shortcut}, 0, {createdAt}, NULL, '{Esc(targetPath)}')");
        return id;
    }

    // ── Tree printer ───────────────────────────────────────────────────────

    private async Task PrintTreeAsync(TextWriter output, int nodeId, string indent)
    {
        await using (var result = await Db.ExecuteAsync(
            $"SELECT name, entry_type, size, target_path FROM fs_entries WHERE id = {nodeId}"))
        {
            var rows = await result.ToListAsync();
            if (rows.Count == 0) return;

            var name = rows[0][0].AsText;
            var entryType = rows[0][1].AsInteger;
            var size = rows[0][2].AsInteger;
            var target = rows[0][3].IsNull ? null : rows[0][3].AsText;

            var suffix = entryType switch
            {
                File => $" ({size} bytes)",
                Shortcut => $" -> {target}",
                _ => ""
            };

            output.WriteLine($"{indent}{TypeIcon(entryType)} {name}{suffix}");
        }

        var children = new List<(long Id, long Type)>();
        await using (var result = await Db.ExecuteAsync(
            $"SELECT id, entry_type FROM fs_entries WHERE parent_id = {nodeId} ORDER BY entry_type, name"))
        {
            await foreach (var row in result.GetRowsAsync())
                children.Add((row[0].AsInteger, row[1].AsInteger));
        }

        for (var i = 0; i < children.Count; i++)
        {
            var isLast = i == children.Count - 1;
            var childIndent = indent + (isLast ? "    " : "|   ");
            var connector = indent + (isLast ? "`-- " : "|-- ");
            await PrintNodeWithConnectorAsync(output, (int)children[i].Id, connector, childIndent);
        }
    }

    private async Task PrintNodeWithConnectorAsync(TextWriter output, int nodeId, string connector, string childIndent)
    {
        await using (var result = await Db.ExecuteAsync(
            $"SELECT name, entry_type, size, target_path FROM fs_entries WHERE id = {nodeId}"))
        {
            var rows = await result.ToListAsync();
            if (rows.Count == 0) return;

            var name = rows[0][0].AsText;
            var entryType = rows[0][1].AsInteger;
            var size = rows[0][2].AsInteger;
            var target = rows[0][3].IsNull ? null : rows[0][3].AsText;

            var suffix = entryType switch
            {
                File => $" ({size} bytes)",
                Shortcut => $" -> {target}",
                _ => ""
            };

            output.WriteLine($"{connector}{TypeIcon(entryType)} {name}{suffix}");
        }

        var children = new List<long>();
        await using (var result = await Db.ExecuteAsync(
            $"SELECT id FROM fs_entries WHERE parent_id = {nodeId} ORDER BY entry_type, name"))
        {
            await foreach (var row in result.GetRowsAsync())
                children.Add(row[0].AsInteger);
        }

        for (var i = 0; i < children.Count; i++)
        {
            var isLast = i == children.Count - 1;
            var nextChildIndent = childIndent + (isLast ? "    " : "|   ");
            var nextConnector = childIndent + (isLast ? "`-- " : "|-- ");
            await PrintNodeWithConnectorAsync(output, (int)children[i], nextConnector, nextChildIndent);
        }
    }

    // ── Query helpers ──────────────────────────────────────────────────────

    private async Task ListDirectoryAsync(TextWriter output, int dirId)
    {
        await using var result = await Db.ExecuteAsync(
            $"SELECT name, entry_type, size, target_path FROM fs_entries WHERE parent_id = {dirId} ORDER BY entry_type, name");
        await foreach (var row in result.GetRowsAsync())
        {
            var name = row[0].AsText;
            var entryType = row[1].AsInteger;
            var size = row[2].AsInteger;
            var target = row[3].IsNull ? null : row[3].AsText;

            var details = entryType switch
            {
                Folder => "",
                File => $"{size,8} bytes",
                Shortcut => $"-> {target}",
                _ => ""
            };

            output.WriteLine($"  {TypeIcon(entryType)} {name,-24} {details}");
        }
    }

    private async Task ReadFileAsync(TextWriter output, int dirId, string fileName)
    {
        await using var result = await Db.ExecuteAsync(
            $"SELECT content FROM fs_entries WHERE parent_id = {dirId} AND name = '{Esc(fileName)}' AND entry_type = {File}");
        var rows = await result.ToListAsync();

        if (rows.Count == 0)
        {
            output.WriteLine($"  File not found: {fileName}");
            return;
        }

        if (rows[0][0].IsNull)
        {
            output.WriteLine("  (empty file)");
            return;
        }

        var content = Encoding.UTF8.GetString(rows[0][0].AsBlob);
        foreach (var line in content.Split('\n'))
            output.WriteLine($"  {line}");
    }

    private async Task ResolveShortcutAsync(TextWriter output, int dirId, string shortcutName)
    {
        await using var result = await Db.ExecuteAsync(
            $"SELECT target_path FROM fs_entries WHERE parent_id = {dirId} AND name = '{Esc(shortcutName)}' AND entry_type = {Shortcut}");
        var rows = await result.ToListAsync();
        if (rows.Count == 0)
        {
            output.WriteLine($"  Not a shortcut: {shortcutName}");
            return;
        }

        var targetPath = rows[0][0].AsText;
        output.WriteLine($"  Shortcut target: {targetPath}");

        var parts = targetPath.Split('/', StringSplitOptions.RemoveEmptyEntries);
        var currentId = -1;

        await using (var rootResult = await Db.ExecuteAsync("SELECT id FROM fs_entries WHERE parent_id = -1"))
        {
            var rootRows = await rootResult.ToListAsync();
            if (rootRows.Count > 0)
                currentId = (int)rootRows[0][0].AsInteger;
        }

        var resolved = true;
        foreach (var part in parts)
        {
            await using var partResult = await Db.ExecuteAsync(
                $"SELECT id, entry_type FROM fs_entries WHERE parent_id = {currentId} AND name = '{Esc(part)}'");
            var partRows = await partResult.ToListAsync();

            if (partRows.Count == 0)
            {
                output.WriteLine($"  Resolution failed at: {part}");
                resolved = false;
                break;
            }

            currentId = (int)partRows[0][0].AsInteger;
        }

        if (resolved)
        {
            await using var targetResult = await Db.ExecuteAsync(
                $"SELECT name, entry_type, size FROM fs_entries WHERE id = {currentId}");
            var targetRows = await targetResult.ToListAsync();
            if (targetRows.Count > 0)
            {
                var name = targetRows[0][0].AsText;
                var entryType = targetRows[0][1].AsInteger;
                var size = targetRows[0][2].AsInteger;
                output.WriteLine($"  Resolved to: {TypeIcon(entryType)} {name}" +
                                  (entryType == File ? $" ({size} bytes)" : ""));
            }
        }
    }

    private async Task DeleteRecursiveAsync(int nodeId)
    {
        var childIds = new List<long>();
        await using (var result = await Db.ExecuteAsync(
            $"SELECT id FROM fs_entries WHERE parent_id = {nodeId}"))
        {
            await foreach (var row in result.GetRowsAsync())
                childIds.Add(row[0].AsInteger);
        }

        foreach (var childId in childIds)
            await DeleteRecursiveAsync((int)childId);

        await Db.ExecuteAsync($"DELETE FROM fs_entries WHERE id = {nodeId}");
    }
}
