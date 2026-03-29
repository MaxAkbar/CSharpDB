using CSharpDB.Storage.Paging;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.VirtualFS;

// ──────────────────────────────────────────────────────────────
//  Virtual file system built on CSharpDB.Storage B+trees
// ──────────────────────────────────────────────────────────────

/// <summary>
/// A virtual file system that stores directories, files, and shortcuts
/// inside a single CSharpDB.Storage database file.
///
/// Internally it uses four B+trees:
///   1. Entries  – id → serialised FsEntry (metadata)
///   2. Content  – id → raw file bytes
///   3. PathIdx  – hash(parentId,name) → id   (fast lookup by path)
///   4. Children – (parentId &lt;&lt; 24 | seq) → id  (directory listing)
///
/// All mutating operations are wrapped in a Pager transaction so the
/// file system is crash-safe.
/// </summary>
public sealed class VirtualFileSystem : IAsyncDisposable
{
    private readonly IVirtualFileSystemStore _store;
    private readonly IVirtualFileSystemTreeRenderer _treeRenderer;

    private long _nextId;

    public const long RootDirectoryId = 1;

    private VirtualFileSystem(IVirtualFileSystemStore store, IVirtualFileSystemTreeRenderer treeRenderer)
    {
        _store = store;
        _treeRenderer = treeRenderer;
    }

    // ──────────────────────────────────────────────────────────
    //  Factory – open or create
    // ──────────────────────────────────────────────────────────

    /// <summary>
    /// Open (or create) a virtual file system backed by <paramref name="filePath"/>.
    /// </summary>
    public static async Task<VirtualFileSystem> OpenAsync(string filePath, CancellationToken ct = default)
    {
        var isNew = !File.Exists(filePath);

        var options = new StorageEngineOptionsBuilder()
            .UsePagerOptions(new PagerOptions { MaxCachedPages = 2048 })
            .UseBTreeIndexes()
            .Build();

        var factory = new DefaultStorageEngineFactory();
        var context = await factory.OpenAsync(filePath, options, ct);

        var store = new VirtualFileSystemStore(context.Pager);
        var vfs = new VirtualFileSystem(store, new VirtualFileSystemTreeRenderer());

        if (isNew)
        {
            vfs._nextId = await store.InitializeNewAsync(ct);
        }
        else
        {
            vfs._nextId = await store.LoadAsync(ct);
        }

        return vfs;
    }

    // ──────────────────────────────────────────────────────────
    //  Public API – file system operations
    // ──────────────────────────────────────────────────────────

    /// <summary>Create a directory. <paramref name="path"/> is slash-separated, e.g. "/documents/work".</summary>
    public async Task<FsEntry> CreateDirectoryAsync(string path, CancellationToken ct = default)
    {
        var parts = VirtualFileSystemPathUtility.SplitPath(path);
        var parentId = await ResolveParentAsync(parts, ct);
        var name = parts[^1];

        if (await _store.LookupChildAsync(parentId, name, ct) is not null)
            throw new IOException($"An entry named '{name}' already exists in the parent directory.");

        var entry = new FsEntry
        {
            Id = _nextId++,
            ParentId = parentId,
            Name = name,
            Kind = EntryKind.Directory,
            CreatedUtc = DateTime.UtcNow,
            ModifiedUtc = DateTime.UtcNow,
        };

        await _store.Pager.BeginTransactionAsync(ct);
        try
        {
            await _store.WriteEntryAsync(entry, ct);
            await _store.IndexEntryAsync(entry, ct);
            await _store.PersistNextIdAsync(_nextId, ct);
            await _store.Pager.CommitAsync(ct);
        }
        catch
        {
            await _store.Pager.RollbackAsync(ct);
            _nextId--;
            throw;
        }

        return entry;
    }

    /// <summary>List immediate children of a directory.</summary>
    public async Task<List<FsEntry>> ListDirectoryAsync(string path, CancellationToken ct = default)
    {
        var dirId = await ResolvePathAsync(path, ct);
        return await _store.ListChildrenAsync(dirId, ct);
    }

    // ──────────────────────────────────────────────────────────
    //  Public API – files
    // ──────────────────────────────────────────────────────────

    /// <summary>Write a file. Creates or overwrites.</summary>
    public async Task<FsEntry> WriteFileAsync(string path, byte[] data, CancellationToken ct = default)
    {
        var parts = VirtualFileSystemPathUtility.SplitPath(path);
        var parentId = await ResolveParentAsync(parts, ct);
        var name = parts[^1];

        var existing = await _store.LookupChildAsync(parentId, name, ct);

        await _store.Pager.BeginTransactionAsync(ct);
        try
        {
            FsEntry entry;
            if (existing is not null)
            {
                // Overwrite
                existing.Size = data.Length;
                existing.ModifiedUtc = DateTime.UtcNow;
                await _store.WriteEntryAsync(existing, ct);
                await _store.WriteContentAsync(existing.Id, data, ct);
                entry = existing;
            }
            else
            {
                // Create new
                entry = new FsEntry
                {
                    Id = _nextId++,
                    ParentId = parentId,
                    Name = name,
                    Kind = EntryKind.File,
                    Size = data.Length,
                    CreatedUtc = DateTime.UtcNow,
                    ModifiedUtc = DateTime.UtcNow,
                };

                await _store.WriteEntryAsync(entry, ct);
                await _store.WriteContentAsync(entry.Id, data, ct);
                await _store.IndexEntryAsync(entry, ct);
                await _store.PersistNextIdAsync(_nextId, ct);
            }

            await _store.Pager.CommitAsync(ct);
            return entry;
        }
        catch
        {
            await _store.Pager.RollbackAsync(ct);
            if (existing is null)
                _nextId--;
            throw;
        }
    }

    /// <summary>Read file content by path.</summary>
    public async Task<byte[]> ReadFileAsync(string path, CancellationToken ct = default)
    {
        var id = await ResolvePathAsync(path, ct);
        var entry = await _store.GetEntryAsync(id, ct);
        if (entry.Kind == EntryKind.Directory)
            throw new IOException($"'{path}' is a directory, not a file.");

        // Follow shortcut
        if (entry.Kind == EntryKind.Shortcut && entry.TargetId.HasValue)
            id = entry.TargetId.Value;

        var data = await _store.ReadContentAsync(id, ct);
        return data ?? Array.Empty<byte>();
    }

    /// <summary>Get metadata for an entry by path.</summary>
    public async Task<FsEntry> GetEntryInfoAsync(string path, CancellationToken ct = default)
    {
        var id = await ResolvePathAsync(path, ct);
        return await _store.GetEntryAsync(id, ct);
    }

    // ──────────────────────────────────────────────────────────
    //  Public API – shortcuts (like Windows .lnk / Unix symlinks)
    // ──────────────────────────────────────────────────────────

    /// <summary>Create a shortcut at <paramref name="shortcutPath"/> pointing to <paramref name="targetPath"/>.</summary>
    public async Task<FsEntry> CreateShortcutAsync(string shortcutPath, string targetPath, CancellationToken ct = default)
    {
        var targetId = await ResolvePathAsync(targetPath, ct);

        var parts = VirtualFileSystemPathUtility.SplitPath(shortcutPath);
        var parentId = await ResolveParentAsync(parts, ct);
        var name = parts[^1];

        if (await _store.LookupChildAsync(parentId, name, ct) is not null)
            throw new IOException($"An entry named '{name}' already exists.");

        var entry = new FsEntry
        {
            Id = _nextId++,
            ParentId = parentId,
            Name = name,
            Kind = EntryKind.Shortcut,
            TargetId = targetId,
            CreatedUtc = DateTime.UtcNow,
            ModifiedUtc = DateTime.UtcNow,
        };

        await _store.Pager.BeginTransactionAsync(ct);
        try
        {
            await _store.WriteEntryAsync(entry, ct);
            await _store.IndexEntryAsync(entry, ct);
            await _store.PersistNextIdAsync(_nextId, ct);
            await _store.Pager.CommitAsync(ct);
        }
        catch
        {
            await _store.Pager.RollbackAsync(ct);
            _nextId--;
            throw;
        }

        return entry;
    }

    // ──────────────────────────────────────────────────────────
    //  Public API – delete
    // ──────────────────────────────────────────────────────────

    /// <summary>Delete an entry. Directories must be empty.</summary>
    public async Task DeleteAsync(string path, CancellationToken ct = default)
    {
        var id = await ResolvePathAsync(path, ct);
        if (id == RootDirectoryId)
            throw new IOException("Cannot delete the root directory.");

        var entry = await _store.GetEntryAsync(id, ct);

        if (entry.Kind == EntryKind.Directory)
        {
            var children = await _store.ListChildrenAsync(id, ct);
            if (children.Count > 0)
                throw new IOException($"Directory '{path}' is not empty.");
        }

        await _store.Pager.BeginTransactionAsync(ct);
        try
        {
            await _store.DeleteEntryAsync(id, ct);
            if (entry.Kind == EntryKind.File)
                await _store.DeleteContentAsync(id, ct);

            await _store.RemoveEntryIndexesAsync(entry, ct);
            await _store.Pager.CommitAsync(ct);
        }
        catch
        {
            await _store.Pager.RollbackAsync(ct);
            throw;
        }
    }

    // ──────────────────────────────────────────────────────────
    //  Public API – tree view (recursive listing)
    // ──────────────────────────────────────────────────────────

    /// <summary>Render a tree view of the file system as lines of text.</summary>
    public async Task<IReadOnlyList<string>> RenderTreeAsync(string path = "/", string indent = "", CancellationToken ct = default)
    {
        var dirId = await ResolvePathAsync(path, ct);
        var root = await _store.GetEntryAsync(dirId, ct);
        return await _treeRenderer.RenderAsync(root, _store.ListChildrenAsync, indent, ct);
    }

    /// <summary>Write a tree view of the file system to the specified writer.</summary>
    public async Task WriteTreeAsync(TextWriter writer, string path = "/", string indent = "", CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(writer);

        var lines = await RenderTreeAsync(path, indent, ct);
        foreach (var line in lines)
        {
            await writer.WriteLineAsync(line);
        }
    }

    /// <summary>Print a tree view of the file system to the console.</summary>
    public Task PrintTreeAsync(string path = "/", string indent = "", CancellationToken ct = default)
    {
        return WriteTreeAsync(Console.Out, path, indent, ct);
    }

    // ──────────────────────────────────────────────────────────
    //  Internal helpers – path operations
    // ──────────────────────────────────────────────────────────

    /// <summary>Resolve a full path like "/documents/work/report.txt" to an entry ID.</summary>
    private async Task<long> ResolvePathAsync(string path, CancellationToken ct)
    {
        if (path == "/" || string.IsNullOrEmpty(path))
            return RootDirectoryId;

        var parts = VirtualFileSystemPathUtility.SplitPath(path);
        var current = RootDirectoryId;

        foreach (var part in parts)
        {
            var child = await _store.LookupChildAsync(current, part, ct);
            if (child is null)
                throw new FileNotFoundException($"Path not found: '{path}' (missing '{part}').");
            current = child.Id;
        }

        return current;
    }

    /// <summary>Resolve the parent directory for a path's segments.</summary>
    private async Task<long> ResolveParentAsync(string[] parts, CancellationToken ct)
    {
        var current = RootDirectoryId;
        for (var i = 0; i < parts.Length - 1; i++)
        {
            var child = await _store.LookupChildAsync(current, parts[i], ct);
            if (child is null)
                throw new DirectoryNotFoundException($"Directory '{parts[i]}' not found.");
            if (child.Kind != EntryKind.Directory)
                throw new IOException($"'{parts[i]}' is not a directory.");
            current = child.Id;
        }

        return current;
    }

    // ──────────────────────────────────────────────────────────
    //  Dispose
    // ──────────────────────────────────────────────────────────

    public ValueTask DisposeAsync()
    {
        return _store.DisposeAsync();
    }
}
