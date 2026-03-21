using System.Buffers.Binary;
using System.Text.Json;
using CSharpDB.Storage.BTrees;
using CSharpDB.Storage.Paging;

namespace CSharpDB.VirtualFS;

internal interface IVirtualFileSystemStore : IAsyncDisposable
{
    Pager Pager { get; }

    Task<long> InitializeNewAsync(CancellationToken ct);
    Task<long> LoadAsync(CancellationToken ct);
    Task PersistNextIdAsync(long nextId, CancellationToken ct);
    Task WriteEntryAsync(FsEntry entry, CancellationToken ct);
    Task DeleteEntryAsync(long id, CancellationToken ct);
    Task<FsEntry> GetEntryAsync(long id, CancellationToken ct);
    Task WriteContentAsync(long id, byte[] data, CancellationToken ct);
    Task<byte[]?> ReadContentAsync(long id, CancellationToken ct);
    Task DeleteContentAsync(long id, CancellationToken ct);
    Task IndexEntryAsync(FsEntry entry, CancellationToken ct);
    Task RemoveEntryIndexesAsync(FsEntry entry, CancellationToken ct);
    Task<List<FsEntry>> ListChildrenAsync(long parentId, CancellationToken ct);
    Task<FsEntry?> LookupChildAsync(long parentId, string name, CancellationToken ct);
}

internal sealed class VirtualFileSystemStore(Pager pager) : IVirtualFileSystemStore
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private BTree _entries = null!;
    private BTree _content = null!;
    private BTree _pathIndex = null!;
    private BTree _children = null!;

    public Pager Pager { get; } = pager;

    public async Task<long> InitializeNewAsync(CancellationToken ct)
    {
        await Pager.BeginTransactionAsync(ct);
        try
        {
            var entriesRoot = await BTree.CreateNewAsync(Pager, ct);
            var contentRoot = await BTree.CreateNewAsync(Pager, ct);
            var pathIndexRoot = await BTree.CreateNewAsync(Pager, ct);
            var childrenRoot = await BTree.CreateNewAsync(Pager, ct);

            _entries = new BTree(Pager, entriesRoot);
            _content = new BTree(Pager, contentRoot);
            _pathIndex = new BTree(Pager, pathIndexRoot);
            _children = new BTree(Pager, childrenRoot);

            var nextId = VirtualFileSystem.RootDirectoryId + 1;
            await PersistNextIdAsync(nextId, ct);

            var root = new FsEntry
            {
                Id = VirtualFileSystem.RootDirectoryId,
                ParentId = 0,
                Name = "/",
                Kind = EntryKind.Directory,
                CreatedUtc = DateTime.UtcNow,
                ModifiedUtc = DateTime.UtcNow,
            };

            await WriteEntryAsync(root, ct);
            await Pager.CommitAsync(ct);
            return nextId;
        }
        catch
        {
            await Pager.RollbackAsync(ct);
            throw;
        }
    }

    public async Task<long> LoadAsync(CancellationToken ct)
    {
        _entries = new BTree(Pager, 1);
        var superblock = await _entries.FindAsync(0, ct);
        if (superblock is null)
        {
            throw new InvalidOperationException("Superblock not found – file is corrupted or not a VFS.");
        }

        var entriesRoot = BinaryPrimitives.ReadUInt32LittleEndian(superblock.AsSpan(0));
        var contentRoot = BinaryPrimitives.ReadUInt32LittleEndian(superblock.AsSpan(4));
        var pathIndexRoot = BinaryPrimitives.ReadUInt32LittleEndian(superblock.AsSpan(8));
        var childrenRoot = BinaryPrimitives.ReadUInt32LittleEndian(superblock.AsSpan(12));
        var nextId = BinaryPrimitives.ReadInt64LittleEndian(superblock.AsSpan(16));

        _entries = new BTree(Pager, entriesRoot);
        _content = new BTree(Pager, contentRoot);
        _pathIndex = new BTree(Pager, pathIndexRoot);
        _children = new BTree(Pager, childrenRoot);

        return nextId;
    }

    public async Task PersistNextIdAsync(long nextId, CancellationToken ct)
    {
        var buffer = new byte[24];
        BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(0), _entries.RootPageId);
        BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(4), _content.RootPageId);
        BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(8), _pathIndex.RootPageId);
        BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(12), _children.RootPageId);
        BinaryPrimitives.WriteInt64LittleEndian(buffer.AsSpan(16), nextId);

        await _entries.DeleteAsync(0, ct);
        await _entries.InsertAsync(0, buffer, ct);
    }

    public async Task WriteEntryAsync(FsEntry entry, CancellationToken ct)
    {
        var json = JsonSerializer.SerializeToUtf8Bytes(entry, JsonOptions);
        await _entries.DeleteAsync(entry.Id, ct);
        await _entries.InsertAsync(entry.Id, json, ct);
    }

    public async Task DeleteEntryAsync(long id, CancellationToken ct)
    {
        await _entries.DeleteAsync(id, ct);
    }

    public async Task<FsEntry> GetEntryAsync(long id, CancellationToken ct)
    {
        var data = await _entries.FindAsync(id, ct);
        if (data is null)
        {
            throw new FileNotFoundException($"Entry #{id} not found.");
        }

        return JsonSerializer.Deserialize<FsEntry>(data, JsonOptions)!;
    }

    public async Task WriteContentAsync(long id, byte[] data, CancellationToken ct)
    {
        await _content.DeleteAsync(id, ct);
        await _content.InsertAsync(id, data, ct);
    }

    public async Task<byte[]?> ReadContentAsync(long id, CancellationToken ct)
    {
        return await _content.FindAsync(id, ct);
    }

    public async Task DeleteContentAsync(long id, CancellationToken ct)
    {
        await _content.DeleteAsync(id, ct);
    }

    public async Task IndexEntryAsync(FsEntry entry, CancellationToken ct)
    {
        var pathHash = VirtualFileSystemPathUtility.ComputePathHash(entry.ParentId, entry.Name);
        var idBuffer = new byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(idBuffer, entry.Id);
        await _pathIndex.InsertAsync(pathHash, idBuffer, ct);

        var childKey = VirtualFileSystemPathUtility.MakeChildKey(entry.ParentId, entry.Id);
        await _children.InsertAsync(childKey, idBuffer, ct);
    }

    public async Task RemoveEntryIndexesAsync(FsEntry entry, CancellationToken ct)
    {
        var pathHash = VirtualFileSystemPathUtility.ComputePathHash(entry.ParentId, entry.Name);
        await _pathIndex.DeleteAsync(pathHash, ct);

        var childKey = VirtualFileSystemPathUtility.MakeChildKey(entry.ParentId, entry.Id);
        await _children.DeleteAsync(childKey, ct);
    }

    public async Task<List<FsEntry>> ListChildrenAsync(long parentId, CancellationToken ct)
    {
        var result = new List<FsEntry>();
        var lowerBound = VirtualFileSystemPathUtility.MakeChildKey(parentId, 0);
        var upperBound = VirtualFileSystemPathUtility.MakeChildKey(parentId, 999_999_999);

        var cursor = _children.CreateCursor();
        if (!await cursor.SeekAsync(lowerBound, ct))
        {
            return result;
        }

        do
        {
            if (cursor.CurrentKey > upperBound)
            {
                break;
            }

            var childId = BinaryPrimitives.ReadInt64LittleEndian(cursor.CurrentValue.Span);
            result.Add(await GetEntryAsync(childId, ct));
        }
        while (await cursor.MoveNextAsync(ct));

        return result;
    }

    public async Task<FsEntry?> LookupChildAsync(long parentId, string name, CancellationToken ct)
    {
        var pathHash = VirtualFileSystemPathUtility.ComputePathHash(parentId, name);
        var idBuffer = await _pathIndex.FindAsync(pathHash, ct);
        if (idBuffer is null)
        {
            return null;
        }

        var id = BinaryPrimitives.ReadInt64LittleEndian(idBuffer);
        return await GetEntryAsync(id, ct);
    }

    public ValueTask DisposeAsync()
    {
        return Pager.DisposeAsync();
    }
}
