namespace CSharpDB.VirtualFS;

public interface IVirtualFileSystemTreeRenderer
{
    Task<IReadOnlyList<string>> RenderAsync(
        FsEntry root,
        Func<long, CancellationToken, Task<List<FsEntry>>> listChildrenAsync,
        string indent,
        CancellationToken ct);
}

public sealed class VirtualFileSystemTreeRenderer : IVirtualFileSystemTreeRenderer
{
    public async Task<IReadOnlyList<string>> RenderAsync(
        FsEntry root,
        Func<long, CancellationToken, Task<List<FsEntry>>> listChildrenAsync,
        string indent,
        CancellationToken ct)
    {
        var lines = new List<string> { $"{indent}{FormatEntry(root)}" };
        if (root.Kind != EntryKind.Directory)
            return lines;

        await RenderChildrenAsync(root.Id, indent, lines, listChildrenAsync, ct);
        return lines;
    }

    private static async Task RenderChildrenAsync(
        long parentId,
        string indent,
        List<string> lines,
        Func<long, CancellationToken, Task<List<FsEntry>>> listChildrenAsync,
        CancellationToken ct)
    {
        var children = await listChildrenAsync(parentId, ct);
        for (var i = 0; i < children.Count; i++)
        {
            var isLast = i == children.Count - 1;
            var prefix = indent + (isLast ? "└── " : "├── ");
            var childIndent = indent + (isLast ? "    " : "│   ");
            var child = children[i];

            lines.Add($"{prefix}{FormatEntry(child)}");
            if (child.Kind == EntryKind.Directory)
            {
                await RenderChildrenAsync(child.Id, childIndent, lines, listChildrenAsync, ct);
            }
        }
    }

    private static string FormatEntry(FsEntry entry)
    {
        return entry.Kind switch
        {
            EntryKind.Directory => $"[DIR]  {entry.Name}/",
            EntryKind.File => $"[FILE] {entry.Name} ({entry.Size} bytes)",
            EntryKind.Shortcut => $"[LNK]  {entry.Name} -> target #{entry.TargetId}",
            _ => entry.Name,
        };
    }
}
