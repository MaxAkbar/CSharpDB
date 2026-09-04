using CSharpDB.Admin.Models;

namespace CSharpDB.Admin.Services;

public readonly record struct DataModelGroupBounds(double X, double Y, double Width, double Height)
{
    public double Right => X + Width;
    public double Bottom => Y + Height;
    public bool Intersects(DataModelGroupBounds other) =>
        X < other.Right && Right > other.X && Y < other.Bottom && Bottom > other.Y;
}

/// <summary>Diagram-only membership and geometry. Never creates schema operations.</summary>
public static class DataModelGroups
{
    public const double Padding = 16;
    public const double TitleHeight = 28;
    public const int CanvasInset = 64;

    public static List<DataModelNode> Members(DataModelState state, string id) =>
        state.Nodes.Where(node => node.GroupId == id).ToList();

    public static DataModelGroupBounds? Bounds(DataModelState state, string id)
    {
        var members = Members(state, id);
        if (members.Count == 0) return null;
        double x = members.Min(node => node.X) - Padding;
        double y = members.Min(node => node.Y) - Padding - TitleHeight;
        return new(x, y,
            members.Max(node => node.X + DataModelCanvasMetrics.NodeWidth) + Padding - x,
            members.Max(node => node.Y + DataModelCanvasMetrics.NodeHeight(state, node)) + Padding - y);
    }

    public static void Normalize(DataModelState state)
    {
        state.Groups ??= [];
        var ids = new HashSet<string>(StringComparer.Ordinal);
        state.Groups.RemoveAll(group => group is null || string.IsNullOrWhiteSpace(group.Id) || !ids.Add(group.Id));
        foreach (var group in state.Groups)
        {
            group.Name = string.IsNullOrWhiteSpace(group.Name) ? "Group" : group.Name.Trim();
            if (!Enum.IsDefined(group.Color)) group.Color = DataModelGroupColor.Blue;
        }
        foreach (var node in state.Nodes)
            if (node.GroupId is not null && !ids.Contains(node.GroupId)) node.GroupId = null;
        state.Groups.RemoveAll(group => !state.Nodes.Any(node => node.GroupId == group.Id));
    }

    public static void Preserve(DataModelState previous, DataModelState target)
    {
        target.Groups = previous.Groups.Select(group => new DataModelGroup
            { Id = group.Id, Name = group.Name, Color = group.Color }).ToList();
        var membership = previous.Nodes.ToDictionary(node => node.Name, node => node.GroupId, StringComparer.OrdinalIgnoreCase);
        foreach (var node in target.Nodes) node.GroupId = membership.GetValueOrDefault(node.Name);
        Normalize(target);
    }

    public static DataModelGroup? Create(DataModelState state, IEnumerable<string> names)
    {
        var selected = names.ToHashSet(StringComparer.OrdinalIgnoreCase);
        var members = state.Nodes.Where(node => selected.Contains(node.Name)).ToList();
        if (members.Count < 2) return null;
        int index = 1;
        while (state.Groups.Any(group => group.Name.Equals($"Group {index}", StringComparison.OrdinalIgnoreCase))) index++;
        var created = new DataModelGroup { Name = $"Group {index}" };
        state.Groups.Add(created);
        foreach (var member in members) member.GroupId = created.Id;
        Normalize(state);
        return created;
    }

    public static void Assign(DataModelState state, IEnumerable<string> names, string? groupId)
    {
        if (groupId is not null && !state.Groups.Any(group => group.Id == groupId)) return;
        var selected = names.ToHashSet(StringComparer.OrdinalIgnoreCase);
        foreach (var node in state.Nodes.Where(node => selected.Contains(node.Name))) node.GroupId = groupId;
        Normalize(state);
    }

    public static void Ungroup(DataModelState state, string id) =>
        Assign(state, Members(state, id).Select(node => node.Name), null);

    public static IEnumerable<DataModelRelationship> InternalRelationships(DataModelState state, string id)
    {
        var names = Members(state, id).Select(node => node.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);
        return state.Relationships.Where(edge => names.Contains(edge.LeftTable) && names.Contains(edge.RightTable));
    }

    public static bool Move(DataModelState state, string id, double dx, double dy)
    {
        if (!double.IsFinite(dx) || !double.IsFinite(dy) || !state.Groups.Any(group => group.Id == id)) return false;
        var members = Members(state, id);
        if (members.Count == 0) return false;
        var guides = InternalRelationships(state, id).SelectMany(edge => edge.ConnectorLayout?.Waypoints ?? []).ToList();
        dx = Math.Max(dx, Math.Max(-members.Min(node => node.X), guides.Select(point => 4 - point.X).DefaultIfEmpty(double.MinValue).Max()));
        dy = Math.Max(dy, Math.Max(-members.Min(node => node.Y), guides.Select(point => 4 - point.Y).DefaultIfEmpty(double.MinValue).Max()));
        if (dx == 0 && dy == 0) return false;
        foreach (var node in members) { node.X += dx; node.Y += dy; }
        foreach (var point in guides) { point.X += dx; point.Y += dy; }
        return true;
    }

    public static int CountOverlaps(DataModelState state)
    {
        var groups = state.Groups.Select(group => (Group: group, Bounds: Bounds(state, group.Id)))
            .Where(item => item.Bounds.HasValue).ToArray();
        int count = 0;
        for (int i = 0; i < groups.Length; i++)
        {
            var bounds = groups[i].Bounds!.Value;
            for (int j = i + 1; j < groups.Length; j++)
                if (bounds.Intersects(groups[j].Bounds!.Value)) count++;
            foreach (var node in state.Nodes.Where(node => node.GroupId is null))
                if (bounds.Intersects(new(node.X, node.Y, DataModelCanvasMetrics.NodeWidth, DataModelCanvasMetrics.NodeHeight(state, node)))) count++;
        }
        return count;
    }
}
