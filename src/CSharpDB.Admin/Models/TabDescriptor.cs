namespace CSharpDB.Admin.Models;

public enum TabKind
{
    Welcome,
    Query,
    TableData,
    ViewData,
    Procedure,
    Pipeline,
    Storage
}

public sealed class TabDescriptor
{
    public string Id { get; }
    public string Title { get; set; }
    public string Icon { get; set; }
    public TabKind Kind { get; }
    public bool Closable { get; }
    public Dictionary<string, object?> State { get; } = new();

    public TabDescriptor(string id, string title, string icon, TabKind kind, bool closable = true)
    {
        Id = id;
        Title = title;
        Icon = icon;
        Kind = kind;
        Closable = closable;
    }

    /// <summary>Get the object name for data/view tabs (e.g. table name, view name).</summary>
    public string? ObjectName
    {
        get => State.TryGetValue("ObjectName", out var v) ? v as string : null;
        set => State["ObjectName"] = value;
    }

    /// <summary>Get/set the SQL text for query tabs.</summary>
    public string? SqlText
    {
        get => State.TryGetValue("SqlText", out var v) ? v as string : null;
        set => State["SqlText"] = value;
    }

    /// <summary>Get/set the serialized QueryDesignerState JSON for designer mode.</summary>
    public string? DesignerStateJson
    {
        get => State.TryGetValue("DesignerStateJson", out var v) ? v as string : null;
        set => State["DesignerStateJson"] = value;
    }

    public string? PipelinePackageJson
    {
        get => State.TryGetValue("PipelinePackageJson", out var v) ? v as string : null;
        set => State["PipelinePackageJson"] = value;
    }
}
