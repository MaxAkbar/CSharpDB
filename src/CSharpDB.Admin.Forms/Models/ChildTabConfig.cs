namespace CSharpDB.Admin.Forms.Models;

public sealed record ChildTabConfig(
    string Id,
    string Label,
    string ChildTable,
    string ForeignKeyField,
    string ParentKeyField,
    IReadOnlyList<string> VisibleColumns,
    bool AllowAdd,
    bool AllowEdit,
    bool AllowDelete,
    IReadOnlyList<ChildTabConfig> ChildTabs);
