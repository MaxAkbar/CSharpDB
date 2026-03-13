namespace CSharpDB.Admin.Models;

public class QueryDesignerState
{
    public List<DesignerTableNode> Tables { get; set; } = [];
    public List<DesignerJoin> Joins { get; set; } = [];
    public List<DesignerGridRow> GridRows { get; set; } = [];
    public string? SavedLayoutName { get; set; }
}

public class DesignerTableNode
{
    public string TableName { get; set; } = "";
    public double X { get; set; } = 20;
    public double Y { get; set; } = 20;
    public List<DesignerColumn> Columns { get; set; } = [];
}

public class DesignerColumn
{
    public string Name { get; set; } = "";
    public string TypeLabel { get; set; } = "";   // "INT", "TEXT", "REAL", "BLOB"
    public bool IsPrimaryKey { get; set; }
    public bool IsSelected { get; set; }
}

public class DesignerJoin
{
    public string LeftTable { get; set; } = "";
    public string LeftColumn { get; set; } = "";
    public string RightTable { get; set; } = "";
    public string RightColumn { get; set; } = "";
    public DesignerJoinType JoinType { get; set; } = DesignerJoinType.Inner;
}

public enum DesignerJoinType { Inner, Left, Right, Full }

public class DesignerGridRow
{
    public string ColumnExpr { get; set; } = "";
    public string TableName { get; set; } = "";
    public string? Alias { get; set; }
    public bool Output { get; set; } = true;
    public DesignerSortDirection? SortType { get; set; }
    public int? SortOrder { get; set; }
    public string? Filter { get; set; }
}

public enum DesignerSortDirection { Ascending, Descending }
