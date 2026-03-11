namespace CSharpDB.Admin.Models;

public enum RowState
{
    Unmodified,
    Modified,
    New,
    Deleted
}

public sealed class DataGridRow
{
    public object?[] OriginalValues { get; }
    public object?[] CurrentValues { get; set; }
    public RowState State { get; set; }

    public DataGridRow(object?[] values, RowState state = RowState.Unmodified)
        : this(values, values, state)
    {
    }

    public DataGridRow(object?[] originalValues, object?[] currentValues, RowState state = RowState.Unmodified)
    {
        OriginalValues = (object?[])originalValues.Clone();
        CurrentValues = (object?[])currentValues.Clone();
        State = state;
    }

    /// <summary>Check if a specific column value has been modified.</summary>
    public bool IsColumnModified(int colIndex)
    {
        if (State == RowState.New) return false;
        var orig = OriginalValues[colIndex];
        var curr = CurrentValues[colIndex];
        if (orig is null && curr is null) return false;
        if (orig is null || curr is null) return true;
        return !orig.Equals(curr);
    }
}

public sealed class EditingCell
{
    public int RowIndex { get; set; }
    public int ColIndex { get; set; }
    public string Value { get; set; } = string.Empty;
}
