namespace CSharpDB.Admin.Forms.Models;

public sealed record FormRecordWindow(
    IReadOnlyList<Dictionary<string, object?>> Records,
    int SelectedIndex,
    bool HasPreviousRecords,
    bool HasNextRecords);
