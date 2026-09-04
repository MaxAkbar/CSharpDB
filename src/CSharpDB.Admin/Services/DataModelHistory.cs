using CSharpDB.Admin.Models;
using System.Text.Json;

namespace CSharpDB.Admin.Services;

/// <summary>Session-local diagram/intent history, never database rollback history.</summary>
public sealed class DataModelHistory
{
    private readonly List<string> _past = [];
    private readonly Stack<string> _future = [];
    public bool CanUndo => _past.Count > 1;
    public bool CanRedo => _future.Count > 0;
    public void Reset(DataModelState state) { _past.Clear(); _future.Clear(); _past.Add(DataModelGraphBuilder.SerializeState(state)); }
    public void Record(DataModelState state)
    {
        string snapshot = DataModelGraphBuilder.SerializeState(state);
        if (_past.Count > 0 && ContentKey(_past[^1]) == ContentKey(snapshot)) { _past[^1] = snapshot; return; }
        _past.Add(snapshot); _future.Clear();
        if (_past.Count > 101) _past.RemoveAt(0);
    }
    public DataModelState? Undo()
    {
        if (!CanUndo) return null;
        _future.Push(_past[^1]); _past.RemoveAt(_past.Count - 1);
        return DataModelGraphBuilder.DeserializeState(_past[^1]);
    }
    public DataModelState? Redo()
    {
        if (!CanRedo) return null;
        _past.Add(_future.Pop()); return DataModelGraphBuilder.DeserializeState(_past[^1]);
    }
    private static string ContentKey(string json)
    {
        var state = DataModelGraphBuilder.DeserializeState(json)!;
        return JsonSerializer.Serialize(new { state.Nodes, state.Groups, state.Relationships, state.PendingOperations });
    }
}
