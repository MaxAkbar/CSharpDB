using System.Collections;
using System.Data.Common;

namespace CSharpDB.Data;

public sealed class CSharpDbParameterCollection : DbParameterCollection
{
    private readonly List<CSharpDbParameter> _parameters = [];

    public override int Count => _parameters.Count;
    public override object SyncRoot => ((ICollection)_parameters).SyncRoot;

    public new CSharpDbParameter this[int index]
    {
        get => _parameters[index];
        set => _parameters[index] = value;
    }

    public CSharpDbParameter AddWithValue(string name, object? value)
    {
        var p = new CSharpDbParameter(name, value);
        _parameters.Add(p);
        return p;
    }

    public override int Add(object value)
    {
        _parameters.Add((CSharpDbParameter)value);
        return _parameters.Count - 1;
    }

    public override void AddRange(Array values)
    {
        foreach (CSharpDbParameter p in values)
            _parameters.Add(p);
    }

    public override void Clear() => _parameters.Clear();

    public override bool Contains(object value) => _parameters.Contains((CSharpDbParameter)value);

    public override bool Contains(string value) => IndexOf(value) >= 0;

    public override void CopyTo(Array array, int index)
        => ((ICollection)_parameters).CopyTo(array, index);

    public override IEnumerator GetEnumerator() => _parameters.GetEnumerator();

    public override int IndexOf(object value) => _parameters.IndexOf((CSharpDbParameter)value);

    public override int IndexOf(string parameterName)
    {
        ReadOnlySpan<char> normalized = NormalizeName(parameterName.AsSpan());
        for (int i = 0; i < _parameters.Count; i++)
        {
            if (NormalizeName(_parameters[i].ParameterName.AsSpan())
                .Equals(normalized, StringComparison.OrdinalIgnoreCase))
                return i;
        }
        return -1;
    }

    internal bool TryGetValue(ReadOnlySpan<char> parameterName, out object? value)
    {
        ReadOnlySpan<char> normalized = NormalizeName(parameterName);
        for (int i = 0; i < _parameters.Count; i++)
        {
            if (NormalizeName(_parameters[i].ParameterName.AsSpan())
                .Equals(normalized, StringComparison.OrdinalIgnoreCase))
            {
                value = _parameters[i].Value;
                return true;
            }
        }

        value = null;
        return false;
    }

    public override void Insert(int index, object value)
        => _parameters.Insert(index, (CSharpDbParameter)value);

    public override void Remove(object value)
        => _parameters.Remove((CSharpDbParameter)value);

    public override void RemoveAt(int index) => _parameters.RemoveAt(index);

    public override void RemoveAt(string parameterName)
    {
        int idx = IndexOf(parameterName);
        if (idx >= 0) _parameters.RemoveAt(idx);
    }

    protected override DbParameter GetParameter(int index) => _parameters[index];

    protected override DbParameter GetParameter(string parameterName)
    {
        int idx = IndexOf(parameterName);
        if (idx < 0)
            throw new KeyNotFoundException($"Parameter '{parameterName}' not found.");
        return _parameters[idx];
    }

    protected override void SetParameter(int index, DbParameter value)
        => _parameters[index] = (CSharpDbParameter)value;

    protected override void SetParameter(string parameterName, DbParameter value)
    {
        int idx = IndexOf(parameterName);
        if (idx >= 0)
            _parameters[idx] = (CSharpDbParameter)value;
        else
            _parameters.Add((CSharpDbParameter)value);
    }

    private static ReadOnlySpan<char> NormalizeName(ReadOnlySpan<char> name)
        => name.Length > 0 && name[0] == '@' ? name[1..] : name;
}
