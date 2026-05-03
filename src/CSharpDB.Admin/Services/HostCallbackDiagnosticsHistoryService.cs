using CSharpDB.Primitives;

namespace CSharpDB.Admin.Services;

public sealed class HostCallbackDiagnosticsHistoryService : IObserver<KeyValuePair<string, object?>>, IDisposable
{
    private const int MaxEntries = 200;
    private readonly object _gate = new();
    private readonly IDisposable _subscription;
    private readonly List<DbCallbackInvocationDiagnostic> _entries = [];

    public HostCallbackDiagnosticsHistoryService()
    {
        _subscription = DbCallbackDiagnostics.Listener.Subscribe(this);
    }

    public event Action? Changed;

    public IReadOnlyList<DbCallbackInvocationDiagnostic> Snapshot()
    {
        lock (_gate)
            return _entries.ToArray();
    }

    public void Clear()
    {
        lock (_gate)
            _entries.Clear();

        Changed?.Invoke();
    }

    public void OnCompleted()
    {
    }

    public void OnError(Exception error)
    {
    }

    public void OnNext(KeyValuePair<string, object?> value)
    {
        if (value.Key != DbCallbackDiagnostics.InvocationEventName ||
            value.Value is not DbCallbackInvocationDiagnostic diagnostic)
        {
            return;
        }

        lock (_gate)
        {
            _entries.Insert(0, diagnostic);
            if (_entries.Count > MaxEntries)
                _entries.RemoveRange(MaxEntries, _entries.Count - MaxEntries);
        }

        Changed?.Invoke();
    }

    public void Dispose()
    {
        _subscription.Dispose();
    }
}
