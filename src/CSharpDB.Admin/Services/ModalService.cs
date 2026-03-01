namespace CSharpDB.Admin.Services;

public sealed record ModalOptions(
    string Title,
    string Message,
    string ConfirmText = "Confirm",
    string CancelText = "Cancel",
    bool IsDanger = false);

public sealed class ModalService
{
    private TaskCompletionSource<bool>? _tcs;

    public ModalOptions? Current { get; private set; }
    public bool IsVisible => Current is not null;

    public event Action? OnChange;

    public Task<bool> ConfirmAsync(string title, string message, string confirmText = "Confirm", bool isDanger = false)
    {
        _tcs = new TaskCompletionSource<bool>();
        Current = new ModalOptions(title, message, confirmText, IsDanger: isDanger);
        OnChange?.Invoke();
        return _tcs.Task;
    }

    public void Respond(bool accepted)
    {
        _tcs?.TrySetResult(accepted);
        _tcs = null;
        Current = null;
        OnChange?.Invoke();
    }
}
