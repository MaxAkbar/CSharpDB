namespace CSharpDB.Admin.Services;

public sealed record ModalOptions(
    string Title,
    string Message,
    string ConfirmText = "Confirm",
    string CancelText = "Cancel",
    bool IsDanger = false,
    bool ShowInput = false,
    string InputPlaceholder = "",
    string InputValue = "");

public sealed class ModalService
{
    private TaskCompletionSource<bool>? _tcs;
    private TaskCompletionSource<string?>? _promptTcs;

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

    public Task<string?> PromptAsync(string title, string message, string confirmText = "OK", string placeholder = "", string defaultValue = "")
    {
        _promptTcs = new TaskCompletionSource<string?>();
        Current = new ModalOptions(title, message, confirmText, ShowInput: true, InputPlaceholder: placeholder, InputValue: defaultValue);
        OnChange?.Invoke();
        return _promptTcs.Task;
    }

    public void Respond(bool accepted)
    {
        _tcs?.TrySetResult(accepted);
        _tcs = null;
        _promptTcs?.TrySetResult(null);
        _promptTcs = null;
        Current = null;
        OnChange?.Invoke();
    }

    public void RespondWithValue(string? value)
    {
        _promptTcs?.TrySetResult(value);
        _promptTcs = null;
        _tcs?.TrySetResult(value is not null);
        _tcs = null;
        Current = null;
        OnChange?.Invoke();
    }
}
