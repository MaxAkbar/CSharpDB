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
        var completion = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        _tcs = completion;
        Current = new ModalOptions(title, message, confirmText, IsDanger: isDanger);
        OnChange?.Invoke();
        return completion.Task;
    }

    public Task<string?> PromptAsync(string title, string message, string confirmText = "OK", string placeholder = "", string defaultValue = "")
    {
        var completion = new TaskCompletionSource<string?>(TaskCreationOptions.RunContinuationsAsynchronously);
        _promptTcs = completion;
        Current = new ModalOptions(title, message, confirmText, ShowInput: true, InputPlaceholder: placeholder, InputValue: defaultValue);
        OnChange?.Invoke();
        return completion.Task;
    }

    public void Respond(bool accepted)
    {
        TaskCompletionSource<bool>? confirmation = _tcs;
        TaskCompletionSource<string?>? prompt = _promptTcs;
        _tcs = null;
        _promptTcs = null;
        Current = null;
        OnChange?.Invoke();
        confirmation?.TrySetResult(accepted);
        prompt?.TrySetResult(null);
    }

    public void RespondWithValue(string? value)
    {
        TaskCompletionSource<bool>? confirmation = _tcs;
        TaskCompletionSource<string?>? prompt = _promptTcs;
        _promptTcs = null;
        _tcs = null;
        Current = null;
        OnChange?.Invoke();
        prompt?.TrySetResult(value);
        confirmation?.TrySetResult(value is not null);
    }

    public void Cancel() => Respond(false);
}
