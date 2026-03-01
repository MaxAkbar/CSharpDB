namespace CSharpDB.Admin.Services;

public enum ToastLevel
{
    Success,
    Error,
    Warning,
    Info
}

public sealed record ToastMessage(string Id, string Message, ToastLevel Level, DateTime Created);

public sealed class ToastService
{
    private readonly List<ToastMessage> _toasts = new();

    public IReadOnlyList<ToastMessage> Toasts => _toasts;

    public event Action? OnChange;

    public void Show(string message, ToastLevel level = ToastLevel.Info)
    {
        var toast = new ToastMessage(Guid.NewGuid().ToString("N"), message, level, DateTime.UtcNow);
        _toasts.Add(toast);
        OnChange?.Invoke();
    }

    public void Success(string message) => Show(message, ToastLevel.Success);
    public void Error(string message) => Show(message, ToastLevel.Error);
    public void Warning(string message) => Show(message, ToastLevel.Warning);
    public void Info(string message) => Show(message, ToastLevel.Info);

    public void Dismiss(string id)
    {
        _toasts.RemoveAll(t => t.Id == id);
        OnChange?.Invoke();
    }
}
