using Microsoft.JSInterop;

namespace CSharpDB.Admin.Services;

public sealed class ThemeService
{
    private IJSRuntime? _js;

    public string Theme { get; private set; } = "dark";
    public bool IsDark => Theme == "dark";

    public event Action? ThemeChanged;

    public async Task InitializeAsync(IJSRuntime js)
    {
        _js = js;
        try
        {
            var saved = await js.InvokeAsync<string>("themeInterop.get");
            if (!string.IsNullOrEmpty(saved))
                Theme = saved;
        }
        catch
        {
            // localStorage not available or first load
        }

        await ApplyAsync();
    }

    public async Task ToggleAsync()
    {
        Theme = IsDark ? "light" : "dark";
        await ApplyAsync();
        ThemeChanged?.Invoke();
    }

    private async Task ApplyAsync()
    {
        if (_js is null) return;
        try
        {
            await _js.InvokeVoidAsync("themeInterop.set", Theme);
        }
        catch
        {
            // Circuit may have disconnected
        }
    }
}
