using System;
using System.IO;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;
using System.Threading;
using System.Windows;
using System.Windows.Media.Imaging;
using Microsoft.Win32;
using Microsoft.Web.WebView2.Core;

namespace CSharpDB.Admin.Desktop;

public partial class MainWindow : Window
{
    private const string DialogRequestType = "desktop-shell-dialog-request";
    private const string DialogResultType = "desktop-shell-dialog-result";
    private static readonly JsonSerializerOptions WebMessageJsonOptions = new(JsonSerializerDefaults.Web);

    private readonly CancellationTokenSource _shutdown = new();
    private readonly AdminHostProcess _adminHost = new();
    private AdminHostSession? _adminHostSession;
    private bool _nativeDialogOpen;
    private bool _isClosing;

    public MainWindow()
    {
        InitializeComponent();
        LoadBrandAssets();
    }

    private async void OnLoaded(object sender, RoutedEventArgs e)
    {
        try
        {
            DesktopPaths.EnsureCreated();

            SetStatus("Starting local Admin host...");
            _adminHostSession = await _adminHost.StartAsync(SetStatus, _shutdown.Token);

            SetStatus("Preparing WebView2...");
            CoreWebView2Environment environment = await CoreWebView2Environment.CreateAsync(
                browserExecutableFolder: null,
                userDataFolder: DesktopPaths.WebViewDataDirectory);

            await Browser.EnsureCoreWebView2Async(environment);
            Browser.CoreWebView2.WebMessageReceived += OnWebMessageReceived;
            Browser.CoreWebView2.Settings.AreDevToolsEnabled = false;
            Browser.CoreWebView2.Settings.AreDefaultContextMenusEnabled = true;
            Browser.Source = _adminHostSession.BaseUri;
            Browser.Visibility = Visibility.Visible;
            StartupPanel.Visibility = Visibility.Collapsed;
            OpenDatabaseMenuItem.IsEnabled = true;
        }
        catch (OperationCanceledException) when (_isClosing)
        {
            // Normal shutdown can cancel host or WebView initialization.
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            SetStatus("CSharpDB Studio could not start.\n\n" + ex.Message);
        }
    }

    protected override async void OnClosed(EventArgs e)
    {
        _isClosing = true;
        _shutdown.Cancel();

        if (Browser.CoreWebView2 is not null)
            Browser.CoreWebView2.WebMessageReceived -= OnWebMessageReceived;

        await _adminHost.DisposeAsync();
        _shutdown.Dispose();

        base.OnClosed(e);
    }

    private void SetStatus(string message)
        => Dispatcher.Invoke(() => StatusText.Text = message);

    private void LoadBrandAssets()
    {
        string assetsDirectory = Path.Combine(AppContext.BaseDirectory, "Assets");
        string iconPath = Path.Combine(assetsDirectory, "CSharpDB.ico");
        if (File.Exists(iconPath))
            Icon = BitmapFrame.Create(new Uri(iconPath, UriKind.Absolute));

        string brandIconPath = Path.Combine(assetsDirectory, "icon2.png");
        if (File.Exists(brandIconPath))
            BrandIcon.Source = new BitmapImage(new Uri(brandIconPath, UriKind.Absolute));
    }

    private async void OnOpenDatabase(object sender, RoutedEventArgs e)
        => await OpenDatabaseAsync(showErrors: true);

    private async void OnWebMessageReceived(object? sender, CoreWebView2WebMessageReceivedEventArgs e)
    {
        DesktopDialogRequest? request = null;
        try
        {
            if (!IsCurrentAdminSource(e.Source))
                return;

            request = ParseDialogRequest(e.WebMessageAsJson);
            if (request is null)
                return;

            DialogOutcome outcome = request.Dialog switch
            {
                "open-database" => await OpenDatabaseAsync(showErrors: true),
                "open-file" => ShowPathDialog(() => ShowOpenFileDialog(request.Options)),
                "save-file" => ShowPathDialog(() => ShowSaveFileDialog(request.Options)),
                "open-folder" => ShowPathDialog(() => ShowOpenFolderDialog(request.Options)),
                _ => DialogOutcome.Failed($"Unknown native dialog '{request.Dialog}'."),
            };

            PostDialogResponse(request.RequestId, outcome);
        }
        catch (OperationCanceledException) when (_isClosing)
        {
            if (request is not null)
                PostDialogResponse(request.RequestId, DialogOutcome.CanceledByUser());
        }
        catch (Exception ex)
        {
            // WebView messages are an external input boundary. Never allow a malformed
            // message or a dialog failure to escape this async-void event handler.
            if (request is not null)
                PostDialogResponse(request.RequestId, DialogOutcome.Failed(ex.Message));
        }
    }

    private async Task<DialogOutcome> OpenDatabaseAsync(bool showErrors)
    {
        AdminHostSession? session = _adminHostSession;
        if (session is null)
            return DialogOutcome.Failed("The local Admin host is not ready.");
        if (_isClosing)
            return DialogOutcome.CanceledByUser();
        if (_nativeDialogOpen)
            return DialogOutcome.Failed("Another native dialog is already open.");

        _nativeDialogOpen = true;
        OpenDatabaseMenuItem.IsEnabled = false;
        try
        {
            var dialog = new OpenFileDialog
            {
                Title = "Open CSharpDB Database",
                Filter = "Database files (*.db;*.cdb;*.csdb)|*.db;*.cdb;*.csdb|All files (*.*)|*.*",
                CheckFileExists = false,
                AddExtension = true,
                DefaultExt = ".db",
            };

            if (dialog.ShowDialog(this) != true)
                return DialogOutcome.CanceledByUser();

            using var client = new HttpClient { BaseAddress = session.BaseUri };
            using var request = new HttpRequestMessage(HttpMethod.Post, "_desktop/open-database")
            {
                Content = JsonContent.Create(new { databasePath = dialog.FileName }),
            };
            request.Headers.TryAddWithoutValidation(
                "X-CSharpDB-Desktop-Shell-Token",
                session.DesktopShellToken);

            using HttpResponseMessage response = await client.SendAsync(request, _shutdown.Token);
            response.EnsureSuccessStatusCode();

            if (!_isClosing)
                Browser.CoreWebView2?.Reload();

            // The selected database path deliberately stays in the native host and is
            // sent only to the token-protected loopback endpoint.
            return DialogOutcome.Completed();
        }
        catch (OperationCanceledException) when (_isClosing)
        {
            return DialogOutcome.CanceledByUser();
        }
        catch (Exception ex)
        {
            if (showErrors && !_isClosing)
                MessageBox.Show(this, ex.Message, "Open Database Failed", MessageBoxButton.OK, MessageBoxImage.Error);

            return DialogOutcome.Failed(ex.Message);
        }
        finally
        {
            _nativeDialogOpen = false;
            OpenDatabaseMenuItem.IsEnabled = _adminHostSession is not null && !_isClosing;
        }
    }

    private bool IsCurrentAdminSource(string source)
    {
        if (_adminHostSession is null || !Uri.TryCreate(source, UriKind.Absolute, out Uri? sourceUri))
            return false;

        Uri adminUri = _adminHostSession.BaseUri;
        return sourceUri.Scheme.Equals(adminUri.Scheme, StringComparison.OrdinalIgnoreCase) &&
               sourceUri.IdnHost.Equals(adminUri.IdnHost, StringComparison.OrdinalIgnoreCase) &&
               sourceUri.Port == adminUri.Port;
    }

    private static DesktopDialogRequest? ParseDialogRequest(string json)
    {
        if (string.IsNullOrWhiteSpace(json) || json.Length > 16_384)
            return null;

        DesktopDialogRequest? request;
        try
        {
            request = JsonSerializer.Deserialize<DesktopDialogRequest>(json, WebMessageJsonOptions);
        }
        catch (JsonException)
        {
            return null;
        }
        catch (NotSupportedException)
        {
            return null;
        }

        if (request is null ||
            request.Type != DialogRequestType ||
            string.IsNullOrWhiteSpace(request.RequestId) ||
            request.RequestId.Length > 128 ||
            string.IsNullOrWhiteSpace(request.Dialog))
        {
            return null;
        }

        return request;
    }

    private DialogOutcome ShowPathDialog(Func<DialogOutcome> showDialog)
    {
        if (_isClosing)
            return DialogOutcome.CanceledByUser();
        if (_nativeDialogOpen)
            return DialogOutcome.Failed("Another native dialog is already open.");

        _nativeDialogOpen = true;
        OpenDatabaseMenuItem.IsEnabled = false;
        try
        {
            return showDialog();
        }
        catch (Exception ex)
        {
            return DialogOutcome.Failed(ex.Message);
        }
        finally
        {
            _nativeDialogOpen = false;
            OpenDatabaseMenuItem.IsEnabled = _adminHostSession is not null && !_isClosing;
        }
    }

    private DialogOutcome ShowOpenFileDialog(DesktopDialogOptions? options)
    {
        var dialog = new OpenFileDialog
        {
            Title = ValueOrDefault(options?.Title, "Open File"),
            Filter = ValueOrDefault(options?.Filter, "All files (*.*)|*.*"),
            CheckFileExists = options?.CheckFileExists ?? true,
            CheckPathExists = options?.CheckPathExists ?? true,
            AddExtension = options?.AddExtension ?? true,
            Multiselect = false,
        };
        ApplyFileDialogOptions(dialog, options);

        return dialog.ShowDialog(this) == true
            ? DialogOutcome.Completed(dialog.FileName)
            : DialogOutcome.CanceledByUser();
    }

    private DialogOutcome ShowSaveFileDialog(DesktopDialogOptions? options)
    {
        var dialog = new SaveFileDialog
        {
            Title = ValueOrDefault(options?.Title, "Save File"),
            Filter = ValueOrDefault(options?.Filter, "All files (*.*)|*.*"),
            CheckFileExists = options?.CheckFileExists ?? false,
            CheckPathExists = options?.CheckPathExists ?? true,
            AddExtension = options?.AddExtension ?? true,
            OverwritePrompt = options?.OverwritePrompt ?? true,
            CreatePrompt = options?.CreatePrompt ?? false,
        };
        ApplyFileDialogOptions(dialog, options);

        return dialog.ShowDialog(this) == true
            ? DialogOutcome.Completed(dialog.FileName)
            : DialogOutcome.CanceledByUser();
    }

    private DialogOutcome ShowOpenFolderDialog(DesktopDialogOptions? options)
    {
        var dialog = new OpenFolderDialog
        {
            Title = ValueOrDefault(options?.Title, "Select Folder"),
            InitialDirectory = options?.Path ?? options?.InitialDirectory ?? string.Empty,
            FolderName = options?.Path ?? options?.FolderName ?? string.Empty,
            Multiselect = false,
        };

        return dialog.ShowDialog(this) == true
            ? DialogOutcome.Completed(dialog.FolderName)
            : DialogOutcome.CanceledByUser();
    }

    private static void ApplyFileDialogOptions(FileDialog dialog, DesktopDialogOptions? options)
    {
        if (options is null)
            return;

        dialog.InitialDirectory = options.InitialDirectory ?? string.Empty;
        dialog.FileName = options.Path ?? options.FileName ?? string.Empty;
        dialog.DefaultExt = options.DefaultExt ?? options.DefaultExtension ?? string.Empty;
    }

    private static string ValueOrDefault(string? value, string defaultValue)
        => string.IsNullOrWhiteSpace(value) ? defaultValue : value;

    private void PostDialogResponse(string requestId, DialogOutcome outcome)
    {
        CoreWebView2? webView = Browser.CoreWebView2;
        if (_isClosing || webView is null || !IsCurrentAdminSource(webView.Source))
            return;

        try
        {
            var response = new DesktopDialogResponse(
                DialogResultType,
                requestId,
                outcome.Canceled,
                outcome.Succeeded,
                outcome.Path,
                outcome.Error);
            webView.PostWebMessageAsJson(
                JsonSerializer.Serialize(response, WebMessageJsonOptions));
        }
        catch
        {
            // The page or WebView may have gone away while a native dialog was open.
        }
    }

    private sealed record DesktopDialogRequest(
        string? Type,
        string RequestId,
        string Dialog,
        DesktopDialogOptions? Options);

    private sealed record DesktopDialogOptions(
        string? Title,
        string? Filter,
        string? DefaultExtension,
        string? DefaultExt,
        string? InitialDirectory,
        string? FileName,
        string? FolderName,
        string? Path,
        bool? AddExtension,
        bool? CheckFileExists,
        bool? CheckPathExists,
        bool? OverwritePrompt,
        bool? CreatePrompt);

    private sealed record DesktopDialogResponse(
        string Type,
        string RequestId,
        bool Canceled,
        bool Succeeded,
        string? Path,
        string? Error);

    private readonly record struct DialogOutcome(
        bool Canceled,
        bool Succeeded,
        string? Path,
        string? Error)
    {
        public static DialogOutcome Completed(string? path = null) => new(false, true, path, null);
        public static DialogOutcome CanceledByUser() => new(true, false, null, null);
        public static DialogOutcome Failed(string error) => new(false, false, null, error);
    }

    private void OnExit(object sender, RoutedEventArgs e)
        => Close();
}
