using System;
using System.IO;
using System.Net.Http;
using System.Net.Http.Json;
using System.Threading;
using System.Windows;
using System.Windows.Media.Imaging;
using Microsoft.Win32;
using Microsoft.Web.WebView2.Core;

namespace CSharpDB.Admin.Desktop;

public partial class MainWindow : Window
{
    private readonly CancellationTokenSource _shutdown = new();
    private readonly AdminHostProcess _adminHost = new();
    private AdminHostSession? _adminHostSession;

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
            Browser.CoreWebView2.Settings.AreDevToolsEnabled = false;
            Browser.CoreWebView2.Settings.AreDefaultContextMenusEnabled = true;
            Browser.Source = _adminHostSession.BaseUri;
            Browser.Visibility = Visibility.Visible;
            StartupPanel.Visibility = Visibility.Collapsed;
            OpenDatabaseMenuItem.IsEnabled = true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            SetStatus("CSharpDB Studio could not start.\n\n" + ex.Message);
        }
    }

    protected override async void OnClosed(EventArgs e)
    {
        _shutdown.Cancel();

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
    {
        if (_adminHostSession is null)
            return;

        var dialog = new OpenFileDialog
        {
            Title = "Open CSharpDB Database",
            Filter = "Database files (*.db;*.cdb;*.csdb)|*.db;*.cdb;*.csdb|All files (*.*)|*.*",
            CheckFileExists = false,
            AddExtension = true,
            DefaultExt = ".db",
        };

        if (dialog.ShowDialog(this) != true)
            return;

        OpenDatabaseMenuItem.IsEnabled = false;
        try
        {
            using var client = new HttpClient { BaseAddress = _adminHostSession.BaseUri };
            using var request = new HttpRequestMessage(HttpMethod.Post, "_desktop/open-database")
            {
                Content = JsonContent.Create(new { databasePath = dialog.FileName }),
            };
            request.Headers.TryAddWithoutValidation(
                "X-CSharpDB-Desktop-Shell-Token",
                _adminHostSession.DesktopShellToken);

            using HttpResponseMessage response = await client.SendAsync(request, _shutdown.Token);
            response.EnsureSuccessStatusCode();

            Browser.CoreWebView2?.Reload();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            MessageBox.Show(this, ex.Message, "Open Database Failed", MessageBoxButton.OK, MessageBoxImage.Error);
        }
        finally
        {
            OpenDatabaseMenuItem.IsEnabled = _adminHostSession is not null && !_shutdown.IsCancellationRequested;
        }
    }

    private void OnExit(object sender, RoutedEventArgs e)
        => Close();
}
