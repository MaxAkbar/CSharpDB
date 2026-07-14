using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Security.Cryptography;

namespace CSharpDB.Admin.Desktop;

internal sealed class AdminHostProcess : IAsyncDisposable
{
    private const string AdminExecutableName = "CSharpDB.Admin.exe";
    private const string AdminExecutableEnvironmentVariable = "CSHARPDB_ADMIN_EXE";
    private const string DesktopShellTokenHeaderName = "X-CSharpDB-Desktop-Shell-Token";
    private const string AdminTargetFramework = "net10.0";

#if DEBUG
    private const string BuildConfiguration = "Debug";
#else
    private const string BuildConfiguration = "Release";
#endif

    private readonly CancellationTokenSource _logCancellation = new();
    private Process? _process;
    private Task? _stdoutTask;
    private Task? _stderrTask;

    public async Task<AdminHostSession> StartAsync(Action<string> reportStatus, CancellationToken ct)
    {
        string adminExecutablePath = ResolveAdminExecutablePath();
        string adminEnvironment = ResolveAdminEnvironment(adminExecutablePath);
        int port = ReserveLoopbackPort();
        string token = Convert.ToHexString(RandomNumberGenerator.GetBytes(32));
        var adminUri = new Uri($"http://127.0.0.1:{port}/");

        string timestamp = DateTime.UtcNow.ToString("yyyyMMdd-HHmmss");
        string stdoutPath = Path.Combine(DesktopPaths.LogDirectory, $"admin-{timestamp}.stdout.log");
        string stderrPath = Path.Combine(DesktopPaths.LogDirectory, $"admin-{timestamp}.stderr.log");

        var startInfo = new ProcessStartInfo(adminExecutablePath)
        {
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            WorkingDirectory = Path.GetDirectoryName(adminExecutablePath) ?? AppContext.BaseDirectory,
        };

        startInfo.Environment["ASPNETCORE_URLS"] = adminUri.GetLeftPart(UriPartial.Authority);
        startInfo.Environment["ConnectionStrings__CSharpDB"] = $"Data Source={DesktopPaths.DefaultDatabasePath}";
        startInfo.Environment["CSharpDB__Transport"] = "direct";
        startInfo.Environment["CSharpDB__DesktopShell"] = "true";
        startInfo.Environment["CSharpDB__DesktopShellToken"] = token;
        startInfo.Environment["CSharpDB__DesktopShellLogDirectory"] = DesktopPaths.LogDirectory;
        startInfo.Environment["DOTNET_ENVIRONMENT"] = adminEnvironment;
        startInfo.Environment["ASPNETCORE_ENVIRONMENT"] = adminEnvironment;

        _process = Process.Start(startInfo)
            ?? throw new InvalidOperationException("Failed to start the CSharpDB Admin host process.");

        _stdoutTask = CopyOutputAsync(_process.StandardOutput, stdoutPath, _logCancellation.Token);
        _stderrTask = CopyOutputAsync(_process.StandardError, stderrPath, _logCancellation.Token);

        reportStatus("Waiting for local Admin host...");
        await WaitForHealthAsync(adminUri, token, ct);

        return new AdminHostSession(adminUri, token);
    }

    public async ValueTask DisposeAsync()
    {
        _logCancellation.Cancel();

        if (_process is { HasExited: false } process)
        {
            try
            {
                process.Kill(entireProcessTree: true);
                await process.WaitForExitAsync();
            }
            catch (InvalidOperationException)
            {
            }
        }

        if (_stdoutTask is not null || _stderrTask is not null)
        {
            try
            {
                await Task.WhenAll(_stdoutTask ?? Task.CompletedTask, _stderrTask ?? Task.CompletedTask);
            }
            catch (OperationCanceledException)
            {
            }
            catch (ObjectDisposedException)
            {
            }
        }

        _process?.Dispose();
        _logCancellation.Dispose();
    }

    private async Task WaitForHealthAsync(Uri baseUri, string token, CancellationToken ct)
    {
        using var client = new HttpClient
        {
            BaseAddress = baseUri,
            Timeout = TimeSpan.FromSeconds(2),
        };

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, timeout.Token);

        while (!linked.IsCancellationRequested)
        {
            ct.ThrowIfCancellationRequested();

            if (_process is { HasExited: true })
                throw new InvalidOperationException($"The CSharpDB Admin host exited early with code {_process.ExitCode}.");

            try
            {
                using var request = new HttpRequestMessage(HttpMethod.Get, "healthz");
                request.Headers.TryAddWithoutValidation(DesktopShellTokenHeaderName, token);

                using HttpResponseMessage response = await client.SendAsync(request, linked.Token);
                if (response.IsSuccessStatusCode)
                    return;
            }
            catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or OperationCanceledException)
            {
                if (linked.IsCancellationRequested)
                    break;
            }

            try
            {
                await Task.Delay(250, linked.Token);
            }
            catch (OperationCanceledException) when (linked.IsCancellationRequested)
            {
                break;
            }
        }

        ct.ThrowIfCancellationRequested();
        throw new TimeoutException("Timed out waiting for the CSharpDB Admin host to become ready.");
    }

    private static int ReserveLoopbackPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static string ResolveAdminExecutablePath()
    {
        string? configured = Environment.GetEnvironmentVariable(AdminExecutableEnvironmentVariable);
        if (!string.IsNullOrWhiteSpace(configured) && File.Exists(configured))
            return Path.GetFullPath(configured);

        string baseDirectory = AppContext.BaseDirectory;
        foreach (string candidate in EnumerateAdminExecutableCandidates(baseDirectory).Distinct(StringComparer.OrdinalIgnoreCase))
        {
            if (File.Exists(candidate))
                return candidate;
        }

        throw new FileNotFoundException(
            $"Could not find the {BuildConfiguration} {AdminExecutableName}. Publish CSharpDB.Admin into an 'admin' subfolder next to the desktop shell, or set {AdminExecutableEnvironmentVariable}.");
    }

    private static IEnumerable<string> EnumerateAdminExecutableCandidates(string baseDirectory)
    {
        // Packaged layouts take precedence over development output discovery.
        yield return Path.Combine(baseDirectory, "admin", AdminExecutableName);
        yield return Path.Combine(baseDirectory, AdminExecutableName);
        yield return Path.GetFullPath(Path.Combine(baseDirectory, "..", "CSharpDB.Admin", AdminExecutableName));

        var current = new DirectoryInfo(baseDirectory);
        while (current is not null)
        {
            if (IsBuildConfigurationDirectory(current.Name))
            {
                foreach (string candidate in EnumerateArtifactsOutputCandidates(current))
                    yield return candidate;

                foreach (string candidate in EnumerateConventionalOutputCandidates(current, baseDirectory))
                    yield return candidate;
            }

            current = current.Parent;
        }
    }

    private static IEnumerable<string> EnumerateArtifactsOutputCandidates(DirectoryInfo configurationDirectory)
    {
        // --artifacts-path uses <output-kind>/<ProjectName>/<configuration-pivot>.
        DirectoryInfo? desktopProjectOutput = configurationDirectory.Parent;
        DirectoryInfo? outputDirectory = desktopProjectOutput?.Parent;
        if (desktopProjectOutput is null || outputDirectory is null ||
            !desktopProjectOutput.Name.Equals("CSharpDB.Admin.Desktop", StringComparison.OrdinalIgnoreCase))
        {
            yield break;
        }

        if (outputDirectory.Name.Equals("bin", StringComparison.OrdinalIgnoreCase))
        {
            yield return Path.Combine(
                outputDirectory.FullName,
                "CSharpDB.Admin",
                configurationDirectory.Name,
                AdminExecutableName);
            yield break;
        }

        if (!outputDirectory.Name.Equals("publish", StringComparison.OrdinalIgnoreCase) ||
            outputDirectory.Parent is not { } artifactsDirectory)
        {
            yield break;
        }

        // Publishing with --artifacts-path places this executable under
        // publish/<ProjectName>/<configuration-pivot>, while project references
        // are built under bin/<ProjectName>/<configuration-pivot>. Prefer a
        // separately published Admin host when present, then fall back to the
        // referenced project's build output produced by the desktop publish.
        yield return Path.Combine(
            outputDirectory.FullName,
            "CSharpDB.Admin",
            configurationDirectory.Name,
            AdminExecutableName);
        yield return Path.Combine(
            artifactsDirectory.FullName,
            "bin",
            "CSharpDB.Admin",
            configurationDirectory.Name,
            AdminExecutableName);
    }

    private static IEnumerable<string> EnumerateConventionalOutputCandidates(
        DirectoryInfo configurationDirectory,
        string baseDirectory)
    {
        // Normal SDK output uses <project>/bin/<configuration>/<tfm>[/<rid>][/publish].
        DirectoryInfo? binDirectory = configurationDirectory.Parent;
        DirectoryInfo? desktopProjectDirectory = binDirectory?.Parent;
        DirectoryInfo? sourceDirectory = desktopProjectDirectory?.Parent;
        if (binDirectory is null || desktopProjectDirectory is null || sourceDirectory is null ||
            !binDirectory.Name.Equals("bin", StringComparison.OrdinalIgnoreCase) ||
            !desktopProjectDirectory.Name.Equals("CSharpDB.Admin.Desktop", StringComparison.OrdinalIgnoreCase))
        {
            yield break;
        }

        string adminOutputDirectory = Path.Combine(
            sourceDirectory.FullName,
            "CSharpDB.Admin",
            "bin",
            configurationDirectory.Name,
            AdminTargetFramework);

        string[] relativeSegments = Path.GetRelativePath(configurationDirectory.FullName, baseDirectory)
            .Split([Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar], StringSplitOptions.RemoveEmptyEntries);
        string? runtimeIdentifier = relativeSegments.Length >= 2 &&
            !relativeSegments[1].Equals("publish", StringComparison.OrdinalIgnoreCase)
                ? relativeSegments[1]
                : null;
        bool isPublished = relativeSegments.Any(segment =>
            segment.Equals("publish", StringComparison.OrdinalIgnoreCase));

        if (runtimeIdentifier is not null)
        {
            string runtimeOutputDirectory = Path.Combine(adminOutputDirectory, runtimeIdentifier);
            if (isPublished)
                yield return Path.Combine(runtimeOutputDirectory, "publish", AdminExecutableName);

            yield return Path.Combine(runtimeOutputDirectory, AdminExecutableName);
        }
        else if (isPublished)
        {
            yield return Path.Combine(adminOutputDirectory, "publish", AdminExecutableName);
        }

        yield return Path.Combine(adminOutputDirectory, AdminExecutableName);
    }

    private static bool IsBuildConfigurationDirectory(string directoryName) =>
        directoryName.Equals(BuildConfiguration, StringComparison.OrdinalIgnoreCase) ||
        directoryName.StartsWith($"{BuildConfiguration}_", StringComparison.OrdinalIgnoreCase);

    private static string ResolveAdminEnvironment(string adminExecutablePath)
    {
        string adminDirectory = Path.GetDirectoryName(adminExecutablePath) ?? AppContext.BaseDirectory;
        string staticAssetsManifest = Path.Combine(adminDirectory, "CSharpDB.Admin.staticwebassets.runtime.json");
        return File.Exists(staticAssetsManifest) ? "Development" : "Production";
    }

    private static async Task CopyOutputAsync(StreamReader reader, string path, CancellationToken ct)
    {
        await using var stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read);
        await using var writer = new StreamWriter(stream);

        while (!ct.IsCancellationRequested)
        {
            string? line = await reader.ReadLineAsync(ct);
            if (line is null)
                break;

            await writer.WriteLineAsync(line);
            await writer.FlushAsync(ct);
        }
    }
}

internal sealed record AdminHostSession(Uri BaseUri, string DesktopShellToken);
