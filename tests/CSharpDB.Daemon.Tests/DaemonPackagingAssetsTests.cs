namespace CSharpDB.Daemon.Tests;

public sealed class DaemonPackagingAssetsTests
{
    [Fact]
    public void PublishScript_UsesExpectedDaemonArchiveContract()
    {
        string repoRoot = FindRepoRoot();
        string script = File.ReadAllText(Path.Combine(repoRoot, "scripts", "Publish-CSharpDbDaemonRelease.ps1"));

        Assert.Contains("win-x64", script);
        Assert.Contains("linux-x64", script);
        Assert.Contains("osx-arm64", script);
        Assert.Contains("-p:PublishSingleFile=true", script);
        Assert.Contains("-p:PublishTrimmed=false", script);
        Assert.Contains("csharpdb-daemon-v$ReleaseVersion-$Rid", script);
        Assert.Contains("SHA256SUMS.txt", script);
    }

    [Fact]
    public void ServiceInstallAssets_ContainExpectedDefaults()
    {
        string repoRoot = FindRepoRoot();

        string windowsInstall = File.ReadAllText(Path.Combine(
            repoRoot,
            "deploy",
            "daemon",
            "windows",
            "install-csharpdb-daemon.ps1"));
        string linuxInstall = File.ReadAllText(Path.Combine(
            repoRoot,
            "deploy",
            "daemon",
            "linux",
            "install-csharpdb-daemon.sh"));
        string macInstall = File.ReadAllText(Path.Combine(
            repoRoot,
            "deploy",
            "daemon",
            "macos",
            "install-csharpdb-daemon.sh"));

        Assert.Contains("CSharpDBDaemon", windowsInstall);
        Assert.Contains("CSharpDB\\Daemon", windowsInstall);
        Assert.Contains("CSharpDB", windowsInstall);
        Assert.Contains("http://127.0.0.1:5820", windowsInstall);
        Assert.Contains("CSharpDB__Daemon__EnableRestApi=true", windowsInstall);

        Assert.Contains("/opt/csharpdb-daemon", linuxInstall);
        Assert.Contains("/var/lib/csharpdb", linuxInstall);
        Assert.Contains("SERVICE_USER=\"csharpdb\"", linuxInstall);
        Assert.Contains("http://127.0.0.1:5820", linuxInstall);
        Assert.Contains("CSharpDB__Daemon__EnableRestApi=true", linuxInstall);

        Assert.Contains("com.csharpdb.daemon", macInstall);
        Assert.Contains("/usr/local/lib/csharpdb-daemon", macInstall);
        Assert.Contains("/usr/local/var/csharpdb", macInstall);
        Assert.Contains("http://127.0.0.1:5820", macInstall);
        Assert.Contains("\"EnableRestApi\": true", macInstall);
    }

    [Fact]
    public void ServiceTemplates_AreParameterizedForInstallScripts()
    {
        string repoRoot = FindRepoRoot();
        string systemdTemplate = File.ReadAllText(Path.Combine(
            repoRoot,
            "deploy",
            "daemon",
            "linux",
            "csharpdb-daemon.service"));
        string launchdTemplate = File.ReadAllText(Path.Combine(
            repoRoot,
            "deploy",
            "daemon",
            "macos",
            "com.csharpdb.daemon.plist"));

        Assert.Contains("{{INSTALL_DIR}}", systemdTemplate);
        Assert.Contains("{{ENV_FILE}}", systemdTemplate);
        Assert.Contains("{{SERVICE_USER}}", systemdTemplate);
        Assert.Contains("{{SERVICE_GROUP}}", systemdTemplate);

        Assert.Contains("{{SERVICE_NAME}}", launchdTemplate);
        Assert.Contains("{{INSTALL_DIR}}", launchdTemplate);
        Assert.Contains("{{DATABASE_PATH}}", launchdTemplate);
        Assert.Contains("{{URL}}", launchdTemplate);
        Assert.Contains("CSharpDB__Daemon__EnableRestApi", launchdTemplate);
    }

    private static string FindRepoRoot()
    {
        DirectoryInfo? directory = new(AppContext.BaseDirectory);

        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "CSharpDB.slnx")))
                return directory.FullName;

            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException("Could not locate repository root from test base directory.");
    }
}
