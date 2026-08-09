using System.Diagnostics;
using System.Xml.Linq;

namespace CSharpDB.Cli.Tests;

public sealed class MigrationReleasePackagingTests
{
    [Fact]
    public void PublishScript_ComposesAuditedFrameworkDependentBundles()
    {
        string repoRoot = FindRepoRoot();
        string releaseScript = Read(
            repoRoot,
            "scripts",
            "Publish-CSharpDbMigrationRelease.ps1");
        string sqlServerPublisher = Read(
            repoRoot,
            "scripts",
            "Publish-CSharpDbSqlServerMigrationBundle.ps1");
        string mySqlPublisher = Read(
            repoRoot,
            "scripts",
            "Publish-CSharpDbMySqlMigrationBundle.ps1");
        string accessPublisher = Read(
            repoRoot,
            "scripts",
            "Publish-CSharpDbAccessMigrationBundle.ps1");

        Assert.Contains(
            "Publish-CSharpDbSqlServerMigrationBundle.ps1",
            releaseScript,
            StringComparison.Ordinal);
        Assert.Contains(
            "Publish-CSharpDbMySqlMigrationBundle.ps1",
            releaseScript,
            StringComparison.Ordinal);
        Assert.Contains(
            "Publish-CSharpDbAccessMigrationBundle.ps1",
            releaseScript,
            StringComparison.Ordinal);
        Assert.Contains(
            "Assert-ByteIdenticalBaseRoots",
            releaseScript,
            StringComparison.Ordinal);
        Assert.Contains(
            "Get-BaseRootManifest",
            releaseScript,
            StringComparison.Ordinal);
        Assert.Contains(
            "Assert-ExistingPathHasNoReparsePoints",
            releaseScript,
            StringComparison.Ordinal);
        Assert.Contains(
            "'adapters/'",
            releaseScript,
            StringComparison.Ordinal);

        int sqlPublishIndex = releaseScript.IndexOf(
            "& $sqlServerPublisher",
            StringComparison.Ordinal);
        int mySqlPublishIndex = releaseScript.IndexOf(
            "& $mySqlPublisher",
            StringComparison.Ordinal);
        int accessPublishIndex = releaseScript.IndexOf(
            "& $accessPublisher",
            StringComparison.Ordinal);
        int identityCheckIndex = releaseScript.LastIndexOf(
            "Assert-ByteIdenticalBaseRoots",
            StringComparison.Ordinal);
        int mergeIndex = releaseScript.LastIndexOf(
            "Copy-DirectoryContents",
            StringComparison.Ordinal);
        Assert.True(
            sqlPublishIndex >= 0 &&
            mySqlPublishIndex > sqlPublishIndex &&
            accessPublishIndex > mySqlPublishIndex &&
            identityCheckIndex > accessPublishIndex &&
            mergeIndex > identityCheckIndex,
            "Every applicable audited bundle must publish and pass the byte-identity check before adapter merging.");

        foreach (string publisher in
                 new[]
                 {
                     sqlServerPublisher,
                     mySqlPublisher,
                     accessPublisher,
                 })
        {
            Assert.Contains(
                "Assert-ExistingPathHasNoReparsePoints",
                publisher,
                StringComparison.Ordinal);
            Assert.Contains(
                "-Description 'The bundle destination'",
                publisher,
                StringComparison.Ordinal);
            Assert.Contains(
                "'--self-contained'",
                publisher,
                StringComparison.Ordinal);
            Assert.Contains(
                "'false'",
                publisher,
                StringComparison.Ordinal);
            Assert.Contains(
                "'-p:UseAppHost=true'",
                publisher,
                StringComparison.Ordinal);
        }

        Assert.Contains(
            "[ValidateSet('win-x64')]",
            accessPublisher,
            StringComparison.Ordinal);
        Assert.Contains(
            "Assert-NoAccessAssets",
            accessPublisher,
            StringComparison.Ordinal);
        Assert.Contains(
            "Assert-ReviewedWorkerPackageClosure",
            accessPublisher,
            StringComparison.Ordinal);
        Assert.Contains(
            "System.Data.OleDb/10.0.9",
            accessPublisher,
            StringComparison.Ordinal);

        Assert.Contains(
            "Assert-DotNetTenRuntimeConfig",
            releaseScript,
            StringComparison.Ordinal);
        Assert.Contains(
            "Microsoft.NETCore.App",
            releaseScript,
            StringComparison.Ordinal);
        Assert.Contains(
            "framework-dependent (.NET 10 runtime required)",
            releaseScript,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "'--self-contained', 'true'",
            releaseScript,
            StringComparison.Ordinal);
    }

    [Fact]
    public void PublishScript_UsesExpectedCombinedArchiveContract()
    {
        string script = Read(
            FindRepoRoot(),
            "scripts",
            "Publish-CSharpDbMigrationRelease.ps1");

        Assert.Contains(
            "[string] $Version = '4.5.0'",
            script,
            StringComparison.Ordinal);
        Assert.Contains("win-x64", script, StringComparison.Ordinal);
        Assert.Contains("linux-x64", script, StringComparison.Ordinal);
        Assert.Contains("osx-arm64", script, StringComparison.Ordinal);
        Assert.Contains(
            "csharpdb-migration-tool-v$releaseVersion-$rid",
            script,
            StringComparison.Ordinal);
        Assert.Contains("'zip'", script, StringComparison.Ordinal);
        Assert.Contains("'tar.gz'", script, StringComparison.Ordinal);
        Assert.Contains(
            "SHA256SUMS.txt",
            script,
            StringComparison.Ordinal);
        Assert.Contains(
            "Get-FileHash",
            script,
            StringComparison.Ordinal);

        string[] requiredLayout =
        [
            "adapters/sqlserver/csharpdb-migration-sqlserver-worker",
            "adapters/sqlserver/THIRD-PARTY-NOTICES.md",
            "adapters/sqlserver/licenses/Microsoft.Data.SqlClient.SNI.runtime-6.0.2-LICENSE.txt",
            "adapters/mysql/csharpdb-migration-mysql-worker",
            "adapters/mysql/THIRD-PARTY-NOTICES.md",
            "adapters/access/csharpdb-migration-access-worker",
            "adapters/access/CSharpDB.Migration.Access.dll",
            "adapters/access/CSharpDB.Migration.Retained.dll",
            "adapters/access/System.Data.OleDb.dll",
            "adapters/access/THIRD-PARTY-NOTICES.md",
            "install/windows/install-csharpdb-migration-tool.ps1",
            "install/posix/install-csharpdb-migration-tool.sh",
            "LICENSE",
            "README.md",
            "VERSION.txt",
        ];
        foreach (string expected in requiredLayout)
        {
            Assert.Contains(
                expected,
                script,
                StringComparison.Ordinal);
        }

        Assert.Contains(
            "-LiteralPath (Join-Path $mySqlBundle 'adapters/mysql')",
            script,
            StringComparison.Ordinal);
        Assert.Contains(
            "-LiteralPath (Join-Path $accessBundle 'adapters/access')",
            script,
            StringComparison.Ordinal);
        Assert.Contains(
            "if ($targetIsWindows)",
            script,
            StringComparison.Ordinal);
        Assert.Contains(
            "A POSIX migration release cannot contain the Windows-only Access adapter.",
            script,
            StringComparison.Ordinal);
        Assert.Contains(
            "A POSIX migration release dependency graph contains the Windows-only Access dependency",
            script,
            StringComparison.Ordinal);
        Assert.Contains(
            "-Source $sqlServerBundle",
            script,
            StringComparison.Ordinal);
        Assert.Contains(
            "Assert-PosixTarArchiveModes",
            script,
            StringComparison.Ordinal);
        Assert.Contains(
            "#requires -Version 7.4",
            script,
            StringComparison.Ordinal);
        Assert.Contains(
            "[System.Formats.Tar.PaxTarEntry]",
            script,
            StringComparison.Ordinal);
        Assert.Contains(
            "[IO.UnixFileMode] 0x1ED",
            script,
            StringComparison.Ordinal);
        Assert.Contains(
            "[IO.UnixFileMode] 0x1A4",
            script,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "'-czf'",
            script,
            StringComparison.Ordinal);

        int tarCreateIndex = script.LastIndexOf(
            "New-TarGzArchive",
            StringComparison.Ordinal);
        int tarModeCheckIndex = script.LastIndexOf(
            "Assert-PosixTarArchiveModes",
            StringComparison.Ordinal);
        int archiveRegistrationIndex = script.LastIndexOf(
            "$createdArchives.Add",
            StringComparison.Ordinal);
        Assert.True(
            tarCreateIndex >= 0 &&
            tarModeCheckIndex > tarCreateIndex &&
            archiveRegistrationIndex > tarModeCheckIndex,
            "A POSIX tarball must pass its exact mode check before checksum registration.");
    }

    [Fact]
    public void ReleaseWorkflow_PublishesMigrationArchivesAsReleaseAssets()
    {
        string repoRoot = FindRepoRoot();
        string workflow = Read(
            repoRoot,
            ".github",
            "workflows",
            "release.yml");
        string ciWorkflow = Read(
            repoRoot,
            ".github",
            "workflows",
            "ci.yml");

        Assert.Contains("migration-archives:", workflow, StringComparison.Ordinal);
        Assert.Contains("rid: win-x64", workflow, StringComparison.Ordinal);
        Assert.Contains("rid: linux-x64", workflow, StringComparison.Ordinal);
        Assert.Contains("rid: osx-arm64", workflow, StringComparison.Ordinal);
        Assert.Contains(
            "Publish-CSharpDbMigrationRelease.ps1",
            workflow,
            StringComparison.Ordinal);
        Assert.Contains(
            "RELEASE_TAG: ${{ inputs.release_tag }}",
            workflow,
            StringComparison.Ordinal);
        Assert.Contains(
            "name: migration-${{ matrix.rid }}",
            workflow,
            StringComparison.Ordinal);
        Assert.Contains(
            "needs: [publish-nuget, migration-archives,",
            workflow,
            StringComparison.Ordinal);
        Assert.Contains("pattern: migration-*", workflow, StringComparison.Ordinal);
        Assert.Contains(
            "MIGRATION-SHA256SUMS.txt",
            workflow,
            StringComparison.Ordinal);
        Assert.Contains(
            "artifacts/migration/*",
            workflow,
            StringComparison.Ordinal);

        int publishIndex = workflow.IndexOf(
            "Publish-CSharpDbMigrationRelease.ps1",
            StringComparison.Ordinal);
        int uploadIndex = workflow.IndexOf(
            "name: migration-${{ matrix.rid }}",
            StringComparison.Ordinal);
        int releaseIndex = workflow.IndexOf(
            "artifacts/migration/*",
            StringComparison.Ordinal);
        Assert.True(
            publishIndex >= 0 &&
            uploadIndex > publishIndex &&
            releaseIndex > uploadIndex,
            "Migration archives must be published, uploaded, and then attached to the release.");

        foreach (string provider in new[] { "Access", "SqlServer", "MySql" })
        {
            Assert.Contains(
                $"Test-{provider}MigrationIsolation.ps1 -Configuration Release",
                ciWorkflow,
                StringComparison.Ordinal);
            Assert.DoesNotContain(
                $"Test-{provider}MigrationIsolation.ps1 -Configuration Release -NoRestore",
                ciWorkflow,
                StringComparison.Ordinal);
        }
    }

    [Fact]
    public void SqlServerWorker_PublishesPrivateReviewedSniRuntimeFailClosed()
    {
        string repoRoot = FindRepoRoot();
        XDocument project = XDocument.Load(
            Path.Combine(
                repoRoot,
                "src",
                "CSharpDB.Migration.SqlServer.Worker",
                "CSharpDB.Migration.SqlServer.Worker.csproj"));
        XElement packageReference = Assert.Single(
            project.Descendants("PackageReference"),
            element => string.Equals(
                (string?)element.Attribute("Include"),
                "Microsoft.Data.SqlClient.SNI.runtime",
                StringComparison.Ordinal));

        Assert.Equal(
            "6.0.2",
            (string?)packageReference.Attribute("Version"));
        Assert.Equal(
            "all",
            (string?)packageReference.Attribute("PrivateAssets"));
        Assert.Equal(
            "true",
            (string?)packageReference.Attribute("Publish"));

        string publisher = Read(
            repoRoot,
            "scripts",
            "Publish-CSharpDbSqlServerMigrationBundle.ps1");
        Assert.Contains(
            "'Microsoft.Data.SqlClient.SNI.runtime/6.0.2'",
            publisher,
            StringComparison.Ordinal);
        Assert.Contains(
            "'Microsoft.Data.SqlClient.SNI.dll'",
            publisher,
            StringComparison.Ordinal);
        Assert.Contains(
            "'runtimes/win-x64/native/Microsoft.Data.SqlClient.SNI.dll'",
            publisher,
            StringComparison.Ordinal);
        Assert.Contains(
            "Assert-ReviewedWorkerPackageClosure",
            publisher,
            StringComparison.Ordinal);
        Assert.Contains(
            "$targetIncludesSniRuntime",
            publisher,
            StringComparison.Ordinal);
        Assert.Contains(
            "-IncludesSniRuntime $targetIncludesSniRuntime",
            publisher,
            StringComparison.Ordinal);
        Assert.Contains(
            "Compare-Object",
            publisher,
            StringComparison.Ordinal);
        Assert.Contains(
            "9335E8BAD875DD7BE4EEBD55D2335EB6433D1CEA61AADB3817AF7807BEF8932A",
            publisher,
            StringComparison.Ordinal);
    }

    [Fact]
    public void PublishScript_ConstrainsReplacementToManagedChildDirectories()
    {
        string script = Read(
            FindRepoRoot(),
            "scripts",
            "Publish-CSharpDbMigrationRelease.ps1");

        Assert.Contains(
            "Assert-ManagedChildPath",
            script,
            StringComparison.Ordinal);
        Assert.Contains(
            "OutputRoot cannot be the repository root or a filesystem root.",
            script,
            StringComparison.Ordinal);
        Assert.Contains(
            "links or reparse points",
            script,
            StringComparison.Ordinal);
        Assert.Contains(
            "Pass -Force to replace it",
            script,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "Remove-Item -LiteralPath $OutputRoot",
            script,
            StringComparison.Ordinal);
    }

    [Fact]
    public void Installers_DefaultToNoOverwriteAndPreserveAdapterLayout()
    {
        string repoRoot = FindRepoRoot();
        string windows = Read(
            repoRoot,
            "deploy",
            "migration-tool",
            "windows",
            "install-csharpdb-migration-tool.ps1");
        string posix = Read(
            repoRoot,
            "deploy",
            "migration-tool",
            "posix",
            "install-csharpdb-migration-tool.sh");

        Assert.Contains(
            "[switch] $Force",
            windows,
            StringComparison.Ordinal);
        Assert.Contains(
            "destinationHasContent",
            windows,
            StringComparison.Ordinal);
        Assert.Contains(
            "$Force.IsPresent",
            windows,
            StringComparison.Ordinal);
        Assert.Contains(
            "[IO.File]::Copy",
            windows,
            StringComparison.Ordinal);
        Assert.Contains(
            "Get-ContainedChildPath",
            windows,
            StringComparison.Ordinal);
        Assert.Contains(
            "$destinationLinks",
            windows,
            StringComparison.Ordinal);
        Assert.Contains(
            "including with -Force",
            windows,
            StringComparison.Ordinal);
        Assert.Contains(
            "Assert-ExistingPathHasNoReparsePoints",
            windows,
            StringComparison.Ordinal);
        Assert.Contains(
            "-Description 'The install path'",
            windows,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "Remove-Item",
            windows,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "SetEnvironmentVariable",
            windows,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "Start-Process",
            windows,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "RunAs",
            windows,
            StringComparison.OrdinalIgnoreCase);

        Assert.Contains("--force", posix, StringComparison.Ordinal);
        Assert.Contains(
            "command ls -A",
            posix,
            StringComparison.Ordinal);
        Assert.Contains(
            "command cp -Rp",
            posix,
            StringComparison.Ordinal);
        Assert.Contains(
            "DESTINATION_LINK",
            posix,
            StringComparison.Ordinal);
        Assert.Contains(
            "including with --force",
            posix,
            StringComparison.Ordinal);
        Assert.Contains(
            "A derived executable destination escapes the install directory.",
            posix,
            StringComparison.Ordinal);
        Assert.Contains(
            "The filesystem root cannot be used as the install directory.",
            posix,
            StringComparison.Ordinal);
        Assert.DoesNotContain("rm -rf", posix, StringComparison.Ordinal);
        Assert.DoesNotContain("sudo", posix, StringComparison.Ordinal);
        Assert.DoesNotContain("/etc/", posix, StringComparison.Ordinal);
        Assert.DoesNotContain(
            "adapters/access",
            posix,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "System.Data.OleDb",
            posix,
            StringComparison.Ordinal);

        Assert.Contains(
            "adapters/access/csharpdb-migration-access-worker.exe",
            windows,
            StringComparison.Ordinal);
        Assert.Contains(
            "adapters/access/CSharpDB.Migration.Access.dll",
            windows,
            StringComparison.Ordinal);
        Assert.Contains(
            "adapters/access/CSharpDB.Migration.Retained.dll",
            windows,
            StringComparison.Ordinal);
        Assert.Contains(
            "adapters/access/System.Data.OleDb.dll",
            windows,
            StringComparison.Ordinal);
        Assert.Contains(
            "adapters/access/THIRD-PARTY-NOTICES.md",
            windows,
            StringComparison.Ordinal);

        foreach (string installer in new[] { windows, posix })
        {
            Assert.Contains(
                ".NET 10 runtime",
                installer,
                StringComparison.Ordinal);
            Assert.Contains(
                "adapters/sqlserver",
                installer,
                StringComparison.Ordinal);
            Assert.Contains(
                "adapters/mysql",
                installer,
                StringComparison.Ordinal);
            Assert.Contains(
                "adapters/sqlserver/licenses/Microsoft.Data.SqlClient.SNI.runtime-6.0.2-LICENSE.txt",
                installer,
                StringComparison.Ordinal);
            Assert.Contains(
                "PATH was not changed",
                installer,
                StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task NativeInstaller_ForcePreservesFilesAndRejectsNestedLinks()
    {
        string repoRoot = FindRepoRoot();
        string root = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_migration_installer_{Guid.NewGuid():N}");
        string source = Path.Combine(root, "source");
        string destination = Path.Combine(root, "destination");
        string outside = Path.Combine(root, "outside");
        Directory.CreateDirectory(source);
        Directory.CreateDirectory(destination);
        Directory.CreateDirectory(outside);

        string link = Path.Combine(destination, "redirect");
        try
        {
            CreateMinimalRelease(source);
            File.WriteAllText(
                Path.Combine(destination, "unrelated.txt"),
                "preserve");

            ProcessResult install = await RunNativeInstallerAsync(
                repoRoot,
                source,
                destination);

            Assert.Equal(0, install.ExitCode);
            Assert.Equal(
                "preserve",
                File.ReadAllText(
                    Path.Combine(destination, "unrelated.txt")));
            Assert.Equal(
                "release payload",
                File.ReadAllText(
                    Path.Combine(destination, "redirect", "payload.txt")));

            Directory.Delete(link, recursive: true);
            bool linkCreated;
            try
            {
                Directory.CreateSymbolicLink(link, outside);
                linkCreated = true;
            }
            catch (Exception exception) when (
                exception is UnauthorizedAccessException or
                PlatformNotSupportedException or
                IOException)
            {
                linkCreated = false;
            }

            if (!linkCreated)
                return;

            string outsidePayload = Path.Combine(outside, "payload.txt");
            File.WriteAllText(outsidePayload, "outside sentinel");
            ProcessResult rejected = await RunNativeInstallerAsync(
                repoRoot,
                source,
                destination);

            Assert.NotEqual(0, rejected.ExitCode);
            Assert.Contains(
                OperatingSystem.IsWindows()
                    ? "links or reparse points"
                    : "symbolic links",
                rejected.StdErr,
                StringComparison.OrdinalIgnoreCase);
            Assert.Equal(
                "outside sentinel",
                File.ReadAllText(outsidePayload));
            Assert.Equal(
                "preserve",
                File.ReadAllText(
                    Path.Combine(destination, "unrelated.txt")));

            Directory.Delete(link);
        }
        finally
        {
            if (Directory.Exists(link) ||
                File.Exists(link))
            {
                Directory.Delete(link);
            }
            if (Directory.Exists(root))
                Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task WindowsInstaller_RejectsLinkedInstallAncestor()
    {
        if (!OperatingSystem.IsWindows())
            return;

        string repoRoot = FindRepoRoot();
        string root = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_migration_ancestor_{Guid.NewGuid():N}");
        string source = Path.Combine(root, "source");
        string physicalParent = Path.Combine(root, "physical");
        string linkedParent = Path.Combine(root, "linked");
        Directory.CreateDirectory(source);
        Directory.CreateDirectory(physicalParent);

        try
        {
            CreateMinimalRelease(source);
            try
            {
                Directory.CreateSymbolicLink(
                    linkedParent,
                    physicalParent);
            }
            catch (Exception exception) when (
                exception is UnauthorizedAccessException or
                PlatformNotSupportedException or
                IOException)
            {
                return;
            }

            string sentinel = Path.Combine(
                physicalParent,
                "outside-sentinel.txt");
            File.WriteAllText(sentinel, "outside sentinel");
            ProcessResult rejected = await RunNativeInstallerAsync(
                repoRoot,
                source,
                Path.Combine(linkedParent, "destination"));

            Assert.NotEqual(0, rejected.ExitCode);
            Assert.Contains(
                "cannot pass through a link or reparse point",
                rejected.StdErr,
                StringComparison.OrdinalIgnoreCase);
            Assert.Equal(
                "outside sentinel",
                File.ReadAllText(sentinel));
            Assert.False(
                Directory.Exists(
                    Path.Combine(
                        physicalParent,
                        "destination")));
        }
        finally
        {
            if (Directory.Exists(linkedParent) ||
                File.Exists(linkedParent))
            {
                Directory.Delete(linkedParent);
            }
            if (Directory.Exists(root))
                Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void MigrationReadme_DisclosesRuntimeDataAndQualificationBoundaries()
    {
        string readme = Read(
            FindRepoRoot(),
            "deploy",
            "migration-tool",
            "README.md");

        Assert.Contains(
            "framework-dependent release",
            readme,
            StringComparison.Ordinal);
        Assert.Contains(
            "not a self-contained application",
            readme,
            StringComparison.Ordinal);
        Assert.Contains(
            "Microsoft .NET 10 runtime",
            readme,
            StringComparison.Ordinal);
        Assert.Contains(
            "THIRD-PARTY-NOTICES.md",
            readme,
            StringComparison.Ordinal);
        Assert.Contains(
            "licenses/",
            readme,
            StringComparison.Ordinal);
        Assert.Contains(
            "plaintext-sensitive",
            readme,
            StringComparison.Ordinal);
        Assert.Contains(
            "trusted record",
            readme,
            StringComparison.Ordinal);
        Assert.Contains(
            "MIGRATION-SHA256SUMS.txt",
            readme,
            StringComparison.Ordinal);
        Assert.Contains(
            "not claim broad live Access, SQL Server, or MySQL qualification",
            readme,
            StringComparison.Ordinal);
        Assert.Contains(
            "win-x64",
            readme,
            StringComparison.Ordinal);
        Assert.Contains(
            "Linux and macOS archives do not contain `adapters/access`",
            readme,
            StringComparison.Ordinal);
        Assert.Contains(
            "does not redistribute or install ACE",
            readme,
            StringComparison.Ordinal);
        Assert.Contains(
            ".csdbaccess",
            readme,
            StringComparison.Ordinal);
        Assert.Contains(
            "administrator access",
            readme,
            StringComparison.Ordinal);
        Assert.Contains(
            "does not use `sudo`",
            readme,
            StringComparison.Ordinal);
        Assert.Contains(
            "must not pass through links",
            readme,
            StringComparison.Ordinal);
        Assert.Contains(
            "reparse points",
            readme,
            StringComparison.Ordinal);
        Assert.Contains(
            "filesystem root",
            readme,
            StringComparison.Ordinal);
    }

    private static string Read(
        string repoRoot,
        params string[] components) =>
        File.ReadAllText(
            Path.Combine(
                new[] { repoRoot }
                    .Concat(components)
                    .ToArray()));

    private static void CreateMinimalRelease(string source)
    {
        string executableSuffix =
            OperatingSystem.IsWindows() ? ".exe" : string.Empty;
        var files =
            new Dictionary<string, string>(StringComparer.Ordinal)
            {
                [$"csharpdb{executableSuffix}"] = "release cli",
                ["LICENSE"] = "license",
                ["README.md"] = "readme",
                [$"adapters/sqlserver/csharpdb-migration-sqlserver-worker{executableSuffix}"] =
                    "sql server worker",
                ["adapters/sqlserver/THIRD-PARTY-NOTICES.md"] =
                    "sql server notices",
                ["adapters/sqlserver/licenses/Microsoft.Data.SqlClient.SNI.runtime-6.0.2-LICENSE.txt"] =
                    "SNI license",
                [$"adapters/mysql/csharpdb-migration-mysql-worker{executableSuffix}"] =
                    "mysql worker",
                ["adapters/mysql/THIRD-PARTY-NOTICES.md"] =
                    "mysql notices",
                ["redirect/payload.txt"] = "release payload",
            };
        if (OperatingSystem.IsWindows())
        {
            files["adapters/access/csharpdb-migration-access-worker.exe"] =
                "access worker";
            files["adapters/access/CSharpDB.Migration.Access.dll"] =
                "access adapter";
            files["adapters/access/CSharpDB.Migration.Retained.dll"] =
                "retained package reader";
            files["adapters/access/System.Data.OleDb.dll"] =
                "OLE DB provider bridge";
            files["adapters/access/THIRD-PARTY-NOTICES.md"] =
                "access notices";
            files["adapters/access/csharpdb-migration-access-worker.deps.json"] =
                "access dependencies";
        }

        foreach ((string relative, string contents) in files)
        {
            string path = Path.Combine(
                source,
                relative.Replace(
                    '/',
                    Path.DirectorySeparatorChar));
            Directory.CreateDirectory(
                Path.GetDirectoryName(path)!);
            File.WriteAllText(path, contents);
        }
    }

    private static async Task<ProcessResult> RunNativeInstallerAsync(
        string repoRoot,
        string source,
        string destination)
    {
        var startInfo = new ProcessStartInfo
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        if (OperatingSystem.IsWindows())
        {
            startInfo.FileName = "pwsh";
            startInfo.ArgumentList.Add("-NoLogo");
            startInfo.ArgumentList.Add("-NoProfile");
            startInfo.ArgumentList.Add("-File");
            startInfo.ArgumentList.Add(
                Path.Combine(
                    repoRoot,
                    "deploy",
                    "migration-tool",
                    "windows",
                    "install-csharpdb-migration-tool.ps1"));
            startInfo.ArgumentList.Add("-InstallDirectory");
            startInfo.ArgumentList.Add(destination);
            startInfo.ArgumentList.Add("-SourceDirectory");
            startInfo.ArgumentList.Add(source);
            startInfo.ArgumentList.Add("-Force");
        }
        else
        {
            startInfo.FileName = "/bin/sh";
            startInfo.ArgumentList.Add(
                Path.Combine(
                    repoRoot,
                    "deploy",
                    "migration-tool",
                    "posix",
                    "install-csharpdb-migration-tool.sh"));
            startInfo.ArgumentList.Add("--install-dir");
            startInfo.ArgumentList.Add(destination);
            startInfo.ArgumentList.Add("--source-dir");
            startInfo.ArgumentList.Add(source);
            startInfo.ArgumentList.Add("--force");
        }

        using var process = new Process { StartInfo = startInfo };
        Assert.True(
            process.Start(),
            "Failed to start the migration release installer.");
        Task<string> stdoutTask =
            process.StandardOutput.ReadToEndAsync();
        Task<string> stderrTask =
            process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync();

        return new ProcessResult(
            process.ExitCode,
            await stdoutTask,
            await stderrTask);
    }

    private static string FindRepoRoot()
    {
        DirectoryInfo? directory =
            new(AppContext.BaseDirectory);
        while (directory is not null)
        {
            if (File.Exists(
                    Path.Combine(
                        directory.FullName,
                        "CSharpDB.slnx")))
            {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException(
            "Could not locate repository root from test base directory.");
    }

    private sealed record ProcessResult(
        int ExitCode,
        string StdOut,
        string StdErr);
}
