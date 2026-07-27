using System.Reflection;
using System.Runtime.Versioning;
using System.Security.AccessControl;
using System.Security.Principal;
using CSharpDB.Migration.Files.Csv;

namespace CSharpDB.Migration.Files.Tests;

public sealed class CsvSnapshotWorkspaceSafetyTests
{
    private static readonly Type WorkspaceType = typeof(CsvSnapshotPackage).Assembly.GetType(
        "CSharpDB.Migration.Files.Csv.CsvSnapshotWorkspace",
        throwOnError: true)!;

    private static readonly MethodInfo CreatePrivateDirectoryExclusiveMethod =
        WorkspaceType.GetMethod(
            "CreatePrivateDirectoryExclusive",
            BindingFlags.Static | BindingFlags.NonPublic,
            binder: null,
            types: [typeof(string)],
            modifiers: null)!;

    private static readonly MethodInfo DisposeAsyncMethod = WorkspaceType.GetMethod(
        nameof(IAsyncDisposable.DisposeAsync),
        BindingFlags.Instance | BindingFlags.Public)!;

    private static readonly PropertyInfo DirectoryPathProperty = WorkspaceType.GetProperty(
        "DirectoryPath",
        BindingFlags.Instance | BindingFlags.Public)!;

    [Fact]
    public void ExclusiveDirectoryClaimRejectsAndPreservesAnExistingDirectory()
    {
        using var temporary = new TemporaryDirectory();
        string candidatePath = temporary.PathFor("existing");
        Directory.CreateDirectory(candidatePath);
        string sentinelPath = Path.Combine(candidatePath, "sentinel.txt");
        File.WriteAllText(sentinelPath, "preserve");

        IOException error = AssertExclusiveClaimFails(candidatePath);

        Assert.Contains("already exists", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.True(Directory.Exists(candidatePath));
        Assert.Equal("preserve", File.ReadAllText(sentinelPath));
    }

    [Fact]
    public void ExclusiveDirectoryClaimRejectsAndPreservesAnExistingSymbolicLinkWhereSupported()
    {
        using var temporary = new TemporaryDirectory();
        string targetPath = temporary.PathFor("target");
        string candidatePath = temporary.PathFor("candidate");
        Directory.CreateDirectory(targetPath);
        string sentinelPath = Path.Combine(targetPath, "sentinel.txt");
        File.WriteAllText(sentinelPath, "preserve");
        if (!TryCreateDirectorySymbolicLink(candidatePath, targetPath))
            return;

        IOException error = AssertExclusiveClaimFails(candidatePath);

        Assert.Contains("already exists", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.True(Directory.Exists(candidatePath));
        Assert.Equal("preserve", File.ReadAllText(sentinelPath));
    }

    [Fact]
    public async Task DisposalRefusesAndPreservesAnInjectedChildDirectory()
    {
        using var temporary = new TemporaryDirectory();
        object workspace = CreateWorkspace(temporary.Root);
        string workspacePath = GetWorkspacePath(workspace);
        string injectedPath = Path.Combine(workspacePath, "injected");
        Directory.CreateDirectory(injectedPath);
        string sentinelPath = Path.Combine(injectedPath, "sentinel.txt");
        File.WriteAllText(sentinelPath, "preserve");

        Exception error = await CaptureDisposeFailureAsync(workspace);

        Assert.IsType<IOException>(error);
        Assert.True(Directory.Exists(workspacePath));
        Assert.True(Directory.Exists(injectedPath));
        Assert.Equal("preserve", File.ReadAllText(sentinelPath));
    }

    [Fact]
    public async Task DisposalRefusesAndPreservesAnInjectedDirectoryLinkAndItsTargetWhereSupported()
    {
        using var temporary = new TemporaryDirectory();
        string targetPath = temporary.PathFor("external-target");
        Directory.CreateDirectory(targetPath);
        string sentinelPath = Path.Combine(targetPath, "sentinel.txt");
        File.WriteAllText(sentinelPath, "preserve");

        object workspace = CreateWorkspace(temporary.Root);
        string workspacePath = GetWorkspacePath(workspace);
        string linkPath = Path.Combine(workspacePath, "injected-link");
        if (!TryCreateDirectorySymbolicLink(linkPath, targetPath))
        {
            await DisposeWorkspaceAsync(workspace);
            return;
        }

        Exception error = await CaptureDisposeFailureAsync(workspace);

        Assert.IsType<IOException>(error);
        Assert.True(Directory.Exists(workspacePath));
        Assert.True(Directory.Exists(linkPath));
        Assert.True(Directory.Exists(targetPath));
        Assert.Equal("preserve", File.ReadAllText(sentinelPath));
    }

    [Fact]
    [SupportedOSPlatform("windows")]
    public async Task WindowsWorkspaceHasAProtectedCurrentUserOnlyAccessPolicy()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var temporary = new TemporaryDirectory();
        object workspace = CreateWorkspace(temporary.Root);
        try
        {
            string workspacePath = GetWorkspacePath(workspace);
            DirectorySecurity security = FileSystemAclExtensions.GetAccessControl(
                new DirectoryInfo(workspacePath),
                AccessControlSections.Access | AccessControlSections.Owner);
            using WindowsIdentity identity = WindowsIdentity.GetCurrent(TokenAccessLevels.Query);
            SecurityIdentifier currentUser = identity.User ?? throw new InvalidOperationException(
                "The current Windows identity did not expose a user SID.");
            var localSystem = new SecurityIdentifier(WellKnownSidType.LocalSystemSid, domainSid: null);
            SecurityIdentifier owner = Assert.IsType<SecurityIdentifier>(
                security.GetOwner(typeof(SecurityIdentifier)));
            FileSystemAccessRule[] rules = security
                .GetAccessRules(
                    includeExplicit: true,
                    includeInherited: true,
                    targetType: typeof(SecurityIdentifier))
                .Cast<FileSystemAccessRule>()
                .ToArray();

            Assert.True(security.AreAccessRulesProtected);
            Assert.Equal(currentUser, owner);
            Assert.NotEmpty(rules);
            Assert.All(rules, rule =>
            {
                Assert.False(rule.IsInherited);
                Assert.Equal(AccessControlType.Allow, rule.AccessControlType);
                SecurityIdentifier sid = Assert.IsType<SecurityIdentifier>(rule.IdentityReference);
                Assert.True(
                    sid.Equals(currentUser) || sid.Equals(localSystem),
                    $"Unexpected workspace access principal: {sid.Value}");
                Assert.Equal(
                    FileSystemRights.FullControl,
                    rule.FileSystemRights & FileSystemRights.FullControl);
            });
            Assert.Contains(
                rules,
                rule =>
                    Assert.IsType<SecurityIdentifier>(rule.IdentityReference).Equals(currentUser) &&
                    rule.AccessControlType == AccessControlType.Allow &&
                    (rule.FileSystemRights & FileSystemRights.FullControl) == FileSystemRights.FullControl);
        }
        finally
        {
            await DisposeWorkspaceAsync(workspace);
        }
    }

    private static IOException AssertExclusiveClaimFails(string path)
    {
        TargetInvocationException invocation = Assert.Throws<TargetInvocationException>(
            () => CreatePrivateDirectoryExclusiveMethod.Invoke(obj: null, parameters: [path]));
        return Assert.IsType<IOException>(invocation.InnerException);
    }

    private static object CreateWorkspace(string rootPath) =>
        Activator.CreateInstance(WorkspaceType, [rootPath])!;

    private static string GetWorkspacePath(object workspace) =>
        Assert.IsType<string>(DirectoryPathProperty.GetValue(workspace));

    private static async Task<Exception> CaptureDisposeFailureAsync(object workspace)
    {
        try
        {
            await DisposeWorkspaceAsync(workspace);
        }
        catch (TargetInvocationException exception) when (exception.InnerException is not null)
        {
            return exception.InnerException;
        }
        catch (Exception exception)
        {
            return exception;
        }

        throw new InvalidOperationException("Workspace disposal unexpectedly succeeded.");
    }

    private static async ValueTask DisposeWorkspaceAsync(object workspace)
    {
        object? result = DisposeAsyncMethod.Invoke(workspace, parameters: null);
        await Assert.IsType<ValueTask>(result);
    }

    private static bool TryCreateDirectorySymbolicLink(string linkPath, string targetPath)
    {
        try
        {
            Directory.CreateSymbolicLink(linkPath, targetPath);
            return true;
        }
        catch (Exception exception) when (
            exception is PlatformNotSupportedException or
                UnauthorizedAccessException or
                IOException)
        {
            return false;
        }
    }

    private sealed class TemporaryDirectory : IDisposable
    {
        internal TemporaryDirectory()
        {
            Root = Path.Combine(
                Path.GetTempPath(),
                $"csharpdb-snapshot-workspace-safety-{Guid.NewGuid():N}");
            Directory.CreateDirectory(Root);
        }

        internal string Root { get; }

        internal string PathFor(string fileName) => Path.Combine(Root, fileName);

        public void Dispose()
        {
            if (Directory.Exists(Root))
                Directory.Delete(Root, recursive: true);
        }
    }
}
