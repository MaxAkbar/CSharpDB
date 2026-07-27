using System.Buffers.Binary;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Security.AccessControl;
using System.Security.Principal;

namespace CSharpDB.Cli.Tests;

public sealed class RetainedCaptureDirectoryLeaseTests
{
    [Fact]
    public void Open_PrivateDirectory_RemainsStableAndDisposesIdempotently()
    {
        using TestDirectory directory = TestDirectory.Create();
        RetainedCaptureDirectoryLease lease =
            RetainedCaptureDirectoryLease.Open(directory.Path);

        lease.AssertUnchanged();
        lease.Dispose();
        lease.Dispose();

        Assert.Throws<ObjectDisposedException>(
            lease.AssertUnchanged);
    }

    [Fact]
    public void Open_RejectsMissingFileRootAndNonNormalizedPaths()
    {
        using TestDirectory directory = TestDirectory.Create();
        string file = System.IO.Path.Combine(
            directory.Path,
            "not-a-directory.txt");
        File.WriteAllText(file, "not a directory");

        Assert.Throws<InvalidDataException>(
            () => RetainedCaptureDirectoryLease.Open(file));
        Assert.Throws<DirectoryNotFoundException>(
            () => RetainedCaptureDirectoryLease.Open(
                System.IO.Path.Combine(
                    directory.Path,
                    "missing")));
        Assert.Throws<InvalidDataException>(
            () => RetainedCaptureDirectoryLease.Open(
                directory.Path +
                System.IO.Path.DirectorySeparatorChar));
        Assert.Throws<InvalidDataException>(
            () => RetainedCaptureDirectoryLease.Open(
                System.IO.Path.Combine(
                    directory.Path,
                    ".")));
        Assert.Throws<InvalidDataException>(
            () => RetainedCaptureDirectoryLease.Open(
                System.IO.Path.GetPathRoot(directory.Path)!));
    }

    [Fact]
    public void Open_RejectsSymbolicLinkComponent()
    {
        using TestDirectory directory = TestDirectory.Create();
        string target = directory.CreateChild("target");
        string link = System.IO.Path.Combine(
            directory.Path,
            "link");
        try
        {
            Directory.CreateSymbolicLink(link, target);
        }
        catch (Exception exception) when (
            exception is UnauthorizedAccessException or
                IOException or
                PlatformNotSupportedException)
        {
            return;
        }

        try
        {
            Assert.Throws<InvalidDataException>(
                () => RetainedCaptureDirectoryLease.Open(link));
        }
        finally
        {
            Directory.Delete(link);
        }
    }

    [Fact]
    public void Lease_BlocksWindowsAncestorRenameUntilDisposed()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using TestDirectory directory = TestDirectory.Create();
        string ancestor = directory.CreateChild("ancestor");
        string leasedPath =
            TestDirectory.CreatePrivateChild(
                ancestor,
                "leased");
        string moved = System.IO.Path.Combine(
            directory.Path,
            "moved-ancestor");
        RetainedCaptureDirectoryLease lease =
            RetainedCaptureDirectoryLease.Open(leasedPath);
        try
        {
            Assert.ThrowsAny<IOException>(
                () => Directory.Move(ancestor, moved));
            lease.AssertUnchanged();
        }
        finally
        {
            lease.Dispose();
        }

        Directory.Move(ancestor, moved);
        Directory.Move(moved, ancestor);
    }

    [Fact]
    public void AssertUnchanged_RejectsUnixDirectoryReplacement()
    {
        if (OperatingSystem.IsWindows())
            return;

        using TestDirectory directory = TestDirectory.Create();
        string leasedPath = directory.CreateChild("leased");
        string moved = System.IO.Path.Combine(
            directory.Path,
            "moved");
        using RetainedCaptureDirectoryLease lease =
            RetainedCaptureDirectoryLease.Open(leasedPath);

        Directory.Move(leasedPath, moved);
        TestDirectory.CreatePrivateDirectory(leasedPath);

        Assert.Throws<IOException>(
            lease.AssertUnchanged);
    }

    [Fact]
    public void AssertUnchanged_RejectsPermissionRelaxation()
    {
        using TestDirectory directory = TestDirectory.Create();
        using RetainedCaptureDirectoryLease lease =
            RetainedCaptureDirectoryLease.Open(directory.Path);

        if (OperatingSystem.IsWindows())
        {
            AddWindowsAccess(
                directory.Path,
                new SecurityIdentifier(
                    WellKnownSidType.WorldSid,
                    domainSid: null),
                FileSystemRights.CreateFiles);
        }
        else
        {
            File.SetUnixFileMode(
                directory.Path,
                File.GetUnixFileMode(directory.Path) |
                    UnixFileMode.GroupWrite);
        }

        Assert.Throws<InvalidDataException>(
            lease.AssertUnchanged);
    }

    [Fact]
    public void Open_RejectsUntrustedWriteAuthority()
    {
        using TestDirectory directory = TestDirectory.Create();
        if (OperatingSystem.IsWindows())
        {
            AddWindowsAccess(
                directory.Path,
                new SecurityIdentifier(
                    WellKnownSidType.WorldSid,
                    domainSid: null),
                FileSystemRights.CreateDirectories);
        }
        else
        {
            File.SetUnixFileMode(
                directory.Path,
                File.GetUnixFileMode(directory.Path) |
                    UnixFileMode.OtherWrite);
        }

        Assert.Throws<InvalidDataException>(
            () => RetainedCaptureDirectoryLease.Open(
                directory.Path));
    }

    [Fact]
    public void Open_AllowsUntrustedReadOnlyWindowsRule()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using TestDirectory directory = TestDirectory.Create();
        AddWindowsAccess(
            directory.Path,
            new SecurityIdentifier(
                WellKnownSidType.WorldSid,
                domainSid: null),
            FileSystemRights.ReadAndExecute);

        using RetainedCaptureDirectoryLease lease =
            RetainedCaptureDirectoryLease.Open(directory.Path);
        lease.AssertUnchanged();
    }

    [Fact]
    public void Open_RejectsWritableUnixAncestorButAllowsStickyOwnedChild()
    {
        if (OperatingSystem.IsWindows())
            return;

        using TestDirectory directory = TestDirectory.Create();
        string writable = directory.CreateChild("writable");
        string rejected =
            TestDirectory.CreatePrivateChild(
                writable,
                "rejected");
        File.SetUnixFileMode(
            writable,
            UnixFileMode.UserRead |
                UnixFileMode.UserWrite |
                UnixFileMode.UserExecute |
                UnixFileMode.GroupRead |
                UnixFileMode.GroupWrite |
                UnixFileMode.GroupExecute);
        Assert.Throws<InvalidDataException>(
            () => RetainedCaptureDirectoryLease.Open(rejected));

        File.SetUnixFileMode(
            writable,
            UnixFileMode.UserRead |
                UnixFileMode.UserWrite |
                UnixFileMode.UserExecute |
                UnixFileMode.GroupRead |
                UnixFileMode.GroupWrite |
                UnixFileMode.GroupExecute |
                UnixFileMode.OtherRead |
                UnixFileMode.OtherWrite |
                UnixFileMode.OtherExecute |
                UnixFileMode.StickyBit);
        using RetainedCaptureDirectoryLease lease =
            RetainedCaptureDirectoryLease.Open(rejected);
        lease.AssertUnchanged();
    }

    [Fact]
    public void Open_RejectsLinuxExtendedAccessAcl()
    {
        if (!OperatingSystem.IsLinux())
            return;

        using TestDirectory directory = TestDirectory.Create();
        if (!TrySetLinuxReadOnlyExtendedAcl(directory.Path))
            return;

        Assert.Throws<InvalidDataException>(
            () => RetainedCaptureDirectoryLease.Open(
                directory.Path));
    }

    [Fact]
    public void CaptureWorkspace_DisposeDeletesOnlyKnownEntriesNonrecursively()
    {
        using TestDirectory directory = TestDirectory.Create();
        using RetainedCaptureDirectoryLease parentLease =
            RetainedCaptureDirectoryLease.Open(directory.Path);
        MigrationCommandRunner.SqlServerCaptureWorkspace
            workspace =
                MigrationCommandRunner
                    .SqlServerCaptureWorkspace
                    .Create(
                        directory.Path,
                        parentLease);
        string workspaceRoot =
            System.IO.Path.GetDirectoryName(
                workspace.CapturePath)!;
        File.WriteAllText(
            workspace.CapturePath,
            "retained package");

        workspace.AssertUnchanged();
        workspace.Dispose();

        Assert.False(Directory.Exists(workspaceRoot));
    }

    [Fact]
    public void CaptureWorkspace_UnexpectedEntryIsPreservedAndReported()
    {
        using TestDirectory directory = TestDirectory.Create();
        using RetainedCaptureDirectoryLease parentLease =
            RetainedCaptureDirectoryLease.Open(directory.Path);
        MigrationCommandRunner.SqlServerCaptureWorkspace
            workspace =
                MigrationCommandRunner
                    .SqlServerCaptureWorkspace
                    .Create(
                        directory.Path,
                        parentLease);
        string workspaceRoot =
            System.IO.Path.GetDirectoryName(
                workspace.CapturePath)!;
        string unexpected =
            System.IO.Path.Combine(
                workspaceRoot,
                "unexpected.txt");
        File.WriteAllText(unexpected, "preserve");

        try
        {
            Assert.ThrowsAny<IOException>(
                workspace.AssertUnchanged);
            Assert.ThrowsAny<IOException>(
                workspace.Dispose);
            Assert.True(File.Exists(unexpected));
            Assert.True(Directory.Exists(workspaceRoot));
        }
        finally
        {
            if (Directory.Exists(workspaceRoot))
            {
                Directory.Delete(
                    workspaceRoot,
                    recursive: true);
            }
        }
    }

    [SupportedOSPlatform("windows")]
    private static void AddWindowsAccess(
        string path,
        SecurityIdentifier sid,
        FileSystemRights rights)
    {
        DirectoryInfo directory = new(path);
        DirectorySecurity security =
            FileSystemAclExtensions.GetAccessControl(
                directory,
                AccessControlSections.Owner |
                    AccessControlSections.Access);
        security.AddAccessRule(
            new FileSystemAccessRule(
                sid,
                rights,
                InheritanceFlags.ContainerInherit |
                    InheritanceFlags.ObjectInherit,
                PropagationFlags.None,
                AccessControlType.Allow));
        FileSystemAclExtensions.SetAccessControl(
            directory,
            security);
    }

    private static bool TrySetLinuxReadOnlyExtendedAcl(
        string path)
    {
        uint effectiveUserId = UnixGetEffectiveUserId();
        uint unrelatedUserId =
            effectiveUserId == uint.MaxValue
                ? effectiveUserId - 1
                : effectiveUserId + 1;
        byte[] acl = new byte[4 + (5 * 8)];
        BinaryPrimitives.WriteUInt32LittleEndian(
            acl.AsSpan(0, 4),
            2);
        WriteLinuxAclEntry(
            acl.AsSpan(4, 8),
            tag: 0x01,
            permissions: 0x07,
            id: uint.MaxValue);
        WriteLinuxAclEntry(
            acl.AsSpan(12, 8),
            tag: 0x02,
            permissions: 0x04,
            id: unrelatedUserId);
        WriteLinuxAclEntry(
            acl.AsSpan(20, 8),
            tag: 0x04,
            permissions: 0x05,
            id: uint.MaxValue);
        WriteLinuxAclEntry(
            acl.AsSpan(28, 8),
            tag: 0x10,
            permissions: 0x05,
            id: uint.MaxValue);
        WriteLinuxAclEntry(
            acl.AsSpan(36, 8),
            tag: 0x20,
            permissions: 0x05,
            id: uint.MaxValue);
        if (LinuxSetExtendedAttribute(
                path,
                "system.posix_acl_access",
                acl,
                checked((UIntPtr)acl.Length),
                0) == 0)
        {
            return true;
        }

        int error = Marshal.GetLastPInvokeError();
        if (error is 45 or 95)
            return false;
        throw new Win32Exception(
            error,
            "The Linux extended ACL test fixture could not be created.");
    }

    private static void WriteLinuxAclEntry(
        Span<byte> destination,
        ushort tag,
        ushort permissions,
        uint id)
    {
        BinaryPrimitives.WriteUInt16LittleEndian(
            destination[..2],
            tag);
        BinaryPrimitives.WriteUInt16LittleEndian(
            destination.Slice(2, 2),
            permissions);
        BinaryPrimitives.WriteUInt32LittleEndian(
            destination.Slice(4, 4),
            id);
    }

    private sealed class TestDirectory : IDisposable
    {
        private TestDirectory(string path)
        {
            Path = path;
        }

        internal string Path { get; }

        internal static TestDirectory Create()
        {
            string path = System.IO.Path.Combine(
                System.IO.Path.GetTempPath(),
                "csharpdb-directory-lease-" +
                    Guid.NewGuid().ToString("N"));
            CreatePrivateDirectory(path);
            return new TestDirectory(path);
        }

        internal string CreateChild(string name) =>
            CreatePrivateChild(Path, name);

        internal static string CreatePrivateChild(
            string parent,
            string name)
        {
            string path = System.IO.Path.Combine(
                parent,
                name);
            CreatePrivateDirectory(path);
            return path;
        }

        internal static void CreatePrivateDirectory(
            string path)
        {
            Directory.CreateDirectory(path);
            if (OperatingSystem.IsWindows())
            {
                using WindowsIdentity identity =
                    WindowsIdentity.GetCurrent(
                        TokenAccessLevels.Query);
                SecurityIdentifier owner =
                    identity.User ??
                    throw new IOException(
                        "The Windows test identity has no SID.");
                var security = new DirectorySecurity();
                security.SetOwner(owner);
                security.SetAccessRuleProtection(
                    isProtected: true,
                    preserveInheritance: false);
                AddTrustedWindowsFullControl(
                    security,
                    owner);
                AddTrustedWindowsFullControl(
                    security,
                    new SecurityIdentifier(
                        WellKnownSidType.LocalSystemSid,
                        domainSid: null));
                AddTrustedWindowsFullControl(
                    security,
                    new SecurityIdentifier(
                        WellKnownSidType.BuiltinAdministratorsSid,
                        domainSid: null));
                FileSystemAclExtensions.SetAccessControl(
                    new DirectoryInfo(path),
                    security);
                return;
            }

            File.SetUnixFileMode(
                path,
                UnixFileMode.UserRead |
                    UnixFileMode.UserWrite |
                    UnixFileMode.UserExecute);
        }

        [SupportedOSPlatform("windows")]
        private static void AddTrustedWindowsFullControl(
            DirectorySecurity security,
            SecurityIdentifier sid)
        {
            security.AddAccessRule(
                new FileSystemAccessRule(
                    sid,
                    FileSystemRights.FullControl,
                    InheritanceFlags.ContainerInherit |
                        InheritanceFlags.ObjectInherit,
                    PropagationFlags.None,
                    AccessControlType.Allow));
        }

        public void Dispose()
        {
            if (!Directory.Exists(Path))
                return;
            Directory.Delete(
                Path,
                recursive: true);
        }
    }

    [DllImport(
        "libc",
        EntryPoint = "geteuid")]
    private static extern uint UnixGetEffectiveUserId();

    [DllImport(
        "libc",
        EntryPoint = "setxattr",
        SetLastError = true)]
    private static extern int LinuxSetExtendedAttribute(
        [MarshalAs(UnmanagedType.LPUTF8Str)]
        string path,
        [MarshalAs(UnmanagedType.LPUTF8Str)]
        string name,
        byte[] value,
        UIntPtr size,
        int flags);
}
