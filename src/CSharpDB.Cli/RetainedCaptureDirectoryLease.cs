using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Security.AccessControl;
using System.Security.Principal;
using System.Text;
using Microsoft.Win32.SafeHandles;

namespace CSharpDB.Cli;

/// <summary>
/// Pins an existing caller-controlled directory while retained capture
/// artifacts are created and published beneath it.
/// </summary>
internal sealed class RetainedCaptureDirectoryLease : IDisposable
{
    private static readonly string[] LinuxAclAttributeNames =
    [
        "system.posix_acl_access",
        "system.posix_acl_default",
        "system.nfs4_acl",
        "system.richacl",
    ];

    private readonly object gate = new();
    private readonly string path;
    private readonly List<SafeFileHandle> handles;
    private readonly WindowsDirectoryIdentity[]? windowsIdentities;
    private readonly UnixDirectoryIdentity[]? unixIdentities;
    private readonly SecurityIdentifier? windowsOwner;
    private readonly uint unixEffectiveUserId;
    private bool disposed;

    private RetainedCaptureDirectoryLease(
        string path,
        List<SafeFileHandle> handles,
        WindowsDirectoryIdentity[] windowsIdentities,
        SecurityIdentifier windowsOwner)
    {
        this.path = path;
        this.handles = handles;
        this.windowsIdentities = windowsIdentities;
        this.windowsOwner = windowsOwner;
    }

    private RetainedCaptureDirectoryLease(
        string path,
        List<SafeFileHandle> handles,
        UnixDirectoryIdentity[] unixIdentities,
        uint unixEffectiveUserId)
    {
        this.path = path;
        this.handles = handles;
        this.unixIdentities = unixIdentities;
        this.unixEffectiveUserId = unixEffectiveUserId;
    }

    internal static RetainedCaptureDirectoryLease Open(string path)
    {
        string normalizedPath = RequireNormalizedPath(path);
        if (OperatingSystem.IsWindows())
        {
            SecurityIdentifier owner = ReadCurrentWindowsSid();
            WindowsOpenedChain chain =
                OpenWindowsChain(normalizedPath, owner);
            return new RetainedCaptureDirectoryLease(
                normalizedPath,
                chain.Handles,
                chain.Identities,
                owner);
        }

        if (!OperatingSystem.IsLinux() &&
            !OperatingSystem.IsMacOS())
        {
            throw new PlatformNotSupportedException(
                "Retained capture directory leases are supported only on Windows, Linux, and macOS.");
        }

        uint effectiveUserId = UnixGetEffectiveUserId();
        UnixOpenedChain unixChain =
            OpenUnixChain(normalizedPath, effectiveUserId);
        return new RetainedCaptureDirectoryLease(
            normalizedPath,
            unixChain.Handles,
            unixChain.Identities,
            effectiveUserId);
    }

    internal void AssertUnchanged()
    {
        lock (gate)
        {
            ObjectDisposedException.ThrowIf(disposed, this);
            if (OperatingSystem.IsWindows())
            {
                AssertWindowsUnchanged();
                return;
            }

            AssertUnixUnchanged();
        }
    }

    public void Dispose()
    {
        lock (gate)
        {
            if (disposed)
                return;

            disposed = true;
            DisposeHandles(handles);
        }
    }

    private static string RequireNormalizedPath(string candidate)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(candidate);
        if (candidate.IndexOf('\0') >= 0)
        {
            throw new InvalidDataException(
                "The retained capture directory path contains a null character.");
        }

        string fullPath;
        try
        {
            fullPath = Path.TrimEndingDirectorySeparator(
                Path.GetFullPath(candidate));
        }
        catch (Exception exception) when (
            exception is ArgumentException or
                NotSupportedException or
                PathTooLongException)
        {
            throw new InvalidDataException(
                "The retained capture directory path is invalid.",
                exception);
        }

        if (!Path.IsPathFullyQualified(candidate) ||
            !string.Equals(candidate, fullPath, StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                "The retained capture directory path must be an exact normalized absolute path.");
        }

        string root = Path.TrimEndingDirectorySeparator(
            Path.GetPathRoot(fullPath) ??
            throw new InvalidDataException(
                "The retained capture directory path has no filesystem root."));
        StringComparison comparison = OperatingSystem.IsWindows()
            ? StringComparison.OrdinalIgnoreCase
            : StringComparison.Ordinal;
        if (string.Equals(fullPath, root, comparison))
        {
            throw new InvalidDataException(
                "A filesystem root cannot be used as a retained capture directory.");
        }

        if (OperatingSystem.IsWindows())
            ValidateWindowsPathSyntax(fullPath);

        if (!Directory.Exists(fullPath))
        {
            if (File.Exists(fullPath))
            {
                throw new InvalidDataException(
                    "The retained capture directory path names a file.");
            }
            throw new DirectoryNotFoundException(
                "The retained capture directory does not exist.");
        }

        return fullPath;
    }

    [SupportedOSPlatform("windows")]
    private static void ValidateWindowsPathSyntax(string path)
    {
        if (path.StartsWith(@"\\", StringComparison.Ordinal) ||
            path.StartsWith(@"\\?\", StringComparison.Ordinal) ||
            path.StartsWith(@"\\.\", StringComparison.Ordinal) ||
            path.StartsWith(@"\??\", StringComparison.Ordinal) ||
            path.Length < 4 ||
            !char.IsAsciiLetter(path[0]) ||
            path[1] != ':' ||
            path[2] != Path.DirectorySeparatorChar ||
            path.IndexOf(':', 2) >= 0)
        {
            throw new InvalidDataException(
                "The retained capture directory must use a local drive path without a device namespace or alternate data stream.");
        }

        string root = Path.GetPathRoot(path)!;
        string relative = Path.GetRelativePath(root, path);
        foreach (string segment in relative.Split(
                     [Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar],
                     StringSplitOptions.RemoveEmptyEntries))
        {
            if (segment is "." or ".." ||
                segment.EndsWith(' ') ||
                segment.EndsWith('.') ||
                IsReservedWindowsName(segment))
            {
                throw new InvalidDataException(
                    "The retained capture directory contains an unsafe Windows path component.");
            }
        }
    }

    [SupportedOSPlatform("windows")]
    private static bool IsReservedWindowsName(string segment)
    {
        string stem = segment.Split('.')[0];
        return stem.Equals("CON", StringComparison.OrdinalIgnoreCase) ||
            stem.Equals("PRN", StringComparison.OrdinalIgnoreCase) ||
            stem.Equals("AUX", StringComparison.OrdinalIgnoreCase) ||
            stem.Equals("NUL", StringComparison.OrdinalIgnoreCase) ||
            stem.Equals("CONIN$", StringComparison.OrdinalIgnoreCase) ||
            stem.Equals("CONOUT$", StringComparison.OrdinalIgnoreCase) ||
            stem.Equals("CLOCK$", StringComparison.OrdinalIgnoreCase) ||
            IsNumberedWindowsDevice(stem, "COM") ||
            IsNumberedWindowsDevice(stem, "LPT");
    }

    [SupportedOSPlatform("windows")]
    private static bool IsNumberedWindowsDevice(
        string stem,
        string prefix) =>
        stem.Length == 4 &&
        stem.StartsWith(prefix, StringComparison.OrdinalIgnoreCase) &&
        stem[3] is >= '1' and <= '9';

    [SupportedOSPlatform("windows")]
    private static SecurityIdentifier ReadCurrentWindowsSid()
    {
        using WindowsIdentity identity =
            WindowsIdentity.GetCurrent(TokenAccessLevels.Query);
        return identity.User ??
            throw new IOException(
                "The current Windows identity does not have a security identifier.");
    }

    [SupportedOSPlatform("windows")]
    private static WindowsOpenedChain OpenWindowsChain(
        string path,
        SecurityIdentifier expectedOwner)
    {
        string root = Path.GetPathRoot(path) ??
            throw new InvalidDataException(
                "The retained capture directory has no Windows drive root.");
        var drive = new DriveInfo(root);
        if (drive.DriveType != DriveType.Fixed)
        {
            throw new InvalidDataException(
                "Retained capture requires a fixed local Windows filesystem.");
        }

        string[] segments = Path.GetRelativePath(root, path).Split(
            [Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar],
            StringSplitOptions.RemoveEmptyEntries);
        var openedHandles = new List<SafeFileHandle>(segments.Length);
        var openedIdentities =
            new List<WindowsDirectoryIdentity>(segments.Length);
        string current = root;
        try
        {
            for (int index = 0; index < segments.Length; index++)
            {
                current = Path.Combine(current, segments[index]);
                SafeFileHandle handle = OpenWindowsDirectory(
                    current,
                    requireReadControl: index == segments.Length - 1);
                openedHandles.Add(handle);
                openedIdentities.Add(ReadWindowsDirectoryIdentity(handle));
                RequireCanonicalWindowsPath(current, handle);
            }

            ValidateWindowsDirectorySecurity(path, expectedOwner);
            return new WindowsOpenedChain(
                openedHandles,
                openedIdentities.ToArray());
        }
        catch
        {
            DisposeHandles(openedHandles);
            throw;
        }
    }

    [SupportedOSPlatform("windows")]
    private static SafeFileHandle OpenWindowsDirectory(
        string path,
        bool requireReadControl)
    {
        uint desiredAccess = FileReadAttributes;
        if (requireReadControl)
            desiredAccess |= ReadControl;
        SafeFileHandle handle = CreateFileW(
            path,
            desiredAccess,
            FileShareRead | FileShareWrite,
            IntPtr.Zero,
            OpenExisting,
            FileFlagOpenReparsePoint | FileFlagBackupSemantics,
            IntPtr.Zero);
        if (handle.IsInvalid)
        {
            int error = Marshal.GetLastPInvokeError();
            handle.Dispose();
            throw CreateWindowsOpenException(error);
        }

        try
        {
            FileAttributes attributes = File.GetAttributes(handle);
            if ((attributes & FileAttributes.Directory) == 0 ||
                (attributes &
                    (FileAttributes.ReparsePoint |
                     FileAttributes.Device)) != 0)
            {
                throw new InvalidDataException(
                    "The retained capture path contains a link, mount point, device, or non-directory.");
            }
            return handle;
        }
        catch
        {
            handle.Dispose();
            throw;
        }
    }

    [SupportedOSPlatform("windows")]
    private static Exception CreateWindowsOpenException(int error)
    {
        var native = new Win32Exception(error);
        return error switch
        {
            ErrorAccessDenied => new UnauthorizedAccessException(
                "Access to the retained capture directory was denied.",
                native),
            ErrorFileNotFound or ErrorPathNotFound =>
                new DirectoryNotFoundException(
                    "The retained capture directory path changed or disappeared.",
                    native),
            _ => new IOException(
                "The retained capture directory could not be opened safely.",
                native),
        };
    }

    [SupportedOSPlatform("windows")]
    private static WindowsDirectoryIdentity
        ReadWindowsDirectoryIdentity(SafeFileHandle handle)
    {
        if (!GetFileInformationByHandleEx(
                handle,
                FileIdInfoClass,
                out WindowsFileIdInformation information,
                checked((uint)Marshal.SizeOf<WindowsFileIdInformation>())))
        {
            throw new IOException(
                "The retained capture directory Windows identity could not be read.",
                new Win32Exception(Marshal.GetLastPInvokeError()));
        }

        return new WindowsDirectoryIdentity(
            information.VolumeSerialNumber,
            information.FileIdLow,
            information.FileIdHigh);
    }

    [SupportedOSPlatform("windows")]
    private static void RequireCanonicalWindowsPath(
        string expectedPath,
        SafeFileHandle handle)
    {
        string actualPath = ReadFinalWindowsPath(handle);
        if (!string.Equals(
                Path.TrimEndingDirectorySeparator(actualPath),
                Path.TrimEndingDirectorySeparator(expectedPath),
                StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidDataException(
                "The retained capture path did not resolve to its exact local directory.");
        }
    }

    [SupportedOSPlatform("windows")]
    private static string ReadFinalWindowsPath(SafeFileHandle handle)
    {
        var buffer = new StringBuilder(512);
        uint length = GetFinalPathNameByHandleW(
            handle,
            buffer,
            checked((uint)buffer.Capacity),
            0);
        if (length >= buffer.Capacity)
        {
            buffer.EnsureCapacity(checked((int)length + 1));
            length = GetFinalPathNameByHandleW(
                handle,
                buffer,
                checked((uint)buffer.Capacity),
                0);
        }
        if (length == 0 ||
            length >= buffer.Capacity)
        {
            throw new IOException(
                "The retained capture directory final Windows path could not be read.",
                new Win32Exception(Marshal.GetLastPInvokeError()));
        }

        string path = buffer.ToString();
        if (path.StartsWith(
                @"\\?\UNC\",
                StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidDataException(
                "Retained capture does not support a network filesystem.");
        }
        if (path.StartsWith(
                @"\\?\",
                StringComparison.Ordinal))
        {
            path = path[4..];
        }
        return path;
    }

    [SupportedOSPlatform("windows")]
    private static void ValidateWindowsDirectorySecurity(
        string path,
        SecurityIdentifier expectedOwner)
    {
        DirectorySecurity security;
        try
        {
            security = FileSystemAclExtensions.GetAccessControl(
                new DirectoryInfo(path),
                AccessControlSections.Owner |
                    AccessControlSections.Access);
        }
        catch (UnauthorizedAccessException)
        {
            throw;
        }
        catch (Exception exception) when (
            exception is IOException or
                InvalidOperationException or
                SystemException)
        {
            throw new IOException(
                "The retained capture directory Windows owner and access policy could not be read.",
                exception);
        }

        if (security.GetOwner(
                typeof(SecurityIdentifier)) is not
                SecurityIdentifier actualOwner ||
            !expectedOwner.Equals(actualOwner))
        {
            throw new InvalidDataException(
                "The retained capture directory must be owned by the current Windows identity.");
        }

        byte[] descriptorBytes =
            security.GetSecurityDescriptorBinaryForm();
        var descriptor =
            new RawSecurityDescriptor(descriptorBytes, 0);
        RawAcl? acl = descriptor.DiscretionaryAcl;
        if (acl is null)
        {
            throw new InvalidDataException(
                "The retained capture directory has a null Windows DACL.");
        }

        SecurityIdentifier system =
            new(WellKnownSidType.LocalSystemSid, domainSid: null);
        SecurityIdentifier administrators =
            new(WellKnownSidType.BuiltinAdministratorsSid, domainSid: null);
        foreach (GenericAce ace in acl)
        {
            if (ace is not QualifiedAce qualified)
            {
                if (IsWindowsAllowAce(ace.AceType))
                {
                    throw new InvalidDataException(
                        "The retained capture directory contains an unrecognized Windows allow rule.");
                }
                continue;
            }
            if (qualified.AceQualifier != AceQualifier.AccessAllowed)
                continue;

            SecurityIdentifier sid =
                qualified.SecurityIdentifier ??
                throw new InvalidDataException(
                    "The retained capture directory contains an allow rule without a SID.");
            bool trusted = expectedOwner.Equals(sid) ||
                system.Equals(sid) ||
                administrators.Equals(sid);
            if (!trusted &&
                (qualified.AccessMask &
                    DangerousWindowsDirectoryAccessMask) != 0)
            {
                throw new InvalidDataException(
                    "The retained capture directory grants create, write, modify, delete, or ownership access to an untrusted Windows SID.");
            }
        }
    }

    [SupportedOSPlatform("windows")]
    private static bool IsWindowsAllowAce(AceType type) =>
        type is AceType.AccessAllowed or
            AceType.AccessAllowedObject or
            AceType.AccessAllowedCallback or
            AceType.AccessAllowedCallbackObject or
            AceType.AccessAllowedCompound;

    [SupportedOSPlatform("windows")]
    private void AssertWindowsUnchanged()
    {
        SecurityIdentifier expectedOwner = windowsOwner ??
            throw new InvalidOperationException(
                "The Windows retained capture lease is incomplete.");
        if (!expectedOwner.Equals(ReadCurrentWindowsSid()))
        {
            throw new IOException(
                "The effective Windows identity changed while the retained capture directory was leased.");
        }

        WindowsDirectoryIdentity[] expected =
            windowsIdentities ??
            throw new InvalidOperationException(
                "The Windows retained capture lease is incomplete.");
        AssertHeldWindowsIdentities(expected);
        using WindowsOpenedChain current =
            OpenWindowsChain(path, expectedOwner);
        if (current.Identities.Length != expected.Length)
        {
            throw new IOException(
                "The retained capture directory path depth changed while leased.");
        }
        for (int index = 0; index < expected.Length; index++)
        {
            if (current.Identities[index] != expected[index])
            {
                throw new IOException(
                    "The retained capture directory or one of its ancestors changed identity while leased.");
            }
        }
    }

    [SupportedOSPlatform("windows")]
    private void AssertHeldWindowsIdentities(
        IReadOnlyList<WindowsDirectoryIdentity> expected)
    {
        if (handles.Count != expected.Count)
        {
            throw new IOException(
                "The retained capture directory lease lost an ancestor handle.");
        }
        for (int index = 0; index < handles.Count; index++)
        {
            FileAttributes attributes =
                File.GetAttributes(handles[index]);
            if ((attributes & FileAttributes.Directory) == 0 ||
                (attributes &
                    (FileAttributes.ReparsePoint |
                     FileAttributes.Device)) != 0 ||
                ReadWindowsDirectoryIdentity(handles[index]) !=
                    expected[index])
            {
                throw new IOException(
                    "A held retained capture directory identity changed.");
            }
        }
    }

    private static UnixOpenedChain OpenUnixChain(
        string path,
        uint expectedUserId)
    {
        string root = Path.GetPathRoot(path) ??
            throw new InvalidDataException(
                "The retained capture directory has no Unix root.");
        var openedHandles = new List<SafeFileHandle>();
        var openedIdentities =
            new List<UnixDirectoryIdentity>();
        try
        {
            SafeFileHandle rootHandle =
                OpenUnixDirectory(root, rootHandle: null);
            openedHandles.Add(rootHandle);
            UnixDirectoryIdentity rootIdentity =
                ReadUnixDirectoryIdentity(rootHandle);
            ValidateUnixDirectoryType(rootIdentity);
            openedIdentities.Add(rootIdentity);

            string relative = Path.GetRelativePath(root, path);
            foreach (string segment in relative.Split(
                         Path.DirectorySeparatorChar,
                         StringSplitOptions.RemoveEmptyEntries))
            {
                SafeFileHandle child =
                    OpenUnixDirectory(segment, openedHandles[^1]);
                openedHandles.Add(child);
                UnixDirectoryIdentity childIdentity =
                    ReadUnixDirectoryIdentity(child);
                ValidateUnixDirectoryType(childIdentity);
                openedIdentities.Add(childIdentity);
            }

            ValidateUnixChain(
                openedHandles,
                openedIdentities,
                expectedUserId);
            return new UnixOpenedChain(
                openedHandles,
                openedIdentities.ToArray());
        }
        catch
        {
            DisposeHandles(openedHandles);
            throw;
        }
    }

    private static SafeFileHandle OpenUnixDirectory(
        string pathOrSegment,
        SafeFileHandle? rootHandle)
    {
        int descriptor = rootHandle is null
            ? UnixOpen(pathOrSegment, GetUnixDirectoryFlags())
            : UnixOpenAt(
                Descriptor(rootHandle),
                pathOrSegment,
                GetUnixDirectoryFlags());
        if (descriptor == -1)
        {
            int error = Marshal.GetLastPInvokeError();
            throw CreateUnixOpenException(error);
        }
        return WrapUnixDescriptor(descriptor);
    }

    private static Exception CreateUnixOpenException(int error)
    {
        var native = new Win32Exception(error);
        return error switch
        {
            UnixPermissionDenied or UnixOperationNotPermitted =>
                new UnauthorizedAccessException(
                    "Access to the retained capture directory was denied.",
                    native),
            UnixNoEntry => new DirectoryNotFoundException(
                "The retained capture directory path changed or disappeared.",
                native),
            UnixNotDirectory or LinuxTooManyLinks or
                DarwinTooManyLinks => new InvalidDataException(
                    "The retained capture path contains a link or non-directory.",
                    native),
            _ => new IOException(
                "The retained capture directory could not be opened safely.",
                native),
        };
    }

    private static void ValidateUnixDirectoryType(
        UnixDirectoryIdentity identity)
    {
        if ((identity.Mode & UnixFileTypeMask) !=
            UnixDirectoryFileType)
        {
            throw new InvalidDataException(
                "The retained capture path contains a non-directory.");
        }
    }

    private static void ValidateUnixChain(
        IReadOnlyList<SafeFileHandle> chainHandles,
        IReadOnlyList<UnixDirectoryIdentity> identities,
        uint expectedUserId)
    {
        if (chainHandles.Count != identities.Count ||
            identities.Count < 2)
        {
            throw new IOException(
                "The retained capture Unix directory chain is incomplete.");
        }

        for (int index = 0;
             index < identities.Count - 1;
             index++)
        {
            UnixDirectoryIdentity parent = identities[index];
            UnixDirectoryIdentity child = identities[index + 1];
            if (parent.UserId != expectedUserId &&
                parent.UserId != UnixRootUserId)
            {
                throw new InvalidDataException(
                    "A retained capture directory ancestor is not owned by the current Unix user or root.");
            }
            if ((parent.Mode & UnixGroupOtherWriteMask) != 0 &&
                ((parent.UserId != expectedUserId &&
                  parent.UserId != UnixRootUserId) ||
                 (parent.Mode & UnixStickyBit) == 0 ||
                 child.UserId != expectedUserId))
            {
                throw new InvalidDataException(
                    "A retained capture directory ancestor can be renamed by another Unix user.");
            }
        }

        UnixDirectoryIdentity leaf = identities[^1];
        if (leaf.UserId != expectedUserId ||
            (leaf.Mode & UnixGroupOtherWriteMask) != 0)
        {
            throw new InvalidDataException(
                "The retained capture directory must be current-user-owned and not group- or other-writable.");
        }

        for (int index = 0;
             index < chainHandles.Count;
             index++)
        {
            RequireNoUnixExtendedAcl(chainHandles[index]);
        }
    }

    private static UnixDirectoryIdentity ReadUnixDirectoryIdentity(
        SafeFileHandle handle)
    {
        if (SystemNativeFStat(
                handle,
                out UnixFileStatus status) != 0)
        {
            throw new IOException(
                "The retained capture directory Unix identity could not be read.",
                new Win32Exception(Marshal.GetLastPInvokeError()));
        }

        ulong mountId;
        if (OperatingSystem.IsLinux())
        {
            mountId = ReadLinuxMountIdentity(handle);
        }
        else if (OperatingSystem.IsMacOS())
        {
            mountId = ReadDarwinMountIdentity(handle);
        }
        else
        {
            throw new PlatformNotSupportedException(
                "Retained capture Unix identity checks are unsupported on this platform.");
        }
        return new UnixDirectoryIdentity(
            status.Device,
            status.Inode,
            status.Mode,
            status.Uid,
            mountId);
    }

    [SupportedOSPlatform("linux")]
    private static ulong ReadLinuxMountIdentity(
        SafeFileHandle handle)
    {
        if (LinuxFStatFs(
                Descriptor(handle),
                out LinuxFileSystemStatus fileSystem) != 0)
        {
            throw new IOException(
                "The retained capture directory Linux filesystem type could not be read.",
                new Win32Exception(Marshal.GetLastPInvokeError()));
        }
        ulong fileSystemType =
            unchecked((uint)fileSystem.Type);
        if (!IsSupportedLocalLinuxFileSystem(fileSystemType))
        {
            throw new InvalidDataException(
                "Retained capture requires a reviewed local Linux filesystem.");
        }

        if (LinuxStatx(
                Descriptor(handle),
                string.Empty,
                LinuxAtEmptyPath,
                LinuxStatxMountId,
                out LinuxStatxBuffer status) != 0)
        {
            throw new IOException(
                "The retained capture directory Linux mount identity could not be read.",
                new Win32Exception(Marshal.GetLastPInvokeError()));
        }
        if ((status.Mask & LinuxStatxMountId) == 0)
        {
            throw new IOException(
                "The retained capture directory Linux mount identity is incomplete.");
        }
        return status.MountId;
    }

    [SupportedOSPlatform("linux")]
    private static bool IsSupportedLocalLinuxFileSystem(
        ulong fileSystemType) =>
        fileSystemType is
            0x0000EF53UL or // ext2/ext3/ext4
            0x58465342UL or // XFS
            0x9123683EUL or // Btrfs
            0x01021994UL or // tmpfs
            0x858458F6UL or // ramfs
            0x794C7630UL or // OverlayFS
            0x2FC12FC1UL or // ZFS
            0xF2F52010UL or // F2FS
            0x3153464AUL or // JFS
            0x52654973UL or // ReiserFS
            0x00003434UL or // NILFS
            0x0000F15FUL or // eCryptfs
            0xCA451A4EUL or // bcachefs
            0x24051905UL;   // UBIFS

    [SupportedOSPlatform("macos")]
    private static ulong ReadDarwinMountIdentity(
        SafeFileHandle handle)
    {
        if (DarwinFStatFs(
                Descriptor(handle),
                out DarwinFileSystemStatus status) != 0)
        {
            throw new IOException(
                "The retained capture directory macOS filesystem identity could not be read.",
                new Win32Exception(Marshal.GetLastPInvokeError()));
        }
        if ((status.Flags & DarwinMountLocal) == 0 ||
            (status.Flags &
                DarwinMountIgnoreOwnership) != 0)
        {
            throw new InvalidDataException(
                "Retained capture requires a local macOS filesystem with enforced ownership.");
        }
        return ((ulong)status.FileSystemIdHigh << 32) |
            status.FileSystemIdLow;
    }

    private static void RequireNoUnixExtendedAcl(
        SafeFileHandle handle)
    {
        int descriptor = Descriptor(handle);
        if (OperatingSystem.IsLinux())
        {
            foreach (string name in LinuxAclAttributeNames)
            {
                nint length = LinuxGetExtendedAttribute(
                    descriptor,
                    name,
                    IntPtr.Zero,
                    UIntPtr.Zero);
                if (length >= 0)
                {
                    throw new InvalidDataException(
                        "The retained capture directory contains an extended Unix access policy.");
                }
                int error = Marshal.GetLastPInvokeError();
                if (error != LinuxNoExtendedAttribute &&
                    error != LinuxOperationNotSupported)
                {
                    throw new InvalidDataException(
                        "The retained capture directory extended Unix access policy cannot be proven absent.");
                }
            }
            return;
        }

        if (OperatingSystem.IsMacOS())
        {
            IntPtr acl = DarwinGetAcl(
                descriptor,
                DarwinAclTypeExtended);
            if (acl == IntPtr.Zero)
            {
                int error = Marshal.GetLastPInvokeError();
                if (error == DarwinNoEntry)
                {
                    return;
                }
                throw new InvalidDataException(
                    "The retained capture directory extended macOS access policy cannot be proven absent.");
            }
            try
            {
                int result = DarwinGetAclEntry(
                    acl,
                    DarwinAclFirstEntry,
                    out _);
                if (result == 0)
                {
                    throw new InvalidDataException(
                        "The retained capture directory contains an extended macOS access policy.");
                }
                throw new InvalidDataException(
                    "The retained capture directory extended macOS access policy cannot be verified.");
            }
            finally
            {
                _ = DarwinFreeAcl(acl);
            }
        }

        throw new PlatformNotSupportedException(
            "Retained capture Unix ACL verification is unsupported on this platform.");
    }

    private void AssertUnixUnchanged()
    {
        if (UnixGetEffectiveUserId() !=
            unixEffectiveUserId)
        {
            throw new IOException(
                "The effective Unix identity changed while the retained capture directory was leased.");
        }

        UnixDirectoryIdentity[] expected =
            unixIdentities ??
            throw new InvalidOperationException(
                "The Unix retained capture lease is incomplete.");
        AssertHeldUnixIdentities(expected);
        using UnixOpenedChain current =
            OpenUnixChain(path, unixEffectiveUserId);
        if (current.Identities.Length != expected.Length)
        {
            throw new IOException(
                "The retained capture directory path depth changed while leased.");
        }
        for (int index = 0; index < expected.Length; index++)
        {
            if (current.Identities[index].Device !=
                    expected[index].Device ||
                current.Identities[index].Inode !=
                    expected[index].Inode ||
                current.Identities[index].MountId !=
                    expected[index].MountId)
            {
                throw new IOException(
                    "The retained capture directory or one of its ancestors changed identity while leased.");
            }
        }
    }

    private void AssertHeldUnixIdentities(
        IReadOnlyList<UnixDirectoryIdentity> expected)
    {
        if (handles.Count != expected.Count)
        {
            throw new IOException(
                "The retained capture directory lease lost a Unix ancestor handle.");
        }

        var current = new UnixDirectoryIdentity[handles.Count];
        for (int index = 0; index < handles.Count; index++)
        {
            current[index] =
                ReadUnixDirectoryIdentity(handles[index]);
            ValidateUnixDirectoryType(current[index]);
            if (current[index].Device !=
                    expected[index].Device ||
                current[index].Inode !=
                    expected[index].Inode ||
                current[index].MountId !=
                    expected[index].MountId)
            {
                throw new IOException(
                    "A held retained capture Unix directory identity changed.");
            }
        }
        ValidateUnixChain(
            handles,
            current,
            unixEffectiveUserId);
    }

    private static int GetUnixDirectoryFlags()
    {
        if (OperatingSystem.IsLinux())
        {
            return LinuxDirectory |
                LinuxNoFollow |
                LinuxCloseOnExec;
        }
        if (OperatingSystem.IsMacOS())
        {
            return DarwinDirectory |
                DarwinNoFollow |
                DarwinCloseOnExec;
        }
        throw new PlatformNotSupportedException();
    }

    private static SafeFileHandle WrapUnixDescriptor(
        int descriptor)
    {
        if (descriptor == 0)
        {
            int duplicate = UnixDuplicate(descriptor);
            int error = Marshal.GetLastPInvokeError();
            _ = UnixClose(descriptor);
            if (duplicate == -1)
            {
                throw new IOException(
                    "A retained capture Unix directory descriptor could not be preserved.",
                    new Win32Exception(error));
            }
            descriptor = duplicate;
            if (UnixFcntl(
                    descriptor,
                    UnixSetFileDescriptor,
                    UnixCloseOnExec) == -1)
            {
                error = Marshal.GetLastPInvokeError();
                _ = UnixClose(descriptor);
                throw new IOException(
                    "A retained capture Unix directory descriptor could not be secured.",
                    new Win32Exception(error));
            }
        }
        return new SafeFileHandle(
            new IntPtr(descriptor),
            ownsHandle: true);
    }

    private static int Descriptor(SafeFileHandle handle) =>
        checked((int)handle.DangerousGetHandle());

    private static void DisposeHandles(
        IReadOnlyList<SafeFileHandle> handles)
    {
        for (int index = handles.Count - 1;
             index >= 0;
             index--)
        {
            handles[index].Dispose();
        }
    }

    private sealed record WindowsOpenedChain(
        List<SafeFileHandle> Handles,
        WindowsDirectoryIdentity[] Identities)
        : IDisposable
    {
        public void Dispose() =>
            DisposeHandles(Handles);
    }

    private sealed record UnixOpenedChain(
        List<SafeFileHandle> Handles,
        UnixDirectoryIdentity[] Identities)
        : IDisposable
    {
        public void Dispose() =>
            DisposeHandles(Handles);
    }

    private readonly record struct WindowsDirectoryIdentity(
        ulong VolumeSerialNumber,
        ulong FileIdLow,
        ulong FileIdHigh);

    private readonly record struct UnixDirectoryIdentity(
        long Device,
        long Inode,
        int Mode,
        uint UserId,
        ulong MountId);

    private const int DangerousWindowsDirectoryAccessMask =
        0x00000002 | // create files / write data
        0x00000004 | // create directories / append data
        0x00000010 | // write extended attributes
        0x00000040 | // delete children
        0x00000100 | // write attributes
        0x00010000 | // delete
        0x00040000 | // change permissions
        0x00080000 | // take ownership
        0x10000000 | // generic all
        0x40000000;  // generic write

    private const uint FileReadAttributes = 0x00000080;
    private const uint ReadControl = 0x00020000;
    private const uint FileShareRead = 0x00000001;
    private const uint FileShareWrite = 0x00000002;
    private const uint OpenExisting = 3;
    private const uint FileFlagOpenReparsePoint = 0x00200000;
    private const uint FileFlagBackupSemantics = 0x02000000;
    private const int FileIdInfoClass = 18;
    private const int ErrorFileNotFound = 2;
    private const int ErrorPathNotFound = 3;
    private const int ErrorAccessDenied = 5;

    private const int UnixOperationNotPermitted = 1;
    private const int UnixNoEntry = 2;
    private const int UnixPermissionDenied = 13;
    private const int UnixNotDirectory = 20;
    private const int LinuxTooManyLinks = 40;
    private const int DarwinTooManyLinks = 62;
    private const int LinuxDirectory = 0x00010000;
    private const int LinuxNoFollow = 0x00020000;
    private const int LinuxCloseOnExec = 0x00080000;
    private const int DarwinNoFollow = 0x00000100;
    private const int DarwinDirectory = 0x00100000;
    private const int DarwinCloseOnExec = 0x01000000;
    private const int UnixSetFileDescriptor = 2;
    private const int UnixCloseOnExec = 1;
    private const int UnixFileTypeMask = 0xF000;
    private const int UnixDirectoryFileType = 0x4000;
    private const int UnixGroupOtherWriteMask = 0x0012;
    private const int UnixStickyBit = 0x0200;
    private const uint UnixRootUserId = 0;
    private const int LinuxNoExtendedAttribute = 61;
    // Linux reports EOPNOTSUPP when the mounted filesystem does not implement
    // the queried ACL family. Its enforced uid/mode model remains authoritative.
    private const int LinuxOperationNotSupported = 95;
    private const int LinuxAtEmptyPath = 0x1000;
    private const uint LinuxStatxMountId = 0x1000;
    private const int DarwinNoEntry = 2;
    private const int DarwinAclTypeExtended = 0x100;
    private const int DarwinAclFirstEntry = 0;
    private const uint DarwinMountLocal = 0x00001000;
    private const uint DarwinMountIgnoreOwnership = 0x00200000;

    [StructLayout(LayoutKind.Sequential)]
    private struct WindowsFileIdInformation
    {
        internal ulong VolumeSerialNumber;
        internal ulong FileIdLow;
        internal ulong FileIdHigh;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct UnixFileStatus
    {
        internal int Flags;
        internal int Mode;
        internal uint Uid;
        internal uint Gid;
        internal long Size;
        internal long AccessTime;
        internal long AccessTimeNanoseconds;
        internal long ModificationTime;
        internal long ModificationTimeNanoseconds;
        internal long ChangeTime;
        internal long ChangeTimeNanoseconds;
        internal long BirthTime;
        internal long BirthTimeNanoseconds;
        internal long Device;
        internal long RawDevice;
        internal long Inode;
        internal uint UserFlags;
    }

    [StructLayout(LayoutKind.Explicit, Size = 256)]
    private struct LinuxFileSystemStatus
    {
        [FieldOffset(0)]
        internal long Type;
    }

    [StructLayout(LayoutKind.Explicit, Size = 256)]
    private struct LinuxStatxBuffer
    {
        [FieldOffset(0)]
        internal uint Mask;

        [FieldOffset(144)]
        internal ulong MountId;
    }

    [StructLayout(LayoutKind.Explicit, Size = 2168)]
    private struct DarwinFileSystemStatus
    {
        [FieldOffset(48)]
        internal uint FileSystemIdLow;

        [FieldOffset(52)]
        internal uint FileSystemIdHigh;

        [FieldOffset(64)]
        internal uint Flags;
    }

    [DllImport(
        "kernel32.dll",
        EntryPoint = "CreateFileW",
        CharSet = CharSet.Unicode,
        ExactSpelling = true,
        SetLastError = true)]
    private static extern SafeFileHandle CreateFileW(
        string fileName,
        uint desiredAccess,
        uint shareMode,
        IntPtr securityAttributes,
        uint creationDisposition,
        uint flagsAndAttributes,
        IntPtr templateFile);

    [DllImport(
        "kernel32.dll",
        EntryPoint = "GetFileInformationByHandleEx",
        ExactSpelling = true,
        SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool GetFileInformationByHandleEx(
        SafeFileHandle file,
        int fileInformationClass,
        out WindowsFileIdInformation fileInformation,
        uint bufferSize);

    [DllImport(
        "kernel32.dll",
        EntryPoint = "GetFinalPathNameByHandleW",
        CharSet = CharSet.Unicode,
        ExactSpelling = true,
        SetLastError = true)]
    private static extern uint GetFinalPathNameByHandleW(
        SafeFileHandle file,
        StringBuilder filePath,
        uint filePathLength,
        uint flags);

    [DllImport(
        "libc",
        EntryPoint = "open",
        SetLastError = true)]
    private static extern int UnixOpen(
        [MarshalAs(UnmanagedType.LPUTF8Str)]
        string path,
        int flags);

    [DllImport(
        "libc",
        EntryPoint = "openat",
        SetLastError = true)]
    private static extern int UnixOpenAt(
        int directoryDescriptor,
        [MarshalAs(UnmanagedType.LPUTF8Str)]
        string path,
        int flags);

    [DllImport(
        "libc",
        EntryPoint = "geteuid")]
    private static extern uint UnixGetEffectiveUserId();

    [DllImport(
        "libc",
        EntryPoint = "dup",
        SetLastError = true)]
    private static extern int UnixDuplicate(int descriptor);

    [DllImport(
        "libc",
        EntryPoint = "close",
        SetLastError = true)]
    private static extern int UnixClose(int descriptor);

    [DllImport(
        "libc",
        EntryPoint = "fcntl",
        SetLastError = true)]
    private static extern int UnixFcntl(
        int descriptor,
        int command,
        int argument);

    [DllImport(
        "libc",
        EntryPoint = "fstatfs",
        SetLastError = true)]
    private static extern int LinuxFStatFs(
        int descriptor,
        out LinuxFileSystemStatus status);

    [DllImport(
        "libc",
        EntryPoint = "statx",
        SetLastError = true)]
    private static extern int LinuxStatx(
        int directoryDescriptor,
        [MarshalAs(UnmanagedType.LPUTF8Str)]
        string path,
        int flags,
        uint mask,
        out LinuxStatxBuffer status);

    [DllImport(
        "libc",
        EntryPoint = "fgetxattr",
        SetLastError = true)]
    private static extern nint LinuxGetExtendedAttribute(
        int descriptor,
        [MarshalAs(UnmanagedType.LPUTF8Str)]
        string name,
        IntPtr value,
        UIntPtr size);

    [DllImport(
        "libc",
        EntryPoint = "fstatfs",
        SetLastError = true)]
    private static extern int DarwinFStatFs(
        int descriptor,
        out DarwinFileSystemStatus status);

    [DllImport(
        "libc",
        EntryPoint = "acl_get_fd_np",
        SetLastError = true)]
    private static extern IntPtr DarwinGetAcl(
        int descriptor,
        int type);

    [DllImport(
        "libc",
        EntryPoint = "acl_get_entry",
        SetLastError = true)]
    private static extern int DarwinGetAclEntry(
        IntPtr acl,
        int entryId,
        out IntPtr entry);

    [DllImport(
        "libc",
        EntryPoint = "acl_free",
        SetLastError = true)]
    private static extern int DarwinFreeAcl(IntPtr acl);

    [DllImport(
        "System.Native",
        EntryPoint = "SystemNative_FStat",
        SetLastError = true)]
    private static extern int SystemNativeFStat(
        SafeFileHandle descriptor,
        out UnixFileStatus status);
}
