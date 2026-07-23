using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Security.AccessControl;
using System.Security.Cryptography;
using System.Security.Principal;
using System.Text;
using Microsoft.Win32.SafeHandles;

namespace CSharpDB.Migration;

/// <summary>
/// Owns one private, deterministic sibling temporary file. The open handle is
/// also the cross-process claim and survives until atomic no-replace publish.
/// </summary>
internal sealed class MigrationRejectArtifactPublication : IAsyncDisposable
{
    private const int BufferSize = 64 * 1024;
    private const int ErrorAlreadyExists = 183;
    private const int ErrorFileExists = 80;
    private const int UnixAlreadyExists = 17;
    private const int UnixWouldBlockLinux = 11;
    private const int UnixWouldBlockDarwin = 35;
    private const int LockExclusive = 2;
    private const int LockNonBlocking = 4;
    private const uint RenameNoReplace = 1;
    private const uint DarwinRenameExclusiveFlag = 4;

    private static readonly string[] LinuxAclAttributeNames =
    [
        "system.posix_acl_access",
        "system.nfs4_acl",
        "system.richacl",
    ];

    private readonly string _parentPath;
    private readonly string _destinationLeaf;
    private readonly string _temporaryLeaf;
    private readonly string _temporaryPath;
    private readonly long _maximumBytes;
    private readonly SafeFileHandle? _windowsParent;
    private readonly SafeFileHandle? _unixParent;
    private bool _published;
    private bool _temporaryRemoved;
    private bool _disposed;

    private MigrationRejectArtifactPublication(
        string destinationPath,
        string parentPath,
        string destinationLeaf,
        string temporaryLeaf,
        string temporaryPath,
        long maximumBytes,
        FileStream stream,
        SafeFileHandle? windowsParent,
        SafeFileHandle? unixParent)
    {
        DestinationPath = destinationPath;
        _parentPath = parentPath;
        _destinationLeaf = destinationLeaf;
        _temporaryLeaf = temporaryLeaf;
        _temporaryPath = temporaryPath;
        _maximumBytes = maximumBytes;
        Stream = stream;
        _windowsParent = windowsParent;
        _unixParent = unixParent;
    }

    internal string DestinationPath { get; }

    internal FileStream Stream { get; }

    internal static ValueTask<MigrationRejectArtifactPublication> OpenAsync(
        string outputPath,
        string planDigest,
        long maximumBytes,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        DestinationBinding binding = ValidateDestination(outputPath, planDigest);
        SafeFileHandle? windowsParent = null;
        SafeFileHandle? unixParent = null;
        try
        {
            FileStream stream;
            if (OperatingSystem.IsWindows())
            {
                windowsParent = OpenWindowsParent(binding.ParentPath);
                stream = OpenWindowsClaim(binding.TemporaryPath);
            }
            else
            {
                unixParent = OpenUnixParent(binding.ParentPath);
                stream = OpenUnixClaim(
                    unixParent,
                    binding.TemporaryLeaf);
            }

            try
            {
                if (OperatingSystem.IsWindows())
                {
                    RequireWindowsParentIdentity(
                        binding.ParentPath,
                        windowsParent ?? throw new InvalidOperationException());
                }
                stream.SetLength(0);
                stream.Position = 0;
                return ValueTask.FromResult(new MigrationRejectArtifactPublication(
                    binding.DestinationPath,
                    binding.ParentPath,
                    binding.DestinationLeaf,
                    binding.TemporaryLeaf,
                    binding.TemporaryPath,
                    maximumBytes,
                    stream,
                    windowsParent,
                    unixParent));
            }
            catch
            {
                stream.Dispose();
                throw;
            }
        }
        catch
        {
            windowsParent?.Dispose();
            unixParent?.Dispose();
            throw;
        }
    }

    internal async ValueTask FlushAsync(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        await Stream.FlushAsync(cancellationToken).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        Stream.Flush(flushToDisk: true);
    }

    internal async ValueTask<bool> PublishOrReuseAsync(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        PublishStatus status = OperatingSystem.IsWindows()
            ? PublishWindows()
            : PublishUnix();
        if (status == PublishStatus.Published)
        {
            _published = true;
            SyncParentAfterPublish();
            return false;
        }

        bool identical = await ExistingArtifactMatchesAsync(cancellationToken)
            .ConfigureAwait(false);
        if (!identical)
        {
            throw new IOException(
                "The reject artifact destination already contains a different file.");
        }

        RemoveOwnedTemporary();
        SyncParentAfterPublish();
        return true;
    }

    public ValueTask DisposeAsync()
    {
        if (_disposed)
            return ValueTask.CompletedTask;

        try
        {
            if (!_published && !_temporaryRemoved)
                RemoveOwnedTemporary();
        }
        finally
        {
            _disposed = true;
            try
            {
                Stream.Dispose();
            }
            finally
            {
                try
                {
                    _windowsParent?.Dispose();
                }
                finally
                {
                    _unixParent?.Dispose();
                }
            }
        }
        return ValueTask.CompletedTask;
    }

    [SupportedOSPlatform("windows")]
    private PublishStatus PublishWindows()
    {
        SafeFileHandle parent = _windowsParent ??
            throw new InvalidOperationException("The Windows artifact parent is unavailable.");
        RequireWindowsParentIdentity(_parentPath, parent);
        int nameOffset = IntPtr.Size == 8 ? 20 : 12;
        byte[] nameBytes = Encoding.Unicode.GetBytes(DestinationPath);
        int informationLength = checked(nameOffset + nameBytes.Length);
        int allocationLength = checked(informationLength + sizeof(char));
        IntPtr buffer = Marshal.AllocHGlobal(allocationLength);
        try
        {
            // FileNameLength excludes a terminator, but Windows still expects
            // addressable zero padding after the flexible UTF-16 array.
            Marshal.Copy(new byte[allocationLength], 0, buffer, allocationLength);
            int rootOffset = IntPtr.Size == 8 ? 8 : 4;
            int lengthOffset = IntPtr.Size == 8 ? 16 : 8;
            // Win32 SetFileInformationByHandle requires RootDirectory to be
            // null; the similarly shaped native NT structure has different
            // relative-root semantics. Parent identity is pinned and checked
            // immediately before this absolute, no-replace rename.
            Marshal.WriteIntPtr(buffer, rootOffset, IntPtr.Zero);
            Marshal.WriteInt32(buffer, lengthOffset, nameBytes.Length);
            Marshal.Copy(nameBytes, 0, IntPtr.Add(buffer, nameOffset), nameBytes.Length);

            if (SetFileInformationByHandle(
                    Stream.SafeFileHandle,
                    FileRenameInfo,
                    buffer,
                    checked((uint)informationLength)))
            {
                return PublishStatus.Published;
            }

            int error = Marshal.GetLastPInvokeError();
            if (error is ErrorAlreadyExists or ErrorFileExists)
                return PublishStatus.DestinationExists;
            throw new IOException(
                "The reject artifact could not be atomically published.",
                new Win32Exception(error));
        }
        finally
        {
            Marshal.FreeHGlobal(buffer);
        }
    }

    private PublishStatus PublishUnix()
    {
        SafeFileHandle parent = _unixParent ??
            throw new InvalidOperationException("The Unix artifact parent is unavailable.");
        RequireUnixTemporaryIdentity(parent, _temporaryLeaf, Stream.SafeFileHandle);
        int parentDescriptor = Descriptor(parent);
        int result;
        if (OperatingSystem.IsMacOS() ||
            OperatingSystem.IsIOS() ||
            OperatingSystem.IsTvOS() ||
            OperatingSystem.IsMacCatalyst())
        {
            result = DarwinRenameAtExclusive(
                parentDescriptor,
                _temporaryLeaf,
                parentDescriptor,
                _destinationLeaf,
                DarwinRenameExclusiveFlag);
        }
        else if (OperatingSystem.IsLinux() ||
                 OperatingSystem.IsAndroid() ||
                 OperatingSystem.IsFreeBSD())
        {
            result = UnixRenameAt2(
                parentDescriptor,
                _temporaryLeaf,
                parentDescriptor,
                _destinationLeaf,
                RenameNoReplace);
        }
        else
        {
            throw new PlatformNotSupportedException(
                "Atomic reject-artifact publication is not supported on this platform.");
        }

        if (result == 0)
            return PublishStatus.Published;
        int error = Marshal.GetLastPInvokeError();
        if (error == UnixAlreadyExists)
            return PublishStatus.DestinationExists;
        throw new IOException(
            "The reject artifact could not be atomically published.",
            new Win32Exception(error));
    }

    private async ValueTask<bool> ExistingArtifactMatchesAsync(
        CancellationToken cancellationToken)
    {
        if (OperatingSystem.IsWindows())
        {
            RequireWindowsParentIdentity(
                _parentPath,
                _windowsParent ?? throw new InvalidOperationException());
        }
        await using FileStream existing = OperatingSystem.IsWindows()
            ? OpenWindowsExistingArtifact(DestinationPath)
            : OpenUnixExistingArtifact(
                _unixParent ?? throw new InvalidOperationException(),
                _destinationLeaf);
        if (existing.Length > _maximumBytes || existing.Length != Stream.Length)
            return false;

        Stream.Position = 0;
        existing.Position = 0;
        byte[] expectedBuffer = new byte[BufferSize];
        byte[] actualBuffer = new byte[BufferSize];
        while (true)
        {
            int expectedRead = await Stream.ReadAsync(
                expectedBuffer,
                cancellationToken).ConfigureAwait(false);
            int actualRead = await existing.ReadAsync(
                actualBuffer.AsMemory(0, expectedRead),
                cancellationToken).ConfigureAwait(false);
            if (expectedRead != actualRead)
                return false;
            if (expectedRead == 0)
            {
                if (!OperatingSystem.IsWindows())
                {
                    RequireUnixNamedIdentity(
                        _unixParent ?? throw new InvalidOperationException(),
                        _destinationLeaf,
                        existing.SafeFileHandle);
                }
                return true;
            }
            if (!CryptographicOperations.FixedTimeEquals(
                    expectedBuffer.AsSpan(0, expectedRead),
                    actualBuffer.AsSpan(0, actualRead)))
            {
                return false;
            }
        }
    }

    private void RemoveOwnedTemporary()
    {
        if (_temporaryRemoved || _published)
            return;

        if (OperatingSystem.IsWindows())
        {
            IntPtr disposition = Marshal.AllocHGlobal(1);
            try
            {
                Marshal.WriteByte(disposition, 1);
                if (!SetFileInformationByHandle(
                        Stream.SafeFileHandle,
                        FileDispositionInfo,
                        disposition,
                        1))
                {
                    throw new IOException(
                        "The private reject-artifact temporary file could not be removed.",
                        new Win32Exception(Marshal.GetLastPInvokeError()));
                }
            }
            finally
            {
                Marshal.FreeHGlobal(disposition);
            }
        }
        else
        {
            SafeFileHandle parent = _unixParent ?? throw new InvalidOperationException();
            RequireUnixTemporaryIdentity(parent, _temporaryLeaf, Stream.SafeFileHandle);
            if (UnixUnlinkAt(Descriptor(parent), _temporaryLeaf, 0) != 0)
            {
                throw new IOException(
                    "The private reject-artifact temporary file could not be removed.",
                    new Win32Exception(Marshal.GetLastPInvokeError()));
            }
            _temporaryRemoved = true;
            if (UnixFileSync(Descriptor(parent)) != 0)
            {
                throw new IOException(
                    "The reject artifact parent directory could not be synchronized after cleanup.",
                    new Win32Exception(Marshal.GetLastPInvokeError()));
            }
        }
        _temporaryRemoved = true;
    }

    private void SyncParentAfterPublish()
    {
        if (OperatingSystem.IsWindows())
            return;
        SafeFileHandle parent = _unixParent ?? throw new InvalidOperationException();
        if (UnixFileSync(Descriptor(parent)) != 0)
        {
            throw new IOException(
                "The reject artifact parent directory could not be durably synchronized.",
                new Win32Exception(Marshal.GetLastPInvokeError()));
        }
    }

    private static DestinationBinding ValidateDestination(
        string outputPath,
        string planDigest)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(outputPath);
        if (outputPath.Contains('\0'))
            throw new ArgumentException("The reject artifact path cannot contain a null character.", nameof(outputPath));
        RejectInvalidUnicode(outputPath);
        if (!Path.IsPathFullyQualified(outputPath))
        {
            throw new ArgumentException(
                "The reject artifact path must be fully qualified.",
                nameof(outputPath));
        }
        RejectDotSegments(outputPath);
        RejectWindowsSpecialPath(outputPath);

        string destinationPath = Path.GetFullPath(outputPath);
        StringComparison comparison = OperatingSystem.IsWindows()
            ? StringComparison.OrdinalIgnoreCase
            : StringComparison.Ordinal;
        if (!string.Equals(destinationPath, outputPath, comparison))
        {
            throw new ArgumentException(
                "The reject artifact path must be normalized and cannot contain traversal.",
                nameof(outputPath));
        }

        string parentPath = Path.GetDirectoryName(destinationPath) ??
            throw new ArgumentException("The reject artifact path must have a parent directory.", nameof(outputPath));
        string destinationLeaf = Path.GetFileName(destinationPath);
        if (string.IsNullOrWhiteSpace(destinationLeaf) ||
            destinationLeaf is "." or "..")
        {
            throw new ArgumentException("The reject artifact file name is invalid.", nameof(outputPath));
        }
        ValidateDirectoryChain(parentPath);

        if (TryGetAttributes(destinationPath, out FileAttributes destinationAttributes) &&
            (destinationAttributes & UnsafeFileAttributes) != 0)
        {
            throw new InvalidDataException(
                "The reject artifact destination cannot be a link, directory, device, or special file.");
        }

        string claimBinding = planDigest + "\0" + destinationLeaf;
        string claimDigest = Convert.ToHexString(
                SHA256.HashData(Encoding.UTF8.GetBytes(claimBinding)))
            .ToLowerInvariant();
        string temporaryLeaf = $".csharpdb-reject-{claimDigest[..32]}.tmp";
        string temporaryPath = Path.Combine(parentPath, temporaryLeaf);
        return new DestinationBinding(
            destinationPath,
            parentPath,
            destinationLeaf,
            temporaryLeaf,
            temporaryPath);
    }

    private static void RejectDotSegments(string path)
    {
        string root = Path.GetPathRoot(path) ?? string.Empty;
        foreach (string segment in path[root.Length..].Split(
                     [Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar],
                     StringSplitOptions.None))
        {
            if (segment is "." or "..")
            {
                throw new ArgumentException(
                    "The reject artifact path cannot contain traversal segments.",
                    nameof(path));
            }
        }
    }

    private static void RejectInvalidUnicode(string path)
    {
        for (int index = 0; index < path.Length; index++)
        {
            char value = path[index];
            if (!char.IsSurrogate(value))
                continue;
            if (char.IsHighSurrogate(value) &&
                index + 1 < path.Length &&
                char.IsLowSurrogate(path[index + 1]))
            {
                index++;
                continue;
            }

            throw new ArgumentException(
                "The reject artifact path must contain valid Unicode scalar data.",
                nameof(path));
        }
    }

    private static void RejectWindowsSpecialPath(string path)
    {
        if (!OperatingSystem.IsWindows())
            return;
        if (path.StartsWith("\\\\?\\", StringComparison.Ordinal) ||
            path.StartsWith("\\\\.\\", StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "Windows device and extended paths cannot be used for reject artifacts.",
                nameof(path));
        }

        string root = Path.GetPathRoot(path) ?? string.Empty;
        if (path.AsSpan(root.Length).Contains(':'))
        {
            throw new ArgumentException(
                "Windows alternate data streams cannot be used for reject artifacts.",
                nameof(path));
        }

        foreach (string segment in path[root.Length..].Split(
                     [Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar],
                     StringSplitOptions.RemoveEmptyEntries))
        {
            if (segment.EndsWith(' ') || segment.EndsWith('.'))
            {
                throw new ArgumentException(
                    "Windows reject-artifact path segments cannot end in spaces or dots.",
                    nameof(path));
            }
        }

        string leaf = Path.GetFileName(path);
        int firstDot = leaf.IndexOf('.');
        string stem = (firstDot < 0 ? leaf : leaf[..firstDot]).TrimEnd(' ', '.');
        if (stem.Equals("CON", StringComparison.OrdinalIgnoreCase) ||
            stem.Equals("PRN", StringComparison.OrdinalIgnoreCase) ||
            stem.Equals("AUX", StringComparison.OrdinalIgnoreCase) ||
            stem.Equals("NUL", StringComparison.OrdinalIgnoreCase) ||
            (stem.Length == 4 &&
             (stem.StartsWith("COM", StringComparison.OrdinalIgnoreCase) ||
              stem.StartsWith("LPT", StringComparison.OrdinalIgnoreCase)) &&
             (stem[3] is >= '1' and <= '9' or '\u00b9' or '\u00b2' or '\u00b3')))
        {
            throw new ArgumentException(
                "Windows reserved device names cannot be used for reject artifacts.",
                nameof(path));
        }
    }

    private static void ValidateDirectoryChain(string parentPath)
    {
        if (!Directory.Exists(parentPath))
            throw new DirectoryNotFoundException("The reject artifact parent directory does not exist.");

        string root = Path.GetPathRoot(parentPath) ??
            throw new InvalidDataException("The reject artifact parent root is invalid.");
        string relative = Path.GetRelativePath(root, parentPath);
        string current = root;
        if (relative != ".")
        {
            foreach (string segment in relative.Split(
                         [Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar],
                         StringSplitOptions.RemoveEmptyEntries))
            {
                current = Path.Combine(current, segment);
                FileAttributes attributes = File.GetAttributes(current);
                if ((attributes & FileAttributes.Directory) == 0 ||
                    (attributes & (FileAttributes.ReparsePoint | FileAttributes.Device)) != 0)
                {
                    throw new InvalidDataException(
                        "The reject artifact parent path cannot traverse a link, device, or non-directory.");
                }
            }
        }
    }

    [SupportedOSPlatform("windows")]
    private static SafeFileHandle OpenWindowsParent(string parentPath)
    {
        SafeFileHandle handle = CreateFileW(
            parentPath,
            GenericRead | ReadControl,
            FileShareRead | FileShareWrite | FileShareDelete,
            IntPtr.Zero,
            OpenExisting,
            FileFlagOpenReparsePoint | FileFlagBackupSemantics,
            IntPtr.Zero);
        if (handle.IsInvalid)
        {
            int error = Marshal.GetLastPInvokeError();
            handle.Dispose();
            throw new IOException(
                "The reject artifact parent directory cannot be opened safely.",
                new Win32Exception(error));
        }

        try
        {
            FileAttributes attributes = File.GetAttributes(handle);
            if ((attributes & FileAttributes.Directory) == 0 ||
                (attributes & (FileAttributes.ReparsePoint | FileAttributes.Device)) != 0)
            {
                throw new InvalidDataException(
                    "The reject artifact parent must be a real directory rather than a link or device.");
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
    private static void RequireWindowsParentIdentity(
        string parentPath,
        SafeFileHandle expected)
    {
        using SafeFileHandle actual = OpenWindowsParent(parentPath);
        if (!GetFileInformationByHandle(expected, out WindowsFileInformation expectedInfo) ||
            !GetFileInformationByHandle(actual, out WindowsFileInformation actualInfo) ||
            expectedInfo.VolumeSerialNumber != actualInfo.VolumeSerialNumber ||
            expectedInfo.FileIndexHigh != actualInfo.FileIndexHigh ||
            expectedInfo.FileIndexLow != actualInfo.FileIndexLow)
        {
            throw new IOException(
                "The reject artifact parent directory identity changed during publication.");
        }
    }

    [SupportedOSPlatform("windows")]
    private static FileStream OpenWindowsClaim(string temporaryPath)
    {
        try
        {
            FileSecurity security = CreatePrivateWindowsFileSecurity();
            FileStream created = FileSystemAclExtensions.Create(
                new FileInfo(temporaryPath),
                FileMode.CreateNew,
                FileSystemRights.FullControl,
                FileShare.None,
                BufferSize,
                FileOptions.Asynchronous | FileOptions.SequentialScan | FileOptions.WriteThrough,
                security);
            try
            {
                ValidateWindowsRegularPrivateFile(created);
                return created;
            }
            catch
            {
                created.Dispose();
                throw;
            }
        }
        catch (IOException) when (TryGetAttributes(temporaryPath, out _))
        {
            return OpenWindowsStaleClaim(temporaryPath);
        }
    }

    [SupportedOSPlatform("windows")]
    private static FileStream OpenWindowsStaleClaim(string temporaryPath)
    {
        SafeFileHandle handle = CreateFileW(
            temporaryPath,
            GenericRead | GenericWrite | DeleteAccess | ReadControl,
            0,
            IntPtr.Zero,
            OpenExisting,
            FileAttributeNormal |
                FileFlagOpenReparsePoint |
                FileFlagOverlapped |
                FileFlagSequentialScan |
                FileFlagWriteThrough,
            IntPtr.Zero);
        if (handle.IsInvalid)
        {
            int error = Marshal.GetLastPInvokeError();
            handle.Dispose();
            throw new IOException(
                "The private reject-artifact temporary file is unavailable or already in use.",
                new Win32Exception(error));
        }

        try
        {
            var stream = new FileStream(handle, FileAccess.ReadWrite, BufferSize, isAsync: true);
            handle = null!;
            try
            {
                ValidateWindowsRegularPrivateFile(stream);
                return stream;
            }
            catch
            {
                stream.Dispose();
                throw;
            }
        }
        finally
        {
            handle?.Dispose();
        }
    }

    [SupportedOSPlatform("windows")]
    private static FileStream OpenWindowsExistingArtifact(string destinationPath)
    {
        SafeFileHandle handle = CreateFileW(
            destinationPath,
            GenericRead | ReadControl,
            FileShareRead,
            IntPtr.Zero,
            OpenExisting,
            FileAttributeNormal |
                FileFlagOpenReparsePoint |
                FileFlagOverlapped |
                FileFlagSequentialScan,
            IntPtr.Zero);
        if (handle.IsInvalid)
        {
            int error = Marshal.GetLastPInvokeError();
            handle.Dispose();
            throw new IOException(
                "The existing reject artifact cannot be opened safely.",
                new Win32Exception(error));
        }

        try
        {
            var stream = new FileStream(handle, FileAccess.Read, BufferSize, isAsync: true);
            handle = null!;
            try
            {
                ValidateWindowsRegularPrivateFile(stream);
                return stream;
            }
            catch
            {
                stream.Dispose();
                throw;
            }
        }
        finally
        {
            handle?.Dispose();
        }
    }

    [SupportedOSPlatform("windows")]
    private static FileSecurity CreatePrivateWindowsFileSecurity()
    {
        using WindowsIdentity identity = WindowsIdentity.GetCurrent(TokenAccessLevels.Query);
        SecurityIdentifier owner = identity.User ??
            throw new IOException("The current Windows identity has no security identifier.");
        var security = new FileSecurity();
        security.SetOwner(owner);
        security.SetAccessRuleProtection(isProtected: true, preserveInheritance: false);
        security.AddAccessRule(new FileSystemAccessRule(
            owner,
            FileSystemRights.FullControl,
            AccessControlType.Allow));
        return security;
    }

    [SupportedOSPlatform("windows")]
    private static void ValidateWindowsRegularPrivateFile(FileStream stream)
    {
        FileAttributes attributes = File.GetAttributes(stream.SafeFileHandle);
        if ((attributes & UnsafeFileAttributes) != 0)
            throw new InvalidDataException("The reject artifact path is not a regular file.");
        if (!GetFileInformationByHandle(
                stream.SafeFileHandle,
                out WindowsFileInformation fileInformation) ||
            fileInformation.NumberOfLinks != 1)
        {
            throw new InvalidDataException(
                "The reject artifact file must have exactly one filesystem link.");
        }

        FileSecurity security = FileSystemAclExtensions.GetAccessControl(stream);
        using WindowsIdentity identity = WindowsIdentity.GetCurrent(TokenAccessLevels.Query);
        SecurityIdentifier owner = identity.User ??
            throw new IOException("The current Windows identity has no security identifier.");
        if (!security.AreAccessRulesProtected ||
            security.GetOwner(typeof(SecurityIdentifier)) is not SecurityIdentifier actualOwner ||
            !owner.Equals(actualOwner))
        {
            throw new InvalidDataException(
                "The reject artifact file must be private to the current Windows identity.");
        }

        bool ownerHasFullControl = false;
        AuthorizationRuleCollection rules = security.GetAccessRules(
            includeExplicit: true,
            includeInherited: true,
            targetType: typeof(SecurityIdentifier));
        foreach (FileSystemAccessRule rule in rules)
        {
            if (rule.AccessControlType != AccessControlType.Allow)
                continue;
            if (rule.IdentityReference is not SecurityIdentifier sid || !owner.Equals(sid))
            {
                throw new InvalidDataException(
                    "The reject artifact file grants access beyond the current Windows identity.");
            }
            ownerHasFullControl |=
                (rule.FileSystemRights & FileSystemRights.FullControl) ==
                FileSystemRights.FullControl;
        }
        if (!ownerHasFullControl)
        {
            throw new InvalidDataException(
                "The current Windows identity lacks full control of the reject artifact file.");
        }
    }

    private static SafeFileHandle OpenUnixParent(string parentPath)
    {
        string root = Path.GetPathRoot(parentPath) ?? "/";
        int descriptor = UnixOpen(root, GetUnixParentFlags());
        if (descriptor == -1)
        {
            throw new IOException(
                "The reject artifact parent directory cannot be opened safely.",
                new Win32Exception(Marshal.GetLastPInvokeError()));
        }
        SafeFileHandle handle = WrapUnixDescriptor(descriptor);
        try
        {
            string relative = Path.GetRelativePath(root, parentPath);
            if (relative != ".")
            {
                foreach (string segment in relative.Split(
                             Path.DirectorySeparatorChar,
                             StringSplitOptions.RemoveEmptyEntries))
                {
                    int childDescriptor = UnixOpenAt(
                        Descriptor(handle),
                        segment,
                        GetUnixParentFlags(),
                        0);
                    if (childDescriptor == -1)
                    {
                        throw new IOException(
                            "The reject artifact parent path cannot be traversed safely.",
                            new Win32Exception(Marshal.GetLastPInvokeError()));
                    }
                    SafeFileHandle child = WrapUnixDescriptor(childDescriptor);
                    try
                    {
                        UnixFileStatus childStatus = ReadUnixStatus(child);
                        if ((childStatus.Mode & FileTypeMask) != DirectoryFileType)
                        {
                            throw new InvalidDataException(
                                "The reject artifact parent path contains a non-directory.");
                        }
                    }
                    catch
                    {
                        child.Dispose();
                        throw;
                    }
                    handle.Dispose();
                    handle = child;
                }
            }

            UnixFileStatus status = ReadUnixStatus(handle);
            if ((status.Mode & FileTypeMask) != DirectoryFileType)
                throw new InvalidDataException("The reject artifact parent is not a directory.");
            return handle;
        }
        catch
        {
            handle.Dispose();
            throw;
        }
    }

    private static FileStream OpenUnixClaim(
        SafeFileHandle parent,
        string temporaryLeaf)
    {
        int parentDescriptor = Descriptor(parent);
        int descriptor = UnixOpenAt(
            parentDescriptor,
            temporaryLeaf,
            GetUnixClaimFlags(create: true),
            UnixPrivateFileMode);
        if (descriptor == -1)
        {
            int createError = Marshal.GetLastPInvokeError();
            if (createError != UnixAlreadyExists)
            {
                throw new IOException(
                    "The private reject-artifact temporary file cannot be created.",
                    new Win32Exception(createError));
            }
            descriptor = UnixOpenAt(
                parentDescriptor,
                temporaryLeaf,
                GetUnixClaimFlags(create: false),
                0);
            if (descriptor == -1)
            {
                throw new IOException(
                    "The private reject-artifact temporary file cannot be reopened safely.",
                    new Win32Exception(Marshal.GetLastPInvokeError()));
            }
        }

        SafeFileHandle handle = WrapUnixDescriptor(descriptor);
        try
        {
            int lockResult = UnixFlock(
                Descriptor(handle),
                LockExclusive | LockNonBlocking);
            if (lockResult != 0)
            {
                int lockError = Marshal.GetLastPInvokeError();
                string message = lockError is UnixWouldBlockLinux or UnixWouldBlockDarwin
                    ? "Reject artifact publication is already in progress."
                    : "The reject-artifact temporary claim could not be locked.";
                throw new IOException(message, new Win32Exception(lockError));
            }

            ValidateUnixRegularPrivateFile(handle, validateExtendedAcl: false);
            RequireUnixTemporaryIdentity(parent, temporaryLeaf, handle);
            RemoveUnixExtendedAcl(handle);
            if (UnixChangeMode(Descriptor(handle), UnixPrivateFileMode) != 0)
            {
                throw new IOException(
                    "The reject-artifact temporary file permissions could not be secured.",
                    new Win32Exception(Marshal.GetLastPInvokeError()));
            }
            ValidateUnixRegularPrivateFile(handle, validateExtendedAcl: true);
            if (SystemNativeFcntlSetIsNonBlocking(handle, isNonBlocking: 0) != 0)
            {
                throw new IOException(
                    "The reject-artifact temporary file could not be prepared for streaming.",
                    new Win32Exception(Marshal.GetLastPInvokeError()));
            }

            var stream = new FileStream(handle, FileAccess.ReadWrite, BufferSize, isAsync: true);
            handle = null!;
            return stream;
        }
        finally
        {
            handle?.Dispose();
        }
    }

    private static FileStream OpenUnixExistingArtifact(
        SafeFileHandle parent,
        string destinationLeaf)
    {
        int descriptor = UnixOpenAt(
            Descriptor(parent),
            destinationLeaf,
            GetUnixReadFlags(),
            0);
        if (descriptor == -1)
        {
            throw new IOException(
                "The existing reject artifact cannot be opened safely.",
                new Win32Exception(Marshal.GetLastPInvokeError()));
        }
        SafeFileHandle handle = WrapUnixDescriptor(descriptor);
        try
        {
            ValidateUnixRegularPrivateFile(handle, validateExtendedAcl: true);
            if (SystemNativeFcntlSetIsNonBlocking(handle, isNonBlocking: 0) != 0)
            {
                throw new IOException(
                    "The existing reject artifact could not be prepared for reading.",
                    new Win32Exception(Marshal.GetLastPInvokeError()));
            }
            var stream = new FileStream(handle, FileAccess.Read, BufferSize, isAsync: true);
            handle = null!;
            return stream;
        }
        finally
        {
            handle?.Dispose();
        }
    }

    private static void ValidateUnixRegularPrivateFile(
        SafeFileHandle handle,
        bool validateExtendedAcl)
    {
        UnixFileStatus status = ReadUnixStatus(handle);
        if ((status.Mode & FileTypeMask) != RegularFileType ||
            status.Uid != UnixEffectiveUserId() ||
            (status.Mode & GroupAndOtherPermissionMask) != 0 ||
            status.HardLinkCount != 1)
        {
            throw new InvalidDataException(
                "The reject artifact file must be a private, current-user-owned regular file with one link.");
        }
        if (validateExtendedAcl)
            RequireNoUnixExtendedAcl(handle);
    }

    private static void RemoveUnixExtendedAcl(SafeFileHandle handle)
    {
        int descriptor = Descriptor(handle);
        if (OperatingSystem.IsLinux() || OperatingSystem.IsAndroid())
        {
            foreach (string name in LinuxAclAttributeNames)
            {
                if (UnixRemoveExtendedAttribute(descriptor, name) == 0)
                    continue;
                int error = Marshal.GetLastPInvokeError();
                if (error != LinuxNoExtendedAttribute)
                {
                    throw new InvalidDataException(
                        "The reject artifact file extended access policy could not be removed.");
                }
            }
            return;
        }

        if (OperatingSystem.IsMacOS() || OperatingSystem.IsIOS() ||
            OperatingSystem.IsTvOS() || OperatingSystem.IsMacCatalyst())
        {
            if (DarwinDeleteAcl(descriptor, DarwinAclTypeExtended) == 0)
                return;
            int error = Marshal.GetLastPInvokeError();
            if (error is DarwinNoEntry or DarwinInvalidArgument)
                return;
            throw new InvalidDataException(
                "The reject artifact file extended access policy could not be removed.");
        }

        throw new PlatformNotSupportedException(
            "Private reject-artifact ACL enforcement is not supported on this platform.");
    }

    private static void RequireNoUnixExtendedAcl(SafeFileHandle handle)
    {
        int descriptor = Descriptor(handle);
        if (OperatingSystem.IsLinux() || OperatingSystem.IsAndroid())
        {
            foreach (string name in LinuxAclAttributeNames)
            {
                nint length = UnixGetExtendedAttribute(
                    descriptor,
                    name,
                    IntPtr.Zero,
                    UIntPtr.Zero);
                if (length >= 0)
                {
                    throw new InvalidDataException(
                        "The reject artifact file contains an extended access policy.");
                }
                if (Marshal.GetLastPInvokeError() != LinuxNoExtendedAttribute)
                {
                    throw new InvalidDataException(
                        "The reject artifact file extended access policy cannot be verified.");
                }
            }
            return;
        }

        if (OperatingSystem.IsMacOS() || OperatingSystem.IsIOS() ||
            OperatingSystem.IsTvOS() || OperatingSystem.IsMacCatalyst())
        {
            IntPtr acl = DarwinGetAcl(descriptor, DarwinAclTypeExtended);
            if (acl == IntPtr.Zero)
            {
                int error = Marshal.GetLastPInvokeError();
                if (error is DarwinNoEntry or DarwinInvalidArgument)
                    return;
                throw new InvalidDataException(
                    "The reject artifact file extended access policy cannot be verified.");
            }
            try
            {
                int result = DarwinGetAclEntry(acl, DarwinAclFirstEntry, out _);
                if (result == 1)
                {
                    throw new InvalidDataException(
                        "The reject artifact file contains an extended access policy.");
                }
                if (result != 0)
                {
                    throw new InvalidDataException(
                        "The reject artifact file extended access policy cannot be verified.");
                }
                return;
            }
            finally
            {
                _ = DarwinFreeAcl(acl);
            }
        }

        throw new PlatformNotSupportedException(
            "Private reject-artifact ACL verification is not supported on this platform.");
    }

    private static void RequireUnixTemporaryIdentity(
        SafeFileHandle parent,
        string temporaryLeaf,
        SafeFileHandle expected)
    {
        int descriptor = UnixOpenAt(
            Descriptor(parent),
            temporaryLeaf,
            GetUnixReadFlags(),
            0);
        if (descriptor == -1)
        {
            throw new IOException(
                "The reject-artifact temporary claim no longer names the owned file.",
                new Win32Exception(Marshal.GetLastPInvokeError()));
        }
        using SafeFileHandle actual = WrapUnixDescriptor(descriptor);
        UnixFileStatus expectedStatus = ReadUnixStatus(expected);
        UnixFileStatus actualStatus = ReadUnixStatus(actual);
        if (expectedStatus.Device != actualStatus.Device ||
            expectedStatus.Inode != actualStatus.Inode)
        {
            throw new IOException(
                "The reject-artifact temporary claim identity changed during publication.");
        }
    }

    private static void RequireUnixNamedIdentity(
        SafeFileHandle parent,
        string leaf,
        SafeFileHandle expected)
    {
        int descriptor = UnixOpenAt(
            Descriptor(parent),
            leaf,
            GetUnixReadFlags(),
            0);
        if (descriptor == -1)
        {
            throw new IOException(
                "The existing reject artifact identity changed during comparison.",
                new Win32Exception(Marshal.GetLastPInvokeError()));
        }
        using SafeFileHandle actual = WrapUnixDescriptor(descriptor);
        UnixFileStatus expectedStatus = ReadUnixStatus(expected);
        UnixFileStatus actualStatus = ReadUnixStatus(actual);
        if (expectedStatus.Device != actualStatus.Device ||
            expectedStatus.Inode != actualStatus.Inode)
        {
            throw new IOException(
                "The existing reject artifact identity changed during comparison.");
        }
    }

    private static UnixFileStatus ReadUnixStatus(SafeFileHandle handle)
    {
        if (SystemNativeFStat(handle, out UnixFileStatus status) != 0)
        {
            throw new IOException(
                "The reject artifact file metadata cannot be read safely.",
                new Win32Exception(Marshal.GetLastPInvokeError()));
        }
        return status;
    }

    private static int GetUnixParentFlags()
    {
        if (OperatingSystem.IsLinux() || OperatingSystem.IsAndroid())
            return LinuxDirectory | LinuxNoFollow | LinuxCloseOnExec;
        if (OperatingSystem.IsMacOS() || OperatingSystem.IsIOS() ||
            OperatingSystem.IsTvOS() || OperatingSystem.IsMacCatalyst())
        {
            return DarwinDirectory | DarwinNoFollow | DarwinCloseOnExec;
        }
        if (OperatingSystem.IsFreeBSD())
            return FreeBsdDirectory | FreeBsdNoFollow | FreeBsdCloseOnExec;
        throw new PlatformNotSupportedException();
    }

    private static int GetUnixClaimFlags(bool create)
    {
        int flags;
        if (OperatingSystem.IsLinux() || OperatingSystem.IsAndroid())
        {
            flags = UnixReadWrite | LinuxNonBlock | LinuxNoFollow | LinuxCloseOnExec;
            if (create)
                flags |= LinuxCreate | LinuxExclusive;
            return flags;
        }
        if (OperatingSystem.IsMacOS() || OperatingSystem.IsIOS() ||
            OperatingSystem.IsTvOS() || OperatingSystem.IsMacCatalyst())
        {
            flags = UnixReadWrite | DarwinNonBlock | DarwinNoFollow | DarwinCloseOnExec;
            if (create)
                flags |= DarwinCreate | DarwinExclusive;
            return flags;
        }
        if (OperatingSystem.IsFreeBSD())
        {
            flags = UnixReadWrite | FreeBsdNonBlock | FreeBsdNoFollow | FreeBsdCloseOnExec;
            if (create)
                flags |= FreeBsdCreate | FreeBsdExclusive;
            return flags;
        }
        throw new PlatformNotSupportedException();
    }

    private static int GetUnixReadFlags()
    {
        if (OperatingSystem.IsLinux() || OperatingSystem.IsAndroid())
            return LinuxNonBlock | LinuxNoFollow | LinuxCloseOnExec;
        if (OperatingSystem.IsMacOS() || OperatingSystem.IsIOS() ||
            OperatingSystem.IsTvOS() || OperatingSystem.IsMacCatalyst())
        {
            return DarwinNonBlock | DarwinNoFollow | DarwinCloseOnExec;
        }
        if (OperatingSystem.IsFreeBSD())
            return FreeBsdNonBlock | FreeBsdNoFollow | FreeBsdCloseOnExec;
        throw new PlatformNotSupportedException();
    }

    private static SafeFileHandle WrapUnixDescriptor(int descriptor)
    {
        if (descriptor == 0)
        {
            int duplicate = UnixDuplicate(descriptor);
            int error = Marshal.GetLastPInvokeError();
            _ = UnixClose(descriptor);
            if (duplicate == -1)
                throw new IOException("A Unix file descriptor could not be preserved.", new Win32Exception(error));
            descriptor = duplicate;
            if (UnixFcntl(descriptor, FSetFileDescriptor, CloseOnExec) == -1)
            {
                error = Marshal.GetLastPInvokeError();
                _ = UnixClose(descriptor);
                throw new IOException("A Unix file descriptor could not be secured.", new Win32Exception(error));
            }
        }
        return new SafeFileHandle(new IntPtr(descriptor), ownsHandle: true);
    }

    private static int Descriptor(SafeFileHandle handle) =>
        checked((int)handle.DangerousGetHandle());

    private static bool TryGetAttributes(string path, out FileAttributes attributes)
    {
        try
        {
            attributes = File.GetAttributes(path);
            return true;
        }
        catch (FileNotFoundException)
        {
            attributes = default;
            return false;
        }
        catch (DirectoryNotFoundException)
        {
            attributes = default;
            return false;
        }
    }

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, this);

    private enum PublishStatus
    {
        Published,
        DestinationExists,
    }

    private sealed record DestinationBinding(
        string DestinationPath,
        string ParentPath,
        string DestinationLeaf,
        string TemporaryLeaf,
        string TemporaryPath);

    private const FileAttributes UnsafeFileAttributes =
        FileAttributes.Directory | FileAttributes.ReparsePoint | FileAttributes.Device;

    private const int FileRenameInfo = 3;
    private const int FileDispositionInfo = 4;
    private const uint GenericRead = 0x80000000;
    private const uint GenericWrite = 0x40000000;
    private const uint DeleteAccess = 0x00010000;
    private const uint ReadControl = 0x00020000;
    private const uint FileShareRead = 0x00000001;
    private const uint FileShareWrite = 0x00000002;
    private const uint FileShareDelete = 0x00000004;
    private const uint OpenExisting = 3;
    private const uint FileAttributeNormal = 0x00000080;
    private const uint FileFlagOpenReparsePoint = 0x00200000;
    private const uint FileFlagBackupSemantics = 0x02000000;
    private const uint FileFlagSequentialScan = 0x08000000;
    private const uint FileFlagWriteThrough = 0x80000000;
    private const uint FileFlagOverlapped = 0x40000000;

    private const int UnixReadWrite = 2;
    private const int LinuxCreate = 0x00000040;
    private const int LinuxExclusive = 0x00000080;
    private const int LinuxNonBlock = 0x00000800;
    private const int LinuxDirectory = 0x00010000;
    private const int LinuxNoFollow = 0x00020000;
    private const int LinuxCloseOnExec = 0x00080000;
    private const int DarwinNonBlock = 0x00000004;
    private const int DarwinCreate = 0x00000200;
    private const int DarwinExclusive = 0x00000800;
    private const int DarwinDirectory = 0x00100000;
    private const int DarwinNoFollow = 0x00000100;
    private const int DarwinCloseOnExec = 0x01000000;
    private const int FreeBsdNonBlock = 0x00000004;
    private const int FreeBsdCreate = 0x00000200;
    private const int FreeBsdExclusive = 0x00000800;
    private const int FreeBsdDirectory = 0x00020000;
    private const int FreeBsdNoFollow = 0x00000100;
    private const int FreeBsdCloseOnExec = 0x00100000;
    private const int FSetFileDescriptor = 2;
    private const int CloseOnExec = 1;
    private const int FileTypeMask = 0xF000;
    private const int RegularFileType = 0x8000;
    private const int DirectoryFileType = 0x4000;
    private const int GroupAndOtherPermissionMask = 0x3F;
    private const uint UnixPrivateFileMode = 0x180;
    private const int LinuxNoExtendedAttribute = 61;
    private const int DarwinNoEntry = 2;
    private const int DarwinInvalidArgument = 22;
    private const int DarwinAclTypeExtended = 0x100;
    private const int DarwinAclFirstEntry = 0;

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
        internal uint HardLinkCount;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct WindowsFileInformation
    {
        internal uint FileAttributes;
        internal System.Runtime.InteropServices.ComTypes.FILETIME CreationTime;
        internal System.Runtime.InteropServices.ComTypes.FILETIME LastAccessTime;
        internal System.Runtime.InteropServices.ComTypes.FILETIME LastWriteTime;
        internal uint VolumeSerialNumber;
        internal uint FileSizeHigh;
        internal uint FileSizeLow;
        internal uint NumberOfLinks;
        internal uint FileIndexHigh;
        internal uint FileIndexLow;
    }

    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool SetFileInformationByHandle(
        SafeFileHandle file,
        int fileInformationClass,
        IntPtr fileInformation,
        uint bufferSize);

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

    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool GetFileInformationByHandle(
        SafeFileHandle file,
        out WindowsFileInformation fileInformation);

    [DllImport("libc", EntryPoint = "open", SetLastError = true)]
    private static extern int UnixOpen(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string path,
        int flags);

    [DllImport("libc", EntryPoint = "openat", SetLastError = true)]
    private static extern int UnixOpenAt(
        int directoryDescriptor,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string path,
        int flags,
        uint mode);

    [DllImport("libc", EntryPoint = "flock", SetLastError = true)]
    private static extern int UnixFlock(int descriptor, int operation);

    [DllImport("libc", EntryPoint = "fchmod", SetLastError = true)]
    private static extern int UnixChangeMode(int descriptor, uint mode);

    [DllImport("libc", EntryPoint = "renameat2", SetLastError = true)]
    private static extern int UnixRenameAt2(
        int oldDirectoryDescriptor,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string oldPath,
        int newDirectoryDescriptor,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string newPath,
        uint flags);

    [DllImport("libc", EntryPoint = "renameatx_np", SetLastError = true)]
    private static extern int DarwinRenameAtExclusive(
        int oldDirectoryDescriptor,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string oldPath,
        int newDirectoryDescriptor,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string newPath,
        uint flags);

    [DllImport("libc", EntryPoint = "unlinkat", SetLastError = true)]
    private static extern int UnixUnlinkAt(
        int directoryDescriptor,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string path,
        int flags);

    [DllImport("libc", EntryPoint = "fsync", SetLastError = true)]
    private static extern int UnixFileSync(int descriptor);

    [DllImport("libc", EntryPoint = "fremovexattr", SetLastError = true)]
    private static extern int UnixRemoveExtendedAttribute(
        int descriptor,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string name);

    [DllImport("libc", EntryPoint = "fgetxattr", SetLastError = true)]
    private static extern nint UnixGetExtendedAttribute(
        int descriptor,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string name,
        IntPtr value,
        UIntPtr size);

    [DllImport("libc", EntryPoint = "acl_delete_fd_np", SetLastError = true)]
    private static extern int DarwinDeleteAcl(int descriptor, int type);

    [DllImport("libc", EntryPoint = "acl_get_fd_np", SetLastError = true)]
    private static extern IntPtr DarwinGetAcl(int descriptor, int type);

    [DllImport("libc", EntryPoint = "acl_get_entry", SetLastError = true)]
    private static extern int DarwinGetAclEntry(
        IntPtr acl,
        int entryId,
        out IntPtr entry);

    [DllImport("libc", EntryPoint = "acl_free", SetLastError = true)]
    private static extern int DarwinFreeAcl(IntPtr acl);

    [DllImport("libc", EntryPoint = "geteuid")]
    private static extern uint UnixEffectiveUserId();

    [DllImport("libc", EntryPoint = "dup", SetLastError = true)]
    private static extern int UnixDuplicate(int descriptor);

    [DllImport("libc", EntryPoint = "close", SetLastError = true)]
    private static extern int UnixClose(int descriptor);

    [DllImport("libc", EntryPoint = "fcntl", SetLastError = true)]
    private static extern int UnixFcntl(int descriptor, int command, int argument);

    [DllImport("System.Native", EntryPoint = "SystemNative_FStat", SetLastError = true)]
    private static extern int SystemNativeFStat(
        SafeFileHandle descriptor,
        out UnixFileStatus status);

    [DllImport(
        "System.Native",
        EntryPoint = "SystemNative_FcntlSetIsNonBlocking",
        SetLastError = true)]
    private static extern int SystemNativeFcntlSetIsNonBlocking(
        SafeFileHandle descriptor,
        int isNonBlocking);
}
