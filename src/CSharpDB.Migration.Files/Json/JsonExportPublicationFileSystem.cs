using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Security.AccessControl;
using System.Security.Cryptography;
using System.Security.Principal;
using System.Text;
using Microsoft.Win32.SafeHandles;

#pragma warning disable CA1416 // Open rejects non-Windows before constructing this Windows-only substrate.

namespace CSharpDB.Migration.Files.Json;

/// <summary>
/// Windows-only, handle-bound filesystem substrate for one JSON export
/// publication. The parent handle blocks namespace replacement, staging
/// names are deterministic, their handles are exclusive pair leases, and
/// every reclaim, delete, or rename acts on the already-qualified handle
/// rather than reopening a path. Another same-SID actor with independent
/// authority to mutate the parent namespace is outside this boundary's threat
/// model.
/// </summary>
internal sealed class JsonExportPublicationFileSystem :
    IDisposable
{
    private const int BufferSize = 64 * 1024;
    private const int FileRenameInformation = 10;
    private const int FileDispositionInfo = 4;
    private const int ErrorFileNotFound = 2;
    private const int ErrorPathNotFound = 3;
    private const int ErrorFileExists = 80;
    private const int ErrorAlreadyExists = 183;
    private const uint GenericRead = 0x80000000;
    private const uint GenericWrite = 0x40000000;
    private const uint DeleteAccess = 0x00010000;
    private const uint ReadControl = 0x00020000;
    private const uint FileShareRead = 0x00000001;
    private const uint FileShareWrite = 0x00000002;
    private const uint OpenExistingDisposition = 3;
    private const uint FileAttributeNormal = 0x00000080;
    private const uint FileFlagOpenReparsePoint = 0x00200000;
    private const uint FileFlagBackupSemantics = 0x02000000;
    private const uint FileFlagSequentialScan = 0x08000000;
    private const uint FileFlagWriteThrough = 0x80000000;
    private const uint FileFlagOverlapped = 0x40000000;
    private const FileAttributes UnsafeFileAttributes =
        FileAttributes.Directory |
        FileAttributes.ReparsePoint |
        FileAttributes.Device;

    private readonly SafeFileHandle parentHandle;
    private bool disposed;

    private JsonExportPublicationFileSystem(
        PublicationPaths paths,
        SafeFileHandle parentHandle)
    {
        Paths = paths;
        this.parentHandle = parentHandle;
    }

    internal PublicationPaths Paths { get; }

    internal static void ValidatePathsForPreflight(
        string destinationPath,
        string manifestPath)
    {
        using JsonExportPublicationFileSystem fileSystem =
            Open(destinationPath, manifestPath);
    }

    internal static JsonExportPublicationFileSystem Open(
        string destinationPath,
        string manifestPath)
    {
        if (!OperatingSystem.IsWindows())
        {
            throw new PlatformNotSupportedException(
                "Atomic JSON export publication is currently implemented only on Windows.");
        }

        PublicationPaths paths =
            PublicationPaths.Bind(
                destinationPath,
                manifestPath);
        SafeFileHandle? parent = null;
        try
        {
            parent =
                OpenWindowsParent(
                    paths.ParentPath);
            RequireWindowsParentIdentity(
                paths.ParentPath,
                parent);
            var result =
                new JsonExportPublicationFileSystem(
                    paths,
                    parent);
            parent = null;
            return result;
        }
        finally
        {
            parent?.Dispose();
        }
    }

    internal FileStream CreatePrivateStagingFile(
        string path) =>
        CreatePrivateStagingFile(
            path,
            afterCreate: null);

    internal FileStream CreatePrivateStagingFile(
        string path,
        Action? afterCreate)
    {
        ThrowIfDisposed();
        RequireBoundStagingPath(path);
        RequireParentIdentity();
        RequireExactSiblingCase(
            path,
            allowMissing: true);

        FileStream? stream = null;
        try
        {
            try
            {
                stream =
                    FileSystemAclExtensions.Create(
                        new FileInfo(path),
                        FileMode.CreateNew,
                        FileSystemRights.FullControl,
                        FileShare.None,
                        BufferSize,
                        FileOptions.Asynchronous |
                        FileOptions.SequentialScan |
                        FileOptions.WriteThrough,
                        CreatePrivateWindowsSecurity());
            }
            catch (IOException)
                when (PathEntryExists(path))
            {
                stream =
                    OpenWindowsPrivateWritable(
                        path);
                stream.SetLength(0);
                stream.Position = 0;
                stream.Flush(
                    flushToDisk: true);
            }
            catch (UnauthorizedAccessException)
                when (IsUnsafeExistingSibling(path))
            {
                throw new InvalidDataException(
                    "JSON export staging paths must be private regular files.");
            }

            afterCreate?.Invoke();
            ValidateWindowsPrivateFile(stream);
            RequireParentIdentity();
            FileStream result = stream;
            stream = null;
            return result;
        }
        catch (Exception qualificationFailure)
            when (stream is not null)
        {
            try
            {
                RemoveWindowsByHandle(stream);
            }
            catch (Exception cleanupFailure)
            {
                throw new AggregateException(
                    "JSON export staging qualification and handle-bound cleanup did not both complete.",
                    qualificationFailure,
                    cleanupFailure);
            }
            throw;
        }
        finally
        {
            stream?.Dispose();
        }
    }

    internal FileStream? OpenExisting(
        string path,
        bool allowMissing)
    {
        ThrowIfDisposed();
        RequireBoundFinalPath(path);
        RequireParentIdentity();
        RequireExactSiblingCase(
            path,
            allowMissing);
        FileStream? stream =
            OpenWindowsPrivateRead(
                path,
                allowMissing);
        try
        {
            RequireParentIdentity();
            FileStream? result = stream;
            stream = null;
            return result;
        }
        finally
        {
            stream?.Dispose();
        }
    }

    internal FileStream OpenExistingRequired(
        string path) =>
        OpenExisting(
            path,
            allowMissing: false) ??
        throw new IOException(
            "A JSON export publication file disappeared.");

    internal void RequireAbsent(
        string path)
    {
        using FileStream? existing =
            OpenExisting(
                path,
                allowMissing: true);
        if (existing is not null)
        {
            throw new InvalidDataException(
                "The JSON export manifest appeared before the data commit.");
        }
    }

    internal NoReplaceRenameStatus RenameNoReplace(
        FileStream temporary,
        string destinationPath)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(temporary);
        RequireBoundFinalPath(
            destinationPath);
        RequireParentIdentity();
        NoReplaceRenameStatus status =
            RenameWindowsByHandleNoReplace(
                temporary,
                parentHandle,
                GetBoundLeafName(
                    destinationPath));
        RequireParentIdentity();
        return status;
    }

    internal void RemoveByHandle(
        FileStream stream)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(stream);
        RemoveWindowsByHandle(stream);
    }

    internal void RequireParentIdentity()
    {
        ThrowIfDisposed();
        RequireWindowsParentIdentity(
            Paths.ParentPath,
            parentHandle);
    }

    internal static WindowsFileIdentity GetIdentity(
        FileStream stream)
    {
        ArgumentNullException.ThrowIfNull(stream);
        if (!GetFileInformationByHandle(
                stream.SafeFileHandle,
                out WindowsFileInformation information))
        {
            throw new IOException(
                "The JSON export file identity could not be read.",
                new Win32Exception(
                    Marshal.GetLastPInvokeError()));
        }
        if (information.NumberOfLinks != 1)
        {
            throw new InvalidDataException(
                "JSON export publication files cannot have hard-link aliases.");
        }

        return new WindowsFileIdentity(
            information.VolumeSerialNumber,
            information.FileIndexHigh,
            information.FileIndexLow);
    }

    internal static void RequireDistinctFiles(
        FileStream first,
        FileStream second)
    {
        if (GetIdentity(first) ==
            GetIdentity(second))
        {
            throw new InvalidDataException(
                "The JSON export data and manifest paths cannot alias the same file.");
        }
    }

    public void Dispose()
    {
        if (disposed)
            return;

        disposed = true;
        parentHandle.Dispose();
    }

    private void ThrowIfDisposed() =>
        ObjectDisposedException.ThrowIf(
            disposed,
            this);

    private void RequireBoundStagingPath(
        string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            path);
        if (!string.Equals(
                path,
                Paths.DataStagingPath,
                StringComparison.Ordinal) &&
            !string.Equals(
                path,
                Paths.ManifestStagingPath,
                StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "The JSON export staging path is outside this publication binding.",
                nameof(path));
        }
    }

    private void RequireBoundFinalPath(
        string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            path);
        if (!string.Equals(
                path,
                Paths.DestinationPath,
                StringComparison.Ordinal) &&
            !string.Equals(
                path,
                Paths.ManifestPath,
                StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "The JSON export final path is outside this publication binding.",
                nameof(path));
        }
    }

    private static string ValidateAbsoluteNormalizedPath(
        string path,
        string parameterName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            path,
            parameterName);
        if (path.Contains('\0'))
        {
            throw new ArgumentException(
                "JSON export paths cannot contain NUL characters.",
                parameterName);
        }
        RejectInvalidUnicode(
            path,
            parameterName);
        if (!Path.IsPathFullyQualified(path))
        {
            throw new ArgumentException(
                "JSON export paths must be fully qualified.",
                parameterName);
        }
        RejectDotSegments(
            path,
            parameterName);
        RejectWindowsSpecialPath(
            path,
            parameterName);
        if (Path.EndsInDirectorySeparator(path))
        {
            throw new ArgumentException(
                "JSON export final paths must name files.",
                parameterName);
        }

        string fullPath =
            Path.GetFullPath(path);
        if (!string.Equals(
                path,
                fullPath,
                StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "JSON export paths must already be normalized.",
                parameterName);
        }
        string leaf =
            Path.GetFileName(fullPath);
        if (string.IsNullOrWhiteSpace(leaf) ||
            leaf is "." or "..")
        {
            throw new ArgumentException(
                "JSON export file names are invalid.",
                parameterName);
        }

        return fullPath;
    }

    private static void RejectDotSegments(
        string path,
        string parameterName)
    {
        string root =
            Path.GetPathRoot(path) ??
            string.Empty;
        foreach (
            string segment in
            path[root.Length..].Split(
                [
                    Path.DirectorySeparatorChar,
                    Path.AltDirectorySeparatorChar,
                ],
                StringSplitOptions.None))
        {
            if (segment is "." or "..")
            {
                throw new ArgumentException(
                    "JSON export paths cannot contain traversal segments.",
                    parameterName);
            }
        }
    }

    private static void RejectInvalidUnicode(
        string path,
        string parameterName)
    {
        for (
            int index = 0;
            index < path.Length;
            index++)
        {
            char value = path[index];
            if (!char.IsSurrogate(value))
                continue;
            if (char.IsHighSurrogate(value) &&
                index + 1 < path.Length &&
                char.IsLowSurrogate(
                    path[index + 1]))
            {
                index++;
                continue;
            }

            throw new ArgumentException(
                "JSON export paths must contain valid Unicode scalar data.",
                parameterName);
        }
    }

    private static void RejectWindowsSpecialPath(
        string path,
        string parameterName)
    {
        if (path.StartsWith(
                @"\\?\",
                StringComparison.Ordinal) ||
            path.StartsWith(
                @"\\.\",
                StringComparison.Ordinal) ||
            path.StartsWith(
                @"\\",
                StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "Windows device, extended, and network paths are not supported.",
                parameterName);
        }

        string root =
            Path.GetPathRoot(path) ??
            string.Empty;
        if (path.AsSpan(root.Length)
            .Contains(':'))
        {
            throw new ArgumentException(
                "Windows alternate data streams cannot be JSON export paths.",
                parameterName);
        }
        foreach (
            string segment in
            path[root.Length..].Split(
                [
                    Path.DirectorySeparatorChar,
                    Path.AltDirectorySeparatorChar,
                ],
                StringSplitOptions.RemoveEmptyEntries))
        {
            if (segment.IndexOfAny(
                    Path.GetInvalidFileNameChars()) >= 0)
            {
                throw new ArgumentException(
                    "Windows JSON export path segments contain invalid file-name characters.",
                    parameterName);
            }
            if (segment.Contains('~'))
            {
                throw new ArgumentException(
                    "Windows DOS short-name aliases cannot be JSON export paths.",
                    parameterName);
            }
            if (segment.EndsWith(' ') ||
                segment.EndsWith('.'))
            {
                throw new ArgumentException(
                    "Windows JSON export path segments cannot end in spaces or dots.",
                    parameterName);
            }
        }

        string leaf =
            Path.GetFileName(path);
        int firstDot =
            leaf.IndexOf('.');
        string stem =
            (firstDot < 0
                ? leaf
                : leaf[..firstDot])
            .TrimEnd(' ', '.');
        if (stem.Equals(
                "CON",
                StringComparison.OrdinalIgnoreCase) ||
            stem.Equals(
                "PRN",
                StringComparison.OrdinalIgnoreCase) ||
            stem.Equals(
                "AUX",
                StringComparison.OrdinalIgnoreCase) ||
            stem.Equals(
                "NUL",
                StringComparison.OrdinalIgnoreCase) ||
            (stem.Length == 4 &&
             (stem.StartsWith(
                  "COM",
                  StringComparison.OrdinalIgnoreCase) ||
              stem.StartsWith(
                  "LPT",
                  StringComparison.OrdinalIgnoreCase)) &&
             stem[3] is
                 >= '1' and <= '9' or
                 '\u00b9' or
                 '\u00b2' or
                 '\u00b3'))
        {
            throw new ArgumentException(
                "Windows reserved device names cannot be JSON export paths.",
                parameterName);
        }
    }

    [SupportedOSPlatform("windows")]
    private static void ValidateWindowsDirectoryChain(
        string parentPath)
    {
        if (!Directory.Exists(parentPath))
        {
            throw new DirectoryNotFoundException(
                "The JSON export parent directory does not exist.");
        }

        string root =
            Path.GetPathRoot(parentPath) ??
            throw new InvalidDataException(
                "The JSON export parent root is invalid.");
        string relative =
            Path.GetRelativePath(
                root,
                parentPath);
        string current = root;
        if (relative == ".")
            return;

        foreach (
            string segment in
            relative.Split(
                [
                    Path.DirectorySeparatorChar,
                    Path.AltDirectorySeparatorChar,
                ],
                StringSplitOptions.RemoveEmptyEntries))
        {
            RequireExactChildCase(
                current,
                segment);
            current =
                Path.Combine(
                    current,
                    segment);
            FileAttributes attributes =
                File.GetAttributes(current);
            if ((attributes &
                 FileAttributes.Directory) == 0 ||
                (attributes &
                 (FileAttributes.ReparsePoint |
                  FileAttributes.Device)) != 0)
            {
                throw new InvalidDataException(
                    "The JSON export parent cannot traverse a link, device, or non-directory.");
            }
        }
    }

    private static void RequireExactChildCase(
        string parent,
        string requestedLeaf)
    {
        string[] matches =
            Directory
                .EnumerateFileSystemEntries(
                    parent)
                .Where(
                    path =>
                        string.Equals(
                            Path.GetFileName(
                                path),
                            requestedLeaf,
                            StringComparison
                                .OrdinalIgnoreCase))
                .Take(2)
                .ToArray();
        if (matches.Length != 1 ||
            !string.Equals(
                Path.GetFileName(
                    matches[0]),
                requestedLeaf,
                StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                "The JSON export parent has ambiguous or noncanonical casing.");
        }
    }

    private static void RequireExactSiblingCase(
        string path,
        bool allowMissing)
    {
        string parent =
            Path.GetDirectoryName(
                path) ??
            throw new ArgumentException(
                "The JSON export staging path has no parent.",
                nameof(path));
        string leaf =
            Path.GetFileName(
                path);
        string[] matches =
            Directory
                .EnumerateFileSystemEntries(
                    parent)
                .Where(
                    entry =>
                        string.Equals(
                            Path.GetFileName(
                                entry),
                            leaf,
                            StringComparison
                                .OrdinalIgnoreCase))
                .Take(2)
                .ToArray();
        if (matches.Length == 0 &&
            allowMissing)
        {
            return;
        }
        if (matches.Length != 1 ||
            !string.Equals(
                Path.GetFileName(
                    matches[0]),
                leaf,
                StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                "A JSON export staging sibling has ambiguous or noncanonical casing.");
        }
    }

    [SupportedOSPlatform("windows")]
    private static SafeFileHandle OpenWindowsParent(
        string path)
    {
        SafeFileHandle handle =
            CreateFileW(
                path,
                GenericRead |
                ReadControl,
                FileShareRead |
                FileShareWrite,
                IntPtr.Zero,
                OpenExistingDisposition,
                FileFlagOpenReparsePoint |
                FileFlagBackupSemantics,
                IntPtr.Zero);
        if (handle.IsInvalid)
        {
            int error =
                Marshal.GetLastPInvokeError();
            handle.Dispose();
            throw new IOException(
                "The JSON export parent cannot be opened safely.",
                new Win32Exception(error));
        }

        try
        {
            FileAttributes attributes =
                File.GetAttributes(handle);
            if ((attributes &
                 FileAttributes.Directory) == 0 ||
                (attributes &
                 (FileAttributes.ReparsePoint |
                  FileAttributes.Device)) != 0)
            {
                throw new InvalidDataException(
                    "The JSON export parent must be a real directory.");
            }
            ValidateLocalWindowsFilesystem(
                path,
                handle);
            ValidateWindowsParentPathBinding(
                path,
                handle);
            return handle;
        }
        catch
        {
            handle.Dispose();
            throw;
        }
    }

    [SupportedOSPlatform("windows")]
    private static void ValidateLocalWindowsFilesystem(
        string parentPath,
        SafeFileHandle parent)
    {
        string root =
            Path.GetPathRoot(parentPath) ??
            throw new InvalidDataException(
                "The JSON export parent volume is invalid.");
        var drive =
            new DriveInfo(root);
        if (drive.DriveType ==
            DriveType.Network)
        {
            throw new InvalidDataException(
                "JSON export publication requires a local Windows filesystem; mapped network drives are unsupported.");
        }

        if (GetFinalWindowsPath(parent)
            .StartsWith(
                @"\\?\UNC\",
                StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidDataException(
                "JSON export publication requires a local Windows filesystem; network paths are unsupported.");
        }
    }

    [SupportedOSPlatform("windows")]
    private static void ValidateWindowsParentPathBinding(
        string requestedPath,
        SafeFileHandle parent)
    {
        const string extendedPrefix = @"\\?\";
        string resolved =
            GetFinalWindowsPath(parent);
        if (resolved.StartsWith(
                extendedPrefix,
                StringComparison.OrdinalIgnoreCase))
        {
            resolved =
                resolved[
                    extendedPrefix.Length..];
        }
        if (!string.Equals(
                resolved,
                requestedPath,
                StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                "The JSON export parent resolves through an alias or changed namespace.");
        }
    }

    [SupportedOSPlatform("windows")]
    private static string GetFinalWindowsPath(
        SafeFileHandle handle)
    {
        var finalPath =
            new StringBuilder(512);
        uint length =
            GetFinalPathNameByHandleW(
                handle,
                finalPath,
                checked((uint)finalPath.Capacity),
                0);
        if (length >=
            finalPath.Capacity)
        {
            finalPath.EnsureCapacity(
                checked((int)length + 1));
            length =
                GetFinalPathNameByHandleW(
                    handle,
                    finalPath,
                    checked((uint)finalPath.Capacity),
                    0);
        }
        if (length == 0 ||
            length >= finalPath.Capacity)
        {
            throw new IOException(
                "The JSON export parent volume identity could not be resolved.",
                new Win32Exception(
                    Marshal.GetLastPInvokeError()));
        }
        return finalPath.ToString();
    }

    [SupportedOSPlatform("windows")]
    private static void RequireWindowsParentIdentity(
        string path,
        SafeFileHandle expected)
    {
        using SafeFileHandle actual =
            OpenWindowsParent(path);
        if (!GetFileInformationByHandle(
                expected,
                out WindowsFileInformation left) ||
            !GetFileInformationByHandle(
                actual,
                out WindowsFileInformation right) ||
            left.VolumeSerialNumber !=
            right.VolumeSerialNumber ||
            left.FileIndexHigh !=
            right.FileIndexHigh ||
            left.FileIndexLow !=
            right.FileIndexLow)
        {
            throw new IOException(
                "The JSON export parent identity changed during publication.");
        }
    }

    [SupportedOSPlatform("windows")]
    private static FileStream
        OpenWindowsPrivateWritable(
        string path)
    {
        SafeFileHandle handle =
            CreateFileW(
                path,
                GenericRead |
                GenericWrite |
                DeleteAccess |
                ReadControl,
                0,
                IntPtr.Zero,
                OpenExistingDisposition,
                FileAttributeNormal |
                FileFlagOpenReparsePoint |
                FileFlagOverlapped |
                FileFlagSequentialScan |
                FileFlagWriteThrough,
                IntPtr.Zero);
        if (handle.IsInvalid)
        {
            int error =
                Marshal.GetLastPInvokeError();
            handle.Dispose();
            if (IsUnsafeExistingSibling(path))
            {
                throw new InvalidDataException(
                    "JSON export staging paths must be private regular files.");
            }
            if (error ==
                ErrorPathNotFound)
            {
                throw new DirectoryNotFoundException(
                    "The JSON export parent disappeared.");
            }
            throw new IOException(
                "The deterministic JSON export staging file is unavailable or already leased.",
                new Win32Exception(error));
        }

        try
        {
            var stream =
                new FileStream(
                    handle,
                    FileAccess.ReadWrite,
                    BufferSize,
                    isAsync: true);
            handle = null!;
            try
            {
                ValidateWindowsPrivateFile(
                    stream);
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
    private static FileStream?
        OpenWindowsPrivateRead(
        string path,
        bool allowMissing)
    {
        SafeFileHandle handle =
            CreateFileW(
                path,
                GenericRead |
                ReadControl,
                FileShareRead,
                IntPtr.Zero,
                OpenExistingDisposition,
                FileAttributeNormal |
                FileFlagOpenReparsePoint |
                FileFlagOverlapped |
                FileFlagSequentialScan,
                IntPtr.Zero);
        if (handle.IsInvalid)
        {
            int error =
                Marshal.GetLastPInvokeError();
            handle.Dispose();
            if (allowMissing &&
                error == ErrorFileNotFound)
            {
                return null;
            }
            if (IsUnsafeExistingSibling(path))
            {
                throw new InvalidDataException(
                    "JSON export publication paths must be private regular files.");
            }
            if (error ==
                ErrorPathNotFound)
            {
                throw new DirectoryNotFoundException(
                    "The JSON export parent disappeared.");
            }
            throw new IOException(
                "The JSON export publication file cannot be opened safely.",
                new Win32Exception(error));
        }

        try
        {
            var stream =
                new FileStream(
                    handle,
                    FileAccess.Read,
                    BufferSize,
                    isAsync: true);
            handle = null!;
            try
            {
                ValidateWindowsPrivateFile(
                    stream);
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
    private static FileSecurity
        CreatePrivateWindowsSecurity()
    {
        using WindowsIdentity identity =
            WindowsIdentity.GetCurrent(
                TokenAccessLevels.Query);
        SecurityIdentifier owner =
            identity.User ??
            throw new IOException(
                "The current Windows identity has no SID.");
        var security =
            new FileSecurity();
        security.SetOwner(owner);
        security.SetAccessRuleProtection(
            isProtected: true,
            preserveInheritance: false);
        security.AddAccessRule(
            new FileSystemAccessRule(
                owner,
                FileSystemRights.FullControl,
                AccessControlType.Allow));
        return security;
    }

    [SupportedOSPlatform("windows")]
    private static void ValidateWindowsPrivateFile(
        FileStream stream)
    {
        FileAttributes attributes =
            File.GetAttributes(
                stream.SafeFileHandle);
        if ((attributes &
             UnsafeFileAttributes) != 0 ||
            !GetFileInformationByHandle(
                stream.SafeFileHandle,
                out WindowsFileInformation information) ||
            information.NumberOfLinks != 1)
        {
            throw new InvalidDataException(
                "JSON export publication files must be regular files with exactly one link.");
        }

        FileSecurity security =
            FileSystemAclExtensions
                .GetAccessControl(stream);
        using WindowsIdentity identity =
            WindowsIdentity.GetCurrent(
                TokenAccessLevels.Query);
        SecurityIdentifier owner =
            identity.User ??
            throw new IOException(
                "The current Windows identity has no SID.");
        if (!security.AreAccessRulesProtected ||
            security.GetOwner(
                typeof(SecurityIdentifier))
            is not SecurityIdentifier actual ||
            !owner.Equals(actual))
        {
            throw new InvalidDataException(
                "JSON export publication files must be private to the current Windows identity.");
        }

        bool ownerHasFullControl = false;
        AuthorizationRuleCollection rules =
            security.GetAccessRules(
                includeExplicit: true,
                includeInherited: true,
                targetType:
                    typeof(SecurityIdentifier));
        foreach (
            FileSystemAccessRule rule
            in rules)
        {
            if (rule.AccessControlType !=
                AccessControlType.Allow)
            {
                continue;
            }
            if (rule.IdentityReference
                is not SecurityIdentifier sid ||
                !owner.Equals(sid))
            {
                throw new InvalidDataException(
                    "JSON export publication files grant access beyond the current Windows identity.");
            }
            ownerHasFullControl |=
                (rule.FileSystemRights &
                 FileSystemRights.FullControl) ==
                FileSystemRights.FullControl;
        }
        if (!ownerHasFullControl)
        {
            throw new InvalidDataException(
                "The current Windows identity lacks full control of the JSON export publication file.");
        }
    }

    [SupportedOSPlatform("windows")]
    private static NoReplaceRenameStatus
        RenameWindowsByHandleNoReplace(
        FileStream temporary,
        SafeFileHandle parent,
        string destinationLeafName)
    {
        byte[] nameBytes =
            Encoding.Unicode.GetBytes(
                destinationLeafName);
        int nameOffset =
            IntPtr.Size == 8
                ? 20
                : 12;
        int informationLength =
            checked(
                nameOffset +
                nameBytes.Length);
        int allocationLength =
            checked(
                informationLength +
                sizeof(char));
        IntPtr buffer =
            Marshal.AllocHGlobal(
                allocationLength);
        bool parentPinned = false;
        try
        {
            parent.DangerousAddRef(
                ref parentPinned);
            Marshal.Copy(
                new byte[allocationLength],
                0,
                buffer,
                allocationLength);
            Marshal.WriteIntPtr(
                buffer,
                IntPtr.Size == 8
                    ? 8
                    : 4,
                parent.DangerousGetHandle());
            Marshal.WriteInt32(
                buffer,
                IntPtr.Size == 8
                    ? 16
                    : 8,
                nameBytes.Length);
            Marshal.Copy(
                nameBytes,
                0,
                IntPtr.Add(
                    buffer,
                    nameOffset),
                nameBytes.Length);
            int status =
                NtSetInformationFile(
                    temporary.SafeFileHandle,
                    out _,
                    buffer,
                    checked(
                        (uint)informationLength),
                    FileRenameInformation);
            if (status >= 0)
            {
                return NoReplaceRenameStatus
                    .Published;
            }

            int error =
                checked(
                    (int)RtlNtStatusToDosError(
                        status));
            if (error is
                ErrorAlreadyExists or
                ErrorFileExists)
            {
                return NoReplaceRenameStatus
                    .DestinationExists;
            }
            throw new IOException(
                "The JSON export file could not be atomically published.",
                new Win32Exception(error));
        }
        finally
        {
            if (parentPinned)
                parent.DangerousRelease();
            Marshal.FreeHGlobal(
                buffer);
        }
    }

    private string GetBoundLeafName(
        string destinationPath)
    {
        string parent =
            Path.GetDirectoryName(
                destinationPath) ??
            throw new ArgumentException(
                "The JSON export destination has no parent.",
                nameof(destinationPath));
        if (!string.Equals(
                parent,
                Paths.ParentPath,
                StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                "The JSON export rename destination is outside the bound parent.");
        }

        string leaf =
            Path.GetFileName(
                destinationPath);
        if (string.IsNullOrWhiteSpace(leaf) ||
            leaf.Contains(
                Path.DirectorySeparatorChar) ||
            leaf.Contains(
                Path.AltDirectorySeparatorChar))
        {
            throw new ArgumentException(
                "The JSON export rename destination must be a leaf name.",
                nameof(destinationPath));
        }
        return leaf;
    }

    [SupportedOSPlatform("windows")]
    private static void RemoveWindowsByHandle(
        FileStream temporary)
    {
        IntPtr disposition =
            Marshal.AllocHGlobal(1);
        try
        {
            Marshal.WriteByte(
                disposition,
                1);
            if (!SetFileInformationByHandle(
                    temporary.SafeFileHandle,
                    FileDispositionInfo,
                    disposition,
                    1))
            {
                throw new IOException(
                    "The private JSON publication file could not be removed.",
                    new Win32Exception(
                        Marshal.GetLastPInvokeError()));
            }
        }
        finally
        {
            Marshal.FreeHGlobal(
                disposition);
        }
    }

    private static bool IsUnsafeExistingSibling(
        string path)
    {
        try
        {
            return (
                File.GetAttributes(path) &
                UnsafeFileAttributes) != 0;
        }
        catch (Exception exception)
            when (
                exception is
                    FileNotFoundException or
                    DirectoryNotFoundException)
        {
            return false;
        }
    }

    private static bool PathEntryExists(
        string path)
    {
        try
        {
            _ = File.GetAttributes(
                path);
            return true;
        }
        catch (Exception exception)
            when (
                exception is
                    FileNotFoundException or
                    DirectoryNotFoundException)
        {
            return false;
        }
    }

    internal sealed record PublicationPaths(
        string DestinationPath,
        string ManifestPath,
        string ParentPath,
        string DataStagingPath,
        string ManifestStagingPath)
    {
        private const string StagingPathBindingContract =
            "csharpdb-json-export-publication-staging-path/v1";

        internal static PublicationPaths Bind(
            string destinationPath,
            string manifestPath)
        {
            string destination =
                ValidateAbsoluteNormalizedPath(
                    destinationPath,
                    nameof(destinationPath));
            string manifest =
                ValidateAbsoluteNormalizedPath(
                    manifestPath,
                    nameof(manifestPath));
            string destinationParent =
                Path.GetDirectoryName(
                    destination) ??
                throw new ArgumentException(
                    "The JSON export destination has no parent.",
                    nameof(destinationPath));
            string manifestParent =
                Path.GetDirectoryName(
                    manifest) ??
                throw new ArgumentException(
                    "The JSON export manifest has no parent.",
                    nameof(manifestPath));
            if (!string.Equals(
                    destinationParent,
                    manifestParent,
                    StringComparison.Ordinal))
            {
                throw new ArgumentException(
                    "The JSON export data and manifest must be siblings.",
                    nameof(manifestPath));
            }

            ValidateWindowsDirectoryChain(
                destinationParent);
            byte[] bindingBytes =
                Encoding.UTF8.GetBytes(
                    StagingPathBindingContract +
                    "\0" +
                    destination +
                    "\0" +
                    manifest);
            string digest;
            try
            {
                digest =
                    Convert.ToHexString(
                            SHA256.HashData(
                                bindingBytes))
                        .ToLowerInvariant();
            }
            finally
            {
                CryptographicOperations
                    .ZeroMemory(
                        bindingBytes);
            }
            string stem =
                $".csharpdb-json-export-{digest[..32]}.publish";
            string dataStaging =
                Path.Combine(
                    destinationParent,
                    stem +
                    ".data.next");
            string manifestStaging =
                Path.Combine(
                    destinationParent,
                    stem +
                    ".manifest.next");
            var names =
                new HashSet<string>(
                    StringComparer.OrdinalIgnoreCase);
            AddDistinct(
                names,
                destination);
            AddDistinct(
                names,
                manifest);
            AddDistinct(
                names,
                dataStaging);
            AddDistinct(
                names,
                manifestStaging);

            return new PublicationPaths(
                destination,
                manifest,
                destinationParent,
                dataStaging,
                manifestStaging);
        }

        private static void AddDistinct(
            HashSet<string> paths,
            string path)
        {
            if (!paths.Add(path))
            {
                throw new ArgumentException(
                    "JSON export final and staging paths must be distinct.");
            }
        }
    }

    internal readonly record struct
        WindowsFileIdentity(
        uint VolumeSerialNumber,
        uint FileIndexHigh,
        uint FileIndexLow);

    internal enum NoReplaceRenameStatus
    {
        Published,
        DestinationExists,
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct WindowsFileInformation
    {
        internal uint FileAttributes;
        internal System.Runtime.InteropServices
            .ComTypes.FILETIME CreationTime;
        internal System.Runtime.InteropServices
            .ComTypes.FILETIME LastAccessTime;
        internal System.Runtime.InteropServices
            .ComTypes.FILETIME LastWriteTime;
        internal uint VolumeSerialNumber;
        internal uint FileSizeHigh;
        internal uint FileSizeLow;
        internal uint NumberOfLinks;
        internal uint FileIndexHigh;
        internal uint FileIndexLow;
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
        SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool
        GetFileInformationByHandle(
        SafeFileHandle file,
        out WindowsFileInformation
            fileInformation);

    [DllImport(
        "kernel32.dll",
        SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool
        SetFileInformationByHandle(
        SafeFileHandle file,
        int fileInformationClass,
        IntPtr fileInformation,
        uint bufferSize);

    [DllImport(
        "ntdll.dll",
        ExactSpelling = true)]
    private static extern int
        NtSetInformationFile(
        SafeFileHandle file,
        out IoStatusBlock ioStatusBlock,
        IntPtr fileInformation,
        uint length,
        int fileInformationClass);

    [DllImport(
        "ntdll.dll",
        ExactSpelling = true)]
    private static extern uint
        RtlNtStatusToDosError(
        int status);

    [DllImport(
        "kernel32.dll",
        EntryPoint =
            "GetFinalPathNameByHandleW",
        CharSet = CharSet.Unicode,
        ExactSpelling = true,
        SetLastError = true)]
    private static extern uint
        GetFinalPathNameByHandleW(
        SafeFileHandle file,
        StringBuilder filePath,
        uint filePathLength,
        uint flags);

    [StructLayout(LayoutKind.Sequential)]
    private struct IoStatusBlock
    {
        internal IntPtr Status;
        internal IntPtr Information;
    }
}

#pragma warning restore CA1416
