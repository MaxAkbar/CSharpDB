using System.Buffers;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Security.AccessControl;
using System.Security.Principal;
using System.Text;
using Microsoft.Win32.SafeHandles;

#pragma warning disable CA1416 // Open rejects non-Windows before constructing this Windows-only substrate.

namespace CSharpDB.Migration.Files.Csv;

/// <summary>
/// Windows filesystem substrate for one private prepared CSV data file and its
/// canonical checkpoint siblings. The prepared data handle is the exclusive
/// cross-process lease; disposal deliberately preserves every file.
/// </summary>
internal sealed class CsvExportPreparedOutputFileSystem : IAsyncDisposable
{
    private const int BufferSize = 64 * 1024;
    private const int FileRenameInfo = 3;
    private const int ErrorFileNotFound = 2;
    private const int ErrorPathNotFound = 3;
    private const uint GenericRead = 0x80000000;
    private const uint GenericWrite = 0x40000000;
    private const uint DeleteAccess = 0x00010000;
    private const uint ReadControl = 0x00020000;
    private const uint FileShareRead = 0x00000001;
    private const uint FileShareWrite = 0x00000002;
    private const uint OpenExisting = 3;
    private const uint FileAttributeNormal = 0x00000080;
    private const uint FileFlagOpenReparsePoint = 0x00200000;
    private const uint FileFlagBackupSemantics = 0x02000000;
    private const uint FileFlagSequentialScan = 0x08000000;
    private const uint FileFlagWriteThrough = 0x80000000;
    private const uint FileFlagOverlapped = 0x40000000;
    private const FileAttributes UnsafeFileAttributes =
        FileAttributes.Directory | FileAttributes.ReparsePoint | FileAttributes.Device;

    private readonly CsvExportPreparedOutputPaths paths;
    private readonly string parentPath;
    private readonly SafeFileHandle parentHandle;
    private bool disposed;

    private CsvExportPreparedOutputFileSystem(
        CsvExportPreparedOutputPaths paths,
        string parentPath,
        SafeFileHandle parentHandle,
        FileStream dataStream)
    {
        this.paths = paths;
        this.parentPath = parentPath;
        this.parentHandle = parentHandle;
        DataStream = dataStream;
    }

    internal FileStream DataStream { get; }

    internal static CsvExportPreparedOutputFileSystem Open(
        CsvExportPreparedOutputPaths paths)
    {
        if (!OperatingSystem.IsWindows())
        {
            throw new PlatformNotSupportedException(
                "Durable prepared CSV output is currently implemented only on Windows.");
        }

        ArgumentNullException.ThrowIfNull(paths);
        PreparedPathBinding binding = ValidatePaths(paths);
        SafeFileHandle? parent = null;
        FileStream? data = null;
        try
        {
            parent = OpenWindowsParent(binding.ParentPath);
            RejectUnsafeExistingSibling(binding.PreparedDataPath);
            RejectUnsafeExistingSibling(binding.CheckpointPath);
            RejectUnsafeExistingSibling(binding.PendingCheckpointPath);
            data = OpenWindowsPrivateWritable(
                binding.PreparedDataPath,
                requireDeleteAccess: false);
            RequireWindowsParentIdentity(binding.ParentPath, parent);
            ValidateOptionalPrivateSibling(binding.CheckpointPath);
            ValidateOptionalPrivateSibling(binding.PendingCheckpointPath);
            RequireWindowsParentIdentity(binding.ParentPath, parent);

            var result = new CsvExportPreparedOutputFileSystem(
                paths,
                binding.ParentPath,
                parent,
                data);
            parent = null;
            data = null;
            return result;
        }
        finally
        {
            data?.Dispose();
            parent?.Dispose();
        }
    }

    internal async ValueTask<byte[]?> ReadActiveCheckpointAsync(
        CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();
        RequireWindowsParentIdentity(parentPath, parentHandle);

        FileStream? checkpoint = OpenWindowsPrivateRead(
            paths.CheckpointPath,
            allowMissing: true);
        if (checkpoint is null)
        {
            RequireWindowsParentIdentity(parentPath, parentHandle);
            return null;
        }

        await using (checkpoint.ConfigureAwait(false))
        {
            if (checkpoint.Length > CsvExportCheckpointSerializer.MaximumCheckpointBytes)
            {
                throw new InvalidDataException(
                    "The active CSV export checkpoint exceeds its byte ceiling.");
            }

            byte[] buffer = ArrayPool<byte>.Shared.Rent(BufferSize);
            try
            {
                using var bytes = new MemoryStream(
                    capacity: checked((int)checkpoint.Length));
                int maximumRead =
                    checked(CsvExportCheckpointSerializer.MaximumCheckpointBytes + 1);
                int total = 0;
                while (total < maximumRead)
                {
                    int requested = Math.Min(buffer.Length, maximumRead - total);
                    int read = await checkpoint.ReadAsync(
                            buffer.AsMemory(0, requested),
                            cancellationToken)
                        .ConfigureAwait(false);
                    if (read == 0)
                        break;

                    bytes.Write(buffer, 0, read);
                    total = checked(total + read);
                }

                if (total > CsvExportCheckpointSerializer.MaximumCheckpointBytes)
                {
                    throw new InvalidDataException(
                        "The active CSV export checkpoint exceeds its byte ceiling.");
                }

                RequireWindowsParentIdentity(parentPath, parentHandle);
                return bytes.ToArray();
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
            }
        }
    }

    internal async ValueTask FlushDataToDiskAsync(
        CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        RequireWindowsParentIdentity(parentPath, parentHandle);
        await DataStream.FlushAsync(cancellationToken).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        DataStream.Flush(flushToDisk: true);
        RequireWindowsParentIdentity(parentPath, parentHandle);
    }

    internal void TruncateData(long length)
    {
        ThrowIfDisposed();
        if (length < 0 || length > DataStream.Length)
        {
            throw new ArgumentOutOfRangeException(
                nameof(length),
                "A prepared CSV data file can only be truncated to an existing boundary.");
        }

        RequireWindowsParentIdentity(parentPath, parentHandle);
        DataStream.SetLength(length);
        DataStream.Position = length;
        RequireWindowsParentIdentity(parentPath, parentHandle);
    }

    internal async ValueTask ReplaceCheckpointAsync(
        ReadOnlyMemory<byte> canonicalBytes,
        CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        if (canonicalBytes.IsEmpty ||
            canonicalBytes.Length > CsvExportCheckpointSerializer.MaximumCheckpointBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(canonicalBytes),
                "Checkpoint bytes must be nonempty and within the canonical byte ceiling.");
        }

        cancellationToken.ThrowIfCancellationRequested();
        RequireWindowsParentIdentity(parentPath, parentHandle);
        FileStream pending = OpenWindowsPrivateWritable(
            paths.PendingCheckpointPath,
            requireDeleteAccess: true);
        bool renamed = false;
        try
        {
            pending.SetLength(0);
            pending.Position = 0;
            await pending.WriteAsync(canonicalBytes, cancellationToken).ConfigureAwait(false);
            await pending.FlushAsync(cancellationToken).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            pending.Flush(flushToDisk: true);

            // Cancellation is intentionally no longer observed after the pending
            // checkpoint becomes durable. The rename either fails or establishes
            // the new active recovery authority.
            ValidateOptionalPrivateSibling(paths.CheckpointPath);
            RequireWindowsParentIdentity(parentPath, parentHandle);
            ReplaceWindowsByHandle(pending, paths.CheckpointPath);
            renamed = true;
        }
        finally
        {
            try
            {
                pending.Dispose();
            }
            catch when (renamed)
            {
                // A successful handle rename is the commit point. Cleanup
                // cannot retroactively turn it into a reported failure.
            }
        }
    }

    public ValueTask DisposeAsync()
    {
        if (disposed)
            return ValueTask.CompletedTask;

        disposed = true;
        try
        {
            DataStream.Dispose();
        }
        finally
        {
            parentHandle.Dispose();
        }
        return ValueTask.CompletedTask;
    }

    private static PreparedPathBinding ValidatePaths(
        CsvExportPreparedOutputPaths paths)
    {
        string data = ValidateAbsoluteNormalizedPath(
            paths.PreparedDataPath,
            nameof(paths.PreparedDataPath));
        string checkpoint = ValidateAbsoluteNormalizedPath(
            paths.CheckpointPath,
            nameof(paths.CheckpointPath));
        string pending = ValidateAbsoluteNormalizedPath(
            paths.PendingCheckpointPath,
            nameof(paths.PendingCheckpointPath));
        string parent = Path.GetDirectoryName(data)
            ?? throw new ArgumentException("The prepared CSV data path has no parent.");
        string comparisonParent1 = Path.GetDirectoryName(checkpoint)
            ?? throw new ArgumentException("The checkpoint path has no parent.");
        string comparisonParent2 = Path.GetDirectoryName(pending)
            ?? throw new ArgumentException("The pending checkpoint path has no parent.");
        if (!string.Equals(parent, comparisonParent1, StringComparison.OrdinalIgnoreCase) ||
            !string.Equals(parent, comparisonParent2, StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException(
                "Prepared CSV data and checkpoint files must be siblings.");
        }
        if (string.Equals(data, checkpoint, StringComparison.OrdinalIgnoreCase) ||
            string.Equals(data, pending, StringComparison.OrdinalIgnoreCase) ||
            string.Equals(checkpoint, pending, StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException(
                "Prepared CSV data and checkpoint paths must be distinct.");
        }

        ValidateWindowsDirectoryChain(parent);
        return new PreparedPathBinding(data, checkpoint, pending, parent);
    }

    private static string ValidateAbsoluteNormalizedPath(
        string path,
        string parameterName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path, parameterName);
        if (path.Contains('\0'))
            throw new ArgumentException("Prepared CSV paths cannot contain NUL.", parameterName);
        if (!Path.IsPathFullyQualified(path))
            throw new ArgumentException("Prepared CSV paths must be fully qualified.", parameterName);
        if (path.StartsWith(@"\\?\", StringComparison.Ordinal) ||
            path.StartsWith(@"\\.\", StringComparison.Ordinal) ||
            path.StartsWith(@"\\", StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "Windows device, extended, and network paths are not supported.",
                parameterName);
        }

        string full = Path.GetFullPath(path);
        if (!string.Equals(full, path, StringComparison.OrdinalIgnoreCase))
            throw new ArgumentException("Prepared CSV paths must be normalized.", parameterName);
        string root = Path.GetPathRoot(full) ?? string.Empty;
        if (full.AsSpan(root.Length).Contains(':'))
            throw new ArgumentException("Alternate data streams are not supported.", parameterName);
        string leaf = Path.GetFileName(full);
        if (string.IsNullOrWhiteSpace(leaf) || leaf is "." or "..")
            throw new ArgumentException("Prepared CSV file names are invalid.", parameterName);
        return full;
    }

    [SupportedOSPlatform("windows")]
    private static void ValidateWindowsDirectoryChain(string parentPath)
    {
        if (!Directory.Exists(parentPath))
            throw new DirectoryNotFoundException("The prepared CSV parent does not exist.");

        string root = Path.GetPathRoot(parentPath)
            ?? throw new InvalidDataException("The prepared CSV parent root is invalid.");
        string relative = Path.GetRelativePath(root, parentPath);
        string current = root;
        if (relative == ".")
            return;

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
                    "The prepared CSV parent cannot traverse a link, device, or non-directory.");
            }
        }
    }

    [SupportedOSPlatform("windows")]
    private static SafeFileHandle OpenWindowsParent(string path)
    {
        SafeFileHandle handle = CreateFileW(
            path,
            GenericRead | ReadControl,
            FileShareRead | FileShareWrite,
            IntPtr.Zero,
            OpenExisting,
            FileFlagOpenReparsePoint | FileFlagBackupSemantics,
            IntPtr.Zero);
        if (handle.IsInvalid)
        {
            int error = Marshal.GetLastPInvokeError();
            handle.Dispose();
            throw new IOException(
                "The prepared CSV parent cannot be opened safely.",
                new Win32Exception(error));
        }

        try
        {
            FileAttributes attributes = File.GetAttributes(handle);
            if ((attributes & FileAttributes.Directory) == 0 ||
                (attributes & (FileAttributes.ReparsePoint | FileAttributes.Device)) != 0)
            {
                throw new InvalidDataException(
                    "The prepared CSV parent must be a real directory.");
            }
            ValidateLocalWindowsFilesystem(path, handle);
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
        string root = Path.GetPathRoot(parentPath)
            ?? throw new InvalidDataException(
                "The prepared CSV parent volume is invalid.");
        var drive = new DriveInfo(root);
        if (drive.DriveType == DriveType.Network)
        {
            throw new InvalidDataException(
                "Prepared CSV output requires a local Windows filesystem; mapped network drives are unsupported.");
        }

        var finalPath = new StringBuilder(512);
        uint length = GetFinalPathNameByHandleW(
            parent,
            finalPath,
            checked((uint)finalPath.Capacity),
            0);
        if (length >= finalPath.Capacity)
        {
            finalPath.EnsureCapacity(checked((int)length + 1));
            length = GetFinalPathNameByHandleW(
                parent,
                finalPath,
                checked((uint)finalPath.Capacity),
                0);
        }
        if (length == 0 || length >= finalPath.Capacity)
        {
            throw new IOException(
                "The prepared CSV parent volume identity could not be resolved.",
                new Win32Exception(Marshal.GetLastPInvokeError()));
        }
        if (finalPath.ToString().StartsWith(
                @"\\?\UNC\",
                StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidDataException(
                "Prepared CSV output requires a local Windows filesystem; network paths are unsupported.");
        }
    }

    [SupportedOSPlatform("windows")]
    private static void RequireWindowsParentIdentity(
        string path,
        SafeFileHandle expected)
    {
        using SafeFileHandle actual = OpenWindowsParent(path);
        if (!GetFileInformationByHandle(expected, out WindowsFileInformation left) ||
            !GetFileInformationByHandle(actual, out WindowsFileInformation right) ||
            left.VolumeSerialNumber != right.VolumeSerialNumber ||
            left.FileIndexHigh != right.FileIndexHigh ||
            left.FileIndexLow != right.FileIndexLow)
        {
            throw new IOException(
                "The prepared CSV parent identity changed during the operation.");
        }
    }

    [SupportedOSPlatform("windows")]
    private static FileStream OpenWindowsPrivateWritable(
        string path,
        bool requireDeleteAccess)
    {
        try
        {
            FileStream created = FileSystemAclExtensions.Create(
                new FileInfo(path),
                FileMode.CreateNew,
                FileSystemRights.FullControl,
                FileShare.None,
                BufferSize,
                FileOptions.Asynchronous |
                    FileOptions.SequentialScan |
                    FileOptions.WriteThrough,
                CreatePrivateWindowsSecurity());
            try
            {
                ValidateWindowsPrivateFile(created);
                return created;
            }
            catch
            {
                created.Dispose();
                throw;
            }
        }
        catch (IOException) when (PathEntryExists(path))
        {
            uint access = GenericRead | GenericWrite | ReadControl;
            if (requireDeleteAccess)
                access |= DeleteAccess;
            SafeFileHandle handle = CreateFileW(
                path,
                access,
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
                    "The private prepared CSV file is unavailable or already leased.",
                    new Win32Exception(error));
            }

            try
            {
                var stream = new FileStream(
                    handle,
                    FileAccess.ReadWrite,
                    BufferSize,
                    isAsync: true);
                handle = null!;
                try
                {
                    ValidateWindowsPrivateFile(stream);
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
        catch (UnauthorizedAccessException) when (IsUnsafeExistingSibling(path))
        {
            throw new InvalidDataException(
                "Prepared CSV sibling paths must be private regular files.");
        }
    }

    [SupportedOSPlatform("windows")]
    private static FileStream? OpenWindowsPrivateRead(
        string path,
        bool allowMissing)
    {
        SafeFileHandle handle = CreateFileW(
            path,
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
            if (allowMissing && error == ErrorFileNotFound)
                return null;
            if (IsUnsafeExistingSibling(path))
            {
                throw new InvalidDataException(
                    "Prepared CSV sibling paths must be private regular files.");
            }
            if (error == ErrorPathNotFound)
                throw new DirectoryNotFoundException("The prepared CSV parent disappeared.");
            throw new IOException(
                "The private CSV export checkpoint cannot be opened safely.",
                new Win32Exception(error));
        }

        try
        {
            var stream = new FileStream(handle, FileAccess.Read, BufferSize, isAsync: true);
            handle = null!;
            try
            {
                ValidateWindowsPrivateFile(stream);
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
    private static void ValidateOptionalPrivateSibling(string path)
    {
        using FileStream? stream = OpenWindowsPrivateRead(path, allowMissing: true);
    }

    [SupportedOSPlatform("windows")]
    private static FileSecurity CreatePrivateWindowsSecurity()
    {
        using WindowsIdentity identity = WindowsIdentity.GetCurrent(TokenAccessLevels.Query);
        SecurityIdentifier owner = identity.User
            ?? throw new IOException("The current Windows identity has no SID.");
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
    private static void ValidateWindowsPrivateFile(FileStream stream)
    {
        FileAttributes attributes = File.GetAttributes(stream.SafeFileHandle);
        if ((attributes & UnsafeFileAttributes) != 0 ||
            !GetFileInformationByHandle(
                stream.SafeFileHandle,
                out WindowsFileInformation information) ||
            information.NumberOfLinks != 1)
        {
            throw new InvalidDataException(
                "Prepared CSV files must be regular files with exactly one link.");
        }

        FileSecurity security = FileSystemAclExtensions.GetAccessControl(stream);
        using WindowsIdentity identity = WindowsIdentity.GetCurrent(TokenAccessLevels.Query);
        SecurityIdentifier owner = identity.User
            ?? throw new IOException("The current Windows identity has no SID.");
        if (!security.AreAccessRulesProtected ||
            security.GetOwner(typeof(SecurityIdentifier)) is not SecurityIdentifier actual ||
            !owner.Equals(actual))
        {
            throw new InvalidDataException(
                "Prepared CSV files must be private to the current Windows identity.");
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
            if (rule.IdentityReference is not SecurityIdentifier sid ||
                !owner.Equals(sid))
            {
                throw new InvalidDataException(
                    "Prepared CSV files grant access beyond the current Windows identity.");
            }
            ownerHasFullControl |=
                (rule.FileSystemRights & FileSystemRights.FullControl) ==
                FileSystemRights.FullControl;
        }
        if (!ownerHasFullControl)
        {
            throw new InvalidDataException(
                "The current Windows identity lacks full control of the prepared CSV file.");
        }
    }

    [SupportedOSPlatform("windows")]
    private static void ReplaceWindowsByHandle(
        FileStream pending,
        string checkpointPath)
    {
        byte[] nameBytes = Encoding.Unicode.GetBytes(checkpointPath);
        int nameOffset = IntPtr.Size == 8 ? 20 : 12;
        int informationLength = checked(nameOffset + nameBytes.Length);
        int allocationLength = checked(informationLength + sizeof(char));
        IntPtr buffer = Marshal.AllocHGlobal(allocationLength);
        try
        {
            Marshal.Copy(new byte[allocationLength], 0, buffer, allocationLength);
            Marshal.WriteByte(buffer, 0, 1);
            Marshal.WriteIntPtr(buffer, IntPtr.Size == 8 ? 8 : 4, IntPtr.Zero);
            Marshal.WriteInt32(buffer, IntPtr.Size == 8 ? 16 : 8, nameBytes.Length);
            Marshal.Copy(nameBytes, 0, IntPtr.Add(buffer, nameOffset), nameBytes.Length);
            if (!SetFileInformationByHandle(
                    pending.SafeFileHandle,
                    FileRenameInfo,
                    buffer,
                    checked((uint)informationLength)))
            {
                throw new IOException(
                    "The active CSV export checkpoint could not be atomically replaced.",
                    new Win32Exception(Marshal.GetLastPInvokeError()));
            }
        }
        finally
        {
            Marshal.FreeHGlobal(buffer);
        }
    }

    private static bool PathEntryExists(string path)
    {
        try
        {
            _ = File.GetAttributes(path);
            return true;
        }
        catch (Exception exception) when (
            exception is FileNotFoundException or DirectoryNotFoundException)
        {
            return false;
        }
    }

    private static void RejectUnsafeExistingSibling(string path)
    {
        if (IsUnsafeExistingSibling(path))
        {
            throw new InvalidDataException(
                "Prepared CSV sibling paths must be private regular files.");
        }
    }

    private static bool IsUnsafeExistingSibling(string path)
    {
        try
        {
            return (File.GetAttributes(path) & UnsafeFileAttributes) != 0;
        }
        catch (Exception exception) when (
            exception is FileNotFoundException or DirectoryNotFoundException)
        {
            return false;
        }
    }

    private void ThrowIfDisposed() =>
        ObjectDisposedException.ThrowIf(disposed, this);

    private sealed record PreparedPathBinding(
        string PreparedDataPath,
        string CheckpointPath,
        string PendingCheckpointPath,
        string ParentPath);

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

    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool SetFileInformationByHandle(
        SafeFileHandle file,
        int fileInformationClass,
        IntPtr fileInformation,
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
}

#pragma warning restore CA1416
