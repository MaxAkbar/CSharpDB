using System.Buffers;
using System.Buffers.Binary;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using Microsoft.Win32.SafeHandles;
using CSharpDB.Storage.Paging;

namespace CSharpDB.Engine;

internal readonly record struct RetainedDatabaseSnapshotFileHash(long ByteLength, string Sha256);

internal static class RetainedDatabaseSnapshotFile
{
    private const string Sha256Prefix = "sha256:";
    private const uint GenericRead = 0x80000000;
    private const uint FileShareRead = 0x00000001;
    private const uint OpenExisting = 3;
    private const uint FileAttributeNormal = 0x00000080;
    private const uint FileFlagOpenReparsePoint = 0x00200000;
    private const uint FileFlagBackupSemantics = 0x02000000;
    private const uint FileFlagSequentialScan = 0x08000000;
    private const uint FileFlagOverlapped = 0x40000000;
    private const int UnixInterrupted = 4;
    private const int LinuxNonBlock = 0x00000800;
    private const int LinuxNoFollow = 0x00020000;
    private const int LinuxCloseOnExec = 0x00080000;
    private const int DarwinNonBlock = 0x00000004;
    private const int DarwinNoFollow = 0x00000100;
    private const int DarwinCloseOnExec = 0x01000000;
    private const int FreeBsdNonBlock = 0x00000004;
    private const int FreeBsdNoFollow = 0x00000100;
    private const int FreeBsdCloseOnExec = 0x00100000;
    private const int FSetFileDescriptor = 2;
    private const int CloseOnExec = 1;
    private const FileAttributes UnsafeFileAttributes =
        FileAttributes.Directory | FileAttributes.ReparsePoint | FileAttributes.Device;
    private static readonly StringComparison PathComparison =
        OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;

    internal static string GetAbsolutePath(string path)
    {
        string fullPath = Path.TrimEndingDirectorySeparator(Path.GetFullPath(path));
        if (OperatingSystem.IsWindows())
        {
            if (fullPath.StartsWith(@"\\.\", StringComparison.Ordinal) ||
                fullPath.StartsWith(@"\\?\", StringComparison.Ordinal))
            {
                throw new IOException("Windows device and extended-device paths are not accepted.");
            }

            string root = Path.GetPathRoot(fullPath) ?? string.Empty;
            if (fullPath.AsSpan(root.Length).Contains(':'))
                throw new IOException("Windows alternate data streams are not accepted.");
        }
        return fullPath;
    }

    internal static void EnsureDistinctPaths(string first, string second)
    {
        if (string.Equals(first, second, PathComparison))
            throw new IOException("Source and destination paths must be different.");
    }

    internal static void EnsurePairNamespacesDistinct(string source, string destination)
    {
        string sourceWal = source + ".wal";
        string destinationWal = destination + ".wal";
        if (PathsEqual(source, destination) ||
            PathsEqual(sourceWal, destination) ||
            PathsEqual(source, destinationWal) ||
            PathsEqual(sourceWal, destinationWal))
        {
            throw new IOException(
                "Source and destination database/WAL path namespaces must not overlap.");
        }
    }

    internal static bool PathEntryExists(string path)
    {
        try
        {
            _ = File.GetAttributes(path);
            return true;
        }
        catch (Exception exception) when (
            exception is FileNotFoundException or DirectoryNotFoundException)
        {
            try
            {
                return new FileInfo(path).LinkTarget is not null;
            }
            catch (Exception linkException) when (
                linkException is FileNotFoundException or DirectoryNotFoundException)
            {
                return false;
            }
        }
    }

    internal static FileStream OpenExistingRegularReadOnly(string path, long maxBytes)
    {
        EnsureExistingPathHasNoLinks(path);
        FileStream? stream = null;
        try
        {
            SafeFileHandle handle = OperatingSystem.IsWindows()
                ? OpenWindowsNoFollow(path)
                : OpenUnixNoFollow(path);
            try
            {
                ulong hardLinkCount = ValidateRegularFileAndGetHardLinkCount(handle, path);
                if (hardLinkCount != 1)
                    throw new IOException($"Hard-linked files are not accepted: '{path}'.");

                if (!OperatingSystem.IsWindows() &&
                    SystemNativeFcntlSetIsNonBlocking(handle, isNonBlocking: 0) != 0)
                {
                    throw CreateNativeIOException(
                        $"Could not prepare '{path}' for reading.",
                        Marshal.GetLastPInvokeError());
                }

                stream = new FileStream(handle, FileAccess.Read, bufferSize: 1, isAsync: true);
                handle = null!;
            }
            finally
            {
                handle?.Dispose();
            }

            if (!stream.CanSeek)
                throw new IOException($"Path is not a seekable regular file: '{path}'.");
            EnsureExistingPathHasNoLinks(path);
            if (stream.Length <= 0)
                throw new IOException($"File is empty: '{path}'.");
            if (stream.Length > maxBytes)
                throw new IOException($"File exceeds the configured {maxBytes}-byte limit: '{path}'.");
            return stream;
        }
        catch
        {
            stream?.Dispose();
            throw;
        }
    }

    internal static void ValidatePublishDestination(string path)
    {
        string? parent = Path.GetDirectoryName(path);
        if (string.IsNullOrEmpty(parent) || !Directory.Exists(parent))
            throw new DirectoryNotFoundException($"Snapshot destination directory does not exist: '{parent}'.");
        EnsureExistingPathHasNoLinks(parent);
        if (PathEntryExists(path))
            throw new IOException($"Snapshot destination already exists: '{path}'.");
        if (PathEntryExists(path + ".wal"))
            throw new IOException($"Snapshot destination WAL namespace already exists: '{path}.wal'.");
    }

    internal static async ValueTask CopyToNewFileAsync(
        FileStream source,
        string destinationPath,
        long maxBytes,
        int bufferBytes,
        CancellationToken ct)
    {
        _ = await CopyCoreAsync(source, destinationPath, maxBytes, bufferBytes, hash: false, ct);
    }

    internal static ValueTask<RetainedDatabaseSnapshotFileHash> CopyToNewFileAndHashAsync(
        FileStream source,
        string destinationPath,
        long maxBytes,
        int bufferBytes,
        CancellationToken ct) =>
        CopyCoreAsync(source, destinationPath, maxBytes, bufferBytes, hash: true, ct);

    internal static async ValueTask<RetainedDatabaseSnapshotFileHash> PublishNoOverwriteAsync(
        string sourcePath,
        string destinationPath,
        long maxBytes,
        int bufferBytes,
        CancellationToken ct)
    {
        ValidatePublishDestination(destinationPath);
        string? parent = Path.GetDirectoryName(destinationPath);
        string tempPath = Path.Combine(
            parent!,
            "." + Path.GetFileName(destinationPath) + ".retained-" + Guid.NewGuid().ToString("N") + ".tmp");
        bool tempOwned = false;

        try
        {
            await using FileStream source = OpenExistingRegularReadOnly(sourcePath, maxBytes);
            RetainedDatabaseSnapshotFileHash hash = await CopyCoreAsync(
                source,
                tempPath,
                maxBytes,
                bufferBytes,
                hash: true,
                ct);
            tempOwned = true;

            ct.ThrowIfCancellationRequested();
            // The move is the publication boundary. Cancellation is deliberately
            // no longer observed after this point so a successful move always
            // produces a successful receipt.
            File.Move(tempPath, destinationPath, overwrite: false);
            return hash;
        }
        catch
        {
            if (tempOwned)
                TryDeleteFile(tempPath);
            throw;
        }
    }

    internal static async ValueTask PreflightRecoveryExpansionAsync(
        string databasePath,
        string? walPath,
        long maxSnapshotBytes,
        CancellationToken ct)
    {
        await using FileStream database = OpenExistingRegularReadOnly(databasePath, maxSnapshotBytes);
        byte[] header = new byte[PageConstants.FileHeaderSize];
        await ReadExactlyAtStartAsync(database, header, ct);
        ValidateExpansionCount(
            BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(PageConstants.PageCountOffset, sizeof(uint))),
            maxSnapshotBytes,
            "database header page count");

        if (walPath is null)
            return;

        await using FileStream wal = OpenExistingRegularReadOnly(walPath, long.MaxValue);
        if (wal.Length < PageConstants.WalHeaderSize)
            return;

        byte[] walHeader = new byte[PageConstants.WalHeaderSize];
        await ReadExactlyAtStartAsync(wal, walHeader, ct);
        ValidateExpansionCount(
            BinaryPrimitives.ReadUInt32LittleEndian(walHeader.AsSpan(12, sizeof(uint))),
            maxSnapshotBytes,
            "WAL header page count");

        byte[] frameHeader = new byte[PageConstants.WalFrameHeaderSize];
        byte[] embeddedPageCount = new byte[sizeof(uint)];
        for (long offset = PageConstants.WalHeaderSize;
             offset + PageConstants.WalFrameSize <= wal.Length;
             offset += PageConstants.WalFrameSize)
        {
            ct.ThrowIfCancellationRequested();
            wal.Position = offset;
            await wal.ReadExactlyAsync(frameHeader, ct);
            uint pageId = BinaryPrimitives.ReadUInt32LittleEndian(frameHeader);
            uint committedPageCount = BinaryPrimitives.ReadUInt32LittleEndian(frameHeader.AsSpan(4));
            ValidateExpansionCount((ulong)pageId + 1UL, maxSnapshotBytes, "WAL frame page id");
            if (committedPageCount != 0)
            {
                ValidateExpansionCount(
                    committedPageCount,
                    maxSnapshotBytes,
                    "WAL commit page count");
            }

            if (pageId == 0)
            {
                wal.Position = offset +
                    PageConstants.WalFrameHeaderSize +
                    PageConstants.PageCountOffset;
                await wal.ReadExactlyAsync(embeddedPageCount, ct);
                ValidateExpansionCount(
                    BinaryPrimitives.ReadUInt32LittleEndian(embeddedPageCount),
                    maxSnapshotBytes,
                    "WAL page-0 payload page count");
            }
        }
    }

    internal static bool IsCanonicalSha256(string value)
    {
        if (value is null || value.Length != Sha256Prefix.Length + 64 ||
            !value.StartsWith(Sha256Prefix, StringComparison.Ordinal))
        {
            return false;
        }

        foreach (char c in value.AsSpan(Sha256Prefix.Length))
        {
            if (!(c is >= '0' and <= '9' or >= 'a' and <= 'f'))
                return false;
        }
        return true;
    }

    internal static bool HashEquals(string first, string second)
    {
        if (!IsCanonicalSha256(first) || !IsCanonicalSha256(second))
            return false;
        return CryptographicOperations.FixedTimeEquals(
            Convert.FromHexString(first.AsSpan(Sha256Prefix.Length)),
            Convert.FromHexString(second.AsSpan(Sha256Prefix.Length)));
    }

    internal static void EnsureExistingPathHasNoLinks(string path)
    {
        string fullPath = GetAbsolutePath(path);
        string? root = Path.GetPathRoot(fullPath);
        if (string.IsNullOrEmpty(root))
            throw new IOException($"Path has no filesystem root: '{path}'.");

        string current = root;
        foreach (string component in fullPath[root.Length..]
                     .Split(Path.DirectorySeparatorChar, StringSplitOptions.RemoveEmptyEntries))
        {
            current = Path.Combine(current, component);
            if (!PathEntryExists(current))
                throw new FileNotFoundException("Path component not found.", current);

            FileAttributes attributes = File.GetAttributes(current);
            if ((attributes & FileAttributes.ReparsePoint) != 0 ||
                new FileInfo(current).LinkTarget is not null)
            {
                throw new IOException($"Symbolic links and reparse points are not accepted: '{current}'.");
            }
        }
    }

    private static async ValueTask<RetainedDatabaseSnapshotFileHash> CopyCoreAsync(
        FileStream source,
        string destinationPath,
        long maxBytes,
        int bufferBytes,
        bool hash,
        CancellationToken ct)
    {
        source.Position = 0;
        long expectedLength = source.Length;
        if (expectedLength > maxBytes)
            throw new IOException($"Source exceeds the configured {maxBytes}-byte limit.");

        byte[] buffer = ArrayPool<byte>.Shared.Rent(bufferBytes);
        IncrementalHash? hasher = hash ? IncrementalHash.CreateHash(HashAlgorithmName.SHA256) : null;
        byte[]? hashBytes = null;
        long copied = 0;
        bool destinationOwned = false;
        try
        {
            var streamOptions = new FileStreamOptions
            {
                Mode = FileMode.CreateNew,
                Access = FileAccess.Write,
                Share = FileShare.None,
                Options = FileOptions.Asynchronous | FileOptions.SequentialScan | FileOptions.WriteThrough,
                BufferSize = 1,
            };
            if (!OperatingSystem.IsWindows())
            {
                streamOptions.UnixCreateMode =
                    UnixFileMode.UserRead | UnixFileMode.UserWrite;
            }

            await using var destination = new FileStream(destinationPath, streamOptions);
            destinationOwned = true;
            while (true)
            {
                int read = await source.ReadAsync(buffer.AsMemory(0, bufferBytes), ct);
                if (read == 0)
                    break;

                copied = checked(copied + read);
                if (copied > maxBytes)
                    throw new IOException($"Source exceeds the configured {maxBytes}-byte limit.");
                hasher?.AppendData(buffer, 0, read);
                await destination.WriteAsync(buffer.AsMemory(0, read), ct);
            }

            if (copied != expectedLength)
                throw new IOException("Source length changed while it was copied.");
            await destination.FlushAsync(ct);
            destination.Flush(flushToDisk: true);
            hashBytes = hasher?.GetHashAndReset();
        }
        catch
        {
            if (destinationOwned)
                TryDeleteFile(destinationPath);
            throw;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
            hasher?.Dispose();
        }

        string digest = hash
            ? Sha256Prefix + Convert.ToHexString(hashBytes!).ToLowerInvariant()
            : string.Empty;
        return new RetainedDatabaseSnapshotFileHash(copied, digest);
    }

    private static async ValueTask ReadExactlyAtStartAsync(
        FileStream stream,
        Memory<byte> destination,
        CancellationToken ct)
    {
        stream.Position = 0;
        if (stream.Length < destination.Length)
            throw new IOException("File header is truncated.");
        await stream.ReadExactlyAsync(destination, ct);
    }

    private static void ValidateExpansionCount(ulong pageCount, long maxSnapshotBytes, string field)
    {
        ulong maximumPages = (ulong)(maxSnapshotBytes / PageConstants.PageSize);
        if (pageCount > maximumPages)
        {
            throw new IOException(
                $"{field} would expand the private recovery file " +
                $"above the configured {maxSnapshotBytes}-byte limit.");
        }
    }

    private static void TryDeleteFile(string path)
    {
        try
        {
            if (PathEntryExists(path))
                File.Delete(path);
        }
        catch
        {
            // Best-effort cleanup; the caller's original failure is more useful.
        }
    }

    private static bool PathsEqual(string first, string second) =>
        string.Equals(first, second, PathComparison);

    private static SafeFileHandle OpenWindowsNoFollow(string path)
    {
        SafeFileHandle handle = CreateFileW(
            path,
            GenericRead,
            FileShareRead,
            IntPtr.Zero,
            OpenExisting,
            FileAttributeNormal |
                FileFlagOpenReparsePoint |
                FileFlagBackupSemantics |
                FileFlagSequentialScan |
                FileFlagOverlapped,
            IntPtr.Zero);
        if (!handle.IsInvalid)
            return handle;

        int error = Marshal.GetLastPInvokeError();
        handle.Dispose();
        throw error switch
        {
            2 => new FileNotFoundException("File not found.", path),
            3 => new DirectoryNotFoundException($"Parent directory not found for '{path}'."),
            5 => new UnauthorizedAccessException(
                $"Access to '{path}' was denied.",
                new Win32Exception(error)),
            _ => CreateNativeIOException($"Could not open '{path}'.", error),
        };
    }

    private static SafeFileHandle OpenUnixNoFollow(string path)
    {
        int descriptor;
        int error;
        do
        {
            descriptor = UnixOpen(path, GetUnixOpenFlags());
            error = Marshal.GetLastPInvokeError();
        }
        while (descriptor == -1 && error == UnixInterrupted);

        if (descriptor == -1)
        {
            throw error switch
            {
                2 => new FileNotFoundException("File not found.", path),
                20 => new DirectoryNotFoundException($"Parent directory not found for '{path}'."),
                1 or 13 => new UnauthorizedAccessException(
                    $"Access to '{path}' was denied.",
                    new Win32Exception(error)),
                _ => CreateNativeIOException($"Could not securely open '{path}'.", error),
            };
        }

        try
        {
            if (descriptor == 0)
            {
                int duplicate = UnixDuplicate(descriptor);
                int duplicateError = Marshal.GetLastPInvokeError();
                _ = UnixClose(descriptor);
                descriptor = -1;
                if (duplicate == -1)
                    throw CreateNativeIOException($"Could not open '{path}'.", duplicateError);

                descriptor = duplicate;
                if (UnixFcntl(descriptor, FSetFileDescriptor, CloseOnExec) == -1)
                {
                    throw CreateNativeIOException(
                        $"Could not open '{path}'.",
                        Marshal.GetLastPInvokeError());
                }
            }

            SafeFileHandle handle = new(new IntPtr(descriptor), ownsHandle: true);
            descriptor = -1;
            return handle;
        }
        finally
        {
            if (descriptor >= 0)
                _ = UnixClose(descriptor);
        }
    }

    private static int GetUnixOpenFlags()
    {
        if (OperatingSystem.IsLinux() || OperatingSystem.IsAndroid())
            return LinuxNoFollow | LinuxNonBlock | LinuxCloseOnExec;
        if (OperatingSystem.IsMacOS() ||
            OperatingSystem.IsIOS() ||
            OperatingSystem.IsTvOS() ||
            OperatingSystem.IsMacCatalyst())
        {
            return DarwinNoFollow | DarwinNonBlock | DarwinCloseOnExec;
        }
        if (OperatingSystem.IsFreeBSD())
            return FreeBsdNoFollow | FreeBsdNonBlock | FreeBsdCloseOnExec;

        throw new PlatformNotSupportedException(
            "Secure retained-snapshot file opening is not implemented for this operating system.");
    }

    private static ulong ValidateRegularFileAndGetHardLinkCount(
        SafeFileHandle handle,
        string path)
    {
        try
        {
            FileAttributes attributes = File.GetAttributes(handle);
            if ((attributes & UnsafeFileAttributes) != 0)
                throw new IOException($"Path is not a regular file: '{path}'.");
            _ = RandomAccess.GetLength(handle);

            if (OperatingSystem.IsWindows())
            {
                if (!GetFileInformationByHandle(handle, out ByHandleFileInformation information))
                {
                    throw CreateNativeIOException(
                        $"Could not inspect file identity for '{path}'.",
                        Marshal.GetLastPInvokeError());
                }
                return information.NumberOfLinks;
            }

            if (SystemNativeFStat(handle, out UnixFileStatus status) != 0)
            {
                throw CreateNativeIOException(
                    $"Could not inspect file identity for '{path}'.",
                    Marshal.GetLastPInvokeError());
            }
            const int FileTypeMask = 0xF000;
            const int RegularFileType = 0x8000;
            if ((status.Mode & FileTypeMask) != RegularFileType)
                throw new IOException($"Path is not a regular file: '{path}'.");
            return status.HardLinkCount;
        }
        catch (Exception exception) when (
            exception is NotSupportedException or ArgumentException)
        {
            throw new IOException($"Path is not a regular file: '{path}'.", exception);
        }
    }

    private static IOException CreateNativeIOException(string message, int error) =>
        new(message, new Win32Exception(error));

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
        out ByHandleFileInformation fileInformation);

    [DllImport("libc", EntryPoint = "open", SetLastError = true)]
    private static extern int UnixOpen(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string path,
        int flags);

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

    [StructLayout(LayoutKind.Sequential)]
    private struct FileTime
    {
        internal uint LowDateTime;
        internal uint HighDateTime;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct ByHandleFileInformation
    {
        internal uint FileAttributes;
        internal FileTime CreationTime;
        internal FileTime LastAccessTime;
        internal FileTime LastWriteTime;
        internal uint VolumeSerialNumber;
        internal uint FileSizeHigh;
        internal uint FileSizeLow;
        internal uint NumberOfLinks;
        internal uint FileIndexHigh;
        internal uint FileIndexLow;
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
        internal uint HardLinkCount;
    }
}
