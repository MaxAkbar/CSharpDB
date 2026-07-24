using System.Buffers;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Security.AccessControl;
using System.Security.Principal;
using System.Text;
using Microsoft.Win32.SafeHandles;

#pragma warning disable CA1416 // Open rejects non-Windows before constructing this substrate.

namespace CSharpDB.Migration.Files.Json;

/// <summary>Deterministic private sibling paths for one JSON destination.</summary>
public sealed record JsonExportPreparedOutputPaths
{
    public required string PreparedDataPath { get; init; }

    public required string CheckpointPath { get; init; }

    public required string PendingCheckpointPath { get; init; }
}

internal enum JsonExportCheckpointFaultPoint
{
    AfterDataDurablyFlushedBeforePendingCheckpoint,
    AfterPendingCheckpointDurablyFlushedBeforeActiveReplacement,
    AfterActiveCheckpointReplacedBeforeResult,
}

internal interface IJsonExportCheckpointFaultInjector
{
    ValueTask InjectAsync(
        JsonExportCheckpointFaultPoint point,
        CancellationToken cancellationToken);
}

/// <summary>
/// Windows-only, handle-bound filesystem substrate for one private prepared
/// JSON output and its checkpoint journal. The prepared handle is the
/// exclusive compliant-exporter lease and the pinned parent handle prevents
/// parent replacement while the lease is live. Active replacement is relative
/// to that pinned parent and immediately requalifies the renamed pending
/// handle. Windows has no destination-identity compare-and-swap rename;
/// same-SID processes that can mutate the parent namespace are therefore
/// outside this lease's threat model. Disposal deliberately preserves files.
/// </summary>
internal sealed class JsonExportPreparedOutputFileSystem :
    IAsyncDisposable
{
    private const int BufferSize = 64 * 1024;
    private const int FileRenameInformation = 10;
    private const int FileDispositionInfo = 4;
    private const int ErrorFileNotFound = 2;
    private const int ErrorPathNotFound = 3;
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

    private readonly JsonExportPreparedOutputPaths paths;
    private readonly string parentPath;
    private readonly SafeFileHandle parentHandle;
    private bool disposed;

    private JsonExportPreparedOutputFileSystem(
        JsonExportPreparedOutputPaths paths,
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

    internal static JsonExportPreparedOutputFileSystem Open(
        JsonExportPreparedOutputPaths paths,
        bool requireExistingData = false)
    {
        if (!OperatingSystem.IsWindows())
        {
            throw new PlatformNotSupportedException(
                "Durable prepared JSON output is currently implemented only on Windows.");
        }

        ArgumentNullException.ThrowIfNull(paths);
        PreparedPathBinding binding = ValidatePaths(paths);
        SafeFileHandle? parent = null;
        FileStream? data = null;
        try
        {
            parent = OpenWindowsParent(binding.ParentPath);
            RequireExactSiblingCase(binding.PreparedDataPath, allowMissing: true);
            RequireExactSiblingCase(binding.CheckpointPath, allowMissing: true);
            RequireExactSiblingCase(binding.PendingCheckpointPath, allowMissing: true);
            RejectUnsafeExistingSibling(binding.PreparedDataPath);
            RejectUnsafeExistingSibling(binding.CheckpointPath);
            RejectUnsafeExistingSibling(binding.PendingCheckpointPath);

            data = OpenWindowsPrivateWritable(
                binding.PreparedDataPath,
                requireDeleteAccess: false,
                createIfMissing: !requireExistingData);
            RequireWindowsParentIdentity(binding.ParentPath, parent);
            ValidateOptionalPrivateSibling(binding.CheckpointPath);
            ReclaimPendingCheckpoint(binding.PendingCheckpointPath);
            RequireWindowsParentIdentity(binding.ParentPath, parent);

            var result = new JsonExportPreparedOutputFileSystem(
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
        RequireParentIdentity();
        RequireExactSiblingCase(paths.CheckpointPath, allowMissing: true);

        FileStream? checkpoint =
            OpenWindowsPrivateRead(paths.CheckpointPath, allowMissing: true);
        if (checkpoint is null)
        {
            RequireParentIdentity();
            return null;
        }

        await using (checkpoint.ConfigureAwait(false))
        {
            if (checkpoint.Length >
                JsonExportCheckpointSerializer.MaximumCheckpointBytes)
            {
                throw new InvalidDataException(
                    "The active JSON export checkpoint exceeds its byte ceiling.");
            }

            byte[] buffer = ArrayPool<byte>.Shared.Rent(BufferSize);
            try
            {
                using var bytes = new MemoryStream(
                    capacity: checked((int)checkpoint.Length));
                int maximumRead = checked(
                    JsonExportCheckpointSerializer.MaximumCheckpointBytes + 1);
                int total = 0;
                while (total < maximumRead)
                {
                    int requested =
                        Math.Min(buffer.Length, maximumRead - total);
                    int read = await checkpoint.ReadAsync(
                            buffer.AsMemory(0, requested),
                            cancellationToken)
                        .ConfigureAwait(false);
                    if (read == 0)
                        break;
                    bytes.Write(buffer, 0, read);
                    total = checked(total + read);
                }
                if (total >
                    JsonExportCheckpointSerializer.MaximumCheckpointBytes)
                {
                    throw new InvalidDataException(
                        "The active JSON export checkpoint exceeds its byte ceiling.");
                }

                RequireParentIdentity();
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
        RequireParentIdentity();
        await DataStream.FlushAsync(cancellationToken).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        DataStream.Flush(flushToDisk: true);
        RequireParentIdentity();
    }

    internal void TruncateData(long length)
    {
        ThrowIfDisposed();
        if (length < 0 || length > DataStream.Length)
        {
            throw new ArgumentOutOfRangeException(
                nameof(length),
                "Prepared JSON data can only be truncated to an existing boundary.");
        }

        RequireParentIdentity();
        DataStream.SetLength(length);
        DataStream.Position = length;
        RequireParentIdentity();
    }

    internal async ValueTask ReplaceCheckpointAsync(
        ReadOnlyMemory<byte> canonicalBytes,
        IJsonExportCheckpointFaultInjector? faultInjector,
        CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        if (canonicalBytes.IsEmpty ||
            canonicalBytes.Length >
            JsonExportCheckpointSerializer.MaximumCheckpointBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(canonicalBytes),
                "Checkpoint bytes must be nonempty and within the canonical byte ceiling.");
        }

        cancellationToken.ThrowIfCancellationRequested();
        RequireParentIdentity();
        RequireExactSiblingCase(paths.PendingCheckpointPath, allowMissing: true);
        FileStream pending = OpenWindowsPrivateWritable(
            paths.PendingCheckpointPath,
            requireDeleteAccess: true);
        bool renamed = false;
        try
        {
            pending.SetLength(0);
            pending.Position = 0;
            await pending.WriteAsync(canonicalBytes, cancellationToken)
                .ConfigureAwait(false);
            await pending.FlushAsync(cancellationToken).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            pending.Flush(flushToDisk: true);

            // The durable pending file is not recovery authority. Cancellation
            // is intentionally no longer observed once it is durable.
            await InjectFaultAsync(
                    faultInjector,
                    JsonExportCheckpointFaultPoint
                        .AfterPendingCheckpointDurablyFlushedBeforeActiveReplacement,
                    CancellationToken.None)
                .ConfigureAwait(false);

            ValidateOptionalPrivateSibling(paths.CheckpointPath);
            RequireParentIdentity();
            ReplaceWindowsByHandle(
                pending,
                parentHandle,
                GetBoundLeafName(paths.CheckpointPath));
            renamed = true;
            ValidateWindowsPrivateFile(pending);
            RequireParentIdentity();

            await InjectFaultAsync(
                    faultInjector,
                    JsonExportCheckpointFaultPoint
                        .AfterActiveCheckpointReplacedBeforeResult,
                    CancellationToken.None)
                .ConfigureAwait(false);
        }
        finally
        {
            try
            {
                pending.Dispose();
            }
            catch when (renamed)
            {
                // The handle-relative rename is the commit point. A late
                // handle-close failure cannot retroactively undo authority.
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
        JsonExportPreparedOutputPaths paths)
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
            ?? throw new ArgumentException(
                "The prepared JSON data path has no parent.");
        string checkpointParent = Path.GetDirectoryName(checkpoint)
            ?? throw new ArgumentException(
                "The JSON checkpoint path has no parent.");
        string pendingParent = Path.GetDirectoryName(pending)
            ?? throw new ArgumentException(
                "The pending JSON checkpoint path has no parent.");
        if (!string.Equals(parent, checkpointParent, StringComparison.Ordinal) ||
            !string.Equals(parent, pendingParent, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "Prepared JSON data and checkpoint files must be exact siblings.");
        }

        var distinct = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (!distinct.Add(data) ||
            !distinct.Add(checkpoint) ||
            !distinct.Add(pending))
        {
            throw new ArgumentException(
                "Prepared JSON data and checkpoint paths must be distinct.");
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
            throw new ArgumentException(
                "Prepared JSON paths cannot contain NUL.", parameterName);
        RejectInvalidUnicode(path, parameterName);
        if (!Path.IsPathFullyQualified(path))
            throw new ArgumentException(
                "Prepared JSON paths must be fully qualified.", parameterName);
        RejectDotSegments(path, parameterName);
        RejectWindowsSpecialPath(path, parameterName);
        if (Path.EndsInDirectorySeparator(path))
            throw new ArgumentException(
                "Prepared JSON paths must name files.", parameterName);

        string full = Path.GetFullPath(path);
        if (!string.Equals(full, path, StringComparison.Ordinal))
            throw new ArgumentException(
                "Prepared JSON paths must be normalized with exact spelling.",
                parameterName);
        string leaf = Path.GetFileName(full);
        if (string.IsNullOrWhiteSpace(leaf) || leaf is "." or "..")
            throw new ArgumentException(
                "Prepared JSON file names are invalid.", parameterName);
        return full;
    }

    private static void RejectInvalidUnicode(
        string path,
        string parameterName)
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
                "Prepared JSON paths must contain valid Unicode scalar data.",
                parameterName);
        }
    }

    private static void RejectDotSegments(
        string path,
        string parameterName)
    {
        string root = Path.GetPathRoot(path) ?? string.Empty;
        foreach (string segment in path[root.Length..].Split(
                     [Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar],
                     StringSplitOptions.None))
        {
            if (segment is "." or "..")
                throw new ArgumentException(
                    "Prepared JSON paths cannot contain traversal segments.",
                    parameterName);
        }
    }

    private static void RejectWindowsSpecialPath(
        string path,
        string parameterName)
    {
        if (path.StartsWith(@"\\?\", StringComparison.Ordinal) ||
            path.StartsWith(@"\\.\", StringComparison.Ordinal) ||
            path.StartsWith(@"\\", StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "Windows device, extended, and network paths are unsupported.",
                parameterName);
        }

        string root = Path.GetPathRoot(path) ?? string.Empty;
        if (path.AsSpan(root.Length).Contains(':'))
            throw new ArgumentException(
                "Windows alternate data streams are unsupported.",
                parameterName);

        foreach (string segment in path[root.Length..].Split(
                     [Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar],
                     StringSplitOptions.RemoveEmptyEntries))
        {
            if (segment.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
                throw new ArgumentException(
                    "Prepared JSON path segments contain invalid Win32 characters.",
                    parameterName);
            if (segment.Contains('~'))
                throw new ArgumentException(
                    "Windows DOS short-name aliases are unsupported.",
                    parameterName);
            if (segment.EndsWith(' ') || segment.EndsWith('.'))
                throw new ArgumentException(
                    "Prepared JSON path segments cannot end in spaces or dots.",
                    parameterName);
            RejectReservedDeviceName(segment, parameterName);
        }
    }

    private static void RejectReservedDeviceName(
        string segment,
        string parameterName)
    {
        int firstDot = segment.IndexOf('.');
        string stem = (firstDot < 0 ? segment : segment[..firstDot])
            .TrimEnd(' ', '.');
        if (stem.Equals("CON", StringComparison.OrdinalIgnoreCase) ||
            stem.Equals("PRN", StringComparison.OrdinalIgnoreCase) ||
            stem.Equals("AUX", StringComparison.OrdinalIgnoreCase) ||
            stem.Equals("NUL", StringComparison.OrdinalIgnoreCase) ||
            (stem.Length == 4 &&
             (stem.StartsWith("COM", StringComparison.OrdinalIgnoreCase) ||
              stem.StartsWith("LPT", StringComparison.OrdinalIgnoreCase)) &&
             stem[3] is >= '1' and <= '9' or
                 '\u00b9' or '\u00b2' or '\u00b3'))
        {
            throw new ArgumentException(
                "Windows reserved device names are unsupported.",
                parameterName);
        }
    }

    [SupportedOSPlatform("windows")]
    private static void ValidateWindowsDirectoryChain(string parentPath)
    {
        if (!Directory.Exists(parentPath))
            throw new DirectoryNotFoundException(
                "The prepared JSON parent directory does not exist.");

        string root = Path.GetPathRoot(parentPath)
            ?? throw new InvalidDataException(
                "The prepared JSON parent root is invalid.");
        string relative = Path.GetRelativePath(root, parentPath);
        string current = root;
        if (relative == ".")
            return;

        foreach (string segment in relative.Split(
                     [Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar],
                     StringSplitOptions.RemoveEmptyEntries))
        {
            RequireExactChildCase(current, segment);
            current = Path.Combine(current, segment);
            FileAttributes attributes = File.GetAttributes(current);
            if ((attributes & FileAttributes.Directory) == 0 ||
                (attributes &
                 (FileAttributes.ReparsePoint | FileAttributes.Device)) != 0)
            {
                throw new InvalidDataException(
                    "The prepared JSON parent cannot traverse a link, device, or non-directory.");
            }
        }
    }

    private static void RequireExactChildCase(
        string parent,
        string requestedLeaf)
    {
        string[] matches = Directory.EnumerateFileSystemEntries(parent)
            .Where(path => string.Equals(
                Path.GetFileName(path),
                requestedLeaf,
                StringComparison.OrdinalIgnoreCase))
            .Take(2)
            .ToArray();
        if (matches.Length != 1 ||
            !string.Equals(
                Path.GetFileName(matches[0]),
                requestedLeaf,
                StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                "The prepared JSON path has ambiguous or noncanonical casing.");
        }
    }

    private static void RequireExactSiblingCase(
        string path,
        bool allowMissing)
    {
        string parent = Path.GetDirectoryName(path)
            ?? throw new ArgumentException(
                "The prepared JSON sibling has no parent.", nameof(path));
        string leaf = Path.GetFileName(path);
        string[] matches = Directory.EnumerateFileSystemEntries(parent)
            .Where(entry => string.Equals(
                Path.GetFileName(entry),
                leaf,
                StringComparison.OrdinalIgnoreCase))
            .Take(2)
            .ToArray();
        if (matches.Length == 0 && allowMissing)
            return;
        if (matches.Length != 1 ||
            !string.Equals(
                Path.GetFileName(matches[0]),
                leaf,
                StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                "A prepared JSON sibling has ambiguous or noncanonical casing.");
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
            OpenExistingDisposition,
            FileFlagOpenReparsePoint | FileFlagBackupSemantics,
            IntPtr.Zero);
        if (handle.IsInvalid)
        {
            int error = Marshal.GetLastPInvokeError();
            handle.Dispose();
            throw new IOException(
                "The prepared JSON parent cannot be opened safely.",
                new Win32Exception(error));
        }

        try
        {
            FileAttributes attributes = File.GetAttributes(handle);
            if ((attributes & FileAttributes.Directory) == 0 ||
                (attributes &
                 (FileAttributes.ReparsePoint | FileAttributes.Device)) != 0)
            {
                throw new InvalidDataException(
                    "The prepared JSON parent must be a real directory.");
            }
            ValidateLocalWindowsFilesystem(path, handle);
            ValidateWindowsParentPathBinding(path, handle);
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
                "The prepared JSON parent volume is invalid.");
        if (new DriveInfo(root).DriveType == DriveType.Network)
            throw new InvalidDataException(
                "Prepared JSON output requires a local Windows filesystem.");
        if (GetFinalWindowsPath(parent).StartsWith(
                @"\\?\UNC\",
                StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidDataException(
                "Prepared JSON output cannot use a network path.");
        }
    }

    [SupportedOSPlatform("windows")]
    private static void ValidateWindowsParentPathBinding(
        string requestedPath,
        SafeFileHandle parent)
    {
        const string prefix = @"\\?\";
        string resolved = GetFinalWindowsPath(parent);
        if (resolved.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            resolved = resolved[prefix.Length..];
        if (!string.Equals(resolved, requestedPath, StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                "The prepared JSON parent resolves through an alias or different casing.");
        }
    }

    [SupportedOSPlatform("windows")]
    private static string GetFinalWindowsPath(SafeFileHandle handle)
    {
        var finalPath = new StringBuilder(512);
        uint length = GetFinalPathNameByHandleW(
            handle,
            finalPath,
            checked((uint)finalPath.Capacity),
            0);
        if (length >= finalPath.Capacity)
        {
            finalPath.EnsureCapacity(checked((int)length + 1));
            length = GetFinalPathNameByHandleW(
                handle,
                finalPath,
                checked((uint)finalPath.Capacity),
                0);
        }
        if (length == 0 || length >= finalPath.Capacity)
        {
            throw new IOException(
                "The prepared JSON parent identity could not be resolved.",
                new Win32Exception(Marshal.GetLastPInvokeError()));
        }
        return finalPath.ToString();
    }

    [SupportedOSPlatform("windows")]
    private static void RequireWindowsParentIdentity(
        string path,
        SafeFileHandle expected)
    {
        using SafeFileHandle actual = OpenWindowsParent(path);
        if (!GetFileInformationByHandle(
                expected,
                out WindowsFileInformation left) ||
            !GetFileInformationByHandle(
                actual,
                out WindowsFileInformation right) ||
            left.VolumeSerialNumber != right.VolumeSerialNumber ||
            left.FileIndexHigh != right.FileIndexHigh ||
            left.FileIndexLow != right.FileIndexLow)
        {
            throw new IOException(
                "The prepared JSON parent identity changed during the operation.");
        }
    }

    [SupportedOSPlatform("windows")]
    private static FileStream OpenWindowsPrivateWritable(
        string path,
        bool requireDeleteAccess,
        bool createIfMissing = true)
    {
        if (createIfMissing)
        {
            FileStream? created = null;
            try
            {
                created = FileSystemAclExtensions.Create(
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
            catch (IOException) when (PathEntryExists(path))
            {
                // The existing private file is opened below.
            }
            catch (UnauthorizedAccessException)
                when (IsUnsafeExistingSibling(path))
            {
                throw new InvalidDataException(
                    "Prepared JSON siblings must be private regular files.");
            }

            if (created is not null)
            {
                FileStream createdStream = created;
                try
                {
                    ValidateWindowsPrivateFile(createdStream);
                    FileStream result = createdStream;
                    created = null;
                    return result;
                }
                catch (Exception qualificationFailure)
                {
                    try
                    {
                        RemoveWindowsByHandle(createdStream);
                    }
                    catch (Exception cleanupFailure)
                    {
                        throw new AggregateException(
                            "Prepared JSON file qualification and handle-bound cleanup did not both complete.",
                            qualificationFailure,
                            cleanupFailure);
                    }
                    throw;
                }
                finally
                {
                    created?.Dispose();
                }
            }
        }

        uint access = GenericRead | GenericWrite | ReadControl;
        if (requireDeleteAccess)
            access |= DeleteAccess;
        SafeFileHandle handle = CreateFileW(
            path,
            access,
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
            int error = Marshal.GetLastPInvokeError();
            handle.Dispose();
            if (error == ErrorFileNotFound)
                throw new FileNotFoundException(
                    "The private prepared JSON file does not exist.", path);
            if (IsUnsafeExistingSibling(path))
                throw new InvalidDataException(
                    "Prepared JSON siblings must be private regular files.");
            throw new IOException(
                "The private prepared JSON file is unavailable or already leased.",
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
            OpenExistingDisposition,
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
                throw new InvalidDataException(
                    "Prepared JSON siblings must be private regular files.");
            if (error == ErrorPathNotFound)
                throw new DirectoryNotFoundException(
                    "The prepared JSON parent disappeared.");
            throw new IOException(
                "The private JSON checkpoint cannot be opened safely.",
                new Win32Exception(error));
        }

        try
        {
            var stream = new FileStream(
                handle,
                FileAccess.Read,
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

    [SupportedOSPlatform("windows")]
    private static FileSecurity CreatePrivateWindowsSecurity()
    {
        using WindowsIdentity identity =
            WindowsIdentity.GetCurrent(TokenAccessLevels.Query);
        SecurityIdentifier owner = identity.User
            ?? throw new IOException(
                "The current Windows identity has no SID.");
        var security = new FileSecurity();
        security.SetOwner(owner);
        security.SetAccessRuleProtection(
            isProtected: true,
            preserveInheritance: false);
        security.AddAccessRule(new FileSystemAccessRule(
            owner,
            FileSystemRights.FullControl,
            AccessControlType.Allow));
        return security;
    }

    [SupportedOSPlatform("windows")]
    private static void ValidateWindowsPrivateFile(FileStream stream)
    {
        FileAttributes attributes =
            File.GetAttributes(stream.SafeFileHandle);
        if ((attributes & UnsafeFileAttributes) != 0 ||
            !GetFileInformationByHandle(
                stream.SafeFileHandle,
                out WindowsFileInformation information) ||
            information.NumberOfLinks != 1)
        {
            throw new InvalidDataException(
                "Prepared JSON files must be regular files with exactly one link.");
        }

        FileSecurity security =
            FileSystemAclExtensions.GetAccessControl(stream);
        using WindowsIdentity identity =
            WindowsIdentity.GetCurrent(TokenAccessLevels.Query);
        SecurityIdentifier owner = identity.User
            ?? throw new IOException(
                "The current Windows identity has no SID.");
        if (!security.AreAccessRulesProtected ||
            security.GetOwner(typeof(SecurityIdentifier))
            is not SecurityIdentifier actual ||
            !owner.Equals(actual))
        {
            throw new InvalidDataException(
                "Prepared JSON files must be private to the current Windows identity.");
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
                    "Prepared JSON files grant access beyond the current Windows identity.");
            }
            ownerHasFullControl |=
                (rule.FileSystemRights & FileSystemRights.FullControl) ==
                FileSystemRights.FullControl;
        }
        if (!ownerHasFullControl)
            throw new InvalidDataException(
                "The current Windows identity lacks full control of the prepared JSON file.");
    }

    [SupportedOSPlatform("windows")]
    private static void ValidateOptionalPrivateSibling(string path)
    {
        RequireExactSiblingCase(path, allowMissing: true);
        using FileStream? stream =
            OpenWindowsPrivateRead(path, allowMissing: true);
    }

    [SupportedOSPlatform("windows")]
    private static void ReclaimPendingCheckpoint(string path)
    {
        RequireExactSiblingCase(path, allowMissing: true);
        FileStream? pending;
        try
        {
            pending = OpenWindowsPrivateWritable(
                path,
                requireDeleteAccess: true,
                createIfMissing: false);
        }
        catch (FileNotFoundException)
        {
            return;
        }

        using (pending)
        {
            ValidateWindowsPrivateFile(pending);
            RemoveWindowsByHandle(pending);
        }

        if (PathEntryExists(path))
        {
            throw new IOException(
                "The stale pending JSON checkpoint was not reclaimed.");
        }
    }

    [SupportedOSPlatform("windows")]
    private static void RemoveWindowsByHandle(FileStream file)
    {
        IntPtr disposition = Marshal.AllocHGlobal(1);
        try
        {
            Marshal.WriteByte(disposition, 1);
            if (!SetFileInformationByHandle(
                    file.SafeFileHandle,
                    FileDispositionInfo,
                    disposition,
                    1))
            {
                throw new IOException(
                    "The private prepared JSON file could not be removed.",
                    new Win32Exception(Marshal.GetLastPInvokeError()));
            }
        }
        finally
        {
            Marshal.FreeHGlobal(disposition);
        }
    }

    [SupportedOSPlatform("windows")]
    private static void ReplaceWindowsByHandle(
        FileStream pending,
        SafeFileHandle parent,
        string destinationLeaf)
    {
        byte[] nameBytes = Encoding.Unicode.GetBytes(destinationLeaf);
        int nameOffset = IntPtr.Size == 8 ? 20 : 12;
        int informationLength = checked(nameOffset + nameBytes.Length);
        int allocationLength = checked(informationLength + sizeof(char));
        IntPtr buffer = Marshal.AllocHGlobal(allocationLength);
        bool parentPinned = false;
        try
        {
            parent.DangerousAddRef(ref parentPinned);
            Marshal.Copy(
                new byte[allocationLength],
                0,
                buffer,
                allocationLength);
            Marshal.WriteByte(buffer, 0, 1); // ReplaceIfExists.
            Marshal.WriteIntPtr(
                buffer,
                IntPtr.Size == 8 ? 8 : 4,
                parent.DangerousGetHandle());
            Marshal.WriteInt32(
                buffer,
                IntPtr.Size == 8 ? 16 : 8,
                nameBytes.Length);
            Marshal.Copy(
                nameBytes,
                0,
                IntPtr.Add(buffer, nameOffset),
                nameBytes.Length);

            int status = NtSetInformationFile(
                pending.SafeFileHandle,
                out _,
                buffer,
                checked((uint)informationLength),
                FileRenameInformation);
            if (status < 0)
            {
                int error = checked(
                    (int)RtlNtStatusToDosError(status));
                throw new IOException(
                    "The active JSON checkpoint could not be atomically replaced.",
                    new Win32Exception(error));
            }
        }
        finally
        {
            if (parentPinned)
                parent.DangerousRelease();
            Marshal.FreeHGlobal(buffer);
        }
    }

    private string GetBoundLeafName(string destinationPath)
    {
        string parent = Path.GetDirectoryName(destinationPath)
            ?? throw new ArgumentException(
                "The checkpoint destination has no parent.",
                nameof(destinationPath));
        if (!string.Equals(parent, parentPath, StringComparison.Ordinal))
            throw new InvalidDataException(
                "The checkpoint replacement destination is outside the bound parent.");
        string leaf = Path.GetFileName(destinationPath);
        if (string.IsNullOrWhiteSpace(leaf) ||
            leaf.Contains(Path.DirectorySeparatorChar) ||
            leaf.Contains(Path.AltDirectorySeparatorChar))
        {
            throw new ArgumentException(
                "The checkpoint replacement destination must be a leaf name.",
                nameof(destinationPath));
        }
        return leaf;
    }

    private void RequireParentIdentity()
    {
        ThrowIfDisposed();
        RequireWindowsParentIdentity(parentPath, parentHandle);
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
            throw new InvalidDataException(
                "Prepared JSON siblings must be private regular files.");
    }

    private static bool IsUnsafeExistingSibling(string path)
    {
        try
        {
            return (File.GetAttributes(path) &
                    UnsafeFileAttributes) != 0;
        }
        catch (Exception exception) when (
            exception is FileNotFoundException or DirectoryNotFoundException)
        {
            return false;
        }
    }

    private static ValueTask InjectFaultAsync(
        IJsonExportCheckpointFaultInjector? faultInjector,
        JsonExportCheckpointFaultPoint point,
        CancellationToken cancellationToken) =>
        faultInjector?.InjectAsync(point, cancellationToken) ??
        ValueTask.CompletedTask;

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

    [StructLayout(LayoutKind.Sequential)]
    private struct IoStatusBlock
    {
        internal IntPtr Status;
        internal IntPtr Information;
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

    [DllImport("ntdll.dll", ExactSpelling = true)]
    private static extern int NtSetInformationFile(
        SafeFileHandle file,
        out IoStatusBlock ioStatusBlock,
        IntPtr fileInformation,
        uint length,
        int fileInformationClass);

    [DllImport("ntdll.dll", ExactSpelling = true)]
    private static extern uint RtlNtStatusToDosError(int status);

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
