using System.Runtime.InteropServices;
using System.Security.Cryptography;

namespace CSharpDB.Engine;

internal sealed class RetainedDatabaseSnapshotWorkspace : IAsyncDisposable
{
    private const string DirectoryPrefix = ".csharpdb-retained-";
    private const string OwnerMarkerName = ".owner";
    private const int UnixOwnerOnlyDirectoryMode = 0x1C0; // 0700
    private readonly string _parentPath;
    private readonly byte[] _ownerToken;
    private readonly HashSet<string> _ownedFileNames = new(StringComparer.Ordinal);
    private readonly object _ownedFileGate = new();
    private int _disposed;

    private RetainedDatabaseSnapshotWorkspace(
        string parentPath,
        string rootPath,
        byte[] ownerToken)
    {
        _parentPath = parentPath;
        RootPath = rootPath;
        _ownerToken = ownerToken;
        _ownedFileNames.Add(OwnerMarkerName);
    }

    internal string RootPath { get; }

    internal static RetainedDatabaseSnapshotWorkspace Create(string? configuredParent)
    {
        string parent;
        if (configuredParent is not null)
        {
            parent = RetainedDatabaseSnapshotFile.GetAbsolutePath(configuredParent);
            if (!Directory.Exists(parent))
            {
                throw new DirectoryNotFoundException(
                    $"Retained-snapshot workspace parent does not exist: '{parent}'.");
            }
            RetainedDatabaseSnapshotFile.EnsureExistingPathHasNoLinks(parent);
        }
        else
        {
            // A predictable intermediate directory in a shared temp location
            // can be pre-created by another user. Reserve the random 0700
            // workspace directly beneath the validated system temp parent.
            parent = CanonicalizeDefaultParent(Path.GetTempPath());
        }

        string root;
        do
        {
            root = Path.Combine(parent, DirectoryPrefix + Guid.NewGuid().ToString("N"));
        }
        while (!TryCreateDirectoryExclusive(root));

        byte[] ownerToken = RandomNumberGenerator.GetBytes(32);
        try
        {
            WriteOwnerMarker(root, ownerToken);
            RetainedDatabaseSnapshotFile.EnsureExistingPathHasNoLinks(root);
            return new RetainedDatabaseSnapshotWorkspace(parent, root, ownerToken);
        }
        catch
        {
            TryDeleteOwnedFile(Path.Combine(root, OwnerMarkerName));
            TryDeleteEmptyDirectory(root);
            throw;
        }
    }

    internal static string CanonicalizeDefaultParent(string path)
    {
        string parent = RetainedDatabaseSnapshotFile.GetAbsolutePath(path);
        if (!Directory.Exists(parent))
        {
            throw new DirectoryNotFoundException(
                $"Default retained-snapshot workspace parent does not exist: '{parent}'.");
        }

        if (!OperatingSystem.IsWindows())
            parent = ResolveUnixRealPath(parent);

        RetainedDatabaseSnapshotFile.EnsureExistingPathHasNoLinks(parent);
        if (!OperatingSystem.IsWindows())
        {
            if (SystemNativeStat(parent, out UnixFileStatus status) != 0)
            {
                int error = Marshal.GetLastPInvokeError();
                throw new IOException(
                    $"Could not inspect default retained-snapshot workspace parent '{parent}' " +
                    $"(errno {error}).");
            }

            UnixFileMode mode = File.GetUnixFileMode(parent);
            ValidateUnixDefaultParentSecurity(mode, status.Uid, UnixGetEffectiveUserId());
        }

        return parent;
    }

    internal static void ValidateUnixDefaultParentSecurity(
        UnixFileMode mode,
        uint ownerUserId,
        uint effectiveUserId)
    {
        if (ownerUserId != 0 && ownerUserId != effectiveUserId)
        {
            throw new IOException(
                "The default retained-snapshot workspace parent is not owned by " +
                "the current user or the system administrator.");
        }

        UnixFileMode sharedWrite = UnixFileMode.GroupWrite | UnixFileMode.OtherWrite;
        if ((mode & sharedWrite) != 0 && (mode & UnixFileMode.StickyBit) == 0)
        {
            throw new IOException(
                "The default retained-snapshot workspace parent is shared-writable " +
                "without the sticky bit.");
        }
    }

    internal string GetPath(string fileName)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (string.IsNullOrWhiteSpace(fileName) ||
            !string.Equals(Path.GetFileName(fileName), fileName, StringComparison.Ordinal) ||
            string.Equals(fileName, OwnerMarkerName, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "Workspace file name must be one non-reserved path component.",
                nameof(fileName));
        }

        lock (_ownedFileGate)
            _ownedFileNames.Add(fileName);
        return Path.Combine(RootPath, fileName);
    }

    public ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return ValueTask.CompletedTask;

        try
        {
            string expectedPrefix = Path.EndsInDirectorySeparator(_parentPath)
                ? _parentPath
                : _parentPath + Path.DirectorySeparatorChar;
            if (!RootPath.StartsWith(
                    expectedPrefix,
                    OperatingSystem.IsWindows()
                        ? StringComparison.OrdinalIgnoreCase
                        : StringComparison.Ordinal) ||
                !Path.GetFileName(RootPath).StartsWith(DirectoryPrefix, StringComparison.Ordinal) ||
                !OwnerMarkerMatches())
            {
                return ValueTask.CompletedTask;
            }

            string[] ownedNames;
            lock (_ownedFileGate)
            {
                ownedNames = _ownedFileNames
                    .Where(static name => !string.Equals(
                        name,
                        OwnerMarkerName,
                        StringComparison.Ordinal))
                    .ToArray();
            }

            foreach (string name in ownedNames)
                TryDeleteOwnedFile(Path.Combine(RootPath, name));

            TryDeleteOwnedFile(Path.Combine(RootPath, OwnerMarkerName));
            TryDeleteEmptyDirectory(RootPath);
        }
        catch
        {
            // Ownership or cleanup uncertainty leaves the private workspace in
            // place rather than deleting anything outside the registered set.
        }

        return ValueTask.CompletedTask;
    }

    private bool OwnerMarkerMatches()
    {
        string markerPath = Path.Combine(RootPath, OwnerMarkerName);
        try
        {
            using FileStream marker = RetainedDatabaseSnapshotFile.OpenExistingRegularReadOnly(
                markerPath,
                maxBytes: 1024);
            if (marker.Length != _ownerToken.Length)
                return false;

            byte[] actual = new byte[_ownerToken.Length];
            marker.ReadExactly(actual);
            return CryptographicOperations.FixedTimeEquals(actual, _ownerToken);
        }
        catch
        {
            return false;
        }
    }

    private static void WriteOwnerMarker(string root, ReadOnlySpan<byte> ownerToken)
    {
        string markerPath = Path.Combine(root, OwnerMarkerName);
        var options = new FileStreamOptions
        {
            Mode = FileMode.CreateNew,
            Access = FileAccess.Write,
            Share = FileShare.None,
            Options = FileOptions.WriteThrough,
            BufferSize = 1,
        };
        if (!OperatingSystem.IsWindows())
            options.UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;

        using var marker = new FileStream(markerPath, options);
        marker.Write(ownerToken);
        marker.Flush(flushToDisk: true);
    }

    private static bool TryCreateDirectoryExclusive(string path)
    {
        if (OperatingSystem.IsWindows())
        {
            if (CreateDirectoryW(path, IntPtr.Zero))
                return true;

            int error = Marshal.GetLastPInvokeError();
            if (error == 183)
                return false;
            throw new IOException(
                $"Could not create private retained-snapshot workspace '{path}'.",
                Marshal.GetExceptionForHR(Marshal.GetHRForLastWin32Error()));
        }

        if (UnixMkdir(path, UnixOwnerOnlyDirectoryMode) == 0)
        {
            File.SetUnixFileMode(
                path,
                UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
            return true;
        }
        int unixError = Marshal.GetLastPInvokeError();
        if (unixError == 17)
            return false;
        throw new IOException(
            $"Could not create private retained-snapshot workspace '{path}' (errno {unixError}).");
    }

    private static void TryDeleteOwnedFile(string path)
    {
        try
        {
            File.Delete(path);
        }
        catch
        {
            // Failure leaves the non-recursive root deletion unable to proceed.
        }
    }

    private static void TryDeleteEmptyDirectory(string path)
    {
        try
        {
            Directory.Delete(path, recursive: false);
        }
        catch
        {
            // Never recurse: unexpected or injected contents remain untouched.
        }
    }

    private static string ResolveUnixRealPath(string path)
    {
        IntPtr resolved = UnixRealPath(path, IntPtr.Zero);
        if (resolved == IntPtr.Zero)
        {
            int error = Marshal.GetLastPInvokeError();
            throw new IOException(
                $"Could not resolve default retained-snapshot workspace parent '{path}' " +
                $"(errno {error}).");
        }

        try
        {
            string? value = Marshal.PtrToStringUTF8(resolved);
            if (string.IsNullOrEmpty(value))
                throw new IOException("The default retained-snapshot workspace path resolved empty.");
            return RetainedDatabaseSnapshotFile.GetAbsolutePath(value);
        }
        finally
        {
            UnixFree(resolved);
        }
    }

    [DllImport(
        "kernel32.dll",
        EntryPoint = "CreateDirectoryW",
        CharSet = CharSet.Unicode,
        ExactSpelling = true,
        SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool CreateDirectoryW(
        string path,
        IntPtr securityAttributes);

    [DllImport("libc", EntryPoint = "mkdir", SetLastError = true)]
    private static extern int UnixMkdir(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string path,
        int mode);

    [DllImport("libc", EntryPoint = "realpath", SetLastError = true)]
    private static extern IntPtr UnixRealPath(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string path,
        IntPtr resolvedPath);

    [DllImport("libc", EntryPoint = "free")]
    private static extern void UnixFree(IntPtr pointer);

    [DllImport("libc", EntryPoint = "geteuid")]
    private static extern uint UnixGetEffectiveUserId();

    [DllImport("System.Native", EntryPoint = "SystemNative_Stat", SetLastError = true)]
    private static extern int SystemNativeStat(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string path,
        out UnixFileStatus status);

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
