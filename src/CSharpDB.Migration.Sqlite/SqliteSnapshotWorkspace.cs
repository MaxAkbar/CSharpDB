using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Security.AccessControl;
using System.Security.Cryptography;
using System.Security.Principal;
using Microsoft.Win32.SafeHandles;

namespace CSharpDB.Migration.Sqlite;

/// <summary>
/// Exclusively owns one private retained-SQLite workspace. Cleanup verifies a
/// random ownership marker and removes only registered immediate children.
/// </summary>
internal sealed class SqliteSnapshotWorkspace : IAsyncDisposable
{
    private const string WorkspacePrefix = "csharpdb-sqlite-";
    private const string OwnershipFileName = ".csharpdb-sqlite-owner";
    private const int MaximumCreateAttempts = 16;
    private const uint UnixPrivateDirectoryMode = 0x1C0; // 0700
    private const uint UnixFileTypeMask = 0xF000;
    private const uint UnixDirectoryType = 0x4000;
    private const uint UnixStickyBit = 0x0200;
    private const uint UnixGroupWrite = 0x0010;
    private const uint UnixOtherWrite = 0x0002;
    private const int UnixAtEmptyPath = 0x1000;
    private const int UnixAtSymlinkNoFollow = 0x0100;
    private const uint LinuxStatxBasicStats = 0x07FF;
    private const uint LinuxStatxRequired =
        0x0001 | // STATX_TYPE
        0x0002 | // STATX_MODE
        0x0008 | // STATX_UID
        0x0100;  // STATX_INO
    private const int ErrorAlreadyExists = 183;
    private const int UnixAlreadyExists = 17;
    private const int UnixPermissionDenied = 13;
    private const uint FileShareRead = 0x00000001;
    private const uint FileShareWrite = 0x00000002;
    private const uint OpenExisting = 3;
    private const uint FileFlagBackupSemantics = 0x02000000;
    private const uint FileFlagOpenReparsePoint = 0x00200000;

    private readonly byte[] ownershipToken;
    private readonly string ownershipFilePath;
    private readonly HashSet<string> ownedChildPaths = new(PathComparer);
    private readonly IReadOnlyList<SafeFileHandle> directoryLeases;
    private int disposed;

    internal SqliteSnapshotWorkspace(string rootDirectory)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(rootDirectory);
        RootDirectory = Path.TrimEndingDirectorySeparator(
            Path.GetFullPath(rootDirectory));
        ValidateRootDirectory(RootDirectory);

        Exception? finalFailure = null;
        for (int attempt = 0; attempt < MaximumCreateAttempts; attempt++)
        {
            string candidatePath = ImmediateChild(
                RootDirectory,
                WorkspacePrefix + Guid.NewGuid().ToString("N"),
                nameof(rootDirectory));
            bool candidateCreated = false;
            byte[]? token = null;
            IReadOnlyList<SafeFileHandle>? candidateLeases = null;
            string markerPath = Path.Combine(
                candidatePath,
                OwnershipFileName);
            try
            {
                CreatePrivateDirectoryExclusive(candidatePath);
                candidateCreated = true;
                token = RandomNumberGenerator.GetBytes(32);
                using (FileStream marker = CreatePrivateFile(
                    markerPath,
                    FileAccess.Write,
                    FileShare.None,
                    FileOptions.WriteThrough))
                {
                    marker.Write(token);
                    marker.Flush(flushToDisk: true);
                }

                VerifyOwnership(markerPath, token);
                candidateLeases =
                    AcquireDirectoryLeases(candidatePath);
                ValidateRootDirectory(candidatePath);
                VerifyOwnership(markerPath, token);
                DirectoryPath = candidatePath;
                ownershipFilePath = markerPath;
                ownershipToken = token;
                directoryLeases = candidateLeases;
                candidateLeases = null;
                return;
            }
            catch (Exception exception) when (
                exception is IOException or UnauthorizedAccessException)
            {
                finalFailure = exception;
                if (candidateLeases is not null)
                {
                    foreach (SafeFileHandle lease in candidateLeases)
                        lease.Dispose();
                    candidateLeases = null;
                }
                if (candidateCreated)
                    CleanupFailedCandidate(candidatePath, markerPath);
            }
            finally
            {
                if (candidateLeases is not null)
                {
                    foreach (SafeFileHandle lease in candidateLeases)
                        lease.Dispose();
                }
                if (token is not null &&
                    !ReferenceEquals(token, ownershipToken))
                {
                    CryptographicOperations.ZeroMemory(token);
                }
            }
        }

        throw new IOException(
            "An exclusively owned SQLite migration workspace could not be created.",
            finalFailure);
    }

    internal string RootDirectory { get; }

    internal string DirectoryPath { get; } = null!;

    internal string GetImmediateChildPath(string fileName)
    {
        ObjectDisposedException.ThrowIf(
            Volatile.Read(ref disposed) != 0,
            this);
        ArgumentException.ThrowIfNullOrWhiteSpace(fileName);
        if (Path.IsPathRooted(fileName) ||
            fileName.IndexOf(Path.DirectorySeparatorChar) >= 0 ||
            fileName.IndexOf(Path.AltDirectorySeparatorChar) >= 0 ||
            fileName.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0 ||
            !string.Equals(
                Path.GetFileName(fileName),
                fileName,
                StringComparison.Ordinal) ||
            fileName is "." or "..")
        {
            throw new ArgumentException(
                "A snapshot file name must identify one immediate workspace child.",
                nameof(fileName));
        }

        string path = ImmediateChild(
            DirectoryPath,
            fileName,
            nameof(fileName));
        lock (ownedChildPaths)
            ownedChildPaths.Add(path);
        return path;
    }

    public ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0)
            return ValueTask.CompletedTask;

        try
        {
            VerifyOwnedDirectory();
            VerifyOwnership(ownershipFilePath, ownershipToken);
            DeleteOwnedDirectoryContents();
            DisposeDirectoryLeases();
            Directory.Delete(DirectoryPath, recursive: false);
            GC.SuppressFinalize(this);
            return ValueTask.CompletedTask;
        }
        finally
        {
            DisposeDirectoryLeases();
            CryptographicOperations.ZeroMemory(ownershipToken);
        }
    }

    private void DisposeDirectoryLeases()
    {
        foreach (SafeFileHandle lease in directoryLeases)
            lease.Dispose();
    }

    private void VerifyOwnedDirectory()
    {
        FileAttributes attributes = File.GetAttributes(DirectoryPath);
        if ((attributes & FileAttributes.Directory) == 0 ||
            (attributes &
                (FileAttributes.ReparsePoint | FileAttributes.Device)) != 0)
        {
            throw new IOException(
                "The SQLite snapshot workspace directory has changed.");
        }
    }

    private void DeleteOwnedDirectoryContents()
    {
        HashSet<string> expected;
        lock (ownedChildPaths)
            expected = new HashSet<string>(ownedChildPaths, PathComparer);
        expected.Add(ownershipFilePath);

        foreach (string entry in
                 Directory.EnumerateFileSystemEntries(DirectoryPath))
        {
            string immediateEntry = ImmediateChild(
                DirectoryPath,
                Path.GetFileName(entry),
                nameof(entry));
            if (!expected.Contains(immediateEntry))
            {
                throw new IOException(
                    "The SQLite snapshot workspace contains an unowned child.");
            }

            FileAttributes attributes = File.GetAttributes(entry);
            if ((attributes &
                    (FileAttributes.Directory |
                     FileAttributes.ReparsePoint |
                     FileAttributes.Device)) != 0)
            {
                throw new IOException(
                    "The SQLite snapshot workspace contains an unsafe child.");
            }
        }

        foreach (string path in expected)
            File.Delete(path);
    }

    private static void ValidateRootDirectory(string rootDirectory)
    {
        if (!OperatingSystem.IsWindows())
        {
            IReadOnlyList<SafeFileHandle> leases =
                AcquireUnixDirectoryLeases(rootDirectory);
            foreach (SafeFileHandle lease in leases)
                lease.Dispose();
            return;
        }

        DirectoryInfo? current = new(rootDirectory);
        while (current is not null)
        {
            FileAttributes attributes = File.GetAttributes(current.FullName);
            if ((attributes & FileAttributes.Directory) == 0 ||
                (attributes &
                    (FileAttributes.ReparsePoint | FileAttributes.Device)) != 0)
            {
                throw new IOException(
                    "The SQLite workspace and its ancestors must be real directories.");
            }

            current = current.Parent;
        }
    }

    private static string ImmediateChild(
        string parentPath,
        string childName,
        string parameterName)
    {
        string path = Path.GetFullPath(Path.Combine(parentPath, childName));
        string? actualParent = Path.GetDirectoryName(path);
        if (actualParent is null ||
            !PathComparer.Equals(
                Path.GetFullPath(actualParent),
                Path.GetFullPath(parentPath)))
        {
            throw new ArgumentException(
                "The path must be an immediate child of the expected directory.",
                parameterName);
        }

        return path;
    }

    private static void VerifyOwnership(
        string markerPath,
        ReadOnlySpan<byte> expectedToken)
    {
        using FileStream marker = OpenReadNoFollow(
            Path.GetFullPath(markerPath),
            bufferSize: 1);
        if (marker.Length != expectedToken.Length)
        {
            throw new IOException(
                "The SQLite workspace ownership marker has changed.");
        }

        Span<byte> actual = stackalloc byte[32];
        if (expectedToken.Length != actual.Length)
        {
            throw new IOException(
                "The SQLite workspace ownership token is invalid.");
        }

        marker.ReadExactly(actual);
        if (!actual.SequenceEqual(expectedToken))
        {
            throw new IOException(
                "The SQLite workspace ownership marker has changed.");
        }

        CryptographicOperations.ZeroMemory(actual);
    }

    private static FileStream OpenReadNoFollow(string path, int bufferSize)
    {
        var stream = new FileStream(
            path,
            new FileStreamOptions
            {
                Mode = FileMode.Open,
                Access = FileAccess.Read,
                Share = FileShare.Read,
                BufferSize = bufferSize,
                Options = FileOptions.SequentialScan,
            });
        try
        {
            FileAttributes attributes =
                File.GetAttributes(stream.SafeFileHandle);
            if ((attributes &
                    (FileAttributes.Directory |
                     FileAttributes.ReparsePoint |
                     FileAttributes.Device)) != 0)
            {
                throw new IOException(
                    "The SQLite workspace file is not a regular file.");
            }

            return stream;
        }
        catch
        {
            stream.Dispose();
            throw;
        }
    }

    private static FileStream CreatePrivateFile(
        string path,
        FileAccess access,
        FileShare share,
        FileOptions fileOptions)
    {
        var options = new FileStreamOptions
        {
            Mode = FileMode.CreateNew,
            Access = access,
            Share = share,
            BufferSize = 1,
            Options = fileOptions,
        };
        if (!OperatingSystem.IsWindows())
        {
            options.UnixCreateMode =
                UnixFileMode.UserRead | UnixFileMode.UserWrite;
        }

        return new FileStream(path, options);
    }

    private static void CreatePrivateDirectoryExclusive(string path)
    {
        if (OperatingSystem.IsWindows())
        {
            byte[] descriptor = CreatePrivateWindowsSecurityDescriptor();
            GCHandle descriptorHandle =
                GCHandle.Alloc(descriptor, GCHandleType.Pinned);
            int error;
            try
            {
                var securityAttributes = new SecurityAttributes
                {
                    Length = checked(
                        (uint)Marshal.SizeOf<SecurityAttributes>()),
                    SecurityDescriptor =
                        descriptorHandle.AddrOfPinnedObject(),
                };
                if (CreateDirectoryW(path, ref securityAttributes))
                    return;
                error = Marshal.GetLastPInvokeError();
            }
            finally
            {
                descriptorHandle.Free();
                CryptographicOperations.ZeroMemory(descriptor);
            }

            Exception nativeError = new Win32Exception(error);
            if (error == ErrorAlreadyExists)
            {
                throw new IOException(
                    "The SQLite workspace candidate already exists.",
                    nativeError);
            }
            if (error == 5)
            {
                throw new UnauthorizedAccessException(
                    "Access to the SQLite workspace parent was denied.",
                    nativeError);
            }

            throw new IOException(
                "The SQLite workspace could not be created.",
                nativeError);
        }

        if (UnixMakeDirectory(path, UnixPrivateDirectoryMode) == 0)
        {
            File.SetUnixFileMode(
                path,
                UnixFileMode.UserRead |
                UnixFileMode.UserWrite |
                UnixFileMode.UserExecute);
            return;
        }

        int unixError = Marshal.GetLastPInvokeError();
        Exception unixNativeError = new Win32Exception(unixError);
        if (unixError == UnixAlreadyExists)
        {
            throw new IOException(
                "The SQLite workspace candidate already exists.",
                unixNativeError);
        }
        if (unixError is 1 or UnixPermissionDenied)
        {
            throw new UnauthorizedAccessException(
                "Access to the SQLite workspace parent was denied.",
                unixNativeError);
        }

        throw new IOException(
            "The SQLite workspace could not be created.",
            unixNativeError);
    }

    private static IReadOnlyList<SafeFileHandle> AcquireDirectoryLeases(
        string directoryPath)
    {
        if (!OperatingSystem.IsWindows())
            return AcquireUnixDirectoryLeases(directoryPath);

        var paths = new List<string>();
        for (DirectoryInfo? current = new(directoryPath);
             current?.Parent is not null;
             current = current.Parent)
        {
            paths.Add(current.FullName);
        }
        paths.Reverse();

        var leases = new List<SafeFileHandle>(paths.Count);
        try
        {
            foreach (string path in paths)
            {
                SafeFileHandle handle = OpenDirectoryW(
                    path,
                    desiredAccess: 0,
                    FileShareRead | FileShareWrite,
                    IntPtr.Zero,
                    OpenExisting,
                    FileFlagBackupSemantics |
                    FileFlagOpenReparsePoint,
                    IntPtr.Zero);
                if (handle.IsInvalid)
                {
                    int error = Marshal.GetLastPInvokeError();
                    handle.Dispose();
                    throw new IOException(
                        "The SQLite workspace directory identity could not be leased.",
                        new Win32Exception(error));
                }

                leases.Add(handle);
            }

            return leases.AsReadOnly();
        }
        catch
        {
            foreach (SafeFileHandle lease in leases)
                lease.Dispose();
            throw;
        }
    }

    private static IReadOnlyList<SafeFileHandle>
        AcquireUnixDirectoryLeases(string directoryPath)
    {
        string fullPath = Path.TrimEndingDirectorySeparator(
            Path.GetFullPath(directoryPath));
        string root = Path.GetPathRoot(fullPath) ?? throw new IOException(
            "The SQLite workspace path does not have a filesystem root.");
        if (!string.Equals(root, "/", StringComparison.Ordinal))
        {
            throw new IOException(
                "The SQLite workspace path has an unsupported Unix filesystem root.");
        }

        string[] components = fullPath[root.Length..].Split(
            Path.DirectorySeparatorChar,
            StringSplitOptions.RemoveEmptyEntries);
        var leases = new List<SafeFileHandle>(components.Length + 1);
        try
        {
            SafeFileHandle current = OpenUnixDirectory(root);
            ValidateUnixDirectory(current);
            leases.Add(current);

            foreach (string component in components)
            {
                current = OpenUnixDirectoryAt(current, component);
                ValidateUnixDirectory(current);
                leases.Add(current);
            }

            return leases.AsReadOnly();
        }
        catch
        {
            foreach (SafeFileHandle lease in leases)
                lease.Dispose();
            throw;
        }
    }

    private static SafeFileHandle OpenUnixDirectory(string path)
    {
        int descriptor = UnixOpen(
            path,
            UnixDirectoryOpenFlags());
        return CreateUnixDirectoryHandle(descriptor);
    }

    private static SafeFileHandle OpenUnixDirectoryAt(
        SafeFileHandle parent,
        string component)
    {
        if (component is "." or ".." ||
            component.IndexOf(Path.DirectorySeparatorChar) >= 0)
        {
            throw new IOException(
                "The SQLite workspace path contains an unsafe Unix component.");
        }

        int descriptor = UnixOpenAt(
            parent.DangerousGetHandle().ToInt32(),
            component,
            UnixDirectoryOpenFlags());
        return CreateUnixDirectoryHandle(descriptor);
    }

    private static SafeFileHandle CreateUnixDirectoryHandle(
        int descriptor)
    {
        if (descriptor >= 0)
        {
            return new SafeFileHandle(
                new IntPtr(descriptor),
                ownsHandle: true);
        }

        int error = Marshal.GetLastPInvokeError();
        throw new IOException(
            "A SQLite workspace directory could not be opened without following links.",
            new Win32Exception(error));
    }

    private static int UnixDirectoryOpenFlags()
    {
        if (OperatingSystem.IsLinux())
        {
            const int LinuxOpenPath = 0x200000;
            const int LinuxOpenDirectory = 0x010000;
            const int LinuxOpenNoFollow = 0x020000;
            const int LinuxOpenCloseOnExec = 0x080000;
            return LinuxOpenPath |
                LinuxOpenDirectory |
                LinuxOpenNoFollow |
                LinuxOpenCloseOnExec;
        }
        if (OperatingSystem.IsMacOS())
        {
            const int DarwinOpenReadOnly = 0x000000;
            const int DarwinOpenNoFollow = 0x000100;
            const int DarwinOpenDirectory = 0x100000;
            const int DarwinOpenCloseOnExec = 0x1000000;
            return DarwinOpenReadOnly |
                DarwinOpenNoFollow |
                DarwinOpenDirectory |
                DarwinOpenCloseOnExec;
        }

        throw new IOException(
            "This Unix platform does not provide the required SQLite workspace identity checks.");
    }

    private static void ValidateUnixDirectory(
        SafeFileHandle directory)
    {
        UnixDirectoryMetadata metadata =
            ReadUnixDirectoryMetadata(directory);
        uint effectiveUserId = UnixGetEffectiveUserId();
        if (!IsTrustedUnixDirectoryMetadata(
                metadata.Mode,
                metadata.OwnerUserId,
                effectiveUserId))
        {
            throw new IOException(
                "The SQLite workspace chain contains an unsafe or untrusted Unix directory.");
        }
    }

    internal static bool IsTrustedUnixDirectoryMetadata(
        uint mode,
        uint ownerUserId,
        uint effectiveUserId)
    {
        if ((mode & UnixFileTypeMask) != UnixDirectoryType)
            return false;

        bool trustedOwner =
            ownerUserId == 0 ||
            ownerUserId == effectiveUserId;
        if (!trustedOwner)
            return false;

        bool groupOrWorldWritable =
            (mode & (UnixGroupWrite | UnixOtherWrite)) != 0;
        return !groupOrWorldWritable ||
            (mode & UnixStickyBit) != 0;
    }

    private static UnixDirectoryMetadata ReadUnixDirectoryMetadata(
        SafeFileHandle directory)
    {
        int descriptor =
            directory.DangerousGetHandle().ToInt32();
        if (OperatingSystem.IsLinux())
        {
            int result = LinuxStatx(
                descriptor,
                string.Empty,
                UnixAtEmptyPath |
                    UnixAtSymlinkNoFollow,
                LinuxStatxBasicStats,
                out LinuxStatxBuffer metadata);
            if (result != 0)
            {
                throw new IOException(
                    "A SQLite workspace directory identity could not be read.",
                    new Win32Exception(
                        Marshal.GetLastPInvokeError()));
            }
            if ((metadata.Mask & LinuxStatxRequired) !=
                LinuxStatxRequired)
            {
                throw new IOException(
                    "A SQLite workspace directory identity is incomplete.");
            }

            return new UnixDirectoryMetadata(
                metadata.Mode,
                metadata.UserId);
        }
        if (OperatingSystem.IsMacOS())
        {
            if (DarwinFileStatus(
                    descriptor,
                    out DarwinStatBuffer metadata) != 0)
            {
                throw new IOException(
                    "A SQLite workspace directory identity could not be read.",
                    new Win32Exception(
                        Marshal.GetLastPInvokeError()));
            }

            return new UnixDirectoryMetadata(
                metadata.Mode,
                metadata.UserId);
        }

        throw new IOException(
            "This Unix platform does not provide the required SQLite workspace metadata.");
    }

    [SupportedOSPlatform("windows")]
    private static byte[] CreatePrivateWindowsSecurityDescriptor()
    {
        using WindowsIdentity identity =
            WindowsIdentity.GetCurrent(TokenAccessLevels.Query);
        SecurityIdentifier owner = identity.User ?? throw new IOException(
            "The current Windows identity does not have a security identifier.");
        var security = new DirectorySecurity();
        security.SetOwner(owner);
        security.SetAccessRuleProtection(
            isProtected: true,
            preserveInheritance: false);
        security.AddAccessRule(new FileSystemAccessRule(
            owner,
            FileSystemRights.FullControl,
            InheritanceFlags.ContainerInherit |
            InheritanceFlags.ObjectInherit,
            PropagationFlags.None,
            AccessControlType.Allow));
        return security.GetSecurityDescriptorBinaryForm();
    }

    private static void CleanupFailedCandidate(
        string candidatePath,
        string markerPath)
    {
        try
        {
            File.Delete(markerPath);
        }
        catch (Exception exception) when (
            exception is FileNotFoundException or
                DirectoryNotFoundException)
        {
        }

        try
        {
            Directory.Delete(candidatePath, recursive: false);
        }
        catch (Exception exception) when (
            exception is IOException or
                UnauthorizedAccessException or
                DirectoryNotFoundException)
        {
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
        ref SecurityAttributes securityAttributes);

    [DllImport(
        "kernel32.dll",
        EntryPoint = "CreateFileW",
        CharSet = CharSet.Unicode,
        ExactSpelling = true,
        SetLastError = true)]
    private static extern SafeFileHandle OpenDirectoryW(
        string fileName,
        uint desiredAccess,
        uint shareMode,
        IntPtr securityAttributes,
        uint creationDisposition,
        uint flagsAndAttributes,
        IntPtr templateFile);

    [DllImport("libc", EntryPoint = "mkdir", SetLastError = true)]
    private static extern int UnixMakeDirectory(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string path,
        uint mode);

    [DllImport("libc", EntryPoint = "open", SetLastError = true)]
    private static extern int UnixOpen(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string path,
        int flags);

    [DllImport("libc", EntryPoint = "openat", SetLastError = true)]
    private static extern int UnixOpenAt(
        int directoryDescriptor,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string path,
        int flags);

    [DllImport("libc", EntryPoint = "geteuid", SetLastError = false)]
    private static extern uint UnixGetEffectiveUserId();

    [DllImport("libc", EntryPoint = "statx", SetLastError = true)]
    private static extern int LinuxStatx(
        int directoryDescriptor,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string path,
        int flags,
        uint mask,
        out LinuxStatxBuffer metadata);

    [DllImport("libc", EntryPoint = "fstat", SetLastError = true)]
    private static extern int DarwinFileStatus(
        int descriptor,
        out DarwinStatBuffer metadata);

    private static StringComparer PathComparer { get; } =
        OperatingSystem.IsWindows()
            ? StringComparer.OrdinalIgnoreCase
            : StringComparer.Ordinal;

    private readonly record struct UnixDirectoryMetadata(
        uint Mode,
        uint OwnerUserId);

    [StructLayout(LayoutKind.Explicit, Size = 256)]
    private struct LinuxStatxBuffer
    {
        [FieldOffset(0)]
        internal uint Mask;

        [FieldOffset(20)]
        internal uint UserId;

        [FieldOffset(28)]
        internal ushort Mode;
    }

    [StructLayout(LayoutKind.Explicit, Size = 144)]
    private struct DarwinStatBuffer
    {
        [FieldOffset(4)]
        internal ushort Mode;

        [FieldOffset(16)]
        internal uint UserId;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct SecurityAttributes
    {
        internal uint Length;
        internal IntPtr SecurityDescriptor;
        internal int InheritHandle;
    }
}
