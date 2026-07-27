using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Security.AccessControl;
using System.Security.Cryptography;
using System.Security.Principal;

namespace CSharpDB.Migration.LiteDb;

/// <summary>
/// Exclusively owns one private retained-LiteDB workspace. Cleanup verifies a
/// random ownership marker and removes only registered immediate children.
/// </summary>
internal sealed class LiteDbSnapshotWorkspace : IAsyncDisposable
{
    private const string WorkspacePrefix = "csharpdb-litedb-";
    private const string OwnershipFileName =
        ".csharpdb-litedb-owner";
    private const int MaximumCreateAttempts = 16;
    private const uint UnixPrivateDirectoryMode = 0x1C0; // 0700
    private const int ErrorAlreadyExists = 183;
    private const int UnixAlreadyExists = 17;
    private const int UnixPermissionDenied = 13;

    private readonly byte[] ownershipToken = null!;
    private readonly string ownershipFilePath = null!;
    private readonly HashSet<string> ownedChildPaths =
        new(PathComparer);
    private int disposed;

    internal LiteDbSnapshotWorkspace(string rootDirectory)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(rootDirectory);
        RootDirectory = Path.TrimEndingDirectorySeparator(
            Path.GetFullPath(rootDirectory));
        ValidateRootDirectory(RootDirectory);

        Exception? finalFailure = null;
        for (int attempt = 0;
             attempt < MaximumCreateAttempts;
             attempt++)
        {
            string candidatePath = ImmediateChild(
                RootDirectory,
                WorkspacePrefix + Guid.NewGuid().ToString("N"),
                nameof(rootDirectory));
            string markerPath = Path.Combine(
                candidatePath,
                OwnershipFileName);
            bool candidateCreated = false;
            bool accepted = false;
            byte[] token = RandomNumberGenerator.GetBytes(32);
            try
            {
                CreatePrivateDirectoryExclusive(candidatePath);
                candidateCreated = true;
                using (FileStream marker = CreatePrivateFile(
                    markerPath,
                    FileAccess.Write,
                    FileShare.None,
                    FileOptions.WriteThrough))
                {
                    marker.Write(token);
                    marker.Flush(flushToDisk: true);
                }

                ValidateOwnedDirectory(candidatePath);
                VerifyOwnership(markerPath, token);
                DirectoryPath = candidatePath;
                ownershipFilePath = markerPath;
                ownershipToken = token;
                accepted = true;
                return;
            }
            catch (Exception exception) when (
                exception is IOException or
                    UnauthorizedAccessException)
            {
                finalFailure = exception;
                if (candidateCreated)
                {
                    CleanupFailedCandidate(
                        candidatePath,
                        markerPath);
                }
            }
            finally
            {
                if (!accepted)
                    CryptographicOperations.ZeroMemory(token);
            }
        }

        throw new IOException(
            "An exclusively owned LiteDB migration workspace could not be created.",
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
            fileName.IndexOfAny(
                Path.GetInvalidFileNameChars()) >= 0 ||
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
            ValidateOwnedDirectory(DirectoryPath);
            VerifyOwnership(
                ownershipFilePath,
                ownershipToken);
            DeleteOwnedDirectoryContents();
            Directory.Delete(
                DirectoryPath,
                recursive: false);
            GC.SuppressFinalize(this);
            return ValueTask.CompletedTask;
        }
        finally
        {
            CryptographicOperations.ZeroMemory(
                ownershipToken);
        }
    }

    private void DeleteOwnedDirectoryContents()
    {
        HashSet<string> expected;
        lock (ownedChildPaths)
        {
            expected = new HashSet<string>(
                ownedChildPaths,
                PathComparer);
        }
        expected.Add(ownershipFilePath);

        foreach (string entry in
                 Directory.EnumerateFileSystemEntries(
                     DirectoryPath))
        {
            string immediateEntry = ImmediateChild(
                DirectoryPath,
                Path.GetFileName(entry),
                nameof(entry));
            if (!expected.Contains(immediateEntry))
            {
                throw new IOException(
                    "The LiteDB snapshot workspace contains an unowned child.");
            }

            FileAttributes attributes =
                File.GetAttributes(entry);
            if ((attributes &
                    (FileAttributes.Directory |
                     FileAttributes.ReparsePoint |
                     FileAttributes.Device)) != 0)
            {
                throw new IOException(
                    "The LiteDB snapshot workspace contains an unsafe child.");
            }
        }

        foreach (string path in expected)
            File.Delete(path);
    }

    private static void ValidateRootDirectory(
        string rootDirectory)
    {
        if (!Directory.Exists(rootDirectory))
        {
            throw new DirectoryNotFoundException(
                "The retained-source workspace does not exist.");
        }

        for (DirectoryInfo? current =
                 new(rootDirectory);
             current is not null;
             current = current.Parent)
        {
            FileAttributes attributes =
                File.GetAttributes(current.FullName);
            if ((attributes & FileAttributes.Directory) == 0 ||
                (attributes &
                    (FileAttributes.ReparsePoint |
                     FileAttributes.Device)) != 0)
            {
                throw new IOException(
                    "The LiteDB workspace and its ancestors must be real directories.");
            }
        }
    }

    private static void ValidateOwnedDirectory(
        string path)
    {
        FileAttributes attributes =
            File.GetAttributes(path);
        if ((attributes & FileAttributes.Directory) == 0 ||
            (attributes &
                (FileAttributes.ReparsePoint |
                 FileAttributes.Device)) != 0)
        {
            throw new IOException(
                "The LiteDB snapshot workspace directory has changed.");
        }

        if (!OperatingSystem.IsWindows())
        {
            UnixFileMode mode = File.GetUnixFileMode(path);
            UnixFileMode groupOrOther =
                UnixFileMode.GroupRead |
                UnixFileMode.GroupWrite |
                UnixFileMode.GroupExecute |
                UnixFileMode.OtherRead |
                UnixFileMode.OtherWrite |
                UnixFileMode.OtherExecute;
            if ((mode & groupOrOther) != 0)
            {
                throw new IOException(
                    "The LiteDB snapshot workspace is not private.");
            }
        }
    }

    private static string ImmediateChild(
        string parentPath,
        string childName,
        string parameterName)
    {
        string path = Path.GetFullPath(
            Path.Combine(parentPath, childName));
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
        using FileStream marker = OpenRegularReadOnly(
            markerPath,
            bufferSize: 1);
        if (marker.Length != expectedToken.Length)
        {
            throw new IOException(
                "The LiteDB workspace ownership marker has changed.");
        }

        Span<byte> actual = stackalloc byte[32];
        if (expectedToken.Length != actual.Length)
        {
            throw new IOException(
                "The LiteDB workspace ownership token is invalid.");
        }

        marker.ReadExactly(actual);
        bool matches =
            CryptographicOperations.FixedTimeEquals(
                actual,
                expectedToken);
        CryptographicOperations.ZeroMemory(actual);
        if (!matches)
        {
            throw new IOException(
                "The LiteDB workspace ownership marker has changed.");
        }
    }

    private static FileStream OpenRegularReadOnly(
        string path,
        int bufferSize)
    {
        FileAttributes pathAttributes =
            File.GetAttributes(path);
        if ((pathAttributes &
                (FileAttributes.Directory |
                 FileAttributes.ReparsePoint |
                 FileAttributes.Device)) != 0)
        {
            throw new IOException(
                "The LiteDB workspace file is not a regular file.");
        }

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
                File.GetAttributes(
                    stream.SafeFileHandle);
            if ((attributes &
                    (FileAttributes.Directory |
                     FileAttributes.ReparsePoint |
                     FileAttributes.Device)) != 0)
            {
                throw new IOException(
                    "The LiteDB workspace file is not a regular file.");
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
                UnixFileMode.UserRead |
                UnixFileMode.UserWrite;
        }

        return new FileStream(path, options);
    }

    private static void CreatePrivateDirectoryExclusive(
        string path)
    {
        if (OperatingSystem.IsWindows())
        {
            byte[] descriptor =
                CreatePrivateWindowsSecurityDescriptor();
            GCHandle descriptorHandle =
                GCHandle.Alloc(
                    descriptor,
                    GCHandleType.Pinned);
            int error;
            try
            {
                var securityAttributes =
                    new SecurityAttributes
                    {
                        Length = checked(
                            (uint)Marshal.SizeOf<
                                SecurityAttributes>()),
                        SecurityDescriptor =
                            descriptorHandle
                                .AddrOfPinnedObject(),
                    };
                if (CreateDirectoryW(
                        path,
                        ref securityAttributes))
                {
                    return;
                }
                error = Marshal.GetLastPInvokeError();
            }
            finally
            {
                descriptorHandle.Free();
                CryptographicOperations.ZeroMemory(
                    descriptor);
            }

            Exception nativeError =
                new Win32Exception(error);
            if (error == ErrorAlreadyExists)
            {
                throw new IOException(
                    "The LiteDB workspace candidate already exists.",
                    nativeError);
            }
            if (error == 5)
            {
                throw new UnauthorizedAccessException(
                    "Access to the LiteDB workspace parent was denied.",
                    nativeError);
            }

            throw new IOException(
                "The LiteDB workspace could not be created.",
                nativeError);
        }

        if (UnixMakeDirectory(
                path,
                UnixPrivateDirectoryMode) == 0)
        {
            File.SetUnixFileMode(
                path,
                UnixFileMode.UserRead |
                UnixFileMode.UserWrite |
                UnixFileMode.UserExecute);
            return;
        }

        int unixError =
            Marshal.GetLastPInvokeError();
        Exception unixNativeError =
            new Win32Exception(unixError);
        if (unixError == UnixAlreadyExists)
        {
            throw new IOException(
                "The LiteDB workspace candidate already exists.",
                unixNativeError);
        }
        if (unixError is 1 or UnixPermissionDenied)
        {
            throw new UnauthorizedAccessException(
                "Access to the LiteDB workspace parent was denied.",
                unixNativeError);
        }

        throw new IOException(
            "The LiteDB workspace could not be created.",
            unixNativeError);
    }

    [SupportedOSPlatform("windows")]
    private static byte[]
        CreatePrivateWindowsSecurityDescriptor()
    {
        using WindowsIdentity identity =
            WindowsIdentity.GetCurrent(
                TokenAccessLevels.Query);
        SecurityIdentifier owner =
            identity.User ?? throw new IOException(
                "The current Windows identity does not have a security identifier.");
        var security = new DirectorySecurity();
        security.SetOwner(owner);
        security.SetAccessRuleProtection(
            isProtected: true,
            preserveInheritance: false);
        security.AddAccessRule(
            new FileSystemAccessRule(
                owner,
                FileSystemRights.FullControl,
                InheritanceFlags.ContainerInherit |
                    InheritanceFlags.ObjectInherit,
                PropagationFlags.None,
                AccessControlType.Allow));
        return security
            .GetSecurityDescriptorBinaryForm();
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
            Directory.Delete(
                candidatePath,
                recursive: false);
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
        "libc",
        EntryPoint = "mkdir",
        SetLastError = true)]
    private static extern int UnixMakeDirectory(
        [MarshalAs(UnmanagedType.LPUTF8Str)]
        string path,
        uint mode);

    private static StringComparer PathComparer { get; } =
        OperatingSystem.IsWindows() ||
        OperatingSystem.IsMacOS()
            ? StringComparer.OrdinalIgnoreCase
            : StringComparer.Ordinal;

    [StructLayout(LayoutKind.Sequential)]
    private struct SecurityAttributes
    {
        internal uint Length;
        internal IntPtr SecurityDescriptor;
        internal int InheritHandle;
    }
}
