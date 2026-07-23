using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Security.AccessControl;
using System.Security.Cryptography;
using System.Security.Principal;

namespace CSharpDB.Migration.Files.Csv;

/// <summary>
/// Exclusively reserves and marks one private snapshot directory beneath a
/// caller-controlled parent that remains stable for the workspace lifetime.
/// Cleanup verifies the marker and deletes only registered immediate children.
/// </summary>
internal sealed class CsvSnapshotWorkspace : IAsyncDisposable
{
    private const string WorkspacePrefix = "csharpdb-csv-";
    private const string OwnershipFileName = ".csharpdb-csv-owner";
    private const int MaximumCreateAttempts = 16;
    private const uint UnixPrivateDirectoryMode = 0x1C0; // 0700
    private const int ErrorAlreadyExists = 183;
    private const int UnixAlreadyExists = 17;
    private const int UnixPermissionDenied = 13;

    private readonly byte[] ownershipToken;
    private readonly string ownershipFilePath;
    private readonly HashSet<string> ownedChildPaths = new(PathComparer);
    private int disposed;

    public CsvSnapshotWorkspace(string rootDirectory)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(rootDirectory);
        RootDirectory = Path.TrimEndingDirectorySeparator(Path.GetFullPath(rootDirectory));
        Directory.CreateDirectory(RootDirectory);
        ValidateRootDirectory(RootDirectory);

        Exception? finalFailure = null;
        for (int attempt = 0; attempt < MaximumCreateAttempts; attempt++)
        {
            string name = $"{WorkspacePrefix}{Guid.NewGuid():N}";
            string candidatePath = ImmediateChild(RootDirectory, name, nameof(rootDirectory));
            bool candidateCreated = false;
            byte[]? token = null;
            string markerPath = Path.Combine(candidatePath, OwnershipFileName);
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
                DirectoryPath = candidatePath;
                ownershipFilePath = markerPath;
                ownershipToken = token;
                return;
            }
            catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
            {
                finalFailure = exception;
                if (candidateCreated)
                    CleanupFailedCandidate(candidatePath, markerPath);
            }
            finally
            {
                if (token is not null && !ReferenceEquals(token, ownershipToken))
                    CryptographicOperations.ZeroMemory(token);
            }
        }

        throw new IOException(
            $"Could not create an exclusively owned CSV snapshot workspace under '{RootDirectory}'.",
            finalFailure);
    }

    public string RootDirectory { get; }

    public string DirectoryPath { get; } = null!;

    public string GetImmediateChildPath(string fileName)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed) != 0, this);
        ArgumentException.ThrowIfNullOrWhiteSpace(fileName);
        if (Path.IsPathRooted(fileName) ||
            fileName.IndexOf(Path.DirectorySeparatorChar) >= 0 ||
            fileName.IndexOf(Path.AltDirectorySeparatorChar) >= 0 ||
            fileName.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0 ||
            !string.Equals(Path.GetFileName(fileName), fileName, StringComparison.Ordinal) ||
            fileName is "." or "..")
        {
            throw new ArgumentException(
                "A snapshot file name must identify one immediate workspace child.",
                nameof(fileName));
        }

        string path = ImmediateChild(DirectoryPath, fileName, nameof(fileName));
        lock (ownedChildPaths)
            ownedChildPaths.Add(path);
        return path;
    }

    public ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0)
            return ValueTask.CompletedTask;

        // Cleanup is deliberately non-cancelable. The workspace contract
        // requires the parent path to remain caller-controlled and stable.
        // Within it, verify the random marker and use only bounded,
        // nonrecursive operations over explicitly registered child names.
        try
        {
            VerifyOwnedDirectory();
            VerifyOwnership(ownershipFilePath, ownershipToken);
            DeleteOwnedDirectoryContents();
            Directory.Delete(DirectoryPath, recursive: false);
            GC.SuppressFinalize(this);
            return ValueTask.CompletedTask;
        }
        finally
        {
            CryptographicOperations.ZeroMemory(ownershipToken);
        }
    }

    private void VerifyOwnedDirectory()
    {
        FileAttributes attributes = File.GetAttributes(DirectoryPath);
        if ((attributes & FileAttributes.Directory) == 0 ||
            (attributes & (FileAttributes.ReparsePoint | FileAttributes.Device)) != 0)
        {
            throw new IOException("The CSV snapshot workspace directory has changed.");
        }
    }

    private void DeleteOwnedDirectoryContents()
    {
        HashSet<string> expected;
        lock (ownedChildPaths)
            expected = new HashSet<string>(ownedChildPaths, PathComparer);
        expected.Add(ownershipFilePath);

        foreach (string entry in Directory.EnumerateFileSystemEntries(DirectoryPath))
        {
            string immediateEntry = ImmediateChild(
                DirectoryPath,
                Path.GetFileName(entry),
                nameof(entry));
            if (!expected.Contains(immediateEntry))
            {
                throw new IOException(
                    "The CSV snapshot workspace contains an unowned child.");
            }

            FileAttributes attributes = File.GetAttributes(entry);
            if ((attributes & (FileAttributes.Directory | FileAttributes.ReparsePoint | FileAttributes.Device)) != 0)
            {
                throw new IOException(
                    "The CSV snapshot workspace contains an unsafe or unexpected child.");
            }

        }

        foreach (string path in expected)
            File.Delete(path);
    }

    private static void ValidateRootDirectory(string rootDirectory)
    {
        FileAttributes attributes = File.GetAttributes(rootDirectory);
        if ((attributes & FileAttributes.Directory) == 0 ||
            (attributes & (FileAttributes.ReparsePoint | FileAttributes.Device)) != 0)
        {
            throw new IOException(
                "The CSV snapshot workspace parent must be a real, caller-controlled directory.");
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
            !PathComparer.Equals(Path.GetFullPath(actualParent), Path.GetFullPath(parentPath)))
        {
            throw new ArgumentException(
                "The path must be an immediate child of the expected directory.",
                parameterName);
        }

        return path;
    }

    private static void VerifyOwnership(string markerPath, ReadOnlySpan<byte> expectedToken)
    {
        using FileStream marker = CsvSnapshotPackageFile.OpenReadNoFollow(
            Path.GetFullPath(markerPath),
            bufferSize: 1);
        if (marker.Length != expectedToken.Length)
            throw new IOException("The CSV snapshot workspace ownership marker has changed.");

        Span<byte> actual = stackalloc byte[32];
        if (expectedToken.Length != actual.Length)
            throw new IOException("The CSV snapshot workspace ownership token is invalid.");
        marker.ReadExactly(actual);
        if (!actual.SequenceEqual(expectedToken))
            throw new IOException("The CSV snapshot workspace ownership marker has changed.");
        CryptographicOperations.ZeroMemory(actual);
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
            options.UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;
        return new FileStream(path, options);
    }

    private static void CreatePrivateDirectoryExclusive(string path)
    {
        if (OperatingSystem.IsWindows())
        {
            byte[] descriptor = CreatePrivateWindowsSecurityDescriptor();
            GCHandle descriptorHandle = GCHandle.Alloc(descriptor, GCHandleType.Pinned);
            int error;
            try
            {
                var securityAttributes = new SecurityAttributes
                {
                    Length = checked((uint)Marshal.SizeOf<SecurityAttributes>()),
                    SecurityDescriptor = descriptorHandle.AddrOfPinnedObject(),
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
                throw new IOException("The CSV snapshot workspace candidate already exists.", nativeError);
            if (error == 5)
            {
                throw new UnauthorizedAccessException(
                    "Access to the CSV snapshot workspace parent was denied.",
                    nativeError);
            }

            throw new IOException("The CSV snapshot workspace could not be created.", nativeError);
        }

        if (UnixMakeDirectory(path, UnixPrivateDirectoryMode) == 0)
        {
            File.SetUnixFileMode(
                path,
                UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
            return;
        }

        int unixError = Marshal.GetLastPInvokeError();
        Exception unixNativeError = new Win32Exception(unixError);
        if (unixError == UnixAlreadyExists)
            throw new IOException("The CSV snapshot workspace candidate already exists.", unixNativeError);
        if (unixError is 1 or UnixPermissionDenied)
        {
            throw new UnauthorizedAccessException(
                "Access to the CSV snapshot workspace parent was denied.",
                unixNativeError);
        }

        throw new IOException("The CSV snapshot workspace could not be created.", unixNativeError);
    }

    [SupportedOSPlatform("windows")]
    private static byte[] CreatePrivateWindowsSecurityDescriptor()
    {
        using WindowsIdentity identity = WindowsIdentity.GetCurrent(TokenAccessLevels.Query);
        SecurityIdentifier owner = identity.User ?? throw new IOException(
            "The current Windows identity does not have a security identifier.");
        var security = new DirectorySecurity();
        security.SetOwner(owner);
        security.SetAccessRuleProtection(isProtected: true, preserveInheritance: false);
        security.AddAccessRule(new FileSystemAccessRule(
            owner,
            FileSystemRights.FullControl,
            InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit,
            PropagationFlags.None,
            AccessControlType.Allow));
        return security.GetSecurityDescriptorBinaryForm();
    }

    private static void CleanupFailedCandidate(string candidatePath, string markerPath)
    {
        try
        {
            File.Delete(markerPath);
        }
        catch (Exception exception) when (
            exception is FileNotFoundException or DirectoryNotFoundException)
        {
        }

        try
        {
            Directory.Delete(candidatePath, recursive: false);
        }
        catch (Exception exception) when (
            exception is IOException or UnauthorizedAccessException or DirectoryNotFoundException)
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

    [DllImport("libc", EntryPoint = "mkdir", SetLastError = true)]
    private static extern int UnixMakeDirectory(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string path,
        uint mode);

    private static StringComparer PathComparer { get; } =
        OperatingSystem.IsWindows() ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal;

    [StructLayout(LayoutKind.Sequential)]
    private struct SecurityAttributes
    {
        internal uint Length;
        internal IntPtr SecurityDescriptor;
        internal int InheritHandle;
    }
}
