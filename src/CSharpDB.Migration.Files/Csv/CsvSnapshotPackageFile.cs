using System.ComponentModel;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace CSharpDB.Migration.Files.Csv;

/// <summary>Opens retained package files without following the final path component.</summary>
internal static class CsvSnapshotPackageFile
{
    private const uint GenericRead = 0x80000000;
    private const uint FileShareRead = 0x00000001;
    private const uint OpenExisting = 3;
    private const uint FileAttributeNormal = 0x00000080;
    private const uint FileFlagOpenReparsePoint = 0x00200000;
    private const uint FileFlagBackupSemantics = 0x02000000;
    private const uint FileFlagSequentialScan = 0x08000000;
    private const uint FileFlagOverlapped = 0x40000000;

    private const int ErrorFileNotFound = 2;
    private const int ErrorPathNotFound = 3;
    private const int ErrorAccessDenied = 5;
    private const int ErrorCantAccessFile = 1920;
    private const int ErrorInvalidReparseData = 4392;
    private const int ErrorReparseTagInvalid = 4393;
    private const int ErrorReparseTagMismatch = 4394;

    private const int UnixInterrupted = 4;
    private const int UnixNoEntry = 2;
    private const int UnixPermissionDenied = 13;
    private const int UnixNotDirectory = 20;
    private const int UnixIsDirectory = 21;
    private const int UnixNoDeviceOrAddress = 6;
    private const int UnixNoDevice = 19;

    private const int FSetFileDescriptor = 2;
    private const int CloseOnExec = 1;

    private const int LinuxNonBlock = 0x00000800;
    private const int LinuxNoFollow = 0x00020000;
    private const int LinuxCloseOnExec = 0x00080000;

    private const int DarwinNonBlock = 0x00000004;
    private const int DarwinNoFollow = 0x00000100;
    private const int DarwinCloseOnExec = 0x01000000;

    private const int FreeBsdNonBlock = 0x00000004;
    private const int FreeBsdNoFollow = 0x00000100;
    private const int FreeBsdCloseOnExec = 0x00100000;

    private const int FileTypeMask = 0xF000;
    private const int RegularFileType = 0x8000;

    private const FileAttributes UnsafeFileAttributes =
        FileAttributes.Directory | FileAttributes.ReparsePoint | FileAttributes.Device;

    internal static FileStream OpenReadNoFollow(string fullPath, int bufferSize)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(fullPath);
        if (fullPath.Contains('\0'))
            throw new ArgumentException("The package path cannot contain a null character.", nameof(fullPath));
        if (!Path.IsPathFullyQualified(fullPath))
            throw new ArgumentException("A fully qualified package path is required.", nameof(fullPath));
        if (bufferSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(bufferSize), bufferSize, "The buffer size must be positive.");

        SafeFileHandle? handle = null;
        try
        {
            handle = OperatingSystem.IsWindows()
                ? OpenWindows(fullPath)
                : OpenUnix(fullPath);

            ValidateRegularFile(handle);

            if (!OperatingSystem.IsWindows() &&
                SystemNativeFcntlSetIsNonBlocking(handle, isNonBlocking: 0) != 0)
            {
                throw CreateIoException(
                    "The CSV package file could not be prepared for reading.",
                    Marshal.GetLastPInvokeError());
            }

            FileStream stream = new(handle, FileAccess.Read, bufferSize);
            handle = null; // FileStream owns the handle after successful construction.
            return stream;
        }
        finally
        {
            handle?.Dispose();
        }
    }

    private static SafeFileHandle OpenWindows(string fullPath)
    {
        SafeFileHandle handle = CreateFileW(
            fullPath,
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
        throw CreateWindowsOpenException(error);
    }

    private static SafeFileHandle OpenUnix(string fullPath)
    {
        int flags = GetUnixOpenFlags();
        int descriptor;
        int error;

        do
        {
            descriptor = UnixOpen(fullPath, flags);
            error = Marshal.GetLastPInvokeError();
        }
        while (descriptor == -1 && error == UnixInterrupted);

        if (descriptor == -1)
            throw CreateUnixOpenException(error);

        try
        {
            // SafeFileHandle treats zero as invalid. Preserve the valid Unix descriptor
            // by moving it when the process happened to have standard input closed.
            if (descriptor == 0)
            {
                int duplicate = UnixDuplicate(descriptor);
                int duplicateError = Marshal.GetLastPInvokeError();
                _ = UnixClose(descriptor);
                descriptor = -1;

                if (duplicate == -1)
                    throw CreateIoException("The CSV package file could not be opened.", duplicateError);

                descriptor = duplicate;
                if (UnixFcntl(descriptor, FSetFileDescriptor, CloseOnExec) == -1)
                {
                    int fcntlError = Marshal.GetLastPInvokeError();
                    throw CreateIoException("The CSV package file could not be opened.", fcntlError);
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
            "Secure CSV package file opening is not implemented for this operating system.");
    }

    private static void ValidateRegularFile(SafeFileHandle handle)
    {
        try
        {
            FileAttributes attributes = File.GetAttributes(handle);
            if ((attributes & UnsafeFileAttributes) != 0)
                throw CreateUnsafePathException();

            if (!OperatingSystem.IsWindows())
            {
                if (SystemNativeFStat(handle, out UnixFileStatus status) != 0)
                {
                    throw CreateUnsafePathException(
                        new Win32Exception(Marshal.GetLastPInvokeError()));
                }

                if ((status.Mode & FileTypeMask) != RegularFileType)
                    throw CreateUnsafePathException();
            }

            _ = RandomAccess.GetLength(handle);
        }
        catch (CsvSnapshotPackageException)
        {
            throw;
        }
        catch (Exception exception) when (
            exception is IOException or
            NotSupportedException or
            UnauthorizedAccessException or
            ArgumentException)
        {
            throw CreateUnsafePathException(exception);
        }
    }

    private static Exception CreateWindowsOpenException(int error)
    {
        Exception nativeError = new Win32Exception(error);
        return error switch
        {
            ErrorFileNotFound => new FileNotFoundException("The CSV package file was not found."),
            ErrorPathNotFound => new DirectoryNotFoundException(
                "The CSV package parent directory was not found."),
            ErrorAccessDenied => new UnauthorizedAccessException(
                "Access to the CSV package file was denied.", nativeError),
            ErrorCantAccessFile or
            ErrorInvalidReparseData or
            ErrorReparseTagInvalid or
            ErrorReparseTagMismatch => CreateUnsafePathException(nativeError),
            _ => new IOException("The CSV package file could not be opened.", nativeError),
        };
    }

    private static Exception CreateUnixOpenException(int error)
    {
        Exception nativeError = new Win32Exception(error);
        if (error == UnixNoEntry)
            return new FileNotFoundException("The CSV package file was not found.");
        if (error == UnixNotDirectory)
        {
            return new DirectoryNotFoundException(
                "The CSV package parent directory was not found.");
        }
        if (error is 1 or UnixPermissionDenied)
        {
            return new UnauthorizedAccessException(
                "Access to the CSV package file was denied.", nativeError);
        }
        if (IsUnixSymbolicLinkError(error) ||
            error is UnixIsDirectory or UnixNoDeviceOrAddress or UnixNoDevice)
        {
            return CreateUnsafePathException(nativeError);
        }

        return new IOException("The CSV package file could not be opened.", nativeError);
    }

    private static bool IsUnixSymbolicLinkError(int error)
    {
        if (OperatingSystem.IsLinux() || OperatingSystem.IsAndroid())
            return error == 40;

        return error == 62; // ELOOP on Darwin and FreeBSD.
    }

    private static IOException CreateIoException(string message, int error) =>
        new(message, new Win32Exception(error));

    private static CsvSnapshotPackageException CreateUnsafePathException() =>
        new(
            CsvSnapshotPackageRules.UnsafePath,
            "The CSV package path must identify a regular file and cannot be a link or special file.");

    private static CsvSnapshotPackageException CreateUnsafePathException(Exception innerException) =>
        new(
            CsvSnapshotPackageRules.UnsafePath,
            "The CSV package path must identify a regular file and cannot be a link or special file.",
            innerException);

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
}
