using System.ComponentModel;
using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using CSharpDB.Migration.Files.Json;

namespace CSharpDB.Migration.Files.Tests;

public sealed class JsonSnapshotPackageFileSafetyTests
{
    private const int BufferSize = 4096;
    private const uint OwnerReadWrite = 0x180; // 0600

    private static readonly MethodInfo
        OpenReadNoFollowMethod =
            typeof(JsonSnapshotPackage).Assembly
                .GetType(
                    "CSharpDB.Migration.Files.Json.JsonSnapshotPackageFile",
                    throwOnError: true)!
                .GetMethod(
                    "OpenReadNoFollow",
                    BindingFlags.Static |
                    BindingFlags.NonPublic,
                    binder: null,
                    types:
                    [
                        typeof(string),
                        typeof(int),
                    ],
                    modifiers: null)!;

    [Fact]
    public async Task RegularFileSupportsAsynchronousReadsWithoutChangingItsBytes()
    {
        using var workspace = new TemporaryDirectory();
        string path = workspace.PathFor(
            "regular" +
            JsonSnapshotPackage.FileExtension);
        byte[] expected =
            Enumerable.Range(0, 256)
                .Select(value => checked((byte)value))
                .ToArray();
        File.WriteAllBytes(path, expected);

        await using FileStream stream =
            OpenReadNoFollow(path);
        byte[] actual = new byte[expected.Length];
        await stream.ReadExactlyAsync(
            actual,
            TestContext.Current.CancellationToken);

        Assert.Equal(expected, actual);
        Assert.Equal(
            OperatingSystem.IsWindows(),
            stream.IsAsync);
        Assert.Equal(
            expected,
            File.ReadAllBytes(path));
    }

    [Fact]
    public void DirectoryHandleIsRejectedAsUnsafePath()
    {
        using var workspace = new TemporaryDirectory();

        JsonSnapshotPackageException error =
            AssertUnsafePath(workspace.Root);

        Assert.Equal(
            JsonSnapshotPackageRules.UnsafePath,
            error.RuleId);
    }

    [Fact]
    public void FinalComponentSymbolicLinkIsRejectedWhereSupported()
    {
        using var workspace = new TemporaryDirectory();
        string targetPath = workspace.PathFor(
            "target" +
            JsonSnapshotPackage.FileExtension);
        string linkPath = workspace.PathFor(
            "link" +
            JsonSnapshotPackage.FileExtension);
        byte[] targetBytes = [1, 2, 3, 4];
        File.WriteAllBytes(targetPath, targetBytes);
        if (!TryCreateSymbolicLink(
                linkPath,
                targetPath))
        {
            return;
        }

        JsonSnapshotPackageException error =
            AssertUnsafePath(linkPath);

        Assert.Equal(
            JsonSnapshotPackageRules.UnsafePath,
            error.RuleId);
        Assert.Equal(
            targetBytes,
            File.ReadAllBytes(targetPath));
    }

    [Fact]
    public async Task UnixFifoIsRejectedPromptlyWithoutAWriter()
    {
        if (!IsSupportedUnix())
            return;

        using var workspace = new TemporaryDirectory();
        string fifoPath = workspace.PathFor(
            "package.fifo");
        if (MakeFifo(
                fifoPath,
                OwnerReadWrite) != 0)
        {
            throw new Win32Exception(
                Marshal.GetLastPInvokeError(),
                "The test FIFO could not be created.");
        }

        Task<JsonSnapshotPackageException> openTask =
            Task.Run(
                () => AssertUnsafePath(fifoPath),
                CancellationToken.None);
        JsonSnapshotPackageException error =
            await openTask.WaitAsync(
                TimeSpan.FromSeconds(5),
                TestContext.Current.CancellationToken);

        Assert.Equal(
            JsonSnapshotPackageRules.UnsafePath,
            error.RuleId);
    }

    [Fact]
    public void UnixDeviceIsRejectedAsUnsafePath()
    {
        if (!IsSupportedUnix() ||
            !Path.Exists("/dev/null"))
        {
            return;
        }

        JsonSnapshotPackageException error =
            AssertUnsafePath("/dev/null");

        Assert.Equal(
            JsonSnapshotPackageRules.UnsafePath,
            error.RuleId);
    }

    private static JsonSnapshotPackageException
        AssertUnsafePath(string path) =>
        Assert.Throws<JsonSnapshotPackageException>(
            () => OpenReadNoFollow(path).Dispose());

    private static FileStream OpenReadNoFollow(
        string path)
    {
        try
        {
            return (FileStream)
                OpenReadNoFollowMethod.Invoke(
                    obj: null,
                    parameters:
                    [
                        Path.GetFullPath(path),
                        BufferSize,
                    ])!;
        }
        catch (TargetInvocationException exception)
            when (exception.InnerException is not null)
        {
            ExceptionDispatchInfo
                .Capture(exception.InnerException)
                .Throw();
            throw new InvalidOperationException(
                "The reflected JSON package opener unexpectedly returned.");
        }
    }

    private static bool TryCreateSymbolicLink(
        string linkPath,
        string targetPath)
    {
        try
        {
            File.CreateSymbolicLink(
                linkPath,
                targetPath);
            return true;
        }
        catch (Exception exception) when (
            exception is
                PlatformNotSupportedException or
                UnauthorizedAccessException or
                IOException)
        {
            return false;
        }
    }

    private static bool IsSupportedUnix() =>
        OperatingSystem.IsLinux() ||
        OperatingSystem.IsAndroid() ||
        OperatingSystem.IsMacOS() ||
        OperatingSystem.IsIOS() ||
        OperatingSystem.IsTvOS() ||
        OperatingSystem.IsMacCatalyst() ||
        OperatingSystem.IsFreeBSD();

    [DllImport(
        "libc",
        EntryPoint = "mkfifo",
        SetLastError = true)]
    private static extern int MakeFifo(
        [MarshalAs(UnmanagedType.LPUTF8Str)]
        string path,
        uint mode);

    private sealed class TemporaryDirectory :
        IDisposable
    {
        internal TemporaryDirectory()
        {
            Root = Path.Combine(
                Path.GetTempPath(),
                "csharpdb-json-package-file-safety-" +
                Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(Root);
        }

        internal string Root { get; }

        internal string PathFor(string fileName) =>
            Path.Combine(Root, fileName);

        public void Dispose()
        {
            if (Directory.Exists(Root))
            {
                Directory.Delete(
                    Root,
                    recursive: true);
            }
        }
    }
}
