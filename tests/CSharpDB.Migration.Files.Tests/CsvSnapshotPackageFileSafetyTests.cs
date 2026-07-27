using System.ComponentModel;
using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using CSharpDB.Migration.Files.Csv;

namespace CSharpDB.Migration.Files.Tests;

public sealed class CsvSnapshotPackageFileSafetyTests
{
    private const int BufferSize = 4096;
    private const uint OwnerReadWrite = 0x180; // 0600

    private static readonly MethodInfo OpenReadNoFollowMethod =
        typeof(CsvSnapshotPackage).Assembly
            .GetType(
                "CSharpDB.Migration.Files.Csv.CsvSnapshotPackageFile",
                throwOnError: true)!
            .GetMethod(
                "OpenReadNoFollow",
                BindingFlags.Static | BindingFlags.NonPublic,
                binder: null,
                types: [typeof(string), typeof(int)],
                modifiers: null)!;

    [Fact]
    public async Task RegularFileSupportsAsynchronousReads()
    {
        using var workspace = new TemporaryDirectory();
        string path = workspace.PathFor("regular.csdbcsv");
        byte[] expected = Enumerable.Range(0, 256)
            .Select(value => checked((byte)value))
            .ToArray();
        File.WriteAllBytes(path, expected);

        await using FileStream stream = OpenReadNoFollow(path);
        byte[] actual = new byte[expected.Length];
        await stream.ReadExactlyAsync(
            actual,
            TestContext.Current.CancellationToken);

        Assert.Equal(expected, actual);
        Assert.Equal(OperatingSystem.IsWindows(), stream.IsAsync);
    }

    [Fact]
    public void DirectoryHandleIsRejectedAsUnsafePath()
    {
        using var workspace = new TemporaryDirectory();

        CsvSnapshotPackageException error = AssertUnsafePath(workspace.Root);

        Assert.Equal(CsvSnapshotPackageRules.UnsafePath, error.RuleId);
    }

    [Fact]
    public void FinalComponentSymbolicLinkIsRejectedByTheOpenerWhereSupported()
    {
        using var workspace = new TemporaryDirectory();
        string targetPath = workspace.PathFor("target.csdbcsv");
        string linkPath = workspace.PathFor("link.csdbcsv");
        File.WriteAllBytes(targetPath, [1, 2, 3, 4]);
        if (!TryCreateSymbolicLink(linkPath, targetPath))
            return;

        // Invoke the handle opener directly so this proves the final-component
        // no-follow behavior independently of package parsing.
        CsvSnapshotPackageException error = AssertUnsafePath(linkPath);

        Assert.Equal(CsvSnapshotPackageRules.UnsafePath, error.RuleId);
        Assert.True(File.Exists(targetPath));
    }

    [Fact]
    public async Task UnixFifoIsRejectedPromptlyWithoutAWriter()
    {
        if (!IsSupportedUnix())
            return;

        using var workspace = new TemporaryDirectory();
        string fifoPath = workspace.PathFor("package.fifo");
        if (MakeFifo(fifoPath, OwnerReadWrite) != 0)
        {
            throw new Win32Exception(
                Marshal.GetLastPInvokeError(),
                "The test FIFO could not be created.");
        }

        Task<CsvSnapshotPackageException> openTask = Task.Run(
            () => AssertUnsafePath(fifoPath),
            CancellationToken.None);

        CsvSnapshotPackageException error = await openTask.WaitAsync(
            TimeSpan.FromSeconds(5),
            TestContext.Current.CancellationToken);

        Assert.Equal(CsvSnapshotPackageRules.UnsafePath, error.RuleId);
    }

    [Fact]
    public async Task UnixRenameRaceNeverReturnsTheSymbolicLinkTarget()
    {
        if (!IsSupportedUnix())
            return;

        using var workspace = new TemporaryDirectory();
        string targetPath = workspace.PathFor("target.csdbcsv");
        string candidatePath = workspace.PathFor("candidate.csdbcsv");
        string probePath = workspace.PathFor("probe.csdbcsv");
        byte[] targetBytes = Enumerable.Repeat((byte)0xA5, 64).ToArray();
        byte[] safeBytes = Enumerable.Repeat((byte)0x5A, 64).ToArray();
        File.WriteAllBytes(targetPath, targetBytes);
        File.WriteAllBytes(candidatePath, safeBytes);
        if (!TryCreateSymbolicLink(probePath, targetPath))
            return;
        File.Delete(probePath);

        using var stop = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken);
        var raceStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        Task attacker = Task.Run(
            () => SwapRegularFileAndSymbolicLink(
                workspace,
                candidatePath,
                targetPath,
                safeBytes,
                raceStarted,
                stop.Token),
            CancellationToken.None);

        try
        {
            await raceStarted.Task.WaitAsync(
                TimeSpan.FromSeconds(5),
                TestContext.Current.CancellationToken);

            for (int attempt = 0; attempt < 512; attempt++)
            {
                try
                {
                    using FileStream stream = OpenReadNoFollow(candidatePath);
                    byte[] observed = new byte[safeBytes.Length];
                    stream.ReadExactly(observed);
                    Assert.Equal(safeBytes, observed);
                }
                catch (CsvSnapshotPackageException exception)
                {
                    Assert.Equal(CsvSnapshotPackageRules.UnsafePath, exception.RuleId);
                }
                catch (FileNotFoundException)
                {
                    // A rename gap is safe: the opener did not return the target.
                }
                catch (DirectoryNotFoundException)
                {
                    // The isolated workspace remains present; tolerate native race mapping.
                }
                catch (IOException)
                {
                    // Sharing/transient rename failures are safe failures.
                }
            }
        }
        finally
        {
            stop.Cancel();
            await attacker.WaitAsync(TimeSpan.FromSeconds(5), CancellationToken.None);
        }
    }

    private static void SwapRegularFileAndSymbolicLink(
        TemporaryDirectory workspace,
        string candidatePath,
        string targetPath,
        byte[] safeBytes,
        TaskCompletionSource raceStarted,
        CancellationToken cancellationToken)
    {
        int generation = 0;
        while (!cancellationToken.IsCancellationRequested)
        {
            string regularStage = workspace.PathFor($"regular-{generation}.tmp");
            File.WriteAllBytes(regularStage, safeBytes);
            File.Move(regularStage, candidatePath, overwrite: true);

            string linkStage = workspace.PathFor($"link-{generation}.tmp");
            File.CreateSymbolicLink(linkStage, targetPath);
            File.Move(linkStage, candidatePath, overwrite: true);
            raceStarted.TrySetResult();
            generation++;
        }
    }

    private static CsvSnapshotPackageException AssertUnsafePath(string path)
    {
        return Assert.Throws<CsvSnapshotPackageException>(
            () => OpenReadNoFollow(path).Dispose());
    }

    private static FileStream OpenReadNoFollow(string path)
    {
        try
        {
            return (FileStream)OpenReadNoFollowMethod.Invoke(
                obj: null,
                parameters: [Path.GetFullPath(path), BufferSize])!;
        }
        catch (TargetInvocationException exception) when (exception.InnerException is not null)
        {
            ExceptionDispatchInfo.Capture(exception.InnerException).Throw();
            throw new InvalidOperationException("The reflected opener unexpectedly returned.");
        }
    }

    private static bool TryCreateSymbolicLink(string linkPath, string targetPath)
    {
        try
        {
            File.CreateSymbolicLink(linkPath, targetPath);
            return true;
        }
        catch (Exception exception) when (
            exception is PlatformNotSupportedException or
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

    [DllImport("libc", EntryPoint = "mkfifo", SetLastError = true)]
    private static extern int MakeFifo(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string path,
        uint mode);

    private sealed class TemporaryDirectory : IDisposable
    {
        internal TemporaryDirectory()
        {
            Root = Path.Combine(
                Path.GetTempPath(),
                $"csharpdb-package-file-safety-{Guid.NewGuid():N}");
            Directory.CreateDirectory(Root);
        }

        internal string Root { get; }

        internal string PathFor(string fileName) => Path.Combine(Root, fileName);

        public void Dispose()
        {
            if (Directory.Exists(Root))
                Directory.Delete(Root, recursive: true);
        }
    }
}
