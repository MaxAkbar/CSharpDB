using System.Runtime.InteropServices;
using System.Text;
using CSharpDB.Migration.Files.Json;

namespace CSharpDB.Migration.Files.Tests;

public sealed class JsonExportPathPreflightTests
{
    [Fact]
    public void ValidateSourcePath_AcceptsNormalizedOrdinaryFilePath()
    {
        using var workspace = new TemporaryDirectory();

        JsonExportPublisher.ValidateSourcePath(
            workspace.PathFor("snapshot.db"));
    }

    [Theory]
    [InlineData(".csharpdb-json-export-owned.prepared")]
    [InlineData(".CSHARPDB-JSON-EXPORT-owned.checkpoint")]
    public void ValidateSourcePath_RejectsReservedLeafInAnyParent(
        string leaf)
    {
        using var workspace = new TemporaryDirectory();
        string otherParent =
            workspace.CreateDirectory("other");

        Assert.Throws<ArgumentException>(
            () => JsonExportPublisher
                .ValidateSourcePath(
                    Path.Combine(
                        otherParent,
                        leaf)));
    }

    [Fact]
    public void ValidateSourcePath_RejectsWindowsTildeInAnySegment()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();

        Assert.Throws<ArgumentException>(
            () => JsonExportPublisher
                .ValidateSourcePath(
                    Path.Combine(
                        workspace.Root,
                        "SOURCE~1",
                        "snapshot.db")));
        Assert.Throws<ArgumentException>(
            () => JsonExportPublisher
                .ValidateSourcePath(
                    workspace.PathFor(
                        "SNAPSH~1.db")));
    }

    [Fact]
    public void ValidateSourcePath_RejectsActualWindowsShortNameWhenAvailable()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        string sourcePath =
            workspace.PathFor(
                "retained-database-snapshot.db");
        File.WriteAllBytes(
            sourcePath,
            [0]);
        var shortPathBuffer =
            new StringBuilder(512);
        uint length =
            GetShortPathNameW(
                sourcePath,
                shortPathBuffer,
                checked(
                    (uint)shortPathBuffer
                        .Capacity));
        if (length == 0)
            return;
        if (length >= shortPathBuffer.Capacity)
        {
            shortPathBuffer.EnsureCapacity(
                checked((int)length + 1));
            length =
                GetShortPathNameW(
                    sourcePath,
                    shortPathBuffer,
                    checked(
                        (uint)shortPathBuffer
                            .Capacity));
        }
        if (length == 0 ||
            length >= shortPathBuffer.Capacity ||
            !shortPathBuffer
                .ToString()
                .Contains('~'))
        {
            return;
        }

        Assert.Throws<ArgumentException>(
            () => JsonExportPublisher
                .ValidateSourcePath(
                    shortPathBuffer
                        .ToString()));
    }

    [Fact]
    public void ValidateSourcePath_PreservesLexicalSafetyChecks()
    {
        using var workspace = new TemporaryDirectory();

        Assert.Throws<ArgumentException>(
            () => JsonExportPublisher
                .ValidateSourcePath(
                    "relative.db"));
        Assert.Throws<ArgumentException>(
            () => JsonExportPublisher
                .ValidateSourcePath(
                    workspace.Root +
                    Path.DirectorySeparatorChar +
                    "." +
                    Path.DirectorySeparatorChar +
                    "snapshot.db"));
        Assert.Throws<ArgumentException>(
            () => JsonExportPublisher
                .ValidateSourcePath(
                    workspace.PathFor(
                        "invalid-\ud800.db")));
        Assert.Throws<ArgumentException>(
            () => JsonExportPublisher
                .ValidateSourcePath(
                    workspace.Root +
                    Path.DirectorySeparatorChar));

        if (!OperatingSystem.IsWindows())
            return;

        Assert.Throws<ArgumentException>(
            () => JsonExportPublisher
                .ValidateSourcePath(
                    workspace.PathFor(
                        "snapshot.db:stream")));
        Assert.Throws<ArgumentException>(
            () => JsonExportPublisher
                .ValidateSourcePath(
                    workspace.PathFor(
                        "NUL.db")));
    }

    private sealed class TemporaryDirectory :
        IDisposable
    {
        public TemporaryDirectory()
        {
            Root =
                Path.GetFullPath(
                    Path.Combine(
                        Path.GetTempPath(),
                        "csharpdb-json-path-preflight-tests",
                        Guid.NewGuid()
                            .ToString("N")));
            Directory.CreateDirectory(Root);
        }

        public string Root { get; }

        public string PathFor(
            string leaf) =>
            Path.Combine(
                Root,
                leaf);

        public string CreateDirectory(
            string leaf)
        {
            string path =
                PathFor(leaf);
            Directory.CreateDirectory(path);
            return path;
        }

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

    [DllImport(
        "kernel32.dll",
        CharSet = CharSet.Unicode,
        ExactSpelling = true,
        SetLastError = true)]
    private static extern uint
        GetShortPathNameW(
        string longPath,
        StringBuilder shortPath,
        uint shortPathLength);
}
