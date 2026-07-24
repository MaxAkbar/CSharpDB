using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Text;
using CSharpDB.Migration.Files.Json;

namespace CSharpDB.Migration.Files.Tests;

public sealed class JsonTypedIntentSidecarFileSafetyTests
{
    private const uint OwnerReadWrite = 0x180; // 0600

    private static CancellationToken Cancellation =>
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task RegularFileOpensWithoutChangingCanonicalBytes()
    {
        using var workspace = new TemporaryDirectory();
        await using BoundSource source =
            await CreateSourceAsync(workspace);
        string path = workspace.PathFor(
            "regular" +
            JsonTypedIntentSidecar.FileExtension);
        JsonTypedIntentManifest written =
            await WriteAsync(path, source.Binding);
        byte[] before = await File.ReadAllBytesAsync(
            path,
            Cancellation);

        JsonTypedIntentManifest opened =
            await JsonTypedIntentSidecar.OpenAsync(
                path,
                source.Binding,
                new JsonTypedIntentOpenOptions
                {
                    ExpectedManifestDigest =
                        written.ManifestDigest,
                },
                Cancellation);

        Assert.Equal(
            written.ManifestDigest,
            opened.ManifestDigest);
        Assert.Equal(
            before,
            await File.ReadAllBytesAsync(
                path,
                Cancellation));
    }

    [Fact]
    public async Task DirectoryIsRejectedAsUnsafePath()
    {
        using var workspace = new TemporaryDirectory();
        await using BoundSource source =
            await CreateSourceAsync(workspace);

        JsonTypedIntentException error =
            await AssertUnsafePathAsync(
                workspace.Root,
                source.Binding);

        Assert.Equal(
            JsonTypedIntentRules.UnsafePath,
            error.RuleId);
    }

    [Fact]
    public async Task FinalComponentSymbolicLinkIsRejectedWhereSupported()
    {
        using var workspace = new TemporaryDirectory();
        await using BoundSource source =
            await CreateSourceAsync(workspace);
        string targetPath = workspace.PathFor(
            "target" +
            JsonTypedIntentSidecar.FileExtension);
        string linkPath = workspace.PathFor(
            "link" +
            JsonTypedIntentSidecar.FileExtension);
        JsonTypedIntentManifest target =
            await WriteAsync(
                targetPath,
                source.Binding);
        byte[] targetBytes =
            await File.ReadAllBytesAsync(
                targetPath,
                Cancellation);
        if (!TryCreateSymbolicLink(
                linkPath,
                targetPath))
        {
            return;
        }

        JsonTypedIntentException error =
            await AssertUnsafePathAsync(
                linkPath,
                source.Binding);

        Assert.Equal(
            JsonTypedIntentRules.UnsafePath,
            error.RuleId);
        Assert.Equal(
            target.ManifestDigest,
            Digest(targetBytes));
        Assert.Equal(
            targetBytes,
            await File.ReadAllBytesAsync(
                targetPath,
                Cancellation));
    }

    [Fact]
    public async Task UnixFifoIsRejectedPromptlyWithoutAWriter()
    {
        if (!IsSupportedUnix())
            return;

        using var workspace = new TemporaryDirectory();
        await using BoundSource source =
            await CreateSourceAsync(workspace);
        string fifoPath = workspace.PathFor(
            "intent.fifo");
        if (MakeFifo(
                fifoPath,
                OwnerReadWrite) != 0)
        {
            throw new Win32Exception(
                Marshal.GetLastPInvokeError(),
                "The typed intent test FIFO could not be created.");
        }

        Task<JsonTypedIntentException> openTask =
            Task.Run(
                () => AssertUnsafePathAsync(
                        fifoPath,
                        source.Binding)
                    .GetAwaiter()
                    .GetResult(),
                CancellationToken.None);
        JsonTypedIntentException error =
            await openTask.WaitAsync(
                TimeSpan.FromSeconds(5),
                Cancellation);

        Assert.Equal(
            JsonTypedIntentRules.UnsafePath,
            error.RuleId);
    }

    [Fact]
    public async Task UnixDeviceIsRejectedAsUnsafePath()
    {
        if (!IsSupportedUnix() ||
            !Path.Exists("/dev/null"))
        {
            return;
        }

        using var workspace = new TemporaryDirectory();
        await using BoundSource source =
            await CreateSourceAsync(workspace);
        JsonTypedIntentException error =
            await AssertUnsafePathAsync(
                "/dev/null",
                source.Binding);

        Assert.Equal(
            JsonTypedIntentRules.UnsafePath,
            error.RuleId);
    }

    [Fact]
    public async Task ExistingDestinationIsPreservedAndNoTemporaryFileRemains()
    {
        using var workspace = new TemporaryDirectory();
        await using BoundSource source =
            await CreateSourceAsync(workspace);
        string destination = workspace.PathFor(
            "existing" +
            JsonTypedIntentSidecar.FileExtension);
        byte[] sentinel =
            "existing-sidecar-must-survive"u8.ToArray();
        await File.WriteAllBytesAsync(
            destination,
            sentinel,
            Cancellation);
        string[] before = Directory
            .GetFileSystemEntries(workspace.Root);

        await Assert.ThrowsAnyAsync<IOException>(
            async () =>
                await WriteAsync(
                    destination,
                    source.Binding));

        Assert.Equal(
            sentinel,
            await File.ReadAllBytesAsync(
                destination,
                Cancellation));
        Assert.Equal(
            before.Order(StringComparer.Ordinal),
            Directory
                .GetFileSystemEntries(workspace.Root)
                .Order(StringComparer.Ordinal));
        Assert.DoesNotContain(
            Directory.GetFileSystemEntries(
                workspace.Root),
            item => Path.GetFileName(item)
                .StartsWith(
                    ".csdbjson-intent-",
                    StringComparison.Ordinal));
    }

    private static async Task<JsonTypedIntentManifest>
        WriteAsync(
            string path,
            JsonSourceBinding binding) =>
        await JsonTypedIntentSidecar.WriteAsync(
            path,
            binding,
            new JsonTypedIntentOptions
            {
                Columns =
                [
                    new JsonTypedColumnIntent
                    {
                        ColumnIndex = 0,
                        ExpectedPropertyName = "value",
                        Codec =
                            JsonTypedValueCodec.Int64String,
                    },
                ],
                MaxDecodedBinaryBytes = 1024,
                MaxDecimalDigits = 1024,
            },
            Cancellation);

    private static async Task<JsonTypedIntentException>
        AssertUnsafePathAsync(
            string path,
            JsonSourceBinding binding) =>
        await Assert.ThrowsAsync<
            JsonTypedIntentException>(
            async () =>
                await JsonTypedIntentSidecar.OpenAsync(
                    path,
                    binding,
                    cancellationToken: Cancellation));

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

    private static string Digest(
        ReadOnlySpan<byte> bytes) =>
        "sha256:" +
        Convert.ToHexString(
                System.Security.Cryptography
                    .SHA256.HashData(bytes))
            .ToLowerInvariant();

    private static async ValueTask<BoundSource>
        CreateSourceAsync(
            TemporaryDirectory workspace)
    {
        using var stream = new MemoryStream(
            Encoding.UTF8.GetBytes(
                """[{"value":"1"}]"""),
            writable: false);
        JsonSourceSnapshot snapshot =
            await JsonSourceSnapshot.CreateAsync(
                stream,
                new JsonSourceSnapshotOptions
                {
                    WorkspacePath = workspace.Root,
                    MaxSourceBytes = 1024 * 1024,
                    LeaveOpen = true,
                },
                Cancellation);
        try
        {
            JsonSourceBinding binding =
                await JsonSourceBinding.CreateAsync(
                    snapshot,
                    new JsonStreamingReaderOptions
                    {
                        Framing =
                            JsonInputFraming.RootArray,
                        MaxValueBytes = 64 * 1024,
                        MaxDepth = 16,
                        MaxPropertiesPerObject = 32,
                        MaxArrayElements = 32,
                        MaxTotalNodes = 128,
                        MaxPropertyNameBytes = 1024,
                        MaxStringBytes = 16 * 1024,
                        MaxNumberBytes = 1024,
                    },
                    logicalSourceIdentity:
                        "typed/file-safety",
                    Cancellation);
            return new BoundSource(snapshot, binding);
        }
        catch
        {
            await snapshot.DisposeAsync();
            throw;
        }
    }

    [DllImport(
        "libc",
        EntryPoint = "mkfifo",
        SetLastError = true)]
    private static extern int MakeFifo(
        [MarshalAs(UnmanagedType.LPUTF8Str)]
        string path,
        uint mode);

    private sealed class BoundSource(
        JsonSourceSnapshot snapshot,
        JsonSourceBinding binding)
        : IAsyncDisposable
    {
        internal JsonSourceSnapshot Snapshot { get; } =
            snapshot;

        internal JsonSourceBinding Binding { get; } =
            binding;

        public ValueTask DisposeAsync() =>
            Snapshot.DisposeAsync();
    }

    private sealed class TemporaryDirectory :
        IDisposable
    {
        internal TemporaryDirectory()
        {
            Root = Path.Combine(
                Path.GetTempPath(),
                "csharpdb-json-intent-file-safety-" +
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
