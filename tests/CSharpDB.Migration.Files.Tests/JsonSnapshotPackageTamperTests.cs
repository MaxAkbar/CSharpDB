using System.Buffers.Binary;
using System.Text;
using CSharpDB.Migration;
using CSharpDB.Migration.Files.Json;

namespace CSharpDB.Migration.Files.Tests;

public sealed class JsonSnapshotPackageTamperTests
{
    private const int HeaderSize = 64;
    private const int ManifestLengthOffset = 16;

    private static string TargetVersion =>
        CSharpDbCapabilityCatalogLoader.CurrentTargetVersion;

    private static CancellationToken Cancellation =>
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task ExistingDestinationIsNeverOverwritten()
    {
        using var workspace = new TemporaryDirectory();
        string sourcePath = Path.Combine(
            workspace.Root,
            "existing-source.json");
        string packagePath = Path.Combine(
            workspace.Root,
            "existing" + JsonSnapshotPackage.FileExtension);
        await WriteTextAsync(
            sourcePath,
            """[{"id":1},{"id":2}]""");
        byte[] existingBytes =
            "do-not-overwrite"u8.ToArray();
        await File.WriteAllBytesAsync(
            packagePath,
            existingBytes,
            Cancellation);

        await using JsonSourceSnapshot snapshot =
            await CreateSnapshotAsync(
                sourcePath,
                workspace.Root);
        JsonTableSchemaInferenceResult schema =
            await InferAsync(snapshot);

        await Assert.ThrowsAnyAsync<IOException>(
            async () => await JsonSnapshotPackage.WriteAsync(
                packagePath,
                snapshot,
                schema,
                TargetVersion,
                Cancellation));

        Assert.Equal(
            existingBytes,
            await File.ReadAllBytesAsync(
                packagePath,
                Cancellation));
        Assert.Empty(TemporaryPackageFiles(workspace.Root));
        await snapshot.VerifyIntegrityAsync(Cancellation);
    }

    [Fact]
    public async Task PreCanceledWriteLeavesNoDestinationOrTemporaryFile()
    {
        using var workspace = new TemporaryDirectory();
        string sourcePath = Path.Combine(
            workspace.Root,
            "canceled-source.json");
        string packagePath = Path.Combine(
            workspace.Root,
            "canceled" + JsonSnapshotPackage.FileExtension);
        await WriteTextAsync(
            sourcePath,
            """[{"id":1},{"id":2}]""");
        await using JsonSourceSnapshot snapshot =
            await CreateSnapshotAsync(
                sourcePath,
                workspace.Root);
        JsonTableSchemaInferenceResult schema =
            await InferAsync(snapshot);
        using var canceled = new CancellationTokenSource();
        canceled.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await JsonSnapshotPackage.WriteAsync(
                packagePath,
                snapshot,
                schema,
                TargetVersion,
                canceled.Token));

        Assert.False(File.Exists(packagePath));
        Assert.Empty(TemporaryPackageFiles(workspace.Root));
        await snapshot.VerifyIntegrityAsync(Cancellation);
    }

    [Fact]
    public async Task ExpectedManifestDigestPinsBeforeManifestOrWorkspaceUse()
    {
        using var workspace = new TemporaryDirectory();
        PackageOrigin origin = await WritePackageAsync(
            workspace,
            "digest-pin");
        byte[] originalPackage =
            await File.ReadAllBytesAsync(
                origin.PackagePath,
                Cancellation);
        File.Delete(origin.SourcePath);

        await using (JsonSnapshotPackageSession matching =
            await OpenAsync(
                origin.PackagePath,
                workspace.Root,
                origin.Manifest.ManifestDigest))
        {
            Assert.Equal(
                origin.Manifest.ManifestDigest,
                matching.Manifest.ManifestDigest);
        }
        Assert.Empty(
            Directory.EnumerateDirectories(workspace.Root));

        string wrongDigest = DifferentDigest(
            origin.Manifest.ManifestDigest);
        JsonSnapshotPackageException mismatch =
            await Assert.ThrowsAsync<JsonSnapshotPackageException>(
                async () => await OpenAsync(
                    origin.PackagePath,
                    workspace.Root,
                    wrongDigest));
        Assert.Equal(
            JsonSnapshotPackageRules.IntegrityMismatch,
            mismatch.RuleId);
        Assert.Contains(
            "trusted manifest digest",
            mismatch.Message,
            StringComparison.OrdinalIgnoreCase);
        Assert.Empty(
            Directory.EnumerateDirectories(workspace.Root));

        // A corrupt manifest must still lose to an independently wrong pin.
        byte[] tampered = originalPackage.ToArray();
        tampered[HeaderSize] ^= 0x01;
        await File.WriteAllBytesAsync(
            origin.PackagePath,
            tampered,
            Cancellation);
        JsonSnapshotPackageException ordered =
            await Assert.ThrowsAsync<JsonSnapshotPackageException>(
                async () => await OpenAsync(
                    origin.PackagePath,
                    workspace.Root,
                    wrongDigest));
        Assert.Equal(
            JsonSnapshotPackageRules.IntegrityMismatch,
            ordered.RuleId);
        Assert.Contains(
            "trusted manifest digest",
            ordered.Message,
            StringComparison.OrdinalIgnoreCase);
        Assert.Empty(
            Directory.EnumerateDirectories(workspace.Root));
        Assert.Equal(
            tampered,
            await File.ReadAllBytesAsync(
                origin.PackagePath,
                Cancellation));
    }

    [Fact]
    public async Task ExpectedManifestDigestRejectsNoncanonicalTextBeforeIo()
    {
        using var workspace = new TemporaryDirectory();
        PackageOrigin origin = await WritePackageAsync(
            workspace,
            "digest-invalid");
        File.Delete(origin.SourcePath);
        string[] invalidDigests =
        [
            string.Empty,
            " ",
            new string('a', 64),
            "sha256:" + new string('A', 64),
            "sha256:" + new string('g', 64),
            "sha256:" + new string('a', 63),
            "sha512:" + new string('a', 64),
        ];

        foreach (string digest in invalidDigests)
        {
            ArgumentException error =
                await Assert.ThrowsAsync<ArgumentException>(
                    async () => await OpenAsync(
                        origin.PackagePath,
                        workspace.Root,
                        digest));
            Assert.Equal("options", error.ParamName);
            Assert.Empty(
                Directory.EnumerateDirectories(
                    workspace.Root));
        }

        Assert.True(File.Exists(origin.PackagePath));
    }

    [Fact]
    public async Task ManifestBitFlipIsRejectedWithoutPackageMutation()
    {
        using var workspace = new TemporaryDirectory();
        PackageOrigin origin = await WritePackageAsync(
            workspace,
            "manifest-bit-flip");
        File.Delete(origin.SourcePath);
        byte[] bytes = await File.ReadAllBytesAsync(
            origin.PackagePath,
            Cancellation);
        int manifestLength = ReadManifestLength(bytes);
        bytes[
            checked(
                HeaderSize +
                Math.Max(0, manifestLength / 2))] ^= 0x01;
        await File.WriteAllBytesAsync(
            origin.PackagePath,
            bytes,
            Cancellation);

        await AssertOpenFailsAndPreservesAsync(
            origin.PackagePath,
            workspace.Root,
            JsonSnapshotPackageRules.IntegrityMismatch);
    }

    [Fact]
    public async Task RawSnapshotBitFlipIsRejectedWithoutPackageMutation()
    {
        using var workspace = new TemporaryDirectory();
        PackageOrigin origin = await WritePackageAsync(
            workspace,
            "raw-bit-flip");
        File.Delete(origin.SourcePath);
        byte[] bytes = await File.ReadAllBytesAsync(
            origin.PackagePath,
            Cancellation);
        int rawOffset = checked(
            HeaderSize + ReadManifestLength(bytes));
        Assert.True(rawOffset < bytes.Length);
        bytes[
            checked(
                rawOffset +
                ((bytes.Length - rawOffset) / 2))] ^= 0x01;
        await File.WriteAllBytesAsync(
            origin.PackagePath,
            bytes,
            Cancellation);

        await AssertOpenFailsAndPreservesAsync(
            origin.PackagePath,
            workspace.Root,
            JsonSnapshotPackageRules.IntegrityMismatch);
    }

    [Fact]
    public async Task TruncatedPackageIsRejectedWithoutPackageMutation()
    {
        using var workspace = new TemporaryDirectory();
        PackageOrigin origin = await WritePackageAsync(
            workspace,
            "truncated");
        File.Delete(origin.SourcePath);
        byte[] complete = await File.ReadAllBytesAsync(
            origin.PackagePath,
            Cancellation);
        Assert.True(complete.Length > HeaderSize);
        byte[] truncated = complete[..^1];
        await File.WriteAllBytesAsync(
            origin.PackagePath,
            truncated,
            Cancellation);

        await AssertOpenFailsAndPreservesAsync(
            origin.PackagePath,
            workspace.Root,
            JsonSnapshotPackageRules.InvalidFormat);
    }

    [Fact]
    public async Task AppendedPackageByteIsRejectedWithoutPackageMutation()
    {
        using var workspace = new TemporaryDirectory();
        PackageOrigin origin = await WritePackageAsync(
            workspace,
            "appended");
        File.Delete(origin.SourcePath);
        byte[] complete = await File.ReadAllBytesAsync(
            origin.PackagePath,
            Cancellation);
        byte[] appended =
        [
            .. complete,
            0x00,
        ];
        await File.WriteAllBytesAsync(
            origin.PackagePath,
            appended,
            Cancellation);

        await AssertOpenFailsAndPreservesAsync(
            origin.PackagePath,
            workspace.Root,
            JsonSnapshotPackageRules.InvalidFormat);
    }

    [Fact]
    public async Task OpenAcceptsExactSourceLimitAndRejectsOneUnder()
    {
        using var workspace = new TemporaryDirectory();
        PackageOrigin origin = await WritePackageAsync(
            workspace,
            "source-limit");
        File.Delete(origin.SourcePath);
        byte[] original = await File.ReadAllBytesAsync(
            origin.PackagePath,
            Cancellation);

        await using (JsonSnapshotPackageSession exact =
            await JsonSnapshotPackage.OpenAsync(
                origin.PackagePath,
                new JsonSnapshotPackageOpenOptions
                {
                    WorkspacePath = workspace.Root,
                    MaxSourceBytes =
                        origin.Manifest.ContentLength,
                    ExpectedManifestDigest =
                        origin.Manifest.ManifestDigest,
                },
                Cancellation))
        {
            Assert.Equal(
                origin.Manifest.ContentLength,
                exact.Manifest.ContentLength);
        }
        Assert.Empty(
            Directory.EnumerateDirectories(workspace.Root));

        JsonSnapshotPackageException error =
            await Assert.ThrowsAsync<JsonSnapshotPackageException>(
                async () => await JsonSnapshotPackage.OpenAsync(
                    origin.PackagePath,
                    new JsonSnapshotPackageOpenOptions
                    {
                        WorkspacePath = workspace.Root,
                        MaxSourceBytes =
                            origin.Manifest.ContentLength - 1,
                        ExpectedManifestDigest =
                            origin.Manifest.ManifestDigest,
                    },
                    Cancellation));
        Assert.Equal(
            JsonSnapshotPackageRules.SizeLimitExceeded,
            error.RuleId);
        Assert.Empty(
            Directory.EnumerateDirectories(workspace.Root));
        Assert.Equal(
            original,
            await File.ReadAllBytesAsync(
                origin.PackagePath,
                Cancellation));
    }

    [Fact]
    public async Task InvalidOpenLimitsFailBeforePackageOrWorkspaceUse()
    {
        using var workspace = new TemporaryDirectory();
        PackageOrigin origin = await WritePackageAsync(
            workspace,
            "invalid-options");
        File.Delete(origin.SourcePath);
        byte[] original = await File.ReadAllBytesAsync(
            origin.PackagePath,
            Cancellation);
        JsonSnapshotPackageOpenOptions[] invalid =
        [
            new()
            {
                WorkspacePath = workspace.Root,
                MaxSourceBytes = -1,
            },
            new()
            {
                WorkspacePath = workspace.Root,
                MaxSourceBytes = long.MaxValue,
            },
            new()
            {
                WorkspacePath = workspace.Root,
                CopyBufferBytes = 4_095,
            },
            new()
            {
                WorkspacePath = workspace.Root,
                CopyBufferBytes =
                    (16 * 1024 * 1024) + 1,
            },
            new()
            {
                WorkspacePath = " ",
            },
        ];

        foreach (JsonSnapshotPackageOpenOptions options in
                 invalid)
        {
            ArgumentException error =
                await Assert.ThrowsAnyAsync<ArgumentException>(
                    async () => await JsonSnapshotPackage.OpenAsync(
                        origin.PackagePath,
                        options,
                        Cancellation));
            Assert.Equal("options", error.ParamName);
            Assert.Empty(
                Directory.EnumerateDirectories(
                    workspace.Root));
        }

        Assert.Equal(
            original,
            await File.ReadAllBytesAsync(
                origin.PackagePath,
                Cancellation));
    }

    [Fact]
    public async Task PreCanceledOpenLeavesPackageAndWorkspaceUnchanged()
    {
        using var workspace = new TemporaryDirectory();
        PackageOrigin origin = await WritePackageAsync(
            workspace,
            "open-canceled");
        File.Delete(origin.SourcePath);
        byte[] original = await File.ReadAllBytesAsync(
            origin.PackagePath,
            Cancellation);
        using var canceled = new CancellationTokenSource();
        canceled.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await JsonSnapshotPackage.OpenAsync(
                origin.PackagePath,
                new JsonSnapshotPackageOpenOptions
                {
                    WorkspacePath = workspace.Root,
                    MaxSourceBytes = 1024 * 1024,
                    ExpectedManifestDigest =
                        origin.Manifest.ManifestDigest,
                },
                canceled.Token));

        Assert.Empty(
            Directory.EnumerateDirectories(workspace.Root));
        Assert.Equal(
            original,
            await File.ReadAllBytesAsync(
                origin.PackagePath,
                Cancellation));
    }

    private static async Task AssertOpenFailsAndPreservesAsync(
        string packagePath,
        string workspacePath,
        string expectedRule)
    {
        byte[] before = await File.ReadAllBytesAsync(
            packagePath,
            Cancellation);
        JsonSnapshotPackageException error =
            await Assert.ThrowsAsync<JsonSnapshotPackageException>(
                async () =>
                {
                    await using JsonSnapshotPackageSession session =
                        await JsonSnapshotPackage.OpenAsync(
                            packagePath,
                            new JsonSnapshotPackageOpenOptions
                            {
                                WorkspacePath = workspacePath,
                                MaxSourceBytes =
                                    1024 * 1024,
                            },
                            Cancellation);
                });
        Assert.Equal(expectedRule, error.RuleId);
        Assert.Equal(
            before,
            await File.ReadAllBytesAsync(
                packagePath,
                Cancellation));
        Assert.Empty(
            Directory.EnumerateDirectories(workspacePath));
    }

    private static async ValueTask<PackageOrigin>
        WritePackageAsync(
            TemporaryDirectory workspace,
            string name)
    {
        string sourcePath = Path.Combine(
            workspace.Root,
            name + ".json");
        string packagePath = Path.Combine(
            workspace.Root,
            name + JsonSnapshotPackage.FileExtension);
        await WriteTextAsync(
            sourcePath,
            """
            [
              {"id":1,"name":"alpha","payload":{"z":1.2300E+004,"a":null}},
              {"id":2,"name":"bravo","payload":[true,"x"]},
              {"id":3,"name":"charlie","payload":{"emoji":"😀"}}
            ]
            """);

        JsonSourceSnapshot snapshot =
            await CreateSnapshotAsync(
                sourcePath,
                workspace.Root);
        try
        {
            JsonTableSchemaInferenceResult schema =
                await InferAsync(snapshot);
            JsonSnapshotPackageManifest manifest =
                await JsonSnapshotPackage.WriteAsync(
                    packagePath,
                    snapshot,
                    schema,
                    TargetVersion,
                    Cancellation);
            return new PackageOrigin(
                sourcePath,
                packagePath,
                manifest);
        }
        finally
        {
            await snapshot.DisposeAsync();
        }
    }

    private static async ValueTask<JsonSourceSnapshot>
        CreateSnapshotAsync(
            string sourcePath,
            string workspacePath) =>
        await JsonSourceSnapshot.CreateFromFileAsync(
            sourcePath,
            new JsonSourceSnapshotOptions
            {
                WorkspacePath = workspacePath,
                MaxSourceBytes = 1024 * 1024,
            },
            Cancellation);

    private static async ValueTask<
        JsonTableSchemaInferenceResult> InferAsync(
            JsonSourceSnapshot snapshot)
    {
        JsonSourceBinding binding =
            await JsonSourceBinding.CreateAsync(
                snapshot,
                new JsonStreamingReaderOptions
                {
                    Framing = JsonInputFraming.RootArray,
                    MaxValueBytes = 256 * 1024,
                    MaxDepth = 32,
                    MaxPropertiesPerObject = 256,
                    MaxArrayElements = 1_024,
                    MaxTotalNodes = 2_048,
                    MaxPropertyNameBytes = 8 * 1_024,
                    MaxStringBytes = 128 * 1_024,
                    MaxNumberBytes = 8 * 1_024,
                },
                logicalSourceIdentity:
                    "tamper/package-source",
                cancellationToken: Cancellation);
        return await JsonTableSchemaInferer.InferAsync(
            binding,
            snapshot,
            maxProfileRecords: 100,
            cancellationToken: Cancellation);
    }

    private static async ValueTask<JsonSnapshotPackageSession>
        OpenAsync(
            string packagePath,
            string workspacePath,
            string? expectedManifestDigest = null) =>
        await JsonSnapshotPackage.OpenAsync(
            packagePath,
            new JsonSnapshotPackageOpenOptions
            {
                WorkspacePath = workspacePath,
                MaxSourceBytes = 1024 * 1024,
                ExpectedManifestDigest =
                    expectedManifestDigest,
            },
            Cancellation);

    private static int ReadManifestLength(
        ReadOnlySpan<byte> package)
    {
        Assert.True(
            package.Length >= HeaderSize,
            "The package is shorter than its fixed header.");
        int length = BinaryPrimitives.ReadInt32BigEndian(
            package.Slice(
                ManifestLengthOffset,
                sizeof(int)));
        Assert.InRange(
            length,
            1,
            package.Length - HeaderSize);
        return length;
    }

    private static string DifferentDigest(string digest)
    {
        Assert.StartsWith(
            "sha256:",
            digest,
            StringComparison.Ordinal);
        char replacement = digest[^1] == '0'
            ? '1'
            : '0';
        return digest[..^1] + replacement;
    }

    private static IEnumerable<string>
        TemporaryPackageFiles(string directory) =>
        Directory.EnumerateFiles(
            directory,
            ".csdbjson-*.tmp",
            SearchOption.TopDirectoryOnly);

    private static async ValueTask WriteTextAsync(
        string path,
        string contents) =>
        await File.WriteAllTextAsync(
            path,
            contents,
            new UTF8Encoding(false, true),
            Cancellation);

    private sealed record PackageOrigin(
        string SourcePath,
        string PackagePath,
        JsonSnapshotPackageManifest Manifest);

    private sealed class TemporaryDirectory : IDisposable
    {
        internal TemporaryDirectory()
        {
            Root = Path.Combine(
                Path.GetTempPath(),
                "csharpdb-json-package-tamper-tests-" +
                Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(Root);
        }

        internal string Root { get; }

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
