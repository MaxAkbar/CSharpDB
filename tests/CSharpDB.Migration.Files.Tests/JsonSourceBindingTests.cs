using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Migration;
using CSharpDB.Migration.Files.Json;

namespace CSharpDB.Migration.Files.Tests;

public sealed class JsonSourceBindingTests
{
    [Fact]
    public async Task SnapshotIsByteExactAndChunkInvariant()
    {
        byte[] bytes =
        [
            .. Encoding.UTF8.Preamble,
            .. Encoding.UTF8.GetBytes(" \n[{\"x\":1},null]\r\n"),
        ];
        using var workspace = new TemporaryWorkspace();
        await using JsonSourceSnapshot direct = await CreateSnapshotAsync(
            new MemoryStream(bytes),
            workspace.Options());
        await using JsonSourceSnapshot chunked = await CreateSnapshotAsync(
            new ChunkedReadStream(new MemoryStream(bytes), chunkSize: 1),
            workspace.Options());

        string expectedDigest =
            "sha256:" +
            Convert.ToHexString(SHA256.HashData(bytes))
                .ToLowerInvariant();
        Assert.Equal(bytes.LongLength, direct.ContentLength);
        Assert.Equal(expectedDigest, direct.ContentDigest);
        Assert.Equal(direct.ContentDigest, chunked.ContentDigest);
        Assert.Equal(direct.SnapshotIdentity, chunked.SnapshotIdentity);

        await using Stream replay = direct.OpenRead();
        using var copy = new MemoryStream();
        await replay.CopyToAsync(
            copy,
            TestContext.Current.CancellationToken);
        Assert.Equal(bytes, copy.ToArray());
    }

    [Fact]
    public async Task SnapshotAcceptsTheExactByteLimitAndRejectsOneMoreByte()
    {
        using var workspace = new TemporaryWorkspace();
        await using JsonSourceSnapshot exact = await CreateSnapshotAsync(
            new MemoryStream([1, 2, 3]),
            workspace.Options(maxSourceBytes: 3));
        Assert.Equal(3, exact.ContentLength);

        var source = new MemoryStream([1, 2, 3, 4]);
        JsonSourceSnapshotException exception =
            await Assert.ThrowsAsync<JsonSourceSnapshotException>(
                async () => await CreateSnapshotAsync(
                    source,
                    workspace.Options(maxSourceBytes: 3)));

        Assert.Equal(
            JsonSnapshotDiagnosticRules.SourceLimitExceeded,
            exception.RuleId);
        Assert.Throws<ObjectDisposedException>(() => _ = source.Position);
        Assert.Single(Directory.EnumerateDirectories(workspace.Root));
    }

    [Fact]
    public async Task SnapshotDisposalRemovesPrivateFilesAndHonorsLeaveOpen()
    {
        using var workspace = new TemporaryWorkspace();
        var source = new MemoryStream(Utf8Bytes("[1]"));
        JsonSourceSnapshot snapshot = await CreateSnapshotAsync(
            source,
            workspace.Options(leaveOpen: true));

        Assert.Single(Directory.EnumerateDirectories(workspace.Root));
        Assert.True(source.CanRead);
        await snapshot.DisposeAsync();

        Assert.Empty(Directory.EnumerateFileSystemEntries(workspace.Root));
        Assert.True(source.CanRead);
        source.Dispose();
    }

    [Fact]
    public async Task CanceledSnapshotCreationCleansWorkspaceAndOwnedStream()
    {
        using var workspace = new TemporaryWorkspace();
        var source = new MemoryStream(Utf8Bytes("[1]"));
        using var canceled = new CancellationTokenSource();
        canceled.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await JsonSourceSnapshot.CreateAsync(
                source,
                workspace.Options(),
                canceled.Token));

        Assert.Empty(Directory.EnumerateFileSystemEntries(workspace.Root));
        Assert.Throws<ObjectDisposedException>(() => _ = source.Position);
    }

    [Fact]
    public async Task OutstandingReadLeaseDefersCleanupUntilItCloses()
    {
        using var workspace = new TemporaryWorkspace();
        JsonSourceSnapshot snapshot = await CreateSnapshotAsync(
            new MemoryStream(Utf8Bytes("[1,2]")),
            workspace.Options());
        Stream lease = snapshot.OpenRead();

        await snapshot.DisposeAsync();
        Assert.Single(Directory.EnumerateDirectories(workspace.Root));
        Assert.Equal((byte)'[', lease.ReadByte());
        Assert.Throws<ObjectDisposedException>(() => snapshot.OpenRead());

        await lease.DisposeAsync();
        Assert.Empty(Directory.EnumerateFileSystemEntries(workspace.Root));
    }

    [Fact]
    public async Task ConcurrentDisposeCallsJoinOneLifecycleTask()
    {
        using var workspace = new TemporaryWorkspace();
        JsonSourceSnapshot snapshot = await CreateSnapshotAsync(
            new MemoryStream(Utf8Bytes("[1]")),
            workspace.Options());

        Task first = snapshot.DisposeAsync().AsTask();
        Task second = snapshot.DisposeAsync().AsTask();

        Assert.Same(first, second);
        await Task.WhenAll(first, second);
        Assert.Empty(Directory.EnumerateFileSystemEntries(workspace.Root));
    }

    [Fact]
    public async Task IntegrityVerificationDetectsPrivateByteChanges()
    {
        using var workspace = new TemporaryWorkspace();
        JsonSourceSnapshot snapshot = await CreateSnapshotAsync(
            new MemoryStream(Utf8Bytes("[1]")),
            workspace.Options());
        try
        {
            string snapshotPath = Assert.IsType<string>(
                typeof(JsonSourceSnapshot)
                    .GetField(
                        "snapshotPath",
                        BindingFlags.Instance | BindingFlags.NonPublic)!
                    .GetValue(snapshot));
            FileStream guard = Assert.IsType<FileStream>(
                typeof(JsonSourceSnapshot)
                    .GetField(
                        "integrityGuard",
                        BindingFlags.Instance | BindingFlags.NonPublic)!
                    .GetValue(snapshot));
            await guard.DisposeAsync();
            await File.WriteAllBytesAsync(
                snapshotPath,
                Utf8Bytes("[2]"),
                TestContext.Current.CancellationToken);

            JsonSourceSnapshotException exception =
                await Assert.ThrowsAsync<JsonSourceSnapshotException>(
                    async () => await snapshot.VerifyIntegrityAsync(
                        TestContext.Current.CancellationToken));
            Assert.Equal(
                JsonSnapshotDiagnosticRules.IntegrityMismatch,
                exception.RuleId);
        }
        finally
        {
            await snapshot.DisposeAsync();
        }
    }

    [Fact]
    public async Task WorkspaceCleanupRefusesAnUnownedChild()
    {
        using var temporary = new TemporaryWorkspace();
        var workspace = new JsonSnapshotWorkspace(temporary.Root);
        string injected = Path.Combine(
            workspace.DirectoryPath,
            "injected");
        Directory.CreateDirectory(injected);
        string sentinel = Path.Combine(injected, "sentinel.txt");
        await File.WriteAllTextAsync(
            sentinel,
            "preserve",
            TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<IOException>(
            async () => await workspace.DisposeAsync());

        Assert.True(Directory.Exists(workspace.DirectoryPath));
        Assert.Equal("preserve", await File.ReadAllTextAsync(
            sentinel,
            TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task DefaultBindingHasSafeDeterministicJsonIdentity()
    {
        await using JsonSourceSnapshot snapshot =
            await SnapshotAsync("[{\"x\":1}]");

        JsonSourceBinding first = await BindAsync(snapshot);
        JsonSourceBinding second = await BindAsync(snapshot);

        Assert.Equal(MigrationSourceKind.Json, first.Source.Kind);
        Assert.Equal(
            MigrationConsistencyKind.Snapshot,
            first.Source.Consistency.Kind);
        Assert.Equal(
            "csharpdb-json-adapter-v1",
            first.Source.ProviderVersion);
        Assert.Null(first.Source.SourceVersion);
        Assert.StartsWith(
            "json-content:sha256:",
            first.Source.Identity,
            StringComparison.Ordinal);
        Assert.StartsWith(
            "sha256:",
            first.OptionsDigest,
            StringComparison.Ordinal);
        Assert.Equal(first.Source, second.Source);
        Assert.Equal(first.OptionsDigest, second.OptionsDigest);
        Assert.Equal(snapshot.SnapshotIdentity, first.SnapshotIdentity);
        Assert.Equal(snapshot.ContentDigest, first.ContentDigest);
        Assert.Equal(snapshot.ContentLength, first.ContentLength);
        Assert.Equal(JsonInputFraming.RootArray, first.Framing);
    }

    [Fact]
    public async Task LogicalIdentityIsHashedWithoutChangingContentFingerprint()
    {
        const string sensitiveIdentity =
            @"C:\customers\secret\orders.json";
        await using JsonSourceSnapshot snapshot =
            await SnapshotAsync("[{\"x\":1}]");

        JsonSourceBinding content = await BindAsync(snapshot);
        JsonSourceBinding first = await BindAsync(
            snapshot,
            logicalSourceIdentity: sensitiveIdentity);
        JsonSourceBinding repeated = await BindAsync(
            snapshot,
            logicalSourceIdentity: sensitiveIdentity);
        JsonSourceBinding other = await BindAsync(
            snapshot,
            logicalSourceIdentity: "other-source");

        Assert.StartsWith(
            "json-logical:sha256:",
            first.Source.Identity,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            sensitiveIdentity,
            first.Source.Identity,
            StringComparison.OrdinalIgnoreCase);
        Assert.Equal(first.Source.Identity, repeated.Source.Identity);
        Assert.NotEqual(first.Source.Identity, other.Source.Identity);
        Assert.Equal(content.Source.Fingerprint, first.Source.Fingerprint);
        Assert.Equal(first.Source.Fingerprint, other.Source.Fingerprint);
    }

    [Fact]
    public async Task EveryReaderSemanticAndLimitChangesTheBindingDigest()
    {
        await using JsonSourceSnapshot snapshot =
            await SnapshotAsync("[1]");
        var baselineOptions = new JsonStreamingReaderOptions();
        JsonSourceBinding baseline = await BindAsync(
            snapshot,
            baselineOptions);
        JsonStreamingReaderOptions[] changedOptions =
        [
            baselineOptions with
            {
                Framing = JsonInputFraming.MultipleValues,
            },
            baselineOptions with
            {
                MaxValueBytes =
                    baselineOptions.MaxValueBytes + 1,
            },
            baselineOptions with
            {
                MaxDepth = baselineOptions.MaxDepth + 1,
            },
            baselineOptions with
            {
                MaxPropertiesPerObject =
                    baselineOptions.MaxPropertiesPerObject + 1,
            },
            baselineOptions with
            {
                MaxArrayElements =
                    baselineOptions.MaxArrayElements - 1,
            },
            baselineOptions with
            {
                MaxTotalNodes =
                    baselineOptions.MaxTotalNodes - 1,
            },
            baselineOptions with
            {
                MaxPropertyNameBytes =
                    baselineOptions.MaxPropertyNameBytes + 1,
            },
            baselineOptions with
            {
                MaxStringBytes =
                    baselineOptions.MaxStringBytes - 1,
            },
            baselineOptions with
            {
                MaxNumberBytes =
                    baselineOptions.MaxNumberBytes + 1,
            },
        ];

        var optionDigests = new HashSet<string>(StringComparer.Ordinal)
        {
            baseline.OptionsDigest,
        };
        var sourceFingerprints = new HashSet<string>(
            StringComparer.Ordinal)
        {
            baseline.Source.Fingerprint,
        };
        foreach (JsonStreamingReaderOptions options in changedOptions)
        {
            JsonSourceBinding changed = await BindAsync(
                snapshot,
                options);
            Assert.NotEqual(
                baseline.OptionsDigest,
                changed.OptionsDigest);
            Assert.NotEqual(
                baseline.Source.Fingerprint,
                changed.Source.Fingerprint);
            Assert.True(optionDigests.Add(changed.OptionsDigest));
            Assert.True(
                sourceFingerprints.Add(changed.Source.Fingerprint));
        }
    }

    [Fact]
    public async Task LeaveOpenDoesNotChangeBindingAndReplayOwnsItsLease()
    {
        await using JsonSourceSnapshot snapshot =
            await SnapshotAsync("[1]");
        JsonSourceBinding leaving = await BindAsync(
            snapshot,
            new JsonStreamingReaderOptions { LeaveOpen = true });
        JsonSourceBinding owning = await BindAsync(
            snapshot,
            new JsonStreamingReaderOptions { LeaveOpen = false });

        Assert.Equal(leaving.OptionsDigest, owning.OptionsDigest);
        Assert.Equal(
            leaving.Source.Fingerprint,
            owning.Source.Fingerprint);
        Assert.False(leaving.ReaderOptions.LeaveOpen);
        Assert.False(owning.ReaderOptions.LeaveOpen);

        await using JsonStreamingReader reader =
            await leaving.OpenReaderAsync(
                snapshot,
                TestContext.Current.CancellationToken);
        Assert.Single(await CollectAsync(reader));
    }

    [Fact]
    public async Task InvalidOptionsAndIdentityAreRejectedBeforeSnapshotRead()
    {
        JsonSourceSnapshot snapshot = await SnapshotAsync("[1]");
        await snapshot.DisposeAsync();

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            async () => await JsonSourceBinding.CreateAsync(
                snapshot,
                new JsonStreamingReaderOptions { MaxDepth = 0 },
                cancellationToken:
                    TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<ArgumentException>(
            async () => await JsonSourceBinding.CreateAsync(
                snapshot,
                logicalSourceIdentity: " ",
                cancellationToken:
                    TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<ObjectDisposedException>(
            async () => await JsonSourceBinding.CreateAsync(
                snapshot,
                cancellationToken:
                    TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task BindingRejectsADifferentSnapshot()
    {
        await using JsonSourceSnapshot first =
            await SnapshotAsync("[1]");
        await using JsonSourceSnapshot second =
            await SnapshotAsync("[2]");
        JsonSourceBinding binding = await BindAsync(first);

        ArgumentException exception =
            await Assert.ThrowsAsync<ArgumentException>(
                async () => await binding.OpenReaderAsync(
                    second,
                    TestContext.Current.CancellationToken));

        Assert.Equal("snapshot", exception.ParamName);
    }

    [Fact]
    public async Task BindingReplaysRootArrayRepeatedlyWithExactValues()
    {
        const string json =
            """[{"z":1.2300E+004,"a":null},["x",2],false]""";
        await using JsonSourceSnapshot snapshot =
            await SnapshotAsync(json);
        JsonSourceBinding binding = await BindAsync(snapshot);

        string[] first = await ReadCanonicalValuesAsync(
            binding,
            snapshot);
        string[] second = await ReadCanonicalValuesAsync(
            binding,
            snapshot);

        Assert.Equal(
            [
                """{"z":1.2300E+004,"a":null}""",
                """["x",2]""",
                "false",
            ],
            first);
        Assert.Equal(first, second);
    }

    [Fact]
    public async Task BindingReplaysMultipleValuesUnderTheFrozenFraming()
    {
        await using JsonSourceSnapshot snapshot =
            await SnapshotAsync("{}\nnull\n[1,2]");
        JsonSourceBinding binding = await BindAsync(
            snapshot,
            new JsonStreamingReaderOptions
            {
                Framing = JsonInputFraming.MultipleValues,
            });

        Assert.Equal(JsonInputFraming.MultipleValues, binding.Framing);
        Assert.Equal(
            ["{}", "null", "[1,2]"],
            await ReadCanonicalValuesAsync(binding, snapshot));
    }

    [Fact]
    public async Task BindingFreezesLimitsForEveryReplay()
    {
        await using JsonSourceSnapshot snapshot =
            await SnapshotAsync("""["four"]""");
        JsonSourceBinding binding = await BindAsync(
            snapshot,
            new JsonStreamingReaderOptions
            {
                MaxStringBytes = 3,
            });

        for (int attempt = 0; attempt < 2; attempt++)
        {
            await using JsonStreamingReader reader =
                await binding.OpenReaderAsync(
                    snapshot,
                    TestContext.Current.CancellationToken);
            JsonReadException exception =
                await Assert.ThrowsAsync<JsonReadException>(
                    async () => await CollectAsync(reader));
            Assert.Equal(
                JsonDiagnosticRules.StringLimitExceeded,
                exception.Diagnostic.RuleId);
        }
    }

    [Fact]
    public async Task BindingCreationDoesNotParseMalformedJson()
    {
        await using JsonSourceSnapshot snapshot =
            await SnapshotAsync("[{\"x\":");

        JsonSourceBinding binding = await BindAsync(snapshot);
        await using JsonStreamingReader reader =
            await binding.OpenReaderAsync(
                snapshot,
                TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<JsonReadException>(
            async () => await CollectAsync(reader));
    }

    [Fact]
    public async Task BindingReadsFrozenBytesAfterOriginalFileChanges()
    {
        using var workspace = new TemporaryWorkspace();
        string sourcePath = Path.Combine(
            workspace.Root,
            "source.json");
        await File.WriteAllTextAsync(
            sourcePath,
            """[{"value":"old"}]""",
            new UTF8Encoding(false),
            TestContext.Current.CancellationToken);
        await using JsonSourceSnapshot snapshot =
            await JsonSourceSnapshot.CreateFromFileAsync(
                sourcePath,
                workspace.Options(),
                TestContext.Current.CancellationToken);
        JsonSourceBinding binding = await BindAsync(
            snapshot,
            logicalSourceIdentity: sourcePath);

        await File.WriteAllTextAsync(
            sourcePath,
            """[{"value":"new"}]""",
            new UTF8Encoding(false),
            TestContext.Current.CancellationToken);

        Assert.Equal(
            ["""{"value":"old"}"""],
            await ReadCanonicalValuesAsync(binding, snapshot));
        Assert.DoesNotContain(
            sourcePath,
            binding.Source.Identity,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Utf8BomRemainsInSnapshotIdentityAndIsReplayedAsMetadata()
    {
        byte[] bytes =
        [
            .. Encoding.UTF8.Preamble,
            .. Utf8Bytes("[1]"),
        ];
        await using JsonSourceSnapshot snapshot =
            await SnapshotAsync(bytes);
        JsonSourceBinding binding = await BindAsync(snapshot);
        await using JsonStreamingReader reader =
            await binding.OpenReaderAsync(
                snapshot,
                TestContext.Current.CancellationToken);

        Assert.True(reader.HasByteOrderMark);
        Assert.Equal(
            "sha256:" +
            Convert.ToHexString(SHA256.HashData(bytes))
                .ToLowerInvariant(),
            snapshot.ContentDigest);
        Assert.Single(await CollectAsync(reader));
    }

    private static async ValueTask<JsonSourceSnapshot> SnapshotAsync(
        string json) =>
        await SnapshotAsync(Utf8Bytes(json));

    private static async ValueTask<JsonSourceSnapshot> SnapshotAsync(
        byte[] bytes) =>
        await CreateSnapshotAsync(new MemoryStream(bytes));

    private static ValueTask<JsonSourceSnapshot> CreateSnapshotAsync(
        Stream source,
        JsonSourceSnapshotOptions? options = null) =>
        JsonSourceSnapshot.CreateAsync(
            source,
            options,
            TestContext.Current.CancellationToken);

    private static ValueTask<JsonSourceBinding> BindAsync(
        JsonSourceSnapshot snapshot,
        JsonStreamingReaderOptions? options = null,
        string? logicalSourceIdentity = null) =>
        JsonSourceBinding.CreateAsync(
            snapshot,
            options,
            logicalSourceIdentity,
            TestContext.Current.CancellationToken);

    private static async Task<string[]> ReadCanonicalValuesAsync(
        JsonSourceBinding binding,
        JsonSourceSnapshot snapshot)
    {
        await using JsonStreamingReader reader =
            await binding.OpenReaderAsync(
                snapshot,
                TestContext.Current.CancellationToken);
        List<JsonLogicalRecord> records = await CollectAsync(reader);
        return records
            .Select(record => Encoding.UTF8.GetString(
                JsonCanonicalValueSerializer.SerializeToUtf8Bytes(
                    record.Value)))
            .ToArray();
    }

    private static async Task<List<JsonLogicalRecord>> CollectAsync(
        JsonStreamingReader reader)
    {
        var records = new List<JsonLogicalRecord>();
        await foreach (JsonLogicalRecord record in reader.ReadValuesAsync(
                           TestContext.Current.CancellationToken))
        {
            records.Add(record);
        }
        return records;
    }

    private static byte[] Utf8Bytes(string value) =>
        new UTF8Encoding(
            encoderShouldEmitUTF8Identifier: false,
            throwOnInvalidBytes: true)
        .GetBytes(value);

    private sealed class TemporaryWorkspace : IDisposable
    {
        internal TemporaryWorkspace()
        {
            Root = Path.Combine(
                Path.GetTempPath(),
                $"csharpdb-json-binding-tests-{Guid.NewGuid():N}");
            Directory.CreateDirectory(Root);
        }

        internal string Root { get; }

        internal JsonSourceSnapshotOptions Options(
            bool leaveOpen = false,
            long maxSourceBytes = 1024 * 1024) =>
            new()
            {
                WorkspacePath = Root,
                MaxSourceBytes = maxSourceBytes,
                LeaveOpen = leaveOpen,
            };

        public void Dispose()
        {
            if (Directory.Exists(Root))
                Directory.Delete(Root, recursive: true);
        }
    }

    private sealed class ChunkedReadStream : Stream
    {
        private readonly Stream inner;
        private readonly int chunkSize;

        internal ChunkedReadStream(Stream inner, int chunkSize)
        {
            this.inner = inner;
            this.chunkSize = chunkSize;
        }

        public override bool CanRead => inner.CanRead;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length =>
            throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override int Read(
            byte[] buffer,
            int offset,
            int count) =>
            inner.Read(
                buffer,
                offset,
                Math.Min(count, chunkSize));

        public override ValueTask<int> ReadAsync(
            Memory<byte> buffer,
            CancellationToken cancellationToken = default) =>
            inner.ReadAsync(
                buffer[..Math.Min(buffer.Length, chunkSize)],
                cancellationToken);

        public override void Flush() =>
            throw new NotSupportedException();

        public override long Seek(
            long offset,
            SeekOrigin origin) =>
            throw new NotSupportedException();

        public override void SetLength(long value) =>
            throw new NotSupportedException();

        public override void Write(
            byte[] buffer,
            int offset,
            int count) =>
            throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                inner.Dispose();
            base.Dispose(disposing);
        }

        public override async ValueTask DisposeAsync()
        {
            await inner.DisposeAsync();
            GC.SuppressFinalize(this);
        }
    }
}
