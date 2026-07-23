using System.Globalization;
using System.Security.Cryptography;
using System.Security.AccessControl;
using System.Text;
using CSharpDB.Migration.Files.Csv;

namespace CSharpDB.Migration.Files.Tests;

public sealed class CsvExportPreparedOutputPathTests
{
    private static readonly CancellationToken Cancellation =
        TestContext.Current.CancellationToken;

    [Theory]
    [InlineData("relative")]
    [InlineData("double-separator")]
    [InlineData("current-directory-segment")]
    [InlineData("parent-directory-segment")]
    [InlineData("nul")]
    [InlineData("invalid-unicode")]
    public async Task OpenAsync_RejectsNonCanonicalDestinationPaths(string pathKind)
    {
        using var workspace = new TemporaryDirectory();
        string destinationPath = pathKind switch
        {
            "relative" => Path.Combine(
                $"relative-{Guid.NewGuid():N}",
                "output.csv"),
            "double-separator" =>
                workspace.Root +
                Path.DirectorySeparatorChar +
                Path.DirectorySeparatorChar +
                "output.csv",
            "current-directory-segment" =>
                Path.Combine(workspace.Root, ".", "output.csv"),
            "parent-directory-segment" =>
                Path.Combine(workspace.Root, "child", "..", "output.csv"),
            "nul" => workspace.PathFor("output.csv") + "\0",
            "invalid-unicode" =>
                workspace.PathFor("output-" + '\ud800' + ".csv"),
            _ => throw new ArgumentOutOfRangeException(nameof(pathKind)),
        };

        await Assert.ThrowsAsync<ArgumentException>(
            () => OpenAndDisposeAsync(destinationPath, CreateBinding()));
    }

    [Theory]
    [InlineData("device")]
    [InlineData("extended")]
    [InlineData("unc")]
    [InlineData("alternate-data-stream")]
    [InlineData("reserved-con")]
    [InlineData("reserved-com1")]
    [InlineData("trailing-dot")]
    [InlineData("trailing-space")]
    public async Task OpenAsync_RejectsWindowsUnsafeDestinationPaths(
        string pathKind)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        string ordinaryPath = workspace.PathFor("output.csv");
        string destinationPath = pathKind switch
        {
            "device" => @"\\.\" + ordinaryPath,
            "extended" => @"\\?\" + ordinaryPath,
            "unc" => @"\\localhost\__csharpdb_missing_share__\output.csv",
            "alternate-data-stream" => ordinaryPath + ":stream",
            "reserved-con" => workspace.PathFor("CON.csv"),
            "reserved-com1" => workspace.PathFor("COM1.txt"),
            "trailing-dot" => workspace.PathFor("output."),
            "trailing-space" => workspace.PathFor("output "),
            _ => throw new ArgumentOutOfRangeException(nameof(pathKind)),
        };

        await Assert.ThrowsAsync<ArgumentException>(
            () => OpenAndDisposeAsync(destinationPath, CreateBinding()));
    }

    [Fact]
    public async Task SiblingPaths_AreDestinationOnlyAndDeterministicAcrossBindings()
    {
        using var workspace = new TemporaryDirectory();
        string destinationPath = workspace.PathFor("orders.csv");

        CsvExportPreparedOutputPaths first = await CapturePathsAsync(
            destinationPath,
            CreateBinding(snapshotDigestValue: 'a'));
        CsvExportPreparedOutputPaths second = await CapturePathsAsync(
            destinationPath,
            CreateBinding(snapshotDigestValue: 'b'));

        Assert.Equal(first.PreparedDataPath, second.PreparedDataPath);
        Assert.Equal(first.CheckpointPath, second.CheckpointPath);
        Assert.Equal(
            first.PendingCheckpointPath,
            second.PendingCheckpointPath);

        string? destinationDirectory = Path.GetDirectoryName(destinationPath);
        string[] allPaths =
        [
            destinationPath,
            first.PreparedDataPath,
            first.CheckpointPath,
            first.PendingCheckpointPath,
        ];
        Assert.All(
            allPaths,
            path => Assert.Equal(
                destinationDirectory,
                Path.GetDirectoryName(path)));
        Assert.Equal(
            allPaths.Length,
            allPaths.Distinct(PathComparer).Count());
    }

    [Fact]
    public async Task ExistingDestination_IsRejectedWithoutMutationOrSiblingCreation()
    {
        using var workspace = new TemporaryDirectory();
        string destinationPath = workspace.PathFor("existing.csv");
        CsvExportCheckpointBinding binding = CreateBinding();
        CsvExportPreparedOutputPaths paths = await CapturePathsAsync(
            destinationPath,
            binding);
        DeleteRegularFile(paths.PreparedDataPath);
        DeleteRegularFile(paths.CheckpointPath);
        DeleteRegularFile(paths.PendingCheckpointPath);

        byte[] expected = Encoding.UTF8.GetBytes(
            "existing,destination\r\nmust,remain\r\n");
        await File.WriteAllBytesAsync(destinationPath, expected, Cancellation);

        await Assert.ThrowsAsync<InvalidDataException>(
            () => OpenAndDisposeAsync(destinationPath, binding));

        Assert.Equal(
            expected,
            await File.ReadAllBytesAsync(destinationPath, Cancellation));
        Assert.False(File.Exists(paths.PreparedDataPath));
        Assert.False(File.Exists(paths.CheckpointPath));
        Assert.False(File.Exists(paths.PendingCheckpointPath));
    }

    [Theory]
    [InlineData("prepared-data")]
    [InlineData("checkpoint")]
    [InlineData("pending-checkpoint")]
    public async Task ExistingSiblingDirectory_IsRejected(string siblingKind)
    {
        using var workspace = new TemporaryDirectory();
        string destinationPath = workspace.PathFor("directory-collision.csv");
        CsvExportCheckpointBinding binding = CreateBinding();
        CsvExportPreparedOutputPaths paths = await CapturePathsAsync(
            destinationPath,
            binding);
        string siblingPath = SelectSiblingPath(paths, siblingKind);
        DeleteRegularFile(siblingPath);
        Directory.CreateDirectory(siblingPath);

        await Assert.ThrowsAsync<InvalidDataException>(
            () => OpenAndDisposeAsync(destinationPath, binding));

        Assert.True(Directory.Exists(siblingPath));
        Assert.False(File.Exists(destinationPath));
    }

    [Fact]
    public async Task PreparedDataSymbolicLink_IsRejectedWithoutTouchingTarget()
    {
        using var workspace = new TemporaryDirectory();
        string destinationPath = workspace.PathFor("reparse.csv");
        CsvExportCheckpointBinding binding = CreateBinding();
        CsvExportPreparedOutputPaths paths = await CapturePathsAsync(
            destinationPath,
            binding);
        DeleteRegularFile(paths.PreparedDataPath);

        string targetPath = workspace.PathFor("reparse-target.bin");
        byte[] expected = Encoding.UTF8.GetBytes("external-target");
        await File.WriteAllBytesAsync(targetPath, expected, Cancellation);
        if (!TryCreateSymbolicLink(paths.PreparedDataPath, targetPath))
            return;

        Assert.True(
            (File.GetAttributes(paths.PreparedDataPath) &
             FileAttributes.ReparsePoint) != 0);

        await Assert.ThrowsAsync<InvalidDataException>(
            () => OpenAndDisposeAsync(destinationPath, binding));

        Assert.Equal(
            expected,
            await File.ReadAllBytesAsync(targetPath, Cancellation));
        Assert.True(
            (File.GetAttributes(paths.PreparedDataPath) &
             FileAttributes.ReparsePoint) != 0);
        Assert.False(File.Exists(destinationPath));
    }

    [Fact]
    public async Task DisposeAndReacquire_PreservesCheckpointedData()
    {
        using var workspace = new TemporaryDirectory();
        string destinationPath = workspace.PathFor("reacquire.csv");
        CsvExportCheckpointBinding binding = CreateBinding();
        byte[] expected = Encoding.UTF8.GetBytes("id,note\r\n");
        CsvExportPreparedOutputPaths paths;

        await using (CsvExportPreparedOutputLease created =
                     await CsvExportPreparedOutputLease.OpenAsync(
                         destinationPath,
                         binding,
                         Cancellation))
        {
            paths = created.Paths;
            await created.DataStream.WriteAsync(expected, Cancellation);
            await created.PersistCheckpointAsync(
                CreateHeaderCheckpoint(binding, expected),
                Cancellation);
        }

        await using CsvExportPreparedOutputLease recovered =
            await CsvExportPreparedOutputLease.OpenAsync(
                destinationPath,
                binding,
                Cancellation);

        Assert.Equal(CsvExportPreparedOutputState.Recovered, recovered.State);
        Assert.Equal(paths.PreparedDataPath, recovered.Paths.PreparedDataPath);
        Assert.Equal(paths.CheckpointPath, recovered.Paths.CheckpointPath);
        Assert.Equal(
            paths.PendingCheckpointPath,
            recovered.Paths.PendingCheckpointPath);
        Assert.Equal(expected.LongLength, recovered.DataStream.Length);
        recovered.DataStream.Position = 0;
        byte[] actual = new byte[expected.Length];
        await recovered.DataStream.ReadExactlyAsync(actual, Cancellation);
        Assert.Equal(expected, actual);
        Assert.False(File.Exists(destinationPath));
    }

    [Fact]
    public async Task ActiveLease_PreventsParentRenameUntilDisposed()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        string originalParent = workspace.Root;
        string renamedParent = originalParent + "-renamed";
        string destinationPath = workspace.PathFor("parent-lock.csv");
        CsvExportPreparedOutputLease lease =
            await CsvExportPreparedOutputLease.OpenAsync(
                destinationPath,
                CreateBinding(),
                Cancellation);

        Exception? renameFailure;
        try
        {
            renameFailure = Record.Exception(
                () => Directory.Move(originalParent, renamedParent));
        }
        finally
        {
            await lease.DisposeAsync();
            if (Directory.Exists(renamedParent) &&
                !Directory.Exists(originalParent))
            {
                Directory.Move(renamedParent, originalParent);
            }
        }

        Assert.IsType<IOException>(renameFailure);
        Assert.True(Directory.Exists(originalParent));
        Assert.False(Directory.Exists(renamedParent));

        try
        {
            Directory.Move(originalParent, renamedParent);
            Assert.True(Directory.Exists(renamedParent));
            Assert.False(Directory.Exists(originalParent));
        }
        finally
        {
            if (Directory.Exists(renamedParent) &&
                !Directory.Exists(originalParent))
            {
                Directory.Move(renamedParent, originalParent);
            }
        }
    }

    [Fact]
    public async Task OpenAsync_RejectsReadyMappedNetworkDrive_WhenAvailable()
    {
        if (!OperatingSystem.IsWindows())
            return;

        string? mappedRoot = TryGetReadableMappedNetworkRoot();
        if (mappedRoot is null)
            return;

        string destinationPath = Path.Combine(
            mappedRoot,
            $"csharpdb-prepared-network-probe-{Guid.NewGuid():N}.csv");
        CsvExportPreparedOutputLease? unexpectedLease = null;
        CsvExportPreparedOutputPaths? unexpectedPaths = null;
        Exception? error;
        try
        {
            error = await Record.ExceptionAsync(async () =>
            {
                unexpectedLease =
                    await CsvExportPreparedOutputLease.OpenAsync(
                        destinationPath,
                        CreateBinding(),
                        Cancellation);
                unexpectedPaths = unexpectedLease.Paths;
            });
        }
        finally
        {
            if (unexpectedLease is not null)
                await unexpectedLease.DisposeAsync();
            if (unexpectedPaths is not null)
            {
                DeleteRegularFile(unexpectedPaths.PreparedDataPath);
                DeleteRegularFile(unexpectedPaths.CheckpointPath);
                DeleteRegularFile(unexpectedPaths.PendingCheckpointPath);
            }
        }

        InvalidDataException rejection =
            Assert.IsType<InvalidDataException>(error);
        Assert.Contains(
            "network",
            rejection.Message,
            StringComparison.OrdinalIgnoreCase);
        Assert.False(File.Exists(destinationPath));
    }

    private static async Task<CsvExportPreparedOutputPaths> CapturePathsAsync(
        string destinationPath,
        CsvExportCheckpointBinding binding)
    {
        await using CsvExportPreparedOutputLease lease =
            await CsvExportPreparedOutputLease.OpenAsync(
                destinationPath,
                binding,
                Cancellation);
        return lease.Paths;
    }

    private static async Task OpenAndDisposeAsync(
        string destinationPath,
        CsvExportCheckpointBinding binding)
    {
        await using CsvExportPreparedOutputLease lease =
            await CsvExportPreparedOutputLease.OpenAsync(
                destinationPath,
                binding,
                Cancellation);
    }

    private static string SelectSiblingPath(
        CsvExportPreparedOutputPaths paths,
        string siblingKind) => siblingKind switch
        {
            "prepared-data" => paths.PreparedDataPath,
            "checkpoint" => paths.CheckpointPath,
            "pending-checkpoint" => paths.PendingCheckpointPath,
            _ => throw new ArgumentOutOfRangeException(nameof(siblingKind)),
        };

    private static CsvExportCheckpoint CreateHeaderCheckpoint(
        CsvExportCheckpointBinding binding,
        byte[] header)
    {
        using var logicalDigest = new CsvExportOrderedContentDigest();
        CsvExportHashManifest emptyPrefix =
            logicalDigest.GetCurrentPrefixDigest();
        return new CsvExportCheckpoint
        {
            Generation = 0,
            Phase = CsvExportCheckpointPhase.Writing,
            Binding = binding,
            BindingDigest =
                CsvExportCheckpointSerializer.ComputeBindingDigest(binding),
            Progress = new CsvExportCheckpointProgress
            {
                CompletedRowCount = 0,
                LastCompletedRowId = null,
                DataPrefixByteLength = header.LongLength,
                DataPrefixDigest = HashBytes(header),
                LogicalPrefixAggregation =
                    CsvExportCheckpointContracts.LogicalPrefixAggregation,
                SourceLogicalRowHashPrefixDigest = emptyPrefix,
                ExportedLogicalRowHashPrefixDigest = emptyPrefix,
                TransformedRowCount = 0,
                TransformedCellCount = 0,
            },
        };
    }

    private static CsvExportCheckpointBinding CreateBinding(
        char snapshotDigestValue = 'a')
    {
        CsvExportSourceManifest source = new()
        {
            Kind = CsvExportContracts.SourceKind,
            Version = "4.3.0",
            SnapshotByteLength = 4096,
            SnapshotDigest = Hash(snapshotDigestValue),
        };
        CsvExportColumnManifest[] columns =
        [
            Column(0, "id", CsvExportDatabaseType.Integer, nullable: false),
            Column(1, "note", CsvExportDatabaseType.Text),
        ];
        return new CsvExportCheckpointBinding
        {
            Profile = CsvExportProfile.LosslessV1,
            Source = source,
            SourceSnapshotIdentity =
                CsvExportCheckpointContracts.RetainedSnapshotIdentityPrefix +
                source.SnapshotByteLength.ToString(CultureInfo.InvariantCulture) +
                ":sha256:" +
                source.SnapshotDigest.Value,
            Table = new CsvExportTableManifest
            {
                Name = "orders",
                SchemaContract = CsvExportContracts.Schema,
                SchemaDigest =
                    CsvExportManifestSerializer.ComputeSchemaDigest(columns),
                RowOrder = CsvExportContracts.RowOrder,
                Columns = columns,
            },
            Csv = new CsvExportFormatManifest
            {
                Encoding = CsvExportContracts.Encoding,
                HasByteOrderMark = false,
                Culture = CsvExportContracts.Culture,
                Delimiter = ",",
                Quote = '"',
                Newline = CsvExportContracts.Newline,
                HasHeaderRecord = true,
                HasFinalNewline = true,
                NullToken = CsvExportContracts.NullToken,
                NullTokenMatchesQuotedFields = false,
                TextEscape = CsvExportContracts.TextEscape,
            },
            MaxDataBytes = 1L << 20,
            MaximumDecodedBlobBytes =
                CsvExportContracts.MaximumSupportedDecodedBlobBytes,
        };
    }

    private static CsvExportColumnManifest Column(
        int ordinal,
        string name,
        CsvExportDatabaseType databaseType,
        bool nullable = true) => new()
        {
            Ordinal = ordinal,
            SourceName = name,
            Header = name,
            DatabaseType = databaseType,
            Nullable = nullable,
            ValueEncoding = databaseType switch
            {
                CsvExportDatabaseType.Integer =>
                    CsvExportContracts.IntegerValueEncoding,
                CsvExportDatabaseType.Text =>
                    CsvExportContracts.TextValueEncoding,
                _ => throw new ArgumentOutOfRangeException(nameof(databaseType)),
            },
            MaximumDecodedBytes = 0,
        };

    private static CsvExportHashManifest Hash(char value) => new()
    {
        Algorithm = CsvExportHashManifest.Sha256Algorithm,
        Value = new string(value, 64),
    };

    private static CsvExportHashManifest HashBytes(ReadOnlySpan<byte> bytes) => new()
    {
        Algorithm = CsvExportHashManifest.Sha256Algorithm,
        Value = Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant(),
    };

    private static void DeleteRegularFile(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }

    private static bool TryCreateSymbolicLink(
        string linkPath,
        string targetPath)
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

    private static string? TryGetReadableMappedNetworkRoot()
    {
        if (!OperatingSystem.IsWindows())
            return null;

        foreach (DriveInfo drive in DriveInfo.GetDrives())
        {
            try
            {
                if (drive.DriveType != DriveType.Network || !drive.IsReady)
                    continue;

                string root = drive.RootDirectory.FullName;
                if (!Directory.Exists(root))
                    continue;

                _ = FileSystemAclExtensions.GetAccessControl(
                    new DirectoryInfo(root));
                using IEnumerator<string> entries =
                    Directory.EnumerateFileSystemEntries(root).GetEnumerator();
                _ = entries.MoveNext();
                return root;
            }
            catch (Exception exception) when (
                exception is UnauthorizedAccessException or
                    IOException or
                    PlatformNotSupportedException)
            {
                // A mapped drive that cannot be safely inspected is not a
                // usable environment for this conditional integration probe.
            }
        }

        return null;
    }

    private static StringComparer PathComparer =>
        OperatingSystem.IsWindows()
            ? StringComparer.OrdinalIgnoreCase
            : StringComparer.Ordinal;

    private sealed class TemporaryDirectory : IDisposable
    {
        public TemporaryDirectory()
        {
            Root = Path.GetFullPath(Path.Combine(
                Path.GetTempPath(),
                "csharpdb-export-prepared-path-tests",
                Guid.NewGuid().ToString("N")));
            Directory.CreateDirectory(Root);
        }

        public string Root { get; }

        public string PathFor(string leaf) => Path.Combine(Root, leaf);

        public void Dispose()
        {
            if (Directory.Exists(Root))
                Directory.Delete(Root, recursive: true);
        }
    }
}
