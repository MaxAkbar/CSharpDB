using System.Globalization;
using System.Runtime.InteropServices;
using System.Security.AccessControl;
using System.Security.Principal;
using System.Text;
using CSharpDB.Migration.Files.Json;

#pragma warning disable CA1416 // Windows-only tests guard every platform-specific case.

namespace CSharpDB.Migration.Files.Tests;

public sealed class JsonExportPreparedOutputLeaseSecurityTests
{
    private static CancellationToken Cancellation =>
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task ExistingPreparedHardLink_IsRejectedFailClosed()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        string destinationPath =
            workspace.PathFor("hard-link.json");
        JsonExportCheckpointBinding binding = CreateBinding();
        JsonExportPreparedOutputPaths paths =
            await CapturePathsAsync(destinationPath, binding);
        File.Delete(paths.PreparedDataPath);

        string targetPath =
            workspace.PathFor("hard-link-target.bin");
        byte[] expected = Encoding.UTF8.GetBytes(
            "external-hard-link-target");
        await File.WriteAllBytesAsync(
            targetPath,
            expected,
            Cancellation);
        if (!TryCreateHardLink(
                paths.PreparedDataPath,
                targetPath))
        {
            return;
        }

        await Assert.ThrowsAsync<InvalidDataException>(
            () => OpenAndDisposeAsync(
                destinationPath,
                binding));

        Assert.Equal(
            expected,
            await File.ReadAllBytesAsync(
                targetPath,
                Cancellation));
        Assert.Equal(
            expected,
            await File.ReadAllBytesAsync(
                paths.PreparedDataPath,
                Cancellation));
        Assert.False(File.Exists(destinationPath));
    }

    [Fact]
    public async Task ExistingPreparedWithInheritedAcl_IsRejectedFailClosed()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        string destinationPath =
            workspace.PathFor("inherited-acl.json");
        JsonExportCheckpointBinding binding = CreateBinding();
        JsonExportPreparedOutputPaths paths =
            await CapturePathsAsync(destinationPath, binding);
        var file = new FileInfo(paths.PreparedDataPath);
        FileSecurity security =
            FileSystemAclExtensions.GetAccessControl(file);
        security.SetAccessRuleProtection(
            isProtected: false,
            preserveInheritance: true);
        FileSystemAclExtensions.SetAccessControl(
            file,
            security);
        Assert.False(
            FileSystemAclExtensions
                .GetAccessControl(file)
                .AreAccessRulesProtected);

        await Assert.ThrowsAsync<InvalidDataException>(
            () => OpenAndDisposeAsync(
                destinationPath,
                binding));

        Assert.True(File.Exists(paths.PreparedDataPath));
        Assert.False(File.Exists(destinationPath));
    }

    [Theory]
    [InlineData("prepared")]
    [InlineData("active")]
    [InlineData("pending")]
    public async Task ExistingSiblingDirectory_IsRejectedFailClosed(
        string siblingKind)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        string destinationPath =
            workspace.PathFor(
                $"directory-{siblingKind}.json");
        JsonExportCheckpointBinding binding = CreateBinding();
        JsonExportPreparedOutputPaths paths =
            await CapturePathsAsync(destinationPath, binding);
        string collisionPath =
            SelectSiblingPath(paths, siblingKind);
        DeleteRegularFile(collisionPath);
        Directory.CreateDirectory(collisionPath);

        await Assert.ThrowsAsync<InvalidDataException>(
            () => OpenAndDisposeAsync(
                destinationPath,
                binding));

        Assert.True(Directory.Exists(collisionPath));
        Assert.False(File.Exists(destinationPath));
    }

    [Theory]
    [InlineData("prepared")]
    [InlineData("active")]
    [InlineData("pending")]
    public async Task ExistingSiblingSymbolicLink_IsRejectedFailClosed(
        string siblingKind)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        string destinationPath =
            workspace.PathFor(
                $"symbolic-link-{siblingKind}.json");
        JsonExportCheckpointBinding binding = CreateBinding();
        JsonExportPreparedOutputPaths paths =
            await CapturePathsAsync(destinationPath, binding);
        string collisionPath =
            SelectSiblingPath(paths, siblingKind);
        DeleteRegularFile(collisionPath);

        string targetPath =
            workspace.PathFor(
                $"symbolic-link-{siblingKind}-target.bin");
        byte[] expected = Encoding.UTF8.GetBytes(
            $"external-{siblingKind}-target");
        await File.WriteAllBytesAsync(
            targetPath,
            expected,
            Cancellation);
        if (!TryCreateSymbolicLink(
                collisionPath,
                targetPath))
        {
            return;
        }

        await Assert.ThrowsAsync<InvalidDataException>(
            () => OpenAndDisposeAsync(
                destinationPath,
                binding));

        Assert.Equal(
            expected,
            await File.ReadAllBytesAsync(
                targetPath,
                Cancellation));
        Assert.True(
            (File.GetAttributes(collisionPath) &
             FileAttributes.ReparsePoint) != 0);
        Assert.False(File.Exists(destinationPath));
    }

    [Fact]
    public async Task WrongCaseParentSpelling_IsRejectedBeforeSiblingCreation()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        string exactParent =
            workspace.PathFor("LeaseParent");
        Directory.CreateDirectory(exactParent);
        string wrongCaseParent =
            workspace.PathFor("leaseParent");
        string destinationPath =
            Path.Combine(
                wrongCaseParent,
                "output.json");
        JsonExportCheckpointBinding binding = CreateBinding();
        (_, JsonExportPreparedOutputPaths paths) =
            JsonExportPreparedOutputLease.BindPaths(
                destinationPath);

        await Assert.ThrowsAsync<InvalidDataException>(
            () => OpenAndDisposeAsync(
                destinationPath,
                binding));

        Assert.False(File.Exists(paths.PreparedDataPath));
        Assert.False(File.Exists(paths.CheckpointPath));
        Assert.False(File.Exists(
            paths.PendingCheckpointPath));
        Assert.False(File.Exists(destinationPath));
    }

    [Theory]
    [InlineData("invalid")]
    [InlineData("reserved")]
    [InlineData("unc")]
    public async Task UnsafeDestinationPath_IsRejected(
        string pathKind)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        string destinationPath = pathKind switch
        {
            "invalid" =>
                workspace.PathFor("invalid*.json"),
            "reserved" =>
                workspace.PathFor("CON.json"),
            "unc" =>
                @"\\localhost\__csharpdb_missing_share__\output.json",
            _ => throw new ArgumentOutOfRangeException(
                nameof(pathKind)),
        };

        await Assert.ThrowsAsync<ArgumentException>(
            () => OpenAndDisposeAsync(
                destinationPath,
                CreateBinding()));

        Assert.Empty(
            Directory.EnumerateFileSystemEntries(
                workspace.Root));
    }

    [Fact]
    public async Task ExistingFinalDestination_IsRejectedWithoutMutation()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        string destinationPath =
            workspace.PathFor("existing-final.json");
        JsonExportCheckpointBinding binding = CreateBinding();
        JsonExportPreparedOutputPaths paths =
            await CapturePathsAsync(destinationPath, binding);
        DeleteRegularFile(paths.PreparedDataPath);

        byte[] expected = Encoding.UTF8.GetBytes(
            "[{\"existing\":true}]\n");
        await File.WriteAllBytesAsync(
            destinationPath,
            expected,
            Cancellation);

        await Assert.ThrowsAsync<InvalidDataException>(
            () => OpenAndDisposeAsync(
                destinationPath,
                binding));

        Assert.Equal(
            expected,
            await File.ReadAllBytesAsync(
                destinationPath,
                Cancellation));
        Assert.False(File.Exists(paths.PreparedDataPath));
        Assert.False(File.Exists(paths.CheckpointPath));
        Assert.False(File.Exists(
            paths.PendingCheckpointPath));
    }

    private static async Task<JsonExportPreparedOutputPaths>
        CapturePathsAsync(
        string destinationPath,
        JsonExportCheckpointBinding binding)
    {
        await using JsonExportPreparedOutputLease lease =
            await JsonExportPreparedOutputLease.OpenAsync(
                destinationPath,
                binding,
                Cancellation);
        return lease.Paths;
    }

    private static async Task OpenAndDisposeAsync(
        string destinationPath,
        JsonExportCheckpointBinding binding)
    {
        await using JsonExportPreparedOutputLease lease =
            await JsonExportPreparedOutputLease.OpenAsync(
                destinationPath,
                binding,
                Cancellation);
    }

    private static string SelectSiblingPath(
        JsonExportPreparedOutputPaths paths,
        string siblingKind) =>
        siblingKind switch
        {
            "prepared" =>
                paths.PreparedDataPath,
            "active" =>
                paths.CheckpointPath,
            "pending" =>
                paths.PendingCheckpointPath,
            _ => throw new ArgumentOutOfRangeException(
                nameof(siblingKind)),
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
            File.CreateSymbolicLink(
                linkPath,
                targetPath);
            return true;
        }
        catch (Exception exception) when (
            exception is IOException or
                UnauthorizedAccessException or
                PlatformNotSupportedException)
        {
            return false;
        }
    }

    private static bool TryCreateHardLink(
        string linkPath,
        string targetPath)
    {
        try
        {
            return CreateHardLinkW(
                linkPath,
                targetPath,
                IntPtr.Zero);
        }
        catch (Exception exception) when (
            exception is IOException or
                UnauthorizedAccessException or
                PlatformNotSupportedException)
        {
            return false;
        }
    }

    [DllImport(
        "kernel32.dll",
        CharSet = CharSet.Unicode,
        SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool CreateHardLinkW(
        string fileName,
        string existingFileName,
        IntPtr securityAttributes);

    private static JsonExportCheckpointBinding
        CreateBinding()
    {
        JsonExportColumnManifest[] columns =
        [
            new JsonExportColumnManifest
            {
                Ordinal = 0,
                SourceName = "i",
                PropertyName = "i",
                DatabaseType =
                    JsonExportDatabaseType.Integer,
                Nullable = false,
                ValueEncoding =
                    JsonExportContracts
                        .IntegerValueEncoding,
                MaximumDecodedBytes = 0,
            },
        ];
        JsonExportSourceManifest source =
            new()
            {
                Kind =
                    JsonExportContracts.SourceKind,
                Version = "4.3.0",
                SnapshotByteLength = 128,
                SnapshotDigest = Hash('a'),
            };

        return new JsonExportCheckpointBinding
        {
            Profile = JsonExportProfile.LosslessV1,
            Source = source,
            SourceSnapshotIdentity =
                JsonExportCheckpointContracts
                    .RetainedSnapshotIdentityPrefix +
                source.SnapshotByteLength.ToString(
                    CultureInfo.InvariantCulture) +
                ":" +
                JsonExportHashManifest
                    .Sha256Algorithm +
                ":" +
                source.SnapshotDigest.Value,
            Table =
                new JsonExportTableManifest
                {
                    Name = "items",
                    SchemaContract =
                        JsonExportContracts.Schema,
                    SchemaDigest =
                        JsonExportManifestSerializer
                            .ComputeSchemaDigest(
                                columns),
                    RowOrder =
                        JsonExportContracts.RowOrder,
                    Columns = columns,
                },
            Json =
                new JsonExportFormatManifest
                {
                    Encoding =
                        JsonExportContracts.Encoding,
                    HasByteOrderMark = false,
                    Culture =
                        JsonExportContracts.Culture,
                    Framing =
                        JsonExportFraming.RootArray,
                    Compact = true,
                    PropertyOrder =
                        JsonExportContracts
                            .PropertyOrder,
                    Newline =
                        JsonExportContracts.Newline,
                    HasFinalNewline = true,
                    NullEncoding =
                        JsonExportContracts
                            .NullEncoding,
                    TextEscape =
                        JsonExportContracts.TextEscape,
                    MaxDataBytes = 1L << 20,
                    MaximumDecodedBlobBytes =
                        JsonExportContracts
                            .MaximumSupportedDecodedBlobBytes,
                    MaximumValueBytes =
                        JsonInputContracts
                            .MaximumValueBytes,
                    MaximumStringBytes =
                        JsonInputContracts
                            .MaximumStringBytes,
                    MaximumPropertyNameBytes =
                        JsonInputContracts
                            .MaximumPropertyNameBytes,
                    MaximumPropertiesPerObject =
                        JsonInputContracts
                            .MaximumPropertiesPerObject,
                },
        };
    }

    private static JsonExportHashManifest Hash(
        char value) =>
        new()
        {
            Algorithm =
                JsonExportHashManifest
                    .Sha256Algorithm,
            Value = new string(value, 64),
        };

    private sealed class TemporaryDirectory :
        IDisposable
    {
        public TemporaryDirectory()
        {
            Root =
                Path.Combine(
                    Path.GetTempPath(),
                    "csharpdb-json-export-lease-security-tests",
                    Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(Root);
        }

        public string Root { get; }

        public string PathFor(string leaf) =>
            Path.Combine(Root, leaf);

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
