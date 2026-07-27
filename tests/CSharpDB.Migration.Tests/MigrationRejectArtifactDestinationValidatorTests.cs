using System.Runtime.Versioning;
using System.Security.AccessControl;
using System.Security.Principal;
using System.Text;
using CSharpDB.Migration;

namespace CSharpDB.Migration.Tests;

public sealed class MigrationRejectArtifactDestinationValidatorTests
{
    [Fact]
    public async Task ValidateForPublication_ReturnsExactBindingWithoutCreatingFiles()
    {
        string root = CreateRoot();
        try
        {
            MigrationPlan plan = await CreatePlanAsync();
            string outputPath = Path.Combine(root, "rejects.jsonl");

            MigrationRejectArtifactDestinationBinding first =
                MigrationRejectArtifactDestinationValidator.ValidateForPublication(
                    plan,
                    outputPath);
            MigrationRejectArtifactDestinationBinding second =
                MigrationRejectArtifactDestinationValidator.ValidateForPublication(
                    plan,
                    outputPath);

            Assert.Equal(outputPath, first.DestinationPath);
            Assert.Equal(first, second);
            Assert.Equal(root, Path.GetDirectoryName(first.TemporaryPath));
            Assert.StartsWith(
                ".csharpdb-reject-",
                Path.GetFileName(first.TemporaryPath),
                StringComparison.Ordinal);
            Assert.EndsWith(
                ".tmp",
                Path.GetFileName(first.TemporaryPath),
                StringComparison.Ordinal);
            Assert.Empty(Directory.EnumerateFiles(root));
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task ValidateForPublication_RejectsRelativeAndMissingParentPathsWithoutCreatingThem()
    {
        string root = CreateRoot();
        try
        {
            MigrationPlan plan = await CreatePlanAsync();
            string missingParent = Path.Combine(root, "missing", "rejects.jsonl");

            ArgumentException relative = Assert.Throws<ArgumentException>(() =>
                MigrationRejectArtifactDestinationValidator.ValidateForPublication(
                    plan,
                    "rejects.jsonl"));
            DirectoryNotFoundException missing = Assert.Throws<DirectoryNotFoundException>(() =>
                MigrationRejectArtifactDestinationValidator.ValidateForPublication(
                    plan,
                    missingParent));

            Assert.Contains("fully qualified", relative.Message, StringComparison.Ordinal);
            Assert.Contains("does not exist", missing.Message, StringComparison.Ordinal);
            Assert.False(Directory.Exists(Path.GetDirectoryName(missingParent)));
            Assert.Empty(Directory.EnumerateFiles(root));
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task ValidateForPublication_AllowsPrivateExistingDestinationAndRejectsPrivacyDrift()
    {
        string root = CreateRoot();
        try
        {
            MigrationPlan plan = await CreatePlanAsync();
            string privatePath = Path.Combine(root, "private.jsonl");
            string unsafePath = Path.Combine(root, "unsafe.jsonl");
            CreateExistingFile(privatePath, privateFile: true, "private-existing");

            MigrationRejectArtifactDestinationBinding privateBinding =
                MigrationRejectArtifactDestinationValidator.ValidateForPublication(
                    plan,
                    privatePath);
            MigrationRejectArtifactDestinationBinding unsafeBinding =
                MigrationRejectArtifactDestinationValidator.ValidateForPublication(
                    plan,
                    unsafePath);
            CreateExistingFile(unsafePath, privateFile: false, "must-remain");

            InvalidDataException error = Assert.Throws<InvalidDataException>(() =>
                MigrationRejectArtifactDestinationValidator.ValidateForPublication(
                    plan,
                    unsafePath));

            Assert.Equal(privatePath, privateBinding.DestinationPath);
            Assert.Equal("private-existing", File.ReadAllText(privatePath));
            Assert.Equal("must-remain", File.ReadAllText(unsafePath));
            Assert.False(File.Exists(unsafeBinding.TemporaryPath));
            Assert.Contains("access", error.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task ValidateForPublication_RejectsSymlinkedParentWhenSupported()
    {
        string root = CreateRoot();
        try
        {
            string actualParent = Path.Combine(root, "actual");
            string linkedParent = Path.Combine(root, "linked");
            Directory.CreateDirectory(actualParent);
            try
            {
                Directory.CreateSymbolicLink(linkedParent, actualParent);
            }
            catch (Exception linkError) when (linkError is
                UnauthorizedAccessException or
                IOException or
                PlatformNotSupportedException)
            {
                return;
            }

            MigrationPlan plan = await CreatePlanAsync();

            InvalidDataException error = Assert.Throws<InvalidDataException>(() =>
                MigrationRejectArtifactDestinationValidator.ValidateForPublication(
                    plan,
                    Path.Combine(linkedParent, "rejects.jsonl")));

            Assert.Contains("cannot traverse a link", error.Message, StringComparison.Ordinal);
            Assert.Empty(Directory.EnumerateFiles(actualParent));
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    private static async Task<MigrationPlan> CreatePlanAsync()
    {
        MigrationCatalog catalog = await new SyntheticMigrationSourceInspector().InspectAsync(
            new MigrationInspectionRequest
            {
                TargetCSharpDbVersion = CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                IncludeProfile = true,
                ProfileSampleSize = 5,
            },
            TestContext.Current.CancellationToken);
        return new MigrationPlanner().CreatePlan(
            catalog,
            new MigrationPlanningOptions
            {
                Load = new MigrationLoadPolicy
                {
                    BatchSize = 10,
                    RejectMode = MigrationRejectMode.DeterministicRejects,
                    RejectPolicy = new MigrationDeterministicRejectPolicy
                    {
                        ContractVersion = MigrationRejectContract.DeterministicRejectsV1,
                        AllowedRuleIds = ["MIG-TEST-001"],
                        MaxRejectedRowsPerBatch = 2,
                        MaxRejectedRowsPerRun = 10,
                        MaxRawValueBytes = 1_024,
                        MaxRawValueBytesPerBatch = 4_096,
                        MaxRawValueBytesPerRun = 8_192,
                        MaxArtifactBytes = 131_072,
                    },
                },
            });
    }

    private static string CreateRoot()
    {
        string root = Path.GetFullPath(Path.Combine(
            Path.GetTempPath(),
            $"csharpdb-reject-preflight-tests-{Guid.NewGuid():N}"));
        Directory.CreateDirectory(root);
        return root;
    }

    private static void CreateExistingFile(
        string path,
        bool privateFile,
        string content)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(content);
        if (OperatingSystem.IsWindows())
        {
            CreateWindowsFile(path, privateFile, bytes);
            return;
        }

        UnixFileMode mode = UnixFileMode.UserRead | UnixFileMode.UserWrite;
        if (!privateFile)
            mode |= UnixFileMode.GroupRead;
        using var stream = new FileStream(path, new FileStreamOptions
        {
            Mode = FileMode.CreateNew,
            Access = FileAccess.Write,
            Share = FileShare.None,
            UnixCreateMode = mode,
        });
        stream.Write(bytes);
    }

    [SupportedOSPlatform("windows")]
    private static void CreateWindowsFile(
        string path,
        bool privateFile,
        byte[] content)
    {
        using WindowsIdentity identity = WindowsIdentity.GetCurrent(TokenAccessLevels.Query);
        SecurityIdentifier owner = Assert.IsType<SecurityIdentifier>(identity.User);
        var security = new FileSecurity();
        security.SetOwner(owner);
        security.SetAccessRuleProtection(isProtected: true, preserveInheritance: false);
        security.AddAccessRule(new FileSystemAccessRule(
            owner,
            FileSystemRights.FullControl,
            AccessControlType.Allow));
        if (!privateFile)
        {
            security.AddAccessRule(new FileSystemAccessRule(
                new SecurityIdentifier(WellKnownSidType.WorldSid, domainSid: null),
                FileSystemRights.Read,
                AccessControlType.Allow));
        }

        using FileStream stream = FileSystemAclExtensions.Create(
            new FileInfo(path),
            FileMode.CreateNew,
            FileSystemRights.FullControl,
            FileShare.None,
            4_096,
            FileOptions.None,
            security);
        stream.Write(content);
    }

    private static void DeleteRoot(string root)
    {
        if (Directory.Exists(root))
            Directory.Delete(root, recursive: true);
    }
}
