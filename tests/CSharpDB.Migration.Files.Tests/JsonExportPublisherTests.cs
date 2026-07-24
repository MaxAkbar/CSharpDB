using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Security.AccessControl;
using System.Security.Principal;
using System.Text;
using CSharpDB.Migration.Files.Json;
using CSharpDB.Primitives;

#pragma warning disable CA1416 // Windows-only tests guard every platform-specific case.

namespace CSharpDB.Migration.Files.Tests;

public sealed class JsonExportPublisherTests
{
    private static CancellationToken Cancellation =>
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task NonWindows_FailsBeforeCreatingPublicationFiles()
    {
        if (OperatingSystem.IsWindows())
            return;

        using var workspace =
            new TemporaryDirectory();
        PublicationFixture fixture =
            Fixture(
                workspace,
                "unsupported");

        await Assert.ThrowsAsync<
            PlatformNotSupportedException>(
            () => new JsonExportPublisher()
                .PublishAsync(
                    fixture.Request(),
                    Cancellation)
                .AsTask());
        Assert.Throws<
            PlatformNotSupportedException>(
            () => JsonExportPublisher
                .ValidatePaths(
                    fixture.DestinationPath,
                    fixture.ManifestPath));
        Assert.Empty(
            Directory.GetFiles(
                workspace.Root));
    }

    [Fact]
    public async Task FreshPublish_CommitsDataBeforeCanonicalManifest()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        PublicationFixture fixture =
            Fixture(workspace, "fresh");
        var injector = new RecordingFaultInjector(
            (point, _) =>
            {
                switch (point)
                {
                    case JsonExportPublicationFaultPoint
                        .BeforeDataNamespaceCommit:
                    case JsonExportPublicationFaultPoint
                        .AfterManifestAbsenceCheckBeforeDataCommit:
                        Assert.False(File.Exists(
                            fixture.DestinationPath));
                        Assert.False(File.Exists(
                            fixture.ManifestPath));
                        break;
                    case JsonExportPublicationFaultPoint
                        .AfterDataNamespaceCommitBeforeManifest:
                    case JsonExportPublicationFaultPoint
                        .BeforeManifestNamespaceCommit:
                        Assert.True(File.Exists(
                            fixture.DestinationPath));
                        Assert.False(File.Exists(
                            fixture.ManifestPath));
                        break;
                    case JsonExportPublicationFaultPoint
                        .AfterManifestNamespaceCommitBeforeResult:
                        Assert.True(File.Exists(
                            fixture.DestinationPath));
                        Assert.True(File.Exists(
                            fixture.ManifestPath));
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(
                            nameof(point));
                }
            });

        JsonExportPublicationResult result =
            await new JsonExportPublisher(injector)
                .PublishAsync(
                    fixture.Request(),
                    Cancellation);

        Assert.Equal(
            Enum.GetValues<
                JsonExportPublicationFaultPoint>(),
            injector.Observed);
        Assert.False(result.ReusedData);
        Assert.False(result.ReusedManifest);
        Assert.Equal(
            "[{\"id\":1,\"note\":\"alpha\"},{\"id\":2,\"note\":\"beta\"}]\n",
            await File.ReadAllTextAsync(
                fixture.DestinationPath,
                Cancellation));
        Assert.Equal(
            result.CanonicalManifestBytes,
            await File.ReadAllBytesAsync(
                fixture.ManifestPath,
                Cancellation));
        Assert.Equal(
            result.Manifest.Content.DataByteLength,
            new FileInfo(
                fixture.DestinationPath).Length);
        Assert.Empty(StagingFiles(workspace.Root));
    }

    [Fact]
    public async Task ExactPairRetry_ReusesBothWithoutChangingBytes()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        PublicationFixture fixture =
            Fixture(workspace, "pair-reuse");
        JsonExportPublicationResult first =
            await new JsonExportPublisher()
                .PublishAsync(
                    fixture.Request(),
                    Cancellation);
        byte[] data =
            await File.ReadAllBytesAsync(
                fixture.DestinationPath,
                Cancellation);
        byte[] manifest =
            await File.ReadAllBytesAsync(
                fixture.ManifestPath,
                Cancellation);

        JsonExportPublicationResult second =
            await new JsonExportPublisher()
                .PublishAsync(
                    fixture.Request(),
                    Cancellation);

        Assert.True(second.ReusedData);
        Assert.True(second.ReusedManifest);
        Assert.Equal(
            first.ManifestDigest,
            second.ManifestDigest);
        Assert.Equal(
            data,
            await File.ReadAllBytesAsync(
                fixture.DestinationPath,
                Cancellation));
        Assert.Equal(
            manifest,
            await File.ReadAllBytesAsync(
                fixture.ManifestPath,
                Cancellation));
        Assert.Empty(StagingFiles(workspace.Root));
    }

    [Fact]
    public async Task FaultAfterDataCommit_RetryRepairsExactDataOnly()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        PublicationFixture fixture =
            Fixture(workspace, "data-recovery");
        var injector = new ThrowOnceFaultInjector(
            JsonExportPublicationFaultPoint
                .AfterDataNamespaceCommitBeforeManifest);

        InjectedPublicationException failure =
            await Assert.ThrowsAsync<
                InjectedPublicationException>(
                () => new JsonExportPublisher(injector)
                    .PublishAsync(
                        fixture.Request(),
                        Cancellation)
                    .AsTask());

        Assert.Equal(
            JsonExportPublicationFaultPoint
                .AfterDataNamespaceCommitBeforeManifest,
            failure.Point);
        Assert.True(File.Exists(
            fixture.DestinationPath));
        Assert.False(File.Exists(
            fixture.ManifestPath));
        Assert.Empty(StagingFiles(workspace.Root));

        JsonExportPublicationResult recovered =
            await new JsonExportPublisher()
                .PublishAsync(
                    fixture.Request(),
                    Cancellation);

        Assert.True(recovered.ReusedData);
        Assert.False(recovered.ReusedManifest);
        await AssertExactPairAsync(
            fixture,
            recovered);
    }

    [Fact]
    public async Task PreExistingExactDataOnly_IsRecovered()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        PublicationFixture fixture =
            Fixture(workspace, "manual-data-only");
        (JsonStreamingExportResult expected, byte[] data) =
            await ExportExpectedAsync(
                fixture.ExportRequest());
        await WriteFileAsync(
            fixture.DestinationPath,
            data);

        JsonExportPublicationResult result =
            await new JsonExportPublisher()
                .PublishAsync(
                    fixture.Request(),
                    Cancellation);

        Assert.True(result.ReusedData);
        Assert.False(result.ReusedManifest);
        Assert.Equal(
            expected.ManifestDigest,
            result.ManifestDigest);
        await AssertExactPairAsync(
            fixture,
            result);
    }

    [Fact]
    public async Task ManifestOnly_IsRejectedWithoutLateDataRepair()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        PublicationFixture fixture =
            Fixture(workspace, "manifest-only");
        (JsonStreamingExportResult expected, _) =
            await ExportExpectedAsync(
                fixture.ExportRequest());
        await WriteFileAsync(
            fixture.ManifestPath,
            expected.CanonicalManifestBytes);

        await Assert.ThrowsAsync<
            InvalidDataException>(
            () => new JsonExportPublisher()
                .PublishAsync(
                    fixture.Request(),
                    Cancellation)
                .AsTask());

        Assert.False(File.Exists(
            fixture.DestinationPath));
        Assert.Equal(
            expected.CanonicalManifestBytes,
            await File.ReadAllBytesAsync(
                fixture.ManifestPath,
                Cancellation));
        Assert.Empty(StagingFiles(workspace.Root));
    }

    [Fact]
    public async Task DifferentData_IsRejectedWithoutOverwriteOrManifest()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        PublicationFixture fixture =
            Fixture(workspace, "different-data");
        byte[] different =
            "different\n"u8.ToArray();
        await WriteFileAsync(
            fixture.DestinationPath,
            different);

        await Assert.ThrowsAsync<IOException>(
            () => new JsonExportPublisher()
                .PublishAsync(
                    fixture.Request(),
                    Cancellation)
                .AsTask());

        Assert.Equal(
            different,
            await File.ReadAllBytesAsync(
                fixture.DestinationPath,
                Cancellation));
        Assert.False(File.Exists(
            fixture.ManifestPath));
        Assert.Empty(StagingFiles(workspace.Root));
    }

    [Fact]
    public async Task ExactDataWithInheritedAcl_IsRejectedFailClosed()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace =
            new TemporaryDirectory();
        PublicationFixture fixture =
            Fixture(
                workspace,
                "inherited-acl");
        (_, byte[] data) =
            await ExportExpectedAsync(
                fixture.ExportRequest());
        await WriteOrdinaryFileAsync(
            fixture.DestinationPath,
            data);

        await Assert.ThrowsAsync<
            InvalidDataException>(
            () => new JsonExportPublisher()
                .PublishAsync(
                    fixture.Request(),
                    Cancellation)
                .AsTask());

        Assert.Equal(
            data,
            await File.ReadAllBytesAsync(
                fixture.DestinationPath,
                Cancellation));
        Assert.False(
            File.Exists(
                fixture.ManifestPath));
        Assert.Empty(
            StagingFiles(workspace.Root));
    }

    [Fact]
    public async Task DifferentManifest_IsRejectedWithoutOverwrite()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        PublicationFixture fixture =
            Fixture(workspace, "different-manifest");
        (_, byte[] data) =
            await ExportExpectedAsync(
                fixture.ExportRequest());
        byte[] different =
            """{"different":true}"""u8.ToArray();
        await WriteFileAsync(
            fixture.DestinationPath,
            data);
        await WriteFileAsync(
            fixture.ManifestPath,
            different);

        await Assert.ThrowsAsync<IOException>(
            () => new JsonExportPublisher()
                .PublishAsync(
                    fixture.Request(),
                    Cancellation)
                .AsTask());

        Assert.Equal(
            different,
            await File.ReadAllBytesAsync(
                fixture.ManifestPath,
                Cancellation));
        Assert.Empty(StagingFiles(workspace.Root));
    }

    [Fact]
    public async Task CancellationBeforeDataCommit_PublishesNothing()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        PublicationFixture fixture =
            Fixture(workspace, "cancel-before-data");
        using var cancellation =
            new CancellationTokenSource();
        var injector = new RecordingFaultInjector(
            (point, _) =>
            {
                if (point ==
                    JsonExportPublicationFaultPoint
                        .BeforeDataNamespaceCommit)
                {
                    cancellation.Cancel();
                }
            });

        await Assert.ThrowsAnyAsync<
            OperationCanceledException>(
            () => new JsonExportPublisher(injector)
                .PublishAsync(
                    fixture.Request(),
                    cancellation.Token)
                .AsTask());

        Assert.False(File.Exists(
            fixture.DestinationPath));
        Assert.False(File.Exists(
            fixture.ManifestPath));
        Assert.Empty(StagingFiles(workspace.Root));
    }

    [Fact]
    public async Task PreCanceledPublish_CreatesNoFiles()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        PublicationFixture fixture =
            Fixture(workspace, "pre-canceled");
        using var cancellation =
            new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<
            OperationCanceledException>(
            () => new JsonExportPublisher()
                .PublishAsync(
                    fixture.Request(),
                    cancellation.Token)
                .AsTask());

        Assert.Empty(
            Directory.GetFiles(workspace.Root));
    }

    [Fact]
    public async Task SourceFailureBeforeCompletion_PublishesNothing()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        PublicationFixture fixture =
            Fixture(workspace, "source-failure");
        JsonExportPublicationRequest request =
            fixture.Request() with
            {
                Export =
                    fixture.ExportRequest() with
                    {
                        Rows = RowsThenThrow(
                            Cancellation),
                    },
            };

        await Assert.ThrowsAsync<
            InjectedSourceException>(
            () => new JsonExportPublisher()
                .PublishAsync(
                    request,
                    Cancellation)
                .AsTask());

        Assert.False(File.Exists(
            fixture.DestinationPath));
        Assert.False(File.Exists(
            fixture.ManifestPath));
        Assert.Empty(StagingFiles(workspace.Root));
    }

    [Fact]
    public async Task DataStagingPath_CannotBeSwappedBeforeCommit()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace =
            new TemporaryDirectory();
        PublicationFixture fixture =
            Fixture(
                workspace,
                "stage-swap");
        string stolen =
            workspace.PathFor(
                "stolen.stage");
        string? observedStage = null;
        var injector =
            new RecordingFaultInjector(
                (point, _) =>
                {
                    if (point !=
                        JsonExportPublicationFaultPoint
                            .BeforeDataNamespaceCommit)
                    {
                        return;
                    }

                    observedStage =
                        Assert.Single(
                            Directory.GetFiles(
                                workspace.Root,
                                ".csharpdb-json-export-*.publish.data.next"));
                    Assert.ThrowsAny<IOException>(
                        () => File.Move(
                            observedStage,
                            stolen));
                    Assert.True(
                        File.Exists(
                            observedStage));
                    Assert.False(
                        File.Exists(stolen));
                });

        JsonExportPublicationResult result =
            await new JsonExportPublisher(injector)
                .PublishAsync(
                    fixture.Request(),
                    Cancellation);

        Assert.NotNull(observedStage);
        Assert.False(
            File.Exists(observedStage));
        Assert.False(
            File.Exists(stolen));
        await AssertExactPairAsync(
            fixture,
            result);
    }

    [Fact]
    public async Task ManifestAppearingAtDataBarrier_PreventsDataCommit()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        PublicationFixture fixture =
            Fixture(workspace, "manifest-race");
        (JsonStreamingExportResult expected, _) =
            await ExportExpectedAsync(
                fixture.ExportRequest());
        var injector = new RecordingFaultInjector(
            (point, _) =>
            {
                if (point ==
                    JsonExportPublicationFaultPoint
                        .BeforeDataNamespaceCommit)
                {
                    File.WriteAllBytes(
                        fixture.ManifestPath,
                        expected.CanonicalManifestBytes);
                }
            });

        await Assert.ThrowsAsync<
            InvalidDataException>(
            () => new JsonExportPublisher(injector)
                .PublishAsync(
                    fixture.Request(),
                    Cancellation)
                .AsTask());

        Assert.False(File.Exists(
            fixture.DestinationPath));
        Assert.Equal(
            expected.CanonicalManifestBytes,
            await File.ReadAllBytesAsync(
                fixture.ManifestPath,
                Cancellation));
        Assert.Empty(StagingFiles(workspace.Root));
    }

    [Fact]
    public async Task ManifestAppearingAfterAbsenceCheck_RollsBackFreshData()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace =
            new TemporaryDirectory();
        PublicationFixture fixture =
            Fixture(
                workspace,
                "manifest-absence-gap");
        byte[] different =
            """{"gap":true}"""u8.ToArray();
        var injector =
            new RecordingFaultInjector(
                (point, token) =>
                {
                    if (point !=
                        JsonExportPublicationFaultPoint
                            .AfterManifestAbsenceCheckBeforeDataCommit)
                    {
                        return;
                    }

                    Assert.True(
                        token.CanBeCanceled);
                    WritePrivateFile(
                        fixture.ManifestPath,
                        different);
                });

        await Assert.ThrowsAsync<
            InvalidDataException>(
            () => new JsonExportPublisher(injector)
                .PublishAsync(
                    fixture.Request(),
                    Cancellation)
                .AsTask());

        Assert.False(
            File.Exists(
                fixture.DestinationPath));
        Assert.Equal(
            different,
            await File.ReadAllBytesAsync(
                fixture.ManifestPath,
                Cancellation));
        Assert.Empty(
            StagingFiles(workspace.Root));
    }

    [Fact]
    public async Task ManifestAppearingAfterFreshDataCommit_RollsBackData()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace =
            new TemporaryDirectory();
        PublicationFixture fixture =
            Fixture(
                workspace,
                "manifest-gap-race");
        byte[] different =
            """{"raced":true}"""u8.ToArray();
        var injector =
            new RecordingFaultInjector(
                (point, token) =>
                {
                    if (point !=
                        JsonExportPublicationFaultPoint
                            .AfterDataNamespaceCommitBeforeManifest)
                    {
                        return;
                    }

                    Assert.False(
                        token.CanBeCanceled);
                    Assert.True(
                        File.Exists(
                            fixture.DestinationPath));
                    WritePrivateFile(
                        fixture.ManifestPath,
                        different);
                });

        await Assert.ThrowsAsync<
            InvalidDataException>(
            () => new JsonExportPublisher(injector)
                .PublishAsync(
                    fixture.Request(),
                    Cancellation)
                .AsTask());

        Assert.False(
            File.Exists(
                fixture.DestinationPath));
        Assert.Equal(
            different,
            await File.ReadAllBytesAsync(
                fixture.ManifestPath,
                Cancellation));
        Assert.Empty(
            StagingFiles(workspace.Root));
    }

    [Fact]
    public async Task DifferentManifestCollision_RollsBackFreshData()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace =
            new TemporaryDirectory();
        PublicationFixture fixture =
            Fixture(
                workspace,
                "manifest-commit-race");
        byte[] different =
            """{"collision":true}"""u8.ToArray();
        var injector =
            new RecordingFaultInjector(
                (point, token) =>
                {
                    if (point !=
                        JsonExportPublicationFaultPoint
                            .BeforeManifestNamespaceCommit)
                    {
                        return;
                    }

                    Assert.False(
                        token.CanBeCanceled);
                    WritePrivateFile(
                        fixture.ManifestPath,
                        different);
                });

        await Assert.ThrowsAsync<IOException>(
            () => new JsonExportPublisher(injector)
                .PublishAsync(
                    fixture.Request(),
                    Cancellation)
                .AsTask());

        Assert.False(
            File.Exists(
                fixture.DestinationPath));
        Assert.Equal(
            different,
            await File.ReadAllBytesAsync(
                fixture.ManifestPath,
                Cancellation));
        Assert.Empty(
            StagingFiles(workspace.Root));
    }

    [Fact]
    public async Task OpenBoundParent_BlocksAncestorRename()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace =
            new TemporaryDirectory();
        string ancestor =
            workspace.PathFor(
                "ancestor");
        string parent =
            Path.Combine(
                ancestor,
                "bound");
        Directory.CreateDirectory(parent);
        var fixture =
            new PublicationFixture(
                Path.Combine(
                    parent,
                    "ancestor.json"),
                Path.Combine(
                    parent,
                    "ancestor.manifest.json"));
        string moved =
            workspace.PathFor(
                "moved-ancestor");
        var injector =
            new RecordingFaultInjector(
                (point, _) =>
                {
                    if (point ==
                        JsonExportPublicationFaultPoint
                            .BeforeDataNamespaceCommit)
                    {
                        Assert.ThrowsAny<IOException>(
                            () => Directory.Move(
                                ancestor,
                                moved));
                        Assert.True(
                            Directory.Exists(
                                ancestor));
                        Assert.False(
                            Directory.Exists(
                                moved));
                    }
                });

        JsonExportPublicationResult result =
            await new JsonExportPublisher(injector)
                .PublishAsync(
                    fixture.Request(),
                    Cancellation);

        await AssertExactPairAsync(
            fixture,
            result);
    }

    [Fact]
    public async Task CancellationAfterDataCommit_IsIgnoredThroughManifest()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        PublicationFixture fixture =
            Fixture(workspace, "cancel-after-data");
        using var cancellation =
            new CancellationTokenSource();
        var injector = new RecordingFaultInjector(
            (point, token) =>
            {
                if (point ==
                    JsonExportPublicationFaultPoint
                        .AfterDataNamespaceCommitBeforeManifest)
                {
                    Assert.False(
                        token.CanBeCanceled);
                    cancellation.Cancel();
                }
            });

        JsonExportPublicationResult result =
            await new JsonExportPublisher(injector)
                .PublishAsync(
                    fixture.Request(),
                    cancellation.Token);

        Assert.True(
            cancellation.IsCancellationRequested);
        Assert.False(result.ReusedData);
        Assert.False(result.ReusedManifest);
        await AssertExactPairAsync(
            fixture,
            result);
    }

    [Fact]
    public async Task ExactDataRecovery_IgnoresCancellationAfterAuthority()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        PublicationFixture fixture =
            Fixture(workspace, "recover-cancel");
        (_, byte[] data) =
            await ExportExpectedAsync(
                fixture.ExportRequest());
        await WriteFileAsync(
            fixture.DestinationPath,
            data);
        using var cancellation =
            new CancellationTokenSource();
        var injector = new RecordingFaultInjector(
            (point, token) =>
            {
                if (point ==
                    JsonExportPublicationFaultPoint
                        .BeforeManifestNamespaceCommit)
                {
                    Assert.False(
                        token.CanBeCanceled);
                    cancellation.Cancel();
                }
            });

        JsonExportPublicationResult result =
            await new JsonExportPublisher(injector)
                .PublishAsync(
                    fixture.Request(),
                    cancellation.Token);

        Assert.True(result.ReusedData);
        Assert.False(result.ReusedManifest);
        Assert.True(
            cancellation.IsCancellationRequested);
        await AssertExactPairAsync(
            fixture,
            result);
    }

    [Fact]
    public async Task FaultAfterManifestCommit_RetryReusesExactPair()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        PublicationFixture fixture =
            Fixture(workspace, "manifest-recovery");
        var injector = new ThrowOnceFaultInjector(
            JsonExportPublicationFaultPoint
                .AfterManifestNamespaceCommitBeforeResult);

        await Assert.ThrowsAsync<
            InjectedPublicationException>(
            () => new JsonExportPublisher(injector)
                .PublishAsync(
                    fixture.Request(),
                    Cancellation)
                .AsTask());

        Assert.True(File.Exists(
            fixture.DestinationPath));
        Assert.True(File.Exists(
            fixture.ManifestPath));

        JsonExportPublicationResult recovered =
            await new JsonExportPublisher()
                .PublishAsync(
                    fixture.Request(),
                    Cancellation);

        Assert.True(recovered.ReusedData);
        Assert.True(recovered.ReusedManifest);
        await AssertExactPairAsync(
            fixture,
            recovered);
    }

    [Fact]
    public async Task ExistingSymbolicLink_IsRejectedFailClosed()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        PublicationFixture fixture =
            Fixture(workspace, "link");
        string target =
            workspace.PathFor("target.json");
        await WriteFileAsync(
            target,
            "target"u8.ToArray());
        if (!TryCreateSymbolicLink(
                fixture.DestinationPath,
                target))
        {
            return;
        }

        await Assert.ThrowsAsync<
            InvalidDataException>(
            () => new JsonExportPublisher()
                .PublishAsync(
                    fixture.Request(),
                    Cancellation)
                .AsTask());

        Assert.Equal(
            "target",
            await File.ReadAllTextAsync(
                target,
                Cancellation));
        Assert.False(File.Exists(
            fixture.ManifestPath));
        Assert.Empty(StagingFiles(workspace.Root));
    }

    [Fact]
    public async Task ExistingDirectoryAtFinalPath_IsRejectedFailClosed()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        PublicationFixture fixture =
            Fixture(workspace, "directory");
        Directory.CreateDirectory(
            fixture.DestinationPath);

        await Assert.ThrowsAsync<
            InvalidDataException>(
            () => new JsonExportPublisher()
                .PublishAsync(
                    fixture.Request(),
                    Cancellation)
                .AsTask());

        Assert.True(Directory.Exists(
            fixture.DestinationPath));
        Assert.False(File.Exists(
            fixture.ManifestPath));
        Assert.Empty(StagingFiles(workspace.Root));
    }

    [Fact]
    public async Task HardLinkAliasState_IsRejectedFailClosed()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        PublicationFixture fixture =
            Fixture(workspace, "hardlink");
        (JsonStreamingExportResult expected, byte[] data) =
            await ExportExpectedAsync(
                fixture.ExportRequest());
        await WriteFileAsync(
            fixture.DestinationPath,
            data);
        if (!TryCreateHardLink(
                fixture.ManifestPath,
                fixture.DestinationPath))
        {
            return;
        }

        await Assert.ThrowsAsync<
            InvalidDataException>(
            () => new JsonExportPublisher()
                .PublishAsync(
                    fixture.Request(),
                    Cancellation)
                .AsTask());

        Assert.Equal(
            data,
            await File.ReadAllBytesAsync(
                fixture.DestinationPath,
                Cancellation));
        Assert.NotEqual(
            expected.CanonicalManifestBytes,
            await File.ReadAllBytesAsync(
                fixture.ManifestPath,
                Cancellation));
        Assert.Empty(StagingFiles(workspace.Root));
    }

    [Fact]
    public void ValidatePaths_RejectsAliasesNonSiblingsAndNormalization()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        string data =
            workspace.PathFor("data.json");
        string manifest =
            workspace.PathFor("data.manifest.json");
        JsonExportPublisher.ValidatePaths(
            data,
            manifest);

        Assert.Throws<ArgumentException>(
            () => JsonExportPublisher
                .ValidatePaths(data, data));
        if (OperatingSystem.IsWindows() ||
            OperatingSystem.IsMacOS())
        {
            Assert.Throws<ArgumentException>(
                () => JsonExportPublisher
                    .ValidatePaths(
                        data,
                        data.ToUpperInvariant()));
        }
        string other =
            workspace.PathFor("other");
        Directory.CreateDirectory(other);
        Assert.Throws<ArgumentException>(
            () => JsonExportPublisher
                .ValidatePaths(
                    data,
                    Path.Combine(
                        other,
                        "manifest.json")));
        string exactCaseParent =
            workspace.PathFor(
                "CaseSensitiveParent");
        Directory.CreateDirectory(
            exactCaseParent);
        string differentCaseParent =
            workspace.PathFor(
                "casesensitiveparent");
        Assert.Throws<ArgumentException>(
            () => JsonExportPublisher
                .ValidatePaths(
                    Path.Combine(
                        exactCaseParent,
                        "data.json"),
                    Path.Combine(
                        differentCaseParent,
                        "manifest.json")));
        Assert.Throws<InvalidDataException>(
            () => JsonExportPublisher
                .ValidatePaths(
                    Path.Combine(
                        differentCaseParent,
                        "data.json"),
                    Path.Combine(
                        differentCaseParent,
                        "manifest.json")));
        string unnormalized =
            workspace.Root +
            Path.DirectorySeparatorChar +
            "." +
            Path.DirectorySeparatorChar +
            "manifest.json";
        Assert.Throws<ArgumentException>(
            () => JsonExportPublisher
                .ValidatePaths(
                    data,
                    unnormalized));
        Assert.Throws<ArgumentException>(
            () => JsonExportPublisher
                .ValidatePaths(
                    "relative.json",
                    manifest));
        Assert.Throws<ArgumentException>(
            () => JsonExportPublisher
                .ValidatePaths(
                    workspace.PathFor(
                        "DATA~1.json"),
                    manifest));
        Assert.Throws<ArgumentException>(
            () => JsonExportPublisher
                .ValidatePaths(
                    workspace.PathFor(
                        "NUL.json"),
                    manifest));
        Assert.Throws<ArgumentException>(
            () => JsonExportPublisher
                .ValidatePaths(
                    workspace.PathFor(
                        "COM\u00b9.json"),
                    manifest));
        Assert.Throws<ArgumentException>(
            () => JsonExportPublisher
                .ValidatePaths(
                    workspace.PathFor(
                        "invalid-\ud800.json"),
                    manifest));
        Assert.Throws<ArgumentException>(
            () => JsonExportPublisher
                .ValidatePaths(
                    workspace.PathFor(
                        "trailing-dot."),
                    manifest));
        foreach (
            string invalidName in
            new[]
            {
                "star*.json",
                "question?.json",
                "quote\".json",
                "less<.json",
                "greater>.json",
                "pipe|.json",
            })
        {
            Assert.Throws<ArgumentException>(
                () => JsonExportPublisher
                    .ValidatePaths(
                        workspace.PathFor(
                            invalidName),
                        manifest));
        }
    }

    [Fact]
    public void CreatePrivateStagingFile_RemovesFileWhenPostCreateQualificationFails()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace =
            new TemporaryDirectory();
        using JsonExportPublicationFileSystem fileSystem =
            JsonExportPublicationFileSystem.Open(
                workspace.PathFor(
                    "data.json"),
                workspace.PathFor(
                    "data.manifest.json"));

        InvalidOperationException exception =
            Assert.Throws<InvalidOperationException>(
                () => fileSystem
                    .CreatePrivateStagingFile(
                        fileSystem.Paths
                            .DataStagingPath,
                        static () =>
                            throw new InvalidOperationException(
                                "Injected post-create qualification failure.")));

        Assert.Equal(
            "Injected post-create qualification failure.",
            exception.Message);
        Assert.False(
            File.Exists(
                fileSystem.Paths
                    .DataStagingPath));
    }

    [Fact]
    public async Task StaleDeterministicStaging_IsQualifiedReclaimedAndPublished()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace =
            new TemporaryDirectory();
        PublicationFixture fixture =
            Fixture(
                workspace,
                "stale-deterministic-staging");
        JsonExportPublicationFileSystem
            .PublicationPaths paths;
        using (
            JsonExportPublicationFileSystem fileSystem =
                JsonExportPublicationFileSystem.Open(
                    fixture.DestinationPath,
                    fixture.ManifestPath))
        {
            paths = fileSystem.Paths;
            using (FileStream data =
                   fileSystem.CreatePrivateStagingFile(
                       paths.DataStagingPath))
            {
                await data.WriteAsync(
                    "stale-data"u8.ToArray(),
                    Cancellation);
                data.Flush(
                    flushToDisk: true);
            }
            using (FileStream manifest =
                   fileSystem.CreatePrivateStagingFile(
                       paths.ManifestStagingPath))
            {
                await manifest.WriteAsync(
                    "stale-manifest"u8.ToArray(),
                    Cancellation);
                manifest.Flush(
                    flushToDisk: true);
            }
        }

        Assert.True(
            File.Exists(
                paths.DataStagingPath));
        Assert.True(
            File.Exists(
                paths.ManifestStagingPath));

        JsonExportPublicationResult result =
            await new JsonExportPublisher()
                .PublishAsync(
                    fixture.Request(),
                    Cancellation);

        Assert.False(
            result.ReusedData);
        Assert.False(
            result.ReusedManifest);
        Assert.False(
            File.Exists(
                paths.DataStagingPath));
        Assert.False(
            File.Exists(
                paths.ManifestStagingPath));
        Assert.Equal(
            result.CanonicalManifestBytes,
            await File.ReadAllBytesAsync(
                fixture.ManifestPath,
                Cancellation));
    }

    [Fact]
    public async Task UnsafeDeterministicStaging_IsRejectedWithoutMutation()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace =
            new TemporaryDirectory();
        PublicationFixture fixture =
            Fixture(
                workspace,
                "unsafe-deterministic-staging");
        JsonExportPublicationFileSystem
            .PublicationPaths paths =
            JsonExportPublicationFileSystem
                .PublicationPaths.Bind(
                    fixture.DestinationPath,
                    fixture.ManifestPath);
        Directory.CreateDirectory(
            paths.DataStagingPath);

        await Assert.ThrowsAsync<InvalidDataException>(
            () => new JsonExportPublisher()
                .PublishAsync(
                    fixture.Request(),
                    Cancellation)
                .AsTask());

        Assert.True(
            Directory.Exists(
                paths.DataStagingPath));
        Assert.False(
            File.Exists(
                fixture.DestinationPath));
        Assert.False(
            File.Exists(
                fixture.ManifestPath));
    }

    [Fact]
    public async Task DeterministicStaging_HoldsExclusivePairLease()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace =
            new TemporaryDirectory();
        PublicationFixture fixture =
            Fixture(
                workspace,
                "exclusive-deterministic-staging");
        using JsonExportPublicationFileSystem fileSystem =
            JsonExportPublicationFileSystem.Open(
                fixture.DestinationPath,
                fixture.ManifestPath);
        using FileStream held =
            fileSystem.CreatePrivateStagingFile(
                fileSystem.Paths
                    .DataStagingPath);

        await Assert.ThrowsAsync<IOException>(
            () => new JsonExportPublisher()
                .PublishAsync(
                    fixture.Request(),
                    Cancellation)
                .AsTask());

        Assert.False(
            File.Exists(
                fixture.DestinationPath));
        Assert.False(
            File.Exists(
                fixture.ManifestPath));
    }

    [Fact]
    public void PublicationStagingPaths_AreDeterministicAndExactCaseBound()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace =
            new TemporaryDirectory();
        string destination =
            workspace.PathFor(
                "items.json");
        string manifest =
            workspace.PathFor(
                "items.manifest.json");
        JsonExportPublicationFileSystem
            .PublicationPaths first =
            JsonExportPublicationFileSystem
                .PublicationPaths.Bind(
                    destination,
                    manifest);
        JsonExportPublicationFileSystem
            .PublicationPaths second =
            JsonExportPublicationFileSystem
                .PublicationPaths.Bind(
                    destination,
                    manifest);
        JsonExportPublicationFileSystem
            .PublicationPaths changedCase =
            JsonExportPublicationFileSystem
                .PublicationPaths.Bind(
                    workspace.PathFor(
                        "Items.json"),
                    manifest);

        Assert.Equal(
            first.DataStagingPath,
            second.DataStagingPath);
        Assert.Equal(
            first.ManifestStagingPath,
            second.ManifestStagingPath);
        Assert.NotEqual(
            first.DataStagingPath,
            changedCase.DataStagingPath);
        Assert.Matches(
            @"^\.csharpdb-json-export-[0-9a-f]{32}\.publish\.data\.next$",
            Path.GetFileName(
                first.DataStagingPath));
        Assert.Matches(
            @"^\.csharpdb-json-export-[0-9a-f]{32}\.publish\.manifest\.next$",
            Path.GetFileName(
                first.ManifestStagingPath));
    }

    [Fact]
    public async Task ExistingFinalWithDifferentCase_IsRejectedFailClosed()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace =
            new TemporaryDirectory();
        PublicationFixture fixture =
            Fixture(
                workspace,
                "exact-final-case");
        JsonExportPublicationResult published =
            await new JsonExportPublisher()
                .PublishAsync(
                    fixture.Request(),
                    Cancellation);
        byte[] expectedData =
            await File.ReadAllBytesAsync(
                fixture.DestinationPath,
                Cancellation);
        string aliasedDestination =
            Path.Combine(
                workspace.Root,
                Path.GetFileName(
                        fixture.DestinationPath)
                    .ToUpperInvariant());

        await Assert.ThrowsAsync<InvalidDataException>(
            () => new JsonExportPublisher()
                .PublishAsync(
                    new JsonExportPublicationRequest
                    {
                        DestinationPath =
                            aliasedDestination,
                        ManifestPath =
                            fixture.ManifestPath,
                        Export =
                            fixture.ExportRequest(),
                    },
                    Cancellation)
                .AsTask());

        Assert.Equal(
            expectedData,
            await File.ReadAllBytesAsync(
                fixture.DestinationPath,
                Cancellation));
        Assert.Equal(
            published.CanonicalManifestBytes,
            await File.ReadAllBytesAsync(
                fixture.ManifestPath,
                Cancellation));
        Assert.Empty(
            StagingFiles(
                workspace.Root));
    }

    private static PublicationFixture Fixture(
        TemporaryDirectory workspace,
        string stem) =>
        new(
            workspace.PathFor(stem + ".json"),
            workspace.PathFor(
                stem + ".manifest.json"));

    private static async Task<(
        JsonStreamingExportResult Result,
        byte[] Data)> ExportExpectedAsync(
        JsonStreamingExportRequest request)
    {
        await using var destination =
            new MemoryStream();
        JsonStreamingExportResult result =
            await new JsonStreamingExporter()
                .WriteAsync(
                    destination,
                    request,
                    Cancellation);
        return (
            result,
            destination.ToArray());
    }

    private static async Task AssertExactPairAsync(
        PublicationFixture fixture,
        JsonExportPublicationResult result)
    {
        Assert.Equal(
            result.Manifest.Content.DataByteLength,
            new FileInfo(
                fixture.DestinationPath).Length);
        Assert.Equal(
            result.CanonicalManifestBytes,
            await File.ReadAllBytesAsync(
                fixture.ManifestPath,
                Cancellation));
        Assert.Equal(
            result.ManifestDigest,
            JsonExportManifestSerializer
                .ComputeManifestDigest(
                    result.Manifest));
        Assert.Empty(StagingFiles(
            Path.GetDirectoryName(
                fixture.DestinationPath)!));
    }

    private static async Task WriteFileAsync(
        string path,
        byte[] bytes)
    {
        if (OperatingSystem.IsWindows())
        {
            await using FileStream privateStream =
                FileSystemAclExtensions.Create(
                    new FileInfo(path),
                    FileMode.CreateNew,
                    FileSystemRights.FullControl,
                    FileShare.None,
                    4096,
                    FileOptions.Asynchronous |
                    FileOptions.WriteThrough,
                    CreatePrivateWindowsSecurity());
            await privateStream.WriteAsync(
                bytes,
                Cancellation);
            privateStream.Flush(
                flushToDisk: true);
            return;
        }

        var options =
            new FileStreamOptions
            {
                Mode = FileMode.CreateNew,
                Access =
                    FileAccess.ReadWrite,
                Share = FileShare.None,
                BufferSize = 4096,
                Options =
                    FileOptions.Asynchronous |
                    FileOptions.WriteThrough,
            };
        options.UnixCreateMode =
            UnixFileMode.UserRead |
            UnixFileMode.UserWrite;
        await using var stream =
            new FileStream(
                path,
                options);
        await stream.WriteAsync(
            bytes,
            Cancellation);
        stream.Flush(flushToDisk: true);
    }

    [SupportedOSPlatform("windows")]
    private static void WritePrivateFile(
        string path,
        ReadOnlySpan<byte> bytes)
    {
        using FileStream stream =
            FileSystemAclExtensions.Create(
                new FileInfo(path),
                FileMode.CreateNew,
                FileSystemRights.FullControl,
                FileShare.None,
                4096,
                FileOptions.WriteThrough,
                CreatePrivateWindowsSecurity());
        stream.Write(bytes);
        stream.Flush(
            flushToDisk: true);
    }

    [SupportedOSPlatform("windows")]
    private static async Task
        WriteOrdinaryFileAsync(
        string path,
        ReadOnlyMemory<byte> bytes)
    {
        await using var stream =
            new FileStream(
                path,
                new FileStreamOptions
                {
                    Mode = FileMode.CreateNew,
                    Access =
                        FileAccess.ReadWrite,
                    Share = FileShare.None,
                    BufferSize = 4096,
                    Options =
                        FileOptions.Asynchronous |
                        FileOptions.WriteThrough,
                });
        await stream.WriteAsync(
            bytes,
            Cancellation);
        stream.Flush(
            flushToDisk: true);
    }

    [SupportedOSPlatform("windows")]
    private static FileSecurity
        CreatePrivateWindowsSecurity()
    {
        using WindowsIdentity identity =
            WindowsIdentity.GetCurrent(
                TokenAccessLevels.Query);
        SecurityIdentifier owner =
            identity.User ??
            throw new IOException(
                "The current Windows identity has no SID.");
        var security =
            new FileSecurity();
        security.SetOwner(owner);
        security.SetAccessRuleProtection(
            isProtected: true,
            preserveInheritance: false);
        security.AddAccessRule(
            new FileSystemAccessRule(
                owner,
                FileSystemRights.FullControl,
                AccessControlType.Allow));
        return security;
    }

    private static async IAsyncEnumerable<
        JsonExportRow> RowsThenThrow(
        [EnumeratorCancellation]
        CancellationToken cancellationToken =
            default)
    {
        cancellationToken
            .ThrowIfCancellationRequested();
        yield return new JsonExportRow(
            1,
            new DbValue[]
            {
                DbValue.FromInteger(1),
                DbValue.FromText("alpha"),
            });
        await Task.Yield();
        throw new InjectedSourceException();
    }

    private static string[] StagingFiles(
        string root) =>
        Directory.GetFiles(
            root,
            ".csharpdb-json-export-*.publish.*.next");

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
        if (!OperatingSystem.IsWindows())
            return false;

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

    private sealed record PublicationFixture(
        string DestinationPath,
        string ManifestPath)
    {
        internal JsonExportPublicationRequest Request() =>
            new()
            {
                DestinationPath =
                    DestinationPath,
                ManifestPath =
                    ManifestPath,
                Export = ExportRequest(),
            };

        internal JsonStreamingExportRequest
            ExportRequest() =>
            new()
            {
                Profile =
                    JsonExportProfile.LosslessV1,
                Framing =
                    JsonExportFraming.RootArray,
                Source =
                    new JsonExportSourceManifest
                    {
                        Kind =
                            JsonExportContracts
                                .SourceKind,
                        Version = "4.3.0",
                        SnapshotByteLength = 4096,
                        SnapshotDigest =
                            new JsonExportHashManifest
                            {
                                Algorithm =
                                    JsonExportHashManifest
                                        .Sha256Algorithm,
                                Value =
                                    new string(
                                        'a',
                                        64),
                            },
                    },
                Table =
                    new TableSchema
                    {
                        TableName =
                            "publication",
                        Columns =
                        [
                            new ColumnDefinition
                            {
                                Name = "id",
                                Type =
                                    DbType.Integer,
                                Nullable = false,
                            },
                            new ColumnDefinition
                            {
                                Name = "note",
                                Type = DbType.Text,
                                Nullable = false,
                            },
                        ],
                    },
                Rows = Rows(),
                MaxDataBytes = 1L << 20,
                MaximumDecodedBlobBytes = 1024,
            };

        private static async IAsyncEnumerable<
            JsonExportRow> Rows(
            [EnumeratorCancellation]
            CancellationToken cancellationToken =
                default)
        {
            cancellationToken
                .ThrowIfCancellationRequested();
            yield return new JsonExportRow(
                1,
                new DbValue[]
                {
                    DbValue.FromInteger(1),
                    DbValue.FromText("alpha"),
                });
            await Task.Yield();
            cancellationToken
                .ThrowIfCancellationRequested();
            yield return new JsonExportRow(
                2,
                new DbValue[]
                {
                    DbValue.FromInteger(2),
                    DbValue.FromText("beta"),
                });
        }
    }

    private sealed class RecordingFaultInjector(
        Action<
            JsonExportPublicationFaultPoint,
            CancellationToken> callback)
        : IJsonExportPublicationFaultInjector
    {
        internal List<
            JsonExportPublicationFaultPoint>
            Observed
        {
            get;
        } = [];

        public ValueTask InjectAsync(
            JsonExportPublicationFaultPoint point,
            CancellationToken cancellationToken)
        {
            Observed.Add(point);
            callback(
                point,
                cancellationToken);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class ThrowOnceFaultInjector(
        JsonExportPublicationFaultPoint point)
        : IJsonExportPublicationFaultInjector
    {
        private bool thrown;

        public ValueTask InjectAsync(
            JsonExportPublicationFaultPoint observed,
            CancellationToken cancellationToken)
        {
            if (!thrown &&
                observed == point)
            {
                thrown = true;
                throw new InjectedPublicationException(
                    observed);
            }

            return ValueTask.CompletedTask;
        }
    }

    private sealed class InjectedPublicationException(
        JsonExportPublicationFaultPoint point)
        : Exception
    {
        internal JsonExportPublicationFaultPoint
            Point
        {
            get;
        } = point;
    }

    private sealed class InjectedSourceException :
        Exception;

    private sealed class TemporaryDirectory :
        IDisposable
    {
        internal TemporaryDirectory()
        {
            Root = Path.Combine(
                Path.GetTempPath(),
                "csharpdb-json-publisher-tests",
                Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(Root);
        }

        internal string Root { get; }

        internal string PathFor(
            string name) =>
            Path.Combine(
                Root,
                name);

        public void Dispose()
        {
            try
            {
                if (Directory.Exists(Root))
                {
                    Directory.Delete(
                        Root,
                        recursive: true);
                }
            }
            catch
            {
            }
        }
    }
}

#pragma warning restore CA1416
