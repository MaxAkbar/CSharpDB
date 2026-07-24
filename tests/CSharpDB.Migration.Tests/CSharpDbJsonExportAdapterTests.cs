using System.Reflection;
using System.Runtime.CompilerServices;
using System.Security.AccessControl;
using System.Security.Cryptography;
using System.Security.Principal;
using System.Text;
using CSharpDB.Engine;
using CSharpDB.Migration.CSharpDb;
using CSharpDB.Migration.Files.Json;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Tests;

public sealed class CSharpDbJsonExportAdapterTests
{
    private static CancellationToken Cancellation =>
        TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(
        JsonExportFraming.RootArray,
        "[{\"id\":-2,\"note\":null,\"amount\":-2.25},{\"id\":3,\"note\":\"a,\\\"b\\\"\",\"amount\":3.5}]\n")]
    [InlineData(
        JsonExportFraming.Ndjson,
        "{\"id\":-2,\"note\":null,\"amount\":-2.25}\n{\"id\":3,\"note\":\"a,\\\"b\\\"\",\"amount\":3.5}\n")]
    public async Task FreshPublication_BindsSnapshotSchemaAndPhysicalRows(
        JsonExportFraming framing,
        string expected)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(workspace, "fresh");
        string extension = framing == JsonExportFraming.RootArray
            ? "json"
            : "ndjson";
        string destinationPath =
            workspace.PathFor($"rows.{extension}");
        string manifestPath =
            workspace.PathFor($"rows.{extension}.manifest.json");

        JsonExportPublicationResult result =
            await new CSharpDbJsonExportAdapter()
                .WriteAndPublishTableAsync(
                    Request(
                        receipt,
                        destinationPath,
                        "export_rows",
                        framing),
                    manifestPath,
                    Cancellation);

        byte[] data =
            await File.ReadAllBytesAsync(
                destinationPath,
                Cancellation);
        Assert.Equal(expected, Encoding.UTF8.GetString(data));
        Assert.Equal(
            result.CanonicalManifestBytes,
            await File.ReadAllBytesAsync(
                manifestPath,
                Cancellation));
        Assert.Equal(
            result.CanonicalManifestBytes,
            JsonExportManifestSerializer.Serialize(
                result.Manifest));
        Assert.Equal(
            result.ManifestDigest,
            JsonExportManifestSerializer
                .ComputeManifestDigest(result.Manifest));
        Assert.Equal(
            framing,
            result.Manifest.Json.Framing);
        Assert.Equal(
            "export_rows",
            result.Manifest.Table.Name);
        Assert.Equal(
            ["id", "note", "amount"],
            result.Manifest.Table.Columns.Select(
                static column => column.SourceName));
        Assert.Equal(
            [
                JsonExportDatabaseType.Integer,
                JsonExportDatabaseType.Text,
                JsonExportDatabaseType.Real,
            ],
            result.Manifest.Table.Columns.Select(
                static column => column.DatabaseType));
        Assert.Equal(2, result.Manifest.Content.RowCount);
        Assert.Equal(
            data.LongLength,
            result.Manifest.Content.DataByteLength);
        Assert.Equal(
            Sha256(data),
            result.Manifest.Content.DataDigest.Value);
        Assert.Equal(
            receipt.ByteLength,
            result.Manifest.Source.SnapshotByteLength);
        Assert.Equal(
            receipt.Sha256["sha256:".Length..],
            result.Manifest.Source.SnapshotDigest.Value);
        Assert.Equal(
            result.Manifest.Content.SourceLogicalDigest,
            result.Manifest.Content.ExportedLogicalDigest);
        Assert.False(result.ReusedData);
        Assert.False(result.ReusedManifest);
        Assert.False(
            File.Exists(receipt.SnapshotPath + ".wal"));
    }

    [Theory]
    [InlineData(
        JsonExportFraming.RootArray,
        "[{\"id\":-2,\"note\":null,\"amount\":-2.25},{\"id\":3,\"note\":\"a,\\\"b\\\"\",\"amount\":3.5}]\n")]
    [InlineData(
        JsonExportFraming.Ndjson,
        "{\"id\":-2,\"note\":null,\"amount\":-2.25}\n{\"id\":3,\"note\":\"a,\\\"b\\\"\",\"amount\":3.5}\n")]
    public async Task ResumablePreparedOnly_BindsSnapshotAndPreservesPrivateAuthority(
        JsonExportFraming framing,
        string expected)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(
                workspace,
                "prepared");
        string destinationPath =
            workspace.PathFor("prepared.data");
        CSharpDbRetainedJsonExportRequest request =
            Request(
                receipt,
                destinationPath,
                "export_rows",
                framing) with
            {
                CheckpointRowInterval = 1,
            };

        JsonStreamingExportResult result =
            await new CSharpDbJsonExportAdapter()
                .WriteResumableTableAsync(
                    request,
                    Cancellation);
        PreparedArtifacts artifacts =
            FindPreparedArtifacts(workspace.Root);
        byte[] data =
            await File.ReadAllBytesAsync(
                artifacts.DataPath,
                Cancellation);
        JsonExportCheckpoint checkpoint =
            ReadCheckpoint(artifacts.CheckpointPath);

        Assert.Equal(
            expected,
            Encoding.UTF8.GetString(data));
        Assert.Equal(
            JsonExportCheckpointPhase.DataComplete,
            checkpoint.Phase);
        Assert.Equal(
            receipt.SnapshotIdentity,
            checkpoint.Binding.SourceSnapshotIdentity);
        Assert.Equal(
            result.ManifestDigest,
            checkpoint.Completion!.ManifestDigest);
        Assert.Equal(
            data.LongLength,
            result.Manifest.Content.DataByteLength);
        Assert.Equal(
            Sha256(data),
            result.Manifest.Content.DataDigest.Value);
        Assert.False(
            File.Exists(
                artifacts.PendingCheckpointPath));
        Assert.False(File.Exists(destinationPath));
        Assert.False(
            File.Exists(receipt.SnapshotPath + ".wal"));
    }

    [Theory]
    [InlineData(JsonExportFraming.RootArray)]
    [InlineData(JsonExportFraming.Ndjson)]
    public async Task TerminalPreparedOnly_FreshAdapterPublishesExactFinalsWithoutChangingAuthority(
        JsonExportFraming framing)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(
                workspace,
                $"terminal-{framing}");
        string destinationPath =
            workspace.PathFor("terminal.data");
        string manifestPath =
            workspace.PathFor(
                "terminal.manifest.json");
        CSharpDbRetainedJsonExportRequest request =
            Request(
                receipt,
                destinationPath,
                "export_rows",
                framing) with
            {
                CheckpointRowInterval = 1,
            };

        JsonStreamingExportResult prepared =
            await new CSharpDbJsonExportAdapter()
                .WriteResumableTableAsync(
                    request,
                    Cancellation);
        PreparedArtifacts artifacts =
            FindPreparedArtifacts(workspace.Root);
        byte[] preparedBefore =
            await File.ReadAllBytesAsync(
                artifacts.DataPath,
                Cancellation);
        byte[] checkpointBefore =
            await File.ReadAllBytesAsync(
                artifacts.CheckpointPath,
                Cancellation);

        JsonExportPublicationResult published =
            await new CSharpDbJsonExportAdapter()
                .WriteResumableAndPublishTableAsync(
                    request,
                    manifestPath,
                    Cancellation);

        Assert.False(published.ReusedData);
        Assert.False(published.ReusedManifest);
        Assert.Equal(
            prepared.ManifestDigest,
            published.ManifestDigest);
        Assert.Equal(
            preparedBefore,
            await File.ReadAllBytesAsync(
                destinationPath,
                Cancellation));
        Assert.Equal(
            published.CanonicalManifestBytes,
            await File.ReadAllBytesAsync(
                manifestPath,
                Cancellation));
        Assert.Equal(
            preparedBefore,
            await File.ReadAllBytesAsync(
                artifacts.DataPath,
                Cancellation));
        Assert.Equal(
            checkpointBefore,
            await File.ReadAllBytesAsync(
                artifacts.CheckpointPath,
                Cancellation));
        Assert.False(
            File.Exists(
                artifacts.PendingCheckpointPath));
    }

    [Theory]
    [InlineData(
        JsonExportFraming.RootArray,
        "[{\"id\":-2,\"note\":null,\"amount\":-2.25},{\"id\":3,\"note\":\"a,\\\"b\\\"\",\"amount\":3.5}]\n")]
    [InlineData(
        JsonExportFraming.Ndjson,
        "{\"id\":-2,\"note\":null,\"amount\":-2.25}\n{\"id\":3,\"note\":\"a,\\\"b\\\"\",\"amount\":3.5}\n")]
    public async Task InterruptedGenericWriter_FreshAdapterResumesPreparedOutput(
        JsonExportFraming framing,
        string expected)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(
                workspace,
                "resume");
        string destinationPath =
            workspace.PathFor("resume.data");
        await using (
            RetainedDatabaseSnapshotSession session =
                await RetainedDatabaseSnapshot.OpenAsync(
                    receipt.SnapshotPath,
                    receipt.Identity,
                    databaseOptions: null,
                    SnapshotOptions(
                        workspace.CreateDirectory(
                            "resume-open-workspace")),
                    Cancellation))
        {
            TableSchema schema =
                session.GetTableSchema(
                    "export_rows") ??
                throw new InvalidOperationException(
                    "The JSON export fixture schema is missing.");
            var interrupted =
                new JsonResumableExportRequest
                {
                    DestinationPath =
                        destinationPath,
                    Profile =
                        JsonExportProfile.LosslessV1,
                    Framing = framing,
                    Source = Source(receipt),
                    SourceSnapshotIdentity =
                        receipt.SnapshotIdentity,
                    Table = schema,
                    OpenRows =
                        (boundary, token) =>
                        {
                            Assert.Null(boundary);
                            return ReadRowsThenThrowAsync(
                                session,
                                schema.TableName,
                                countBeforeFailure: 1,
                                token);
                        },
                    MaxDataBytes = 1L << 20,
                    MaximumDecodedBlobBytes =
                        1_024,
                    CheckpointRowInterval = 1,
                };

            await Assert.ThrowsAsync<
                InjectedReadFailure>(
                () => new JsonStreamingExporter()
                    .WriteResumableAsync(
                        interrupted,
                        Cancellation)
                    .AsTask());
        }

        PreparedArtifacts artifacts =
            FindPreparedArtifacts(workspace.Root);
        JsonExportCheckpoint writing =
            ReadCheckpoint(artifacts.CheckpointPath);
        Assert.Equal(
            JsonExportCheckpointPhase.Writing,
            writing.Phase);
        Assert.Equal(
            1,
            writing.Progress.CompletedRowCount);
        Assert.Equal(
            -2,
            writing.Progress.LastCompletedRowId);

        JsonStreamingExportResult resumed =
            await new CSharpDbJsonExportAdapter()
                .WriteResumableTableAsync(
                    Request(
                        receipt,
                        destinationPath,
                        "export_rows",
                        framing) with
                    {
                        CheckpointRowInterval = 1,
                    },
                    Cancellation);
        byte[] completedData =
            await File.ReadAllBytesAsync(
                artifacts.DataPath,
                Cancellation);

        Assert.Equal(
            expected,
            Encoding.UTF8.GetString(
                completedData));
        Assert.Equal(
            2,
            resumed.Manifest.Content.RowCount);
        JsonExportCheckpoint completed =
            ReadCheckpoint(artifacts.CheckpointPath);
        Assert.Equal(
            JsonExportCheckpointPhase.DataComplete,
            completed.Phase);
        Assert.Equal(
            3,
            completed.Progress.LastCompletedRowId);
        Assert.False(File.Exists(destinationPath));
    }

    [Theory]
    [InlineData(JsonExportFraming.RootArray)]
    [InlineData(JsonExportFraming.Ndjson)]
    public async Task ExactAndDataOnlyReruns_RequalifyAndRecover(
        JsonExportFraming framing)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(workspace, "retry");
        string destinationPath =
            workspace.PathFor("retry.data");
        string manifestPath =
            workspace.PathFor("retry.manifest.json");
        CSharpDbRetainedJsonExportRequest request =
            Request(
                receipt,
                destinationPath,
                "export_rows",
                framing);
        var adapter = new CSharpDbJsonExportAdapter();

        JsonExportPublicationResult first =
            await adapter.WriteAndPublishTableAsync(
                request,
                manifestPath,
                Cancellation);
        byte[] dataBefore = await File.ReadAllBytesAsync(
            destinationPath,
            Cancellation);
        byte[] manifestBefore = await File.ReadAllBytesAsync(
            manifestPath,
            Cancellation);

        JsonExportPublicationResult exact =
            await adapter.WriteAndPublishTableAsync(
                request,
                manifestPath,
                Cancellation);

        Assert.True(exact.ReusedData);
        Assert.True(exact.ReusedManifest);
        Assert.Equal(first.ManifestDigest, exact.ManifestDigest);
        Assert.Equal(
            dataBefore,
            await File.ReadAllBytesAsync(
                destinationPath,
                Cancellation));

        File.Delete(manifestPath);
        JsonExportPublicationResult recovered =
            await new CSharpDbJsonExportAdapter()
                .WriteAndPublishTableAsync(
                    request,
                    manifestPath,
                    Cancellation);

        Assert.True(recovered.ReusedData);
        Assert.False(recovered.ReusedManifest);
        Assert.Equal(
            dataBefore,
            await File.ReadAllBytesAsync(
                destinationPath,
                Cancellation));
        Assert.Equal(
            manifestBefore,
            await File.ReadAllBytesAsync(
                manifestPath,
                Cancellation));
    }

    [Fact]
    public async Task ExactPairRerun_RequalifiesTamperedSnapshotBeforeReuse()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(workspace, "requalify");
        string destinationPath =
            workspace.PathFor("requalify.json");
        string manifestPath =
            workspace.PathFor("requalify.manifest.json");
        CSharpDbRetainedJsonExportRequest request =
            Request(
                receipt,
                destinationPath,
                "export_rows",
                JsonExportFraming.RootArray);
        var adapter = new CSharpDbJsonExportAdapter();

        JsonExportPublicationResult first =
            await adapter
                .WriteResumableAndPublishTableAsync(
                    request,
                    manifestPath,
                    Cancellation);
        byte[] dataBefore = await File.ReadAllBytesAsync(
            destinationPath,
            Cancellation);
        byte[] manifestBefore =
            await File.ReadAllBytesAsync(
                manifestPath,
                Cancellation);
        PreparedArtifacts artifacts =
            FindPreparedArtifacts(workspace.Root);
        byte[] preparedBefore =
            await File.ReadAllBytesAsync(
                artifacts.DataPath,
                Cancellation);
        byte[] checkpointBefore =
            await File.ReadAllBytesAsync(
                artifacts.CheckpointPath,
                Cancellation);

        JsonExportPublicationResult exact =
            await adapter
                .WriteResumableAndPublishTableAsync(
                    request,
                    manifestPath,
                    Cancellation);

        Assert.True(exact.ReusedData);
        Assert.True(exact.ReusedManifest);
        Assert.Equal(
            first.ManifestDigest,
            exact.ManifestDigest);

        byte[] tamperedSnapshot =
            await File.ReadAllBytesAsync(
                receipt.SnapshotPath,
                Cancellation);
        tamperedSnapshot[tamperedSnapshot.Length / 2] ^= 0x80;
        await File.WriteAllBytesAsync(
            receipt.SnapshotPath,
            tamperedSnapshot,
            Cancellation);

        await Assert.ThrowsAnyAsync<IOException>(
            () => adapter
                .WriteResumableAndPublishTableAsync(
                    request,
                    manifestPath,
                    Cancellation)
                .AsTask());

        Assert.Equal(
            dataBefore,
            await File.ReadAllBytesAsync(
                destinationPath,
                Cancellation));
        Assert.Equal(
            manifestBefore,
            await File.ReadAllBytesAsync(
                manifestPath,
                Cancellation));
        Assert.Equal(
            preparedBefore,
            await File.ReadAllBytesAsync(
                artifacts.DataPath,
                Cancellation));
        Assert.Equal(
            checkpointBefore,
            await File.ReadAllBytesAsync(
                artifacts.CheckpointPath,
                Cancellation));
    }

    [Fact]
    public async Task WrongSnapshotIdentity_FailsBeforePublication()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(workspace, "identity");
        string destinationPath =
            workspace.PathFor("identity.json");
        string manifestPath =
            workspace.PathFor("identity.manifest.json");
        string wrongSha256 =
            DifferentCanonicalIdentity(receipt.Sha256);
        RetainedDatabaseSnapshotIdentity wrongIdentity = new(
            receipt.ByteLength,
            wrongSha256,
            DifferentCanonicalIdentity(
                receipt.SnapshotIdentity));
        CSharpDbRetainedJsonExportRequest request =
            Request(
                receipt,
                destinationPath,
                "export_rows",
                JsonExportFraming.RootArray) with
            {
                SnapshotIdentity = wrongIdentity,
            };

        await Assert.ThrowsAnyAsync<IOException>(
            () => new CSharpDbJsonExportAdapter()
                .WriteAndPublishTableAsync(
                    request,
                    manifestPath,
                    Cancellation)
                .AsTask());

        Assert.False(File.Exists(destinationPath));
        Assert.False(File.Exists(manifestPath));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task InvalidCheckpointInterval_FailsBeforeSnapshotOpen(
        long checkpointRowInterval)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(
                workspace,
                "interval");
        string destinationPath =
            workspace.PathFor("interval.json");
        CSharpDbRetainedJsonExportRequest request =
            Request(
                receipt,
                destinationPath,
                "export_rows",
                JsonExportFraming.RootArray) with
            {
                SnapshotPath =
                    workspace.PathFor(
                        "missing-snapshot.db"),
                CheckpointRowInterval =
                    checkpointRowInterval,
            };

        await Assert.ThrowsAsync<
            ArgumentOutOfRangeException>(
            () => new CSharpDbJsonExportAdapter()
                .WriteResumableTableAsync(
                    request,
                    Cancellation)
                .AsTask());

        Assert.Empty(
            Directory.EnumerateFiles(
                workspace.Root,
                ".csharpdb-json-export-*",
                SearchOption.TopDirectoryOnly));
        Assert.False(File.Exists(destinationPath));
    }

    [Fact]
    public async Task ChangedResumeBinding_DoesNotAlterPreparedAuthority()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(
                workspace,
                "binding");
        string destinationPath =
            workspace.PathFor("binding.json");
        CSharpDbRetainedJsonExportRequest request =
            Request(
                receipt,
                destinationPath,
                "export_rows",
                JsonExportFraming.RootArray) with
            {
                CheckpointRowInterval = 1,
            };
        var adapter =
            new CSharpDbJsonExportAdapter();

        _ = await adapter.WriteResumableTableAsync(
            request,
            Cancellation);
        PreparedArtifacts artifacts =
            FindPreparedArtifacts(workspace.Root);
        byte[] dataBefore =
            await File.ReadAllBytesAsync(
                artifacts.DataPath,
                Cancellation);
        byte[] checkpointBefore =
            await File.ReadAllBytesAsync(
                artifacts.CheckpointPath,
                Cancellation);

        await Assert.ThrowsAsync<InvalidDataException>(
            () => adapter.WriteResumableTableAsync(
                    request with
                    {
                        Framing =
                            JsonExportFraming.Ndjson,
                    },
                    Cancellation)
                .AsTask());

        Assert.Equal(
            dataBefore,
            await File.ReadAllBytesAsync(
                artifacts.DataPath,
                Cancellation));
        Assert.Equal(
            checkpointBefore,
            await File.ReadAllBytesAsync(
                artifacts.CheckpointPath,
                Cancellation));
        Assert.False(
            File.Exists(
                artifacts.PendingCheckpointPath));
        Assert.False(File.Exists(destinationPath));
    }

    [Fact]
    public async Task SnapshotFinalAliases_AreRejectedWithoutSourceMutation()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(
                workspace,
                "final-alias");
        byte[] snapshotBefore =
            await File.ReadAllBytesAsync(
                receipt.SnapshotPath,
                Cancellation);
        string destinationPath =
            workspace.PathFor("final-alias.json");
        CSharpDbRetainedJsonExportRequest request =
            Request(
                receipt,
                destinationPath,
                "export_rows",
                JsonExportFraming.RootArray);
        var adapter =
            new CSharpDbJsonExportAdapter();

        await Assert.ThrowsAsync<ArgumentException>(
            () => adapter.WriteResumableTableAsync(
                    request with
                    {
                        DestinationPath =
                            receipt.SnapshotPath,
                    },
                    Cancellation)
                .AsTask());

        await Assert.ThrowsAsync<ArgumentException>(
            () => adapter
                .WriteResumableAndPublishTableAsync(
                    request,
                    receipt.SnapshotPath,
                    Cancellation)
                .AsTask());

        Assert.Equal(
            snapshotBefore,
            await File.ReadAllBytesAsync(
                receipt.SnapshotPath,
                Cancellation));
        Assert.Empty(
            Directory.EnumerateFiles(
                workspace.Root,
                ".csharpdb-json-export-*",
                SearchOption.TopDirectoryOnly));
        Assert.False(File.Exists(destinationPath));
    }

    [Theory]
    [InlineData("prepared")]
    [InlineData("pending")]
    public async Task ReservedPreparedNamespaceSnapshot_IsRejectedWithoutPrivateMutation(
        string targetKind)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(
                workspace,
                $"reserved-{targetKind}");
        string destinationPath =
            workspace.PathFor("reserved.json");
        CSharpDbRetainedJsonExportRequest request =
            Request(
                receipt,
                destinationPath,
                "export_rows",
                JsonExportFraming.RootArray) with
            {
                CheckpointRowInterval = 1,
            };
        var adapter =
            new CSharpDbJsonExportAdapter();

        _ = await adapter.WriteResumableTableAsync(
            request,
            Cancellation);
        PreparedArtifacts artifacts =
            FindPreparedArtifacts(workspace.Root);
        byte[] snapshotBytes =
            await File.ReadAllBytesAsync(
                receipt.SnapshotPath,
                Cancellation);
        byte[] preparedBefore =
            await File.ReadAllBytesAsync(
                artifacts.DataPath,
                Cancellation);
        byte[] checkpointBefore =
            await File.ReadAllBytesAsync(
                artifacts.CheckpointPath,
                Cancellation);
        string targetPath =
            targetKind switch
            {
                "prepared" =>
                    artifacts.DataPath,
                "pending" =>
                    artifacts.PendingCheckpointPath,
                _ =>
                    throw new InvalidOperationException(
                        "Unknown reserved-path test case."),
            };
        if (targetKind == "prepared")
        {
            File.Delete(artifacts.DataPath);
        }
        await WritePrivateFileAsync(
            targetPath,
            snapshotBytes,
            Cancellation);

        await Assert.ThrowsAsync<ArgumentException>(
            () => adapter.WriteResumableTableAsync(
                    request with
                    {
                        SnapshotPath = targetPath,
                    },
                    Cancellation)
                .AsTask());

        Assert.Equal(
            snapshotBytes,
            await File.ReadAllBytesAsync(
                targetPath,
                Cancellation));
        Assert.Equal(
            checkpointBefore,
            await File.ReadAllBytesAsync(
                artifacts.CheckpointPath,
                Cancellation));
        if (targetKind == "pending")
        {
            Assert.Equal(
                preparedBefore,
                await File.ReadAllBytesAsync(
                    artifacts.DataPath,
                    Cancellation));
        }
    }

    [Theory]
    [InlineData("prepared")]
    [InlineData("resumable-publication")]
    [InlineData("restart-publication")]
    public async Task ReservedSnapshotLeafInDifferentParent_IsRejectedBeforeSourceOpen(
        string route)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(
                workspace,
                $"reserved-other-{route}");
        string otherParent =
            workspace.CreateDirectory(
                $"foreign-{route}");
        string reservedSnapshot =
            Path.Combine(
                otherParent,
                ".csharpdb-json-export-foreign.prepared");
        File.Copy(
            receipt.SnapshotPath,
            reservedSnapshot);
        string destinationPath =
            workspace.PathFor(
                $"reserved-other-{route}.ndjson");
        string manifestPath =
            workspace.PathFor(
                $"reserved-other-{route}.manifest.json");
        CSharpDbRetainedJsonExportRequest request =
            Request(
                receipt,
                destinationPath,
                "export_rows",
                JsonExportFraming.Ndjson) with
            {
                SnapshotPath = reservedSnapshot,
            };
        var adapter =
            new CSharpDbJsonExportAdapter();
        Func<Task> action =
            route switch
            {
                "prepared" =>
                    () => adapter
                        .WriteResumableTableAsync(
                            request,
                            Cancellation)
                        .AsTask(),
                "resumable-publication" =>
                    () => adapter
                        .WriteResumableAndPublishTableAsync(
                            request,
                            manifestPath,
                            Cancellation)
                        .AsTask(),
                "restart-publication" =>
                    () => adapter
                        .WriteAndPublishTableAsync(
                            request,
                            manifestPath,
                            Cancellation)
                        .AsTask(),
                _ =>
                    throw new InvalidOperationException(
                        "Unknown adapter route."),
            };

        await Assert.ThrowsAsync<ArgumentException>(
            action);

        Assert.True(
            File.Exists(reservedSnapshot));
        Assert.False(
            File.Exists(destinationPath));
        Assert.False(
            File.Exists(manifestPath));
        Assert.Empty(
            Directory.EnumerateFiles(
                workspace.Root,
                ".csharpdb-json-export-*",
                SearchOption.TopDirectoryOnly));
    }

    [Theory]
    [InlineData("prepared")]
    [InlineData("resumable-publication")]
    [InlineData("restart-publication")]
    public async Task WindowsTildeSourceSegment_IsRejectedBeforeSourceOpen(
        string route)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(
                workspace,
                $"tilde-{route}");
        string destinationPath =
            workspace.PathFor(
                $"tilde-{route}.ndjson");
        string manifestPath =
            workspace.PathFor(
                $"tilde-{route}.manifest.json");
        CSharpDbRetainedJsonExportRequest request =
            Request(
                receipt,
                destinationPath,
                "export_rows",
                JsonExportFraming.Ndjson) with
            {
                SnapshotPath =
                    Path.Combine(
                        workspace.Root,
                        "SOURCE~1",
                        "snapshot.db"),
            };
        var adapter =
            new CSharpDbJsonExportAdapter();
        Func<Task> action =
            route switch
            {
                "prepared" =>
                    () => adapter
                        .WriteResumableTableAsync(
                            request,
                            Cancellation)
                        .AsTask(),
                "resumable-publication" =>
                    () => adapter
                        .WriteResumableAndPublishTableAsync(
                            request,
                            manifestPath,
                            Cancellation)
                        .AsTask(),
                "restart-publication" =>
                    () => adapter
                        .WriteAndPublishTableAsync(
                            request,
                            manifestPath,
                            Cancellation)
                        .AsTask(),
                _ =>
                    throw new InvalidOperationException(
                        "Unknown adapter route."),
            };

        await Assert.ThrowsAsync<ArgumentException>(
            action);

        Assert.False(
            File.Exists(destinationPath));
        Assert.False(
            File.Exists(manifestPath));
        Assert.Empty(
            Directory.EnumerateFiles(
                workspace.Root,
                ".csharpdb-json-export-*",
                SearchOption.TopDirectoryOnly));
    }

    [Fact]
    public async Task ManifestJournalAlias_FailsBeforeSourceOrPrivateMutation()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(
                workspace,
                "journal-alias");
        string destinationPath =
            workspace.PathFor("journal-alias.json");
        CSharpDbRetainedJsonExportRequest request =
            Request(
                receipt,
                destinationPath,
                "export_rows",
                JsonExportFraming.RootArray) with
            {
                CheckpointRowInterval = 1,
            };

        _ = await new CSharpDbJsonExportAdapter()
            .WriteResumableTableAsync(
                request,
                Cancellation);
        PreparedArtifacts artifacts =
            FindPreparedArtifacts(workspace.Root);
        byte[] preparedBefore =
            await File.ReadAllBytesAsync(
                artifacts.DataPath,
                Cancellation);
        byte[] checkpointBefore =
            await File.ReadAllBytesAsync(
                artifacts.CheckpointPath,
                Cancellation);
        byte[] pendingBefore =
            "private-pending-sentinel"u8.ToArray();
        await WritePrivateFileAsync(
            artifacts.PendingCheckpointPath,
            pendingBefore,
            Cancellation);
        byte[] tamperedSnapshot =
            await File.ReadAllBytesAsync(
                receipt.SnapshotPath,
                Cancellation);
        tamperedSnapshot[
            tamperedSnapshot.Length / 2] ^= 0x80;
        await File.WriteAllBytesAsync(
            receipt.SnapshotPath,
            tamperedSnapshot,
            Cancellation);

        await Assert.ThrowsAsync<ArgumentException>(
            () => new CSharpDbJsonExportAdapter()
                .WriteResumableAndPublishTableAsync(
                    request,
                    artifacts.PendingCheckpointPath,
                    Cancellation)
                .AsTask());

        Assert.Equal(
            tamperedSnapshot,
            await File.ReadAllBytesAsync(
                receipt.SnapshotPath,
                Cancellation));
        Assert.Equal(
            preparedBefore,
            await File.ReadAllBytesAsync(
                artifacts.DataPath,
                Cancellation));
        Assert.Equal(
            checkpointBefore,
            await File.ReadAllBytesAsync(
                artifacts.CheckpointPath,
                Cancellation));
        Assert.Equal(
            pendingBefore,
            await File.ReadAllBytesAsync(
                artifacts.PendingCheckpointPath,
                Cancellation));
        Assert.False(File.Exists(destinationPath));
    }

    [Fact]
    public async Task MissingTableAndPreCancellation_CreateNoPublication()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(workspace, "failures");
        string destinationPath =
            workspace.PathFor("failure.json");
        string manifestPath =
            workspace.PathFor("failure.manifest.json");
        CSharpDbRetainedJsonExportRequest missing =
            Request(
                receipt,
                destinationPath,
                "missing_table",
                JsonExportFraming.RootArray);

        await Assert.ThrowsAnyAsync<Exception>(
            () => new CSharpDbJsonExportAdapter()
                .WriteAndPublishTableAsync(
                    missing,
                    manifestPath,
                    Cancellation)
                .AsTask());
        Assert.False(File.Exists(destinationPath));
        Assert.False(File.Exists(manifestPath));

        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        CSharpDbRetainedJsonExportRequest canceled =
            missing with { TableName = "export_rows" };
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => new CSharpDbJsonExportAdapter()
                .WriteAndPublishTableAsync(
                    canceled,
                    manifestPath,
                    cancellation.Token)
                .AsTask());
        Assert.False(File.Exists(destinationPath));
        Assert.False(File.Exists(manifestPath));
    }

    [Theory]
    [InlineData("export_view")]
    [InlineData("sys.tables")]
    [InlineData("missing_rows")]
    public async Task InvalidPhysicalSource_CreatesNoPublicationAuthority(
        string tableName)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(
                workspace,
                $"invalid-{tableName.Replace('.', '-')}");
        string destinationPath =
            workspace.PathFor("invalid.json");
        string manifestPath =
            workspace.PathFor("invalid.manifest.json");

        Exception? error = await Record.ExceptionAsync(
            () => new CSharpDbJsonExportAdapter()
                .WriteAndPublishTableAsync(
                    Request(
                        receipt,
                        destinationPath,
                        tableName,
                        JsonExportFraming.RootArray),
                    manifestPath,
                    Cancellation)
                .AsTask());

        Assert.NotNull(error);
        if (tableName == "missing_rows")
        {
            CSharpDbException missing =
                Assert.IsType<CSharpDbException>(error);
            Assert.Equal(ErrorCode.TableNotFound, missing.Code);
        }
        else
        {
            Assert.IsType<InvalidOperationException>(error);
        }
        Assert.False(File.Exists(destinationPath));
        Assert.False(File.Exists(manifestPath));
        Assert.Empty(Directory.EnumerateFiles(
            workspace.Root,
            ".csharpdb-json-export-*",
            SearchOption.TopDirectoryOnly));
        Assert.False(
            File.Exists(receipt.SnapshotPath + ".wal"));
    }

    private static async Task<RetainedDatabaseSnapshotReceipt>
        CreateSnapshotAsync(
            TemporaryDirectory workspace,
            string name)
    {
        string sourcePath =
            workspace.PathFor($"{name}-source.db");
        await using (Database database =
                     await Database.OpenAsync(
                         sourcePath,
                         Cancellation))
        {
            await database.ExecuteAsync(
                """
                CREATE TABLE export_rows (
                    id INTEGER PRIMARY KEY,
                    note TEXT,
                    amount REAL NOT NULL
                )
                """,
                Cancellation);
            await database.ExecuteAsync(
                "INSERT INTO export_rows VALUES (3, 'a,\"b\"', 3.5)",
                Cancellation);
            await database.ExecuteAsync(
                "INSERT INTO export_rows VALUES (-2, NULL, -2.25)",
                Cancellation);
            await database.ExecuteAsync(
                "CREATE VIEW export_view AS SELECT id, note, amount FROM export_rows",
                Cancellation);
            await database.CheckpointAsync(Cancellation);
        }

        return await RetainedDatabaseSnapshot.CaptureAsync(
            sourcePath,
            workspace.PathFor($"{name}-snapshot.db"),
            databaseOptions: null,
            new RetainedDatabaseSnapshotOptions
            {
                WorkspacePath = workspace.CreateDirectory(
                    $"{name}-capture-workspace"),
            },
            Cancellation);
    }

    private static CSharpDbRetainedJsonExportRequest Request(
        RetainedDatabaseSnapshotReceipt receipt,
        string destinationPath,
        string tableName,
        JsonExportFraming framing) => new()
        {
            SnapshotPath = receipt.SnapshotPath,
            SnapshotIdentity = receipt.Identity,
            TableName = tableName,
            DestinationPath = destinationPath,
            Framing = framing,
            MaxDataBytes = 1L << 20,
            MaximumDecodedBlobBytes = 1_024,
        };

    private static JsonExportSourceManifest Source(
        RetainedDatabaseSnapshotReceipt receipt) =>
        new()
        {
            Kind = JsonExportContracts.SourceKind,
            Version = ReaderVersion(),
            SnapshotByteLength = receipt.ByteLength,
            SnapshotDigest =
                new JsonExportHashManifest
                {
                    Algorithm =
                        JsonExportHashManifest
                            .Sha256Algorithm,
                    Value =
                        receipt.Sha256[
                            "sha256:".Length..],
                },
        };

    private static string ReaderVersion()
    {
        Assembly assembly =
            typeof(
                RetainedDatabaseSnapshotSession)
                .Assembly;
        string? informational =
            assembly
                .GetCustomAttribute<
                    AssemblyInformationalVersionAttribute>()?
                .InformationalVersion;
        return informational?.Split('+', 2)[0] ??
               assembly.GetName().Version?
                   .ToString() ??
               throw new InvalidOperationException(
                   "The retained snapshot reader version is unavailable.");
    }

    private static async IAsyncEnumerable<
        JsonExportRow>
        ReadRowsThenThrowAsync(
        RetainedDatabaseSnapshotSession session,
        string tableName,
        int countBeforeFailure,
        [EnumeratorCancellation]
        CancellationToken cancellationToken)
    {
        await using RetainedDatabaseSnapshotTableReader
            reader =
                session.OpenTableReader(tableName);
        int yielded = 0;
        while (await reader
                   .MoveNextAsync(cancellationToken)
                   .ConfigureAwait(false))
        {
            yield return new JsonExportRow(
                reader.CurrentRowId,
                reader.Current);
            yielded++;
            if (yielded == countBeforeFailure)
            {
                throw new InjectedReadFailure();
            }
        }
    }

    private static RetainedDatabaseSnapshotOptions
        SnapshotOptions(
        string workspacePath) =>
        new()
        {
            WorkspacePath = workspacePath,
        };

    private static PreparedArtifacts
        FindPreparedArtifacts(
        string root)
    {
        string dataPath =
            Assert.Single(
                Directory.EnumerateFiles(
                    root,
                    ".csharpdb-json-export-*.prepared",
                    SearchOption.TopDirectoryOnly));
        string stem =
            dataPath[..^".prepared".Length];
        return new PreparedArtifacts(
            dataPath,
            stem + ".checkpoint",
            stem + ".checkpoint.next");
    }

    private static JsonExportCheckpoint
        ReadCheckpoint(
        string path) =>
        JsonExportCheckpointSerializer.Deserialize(
            File.ReadAllBytes(path));

    private static async Task WritePrivateFileAsync(
        string path,
        byte[] bytes,
        CancellationToken cancellationToken)
    {
        if (!OperatingSystem.IsWindows())
        {
            throw new PlatformNotSupportedException(
                "Private prepared-output test files require Windows ACLs.");
        }

        using WindowsIdentity identity =
            WindowsIdentity.GetCurrent(
                TokenAccessLevels.Query);
        SecurityIdentifier owner =
            identity.User ??
            throw new InvalidOperationException(
                "The current Windows test identity has no security identifier.");
        var security = new FileSecurity();
        security.SetOwner(owner);
        security.SetAccessRuleProtection(
            isProtected: true,
            preserveInheritance: false);
        security.AddAccessRule(
            new FileSystemAccessRule(
                owner,
                FileSystemRights.FullControl,
                AccessControlType.Allow));
        await using FileStream stream =
            FileSystemAclExtensions.Create(
                new FileInfo(path),
                FileMode.CreateNew,
                FileSystemRights.FullControl,
                FileShare.None,
                bufferSize: 4096,
                FileOptions.Asynchronous |
                FileOptions.WriteThrough,
                security);
        await stream.WriteAsync(
            bytes,
            cancellationToken);
        stream.Flush(flushToDisk: true);
    }

    private static string DifferentCanonicalIdentity(
        string identity)
    {
        char replacement = identity[^1] == '0' ? '1' : '0';
        return identity[..^1] + replacement;
    }

    private static string Sha256(
        ReadOnlySpan<byte> bytes) =>
        Convert.ToHexString(SHA256.HashData(bytes))
            .ToLowerInvariant();

    private sealed record PreparedArtifacts(
        string DataPath,
        string CheckpointPath,
        string PendingCheckpointPath);

    private sealed class InjectedReadFailure :
        Exception;

    private sealed class TemporaryDirectory : IDisposable
    {
        public TemporaryDirectory()
        {
            Root = Path.GetFullPath(Path.Combine(
                Path.GetTempPath(),
                "csharpdb-json-export-adapter-tests",
                Guid.NewGuid().ToString("N")));
            Directory.CreateDirectory(Root);
        }

        public string Root { get; }

        public string PathFor(string leaf) =>
            Path.Combine(Root, leaf);

        public string CreateDirectory(string leaf)
        {
            string path = PathFor(leaf);
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
}
