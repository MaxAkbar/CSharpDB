using System.Globalization;
using System.Security.AccessControl;
using System.Security.Cryptography;
using System.Security.Principal;
using System.Text;
using CSharpDB.Migration.Files.Csv;

namespace CSharpDB.Migration.Files.Tests;

public sealed class CsvExportPreparedOutputLeaseTests
{
    private static readonly CancellationToken Cancellation =
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task NewLease_PersistsHeaderCheckpoint_AndReopensRecovered()
    {
        using var workspace = new TemporaryDirectory();
        string destinationPath = workspace.PathFor("orders.csv");
        CsvExportCheckpointBinding binding = CreateBinding();
        byte[] header = HeaderBytes();
        CsvExportPreparedOutputPaths paths;

        await using (CsvExportPreparedOutputLease lease =
                     await CsvExportPreparedOutputLease.OpenAsync(
                         destinationPath,
                         binding,
                         Cancellation))
        {
            Assert.Equal(destinationPath, lease.DestinationPath);
            Assert.Equal(CsvExportPreparedOutputState.New, lease.State);
            Assert.Null(lease.CurrentCheckpoint);
            Assert.False(File.Exists(destinationPath));

            paths = lease.Paths;
            Assert.Equal(
                Path.GetDirectoryName(destinationPath),
                Path.GetDirectoryName(paths.PreparedDataPath));
            Assert.Equal(
                Path.GetDirectoryName(destinationPath),
                Path.GetDirectoryName(paths.CheckpointPath));
            Assert.Equal(
                Path.GetDirectoryName(destinationPath),
                Path.GetDirectoryName(paths.PendingCheckpointPath));
            Assert.NotEqual(destinationPath, paths.PreparedDataPath);
            Assert.NotEqual(destinationPath, paths.CheckpointPath);
            Assert.NotEqual(paths.CheckpointPath, paths.PendingCheckpointPath);

            await lease.DataStream.WriteAsync(header, Cancellation);
            CsvExportCheckpoint checkpoint =
                CreateWritingCheckpoint(binding, generation: 0, header);
            await lease.PersistCheckpointAsync(checkpoint, Cancellation);

            Assert.Equal(0, lease.CurrentCheckpoint!.Generation);
            Assert.True(File.Exists(paths.PreparedDataPath));
            Assert.True(File.Exists(paths.CheckpointPath));
            Assert.False(File.Exists(destinationPath));
        }

        await using (CsvExportPreparedOutputLease recovered =
                     await CsvExportPreparedOutputLease.OpenAsync(
                         destinationPath,
                         binding,
                         Cancellation))
        {
            Assert.Equal(CsvExportPreparedOutputState.Recovered, recovered.State);
            Assert.Equal(0, recovered.CurrentCheckpoint!.Generation);
            Assert.Equal(header.LongLength, recovered.DataStream.Length);

            recovered.DataStream.Position = 0;
            byte[] actual = new byte[header.Length];
            await recovered.DataStream.ReadExactlyAsync(actual, Cancellation);
            Assert.Equal(header, actual);
            Assert.False(File.Exists(destinationPath));
        }
    }

    [Fact]
    public async Task OpenAsync_HoldsAnExclusivePreparedOutputLease()
    {
        using var workspace = new TemporaryDirectory();
        string destinationPath = workspace.PathFor("exclusive.csv");
        CsvExportCheckpointBinding binding = CreateBinding();

        await using CsvExportPreparedOutputLease first =
            await CsvExportPreparedOutputLease.OpenAsync(
                destinationPath,
                binding,
                Cancellation);

        await Assert.ThrowsAsync<IOException>(
            async () => await CsvExportPreparedOutputLease.OpenAsync(
                destinationPath,
                binding,
                Cancellation));

        Assert.False(File.Exists(destinationPath));
    }

    [Fact]
    public async Task PersistCheckpoint_EmitsEveryDurableFaultBoundaryInOrder()
    {
        using var workspace = new TemporaryDirectory();
        string destinationPath = workspace.PathFor("fault-boundaries.csv");
        CsvExportCheckpointBinding binding = CreateBinding();
        byte[] header = HeaderBytes();
        var injector = new RecordingCheckpointFaultInjector();

        await using CsvExportPreparedOutputLease lease =
            await CsvExportPreparedOutputLease
                .OpenWithCheckpointFaultInjectorAsync(
                    destinationPath,
                    binding,
                    injector,
                    Cancellation);
        await lease.DataStream.WriteAsync(header, Cancellation);
        await lease.PersistCheckpointAsync(
            CreateWritingCheckpoint(binding, generation: 0, header),
            Cancellation);

        Assert.Equal(
            Enum.GetValues<CsvExportCheckpointFaultPoint>(),
            injector.ObservedPoints);
        Assert.Equal(0, lease.CurrentCheckpoint!.Generation);
    }

    [Fact]
    public async Task UncheckpointedData_IsInaccessibleAndPreservedUntilExplicitReset()
    {
        using var workspace = new TemporaryDirectory();
        string destinationPath = workspace.PathFor("uncheckpointed.csv");
        CsvExportCheckpointBinding binding = CreateBinding();
        byte[] uncheckpointed = Encoding.UTF8.GetBytes("not-yet-durable\r\n");
        CsvExportPreparedOutputPaths paths;

        await using (CsvExportPreparedOutputLease created =
                     await CsvExportPreparedOutputLease.OpenAsync(
                         destinationPath,
                         binding,
                         Cancellation))
        {
            paths = created.Paths;
            await created.DataStream.WriteAsync(uncheckpointed, Cancellation);
        }

        await using (CsvExportPreparedOutputLease blocked =
                     await CsvExportPreparedOutputLease.OpenAsync(
                         destinationPath,
                         binding,
                         Cancellation))
        {
            Assert.Equal(
                CsvExportPreparedOutputState.UncheckpointedData,
                blocked.State);
            Assert.Null(blocked.CurrentCheckpoint);
            Assert.Throws<InvalidOperationException>(() => blocked.DataStream);
            Assert.Equal(
                uncheckpointed.LongLength,
                new FileInfo(paths.PreparedDataPath).Length);
        }

        Assert.Equal(uncheckpointed, await File.ReadAllBytesAsync(
            paths.PreparedDataPath,
            Cancellation));

        await using (CsvExportPreparedOutputLease reset =
                     await CsvExportPreparedOutputLease.OpenAsync(
                         destinationPath,
                         binding,
                         Cancellation))
        {
            Assert.Equal(
                CsvExportPreparedOutputState.UncheckpointedData,
                reset.State);

            await reset.ResetUncheckpointedAsync(Cancellation);

            Assert.Equal(CsvExportPreparedOutputState.New, reset.State);
            Assert.Null(reset.CurrentCheckpoint);
            Assert.Equal(0, reset.DataStream.Length);
            Assert.Equal(0, reset.DataStream.Position);
            Assert.False(File.Exists(destinationPath));
        }
    }

    [Fact]
    public async Task Recovery_VerifiesCheckpointedPrefixBeforeTruncatingTail()
    {
        using var workspace = new TemporaryDirectory();
        string destinationPath = workspace.PathFor("tail.csv");
        CsvExportCheckpointBinding binding = CreateBinding();
        byte[] header = HeaderBytes();
        CsvExportPreparedOutputPaths paths = await PersistHeaderCheckpointAsync(
            destinationPath,
            binding,
            header);
        byte[] tail = Encoding.UTF8.GetBytes("partial,row");

        await using (var append = new FileStream(
                         paths.PreparedDataPath,
                         FileMode.Append,
                         FileAccess.Write,
                         FileShare.None))
        {
            await append.WriteAsync(tail, Cancellation);
            append.Flush(flushToDisk: true);
        }

        await using (CsvExportPreparedOutputLease recovered =
                     await CsvExportPreparedOutputLease.OpenAsync(
                         destinationPath,
                         binding,
                         Cancellation))
        {
            Assert.Equal(CsvExportPreparedOutputState.Recovered, recovered.State);
            Assert.Equal(header.LongLength, recovered.DataStream.Length);
            Assert.Equal(header.LongLength, recovered.DataStream.Position);
            Assert.Equal(0, recovered.CurrentCheckpoint!.Generation);
        }

        Assert.Equal(
            header,
            await File.ReadAllBytesAsync(paths.PreparedDataPath, Cancellation));
        Assert.False(File.Exists(destinationPath));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Recovery_RejectsAlteredOrShortPrefixWithoutMutation(
        bool truncatePrefix)
    {
        using var workspace = new TemporaryDirectory();
        string destinationPath = workspace.PathFor(
            truncatePrefix ? "short.csv" : "altered.csv");
        CsvExportCheckpointBinding binding = CreateBinding();
        byte[] header = HeaderBytes();
        CsvExportPreparedOutputPaths paths = await PersistHeaderCheckpointAsync(
            destinationPath,
            binding,
            header);
        byte[] invalid;
        if (truncatePrefix)
        {
            invalid = header[..^1];
        }
        else
        {
            invalid = [.. header, .. Encoding.UTF8.GetBytes("tail")];
            invalid[0] ^= 0x01;
        }
        await File.WriteAllBytesAsync(
            paths.PreparedDataPath,
            invalid,
            Cancellation);

        await Assert.ThrowsAsync<InvalidDataException>(
            async () => await CsvExportPreparedOutputLease.OpenAsync(
                destinationPath,
                binding,
                Cancellation));

        Assert.Equal(
            invalid,
            await File.ReadAllBytesAsync(paths.PreparedDataPath, Cancellation));
        Assert.False(File.Exists(destinationPath));
    }

    [Fact]
    public async Task Recovery_RejectsBindingMismatchWithoutChangingPreparedData()
    {
        using var workspace = new TemporaryDirectory();
        string destinationPath = workspace.PathFor("binding.csv");
        CsvExportCheckpointBinding original = CreateBinding();
        CsvExportCheckpointBinding changed = CreateBinding(snapshotDigestValue: 'b');
        byte[] header = HeaderBytes();
        CsvExportPreparedOutputPaths paths = await PersistHeaderCheckpointAsync(
            destinationPath,
            original,
            header);
        byte[] before = await File.ReadAllBytesAsync(
            paths.PreparedDataPath,
            Cancellation);

        await Assert.ThrowsAsync<InvalidDataException>(
            async () => await CsvExportPreparedOutputLease.OpenAsync(
                destinationPath,
                changed,
                Cancellation));

        Assert.Equal(
            before,
            await File.ReadAllBytesAsync(paths.PreparedDataPath, Cancellation));
        Assert.False(File.Exists(destinationPath));
    }

    [Fact]
    public async Task PersistCheckpoint_EnforcesGenerationIdempotenceAndProgress()
    {
        using var workspace = new TemporaryDirectory();
        string destinationPath = workspace.PathFor("generations.csv");
        CsvExportCheckpointBinding binding = CreateBinding();
        byte[] header = HeaderBytes();
        byte[] firstRow = Encoding.UTF8.GetBytes("1,alpha\r\n");

        await using CsvExportPreparedOutputLease lease =
            await CsvExportPreparedOutputLease.OpenAsync(
                destinationPath,
                binding,
                Cancellation);
        await lease.DataStream.WriteAsync(header, Cancellation);
        CsvExportCheckpoint generation0 =
            CreateWritingCheckpoint(binding, generation: 0, header);

        await lease.PersistCheckpointAsync(generation0, Cancellation);
        await lease.PersistCheckpointAsync(generation0, Cancellation);

        CsvExportCheckpoint conflictingGeneration0 =
            CreateCompletedCheckpoint(binding, generation: 0, header);
        await AssertRejectedAsync(
            async () => await lease.PersistCheckpointAsync(
                conflictingGeneration0,
                Cancellation));

        CsvExportCheckpoint skippedGeneration =
            CreateWritingCheckpoint(binding, generation: 2, header);
        await AssertRejectedAsync(
            async () => await lease.PersistCheckpointAsync(
                skippedGeneration,
                Cancellation));

        await lease.DataStream.WriteAsync(firstRow, Cancellation);
        byte[] firstRowPrefix = [.. header, .. firstRow];
        CsvExportCheckpoint generation1 = CreateWritingCheckpoint(
            binding,
            generation: 1,
            firstRowPrefix,
            completedRowCount: 1,
            lastCompletedRowId: -7);
        await lease.PersistCheckpointAsync(generation1, Cancellation);

        CsvExportCheckpoint regressedGeneration2 =
            CreateWritingCheckpoint(binding, generation: 2, header);
        await AssertRejectedAsync(
            async () => await lease.PersistCheckpointAsync(
                regressedGeneration2,
                Cancellation));

        Assert.Equal(1, lease.CurrentCheckpoint!.Generation);
        Assert.Equal(1, lease.CurrentCheckpoint.Progress.CompletedRowCount);
        Assert.False(File.Exists(destinationPath));
    }

    [Fact]
    public async Task PersistCheckpoint_WrongDataDigestPreservesLastGoodGeneration()
    {
        using var workspace = new TemporaryDirectory();
        string destinationPath = workspace.PathFor("wrong-digest.csv");
        CsvExportCheckpointBinding binding = CreateBinding();
        byte[] header = HeaderBytes();
        byte[] firstRow = Encoding.UTF8.GetBytes("1,alpha\r\n");
        CsvExportPreparedOutputPaths paths;

        await using (CsvExportPreparedOutputLease lease =
                     await CsvExportPreparedOutputLease.OpenAsync(
                         destinationPath,
                         binding,
                         Cancellation))
        {
            paths = lease.Paths;
            await lease.DataStream.WriteAsync(header, Cancellation);
            CsvExportCheckpoint generation0 =
                CreateWritingCheckpoint(binding, generation: 0, header);
            await lease.PersistCheckpointAsync(generation0, Cancellation);

            await lease.DataStream.WriteAsync(firstRow, Cancellation);
            byte[] firstRowPrefix = [.. header, .. firstRow];
            CsvExportCheckpoint generation1 = CreateWritingCheckpoint(
                binding,
                generation: 1,
                firstRowPrefix,
                completedRowCount: 1,
                lastCompletedRowId: 4);
            CsvExportCheckpoint wrongDigest = generation1 with
            {
                Progress = generation1.Progress with
                {
                    DataPrefixDigest = Hash('f'),
                },
            };

            await AssertRejectedAsync(
                async () => await lease.PersistCheckpointAsync(
                    wrongDigest,
                    Cancellation));

            Assert.Equal(0, lease.CurrentCheckpoint!.Generation);
            Assert.Equal(header.LongLength, lease.CurrentCheckpoint
                .Progress.DataPrefixByteLength);
            Assert.Equal(
                firstRowPrefix.LongLength,
                lease.DataStream.Length);
        }

        await using (CsvExportPreparedOutputLease recovered =
                     await CsvExportPreparedOutputLease.OpenAsync(
                         destinationPath,
                         binding,
                         Cancellation))
        {
            Assert.Equal(CsvExportPreparedOutputState.Recovered, recovered.State);
            Assert.Equal(0, recovered.CurrentCheckpoint!.Generation);
            Assert.Equal(header.LongLength, recovered.DataStream.Length);
            Assert.Equal(header.LongLength, recovered.DataStream.Position);
        }

        Assert.Equal(
            header,
            await File.ReadAllBytesAsync(paths.PreparedDataPath, Cancellation));
        Assert.False(File.Exists(destinationPath));
    }

    [Fact]
    public async Task PersistCheckpoint_DataCompleteIsTerminalAndRecovers()
    {
        using var workspace = new TemporaryDirectory();
        string destinationPath = workspace.PathFor("complete.csv");
        CsvExportCheckpointBinding binding = CreateBinding();
        byte[] header = HeaderBytes();

        await using (CsvExportPreparedOutputLease lease =
                     await CsvExportPreparedOutputLease.OpenAsync(
                         destinationPath,
                         binding,
                         Cancellation))
        {
            await lease.DataStream.WriteAsync(header, Cancellation);
            await lease.PersistCheckpointAsync(
                CreateWritingCheckpoint(binding, generation: 0, header),
                Cancellation);
            CsvExportCheckpoint complete =
                CreateCompletedCheckpoint(binding, generation: 1, header);
            await lease.PersistCheckpointAsync(complete, Cancellation);

            CsvExportCheckpoint writingAgain =
                CreateWritingCheckpoint(binding, generation: 2, header);
            await AssertRejectedAsync(
                async () => await lease.PersistCheckpointAsync(
                    writingAgain,
                    Cancellation));

            Assert.Equal(
                CsvExportCheckpointPhase.DataComplete,
                lease.CurrentCheckpoint!.Phase);
        }

        await using CsvExportPreparedOutputLease recovered =
            await CsvExportPreparedOutputLease.OpenAsync(
                destinationPath,
                binding,
                Cancellation);
        Assert.Equal(CsvExportPreparedOutputState.Recovered, recovered.State);
        Assert.Equal(
            CsvExportCheckpointPhase.DataComplete,
            recovered.CurrentCheckpoint!.Phase);
        Assert.Equal(1, recovered.CurrentCheckpoint.Generation);
        Assert.False(File.Exists(destinationPath));
    }

    [Fact]
    public async Task Recovery_ActiveCheckpointAlwaysOutranksPendingCheckpoint()
    {
        using var workspace = new TemporaryDirectory();
        string destinationPath = workspace.PathFor("pending.csv");
        CsvExportCheckpointBinding binding = CreateBinding();
        byte[] header = HeaderBytes();
        CsvExportPreparedOutputPaths paths = await PersistHeaderCheckpointAsync(
            destinationPath,
            binding,
            header);
        CsvExportCheckpoint pending =
            CreateWritingCheckpoint(binding, generation: 99, header);
        await WritePrivateFileAsync(
            paths.PendingCheckpointPath,
            CsvExportCheckpointSerializer.Serialize(pending),
            Cancellation);

        await using CsvExportPreparedOutputLease recovered =
            await CsvExportPreparedOutputLease.OpenAsync(
                destinationPath,
                binding,
                Cancellation);

        Assert.Equal(CsvExportPreparedOutputState.Recovered, recovered.State);
        Assert.Equal(0, recovered.CurrentCheckpoint!.Generation);
        Assert.Equal(header.LongLength, recovered.DataStream.Length);
        Assert.False(File.Exists(destinationPath));
    }

    [Fact]
    public async Task Recovery_IgnoresTornPendingCheckpointBytes()
    {
        using var workspace = new TemporaryDirectory();
        string destinationPath = workspace.PathFor("torn-pending.csv");
        CsvExportCheckpointBinding binding = CreateBinding();
        byte[] header = HeaderBytes();
        CsvExportPreparedOutputPaths paths = await PersistHeaderCheckpointAsync(
            destinationPath,
            binding,
            header);
        byte[] torn = Encoding.UTF8.GetBytes("{\"format\":\"torn");
        await WritePrivateFileAsync(
            paths.PendingCheckpointPath,
            torn,
            Cancellation);

        await using CsvExportPreparedOutputLease recovered =
            await CsvExportPreparedOutputLease.OpenAsync(
                destinationPath,
                binding,
                Cancellation);

        Assert.Equal(CsvExportPreparedOutputState.Recovered, recovered.State);
        Assert.Equal(0, recovered.CurrentCheckpoint!.Generation);
        Assert.Equal(header.LongLength, recovered.DataStream.Length);
        Assert.Equal(
            torn,
            await File.ReadAllBytesAsync(
                paths.PendingCheckpointPath,
                Cancellation));
        Assert.False(File.Exists(destinationPath));
    }

    private static async Task<CsvExportPreparedOutputPaths>
        PersistHeaderCheckpointAsync(
            string destinationPath,
            CsvExportCheckpointBinding binding,
            byte[] header)
    {
        await using CsvExportPreparedOutputLease lease =
            await CsvExportPreparedOutputLease.OpenAsync(
                destinationPath,
                binding,
                Cancellation);
        await lease.DataStream.WriteAsync(header, Cancellation);
        await lease.PersistCheckpointAsync(
            CreateWritingCheckpoint(binding, generation: 0, header),
            Cancellation);
        return lease.Paths;
    }

    private static CsvExportCheckpoint CreateWritingCheckpoint(
        CsvExportCheckpointBinding binding,
        long generation,
        byte[] prefix,
        long completedRowCount = 0,
        long? lastCompletedRowId = null)
    {
        CsvExportCheckpointProgress progress = CreateProgress(
            prefix,
            completedRowCount,
            lastCompletedRowId);
        return new CsvExportCheckpoint
        {
            Generation = generation,
            Phase = CsvExportCheckpointPhase.Writing,
            Binding = binding,
            BindingDigest =
                CsvExportCheckpointSerializer.ComputeBindingDigest(binding),
            Progress = progress,
        };
    }

    private static CsvExportCheckpoint CreateCompletedCheckpoint(
        CsvExportCheckpointBinding binding,
        long generation,
        byte[] prefix,
        long completedRowCount = 0,
        long? lastCompletedRowId = null)
    {
        CsvExportCheckpointProgress progress = CreateProgress(
            prefix,
            completedRowCount,
            lastCompletedRowId);
        CsvExportHashManifest finalLogicalDigest =
            completedRowCount == 0 ? EmptyCompletedLogicalDigest() : Hash('d');
        var preliminaryCompletion = new CsvExportCheckpointCompletion
        {
            SourceLogicalDigest = finalLogicalDigest,
            ExportedLogicalDigest = finalLogicalDigest,
            ManifestDigest = new string('0', 64),
        };
        CsvExportManifest preliminaryManifest = CreateCompletedManifest(
            binding,
            progress,
            preliminaryCompletion);
        CsvExportCheckpointCompletion completion = preliminaryCompletion with
        {
            ManifestDigest =
                CsvExportManifestSerializer.ComputeManifestDigest(
                    preliminaryManifest),
        };
        return new CsvExportCheckpoint
        {
            Generation = generation,
            Phase = CsvExportCheckpointPhase.DataComplete,
            Binding = binding,
            BindingDigest =
                CsvExportCheckpointSerializer.ComputeBindingDigest(binding),
            Progress = progress,
            Completion = completion,
        };
    }

    private static CsvExportCheckpointProgress CreateProgress(
        byte[] prefix,
        long completedRowCount,
        long? lastCompletedRowId)
    {
        CsvExportHashManifest logicalPrefix = completedRowCount == 0
            ? EmptyLogicalPrefixDigest()
            : Hash('c');
        return new CsvExportCheckpointProgress
        {
            CompletedRowCount = completedRowCount,
            LastCompletedRowId = lastCompletedRowId,
            DataPrefixByteLength = prefix.LongLength,
            DataPrefixDigest = HashBytes(prefix),
            LogicalPrefixAggregation =
                CsvExportCheckpointContracts.LogicalPrefixAggregation,
            SourceLogicalRowHashPrefixDigest = logicalPrefix,
            ExportedLogicalRowHashPrefixDigest = logicalPrefix,
            TransformedRowCount = 0,
            TransformedCellCount = 0,
        };
    }

    private static CsvExportManifest CreateCompletedManifest(
        CsvExportCheckpointBinding binding,
        CsvExportCheckpointProgress progress,
        CsvExportCheckpointCompletion completion) => new()
        {
            Profile = binding.Profile,
            Source = binding.Source,
            Table = binding.Table,
            Csv = binding.Csv,
            Content = new CsvExportContentManifest
            {
                RowCount = progress.CompletedRowCount,
                DataByteLength = progress.DataPrefixByteLength,
                DataDigest = progress.DataPrefixDigest,
                Canonicalization = CsvExportContracts.Canonicalization,
                CanonicalizationContractDigest =
                    CsvExportContracts.CanonicalizationContractDigest,
                Aggregation = CsvExportContracts.OrderedContentDigest,
                SourceLogicalDigest = completion.SourceLogicalDigest,
                ExportedLogicalDigest = completion.ExportedLogicalDigest,
            },
        };

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

    private static byte[] HeaderBytes() => Encoding.UTF8.GetBytes("id,note\r\n");

    private static CsvExportHashManifest EmptyLogicalPrefixDigest()
    {
        using var digest = new CsvExportOrderedContentDigest();
        return digest.GetCurrentPrefixDigest();
    }

    private static CsvExportHashManifest EmptyCompletedLogicalDigest()
    {
        using var digest = new CsvExportOrderedContentDigest();
        return digest.Complete();
    }

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

    private static async Task WritePrivateFileAsync(
        string path,
        byte[] bytes,
        CancellationToken cancellationToken)
    {
        if (!OperatingSystem.IsWindows())
        {
            await File.WriteAllBytesAsync(path, bytes, cancellationToken);
            return;
        }

        using WindowsIdentity identity =
            WindowsIdentity.GetCurrent(TokenAccessLevels.Query);
        SecurityIdentifier owner = identity.User ??
            throw new InvalidOperationException(
                "The current Windows test identity has no security identifier.");
        var security = new FileSecurity();
        security.SetOwner(owner);
        security.SetAccessRuleProtection(
            isProtected: true,
            preserveInheritance: false);
        security.AddAccessRule(new FileSystemAccessRule(
            owner,
            FileSystemRights.FullControl,
            AccessControlType.Allow));
        await using FileStream stream = FileSystemAclExtensions.Create(
            new FileInfo(path),
            FileMode.CreateNew,
            FileSystemRights.FullControl,
            FileShare.None,
            bufferSize: 4096,
            FileOptions.Asynchronous | FileOptions.WriteThrough,
            security);
        await stream.WriteAsync(bytes, cancellationToken);
        stream.Flush(flushToDisk: true);
    }

    private static async Task AssertRejectedAsync(Func<Task> action)
    {
        Exception? exception = await Record.ExceptionAsync(action);
        Assert.NotNull(exception);
        Assert.True(
            exception is InvalidDataException or InvalidOperationException,
            $"Unexpected rejection type: {exception.GetType().FullName}");
    }

    private sealed class TemporaryDirectory : IDisposable
    {
        public TemporaryDirectory()
        {
            Root = Path.Combine(
                Path.GetTempPath(),
                "csharpdb-export-lease-tests",
                Guid.NewGuid().ToString("N"));
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

    private sealed class RecordingCheckpointFaultInjector
        : ICsvExportCheckpointFaultInjector
    {
        public List<CsvExportCheckpointFaultPoint> ObservedPoints { get; } =
            [];

        public ValueTask InjectAsync(
            CsvExportCheckpointFaultPoint point,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            ObservedPoints.Add(point);
            return ValueTask.CompletedTask;
        }
    }
}
