using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Migration.Files.Json;

namespace CSharpDB.Migration.Files.Tests;

public sealed class JsonExportCheckpointSerializerTests
{
    [Fact]
    public void CanonicalCheckpointIsStableAndRoundTripsAcrossCultures()
    {
        JsonExportCheckpoint checkpoint =
            CreateWritingCheckpoint();
        CultureInfo originalCulture =
            CultureInfo.CurrentCulture;
        CultureInfo originalUiCulture =
            CultureInfo.CurrentUICulture;
        try
        {
            CultureInfo.CurrentCulture =
                CultureInfo.GetCultureInfo("ar-SA");
            CultureInfo.CurrentUICulture =
                CultureInfo.GetCultureInfo("ar-SA");
            byte[] first =
                JsonExportCheckpointSerializer
                    .Serialize(checkpoint);

            CultureInfo.CurrentCulture =
                CultureInfo.GetCultureInfo("de-DE");
            CultureInfo.CurrentUICulture =
                CultureInfo.GetCultureInfo("de-DE");
            byte[] repeated =
                JsonExportCheckpointSerializer
                    .Serialize(checkpoint);
            JsonExportCheckpoint reopened =
                JsonExportCheckpointSerializer
                    .Deserialize(first);

            Assert.Equal(first, repeated);
            Assert.False(
                first.AsSpan().StartsWith(
                    Encoding.UTF8.Preamble));
            Assert.Equal(
                first,
                JsonExportCheckpointSerializer
                    .Serialize(reopened));
            Assert.Equal(
                checkpoint.Generation,
                reopened.Generation);
            Assert.Equal(
                checkpoint.BindingDigest,
                reopened.BindingDigest);
            Assert.Equal(
                JsonExportCheckpointSerializer
                    .ComputeCheckpointDigest(
                        checkpoint),
                JsonExportCheckpointSerializer
                    .ComputeCheckpointDigest(
                        reopened));
        }
        finally
        {
            CultureInfo.CurrentCulture =
                originalCulture;
            CultureInfo.CurrentUICulture =
                originalUiCulture;
        }
    }

    [Fact]
    public void BindingDigestBindsFramingLimitsAndRetainedSnapshot()
    {
        JsonExportCheckpointBinding binding =
            CreateBinding(
                JsonExportFraming.RootArray);
        JsonExportHashManifest digest =
            JsonExportCheckpointSerializer
                .ComputeBindingDigest(binding);

        Assert.Equal(
            JsonExportHashManifest.Sha256Algorithm,
            digest.Algorithm);
        Assert.NotEqual(
            digest.Value,
            JsonExportCheckpointSerializer
                .ComputeBindingDigest(
                    binding with
                    {
                        Json = binding.Json with
                        {
                            Framing =
                                JsonExportFraming
                                    .Ndjson,
                        },
                    })
                .Value);
        Assert.NotEqual(
            digest.Value,
            JsonExportCheckpointSerializer
                .ComputeBindingDigest(
                    binding with
                    {
                        Json = binding.Json with
                        {
                            MaxDataBytes =
                                binding.Json
                                    .MaxDataBytes +
                                1,
                        },
                    })
                .Value);

        Assert.Throws<InvalidDataException>(
            () =>
                JsonExportCheckpointSerializer
                    .ComputeBindingDigest(
                        binding with
                        {
                            SourceSnapshotIdentity =
                                SnapshotIdentity(
                                    binding.Source
                                        .SnapshotByteLength +
                                    1,
                                    binding.Source
                                        .SnapshotDigest
                                        .Value),
                        }));
    }

    [Fact]
    public void TamperedAndNoncanonicalBytesAreRejected()
    {
        byte[] canonical =
            JsonExportCheckpointSerializer.Serialize(
                CreateWritingCheckpoint());
        string text =
            Encoding.UTF8.GetString(canonical);
        byte[] tampered =
            Encoding.UTF8.GetBytes(
                text.Replace(
                    "\"completedRowCount\":2",
                    "\"completedRowCount\":3",
                    StringComparison.Ordinal));
        byte[] withWhitespace =
            [.. canonical, (byte)'\n'];

        InvalidDataException tamperError =
            Assert.Throws<InvalidDataException>(
                () =>
                    JsonExportCheckpointSerializer
                        .Deserialize(tampered));
        InvalidDataException canonicalError =
            Assert.Throws<InvalidDataException>(
                () =>
                    JsonExportCheckpointSerializer
                        .Deserialize(withWhitespace));

        Assert.Contains(
            "digest",
            tamperError.Message,
            StringComparison.OrdinalIgnoreCase);
        Assert.Contains(
            "canonical",
            canonicalError.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void StrictParserRejectsAdversariesWithoutEchoingInput()
    {
        const string marker =
            "LEAK_MARKER_92fdd";
        string canonical =
            CanonicalText(
                CreateWritingCheckpoint());
        string duplicate =
            canonical.Replace(
                "{\"format\":",
                $"{{\"format\":\"{marker}\",\"format\":",
                StringComparison.Ordinal);
        string unknown =
            canonical.Replace(
                "\"generation\":",
                $"\"{marker}\":true,\"generation\":",
                StringComparison.Ordinal);
        string numericPhase =
            canonical.Replace(
                "\"phase\":\"writing\"",
                "\"phase\":0",
                StringComparison.Ordinal);
        string wrongFormat =
            canonical.Replace(
                JsonExportCheckpointContracts
                    .Format,
                marker,
                StringComparison.Ordinal);

        foreach (string candidate in
                 new[]
                 {
                     duplicate,
                     unknown,
                     numericPhase,
                     wrongFormat,
                 })
        {
            InvalidDataException error =
                Assert.Throws<InvalidDataException>(
                    () =>
                        DeserializeText(candidate));
            Assert.DoesNotContain(
                marker,
                error.ToString(),
                StringComparison.Ordinal);
        }
    }

    [Fact]
    public void BomInvalidUtf8OversizedAndOverDepthInputsAreRejected()
    {
        byte[] canonical =
            JsonExportCheckpointSerializer.Serialize(
                CreateWritingCheckpoint());
        byte[] withBom =
            [.. Encoding.UTF8.Preamble, .. canonical];
        byte[] invalidUtf8 = [0xc3, 0x28];
        byte[] oversized =
            new byte[
                JsonExportCheckpointSerializer
                    .MaximumCheckpointBytes +
                1];
        string overDepth =
            new string('[', 65) +
            "0" +
            new string(']', 65);

        Assert.Throws<InvalidDataException>(
            () =>
                JsonExportCheckpointSerializer
                    .Deserialize(withBom));
        Assert.Throws<InvalidDataException>(
            () =>
                JsonExportCheckpointSerializer
                    .Deserialize(invalidUtf8));
        Assert.Throws<InvalidDataException>(
            () =>
                JsonExportCheckpointSerializer
                    .Deserialize(oversized));
        Assert.Throws<InvalidDataException>(
            () =>
                JsonExportCheckpointSerializer
                    .Deserialize(
                        Encoding.UTF8.GetBytes(
                            overDepth)));
    }

    [Fact]
    public void PhaseProgressAndLosslessMatrixIsStrict()
    {
        JsonExportCheckpoint writing =
            CreateWritingCheckpoint();
        JsonExportCheckpoint complete =
            CreateCompletedCheckpoint();

        JsonExportCheckpoint[] invalid =
        [
            writing with { Generation = -1 },
            writing with
            {
                Phase =
                    (JsonExportCheckpointPhase)999,
            },
            writing with
            {
                Progress =
                    writing.Progress with
                    {
                        LastCompletedRowId = null,
                    },
            },
            writing with
            {
                Progress =
                    writing.Progress with
                    {
                        LogicalPrefixAggregation =
                            "unsupported",
                    },
            },
            writing with
            {
                Progress =
                    writing.Progress with
                    {
                        ExportedLogicalRowHashPrefixDigest =
                            Hash('e'),
                    },
            },
            writing with
            {
                Completion =
                    complete.Completion,
            },
            complete with { Completion = null },
            writing with
            {
                BindingDigest = Hash('f'),
            },
        ];

        foreach (JsonExportCheckpoint candidate in
                 invalid)
        {
            Assert.Throws<InvalidDataException>(
                () =>
                    JsonExportCheckpointSerializer
                        .Serialize(candidate));
        }
    }

    [Theory]
    [InlineData(
        JsonExportFraming.RootArray,
        JsonExportCheckpointPhase.Writing,
        "[")]
    [InlineData(
        JsonExportFraming.RootArray,
        JsonExportCheckpointPhase.DataComplete,
        "[]\n")]
    [InlineData(
        JsonExportFraming.Ndjson,
        JsonExportCheckpointPhase.Writing,
        "")]
    [InlineData(
        JsonExportFraming.Ndjson,
        JsonExportCheckpointPhase.DataComplete,
        "")]
    public void EmptyPrefixesHaveExactPhysicalAndLogicalEvidence(
        JsonExportFraming framing,
        JsonExportCheckpointPhase phase,
        string prefix)
    {
        JsonExportCheckpoint checkpoint =
            CreateEmptyCheckpoint(
                framing,
                phase);
        JsonExportCheckpoint reopened =
            JsonExportCheckpointSerializer
                .Deserialize(
                    JsonExportCheckpointSerializer
                        .Serialize(checkpoint));

        Assert.Equal(
            Encoding.UTF8.GetByteCount(prefix),
            reopened.Progress
                .DataPrefixByteLength);
        Assert.Equal(
            HashBytes(
                Encoding.UTF8.GetBytes(prefix)),
            reopened.Progress
                .DataPrefixDigest);

        Assert.Throws<InvalidDataException>(
            () =>
                JsonExportCheckpointSerializer
                    .Serialize(
                        checkpoint with
                        {
                            Progress =
                                checkpoint.Progress with
                                {
                                    DataPrefixDigest =
                                        Hash('f'),
                                },
                        }));
        Assert.Throws<InvalidDataException>(
            () =>
                JsonExportCheckpointSerializer
                    .Serialize(
                        checkpoint with
                        {
                            Progress =
                                checkpoint.Progress with
                                {
                                    SourceLogicalRowHashPrefixDigest =
                                        Hash('e'),
                                    ExportedLogicalRowHashPrefixDigest =
                                        Hash('e'),
                                },
                        }));
    }

    [Fact]
    public void CompletedCheckpointReconstructsExactManifestAndDigests()
    {
        JsonExportCheckpoint checkpoint =
            CreateCompletedCheckpoint();
        JsonExportManifest manifest =
            JsonExportCheckpointSerializer
                .CreateCompletedManifest(
                    checkpoint);

        Assert.Equal(
            checkpoint.Progress
                .CompletedRowCount,
            manifest.Content.RowCount);
        Assert.Equal(
            checkpoint.Progress
                .DataPrefixByteLength,
            manifest.Content.DataByteLength);
        Assert.Equal(
            checkpoint.Progress
                .DataPrefixDigest,
            manifest.Content.DataDigest);
        Assert.Equal(
            checkpoint.Completion!
                .ManifestDigest,
            JsonExportManifestSerializer
                .ComputeManifestDigest(
                    manifest));

        Assert.Throws<InvalidDataException>(
            () =>
                JsonExportCheckpointSerializer
                    .Serialize(
                        checkpoint with
                        {
                            Completion =
                                checkpoint.Completion!
                                    with
                                    {
                                        ManifestDigest =
                                            new string(
                                                'f',
                                                64),
                                    },
                        }));
        Assert.Throws<InvalidDataException>(
            () =>
                JsonExportCheckpointSerializer
                    .Serialize(
                        checkpoint with
                        {
                            Completion =
                                checkpoint.Completion!
                                    with
                                    {
                                        ExportedLogicalDigest =
                                            Hash('e'),
                                    },
                        }));
        Assert.Throws<InvalidDataException>(
            () =>
                JsonExportCheckpointSerializer
                    .CreateCompletedManifest(
                        CreateWritingCheckpoint()));
    }

    private static JsonExportCheckpoint
        CreateWritingCheckpoint()
    {
        JsonExportCheckpointBinding binding =
            CreateBinding(
                JsonExportFraming.RootArray);
        return new JsonExportCheckpoint
        {
            Generation = 7,
            Phase =
                JsonExportCheckpointPhase.Writing,
            Binding = binding,
            BindingDigest =
                JsonExportCheckpointSerializer
                    .ComputeBindingDigest(binding),
            Progress = CreateProgress(
                completedRowCount: 2,
                lastCompletedRowId: -4,
                dataPrefixByteLength: 128),
        };
    }

    private static JsonExportCheckpoint
        CreateCompletedCheckpoint()
    {
        JsonExportCheckpointBinding binding =
            CreateBinding(
                JsonExportFraming.RootArray);
        JsonExportCheckpointProgress progress =
            CreateProgress(
                completedRowCount: 2,
                lastCompletedRowId: 9,
                dataPrefixByteLength: 130);
        JsonExportCheckpointCompletion preliminary =
            new()
            {
                SourceLogicalDigest = Hash('d'),
                ExportedLogicalDigest = Hash('d'),
                ManifestDigest =
                    new string('0', 64),
            };
        JsonExportManifest manifest =
            CreateExpectedManifest(
                binding,
                progress,
                preliminary);
        JsonExportCheckpointCompletion completion =
            preliminary with
            {
                ManifestDigest =
                    JsonExportManifestSerializer
                        .ComputeManifestDigest(
                            manifest),
            };
        return new JsonExportCheckpoint
        {
            Generation = 8,
            Phase =
                JsonExportCheckpointPhase.DataComplete,
            Binding = binding,
            BindingDigest =
                JsonExportCheckpointSerializer
                    .ComputeBindingDigest(binding),
            Progress = progress,
            Completion = completion,
        };
    }

    private static JsonExportCheckpoint
        CreateEmptyCheckpoint(
            JsonExportFraming framing,
            JsonExportCheckpointPhase phase)
    {
        JsonExportCheckpointBinding binding =
            CreateBinding(framing);
        ReadOnlySpan<byte> prefix =
            framing ==
                JsonExportFraming.RootArray
                ? phase ==
                    JsonExportCheckpointPhase
                        .Writing
                    ? "["u8
                    : "[]\n"u8
                : ReadOnlySpan<byte>.Empty;
        using var logical =
            new JsonExportOrderedContentDigest();
        JsonExportCheckpointProgress progress =
            new()
            {
                CompletedRowCount = 0,
                LastCompletedRowId = null,
                DataPrefixByteLength =
                    prefix.Length,
                DataPrefixDigest =
                    HashBytes(prefix),
                LogicalPrefixAggregation =
                    JsonExportCheckpointContracts
                        .LogicalPrefixAggregation,
                SourceLogicalRowHashPrefixDigest =
                    logical
                        .GetCurrentPrefixDigest(),
                ExportedLogicalRowHashPrefixDigest =
                    logical
                        .GetCurrentPrefixDigest(),
            };

        JsonExportCheckpointCompletion? completion =
            null;
        if (phase ==
            JsonExportCheckpointPhase.DataComplete)
        {
            using var finalLogical =
                new JsonExportOrderedContentDigest();
            JsonExportHashManifest finalDigest =
                finalLogical.Complete();
            var preliminary =
                new JsonExportCheckpointCompletion
                {
                    SourceLogicalDigest =
                        finalDigest,
                    ExportedLogicalDigest =
                        finalDigest with { },
                    ManifestDigest =
                        new string('0', 64),
                };
            JsonExportManifest manifest =
                CreateExpectedManifest(
                    binding,
                    progress,
                    preliminary);
            completion = preliminary with
            {
                ManifestDigest =
                    JsonExportManifestSerializer
                        .ComputeManifestDigest(
                            manifest),
            };
        }

        return new JsonExportCheckpoint
        {
            Generation = 0,
            Phase = phase,
            Binding = binding,
            BindingDigest =
                JsonExportCheckpointSerializer
                    .ComputeBindingDigest(binding),
            Progress = progress,
            Completion = completion,
        };
    }

    private static JsonExportCheckpointBinding
        CreateBinding(
            JsonExportFraming framing)
    {
        JsonExportColumnManifest[] columns =
        [
            Column(
                0,
                "id",
                JsonExportDatabaseType.Integer,
                nullable: false),
            Column(
                1,
                "note",
                JsonExportDatabaseType.Text),
        ];
        JsonExportSourceManifest source =
            new()
            {
                Kind =
                    JsonExportContracts.SourceKind,
                Version = "4.3.0",
                SnapshotByteLength = 4_096,
                SnapshotDigest = Hash('a'),
            };
        return new JsonExportCheckpointBinding
        {
            Profile = JsonExportProfile.LosslessV1,
            Source = source,
            SourceSnapshotIdentity =
                SnapshotIdentity(
                    source.SnapshotByteLength,
                    source.SnapshotDigest.Value),
            Table = new JsonExportTableManifest
            {
                Name = "orders",
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
            Json = FixedFormat(framing),
        };
    }

    private static JsonExportCheckpointProgress
        CreateProgress(
            long completedRowCount,
            long? lastCompletedRowId,
            long dataPrefixByteLength) =>
        new()
        {
            CompletedRowCount =
                completedRowCount,
            LastCompletedRowId =
                lastCompletedRowId,
            DataPrefixByteLength =
                dataPrefixByteLength,
            DataPrefixDigest = Hash('b'),
            LogicalPrefixAggregation =
                JsonExportCheckpointContracts
                    .LogicalPrefixAggregation,
            SourceLogicalRowHashPrefixDigest =
                Hash('c'),
            ExportedLogicalRowHashPrefixDigest =
                Hash('c'),
        };

    private static JsonExportManifest
        CreateExpectedManifest(
            JsonExportCheckpointBinding binding,
            JsonExportCheckpointProgress progress,
            JsonExportCheckpointCompletion completion) =>
        new()
        {
            Profile = binding.Profile,
            Source = binding.Source,
            Table = binding.Table,
            Json = binding.Json,
            Content = new JsonExportContentManifest
            {
                RowCount =
                    progress.CompletedRowCount,
                DataByteLength =
                    progress.DataPrefixByteLength,
                DataDigest =
                    progress.DataPrefixDigest,
                Canonicalization =
                    JsonExportContracts
                        .Canonicalization,
                CanonicalizationContractDigest =
                    JsonExportContracts
                        .CanonicalizationContractDigest,
                Aggregation =
                    JsonExportContracts
                        .OrderedContentDigest,
                SourceLogicalDigest =
                    completion.SourceLogicalDigest,
                ExportedLogicalDigest =
                    completion.ExportedLogicalDigest,
            },
        };

    private static JsonExportFormatManifest FixedFormat(
        JsonExportFraming framing) =>
        new()
        {
            Encoding =
                JsonExportContracts.Encoding,
            HasByteOrderMark = false,
            Culture =
                JsonExportContracts.Culture,
            Framing = framing,
            Compact = true,
            PropertyOrder =
                JsonExportContracts.PropertyOrder,
            Newline =
                JsonExportContracts.Newline,
            HasFinalNewline = true,
            NullEncoding =
                JsonExportContracts.NullEncoding,
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
        };

    private static JsonExportColumnManifest Column(
        int ordinal,
        string name,
        JsonExportDatabaseType databaseType,
        bool nullable = true) =>
        new()
        {
            Ordinal = ordinal,
            SourceName = name,
            PropertyName = name,
            DatabaseType = databaseType,
            Nullable = nullable,
            ValueEncoding =
                databaseType switch
                {
                    JsonExportDatabaseType.Integer =>
                        JsonExportContracts
                            .IntegerValueEncoding,
                    JsonExportDatabaseType.Real =>
                        JsonExportContracts
                            .RealValueEncoding,
                    JsonExportDatabaseType.Text =>
                        JsonExportContracts
                            .TextValueEncoding,
                    JsonExportDatabaseType.Blob =>
                        JsonExportContracts
                            .BlobValueEncoding,
                    _ => throw new
                        ArgumentOutOfRangeException(
                            nameof(databaseType)),
                },
            MaximumDecodedBytes =
                databaseType ==
                    JsonExportDatabaseType.Blob
                    ? JsonExportContracts
                        .MaximumSupportedDecodedBlobBytes
                    : 0,
        };

    private static string SnapshotIdentity(
        long byteLength,
        string digest) =>
        JsonExportCheckpointContracts
            .RetainedSnapshotIdentityPrefix +
        byteLength.ToString(
            CultureInfo.InvariantCulture) +
        ":" +
        JsonExportHashManifest.Sha256Algorithm +
        ":" +
        digest;

    private static JsonExportHashManifest Hash(
        char value) =>
        new()
        {
            Algorithm =
                JsonExportHashManifest
                    .Sha256Algorithm,
            Value = new string(value, 64),
        };

    private static JsonExportHashManifest HashBytes(
        ReadOnlySpan<byte> bytes) =>
        new()
        {
            Algorithm =
                JsonExportHashManifest
                    .Sha256Algorithm,
            Value = Convert.ToHexString(
                    SHA256.HashData(bytes))
                .ToLowerInvariant(),
        };

    private static string CanonicalText(
        JsonExportCheckpoint checkpoint) =>
        Encoding.UTF8.GetString(
            JsonExportCheckpointSerializer
                .Serialize(checkpoint));

    private static void DeserializeText(
        string value) =>
        _ = JsonExportCheckpointSerializer
            .Deserialize(
                Encoding.UTF8.GetBytes(value));
}
