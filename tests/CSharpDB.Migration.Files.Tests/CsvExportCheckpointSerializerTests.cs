using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Migration.Files.Csv;

namespace CSharpDB.Migration.Files.Tests;

public sealed class CsvExportCheckpointSerializerTests
{
    [Fact]
    public void CanonicalCheckpointHasStableBytesAndRoundTripsAcrossCultures()
    {
        CsvExportCheckpoint checkpoint = CreateWritingCheckpoint();
        CultureInfo originalCulture = CultureInfo.CurrentCulture;
        CultureInfo originalUiCulture = CultureInfo.CurrentUICulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("ar-SA");
            CultureInfo.CurrentUICulture = CultureInfo.GetCultureInfo("ar-SA");
            byte[] first = CsvExportCheckpointSerializer.Serialize(checkpoint);

            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("de-DE");
            CultureInfo.CurrentUICulture = CultureInfo.GetCultureInfo("de-DE");
            byte[] repeated = CsvExportCheckpointSerializer.Serialize(checkpoint);
            CsvExportCheckpoint reopened = CsvExportCheckpointSerializer.Deserialize(first);

            Assert.Equal(first, repeated);
            Assert.False(first.AsSpan().StartsWith(Encoding.UTF8.Preamble));
            Assert.Equal(first, CsvExportCheckpointSerializer.Serialize(reopened));
            Assert.Equal(checkpoint.Generation, reopened.Generation);
            Assert.Equal(checkpoint.Phase, reopened.Phase);
            Assert.Equal(
                checkpoint.BindingDigest.Value,
                reopened.BindingDigest.Value);
            Assert.Equal(
                "bc60e627c629c396bd4c4030ada3349977fd727fbcedacb6568f387afcfeb2fe",
                CsvExportCheckpointSerializer.ComputeCheckpointDigest(checkpoint));
            Assert.Equal(
                "beaedd49a92a280566dc84372f7ad66dcc36eec97614d2d6696399e0d9daec9e",
                Convert.ToHexString(SHA256.HashData(first)).ToLowerInvariant());
        }
        finally
        {
            CultureInfo.CurrentCulture = originalCulture;
            CultureInfo.CurrentUICulture = originalUiCulture;
        }
    }

    [Fact]
    public void BindingDigestHasStableVectorAndBindsResourcePolicy()
    {
        CsvExportCheckpointBinding binding = CreateLosslessBinding();
        CsvExportHashManifest digest =
            CsvExportCheckpointSerializer.ComputeBindingDigest(binding);
        CsvExportCheckpointBinding changed = binding with
        {
            MaxDataBytes = binding.MaxDataBytes + 1,
        };

        Assert.Equal(CsvExportHashManifest.Sha256Algorithm, digest.Algorithm);
        Assert.Equal(
            "16940d286ba976493f106c506c6d246f5cf2b938a75e72f3438756d5ed1b9a49",
            digest.Value);
        Assert.NotEqual(
            digest.Value,
            CsvExportCheckpointSerializer.ComputeBindingDigest(changed).Value);

        CsvExportCheckpoint checkpoint = CreateWritingCheckpoint() with
        {
            BindingDigest = Hash('f'),
        };
        Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.Serialize(checkpoint));
    }

    [Fact]
    public void BindingRejectsDataMaximumSmallerThanRenderedHeader()
    {
        CsvExportCheckpointBinding binding = CreateLosslessBinding() with
        {
            // The exact rendered header is "id,note\r\n" (9 bytes).
            MaxDataBytes = 8,
        };

        Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.ComputeBindingDigest(binding));
    }

    [Fact]
    public void TamperedAndNoncanonicalCheckpointBytesAreRejected()
    {
        byte[] canonical =
            CsvExportCheckpointSerializer.Serialize(CreateWritingCheckpoint());
        string json = Encoding.UTF8.GetString(canonical);
        string tampered = json.Replace(
            "\"completedRowCount\":2",
            "\"completedRowCount\":3",
            StringComparison.Ordinal);
        byte[] trailingWhitespace = [.. canonical, (byte)'\n'];

        InvalidDataException tamperError = Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.Deserialize(
                Encoding.UTF8.GetBytes(tampered)));
        InvalidDataException canonicalError = Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.Deserialize(trailingWhitespace));

        Assert.Contains("digest", tamperError.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(
            "canonical",
            canonicalError.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void DuplicateUnknownAndNumericEnumMembersAreRejected()
    {
        byte[] canonical =
            CsvExportCheckpointSerializer.Serialize(CreateWritingCheckpoint());
        string json = Encoding.UTF8.GetString(canonical);
        string duplicate = json.Replace(
            "{\"format\":",
            "{\"format\":\"csharpdb-csv-export-checkpoint/v1\",\"format\":",
            StringComparison.Ordinal);
        string unknown = json.Replace(
            "\"generation\":",
            "\"unexpected\":true,\"generation\":",
            StringComparison.Ordinal);
        string numericPhase = json.Replace(
            "\"phase\":\"writing\"",
            "\"phase\":0",
            StringComparison.Ordinal);

        InvalidDataException duplicateError = Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.Deserialize(
                Encoding.UTF8.GetBytes(duplicate)));
        InvalidDataException unknownError = Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.Deserialize(
                Encoding.UTF8.GetBytes(unknown)));
        Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.Deserialize(
                Encoding.UTF8.GetBytes(numericPhase)));

        Assert.Contains(
            "duplicate",
            duplicateError.Message,
            StringComparison.OrdinalIgnoreCase);
        Assert.Contains("payload", unknownError.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void BomInvalidUtf8OversizedAndOverDepthInputsAreRejected()
    {
        byte[] canonical =
            CsvExportCheckpointSerializer.Serialize(CreateWritingCheckpoint());
        byte[] withBom = [.. Encoding.UTF8.Preamble, .. canonical];
        byte[] invalidUtf8 = [0xc3, 0x28];
        byte[] oversized =
            new byte[CsvExportCheckpointSerializer.MaximumCheckpointBytes + 1];
        string overDepth =
            new string('[', 65) + "0" + new string(']', 65);

        Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.Deserialize(withBom));
        Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.Deserialize(invalidUtf8));
        Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.Deserialize(oversized));
        Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.Deserialize(
                Encoding.UTF8.GetBytes(overDepth)));
    }

    [Fact]
    public void UppercaseHashTextIsRejected()
    {
        CsvExportCheckpoint checkpoint = CreateWritingCheckpoint();
        CsvExportCheckpoint uppercase = checkpoint with
        {
            Progress = checkpoint.Progress with
            {
                DataPrefixDigest = checkpoint.Progress.DataPrefixDigest with
                {
                    Value = checkpoint.Progress.DataPrefixDigest.Value.ToUpperInvariant(),
                },
            },
        };

        Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.Serialize(uppercase));
    }

    [Fact]
    public void RetainedSnapshotIdentityMustExactlyMatchSourceEvidence()
    {
        CsvExportCheckpointBinding binding = CreateLosslessBinding();
        CsvExportCheckpointBinding wrongLength = binding with
        {
            SourceSnapshotIdentity = SnapshotIdentity(
                binding.Source.SnapshotByteLength + 1,
                binding.Source.SnapshotDigest.Value),
        };
        CsvExportCheckpointBinding wrongHash = binding with
        {
            SourceSnapshotIdentity = SnapshotIdentity(
                binding.Source.SnapshotByteLength,
                new string('f', 64)),
        };

        Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.ComputeBindingDigest(wrongLength));
        Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.ComputeBindingDigest(wrongHash));
    }

    [Fact]
    public void BindingRejectsSchemaProfileCodecAndBlobPolicyDrift()
    {
        CsvExportCheckpointBinding binding = CreateLosslessBinding();
        CsvExportCheckpointBinding wrongSchema = binding with
        {
            Table = binding.Table with { SchemaDigest = Hash('f') },
        };
        CsvExportCheckpointBinding wrongProfile = CreateBinding(
            CsvExportProfile.SpreadsheetSafeLossyV1,
            [
                Column(0, "=formula", CsvExportDatabaseType.Text),
            ]);
        CsvExportCheckpointBinding wrongCodec = binding with
        {
            Csv = binding.Csv with { Delimiter = ";" },
        };

        CsvExportColumnManifest blob = Column(
            0,
            "payload",
            CsvExportDatabaseType.Blob) with
        {
            MaximumDecodedBytes = 1024,
        };
        CsvExportCheckpointBinding wrongBlobPolicy = CreateBinding(
            CsvExportProfile.LosslessV1,
            [blob],
            maximumDecodedBlobBytes: 512);

        Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.ComputeBindingDigest(wrongSchema));
        Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.ComputeBindingDigest(wrongProfile));
        Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.ComputeBindingDigest(wrongCodec));
        Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.ComputeBindingDigest(wrongBlobPolicy));
    }

    [Fact]
    public void CheckpointRejectsInvalidMaximumsAndProgressBeyondMaximum()
    {
        CsvExportCheckpointBinding binding = CreateLosslessBinding();
        CsvExportCheckpointBinding zeroDataMaximum = binding with { MaxDataBytes = 0 };
        CsvExportCheckpointBinding excessiveBlobMaximum = binding with
        {
            MaximumDecodedBlobBytes =
                CsvExportContracts.MaximumSupportedDecodedBlobBytes + 1,
        };

        Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.ComputeBindingDigest(zeroDataMaximum));
        Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.ComputeBindingDigest(
                excessiveBlobMaximum));

        CsvExportCheckpointBinding tooSmall = binding with { MaxDataBytes = 127 };
        CsvExportCheckpoint checkpoint = CreateWritingCheckpoint(
            binding: tooSmall,
            progress: CreateLosslessProgress());
        Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.Serialize(checkpoint));
    }

    [Fact]
    public void SignedLastRowIdsAreSupportedAndCountNullRelationshipIsExact()
    {
        CsvExportCheckpoint accepted = CreateWritingCheckpoint();
        CsvExportCheckpoint minimumSignedId = accepted with
        {
            Progress = accepted.Progress with
            {
                CompletedRowCount = 1,
                LastCompletedRowId = long.MinValue,
            },
        };
        CsvExportCheckpoint noRowsWithId = accepted with
        {
            Progress = accepted.Progress with
            {
                CompletedRowCount = 0,
                LastCompletedRowId = -1,
            },
        };
        CsvExportCheckpoint rowsWithoutId = accepted with
        {
            Progress = accepted.Progress with
            {
                CompletedRowCount = 1,
                LastCompletedRowId = null,
            },
        };
        CsvExportCheckpoint negativeCount = accepted with
        {
            Progress = accepted.Progress with { CompletedRowCount = -1 },
        };

        CsvExportCheckpoint reopened = CsvExportCheckpointSerializer.Deserialize(
            CsvExportCheckpointSerializer.Serialize(minimumSignedId));
        Assert.Equal(long.MinValue, reopened.Progress.LastCompletedRowId);
        Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.Serialize(noRowsWithId));
        Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.Serialize(rowsWithoutId));
        Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.Serialize(negativeCount));
    }

    [Fact]
    public void NegativeGenerationAndWrongLogicalPrefixAggregationAreRejected()
    {
        CsvExportCheckpoint checkpoint = CreateWritingCheckpoint();
        CsvExportCheckpoint negativeGeneration = checkpoint with
        {
            Generation = -1,
        };
        CsvExportCheckpoint wrongAggregation = checkpoint with
        {
            Progress = checkpoint.Progress with
            {
                LogicalPrefixAggregation =
                    "csharpdb-csv-export-ordered-content-prefix/v999",
            },
        };

        Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.Serialize(negativeGeneration));
        Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.Serialize(wrongAggregation));
    }

    [Fact]
    public void ZeroRowProgressMustExactlyDescribeTheRenderedHeaderPrefix()
    {
        CsvExportCheckpointBinding binding = CreateLosslessBinding();
        byte[] header = "id,note\r\n"u8.ToArray();
        CsvExportCheckpointProgress progress = CreateZeroRowProgress(header);
        CsvExportCheckpoint exact = CreateWritingCheckpoint(binding, progress);
        CsvExportCheckpoint wrongLength = exact with
        {
            Progress = progress with
            {
                DataPrefixByteLength = progress.DataPrefixByteLength + 1,
            },
        };
        CsvExportCheckpoint wrongDigest = exact with
        {
            Progress = progress with { DataPrefixDigest = Hash('f') },
        };

        CsvExportCheckpoint reopened = CsvExportCheckpointSerializer.Deserialize(
            CsvExportCheckpointSerializer.Serialize(exact));
        Assert.Equal(0, reopened.Progress.CompletedRowCount);
        Assert.Null(reopened.Progress.LastCompletedRowId);
        Assert.Equal(header.LongLength, reopened.Progress.DataPrefixByteLength);
        Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.Serialize(wrongLength));
        Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.Serialize(wrongDigest));
    }

    [Fact]
    public void LosslessProgressRequiresEqualLogicalPrefixesAndNoTransforms()
    {
        CsvExportCheckpoint checkpoint = CreateWritingCheckpoint();
        CsvExportCheckpoint changedLogicalPrefix = checkpoint with
        {
            Progress = checkpoint.Progress with
            {
                ExportedLogicalRowHashPrefixDigest = Hash('e'),
            },
        };
        CsvExportCheckpoint transformed = checkpoint with
        {
            Progress = checkpoint.Progress with
            {
                TransformedRowCount = 1,
                TransformedCellCount = 1,
            },
        };

        Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.Serialize(changedLogicalPrefix));
        Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.Serialize(transformed));
    }

    [Fact]
    public void SpreadsheetProgressRequiresConsistentLossAndLogicalEvidence()
    {
        CsvExportCheckpointBinding binding = CreateSpreadsheetBinding();
        CsvExportCheckpointProgress progress = CreateSpreadsheetProgress();
        CsvExportCheckpoint valid = CreateWritingCheckpoint(binding, progress);
        CsvExportCheckpoint unexplainedDigestChange = valid with
        {
            Progress = progress with
            {
                TransformedRowCount = 0,
                TransformedCellCount = 0,
            },
        };
        CsvExportCheckpoint tooManyRows = valid with
        {
            Progress = progress with { TransformedRowCount = 3 },
        };
        CsvExportCheckpoint tooManyCells = valid with
        {
            Progress = progress with { TransformedCellCount = 3 },
        };

        CsvExportCheckpoint reopened = CsvExportCheckpointSerializer.Deserialize(
            CsvExportCheckpointSerializer.Serialize(valid));
        Assert.Equal(
            CsvExportProfile.SpreadsheetSafeLossyV1,
            reopened.Binding.Profile);
        Assert.Equal(1, reopened.Progress.TransformedRowCount);
        Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.Serialize(unexplainedDigestChange));
        Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.Serialize(tooManyRows));
        Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.Serialize(tooManyCells));
    }

    [Fact]
    public void PhaseAndCompletionMatrixIsStrict()
    {
        CsvExportCheckpoint writing = CreateWritingCheckpoint();
        CsvExportCheckpoint complete = CreateCompletedCheckpoint();
        CsvExportCheckpoint writingWithCompletion = writing with
        {
            Completion = complete.Completion,
        };
        CsvExportCheckpoint completeWithoutCompletion = complete with
        {
            Completion = null,
        };
        CsvExportCheckpoint unknownPhase = writing with
        {
            Phase = (CsvExportCheckpointPhase)999,
        };

        CsvExportCheckpoint reopened = CsvExportCheckpointSerializer.Deserialize(
            CsvExportCheckpointSerializer.Serialize(complete));
        Assert.Equal(CsvExportCheckpointPhase.DataComplete, reopened.Phase);
        Assert.NotNull(reopened.Completion);
        Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.Serialize(writingWithCompletion));
        Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.Serialize(completeWithoutCompletion));
        Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.Serialize(unknownPhase));
    }

    [Fact]
    public void CompletedCheckpointReconstructsTheExactManifest()
    {
        CsvExportCheckpoint checkpoint = CreateCompletedCheckpoint();
        CsvExportManifest expected = CreateExpectedCompletedManifest(
            checkpoint.Binding,
            checkpoint.Progress,
            checkpoint.Completion!);

        CsvExportManifest actual =
            CsvExportCheckpointSerializer.CreateCompletedManifest(checkpoint);

        Assert.Equal(
            CsvExportManifestSerializer.Serialize(expected),
            CsvExportManifestSerializer.Serialize(actual));
        Assert.Equal(
            checkpoint.Completion!.ManifestDigest,
            CsvExportManifestSerializer.ComputeManifestDigest(actual));
        Assert.Equal(
            checkpoint.Progress.DataPrefixDigest,
            actual.Content.DataDigest);
        Assert.Equal(
            checkpoint.Progress.CompletedRowCount,
            actual.Content.RowCount);
    }

    [Fact]
    public void CompletedCheckpointRejectsManifestDigestOrLogicalCompletionMismatch()
    {
        CsvExportCheckpoint checkpoint = CreateCompletedCheckpoint();
        CsvExportCheckpoint wrongManifestDigest = checkpoint with
        {
            Completion = checkpoint.Completion! with
            {
                ManifestDigest = new string('f', 64),
            },
        };
        CsvExportCheckpoint wrongLosslessLogicalDigest = checkpoint with
        {
            Completion = checkpoint.Completion! with
            {
                ExportedLogicalDigest = Hash('e'),
            },
        };

        Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.CreateCompletedManifest(
                wrongManifestDigest));
        Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.Serialize(wrongManifestDigest));
        Assert.Throws<InvalidDataException>(
            () => CsvExportCheckpointSerializer.Serialize(
                wrongLosslessLogicalDigest));
    }

    private static CsvExportCheckpoint CreateWritingCheckpoint(
        CsvExportCheckpointBinding? binding = null,
        CsvExportCheckpointProgress? progress = null)
    {
        binding ??= CreateLosslessBinding();
        progress ??= CreateLosslessProgress();
        return new CsvExportCheckpoint
        {
            Generation = 7,
            Phase = CsvExportCheckpointPhase.Writing,
            Binding = binding,
            BindingDigest = CsvExportCheckpointSerializer.ComputeBindingDigest(binding),
            Progress = progress,
        };
    }

    private static CsvExportCheckpoint CreateCompletedCheckpoint()
    {
        CsvExportCheckpointBinding binding = CreateLosslessBinding();
        CsvExportCheckpointProgress progress = CreateLosslessProgress();
        var preliminaryCompletion = new CsvExportCheckpointCompletion
        {
            SourceLogicalDigest = Hash('d'),
            ExportedLogicalDigest = Hash('d'),
            ManifestDigest = new string('0', 64),
        };
        CsvExportManifest manifest = CreateExpectedCompletedManifest(
            binding,
            progress,
            preliminaryCompletion);
        CsvExportCheckpointCompletion completion = preliminaryCompletion with
        {
            ManifestDigest =
                CsvExportManifestSerializer.ComputeManifestDigest(manifest),
        };
        return new CsvExportCheckpoint
        {
            Generation = 8,
            Phase = CsvExportCheckpointPhase.DataComplete,
            Binding = binding,
            BindingDigest = CsvExportCheckpointSerializer.ComputeBindingDigest(binding),
            Progress = progress,
            Completion = completion,
        };
    }

    private static CsvExportCheckpointBinding CreateLosslessBinding() =>
        CreateBinding(
            CsvExportProfile.LosslessV1,
            [
                Column(0, "id", CsvExportDatabaseType.Integer, nullable: false),
                Column(1, "note", CsvExportDatabaseType.Text),
            ]);

    private static CsvExportCheckpointBinding CreateSpreadsheetBinding()
    {
        CsvExportColumnManifest formula = Column(
            0,
            "=formula",
            CsvExportDatabaseType.Text) with
        {
            Header = CsvSpreadsheetFormulaPolicy.Transform("=formula"),
        };
        return CreateBinding(
            CsvExportProfile.SpreadsheetSafeLossyV1,
            [
                formula,
                Column(1, "amount", CsvExportDatabaseType.Real),
            ]);
    }

    private static CsvExportCheckpointBinding CreateBinding(
        CsvExportProfile profile,
        IReadOnlyList<CsvExportColumnManifest> columns,
        long maxDataBytes = 1L << 20,
        int maximumDecodedBlobBytes =
            CsvExportContracts.MaximumSupportedDecodedBlobBytes)
    {
        CsvExportSourceManifest source = new()
        {
            Kind = CsvExportContracts.SourceKind,
            Version = "4.3.0",
            SnapshotByteLength = 4096,
            SnapshotDigest = Hash('a'),
        };
        return new CsvExportCheckpointBinding
        {
            Profile = profile,
            Source = source,
            SourceSnapshotIdentity = SnapshotIdentity(
                source.SnapshotByteLength,
                source.SnapshotDigest.Value),
            Table = new CsvExportTableManifest
            {
                Name = "orders",
                SchemaContract = CsvExportContracts.Schema,
                SchemaDigest =
                    CsvExportManifestSerializer.ComputeSchemaDigest(columns),
                RowOrder = CsvExportContracts.RowOrder,
                Columns = columns,
            },
            Csv = FixedFormat(),
            MaxDataBytes = maxDataBytes,
            MaximumDecodedBlobBytes = maximumDecodedBlobBytes,
        };
    }

    private static CsvExportCheckpointProgress CreateLosslessProgress() => new()
    {
        CompletedRowCount = 2,
        LastCompletedRowId = -4,
        DataPrefixByteLength = 128,
        DataPrefixDigest = Hash('b'),
        LogicalPrefixAggregation =
            CsvExportCheckpointContracts.LogicalPrefixAggregation,
        SourceLogicalRowHashPrefixDigest = Hash('c'),
        ExportedLogicalRowHashPrefixDigest = Hash('c'),
        TransformedRowCount = 0,
        TransformedCellCount = 0,
    };

    private static CsvExportCheckpointProgress CreateSpreadsheetProgress() => new()
    {
        CompletedRowCount = 2,
        LastCompletedRowId = 9,
        DataPrefixByteLength = 128,
        DataPrefixDigest = Hash('b'),
        LogicalPrefixAggregation =
            CsvExportCheckpointContracts.LogicalPrefixAggregation,
        SourceLogicalRowHashPrefixDigest = Hash('c'),
        ExportedLogicalRowHashPrefixDigest = Hash('d'),
        TransformedRowCount = 1,
        TransformedCellCount = 1,
    };

    private static CsvExportCheckpointProgress CreateZeroRowProgress(byte[] header)
    {
        using var source = new CsvExportOrderedContentDigest();
        using var exported = new CsvExportOrderedContentDigest();
        return new CsvExportCheckpointProgress
        {
            CompletedRowCount = 0,
            LastCompletedRowId = null,
            DataPrefixByteLength = header.LongLength,
            DataPrefixDigest = HashBytes(header),
            LogicalPrefixAggregation =
                CsvExportCheckpointContracts.LogicalPrefixAggregation,
            SourceLogicalRowHashPrefixDigest = source.GetCurrentPrefixDigest(),
            ExportedLogicalRowHashPrefixDigest = exported.GetCurrentPrefixDigest(),
            TransformedRowCount = 0,
            TransformedCellCount = 0,
        };
    }

    private static CsvExportManifest CreateExpectedCompletedManifest(
        CsvExportCheckpointBinding binding,
        CsvExportCheckpointProgress progress,
        CsvExportCheckpointCompletion completion)
    {
        int transformedHeaderCount = binding.Table.Columns.Count(
            static column => !string.Equals(
                column.SourceName,
                column.Header,
                StringComparison.Ordinal));
        return new CsvExportManifest
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
            LossyTransform =
                binding.Profile == CsvExportProfile.SpreadsheetSafeLossyV1
                    ? new CsvExportLossyTransformManifest
                    {
                        RuleId = CsvExportContracts.SpreadsheetFormulaRuleId,
                        Algorithm =
                            CsvExportContracts.SpreadsheetFormulaTransform,
                        TransformedHeaderCount = transformedHeaderCount,
                        TransformedRowCount = progress.TransformedRowCount,
                        TransformedCellCount = progress.TransformedCellCount,
                    }
                    : null,
        };
    }

    private static CsvExportFormatManifest FixedFormat() => new()
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
    };

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
                CsvExportDatabaseType.Real =>
                    CsvExportContracts.RealValueEncoding,
                CsvExportDatabaseType.Text =>
                    CsvExportContracts.TextValueEncoding,
                CsvExportDatabaseType.Blob =>
                    CsvExportContracts.BlobValueEncoding,
                _ => throw new ArgumentOutOfRangeException(nameof(databaseType)),
            },
            MaximumDecodedBytes =
                databaseType == CsvExportDatabaseType.Blob
                    ? CsvExportContracts.MaximumSupportedDecodedBlobBytes
                    : 0,
        };

    private static string SnapshotIdentity(long byteLength, string digest) =>
        CsvExportCheckpointContracts.RetainedSnapshotIdentityPrefix +
        byteLength.ToString(CultureInfo.InvariantCulture) +
        ":sha256:" +
        digest;

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
}
