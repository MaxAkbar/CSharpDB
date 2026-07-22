using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Migration;
using CSharpDB.Migration.Files.Csv;

namespace CSharpDB.Migration.Files.Tests;

public sealed class CsvFormatInspectorTests
{
    [Fact]
    public async Task DetectsSemicolonWhenCommasAreCultureLookingValues()
    {
        await using CsvSourceSnapshot snapshot = await SnapshotAsync(
            "id;amount\n1;1,25\n2;2,50\n");

        CsvFormatInspection result = await InspectAsync(snapshot);

        Assert.Equal(CsvInspectionResolution.Resolved, result.Delimiter.Resolution);
        Assert.Equal(";", result.Delimiter.SelectedDelimiter);
        Assert.Equal(CsvInspectionConfidence.Medium, result.Delimiter.Confidence);
        CsvDelimiterCandidateEvidence semicolon = Candidate(result, ";");
        Assert.Equal(3, semicolon.ExactWidthRecords);
        Assert.Equal(10_000, semicolon.ConsistencyBasisPoints);
        Assert.Equal(CsvDelimiterCandidateStatus.Incompatible, Candidate(result, ",").Status);
    }

    [Fact]
    public async Task ReportsAmbiguousWhenTwoCandidatesHaveIdenticalLogicalShape()
    {
        await using CsvSourceSnapshot snapshot = await SnapshotAsync("a,b;c\nd,e;f\n");

        CsvFormatInspection result = await InspectAsync(
            snapshot,
            inspectionOptions: new CsvInspectionOptions
            {
                DelimiterCandidates = [",", ";"],
            });

        Assert.Equal(CsvInspectionResolution.Ambiguous, result.Delimiter.Resolution);
        Assert.Equal(CsvInspectionConfidence.None, result.Delimiter.Confidence);
        Assert.Null(result.Delimiter.SelectedDelimiter);
        Assert.Null(result.Format);
        Assert.Equal(10_000, Candidate(result, ",").ConsistencyBasisPoints);
        Assert.Equal(10_000, Candidate(result, ";").ConsistencyBasisPoints);
    }

    [Fact]
    public async Task CandidateOrderAndCultureDoNotChangeEvidence()
    {
        await using CsvSourceSnapshot snapshot = await SnapshotAsync("a,b;c\nd,e;f\n");
        CsvFormatInspection first = await InspectAsync(
            snapshot,
            new CsvReaderOptions { Culture = CultureInfo.InvariantCulture },
            new CsvInspectionOptions { DelimiterCandidates = [",", ";"] });
        CsvFormatInspection second = await InspectAsync(
            snapshot,
            new CsvReaderOptions { Culture = CultureInfo.GetCultureInfo("de-DE") },
            new CsvInspectionOptions { DelimiterCandidates = [";", ","] });

        Assert.Equal(first.Delimiter.Resolution, second.Delimiter.Resolution);
        Assert.Equal(
            first.Delimiter.Candidates.Select(EvidenceTuple),
            second.Delimiter.Candidates.Select(EvidenceTuple));
        Assert.Equal([",", ";"], first.Delimiter.Candidates.Select(candidate => candidate.Delimiter));
        Assert.Equal([",", ";"], second.Delimiter.Candidates.Select(candidate => candidate.Delimiter));
    }

    [Fact]
    public async Task IgnoresCandidateCharactersInsideEscapedMultilineQuotedFields()
    {
        const string csv = "id;note\n1;\"a,b\n,c\"\n2;\"x\"\";\"\"y\"\n";
        await using CsvSourceSnapshot snapshot = await SnapshotAsync(csv);

        CsvFormatInspection result = await InspectAsync(
            snapshot,
            inspectionOptions: new CsvInspectionOptions
            {
                DelimiterCandidates = [",", ";"],
            });

        Assert.Equal(";", result.Delimiter.SelectedDelimiter);
        CsvDelimiterCandidateEvidence evidence = Candidate(result, ";");
        Assert.Equal(3, evidence.ExactWidthRecords);
        Assert.Equal(2, evidence.QuotedFields);
        Assert.Equal(1, evidence.MultilineRecords);
    }

    [Fact]
    public async Task DelimiterProbesDoNotRejectANullTokenValidForTheWinner()
    {
        await using CsvSourceSnapshot snapshot = await SnapshotAsync("a;b\n1;N,A\n2;value\n");

        CsvFormatInspection result = await InspectAsync(
            snapshot,
            new CsvReaderOptions { NullToken = "N,A" });
        CsvSourceBinding binding = await CreateBindingAsync(snapshot, result);

        Assert.Equal(";", result.Delimiter.SelectedDelimiter);
        Assert.Equal("N,A", binding.Format.NullToken);
    }

    [Theory]
    [InlineData("")]
    [InlineData("identifier\n")]
    [InlineData("identifier\n001\n002\n")]
    public async Task DoesNotInventADelimiterForInsufficientData(string csv)
    {
        await using CsvSourceSnapshot snapshot = await SnapshotAsync(csv);

        CsvFormatInspection result = await InspectAsync(snapshot);

        Assert.Equal(CsvInspectionResolution.InsufficientData, result.Delimiter.Resolution);
        Assert.Null(result.Delimiter.SelectedDelimiter);
        Assert.Null(result.Format);
    }

    [Fact]
    public async Task ARestrictedSingleCandidateIsAnExplicitChoice()
    {
        await using CsvSourceSnapshot snapshot = await SnapshotAsync(string.Empty);

        CsvFormatInspection result = await InspectAsync(
            snapshot,
            inspectionOptions: new CsvInspectionOptions
            {
                DelimiterCandidates = [";"],
            });

        Assert.Equal(CsvInspectionResolution.Resolved, result.Delimiter.Resolution);
        Assert.Equal(CsvInspectionConfidence.Explicit, result.Delimiter.Confidence);
        Assert.Equal(";", result.Delimiter.SelectedDelimiter);
        Assert.NotNull(result.Format);
    }

    [Theory]
    [MemberData(nameof(ByteOrderMarkEncodings))]
    public async Task DetectsSupportedByteOrderMarks(Encoding encoding)
    {
        byte[] payload = encoding.GetPreamble().Concat(encoding.GetBytes("a,b\n1,2\n")).ToArray();
        await using CsvSourceSnapshot snapshot = await SnapshotAsync(payload);

        CsvFormatInspection result = await InspectAsync(
            snapshot,
            inspectionOptions: new CsvInspectionOptions { DelimiterCandidates = [","] });

        Assert.True(result.Encoding.HasByteOrderMark);
        Assert.Equal(encoding.WebName, result.Encoding.ResolvedEncodingName);
        Assert.Equal(CsvEncodingEvidenceKind.ByteOrderMark, result.Encoding.EvidenceKind);
        Assert.Equal(CsvInspectionConfidence.High, result.Encoding.Confidence);
        Assert.True(result.Encoding.SampleIsValid);

        CsvSourceBinding binding = await CreateBindingAsync(snapshot, result);
        await using CsvStreamingReader reader = await OpenReaderAsync(binding, snapshot);
        CsvLogicalRecord record = Assert.Single(await CollectAsync(ReadRecords(reader)));
        Assert.Equal("1", record.Fields[0].Value);
        Assert.Equal("2", record.Fields[1].Value);
    }

    [Fact]
    public async Task AScalarSplitAtTheInspectionBoundaryIsTruncatedNotInvalid()
    {
        byte[] bytes = Encoding.UTF8.GetBytes("a,b\né,2\nmore,data\n");
        await using CsvSourceSnapshot snapshot = await SnapshotAsync(bytes);

        CsvFormatInspection result = await InspectAsync(
            snapshot,
            inspectionOptions: new CsvInspectionOptions
            {
                DelimiterCandidates = [","],
                MaxSampleBytes = 5,
            });

        Assert.True(result.SampleWasByteLimited);
        Assert.True(result.Encoding.SampleIsValid);
        Assert.Equal(CsvInspectionResolution.Resolved, result.Delimiter.Resolution);
        Assert.Equal(CsvDelimiterCandidateStatus.Truncated, Candidate(result, ",").Status);
    }

    [Fact]
    public async Task InvalidUtf8AtTrueEndOfInputIsRejected()
    {
        byte[] bytes = [0x61, 0x2C, 0x62, 0x0A, 0xC3];
        await using CsvSourceSnapshot snapshot = await SnapshotAsync(bytes);

        CsvFormatInspection result = await InspectAsync(
            snapshot,
            inspectionOptions: new CsvInspectionOptions { DelimiterCandidates = [","] });

        Assert.False(result.Encoding.SampleIsValid);
        Assert.Equal(CsvInspectionConfidence.None, result.Encoding.Confidence);
        Assert.Equal(CsvInspectionResolution.Invalid, result.Delimiter.Resolution);
        Assert.Equal(CsvDiagnosticRules.InvalidEncoding, Candidate(result, ",").DiagnosticRuleId);
    }

    [Fact]
    public async Task ReportsTheInspectionCharacterBudgetWithoutClaimingARecordViolation()
    {
        await using CsvSourceSnapshot snapshot = await SnapshotAsync("a,b\n1,2\n3,4\n");

        CsvFormatInspection result = await InspectAsync(
            snapshot,
            inspectionOptions: new CsvInspectionOptions
            {
                DelimiterCandidates = [","],
                MaxSampleCharacters = 4,
            });

        Assert.True(result.Encoding.SampleIsValid);
        Assert.True(result.Encoding.CharacterLimitReached);
        Assert.Equal(CsvInspectionResolution.InsufficientData, result.Delimiter.Resolution);
        Assert.Equal(
            CsvDiagnosticRules.InspectionCharacterLimitExceeded,
            Candidate(result, ",").DiagnosticRuleId);
    }

    [Fact]
    public async Task BomDisabledUsesTheSamePreambleFreeDecoderForEquivalentUtf8Options()
    {
        byte[] bytes = new UTF8Encoding(true, true).GetPreamble()
            .Concat(Encoding.UTF8.GetBytes("a,b\n1,2\n"))
            .ToArray();
        await using CsvSourceSnapshot snapshot = await SnapshotAsync(bytes);
        CsvSourceBinding emitting = await BindAsync(
            snapshot,
            new CsvReaderOptions
            {
                Encoding = new UTF8Encoding(true, true),
                DetectEncodingFromByteOrderMarks = false,
            });
        CsvSourceBinding nonEmitting = await BindAsync(
            snapshot,
            new CsvReaderOptions
            {
                Encoding = new UTF8Encoding(false, true),
                DetectEncodingFromByteOrderMarks = false,
            });

        Assert.False(emitting.Format.HasByteOrderMark);
        Assert.Equal(emitting.Source.Fingerprint, nonEmitting.Source.Fingerprint);
        await using CsvStreamingReader firstReader = await OpenReaderAsync(emitting, snapshot);
        await using CsvStreamingReader secondReader = await OpenReaderAsync(nonEmitting, snapshot);
        Assert.Equal("\uFEFFa", firstReader.Header!.Fields[0]);
        Assert.Equal(firstReader.Header.Fields[0], secondReader.Header!.Fields[0]);
    }

    [Fact]
    public async Task CallerFallbackDifferencesAreNormalizedToCanonicalStrictUtf8()
    {
        await using CsvSourceSnapshot snapshot = await SnapshotAsync("a,b\n1,2\n");
        CsvSourceBinding strict = await BindAsync(
            snapshot,
            new CsvReaderOptions { Encoding = new UTF8Encoding(false, true) });
        CsvSourceBinding replacing = await BindAsync(
            snapshot,
            new CsvReaderOptions { Encoding = new UTF8Encoding(false, false) });

        Assert.Equal(65001, strict.Format.EncodingCodePage);
        Assert.Equal(strict.OptionsDigest, replacing.OptionsDigest);
        Assert.Equal(strict.Source.Fingerprint, replacing.Source.Fingerprint);
    }

    [Fact]
    public async Task RejectsNonCanonicalEncodingBeforeOpeningTheSnapshot()
    {
        CsvSourceSnapshot snapshot = await SnapshotAsync("a,b\n1,2\n");
        await snapshot.DisposeAsync();

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await InspectAsync(
                snapshot,
                new CsvReaderOptions { Encoding = Encoding.Latin1 },
                new CsvInspectionOptions { DelimiterCandidates = [","] }));
    }

    [Fact]
    public async Task RejectsInvalidCandidatesBeforeOpeningTheSnapshot()
    {
        CsvSourceSnapshot snapshot = await SnapshotAsync("a,b\n1,2\n");
        await snapshot.DisposeAsync();

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await InspectAsync(
                snapshot,
                inspectionOptions: new CsvInspectionOptions
                {
                    DelimiterCandidates = [",", ","],
                }));
    }

    [Fact]
    public async Task HonorsExactByteAndLogicalRecordBudgets()
    {
        string csv = string.Join('\n', Enumerable.Range(0, 100).Select(index => $"{index},{index}")) + "\n";
        await using CsvSourceSnapshot snapshot = await SnapshotAsync(csv);

        CsvFormatInspection result = await InspectAsync(
            snapshot,
            inspectionOptions: new CsvInspectionOptions
            {
                DelimiterCandidates = [",", ";"],
                MaxSampleBytes = 64,
                MaxLogicalRecords = 2,
            });

        Assert.Equal(64, result.SampledBytes);
        Assert.True(result.SampleWasByteLimited);
        Assert.True(result.Delimiter.LogicalRecordLimitReached);
        Assert.Equal(2, Candidate(result, ",").CompleteLogicalRecords);
    }

    [Fact]
    public async Task SnapshotFingerprintCoversEveryRawByteAndIsChunkInvariant()
    {
        byte[] bytes = Encoding.UTF8.GetBytes("a,b\r\n1,2\r\n3,4\r\n");
        using var workspace = new TemporaryWorkspace();
        await using CsvSourceSnapshot direct = await CreateSnapshotAsync(
            new MemoryStream(bytes),
            workspace.Options(leaveOpen: false));
        await using CsvSourceSnapshot chunked = await CreateSnapshotAsync(
            new ChunkedReadStream(new MemoryStream(bytes), 1),
            workspace.Options(leaveOpen: false));

        Assert.Equal(direct.ContentDigest, chunked.ContentDigest);
        Assert.Equal(direct.SnapshotIdentity, chunked.SnapshotIdentity);
        Assert.Equal(bytes.LongLength, direct.ContentLength);
        Assert.Equal(
            "sha256:" + Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant(),
            direct.ContentDigest);
    }

    [Fact]
    public async Task SourceFingerprintIncludesBytesBeyondTheInspectionWindow()
    {
        byte[] first = Encoding.UTF8.GetBytes("a,b\n1,2\n3,first-tail\n");
        byte[] second = Encoding.UTF8.GetBytes("a,b\n1,2\n3,other-tail\n");
        await using CsvSourceSnapshot firstSnapshot = await SnapshotAsync(first);
        await using CsvSourceSnapshot secondSnapshot = await SnapshotAsync(second);
        var limits = new CsvInspectionOptions
        {
            DelimiterCandidates = [","],
            MaxSampleBytes = 8,
        };
        CsvFormatInspection firstInspection = await InspectAsync(
            firstSnapshot,
            inspectionOptions: limits);
        CsvFormatInspection secondInspection = await InspectAsync(
            secondSnapshot,
            inspectionOptions: limits);
        CsvSourceBinding firstBinding = await CreateBindingAsync(firstSnapshot, firstInspection);
        CsvSourceBinding secondBinding = await CreateBindingAsync(secondSnapshot, secondInspection);

        Assert.NotEqual(firstSnapshot.ContentDigest, secondSnapshot.ContentDigest);
        Assert.NotEqual(firstBinding.Source.Fingerprint, secondBinding.Source.Fingerprint);
    }

    [Fact]
    public async Task BindingSeparatesContentSnapshotIdentityFromFormatSemantics()
    {
        await using CsvSourceSnapshot snapshot = await SnapshotAsync("a,b\n1,2\n");
        CsvFormatInspection commaInspection = await InspectAsync(
            snapshot,
            inspectionOptions: new CsvInspectionOptions { DelimiterCandidates = [","] });
        CsvFormatInspection semicolonInspection = await InspectAsync(
            snapshot,
            inspectionOptions: new CsvInspectionOptions { DelimiterCandidates = [";"] });
        CsvSourceBinding comma = await CreateBindingAsync(snapshot, commaInspection);
        CsvSourceBinding semicolon = await CreateBindingAsync(snapshot, semicolonInspection);

        Assert.Equal(comma.SnapshotIdentity, semicolon.SnapshotIdentity);
        Assert.Equal(comma.ContentDigest, semicolon.ContentDigest);
        Assert.NotEqual(comma.OptionsDigest, semicolon.OptionsDigest);
        Assert.NotEqual(comma.Source.Fingerprint, semicolon.Source.Fingerprint);
        Assert.Equal(MigrationSourceKind.Csv, comma.Source.Kind);
        Assert.Equal(MigrationConsistencyKind.Snapshot, comma.Source.Consistency.Kind);
    }

    [Fact]
    public async Task OperationalLimitsDoNotChangeTheSemanticFingerprint()
    {
        await using CsvSourceSnapshot snapshot = await SnapshotAsync("a,b\n1,2\n");
        CsvFormatInspection firstInspection = await InspectAsync(
            snapshot,
            new CsvReaderOptions
            {
                MaxFieldCharacters = 1024,
                MaxRecordCharacters = 2048,
                MaxFieldsPerRecord = 32,
                LeaveOpen = true,
            },
            new CsvInspectionOptions { DelimiterCandidates = [","] });
        CsvFormatInspection secondInspection = await InspectAsync(
            snapshot,
            new CsvReaderOptions
            {
                MaxFieldCharacters = 4096,
                MaxRecordCharacters = 8192,
                MaxFieldsPerRecord = 64,
                LeaveOpen = false,
            },
            new CsvInspectionOptions { DelimiterCandidates = [","] });
        CsvSourceBinding first = await CreateBindingAsync(snapshot, firstInspection);
        CsvSourceBinding second = await CreateBindingAsync(snapshot, secondInspection);

        Assert.Equal(first.OptionsDigest, second.OptionsDigest);
        Assert.Equal(first.Source.Fingerprint, second.Source.Fingerprint);
    }

    [Fact]
    public async Task HeaderAndNullPoliciesChangeTheSemanticFingerprint()
    {
        await using CsvSourceSnapshot snapshot = await SnapshotAsync("a,b\n1,2\n");
        CsvSourceBinding baseline = await BindAsync(snapshot, new CsvReaderOptions());
        CsvSourceBinding headerless = await BindAsync(
            snapshot,
            new CsvReaderOptions { HasHeaderRecord = false });
        CsvSourceBinding withNull = await BindAsync(
            snapshot,
            new CsvReaderOptions { NullToken = "NULL" });

        Assert.NotEqual(baseline.Source.Fingerprint, headerless.Source.Fingerprint);
        Assert.NotEqual(baseline.Source.Fingerprint, withNull.Source.Fingerprint);
    }

    [Fact]
    public async Task SameNamedCulturesWithDifferentConversionPoliciesHaveDifferentFingerprints()
    {
        await using CsvSourceSnapshot snapshot = await SnapshotAsync("a,b\n1,2\n");
        var customized = (CultureInfo)CultureInfo.InvariantCulture.Clone();
        customized.NumberFormat.NumberDecimalSeparator = ",";
        CsvSourceBinding standard = await BindAsync(
            snapshot,
            new CsvReaderOptions { Culture = CultureInfo.InvariantCulture });
        CsvSourceBinding changed = await BindAsync(
            snapshot,
            new CsvReaderOptions { Culture = customized });

        Assert.Equal(standard.Format.CultureName, changed.Format.CultureName);
        Assert.NotEqual(standard.Format.CulturePolicyDigest, changed.Format.CulturePolicyDigest);
        Assert.NotEqual(standard.Source.Fingerprint, changed.Source.Fingerprint);
    }

    [Fact]
    public async Task CultureArrayBoundariesCannotCollideInThePolicyDigest()
    {
        await using CsvSourceSnapshot snapshot = await SnapshotAsync("a,b\n1,2\n");
        var firstCulture = (CultureInfo)CultureInfo.InvariantCulture.Clone();
        var secondCulture = (CultureInfo)CultureInfo.InvariantCulture.Clone();
        firstCulture.DateTimeFormat.AbbreviatedDayNames =
            ["a\u001fb", "c", "d", "e", "f", "g", "h"];
        secondCulture.DateTimeFormat.AbbreviatedDayNames =
            ["a", "b\u001fc", "d", "e", "f", "g", "h"];

        CsvSourceBinding first = await BindAsync(
            snapshot,
            new CsvReaderOptions { Culture = firstCulture });
        CsvSourceBinding second = await BindAsync(
            snapshot,
            new CsvReaderOptions { Culture = secondCulture });

        Assert.NotEqual(first.Format.CulturePolicyDigest, second.Format.CulturePolicyDigest);
        Assert.NotEqual(first.Source.Fingerprint, second.Source.Fingerprint);
    }

    [Fact]
    public async Task BindingReadsTheFrozenSnapshotAfterTheOriginalFileChanges()
    {
        using var workspace = new TemporaryWorkspace();
        string sourcePath = Path.Combine(workspace.Root, "source.csv");
        await File.WriteAllTextAsync(
            sourcePath,
            "a,b\nold,value\n",
            new UTF8Encoding(false),
            TestContext.Current.CancellationToken);
        await using CsvSourceSnapshot snapshot = await CsvSourceSnapshot.CreateFromFileAsync(
            sourcePath,
            workspace.Options(),
            TestContext.Current.CancellationToken);
        CsvFormatInspection inspection = await InspectAsync(
            snapshot,
            inspectionOptions: new CsvInspectionOptions { DelimiterCandidates = [","] });
        CsvSourceBinding binding = await CreateBindingAsync(
            snapshot,
            inspection,
            sourcePath);

        await File.WriteAllTextAsync(
            sourcePath,
            "a,b\nnew,value\n",
            new UTF8Encoding(false),
            TestContext.Current.CancellationToken);
        await using CsvStreamingReader reader = await OpenReaderAsync(binding, snapshot);
        CsvLogicalRecord record = Assert.Single(await CollectAsync(ReadRecords(reader)));

        Assert.Equal("old", record.Fields[0].Value);
        Assert.DoesNotContain(sourcePath, binding.Source.Identity, StringComparison.OrdinalIgnoreCase);
        Assert.StartsWith("csv-logical:sha256:", binding.Source.Identity, StringComparison.Ordinal);
    }

    [Fact]
    public async Task BindingRejectsADifferentSnapshot()
    {
        await using CsvSourceSnapshot first = await SnapshotAsync("a,b\n1,2\n");
        await using CsvSourceSnapshot second = await SnapshotAsync("a,b\n3,4\n");
        CsvSourceBinding binding = await BindAsync(first, new CsvReaderOptions());

        await Assert.ThrowsAsync<ArgumentException>(
            async () => await OpenReaderAsync(binding, second));
    }

    [Fact]
    public async Task SnapshotEnforcesTheByteLimitAndCleansItsPrivateWorkspace()
    {
        using var workspace = new TemporaryWorkspace();
        var source = new MemoryStream([1, 2, 3, 4]);

        CsvSourceSnapshotException exception = await Assert.ThrowsAsync<CsvSourceSnapshotException>(
            async () => await CreateSnapshotAsync(
                source,
                workspace.Options(maxSourceBytes: 3)));

        Assert.Equal(CsvSnapshotDiagnosticRules.SourceLimitExceeded, exception.RuleId);
        Assert.Empty(Directory.EnumerateFileSystemEntries(workspace.Root));
        Assert.Throws<ObjectDisposedException>(() => _ = source.Position);
    }

    [Fact]
    public async Task SnapshotDisposalRemovesPrivateFilesAndHonorsLeaveOpen()
    {
        using var workspace = new TemporaryWorkspace();
        var source = new MemoryStream(Encoding.UTF8.GetBytes("a,b\n1,2\n"));
        CsvSourceSnapshot snapshot = await CreateSnapshotAsync(
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
    public async Task SourceDisposeFailureDoesNotLeakACompletedSnapshot()
    {
        using var workspace = new TemporaryWorkspace();
        var source = new ThrowingDisposeStream(Encoding.UTF8.GetBytes("a,b\n1,2\n"));

        await Assert.ThrowsAsync<IOException>(
            async () => await CreateSnapshotAsync(source, workspace.Options()));

        Assert.Empty(Directory.EnumerateFileSystemEntries(workspace.Root));
    }

    [Fact]
    public async Task SourceDisposeFailureDoesNotMaskThePrimarySnapshotFailure()
    {
        using var workspace = new TemporaryWorkspace();
        var source = new ThrowingDisposeStream(
            [1, 2, 3, 4],
            new InvalidOperationException("Injected cleanup failure."));

        CsvSourceSnapshotException exception = await Assert.ThrowsAsync<CsvSourceSnapshotException>(
            async () => await CreateSnapshotAsync(
                source,
                workspace.Options(maxSourceBytes: 3)));

        Assert.Equal(CsvSnapshotDiagnosticRules.SourceLimitExceeded, exception.RuleId);
        Assert.Empty(Directory.EnumerateFileSystemEntries(workspace.Root));
    }

    [Fact]
    public async Task OutstandingReadLeaseDefersCleanupUntilTheReaderCloses()
    {
        using var workspace = new TemporaryWorkspace();
        CsvSourceSnapshot snapshot = await CreateSnapshotAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("a,b\n1,2\n")),
            workspace.Options());
        Stream reader = snapshot.OpenRead();

        await snapshot.DisposeAsync();
        Assert.Single(Directory.EnumerateDirectories(workspace.Root));
        await reader.DisposeAsync();

        Assert.Empty(Directory.EnumerateFileSystemEntries(workspace.Root));
    }

    [Fact]
    public async Task ConcurrentDisposeCallsJoinTheSameLifecycleTask()
    {
        using var workspace = new TemporaryWorkspace();
        CsvSourceSnapshot snapshot = await CreateSnapshotAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("a,b\n1,2\n")),
            workspace.Options());

        Task first = snapshot.DisposeAsync().AsTask();
        Task second = snapshot.DisposeAsync().AsTask();

        Assert.Same(first, second);
        await Task.WhenAll(first, second);
        Assert.Empty(Directory.EnumerateFileSystemEntries(workspace.Root));
    }

    [Fact]
    public async Task SnapshotWorkspaceAndFilesAreUserOnlyOnUnix()
    {
        if (OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryWorkspace();
        await using CsvSourceSnapshot snapshot = await CreateSnapshotAsync(
            new MemoryStream(Encoding.UTF8.GetBytes("a,b\n1,2\n")),
            workspace.Options());
        string privateDirectory = Assert.Single(Directory.EnumerateDirectories(workspace.Root));
        UnixFileMode allowedDirectory =
            UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute;
        Assert.Equal(allowedDirectory, File.GetUnixFileMode(privateDirectory) & ~UnixFileMode.StickyBit);
        foreach (string file in Directory.EnumerateFiles(privateDirectory))
        {
            Assert.Equal(
                UnixFileMode.UserRead | UnixFileMode.UserWrite,
                File.GetUnixFileMode(file));
        }
    }

    public static TheoryData<Encoding> ByteOrderMarkEncodings => new()
    {
        new UTF8Encoding(true, true),
        new UnicodeEncoding(false, true, true),
        new UnicodeEncoding(true, true, true),
        new UTF32Encoding(false, true, true),
        new UTF32Encoding(true, true, true),
    };

    private static ValueTask<CsvFormatInspection> InspectAsync(
        CsvSourceSnapshot snapshot,
        CsvReaderOptions? readerOptions = null,
        CsvInspectionOptions? inspectionOptions = null) =>
        CsvFormatInspector.InspectAsync(
            snapshot,
            readerOptions,
            inspectionOptions,
            TestContext.Current.CancellationToken);

    private static ValueTask<CsvSourceBinding> CreateBindingAsync(
        CsvSourceSnapshot snapshot,
        CsvFormatInspection inspection,
        string? logicalSourceIdentity = null) =>
        CsvSourceBinding.CreateAsync(
            snapshot,
            inspection,
            logicalSourceIdentity,
            TestContext.Current.CancellationToken);

    private static ValueTask<CsvSourceSnapshot> CreateSnapshotAsync(
        Stream source,
        CsvSourceSnapshotOptions? options = null) =>
        CsvSourceSnapshot.CreateAsync(
            source,
            options,
            TestContext.Current.CancellationToken);

    private static ValueTask<CsvStreamingReader> OpenReaderAsync(
        CsvSourceBinding binding,
        CsvSourceSnapshot snapshot) =>
        binding.OpenReaderAsync(snapshot, TestContext.Current.CancellationToken);

    private static IAsyncEnumerable<CsvLogicalRecord> ReadRecords(CsvStreamingReader reader) =>
        reader.ReadRecordsAsync(TestContext.Current.CancellationToken);

    private static async ValueTask<CsvSourceBinding> BindAsync(
        CsvSourceSnapshot snapshot,
        CsvReaderOptions options)
    {
        CsvFormatInspection inspection = await InspectAsync(
            snapshot,
            options,
            new CsvInspectionOptions { DelimiterCandidates = [","] });
        return await CreateBindingAsync(snapshot, inspection);
    }

    private static CsvDelimiterCandidateEvidence Candidate(
        CsvFormatInspection result,
        string delimiter) =>
        Assert.Single(result.Delimiter.Candidates, candidate => candidate.Delimiter == delimiter);

    private static object EvidenceTuple(CsvDelimiterCandidateEvidence candidate) => new
    {
        candidate.Delimiter,
        candidate.Status,
        candidate.ExpectedFieldCount,
        candidate.CompleteLogicalRecords,
        candidate.ExactWidthRecords,
        candidate.ShortRecords,
        candidate.ExtraRecords,
        candidate.ConsistencyBasisPoints,
    };

    private static async ValueTask<CsvSourceSnapshot> SnapshotAsync(string text) =>
        await SnapshotAsync(Encoding.UTF8.GetBytes(text));

    private static async ValueTask<CsvSourceSnapshot> SnapshotAsync(byte[] bytes) =>
        await CreateSnapshotAsync(new MemoryStream(bytes));

    private static async Task<List<CsvLogicalRecord>> CollectAsync(
        IAsyncEnumerable<CsvLogicalRecord> records)
    {
        var result = new List<CsvLogicalRecord>();
        await foreach (CsvLogicalRecord record in records)
            result.Add(record);
        return result;
    }

    private sealed class TemporaryWorkspace : IDisposable
    {
        public TemporaryWorkspace()
        {
            Root = Path.Combine(Path.GetTempPath(), $"csharpdb-csv-tests-{Guid.NewGuid():N}");
            Directory.CreateDirectory(Root);
        }

        public string Root { get; }

        public CsvSourceSnapshotOptions Options(
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

        public ChunkedReadStream(Stream inner, int chunkSize)
        {
            this.inner = inner;
            this.chunkSize = chunkSize;
        }

        public override bool CanRead => inner.CanRead;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count) =>
            inner.Read(buffer, offset, Math.Min(count, chunkSize));

        public override ValueTask<int> ReadAsync(
            Memory<byte> buffer,
            CancellationToken cancellationToken = default) =>
            inner.ReadAsync(buffer[..Math.Min(buffer.Length, chunkSize)], cancellationToken);

        public override void Flush() => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

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

    private sealed class ThrowingDisposeStream : MemoryStream
    {
        private readonly Exception exception;

        public ThrowingDisposeStream(byte[] bytes)
            : this(bytes, new IOException("Injected source disposal failure."))
        {
        }

        public ThrowingDisposeStream(byte[] bytes, Exception exception)
            : base(bytes)
        {
            this.exception = exception;
        }

        public override ValueTask DisposeAsync() =>
            ValueTask.FromException(exception);
    }
}
