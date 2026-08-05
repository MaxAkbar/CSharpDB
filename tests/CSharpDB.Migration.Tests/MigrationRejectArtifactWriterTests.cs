using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Security.AccessControl;
using System.Security.Cryptography;
using System.Security.Principal;
using System.Text;
using CSharpDB.Migration.Validation;
using CSharpDB.Primitives;
using Microsoft.Win32.SafeHandles;

namespace CSharpDB.Migration.Tests;

public sealed class MigrationRejectArtifactWriterTests
{
    private const string RuleId = "MIG-TEST-001";
    private const string ExpectedTargetIdentity = "target:reject-artifact-test";
    private const string TargetSnapshotIdentity = "target:snapshot:reject-artifact-test";
    private const string SourceSnapshotIdentity = "source:snapshot:reject-artifact-test";
    private const string EvidenceFailureMessage =
        "The authoritative migration reject evidence cannot be materialized.";

    [Fact]
    public async Task WriteAsync_PublishesHeaderOnlyArtifactForEmptyEvidence()
    {
        string root = CreateRoot();
        try
        {
            ArtifactFixture fixture = await CreateFixtureAsync([]);
            string outputPath = Path.Combine(root, "rejects.jsonl");
            var target = new ArtifactTarget(new ArtifactSnapshot([], []));

            MigrationRejectArtifactWriteResult result = await WriteAsync(
                fixture,
                target,
                outputPath,
                TestContext.Current.CancellationToken);

            string expected =
                MigrationRejectLedgerCodec.SerializeArtifactHeader(fixture.PlanDigest) + "\n";
            byte[] expectedBytes = Encoding.UTF8.GetBytes(expected);
            Assert.Equal(expectedBytes, await File.ReadAllBytesAsync(
                outputPath,
                TestContext.Current.CancellationToken));
            Assert.Equal(outputPath, result.ArtifactPath);
            Assert.Equal(fixture.PlanDigest, result.PlanDigest);
            Assert.Equal(ExpectedTargetIdentity, result.TargetIdentity);
            Assert.Equal(TargetSnapshotIdentity, result.TargetSnapshotIdentity);
            Assert.Equal(Sha256(expectedBytes), result.ArtifactDigest);
            Assert.Equal(0, result.RejectedRowCount);
            Assert.Equal(expectedBytes.LongLength, result.ArtifactBytes);
            Assert.False(result.ReusedExistingArtifact);
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task WriteAsync_PublishesExactCanonicalMixedOutcomeBytes()
    {
        string root = CreateRoot();
        try
        {
            ArtifactFixture fixture = await CreateFixtureAsync(
            [
                new OutcomeSpec(
                    AcceptedRows: 1,
                    RejectedValues: ["source-private-value"]),
            ]);
            string outputPath = Path.Combine(root, "rejects.jsonl");
            var target = new ArtifactTarget(
                new ArtifactSnapshot(fixture.Receipts, fixture.Ledger));

            MigrationRejectArtifactWriteResult result = await WriteAsync(
                fixture,
                target,
                outputPath,
                TestContext.Current.CancellationToken);

            MigrationRejectLedgerEntry entry = Assert.Single(fixture.Ledger);
            string expected =
                MigrationRejectLedgerCodec.SerializeArtifactHeader(fixture.PlanDigest) + "\n" +
                MigrationRejectLedgerCodec.SerializeEntry(
                    entry.SourceObjectId,
                    entry.BatchOrdinal,
                    entry.RejectedRow) + "\n";
            byte[] expectedBytes = Encoding.UTF8.GetBytes(expected);
            Assert.Equal(expectedBytes, await File.ReadAllBytesAsync(
                outputPath,
                TestContext.Current.CancellationToken));
            Assert.Equal(Sha256(expectedBytes), result.ArtifactDigest);
            Assert.Equal(1, result.RejectedRowCount);
            Assert.Equal(expectedBytes.LongLength, result.ArtifactBytes);
            Assert.False(result.ReusedExistingArtifact);
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task WriteAsync_ReusesAnExactExistingArtifact()
    {
        string root = CreateRoot();
        try
        {
            ArtifactFixture fixture = await CreateFixtureAsync(
            [
                new OutcomeSpec(
                    AcceptedRows: 0,
                    RejectedValues: ["source-private-value"]),
            ]);
            string outputPath = Path.Combine(root, "rejects.jsonl");
            var target = new ArtifactTarget(
                new ArtifactSnapshot(fixture.Receipts, fixture.Ledger));

            MigrationRejectArtifactWriteResult first = await WriteAsync(
                fixture,
                target,
                outputPath,
                TestContext.Current.CancellationToken);
            byte[] original = await File.ReadAllBytesAsync(
                outputPath,
                TestContext.Current.CancellationToken);
            MigrationRejectArtifactWriteResult second = await WriteAsync(
                fixture,
                target,
                outputPath,
                TestContext.Current.CancellationToken);

            Assert.False(first.ReusedExistingArtifact);
            Assert.True(second.ReusedExistingArtifact);
            Assert.Equal(first.ArtifactDigest, second.ArtifactDigest);
            Assert.Equal(original, await File.ReadAllBytesAsync(
                outputPath,
                TestContext.Current.CancellationToken));
            Assert.Single(Directory.EnumerateFiles(root));
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task DisposeAsync_ReleasesUnixClaimLockBeforeClosingHandle()
    {
        if (OperatingSystem.IsWindows())
            return;

        string root = CreateRoot();
        MigrationRejectArtifactPublication? publication = null;
        SafeFileHandle? retainedHandle = null;
        SafeFileHandle? competingHandle = null;
        bool competingLockHeld = false;
        try
        {
            string outputPath = Path.Combine(root, "rejects.jsonl");
            byte[] expected = "released-lock\n"u8.ToArray();
            publication = await MigrationRejectArtifactPublication.OpenAsync(
                outputPath,
                "sha256:" + new string('a', 64),
                maximumBytes: 1_024,
                TestContext.Current.CancellationToken);
            await publication.Stream.WriteAsync(
                expected,
                TestContext.Current.CancellationToken);
            await publication.FlushAsync(TestContext.Current.CancellationToken);
            Assert.False(await publication.PublishOrReuseAsync(
                TestContext.Current.CancellationToken));

            int duplicateDescriptor = UnixDuplicate(checked(
                (int)publication.Stream.SafeFileHandle.DangerousGetHandle()));
            Assert.True(
                duplicateDescriptor >= 0,
                $"dup failed with errno {Marshal.GetLastPInvokeError()}.");
            retainedHandle = new SafeFileHandle(
                new IntPtr(duplicateDescriptor),
                ownsHandle: true);

            await publication.DisposeAsync();
            publication = null;

            Assert.False(retainedHandle.IsClosed);
            competingHandle = File.OpenHandle(
                outputPath,
                FileMode.Open,
                FileAccess.ReadWrite,
                FileShare.ReadWrite | FileShare.Delete);
            int lockResult = UnixFlock(
                competingHandle,
                LockExclusive | LockNonBlocking);
            Assert.True(
                lockResult == 0,
                $"A separately opened descriptor could not claim the released lock; " +
                $"errno {Marshal.GetLastPInvokeError()}.");
            competingLockHeld = true;
        }
        finally
        {
            if (publication is not null)
                await publication.DisposeAsync();
            if (competingLockHeld && competingHandle is not null)
                _ = UnixFlock(competingHandle, LockUnlock);
            competingHandle?.Dispose();
            retainedHandle?.Dispose();
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task WriteAsync_CompetingWriterCannotReplaceAnActivePublication()
    {
        string root = CreateRoot();
        using var firstReached = new ManualResetEventSlim();
        using var releaseFirst = new ManualResetEventSlim();
        Task<MigrationRejectArtifactWriteResult>? firstTask = null;
        try
        {
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            ArtifactFixture fixture = await CreateFixtureAsync(
            [
                new OutcomeSpec(
                    AcceptedRows: 0,
                    RejectedValues: ["source-private-value"]),
            ]);
            string outputPath = Path.Combine(root, "rejects.jsonl");
            int blocked = 0;
            var firstTarget = new ArtifactTarget(new ArtifactSnapshot(
                fixture.Receipts,
                fixture.Ledger,
                beforeReceiptYield: () =>
                {
                    if (Interlocked.Exchange(ref blocked, 1) != 0)
                        return;
                    firstReached.Set();
                    releaseFirst.Wait(cancellationToken);
                }));
            var competingTarget = new ArtifactTarget(
                new ArtifactSnapshot(fixture.Receipts, fixture.Ledger));
            firstTask = Task.Run(async () => await WriteAsync(
                fixture,
                firstTarget,
                outputPath,
                cancellationToken));

            Assert.True(firstReached.Wait(TimeSpan.FromSeconds(30), cancellationToken));
            _ = await Assert.ThrowsAsync<IOException>(async () =>
                await WriteAsync(
                    fixture,
                    competingTarget,
                    outputPath,
                    cancellationToken));

            releaseFirst.Set();
            MigrationRejectArtifactWriteResult first = await firstTask;
            MigrationRejectArtifactWriteResult retry = await WriteAsync(
                fixture,
                competingTarget,
                outputPath,
                cancellationToken);

            Assert.False(first.ReusedExistingArtifact);
            Assert.True(retry.ReusedExistingArtifact);
            Assert.Equal(first.ArtifactDigest, retry.ArtifactDigest);
            Assert.Single(Directory.EnumerateFiles(root));
        }
        finally
        {
            releaseFirst.Set();
            if (firstTask is not null)
            {
                try
                {
                    await firstTask;
                }
                catch
                {
                }
            }
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task WriteAsync_PreservesADifferentExistingArtifact()
    {
        string root = CreateRoot();
        try
        {
            ArtifactFixture fixture = await CreateFixtureAsync(
            [
                new OutcomeSpec(
                    AcceptedRows: 0,
                    RejectedValues: ["source-private-value"]),
            ]);
            string outputPath = Path.Combine(root, "rejects.jsonl");
            var target = new ArtifactTarget(
                new ArtifactSnapshot(fixture.Receipts, fixture.Ledger));
            await WriteAsync(
                fixture,
                target,
                outputPath,
                TestContext.Current.CancellationToken);
            byte[] different = await File.ReadAllBytesAsync(
                outputPath,
                TestContext.Current.CancellationToken);
            different[0] ^= 0x01;
            await File.WriteAllBytesAsync(
                outputPath,
                different,
                TestContext.Current.CancellationToken);

            IOException error = await Assert.ThrowsAsync<IOException>(async () =>
                await WriteAsync(
                    fixture,
                    target,
                    outputPath,
                    TestContext.Current.CancellationToken));

            Assert.Equal(
                "The reject artifact destination already contains a different file.",
                error.Message);
            Assert.Equal(different, await File.ReadAllBytesAsync(
                outputPath,
                TestContext.Current.CancellationToken));
            Assert.Single(Directory.EnumerateFiles(root));
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Theory]
    [InlineData("reordered")]
    [InlineData("tampered-count")]
    [InlineData("wrong-target")]
    [InlineData("wrong-plan")]
    [InlineData("wrong-catalog")]
    [InlineData("wrong-source")]
    [InlineData("wrong-snapshot")]
    [InlineData("wrong-cursor")]
    [InlineData("nonterminal")]
    [InlineData("wrong-batch-ordinal")]
    [InlineData("wrong-reject-digest")]
    [InlineData("wrong-rule")]
    [InlineData("wrong-column")]
    [InlineData("missing-ledger")]
    [InlineData("extra-ledger")]
    [InlineData("orphan-ledger")]
    public async Task WriteAsync_RejectsInvalidLedgerEvidenceWithFixedPrivacyError(
        string scenario)
    {
        string root = CreateRoot();
        try
        {
            ArtifactFixture fixture = await CreateFixtureAsync(
            [
                new OutcomeSpec(
                    AcceptedRows: 0,
                    RejectedValues:
                    [
                        "first-private-value",
                        "second-private-value",
                    ]),
            ]);
            List<MigrationBatchReceipt> invalidReceipts = fixture.Receipts.ToList();
            List<MigrationRejectLedgerEntry> invalidLedger = fixture.Ledger.ToList();
            switch (scenario)
            {
                case "reordered":
                    invalidLedger.Reverse();
                    break;
                case "tampered-count":
                    invalidLedger[0] = invalidLedger[0] with
                    {
                        RawValueByteCount = checked(invalidLedger[0].RawValueByteCount + 1),
                    };
                    break;
                case "wrong-target":
                    invalidReceipts[0] = invalidReceipts[0] with
                    {
                        TargetIdentity = "target:other",
                    };
                    break;
                case "wrong-plan":
                    invalidReceipts[0] = invalidReceipts[0] with
                    {
                        PlanDigest = new string('0', 64),
                    };
                    break;
                case "wrong-catalog":
                    invalidReceipts[0] = invalidReceipts[0] with
                    {
                        CatalogDigest = new string('0', 64),
                    };
                    break;
                case "wrong-source":
                    invalidReceipts[0] = invalidReceipts[0] with
                    {
                        SourceFingerprint = "source:other",
                    };
                    break;
                case "wrong-snapshot":
                    invalidReceipts[0] = invalidReceipts[0] with
                    {
                        SourceSnapshotIdentity = "source:snapshot:other",
                    };
                    break;
                case "wrong-cursor":
                    invalidReceipts[0] = invalidReceipts[0] with
                    {
                        StartCursor = "cursor:unexpected",
                    };
                    break;
                case "nonterminal":
                    invalidReceipts[0] = invalidReceipts[0] with
                    {
                        NextCursor = "cursor:unexpected",
                    };
                    break;
                case "wrong-batch-ordinal":
                    invalidReceipts[0] = invalidReceipts[0] with
                    {
                        BatchOrdinal = 1,
                    };
                    break;
                case "wrong-reject-digest":
                    invalidReceipts[0] = invalidReceipts[0] with
                    {
                        RejectDigest = new string('0', 64),
                    };
                    break;
                case "wrong-rule":
                    invalidLedger[0] = invalidLedger[0] with
                    {
                        RejectedRow = invalidLedger[0].RejectedRow with
                        {
                            RuleId = "MIG-OTHER-001",
                        },
                    };
                    break;
                case "wrong-column":
                    invalidLedger[0] = invalidLedger[0] with
                    {
                        RejectedRow = invalidLedger[0].RejectedRow with
                        {
                            ColumnObjectId = "syn:column:unknown",
                        },
                    };
                    break;
                case "missing-ledger":
                    invalidLedger.RemoveAt(invalidLedger.Count - 1);
                    break;
                case "extra-ledger":
                    invalidLedger.Add(invalidLedger[^1]);
                    break;
                case "orphan-ledger":
                    invalidReceipts.Clear();
                    break;
                default:
                    throw new InvalidOperationException($"Unknown tamper scenario '{scenario}'.");
            }

            string outputPath = Path.Combine(root, "rejects.jsonl");
            var target = new ArtifactTarget(
                new ArtifactSnapshot(invalidReceipts, invalidLedger));

            InvalidDataException error =
                await Assert.ThrowsAsync<InvalidDataException>(async () =>
                    await WriteAsync(
                        fixture,
                        target,
                        outputPath,
                        TestContext.Current.CancellationToken));

            Assert.Equal(EvidenceFailureMessage, error.Message);
            Assert.DoesNotContain("first-private-value", error.ToString(), StringComparison.Ordinal);
            Assert.DoesNotContain("second-private-value", error.ToString(), StringComparison.Ordinal);
            Assert.False(File.Exists(outputPath));
            Assert.Empty(Directory.EnumerateFiles(root));
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task WriteAsync_CancellationRemovesOwnedTemporaryArtifact()
    {
        string root = CreateRoot();
        try
        {
            ArtifactFixture fixture = await CreateFixtureAsync(
            [
                new OutcomeSpec(
                    AcceptedRows: 0,
                    RejectedValues: ["source-private-value"]),
            ]);
            using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(
                TestContext.Current.CancellationToken);
            var snapshot = new ArtifactSnapshot(
                fixture.Receipts,
                fixture.Ledger,
                beforeReceiptYield: cancellation.Cancel);
            var target = new ArtifactTarget(snapshot);
            string outputPath = Path.Combine(root, "rejects.jsonl");

            OperationCanceledException error =
                await Assert.ThrowsAsync<OperationCanceledException>(async () =>
                    await WriteAsync(
                        fixture,
                        target,
                        outputPath,
                        cancellation.Token));

            Assert.Equal(cancellation.Token, error.CancellationToken);
            Assert.False(File.Exists(outputPath));
            Assert.Empty(Directory.EnumerateFiles(root));
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task WriteAsync_StreamsEachReceiptBeforeItsLedgerEntries()
    {
        string root = CreateRoot();
        try
        {
            ArtifactFixture fixture = await CreateFixtureAsync(
            [
                new OutcomeSpec(AcceptedRows: 0, RejectedValues: ["private-first"]),
                new OutcomeSpec(AcceptedRows: 0, RejectedValues: ["private-second"]),
            ]);
            var sequence = new List<string>();
            var target = new ArtifactTarget(new ArtifactSnapshot(
                fixture.Receipts,
                fixture.Ledger,
                beforeReceiptYield: () => sequence.Add("receipt"),
                beforeLedgerYield: () => sequence.Add("ledger")));

            MigrationRejectArtifactWriteResult result = await WriteAsync(
                fixture,
                target,
                Path.Combine(root, "rejects.jsonl"),
                TestContext.Current.CancellationToken);

            Assert.Equal(["receipt", "ledger", "receipt", "ledger"], sequence);
            Assert.Equal(2, result.RejectedRowCount);
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task WriteAsync_StreamsLargeLedgerWithinPlanBounds()
    {
        const int rejectedRows = 2_048;
        const int rejectedRowsPerBatch = 4;
        string root = CreateRoot();
        try
        {
            OutcomeSpec[] outcomes = Enumerable.Range(
                    0,
                    rejectedRows / rejectedRowsPerBatch)
                .Select(batch => new OutcomeSpec(
                    AcceptedRows: 0,
                    RejectedValues: Enumerable.Range(0, rejectedRowsPerBatch)
                        .Select(index => $"private-{batch:D4}-{index}")
                        .ToArray()))
                .ToArray();
            ArtifactFixture fixture = await CreateFixtureAsync(
                outcomes,
                maximumArtifactBytes: 4L * 1024 * 1024,
                maximumRejectedRowsPerRun: rejectedRows,
                maximumRawValueBytesPerRun: 1024 * 1024);
            int receiptsRead = 0;
            int ledgerEntriesRead = 0;
            bool readAheadDetected = false;
            var target = new ArtifactTarget(new ArtifactSnapshot(
                fixture.Receipts,
                fixture.Ledger,
                beforeReceiptYield: () =>
                {
                    int receipt = Interlocked.Increment(ref receiptsRead);
                    if (Volatile.Read(ref ledgerEntriesRead) !=
                        (receipt - 1) * rejectedRowsPerBatch)
                    {
                        readAheadDetected = true;
                    }
                },
                beforeLedgerYield: () => Interlocked.Increment(ref ledgerEntriesRead)));

            MigrationRejectArtifactWriteResult result = await WriteAsync(
                fixture,
                target,
                Path.Combine(root, "rejects.jsonl"),
                TestContext.Current.CancellationToken);

            Assert.False(readAheadDetected);
            Assert.Equal(outcomes.Length, receiptsRead);
            Assert.Equal(rejectedRows, ledgerEntriesRead);
            Assert.Equal(rejectedRows, result.RejectedRowCount);
            Assert.InRange(result.ArtifactBytes, 1, 4L * 1024 * 1024);
            Assert.Single(Directory.EnumerateFiles(root));
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task WriteAsync_FreezesProviderEvidenceBeforeAccountingAndDigesting()
    {
        string root = CreateRoot();
        try
        {
            ArtifactFixture fixture = await CreateFixtureAsync(
            [
                new OutcomeSpec(
                    AcceptedRows: 0,
                    RejectedValues: ["source-private-value"]),
            ]);
            MigrationRejectLedgerEntry original = Assert.Single(fixture.Ledger);
            MigrationRejectEvidence originalEvidence = Assert.Single(
                original.RejectedRow.Evidence);
            var volatileEvidence = new VolatileEvidenceList(
                originalEvidence,
                new MigrationRejectEvidence
                {
                    Name = originalEvidence.Name,
                    Value = "different-private-value",
                });
            MigrationRejectLedgerEntry volatileEntry = original with
            {
                RejectedRow = original.RejectedRow with { Evidence = volatileEvidence },
            };
            var target = new ArtifactTarget(new ArtifactSnapshot(
                fixture.Receipts,
                [volatileEntry]));
            string outputPath = Path.Combine(root, "rejects.jsonl");

            _ = await WriteAsync(
                fixture,
                target,
                outputPath,
                TestContext.Current.CancellationToken);

            Assert.Equal(1, volatileEvidence.IndexReads);
            Assert.Equal(0, volatileEvidence.EnumerationCount);
            string artifact = await File.ReadAllTextAsync(
                outputPath,
                TestContext.Current.CancellationToken);
            Assert.Contains("source-private-value", artifact, StringComparison.Ordinal);
            Assert.DoesNotContain("different-private-value", artifact, StringComparison.Ordinal);
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task WriteAsync_RegeneratesCustomMappingPlanWithoutOriginalPlugin()
    {
        string root = CreateRoot();
        try
        {
            ArtifactFixture fixture = await CreateFixtureAsync(
            [
                new OutcomeSpec(
                    AcceptedRows: 0,
                    RejectedValues: ["source-private-value"]),
            ],
            mappingProvider: new CustomMappingProvider());
            var target = new ArtifactTarget(
                new ArtifactSnapshot(fixture.Receipts, fixture.Ledger));

            MigrationRejectArtifactWriteResult result = await WriteAsync(
                fixture,
                target,
                Path.Combine(root, "rejects.jsonl"),
                TestContext.Current.CancellationToken);

            Assert.Equal(CustomMappingProvider.CustomPolicyId, fixture.Plan.MappingPolicyId);
            Assert.Equal(fixture.PlanDigest, result.PlanDigest);
            Assert.Equal(1, result.RejectedRowCount);
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task WriteAsync_UsesArtifactSpecificPolicyCodeForFailFastPlan()
    {
        string root = CreateRoot();
        try
        {
            ArtifactFixture fixture = await CreateFixtureAsync([]);
            MigrationPlan failFastPlan = fixture.Plan with
            {
                Load = fixture.Plan.Load with
                {
                    RejectMode = MigrationRejectMode.FailFast,
                    RejectPolicy = null,
                },
            };
            ArtifactFixture failFastFixture = fixture with
            {
                Plan = failFastPlan,
                PlanDigest = MigrationArtifactSerializer.ComputePlanDigest(failFastPlan),
            };

            MigrationExecutionPolicyException error =
                await Assert.ThrowsAsync<MigrationExecutionPolicyException>(async () =>
                    await WriteAsync(
                        failFastFixture,
                        new ArtifactTarget(new ArtifactSnapshot([], [])),
                        Path.Combine(root, "rejects.jsonl"),
                        TestContext.Current.CancellationToken));

            Assert.Equal(MigrationRejectArtifactWriter.UnsupportedRejectModeCode, error.Code);
            Assert.Empty(Directory.EnumerateFiles(root));
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Theory]
    [InlineData("null-object")]
    [InlineData("duplicate-mapping")]
    public async Task WriteAsync_NormalizesMalformedRecordedMappingFailures(string scenario)
    {
        string root = CreateRoot();
        try
        {
            ArtifactFixture fixture = await CreateFixtureAsync([]);
            MigrationPlan invalidPlan;
            if (scenario == "null-object")
            {
                invalidPlan = fixture.Plan with { Objects = [null!] };
            }
            else
            {
                MigrationPlanObject mappedObject = fixture.Plan.Objects.First(item =>
                    item.TypeMappings.Count > 0);
                MigrationTypeMapping mapping = mappedObject.TypeMappings[0];
                invalidPlan = fixture.Plan with
                {
                    Objects = fixture.Plan.Objects.Select(item =>
                        ReferenceEquals(item, mappedObject)
                            ? item with { TypeMappings = [mapping, mapping] }
                            : item).ToArray(),
                };
            }

            InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(async () =>
                await new MigrationRejectArtifactWriter().WriteAsync(
                    new MigrationRejectArtifactWriteRequest
                    {
                        Plan = invalidPlan,
                        Catalog = fixture.Catalog,
                        Target = new ArtifactTarget(new ArtifactSnapshot([], [])),
                        OutputPath = Path.Combine(root, "rejects.jsonl"),
                    },
                    TestContext.Current.CancellationToken));

            Assert.Equal("The migration plan contains invalid recorded type mappings.", error.Message);
            Assert.Empty(Directory.EnumerateFiles(root));
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task WriteAsync_RejectsNonEstablishedTargetSnapshot()
    {
        string root = CreateRoot();
        try
        {
            ArtifactFixture fixture = await CreateFixtureAsync([]);
            var snapshot = new ArtifactSnapshot(
                [],
                [],
                consistencyStatus: MigrationSnapshotConsistencyStatus.NotEstablished);
            var target = new ArtifactTarget(snapshot);
            string outputPath = Path.Combine(root, "rejects.jsonl");

            InvalidDataException error =
                await Assert.ThrowsAsync<InvalidDataException>(async () =>
                    await WriteAsync(
                        fixture,
                        target,
                        outputPath,
                        TestContext.Current.CancellationToken));

            Assert.Equal(EvidenceFailureMessage, error.Message);
            Assert.False(File.Exists(outputPath));
            Assert.Empty(Directory.EnumerateFiles(root));
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task WriteAsync_SanitizesMaliciousProviderCancellation()
    {
        string root = CreateRoot();
        try
        {
            const string privateMessage = "source-private-cancellation-value";
            ArtifactFixture fixture = await CreateFixtureAsync(
            [
                new OutcomeSpec(
                    AcceptedRows: 0,
                    RejectedValues: ["source-private-value"]),
            ]);
            var snapshot = new ArtifactSnapshot(
                fixture.Receipts,
                fixture.Ledger,
                beforeReceiptYield: () => throw new OperationCanceledException(privateMessage));
            var target = new ArtifactTarget(snapshot);
            string outputPath = Path.Combine(root, "rejects.jsonl");

            InvalidDataException error =
                await Assert.ThrowsAsync<InvalidDataException>(async () =>
                    await WriteAsync(
                        fixture,
                        target,
                        outputPath,
                        TestContext.Current.CancellationToken));

            Assert.Equal(EvidenceFailureMessage, error.Message);
            Assert.DoesNotContain(privateMessage, error.ToString(), StringComparison.Ordinal);
            Assert.False(File.Exists(outputPath));
            Assert.Empty(Directory.EnumerateFiles(root));
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task WriteAsync_RejectsRelativeAndTraversalPaths()
    {
        string root = CreateRoot();
        try
        {
            ArtifactFixture fixture = await CreateFixtureAsync([]);
            var target = new ArtifactTarget(new ArtifactSnapshot([], []));

            ArgumentException relative = await Assert.ThrowsAsync<ArgumentException>(async () =>
                await WriteAsync(
                    fixture,
                    target,
                    "rejects.jsonl",
                    TestContext.Current.CancellationToken));
            string traversalPath = Path.Combine(
                root,
                "child",
                "..",
                "rejects.jsonl");
            ArgumentException traversal = await Assert.ThrowsAsync<ArgumentException>(async () =>
                await WriteAsync(
                    fixture,
                    target,
                    traversalPath,
                    TestContext.Current.CancellationToken));

            Assert.Contains("fully qualified", relative.Message, StringComparison.Ordinal);
            Assert.Contains("traversal", traversal.Message, StringComparison.Ordinal);
            Assert.Empty(Directory.EnumerateFiles(root));
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task WriteAsync_RejectsInvalidUnicodePathWithoutPublishingAnAlias()
    {
        string root = CreateRoot();
        try
        {
            ArtifactFixture fixture = await CreateFixtureAsync([]);
            var target = new ArtifactTarget(new ArtifactSnapshot([], []));
            string outputPath = Path.Combine(root, "reject-\ud800.jsonl");

            ArgumentException error = await Assert.ThrowsAsync<ArgumentException>(async () =>
                await WriteAsync(
                    fixture,
                    target,
                    outputPath,
                    TestContext.Current.CancellationToken));

            Assert.Contains("Unicode scalar", error.Message, StringComparison.Ordinal);
            Assert.Empty(Directory.EnumerateFiles(root));
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Theory]
    [InlineData("rejects.jsonl.")]
    [InlineData("rejects.jsonl ")]
    [InlineData("CON.jsonl")]
    [InlineData("NUL.tar.gz")]
    [InlineData("COM1.data.log")]
    [InlineData("LPT\u00b9.log")]
    public async Task WriteAsync_RejectsWindowsPathAliases(string fileName)
    {
        if (!OperatingSystem.IsWindows())
            return;

        string root = CreateRoot();
        try
        {
            ArtifactFixture fixture = await CreateFixtureAsync([]);
            var target = new ArtifactTarget(new ArtifactSnapshot([], []));

            _ = await Assert.ThrowsAsync<ArgumentException>(async () =>
                await WriteAsync(
                    fixture,
                    target,
                    Path.Combine(root, fileName),
                    TestContext.Current.CancellationToken));

            Assert.Empty(Directory.EnumerateFiles(root));
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task WriteAsync_RejectsSymlinkedParentWhenSupported()
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

            ArtifactFixture fixture = await CreateFixtureAsync([]);
            var target = new ArtifactTarget(new ArtifactSnapshot([], []));
            string outputPath = Path.Combine(linkedParent, "rejects.jsonl");

            InvalidDataException error =
                await Assert.ThrowsAsync<InvalidDataException>(async () =>
                    await WriteAsync(
                        fixture,
                        target,
                        outputPath,
                        TestContext.Current.CancellationToken));

            Assert.Contains("cannot traverse a link", error.Message, StringComparison.Ordinal);
            Assert.Empty(Directory.EnumerateFiles(actualParent));
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task WriteAsync_EnforcesExactCanonicalArtifactByteLimit()
    {
        string root = CreateRoot();
        try
        {
            OutcomeSpec[] outcomes =
            [
                new OutcomeSpec(
                    AcceptedRows: 0,
                    RejectedValues: ["private-value-with-json-escape\nline"]),
            ];
            ArtifactFixture baseline = await CreateFixtureAsync(outcomes);
            long exactBytes = MigrationRejectLedgerCodec.GetArtifactHeaderByteCount(
                    baseline.PlanDigest) +
                baseline.Ledger.Sum(entry =>
                    MigrationRejectLedgerCodec.GetCanonicalArtifactEntryByteCount(
                        entry.SourceObjectId,
                        entry.BatchOrdinal,
                        entry.RejectedRow));

            ArtifactFixture exact = await CreateFixtureAsync(outcomes, exactBytes);
            string exactPath = Path.Combine(root, "exact.jsonl");
            MigrationRejectArtifactWriteResult result = await WriteAsync(
                exact,
                new ArtifactTarget(new ArtifactSnapshot(exact.Receipts, exact.Ledger)),
                exactPath,
                TestContext.Current.CancellationToken);
            Assert.Equal(exactBytes, result.ArtifactBytes);
            Assert.Equal(exactBytes, new FileInfo(exactPath).Length);

            ArtifactFixture tooSmall = await CreateFixtureAsync(
                outcomes,
                exactBytes - 1);
            string tooSmallPath = Path.Combine(root, "too-small.jsonl");
            InvalidDataException error =
                await Assert.ThrowsAsync<InvalidDataException>(async () =>
                    await WriteAsync(
                        tooSmall,
                        new ArtifactTarget(new ArtifactSnapshot(
                            tooSmall.Receipts,
                            tooSmall.Ledger)),
                        tooSmallPath,
                        TestContext.Current.CancellationToken));
            Assert.Equal(EvidenceFailureMessage, error.Message);
            Assert.False(File.Exists(tooSmallPath));
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task WriteAsync_ReclaimsPrivateStaleTemporaryArtifact()
    {
        string root = CreateRoot();
        try
        {
            ArtifactFixture fixture = await CreateFixtureAsync(
            [
                new OutcomeSpec(
                    AcceptedRows: 0,
                    RejectedValues: ["current-private-value"]),
            ]);
            string outputPath = Path.Combine(root, "rejects.jsonl");
            string claimBinding = fixture.PlanDigest + "\0" + Path.GetFileName(outputPath);
            string claimDigest = Convert.ToHexString(
                    SHA256.HashData(Encoding.UTF8.GetBytes(claimBinding)))
                .ToLowerInvariant();
            string temporaryPath = Path.Combine(
                root,
                $".csharpdb-reject-{claimDigest[..32]}.tmp");
            await using (FileStream stale = CreatePrivateStaleFile(temporaryPath))
            {
                await stale.WriteAsync(
                    Encoding.UTF8.GetBytes("stale-private-value"),
                    TestContext.Current.CancellationToken);
                stale.Flush(flushToDisk: true);
            }

            MigrationRejectArtifactWriteResult result = await WriteAsync(
                fixture,
                new ArtifactTarget(new ArtifactSnapshot(
                    fixture.Receipts,
                    fixture.Ledger)),
                outputPath,
                TestContext.Current.CancellationToken);

            Assert.False(result.ReusedExistingArtifact);
            Assert.True(File.Exists(outputPath));
            Assert.False(File.Exists(temporaryPath));
            Assert.DoesNotContain(
                "stale-private-value",
                await File.ReadAllTextAsync(
                    outputPath,
                    TestContext.Current.CancellationToken),
                StringComparison.Ordinal);
            Assert.Single(Directory.EnumerateFiles(root));
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task WriteAsync_RemovesMacOsAclFromPrivateStaleTemporaryArtifact()
    {
        if (!OperatingSystem.IsMacOS())
            return;

        string root = CreateRoot();
        try
        {
            ArtifactFixture fixture = await CreateFixtureAsync(
            [
                new OutcomeSpec(
                    AcceptedRows: 0,
                    RejectedValues: ["current-private-value"]),
            ]);
            string outputPath = Path.Combine(root, "rejects.jsonl");
            string claimBinding = fixture.PlanDigest + "\0" + Path.GetFileName(outputPath);
            string claimDigest = Convert.ToHexString(
                    SHA256.HashData(Encoding.UTF8.GetBytes(claimBinding)))
                .ToLowerInvariant();
            string temporaryPath = Path.Combine(
                root,
                $".csharpdb-reject-{claimDigest[..32]}.tmp");
            await using (FileStream stale = CreatePrivateStaleFile(temporaryPath))
            {
                await stale.WriteAsync(
                    Encoding.UTF8.GetBytes("stale-private-value"),
                    TestContext.Current.CancellationToken);
                stale.Flush(flushToDisk: true);
            }
            await AddMacOsAclAsync(
                temporaryPath,
                TestContext.Current.CancellationToken);

            MigrationRejectArtifactWriteResult result = await WriteAsync(
                fixture,
                new ArtifactTarget(new ArtifactSnapshot(
                    fixture.Receipts,
                    fixture.Ledger)),
                outputPath,
                TestContext.Current.CancellationToken);

            Assert.False(result.ReusedExistingArtifact);
            Assert.True(File.Exists(outputPath));
            Assert.False(File.Exists(temporaryPath));
            Assert.Single(Directory.EnumerateFiles(root));
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task WriteAsync_RejectsMacOsAclOnExistingArtifact()
    {
        if (!OperatingSystem.IsMacOS())
            return;

        string root = CreateRoot();
        try
        {
            ArtifactFixture fixture = await CreateFixtureAsync([]);
            string outputPath = Path.Combine(root, "rejects.jsonl");
            var target = new ArtifactTarget(new ArtifactSnapshot([], []));
            _ = await WriteAsync(
                fixture,
                target,
                outputPath,
                TestContext.Current.CancellationToken);
            await AddMacOsAclAsync(
                outputPath,
                TestContext.Current.CancellationToken);

            InvalidDataException error =
                await Assert.ThrowsAsync<InvalidDataException>(async () =>
                    await WriteAsync(
                        fixture,
                        target,
                        outputPath,
                        TestContext.Current.CancellationToken));

            Assert.Contains(
                "extended access policy",
                error.Message,
                StringComparison.Ordinal);
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task WriteAsync_RefusesHardLinkedTemporaryWithoutTruncatingVictim()
    {
        string root = CreateRoot();
        try
        {
            ArtifactFixture fixture = await CreateFixtureAsync([]);
            string outputPath = Path.Combine(root, "rejects.jsonl");
            string claimBinding = fixture.PlanDigest + "\0" + Path.GetFileName(outputPath);
            string claimDigest = Convert.ToHexString(
                    SHA256.HashData(Encoding.UTF8.GetBytes(claimBinding)))
                .ToLowerInvariant();
            string temporaryPath = Path.Combine(
                root,
                $".csharpdb-reject-{claimDigest[..32]}.tmp");
            string victimPath = Path.Combine(root, "private-victim.txt");
            byte[] victimBytes = Encoding.UTF8.GetBytes("must-not-be-truncated");
            await using (FileStream victim = CreatePrivateStaleFile(victimPath))
            {
                await victim.WriteAsync(victimBytes, TestContext.Current.CancellationToken);
                victim.Flush(flushToDisk: true);
            }
            if (!TryCreateHardLink(temporaryPath, victimPath))
                return;

            InvalidDataException error =
                await Assert.ThrowsAsync<InvalidDataException>(async () =>
                    await WriteAsync(
                        fixture,
                        new ArtifactTarget(new ArtifactSnapshot([], [])),
                        outputPath,
                        TestContext.Current.CancellationToken));

            Assert.Contains("link", error.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(victimBytes, await File.ReadAllBytesAsync(
                victimPath,
                TestContext.Current.CancellationToken));
            Assert.Equal(victimBytes, await File.ReadAllBytesAsync(
                temporaryPath,
                TestContext.Current.CancellationToken));
            Assert.False(File.Exists(outputPath));
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    private static async Task<ArtifactFixture> CreateFixtureAsync(
        IReadOnlyList<OutcomeSpec> outcomes,
        long? maximumArtifactBytes = null,
        IDataTypeMappingProvider? mappingProvider = null,
        long? maximumRejectedRowsPerRun = null,
        long? maximumRawValueBytesPerRun = null)
    {
        MigrationCatalog catalog = await new SyntheticMigrationSourceInspector().InspectAsync(
            new MigrationInspectionRequest
            {
                TargetCSharpDbVersion = CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                IncludeProfile = true,
                ProfileSampleSize = 5,
            },
            TestContext.Current.CancellationToken);
        MigrationPlanner planner = mappingProvider is null
            ? new MigrationPlanner()
            : new MigrationPlanner(typeMapper: mappingProvider);
        MigrationPlan planned = planner.CreatePlan(catalog);
        MigrationPlan plan = planned with
        {
            AcceptedExclusionObjectIds = planned.Objects
                .Where(item => !item.Included)
                .Select(item => item.SourceObjectId)
                .OrderBy(item => item, StringComparer.Ordinal)
                .ToArray(),
            Load = planned.Load with
            {
                BatchSize = 4,
                RejectMode = MigrationRejectMode.DeterministicRejects,
                RejectPolicy = new MigrationDeterministicRejectPolicy
                {
                    ContractVersion = MigrationRejectContract.DeterministicRejectsV1,
                    AllowedRuleIds = [RuleId],
                    MaxRejectedRowsPerBatch = 4,
                    MaxRejectedRowsPerRun = maximumRejectedRowsPerRun ?? 16,
                    MaxRawValueBytes = 1_024,
                    MaxRawValueBytesPerBatch = 4_096,
                    MaxRawValueBytesPerRun = maximumRawValueBytesPerRun ?? 16_384,
                    MaxArtifactBytes = maximumArtifactBytes ?? 131_072,
                },
            },
        };
        string planDigest = MigrationArtifactSerializer.ComputePlanDigest(plan);
        Projection projection = catalog.Objects
            .Where(item => item.Kind is MigrationObjectKind.Table or MigrationObjectKind.Collection)
            .Where(item => plan.Objects.Single(plannedObject =>
                string.Equals(
                    plannedObject.SourceObjectId,
                    item.ObjectId,
                    StringComparison.Ordinal)).Included)
            .OrderBy(item => item.ObjectId, StringComparer.Ordinal)
            .Select(item => new Projection(
                item.ObjectId,
                catalog.Objects
                    .Where(column =>
                        column.Kind == MigrationObjectKind.Column &&
                        string.Equals(
                            column.ParentObjectId,
                            item.ObjectId,
                            StringComparison.Ordinal) &&
                        plan.Objects.Single(plannedObject =>
                            string.Equals(
                                plannedObject.SourceObjectId,
                                column.ObjectId,
                                StringComparison.Ordinal)).Included)
                    .OrderBy(column => column.ObjectId, StringComparer.Ordinal)
                    .Select(column => column.ObjectId)
                    .ToArray()))
            .First();
        Assert.NotEmpty(projection.ColumnObjectIds);

        var batches = new List<MigrationTargetBatch>();
        string? startCursor = null;
        for (int batchOrdinal = 0; batchOrdinal < outcomes.Count; batchOrdinal++)
        {
            OutcomeSpec outcome = outcomes[batchOrdinal];
            long firstRowOrdinal = batches.Sum(batch =>
                batch.Rows.Count + batch.RejectedRows.Count);
            MigrationTargetRow[] rows = Enumerable.Range(0, outcome.AcceptedRows)
                .Select(index => new MigrationTargetRow
                {
                    SourceRowOrdinal = checked(firstRowOrdinal + index),
                    StableKey = $"stable:{firstRowOrdinal + index}",
                    Values = projection.ColumnObjectIds.Select(_ => DbValue.Null).ToArray(),
                })
                .ToArray();
            MigrationRejectedRow[] rejectedRows = outcome.RejectedValues
                .Select((value, index) => new MigrationRejectedRow
                {
                    SourceRowOrdinal = checked(firstRowOrdinal + rows.Length + index),
                    RuleId = RuleId,
                    ColumnObjectId = projection.ColumnObjectIds[0],
                    Evidence =
                    [
                        new MigrationRejectEvidence
                        {
                            Name = MigrationRejectLedgerCodec.RawValueEvidenceName,
                            Value = value,
                        },
                    ],
                })
                .ToArray();
            string? nextCursor = batchOrdinal == outcomes.Count - 1
                ? null
                : $"cursor:{batchOrdinal + 1}";
            var unsigned = new MigrationTargetBatch
            {
                PlanDigest = planDigest,
                CatalogDigest = plan.CatalogDigest,
                SourceFingerprint = plan.Source.Fingerprint,
                SourceSnapshotIdentity = SourceSnapshotIdentity,
                SourceObjectId = projection.SourceObjectId,
                ColumnObjectIds = projection.ColumnObjectIds,
                BatchOrdinal = batchOrdinal,
                StartCursor = startCursor,
                NextCursor = nextCursor,
                BatchDigest = string.Empty,
                RejectContractVersion = MigrationRejectContract.DeterministicRejectsV1,
                RejectDigest = string.Empty,
                Rows = rows,
                RejectedRows = rejectedRows,
            };
            MigrationTargetBatch rejectSealed = unsigned with
            {
                RejectDigest = MigrationRejectDigest.Compute(unsigned),
            };
            batches.Add(rejectSealed with
            {
                BatchDigest = MigrationBatchDigest.Compute(rejectSealed),
            });
            startCursor = nextCursor;
        }

        MigrationBatchReceipt[] receipts = batches.Select(batch =>
            new MigrationBatchReceipt
            {
                TargetIdentity = ExpectedTargetIdentity,
                PlanDigest = batch.PlanDigest,
                CatalogDigest = batch.CatalogDigest,
                SourceFingerprint = batch.SourceFingerprint,
                SourceSnapshotIdentity = batch.SourceSnapshotIdentity,
                SourceObjectId = batch.SourceObjectId,
                BatchOrdinal = batch.BatchOrdinal,
                StartCursor = batch.StartCursor,
                NextCursor = batch.NextCursor,
                BatchDigest = batch.BatchDigest,
                RejectContractVersion = batch.RejectContractVersion,
                RejectDigest = batch.RejectDigest,
                RowCount = batch.Rows.Count,
                RejectedRowCount = batch.RejectedRows.Count,
            }).ToArray();
        MigrationRejectLedgerEntry[] ledger = batches
            .SelectMany(batch => batch.RejectedRows.Select(rejectedRow =>
                new MigrationRejectLedgerEntry
                {
                    PlanDigest = planDigest,
                    SourceObjectId = batch.SourceObjectId,
                    BatchOrdinal = batch.BatchOrdinal,
                    RejectedRow = rejectedRow,
                    RawValueByteCount =
                        MigrationRejectLedgerCodec.GetRawValueByteCount(rejectedRow),
                    CanonicalEntryByteCount =
                        MigrationRejectLedgerCodec.GetCanonicalEntryByteCount(
                            batch.SourceObjectId,
                            batch.BatchOrdinal,
                            rejectedRow),
                }))
            .ToArray();
        return new ArtifactFixture(catalog, plan, planDigest, receipts, ledger);
    }

    private static async Task<MigrationRejectArtifactWriteResult> WriteAsync(
        ArtifactFixture fixture,
        IMigrationTarget target,
        string outputPath,
        CancellationToken cancellationToken) =>
        await new MigrationRejectArtifactWriter().WriteAsync(
            new MigrationRejectArtifactWriteRequest
            {
                Plan = fixture.Plan,
                Catalog = fixture.Catalog,
                Target = target,
                OutputPath = outputPath,
            },
            cancellationToken);

    private static string Sha256(byte[] value) =>
        Convert.ToHexString(SHA256.HashData(value)).ToLowerInvariant();

    private static string CreateRoot()
    {
        string root = Path.GetFullPath(Path.Combine(
            Path.GetTempPath(),
            $"csharpdb-reject-artifact-tests-{Guid.NewGuid():N}"));
        Directory.CreateDirectory(root);
        return root;
    }

    private static FileStream CreatePrivateStaleFile(string path)
    {
        if (OperatingSystem.IsWindows())
            return CreatePrivateWindowsFile(path);

        return new FileStream(path, new FileStreamOptions
        {
            Mode = FileMode.CreateNew,
            Access = FileAccess.ReadWrite,
            Share = FileShare.None,
            BufferSize = 4_096,
            Options = FileOptions.Asynchronous | FileOptions.WriteThrough,
            UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite,
        });
    }

    private static async Task AddMacOsAclAsync(
        string path,
        CancellationToken cancellationToken)
    {
        var startInfo = new ProcessStartInfo("/bin/chmod")
        {
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        startInfo.ArgumentList.Add("+a");
        startInfo.ArgumentList.Add("everyone allow read");
        startInfo.ArgumentList.Add(path);
        using Process process = Process.Start(startInfo) ??
            throw new InvalidOperationException("The macOS chmod process could not be started.");
        string standardError = await process.StandardError
            .ReadToEndAsync(cancellationToken);
        await process.WaitForExitAsync(cancellationToken);
        Assert.True(
            process.ExitCode == 0,
            $"macOS chmod failed with exit code {process.ExitCode}: {standardError}");
    }

    private static bool TryCreateHardLink(string linkPath, string existingPath) =>
        OperatingSystem.IsWindows()
            ? CreateHardLinkW(linkPath, existingPath, IntPtr.Zero)
            : UnixCreateHardLink(existingPath, linkPath) == 0;

    [SupportedOSPlatform("windows")]
    private static FileStream CreatePrivateWindowsFile(string path)
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
        return FileSystemAclExtensions.Create(
            new FileInfo(path),
            FileMode.CreateNew,
            FileSystemRights.FullControl,
            FileShare.None,
            4_096,
            FileOptions.Asynchronous | FileOptions.WriteThrough,
            security);
    }

    private static void DeleteRoot(string root)
    {
        if (Directory.Exists(root))
            Directory.Delete(root, recursive: true);
    }

    [DllImport(
        "kernel32.dll",
        EntryPoint = "CreateHardLinkW",
        CharSet = CharSet.Unicode,
        ExactSpelling = true,
        SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool CreateHardLinkW(
        string fileName,
        string existingFileName,
        IntPtr securityAttributes);

    [DllImport("libc", EntryPoint = "link", SetLastError = true)]
    private static extern int UnixCreateHardLink(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string existingPath,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string linkPath);

    [DllImport("libc", EntryPoint = "dup", SetLastError = true)]
    private static extern int UnixDuplicate(int descriptor);

    [DllImport("libc", EntryPoint = "flock", SetLastError = true)]
    private static extern int UnixFlock(SafeFileHandle handle, int operation);

    private const int LockExclusive = 2;
    private const int LockNonBlocking = 4;
    private const int LockUnlock = 8;

    private sealed record OutcomeSpec(
        int AcceptedRows,
        IReadOnlyList<string> RejectedValues);

    private sealed record Projection(
        string SourceObjectId,
        IReadOnlyList<string> ColumnObjectIds);

    private sealed record ArtifactFixture(
        MigrationCatalog Catalog,
        MigrationPlan Plan,
        string PlanDigest,
        IReadOnlyList<MigrationBatchReceipt> Receipts,
        IReadOnlyList<MigrationRejectLedgerEntry> Ledger);

    private sealed class ArtifactSnapshot(
        IReadOnlyList<MigrationBatchReceipt> receipts,
        IReadOnlyList<MigrationRejectLedgerEntry> ledger,
        MigrationSnapshotConsistencyStatus consistencyStatus =
            MigrationSnapshotConsistencyStatus.Established,
        Action? beforeReceiptYield = null,
        Action? beforeLedgerYield = null) :
        IMigrationRejectTargetValidationSnapshot
    {
        public string SnapshotIdentity => TargetSnapshotIdentity;

        public MigrationSnapshotConsistencyStatus ConsistencyStatus => consistencyStatus;

        public async IAsyncEnumerable<MigrationBatchReceipt> ReadOutcomeReceiptsAsync(
            string planDigest,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (MigrationBatchReceipt receipt in receipts)
            {
                beforeReceiptYield?.Invoke();
                cancellationToken.ThrowIfCancellationRequested();
                await Task.Yield();
                yield return receipt;
            }
        }

        public async IAsyncEnumerable<MigrationRejectLedgerEntry> ReadRejectLedgerAsync(
            string planDigest,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (MigrationRejectLedgerEntry entry in ledger)
            {
                beforeLedgerYield?.Invoke();
                cancellationToken.ThrowIfCancellationRequested();
                await Task.Yield();
                yield return entry;
            }
        }

        public ValueTask<MigrationNormalizedSchema> ReadSchemaAsync(
            CancellationToken cancellationToken = default) =>
            ValueTask.FromException<MigrationNormalizedSchema>(new NotSupportedException());

        public ValueTask<long> CountAsync(
            string objectId,
            CancellationToken cancellationToken = default) =>
            ValueTask.FromException<long>(new NotSupportedException());

        public async IAsyncEnumerable<MigrationValidationRow> ReadRowsAsync(
            string objectId,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await Task.Yield();
            yield break;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class CustomMappingProvider : IDataTypeMappingProvider
    {
        internal const string CustomPolicyId = "csharpdb-test-custom-mapping";

        private readonly StandardDataTypeMappingProvider _inner = new();

        public string PolicyId => CustomPolicyId;

        public int PolicyVersion => 7;

        public MigrationTypeMappingDecision Map(MigrationTypeMappingRequest request) =>
            _inner.Map(request);
    }

    private sealed class VolatileEvidenceList(
        MigrationRejectEvidence indexedValue,
        MigrationRejectEvidence enumeratedValue) : IReadOnlyList<MigrationRejectEvidence>
    {
        private int _enumerationCount;
        private int _indexReads;

        public int Count => 1;

        public int EnumerationCount => Volatile.Read(ref _enumerationCount);

        public int IndexReads => Volatile.Read(ref _indexReads);

        public MigrationRejectEvidence this[int index]
        {
            get
            {
                if (index != 0)
                    throw new ArgumentOutOfRangeException(nameof(index));
                Interlocked.Increment(ref _indexReads);
                return indexedValue;
            }
        }

        public IEnumerator<MigrationRejectEvidence> GetEnumerator()
        {
            Interlocked.Increment(ref _enumerationCount);
            yield return enumeratedValue;
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() =>
            GetEnumerator();
    }

    private sealed class ArtifactTarget(ArtifactSnapshot snapshot) :
        IMigrationTarget,
        IMigrationBatchDigestContractTarget
    {
        public string TargetIdentity => ExpectedTargetIdentity;

        public string BatchDigestFormat => MigrationBatchDigest.Format;

        public ValueTask<IValidationSnapshot> OpenValidationSnapshotAsync(
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult<IValidationSnapshot>(snapshot);
        }

        public ValueTask ApplySchemaAsync(
            MigrationPlan plan,
            MigrationCatalog catalog,
            MigrationSchemaStage stage,
            CancellationToken cancellationToken = default) =>
            ValueTask.FromException(new NotSupportedException());

        public ValueTask<MigrationBatchReceipt> WriteBatchAsync(
            MigrationTargetBatch batch,
            CancellationToken cancellationToken = default) =>
            ValueTask.FromException<MigrationBatchReceipt>(new NotSupportedException());

        public ValueTask<MigrationBatchReceipt?> ReadReceiptAsync(
            string planDigest,
            string sourceObjectId,
            long batchOrdinal,
            CancellationToken cancellationToken = default) =>
            ValueTask.FromException<MigrationBatchReceipt?>(new NotSupportedException());

        public async IAsyncEnumerable<MigrationBatchReceipt> ReadReceiptsAsync(
            string planDigest,
            string sourceObjectId,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await Task.Yield();
            yield break;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
