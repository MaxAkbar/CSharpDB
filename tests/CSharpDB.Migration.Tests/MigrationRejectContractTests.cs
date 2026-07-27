using CSharpDB.Migration;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Tests;

public sealed class MigrationRejectContractTests
{
    private const string SecretValue = "TOP-SECRET-ROW-VALUE";

    [Fact]
    public void RejectDigest_EmptySetIsDeterministicLowerSha256AndBatchBound()
    {
        MigrationTargetBatch batch = Unsigned();

        string digest = MigrationRejectDigest.Compute(batch);

        Assert.Equal(digest, MigrationRejectDigest.Compute(batch));
        Assert.Equal(64, digest.Length);
        Assert.All(digest, character =>
            Assert.True(character is >= '0' and <= '9' or >= 'a' and <= 'f'));
        Assert.NotEqual(
            digest,
            MigrationRejectDigest.Compute(batch with { BatchOrdinal = 8 }));
        Assert.NotEqual(
            digest,
            MigrationRejectDigest.Compute(batch with { NextCursor = "cursor:changed" }));
        Assert.NotEqual(
            digest,
            MigrationRejectDigest.Compute(batch with
            {
                RejectContractVersion = MigrationRejectContract.DeterministicRejectsV1,
            }));
    }

    [Fact]
    public void RejectDigest_BindsEveryFieldWithoutUnicodeOrNullNormalization()
    {
        MigrationTargetBatch batch = Unsigned(
            rejectedRows:
            [
                Reject(
                    40,
                    "MIG-CSV-DATA-TYPE-001",
                    "column:value",
                    Evidence("logicalRecordNumber", "41"),
                    Evidence("rawValue", "caf\u00e9")),
            ],
            rejectContractVersion: MigrationRejectContract.DeterministicRejectsV1);
        string digest = MigrationRejectDigest.Compute(batch);

        Assert.NotEqual(
            digest,
            MigrationRejectDigest.Compute(batch with
            {
                RejectedRows =
                [
                    Reject(
                        40,
                        "MIG-CSV-DATA-TYPE-001",
                        "column:value",
                        Evidence("logicalRecordNumber", "41"),
                        Evidence("rawValue", "cafe\u0301")),
                ],
            }));
        Assert.NotEqual(
            digest,
            MigrationRejectDigest.Compute(batch with
            {
                RejectedRows =
                [
                    Reject(
                        40,
                        "MIG-CSV-DATA-TYPE-001",
                        "column:value",
                        Evidence("logicalRecordNumber", "41"),
                        Evidence("rawValue", null)),
                ],
            }));
        Assert.NotEqual(
            MigrationRejectDigest.Compute(batch with
            {
                RejectedRows =
                [
                    Reject(
                        40,
                        "MIG-CSV-DATA-TYPE-001",
                        "column:value",
                        Evidence("logicalRecordNumber", "4"),
                        Evidence("rawValue", "1")),
                ],
            }),
            MigrationRejectDigest.Compute(batch with
            {
                RejectedRows =
                [
                    Reject(
                        40,
                        "MIG-CSV-DATA-TYPE-001",
                        "column:value",
                        Evidence("logicalRecordNumber", "41"),
                        Evidence("rawValue", string.Empty)),
                ],
            }));
    }

    [Fact]
    public void RejectValidation_EnforcesMetadataAndEvidenceBoundsWithoutDisclosingValues()
    {
        string maximumRule = "MIG-" + new string('A',
            MigrationRejectContract.MaximumRuleIdCharacters - 4);
        string maximumColumn = new('c', MigrationRejectContract.MaximumObjectIdCharacters);
        string maximumName = "a" + new string('n',
            MigrationRejectContract.MaximumEvidenceNameCharacters - 1);
        string maximumValue = new('\u00e9',
            MigrationRejectContract.MaximumEvidenceValueBytes / 2);
        MigrationTargetBatch valid = Unsigned(
            rejectedRows: [Reject(0, maximumRule, maximumColumn, Evidence(maximumName, maximumValue))],
            rejectContractVersion: MigrationRejectContract.DeterministicRejectsV1);

        Assert.Equal(64, MigrationRejectDigest.Compute(valid).Length);
        Assert.DoesNotContain(
            SecretValue,
            Evidence("rawValue", SecretValue).ToString(),
            StringComparison.Ordinal);

        AssertInvalid(Reject(0, maximumRule + "A", "column:value"));
        AssertInvalid(Reject(0, "mig-invalid", "column:value"));
        AssertInvalid(Reject(0, "MIG-TEST-001", string.Empty));
        AssertInvalid(Reject(0, "MIG-TEST-001", maximumColumn + "c"));
        AssertInvalid(Reject(0, "MIG-TEST-001", "column:\u0001"));
        AssertInvalid(Reject(0, "MIG-TEST-001", null, Evidence("BadName", "value")));
        AssertInvalid(Reject(0, "MIG-TEST-001", null, Evidence("bad-name", "value")));
        AssertInvalid(Reject(
            0,
            "MIG-TEST-001",
            null,
            Evidence("second", "value"),
            Evidence("first", "value")));
        AssertInvalid(Reject(
            0,
            "MIG-TEST-001",
            null,
            Evidence("duplicate", "one"),
            Evidence("duplicate", "two")));
        AssertInvalid(Reject(
            0,
            "MIG-TEST-001",
            null,
            Evidence("rawValue", maximumValue + "a")));
        AssertInvalid(Reject(
            0,
            "MIG-TEST-001",
            null,
            Evidence("rawValue", "\ud800")));

        MigrationRejectedRow secret = Reject(
            0,
            "MIG-TEST-001",
            null,
            Evidence("z", SecretValue),
            Evidence("a", "forces-order-failure"));
        InvalidDataException error = Assert.Throws<InvalidDataException>(() =>
            MigrationRejectDigest.Compute(Unsigned(
                rejectedRows: [secret],
                rejectContractVersion: MigrationRejectContract.DeterministicRejectsV1)));
        Assert.DoesNotContain(SecretValue, error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void RejectValidation_RejectsInvalidRowOrderingAndEvidenceCollections()
    {
        Assert.Throws<InvalidDataException>(() => MigrationRejectDigest.Compute(Unsigned(
            rejectedRows: [Reject(-1, "MIG-TEST-001", null)],
            rejectContractVersion: MigrationRejectContract.DeterministicRejectsV1)));
        Assert.Throws<InvalidDataException>(() => MigrationRejectDigest.Compute(Unsigned(
            rejectedRows:
            [
                Reject(2, "MIG-TEST-001", null),
                Reject(2, "MIG-TEST-002", null),
            ],
            rejectContractVersion: MigrationRejectContract.DeterministicRejectsV1)));
        Assert.Throws<InvalidDataException>(() => MigrationRejectDigest.Compute(Unsigned(
            rejectedRows:
            [
                Reject(3, "MIG-TEST-001", null),
                Reject(2, "MIG-TEST-002", null),
            ],
            rejectContractVersion: MigrationRejectContract.DeterministicRejectsV1)));
        AssertInvalid(Reject(0, "MIG-TEST-001", null) with { Evidence = null! });
        AssertInvalid(Reject(0, "MIG-TEST-001", null) with
        {
            Evidence = Enumerable.Repeat<MigrationRejectEvidence>(null!, 1).ToArray(),
        });
        AssertInvalid(Reject(0, "MIG-TEST-001", null) with
        {
            Evidence = Enumerable.Range(
                    0,
                    MigrationRejectContract.MaximumEvidenceEntriesPerRow + 1)
                .Select(index => Evidence($"e{index:D2}", "value"))
                .ToArray(),
        });
    }

    [Fact]
    public void OutcomeValidator_AcceptsAcceptedMixedAndRejectOnlyIntervals()
    {
        MigrationTargetBatch accepted = Seal(Unsigned(
            rows: [Row(40), Row(41)],
            rejectContractVersion: MigrationRejectContract.DeterministicFailFastV1));
        MigrationBatchOutcomeValidator.Validate(accepted, 40, 2);

        MigrationTargetBatch mixed = Seal(Unsigned(
            rows: [Row(40), Row(42)],
            rejectedRows: [Reject(41, "MIG-TEST-001", "column:value")],
            rejectContractVersion: MigrationRejectContract.DeterministicRejectsV1));
        MigrationBatchOutcomeValidator.Validate(mixed, 40, 3);

        MigrationTargetBatch rejected = Seal(Unsigned(
            rows: [],
            rejectedRows:
            [
                Reject(50, "MIG-TEST-001", "column:value"),
                Reject(51, "MIG-TEST-001", "column:value"),
            ],
            rejectContractVersion: MigrationRejectContract.DeterministicRejectsV1));
        MigrationBatchOutcomeValidator.Validate(rejected, 50, 2);
    }

    [Fact]
    public void OutcomeValidator_RequiresOneBoundedContiguousOutcomePerSourceRow()
    {
        MigrationTargetBatch empty = WithRejectDigest(Unsigned(
            rows: [],
            rejectedRows: [],
            rejectContractVersion: MigrationRejectContract.DeterministicRejectsV1));
        Assert.Throws<InvalidDataException>(() =>
            MigrationBatchOutcomeValidator.Validate(empty, 0, 1));

        MigrationTargetBatch gap = WithRejectDigest(Unsigned(
            rows: [Row(40), Row(42)],
            rejectContractVersion: MigrationRejectContract.DeterministicRejectsV1));
        Assert.Throws<InvalidDataException>(() =>
            MigrationBatchOutcomeValidator.Validate(gap, 40, 3));

        MigrationTargetBatch duplicate = WithRejectDigest(Unsigned(
            rows: [Row(40)],
            rejectedRows: [Reject(40, "MIG-TEST-001", null)],
            rejectContractVersion: MigrationRejectContract.DeterministicRejectsV1));
        Assert.Throws<InvalidDataException>(() =>
            MigrationBatchOutcomeValidator.Validate(duplicate, 40, 2));

        MigrationTargetBatch overLimit = Seal(Unsigned(
            rows: [Row(40), Row(41)],
            rejectContractVersion: MigrationRejectContract.DeterministicRejectsV1));
        Assert.Throws<InvalidDataException>(() =>
            MigrationBatchOutcomeValidator.Validate(overLimit, 40, 1));
        Assert.Throws<InvalidDataException>(() =>
            MigrationBatchOutcomeValidator.Validate(overLimit, 41, 2));

        MigrationTargetBatch overflow = WithRejectDigest(Unsigned(
            rows: [Row(long.MaxValue)],
            rejectContractVersion: MigrationRejectContract.DeterministicFailFastV1));
        Assert.Throws<InvalidDataException>(() =>
            MigrationBatchOutcomeValidator.Validate(overflow, long.MaxValue - 1, 1));
    }

    [Fact]
    public void OutcomeValidator_EnforcesContractAndRejectDigest()
    {
        MigrationTargetBatch reject = Unsigned(
            rows: [],
            rejectedRows: [Reject(0, "MIG-TEST-001", null)],
            rejectContractVersion: MigrationRejectContract.DeterministicRejectsV1);

        MigrationTargetBatch unknown = WithRejectDigest(reject with
        {
            RejectContractVersion = "unknown/v1",
        });
        Assert.Throws<InvalidDataException>(() =>
            MigrationBatchOutcomeValidator.Validate(unknown, 0, 1));

        MigrationTargetBatch failFast = WithRejectDigest(reject with
        {
            RejectContractVersion = MigrationRejectContract.DeterministicFailFastV1,
        });
        Assert.Throws<InvalidDataException>(() =>
            MigrationBatchOutcomeValidator.Validate(failFast, 0, 1));

        MigrationTargetBatch stale = WithRejectDigest(reject) with
        {
            RejectDigest = new string('0', 64),
        };
        Assert.Throws<InvalidDataException>(() =>
            MigrationBatchOutcomeValidator.Validate(stale, 0, 1));
        Assert.Throws<InvalidDataException>(() =>
            MigrationBatchOutcomeValidator.Validate(stale with
            {
                RejectDigest = stale.RejectDigest.ToUpperInvariant(),
            }, 0, 1));
    }

    [Fact]
    public void BatchDigestV2_BindsRejectSetAndOutcomeShape()
    {
        Assert.Equal("csharpdb-migration-batch/v2", MigrationBatchDigest.Format);
        MigrationTargetBatch batch = Seal(Unsigned(
            rows: [Row(40), Row(42)],
            rejectedRows:
            [
                Reject(
                    41,
                    "MIG-TEST-001",
                    "column:value",
                    Evidence("rawValue", "not-an-integer")),
            ],
            rejectContractVersion: MigrationRejectContract.DeterministicRejectsV1));
        MigrationTargetBatch changed = Seal(batch with
        {
            BatchDigest = string.Empty,
            RejectedRows =
            [
                Reject(
                    41,
                    "MIG-TEST-002",
                    "column:value",
                    Evidence("rawValue", "not-an-integer")),
            ],
        });

        Assert.NotEqual(batch.RejectDigest, changed.RejectDigest);
        Assert.NotEqual(batch.BatchDigest, changed.BatchDigest);
        Assert.Throws<InvalidDataException>(() => MigrationBatchDigest.Compute(batch with
        {
            RejectedRows = changed.RejectedRows,
        }));
        Assert.Equal(64, batch.BatchDigest.Length);
        Assert.Equal(
            "332bd454a34e91412e29d388d1680afd7e9bbb857c088d01ebaa8579e04958ba",
            batch.BatchDigest);
    }

    [Fact]
    public void BatchDigestV1_PreservesTheReviewedFailFastVector()
    {
        MigrationTargetBatch batch = WithRejectDigest(Unsigned());

        string digest = MigrationBatchDigest.Compute(
            batch,
            MigrationBatchDigest.LegacyFormat);

        Assert.Equal(
            "ff22479fa8604872b35cfb05c6192a4d7034b4ad18a217c2e67666517a133728",
            digest);
        Assert.Equal(
            digest,
            MigrationBatchDigest.Compute(
                batch with { RejectDigest = new string('0', 64) },
                MigrationBatchDigest.LegacyFormat));
    }

    [Fact]
    public void BatchDigests_RejectInvalidUnicodeWithoutNormalizingValidText()
    {
        MigrationTargetBatch highSurrogate = WithRejectDigest(Unsigned(
            rows:
            [
                new MigrationTargetRow
                {
                    SourceRowOrdinal = 40,
                    StableKey = "\ud800",
                    Values = [DbValue.FromText("valid")],
                },
            ]));
        MigrationTargetBatch lowSurrogate = highSurrogate with
        {
            Rows =
            [
                highSurrogate.Rows[0] with { StableKey = "\udc00" },
            ],
        };

        Assert.Throws<InvalidDataException>(() =>
            MigrationBatchDigest.Compute(highSurrogate));
        Assert.Throws<InvalidDataException>(() =>
            MigrationBatchDigest.Compute(lowSurrogate));
        Assert.Throws<InvalidDataException>(() =>
            MigrationBatchDigest.Compute(highSurrogate, MigrationBatchDigest.LegacyFormat));
        Assert.Throws<InvalidDataException>(() =>
            MigrationBatchDigest.Compute(lowSurrogate, MigrationBatchDigest.LegacyFormat));

        MigrationTargetBatch composed = WithRejectDigest(Unsigned(
            rows:
            [
                new MigrationTargetRow
                {
                    SourceRowOrdinal = 40,
                    StableKey = "caf\u00e9",
                    Values = [DbValue.FromText("valid")],
                },
            ]));
        MigrationTargetBatch decomposed = composed with
        {
            Rows =
            [
                composed.Rows[0] with { StableKey = "cafe\u0301" },
            ],
        };
        Assert.NotEqual(
            MigrationBatchDigest.Compute(composed),
            MigrationBatchDigest.Compute(decomposed));
    }

    [Fact]
    public void BatchDigestV2_RejectsNullFirstOutcomeAsContractData()
    {
        MigrationTargetBatch nullAccepted = Unsigned(
            rows: new MigrationTargetRow[] { null! }) with
        {
            RejectDigest = new string('0', 64),
        };
        Assert.Throws<InvalidDataException>(() =>
            MigrationBatchDigest.Compute(nullAccepted));

        MigrationTargetBatch nullRejected = Unsigned(
            rows: [],
            rejectedRows: new MigrationRejectedRow[] { null! },
            rejectContractVersion: MigrationRejectContract.DeterministicRejectsV1) with
        {
            RejectDigest = new string('0', 64),
        };
        Assert.Throws<InvalidDataException>(() =>
            MigrationBatchDigest.Compute(nullRejected));
    }

    private static void AssertInvalid(MigrationRejectedRow rejectedRow)
    {
        Assert.Throws<InvalidDataException>(() => MigrationRejectDigest.Compute(Unsigned(
            rejectedRows: [rejectedRow],
            rejectContractVersion: MigrationRejectContract.DeterministicRejectsV1)));
    }

    private static MigrationTargetBatch Seal(MigrationTargetBatch batch)
    {
        batch = WithRejectDigest(batch);
        return batch with { BatchDigest = MigrationBatchDigest.Compute(batch) };
    }

    private static MigrationTargetBatch WithRejectDigest(MigrationTargetBatch batch) =>
        batch with { RejectDigest = MigrationRejectDigest.Compute(batch) };

    private static MigrationTargetBatch Unsigned(
        IReadOnlyList<MigrationTargetRow>? rows = null,
        IReadOnlyList<MigrationRejectedRow>? rejectedRows = null,
        string rejectContractVersion = MigrationRejectContract.DeterministicFailFastV1) => new()
        {
            PlanDigest = new string('1', 64),
            CatalogDigest = new string('2', 64),
            SourceFingerprint = "source:fingerprint",
            SourceSnapshotIdentity = "source:snapshot",
            SourceObjectId = "table:sample",
            ColumnObjectIds = ["column:value"],
            BatchOrdinal = 7,
            StartCursor = "cursor:40",
            NextCursor = "cursor:43",
            BatchDigest = string.Empty,
            RejectContractVersion = rejectContractVersion,
            Rows = rows ?? [Row(40), Row(41), Row(42)],
            RejectedRows = rejectedRows ?? [],
        };

    private static MigrationTargetRow Row(long sourceRowOrdinal) => new()
    {
        SourceRowOrdinal = sourceRowOrdinal,
        Values = [DbValue.FromInteger(sourceRowOrdinal)],
    };

    private static MigrationRejectedRow Reject(
        long sourceRowOrdinal,
        string ruleId,
        string? columnObjectId,
        params MigrationRejectEvidence[] evidence) => new()
        {
            SourceRowOrdinal = sourceRowOrdinal,
            RuleId = ruleId,
            ColumnObjectId = columnObjectId,
            Evidence = evidence,
        };

    private static MigrationRejectEvidence Evidence(string name, string? value) => new()
    {
        Name = name,
        Value = value,
    };
}
