using System.Text;
using CSharpDB.Migration;

namespace CSharpDB.Migration.Tests;

public sealed class MigrationRejectLedgerCodecTests
{
    private const string SecretValue = "TOP-SECRET-LEDGER-VALUE";

    [Fact]
    public void EvidenceCodec_RoundTripsCanonicalOrderedEvidence()
    {
        MigrationRejectEvidence[] evidence =
        [
            new() { Name = "logicalRecordNumber", Value = "7" },
            new() { Name = "rawValue", Value = "café\r\n\"quoted\"" },
            new() { Name = "wasQuoted", Value = null },
        ];

        string json = MigrationRejectLedgerCodec.SerializeEvidence(evidence);
        IReadOnlyList<MigrationRejectEvidence> decoded =
            MigrationRejectLedgerCodec.DeserializeEvidence(json);

        Assert.Equal(
            "[{\"name\":\"logicalRecordNumber\",\"value\":\"7\"}," +
            "{\"name\":\"rawValue\",\"value\":\"caf\\u00E9\\r\\n\\u0022quoted\\u0022\"}," +
            "{\"name\":\"wasQuoted\",\"value\":null}]",
            json);
        Assert.Equal(evidence, decoded);
    }

    [Theory]
    [InlineData("[{\"value\":\"7\",\"name\":\"logicalRecordNumber\"}]")]
    [InlineData("[ {\"name\":\"logicalRecordNumber\",\"value\":\"7\"} ]")]
    [InlineData("[{\"name\":\"logicalRecordNumber\",\"value\":\"7\",\"extra\":0}]")]
    [InlineData("[{\"name\":\"logicalRecordNumber\",\"name\":\"other\",\"value\":\"7\"}]")]
    public void EvidenceCodec_RejectsNoncanonicalOrAmbiguousJson(string json)
    {
        Assert.Throws<InvalidDataException>(() =>
            MigrationRejectLedgerCodec.DeserializeEvidence(json));
    }

    [Fact]
    public void EvidenceCodec_DistinguishesNullEmptyAndUnicodeNormalization()
    {
        string nullJson = MigrationRejectLedgerCodec.SerializeEvidence(
            [new MigrationRejectEvidence { Name = "rawValue", Value = null }]);
        string emptyJson = MigrationRejectLedgerCodec.SerializeEvidence(
            [new MigrationRejectEvidence { Name = "rawValue", Value = string.Empty }]);
        string composedJson = MigrationRejectLedgerCodec.SerializeEvidence(
            [new MigrationRejectEvidence { Name = "rawValue", Value = "é" }]);
        string decomposedJson = MigrationRejectLedgerCodec.SerializeEvidence(
            [new MigrationRejectEvidence { Name = "rawValue", Value = "é" }]);

        Assert.NotEqual(nullJson, emptyJson);
        Assert.NotEqual(composedJson, decomposedJson);
        Assert.Equal(0, MigrationRejectLedgerCodec.GetRawValueByteCount(Reject(null)));
        Assert.Equal(0, MigrationRejectLedgerCodec.GetRawValueByteCount(Reject(string.Empty)));
        Assert.Equal(2, MigrationRejectLedgerCodec.GetRawValueByteCount(Reject("é")));
        Assert.Equal(3, MigrationRejectLedgerCodec.GetRawValueByteCount(Reject("é")));
    }

    [Fact]
    public void SensitiveValueAccounting_ChargesEveryEvidenceValue()
    {
        var rejectedRow = new MigrationRejectedRow
        {
            SourceRowOrdinal = 4,
            RuleId = "MIG-TEST-LEDGER-001",
            ColumnObjectId = "column:value",
            Evidence =
            [
                new MigrationRejectEvidence { Name = "columnKind", Value = "Text" },
                new MigrationRejectEvidence { Name = "rawValue", Value = "é" },
                new MigrationRejectEvidence { Name = "wasQuoted", Value = "true" },
            ],
        };

        Assert.Equal(10, MigrationRejectLedgerCodec.GetRawValueByteCount(rejectedRow));
    }

    [Fact]
    public void ArtifactHeader_HasFixedCanonicalJsonAndUtf8ByteCount()
    {
        string planDigest = new('0', 64);
        const string expected =
            "{\"format\":\"csharpdb-migration-reject-artifact/v1\"," +
            "\"planDigest\":\"0000000000000000000000000000000000000000000000000000000000000000\"}";

        Assert.Equal(expected, MigrationRejectLedgerCodec.SerializeArtifactHeader(planDigest));
        Assert.Equal(131, MigrationRejectLedgerCodec.GetArtifactHeaderByteCount(planDigest));
        Assert.Equal(131, MigrationRejectLedgerCodec.MinimumCanonicalArtifactBytes);
    }

    [Fact]
    public void EntryCodec_HasFixedCanonicalJsonAndUtf8ByteCounts()
    {
        MigrationRejectedRow rejectedRow = Reject("bad");
        const string expected =
            "{\"format\":\"csharpdb-migration-reject-entry/v1\"," +
            "\"sourceObjectId\":\"table:sample\",\"batchOrdinal\":3,\"sourceRowOrdinal\":4," +
            "\"ruleId\":\"MIG-TEST-LEDGER-001\",\"columnObjectId\":\"column:value\"," +
            "\"evidence\":[{\"name\":\"rawValue\",\"value\":\"bad\"}]}";

        string json = MigrationRejectLedgerCodec.SerializeEntry("table:sample", 3, rejectedRow);

        Assert.Equal(expected, json);
        Assert.Equal(227, Encoding.UTF8.GetByteCount(json));
        Assert.Equal(227, MigrationRejectLedgerCodec.GetCanonicalEntryByteCount(
            "table:sample",
            3,
            rejectedRow));
        Assert.Equal(228, MigrationRejectLedgerCodec.GetCanonicalArtifactEntryByteCount(
            "table:sample",
            3,
            rejectedRow));
        Assert.Equal(json, MigrationRejectLedgerCodec.SerializeEntry("table:sample", 3, rejectedRow));
    }

    [Fact]
    public void EntryCodec_RejectsOversizedSourceObjectId()
    {
        string sourceObjectId = new('x', MigrationRejectContract.MaximumObjectIdCharacters + 1);

        ArgumentException error = Assert.Throws<ArgumentException>(() =>
            MigrationRejectLedgerCodec.SerializeEntry(sourceObjectId, 3, Reject("bad")));

        Assert.Equal("sourceObjectId", error.ParamName);
    }

    [Fact]
    public void CodecFailures_DoNotExposeEvidenceValues()
    {
        MigrationRejectEvidence[] evidence =
        [
            new() { Name = "z", Value = SecretValue },
            new() { Name = "a", Value = "forces-order-failure" },
        ];

        InvalidDataException error = Assert.Throws<InvalidDataException>(() =>
            MigrationRejectLedgerCodec.SerializeEvidence(evidence));

        Assert.DoesNotContain(SecretValue, error.ToString(), StringComparison.Ordinal);
    }

    private static MigrationRejectedRow Reject(string? rawValue) => new()
    {
        SourceRowOrdinal = 4,
        RuleId = "MIG-TEST-LEDGER-001",
        ColumnObjectId = "column:value",
        Evidence = [new MigrationRejectEvidence { Name = "rawValue", Value = rawValue }],
    };
}
