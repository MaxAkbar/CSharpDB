using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using CSharpDB.Migration;

namespace CSharpDB.Migration.Tests;

public sealed class MigrationValidationReportTests
{
    [Fact]
    public void Serialize_IsDeterministic_AndRoundTripsCanonicalReport()
    {
        MigrationValidationReport report = CreateReport();
        MigrationValidationReport differentlyOrdered = report with
        {
            Schema = report.Schema with
            {
                Differences = report.Schema.Differences.Reverse().ToArray(),
            },
            Objects = report.Objects
                .Reverse()
                .Select(item => item with
                {
                    Partitions = item.Partitions
                        .Reverse()
                        .Select(partition => partition with
                        {
                            Mismatches = partition.Mismatches.Reverse().ToArray(),
                        })
                        .ToArray(),
                })
                .ToArray(),
            Diagnostics = report.Diagnostics.Reverse().ToArray(),
        };

        string first = MigrationValidationReportSerializer.Serialize(report);
        string second = MigrationValidationReportSerializer.Serialize(differentlyOrdered);

        Assert.Equal(first, second);
        Assert.Contains("\"format\": \"csharpdb-migration-validation/v1\"", first, StringComparison.Ordinal);
        Assert.DoesNotContain("timestamp", first, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("rowValue", first, StringComparison.OrdinalIgnoreCase);

        MigrationValidationReport restored = MigrationValidationReportSerializer.Deserialize(first);
        Assert.Equal(first, MigrationValidationReportSerializer.Serialize(restored));
        Assert.Equal("table:alpha", restored.Objects[0].SourceObjectId);
        Assert.Equal(0, restored.Objects[0].Partitions[0].PartitionId);
        Assert.Equal("diagnostic:a", restored.Diagnostics[0].DiagnosticId);

        using JsonDocument document = JsonDocument.Parse(first);
        Assert.Equal(
            document.RootElement.GetProperty("digest").GetString(),
            MigrationValidationReportSerializer.ComputeDigest(report));
    }

    [Fact]
    public void Deserialize_RejectsTamperedPayload()
    {
        JsonObject envelope = JsonNode.Parse(
                MigrationValidationReportSerializer.Serialize(CreateReport()))!
            .AsObject();
        JsonArray objects = envelope["payload"]!["objects"]!.AsArray();
        objects[0]!["sourceRowCount"] = 99_999;

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => MigrationValidationReportSerializer.Deserialize(envelope.ToJsonString()));

        Assert.Contains("digest does not match", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Deserialize_RejectsRedigestedNonCanonicalCollectionOrder()
    {
        JsonObject envelope = JsonNode.Parse(
                MigrationValidationReportSerializer.Serialize(CreateReport()))!
            .AsObject();
        JsonArray objects = envelope["payload"]!["objects"]!.AsArray();
        JsonNode first = objects[0]!.DeepClone();
        JsonNode second = objects[1]!.DeepClone();
        objects[0] = second;
        objects[1] = first;
        Redigest(envelope);

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => MigrationValidationReportSerializer.Deserialize(envelope.ToJsonString()));

        Assert.Contains("deterministic order", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Normalize_RejectsMissingBindingIdentity()
    {
        MigrationValidationReport report = CreateReport();
        report = report with
        {
            Binding = report.Binding with { TargetSnapshotIdentity = " " },
        };

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => MigrationValidationReportSerializer.Normalize(report));

        Assert.Contains("Target snapshot identity", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Normalize_RejectsDuplicateObjectIdentity()
    {
        MigrationValidationReport report = CreateReport();
        MigrationObjectValidationEvidence duplicate = report.Objects[0] with
        {
            TargetObjectId = "target:duplicate",
        };
        report = report with { Objects = [.. report.Objects, duplicate] };

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => MigrationValidationReportSerializer.Normalize(report));

        Assert.Contains("duplicate source validation object identity", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Normalize_RejectsDuplicatePartitionIdentity()
    {
        MigrationValidationReport report = CreateReport();
        MigrationObjectValidationEvidence first = report.Objects[1];
        first = first with { Partitions = [.. first.Partitions, first.Partitions[0]] };
        report = report with { Objects = [report.Objects[0], first] };

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => MigrationValidationReportSerializer.Normalize(report));

        Assert.Contains("duplicate partition identity", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Normalize_RejectsCredentialShapedSnapshotIdentity()
    {
        MigrationValidationReport report = CreateReport() with
        {
            Binding = CreateReport().Binding with
            {
                SourceSnapshotIdentity = "server=fixture;password=do-not-store",
            },
        };

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => MigrationValidationReportSerializer.Normalize(report));

        Assert.Contains("credential material", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Deserialize_RejectsRedigestedOutcomeThatContradictsEvidence()
    {
        JsonObject envelope = JsonNode.Parse(
                MigrationValidationReportSerializer.Serialize(CreateReport()))!
            .AsObject();
        envelope["payload"]!["outcome"] = "passed";
        Redigest(envelope);

        InvalidDataException error = Assert.Throws<InvalidDataException>(
            () => MigrationValidationReportSerializer.Deserialize(envelope.ToJsonString()));

        Assert.Contains("contradicts", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    private static MigrationValidationReport CreateReport() => new()
    {
        Binding = new MigrationValidationBinding
        {
            TargetCSharpDbVersion = "4.2.0",
            PlanDigest = Hash('1'),
            CatalogDigest = Hash('2'),
            CapabilityDigest = Hash('3'),
            SourceIdentity = "sqlite:inventory",
            SourceFingerprint = "inventory-fixture-v1",
            TargetIdentity = "csharpdb:staged-inventory",
            SourceSnapshotIdentity = "sqlite-snapshot:42",
            TargetSnapshotIdentity = "csharpdb-snapshot:84",
            CanonicalizationVersion = "csharpdb-canon-v1",
            CanonicalizationContractDigest = Hash('4'),
        },
        Level = MigrationValidationLevel.Checksum,
        Outcome = MigrationValidationStatus.Different,
        SnapshotConsistency = new MigrationSnapshotConsistencyEvidence
        {
            Status = MigrationSnapshotConsistencyStatus.Established,
        },
        Schema = new MigrationSchemaValidationEvidence
        {
            Status = MigrationValidationStatus.Different,
            SourceSchemaDigest = Hash('5'),
            TargetSchemaDigest = Hash('6'),
            Differences =
            [
                new MigrationSchemaDifferenceEvidence
                {
                    ObjectId = "table:beta",
                    Kind = MigrationSchemaDifferenceKind.MissingFromTarget,
                    SourceDefinitionDigest = Hash('7'),
                },
                new MigrationSchemaDifferenceEvidence
                {
                    ObjectId = "table:alpha",
                    Kind = MigrationSchemaDifferenceKind.DefinitionMismatch,
                    SourceDefinitionDigest = Hash('8'),
                    TargetDefinitionDigest = Hash('9'),
                },
            ],
        },
        Objects =
        [
            new MigrationObjectValidationEvidence
            {
                SourceObjectId = "table:beta",
                TargetObjectId = "target:beta",
                Status = MigrationValidationStatus.Passed,
                CanonicalTypeContractDigest = Hash('a'),
                ObjectContractDigest = Hash('b'),
                SourceRowCount = 5,
                TargetRowCount = 5,
                SourceChecksum = Hash('c'),
                TargetChecksum = Hash('c'),
                Partitions = Partitions(5, 5),
            },
            new MigrationObjectValidationEvidence
            {
                SourceObjectId = "table:alpha",
                TargetObjectId = "target:alpha",
                Status = MigrationValidationStatus.Different,
                CanonicalTypeContractDigest = Hash('d'),
                ObjectContractDigest = Hash('e'),
                SourceRowCount = 12,
                TargetRowCount = 11,
                SourceChecksum = Hash('a'),
                TargetChecksum = Hash('b'),
                Partitions = Partitions(12, 11),
            },
        ],
        Diagnostics =
        [
            new MigrationValidationDiagnosticEvidence
            {
                DiagnosticId = "diagnostic:z",
                RuleId = "validation.schema.definition",
                Severity = MigrationDiagnosticSeverity.Error,
                Status = MigrationValidationStatus.Different,
                Evidence = MigrationEvidenceLevel.DifferentiallyValidated,
                ObjectId = "table:beta",
            },
            new MigrationValidationDiagnosticEvidence
            {
                DiagnosticId = "diagnostic:a",
                RuleId = "validation.row.changed",
                Severity = MigrationDiagnosticSeverity.Error,
                Status = MigrationValidationStatus.Different,
                Evidence = MigrationEvidenceLevel.DifferentiallyValidated,
                ObjectId = "table:alpha",
                PartitionId = 2,
            },
        ],
    };

    private static IReadOnlyList<MigrationValidationPartitionEvidence> Partitions(
        long sourceRowCount,
        long targetRowCount) => Enumerable.Range(0, 256)
        .Select(partitionId =>
        {
            long source = partitionId == 2 ? sourceRowCount : 0;
            long target = partitionId == 2 ? targetRowCount : 0;
            bool passed = source == target;
            return new MigrationValidationPartitionEvidence
            {
                PartitionId = partitionId,
                Status = passed
                    ? MigrationValidationStatus.Passed
                    : MigrationValidationStatus.Different,
                SourceRowCount = source,
                TargetRowCount = target,
                SourceDigest = passed ? Hash('c') : Hash('e'),
                TargetDigest = passed ? Hash('c') : Hash('f'),
                Mismatches = passed
                    ? []
                    :
                    [
                        new MigrationValidationMismatchEvidence
                        {
                            Kind = MigrationValidationMismatchKind.SourceOnly,
                            SourceRowHash = Hash('3'),
                            SourceMultiplicity = source - target,
                        },
                    ],
            };
        })
        .ToArray();

    private static void Redigest(JsonObject envelope)
    {
        using JsonDocument payloadDocument = JsonDocument.Parse(envelope["payload"]!.ToJsonString());
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web)
        {
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        };
        byte[] bytes = JsonSerializer.SerializeToUtf8Bytes(
            new
            {
                Format = MigrationArtifactFormats.ValidationReportV1,
                DigestAlgorithm = MigrationArtifactFormats.DigestAlgorithm,
                Payload = payloadDocument.RootElement,
            },
            options);
        envelope["digest"] = Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();
    }

    private static string Hash(char value) => new(value, 64);
}
