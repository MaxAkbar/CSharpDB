using System.Text.Json;
using System.Text.RegularExpressions;

namespace CSharpDB.Tests;

public sealed class SqlCompatibilityManifestTests
{
    private static readonly string[] FacetNames =
    [
        "parser",
        "execution",
        "persistence",
        "catalog",
        "embedded_ado_net",
        "rest_stateless",
        "rest_session",
        "grpc_unary",
        "grpc_session",
        "ef_query",
        "ef_migration",
    ];

    [Fact]
    public void Manifest_HasConsistentStatusesAndExecutableEvidence()
    {
        string repositoryRoot = FindRepositoryRoot();
        string manifestPath = Path.Combine(repositoryRoot, "www", "docs", "sql-compatibility.json");
        using JsonDocument document = JsonDocument.Parse(File.ReadAllText(manifestPath));
        JsonElement root = document.RootElement;

        var proofs = root.GetProperty("proofs")
            .EnumerateArray()
            .ToDictionary(
                proof => proof.GetProperty("id").GetString()!,
                proof => proof.Clone(),
                StringComparer.Ordinal);
        Assert.Equal(proofs.Count, root.GetProperty("proofs").GetArrayLength());

        var referencedProofs = new HashSet<string>(StringComparer.Ordinal);
        var featureIds = new HashSet<string>(StringComparer.Ordinal);
        foreach (JsonElement feature in root.GetProperty("features").EnumerateArray())
        {
            string featureId = feature.GetProperty("id").GetString()!;
            Assert.True(featureIds.Add(featureId), $"Duplicate feature id '{featureId}'.");

            JsonElement facets = feature.GetProperty("facets");
            string[] appliesTo = feature.GetProperty("applies_to")
                .EnumerateArray()
                .Select(value => value.GetString()!)
                .ToArray();
            var applicableFacetNames = new HashSet<string>(appliesTo, StringComparer.Ordinal);
            Assert.Equal(appliesTo.Length, applicableFacetNames.Count);

            var applicableStatuses = new List<string>();
            foreach (string facetName in FacetNames)
            {
                string status = facets.GetProperty(facetName).GetString()!;
                Assert.Equal(status != "not_applicable", applicableFacetNames.Contains(facetName));
                if (status != "not_applicable")
                    applicableStatuses.Add(status);
            }

            Assert.NotEmpty(applicableStatuses);
            string expectedAvailability = applicableStatuses.All(status => status == "supported")
                ? "supported"
                : applicableStatuses.Any(status => status is "supported" or "partial")
                    ? "partial"
                    : "unsupported";
            string availability = feature.GetProperty("availability").GetString()!;
            Assert.Equal(expectedAvailability, availability);

            string[] positiveProofIds = ReadStringArray(feature, "positive_proof_ids");
            string[] negativeProofIds = ReadStringArray(feature, "negative_proof_ids");
            foreach (string proofId in positiveProofIds.Concat(negativeProofIds))
            {
                Assert.True(proofs.ContainsKey(proofId), $"Feature '{featureId}' references unknown proof '{proofId}'.");
                referencedProofs.Add(proofId);
            }

            if (availability == "unsupported")
            {
                Assert.False(feature.TryGetProperty("first_supported_version", out _));
            }
            else
            {
                Assert.True(feature.TryGetProperty("first_supported_version", out JsonElement firstSupportedVersion));
                Assert.Equal(JsonValueKind.String, firstSupportedVersion.ValueKind);
                Assert.False(string.IsNullOrWhiteSpace(firstSupportedVersion.GetString()));
                Assert.NotEmpty(positiveProofIds);
            }

            if (availability == "partial")
            {
                Assert.True(
                    feature.GetProperty("limitations").GetArrayLength() > 0 || negativeProofIds.Length > 0,
                    $"Partial feature '{featureId}' needs a limitation or rejection proof.");
            }

            if (availability == "supported" && feature.GetProperty("persistent_ddl").GetBoolean())
            {
                Assert.Contains(
                    positiveProofIds,
                    proofId => proofs[proofId].GetProperty("dimension").GetString() == "persistence");
                Assert.Contains(
                    positiveProofIds,
                    proofId => proofs[proofId].GetProperty("dimension").GetString() == "catalog");
            }
        }

        Assert.Equal(proofs.Keys.Order(), referencedProofs.Order());
        ValidateProofRegistry(repositoryRoot, proofs);
    }

    private static void ValidateProofRegistry(
        string repositoryRoot,
        IReadOnlyDictionary<string, JsonElement> proofs)
    {
        string ciWorkflow = File.ReadAllText(Path.Combine(repositoryRoot, ".github", "workflows", "ci.yml"))
            .Replace('\\', '/');

        foreach ((string proofId, JsonElement proof) in proofs)
        {
            Assert.True(proof.GetProperty("ci_executed").GetBoolean(), $"Proof '{proofId}' is not CI executed.");

            string project = proof.GetProperty("project").GetString()!.Replace('\\', '/');
            Assert.Contains(project, ciWorkflow, StringComparison.Ordinal);

            string relativeSource = proof.GetProperty("source").GetString()!.Replace('/', Path.DirectorySeparatorChar);
            string sourcePath = Path.Combine(repositoryRoot, relativeSource);
            Assert.True(File.Exists(sourcePath), $"Proof '{proofId}' source '{relativeSource}' does not exist.");

            string testName = proof.GetProperty("test").GetString()!;
            string source = File.ReadAllText(sourcePath);
            Assert.Matches(
                new Regex($@"\b{Regex.Escape(testName)}\s*\(", RegexOptions.CultureInvariant),
                source);
        }
    }

    private static string[] ReadStringArray(JsonElement owner, string propertyName)
        => owner.GetProperty(propertyName)
            .EnumerateArray()
            .Select(value => value.GetString()!)
            .ToArray();

    private static string FindRepositoryRoot()
    {
        DirectoryInfo? directory = new(AppContext.BaseDirectory);
        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "www", "docs", "sql-compatibility.json")))
                return directory.FullName;

            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException("Could not locate the CSharpDB repository root.");
    }
}
