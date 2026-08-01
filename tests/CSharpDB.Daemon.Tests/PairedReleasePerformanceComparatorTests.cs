using System.Diagnostics;
using System.Globalization;

namespace CSharpDB.Daemon.Tests;

public sealed class PairedReleasePerformanceComparatorTests
{
    private const int RepeatCount = 3;
    private const string SuiteName = "suite";
    private const string HybridQualificationSuiteName =
        "hybrid-storage-mode-scenario";
    private const string ValidQualificationExtraInfo =
        "qualification=true; unrecorded-warmup-seconds=2; " +
        "minimum-measured-seconds=30; " +
        "minimum-retained-latency-samples=10000; " +
        "measurement-cap-seconds=120; " +
        "measurement-begin-utc=2026-07-31T12:00:00.0000000+00:00; " +
        "measurement-end-utc=2026-07-31T12:00:30.0000000+00:00";
    private const string Plan2LegacyProseExtraInfo =
        "durability=full durable file-backed commits; " +
        "each acknowledged commit forces durable backing-file visibility; " +
        "residency=bounded resident set; " +
        ValidQualificationExtraInfo;
    private const string EvidenceHeader =
        "Name,TotalOps,LatencySamples,ElapsedMs,OpsPerSec,P50,P90,P95,P99,P999," +
        "Min,Max,Mean,StdDev,ExtraInfo";
    private const string ManifestHeader =
        "Suite,PairId,Order,FirstRevision,SecondRevision,BaselineRaw,CandidateRaw";

    [Fact]
    public async Task PairedComparer_CancelsSyntheticDriftAndPreservesSlowdownThreshold()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            ComparisonLayout layout = CreateLayout(temporaryRoot);
            CreateBalancedEvidence(
                layout,
                (order, index) =>
                {
                    decimal drift = order == "previous-candidate"
                        ? new decimal[] { 100m, 250m, 600m }[index]
                        : new decimal[] { 140m, 350m, 840m }[index];
                    return new Dictionary<string, Measurement>(StringComparer.Ordinal)
                    {
                        ["drift"] = new(drift, drift, 10m, 10m),
                        ["slowdown-10"] = new(drift, drift * 0.90m, 10m, 10m),
                        ["slowdown-20"] = new(drift, drift * 0.80m, 10m, 10m),
                    };
                });

            ProcessResult result = await RunComparerAsync(layout);

            Assert.NotEqual(0, result.ExitCode);
            string report = File.ReadAllText(layout.ReportPath);
            Assert.Contains(
                "| suite | drift | 0.00% | 0.00% | 0.0000 ms | PASS |",
                report);
            Assert.Contains(
                "| suite | slowdown-10 | 10.00% | 0.00% | 0.0000 ms | PASS |",
                report);
            Assert.Contains(
                "| suite | slowdown-20 | 20.00% | 0.00% | 0.0000 ms | REGRESSION |",
                report);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task PairedComparer_FlagsOrderPenaltyAsOrderSensitive()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            ComparisonLayout layout = CreateLayout(temporaryRoot);
            CreateBalancedEvidence(
                layout,
                (order, _) =>
                {
                    Measurement measurement = order == "previous-candidate"
                        ? new(100m, 80m, 10m, 10m)
                        : new(80m, 100m, 10m, 10m);
                    return SingleRow("order-penalty", measurement);
                });

            ProcessResult result = await RunComparerAsync(layout);

            Assert.NotEqual(0, result.ExitCode);
            string report = File.ReadAllText(layout.ReportPath);
            Assert.Contains("- Order-sensitive rows: 1", report);
            Assert.Contains(
                "| suite | order-penalty | 0.00% | 0.00% | 0.0000 ms | " +
                "ORDER-SENSITIVE |",
                report);
            Assert.Contains("the two order strata disagree", report);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task PairedComparer_ToleratesSingleContaminatedPair()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            ComparisonLayout layout = CreateLayout(temporaryRoot);
            CreateBalancedEvidence(
                layout,
                (order, index) =>
                {
                    Measurement measurement = order == "previous-candidate" && index == 0
                        ? new(100m, 50m, 10m, 30m)
                        : new(100m, 100m, 10m, 10m);
                    return SingleRow("contaminated", measurement);
                });

            ProcessResult result = await RunComparerAsync(layout);

            Assert.True(result.ExitCode == 0, result.CombinedOutput);
            string report = File.ReadAllText(layout.ReportPath);
            Assert.Contains(
                "| suite | contaminated | 0.00% | 0.00% | 0.0000 ms | PASS |",
                report);
            Assert.Contains("Tolerated paired outlier", report);
            Assert.Contains("(2/3; 2 required)", report);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task PairedComparer_AllowsDisjointMetricOutliersWithSeparateMajorities()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            ComparisonLayout layout = CreateLayout(temporaryRoot);
            CreateBalancedEvidence(
                layout,
                (_, index) =>
                {
                    Measurement measurement = index switch
                    {
                        0 => new(100m, 50m, 10m, 10m),
                        1 => new(100m, 100m, 10m, 30m),
                        2 => new(100m, 100m, 10m, 10m),
                        _ => throw new ArgumentOutOfRangeException(nameof(index)),
                    };
                    return SingleRow("disjoint-metric-outliers", measurement);
                });

            ProcessResult result = await RunComparerAsync(layout);

            Assert.True(result.ExitCode == 0, result.CombinedOutput);
            string report = File.ReadAllText(layout.ReportPath);
            Assert.Contains(
                "| suite | disjoint-metric-outliers | 0.00% | 0.00% | 0.0000 ms | " +
                "PASS |",
                report);
            Assert.Contains("stability throughput=2/3, P99=2/3", report);
            Assert.Contains("Tolerated paired outlier for throughput", report);
            Assert.Contains("Tolerated paired outlier for P99", report);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task PairedComparer_RejectsWhenOneMetricLacksStableMajority()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            ComparisonLayout layout = CreateLayout(temporaryRoot);
            CreateBalancedEvidence(
                layout,
                (order, index) =>
                {
                    decimal candidateOps = order == "previous-candidate"
                        ? new decimal[] { 50m, 100m, 150m }[index]
                        : 100m;
                    return SingleRow(
                        "unstable-contamination",
                        new(100m, candidateOps, 10m, 10m));
                });

            ProcessResult result = await RunComparerAsync(layout);

            Assert.NotEqual(0, result.ExitCode);
            string report = File.ReadAllText(layout.ReportPath);
            Assert.Contains("- Insufficient stability rows: 1", report);
            Assert.Contains(
                "| suite | unstable-contamination | n/a | n/a | n/a | UNSTABLE |",
                report);
            Assert.Contains("throughput stability=1/3 (2 required)", report);
            Assert.Contains("P99 stability=3/3 (2 required)", report);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task LegacyComparer_WithoutPairManifest_RetainsWholeRunStabilityRule()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            ComparisonLayout layout = CreateLayout(temporaryRoot);
            Measurement[] runs =
            [
                new(100m, 50m, 10m, 10m),
                new(100m, 100m, 10m, 30m),
                new(100m, 100m, 10m, 10m),
            ];
            WriteLegacyEvidence(layout, "legacy-disjoint-outliers", runs);

            ProcessResult result = await RunLegacyComparerAsync(layout);

            Assert.NotEqual(0, result.ExitCode);
            string report = File.ReadAllText(layout.ReportPath);
            Assert.StartsWith("# Previous-release performance comparison", report);
            Assert.Contains(
                "| suite | legacy-disjoint-outliers | n/a | n/a | UNSTABLE |",
                report);
            Assert.Contains("has 1/3 whole runs within both limits", report);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task PairedComparer_RequiresRelativeAndAbsoluteP99Limits()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            ComparisonLayout layout = CreateLayout(temporaryRoot);
            CreateBalancedEvidence(
                layout,
                (_, _) => new Dictionary<string, Measurement>(StringComparer.Ordinal)
                {
                    ["relative-only"] = new(100m, 100m, 0.10m, 0.13m),
                    ["relative-and-absolute"] = new(100m, 100m, 1.00m, 1.30m),
                });

            ProcessResult result = await RunComparerAsync(layout);

            Assert.NotEqual(0, result.ExitCode);
            string report = File.ReadAllText(layout.ReportPath);
            Assert.Contains(
                "| suite | relative-only | 0.00% | 30.00% | 0.0300 ms | PASS |",
                report);
            Assert.Contains("P99 percentage-only crossing", report);
            Assert.Contains(
                "| suite | relative-and-absolute | 0.00% | 30.00% | 0.3000 ms | " +
                "REGRESSION |",
                report);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task PairedComparer_DisjointP99CrossingsDoNotSatisfyAndRule()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            ComparisonLayout layout = CreateLayout(temporaryRoot);
            CreateBalancedEvidence(
                layout,
                (_, index) =>
                {
                    Measurement measurement = index switch
                    {
                        0 => new(100m, 100m, 0.01m, 0.014m),
                        1 => new(100m, 100m, 1.00m, 1.30m),
                        2 => new(100m, 100m, 10.00m, 10.10m),
                        _ => throw new ArgumentOutOfRangeException(nameof(index)),
                    };
                    return SingleRow("disjoint-p99", measurement);
                });

            ProcessResult result = await RunComparerAsync(layout);

            Assert.True(result.ExitCode == 0, result.CombinedOutput);
            string report = File.ReadAllText(layout.ReportPath);
            Assert.Contains(
                "| suite | disjoint-p99 | 0.00% | 30.00% | 0.1000 ms | PASS |",
                report);
            Assert.Contains("Disjoint P99 crossings", report);
            Assert.Contains("P99 both-limit pairs=1/3", report);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Theory]
    [InlineData("missing")]
    [InlineData("duplicate-pair")]
    [InlineData("duplicate-file")]
    public async Task PairedComparer_RejectsMissingOrDuplicatePairsAndFiles(string defect)
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            ComparisonLayout layout = CreateLayout(temporaryRoot);
            List<PairDefinition> pairs = CreateBalancedEvidence(
                layout,
                (_, _) => SingleRow("lookup", new(100m, 100m, 10m, 10m)));
            switch (defect)
            {
                case "missing":
                    pairs.RemoveAt(pairs.Count - 1);
                    break;
                case "duplicate-pair":
                    pairs[^1] = pairs[^1] with { PairId = pairs[0].PairId };
                    break;
                case "duplicate-file":
                    pairs[^1] = pairs[^1] with { BaselineRaw = pairs[0].BaselineRaw };
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(defect));
            }
            WriteManifest(layout.ManifestPath, pairs);

            ProcessResult result = await RunComparerAsync(layout);

            Assert.NotEqual(0, result.ExitCode);
            string report = File.ReadAllText(layout.ReportPath);
            Assert.Contains("- Invalid evidence rows: 1", report);
            Assert.Contains("| <manifest> | <manifest> |", report);
            Assert.Contains("INVALID", report);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task PairedComparer_RejectsMismatchedAdjacencyIdentity()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            ComparisonLayout layout = CreateLayout(temporaryRoot);
            List<PairDefinition> pairs = CreateBalancedEvidence(
                layout,
                (_, _) => SingleRow("lookup", new(100m, 100m, 10m, 10m)));
            pairs[0] = pairs[0] with
            {
                FirstRevision = "candidate",
                SecondRevision = "previous",
            };
            WriteManifest(layout.ManifestPath, pairs);

            ProcessResult result = await RunComparerAsync(layout);

            Assert.NotEqual(0, result.ExitCode);
            string report = File.ReadAllText(layout.ReportPath);
            Assert.Contains("INVALID", report);
            Assert.Contains("adjacency identity", report);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task PairedComparer_RejectsRawPathOutsideRevisionRoot()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            ComparisonLayout layout = CreateLayout(temporaryRoot);
            List<PairDefinition> pairs = CreateBalancedEvidence(
                layout,
                (_, _) => SingleRow("lookup", new(100m, 100m, 10m, 10m)));
            pairs[0] = pairs[0] with { BaselineRaw = pairs[0].CandidateRaw };
            WriteManifest(layout.ManifestPath, pairs);

            ProcessResult result = await RunComparerAsync(layout);

            Assert.NotEqual(0, result.ExitCode);
            string report = File.ReadAllText(layout.ReportPath);
            Assert.Contains("INVALID", report);
            Assert.Contains("escapes its supplied baseline raw root", report);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task PairedComparer_RejectsInsufficientPairSamples()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            ComparisonLayout layout = CreateLayout(temporaryRoot);
            CreateBalancedEvidence(
                layout,
                (order, index) =>
                {
                    int candidateSamples = order == "candidate-previous" && index == 2
                        ? 99
                        : 200;
                    return SingleRow(
                        "lookup",
                        new(100m, 100m, 10m, 10m, 200, candidateSamples));
                });

            ProcessResult result = await RunComparerAsync(layout);

            Assert.NotEqual(0, result.ExitCode);
            string report = File.ReadAllText(layout.ReportPath);
            Assert.Contains("| suite | lookup | n/a | n/a | n/a | INVALID |", report);
            Assert.Contains("LatencySamples", report);
            Assert.Contains("must be at least 100", report);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task PairedComparer_AcceptsCompleteHybridQualificationEvidence()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            ComparisonLayout layout = CreateLayout(
                temporaryRoot,
                HybridQualificationSuiteName);
            CreateBalancedEvidence(
                layout,
                (_, _) => SingleRow(
                    "qualified-scenario",
                    CreateHybridQualificationMeasurement()));

            ProcessResult result = await RunComparerAsync(layout);

            Assert.True(result.ExitCode == 0, result.CombinedOutput);
            string report = File.ReadAllText(layout.ReportPath);
            Assert.Contains(
                "| hybrid-storage-mode-scenario | qualified-scenario | 0.00% | " +
                "0.00% | 0.0000 ms | PASS |",
                report);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task PairedComparer_AcceptsPlan2LegacyProseAlongsideQualificationEvidence()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            ComparisonLayout layout = CreateLayout(
                temporaryRoot,
                HybridQualificationSuiteName);
            CreateBalancedEvidence(
                layout,
                (_, _) =>
                {
                    Measurement measurement = CreateHybridQualificationMeasurement();
                    return SingleRow(
                        "qualified-plan2-scenario",
                        measurement with
                        {
                            BaselineExtraInfo = Plan2LegacyProseExtraInfo,
                            CandidateExtraInfo = Plan2LegacyProseExtraInfo,
                        });
                });

            ProcessResult result = await RunComparerAsync(layout);

            Assert.True(result.ExitCode == 0, result.CombinedOutput);
            string report = File.ReadAllText(layout.ReportPath);
            Assert.Contains(
                "| hybrid-storage-mode-scenario | qualified-plan2-scenario | 0.00% | " +
                "0.00% | 0.0000 ms | PASS |",
                report);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Theory]
    [InlineData("missing-token", "missing required token 'qualification'")]
    [InlineData("malformed-token", "must contain a non-empty key and value")]
    [InlineData("wrong-case-token", "missing required token 'qualification'")]
    [InlineData("duplicate-token", "contains duplicate token 'qualification'")]
    [InlineData("qualification-false", "token 'qualification' must be 'true'")]
    [InlineData("warmup", "token 'unrecorded-warmup-seconds' must be 2")]
    [InlineData("minimum-duration", "token 'minimum-measured-seconds' must be 30")]
    [InlineData("minimum-samples", "token 'minimum-retained-latency-samples' must be 10000")]
    [InlineData("cap", "token 'measurement-cap-seconds' must be 120")]
    [InlineData("non-integer", "token 'measurement-cap-seconds' must be an integer")]
    [InlineData("invalid-timestamp", "must be a round-trip UTC timestamp")]
    [InlineData("non-utc-timestamp", "must be a round-trip UTC timestamp")]
    [InlineData("non-positive-interval", "measurement interval must be strictly positive")]
    [InlineData("elapsed-mismatch", "must match the UTC measurement interval within 1 ms")]
    [InlineData("elapsed-below-minimum", "ElapsedMs' must be at least 30000 ms")]
    [InlineData("elapsed-above-cap", "ElapsedMs' must not exceed 120000 ms")]
    [InlineData("samples-below-minimum", "declared minimum of 10000")]
    public async Task PairedComparer_RejectsIncompleteHybridQualificationEvidence(
        string defect,
        string expectedDiagnostic)
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            ComparisonLayout layout = CreateLayout(
                temporaryRoot,
                HybridQualificationSuiteName);
            CreateBalancedEvidence(
                layout,
                (_, _) => SingleRow(
                    "qualified-scenario",
                    CreateHybridQualificationMeasurement(defect)));

            ProcessResult result = await RunComparerAsync(layout);

            Assert.NotEqual(0, result.ExitCode);
            string report = File.ReadAllText(layout.ReportPath);
            Assert.Contains(
                "| hybrid-storage-mode-scenario | qualified-scenario | n/a | n/a | " +
                "n/a | INVALID |",
                report);
            Assert.Contains(expectedDiagnostic, report);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task PairedComparer_AllowsEffectsExactlyAtConfiguredLimits()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            ComparisonLayout layout = CreateLayout(temporaryRoot);
            CreateBalancedEvidence(
                layout,
                (_, _) => new Dictionary<string, Measurement>(StringComparer.Ordinal)
                {
                    ["throughput-boundary"] = new(100m, 85m, 1m, 1m),
                    ["p99-relative-boundary"] = new(100m, 100m, 1m, 1.25m),
                    ["p99-absolute-boundary"] = new(100m, 100m, 0.10m, 0.15m),
                });

            ProcessResult result = await RunComparerAsync(layout);

            Assert.True(result.ExitCode == 0, result.CombinedOutput);
            string report = File.ReadAllText(layout.ReportPath);
            Assert.Contains(
                "| suite | throughput-boundary | 15.00% | 0.00% | 0.0000 ms | PASS |",
                report);
            Assert.Contains(
                "| suite | p99-relative-boundary | 0.00% | 25.00% | 0.2500 ms | PASS |",
                report);
            Assert.Contains(
                "| suite | p99-absolute-boundary | 0.00% | 50.00% | 0.0500 ms | PASS |",
                report);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task PairedComparer_RejectsRawFilesSwappedBetweenPairIdentities()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            ComparisonLayout layout = CreateLayout(temporaryRoot);
            List<PairDefinition> pairs = CreateBalancedEvidence(
                layout,
                (_, _) => SingleRow("lookup", new(100m, 100m, 10m, 10m)));
            (string firstPath, string secondPath) =
                (pairs[0].BaselineRaw, pairs[1].BaselineRaw);
            pairs[0] = pairs[0] with { BaselineRaw = secondPath };
            pairs[1] = pairs[1] with { BaselineRaw = firstPath };
            WriteManifest(layout.ManifestPath, pairs);

            ProcessResult result = await RunComparerAsync(layout);

            Assert.NotEqual(0, result.ExitCode);
            string report = File.ReadAllText(layout.ReportPath);
            Assert.Contains("INVALID", report);
            Assert.Contains("does not identify pair", report);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task PairedComparer_RejectsNonAlternatingPairOrder()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            ComparisonLayout layout = CreateLayout(temporaryRoot);
            List<PairDefinition> pairs = CreateBalancedEvidence(
                layout,
                (_, _) => SingleRow("lookup", new(100m, 100m, 10m, 10m)));
            List<PairDefinition> groupedPairs = pairs
                .OrderBy(
                    pair => pair.Order == "previous-candidate" ? 0 : 1)
                .ThenBy(pair => pair.PairId, StringComparer.Ordinal)
                .ToList();
            WriteManifest(layout.ManifestPath, groupedPairs);

            ProcessResult result = await RunComparerAsync(layout);

            Assert.NotEqual(0, result.ExitCode);
            string report = File.ReadAllText(layout.ReportPath);
            Assert.Contains("INVALID", report);
            Assert.Contains("must alternate pair order", report);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task PairedComparer_RejectsUnreferencedRawEvidence()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            ComparisonLayout layout = CreateLayout(temporaryRoot);
            List<PairDefinition> pairs = CreateBalancedEvidence(
                layout,
                (_, _) => SingleRow("lookup", new(100m, 100m, 10m, 10m)));
            File.Copy(
                pairs[0].BaselineRaw,
                Path.Combine(layout.BaselineRawRoot, "orphan.csv"));

            ProcessResult result = await RunComparerAsync(layout);

            Assert.NotEqual(0, result.ExitCode);
            string report = File.ReadAllText(layout.ReportPath);
            Assert.Contains("INVALID", report);
            Assert.Contains("referenced exactly once by the manifest", report);
            Assert.Contains("orphan.csv", report);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task PairedComparer_RejectsAggregateSuiteMissingFromManifest()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            ComparisonLayout layout = CreateLayout(temporaryRoot);
            List<PairDefinition> pairs = CreateBalancedEvidence(
                layout,
                (_, _) => SingleRow("lookup", new(100m, 100m, 10m, 10m)));
            File.Copy(
                pairs[0].BaselineRaw,
                Path.Combine(layout.BaselineRoot, "omitted-suite.csv"));
            File.Copy(
                pairs[0].CandidateRaw,
                Path.Combine(layout.CandidateRoot, "omitted-suite.csv"));

            ProcessResult result = await RunComparerAsync(layout);

            Assert.NotEqual(0, result.ExitCode);
            string report = File.ReadAllText(layout.ReportPath);
            Assert.Contains("INVALID", report);
            Assert.Contains("suites do not match the aggregate result files", report);
            Assert.Contains("omitted-suite.csv", report);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task PairedComparer_RejectsNonCanonicalRawSchema()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            ComparisonLayout layout = CreateLayout(temporaryRoot);
            List<PairDefinition> pairs = CreateBalancedEvidence(
                layout,
                (_, _) => SingleRow("lookup", new(100m, 100m, 10m, 10m)));
            string[] malformedLines = File.ReadAllLines(pairs[0].BaselineRaw);
            malformedLines[0] += ",Unexpected";
            for (int index = 1; index < malformedLines.Length; index++)
                malformedLines[index] += ",value";
            File.WriteAllLines(pairs[0].BaselineRaw, malformedLines);

            ProcessResult result = await RunComparerAsync(layout);

            Assert.NotEqual(0, result.ExitCode);
            string report = File.ReadAllText(layout.ReportPath);
            Assert.Contains("INVALID", report);
            Assert.Contains("must contain exactly these columns", report);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    private static ComparisonLayout CreateLayout(
        string root,
        string suiteName = SuiteName)
    {
        string baseline = Directory.CreateDirectory(Path.Combine(root, "baseline")).FullName;
        string candidate = Directory.CreateDirectory(Path.Combine(root, "candidate")).FullName;
        string baselineRaw = Directory
            .CreateDirectory(Path.Combine(baseline, "raw", suiteName))
            .FullName;
        string candidateRaw = Directory
            .CreateDirectory(Path.Combine(candidate, "raw", suiteName))
            .FullName;
        return new(
            baseline,
            candidate,
            baselineRaw,
            candidateRaw,
            Path.Combine(root, "pairs.csv"),
            Path.Combine(root, "comparison.md"),
            suiteName);
    }

    private static List<PairDefinition> CreateBalancedEvidence(
        ComparisonLayout layout,
        Func<string, int, IReadOnlyDictionary<string, Measurement>> measurementFactory)
    {
        List<PairDefinition> pairs = [];
        for (int pairNumber = 1; pairNumber <= RepeatCount * 2; pairNumber++)
        {
            string order = pairNumber % 2 == 1
                ? "previous-candidate"
                : "candidate-previous";
            int orderIndex = (pairNumber - 1) / 2;
            string pairId = $"pair-{pairNumber:00}";
            string baselineRaw = Path.Combine(
                layout.BaselineRawRoot,
                $"{pairId}.csv");
            string candidateRaw = Path.Combine(
                layout.CandidateRawRoot,
                $"{pairId}.csv");
            IReadOnlyDictionary<string, Measurement> measurements =
                measurementFactory(order, orderIndex);
            WriteRawEvidence(baselineRaw, measurements, baseline: true);
            WriteRawEvidence(candidateRaw, measurements, baseline: false);
            string firstRevision = order == "previous-candidate"
                ? "previous"
                : "candidate";
            string secondRevision = order == "previous-candidate"
                ? "candidate"
                : "previous";
            pairs.Add(new(
                layout.SuiteName,
                pairId,
                order,
                firstRevision,
                secondRevision,
                baselineRaw,
                candidateRaw));
        }
        WriteManifest(layout.ManifestPath, pairs);
        File.Copy(
            pairs[0].BaselineRaw,
            Path.Combine(layout.BaselineRoot, $"{layout.SuiteName}.csv"),
            overwrite: true);
        File.Copy(
            pairs[0].CandidateRaw,
            Path.Combine(layout.CandidateRoot, $"{layout.SuiteName}.csv"),
            overwrite: true);
        return pairs;
    }

    private static Measurement CreateHybridQualificationMeasurement(
        string? defect = null)
    {
        string candidateExtraInfo = defect switch
        {
            null => ValidQualificationExtraInfo,
            "missing-token" => ValidQualificationExtraInfo.Replace(
                "qualification=true; ",
                string.Empty,
                StringComparison.Ordinal),
            "malformed-token" => ValidQualificationExtraInfo.Replace(
                "qualification=true",
                "qualification=",
                StringComparison.Ordinal),
            "wrong-case-token" => ValidQualificationExtraInfo.Replace(
                "qualification=true",
                "Qualification=true",
                StringComparison.Ordinal),
            "duplicate-token" =>
                ValidQualificationExtraInfo + "; qualification=true",
            "qualification-false" => ValidQualificationExtraInfo.Replace(
                "qualification=true",
                "qualification=false",
                StringComparison.Ordinal),
            "warmup" => ValidQualificationExtraInfo.Replace(
                "unrecorded-warmup-seconds=2",
                "unrecorded-warmup-seconds=1",
                StringComparison.Ordinal),
            "minimum-duration" => ValidQualificationExtraInfo.Replace(
                "minimum-measured-seconds=30",
                "minimum-measured-seconds=29",
                StringComparison.Ordinal),
            "minimum-samples" => ValidQualificationExtraInfo.Replace(
                "minimum-retained-latency-samples=10000",
                "minimum-retained-latency-samples=9999",
                StringComparison.Ordinal),
            "cap" => ValidQualificationExtraInfo.Replace(
                "measurement-cap-seconds=120",
                "measurement-cap-seconds=119",
                StringComparison.Ordinal),
            "non-integer" => ValidQualificationExtraInfo.Replace(
                "measurement-cap-seconds=120",
                "measurement-cap-seconds=120.0",
                StringComparison.Ordinal),
            "invalid-timestamp" => ValidQualificationExtraInfo.Replace(
                "2026-07-31T12:00:00.0000000+00:00",
                "not-a-timestamp",
                StringComparison.Ordinal),
            "non-utc-timestamp" => ValidQualificationExtraInfo.Replace(
                "2026-07-31T12:00:00.0000000+00:00",
                "2026-07-31T12:00:00.0000000-07:00",
                StringComparison.Ordinal),
            "non-positive-interval" => ValidQualificationExtraInfo.Replace(
                "2026-07-31T12:00:30.0000000+00:00",
                "2026-07-31T12:00:00.0000000+00:00",
                StringComparison.Ordinal),
            "elapsed-below-minimum" => ValidQualificationExtraInfo.Replace(
                "2026-07-31T12:00:30.0000000+00:00",
                "2026-07-31T12:00:29.0000000+00:00",
                StringComparison.Ordinal),
            "elapsed-above-cap" => ValidQualificationExtraInfo.Replace(
                "2026-07-31T12:00:30.0000000+00:00",
                "2026-07-31T12:02:01.0000000+00:00",
                StringComparison.Ordinal),
            "elapsed-mismatch" or "samples-below-minimum" =>
                ValidQualificationExtraInfo,
            _ => throw new ArgumentOutOfRangeException(nameof(defect), defect, null),
        };
        decimal candidateElapsedMs = defect switch
        {
            "elapsed-mismatch" => 30_002m,
            "elapsed-below-minimum" => 29_000m,
            "elapsed-above-cap" => 121_000m,
            _ => 30_000m,
        };
        int candidateSamples = defect == "samples-below-minimum" ? 9_999 : 10_000;

        return new(
            BaselineOps: 100m,
            CandidateOps: 100m,
            BaselineP99: 10m,
            CandidateP99: 10m,
            BaselineSamples: 10_000,
            CandidateSamples: candidateSamples,
            BaselineElapsedMs: 30_000m,
            CandidateElapsedMs: candidateElapsedMs,
            BaselineExtraInfo: ValidQualificationExtraInfo,
            CandidateExtraInfo: candidateExtraInfo);
    }

    private static IReadOnlyDictionary<string, Measurement> SingleRow(
        string name,
        Measurement measurement)
    {
        return new Dictionary<string, Measurement>(StringComparer.Ordinal)
        {
            [name] = measurement,
        };
    }

    private static void WriteLegacyEvidence(
        ComparisonLayout layout,
        string rowName,
        IReadOnlyList<Measurement> runs)
    {
        Assert.Equal(RepeatCount, runs.Count);
        for (int index = 0; index < runs.Count; index++)
        {
            IReadOnlyDictionary<string, Measurement> measurement = SingleRow(
                rowName,
                runs[index]);
            WriteRawEvidence(
                Path.Combine(layout.BaselineRawRoot, $"run-{index + 1}.csv"),
                measurement,
                baseline: true);
            WriteRawEvidence(
                Path.Combine(layout.CandidateRawRoot, $"run-{index + 1}.csv"),
                measurement,
                baseline: false);
        }

        const string aggregateTag = "Aggregate=median-of-3";
        File.WriteAllLines(
            Path.Combine(layout.BaselineRoot, $"{layout.SuiteName}.csv"),
            [
                EvidenceHeader,
                CreateEvidenceRow(rowName, 100m, 10m, 200, aggregateTag),
            ]);
        File.WriteAllLines(
            Path.Combine(layout.CandidateRoot, $"{layout.SuiteName}.csv"),
            [
                EvidenceHeader,
                CreateEvidenceRow(rowName, 100m, 10m, 200, aggregateTag),
            ]);
    }

    private static void WriteRawEvidence(
        string path,
        IReadOnlyDictionary<string, Measurement> measurements,
        bool baseline)
    {
        List<string> lines = [EvidenceHeader];
        foreach ((string name, Measurement measurement) in measurements.OrderBy(
                     entry => entry.Key,
                     StringComparer.Ordinal))
        {
            decimal ops = baseline ? measurement.BaselineOps : measurement.CandidateOps;
            decimal p99 = baseline ? measurement.BaselineP99 : measurement.CandidateP99;
            int samples = baseline
                ? measurement.BaselineSamples
                : measurement.CandidateSamples;
            decimal elapsedMs = baseline
                ? measurement.BaselineElapsedMs
                : measurement.CandidateElapsedMs;
            string extraInfo = baseline
                ? measurement.BaselineExtraInfo
                : measurement.CandidateExtraInfo;
            lines.Add(CreateEvidenceRow(
                name,
                ops,
                p99,
                samples,
                extraInfo,
                elapsedMs));
        }
        File.WriteAllLines(path, lines);
    }

    private static string CreateEvidenceRow(
        string name,
        decimal opsPerSecond,
        decimal p99,
        int latencySamples,
        string extraInfo = "Pair=raw",
        decimal elapsedMs = 10_000m)
    {
        return string.Join(
            ',',
            [
                ToCsvCell(name),
                "1000",
                latencySamples.ToString(CultureInfo.InvariantCulture),
                FormatNumber(elapsedMs),
                FormatNumber(opsPerSecond),
                "1",
                "2",
                "3",
                FormatNumber(p99),
                FormatNumber(p99),
                "0.1",
                "100",
                "1",
                "0.1",
                ToCsvCell(extraInfo),
            ]);
    }

    private static void WriteManifest(string path, IReadOnlyList<PairDefinition> pairs)
    {
        List<string> lines = [ManifestHeader];
        lines.AddRange(pairs.Select(pair => string.Join(
            ',',
            [
                ToCsvCell(pair.Suite),
                ToCsvCell(pair.PairId),
                ToCsvCell(pair.Order),
                ToCsvCell(pair.FirstRevision),
                ToCsvCell(pair.SecondRevision),
                ToCsvCell(pair.BaselineRaw),
                ToCsvCell(pair.CandidateRaw),
            ])));
        File.WriteAllLines(path, lines);
    }

    private static string ToCsvCell(string value)
    {
        return $"\"{value.Replace("\"", "\"\"", StringComparison.Ordinal)}\"";
    }

    private static string FormatNumber(decimal value)
    {
        return value.ToString(CultureInfo.InvariantCulture);
    }

    private static Task<ProcessResult> RunComparerAsync(ComparisonLayout layout)
    {
        return RunProcessAsync(
            "pwsh",
            "-NoLogo",
            "-NoProfile",
            "-File",
            Path.Combine(
                FindRepoRoot(),
                "tests",
                "CSharpDB.Benchmarks",
                "scripts",
                "Compare-ReleaseCore.ps1"),
            "-BaselineResultsPath",
            layout.BaselineRoot,
            "-CandidateResultsPath",
            layout.CandidateRoot,
            "-BaselineRawResultsPath",
            Path.GetDirectoryName(layout.BaselineRawRoot)!,
            "-CandidateRawResultsPath",
            Path.GetDirectoryName(layout.CandidateRawRoot)!,
            "-RepeatCount",
            RepeatCount.ToString(CultureInfo.InvariantCulture),
            "-ReportPath",
            layout.ReportPath,
            "-PairManifestPath",
            layout.ManifestPath);
    }

    private static Task<ProcessResult> RunLegacyComparerAsync(ComparisonLayout layout)
    {
        return RunProcessAsync(
            "pwsh",
            "-NoLogo",
            "-NoProfile",
            "-File",
            Path.Combine(
                FindRepoRoot(),
                "tests",
                "CSharpDB.Benchmarks",
                "scripts",
                "Compare-ReleaseCore.ps1"),
            "-BaselineResultsPath",
            layout.BaselineRoot,
            "-CandidateResultsPath",
            layout.CandidateRoot,
            "-BaselineRawResultsPath",
            Path.GetDirectoryName(layout.BaselineRawRoot)!,
            "-CandidateRawResultsPath",
            Path.GetDirectoryName(layout.CandidateRawRoot)!,
            "-RepeatCount",
            RepeatCount.ToString(CultureInfo.InvariantCulture),
            "-ReportPath",
            layout.ReportPath);
    }

    private static async Task<ProcessResult> RunProcessAsync(
        string fileName,
        params string[] arguments)
    {
        ProcessStartInfo startInfo = new()
        {
            FileName = fileName,
            RedirectStandardError = true,
            RedirectStandardOutput = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
        foreach (string argument in arguments)
            startInfo.ArgumentList.Add(argument);

        using Process process = new() { StartInfo = startInfo };
        Assert.True(process.Start(), $"Could not start {fileName}.");
        Task<string> standardOutput = process.StandardOutput.ReadToEndAsync();
        Task<string> standardError = process.StandardError.ReadToEndAsync();
        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(30));
        try
        {
            await process.WaitForExitAsync(timeout.Token);
        }
        catch (OperationCanceledException)
        {
            process.Kill(entireProcessTree: true);
            throw new TimeoutException($"{fileName} did not finish within 30 seconds.");
        }

        return new(
            process.ExitCode,
            await standardOutput,
            await standardError);
    }

    private static string CreateTemporaryRoot()
    {
        string path = Path.Combine(
            Path.GetTempPath(),
            "csharpdb-paired-comparer-tests",
            Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private static void DeleteTemporaryRoot(string path)
    {
        for (int attempt = 1; attempt <= 3; attempt++)
        {
            try
            {
                foreach (string entry in Directory.EnumerateFileSystemEntries(
                             path,
                             "*",
                             SearchOption.AllDirectories))
                {
                    File.SetAttributes(entry, FileAttributes.Normal);
                }
                File.SetAttributes(path, FileAttributes.Normal);
                Directory.Delete(path, recursive: true);
                return;
            }
            catch (IOException) when (attempt < 3)
            {
                Thread.Sleep(TimeSpan.FromMilliseconds(100));
            }
            catch (UnauthorizedAccessException) when (attempt < 3)
            {
                Thread.Sleep(TimeSpan.FromMilliseconds(100));
            }
        }
    }

    private static string FindRepoRoot()
    {
        DirectoryInfo? directory = new(AppContext.BaseDirectory);
        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "CSharpDB.slnx")))
                return directory.FullName;

            directory = directory.Parent;
        }
        throw new DirectoryNotFoundException(
            "Could not locate repository root from test base directory.");
    }

    private sealed record ComparisonLayout(
        string BaselineRoot,
        string CandidateRoot,
        string BaselineRawRoot,
        string CandidateRawRoot,
        string ManifestPath,
        string ReportPath,
        string SuiteName);

    private sealed record Measurement(
        decimal BaselineOps,
        decimal CandidateOps,
        decimal BaselineP99,
        decimal CandidateP99,
        int BaselineSamples = 200,
        int CandidateSamples = 200,
        decimal BaselineElapsedMs = 10_000m,
        decimal CandidateElapsedMs = 10_000m,
        string BaselineExtraInfo = "Pair=raw",
        string CandidateExtraInfo = "Pair=raw");

    private sealed record PairDefinition(
        string Suite,
        string PairId,
        string Order,
        string FirstRevision,
        string SecondRevision,
        string BaselineRaw,
        string CandidateRaw);

    private sealed record ProcessResult(
        int ExitCode,
        string StandardOutput,
        string StandardError)
    {
        public string CombinedOutput =>
            StandardOutput + Environment.NewLine + StandardError;
    }
}
