using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace CSharpDB.Benchmarks.Tests;

public sealed class ObservabilityPerformanceQualificationTests
{
    [Fact]
    public void DefaultPolicyFailsClosedUntilTracingCeilingsAreApproved()
    {
        using QualificationCase testCase = QualificationCase.Create();

        ProcessResult result = testCase.RunComparator();

        Assert.NotEqual(0, result.ExitCode);
        using JsonDocument attestation = testCase.ReadAttestation();
        Assert.Equal(
            "FAIL",
            attestation.RootElement.GetProperty("verdict").GetString());
        Assert.Contains(
            attestation.RootElement.GetProperty("findings").EnumerateArray(),
            static finding =>
                finding.GetProperty("code").GetString() ==
                    "thresholdDecisionRequired" &&
                finding.GetProperty("mode").GetString() == "SampledTracing");
        Assert.DoesNotContain(
            attestation.RootElement.GetProperty("findings").EnumerateArray(),
            static finding =>
                finding.GetProperty("code").GetString() ==
                    "confoundedModeConfiguration");
    }

    [Fact]
    public void ExactDocumentedBoundariesPassWhenTracingHasAnApprovedFixtureCeiling()
    {
        using QualificationCase testCase = QualificationCase.Create(
            ApproveTracingForFixture);

        ProcessResult result = testCase.RunComparator();

        Assert.True(result.ExitCode == 0, result.CombinedOutput);
        using JsonDocument attestation = testCase.ReadAttestation();
        Assert.Equal(
            "PASS",
            attestation.RootElement.GetProperty("verdict").GetString());
        Assert.True(
            attestation.RootElement.GetProperty("formalGateEligible").GetBoolean());
    }

    [Fact]
    public void MetricsElapsedRegressionFailsItsIndividualPath()
    {
        using QualificationCase testCase = QualificationCase.Create(
            ApproveTracingForFixture,
            evidence =>
            {
                foreach (int pairNumber in new[] { 2, 3 })
                {
                    JsonObject measurement = FindMeasurement(
                        evidence,
                        pairNumber,
                        role: "candidate",
                        pathId: "engine.sql-primary-key-fast-lookup",
                        mode: "MetricsOnly");
                    measurement["medianNanoseconds"] = 1134.01d;
                }
            });

        ProcessResult result = testCase.RunComparator();

        Assert.NotEqual(0, result.ExitCode);
        AssertFinding(
            testCase,
            "elapsedThresholdExceeded",
            "MetricsOnly",
            "engine.sql-primary-key-fast-lookup");
    }

    [Fact]
    public void MetricsAllocationRegressionFailsItsIndividualPath()
    {
        using QualificationCase testCase = QualificationCase.Create(
            ApproveTracingForFixture,
            evidence =>
            {
                foreach (int pairNumber in new[] { 2, 3 })
                {
                    JsonObject measurement = FindMeasurement(
                        evidence,
                        pairNumber,
                        role: "candidate",
                        pathId: "engine.preparsed-primary-key-lookup",
                        mode: "MetricsOnly");
                    measurement["allocatedBytes"] = 1065.01d;
                }
            });

        ProcessResult result = testCase.RunComparator();

        Assert.NotEqual(0, result.ExitCode);
        AssertFinding(
            testCase,
            "allocationThresholdExceeded",
            "MetricsOnly",
            "engine.preparsed-primary-key-lookup");
    }

    [Fact]
    public void HistoryUsesTheMaximumOfRelativeAndFixedAllowance()
    {
        using QualificationCase testCase = QualificationCase.Create(
            ApproveTracingForFixture,
            evidence =>
            {
                foreach (int pairNumber in new[] { 2, 3 })
                {
                    JsonObject measurement = FindMeasurement(
                        evidence,
                        pairNumber,
                        role: "candidate",
                        pathId: "engine.sql-primary-key-fast-lookup",
                        mode: "HistoryCapture");
                    measurement["medianNanoseconds"] = 2530.01d;
                }
            });

        ProcessResult result = testCase.RunComparator();

        Assert.NotEqual(0, result.ExitCode);
        AssertFinding(
            testCase,
            "elapsedThresholdExceeded",
            "HistoryCapture",
            "engine.sql-primary-key-fast-lookup");
    }

    [Fact]
    public void MedianOfThreeUsesPairedMarginsWithoutAveragingPaths()
    {
        using QualificationCase testCase = QualificationCase.Create(
            ApproveTracingForFixture,
            evidence =>
            {
                const string pathId = "engine.sql-primary-key-fast-lookup";
                SetRelativeCandidateTime(evidence, 1, pathId, "MetricsOnly", 10.2d);
                SetRelativeCandidateTime(evidence, 2, pathId, "MetricsOnly", 9.9d);
                SetRelativeCandidateTime(evidence, 3, pathId, "MetricsOnly", 9.9d);
            });

        ProcessResult result = testCase.RunComparator();

        Assert.True(result.ExitCode == 0, result.CombinedOutput);
    }

    [Fact]
    public void MissingOrDuplicateRowsFailClosed()
    {
        using QualificationCase missingCase = QualificationCase.Create(
            ApproveTracingForFixture,
            evidence =>
            {
                JsonArray measurements = FindRun(
                    evidence,
                    pairNumber: 1,
                    role: "candidate")["measurements"]!.AsArray();
                measurements.RemoveAt(measurements.Count - 1);
            });
        ProcessResult missingResult = missingCase.RunComparator();
        Assert.NotEqual(0, missingResult.ExitCode);
        AssertFinding(missingCase, "invalidEvidence");

        using QualificationCase duplicateCase = QualificationCase.Create(
            ApproveTracingForFixture,
            evidence =>
            {
                JsonArray measurements = FindRun(
                    evidence,
                    pairNumber: 1,
                    role: "candidate")["measurements"]!.AsArray();
                measurements[^1] = measurements[0]!.DeepClone();
            });
        ProcessResult duplicateResult = duplicateCase.RunComparator();
        Assert.NotEqual(0, duplicateResult.ExitCode);
        AssertFinding(duplicateCase, "invalidEvidence");
    }

    [Fact]
    public void LaunchSpreadOverFivePercentFailsEvenWhenMedianThresholdPasses()
    {
        using QualificationCase testCase = QualificationCase.Create(
            ApproveTracingForFixture,
            evidence =>
            {
                JsonObject measurement = FindMeasurement(
                    evidence,
                    pairNumber: 3,
                    role: "candidate",
                    pathId: "engine.sql-primary-key-fast-lookup",
                    mode: "MetricsOnly");
                measurement["medianNanoseconds"] = 1195d;
            });

        ProcessResult result = testCase.RunComparator();

        Assert.NotEqual(0, result.ExitCode);
        AssertFinding(
            testCase,
            "unstableLaunchSeries",
            "MetricsOnly",
            "engine.sql-primary-key-fast-lookup");
    }

    [Fact]
    public void MachineFingerprintAndArtifactHashesAreMandatory()
    {
        using QualificationCase fingerprintCase = QualificationCase.Create(
            ApproveTracingForFixture,
            evidence =>
            {
                FindRun(evidence, pairNumber: 3, role: "candidate")
                    ["machine"]!["powerProfile"] = "different-profile";
            });
        ProcessResult fingerprintResult = fingerprintCase.RunComparator();
        Assert.NotEqual(0, fingerprintResult.ExitCode);
        AssertFinding(fingerprintCase, "invalidEvidence");

        using QualificationCase hashCase = QualificationCase.Create(
            ApproveTracingForFixture,
            evidence =>
            {
                FindRun(evidence, pairNumber: 1, role: "reference")
                    ["artifacts"]![0]!["sha256"] = new string('0', 64);
            });
        ProcessResult hashResult = hashCase.RunComparator();
        Assert.NotEqual(0, hashResult.ExitCode);
        AssertFinding(hashCase, "invalidEvidence");
    }

    [Fact]
    public void PoolSignalRowsRemainExplicitCharacterization()
    {
        using QualificationCase testCase = QualificationCase.Create(
            ApproveTracingForFixture,
            evidence =>
            {
                foreach (int pairNumber in Enumerable.Range(1, 3))
                {
                    JsonObject measurement = FindMeasurement(
                        evidence,
                        pairNumber,
                        "candidate",
                        "pool.open-close-dispose",
                        "MetricsOnly");
                    measurement["medianNanoseconds"] = 10_000_000d;
                    measurement["allocatedBytes"] = 10_000_000d;
                }
            });

        ProcessResult result = testCase.RunComparator();

        Assert.True(result.ExitCode == 0, result.CombinedOutput);
        using JsonDocument attestation = testCase.ReadAttestation();
        JsonElement poolMetrics = Assert.Single(
            attestation.RootElement
                .GetProperty("comparisons")
                .EnumerateArray(),
            static comparison =>
                comparison.GetProperty("pathId").GetString() ==
                    "pool.open-close-dispose" &&
                comparison.GetProperty("mode").GetString() == "MetricsOnly");
        Assert.Equal(
            "CHARACTERIZATION",
            poolMetrics.GetProperty("status").GetString());
        Assert.False(poolMetrics.GetProperty("gate").GetBoolean());
    }

    [Fact]
    public void ApprovedTracingWithoutCompleteThresholdsStillFailsClosed()
    {
        using QualificationCase testCase = QualificationCase.Create(policy =>
        {
            JsonObject tracing = policy["modes"]!["SampledTracing"]!.AsObject();
            tracing["status"] = "approved";
        });

        ProcessResult result = testCase.RunComparator();

        Assert.NotEqual(0, result.ExitCode);
        AssertFinding(testCase, "invalidEvidence");
    }

    [Fact]
    public void WorkflowRequiresDedicatedRunnerAndRetainsAttestationArtifacts()
    {
        string workflow = File.ReadAllText(Path.Combine(
            QualificationCase.RepositoryRoot,
            ".github",
            "workflows",
            "observability-performance.yml"));
        string normalized = workflow.ReplaceLineEndings("\n");

        Assert.Contains(
            "runs-on: [self-hosted, Windows, X64, csharpdb-performance]",
            normalized,
            StringComparison.Ordinal);
        Assert.Contains(
            "Test-ObservabilityPerformance.ps1",
            normalized,
            StringComparison.Ordinal);
        Assert.Contains(
            "continue-on-error: true",
            normalized,
            StringComparison.Ordinal);
        Assert.Contains(
            "retention-days: 90",
            normalized,
            StringComparison.Ordinal);
        Assert.Contains(
            "observability-performance-attestation.json",
            normalized,
            StringComparison.Ordinal);
        Assert.Contains(
            "formalGateEligible",
            normalized,
            StringComparison.Ordinal);
        Assert.Contains(
            "reference_commit is not the reviewed reference allowlisted",
            normalized,
            StringComparison.Ordinal);
        Assert.Contains(
            "EVIDENCE_PATH: observability-performance-evidence-${{ github.run_id }}-${{ github.run_attempt }}",
            normalized,
            StringComparison.Ordinal);
        Assert.Contains(
            "The evidence manifest does not cover every retained file exactly once.",
            normalized,
            StringComparison.Ordinal);
    }

    private static void ApproveTracingForFixture(JsonObject policy)
    {
        JsonObject tracing = policy["modes"]!["SampledTracing"]!.AsObject();
        tracing["status"] = "approved";
        tracing["elapsedAllowance"] = "relative";
        tracing["maxElapsedPercent"] = 100d;
        tracing["maxAdditionalAllocatedBytes"] = 4096d;
    }

    private static void SetRelativeCandidateTime(
        JsonObject evidence,
        int pairNumber,
        string pathId,
        string mode,
        double relativePercent)
    {
        JsonObject disabled = FindMeasurement(
            evidence,
            pairNumber,
            "candidate",
            pathId,
            "Disabled");
        JsonObject candidate = FindMeasurement(
            evidence,
            pairNumber,
            "candidate",
            pathId,
            mode);
        candidate["medianNanoseconds"] =
            disabled["medianNanoseconds"]!.GetValue<double>() *
            (1d + relativePercent / 100d);
    }

    private static JsonObject FindRun(
        JsonObject evidence,
        int pairNumber,
        string role)
        => Assert.Single(
            evidence["runs"]!.AsArray().Select(static node => node!.AsObject()),
            run =>
                run["pairNumber"]!.GetValue<int>() == pairNumber &&
                run["role"]!.GetValue<string>() == role);

    private static JsonObject FindMeasurement(
        JsonObject evidence,
        int pairNumber,
        string role,
        string pathId,
        string mode)
        => Assert.Single(
            FindRun(evidence, pairNumber, role)
                ["measurements"]!
                .AsArray()
                .Select(static node => node!.AsObject()),
            measurement =>
                measurement["pathId"]!.GetValue<string>() == pathId &&
                measurement["mode"]!.GetValue<string>() == mode);

    private static void AssertFinding(
        QualificationCase testCase,
        string code,
        string? mode = null,
        string? pathId = null)
    {
        using JsonDocument attestation = testCase.ReadAttestation();
        Assert.Contains(
            attestation.RootElement.GetProperty("findings").EnumerateArray(),
            finding =>
                finding.GetProperty("code").GetString() == code &&
                (mode is null || finding.GetProperty("mode").GetString() == mode) &&
                (pathId is null ||
                    finding.GetProperty("pathId").GetString() == pathId));
    }

    private sealed class QualificationCase : IDisposable
    {
        private static readonly JsonSerializerOptions JsonOptions = new()
        {
            WriteIndented = true,
        };

        private QualificationCase(string root)
        {
            Root = root;
        }

        public static string RepositoryRoot { get; } = FindRepositoryRoot();

        private string Root { get; }

        private string PolicyPath => Path.Combine(Root, "policy.json");

        private string EvidencePath => Path.Combine(Root, "evidence.json");

        private string AttestationPath => Path.Combine(Root, "attestation.json");

        private string ReportPath => Path.Combine(Root, "report.md");

        public string ReportPathForAssertions => ReportPath;

        public static QualificationCase Create(
            Action<JsonObject>? mutatePolicy = null,
            Action<JsonObject>? mutateEvidence = null)
        {
            string root = Path.Combine(
                Path.GetTempPath(),
                $"csharpdb-observability-qualifier-{Guid.NewGuid():N}");
            Directory.CreateDirectory(root);
            var testCase = new QualificationCase(root);

            string canonicalPolicyPath = Path.Combine(
                RepositoryRoot,
                "tests",
                "CSharpDB.Benchmarks",
                "observability-perf-thresholds.json");
            JsonObject policy = JsonNode.Parse(
                File.ReadAllText(canonicalPolicyPath))!.AsObject();
            policy["reference"]!["status"] = "approved";
            policy["reference"]!["commit"] = new string('a', 40);
            mutatePolicy?.Invoke(policy);
            WriteJson(testCase.PolicyPath, policy);

            JsonObject evidence = testCase.CreateEvidence(policy);
            mutateEvidence?.Invoke(evidence);
            WriteJson(testCase.EvidencePath, evidence);
            return testCase;
        }

        public ProcessResult RunComparator()
        {
            string scriptPath = Path.Combine(
                RepositoryRoot,
                "tests",
                "CSharpDB.Benchmarks",
                "scripts",
                "Compare-ObservabilityPerformance.ps1");
            var startInfo = new ProcessStartInfo
            {
                FileName = "pwsh",
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true,
            };
            foreach (string argument in new[]
            {
                "-NoLogo",
                "-NoProfile",
                "-File",
                scriptPath,
                "-EvidencePath",
                EvidencePath,
                "-ThresholdsPath",
                PolicyPath,
                "-OutputJsonPath",
                AttestationPath,
                "-OutputMarkdownPath",
                ReportPath,
            })
            {
                startInfo.ArgumentList.Add(argument);
            }

            using Process process = Process.Start(startInfo) ??
                throw new InvalidOperationException("Could not start pwsh.");
            string standardOutput = process.StandardOutput.ReadToEnd();
            string standardError = process.StandardError.ReadToEnd();
            Assert.True(
                process.WaitForExit(30_000),
                "The observability comparator fixture timed out.");
            return new ProcessResult(
                process.ExitCode,
                standardOutput,
                standardError);
        }

        public JsonDocument ReadAttestation()
            => JsonDocument.Parse(File.ReadAllText(AttestationPath));

        public void Dispose()
        {
            try
            {
                Directory.Delete(Root, recursive: true);
            }
            catch
            {
            }
        }

        private JsonObject CreateEvidence(JsonObject policy)
        {
            JsonArray paths = policy["paths"]!.AsArray();
            string[] modes = policy["qualification"]!["candidateModeOrder"]!
                .AsArray()
                .Select(static value => value!.GetValue<string>())
                .ToArray();
            var runs = new JsonArray();
            DateTimeOffset origin = new(
                2026,
                8,
                12,
                12,
                0,
                0,
                TimeSpan.Zero);
            for (int pairNumber = 1; pairNumber <= 3; pairNumber++)
            {
                runs.Add(CreateRun(
                    paths,
                    modes,
                    pairNumber,
                    "reference",
                    new string('a', 40),
                    origin.AddMinutes(pairNumber * 10),
                    reference: true));
                runs.Add(CreateRun(
                    paths,
                    modes,
                    pairNumber,
                    "candidate",
                    new string('b', 40),
                    origin.AddMinutes(pairNumber * 10).AddMinutes(1).AddSeconds(1),
                    reference: false));
            }

            string environmentRelativePath = "environment/summary.txt";
            string environmentPath = Path.Combine(
                Root,
                environmentRelativePath.Replace('/', Path.DirectorySeparatorChar));
            Directory.CreateDirectory(Path.GetDirectoryName(environmentPath)!);
            File.WriteAllText(environmentPath, "Result=PASS\n");

            string scriptsRoot = Path.Combine(
                RepositoryRoot,
                "tests",
                "CSharpDB.Benchmarks",
                "scripts");
            return new JsonObject
            {
                ["schemaVersion"] = 1,
                ["evidenceKind"] =
                    "csharpdb.observability-performance.paired",
                ["generatedUtc"] = origin.ToString("O"),
                ["policyId"] = policy["policyId"]!.GetValue<string>(),
                ["policySha256"] = HashFile(PolicyPath),
                ["referenceCommit"] = new string('a', 40),
                ["candidateCommit"] = new string('b', 40),
                ["configuration"] = "Release",
                ["producer"] = new JsonObject
                {
                    ["runnerSha256"] = HashFile(Path.Combine(
                        scriptsRoot,
                        "Test-ObservabilityPerformance.ps1")),
                    ["comparatorSha256"] = HashFile(Path.Combine(
                        scriptsRoot,
                        "Compare-ObservabilityPerformance.ps1")),
                    ["environmentMonitorSha256"] = HashFile(Path.Combine(
                        scriptsRoot,
                        "Watch-LocalPerformanceEnvironment.ps1")),
                    ["referenceBenchmarkSourceSha256"] = HashText(
                        "reference-source"),
                    ["candidateBenchmarkSourceSha256"] = HashText(
                        "candidate-source"),
                },
                ["environment"] = new JsonObject
                {
                    ["status"] = "PASS",
                    ["artifacts"] = new JsonArray
                    {
                        new JsonObject
                        {
                            ["relativePath"] = environmentRelativePath,
                            ["sha256"] = HashFile(environmentPath),
                        },
                    },
                },
                ["runs"] = runs,
            };
        }

        private JsonObject CreateRun(
            JsonArray paths,
            string[] modes,
            int pairNumber,
            string role,
            string commit,
            DateTimeOffset started,
            bool reference)
        {
            string relativePath = $"raw/pair-{pairNumber}-{role}.csv";
            string artifactPath = Path.Combine(
                Root,
                relativePath.Replace('/', Path.DirectorySeparatorChar));
            Directory.CreateDirectory(Path.GetDirectoryName(artifactPath)!);
            File.WriteAllText(artifactPath, $"fixture,{pairNumber},{role}\n");

            var measurements = new JsonArray();
            for (int pathIndex = 0; pathIndex < paths.Count; pathIndex++)
            {
                JsonObject path = paths[pathIndex]!.AsObject();
                double detachedTime = pathIndex == 0
                    ? 1000d
                    : 2000d + pathIndex * 1000d;
                double detachedAllocation = 1000d + pathIndex;
                if (reference)
                {
                    measurements.Add(CreateMeasurement(
                        path,
                        "DetachedReference",
                        detachedTime,
                        detachedAllocation));
                    continue;
                }

                double disabledTime = detachedTime * 1.03d;
                double disabledAllocation = detachedAllocation;
                foreach (string mode in modes)
                {
                    (double time, double allocation) = mode switch
                    {
                        "Disabled" => (disabledTime, disabledAllocation),
                        "HistoryCapture" => (
                            disabledTime + Math.Max(disabledTime * 0.20d, 1500d),
                            disabledAllocation + 1024d),
                        "StructuredLogging" => (
                            disabledTime * 2d,
                            disabledAllocation + 2048d),
                        "MetricsOnly" => (
                            disabledTime * 1.10d,
                            disabledAllocation + 64d),
                        "SampledTracing" => (
                            disabledTime * 1.50d,
                            disabledAllocation + 4096d),
                        _ => throw new InvalidOperationException(
                            $"Unexpected fixture mode '{mode}'."),
                    };
                    measurements.Add(CreateMeasurement(
                        path,
                        mode,
                        time,
                        allocation));
                }
            }

            return new JsonObject
            {
                ["pairNumber"] = pairNumber,
                ["role"] = role,
                ["commit"] = commit,
                ["configuration"] = "Release",
                ["startedUtc"] = started.ToString("O"),
                ["completedUtc"] = started.AddMinutes(1).ToString("O"),
                ["machine"] = new JsonObject
                {
                    ["runnerId"] = "fixture-runner",
                    ["machineName"] = "fixture-machine",
                    ["cpuName"] = "fixture-cpu",
                    ["logicalCoreCount"] = "8",
                    ["osDescription"] = "fixture-os",
                    ["osArchitecture"] = "X64",
                    ["dotnetSdk"] = "10.0.203",
                    ["dotnetRuntime"] = "Microsoft.NETCore.App 10.0.10",
                    ["powerProfile"] = "fixture-fixed-power",
                },
                ["benchmark"] = new JsonObject
                {
                    ["warmupCount"] = 3,
                    ["iterationCount"] = 10,
                    ["launchCount"] = 1,
                },
                ["artifacts"] = new JsonArray
                {
                    new JsonObject
                    {
                        ["relativePath"] = relativePath,
                        ["sha256"] = HashFile(artifactPath),
                    },
                },
                ["measurements"] = measurements,
            };
        }

        private static JsonObject CreateMeasurement(
            JsonObject path,
            string mode,
            double medianNanoseconds,
            double allocatedBytes)
            => new()
            {
                ["pathId"] = path["id"]!.GetValue<string>(),
                ["method"] = path["method"]!.GetValue<string>(),
                ["suite"] = path["suite"]!.GetValue<string>(),
                ["mode"] = mode,
                ["medianNanoseconds"] = medianNanoseconds,
                ["allocatedBytes"] = allocatedBytes,
            };

        private static void WriteJson(string path, JsonObject value)
            => File.WriteAllText(path, value.ToJsonString(JsonOptions));

        private static string HashFile(string path)
            => Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(path)))
                .ToLowerInvariant();

        private static string HashText(string value)
            => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value)))
                .ToLowerInvariant();

        private static string FindRepositoryRoot()
        {
            DirectoryInfo? directory = new(AppContext.BaseDirectory);
            while (directory is not null)
            {
                if (File.Exists(Path.Combine(directory.FullName, "CSharpDB.slnx")))
                    return directory.FullName;
                directory = directory.Parent;
            }

            throw new DirectoryNotFoundException(
                "Could not locate the repository root.");
        }
    }

    private sealed record ProcessResult(
        int ExitCode,
        string StandardOutput,
        string StandardError)
    {
        public string CombinedOutput =>
            StandardOutput + Environment.NewLine + StandardError;
    }
}
