using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;

namespace CSharpDB.Daemon.Tests;

public sealed class PreviousReleasePairedRunnerScriptTests
{
    [Theory]
    [InlineData(1, "previous", "candidate")]
    [InlineData(2, "candidate", "previous")]
    public async Task PairedPreflight_SchedulesBalancedAdjacentPairs(
        int qualificationPass,
        string firstRevision,
        string secondRevision)
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            TestRepository repository = await CreateTestRepositoryAsync(temporaryRoot);
            string evidence = Path.Combine(temporaryRoot, $"preflight-pass-{qualificationPass}");

            ProcessResult result = await RunProcessAsync(
                "pwsh",
                "-NoLogo",
                "-NoProfile",
                "-File",
                repository.RunnerScript,
                "-PreviousRef",
                "v4.3.0",
                "-CandidateRef",
                "HEAD",
                "-OutputPath",
                evidence,
                "-QualificationPass",
                qualificationPass.ToString(),
                "-RepeatCount",
                "3",
                "-Paired",
                "-SuiteName",
                "master-table",
                "-PreflightOnly");

            Assert.True(result.ExitCode == 0, result.CombinedOutput);
            string preflight = File.ReadAllText(
                Path.Combine(evidence, "previous-release-performance-preflight.md"));
            Assert.Contains("- Execution strategy: balanced-paired-raw-repeats", preflight);
            Assert.Contains("- Suite order: master-table", preflight);
            Assert.Contains(
                "- Paired repeats per order: 3 (total pairs per suite: 6; " +
                "recorded samples per revision: 6)",
                preflight);
            Assert.Contains("- Planned pair manifest: `", preflight);
            Assert.Contains("- Planned paired benchmark artifact manifest: `", preflight);

            string executionLine = Assert.Single(
                File.ReadLines(Path.Combine(
                    evidence,
                    "previous-release-performance-preflight.md")),
                line => line.StartsWith("- Execution order: ", StringComparison.Ordinal));
            string[] scheduledEntries = executionLine["- Execution order: ".Length..]
                .Split(", ", StringSplitOptions.RemoveEmptyEntries);
            Assert.Equal(12, scheduledEntries.Length);

            for (int pairIndex = 0; pairIndex < 6; pairIndex++)
            {
                bool usesStartingOrder = pairIndex % 2 == 0;
                string expectedFirst = usesStartingOrder ? firstRevision : secondRevision;
                string expectedSecond = usesStartingOrder ? secondRevision : firstRevision;
                string pairId = $"pair-{pairIndex + 1:00}";
                Assert.Equal(
                    $"master-table/{pairId}/{expectedFirst}",
                    scheduledEntries[pairIndex * 2]);
                Assert.Equal(
                    $"master-table/{pairId}/{expectedSecond}",
                    scheduledEntries[(pairIndex * 2) + 1]);
            }
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task SameRevision_RequiresExplicitOptIn()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            TestRepository repository = await CreateTestRepositoryAsync(temporaryRoot);

            ProcessResult rejected = await RunProcessAsync(
                "pwsh",
                "-NoLogo",
                "-NoProfile",
                "-File",
                repository.RunnerScript,
                "-PreviousRef",
                "HEAD",
                "-CandidateRef",
                "HEAD",
                "-OutputPath",
                Path.Combine(temporaryRoot, "same-revision-rejected"),
                "-RepeatCount",
                "3",
                "-Paired",
                "-SuiteName",
                "master-table",
                "-PreflightOnly");

            Assert.NotEqual(0, rejected.ExitCode);
            Assert.Contains(
                "Previous and candidate refs resolve to the same commit",
                rejected.CombinedOutput);

            string acceptedEvidence = Path.Combine(temporaryRoot, "same-revision-accepted");
            ProcessResult accepted = await RunProcessAsync(
                "pwsh",
                "-NoLogo",
                "-NoProfile",
                "-File",
                repository.RunnerScript,
                "-PreviousRef",
                "HEAD",
                "-CandidateRef",
                "HEAD",
                "-OutputPath",
                acceptedEvidence,
                "-RepeatCount",
                "3",
                "-Paired",
                "-SuiteName",
                "master-table",
                "-AllowSameRevision",
                "-PreflightOnly");

            Assert.True(accepted.ExitCode == 0, accepted.CombinedOutput);
            Assert.Contains(
                "- Execution strategy: balanced-paired-raw-repeats",
                File.ReadAllText(Path.Combine(
                    acceptedEvidence,
                    "previous-release-performance-preflight.md")));
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task DiagnosticOptions_RequireTheirPairedSameRevisionPrerequisites()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            TestRepository repository = await CreateTestRepositoryAsync(temporaryRoot);

            ProcessResult scenarioWithoutPaired = await RunProcessAsync(
                "pwsh",
                "-NoLogo",
                "-NoProfile",
                "-File",
                repository.RunnerScript,
                "-HybridStorageScenarioName",
                "Storage_FileBacked_Sql_SingleInsert_5s",
                "-PreflightOnly");
            Assert.NotEqual(0, scenarioWithoutPaired.ExitCode);
            Assert.Contains(
                "HybridStorageScenarioName is valid only with -Paired",
                scenarioWithoutPaired.CombinedOutput);

            ProcessResult unknownScenario = await RunProcessAsync(
                "pwsh",
                "-NoLogo",
                "-NoProfile",
                "-File",
                repository.RunnerScript,
                "-Paired",
                "-HybridStorageScenarioName",
                "not-a-real-hybrid-storage-scenario",
                "-PreflightOnly");
            Assert.NotEqual(0, unknownScenario.ExitCode);
            Assert.Contains(
                "Unknown hybrid storage scenario",
                unknownScenario.CombinedOutput);

            ProcessResult sharingWithoutPaired = await RunProcessAsync(
                "pwsh",
                "-NoLogo",
                "-NoProfile",
                "-File",
                repository.RunnerScript,
                "-ShareSameRevisionArtifact",
                "-AllowSameRevision",
                "-PreflightOnly");
            Assert.NotEqual(0, sharingWithoutPaired.ExitCode);
            Assert.Contains(
                "ShareSameRevisionArtifact is valid only with -Paired",
                sharingWithoutPaired.CombinedOutput);

            ProcessResult sharingWithoutOptIn = await RunProcessAsync(
                "pwsh",
                "-NoLogo",
                "-NoProfile",
                "-File",
                repository.RunnerScript,
                "-Paired",
                "-ShareSameRevisionArtifact",
                "-PreflightOnly");
            Assert.NotEqual(0, sharingWithoutOptIn.ExitCode);
            Assert.Contains(
                "ShareSameRevisionArtifact requires -AllowSameRevision",
                sharingWithoutOptIn.CombinedOutput);

            ProcessResult sharingDifferentRevisions = await RunProcessAsync(
                "pwsh",
                "-NoLogo",
                "-NoProfile",
                "-File",
                repository.RunnerScript,
                "-PreviousRef",
                "v4.3.0",
                "-CandidateRef",
                "HEAD",
                "-OutputPath",
                Path.Combine(temporaryRoot, "different-revisions"),
                "-Paired",
                "-AllowSameRevision",
                "-ShareSameRevisionArtifact",
                "-PreflightOnly");
            Assert.NotEqual(0, sharingDifferentRevisions.ExitCode);
            AssertDiagnosticContains(
                "requires previous and candidate refs to resolve to the same commit",
                sharingDifferentRevisions.CombinedOutput);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task HybridScenarioPreflight_ReplacesSuiteSelectionAndSchedulesFivePairsPerOrder()
    {
        const string scenarioName = "Storage_FileBacked_Sql_SingleInsert_5s";
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            TestRepository repository = await CreateTestRepositoryAsync(temporaryRoot);
            string evidence = Path.Combine(temporaryRoot, "scenario-preflight");

            ProcessResult result = await RunProcessAsync(
                "pwsh",
                "-NoLogo",
                "-NoProfile",
                "-File",
                repository.RunnerScript,
                "-PreviousRef",
                "v4.3.0",
                "-CandidateRef",
                "HEAD",
                "-OutputPath",
                evidence,
                "-RepeatCount",
                "5",
                "-Paired",
                "-SuiteName",
                "master-table",
                "-HybridStorageScenarioName",
                scenarioName,
                "-PostBuildQuiescenceSeconds",
                "17",
                "-PreflightOnly");

            Assert.True(result.ExitCode == 0, result.CombinedOutput);
            string preflight = File.ReadAllText(
                Path.Combine(evidence, "previous-release-performance-preflight.md"));
            Assert.Contains("- Suite order: hybrid-storage-mode-scenario", preflight);
            Assert.DoesNotContain("- Suite order: master-table", preflight);
            Assert.Contains($"- Hybrid storage scenario: `{scenarioName}`", preflight);
            Assert.Contains(
                "- Paired repeats per order: 5 (total pairs per suite: 10; " +
                "recorded samples per revision: 10)",
                preflight);
            Assert.Contains(
                "measurements will wait 17 second(s)",
                preflight);

            string executionLine = Assert.Single(
                File.ReadLines(Path.Combine(
                    evidence,
                    "previous-release-performance-preflight.md")),
                line => line.StartsWith("- Execution order: ", StringComparison.Ordinal));
            string[] scheduledEntries = executionLine["- Execution order: ".Length..]
                .Split(", ", StringSplitOptions.RemoveEmptyEntries);
            Assert.Equal(20, scheduledEntries.Length);
            Assert.All(
                scheduledEntries,
                entry => Assert.StartsWith("hybrid-storage-mode-scenario/pair-", entry));
            Assert.Equal(
                5,
                scheduledEntries
                    .Chunk(2)
                    .Count(pair => pair[0].EndsWith("/previous", StringComparison.Ordinal)));
            Assert.Equal(
                5,
                scheduledEntries
                    .Chunk(2)
                    .Count(pair => pair[0].EndsWith("/candidate", StringComparison.Ordinal)));
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task SharedArtifactScenarioRunner_UsesOneCandidateArtifactForBothLabels()
    {
        const string scenarioName = "Storage_FileBacked_Sql_SingleInsert_5s";
        const string artifactPayload = "fake release-core benchmark artifact:candidate-source";
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            TestRepository repository = await CreateTestRepositoryAsync(temporaryRoot);
            string fakeToolRoot = Path.Combine(temporaryRoot, "fake-tools");
            CreatePairedFakeDotnetTool(fakeToolRoot);
            string invocationLog = Path.Combine(temporaryRoot, "fake-dotnet.log");
            Dictionary<string, string> environment = new()
            {
                ["PATH"] = fakeToolRoot + Path.PathSeparator +
                    (Environment.GetEnvironmentVariable("PATH") ?? string.Empty),
                ["FAKE_DOTNET_LOG"] = invocationLog,
            };
            string evidence = Path.Combine(temporaryRoot, "shared-scenario-evidence");

            ProcessResult result = await RunProcessWithEnvironmentAsync(
                "pwsh",
                environment,
                "-NoLogo",
                "-NoProfile",
                "-File",
                repository.RunnerScript,
                "-PreviousRef",
                "HEAD",
                "-CandidateRef",
                "HEAD",
                "-OutputPath",
                evidence,
                "-RepeatCount",
                "5",
                "-Paired",
                "-AllowSameRevision",
                "-ShareSameRevisionArtifact",
                "-HybridStorageScenarioName",
                scenarioName,
                "-PostBuildQuiescenceSeconds",
                "1");

            Assert.True(result.ExitCode == 0, result.CombinedOutput);
            string[] invocations = File.ReadAllLines(invocationLog);
            string build = Assert.Single(
                invocations,
                line => line.Contains("|build", StringComparison.Ordinal) &&
                    !line.Contains("|build-server|", StringComparison.Ordinal));
            Assert.Equal("candidate-source|build", build);
            Assert.Single(
                invocations,
                line => line.EndsWith("|build-server|shutdown", StringComparison.Ordinal));
            int shutdownIndex = Array.FindIndex(
                invocations,
                line => line.EndsWith("|build-server|shutdown", StringComparison.Ordinal));
            int buildIndex = Array.IndexOf(invocations, build);

            string[] runs = invocations
                .Where(line => line.Contains("|run|", StringComparison.Ordinal))
                .ToArray();
            Assert.Equal(20, runs.Length);
            int firstRunIndex = Array.FindIndex(
                invocations,
                line => line.Contains("|run|", StringComparison.Ordinal));
            Assert.True(buildIndex < shutdownIndex, "The shared artifact must be built before shutdown.");
            Assert.True(shutdownIndex < firstRunIndex, "Measurements must begin after shutdown.");
            Assert.DoesNotContain(
                invocations.Skip(shutdownIndex + 1),
                line => line.Contains("|build", StringComparison.Ordinal) ||
                    line.Contains("|mode=project|", StringComparison.Ordinal));
            Assert.All(
                runs,
                run =>
                {
                    Assert.StartsWith("candidate-source|run|", run);
                    Assert.Contains("|mode=direct|", run);
                    Assert.Contains("|hybrid-storage-mode-scenario|", run);
                    Assert.Contains($"|scenario={scenarioName}|", run);
                    Assert.Contains("|repeat=1|warmup=True|", run);
                    Assert.Contains("|artifact=", run);
                    Assert.DoesNotContain("|project=", run);
                    Assert.Contains("candidate-source", run);
                    Assert.DoesNotContain("baseline-source", run);
                });
            string invokedArtifact = Assert.Single(
                runs.Select(run => run[(run.IndexOf("|artifact=", StringComparison.Ordinal) +
                    "|artifact=".Length)..]).Distinct(StringComparer.Ordinal));
            Assert.Equal(
                Path.GetFullPath(Path.Combine(
                    evidence,
                    "candidate-source",
                    "tests",
                    "CSharpDB.Benchmarks",
                    "bin",
                    "Release",
                    "net10.0",
                    "CSharpDB.Benchmarks.dll")),
                invokedArtifact);

            string manifestPath = Path.Combine(evidence, "logs", "paired-execution.csv");
            string[] manifestLines = File.ReadAllLines(manifestPath);
            Assert.Equal(11, manifestLines.Length);
            Assert.Equal(
                5,
                manifestLines.Skip(1).Count(
                    line => line.Contains(",previous-candidate,", StringComparison.Ordinal)));
            Assert.Equal(
                5,
                manifestLines.Skip(1).Count(
                    line => line.Contains(",candidate-previous,", StringComparison.Ordinal)));
            AssertRawDigestManifest(evidence, expectedFileCount: 20);

            string expectedArtifactHash = Convert
                .ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(artifactPayload)))
                .ToLowerInvariant();
            string artifactIdentityManifest = Path.Combine(
                evidence,
                "logs",
                "paired-benchmark-artifacts.sha256");
            Assert.Equal("true", ReadManifestValue(
                artifactIdentityManifest,
                "SharedSameRevisionArtifact"));
            Assert.Equal(invokedArtifact, ReadManifestValue(
                artifactIdentityManifest,
                "PreviousArtifactPath"));
            Assert.Equal(invokedArtifact, ReadManifestValue(
                artifactIdentityManifest,
                "CandidateArtifactPath"));
            Assert.Equal(expectedArtifactHash, ReadManifestValue(
                artifactIdentityManifest,
                "PreviousArtifactSha256"));
            Assert.Equal(expectedArtifactHash, ReadManifestValue(
                artifactIdentityManifest,
                "CandidateArtifactSha256"));
            string expectedClosureHash = GetFakeClosureHash("candidate-source");
            Assert.Equal("7", ReadManifestValue(
                artifactIdentityManifest,
                "PreviousClosureFileCount"));
            Assert.Equal("7", ReadManifestValue(
                artifactIdentityManifest,
                "CandidateClosureFileCount"));
            Assert.Equal(expectedClosureHash, ReadManifestValue(
                artifactIdentityManifest,
                "PreviousClosureSha256"));
            Assert.Equal(expectedClosureHash, ReadManifestValue(
                artifactIdentityManifest,
                "CandidateClosureSha256"));
            AssertArtifactClosureRecords(
                artifactIdentityManifest,
                "Previous",
                "candidate-source");
            AssertArtifactClosureRecords(
                artifactIdentityManifest,
                "Candidate",
                "candidate-source");
            string preflight = File.ReadAllText(Path.Combine(
                evidence,
                "previous-release-performance-preflight.md"));
            string report = File.ReadAllText(Path.Combine(
                evidence,
                "previous-release-performance.md"));
            foreach (string document in new[] { preflight, report })
            {
                Assert.Contains("- Same-revision artifact sharing: enabled", document);
                Assert.Contains("- Shared benchmark artifact path: `", document);
                Assert.Contains("candidate-source", document);
                Assert.Contains(
                    $"- Shared benchmark artifact SHA-256: `{expectedArtifactHash}`",
                    document);
                Assert.Contains(
                    $"- Previous benchmark artifact execution path: `{invokedArtifact}`",
                    document);
                Assert.Contains(
                    $"- Candidate benchmark artifact execution path: `{invokedArtifact}`",
                    document);
                Assert.Contains(
                    $"runnable closure: 7 files; SHA-256 `{expectedClosureHash}`",
                    document);
                Assert.Contains("paired-benchmark-artifacts.sha256", document);
                Assert.Contains("measurements will wait 1 second(s)", document);
                Assert.Contains("paired-raw-evidence.sha256", document);
                Assert.Contains("20 files verified", document);
                Assert.Contains("Previous effective build-input SHA-256", document);
                Assert.Contains("Candidate effective build-input SHA-256", document);
                Assert.Contains("identities are revision-specific", document);
            }

            string previousBuildInputsManifest = Path.Combine(
                evidence,
                "logs",
                "previous-effective-build-inputs.sha256");
            string candidateBuildInputsManifest = Path.Combine(
                evidence,
                "logs",
                "candidate-effective-build-inputs.sha256");
            Assert.Contains("*Directory.Build.props", File.ReadAllText(previousBuildInputsManifest));
            Assert.Contains("*Directory.Build.props", File.ReadAllText(candidateBuildInputsManifest));
            Assert.Equal(
                ReadManifestValue(previousBuildInputsManifest, "BuildInputsSha256"),
                ReadManifestValue(candidateBuildInputsManifest, "BuildInputsSha256"));

            Assert.Contains("- Result: **PASS**", report);
            string baselineAggregate = Path.Combine(
                evidence,
                "baseline-results",
                "hybrid-storage-mode-scenario.csv");
            string candidateAggregate = Path.Combine(
                evidence,
                "candidate-results",
                "hybrid-storage-mode-scenario.csv");
            Assert.Contains("Aggregate=median-of-10", File.ReadAllText(baselineAggregate));
            Assert.Contains("Aggregate=median-of-10", File.ReadAllText(candidateAggregate));
            Assert.Equal(2, File.ReadAllLines(baselineAggregate).Length);
            Assert.Equal(2, File.ReadAllLines(candidateAggregate).Length);
            Assert.StartsWith(scenarioName + ",", File.ReadLines(baselineAggregate).Last());
            Assert.StartsWith(scenarioName + ",", File.ReadLines(candidateAggregate).Last());
            AssertCloseoutResult(evidence, "PASS");
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task PairedRunner_UsesRevisionSpecificDirectArtifactsAndProducesAdjacentPairs()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            TestRepository repository = await CreateTestRepositoryAsync(temporaryRoot);
            string fakeToolRoot = Path.Combine(temporaryRoot, "fake-tools");
            CreatePairedFakeDotnetTool(fakeToolRoot);
            string invocationLog = Path.Combine(temporaryRoot, "fake-dotnet.log");
            Dictionary<string, string> environment = new()
            {
                ["PATH"] = fakeToolRoot + Path.PathSeparator +
                    (Environment.GetEnvironmentVariable("PATH") ?? string.Empty),
                ["FAKE_DOTNET_LOG"] = invocationLog,
            };
            string evidence = Path.Combine(temporaryRoot, "paired-evidence");

            ProcessResult result = await RunProcessWithEnvironmentAsync(
                "pwsh",
                environment,
                "-NoLogo",
                "-NoProfile",
                "-File",
                repository.RunnerScript,
                "-PreviousRef",
                "v4.3.0",
                "-CandidateRef",
                "HEAD",
                "-OutputPath",
                evidence,
                "-QualificationPass",
                "1",
                "-RepeatCount",
                "3",
                "-Paired",
                "-SuiteName",
                "master-table",
                "-PostBuildQuiescenceSeconds",
                "1");

            Assert.True(result.ExitCode == 0, result.CombinedOutput);

            string[] invocations = File.ReadAllLines(invocationLog);
            string[] builds = invocations
                .Where(line => line.Contains("|build", StringComparison.Ordinal) &&
                    !line.Contains("|build-server|", StringComparison.Ordinal))
                .ToArray();
            Assert.Equal(2, builds.Length);
            Assert.Contains("baseline-source|build", builds);
            Assert.Contains("candidate-source|build", builds);
            int shutdownIndex = Array.FindIndex(
                invocations,
                line => line.EndsWith("|build-server|shutdown", StringComparison.Ordinal));
            Assert.True(shutdownIndex >= 0, "Expected one build-server shutdown.");
            Assert.Single(
                invocations,
                line => line.EndsWith("|build-server|shutdown", StringComparison.Ordinal));
            Assert.All(
                builds,
                build => Assert.True(
                    Array.IndexOf(invocations, build) < shutdownIndex,
                    "Both revision artifacts must be built before shutdown."));
            string[] runs = invocations
                .Where(line => line.Contains("|run|", StringComparison.Ordinal))
                .ToArray();
            Assert.Equal(12, runs.Length);
            int firstRunIndex = Array.FindIndex(
                invocations,
                line => line.Contains("|run|", StringComparison.Ordinal));
            Assert.True(shutdownIndex < firstRunIndex, "Measurements must begin after shutdown.");
            Assert.DoesNotContain(
                invocations.Skip(shutdownIndex + 1),
                line => line.Contains("|build", StringComparison.Ordinal) ||
                    line.Contains("|mode=project|", StringComparison.Ordinal));

            string previousArtifact = Path.GetFullPath(Path.Combine(
                evidence,
                "baseline-source",
                "tests",
                "CSharpDB.Benchmarks",
                "bin",
                "Release",
                "net10.0",
                "CSharpDB.Benchmarks.dll"));
            string candidateArtifact = Path.GetFullPath(Path.Combine(
                evidence,
                "candidate-source",
                "tests",
                "CSharpDB.Benchmarks",
                "bin",
                "Release",
                "net10.0",
                "CSharpDB.Benchmarks.dll"));
            string previousArtifactHash = GetFakeArtifactHash("baseline-source");
            string candidateArtifactHash = GetFakeArtifactHash("candidate-source");
            Assert.NotEqual(previousArtifactHash, candidateArtifactHash);
            Assert.All(
                runs,
                run =>
                {
                    Assert.Contains("|mode=direct|", run);
                    Assert.DoesNotContain("|project=", run);
                });
            for (int pairIndex = 0; pairIndex < 6; pairIndex++)
            {
                bool previousFirst = pairIndex % 2 == 0;
                Assert.Contains(
                    previousFirst ? "baseline-source|run|" : "candidate-source|run|",
                    runs[pairIndex * 2]);
                Assert.Contains(
                    previousFirst ? "candidate-source|run|" : "baseline-source|run|",
                    runs[(pairIndex * 2) + 1]);
                Assert.Contains("|repeat=1|warmup=True", runs[pairIndex * 2]);
                Assert.Contains("|repeat=1|warmup=True", runs[(pairIndex * 2) + 1]);
                Assert.EndsWith(
                    "|artifact=" + (previousFirst ? previousArtifact : candidateArtifact),
                    runs[pairIndex * 2]);
                Assert.EndsWith(
                    "|artifact=" + (previousFirst ? candidateArtifact : previousArtifact),
                    runs[(pairIndex * 2) + 1]);
            }

            string artifactManifestPath = Path.Combine(
                evidence,
                "logs",
                "paired-benchmark-artifacts.sha256");
            Assert.Equal(
                "csharpdb-paired-benchmark-artifacts/v2",
                ReadManifestValue(artifactManifestPath, "FormatVersion"));
            Assert.Equal("false", ReadManifestValue(
                artifactManifestPath,
                "SharedSameRevisionArtifact"));
            Assert.Equal(previousArtifact, ReadManifestValue(
                artifactManifestPath,
                "PreviousArtifactPath"));
            Assert.Equal(previousArtifactHash, ReadManifestValue(
                artifactManifestPath,
                "PreviousArtifactSha256"));
            Assert.Equal(candidateArtifact, ReadManifestValue(
                artifactManifestPath,
                "CandidateArtifactPath"));
            Assert.Equal(candidateArtifactHash, ReadManifestValue(
                artifactManifestPath,
                "CandidateArtifactSha256"));
            string previousClosureHash = GetFakeClosureHash("baseline-source");
            string candidateClosureHash = GetFakeClosureHash("candidate-source");
            Assert.Equal("7", ReadManifestValue(
                artifactManifestPath,
                "PreviousClosureFileCount"));
            Assert.Equal(previousClosureHash, ReadManifestValue(
                artifactManifestPath,
                "PreviousClosureSha256"));
            Assert.Equal("7", ReadManifestValue(
                artifactManifestPath,
                "CandidateClosureFileCount"));
            Assert.Equal(candidateClosureHash, ReadManifestValue(
                artifactManifestPath,
                "CandidateClosureSha256"));
            AssertArtifactClosureRecords(
                artifactManifestPath,
                "Previous",
                "baseline-source");
            AssertArtifactClosureRecords(
                artifactManifestPath,
                "Candidate",
                "candidate-source");

            string manifestPath = Path.Combine(evidence, "logs", "paired-execution.csv");
            string[] manifestLines = File.ReadAllLines(manifestPath);
            Assert.Equal(7, manifestLines.Length);
            Assert.Equal(
                "Suite,PairId,Order,FirstRevision,SecondRevision,BaselineRaw,CandidateRaw",
                manifestLines[0]);
            for (int pairIndex = 0; pairIndex < 6; pairIndex++)
            {
                string[] fields = manifestLines[pairIndex + 1].Split(',');
                bool previousFirst = pairIndex % 2 == 0;
                Assert.Equal("master-table", fields[0]);
                Assert.Equal($"pair-{pairIndex + 1:00}", fields[1]);
                Assert.Equal(
                    previousFirst ? "previous-candidate" : "candidate-previous",
                    fields[2]);
                Assert.Equal(previousFirst ? "previous" : "candidate", fields[3]);
                Assert.Equal(previousFirst ? "candidate" : "previous", fields[4]);
                Assert.True(File.Exists(fields[5]), $"Missing baseline raw evidence: {fields[5]}");
                Assert.True(File.Exists(fields[6]), $"Missing candidate raw evidence: {fields[6]}");
            }
            AssertRawDigestManifest(evidence, expectedFileCount: 12);
            string previousBuildInputsManifest = Path.Combine(
                evidence,
                "logs",
                "previous-effective-build-inputs.sha256");
            string candidateBuildInputsManifest = Path.Combine(
                evidence,
                "logs",
                "candidate-effective-build-inputs.sha256");
            Assert.NotEqual(
                ReadManifestValue(previousBuildInputsManifest, "BuildInputsSha256"),
                ReadManifestValue(candidateBuildInputsManifest, "BuildInputsSha256"));

            foreach (string revisionRoot in new[] { "baseline-results", "candidate-results" })
            {
                string resultRoot = Path.Combine(evidence, revisionRoot);
                string aggregatePath = Assert.Single(
                    Directory.GetFiles(resultRoot, "*.csv", SearchOption.TopDirectoryOnly));
                Assert.Contains("Aggregate=median-of-6", File.ReadAllText(aggregatePath));
                Assert.Equal(
                    6,
                    Directory.GetFiles(
                        Path.Combine(resultRoot, "raw", "master-table"),
                        "*.csv",
                        SearchOption.TopDirectoryOnly).Length);
            }

            string[] executionEvents = File.ReadAllLines(
                Path.Combine(evidence, "logs", "execution-order.log"));
            string[] starts = executionEvents
                .Where(line => line.Contains("|START|", StringComparison.Ordinal))
                .ToArray();
            Assert.Equal(12, starts.Length);
            Assert.Equal(12, executionEvents.Count(line => line.Contains("|PASS|", StringComparison.Ordinal)));
            Assert.DoesNotContain(
                executionEvents,
                line => line.Contains("|FAIL|", StringComparison.Ordinal));
            Assert.All(
                starts.Where(line => line.Contains("|previous|", StringComparison.Ordinal)),
                line =>
                {
                    Assert.Contains($"SourceRoot={Path.Combine(evidence, "baseline-source")}", line);
                    Assert.Contains($"ArtifactPath={previousArtifact}", line);
                    Assert.Contains($"ArtifactSha256={previousArtifactHash}", line);
                    Assert.Contains("ClosureFileCount=7", line);
                    Assert.Contains($"ClosureSha256={previousClosureHash}", line);
                });
            Assert.All(
                starts.Where(line => line.Contains("|candidate|", StringComparison.Ordinal)),
                line =>
                {
                    Assert.Contains($"SourceRoot={Path.Combine(evidence, "candidate-source")}", line);
                    Assert.Contains($"ArtifactPath={candidateArtifact}", line);
                    Assert.Contains($"ArtifactSha256={candidateArtifactHash}", line);
                    Assert.Contains("ClosureFileCount=7", line);
                    Assert.Contains($"ClosureSha256={candidateClosureHash}", line);
                });

            string preflight = File.ReadAllText(Path.Combine(
                evidence,
                "previous-release-performance-preflight.md"));
            string report = File.ReadAllText(Path.Combine(
                evidence,
                "previous-release-performance.md"));
            foreach (string document in new[] { preflight, report })
            {
                Assert.Contains("- Same-revision artifact sharing: disabled", document);
                Assert.Contains($"- Previous benchmark artifact execution path: `{previousArtifact}`", document);
                Assert.Contains($"- Previous benchmark artifact SHA-256: `{previousArtifactHash}`", document);
                Assert.Contains($"- Previous runnable closure: 7 files; SHA-256 `{previousClosureHash}`", document);
                Assert.Contains($"- Candidate benchmark artifact execution path: `{candidateArtifact}`", document);
                Assert.Contains($"- Candidate benchmark artifact SHA-256: `{candidateArtifactHash}`", document);
                Assert.Contains($"- Candidate runnable closure: 7 files; SHA-256 `{candidateClosureHash}`", document);
                Assert.Contains("paired-benchmark-artifacts.sha256", document);
                Assert.Contains("may not exist after cleanup", document);
            }
            Assert.Contains("- Result: **PASS**", report);
            Assert.Contains("12 files verified", preflight);
            AssertCloseoutResult(evidence, "PASS");
            Assert.Contains("artifact closeout: **PASS**", File.ReadAllText(Path.Combine(
                evidence,
                "previous-release-performance-preflight.md")));
            Assert.False(Directory.Exists(Path.Combine(evidence, "baseline-source")));
            Assert.False(Directory.Exists(Path.Combine(evidence, "candidate-source")));
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task PairedRunner_MidRunFailureRetainsDigestsAndCleansUpProcessState()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            TestRepository repository = await CreateTestRepositoryAsync(temporaryRoot);
            string fakeToolRoot = Path.Combine(temporaryRoot, "fake-tools");
            CreatePairedFakeDotnetTool(fakeToolRoot);
            string invocationLog = Path.Combine(temporaryRoot, "fake-dotnet.log");
            Dictionary<string, string> environment = new()
            {
                ["PATH"] = fakeToolRoot + Path.PathSeparator +
                    (Environment.GetEnvironmentVariable("PATH") ?? string.Empty),
                ["FAKE_DOTNET_LOG"] = invocationLog,
                ["FAKE_DOTNET_FAIL_ON_RUN"] = "4",
            };
            string evidence = Path.Combine(temporaryRoot, "failed-paired-evidence");
            string wrapper = Path.Combine(temporaryRoot, "invoke-failing-runner.ps1");
            File.WriteAllText(
                wrapper,
                """
                param(
                    [Parameter(Mandatory)][string] $RunnerScript,
                    [Parameter(Mandatory)][string] $EvidencePath)

                $env:NUGET_PACKAGES = 'sentinel-nuget-packages'
                $env:DOTNET_CLI_HOME = 'sentinel-dotnet-home'
                try {
                    & $RunnerScript `
                        -PreviousRef v4.3.0 `
                        -CandidateRef HEAD `
                        -OutputPath $EvidencePath `
                        -QualificationPass 1 `
                        -RepeatCount 3 `
                        -Paired `
                        -SuiteName master-table
                }
                catch {
                    Write-Output "RUNNER_ERROR=$($_.Exception.Message)"
                    Write-Output "NUGET_PACKAGES_AFTER=$env:NUGET_PACKAGES"
                    Write-Output "DOTNET_CLI_HOME_AFTER=$env:DOTNET_CLI_HOME"
                    exit 17
                }

                Write-Error 'The injected paired-run failure did not occur.'
                exit 1
                """);

            ProcessResult result = await RunProcessWithEnvironmentAsync(
                "pwsh",
                environment,
                "-NoLogo",
                "-NoProfile",
                "-File",
                wrapper,
                "-RunnerScript",
                repository.RunnerScript,
                "-EvidencePath",
                evidence);

            Assert.Equal(17, result.ExitCode);
            Assert.Contains(
                "NUGET_PACKAGES_AFTER=sentinel-nuget-packages",
                result.CombinedOutput);
            Assert.Contains(
                "DOTNET_CLI_HOME_AFTER=sentinel-dotnet-home",
                result.CombinedOutput);
            string[] runs = File.ReadLines(invocationLog)
                .Where(line => line.Contains("|run|", StringComparison.Ordinal))
                .ToArray();
            Assert.Equal(4, runs.Length);
            Assert.All(runs, run => Assert.Contains("|mode=direct|", run));
            Assert.DoesNotContain(runs, run => run.Contains("|mode=project|", StringComparison.Ordinal));

            string artifactManifestPath = Path.Combine(
                evidence,
                "logs",
                "paired-benchmark-artifacts.sha256");
            Assert.True(File.Exists(artifactManifestPath));
            Assert.Equal(GetFakeArtifactHash("baseline-source"), ReadManifestValue(
                artifactManifestPath,
                "PreviousArtifactSha256"));
            Assert.Equal(GetFakeArtifactHash("candidate-source"), ReadManifestValue(
                artifactManifestPath,
                "CandidateArtifactSha256"));
            AssertCloseoutResult(evidence, "PASS");

            string[] executionEvents = File.ReadAllLines(Path.Combine(
                evidence,
                "logs",
                "execution-order.log"));
            Assert.Equal(
                4,
                executionEvents.Count(line => line.Contains("|START|", StringComparison.Ordinal)));
            Assert.Equal(
                3,
                executionEvents.Count(line => line.Contains("|PASS|", StringComparison.Ordinal)));
            Assert.Single(
                executionEvents,
                line => line.Contains("|FAIL|", StringComparison.Ordinal));
            AssertRawDigestManifest(evidence, expectedFileCount: 3);

            string[] pairManifest = File.ReadAllLines(Path.Combine(
                evidence,
                "logs",
                "paired-execution.csv"));
            Assert.Equal(2, pairManifest.Length);
            Assert.Contains(",pair-01,", pairManifest[1]);
            Assert.False(Directory.Exists(Path.Combine(evidence, "baseline-source")));
            Assert.False(Directory.Exists(Path.Combine(evidence, "candidate-source")));

            ProcessResult worktreeList = await RunProcessAsync(
                "git",
                "-C",
                repository.SourceRoot,
                "worktree",
                "list",
                "--porcelain");
            Assert.True(worktreeList.ExitCode == 0, worktreeList.CombinedOutput);
            Assert.DoesNotContain(
                Path.Combine(evidence, "baseline-source"),
                worktreeList.CombinedOutput,
                StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain(
                Path.Combine(evidence, "candidate-source"),
                worktreeList.CombinedOutput,
                StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task PairedRunner_RejectsLinkedDirectoryInArtifactClosure()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            TestRepository repository = await CreateTestRepositoryAsync(temporaryRoot);
            string fakeToolRoot = Path.Combine(temporaryRoot, "fake-tools");
            CreatePairedFakeDotnetTool(fakeToolRoot);
            string invocationLog = Path.Combine(temporaryRoot, "fake-dotnet.log");
            string linkTarget = Path.Combine(temporaryRoot, "closure-link-target");
            Directory.CreateDirectory(linkTarget);
            string targetSentinel = Path.Combine(linkTarget, "must-not-be-followed.bin");
            File.WriteAllText(targetSentinel, "external linked content");
            string evidence = Path.Combine(temporaryRoot, "linked-closure-evidence");
            Dictionary<string, string> environment = new()
            {
                ["PATH"] = fakeToolRoot + Path.PathSeparator +
                    (Environment.GetEnvironmentVariable("PATH") ?? string.Empty),
                ["FAKE_DOTNET_LOG"] = invocationLog,
                ["FAKE_DOTNET_CREATE_CLOSURE_DIRECTORY_LINK"] = "1",
                ["FAKE_DOTNET_CLOSURE_DIRECTORY_LINK_TARGET"] = linkTarget,
            };

            ProcessResult result = await RunProcessWithEnvironmentAsync(
                "pwsh",
                environment,
                "-NoLogo",
                "-NoProfile",
                "-File",
                repository.RunnerScript,
                "-PreviousRef",
                "v4.3.0",
                "-CandidateRef",
                "HEAD",
                "-OutputPath",
                evidence,
                "-QualificationPass",
                "1",
                "-RepeatCount",
                "3",
                "-Paired",
                "-SuiteName",
                "master-table");

            Assert.NotEqual(0, result.ExitCode);
            Assert.Contains(
                "Benchmark artifact closure directory cannot be a reparse point or link",
                result.CombinedOutput);
            Assert.Contains(
                "worktree link entry or entries before cleanup",
                result.CombinedOutput);
            Assert.DoesNotContain(
                File.ReadLines(invocationLog),
                line => line.Contains("|run|", StringComparison.Ordinal));
            Assert.True(File.Exists(targetSentinel));
            Assert.False(Directory.Exists(Path.Combine(evidence, "baseline-source")));
            Assert.False(Directory.Exists(Path.Combine(evidence, "candidate-source")));
            ProcessResult worktreeList = await RunProcessAsync(
                "git",
                "-C",
                repository.SourceRoot,
                "worktree",
                "list",
                "--porcelain");
            Assert.True(worktreeList.ExitCode == 0, worktreeList.CombinedOutput);
            Assert.DoesNotContain(
                Path.Combine(evidence, "baseline-source"),
                worktreeList.CombinedOutput,
                StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain(
                Path.Combine(evidence, "candidate-source"),
                worktreeList.CombinedOutput,
                StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task PairedRunner_RejectsDependencyChangedBeforeInvocation()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            TestRepository repository = await CreateTestRepositoryAsync(temporaryRoot);
            string fakeToolRoot = Path.Combine(temporaryRoot, "fake-tools");
            CreatePairedFakeDotnetTool(fakeToolRoot);
            string invocationLog = Path.Combine(temporaryRoot, "fake-dotnet.log");
            string evidence = Path.Combine(temporaryRoot, "before-invocation-tamper");
            string previousDependency = Path.GetFullPath(Path.Combine(
                evidence,
                "baseline-source",
                "tests",
                "CSharpDB.Benchmarks",
                "bin",
                "Release",
                "net10.0",
                "CSharpDB.Fake.Dependency.dll"));
            Dictionary<string, string> environment = new()
            {
                ["PATH"] = fakeToolRoot + Path.PathSeparator +
                    (Environment.GetEnvironmentVariable("PATH") ?? string.Empty),
                ["FAKE_DOTNET_LOG"] = invocationLog,
                ["FAKE_DOTNET_MUTATE_AFTER_SHUTDOWN"] = previousDependency,
            };

            ProcessResult result = await RunProcessWithEnvironmentAsync(
                "pwsh",
                environment,
                "-NoLogo",
                "-NoProfile",
                "-File",
                repository.RunnerScript,
                "-PreviousRef",
                "v4.3.0",
                "-CandidateRef",
                "HEAD",
                "-OutputPath",
                evidence,
                "-QualificationPass",
                "1",
                "-RepeatCount",
                "3",
                "-Paired",
                "-SuiteName",
                "master-table",
                "-PostBuildQuiescenceSeconds",
                "1");

            Assert.NotEqual(0, result.ExitCode);
            Assert.Contains(
                "Benchmark artifact closure changed before invocation.",
                result.CombinedOutput);
            Assert.DoesNotContain(
                File.ReadLines(invocationLog),
                line => line.Contains("|run|", StringComparison.Ordinal));
            AssertArtifactIntegrityFailureEvidence(
                evidence,
                expectedFailureText: "closure changed before invocation");
            Assert.False(Directory.Exists(Path.Combine(evidence, "baseline-source")));
            Assert.False(Directory.Exists(Path.Combine(evidence, "candidate-source")));
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task PairedRunner_RejectsDependencyChangedAfterInvocation()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            TestRepository repository = await CreateTestRepositoryAsync(temporaryRoot);
            string fakeToolRoot = Path.Combine(temporaryRoot, "fake-tools");
            CreatePairedFakeDotnetTool(fakeToolRoot);
            string invocationLog = Path.Combine(temporaryRoot, "fake-dotnet.log");
            string evidence = Path.Combine(temporaryRoot, "after-invocation-tamper");
            Dictionary<string, string> environment = new()
            {
                ["PATH"] = fakeToolRoot + Path.PathSeparator +
                    (Environment.GetEnvironmentVariable("PATH") ?? string.Empty),
                ["FAKE_DOTNET_LOG"] = invocationLog,
                ["FAKE_DOTNET_MUTATE_DEPENDENCY_ON_RUN"] = "1",
            };

            ProcessResult result = await RunProcessWithEnvironmentAsync(
                "pwsh",
                environment,
                "-NoLogo",
                "-NoProfile",
                "-File",
                repository.RunnerScript,
                "-PreviousRef",
                "v4.3.0",
                "-CandidateRef",
                "HEAD",
                "-OutputPath",
                evidence,
                "-QualificationPass",
                "1",
                "-RepeatCount",
                "3",
                "-Paired",
                "-SuiteName",
                "master-table");

            Assert.NotEqual(0, result.ExitCode);
            Assert.Contains(
                "Benchmark artifact closure changed after invocation.",
                result.CombinedOutput);
            Assert.Single(
                File.ReadLines(invocationLog),
                line => line.Contains("|run|", StringComparison.Ordinal));
            AssertArtifactIntegrityFailureEvidence(
                evidence,
                expectedFailureText: "closure changed after invocation");
            Assert.False(Directory.Exists(Path.Combine(evidence, "baseline-source")));
            Assert.False(Directory.Exists(Path.Combine(evidence, "candidate-source")));
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task PairedRunner_RejectsArtifactManifestChangedMidRunAtCloseout()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            TestRepository repository = await CreateTestRepositoryAsync(temporaryRoot);
            string fakeToolRoot = Path.Combine(temporaryRoot, "fake-tools");
            CreatePairedFakeDotnetTool(fakeToolRoot);
            string invocationLog = Path.Combine(temporaryRoot, "fake-dotnet.log");
            string evidence = Path.Combine(temporaryRoot, "manifest-tamper");
            string artifactManifestPath = Path.Combine(
                evidence,
                "logs",
                "paired-benchmark-artifacts.sha256");
            Dictionary<string, string> environment = new()
            {
                ["PATH"] = fakeToolRoot + Path.PathSeparator +
                    (Environment.GetEnvironmentVariable("PATH") ?? string.Empty),
                ["FAKE_DOTNET_LOG"] = invocationLog,
                ["FAKE_DOTNET_MUTATE_MANIFEST_ON_RUN"] = "4",
                ["FAKE_DOTNET_ARTIFACT_MANIFEST"] = artifactManifestPath,
            };

            ProcessResult result = await RunProcessWithEnvironmentAsync(
                "pwsh",
                environment,
                "-NoLogo",
                "-NoProfile",
                "-File",
                repository.RunnerScript,
                "-PreviousRef",
                "v4.3.0",
                "-CandidateRef",
                "HEAD",
                "-OutputPath",
                evidence,
                "-QualificationPass",
                "1",
                "-RepeatCount",
                "3",
                "-Paired",
                "-SuiteName",
                "master-table");

            Assert.NotEqual(0, result.ExitCode);
            AssertDiagnosticContains(
                "Paired benchmark artifact manifest changed before closeout",
                result.CombinedOutput);
            Assert.Equal(
                12,
                File.ReadLines(invocationLog)
                    .Count(line => line.Contains("|run|", StringComparison.Ordinal)));
            AssertRawDigestManifest(evidence, expectedFileCount: 12);
            Assert.Equal(
                7,
                File.ReadAllLines(Path.Combine(
                    evidence,
                    "logs",
                    "paired-execution.csv")).Length);
            string[] executionEvents = File.ReadAllLines(Path.Combine(
                evidence,
                "logs",
                "execution-order.log"));
            Assert.Equal(
                12,
                executionEvents.Count(line => line.Contains("|PASS|", StringComparison.Ordinal)));
            Assert.DoesNotContain(
                executionEvents,
                line => line.Contains("|FAIL|", StringComparison.Ordinal));
            Assert.Contains("TamperedOnRun=4", File.ReadAllText(artifactManifestPath));
            AssertCloseoutResult(evidence, "FAIL");
            Assert.Contains(
                "artifact closeout: **FAIL**",
                File.ReadAllText(Path.Combine(
                    evidence,
                    "previous-release-performance-preflight.md")));
            Assert.Contains(
                "artifact closeout: **FAIL**",
                File.ReadAllText(Path.Combine(
                    evidence,
                    "previous-release-performance.md")));
            Assert.False(Directory.Exists(Path.Combine(evidence, "baseline-source")));
            Assert.False(Directory.Exists(Path.Combine(evidence, "candidate-source")));
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    private static async Task<TestRepository> CreateTestRepositoryAsync(string temporaryRoot)
    {
        string sourceRoot = Path.Combine(temporaryRoot, "repository");
        string benchmarkRoot = Path.Combine(sourceRoot, "tests", "CSharpDB.Benchmarks");
        string scriptRoot = Path.Combine(benchmarkRoot, "scripts");
        Directory.CreateDirectory(scriptRoot);

        string repositoryRoot = FindRepoRoot();
        foreach (string scriptName in new[]
                 {
                     "Compare-ReleaseCore.ps1",
                     "Test-PreviousReleasePerformance.ps1",
                 })
        {
            File.Copy(
                Path.Combine(
                    repositoryRoot,
                    "tests",
                    "CSharpDB.Benchmarks",
                    "scripts",
                    scriptName),
                Path.Combine(scriptRoot, scriptName));
        }
        File.WriteAllText(
            Path.Combine(benchmarkRoot, "CSharpDB.Benchmarks.csproj"),
            "<Project Sdk=\"Microsoft.NET.Sdk\" />");
        File.WriteAllText(
            Path.Combine(benchmarkRoot, "Program.cs"),
            "internal static class Program { private static void Main() { } }");
        string trackedFile = Path.Combine(sourceRoot, "release.txt");
        string buildProps = Path.Combine(sourceRoot, "Directory.Build.props");
        File.WriteAllText(trackedFile, "previous");
        File.WriteAllText(
            buildProps,
            "<Project><PropertyGroup><RevisionMarker>previous</RevisionMarker></PropertyGroup></Project>");

        await AssertProcessSucceeded("git", "-C", sourceRoot, "init");
        await AssertProcessSucceeded("git", "-C", sourceRoot, "config", "user.email", "test@example.invalid");
        await AssertProcessSucceeded("git", "-C", sourceRoot, "config", "user.name", "CSharpDB Tests");
        await AssertProcessSucceeded("git", "-C", sourceRoot, "config", "commit.gpgsign", "false");
        await AssertProcessSucceeded("git", "-C", sourceRoot, "add", ".");
        await AssertProcessSucceeded("git", "-C", sourceRoot, "commit", "-m", "previous release");
        await AssertProcessSucceeded("git", "-C", sourceRoot, "tag", "v4.3.0");
        File.WriteAllText(trackedFile, "candidate");
        File.WriteAllText(
            buildProps,
            "<Project><PropertyGroup><RevisionMarker>candidate</RevisionMarker></PropertyGroup></Project>");
        await AssertProcessSucceeded(
            "git",
            "-C",
            sourceRoot,
            "add",
            "release.txt",
            "Directory.Build.props");
        await AssertProcessSucceeded("git", "-C", sourceRoot, "commit", "-m", "candidate");

        return new TestRepository(
            sourceRoot,
            Path.Combine(scriptRoot, "Test-PreviousReleasePerformance.ps1"));
    }

    private static void AssertRawDigestManifest(string evidenceRoot, int expectedFileCount)
    {
        string digestPath = Path.Combine(
            evidenceRoot,
            "logs",
            "paired-raw-evidence.sha256");
        string[] lines = File.ReadAllLines(digestPath);
        Assert.Equal(expectedFileCount, lines.Length);
        foreach (string line in lines)
        {
            int separator = line.IndexOf(" *", StringComparison.Ordinal);
            Assert.Equal(64, separator);
            string expectedHash = line[..separator];
            Assert.All(
                expectedHash,
                character => Assert.True(
                    character is >= '0' and <= '9' or >= 'a' and <= 'f',
                    $"Invalid SHA-256 character '{character}'."));
            string relativePath = line[(separator + 2)..];
            Assert.False(Path.IsPathRooted(relativePath));
            Assert.DoesNotContain("..", relativePath.Split('/'));
            string rawPath = Path.GetFullPath(Path.Combine(
                evidenceRoot,
                relativePath.Replace('/', Path.DirectorySeparatorChar)));
            Assert.True(File.Exists(rawPath), $"Missing digested raw evidence: {rawPath}");
            string actualHash = Convert
                .ToHexString(SHA256.HashData(File.ReadAllBytes(rawPath)))
                .ToLowerInvariant();
            Assert.Equal(expectedHash, actualHash);
        }
    }

    private static void AssertArtifactIntegrityFailureEvidence(
        string evidenceRoot,
        string expectedFailureText)
    {
        string artifactManifestPath = Path.Combine(
            evidenceRoot,
            "logs",
            "paired-benchmark-artifacts.sha256");
        Assert.True(File.Exists(artifactManifestPath));
        Assert.Equal(GetFakeArtifactHash("baseline-source"), ReadManifestValue(
            artifactManifestPath,
            "PreviousArtifactSha256"));
        Assert.Equal(GetFakeArtifactHash("candidate-source"), ReadManifestValue(
            artifactManifestPath,
            "CandidateArtifactSha256"));
        AssertCloseoutResult(evidenceRoot, "FAIL");

        string[] executionEvents = File.ReadAllLines(Path.Combine(
            evidenceRoot,
            "logs",
            "execution-order.log"));
        Assert.Single(
            executionEvents,
            line => line.Contains("|START|", StringComparison.Ordinal));
        Assert.DoesNotContain(
            executionEvents,
            line => line.Contains("|PASS|", StringComparison.Ordinal));
        string failure = Assert.Single(
            executionEvents,
            line => line.Contains("|FAIL|", StringComparison.Ordinal));
        Assert.Contains(expectedFailureText, failure);

        AssertRawDigestManifest(evidenceRoot, expectedFileCount: 0);
        Assert.Single(File.ReadAllLines(Path.Combine(
            evidenceRoot,
            "logs",
            "paired-execution.csv")));
    }

    private static string ReadManifestValue(string manifestPath, string name)
    {
        string prefix = name + "=";
        string line = Assert.Single(
            File.ReadLines(manifestPath),
            candidate => candidate.StartsWith(prefix, StringComparison.Ordinal));
        return line[prefix.Length..];
    }

    private static string GetFakeArtifactHash(string sourceName)
    {
        string payload = $"fake release-core benchmark artifact:{sourceName}";
        return GetTextSha256(payload);
    }

    private static IReadOnlyList<(string RelativePath, string Sha256)>
        GetFakeClosureRecords(string sourceName)
    {
        return new (string RelativePath, string Sha256)[]
        {
            (
                "CSharpDB.Benchmarks.dll",
                GetFakeArtifactHash(sourceName)),
            (
                "CSharpDB.Benchmarks.deps.json",
                GetTextSha256($"{{\"source\":\"{sourceName}\"}}")),
            (
                "CSharpDB.Benchmarks.runtimeconfig.json",
                GetTextSha256($"{{\"runtimeOptions\":{{\"source\":\"{sourceName}\"}}}}")),
            (
                "CSharpDB.Fake.Dependency.dll",
                GetTextSha256($"fake managed dependency:{sourceName}")),
            (
                "runtimes/fake/native/csharpdb-fake-native.bin",
                GetTextSha256($"fake native dependency:{sourceName}")),
            (
                "runtimes/fake/results/nested-results-dependency.bin",
                GetTextSha256($"fake nested results dependency:{sourceName}")),
            (
                "runtimes/fake/CSharpDB.Benchmarks-Job-runtime/nested-job-dependency.bin",
                GetTextSha256($"fake nested job dependency:{sourceName}")),
        }
        .OrderBy(record => record.RelativePath, StringComparer.Ordinal)
        .ToArray();
    }

    private static string GetFakeClosureHash(string sourceName)
    {
        string payload = string.Join(
            "\n",
            GetFakeClosureRecords(sourceName)
                .Select(record => $"{record.Sha256} *{record.RelativePath}")) + "\n";
        return GetTextSha256(payload);
    }

    private static string GetTextSha256(string value)
    {
        return Convert
            .ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value)))
            .ToLowerInvariant();
    }

    private static void AssertArtifactClosureRecords(
        string manifestPath,
        string revisionPrefix,
        string sourceName)
    {
        string prefix = revisionPrefix + "ClosureFile=";
        string[] actualRecords = File.ReadLines(manifestPath)
            .Where(line => line.StartsWith(prefix, StringComparison.Ordinal))
            .Select(line => line[prefix.Length..])
            .ToArray();
        string[] expectedRecords = GetFakeClosureRecords(sourceName)
            .Select(record => $"{record.Sha256} *{record.RelativePath}")
            .ToArray();
        Assert.Equal(expectedRecords, actualRecords);
        Assert.DoesNotContain(
            actualRecords,
            record =>
            {
                string relativePath = record[(record.IndexOf(" *", StringComparison.Ordinal) + 2)..];
                return relativePath.StartsWith("results/", StringComparison.Ordinal) ||
                    relativePath.StartsWith("CSharpDB.Benchmarks-Job-", StringComparison.Ordinal);
            });
        Assert.Contains(
            actualRecords,
            record => record.EndsWith(
                " *runtimes/fake/results/nested-results-dependency.bin",
                StringComparison.Ordinal));
        Assert.Contains(
            actualRecords,
            record => record.EndsWith(
                " *runtimes/fake/CSharpDB.Benchmarks-Job-runtime/nested-job-dependency.bin",
                StringComparison.Ordinal));

        string[] manifestLines = File.ReadAllLines(manifestPath);
        Assert.Contains("ClosureExclusion=top-level directory segment results", manifestLines);
        Assert.Contains(
            "ClosureExclusion=top-level directory segment CSharpDB.Benchmarks-Job-*",
            manifestLines);
    }

    private static void AssertCloseoutResult(string evidenceRoot, string expectedResult)
    {
        string closeoutPath = Path.Combine(
            evidenceRoot,
            "logs",
            "paired-benchmark-artifact-closeout.log");
        Assert.True(File.Exists(closeoutPath));
        Assert.Equal(expectedResult, ReadManifestValue(closeoutPath, "Result"));
    }

    private static void CreatePairedFakeDotnetTool(string toolRoot)
    {
        Directory.CreateDirectory(toolRoot);
        string fakeScript = Path.Combine(toolRoot, "fake-dotnet.ps1");
        File.WriteAllText(
            fakeScript,
            """
            $ErrorActionPreference = 'Stop'
            $CommandArgs = @($args)
            if ($CommandArgs.Count -eq 1 -and $CommandArgs[0] -eq '--version') {
                Write-Output '10.0.203'
                exit 0
            }

            $command = $CommandArgs[0]
            $sourceName = Split-Path -Leaf (Get-Location).Path
            if ($command -eq 'build') {
                $artifactRoot = Join-Path `
                    (Get-Location).Path `
                    'tests/CSharpDB.Benchmarks/bin/Release/net10.0'
                New-Item -ItemType Directory -Path $artifactRoot -Force | Out-Null
                [IO.File]::WriteAllText(
                    (Join-Path $artifactRoot 'CSharpDB.Benchmarks.dll'),
                    "fake release-core benchmark artifact:$sourceName")
                [IO.File]::WriteAllText(
                    (Join-Path $artifactRoot 'CSharpDB.Benchmarks.deps.json'),
                    "{`"source`":`"$sourceName`"}")
                [IO.File]::WriteAllText(
                    (Join-Path $artifactRoot 'CSharpDB.Benchmarks.runtimeconfig.json'),
                    "{`"runtimeOptions`":{`"source`":`"$sourceName`"}}")
                [IO.File]::WriteAllText(
                    (Join-Path $artifactRoot 'CSharpDB.Fake.Dependency.dll'),
                    "fake managed dependency:$sourceName")
                $nativeRoot = Join-Path $artifactRoot 'runtimes/fake/native'
                New-Item -ItemType Directory -Path $nativeRoot -Force | Out-Null
                [IO.File]::WriteAllText(
                    (Join-Path $nativeRoot 'csharpdb-fake-native.bin'),
                    "fake native dependency:$sourceName")
                $nestedResultsRoot = Join-Path $artifactRoot 'runtimes/fake/results'
                New-Item -ItemType Directory -Path $nestedResultsRoot -Force | Out-Null
                [IO.File]::WriteAllText(
                    (Join-Path $nestedResultsRoot 'nested-results-dependency.bin'),
                    "fake nested results dependency:$sourceName")
                $nestedJobRoot = Join-Path `
                    $artifactRoot `
                    'runtimes/fake/CSharpDB.Benchmarks-Job-runtime'
                New-Item -ItemType Directory -Path $nestedJobRoot -Force | Out-Null
                [IO.File]::WriteAllText(
                    (Join-Path $nestedJobRoot 'nested-job-dependency.bin'),
                    "fake nested job dependency:$sourceName")
                $ignoredResultsRoot = Join-Path $artifactRoot 'results'
                New-Item -ItemType Directory -Path $ignoredResultsRoot -Force | Out-Null
                [IO.File]::WriteAllText(
                    (Join-Path $ignoredResultsRoot 'runtime-output.tmp'),
                    'excluded runtime output')
                $ignoredJobRoot = Join-Path `
                    $artifactRoot `
                    'CSharpDB.Benchmarks-Job-fake'
                New-Item -ItemType Directory -Path $ignoredJobRoot -Force | Out-Null
                [IO.File]::WriteAllText(
                    (Join-Path $ignoredJobRoot 'runtime-output.tmp'),
                    'excluded benchmark job output')
                if (-not [string]::IsNullOrWhiteSpace(
                        $env:FAKE_DOTNET_CREATE_CLOSURE_DIRECTORY_LINK)) {
                    if ([string]::IsNullOrWhiteSpace(
                            $env:FAKE_DOTNET_CLOSURE_DIRECTORY_LINK_TARGET)) {
                        Write-Error 'Missing fake closure directory link target.'
                        exit 1
                    }
                    $linkItemType = if ($IsWindows) { 'Junction' } else { 'SymbolicLink' }
                    New-Item `
                        -ItemType $linkItemType `
                        -Path (Join-Path $artifactRoot 'linked-dependency-directory') `
                        -Target ([IO.Path]::GetFullPath(
                            $env:FAKE_DOTNET_CLOSURE_DIRECTORY_LINK_TARGET)) |
                        Out-Null
                    New-Item `
                        -ItemType $linkItemType `
                        -Path (Join-Path (Get-Location).Path 'linked-worktree-directory') `
                        -Target ([IO.Path]::GetFullPath(
                            $env:FAKE_DOTNET_CLOSURE_DIRECTORY_LINK_TARGET)) |
                        Out-Null
                }
                Add-Content -LiteralPath $env:FAKE_DOTNET_LOG -Value "$sourceName|build"
                Write-Output "Fake build: $sourceName"
                exit 0
            }
            if ($command -eq 'build-server') {
                if ($CommandArgs.Count -ne 2 -or $CommandArgs[1] -ne 'shutdown') {
                    Write-Error 'Expected dotnet build-server shutdown.'
                    exit 1
                }
                Add-Content `
                    -LiteralPath $env:FAKE_DOTNET_LOG `
                    -Value "$sourceName|build-server|shutdown"
                if (-not [string]::IsNullOrWhiteSpace(
                        $env:FAKE_DOTNET_MUTATE_AFTER_SHUTDOWN)) {
                    [IO.File]::AppendAllText(
                        [IO.Path]::GetFullPath(
                            $env:FAKE_DOTNET_MUTATE_AFTER_SHUTDOWN),
                        ':mutated-after-shutdown')
                }
                Write-Output 'Fake build servers shut down.'
                exit 0
            }
            $isDirectArtifact =
                [IO.Path]::GetFileName($command) -ceq 'CSharpDB.Benchmarks.dll'
            $isProjectRun = $command -eq 'run'
            if (-not $isDirectArtifact -and -not $isProjectRun) {
                Write-Error "Unsupported fake dotnet command: $command"
                exit 1
            }

            $suiteMap = @{
                '--master-table' = 'master-table'
                '--durable-sql-batching' = 'durable-sql-batching'
                '--concurrent-write-diagnostics' = 'concurrent-write-diagnostics'
                '--hybrid-storage-mode' = 'hybrid-storage-mode'
                '--hybrid-hot-set-read' = 'hybrid-hot-set-read'
                '--hybrid-cold-open' = 'hybrid-cold-open'
                '--sqlite-compare' = 'sqlite-compare'
            }
            $suiteArguments = @(
                $CommandArgs | Where-Object { $suiteMap.ContainsKey($_) }
            )
            $scenarioIndex =
                [Array]::IndexOf[string]($CommandArgs, '--hybrid-storage-mode-scenario')
            $scenarioName = ''
            if ($scenarioIndex -ge 0) {
                if ($suiteArguments.Count -ne 0 -or
                    $scenarioIndex + 1 -ge $CommandArgs.Count) {
                    Write-Error 'Expected one hybrid storage scenario argument and value.'
                    exit 1
                }
                $suiteName = 'hybrid-storage-mode-scenario'
                $scenarioName = $CommandArgs[$scenarioIndex + 1]
            }
            else {
                if ($suiteArguments.Count -ne 1) {
                    Write-Error 'Expected one release-core suite argument.'
                    exit 1
                }
                $suiteName = $suiteMap[$suiteArguments[0]]
            }
            $repeatIndex = [Array]::IndexOf[string]($CommandArgs, '--repeat')
            if ($repeatIndex -lt 0 -or $repeatIndex + 1 -ge $CommandArgs.Count) {
                Write-Error 'Missing --repeat value.'
                exit 1
            }
            $repeatCount = $CommandArgs[$repeatIndex + 1]
            $hasWarmup = $CommandArgs -contains '--warmup-single-sample'
            if ($repeatCount -ne '1' -or -not $hasWarmup) {
                Write-Error 'Paired samples require --repeat 1 --warmup-single-sample.'
                exit 1
            }
            $executionMode = if ($isDirectArtifact) { 'direct' } else { 'project' }
            if ($isDirectArtifact) {
                if ($CommandArgs -contains '--project') {
                    Write-Error 'Direct artifact execution cannot include --project.'
                    exit 1
                }
                $executionTarget = [IO.Path]::GetFullPath($command)
                $targetField = "artifact=$executionTarget"
            }
            else {
                $projectIndex = [Array]::IndexOf[string]($CommandArgs, '--project')
                if ($projectIndex -lt 0 -or $projectIndex + 1 -ge $CommandArgs.Count) {
                    Write-Error 'Missing --project value.'
                    exit 1
                }
                $projectPath = $CommandArgs[$projectIndex + 1]
                $targetField = "project=$projectPath"
            }

            $priorRuns = @(
                if (Test-Path -LiteralPath $env:FAKE_DOTNET_LOG -PathType Leaf) {
                    Get-Content -LiteralPath $env:FAKE_DOTNET_LOG |
                        Where-Object { $_.Contains('|run|', [StringComparison]::Ordinal) }
                }
            )
            $currentRunNumber = $priorRuns.Count + 1
            Add-Content `
                -LiteralPath $env:FAKE_DOTNET_LOG `
                -Value (
                    "$sourceName|run|mode=$executionMode|$suiteName|scenario=$scenarioName|" +
                    "repeat=$repeatCount|warmup=$hasWarmup|$targetField")
            [int] $failOnRun = 0
            if (-not [string]::IsNullOrWhiteSpace($env:FAKE_DOTNET_FAIL_ON_RUN) -and
                [int]::TryParse($env:FAKE_DOTNET_FAIL_ON_RUN, [ref] $failOnRun) -and
                $currentRunNumber -eq $failOnRun) {
                Write-Error "Injected fake dotnet failure on run $currentRunNumber."
                exit 1
            }

            $resultRoot = Join-Path `
                (Get-Location).Path `
                'tests/CSharpDB.Benchmarks/bin/Release/net10.0/results'
            New-Item -ItemType Directory -Path $resultRoot -Force | Out-Null
            $resultPath = Join-Path `
                $resultRoot `
                "$suiteName-$([Guid]::NewGuid().ToString('N')).csv"
            $header = 'Name,TotalOps,LatencySamples,ElapsedMs,OpsPerSec,P50,P90,P95,P99,P999,Min,Max,Mean,StdDev,ExtraInfo'
            $outputRows = @($header)
            if (-not [string]::IsNullOrWhiteSpace($scenarioName)) {
                $qualificationInfo =
                    'qualification=true; unrecorded-warmup-seconds=2; ' +
                    'minimum-measured-seconds=30; ' +
                    'minimum-retained-latency-samples=10000; ' +
                    'measurement-cap-seconds=120; ' +
                    'measurement-begin-utc=2026-07-31T12:00:00.0000000+00:00; ' +
                    'measurement-end-utc=2026-07-31T12:00:30.0000000+00:00'
                $outputRows += "$scenarioName,10000,10000,30000,333.3333,1,1,1,1,1,1,1,1,1,$qualificationInfo"
            }
            else {
                $outputRows += "$suiteName-row-a,1000,1000,10000,100,1,1,1,1,1,1,1,1,1,Sample=$sourceName"
                $outputRows += "$suiteName-row-b,1000,1000,10000,100,1,1,1,1,1,1,1,1,1,Sample=$sourceName"
            }
            [IO.File]::WriteAllLines(
                $resultPath,
                [string[]] $outputRows)
            [int] $mutateArtifactOnRun = 0
            if ($isDirectArtifact -and
                -not [string]::IsNullOrWhiteSpace(
                    $env:FAKE_DOTNET_MUTATE_ARTIFACT_ON_RUN) -and
                [int]::TryParse(
                    $env:FAKE_DOTNET_MUTATE_ARTIFACT_ON_RUN,
                    [ref] $mutateArtifactOnRun) -and
                $currentRunNumber -eq $mutateArtifactOnRun) {
                [IO.File]::AppendAllText(
                    [IO.Path]::GetFullPath($command),
                    ":mutated-on-run-$currentRunNumber")
            }
            [int] $mutateDependencyOnRun = 0
            if ($isDirectArtifact -and
                -not [string]::IsNullOrWhiteSpace(
                    $env:FAKE_DOTNET_MUTATE_DEPENDENCY_ON_RUN) -and
                [int]::TryParse(
                    $env:FAKE_DOTNET_MUTATE_DEPENDENCY_ON_RUN,
                    [ref] $mutateDependencyOnRun) -and
                $currentRunNumber -eq $mutateDependencyOnRun) {
                [IO.File]::AppendAllText(
                    (Join-Path `
                        (Split-Path -Parent ([IO.Path]::GetFullPath($command))) `
                        'CSharpDB.Fake.Dependency.dll'),
                    ":mutated-on-run-$currentRunNumber")
            }
            [int] $mutateManifestOnRun = 0
            if (-not [string]::IsNullOrWhiteSpace(
                    $env:FAKE_DOTNET_MUTATE_MANIFEST_ON_RUN) -and
                [int]::TryParse(
                    $env:FAKE_DOTNET_MUTATE_MANIFEST_ON_RUN,
                    [ref] $mutateManifestOnRun) -and
                $currentRunNumber -eq $mutateManifestOnRun) {
                [IO.File]::AppendAllText(
                    [IO.Path]::GetFullPath($env:FAKE_DOTNET_ARTIFACT_MANIFEST),
                    "`nTamperedOnRun=$currentRunNumber")
            }
            Write-Output "Fake paired run: $sourceName/$suiteName"
            """);

        if (OperatingSystem.IsWindows())
        {
            File.WriteAllText(
                Path.Combine(toolRoot, "dotnet.cmd"),
                """
                @echo off
                pwsh -NoLogo -NoProfile -File "%~dp0fake-dotnet.ps1" %*
                exit /b %ERRORLEVEL%
                """);
            return;
        }

        string launcher = Path.Combine(toolRoot, "dotnet");
        File.WriteAllText(
            launcher,
            """
            #!/usr/bin/env sh
            script_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
            exec pwsh -NoLogo -NoProfile -File "${script_dir}/fake-dotnet.ps1" "$@"
            """);
        File.SetUnixFileMode(
            launcher,
            UnixFileMode.UserRead |
            UnixFileMode.UserWrite |
            UnixFileMode.UserExecute |
            UnixFileMode.GroupRead |
            UnixFileMode.GroupExecute |
            UnixFileMode.OtherRead |
            UnixFileMode.OtherExecute);
    }

    private static async Task AssertProcessSucceeded(string fileName, params string[] arguments)
    {
        ProcessResult result = await RunProcessAsync(fileName, arguments);
        Assert.True(result.ExitCode == 0, result.CombinedOutput);
    }

    private static Task<ProcessResult> RunProcessAsync(
        string fileName,
        params string[] arguments)
    {
        return RunProcessCoreAsync(
            fileName,
            environment: null,
            TimeSpan.FromSeconds(30),
            arguments);
    }

    private static Task<ProcessResult> RunProcessWithEnvironmentAsync(
        string fileName,
        IReadOnlyDictionary<string, string> environment,
        params string[] arguments)
    {
        return RunProcessCoreAsync(
            fileName,
            environment,
            TimeSpan.FromSeconds(180),
            arguments);
    }

    private static async Task<ProcessResult> RunProcessCoreAsync(
        string fileName,
        IReadOnlyDictionary<string, string>? environment,
        TimeSpan timeoutDuration,
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
        if (environment is not null)
        {
            foreach ((string name, string value) in environment)
                startInfo.Environment[name] = value;
        }

        using Process process = new() { StartInfo = startInfo };
        Assert.True(process.Start(), $"Could not start {fileName}.");
        Task<string> standardOutput = process.StandardOutput.ReadToEndAsync();
        Task<string> standardError = process.StandardError.ReadToEndAsync();
        using CancellationTokenSource timeout = new(timeoutDuration);
        try
        {
            await process.WaitForExitAsync(timeout.Token);
        }
        catch (OperationCanceledException)
        {
            process.Kill(entireProcessTree: true);
            throw new TimeoutException(
                $"{fileName} did not finish within {timeoutDuration.TotalSeconds:N0} seconds.");
        }

        return new ProcessResult(
            process.ExitCode,
            await standardOutput,
            await standardError);
    }

    private static string CreateTemporaryRoot()
    {
        string path = Path.Combine(
            Path.GetTempPath(),
            "csharpdb-previous-release-paired-tests",
            Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private static void AssertDiagnosticContains(string expected, string actual)
    {
        Assert.Contains(
            NormalizeDiagnostic(expected),
            NormalizeDiagnostic(actual),
            StringComparison.Ordinal);
    }

    private static string NormalizeDiagnostic(string value)
    {
        string withoutAnsi = System.Text.RegularExpressions.Regex.Replace(
            value,
            "\u001b\\[[0-?]*[ -/]*[@-~]",
            string.Empty);
        return System.Text.RegularExpressions.Regex.Replace(withoutAnsi, "\\s+", " ").Trim();
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

    private sealed record TestRepository(string SourceRoot, string RunnerScript);

    private sealed record ProcessResult(
        int ExitCode,
        string StandardOutput,
        string StandardError)
    {
        public string CombinedOutput =>
            StandardOutput + Environment.NewLine + StandardError;
    }
}
