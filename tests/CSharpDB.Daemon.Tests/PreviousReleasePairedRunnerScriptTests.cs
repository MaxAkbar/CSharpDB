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
            Assert.Contains(
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
        const string artifactPayload = "fake release-core benchmark artifact";
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
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task PairedRunner_ProducesAdjacentManifestAndSixSamplesPerRevision()
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
                "master-table");

            Assert.True(result.ExitCode == 0, result.CombinedOutput);

            string[] invocations = File.ReadAllLines(invocationLog);
            Assert.Equal(2, invocations.Count(line => line.Contains("|build", StringComparison.Ordinal)));
            string[] runs = invocations
                .Where(line => line.Contains("|run|", StringComparison.Ordinal))
                .ToArray();
            Assert.Equal(12, runs.Length);
            Assert.All(runs, run => Assert.Contains("|mode=project|", run));
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
            }

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
            Assert.Contains(
                "- Result: **PASS**",
                File.ReadAllText(Path.Combine(evidence, "previous-release-performance.md")));
            Assert.Contains(
                "12 files verified",
                File.ReadAllText(Path.Combine(
                    evidence,
                    "previous-release-performance-preflight.md")));
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
            Assert.Equal(
                4,
                File.ReadLines(invocationLog)
                    .Count(line => line.Contains("|run|", StringComparison.Ordinal)));

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

    private static string ReadManifestValue(string manifestPath, string name)
    {
        string prefix = name + "=";
        string line = Assert.Single(
            File.ReadLines(manifestPath),
            candidate => candidate.StartsWith(prefix, StringComparison.Ordinal));
        return line[prefix.Length..];
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
                    'fake release-core benchmark artifact')
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
