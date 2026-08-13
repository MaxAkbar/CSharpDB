namespace CSharpDB.Daemon.Tests;

public sealed class DaemonPackagingAssetsTests
{
    [Fact]
    public void PublishScript_UsesExpectedDaemonArchiveContract()
    {
        string repoRoot = FindRepoRoot();
        string script = File.ReadAllText(Path.Combine(repoRoot, "scripts", "Publish-CSharpDbDaemonRelease.ps1"));

        Assert.Contains("win-x64", script);
        Assert.Contains("linux-x64", script);
        Assert.Contains("osx-arm64", script);
        Assert.Contains("-p:PublishSingleFile=true", script);
        Assert.Contains("-p:PublishTrimmed=false", script);
        Assert.Contains("csharpdb-daemon-v$ReleaseVersion-$Rid", script);
        Assert.Contains("SHA256SUMS.txt", script);
    }

    [Fact]
    public void MainBranchCiPackSmoke_IncludesMetaPackage()
    {
        string repoRoot = FindRepoRoot();
        string workflow = File.ReadAllText(Path.Combine(repoRoot, ".github", "workflows", "ci.yml"));

        Assert.Contains("src/CSharpDB/README.md", workflow);
        Assert.Contains("src/CSharpDB.Observability/README.md", workflow);
        Assert.Contains("dotnet pack src/CSharpDB.Observability/CSharpDB.Observability.csproj", workflow);
        Assert.Contains("dotnet pack src/CSharpDB/CSharpDB.csproj", workflow);
        Assert.Contains("Test-ObservabilityNuGetPackage.ps1", workflow);
        Assert.Contains("Test-CSharpDbNuGetReleaseGraph.ps1", workflow);
        Assert.Contains("CSharpDB-PACKAGE-MANIFEST.json", workflow);
    }

    [Fact]
    public void Ci_BuildsTheSynchronizedReleaseHarnessAgainstThePublishedBaseline()
    {
        string repoRoot = FindRepoRoot();
        string workflow = File.ReadAllText(Path.Combine(
            repoRoot,
            ".github",
            "workflows",
            "ci.yml"));

        Assert.Contains("release-baseline-compatibility:", workflow);
        Assert.Contains("fetch-depth: 0", workflow);
        Assert.Contains("Test-PreviousReleasePerformance.ps1", workflow);
        Assert.Contains("-PreviousRef 2f141433298bcd3137e2bcaa2930c796c4222092", workflow);
        Assert.Contains("-CandidateRef $env:CANDIDATE_REF", workflow);
        Assert.Contains("-BuildOnly", workflow);
        Assert.Contains("previous-release-performance.md", workflow);
    }

    [Fact]
    public void CiAndRelease_QualifyPublishedApiAndDaemonObservabilityOnEverySupportedHost()
    {
        string repoRoot = FindRepoRoot();
        string ci = File.ReadAllText(Path.Combine(
            repoRoot,
            ".github",
            "workflows",
            "ci.yml"));
        string release = File.ReadAllText(Path.Combine(
            repoRoot,
            ".github",
            "workflows",
            "release.yml"));
        string publisher = File.ReadAllText(Path.Combine(
            repoRoot,
            ".github",
            "workflows",
            "publish-release.yml"));
        string qualifier = File.ReadAllText(Path.Combine(
            repoRoot,
            "scripts",
            "Test-CSharpDbPublishedHostObservability.ps1"));

        foreach (string runtime in new[] { "win-x64", "linux-x64", "osx-arm64" })
        {
            Assert.Contains(runtime, ci);
            Assert.Contains(runtime, release);
            Assert.Contains(runtime, qualifier);
        }

        Assert.Contains("published-host-observability:", ci);
        Assert.Contains("host: Api", ci);
        Assert.Contains("host: Daemon", ci);
        Assert.Contains("api-host-observability:", release);
        Assert.Contains("-HostName Api", release);
        Assert.Contains("-HostName Daemon", release);
        Assert.Contains("/health/live", qualifier);
        Assert.Contains("/health/ready", qualifier);
        Assert.Contains("/metrics", qualifier);
        Assert.Contains("/api/diagnostics/runtime", qualifier);
        Assert.Contains("private database path canary", qualifier);
        Assert.Contains("Stop-QualifiedHost", qualifier);
        Assert.Contains("CSharpDbHostQualificationCleanup.ps1", qualifier);
        Assert.Contains("Remove-CSharpDbDirectoryWithRetry", qualifier);
        Assert.Contains("orderly shutdown", qualifier, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("did not complete orderly shutdown", qualifier);
        Assert.Contains("cleanup only", qualifier, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("return $false", qualifier);
        Assert.Contains("--self-contained true", ci);
        Assert.Contains("--self-contained true", release);
        Assert.Contains("NativeAOT executable was not produced", ci);
        Assert.Contains("NativeAOT executable was not produced", release);
    }

    [Fact]
    public void NuGetReleaseGraph_RequiresExactClosedTopologicalHashedCandidates()
    {
        string repoRoot = FindRepoRoot();
        string script = File.ReadAllText(Path.Combine(
            repoRoot,
            "scripts",
            "Test-CSharpDbNuGetReleaseGraph.ps1"));
        string ci = File.ReadAllText(Path.Combine(
            repoRoot,
            ".github",
            "workflows",
            "ci.yml"));
        string release = File.ReadAllText(Path.Combine(
            repoRoot,
            ".github",
            "workflows",
            "release.yml"));
        string publisher = File.ReadAllText(Path.Combine(
            repoRoot,
            ".github",
            "workflows",
            "publish-release.yml"));

        Assert.Contains("Test-ExactCandidateRange", script);
        Assert.Contains("$normalized -ceq \"[$CandidateVersion]\"", script);
        Assert.Contains("references in-release dependency", script);
        Assert.Contains("Release package order is not topological", script);
        Assert.Contains("dependency graph contains a cycle", script);
        Assert.Contains("uses floating dependency", script);
        Assert.Contains("contains unreviewed prerelease dependency", script);
        Assert.Contains("required package metadata", script);
        Assert.Contains("Get-FileHash", script);
        Assert.Contains("Sort-Object", script);
        Assert.Contains("CSharpDB-PACKAGE-MANIFEST.json", script);
        Assert.Contains("Test-CSharpDbNuGetReleaseGraph.ps1", ci);
        Assert.Contains("Test-CSharpDbNuGetReleaseGraph.ps1", release);
        Assert.Contains("CSharpDB-PACKAGE-MANIFEST.json", ci);
        Assert.Contains("CSharpDB-PACKAGE-MANIFEST.json", release);
        Assert.Contains("Test-CSharpDbNuGetCandidateAbsent.ps1", release);
        Assert.Contains("-ValidateExistingManifest", release);
        Assert.Contains("--skip-duplicate", publisher);
        Assert.Contains("--target main", publisher);
        Assert.Contains("$release.target_commitish -cne 'main'", publisher);
        Assert.Contains("--json databaseId", publisher);
        Assert.Contains("releases/$releaseId/assets?per_page=100", publisher);
        Assert.DoesNotContain("releases/tags/$env:RELEASE_TAG", publisher);
        Assert.Contains(
            "Publish packages in validated topological order",
            publisher);
        Assert.Contains("foreach ($package in $ordered)", publisher);
        Assert.DoesNotContain(
            "for package in artifacts/nuget/*.nupkg",
            release);

        string candidateAbsence = File.ReadAllText(Path.Combine(
            repoRoot,
            "scripts",
            "Test-CSharpDbNuGetCandidateAbsent.ps1"));
        Assert.Contains("Invoke-WebRequest", candidateAbsence);
        Assert.Contains("-Method Head", candidateAbsence);
        Assert.Contains("is already published", candidateAbsence);
        Assert.Contains("$statusCode -eq 404", candidateAbsence);
    }

    [Fact]
    public void PublicationWorkflow_PublishesObservabilityBeforeDependentPackages()
    {
        string repoRoot = FindRepoRoot();
        string workflow = File.ReadAllText(Path.Combine(repoRoot, ".github", "workflows", "publish-release.yml"));

        int observabilityPushIndex = workflow.IndexOf(
            "@($manifest.packages | Where-Object { $_.id -ceq 'CSharpDB.Observability' })",
            StringComparison.Ordinal);
        int dependentPushIndex = workflow.IndexOf(
            "@($manifest.packages | Where-Object { $_.id -cne 'CSharpDB.Observability' })",
            StringComparison.Ordinal);
        int observabilityWaitIndex = workflow.IndexOf(
            "Wait-NuGetPackageVersion.ps1",
            dependentPushIndex,
            StringComparison.Ordinal);

        Assert.True(observabilityPushIndex >= 0, "Observability must be first in the release order.");
        Assert.True(
            dependentPushIndex > observabilityPushIndex,
            "Dependent packages must be ordered after observability.");
        Assert.True(
            observabilityWaitIndex > observabilityPushIndex,
            "The ordered publication loop must verify package visibility.");
        Assert.Contains("foreach ($package in $ordered)", workflow);
        Assert.Contains("-PackageId $package.id", workflow);

        string versionResolver = File.ReadAllText(Path.Combine(
            repoRoot,
            "scripts",
            "Get-NuGetPackageIdentityVersion.ps1"));
        string packageSmoke = File.ReadAllText(Path.Combine(
            repoRoot,
            "scripts",
            "Test-ObservabilityNuGetPackage.ps1"));
        string packageWait = File.ReadAllText(Path.Combine(
            repoRoot,
            "scripts",
            "Wait-NuGetPackageVersion.ps1"));

        Assert.Contains("IndexOf('+')", versionResolver);
        Assert.Contains("Get-NuGetPackageIdentityVersion.ps1", packageSmoke);
        Assert.Contains("Get-NuGetPackageIdentityVersion.ps1", packageWait);
    }

    [Fact]
    public void PublicationWorkflow_StagesAssetsThenVerifiesPackagesBeforeFinalizingRelease()
    {
        string repoRoot = FindRepoRoot();
        string workflow = File.ReadAllText(Path.Combine(repoRoot, ".github", "workflows", "publish-release.yml"));

        int draftIndex = workflow.IndexOf(
            "- name: Create or refresh draft GitHub Release",
            StringComparison.Ordinal);

        int verifyStepIndex = workflow.IndexOf(
            "- name: Verify every package is visible",
            StringComparison.Ordinal);
        int verifyIndex = verifyStepIndex < 0
            ? -1
            : workflow.IndexOf(
                "Wait-NuGetPackageVersion.ps1",
                verifyStepIndex,
                StringComparison.Ordinal);
        int releaseIndex = workflow.IndexOf(
            "- name: Publish the staged GitHub Release",
            StringComparison.Ordinal);

        Assert.True(draftIndex >= 0);
        Assert.True(
            verifyIndex > verifyStepIndex,
            "The final package-visibility step must call the NuGet verification script.");
        Assert.True(releaseIndex > verifyIndex, "NuGet verification must run before the GitHub Release is created.");

        Assert.Contains("-PackageId @($manifest.packages.id)", workflow);
    }

    [Fact]
    public void ReleaseWorkflows_AreManualTagLastAndUseTrustedPublishing()
    {
        string repoRoot = FindRepoRoot();
        string implementation = File.ReadAllText(Path.Combine(
            repoRoot,
            ".github",
            "workflows",
            "release.yml")).ReplaceLineEndings("\n");
        string publication = File.ReadAllText(Path.Combine(
            repoRoot,
            ".github",
            "workflows",
            "publish-release.yml")).ReplaceLineEndings("\n");

        Assert.Contains("name: Release qualification\n", implementation);
        Assert.Contains("workflow_dispatch:", implementation);
        Assert.Contains("run-name: Qualify release ${{ inputs.release_tag }} at ${{ inputs.release_commit }}", implementation);
        Assert.Contains("group: release-${{ inputs.release_tag }}", implementation);
        Assert.DoesNotContain("push:\n    tags:", implementation);
        Assert.DoesNotContain("workflow_call:", implementation);
        Assert.DoesNotContain("git/refs", implementation);
        Assert.Contains("cancel-in-progress: false", implementation);
        Assert.Contains("name: Publish qualified release\n", publication);
        Assert.Contains("workflow_dispatch:", publication);
        Assert.Contains("preflight_only:", publication);
        Assert.Contains("uses: NuGet/login@v1", publication);
        Assert.Contains("id-token: write", publication);
        Assert.DoesNotContain("push:\n    tags:", publication);
    }

    [Fact]
    public void SqlReleaseQualificationWorkflow_RunsOneFunctionalPassPerOsAndOneHostedPairedBenchmark()
    {
        string repoRoot = FindRepoRoot();
        string workflow = File.ReadAllText(Path.Combine(
            repoRoot,
            ".github",
            "workflows",
            "sql-release-qualification.yml"));
        string normalized = workflow.ReplaceLineEndings("\n");

        Assert.Contains("workflow_call:", normalized);
        Assert.Contains("workflow_dispatch:", normalized);
        Assert.Contains("qualification-*", normalized);
        Assert.Contains("clean: true", normalized);
        Assert.Contains("- ubuntu-latest", normalized);
        Assert.Contains("- windows-latest", normalized);
        Assert.Contains("- macos-latest", normalized);
        Assert.DoesNotContain("qualification_pass:", normalized);
        Assert.Equal(
            2,
            System.Text.RegularExpressions.Regex.Matches(
                normalized,
                @"(?m)^\s+-QualificationPass 1 `$").Count);
        Assert.DoesNotContain("-QualificationPass 2", normalized);
        Assert.Contains("Test-SqlReleaseQualification.ps1", normalized);
        Assert.Contains("${{ runner.temp }}", normalized);
        Assert.Equal(
            2,
            System.Text.RegularExpressions.Regex.Matches(
                normalized,
                @"(?m)^          ref: \$\{\{ inputs\.release_commit \|\| github\.sha \}\}$").Count);
        Assert.Contains(
            "sql-release-qualification-${{ inputs.release_commit || github.sha }}",
            normalized);
        Assert.Contains(
            "sql-release-qualification-${{ inputs.release_commit || github.sha }}-${{ matrix.os }}-attempt-${{ github.run_attempt }}",
            normalized);
        Assert.Contains(
            "previous-release-hosted-stable-performance-${{ inputs.release_commit || github.sha }}",
            normalized);
        Assert.Contains(
            "previous-release-hosted-stable-performance-${{ inputs.release_commit || github.sha }}-attempt-${{ github.run_attempt }}",
            normalized);
        Assert.DoesNotContain(
            "    env:\n      QUALIFICATION_OUTPUT: ${{ runner.temp }}",
            normalized);
        Assert.Contains("previous_release_ref:", normalized);
        Assert.Contains("previous-release-hosted-stable-performance:", normalized);
        Assert.Contains("Test-PreviousReleasePerformance.ps1", normalized);
        Assert.Contains("PERFORMANCE_OUTPUT", normalized);
        Assert.Contains("-SuiteName master-table-hosted-stable", normalized);
        Assert.Contains("-RepeatCount 3", normalized);
        Assert.Contains("-PostBuildQuiescenceSeconds 30", normalized);
        Assert.Contains("-MaxThroughputRegressionPercent 15", normalized);
        Assert.Contains("-MaxP99RegressionPercent 25", normalized);
        Assert.Contains("-MaxP99RegressionMilliseconds 0.05", normalized);
        Assert.Contains("-BlockingLatencyPercentile P95", normalized);
        Assert.Contains("timeout-minutes: 180", normalized);
        Assert.DoesNotContain("master-table-durable-writes", normalized);
    }

    [Fact]
    public void LocalDurablePerformanceScript_RunsTwoSequentialFailClosedPassesOutsideGitHub()
    {
        string repoRoot = FindRepoRoot();
        string wrapper = File.ReadAllText(Path.Combine(
            repoRoot,
            "tests",
            "CSharpDB.Benchmarks",
            "scripts",
            "Test-LocalDurablePerformance.ps1"));
        string comparison = File.ReadAllText(Path.Combine(
            repoRoot,
            "tests",
            "CSharpDB.Benchmarks",
            "scripts",
            "Test-PreviousReleasePerformance.ps1"));

        Assert.Contains("foreach ($qualificationPass in 1, 2)", wrapper);
        Assert.Contains("CandidateRef = $candidateCommit", wrapper);
        Assert.Contains("PreviousRef = $previousCommit", wrapper);
        Assert.Contains("Paired = $true", wrapper);
        Assert.Contains("SuiteName = @('master-table-durable-write-scenarios')", wrapper);
        Assert.Contains("RepeatCount = $RepeatCount", wrapper);
        Assert.Contains("PostBuildQuiescenceSeconds = $PostBuildQuiescenceSeconds", wrapper);
        Assert.Contains("InterSampleQuiescenceSeconds = $InterSampleQuiescenceSeconds", wrapper);
        Assert.Contains("$parameters.MonitorLocalEnvironment = $true", wrapper);
        Assert.Contains("Watch-LocalPerformanceEnvironment.ps1", wrapper);
        Assert.Contains("minimum-measured-seconds=30", comparison);
        Assert.Contains("minimum-retained-latency-samples=10000", comparison);
        Assert.Contains("MaxThroughputRegressionPercent = $MaxThroughputRegressionPercent", wrapper);
        Assert.Contains("MaxP99RegressionPercent = $MaxP99RegressionPercent", wrapper);
        Assert.Contains("MaxP99RegressionMilliseconds = $MaxP99RegressionMilliseconds", wrapper);
        Assert.Contains("BlockingLatencyPercentile = $BlockingLatencyPercentile", wrapper);
        Assert.Contains("CSHARPDB_BENCH_DURABILITY", wrapper);
        Assert.Contains("'Durable'", wrapper);
        Assert.Contains("ConfirmDedicatedFixedSsd", wrapper);
        Assert.Contains("csharpdb/local-durable-performance", wrapper);
        Assert.Contains("durable-v3", wrapper);
        Assert.DoesNotContain("durable-v2", wrapper, StringComparison.Ordinal);
        Assert.Contains("[string] $BlockingLatencyPercentile = 'P95'", wrapper);
        Assert.Contains("P99 latency: diagnostic only", wrapper);
        Assert.Contains("Use -NoGitHubStatus for diagnostic overrides", wrapper);
        Assert.Contains("Invoke-GitHubStatus", wrapper);
        Assert.Contains("continuing to collect the second pass", wrapper);
        Assert.Contains("requires a clean repository worktree", wrapper);
        Assert.Contains("output must be outside the repository", wrapper);
        Assert.Contains("local-durable-performance.md", wrapper);
        Assert.DoesNotContain("Start-Job", wrapper);
        Assert.DoesNotContain(".json", wrapper, StringComparison.OrdinalIgnoreCase);

        Assert.Contains("Name = 'master-table-durable-writes'", comparison);
        Assert.Contains("Arguments = @('--master-table-durable-writes')", comparison);
        Assert.Contains("Name = 'master-table-hosted-stable'", comparison);
        Assert.Contains("Arguments = @('--master-table-hosted-stable')", comparison);
    }

    [Fact]
    public void SupplementalPerformanceSuites_RemainReportOnlyOrManualDiagnostics()
    {
        string repoRoot = FindRepoRoot();
        string releaseWorkflow = File.ReadAllText(Path.Combine(
            repoRoot,
            ".github",
            "workflows",
            "sql-release-qualification.yml"));
        string guardrailWorkflow = File.ReadAllText(Path.Combine(
            repoRoot,
            ".github",
            "workflows",
            "perf-guardrails.yml"));
        string releaseThresholds = File.ReadAllText(Path.Combine(
            repoRoot,
            "tests",
            "CSharpDB.Benchmarks",
            "perf-thresholds.json"));
        string manualComparisonScript = File.ReadAllText(Path.Combine(
            repoRoot,
            "tests",
            "CSharpDB.Benchmarks",
            "scripts",
            "Test-PreviousReleasePerformance.ps1"));

        Assert.Contains("schedule:", guardrailWorkflow);
        Assert.Contains("-NoFailOnRegression", guardrailWorkflow);
        Assert.Contains("--durable-sql-batching", releaseThresholds);

        string[] manualSupplementalSuites =
        [
            "durable-sql-batching",
            "concurrent-write-diagnostics",
            "hybrid-storage-mode",
            "hybrid-hot-set-read",
            "hybrid-cold-open",
            "sqlite-compare",
        ];
        foreach (string suite in manualSupplementalSuites)
        {
            Assert.Contains($"Name = '{suite}'", manualComparisonScript);
            Assert.DoesNotContain(suite, releaseWorkflow);
        }
    }

    [Fact]
    public void ReleaseWorkflow_GatesEveryPublisherOnExactTargetFunctionalAndHostedQualification()
    {
        string repoRoot = FindRepoRoot();
        string workflow = File.ReadAllText(Path.Combine(
            repoRoot,
            ".github",
            "workflows",
            "release.yml"));
        string normalized = workflow.ReplaceLineEndings("\n");

        Assert.Contains(
            "uses: ./.github/workflows/sql-release-qualification.yml",
            normalized);
        Assert.Contains(
            "release_version: ${{ inputs.release_tag }}",
            normalized);
        Assert.Contains(
            "release_commit: ${{ inputs.release_commit }}",
            normalized);
        Assert.Contains(
            "previous_release_ref: ${{ inputs.release_tag == 'v4.6.2' && '2f141433298bcd3137e2bcaa2930c796c4222092' || " +
            "inputs.release_tag == 'v4.6.1' && '2f141433298bcd3137e2bcaa2930c796c4222092' || " +
            "inputs.release_tag == 'v4.5.1' && '86e25435f3c64f47afe2a776c6b03cbe84e56858' || '' }}",
            normalized);
        Assert.Contains("verify-release-candidate:", normalized);
        Assert.Contains("name: Verify exact release candidate", normalized);
        Assert.DoesNotContain("statuses:", normalized);
        Assert.DoesNotContain("LOCAL_DURABLE_ATTESTOR", normalized);
        Assert.Contains("- name: Checkout exact release source", normalized);
        Assert.Contains("uses: actions/checkout@v7", normalized);
        Assert.Contains("- name: Require exact untagged main and matching package version", normalized);
        Assert.DoesNotContain("Test-LocalDurableStatus.ps1", normalized);
        Assert.DoesNotContain("waive_local_durable", normalized);
        Assert.DoesNotContain("github.ref_name", normalized);
        Assert.DoesNotContain("github.sha", normalized);
        Assert.Equal(
            8,
            System.Text.RegularExpressions.Regex.Matches(
                normalized,
                @"(?m)^          ref: \$\{\{ inputs\.release_commit \}\}$").Count);
        Assert.Equal(
            6,
            System.Text.RegularExpressions.Regex.Matches(
                normalized,
                @"(?m)^    needs: build-and-test$").Count);
        Assert.Contains(
            "needs: [prepare-nuget, migration-archives, native-aot, api-host-observability, daemon-archives, admin-desktop-archive]",
            normalized);
        Assert.Contains(
            "needs: [prepare-nuget, migration-archives, native-aot, api-host-observability, daemon-archives, admin-desktop-archive]",
            normalized);
        Assert.Single(
            System.Text.RegularExpressions.Regex.Matches(
                normalized,
                @"(?m)^    needs: verify-release-candidate$"));
    }

    [Fact]
    public void ReleaseWorkflow_RecoveryTagsUseTheirLastPublishedBaselinesOnly()
    {
        string repoRoot = FindRepoRoot();
        string implementation = File.ReadAllText(Path.Combine(
            repoRoot,
            ".github",
            "workflows",
            "release.yml")).ReplaceLineEndings("\n");

        const string expectedBaselineInput =
            "previous_release_ref: ${{ inputs.release_tag == 'v4.6.2' && " +
            "'2f141433298bcd3137e2bcaa2930c796c4222092' || " +
            "inputs.release_tag == 'v4.6.1' && " +
            "'2f141433298bcd3137e2bcaa2930c796c4222092' || " +
            "inputs.release_tag == 'v4.5.1' && " +
            "'86e25435f3c64f47afe2a776c6b03cbe84e56858' || '' }}";

        Assert.Contains("v4.6.0 and v4.6.1 were tagged but never published", implementation);
        Assert.Contains("published v4.5.1 commit", implementation);
        Assert.Contains(expectedBaselineInput, implementation);
        Assert.Single(
            System.Text.RegularExpressions.Regex.Matches(
                implementation,
                @"(?m)^\s+previous_release_ref:"));
        Assert.DoesNotContain("waive", implementation, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ReleaseWorkflow_IsTagLastAndHasNoManualTagTriggerOrWaiver()
    {
        string repoRoot = FindRepoRoot();
        string implementation = File.ReadAllText(Path.Combine(
            repoRoot,
            ".github",
            "workflows",
            "release.yml")).ReplaceLineEndings("\n");

        Assert.Contains("workflow_dispatch:", implementation);
        Assert.DoesNotContain("push:\n    tags:", implementation);
        Assert.Contains("- name: Require exact untagged main and matching package version", implementation);
        Assert.Contains("prepare-release-bundle:", implementation);
        Assert.Contains("name: Prepare final release bundle", implementation);
        Assert.Contains("needs: [prepare-nuget, migration-archives, native-aot, api-host-observability, daemon-archives, admin-desktop-archive]", implementation);
        Assert.Contains("- name: Verify complete immutable release bundle", implementation);
        Assert.Contains("-ValidateExistingManifest", implementation);
        Assert.Contains("$expectedHeading = \"## CSharpDB ${{ needs.prepare-nuget.outputs.version }}\"", implementation);
        Assert.DoesNotContain("matching-commit local performance status", implementation);
        Assert.DoesNotContain("Test-LocalDurableStatus.ps1", implementation);
        Assert.DoesNotContain("waive", implementation, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("7aeb66031237283c3643e73849618bf299729ed6", implementation);
        Assert.DoesNotContain("Publish-DurableCarryForwardStatus.ps1", implementation);
        Assert.DoesNotContain("statuses: write", implementation);
        Assert.DoesNotContain("git/refs", implementation);
        Assert.DoesNotContain("dotnet nuget push", implementation);
        Assert.DoesNotContain("softprops/action-gh-release", implementation);
    }

    [Fact]
    public void PreviousReleasePerformanceScript_UsesLongPathSafeWorktreeLifecycle()
    {
        string repoRoot = FindRepoRoot();
        string script = File.ReadAllText(Path.Combine(
            repoRoot,
            "tests",
            "CSharpDB.Benchmarks",
            "scripts",
            "Test-PreviousReleasePerformance.ps1"));

        Assert.Equal(
            4,
            System.Text.RegularExpressions.Regex.Matches(
                script,
                @"(?m)^\s*& git -C \$repositoryRoot -c core\.longpaths=true").Count);
        Assert.Contains("$candidateRemoveOutput = @(", script);
        Assert.Contains("$baselineRemoveOutput = @(", script);
        Assert.Contains("$candidateRemoveFailure +=", script);
        Assert.Contains("$baselineRemoveFailure +=", script);
    }

    [Fact]
    public void SqlReleaseQualificationScript_CoversFullSuiteAndIsolationBoundaries()
    {
        string repoRoot = FindRepoRoot();
        string script = File.ReadAllText(Path.Combine(
            repoRoot,
            "scripts",
            "Test-SqlReleaseQualification.ps1"));

        Assert.Contains("status --porcelain=v1 --untracked-files=all", script);
        Assert.Contains("Qualification output must be outside the repository", script);
        Assert.Contains("'test',", script);
        Assert.Contains("$solutionPath", script);
        Assert.Contains("Test-Documentation.ps1", script);
        Assert.Contains("Test-NuGetPackageClosure.ps1", script);
        Assert.Contains("Test-EfCoreVersionConsistency.ps1", script);
        Assert.Contains("Test-SqlServerMigrationIsolation.ps1", script);
        Assert.Contains("Test-MySqlMigrationIsolation.ps1", script);
        Assert.Contains("Test-AccessMigrationIsolation.ps1", script);
        Assert.Contains("Test-EfCoreMigrationTool.ps1", script);

        System.Text.RegularExpressions.Match fullTestArguments =
            System.Text.RegularExpressions.Regex.Match(
                script,
                @"(?s)-StepName 'full-test-suite'.*?-ArgumentList @\((?<arguments>.*?)\)");
        Assert.True(fullTestArguments.Success);
        Assert.Contains(
            "'--maxcpucount:1'",
            fullTestArguments.Groups["arguments"].Value,
            StringComparison.Ordinal);

        System.Text.RegularExpressions.Match accessArguments =
            System.Text.RegularExpressions.Regex.Match(
                script,
                @"(?s)-StepName 'access-migration-isolation'.*?-ArgumentList @\((?<arguments>.*?)\)");
        Assert.True(accessArguments.Success);
        Assert.DoesNotContain(
            "'-NoRestore'",
            accessArguments.Groups["arguments"].Value,
            StringComparison.Ordinal);
    }

    [Fact]
    public void NuGetVerificationScript_PollsHandlesMissingPackagesAndTimesOut()
    {
        string repoRoot = FindRepoRoot();
        string script = File.ReadAllText(Path.Combine(repoRoot, "scripts", "Wait-NuGetPackageVersion.ps1"));

        Assert.Contains("Invoke-WebRequest", script);
        Assert.Contains("-Method Head", script);
        Assert.Contains("$statusCode -eq 404", script);
        Assert.Contains("TimeoutSeconds", script);
        Assert.Contains("Timed out waiting for NuGet package version", script);
    }

    [Fact]
    public void ServiceInstallAssets_ContainExpectedDefaults()
    {
        string repoRoot = FindRepoRoot();

        string windowsInstall = File.ReadAllText(Path.Combine(
            repoRoot,
            "deploy",
            "daemon",
            "windows",
            "install-csharpdb-daemon.ps1"));
        string linuxInstall = File.ReadAllText(Path.Combine(
            repoRoot,
            "deploy",
            "daemon",
            "linux",
            "install-csharpdb-daemon.sh"));
        string macInstall = File.ReadAllText(Path.Combine(
            repoRoot,
            "deploy",
            "daemon",
            "macos",
            "install-csharpdb-daemon.sh"));

        Assert.Contains("CSharpDBDaemon", windowsInstall);
        Assert.Contains("CSharpDB\\Daemon", windowsInstall);
        Assert.Contains("CSharpDB", windowsInstall);
        Assert.Contains("http://127.0.0.1:5820", windowsInstall);
        Assert.Contains("CSharpDB__Daemon__EnableRestApi=true", windowsInstall);
        Assert.Contains("CSharpDB__Daemon__Security__Mode=None", windowsInstall);
        Assert.Contains("CSharpDB__Daemon__Security__ApiKeyHeaderName=X-CSharpDB-Api-Key", windowsInstall);

        Assert.Contains("/opt/csharpdb-daemon", linuxInstall);
        Assert.Contains("/var/lib/csharpdb", linuxInstall);
        Assert.Contains("SERVICE_USER=\"csharpdb\"", linuxInstall);
        Assert.Contains("http://127.0.0.1:5820", linuxInstall);
        Assert.Contains("CSharpDB__Daemon__EnableRestApi=true", linuxInstall);
        Assert.Contains("CSharpDB__Daemon__Security__Mode=None", linuxInstall);
        Assert.Contains("CSharpDB__Daemon__Security__ApiKeyHeaderName=X-CSharpDB-Api-Key", linuxInstall);

        Assert.Contains("com.csharpdb.daemon", macInstall);
        Assert.Contains("/usr/local/lib/csharpdb-daemon", macInstall);
        Assert.Contains("/usr/local/var/csharpdb", macInstall);
        Assert.Contains("http://127.0.0.1:5820", macInstall);
        Assert.Contains("\"EnableRestApi\": true", macInstall);
        Assert.Contains("\"Mode\": \"None\"", macInstall);
        Assert.Contains("\"ApiKeyHeaderName\": \"X-CSharpDB-Api-Key\"", macInstall);
    }

    [Fact]
    public void ServiceTemplates_AreParameterizedForInstallScripts()
    {
        string repoRoot = FindRepoRoot();
        string systemdTemplate = File.ReadAllText(Path.Combine(
            repoRoot,
            "deploy",
            "daemon",
            "linux",
            "csharpdb-daemon.service"));
        string launchdTemplate = File.ReadAllText(Path.Combine(
            repoRoot,
            "deploy",
            "daemon",
            "macos",
            "com.csharpdb.daemon.plist"));

        Assert.Contains("{{INSTALL_DIR}}", systemdTemplate);
        Assert.Contains("{{ENV_FILE}}", systemdTemplate);
        Assert.Contains("{{SERVICE_USER}}", systemdTemplate);
        Assert.Contains("{{SERVICE_GROUP}}", systemdTemplate);

        Assert.Contains("{{SERVICE_NAME}}", launchdTemplate);
        Assert.Contains("{{INSTALL_DIR}}", launchdTemplate);
        Assert.Contains("{{DATABASE_PATH}}", launchdTemplate);
        Assert.Contains("{{URL}}", launchdTemplate);
        Assert.Contains("CSharpDB__Daemon__EnableRestApi", launchdTemplate);
        Assert.Contains("CSharpDB__Daemon__Security__Mode", launchdTemplate);
        Assert.Contains("CSharpDB__Daemon__Security__ApiKeyHeaderName", launchdTemplate);
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

        throw new DirectoryNotFoundException("Could not locate repository root from test base directory.");
    }
}
