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
        Assert.Contains("dotnet pack src/CSharpDB/CSharpDB.csproj", workflow);
    }

    [Fact]
    public void ReleaseWorkflow_VerifiesNuGetPackagesBeforeCreatingGitHubRelease()
    {
        string repoRoot = FindRepoRoot();
        string workflow = File.ReadAllText(Path.Combine(repoRoot, ".github", "workflows", "release.yml"));

        int verifyIndex = workflow.IndexOf("Wait-NuGetPackageVersion.ps1", StringComparison.Ordinal);
        int releaseIndex = workflow.IndexOf("softprops/action-gh-release", StringComparison.Ordinal);

        Assert.True(verifyIndex >= 0, "Release workflow must call the NuGet visibility verification script.");
        Assert.True(releaseIndex > verifyIndex, "NuGet verification must run before the GitHub Release is created.");

        string[] packageIds =
        [
            "CSharpDB",
            "CSharpDB.Primitives",
            "CSharpDB.Sql",
            "CSharpDB.Storage",
            "CSharpDB.Execution",
            "CSharpDB.Engine",
            "CSharpDB.Pipelines",
            "CSharpDB.Data",
            "CSharpDB.EntityFrameworkCore",
            "CSharpDB.Storage.Diagnostics",
            "CSharpDB.Client",
        ];

        foreach (string packageId in packageIds)
            Assert.Contains($"'{packageId}'", workflow);
    }

    [Fact]
    public void ReleaseTrigger_UsesFreshRegistrationAndTrustedPublishingImplementation()
    {
        string repoRoot = FindRepoRoot();
        string trigger = File.ReadAllText(Path.Combine(
            repoRoot,
            ".github",
            "workflows",
            "publish-release.yml")).ReplaceLineEndings("\n");
        string implementation = File.ReadAllText(Path.Combine(
            repoRoot,
            ".github",
            "workflows",
            "release.yml")).ReplaceLineEndings("\n");

        Assert.Contains("name: Release\n", trigger);
        Assert.Contains("      - \"v*\"", trigger);
        Assert.DoesNotContain("workflow_dispatch:", trigger);
        Assert.Contains("group: release-${{ github.ref }}", trigger);
        Assert.Contains("uses: ./.github/workflows/release.yml", trigger);
        Assert.Contains("NUGET_USER: ${{ secrets.NUGET_USER }}", trigger);
        Assert.DoesNotContain("secrets: inherit", trigger);
        Assert.DoesNotContain("waive_local_durable", trigger);
        Assert.Contains("contents: write", trigger);
        Assert.Contains("statuses: read", trigger);
        Assert.Contains("id-token: write", trigger);
        Assert.Contains("cancel-in-progress: false", trigger);

        Assert.Contains("workflow_call:", implementation);
        Assert.DoesNotContain("tags:", implementation);
        Assert.Contains("uses: NuGet/login@v1", implementation);
    }

    [Fact]
    public void SqlReleaseQualificationWorkflow_RunsFunctionalAndHostedStablePerformancePasses()
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
        Assert.Contains(
            """
            qualification_pass:
                      - 1
                      - 2
            """.ReplaceLineEndings("\n"),
            normalized);
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
            "previous-release-hosted-stable-performance-${{ inputs.release_commit || github.sha }}",
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
    public void ReleaseWorkflow_GatesEveryPublisherOnFunctionalHostedAndLocalQualification()
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
            "previous_release_ref: ${{ inputs.release_tag == 'v4.5.1' && '86e25435f3c64f47afe2a776c6b03cbe84e56858' || '' }}",
            normalized);
        Assert.Contains("verify-local-durable-performance:", normalized);
        Assert.Contains("name: Verify local durable performance qualification", normalized);
        Assert.Contains("statuses: read", normalized);
        Assert.Contains("LOCAL_DURABLE_ATTESTOR", normalized);
        Assert.Contains("github.repository_owner", normalized);
        Assert.Contains("- name: Checkout exact release source", normalized);
        Assert.Contains("uses: actions/checkout@v7", normalized);
        Assert.Contains(
            "./scripts/Test-LocalDurableStatus.ps1",
            normalized);
        Assert.Contains(
            """
                  - name: Require a successful matching-commit local performance status
                    shell: pwsh
            """.ReplaceLineEndings("\n"),
            normalized);
        Assert.DoesNotContain("waive_local_durable", normalized);
        Assert.Contains("-Commit '${{ inputs.release_commit }}'", normalized);
        Assert.Contains("-ReleaseVersion '${{ inputs.release_tag }}'", normalized);
        Assert.Contains("-GitHubRepository '${{ github.repository }}'", normalized);
        Assert.Contains("-ExpectedCreator $env:EXPECTED_STATUS_CREATOR", normalized);
        Assert.DoesNotContain("github.ref_name", normalized);
        Assert.DoesNotContain("github.sha", normalized);
        Assert.Equal(
            7,
            System.Text.RegularExpressions.Regex.Matches(
                normalized,
                @"(?m)^          ref: \$\{\{ inputs\.release_commit \}\}$").Count);
        Assert.Equal(
            5,
            System.Text.RegularExpressions.Regex.Matches(
                normalized,
                @"(?m)^    needs: build-and-test$").Count);
        Assert.Single(
            System.Text.RegularExpressions.Regex.Matches(
                normalized,
                @"(?m)^    needs: verify-local-durable-performance$"));
    }

    [Fact]
    public void ReleaseWorkflow_V451HostedBaselineUsesLastPublishedV440Only()
    {
        string repoRoot = FindRepoRoot();
        string implementation = File.ReadAllText(Path.Combine(
            repoRoot,
            ".github",
            "workflows",
            "release.yml")).ReplaceLineEndings("\n");

        const string expectedBaselineInput =
            "previous_release_ref: ${{ inputs.release_tag == 'v4.5.1' && " +
            "'86e25435f3c64f47afe2a776c6b03cbe84e56858' || '' }}";

        Assert.Contains("v4.5.0 was tagged but never published", implementation);
        Assert.Contains(expectedBaselineInput, implementation);
        Assert.Single(
            System.Text.RegularExpressions.Regex.Matches(
                implementation,
                @"(?m)^\s+previous_release_ref:"));
        Assert.DoesNotContain("waive", implementation, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ReleaseWorkflow_IsTagOnlyAndHasNoManualRecoveryOrDurableWaiver()
    {
        string repoRoot = FindRepoRoot();
        string trigger = File.ReadAllText(Path.Combine(
            repoRoot,
            ".github",
            "workflows",
            "publish-release.yml")).ReplaceLineEndings("\n");
        string implementation = File.ReadAllText(Path.Combine(
            repoRoot,
            ".github",
            "workflows",
            "release.yml")).ReplaceLineEndings("\n");

        Assert.Contains("      - \"v*\"", trigger);
        Assert.Contains("group: release-${{ github.ref }}", trigger);
        Assert.Contains("release_tag: ${{ github.ref_name }}", trigger);
        Assert.Contains("release_commit: ${{ github.sha }}", trigger);
        Assert.Contains("NUGET_USER: ${{ secrets.NUGET_USER }}", trigger);
        Assert.DoesNotContain("workflow_dispatch:", trigger);
        Assert.DoesNotContain("recovery", trigger, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("waive", trigger, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("v4.5.0", trigger);
        Assert.DoesNotContain("statuses: write", trigger);
        Assert.DoesNotContain("secrets: inherit", trigger);

        Assert.Contains("- name: Require the exact existing release tag and commit", implementation);
        Assert.Contains("- name: Require a successful matching-commit local performance status", implementation);
        Assert.Contains("./scripts/Test-LocalDurableStatus.ps1", implementation);
        Assert.DoesNotContain("workflow_dispatch", implementation);
        Assert.DoesNotContain("waive", implementation, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("7aeb66031237283c3643e73849618bf299729ed6", implementation);
        Assert.Contains("- name: Revalidate the immutable publication target", implementation);
        Assert.Contains("- name: Require the immutable target and absent release before creation", implementation);
        Assert.Contains("packages will not be published", implementation);
        Assert.Contains("publication will not update it", implementation);
        Assert.DoesNotContain("Publish-DurableCarryForwardStatus.ps1", implementation);
        Assert.DoesNotContain("statuses: write", implementation);
        Assert.Equal(
            2,
            System.Text.RegularExpressions.Regex.Matches(
                implementation,
                @"(?m)^          tag_name: \$\{\{ inputs\.release_tag \}\}$").Count);
        Assert.Equal(
            2,
            System.Text.RegularExpressions.Regex.Matches(
                implementation,
                @"(?m)^          target_commitish: \$\{\{ inputs\.release_commit \}\}$").Count);
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
