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
    public void SqlReleaseQualificationWorkflow_RunsTwoCleanBalancedPairedPassesAndBlocksOnlyOnMasterTable()
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
        Assert.DoesNotContain(
            "    env:\n      QUALIFICATION_OUTPUT: ${{ runner.temp }}",
            normalized);
        Assert.DoesNotContain(
            "    env:\n      PERFORMANCE_OUTPUT: ${{ runner.temp }}",
            normalized);
        Assert.Contains(
            "PERFORMANCE_OUTPUT: ${{ runner.temp }}/cdb-perf/p${{ matrix.qualification_pass }}",
            normalized);
        Assert.DoesNotContain(
            "csharpdb-previous-release-performance/${{ github.sha }}",
            normalized);
        Assert.Contains("previous_release_ref:", normalized);
        Assert.Contains(
            "empty discovers the nearest prior semantic release",
            normalized);
        Assert.DoesNotContain("default: v4.3.0", normalized);
        int performanceJobIndex = normalized.IndexOf(
            "  previous-release-performance:\n",
            StringComparison.Ordinal);
        Assert.True(
            performanceJobIndex >= 0,
            "Performance qualification job must be present.");
        string performanceJob = normalized[performanceJobIndex..];
        Assert.Contains(
            "  previous-release-performance:\n" +
            "    name: Windows previous-release master-table / balanced paired pass ${{ matrix.qualification_pass }}\n" +
            "    needs: qualify\n",
            performanceJob);
        Assert.Contains(
            "        qualification_pass:\n" +
            "          - 1\n" +
            "          - 2\n",
            performanceJob);
        Assert.Contains(
            "    strategy:\n      fail-fast: false\n      matrix:\n",
            performanceJob);
        Assert.DoesNotContain("        suite:\n", performanceJob);
        Assert.Contains("Test-PreviousReleasePerformance.ps1", normalized);
        Assert.Contains(
            "-CandidateRef $env:CANDIDATE_REF",
            normalized);
        Assert.Contains(
            "-QualificationPass ${{ matrix.qualification_pass }}",
            normalized);
        Assert.Contains("-SuiteName master-table", performanceJob);
        Assert.Contains("timeout-minutes: 300", performanceJob);
        Assert.Contains("            -Paired `\n", performanceJob);
        Assert.DoesNotContain("-Paired:", performanceJob);
        string[] supplementalSuites =
        [
            "durable-sql-batching",
            "concurrent-write-diagnostics",
            "hybrid-storage-mode",
            "hybrid-hot-set-read",
            "hybrid-cold-open",
            "sqlite-compare",
        ];
        foreach (string suite in supplementalSuites)
            Assert.DoesNotContain(suite, performanceJob);
        Assert.Contains("-RepeatCount 3", normalized);
        Assert.Contains("-PostBuildQuiescenceSeconds 30", normalized);
        Assert.Contains("-MaxThroughputRegressionPercent 15", normalized);
        Assert.Contains("-MaxP99RegressionPercent 25", normalized);
        Assert.Contains("-MaxP99RegressionMilliseconds 0.05", normalized);
        Assert.Contains(
            "previous-release-performance-preflight.md",
            normalized);
        Assert.Contains("baseline-results", normalized);
        Assert.Contains("candidate-results", normalized);
        Assert.Contains(
            "name: previous-release-performance-${{ github.sha }}-master-table-pass-${{ matrix.qualification_pass }}-attempt-${{ github.run_attempt }}",
            normalized);
        Assert.Equal(
            5,
            System.Text.RegularExpressions.Regex.Matches(
                performanceJob,
                @"\$\{\{ runner\.temp \}\}/cdb-perf/p\$\{\{ matrix\.qualification_pass \}\}/(?:previous-release-performance(?:-preflight)?\.md|baseline-results|candidate-results|logs)").Count);
        Assert.DoesNotContain(
            "cdb-perf/${{ matrix.suite }}",
            performanceJob);
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
    public void ReleaseWorkflow_GatesEveryPublisherOnReusableQualification()
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
            "release_version: ${{ github.ref_name }}",
            normalized);
        Assert.Contains(
            "release_commit: ${{ github.sha }}",
            normalized);
        Assert.DoesNotContain("previous_release_ref:", normalized);
        Assert.Equal(
            5,
            System.Text.RegularExpressions.Regex.Matches(
                normalized,
                @"(?m)^    needs: build-and-test$").Count);
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
