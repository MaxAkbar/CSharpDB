using System.Diagnostics;

namespace CSharpDB.Daemon.Tests;

public sealed class ReleaseStatusScriptTests
{
    private const string CandidateCommit =
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    [Fact]
    public async Task Verifier_AcceptsLatestCanonicalStatusForExactCommit()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            string ghLog = Path.Combine(temporaryRoot, "gh.log");
            string fakeGhRoot = CreateFakeGitHubCli(temporaryRoot);

            ProcessResult result = await RunVerifierAsync(
                fakeGhRoot,
                ghLog,
                scenario: "success",
                releaseVersion: "v4.5.0-rc.1+build.1");

            Assert.True(result.ExitCode == 0, result.CombinedOutput);
            AssertDiagnosticContains(
                $"Verified canonical durable-v3 status for exact commit {CandidateCommit}",
                result.CombinedOutput);
            Assert.Equal(
                [
                    "api|repos/example/csharpdb/commits/" +
                    $"{CandidateCommit}/statuses?per_page=100",
                ],
                File.ReadAllLines(ghLog));
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task Verifier_AcceptsPinnedOneTimeCarryForwardWithoutRelabelingV3()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            string ghLog = Path.Combine(temporaryRoot, "gh.log");
            string fakeGhRoot = CreateFakeGitHubCli(temporaryRoot);

            ProcessResult result = await RunVerifierAsync(
                fakeGhRoot,
                ghLog,
                scenario: "carry-forward",
                releaseVersion: "v4.4.0");

            Assert.True(result.ExitCode == 0, result.CombinedOutput);
            AssertDiagnosticContains(
                "Verified the explicit one-time durable-v2 carry-forward for release " +
                $"4.4.0 at exact commit {CandidateCommit}",
                result.CombinedOutput);
            AssertDiagnosticContains(
                "durable-v3 failure 51664261883 remains preserved",
                result.CombinedOutput);
            Assert.Equal(
                [
                    "api|repos/example/csharpdb/commits/" +
                    $"{CandidateCommit}/statuses?per_page=100",
                    "api|repos/example/csharpdb/commits/" +
                    "61e4d025087f4fae7208381288fba6115f0d1e30/statuses?per_page=100",
                    "api|repos/example/csharpdb/commits/" +
                    "ee1ea0e996fc22e093e950ec32e14543cd5caeca/statuses?per_page=100",
                ],
                File.ReadAllLines(ghLog));
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task Verifier_RejectsCarryForwardForAnyOtherRelease()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            string ghLog = Path.Combine(temporaryRoot, "gh.log");
            string fakeGhRoot = CreateFakeGitHubCli(temporaryRoot);

            ProcessResult result = await RunVerifierAsync(
                fakeGhRoot,
                ghLog,
                scenario: "carry-forward",
                releaseVersion: "4.4.1");

            Assert.NotEqual(0, result.ExitCode);
            AssertDiagnosticContains(
                $"Commit {CandidateCommit} has no csharpdb/local-durable-performance status",
                result.CombinedOutput);
            Assert.Single(File.ReadAllLines(ghLog));
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Theory]
    [InlineData(
        "carry-forward-source-mismatch",
        "durable-v2 source evidence no longer matches the exact approved GitHub status record")]
    [InlineData(
        "carry-forward-wrong-tree",
        "Approved product source tree is bee4859c14381fc2dbe209e2e0c84909dc98adc9")]
    [InlineData(
        "carry-forward-stale",
        "status at or after the approved carry-forward requires fresh release qualification")]
    public async Task Verifier_RejectsTamperedOrStaleCarryForward(
        string scenario,
        string expectedDiagnostic)
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            string ghLog = Path.Combine(temporaryRoot, "gh.log");
            string fakeGhRoot = CreateFakeGitHubCli(temporaryRoot);

            ProcessResult result = await RunVerifierAsync(
                fakeGhRoot,
                ghLog,
                scenario,
                releaseVersion: "4.4.0");

            Assert.NotEqual(0, result.ExitCode);
            AssertDiagnosticContains(expectedDiagnostic, result.CombinedOutput);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Theory]
    [InlineData(
        "missing",
        "has no csharpdb/local-durable-performance status")]
    [InlineData(
        "pending",
        "is 'pending', not 'success'")]
    [InlineData(
        "failure",
        "is 'failure', not 'success'")]
    [InlineData(
        "wrong-creator",
        "was created by 'UnexpectedAttestor', not expected creator 'MaxAkbar'")]
    [InlineData(
        "malformed",
        "does not contain a canonical durable-v3 attestation")]
    [InlineData(
        "legacy-v2",
        "does not contain a canonical durable-v3 attestation")]
    [InlineData(
        "lowercase-design",
        "does not contain a canonical durable-v3 attestation")]
    public async Task Verifier_RejectsMissingOrInvalidLatestExactCommitStatus(
        string scenario,
        string expectedDiagnostic)
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            string ghLog = Path.Combine(temporaryRoot, "gh.log");
            string fakeGhRoot = CreateFakeGitHubCli(temporaryRoot);

            ProcessResult result = await RunVerifierAsync(
                fakeGhRoot,
                ghLog,
                scenario);

            Assert.NotEqual(0, result.ExitCode);
            AssertDiagnosticContains(expectedDiagnostic, result.CombinedOutput);

            string[] calls = File.ReadAllLines(ghLog);
            Assert.Single(calls);
            Assert.Equal(
                "api|repos/example/csharpdb/commits/" +
                $"{CandidateCommit}/statuses?per_page=100",
                calls[0]);
            Assert.DoesNotContain(
                calls,
                call => call.Contains("parents", StringComparison.OrdinalIgnoreCase));
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task Verifier_FailsClosedWhenGitHubApiFails()
    {
        string temporaryRoot = CreateTemporaryRoot();
        try
        {
            string ghLog = Path.Combine(temporaryRoot, "gh.log");
            string fakeGhRoot = CreateFakeGitHubCli(temporaryRoot);

            ProcessResult result = await RunVerifierAsync(
                fakeGhRoot,
                ghLog,
                scenario: "api-failure");

            Assert.NotEqual(0, result.ExitCode);
            AssertDiagnosticContains(
                $"Could not read GitHub statuses for commit {CandidateCommit}",
                result.CombinedOutput);
            AssertDiagnosticContains(
                "Simulated GitHub status API failure",
                result.CombinedOutput);
        }
        finally
        {
            DeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public void PublishReleaseTag_RequiresExactCleanMainStatusBeforeExactTagPush()
    {
        string repoRoot = FindRepoRoot();
        string publisher = File.ReadAllText(Path.Combine(
            repoRoot,
            "scripts",
            "Publish-ReleaseTag.ps1"));
        string verifier = File.ReadAllText(Path.Combine(
            repoRoot,
            "scripts",
            "Test-LocalDurableStatus.ps1"));
        string carryForwardPublisher = File.ReadAllText(Path.Combine(
            repoRoot,
            "scripts",
            "Publish-DurableCarryForwardStatus.ps1"));

        Assert.Contains(
            "status', '--porcelain=v1', '--untracked-files=all",
            publisher);
        Assert.Contains("branch', '--show-current", publisher);
        Assert.Contains("currentBranch -cne 'main'", publisher);
        Assert.Contains("refs/heads/main:refs/remotes/origin/main", publisher);
        Assert.Contains("origin/main^{commit}", publisher);
        Assert.Contains("local main to equal origin/main exactly", publisher);
        Assert.Contains("Test-LocalDurableStatus.ps1", publisher);
        Assert.Contains("-Commit $headCommit", publisher);
        Assert.Contains("exact commit $headCommit", publisher);
        Assert.Contains("Test-LocalDurablePerformance.ps1", publisher);
        Assert.Contains("-CandidateRef $headCommit", publisher);
        Assert.Contains("refs/tags/$releaseTag`:refs/tags/$releaseTag", publisher);
        Assert.DoesNotContain("--force", publisher, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("parents", verifier, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("exit 0", verifier, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(
            "^policy=durable-v3; baseline=[0-9a-f]{40}; design=[0-9A-F]{8}; " +
            "reports=[0-9A-F]{8}/[0-9A-F]{8}$",
            verifier);
        Assert.Contains(
            "csharpdb/local-durable-performance-carry-forward-v4.4.0",
            verifier);
        Assert.Contains(
            "61e4d025087f4fae7208381288fba6115f0d1e30",
            verifier);
        Assert.Contains("51598901859", verifier);
        Assert.Contains(
            "policy=durable-v2; baseline=7880dad112f3fdf011c134db2f8a08ec646ee326;",
            verifier);
        Assert.Contains("ee1ea0e996fc22e093e950ec32e14543cd5caeca", verifier);
        Assert.Contains("51664261883", verifier);
        Assert.Contains("local durable qualification failed", verifier);
        Assert.Contains(
            "src/CSharpDB.Migration/MigrationRejectArtifactPublication.cs",
            verifier);
        Assert.Contains("'merge-base', '--is-ancestor'", verifier);
        Assert.Contains("'--name-status'", verifier);
        Assert.Contains("'--no-renames'", verifier);
        Assert.Contains("8e43642cfcd3e523046302b99253673ceb5a33ce", verifier);
        Assert.Contains("bee4859c14381fc2dbe209e2e0c84909dc98adc9", verifier);

        Assert.Contains("Authenticated GitHub user", carryForwardPublisher);
        Assert.Contains("statuses/$headCommit", carryForwardPublisher);
        Assert.Contains("Invoke-StatusVerifier -EligibilityOnly", carryForwardPublisher);
        Assert.Contains("Publish-DurableCarryForwardStatus.ps1", publisher);
        Assert.Contains("-ApproveDurableV2CarryForward", publisher);
        Assert.Contains("mutually exclusive", publisher);
        Assert.Contains("-ReleaseVersion $releaseVersion", publisher);
        Assert.DoesNotContain("refs/tags/", carryForwardPublisher, StringComparison.Ordinal);

        int sourceEvidence = carryForwardPublisher.IndexOf(
            "Invoke-StatusVerifier -EligibilityOnly",
            StringComparison.Ordinal);
        int publishStatus = carryForwardPublisher.IndexOf(
            "--method POST",
            StringComparison.Ordinal);
        Assert.True(sourceEvidence >= 0);
        Assert.True(
            publishStatus > sourceEvidence,
            "The source evidence must be verified before a carry-forward status is published.");

        int initialVerifier = publisher.IndexOf(
            "Invoke-StatusVerifier",
            publisher.IndexOf("$hasReusableStatus", StringComparison.Ordinal),
            StringComparison.Ordinal);
        int localTagMutation = publisher.IndexOf(
            "'tag', $releaseTag, $headCommit",
            StringComparison.Ordinal);
        int exactTagPush = publisher.IndexOf(
            "refs/tags/$releaseTag`:refs/tags/$releaseTag",
            localTagMutation,
            StringComparison.Ordinal);

        Assert.True(initialVerifier >= 0, "The exact status must be verified.");
        Assert.True(
            localTagMutation > initialVerifier,
            "The exact status must be verified before a local tag is created.");
        Assert.True(
            exactTagPush > localTagMutation,
            "Only the exact validated tag may be pushed after local validation.");
    }

    [Fact]
    public void LocalDurableWrapper_RejectsInstallerContaminationAndStopsRemainingPasses()
    {
        string script = File.ReadAllText(Path.Combine(
            FindRepoRoot(),
            "tests",
            "CSharpDB.Benchmarks",
            "scripts",
            "Test-LocalDurablePerformance.ps1"));

        Assert.Contains("Component Based Servicing", script);
        Assert.Contains("WindowsUpdate\\Auto Update\\RebootRequired", script);
        Assert.Contains("PendingFileRenameOperations", script);
        Assert.Contains("Get-PendingFileRenameOperationsSnapshot", script);
        Assert.Contains("Get-PendingFileRenamePolicyReasons", script);
        Assert.Contains("Get-PendingFileRenameChangeReasons", script);
        Assert.DoesNotContain("Get-Process -Name msiexec", script);
        Assert.Contains("ProviderName = 'MsiInstaller'", script);
        Assert.Contains("Id = @(1040, 1042)", script);
        Assert.Contains("Get-ActiveInstallerTransactionReasons", script);
        Assert.Contains("Get-ApplicationEventLogAnchor", script);
        Assert.Contains("Get-ApplicationEventXmlFingerprint", script);
        Assert.Contains("$Event.ToXml()", script);
        Assert.Contains("-ListLog 'Application'", script);
        Assert.Contains("IsEnabled", script);
        Assert.Contains("IsLogFull", script);
        Assert.Contains("before reading Windows Installer events", script);
        Assert.Contains("after reading Windows Installer events", script);
        Assert.Contains("record ID reused", script);
        Assert.Contains("Get-PassMeasurementStartUtc", script);
        Assert.Contains("-NotBeforeUtc $installerQuietCutoffUtc", script);
        Assert.Contains("-Stage 'preflight'", script);
        Assert.Contains("-Stage \"the start of pass $qualificationPass\"", script);
        Assert.Contains("Get-LocalEnvironmentIssues", script);
        Assert.Contains("environment contamination", script);
        Assert.Contains("remaining passes will not run", script);

        int installerActivityFunction = script.IndexOf(
            "function Get-InstallerActivityReasons",
            StringComparison.Ordinal);
        int anchorCheckBeforeRead = script.IndexOf(
            "-Stage 'before reading Windows Installer events'",
            installerActivityFunction,
            StringComparison.Ordinal);
        int installerEventRead = script.IndexOf(
            "$events = @(Get-MsiInstallerTransactionEvents)",
            anchorCheckBeforeRead,
            StringComparison.Ordinal);
        int anchorCheckAfterRead = script.IndexOf(
            "-Stage 'after reading Windows Installer events'",
            installerEventRead,
            StringComparison.Ordinal);
        int installerEventFilter = script.IndexOf(
            "$newEvents = @(",
            anchorCheckAfterRead,
            StringComparison.Ordinal);

        Assert.True(installerActivityFunction >= 0);
        Assert.True(anchorCheckBeforeRead > installerActivityFunction);
        Assert.True(installerEventRead > anchorCheckBeforeRead);
        Assert.True(anchorCheckAfterRead > installerEventRead);
        Assert.True(
            installerEventFilter > anchorCheckAfterRead,
            "The Application-log anchor must be revalidated before installer IDs are filtered.");

        int loop = script.IndexOf(
            "foreach ($qualificationPass in 1, 2)",
            StringComparison.Ordinal);
        int passStartGuard = script.IndexOf(
            "-Stage \"the start of pass $qualificationPass\"",
            loop,
            StringComparison.Ordinal);
        int passStartAnchorGuard = script.IndexOf(
            "-ApplicationEventLogAnchor $applicationEventLogAnchor",
            passStartGuard,
            StringComparison.Ordinal);
        int comparison = script.IndexOf("& $comparisonScript @parameters", loop, StringComparison.Ordinal);
        int installerAudit = script.IndexOf(
            "Get-LocalEnvironmentIssues",
            comparison,
            StringComparison.Ordinal);
        int stopRemainingPasses = script.IndexOf("break", installerAudit, StringComparison.Ordinal);

        Assert.True(loop >= 0);
        Assert.True(passStartGuard > loop && passStartGuard < comparison);
        Assert.True(passStartAnchorGuard > passStartGuard && passStartAnchorGuard < comparison);
        Assert.True(installerAudit > comparison);
        Assert.True(
            stopRemainingPasses > installerAudit,
            "Installer contamination after a pass must stop the second pass.");
    }

    private static Task<ProcessResult> RunVerifierAsync(
        string fakeGhRoot,
        string ghLog,
        string scenario,
        string? releaseVersion = null)
    {
        Dictionary<string, string> environment = new()
        {
            ["FAKE_GH_LOG"] = ghLog,
            ["FAKE_GH_SCENARIO"] = scenario,
            ["PATH"] = fakeGhRoot + Path.PathSeparator +
                (Environment.GetEnvironmentVariable("PATH") ?? string.Empty),
        };
        List<string> arguments =
        [
            "-NoLogo",
            "-NoProfile",
            "-File",
            Path.Combine(FindRepoRoot(), "scripts", "Test-LocalDurableStatus.ps1"),
            "-Commit",
            CandidateCommit,
            "-GitHubRepository",
            "example/csharpdb",
            "-ExpectedCreator",
            "MaxAkbar",
        ];
        if (releaseVersion is not null)
        {
            arguments.Add("-ReleaseVersion");
            arguments.Add(releaseVersion);
        }
        return RunProcessAsync("pwsh", environment, [.. arguments]);
    }

    private static string CreateFakeGitHubCli(string temporaryRoot)
    {
        string toolRoot = Directory.CreateDirectory(
            Path.Combine(temporaryRoot, "fake-gh")).FullName;
        File.WriteAllText(
            Path.Combine(toolRoot, "fake-gh.ps1"),
            """
            param(
                [Parameter(ValueFromRemainingArguments = $true)]
                [string[]] $CliArguments)

            Add-Content -LiteralPath $env:FAKE_GH_LOG -Value ($CliArguments -join '|')
            if ($CliArguments.Count -lt 2 -or $CliArguments[0] -cne 'api') {
                Write-Error "Unexpected fake gh invocation: $($CliArguments -join ' ')"
                exit 1
            }
            if ($env:FAKE_GH_SCENARIO -ceq 'api-failure') {
                Write-Error 'Simulated GitHub status API failure.'
                exit 1
            }

            $context = 'csharpdb/local-durable-performance'
            $carryForwardContext =
                'csharpdb/local-durable-performance-carry-forward-v4.4.0'
            $canonical =
                'policy=durable-v3; baseline=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb; ' +
                'design=89ABCDEF; ' +
                'reports=1234ABCD/5678EFAB'
            function New-FakeStatus {
                param(
                    [long] $Id,
                    [string] $CreatedAt,
                    [string] $State,
                    [string] $Context = $context,
                    [string] $Creator = 'maxakbar',
                    [long] $CreatorId = 13856299,
                    [string] $Description = $canonical)

                [pscustomobject]@{
                    id = $Id
                    created_at = $CreatedAt
                    updated_at = $CreatedAt
                    state = $State
                    context = $Context
                    creator = [pscustomobject]@{
                        login = $Creator
                        id = $CreatorId
                    }
                    description = $Description
                }
            }

            $apiPath = $CliArguments[1]
            if ($apiPath -match '61e4d025087f4fae7208381288fba6115f0d1e30') {
                $sourceId = if ($env:FAKE_GH_SCENARIO -ceq 'carry-forward-source-mismatch') {
                    51598901860
                }
                else {
                    51598901859
                }
                $statuses = @(
                    (New-FakeStatus `
                        -Id $sourceId `
                        -CreatedAt '2026-08-04T07:38:51Z' `
                        -State success `
                        -Description (
                            'policy=durable-v2; ' +
                            'baseline=7880dad112f3fdf011c134db2f8a08ec646ee326; ' +
                            'reports=BFF306E7/B9C20AD6'))
                )
                ConvertTo-Json -InputObject @($statuses) -Depth 10 -Compress
                exit 0
            }
            if ($apiPath -match 'ee1ea0e996fc22e093e950ec32e14543cd5caeca') {
                $statuses = @(
                    (New-FakeStatus `
                        -Id 51664261883 `
                        -CreatedAt '2026-08-05T02:52:58Z' `
                        -State failure `
                        -Description (
                            'policy=durable-v3; design=6B500421; ' +
                            'local durable qualification failed'))
                )
                ConvertTo-Json -InputObject @($statuses) -Depth 10 -Compress
                exit 0
            }

            $olderSuccess = New-FakeStatus `
                -Id 100 `
                -CreatedAt '2026-08-01T00:00:00Z' `
                -State success
            $statuses = switch ($env:FAKE_GH_SCENARIO) {
                'success' {
                    @(
                        (New-FakeStatus `
                            -Id 500 `
                            -CreatedAt '2026-08-03T00:00:00Z' `
                            -State failure `
                            -Context 'unrelated/check'),
                        $olderSuccess,
                        (New-FakeStatus `
                            -Id 50 `
                            -CreatedAt '2026-07-01T00:00:00Z' `
                            -State failure)
                    )
                }
                'missing' {
                    @(
                        (New-FakeStatus `
                            -Id 500 `
                            -CreatedAt '2026-08-03T00:00:00Z' `
                            -State success `
                            -Context 'unrelated/check')
                    )
                }
                'pending' {
                    @(
                        $olderSuccess,
                        (New-FakeStatus `
                            -Id 200 `
                            -CreatedAt '2026-08-02T00:00:00Z' `
                            -State pending)
                    )
                }
                'failure' {
                    @(
                        $olderSuccess,
                        (New-FakeStatus `
                            -Id 200 `
                            -CreatedAt '2026-08-02T00:00:00Z' `
                            -State failure)
                    )
                }
                'wrong-creator' {
                    @(
                        (New-FakeStatus `
                            -Id 200 `
                            -CreatedAt '2026-08-02T00:00:00Z' `
                            -State success `
                            -Creator 'UnexpectedAttestor')
                    )
                }
                'malformed' {
                    @(
                        (New-FakeStatus `
                            -Id 200 `
                            -CreatedAt '2026-08-02T00:00:00Z' `
                            -State success `
                            -Description 'policy=durable-v3; baseline=not-a-sha; design=bad; reports=bad')
                    )
                }
                'legacy-v2' {
                    @(
                        $olderSuccess,
                        (New-FakeStatus `
                            -Id 200 `
                            -CreatedAt '2026-08-02T00:00:00Z' `
                            -State success `
                            -Description (
                                'policy=durable-v2; ' +
                                'baseline=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb; ' +
                                'reports=1234ABCD/5678EFAB'))
                    )
                }
                'lowercase-design' {
                    @(
                        (New-FakeStatus `
                            -Id 200 `
                            -CreatedAt '2026-08-02T00:00:00Z' `
                            -State success `
                            -Description (
                                'policy=durable-v3; ' +
                                'baseline=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb; ' +
                                'design=89abcdef; ' +
                                'reports=1234ABCD/5678EFAB'))
                    )
                }
                { $_ -in @(
                    'carry-forward',
                    'carry-forward-source-mismatch',
                    'carry-forward-wrong-tree') } {
                    @(
                        (New-FakeStatus `
                            -Id 51665000000 `
                            -CreatedAt '2026-08-05T03:00:00Z' `
                            -State success `
                            -Context $carryForwardContext `
                            -Description (
                                'policy=durable-v2-carry-forward-v4.4.0; source=61e4d025; ' +
                                'success=51598901859; failed-v3=51664261883; ' +
                                'tree=bee4859c'))
                    )
                }
                'carry-forward-stale' {
                    @(
                        (New-FakeStatus `
                            -Id 51665000000 `
                            -CreatedAt '2026-08-05T03:00:00Z' `
                            -State success `
                            -Context $carryForwardContext `
                            -Description (
                                'policy=durable-v2-carry-forward-v4.4.0; source=61e4d025; ' +
                                'success=51598901859; failed-v3=51664261883; ' +
                                'tree=bee4859c')),
                        (New-FakeStatus `
                            -Id 51666000000 `
                            -CreatedAt '2026-08-05T04:00:00Z' `
                            -State failure `
                            -Description (
                                'policy=durable-v3; design=12345678; ' +
                                'local durable qualification failed'))
                    )
                }
                default {
                    Write-Error "Unknown fake gh scenario: $env:FAKE_GH_SCENARIO"
                    exit 1
                }
            }

            ConvertTo-Json -InputObject @($statuses) -Depth 10 -Compress
            """);
        if (OperatingSystem.IsWindows())
        {
            File.WriteAllText(
                Path.Combine(toolRoot, "gh.cmd"),
                """
                @echo off
                pwsh -NoLogo -NoProfile -File "%~dp0fake-gh.ps1" %*
                exit /b %ERRORLEVEL%
                """);
            File.WriteAllText(
                Path.Combine(toolRoot, "git.cmd"),
                """
                @echo off
                if /I "%~3"=="merge-base" exit /b 0
                if /I "%~3"=="diff" (
                    echo M	src/CSharpDB.Migration/MigrationRejectArtifactPublication.cs
                    exit /b 0
                )
                if /I "%~3"=="rev-parse" (
                    if /I "%~5"=="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:src/CSharpDB.Migration/MigrationRejectArtifactPublication.cs" (
                        echo 8e43642cfcd3e523046302b99253673ceb5a33ce
                        exit /b 0
                    )
                    if /I "%~5"=="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:src" (
                        if /I "%FAKE_GH_SCENARIO%"=="carry-forward-wrong-tree" (
                            echo 1111111111111111111111111111111111111111
                        ) else (
                            echo bee4859c14381fc2dbe209e2e0c84909dc98adc9
                        )
                        exit /b 0
                    )
                )
                echo Unexpected fake git invocation: %* 1>&2
                exit /b 1
                """);
        }
        else
        {
            string launcher = Path.Combine(toolRoot, "gh");
            File.WriteAllText(
                launcher,
                """
                #!/usr/bin/env sh
                script_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
                exec pwsh -NoLogo -NoProfile -File "${script_dir}/fake-gh.ps1" "$@"
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
            string gitLauncher = Path.Combine(toolRoot, "git");
            File.WriteAllText(
                gitLauncher,
                """
                #!/usr/bin/env sh
                if [ "$#" -lt 3 ] || [ "$1" != "-C" ]; then
                  printf '%s\n' "Unexpected fake git invocation: $*" >&2
                  exit 1
                fi
                command_name="$3"
                case "$command_name" in
                  merge-base)
                    exit 0
                    ;;
                  diff)
                    printf 'M\tsrc/CSharpDB.Migration/MigrationRejectArtifactPublication.cs\n'
                    exit 0
                    ;;
                  rev-parse)
                    object_name=''
                    for argument in "$@"; do object_name="$argument"; done
                    case "$object_name" in
                      aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:src/CSharpDB.Migration/MigrationRejectArtifactPublication.cs)
                        printf '%s\n' '8e43642cfcd3e523046302b99253673ceb5a33ce'
                        exit 0
                        ;;
                      aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:src)
                        if [ "$FAKE_GH_SCENARIO" = 'carry-forward-wrong-tree' ]; then
                          printf '%s\n' '1111111111111111111111111111111111111111'
                        else
                          printf '%s\n' 'bee4859c14381fc2dbe209e2e0c84909dc98adc9'
                        fi
                        exit 0
                        ;;
                    esac
                    ;;
                esac
                printf '%s\n' "Unexpected fake git invocation: $*" >&2
                exit 1
                """);
            File.SetUnixFileMode(
                gitLauncher,
                UnixFileMode.UserRead |
                UnixFileMode.UserWrite |
                UnixFileMode.UserExecute |
                UnixFileMode.GroupRead |
                UnixFileMode.GroupExecute |
                UnixFileMode.OtherRead |
                UnixFileMode.OtherExecute);
        }

        return toolRoot;
    }

    private static async Task<ProcessResult> RunProcessAsync(
        string fileName,
        IReadOnlyDictionary<string, string> environment,
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
        foreach ((string name, string value) in environment)
            startInfo.Environment[name] = value;

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

        return new ProcessResult(
            process.ExitCode,
            await standardOutput,
            await standardError);
    }

    private static string CreateTemporaryRoot()
    {
        string path = Path.Combine(
            Path.GetTempPath(),
            "csharpdb-release-status-tests",
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
        string withoutPowerShellGutters = System.Text.RegularExpressions.Regex.Replace(
            withoutAnsi,
            "^[\\t ]+\\|[\\t ]?",
            string.Empty,
            System.Text.RegularExpressions.RegexOptions.Multiline);
        return System.Text.RegularExpressions.Regex.Replace(
            withoutPowerShellGutters,
            "\\s+",
            " ").Trim();
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

    private sealed record ProcessResult(
        int ExitCode,
        string StandardOutput,
        string StandardError)
    {
        public string CombinedOutput =>
            StandardOutput + Environment.NewLine + StandardError;
    }
}
