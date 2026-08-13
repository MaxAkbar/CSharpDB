using System.Diagnostics;

namespace CSharpDB.Daemon.Tests;

public sealed class ReleaseStatusScriptTests
{
    private const string CandidateCommit =
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    private static readonly TimeSpan PublisherProcessTimeout = TimeSpan.FromMinutes(3);

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
    public void PublishReleaseTag_QualifiesThenTagsAndRunsResumablePublication()
    {
        string repoRoot = FindRepoRoot();
        string publisher = File.ReadAllText(Path.Combine(
            repoRoot,
            "scripts",
            "Publish-ReleaseTag.ps1"));
        Assert.Contains(
            "status', '--porcelain=v1', '--untracked-files=all",
            publisher);
        Assert.Contains("branch', '--show-current", publisher);
        Assert.Contains("currentBranch -cne 'main'", publisher);
        Assert.Contains("refs/heads/main:refs/remotes/origin/main", publisher);
        Assert.Contains("origin/main^{commit}", publisher);
        Assert.Contains("local main to equal origin/main exactly", publisher);
        Assert.Contains("$versionNodes[0].InnerText.Trim() -cne $releaseVersion", publisher);
        Assert.Contains("(Get-ExactCurrentMainCommit) -cne $headCommit", publisher);
        Assert.Contains("$qualificationWorkflow = 'release.yml'", publisher);
        Assert.Contains("$publicationWorkflow = 'publish-release.yml'", publisher);
        Assert.Contains("-PreflightOnly $true", publisher);
        Assert.Contains("-PreflightOnly $false", publisher);
        Assert.Contains("'tag', $releaseTag, $headCommit", publisher);
        Assert.Contains("refs/tags/$releaseTag`:refs/tags/$releaseTag", publisher);
        Assert.Contains("& gh run watch", publisher);
        Assert.Contains("--exit-status", publisher);
        Assert.Contains("Successful publication did not create a published GitHub Release", publisher);
        Assert.Contains("Every reversible gate runs before tag creation", publisher);
        Assert.DoesNotContain("--force", publisher, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("Test-LocalDurableStatus.ps1", publisher);
        Assert.DoesNotContain("Test-LocalDurablePerformance.ps1", publisher);
        Assert.DoesNotContain("Publish-DurableCarryForwardStatus.ps1", publisher);
        Assert.DoesNotContain("ConfirmDedicatedFixedSsd", publisher);
        Assert.DoesNotContain("ApproveDurableV2CarryForward", publisher);
        Assert.DoesNotContain("csharpdb/local-durable-performance", publisher);

        int exactMainValidation = publisher.IndexOf(
            "$headCommit = Get-ExactCurrentMainCommit",
            StringComparison.Ordinal);
        int versionValidation = publisher.IndexOf(
            "$versionNodes[0].InnerText.Trim() -cne $releaseVersion",
            StringComparison.Ordinal);
        int workflowDispatch = publisher.IndexOf(
            "$qualificationRun = Find-OrDispatchRun",
            StringComparison.Ordinal);
        int qualificationWatch = publisher.IndexOf(
            "Wait-HostedRun -Run $qualificationRun",
            StringComparison.Ordinal);
        int publicationPreflight = publisher.IndexOf(
            "-PreflightOnly $true",
            qualificationWatch,
            StringComparison.Ordinal);
        int localTagCreation = publisher.IndexOf(
            "@('tag', $releaseTag, $headCommit)",
            publicationPreflight,
            StringComparison.Ordinal);
        int publication = publisher.IndexOf(
            "-PreflightOnly $false",
            localTagCreation,
            StringComparison.Ordinal);

        Assert.True(exactMainValidation >= 0, "The exact main commit must be validated.");
        Assert.True(
            versionValidation > exactMainValidation,
            "The package version must be validated after resolving exact main.");
        Assert.True(
            workflowDispatch > versionValidation,
            "The package version must be validated before the hosted release is dispatched.");
        Assert.True(
            qualificationWatch > workflowDispatch,
            "The publisher must wait for hosted qualification after dispatch.");
        Assert.True(
            publicationPreflight > qualificationWatch,
            "Publication credentials must be preflighted after qualification.");
        Assert.True(
            localTagCreation > publicationPreflight,
            "The tag must be created only after qualification and publication preflight.");
        Assert.True(
            publication > localTagCreation,
            "Resumable publication must start only after the exact tag exists.");
    }

    [Fact]
    public async Task Publisher_HappyPath_QualifiesPreflightsTagsAndPublishesInOrder()
    {
        using PublisherFixture fixture = new("happy");

        ProcessResult result = await fixture.RunAsync();

        Assert.True(result.ExitCode == 0, result.CombinedOutput);
        AssertDiagnosticContains($"Published {fixture.ReleaseTag} at exact commit {CandidateCommit}", result.CombinedOutput);
        Assert.True(fixture.RemoteTagExists);
        Assert.True(fixture.ReleaseExists);
        AssertAuditOrder(
            fixture.AuditLines,
            "gh|workflow|run|release.yml|",
            "gh|run|watch|301|",
            "gh|workflow|run|publish-release.yml|*preflight_only=true",
            "gh|run|watch|302|",
            $"git|*|tag|{fixture.ReleaseTag}|{CandidateCommit}",
            $"git|*|push|origin|refs/tags/{fixture.ReleaseTag}:refs/tags/{fixture.ReleaseTag}",
            "gh|workflow|run|publish-release.yml|*preflight_only=false",
            "gh|run|watch|303|");
    }

    [Fact]
    public async Task Publisher_QualificationFailure_DoesNotCreateOrPushTag()
    {
        using PublisherFixture fixture = new("qualification-failure");

        ProcessResult result = await fixture.RunAsync();

        Assert.NotEqual(0, result.ExitCode);
        AssertDiagnosticContains("Release qualification failed", result.CombinedOutput);
        Assert.False(fixture.RemoteTagExists);
        Assert.False(fixture.ReleaseExists);
        Assert.DoesNotContain(fixture.GitLines, IsTagMutation);
        Assert.DoesNotContain(
            fixture.GitHubLines,
            line => line.Contains("preflight_only=", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Publisher_PublicationPreflightFailure_DoesNotCreateOrPushTag()
    {
        using PublisherFixture fixture = new("preflight-failure");

        ProcessResult result = await fixture.RunAsync();

        Assert.NotEqual(0, result.ExitCode);
        AssertDiagnosticContains("Release preflight failed", result.CombinedOutput);
        Assert.False(fixture.RemoteTagExists);
        Assert.False(fixture.ReleaseExists);
        Assert.DoesNotContain(fixture.GitLines, IsTagMutation);
        Assert.Contains(
            fixture.GitHubLines,
            line => line.Contains("preflight_only=true", StringComparison.Ordinal));
        Assert.DoesNotContain(
            fixture.GitHubLines,
            line => line.Contains("preflight_only=false", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Publisher_ReusesSuccessfulExactQualificationWithLiveArtifactAfterInterruption()
    {
        using PublisherFixture fixture = new("reuse-qualification");

        ProcessResult result = await fixture.RunAsync();

        Assert.True(result.ExitCode == 0, result.CombinedOutput);
        Assert.Contains(
            fixture.GitHubLines,
            line => line.Contains("actions/runs/201/artifacts", StringComparison.Ordinal));
        Assert.DoesNotContain(
            fixture.GitHubLines,
            line => line.Contains("workflow|run|release.yml|", StringComparison.Ordinal));
        Assert.Contains(
            fixture.GitHubLines,
            line => line.Contains("qualification_run_id=201", StringComparison.Ordinal));
        Assert.True(fixture.RemoteTagExists);
        Assert.True(fixture.ReleaseExists);
    }

    [Fact]
    public async Task Publisher_ExistingExactTag_ResumesOriginalBindingAfterMainAdvances()
    {
        using PublisherFixture fixture = new(
            "existing-binding",
            exactTagExists: true,
            originMainCommit: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

        ProcessResult result = await fixture.RunAsync();

        Assert.True(result.ExitCode == 0, result.CombinedOutput);
        Assert.DoesNotContain(
            fixture.GitHubLines,
            line => line.Contains("workflow|run|release.yml|", StringComparison.Ordinal));
        Assert.DoesNotContain(
            fixture.GitHubLines,
            line => line.Contains("preflight_only=true", StringComparison.Ordinal));
        Assert.Contains(
            fixture.GitHubLines,
            line => line.Contains("qualification_run_id=201", StringComparison.Ordinal) &&
                line.Contains("preflight_only=false", StringComparison.Ordinal));
        Assert.DoesNotContain(fixture.GitLines, IsTagMutation);
        Assert.DoesNotContain(
            fixture.GitLines,
            line => line.Contains("origin/main", StringComparison.Ordinal));
        Assert.True(fixture.ReleaseExists);
    }

    [Fact]
    public async Task Publisher_ExistingTag_IgnoresFailedPreflightForUnrelatedQualification()
    {
        using PublisherFixture fixture = new("failed-unrelated-preflight", exactTagExists: true);

        ProcessResult result = await fixture.RunAsync();

        Assert.True(result.ExitCode == 0, result.CombinedOutput);
        Assert.Contains(
            fixture.GitHubLines,
            line => line.Contains("qualification_run_id=201", StringComparison.Ordinal) &&
                line.Contains("preflight_only=false", StringComparison.Ordinal));
        Assert.DoesNotContain(
            fixture.GitHubLines,
            line => line.Contains("qualification_run_id=999", StringComparison.Ordinal) &&
                line.Contains("workflow|run|", StringComparison.Ordinal));
        Assert.DoesNotContain(fixture.GitLines, IsTagMutation);
    }

    [Fact]
    public async Task Publisher_ExistingTag_RejectsMultipleSuccessfulPreflightBindings()
    {
        using PublisherFixture fixture = new("multiple-successful-preflights", exactTagExists: true);

        ProcessResult result = await fixture.RunAsync();

        Assert.NotEqual(0, result.ExitCode);
        AssertDiagnosticContains(
            $"Existing tag '{fixture.ReleaseTag}' is not bound to exactly one qualification run",
            result.CombinedOutput);
        Assert.DoesNotContain(
            fixture.GitHubLines,
            line => line.Contains("workflow|run|", StringComparison.Ordinal));
        Assert.DoesNotContain(fixture.GitLines, IsTagMutation);
        Assert.False(fixture.ReleaseExists);
    }

    [Fact]
    public async Task Publisher_FailedPublication_RetriesWithSameBoundQualificationId()
    {
        using PublisherFixture fixture = new("publication-retry");

        ProcessResult first = await fixture.RunAsync();
        int firstGitLineCount = fixture.GitLines.Length;
        int firstGitHubLineCount = fixture.GitHubLines.Length;

        Assert.NotEqual(0, first.ExitCode);
        AssertDiagnosticContains("Release publication failed", first.CombinedOutput);
        Assert.True(fixture.RemoteTagExists);
        Assert.False(fixture.ReleaseExists);

        ProcessResult second = await fixture.RunAsync();

        Assert.True(second.ExitCode == 0, second.CombinedOutput);
        Assert.True(fixture.ReleaseExists);
        string[] secondGitLines = fixture.GitLines[firstGitLineCount..];
        string[] secondGitHubLines = fixture.GitHubLines[firstGitHubLineCount..];
        Assert.DoesNotContain(secondGitLines, IsTagMutation);
        Assert.DoesNotContain(
            secondGitHubLines,
            line => line.Contains("workflow|run|release.yml|", StringComparison.Ordinal));
        Assert.DoesNotContain(
            secondGitHubLines,
            line => line.Contains("preflight_only=true", StringComparison.Ordinal));
        Assert.Contains(
            secondGitHubLines,
            line => line.Contains("qualification_run_id=301", StringComparison.Ordinal) &&
                line.Contains("preflight_only=false", StringComparison.Ordinal));
        Assert.Equal(
            2,
            fixture.GitHubLines.Count(line =>
                line.Contains("qualification_run_id=301", StringComparison.Ordinal) &&
                line.Contains("preflight_only=false", StringComparison.Ordinal)));
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

    private static bool IsTagMutation(string line) =>
        System.Text.RegularExpressions.Regex.IsMatch(
            line,
            "\\|tag\\|v[0-9]",
            System.Text.RegularExpressions.RegexOptions.CultureInvariant) ||
        line.Contains("|push|origin|refs/tags/", StringComparison.Ordinal);

    private static void AssertAuditOrder(
        IReadOnlyList<string> auditLines,
        params string[] orderedPatterns)
    {
        int previousIndex = -1;
        foreach (string pattern in orderedPatterns)
        {
            int foundIndex = -1;
            for (int index = previousIndex + 1; index < auditLines.Count; index++)
            {
                if (MatchesAuditPattern(auditLines[index], pattern))
                {
                    foundIndex = index;
                    break;
                }
            }

            Assert.True(
                foundIndex >= 0,
                $"Could not find audit pattern '{pattern}' after line {previousIndex}." +
                Environment.NewLine + string.Join(Environment.NewLine, auditLines));
            previousIndex = foundIndex;
        }
    }

    private static bool MatchesAuditPattern(string value, string pattern)
    {
        int offset = 0;
        foreach (string part in pattern.Split('*'))
        {
            if (part.Length == 0)
                continue;
            int match = value.IndexOf(part, offset, StringComparison.Ordinal);
            if (match < 0)
                return false;
            offset = match + part.Length;
        }
        return true;
    }

    private sealed class PublisherFixture : IDisposable
    {
        private readonly string temporaryRoot;
        private readonly string toolRoot;
        private readonly string auditLog;
        private readonly string gitLog;
        private readonly string gitHubLog;
        private readonly string eventLog;
        private readonly string localTagMarker;
        private readonly string remoteTagMarker;
        private readonly string releaseMarker;
        private readonly string scenario;
        private readonly string originMainCommit;

        public PublisherFixture(
            string scenario,
            bool exactTagExists = false,
            string? originMainCommit = null)
        {
            this.scenario = scenario;
            this.originMainCommit = originMainCommit ?? CandidateCommit;
            temporaryRoot = CreateTemporaryRoot();
            toolRoot = Directory.CreateDirectory(
                Path.Combine(temporaryRoot, "fake-release-tools")).FullName;
            auditLog = Path.Combine(temporaryRoot, "audit.log");
            gitLog = Path.Combine(temporaryRoot, "git.log");
            gitHubLog = Path.Combine(temporaryRoot, "gh.log");
            eventLog = Path.Combine(temporaryRoot, "events.log");
            localTagMarker = Path.Combine(temporaryRoot, "local-tag.txt");
            remoteTagMarker = Path.Combine(temporaryRoot, "remote-tag.txt");
            releaseMarker = Path.Combine(temporaryRoot, "release.txt");
            File.WriteAllText(auditLog, string.Empty);
            File.WriteAllText(gitLog, string.Empty);
            File.WriteAllText(gitHubLog, string.Empty);
            File.WriteAllText(eventLog, string.Empty);

            string propsPath = Path.Combine(FindRepoRoot(), "src", "Directory.Build.props");
            System.Xml.Linq.XDocument props = System.Xml.Linq.XDocument.Load(propsPath);
            ReleaseVersion = props.Descendants("Version").Single().Value.Trim();
            ReleaseTag = "v" + ReleaseVersion;

            if (exactTagExists)
            {
                File.WriteAllText(localTagMarker, CandidateCommit);
                File.WriteAllText(remoteTagMarker, CandidateCommit);
            }

            CreateFakeReleaseGit();
            CreateFakeReleaseGitHubCli();
        }

        public string ReleaseVersion { get; }

        public string ReleaseTag { get; }

        public bool RemoteTagExists => File.Exists(remoteTagMarker);

        public bool ReleaseExists => File.Exists(releaseMarker);

        public string[] AuditLines => File.ReadAllLines(auditLog);

        public string[] GitLines => File.ReadAllLines(gitLog);

        public string[] GitHubLines => File.ReadAllLines(gitHubLog);

        public Task<ProcessResult> RunAsync()
        {
            Dictionary<string, string> environment = new()
            {
                ["FAKE_RELEASE_AUDIT_LOG"] = auditLog,
                ["FAKE_RELEASE_COMMIT"] = CandidateCommit,
                ["FAKE_RELEASE_EVENT_LOG"] = eventLog,
                ["FAKE_RELEASE_GH_LOG"] = gitHubLog,
                ["FAKE_RELEASE_GIT_LOG"] = gitLog,
                ["FAKE_RELEASE_LOCAL_TAG"] = localTagMarker,
                ["FAKE_RELEASE_ORIGIN_MAIN_COMMIT"] = originMainCommit,
                ["FAKE_RELEASE_RELEASE"] = releaseMarker,
                ["FAKE_RELEASE_REMOTE_TAG"] = remoteTagMarker,
                ["FAKE_RELEASE_SCENARIO"] = scenario,
                ["FAKE_RELEASE_TAG"] = ReleaseTag,
                ["PATH"] = toolRoot + Path.PathSeparator +
                    (Environment.GetEnvironmentVariable("PATH") ?? string.Empty),
            };
            return RunProcessAsync(
                "pwsh",
                environment,
                PublisherProcessTimeout,
                "-NoLogo",
                "-NoProfile",
                "-File",
                Path.Combine(FindRepoRoot(), "scripts", "Publish-ReleaseTag.ps1"),
                "-Version",
                ReleaseVersion);
        }

        public void Dispose() => DeleteTemporaryRoot(temporaryRoot);

        private void CreateFakeReleaseGit()
        {
            File.WriteAllText(
                Path.Combine(toolRoot, "fake-release-git.ps1"),
                """
                param(
                    [Alias('C')]
                    [string] $RepositoryRoot,

                    [Alias('e')]
                    [switch] $ObjectExists,

                    [Parameter(Position = 0, ValueFromRemainingArguments = $true)]
                    [string[]] $RemainingArguments)

                $ErrorActionPreference = 'Stop'
                if ($ObjectExists -and $RemainingArguments.Count -ge 1) {
                    $RemainingArguments = @($RemainingArguments[0], '-e') +
                        @($RemainingArguments[1..($RemainingArguments.Count - 1)])
                }
                $CliArguments = if ([string]::IsNullOrWhiteSpace($RepositoryRoot)) {
                    @($RemainingArguments)
                }
                else {
                    @('-C', $RepositoryRoot) + @($RemainingArguments)
                }
                $raw = $CliArguments -join '|'
                Add-Content -LiteralPath $env:FAKE_RELEASE_GIT_LOG -Value $raw
                Add-Content -LiteralPath $env:FAKE_RELEASE_AUDIT_LOG -Value "git|$raw"

                $effective = @($CliArguments)
                if ($effective.Count -ge 3 -and $effective[0] -ceq '-C') {
                    $effective = @($effective[2..($effective.Count - 1)])
                }
                if ($effective.Count -eq 0) {
                    Write-Error 'Fake git received no command.'
                    exit 1
                }

                $command = $effective[0]
                $commit = $env:FAKE_RELEASE_COMMIT
                $tag = $env:FAKE_RELEASE_TAG
                switch ($command) {
                    'status' { exit 0 }
                    'branch' {
                        if ($effective -contains '--show-current') {
                            'main'
                            exit 0
                        }
                    }
                    'fetch' { exit 0 }
                    'cat-file' {
                        if ($effective.Count -eq 3 -and
                            $effective[1] -ceq '-e' -and
                            $effective[2] -in @("$commit`^{commit}", "$commit{commit}")) {
                            exit 0
                        }
                    }
                    'rev-parse' {
                        $ref = $effective[-1]
                        if ($ref -in @(
                            'HEAD^{commit}',
                            'HEAD{commit}',
                            "$commit`^{commit}",
                            "$commit{commit}")) {
                            $commit
                            exit 0
                        }
                        if ($ref -in @('origin/main^{commit}', 'origin/main{commit}')) {
                            $env:FAKE_RELEASE_ORIGIN_MAIN_COMMIT
                            exit 0
                        }
                        if ($ref -in @(
                            "refs/tags/$tag`^{commit}",
                            "refs/tags/$tag{commit}",
                            "$tag`^{commit}",
                            "$tag{commit}")) {
                            if (Test-Path -LiteralPath $env:FAKE_RELEASE_LOCAL_TAG) {
                                $commit
                                exit 0
                            }
                            exit 1
                        }
                    }
                    'tag' {
                        if ($effective -contains '--list' -or
                            $effective -contains '--points-at') {
                            exit 0
                        }
                        if ($effective.Count -eq 3 -and
                            $effective[1] -ceq $tag -and
                            $effective[2] -ceq $commit) {
                            Set-Content -LiteralPath $env:FAKE_RELEASE_LOCAL_TAG -Value $commit
                            exit 0
                        }
                    }
                    'describe' { exit 1 }
                    'ls-remote' {
                        if (Test-Path -LiteralPath $env:FAKE_RELEASE_REMOTE_TAG) {
                            "$commit`trefs/tags/$tag"
                        }
                        exit 0
                    }
                    'push' {
                        if (-not (Test-Path -LiteralPath $env:FAKE_RELEASE_LOCAL_TAG)) {
                            Write-Error 'Cannot push a tag that does not exist locally.'
                            exit 1
                        }
                        Set-Content -LiteralPath $env:FAKE_RELEASE_REMOTE_TAG -Value $commit
                        exit 0
                    }
                }

                Write-Error "Unexpected fake git invocation: $($CliArguments -join ' ')"
                exit 1
                """);
            CreateToolLauncher(toolRoot, "git", "fake-release-git.ps1");
        }

        private void CreateFakeReleaseGitHubCli()
        {
            File.WriteAllText(
                Path.Combine(toolRoot, "fake-release-gh.ps1"),
                """
                param(
                    [Parameter(ValueFromRemainingArguments = $true)]
                    [string[]] $CliArguments)

                $ErrorActionPreference = 'Stop'
                $raw = $CliArguments -join '|'
                Add-Content -LiteralPath $env:FAKE_RELEASE_GH_LOG -Value $raw
                Add-Content -LiteralPath $env:FAKE_RELEASE_AUDIT_LOG -Value "gh|$raw"

                function New-FakeRecord {
                    param(
                        [long] $Id,
                        [string] $Workflow,
                        [string] $Kind,
                        [long] $QualificationId,
                        [string] $Conclusion,
                        [bool] $Artifact = $false)

                    [pscustomobject]@{
                        Id = $Id
                        Workflow = $Workflow
                        Kind = $Kind
                        QualificationId = $QualificationId
                        Conclusion = $Conclusion
                        Artifact = $Artifact
                    }
                }

                function Get-SeedRecords {
                    switch ($env:FAKE_RELEASE_SCENARIO) {
                        'reuse-qualification' {
                            New-FakeRecord 201 'release.yml' 'qualification' 0 'success' $true
                        }
                        'existing-binding' {
                            New-FakeRecord 201 'release.yml' 'qualification' 0 'success' $true
                            New-FakeRecord 202 'publish-release.yml' 'preflight' 201 'success'
                        }
                        'failed-unrelated-preflight' {
                            New-FakeRecord 201 'release.yml' 'qualification' 0 'success' $true
                            New-FakeRecord 202 'publish-release.yml' 'preflight' 999 'failure'
                            New-FakeRecord 203 'publish-release.yml' 'preflight' 201 'success'
                        }
                        'multiple-successful-preflights' {
                            New-FakeRecord 201 'release.yml' 'qualification' 0 'success' $true
                            New-FakeRecord 202 'release.yml' 'qualification' 0 'success' $true
                            New-FakeRecord 203 'publish-release.yml' 'preflight' 201 'success'
                            New-FakeRecord 204 'publish-release.yml' 'preflight' 202 'success'
                        }
                    }
                }

                function Get-EventRecords {
                    foreach ($line in @(Get-Content -LiteralPath $env:FAKE_RELEASE_EVENT_LOG)) {
                        if ([string]::IsNullOrWhiteSpace($line)) { continue }
                        $fields = $line -split '\|', 6
                        New-FakeRecord `
                            ([long] $fields[0]) `
                            $fields[1] `
                            $fields[2] `
                            ([long] $fields[3]) `
                            $fields[4] `
                            ([bool]::Parse($fields[5]))
                    }
                }

                function Get-AllRecords {
                    $records = [Collections.Generic.List[object]]::new()
                    foreach ($record in @(Get-SeedRecords)) { $records.Add($record) | Out-Null }
                    foreach ($record in @(Get-EventRecords)) { $records.Add($record) | Out-Null }
                    return @($records)
                }

                function ConvertTo-FakeRun {
                    param([Parameter(Mandatory)] $Record)

                    $title = switch ($Record.Kind) {
                        'qualification' {
                            "Qualify release $($env:FAKE_RELEASE_TAG) at $($env:FAKE_RELEASE_COMMIT)"
                        }
                        'preflight' {
                            "Publish $($env:FAKE_RELEASE_TAG) from qualification $($Record.QualificationId) (preflight)"
                        }
                        'publication' {
                            "Publish $($env:FAKE_RELEASE_TAG) from qualification $($Record.QualificationId) (release)"
                        }
                    }
                    [pscustomobject]@{
                        databaseId = $Record.Id
                        displayTitle = $title
                        event = 'workflow_dispatch'
                        headSha = $env:FAKE_RELEASE_COMMIT
                        status = 'completed'
                        conclusion = $Record.Conclusion
                        url = "https://example.invalid/actions/runs/$($Record.Id)"
                    }
                }

                function Get-ArgumentValue {
                    param([string] $Name)
                    $index = [Array]::IndexOf($CliArguments, $Name)
                    if ($index -lt 0 -or $index + 1 -ge $CliArguments.Count) { return $null }
                    return $CliArguments[$index + 1]
                }

                if ($CliArguments.Count -ge 2 -and
                    $CliArguments[0] -ceq 'auth' -and
                    $CliArguments[1] -ceq 'status') {
                    exit 0
                }

                if ($CliArguments.Count -ge 2 -and
                    $CliArguments[0] -ceq 'repo' -and
                    $CliArguments[1] -ceq 'view') {
                    'example/csharpdb'
                    exit 0
                }

                if ($CliArguments.Count -ge 2 -and
                    $CliArguments[0] -ceq 'run' -and
                    $CliArguments[1] -ceq 'list') {
                    $workflow = Get-ArgumentValue '--workflow'
                    $runs = @(
                        Get-AllRecords |
                            Where-Object { $_.Workflow -ceq $workflow } |
                            ForEach-Object { ConvertTo-FakeRun $_ }
                    )
                    ConvertTo-Json -InputObject @($runs) -Depth 10 -Compress
                    exit 0
                }

                if ($CliArguments.Count -ge 3 -and
                    $CliArguments[0] -ceq 'workflow' -and
                    $CliArguments[1] -ceq 'run') {
                    $workflow = $CliArguments[2]
                    $fields = @{}
                    for ($index = 3; $index -lt $CliArguments.Count - 1; $index++) {
                        if ($CliArguments[$index] -cne '--raw-field') { continue }
                        $pair = $CliArguments[$index + 1] -split '=', 2
                        $fields[$pair[0]] = $pair[1]
                        $index++
                    }

                    if ($workflow -ceq 'release.yml') {
                        $kind = 'qualification'
                        $qualificationId = 0
                    }
                    elseif ($workflow -ceq 'publish-release.yml' -and
                        $fields.preflight_only -ceq 'true') {
                        $kind = 'preflight'
                        $qualificationId = [long] $fields.qualification_run_id
                    }
                    elseif ($workflow -ceq 'publish-release.yml' -and
                        $fields.preflight_only -ceq 'false') {
                        $kind = 'publication'
                        $qualificationId = [long] $fields.qualification_run_id
                    }
                    else {
                        Write-Error "Unexpected fake workflow dispatch: $raw"
                        exit 1
                    }

                    $eventRecords = @(Get-EventRecords)
                    $id = 301 + $eventRecords.Count
                    $conclusion = 'success'
                    if ($env:FAKE_RELEASE_SCENARIO -ceq 'qualification-failure' -and
                        $kind -ceq 'qualification') {
                        $conclusion = 'failure'
                    }
                    elseif ($env:FAKE_RELEASE_SCENARIO -ceq 'preflight-failure' -and
                        $kind -ceq 'preflight') {
                        $conclusion = 'failure'
                    }
                    elseif ($env:FAKE_RELEASE_SCENARIO -ceq 'publication-retry' -and
                        $kind -ceq 'publication' -and
                        @($eventRecords | Where-Object Kind -ceq 'publication').Count -eq 0) {
                        $conclusion = 'failure'
                    }
                    $artifact = $kind -ceq 'qualification' -and $conclusion -ceq 'success'
                    Add-Content `
                        -LiteralPath $env:FAKE_RELEASE_EVENT_LOG `
                        -Value "$id|$workflow|$kind|$qualificationId|$conclusion|$artifact"
                    exit 0
                }

                if ($CliArguments.Count -ge 3 -and
                    $CliArguments[0] -ceq 'run' -and
                    $CliArguments[1] -ceq 'watch') {
                    $id = [long] $CliArguments[2]
                    $record = @(Get-AllRecords | Where-Object Id -eq $id) | Select-Object -First 1
                    if ($null -eq $record) {
                        Write-Error "Unknown fake run $id."
                        exit 1
                    }
                    if ($record.Conclusion -cne 'success') {
                        Write-Error "Simulated $($record.Kind) failure for run $id."
                        exit 1
                    }
                    if ($record.Kind -ceq 'publication') {
                        Set-Content -LiteralPath $env:FAKE_RELEASE_RELEASE -Value $env:FAKE_RELEASE_TAG
                    }
                    exit 0
                }

                if ($CliArguments.Count -ge 2 -and $CliArguments[0] -ceq 'api') {
                    $apiPath = @($CliArguments | Where-Object { $_ -like 'repos/*' }) |
                        Select-Object -First 1
                    if ($apiPath -match '/actions/runs/(?<id>[0-9]+)/artifacts$') {
                        $id = [long] $Matches.id
                        $record = @(Get-AllRecords | Where-Object Id -eq $id) |
                            Select-Object -First 1
                        if ($null -ne $record -and $record.Artifact) { '1' } else { '0' }
                        exit 0
                    }
                    if ($apiPath -ceq 'repos/example/csharpdb/releases') {
                        if (Test-Path -LiteralPath $env:FAKE_RELEASE_RELEASE) {
                            $env:FAKE_RELEASE_TAG
                        }
                        exit 0
                    }
                }

                if ($CliArguments.Count -ge 3 -and
                    $CliArguments[0] -ceq 'release' -and
                    $CliArguments[1] -ceq 'view') {
                    if (-not (Test-Path -LiteralPath $env:FAKE_RELEASE_RELEASE)) {
                        Write-Error 'Fake release does not exist.'
                        exit 1
                    }
                    [pscustomobject]@{
                        isDraft = $false
                        tagName = $env:FAKE_RELEASE_TAG
                        url = "https://example.invalid/releases/$($env:FAKE_RELEASE_TAG)"
                    } | ConvertTo-Json -Compress
                    exit 0
                }

                Write-Error "Unexpected fake gh invocation: $($CliArguments -join ' ')"
                exit 1
                """);
            CreateToolLauncher(toolRoot, "gh", "fake-release-gh.ps1");
        }
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

    private static void CreateToolLauncher(
        string toolRoot,
        string commandName,
        string scriptName)
    {
        if (OperatingSystem.IsWindows())
        {
            File.WriteAllText(
                Path.Combine(toolRoot, commandName + ".cmd"),
                $"@echo off{Environment.NewLine}" +
                $"pwsh -NoLogo -NoProfile -File \"%~dp0{scriptName}\" %*{Environment.NewLine}" +
                $"exit /b %ERRORLEVEL%{Environment.NewLine}");
            return;
        }

        string launcher = Path.Combine(toolRoot, commandName);
        File.WriteAllText(
            launcher,
            "#!/usr/bin/env sh\n" +
            "script_dir=\"$(CDPATH= cd -- \"$(dirname -- \"$0\")\" && pwd)\"\n" +
            $"exec pwsh -NoLogo -NoProfile -File \"${{script_dir}}/{scriptName}\" \"$@\"\n");
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
        return await RunProcessAsync(
            fileName,
            environment,
            TimeSpan.FromSeconds(30),
            arguments);
    }

    private static async Task<ProcessResult> RunProcessAsync(
        string fileName,
        IReadOnlyDictionary<string, string> environment,
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
        foreach ((string name, string value) in environment)
            startInfo.Environment[name] = value;

        using Process process = new() { StartInfo = startInfo };
        Assert.True(process.Start(), $"Could not start {fileName}.");
        Task<string> standardOutput = process.StandardOutput.ReadToEndAsync();
        Task<string> standardError = process.StandardError.ReadToEndAsync();
        using CancellationTokenSource timeout = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken);
        timeout.CancelAfter(timeoutDuration);
        try
        {
            await process.WaitForExitAsync(timeout.Token);
        }
        catch (OperationCanceledException)
            when (!TestContext.Current.CancellationToken.IsCancellationRequested)
        {
            KillProcessTree(process);
            throw new TimeoutException(
                $"{fileName} did not finish within {timeoutDuration.TotalSeconds:N0} seconds.");
        }
        catch
        {
            KillProcessTree(process);
            throw;
        }

        return new ProcessResult(
            process.ExitCode,
            await standardOutput,
            await standardError);
    }

    private static void KillProcessTree(Process process)
    {
        try
        {
            if (!process.HasExited)
                process.Kill(entireProcessTree: true);
        }
        catch (InvalidOperationException)
        {
            // The process exited between the state check and the kill request.
        }
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
