using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using CSharpDB.Migration.Files.Json;

namespace CSharpDB.Migration.Files.Tests;

[Collection(CsvExportCrashHarnessProcessCollection.Name)]
public sealed class JsonSnapshotPackageProcessTests
{
#if DEBUG
    private const string BuildConfiguration = "Debug";
#else
    private const string BuildConfiguration = "Release";
#endif

    private static readonly UTF8Encoding s_strictUtf8 =
        new(
            encoderShouldEmitUTF8Identifier: false,
            throwOnInvalidBytes: true);

    private static readonly JsonSerializerOptions s_jsonOptions =
        new(JsonSerializerDefaults.Web)
        {
            PropertyNameCaseInsensitive = false,
            UnmappedMemberHandling =
                JsonUnmappedMemberHandling.Disallow,
        };

    private static CancellationToken Cancellation =>
        TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(nameof(JsonInputFraming.RootArray))]
    [InlineData(nameof(JsonInputFraming.MultipleValues))]
    public async Task FreshProcessesWriteReadAndResumeExactPackageRows(
        string framingName)
    {
        JsonInputFraming framing =
            Enum.Parse<JsonInputFraming>(
                framingName,
                ignoreCase: false);
        using var fixture = new TemporaryDirectory();
        string sourcePath = fixture.PathFor("source.json");
        string packagePath =
            fixture.PathFor("source.csdbjson");
        string writeResultPath =
            fixture.PathFor("write-result.json");
        string readResultPath =
            fixture.PathFor("read-result.json");
        string resumeResultPath =
            fixture.PathFor("resume-result.json");
        string writeWorkspace =
            fixture.CreateDirectory("write-workspace");
        string readWorkspace =
            fixture.CreateDirectory("read-workspace");
        string resumeWorkspace =
            fixture.CreateDirectory("resume-workspace");

        await File.WriteAllTextAsync(
            sourcePath,
            Frame(
                framing,
                """{"id":1,"name":"alpha","active":true,"payload":{"n":1.2300e+4}}""",
                """{"id":2,"name":"bravo","active":false,"payload":[1,true,"x"]}""",
                """{"id":3,"name":"charlie","active":true,"payload":{"emoji":"😀"}}""",
                """{"id":4,"name":"delta","active":false,"payload":{"n":-0.50}}""",
                """{"id":5,"name":"echo","active":true,"payload":[null,{"x":2}]}""",
                """{"id":6,"name":"foxtrot","active":false,"payload":{"text":"line\nbreak"}}""",
                """{"id":7,"name":"golf","active":true,"payload":{"last":true}}"""),
            s_strictUtf8,
            Cancellation);

        HarnessProcessResult writeProcess =
            await RunHarnessAsync(
                [
                    "--json-package-mode",
                    "write",
                    "--json-package",
                    packagePath,
                    "--json-workspace",
                    writeWorkspace,
                    "--json-result",
                    writeResultPath,
                    "--json-source",
                    sourcePath,
                    "--json-framing",
                    framingName,
                ],
                Cancellation);
        AssertSuccessful(writeProcess);
        JsonPackageProcessResult write =
            await ReadResultAsync(
                writeResultPath,
                sourcePath,
                Cancellation);
        Assert.Equal("write", write.Mode);
        AssertCanonicalPrefixedDigest(
            write.ManifestDigest);
        AssertCanonicalHexDigest(write.CatalogDigest);
        Assert.False(
            string.IsNullOrWhiteSpace(
                write.FirstBatchCursor));
        Assert.Equal(3, write.AcceptedRowCount);
        Assert.Equal(0, write.RejectedRowCount);
        Assert.Equal(3, write.RowDigests.Length);
        Assert.All(
            write.RowDigests,
            AssertCanonicalPrefixedDigest);
        AssertWorkspaceEmpty(writeWorkspace);

        byte[] packageBytes =
            await File.ReadAllBytesAsync(
                packagePath,
                Cancellation);
        File.Delete(sourcePath);
        Assert.False(File.Exists(sourcePath));

        HarnessProcessResult readProcess =
            await RunHarnessAsync(
                [
                    "--json-package-mode",
                    "read",
                    "--json-package",
                    packagePath,
                    "--json-workspace",
                    readWorkspace,
                    "--json-result",
                    readResultPath,
                    "--json-expected-manifest-digest",
                    write.ManifestDigest,
                ],
                Cancellation);
        AssertSuccessful(readProcess);
        JsonPackageProcessResult read =
            await ReadResultAsync(
                readResultPath,
                sourcePath,
                Cancellation);
        AssertEquivalentPackage(write, read);
        Assert.Equal("read", read.Mode);
        Assert.Null(read.FirstBatchCursor);
        Assert.Equal(7, read.AcceptedRowCount);
        Assert.Equal(0, read.RejectedRowCount);
        Assert.Equal(7, read.RowDigests.Length);
        Assert.Equal(
            7,
            read.RowDigests.Distinct(
                StringComparer.Ordinal).Count());
        Assert.Equal(
            write.RowDigests,
            read.RowDigests.Take(
                write.AcceptedRowCount));
        Assert.Equal(
            packageBytes,
            await File.ReadAllBytesAsync(
                packagePath,
                Cancellation));
        AssertWorkspaceEmpty(readWorkspace);

        HarnessProcessResult resumeProcess =
            await RunHarnessAsync(
                [
                    "--json-package-mode",
                    "resume",
                    "--json-package",
                    packagePath,
                    "--json-workspace",
                    resumeWorkspace,
                    "--json-result",
                    resumeResultPath,
                    "--json-expected-manifest-digest",
                    write.ManifestDigest,
                    "--json-resume-cursor",
                    write.FirstBatchCursor!,
                ],
                Cancellation);
        AssertSuccessful(resumeProcess);
        JsonPackageProcessResult resume =
            await ReadResultAsync(
                resumeResultPath,
                sourcePath,
                Cancellation);
        AssertEquivalentPackage(write, resume);
        Assert.Equal("resume", resume.Mode);
        Assert.Null(resume.FirstBatchCursor);
        Assert.Equal(4, resume.AcceptedRowCount);
        Assert.Equal(0, resume.RejectedRowCount);
        Assert.Equal(
            read.RowDigests.Skip(
                write.AcceptedRowCount),
            resume.RowDigests);
        Assert.Equal(
            packageBytes,
            await File.ReadAllBytesAsync(
                packagePath,
                Cancellation));
        AssertWorkspaceEmpty(resumeWorkspace);
    }

    private static void AssertEquivalentPackage(
        JsonPackageProcessResult expected,
        JsonPackageProcessResult actual)
    {
        Assert.Equal(
            expected.ManifestDigest,
            actual.ManifestDigest);
        Assert.Equal(
            expected.CatalogDigest,
            actual.CatalogDigest);
        Assert.Equal(
            expected.SnapshotIdentity,
            actual.SnapshotIdentity);
    }

    private static void AssertSuccessful(
        HarnessProcessResult result)
    {
        Assert.Equal(0, result.ExitCode);
        Assert.Equal(string.Empty, result.StandardOutput);
        Assert.Equal(string.Empty, result.StandardError);
    }

    private static void AssertWorkspaceEmpty(string path) =>
        Assert.Empty(
            Directory.EnumerateFileSystemEntries(path));

    private static void AssertCanonicalPrefixedDigest(
        string digest)
    {
        Assert.Equal(71, digest.Length);
        Assert.StartsWith(
            "sha256:",
            digest,
            StringComparison.Ordinal);
        AssertLowerHex(digest.AsSpan(7));
    }

    private static void AssertCanonicalHexDigest(string digest)
    {
        Assert.Equal(64, digest.Length);
        AssertLowerHex(digest);
    }

    private static void AssertLowerHex(ReadOnlySpan<char> value)
    {
        foreach (char character in value)
        {
            Assert.True(
                character is >= '0' and <= '9' or
                    >= 'a' and <= 'f');
        }
    }

    private static async Task<JsonPackageProcessResult>
        ReadResultAsync(
            string path,
            string forbiddenSourcePath,
            CancellationToken cancellationToken)
    {
        byte[] bytes =
            await File.ReadAllBytesAsync(
                path,
                cancellationToken);
        Assert.NotEmpty(bytes);
        Assert.False(
            bytes.AsSpan().StartsWith(
                Encoding.UTF8.Preamble));
        Assert.DoesNotContain((byte)0, bytes);
        string json = s_strictUtf8.GetString(bytes);
        Assert.DoesNotContain(
            forbiddenSourcePath,
            json,
            StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(
            "alpha",
            json,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "bravo",
            json,
            StringComparison.Ordinal);

        return JsonSerializer.Deserialize<
                JsonPackageProcessResult>(
                bytes,
                s_jsonOptions)
            ?? throw new InvalidDataException(
                "The JSON package process result is empty.");
    }

    private static async Task<HarnessProcessResult>
        RunHarnessAsync(
            IReadOnlyList<string> arguments,
            CancellationToken cancellationToken)
    {
        string assemblyPath = FindCrashHarnessAssembly();
        string dotnetHost =
            Environment.GetEnvironmentVariable(
                "DOTNET_HOST_PATH")
            is { Length: > 0 } configuredHost
                ? configuredHost
                : "dotnet";
        var startInfo = new ProcessStartInfo(dotnetHost)
        {
            WorkingDirectory =
                Path.GetDirectoryName(assemblyPath)!,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
        startInfo.ArgumentList.Add(assemblyPath);
        foreach (string argument in arguments)
            startInfo.ArgumentList.Add(argument);

        using var process = new Process
        {
            StartInfo = startInfo,
        };
        if (!process.Start())
        {
            throw new InvalidOperationException(
                "Failed to start the JSON package process harness.");
        }

        Task<string> standardOutput =
            process.StandardOutput.ReadToEndAsync(
                cancellationToken);
        Task<string> standardError =
            process.StandardError.ReadToEndAsync(
                cancellationToken);
        using var timeout =
            CancellationTokenSource
                .CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(30));
        try
        {
            await process
                .WaitForExitAsync(timeout.Token)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
            when (!cancellationToken.IsCancellationRequested)
        {
            if (!process.HasExited)
                process.Kill(entireProcessTree: true);
            await process
                .WaitForExitAsync(CancellationToken.None)
                .ConfigureAwait(false);
            throw new TimeoutException(
                "The JSON package process harness exceeded its 30-second timeout.");
        }
        catch (OperationCanceledException)
        {
            if (!process.HasExited)
                process.Kill(entireProcessTree: true);
            await process
                .WaitForExitAsync(CancellationToken.None)
                .ConfigureAwait(false);
            throw;
        }

        return new HarnessProcessResult(
            process.ExitCode,
            await standardOutput.ConfigureAwait(false),
            await standardError.ConfigureAwait(false));
    }

    private static string FindCrashHarnessAssembly()
    {
        DirectoryInfo? current =
            new(AppContext.BaseDirectory);
        while (current is not null)
        {
            string candidate = Path.Combine(
                current.FullName,
                "tests",
                "CSharpDB.Migration.CrashHarness",
                "bin",
                BuildConfiguration,
                "net10.0",
                "CSharpDB.Migration.CrashHarness.dll");
            if (File.Exists(candidate))
                return candidate;
            current = current.Parent;
        }

        throw new FileNotFoundException(
            $"Could not locate the {BuildConfiguration} migration crash harness assembly.");
    }

    private static string Frame(
        JsonInputFraming framing,
        params string[] values) =>
        framing switch
        {
            JsonInputFraming.RootArray =>
                "[\n" +
                string.Join(",\n", values) +
                "\n]",
            JsonInputFraming.MultipleValues =>
                string.Join("\n", values) + "\n",
            _ => throw new ArgumentOutOfRangeException(
                nameof(framing)),
        };

    private sealed record HarnessProcessResult(
        int ExitCode,
        string StandardOutput,
        string StandardError);

    private sealed record JsonPackageProcessResult
    {
        public required string Mode { get; init; }

        public required string ManifestDigest { get; init; }

        public required string CatalogDigest { get; init; }

        public required string SnapshotIdentity { get; init; }

        public string? FirstBatchCursor { get; init; }

        public required int AcceptedRowCount { get; init; }

        public required int RejectedRowCount { get; init; }

        public required string[] RowDigests { get; init; }
    }

    private sealed class TemporaryDirectory : IDisposable
    {
        internal TemporaryDirectory()
        {
            Root = Path.GetFullPath(
                Path.Combine(
                    Path.GetTempPath(),
                    "csharpdb-json-package-process-tests",
                    Guid.NewGuid().ToString("N")));
            Directory.CreateDirectory(Root);
        }

        internal string Root { get; }

        internal string PathFor(string leaf) =>
            Path.Combine(Root, leaf);

        internal string CreateDirectory(string leaf)
        {
            string path = PathFor(leaf);
            Directory.CreateDirectory(path);
            return path;
        }

        public void Dispose()
        {
            if (Directory.Exists(Root))
                Directory.Delete(Root, recursive: true);
        }
    }
}
