using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CSharpDB.Migration;
using CSharpDB.Migration.Retained;

const string protocol = "csharpdb-sqlserver-worker/v1";
const string ddlProtocol = "csharpdb-sqlserver-ddl-worker/v1";
const string captureProtocol =
    "csharpdb-sqlserver-capture-worker/v1";
const string modeVariable = "CSHARPDB_TEST_SQLSERVER_WORKER_MODE";
const string pidFileVariable = "CSHARPDB_TEST_SQLSERVER_WORKER_PID_FILE";
const string childVariable = "CSHARPDB_TEST_SQLSERVER_WORKER_CHILD";
const string secret = "Password=worker-stderr-secret";

if (args.Length >= 2 &&
    string.Equals(
        args[0],
        "--protocol",
        StringComparison.Ordinal) &&
    string.Equals(
        args[1],
        captureProtocol,
        StringComparison.Ordinal))
{
    return await RunCaptureAsync(args);
}

if (args.Length == 4 &&
    string.Equals(args[0], "--protocol", StringComparison.Ordinal) &&
    string.Equals(args[1], ddlProtocol, StringComparison.Ordinal) &&
    string.Equals(args[2], "--target-version", StringComparison.Ordinal) &&
    !string.IsNullOrWhiteSpace(args[3]))
{
    return await RunDdlAsync(args, args[3]);
}

if (args.Length != 6 ||
    !string.Equals(args[0], "--protocol", StringComparison.Ordinal) ||
    !string.Equals(args[1], protocol, StringComparison.Ordinal) ||
    !string.Equals(args[2], "--connection-env", StringComparison.Ordinal) ||
    !IsSafeEnvironmentVariableName(args[3]) ||
    !string.Equals(args[4], "--target-version", StringComparison.Ordinal) ||
    string.IsNullOrWhiteSpace(args[5]))
{
    return 10;
}

string connectionEnvironment = args[3];
string targetVersion = args[5];
string mode = Environment.GetEnvironmentVariable(modeVariable) ?? "success";
switch (mode)
{
    case "connection-check":
        if (string.IsNullOrWhiteSpace(
                Environment.GetEnvironmentVariable(connectionEnvironment)))
        {
            await Console.Error.WriteLineAsync(secret);
            return 11;
        }
        break;
    case "connection-error":
        await Console.Error.WriteLineAsync(secret);
        return 11;
    case "inspection-error":
        await Console.Error.WriteLineAsync(secret);
        return 12;
    case "internal-error":
        await Console.Error.WriteLineAsync(secret);
        return 13;
    case "bad-header":
        await WriteBytesAsync("wrong-worker/v1\n{}"u8.ToArray());
        return 0;
    case "invalid-utf8":
        await WriteBytesAsync(
            [.. Encoding.ASCII.GetBytes(protocol + "\n"), 0xff]);
        return 0;
    case "stdout-overflow":
        await WriteBytesAsync(Encoding.ASCII.GetBytes(protocol + "\n"));
        await WriteRepeatedAsync(
            Console.OpenStandardOutput(),
            64L * 1024 * 1024 + 1);
        return 0;
    case "stderr-overflow":
        await WriteRepeatedAsync(
            Console.OpenStandardError(),
            64L * 1024 + 1);
        return 13;
    case "stdout-overflow-tree":
        await RecordPidAsync();
        if (string.Equals(
                Environment.GetEnvironmentVariable(childVariable),
                "1",
                StringComparison.Ordinal))
        {
            await Task.Delay(Timeout.InfiniteTimeSpan);
            return 13;
        }
        int overflowChildId = StartChild(args);
        await RecordPidAsync(overflowChildId);
        await WriteBytesAsync(Encoding.ASCII.GetBytes(protocol + "\n"));
        await WriteRepeatedAsync(
            Console.OpenStandardOutput(),
            64L * 1024 * 1024 + 1);
        await Task.Delay(Timeout.InfiniteTimeSpan);
        return 13;
    case "hang-tree":
        await RecordPidAsync();
        if (!string.Equals(
                Environment.GetEnvironmentVariable(childVariable),
                "1",
                StringComparison.Ordinal))
        {
            int childId = StartChild(args);
            await RecordPidAsync(childId);
        }
        await Task.Delay(Timeout.InfiniteTimeSpan);
        return 13;
}

MigrationCatalog synthetic =
    await new SyntheticMigrationSourceInspector().InspectAsync(
        new MigrationInspectionRequest
        {
            TargetCSharpDbVersion = targetVersion,
            IncludeProfile = false,
        });
MigrationCatalog catalog = synthetic with
{
    TargetCSharpDbVersion = mode == "wrong-target"
        ? targetVersion + "-mismatch"
        : targetVersion,
    Source = synthetic.Source with
    {
        Kind = mode == "wrong-source"
            ? MigrationSourceKind.Synthetic
            : MigrationSourceKind.SqlServer,
        Identity = "sqlserver:worker-harness-v1",
        ProviderVersion = "worker-harness-v1",
        SourceVersion = "worker-harness-v1",
    },
    Diagnostics = [],
};

string json = MigrationArtifactSerializer.SerializeCatalog(
    catalog,
    writeIndented: false);
await WriteBytesAsync(
    [
        .. Encoding.ASCII.GetBytes(protocol + "\n"),
        .. new UTF8Encoding(false, true).GetBytes(json),
    ]);
return 0;

static async Task<int> RunCaptureAsync(
    IReadOnlyList<string> arguments)
{
    const string protocol =
        "csharpdb-sqlserver-capture-worker/v1";
    const string modeVariable =
        "CSHARPDB_TEST_SQLSERVER_WORKER_MODE";
    const string childVariable =
        "CSHARPDB_TEST_SQLSERVER_WORKER_CHILD";
    const string secret =
        "Password=worker-stderr-secret";
    if (arguments.Count != 12 ||
        !string.Equals(
            arguments[0],
            "--protocol",
            StringComparison.Ordinal) ||
        !string.Equals(
            arguments[1],
            protocol,
            StringComparison.Ordinal) ||
        !string.Equals(
            arguments[2],
            "--connection-env",
            StringComparison.Ordinal) ||
        !IsSafeEnvironmentVariableName(
            arguments[3]) ||
        !string.Equals(
            arguments[4],
            "--target-version",
            StringComparison.Ordinal) ||
        string.IsNullOrWhiteSpace(
            arguments[5]) ||
        !string.Equals(
            arguments[6],
            "--output",
            StringComparison.Ordinal) ||
        !Path.IsPathFullyQualified(
            arguments[7]) ||
        !string.Equals(
            Path.GetFileName(arguments[7]),
            "capture.csdbsqlserver",
            StringComparison.Ordinal) ||
        !string.Equals(
            arguments[8],
            "--max-source-bytes",
            StringComparison.Ordinal) ||
        !long.TryParse(
            arguments[9],
            System.Globalization.NumberStyles.None,
            System.Globalization.CultureInfo
                .InvariantCulture,
            out long maximumBytes) ||
        maximumBytes <= 0 ||
        !string.Equals(
            arguments[10],
            "--table-timeout-seconds",
            StringComparison.Ordinal) ||
        !int.TryParse(
            arguments[11],
            System.Globalization.NumberStyles.None,
            System.Globalization.CultureInfo
                .InvariantCulture,
            out int tableTimeoutSeconds) ||
        tableTimeoutSeconds <= 0 ||
        tableTimeoutSeconds > 86_400)
    {
        return 10;
    }

    string connectionEnvironment =
        arguments[3];
    string targetVersion = arguments[5];
    string outputPath = Path.GetFullPath(
        arguments[7]);
    string mode =
        Environment.GetEnvironmentVariable(
            modeVariable) ??
        "capture-success";
    switch (mode)
    {
        case "capture-connection-check":
            if (string.IsNullOrWhiteSpace(
                    Environment.GetEnvironmentVariable(
                        connectionEnvironment)))
            {
                await Console.Error.WriteLineAsync(
                    secret);
                return 11;
            }
            break;
        case "capture-connection-error":
            await Console.Error.WriteLineAsync(
                secret);
            return 11;
        case "capture-error":
            await Console.Error.WriteLineAsync(
                secret);
            return 12;
        case "capture-internal-error":
            await Console.Error.WriteLineAsync(
                secret);
            return 13;
        case "capture-limit-error":
            await Console.Error.WriteLineAsync(
                secret);
            return 14;
        case "capture-bad-header":
            await WriteBytesAsync(
                "wrong-capture/v1\n{}"u8.ToArray());
            return 0;
        case "capture-invalid-utf8":
            await WriteBytesAsync(
                [
                    .. Encoding.ASCII.GetBytes(
                        protocol + "\n"),
                    0xff,
                ]);
            return 0;
        case "capture-stdout-overflow":
            await WriteBytesAsync(
                Encoding.ASCII.GetBytes(
                    protocol + "\n"));
            await WriteRepeatedAsync(
                Console.OpenStandardOutput(),
                64L * 1024 + 1);
            return 0;
        case "capture-stderr-overflow":
            await WriteRepeatedAsync(
                Console.OpenStandardError(),
                64L * 1024 + 1);
            return 13;
        case "capture-hang-tree":
            await RecordPidAsync();
            if (!string.Equals(
                    Environment.GetEnvironmentVariable(
                        childVariable),
                    "1",
                    StringComparison.Ordinal))
            {
                int childId =
                    StartChild(arguments);
                await RecordPidAsync(childId);
            }
            await Task.Delay(
                Timeout.InfiniteTimeSpan);
            return 13;
    }

    RetainedMigrationPackageWriteResult result;
    try
    {
        result =
            await RetainedMigrationPackageWriter
                .WriteAsync(
                    new RetainedMigrationPackageCaptureRequest
                    {
                        OutputPath = outputPath,
                        Tables = [],
                        CatalogFactory =
                            (
                                summary,
                                _) =>
                                ValueTask.FromResult(
                                    new RetainedMigrationCatalogBinding
                                    {
                                        Catalog =
                                            CreateCaptureCatalog(
                                                targetVersion,
                                                summary
                                                    .ContentDigest),
                                        SnapshotIdentity =
                                            "sqlserver-retained:" +
                                            summary
                                                .ContentDigest,
                                    }),
                        Options =
                            new RetainedMigrationPackageWriteOptions
                            {
                                MaxPackageBytes =
                                    maximumBytes,
                            },
                    });
    }
    catch
    {
        await Console.Error.WriteLineAsync(
            secret);
        return 12;
    }

    if (mode == "capture-tampered")
    {
        await using FileStream package =
            new(
                outputPath,
                FileMode.Open,
                FileAccess.ReadWrite,
                FileShare.None);
        package.Position =
            Math.Max(0, package.Length - 1);
        int original = package.ReadByte();
        package.Position =
            Math.Max(0, package.Length - 1);
        package.WriteByte(
            unchecked((byte)(original ^ 0xff)));
        await package.FlushAsync();
    }
    else if (mode == "capture-truncated")
    {
        await using FileStream package =
            new(
                outputPath,
                FileMode.Open,
                FileAccess.ReadWrite,
                FileShare.None);
        package.SetLength(
            Math.Max(1, package.Length / 2));
        await package.FlushAsync();
    }

    var file = new FileInfo(outputPath);
    var response = new
    {
        Format =
            "csharpdb-sqlserver-capture-result/v1",
        result.PackageDigest,
        result.Manifest.CatalogDigest,
        result.Manifest.SnapshotIdentity,
        PackageBytes = file.Length,
        TableCount =
            result.Manifest.Tables.Count,
        RowCount =
            result.Manifest.Tables.Sum(
                static table =>
                    table.RowCount),
    };
    byte[] json =
        JsonSerializer.SerializeToUtf8Bytes(
            response,
            new JsonSerializerOptions(
                JsonSerializerDefaults.Web));
    try
    {
        await WriteBytesAsync(
            [
                .. Encoding.ASCII.GetBytes(
                    protocol + "\n"),
                .. json,
            ]);
    }
    finally
    {
        CryptographicOperations.ZeroMemory(
            json);
    }
    return 0;
}

static MigrationCatalog CreateCaptureCatalog(
    string targetVersion,
    string contentDigest) =>
    new()
    {
        TargetCSharpDbVersion =
            targetVersion,
        Source =
            new MigrationSourceIdentity
            {
                Kind =
                    MigrationSourceKind.SqlServer,
                Identity =
                    "sqlserver:worker-harness-retained-v1",
                Fingerprint =
                    contentDigest,
                ProviderVersion =
                    "worker-harness-v1",
                SourceVersion =
                    "worker-harness-v1",
                Consistency =
                    new MigrationConsistencyStrategy
                    {
                        Kind =
                            MigrationConsistencyKind.Snapshot,
                        Description =
                            "Deterministic worker-harness retained snapshot.",
                    },
            },
        Objects =
        [
            new MigrationCatalogObject
            {
                ObjectId =
                    "sqlserver:database:worker-harness",
                Kind =
                    MigrationObjectKind.Database,
                SourceName =
                    "worker_harness",
                Facets =
                [
                    new MigrationCatalogFacet
                    {
                        Name =
                            "sqlServerAnalyzerCatalogContract",
                        Value =
                            "csharpdb-sqlserver-catalog/v6",
                    },
                    new MigrationCatalogFacet
                    {
                        Name =
                            "sqlServerCatalogContract",
                        Value =
                            "csharpdb-sqlserver-retained-catalog/v1",
                    },
                    new MigrationCatalogFacet
                    {
                        Name =
                            "sqlServerDataContract",
                        Value =
                            "csharpdb-sqlserver-retained-data/v1",
                    },
                ],
            },
        ],
        Diagnostics = [],
    };

static async Task<int> RunDdlAsync(
    IReadOnlyList<string> arguments,
    string targetVersion)
{
    const string ddlProtocol =
        "csharpdb-sqlserver-ddl-worker/v1";
    const string modeVariable =
        "CSHARPDB_TEST_SQLSERVER_WORKER_MODE";
    const string childVariable =
        "CSHARPDB_TEST_SQLSERVER_WORKER_CHILD";
    const string secret =
        "Password=worker-stderr-secret";
    const string diagnosticSecret =
        "Password=ddl-worker-diagnostic-secret";
    const int maxInputBytes = 16 * 1024 * 1024;

    byte[] input;
    try
    {
        input = await ReadBoundedAsync(
            Console.OpenStandardInput(),
            maxInputBytes);
    }
    catch
    {
        return 10;
    }

    try
    {
        string source;
        try
        {
            source = new UTF8Encoding(
                encoderShouldEmitUTF8Identifier: false,
                throwOnInvalidBytes: true)
                .GetString(input);
        }
        catch (DecoderFallbackException)
        {
            return 10;
        }

        string mode =
            Environment.GetEnvironmentVariable(modeVariable) ??
            "ddl-success";
        switch (mode)
        {
            case "ddl-analysis-error":
                await Console.Error.WriteLineAsync(secret);
                return 12;
            case "ddl-internal-error":
                await Console.Error.WriteLineAsync(secret);
                return 13;
            case "ddl-bad-header":
                await WriteBytesAsync(
                    Encoding.ASCII.GetBytes("wrong-worker/v1\n{}"));
                return 0;
            case "ddl-invalid-utf8":
                await WriteBytesAsync(
                    [
                        .. Encoding.ASCII.GetBytes(
                            ddlProtocol + "\n"),
                        0xff,
                    ]);
                return 0;
            case "ddl-stdout-overflow":
                await WriteBytesAsync(
                    Encoding.ASCII.GetBytes(
                        ddlProtocol + "\n"));
                await WriteRepeatedAsync(
                    Console.OpenStandardOutput(),
                    8L * 1024 * 1024 + 1);
                return 0;
            case "ddl-stderr-overflow":
                await WriteRepeatedAsync(
                    Console.OpenStandardError(),
                    64L * 1024 + 1);
                return 13;
            case "ddl-stdout-overflow-tree":
                await RecordPidAsync();
                if (string.Equals(
                        Environment.GetEnvironmentVariable(
                            childVariable),
                        "1",
                        StringComparison.Ordinal))
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan);
                    return 13;
                }
                int overflowChildId =
                    StartChild(arguments);
                await RecordPidAsync(overflowChildId);
                await WriteBytesAsync(
                    Encoding.ASCII.GetBytes(
                        ddlProtocol + "\n"));
                await WriteRepeatedAsync(
                    Console.OpenStandardOutput(),
                    8L * 1024 * 1024 + 1);
                await Task.Delay(Timeout.InfiniteTimeSpan);
                return 13;
            case "ddl-hang-tree":
                await RecordPidAsync();
                if (!string.Equals(
                        Environment.GetEnvironmentVariable(
                            childVariable),
                        "1",
                        StringComparison.Ordinal))
                {
                    int childId = StartChild(arguments);
                    await RecordPidAsync(childId);
                }
                await Task.Delay(Timeout.InfiniteTimeSpan);
                return 13;
        }

        string sourceDigest = ComputeDdlDigest(input);
        bool contradictoryDiagnostic =
            mode == "ddl-contradictory-success-diagnostic";
        bool hostileDiagnostic =
            mode is "ddl-malicious-diagnostic-prose" or
                "ddl-wrong-capability" or
                "ddl-contradictory-success-diagnostic";
        string diagnosticRule = contradictoryDiagnostic
            ? "tsql.ddl.statement.unsupported"
            : "csharpdb.ddl.canonical-rewrite";
        var report = new
        {
            Format = mode == "ddl-wrong-format"
                ? "wrong-ddl-report/v1"
                : "csharpdb-ddl-compatibility/v1",
            Dialect = mode == "ddl-wrong-dialect"
                ? "csharpdb"
                : "tsql",
            SourceGrammar = mode == "ddl-wrong-grammar"
                ? "tsql170"
                : "tsql160",
            TargetCSharpDbVersion =
                mode == "ddl-wrong-target"
                    ? targetVersion + "-mismatch"
                    : targetVersion,
            CapabilityDigest =
                mode == "ddl-wrong-capability"
                    ? new string('b', 64)
                    : CSharpDbCapabilityCatalogLoader
                        .LoadEmbedded()
                        .Digest,
            ScriptDigest = mode == "ddl-wrong-digest"
                ? new string('c', 64)
                : sourceDigest,
            Status = mode == "ddl-overclaim-compatible"
                ? "compatible"
                : "compatibleWithRewrite",
            HighestEvidence = "scratchExecuted",
            RuleId = "csharpdb.ddl.canonical-rewrite",
            StatementCount = 1,
            ProvenStatementCount = 1,
            CandidateActionCount = 1,
            CatalogDigest = new string('d', 64),
            PlanContractDigest = new string('e', 64),
            GeneratedDdlDigest = new string('f', 64),
            ExpectedSchemaDigest = new string('1', 64),
            ActualSchemaDigest = new string('1', 64),
            Statements = mode == "ddl-null-statement"
                ? new object?[] { null }
                : new object?[]
                {
                    new
                    {
                        Index = 0,
                        Kind = "create-table",
                        Span = new
                        {
                            SourceId = "input",
                            Start = 0,
                            Length = source.Length,
                            Line = 1,
                            Column = 1,
                        },
                        Status = mode == "ddl-overclaim-compatible"
                            ? "compatible"
                            : "compatibleWithRewrite",
                        Evidence = "scratchExecuted",
                        RuleId =
                            "csharpdb.ddl.canonical-rewrite",
                    },
                },
            Diagnostics = mode == "ddl-null-diagnostic"
                ? new object?[] { null }
                : new object?[]
                {
                    new
                    {
                        Ordinal = 0,
                        DiagnosticId = string.Concat(
                            contradictoryDiagnostic
                                ? "tsql-ddl/000000/"
                                : "csharpdb-ddl/000000/",
                            diagnosticRule),
                        RuleId = diagnosticRule,
                        Severity = contradictoryDiagnostic
                            ? "error"
                            : "warning",
                        Status = contradictoryDiagnostic
                            ? "unsupported"
                            : mode == "ddl-overclaim-compatible"
                                ? "compatible"
                                : "compatibleWithRewrite",
                        Evidence = contradictoryDiagnostic
                            ? "parsed"
                            : "scratchExecuted",
                        StatementIndex = contradictoryDiagnostic
                            ? 0
                            : (int?)null,
                        SourceSpan = contradictoryDiagnostic
                            ? new
                            {
                                SourceId = "input",
                                Start = 0,
                                Length = source.Length,
                                Line = 1,
                                Column = 1,
                            }
                            : null,
                        Summary = hostileDiagnostic
                                ? string.Concat(
                                    diagnosticSecret,
                                    "\r\nDROP TABLE private_data;",
                                    "\u001b[31mINJECTED-CONTROL")
                                : "Untrusted harness summary.",
                        Remediation = hostileDiagnostic
                                ? string.Concat(
                                    diagnosticSecret,
                                    "\u0000hostile-remediation")
                                : "Untrusted harness remediation.",
                    },
                },
            Differences = mode == "ddl-null-difference"
                ? new object?[] { null }
                : Array.Empty<object?>(),
        };
        byte[] json = JsonSerializer.SerializeToUtf8Bytes(
            report,
            new JsonSerializerOptions(JsonSerializerDefaults.Web));
        try
        {
            await WriteBytesAsync(
                [
                    .. Encoding.ASCII.GetBytes(
                        ddlProtocol + "\n"),
                    .. json,
                ]);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(json);
        }
        return 0;
    }
    finally
    {
        CryptographicOperations.ZeroMemory(input);
    }
}

static async Task<byte[]> ReadBoundedAsync(
    Stream stream,
    int maximumBytes)
{
    using var output = new MemoryStream();
    byte[] buffer = new byte[64 * 1024];
    try
    {
        while (true)
        {
            int read = await stream.ReadAsync(buffer);
            if (read == 0)
                return output.ToArray();
            if (output.Length > maximumBytes - read)
                throw new IOException("Input limit exceeded.");
            output.Write(buffer, 0, read);
        }
    }
    finally
    {
        CryptographicOperations.ZeroMemory(buffer);
        if (output.TryGetBuffer(out ArraySegment<byte> written))
            CryptographicOperations.ZeroMemory(written.AsSpan());
    }
}

static string ComputeDdlDigest(ReadOnlySpan<byte> source)
{
    using IncrementalHash hash =
        IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
    hash.AppendData("tsql-ddl-input/v1"u8);
    hash.AppendData([0]);
    hash.AppendData(source);
    return Convert.ToHexString(hash.GetHashAndReset())
        .ToLowerInvariant();
}

static bool IsSafeEnvironmentVariableName(string value)
{
    if (value is not { Length: > 0 and <= 128 } ||
        value[0] is not (>= 'A' and <= 'Z') and
            not (>= 'a' and <= 'z') and
            not '_')
    {
        return false;
    }

    foreach (char character in value.AsSpan(1))
    {
        if (character is not (>= 'A' and <= 'Z') and
            not (>= 'a' and <= 'z') and
            not (>= '0' and <= '9') and
            not '_')
        {
            return false;
        }
    }

    return true;
}

static int StartChild(IReadOnlyList<string> arguments)
{
    var startInfo = new ProcessStartInfo
    {
        FileName = Environment.ProcessPath
            ?? throw new InvalidOperationException(
                "The worker harness apphost path is unavailable."),
        UseShellExecute = false,
        CreateNoWindow = true,
    };
    foreach (string argument in arguments)
        startInfo.ArgumentList.Add(argument);
    startInfo.Environment[childVariable] = "1";
    using Process child = Process.Start(startInfo)
        ?? throw new InvalidOperationException(
            "The worker harness child did not start.");
    return child.Id;
}

static async Task WriteBytesAsync(byte[] bytes)
{
    Stream stdout = Console.OpenStandardOutput();
    await stdout.WriteAsync(bytes);
    await stdout.FlushAsync();
}

static async Task WriteRepeatedAsync(Stream stream, long byteCount)
{
    byte[] bytes = new byte[16 * 1024];
    while (byteCount > 0)
    {
        int count = (int)Math.Min(bytes.Length, byteCount);
        await stream.WriteAsync(bytes.AsMemory(0, count));
        byteCount -= count;
    }
    await stream.FlushAsync();
}

static async Task RecordPidAsync(int? processId = null)
{
    string? pidFile = Environment.GetEnvironmentVariable(pidFileVariable);
    if (string.IsNullOrWhiteSpace(pidFile))
        return;

    for (int attempt = 0; attempt < 20; attempt++)
    {
        try
        {
            await File.AppendAllTextAsync(
                pidFile,
                (processId ?? Environment.ProcessId).ToString(
                    System.Globalization.CultureInfo.InvariantCulture) +
                Environment.NewLine);
            return;
        }
        catch (IOException) when (attempt < 19)
        {
            await Task.Delay(25);
        }
    }
}
