using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CSharpDB.Migration;
using CSharpDB.Migration.Retained;

const string protocol = "csharpdb-mysql-worker/v1";
const string captureProtocol =
    "csharpdb-mysql-capture-worker/v1";
const string modeVariable = "CSHARPDB_TEST_MYSQL_WORKER_MODE";
const string pidFileVariable = "CSHARPDB_TEST_MYSQL_WORKER_PID_FILE";
const string childVariable = "CSHARPDB_TEST_MYSQL_WORKER_CHILD";
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
            : MigrationSourceKind.MySql,
        Identity = "mysql:worker-harness-v1",
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
        "csharpdb-mysql-capture-worker/v1";
    const string modeVariable =
        "CSHARPDB_TEST_MYSQL_WORKER_MODE";
    const string childVariable =
        "CSHARPDB_TEST_MYSQL_WORKER_CHILD";
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
            "capture.csdbmysql",
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
        maximumBytes >
            256L * 1024 * 1024 * 1024 ||
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
                                            "mysql-retained:" +
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
            unchecked(
                (byte)(original ^ 0xff)));
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
            mode == "capture-wrong-format"
                ? "csharpdb-mysql-capture-result/v2"
                : "csharpdb-mysql-capture-result/v1",
        result.PackageDigest,
        result.Manifest.CatalogDigest,
        SnapshotIdentity =
            mode == "capture-secret-identity"
                ? "mysql:private-database-name"
                : result.Manifest.SnapshotIdentity,
        PackageBytes =
            mode == "capture-wrong-length"
                ? file.Length + 1
                : file.Length,
        TableCount =
            result.Manifest.Tables.Count,
        RowCount =
            result.Manifest.Tables.Sum(
                static table =>
                    table.RowCount),
    };
    byte[] responseJson =
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
                .. responseJson,
            ]);
    }
    finally
    {
        CryptographicOperations.ZeroMemory(
            responseJson);
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
                    MigrationSourceKind.MySql,
                Identity =
                    "mysql:worker-harness-retained-v1",
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
                    "mysql:database:worker-harness",
                Kind =
                    MigrationObjectKind.Database,
                SourceName =
                    "worker_harness",
                Facets =
                [
                    new MigrationCatalogFacet
                    {
                        Name =
                            "mysqlAnalyzerCatalogContract",
                        Value =
                            "csharpdb-mysql-catalog/v3",
                    },
                    new MigrationCatalogFacet
                    {
                        Name =
                            "mysqlCatalogContract",
                        Value =
                            "csharpdb-mysql-retained-catalog/v1",
                    },
                    new MigrationCatalogFacet
                    {
                        Name =
                            "mysqlDataContract",
                        Value =
                            "csharpdb-mysql-retained-data/v1",
                    },
                    new MigrationCatalogFacet
                    {
                        Name =
                            "mysqlRetainedContentDigest",
                        Value =
                            contentDigest,
                    },
                    new MigrationCatalogFacet
                    {
                        Name =
                            "mysqlRetainedSnapshotIdentity",
                        Value =
                            "mysql-retained:" +
                            contentDigest,
                    },
                ],
            },
        ],
        Diagnostics = [],
    };

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
