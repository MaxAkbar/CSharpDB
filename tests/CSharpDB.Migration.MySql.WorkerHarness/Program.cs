using System.Diagnostics;
using System.Text;
using CSharpDB.Migration;

const string protocol = "csharpdb-mysql-worker/v1";
const string modeVariable = "CSHARPDB_TEST_MYSQL_WORKER_MODE";
const string pidFileVariable = "CSHARPDB_TEST_MYSQL_WORKER_PID_FILE";
const string childVariable = "CSHARPDB_TEST_MYSQL_WORKER_CHILD";
const string secret = "Password=worker-stderr-secret";

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
