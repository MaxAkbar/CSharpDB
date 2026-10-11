using System.Diagnostics;
using System.Text.Json;
using CSharpDB.WalPrototype.Tests;

if (args.Length != 4) throw new ArgumentException("Expected operation, crash point, scratch directory, and new marker path.");
string operation = args[0], target = args[1], directory = Path.GetFullPath(args[2]), marker = Path.GetFullPath(args[3]);
bool armed = operation == "recover";
void Point(string name)
{
    if (!armed || name != target) return;
    using (var stream = new FileStream(marker, FileMode.CreateNew, FileAccess.Write, FileShare.Read))
    {
        JsonSerializer.Serialize(stream, new { operation, point = name, pid = Environment.ProcessId,
            walLength = new FileInfo(Path.Combine(directory, "database.wal")).Length });
        stream.Flush(true);
    }
    // Kill only this explicitly created worker, with no graceful store disposal.
    Process.GetCurrentProcess().Kill();
    Thread.Sleep(Timeout.Infinite);
}
byte[] Page(byte value) => Enumerable.Repeat(value, ControlCodec.PageSize).ToArray();
using var store = ExperimentalWalStore.Open(directory, point: Point);
switch (operation)
{
    case "commit":
        armed = true;
        store.Commit([new(0, Page(50)), new(1, Page(60))], 3);
        Point("after-commit-return");
        break;
    case "copy":
        var copying = store.CaptureCheckpoint();
        armed = true;
        store.CopyCheckpoint(copying);
        break;
    case "retire":
        var retiring = store.CaptureCheckpoint();
        store.CopyCheckpoint(retiring);
        store.Commit([new(2, Page(31))], 3);
        armed = true;
        store.RetireCheckpoint(retiring);
        break;
    case "reset":
        armed = true;
        store.Reset();
        break;
    case "recover":
        Point("after-recovery-return");
        break;
    default: throw new ArgumentException("Unknown operation.");
}
throw new InvalidOperationException("Requested crash point was not reached.");
