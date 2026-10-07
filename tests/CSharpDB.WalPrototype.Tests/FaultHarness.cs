using System.Buffers.Binary;
using System.Text.Json;

namespace CSharpDB.WalPrototype.Tests;

internal sealed record CrashImage(string Point, byte[] Wal, byte[] Database);

internal static class Scratch
{
    // Keep every invocation separate, including failed tests. Never reuse or delete evidence.
    public static string Create(string name)
    {
        string root = Environment.GetEnvironmentVariable("CSHARPDB_WAL_PROTOTYPE_EVIDENCE") ??
            Path.Combine(Path.GetTempPath(), "CSharpDB.WalPrototype");
        string path = Path.Combine(root, name + "-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    public static void WriteJson(string path, object value)
    {
        using var stream = new FileStream(path, FileMode.CreateNew);
        JsonSerializer.Serialize(stream, value, new JsonSerializerOptions { WriteIndented = true });
    }
}

// This fixture has whole, individually committed page records. It intentionally does NOT
// implement CSharpDB's frame checksum, commit markers, partial transactions or replay parser.
internal static class PageFixture
{
    public static readonly int[] InitialPages = [10, 20, 30];
    public static readonly int[] RetiredPages = [11, 21, 30];
    public static readonly int[] FinalPages = [12, 21, 30];

    public static byte[] Database(int[] values)
    {
        byte[] bytes = new byte[values.Length * ControlCodec.PageSize];
        for (int i = 0; i < values.Length; i++)
            bytes.AsSpan(i * ControlCodec.PageSize, ControlCodec.PageSize).Fill(checked((byte)values[i]));
        return bytes;
    }

    public static byte[] Frame(int page, int value, GenerationSalt salt)
    {
        byte[] bytes = new byte[ControlCodec.FrameSize];
        BinaryPrimitives.WriteInt32LittleEndian(bytes, page);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(4), value);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(8), salt.First);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(12), salt.Second);
        "FIXTURE!"u8.CopyTo(bytes.AsSpan(16));
        bytes.AsSpan(24).Fill(checked((byte)value));
        return bytes;
    }

    public static byte[] Wal() => [.. ControlCodecTests.Header(ControlCodecTests.Record(1),
        ControlCodecTests.Record(2)), .. Frame(0, 11, new(3, 4)), .. Frame(1, 21, new(3, 4)),
        .. Frame(0, 12, new(3, 4))];

    public static byte[] Recover(byte[] wal, byte[] database)
    {
        var record = ControlCodec.Select(wal.AsSpan(0, Math.Min(wal.Length, ControlCodec.DataStart)), wal.Length).Current;
        if (record.PageCount != 3 || database.Length != 3 * ControlCodec.PageSize)
            throw new InvalidDataException("Invalid fixture database extent.");
        byte[] result = (byte[])database.Clone();
        if (record.Mode == ControlMode.Empty) return result;
        for (long offset = record.Floor; offset < wal.Length; offset += ControlCodec.FrameSize)
        {
            if (wal.Length - offset < ControlCodec.FrameSize)
                throw new InvalidDataException("Partial fixture frame.");
            var frame = wal.AsSpan((int)offset, ControlCodec.FrameSize);
            int page = BinaryPrimitives.ReadInt32LittleEndian(frame);
            int value = BinaryPrimitives.ReadInt32LittleEndian(frame[4..]);
            if (page is < 0 or >= 3 || value is < 0 or > 255 ||
                !frame.SequenceEqual(Frame(page, value, record.Salt)))
                throw new InvalidDataException("Invalid active fixture frame.");
            frame[24..].CopyTo(result.AsSpan(page * ControlCodec.PageSize, ControlCodec.PageSize));
        }
        return result;
    }
}

// Deterministic persistence model over actual files. We first exercise real writes and
// Flush(true), then materialize each allowed interrupted image in a distinct file and reopen
// it through the real codec. These are injected images, not a claim about power-loss behavior.
internal sealed class TraceFile : IControlFile, IDisposable
{
    private readonly FileControlStore _walFile;
    private readonly FileStream _databaseFile;
    private byte[] _wal, _database, _durableWal, _durableDatabase;
    private int? _pendingSlot;
    private bool _pendingTruncate;
    private readonly int _throwAt;
    private readonly bool _throwAfter;
    public List<CrashImage> Images { get; } = [];
    public List<string> Operations { get; } = [];
    public Action<string>? OnOperation { get; set; }
    public string DirectoryPath { get; }
    public long Length => _walFile.Length;
    public byte[] ReadHeader() => _walFile.ReadHeader();

    public TraceFile(string path, byte[] wal, byte[] database, int throwAt = 0, bool throwAfter = false)
    {
        DirectoryPath = path;
        File.WriteAllBytes(Path.Combine(path, "working.wal"), wal);
        File.WriteAllBytes(Path.Combine(path, "working.base"), database);
        _walFile = new(Path.Combine(path, "working.wal"));
        _databaseFile = new(Path.Combine(path, "working.base"), FileMode.Open, FileAccess.ReadWrite,
            FileShare.None, bufferSize: 1, FileOptions.RandomAccess);
        _walFile.Flush();
        _databaseFile.Flush(true);
        _wal = (byte[])wal.Clone(); _durableWal = (byte[])wal.Clone();
        _database = (byte[])database.Clone(); _durableDatabase = (byte[])database.Clone();
        _throwAt = throwAt; _throwAfter = throwAfter;
        Capture("initial", _durableWal, _durableDatabase);
    }

    public void CopyAndFlushBase(int[] values)
    {
        Before("write-base");
        _database = PageFixture.Database(values);
        _databaseFile.Position = 0;
        _databaseFile.Write(_database);
        BaseCuts("during-base-write");
        After();
        Before("flush-base");
        BaseCuts("during-base-flush");
        _databaseFile.Flush(true);
        _durableDatabase = (byte[])_database.Clone();
        Capture("after-base-flush", _durableWal, _durableDatabase);
        After();
    }

    public void WriteSlot(int slot, byte[] bytes)
    {
        Before($"write-slot-{slot}");
        if (_pendingSlot is not null || _pendingTruncate)
            throw new InvalidOperationException("Prototype must flush each mutation separately.");
        _walFile.WriteSlot(slot, bytes);
        bytes.CopyTo(_wal, slot * ControlCodec.SlotSize);
        _pendingSlot = slot;
        WalCuts($"during-slot-{slot}-write");
        After();
    }

    public void Flush()
    {
        Before("flush-wal");
        WalCuts("during-wal-flush");
        _walFile.Flush();
        _durableWal = (byte[])_wal.Clone();
        _pendingSlot = null; _pendingTruncate = false;
        Capture("after-wal-flush", _durableWal, _durableDatabase);
        After();
    }

    public void Truncate()
    {
        Before("truncate");
        _walFile.Truncate();
        Array.Resize(ref _wal, ControlCodec.DataStart);
        _pendingTruncate = true;
        WalCuts("during-truncate");
        After();
    }

    private void WalCuts(string point)
    {
        if (_pendingSlot is int slot)
        {
            int start = slot * ControlCodec.SlotSize;
            foreach (int cut in new[] { 0, 1, 4, 48, 64, 80, 92, 512, 2048, 4064, 4095, 4096 })
            {
                byte[] prefix = (byte[])_durableWal.Clone(), suffix = (byte[])_durableWal.Clone();
                _wal.AsSpan(start, cut).CopyTo(prefix.AsSpan(start));
                _wal.AsSpan(start + cut, ControlCodec.SlotSize - cut).CopyTo(suffix.AsSpan(start + cut));
                Capture($"{point}-prefix-{cut}", prefix, _durableDatabase);
                Capture($"{point}-suffix-{cut}", suffix, _durableDatabase);
            }
            // Noncontiguous persistence complements all byte-boundary prefix/suffix codec tests.
            foreach (int mask in new[] { 0x55, 0xAA, 0x81, 0x7E })
            {
                byte[] bytes = (byte[])_durableWal.Clone();
                for (int sector = 0; sector < 8; sector++)
                    if ((mask & (1 << sector)) != 0)
                        _wal.AsSpan(start + sector * 512, 512).CopyTo(bytes.AsSpan(start + sector * 512));
                Capture($"{point}-sectors-{mask}", bytes, _durableDatabase);
            }
        }
        else if (_pendingTruncate)
        {
            foreach (int length in new[] { ControlCodec.DataStart, ControlCodec.DataStart + 1,
                (_durableWal.Length + ControlCodec.DataStart) / 2, _durableWal.Length - 1, _durableWal.Length }
                .Where(x => x >= ControlCodec.DataStart && x <= _durableWal.Length).Distinct())
                Capture($"{point}-length-{length}", _durableWal[..length], _durableDatabase);
        }
    }

    private void BaseCuts(string point)
    {
        for (int mask = 0; mask < 8; mask++)
        {
            byte[] bytes = (byte[])_durableDatabase.Clone();
            for (int page = 0; page < 3; page++)
                if ((mask & (1 << page)) != 0)
                    _database.AsSpan(page * ControlCodec.PageSize, ControlCodec.PageSize)
                        .CopyTo(bytes.AsSpan(page * ControlCodec.PageSize));
            Capture($"{point}-pages-{mask}", _durableWal, bytes);
        }
        for (int page = 0; page < 3; page++)
        {
            byte[] bytes = (byte[])_durableDatabase.Clone();
            _database.AsSpan(page * ControlCodec.PageSize, ControlCodec.PageSize / 2)
                .CopyTo(bytes.AsSpan(page * ControlCodec.PageSize));
            Capture($"{point}-half-page-{page}", _durableWal, bytes);
        }
    }

    private void Before(string operation)
    {
        Operations.Add(operation);
        OnOperation?.Invoke(operation);
        Capture($"before-{operation}", _durableWal, _durableDatabase);
        if (Operations.Count == _throwAt && !_throwAfter) throw new IOException("Injected before " + operation);
    }

    private void After()
    {
        if (Operations.Count == _throwAt && _throwAfter) throw new IOException("Injected after " + Operations[^1]);
    }

    private void Capture(string point, byte[] wal, byte[] database) =>
        Images.Add(new($"{Operations.Count:D2}-{point}", (byte[])wal.Clone(), (byte[])database.Clone()));

    public void Dispose()
    {
        _databaseFile.Dispose();
        _walFile.Dispose();
    }
}
