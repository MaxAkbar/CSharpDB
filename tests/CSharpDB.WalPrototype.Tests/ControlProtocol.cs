namespace CSharpDB.WalPrototype.Tests;

internal interface IControlFile
{
    long Length { get; }
    byte[] ReadHeader();
    void WriteSlot(int slot, byte[] bytes);
    void Flush();
    void Truncate();
}

// Single-owner prototype. There is no engine, transaction, reader or frame parser here.
internal sealed class ControlProtocol
{
    private readonly IControlFile _file;
    private ControlSelection _selection;
    private bool _failed;
    public bool WritersAdmitted { get; private set; }
    public ControlRecord Current => _selection.Current;

    public ControlProtocol(IControlFile file)
    {
        _file = file;
        _selection = ControlCodec.Select(file.ReadHeader(), file.Length);
    }

    public void PrepareForWrites() => Run(() =>
    {
        int repairWrites = _selection.Mirrored ? 0 : 1;
        ReserveSequences(repairWrites + (Current.Mode == ControlMode.Empty ? 2 : 0));
        if (_selection.Mirrored)
        {
            // A process-only crash may leave readable, unflushed control bytes in the OS
            // cache. Establish their durability before truncation or writer admission.
            _file.Flush();
        }
        else
            RepairPeer(); // Its flush also makes the selected record durable.
        if (Current.Mode == ControlMode.Empty)
        {
            _file.Truncate();
            _file.Flush();
            PublishBoth(Current with { Mode = ControlMode.Active });
        }
        WritersAdmitted = true;
    });

    public void Retire(long floor, uint pageCount, Action copyAndFlushBase) => Run(() =>
    {
        RequireAdmitted();
        ReserveSequences(2);
        if (floor < Current.Floor) throw new InvalidDataException("Cannot move floor backwards.");
        var next = Current with { Floor = floor, PageCount = pageCount };
        ControlCodec.Validate(next, _file.Length);
        WritersAdmitted = false;
        copyAndFlushBase();
        PublishBoth(next);
        WritersAdmitted = true;
    });

    public void PublishCommittedTail(long tail) => Run(() =>
    {
        RequireAdmitted();
        ReserveSequences(2);
        if (Current.CommittedTail is not long previous || tail <= previous)
            throw new InvalidOperationException("A committed-tail feature and advancing boundary are required.");
        var next = Current with { CommittedTail = tail };
        ControlCodec.Validate(next, _file.Length);
        WritersAdmitted = false;
        // Establish the complete batch before publishing any record that calls it committed.
        _file.Flush();
        PublishBoth(next);
        WritersAdmitted = true;
    });

    public void Reset(uint pageCount, bool quiescent, Action copyAndFlushBase,
        Func<GenerationSalt, GenerationSalt>? newSalt = null) => Run(() =>
    {
        RequireAdmitted();
        if (!quiescent) throw new InvalidOperationException("Reset requires no readers, transaction or pending commit.");
        ReserveSequences(4);
        ulong epoch = checked(Current.Epoch + 1);
        // A terminal epoch is readable, but cannot admit writers that would need another reset.
        if (epoch == ulong.MaxValue) throw new InvalidOperationException("Epoch exhausted.");
        var salt = (newSalt ?? GenerationSalt.Create)(Current.Salt);
        if (salt == Current.Salt || salt.First == 0 || salt.Second == 0)
            throw new InvalidDataException("Reset needs fresh nonzero salts.");
        var next = Current with { Epoch = epoch, Salt = salt, Mode = ControlMode.Empty,
            Floor = ControlCodec.DataStart, PageCount = pageCount,
            CommittedTail = Current.CommittedTail.HasValue ? ControlCodec.DataStart : null };
        WritersAdmitted = false;
        copyAndFlushBase();
        PublishBoth(next);
        _file.Truncate();
        _file.Flush();
        PublishBoth(Current with { Mode = ControlMode.Active });
        WritersAdmitted = true;
    });

    private void RepairPeer()
    {
        if (!_selection.Mirrored)
            Publish(1 - _selection.CurrentSlot, Current);
    }

    private void PublishBoth(ControlRecord record)
    {
        Publish(0, record);
        Publish(1, record);
    }

    private void Publish(int slot, ControlRecord record)
    {
        record = record with { Sequence = checked(Current.Sequence + 1) };
        _file.WriteSlot(slot, ControlCodec.Encode(record));
        _file.Flush();
        _selection = new(record, slot, slot == 0 ? record : _selection.First,
            slot == 1 ? record : _selection.Second);
    }

    private void ReserveSequences(int count)
    {
        if (Current.Epoch == ulong.MaxValue || Current.Sequence >= ulong.MaxValue - (ulong)count)
            throw new InvalidOperationException("Control counters exhausted.");
    }

    private void RequireAdmitted()
    {
        if (!WritersAdmitted || Current.Mode != ControlMode.Active)
            throw new InvalidOperationException("Writer admission is closed.");
    }

    private void Run(Action action)
    {
        if (_failed) throw new InvalidOperationException("Reopen after failed control I/O or validation.");
        try { action(); }
        catch
        {
            WritersAdmitted = false;
            _failed = true;
            throw;
        }
    }
}

internal sealed class FileControlStore : IControlFile, IDisposable
{
    private readonly FileStream _stream;
    public long Length => _stream.Length;

    public FileControlStore(string path) => _stream = new(path, FileMode.Open, FileAccess.ReadWrite,
        FileShare.None, bufferSize: 1, FileOptions.RandomAccess);

    public byte[] ReadHeader()
    {
        byte[] header = new byte[(int)Math.Min(Length, ControlCodec.DataStart)];
        _stream.Position = 0;
        _stream.ReadExactly(header);
        return header;
    }

    public void WriteSlot(int slot, byte[] bytes)
    {
        if (slot is < 0 or > 1 || bytes.Length != ControlCodec.SlotSize)
            throw new ArgumentOutOfRangeException(nameof(slot));
        _stream.Position = slot * ControlCodec.SlotSize;
        _stream.Write(bytes);
    }

    public void Flush() => _stream.Flush(flushToDisk: true);
    public void Truncate() => _stream.SetLength(ControlCodec.DataStart);
    public void Dispose() => _stream.Dispose();
}
