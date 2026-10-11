using System.Buffers.Binary;

namespace CSharpDB.WalPrototype.Tests;

internal sealed class ExperimentalWalFile : IControlFile, IDisposable
{
    private readonly FileStream _stream;
    private readonly Action<string>? _point;
    private string _lastWrite = "recovery";
    public long Length => _stream.Length;

    public ExperimentalWalFile(string path, Action<string>? point)
    {
        _stream = new(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None, 1, FileOptions.RandomAccess);
        _point = point;
    }

    public byte[] ReadHeader() => ReadAt(0, (int)Math.Min(Length, ControlCodec.DataStart));

    public byte[] ReadAt(long offset, int count)
    {
        byte[] bytes = new byte[count];
        _stream.Position = offset;
        _stream.ReadExactly(bytes);
        return bytes;
    }

    public void WriteAt(long offset, ReadOnlySpan<byte> bytes, string operation)
    {
        _stream.Position = offset;
        _stream.Write(bytes);
        _lastWrite = operation;
        _point?.Invoke(operation + "-written");
    }

    public void WriteSlot(int slot, byte[] bytes)
    {
        if (slot is < 0 or > 1 || bytes.Length != ControlCodec.SlotSize) throw new ArgumentOutOfRangeException(nameof(slot));
        var mode = (ControlMode)BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(72));
        WriteAt(slot * ControlCodec.SlotSize, bytes, $"control-{mode.ToString().ToLowerInvariant()}-{slot}");
    }

    public void Flush()
    {
        _stream.Flush(true);
        _point?.Invoke(_lastWrite + "-flushed");
    }

    public void Trim(long length)
    {
        _stream.SetLength(length);
        _lastWrite = "truncate";
        _point?.Invoke("truncate-written");
    }

    public void Truncate() => Trim(ControlCodec.DataStart);
    public void Dispose() => _stream.Dispose();
}
