namespace CSharpDB.WalPrototype.Tests;

internal sealed class PrototypeBusyException(string message) : InvalidOperationException(message);

// Fresh raw page files only. This joins the control protocol to real frame encoding,
// transactional replay, physical offsets and a conservative single-owner page store.
// It is not the production Pager, SQL engine, snapshot format or group-commit path.
internal sealed class ExperimentalWalStore : IDisposable
{
    public const long DefaultBudget = 64L * 1024 * 1024;
    private readonly object _gate = new();
    private readonly ExperimentalWalFile _wal;
    private readonly FileStream _database;
    private readonly ControlProtocol _control;
    private readonly Action<string>? _point;
    private readonly long _budget;
    private Dictionary<uint, long> _pages;
    private uint _pageCount;
    private long _publishedTail;
    private long _reserved;
    private int _readers;
    private bool _failed, _disposed;
    private CheckpointCapture? _capture;

    public long PublishedTail { get { lock (_gate) return _publishedTail; } }
    public long PhysicalLength { get { lock (_gate) return _wal.Length; } }
    public long ReservedBytes { get { lock (_gate) return _reserved; } }
    public long RetiredBytes { get { lock (_gate) return _control.Current.Floor - ControlCodec.DataStart; } }
    public ulong Epoch { get { lock (_gate) return _control.Current.Epoch; } }
    public GenerationSalt Salt { get { lock (_gate) return _control.Current.Salt; } }
    public uint PageCount { get { lock (_gate) return _pageCount; } }

    public static ExperimentalWalStore Create(string directory, IReadOnlyList<byte[]> initialPages,
        long budget = DefaultBudget, Action<string>? point = null, bool committedTail = false)
    {
        ValidateBudget(budget);
        if (initialPages.Count is < 1 or > (int)WalFrameCodec.MaximumPageCount ||
            initialPages.Any(p => p.Length != ControlCodec.PageSize)) throw new ArgumentException("Invalid initial pages.");
        string basePath = Path.Combine(directory, "database.pages"), walPath = Path.Combine(directory, "database.wal");
        if (File.Exists(basePath) || File.Exists(walPath)) throw new IOException("Fresh files are required.");
        Directory.CreateDirectory(directory);
        using (var database = new FileStream(basePath, FileMode.CreateNew, FileAccess.Write, FileShare.None))
        {
            foreach (byte[] page in initialPages) database.Write(page);
            database.Flush(true);
        }
        var record = new ControlRecord(FileIdentity.Create(), 1, 1, GenerationSalt.Create(default),
            ControlMode.Active, ControlCodec.DataStart, (uint)initialPages.Count)
        { CommittedTail = committedTail ? ControlCodec.DataStart : null };
        using (var wal = new FileStream(walPath, FileMode.CreateNew, FileAccess.Write, FileShare.None))
        {
            wal.Write(ControlCodec.Encode(record));
            wal.Flush(true);
            wal.Write(ControlCodec.Encode(record with { Sequence = 2 }));
            wal.Flush(true);
        }
        return Open(directory, budget, point);
    }

    public static ExperimentalWalStore Open(string directory, long budget = DefaultBudget, Action<string>? point = null) =>
        new(directory, budget, point);

    private ExperimentalWalStore(string directory, long budget, Action<string>? point)
    {
        ValidateBudget(budget);
        _budget = budget; _point = point;
        _wal = new(Path.Combine(directory, "database.wal"), point);
        FileStream? database = null;
        try
        {
            if (_wal.Length > budget) throw new InvalidDataException("Physical WAL exceeds configured prototype budget.");
            _control = new(_wal);
            uint basePages = _control.Current.PageCount;
            if (basePages is 0 or > WalFrameCodec.MaximumPageCount) throw new InvalidDataException("Invalid checkpoint page count.");
            database = new(Path.Combine(directory, "database.pages"), FileMode.Open, FileAccess.ReadWrite,
                FileShare.None, 1, FileOptions.RandomAccess);
            if (database.Length < checked((long)basePages * ControlCodec.PageSize))
                throw new InvalidDataException("Truncated checkpoint database.");
            // Validate the entire declared committed region before repairing or truncating.
            // With no committed-tail feature, all physical bytes must still be examined.
            var recovered = WalFrameCodec.Recover(_wal, _control.Current);
            _control.PrepareForWrites();
            if (_wal.Length > recovered.PublishedTail)
            {
                _wal.Trim(recovered.PublishedTail);
                _wal.Flush();
            }
            _database = database;
            _pages = recovered.Pages; _pageCount = recovered.PageCount; _publishedTail = recovered.PublishedTail;
        }
        catch { database?.Dispose(); _wal.Dispose(); throw; }
    }

    public void Commit(IReadOnlyList<PageWrite> writes, uint pageCount) => Commit(writes, pageCount, CancellationToken.None);

    public void Commit(IReadOnlyList<PageWrite> writes, uint pageCount, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            RequireUsable();
            cancellationToken.ThrowIfCancellationRequested();
            if (writes.Count == 0 || pageCount < _pageCount || pageCount > WalFrameCodec.MaximumPageCount ||
                writes.Any(p => p.PageId >= pageCount || p.Data.Length != ControlCodec.PageSize))
                throw new ArgumentException("Invalid batch or unsupported page-count shrink.");
            for (uint page = _pageCount; page < pageCount; page++)
                if (!writes.Any(p => p.PageId == page)) throw new ArgumentException("Growth requires every new page image.");
            long bytes = checked((long)writes.Count * ControlCodec.FrameSize);
            if (bytes > _budget - ControlCodec.DataStart) throw new InvalidOperationException("Batch exceeds the post-reset WAL capacity.");
            if (checked(_publishedTail + _reserved + bytes) > _budget) ResetCore();
            _reserved = bytes;
            long start = _publishedTail;
            bool publicationStarted = false;
            try
            {
                Dictionary<uint, long> committed = [];
                byte[]? lastFrame = null;
                for (int i = 0; i < writes.Count; i++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    long offset = checked(start + (long)i * ControlCodec.FrameSize);
                    byte[] frame = WalFrameCodec.Encode(writes[i], 0, _control.Current.Salt);
                    lastFrame = frame;
                    // Separate writes expose deterministic process-kill points inside a frame.
                    _wal.WriteAt(offset, frame.AsSpan(0, 24), $"frame-{i}-header");
                    _wal.WriteAt(offset + 24, frame.AsSpan(24, 2048), $"frame-{i}-half");
                    _wal.WriteAt(offset + 2072, frame.AsSpan(2072), $"frame-{i}");
                    committed[writes[i].PageId] = offset;
                }
                cancellationToken.ThrowIfCancellationRequested();
                publicationStarted = true;
                byte[] marker = WalFrameCodec.CommitHeader(lastFrame!, pageCount);
                long markerOffset = checked(start + (writes.Count - 1L) * ControlCodec.FrameSize);
                _wal.WriteAt(markerOffset, marker.AsSpan(0, 8), "commit-marker-half");
                _wal.WriteAt(markerOffset + 8, marker.AsSpan(8, 16), "commit-marker");
                if (_control.Current.CommittedTail.HasValue)
                    _control.PublishCommittedTail(checked(start + bytes));
                else
                    _wal.Flush();
                foreach (var entry in committed) _pages[entry.Key] = entry.Value;
                _pageCount = pageCount;
                _publishedTail = checked(start + bytes);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested && !publicationStarted)
            {
                // No commit marker was published by the cancellation checks. Restore the
                // previous physical tail before allowing another batch to reuse its offsets.
                try { _wal.Trim(start); _wal.Flush(); }
                catch { _failed = true; throw; }
                throw;
            }
            catch { _failed = true; throw; }
            finally { _reserved = 0; }
        }
    }

    public byte[] Read(uint pageId)
    {
        lock (_gate) { RequireUsable(); return ReadCore(pageId, _pageCount, _pages, _control.Current.Salt); }
    }

    public ReaderSnapshot BeginRead()
    {
        lock (_gate)
        {
            RequireUsable();
            var snapshot = new ReaderSnapshot(this, _pageCount, new(_pages), _control.Current.Salt);
            _readers++;
            return snapshot;
        }
    }

    public CheckpointCapture CaptureCheckpoint()
    {
        lock (_gate)
        {
            RequireUsable();
            if (_capture is not null) throw new PrototypeBusyException("A checkpoint is already pending.");
            return _capture = new(this, _control.Current.Epoch, _publishedTail, _pageCount, new(_pages));
        }
    }

    public void CopyCheckpoint(CheckpointCapture capture)
    {
        lock (_gate)
        {
            ValidateCapture(capture);
            RequireNoReaders();
            if (capture.Copied) throw new InvalidOperationException("Checkpoint already copied.");
            Io(() => { CopyPages(capture.Pages, capture.PageCount); capture.Copied = true; });
        }
    }

    public void RetireCheckpoint(CheckpointCapture capture)
    {
        lock (_gate)
        {
            ValidateCapture(capture);
            RequireNoReaders();
            if (!capture.Copied) throw new InvalidOperationException("Checkpoint must be flushed first.");
            Io(() =>
            {
                // CopyCheckpoint already flushed this exact immutable capture under the gate.
                _control.Retire(capture.Floor, capture.PageCount, () => { });
                foreach (uint id in _pages.Where(p => p.Value < capture.Floor).Select(p => p.Key).ToArray()) _pages.Remove(id);
                _capture = null;
                _point?.Invoke("floor-published");
            });
        }
    }

    public void Reset()
    {
        lock (_gate) { RequireUsable(); ResetCore(); }
    }

    private void ResetCore()
    {
        RequireNoReaders();
        if (_capture is not null || _reserved != 0) throw new PrototypeBusyException("Checkpoint or append is pending.");
        Io(() =>
        {
            _control.Reset(_pageCount, true, () => CopyPages(_pages, _pageCount));
            _pages.Clear();
            _publishedTail = ControlCodec.DataStart;
            _point?.Invoke("reset-complete");
        });
    }

    private void CopyPages(Dictionary<uint, long> pages, uint count)
    {
        foreach (var pair in pages.OrderBy(p => p.Key))
        {
            byte[] data = ReadCore(pair.Key, count, pages, _control.Current.Salt);
            _database.Position = checked((long)pair.Key * ControlCodec.PageSize);
            _database.Write(data);
            _point?.Invoke($"base-page-{pair.Key}-written");
        }
        // The prototype supports growth only, so an interrupted copy never removes base pages.
        if (_database.Length < checked((long)count * ControlCodec.PageSize))
            throw new InvalidDataException("Checkpoint lacks a required page.");
        _database.Flush(true);
        _point?.Invoke("base-flushed");
    }

    private byte[] ReadCore(uint pageId, uint count, Dictionary<uint, long> pages, GenerationSalt salt)
    {
        if (pageId >= count) throw new ArgumentOutOfRangeException(nameof(pageId));
        if (pages.TryGetValue(pageId, out long offset))
        {
            byte[] frame = _wal.ReadAt(offset, ControlCodec.FrameSize);
            if (WalFrameCodec.Decode(frame, salt).PageId != pageId) throw new InvalidDataException("Frame map page mismatch.");
            return frame[WalFrameCodec.HeaderSize..];
        }
        byte[] result = new byte[ControlCodec.PageSize];
        _database.Position = checked((long)pageId * ControlCodec.PageSize);
        _database.ReadExactly(result);
        return result;
    }

    private void ValidateCapture(CheckpointCapture capture)
    {
        RequireUsable();
        if (!ReferenceEquals(capture, _capture) || !ReferenceEquals(capture.Owner, this) || capture.Epoch != _control.Current.Epoch ||
            capture.Floor < _control.Current.Floor || capture.Floor > _publishedTail)
            throw new InvalidOperationException("Stale or foreign checkpoint capture.");
    }

    private void RequireNoReaders()
    {
        if (_readers != 0) throw new PrototypeBusyException("Reader snapshots must drain before checkpoint copy or reset.");
    }

    private void RequireUsable()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_failed) throw new InvalidOperationException("Reopen after failed storage I/O.");
    }

    private void Io(Action action)
    {
        try { action(); }
        catch { _failed = true; throw; }
        finally { _reserved = 0; }
    }

    private static void ValidateBudget(long budget)
    {
        if (budget < ControlCodec.DataStart + ControlCodec.FrameSize) throw new ArgumentOutOfRangeException(nameof(budget));
    }

    public void Dispose()
    {
        lock (_gate)
        {
            if (_disposed) return;
            RequireNoReaders();
            _disposed = true;
            _database.Dispose(); _wal.Dispose();
        }
    }

    internal sealed class CheckpointCapture(ExperimentalWalStore owner, ulong epoch, long floor, uint count, Dictionary<uint, long> pages)
    {
        internal ExperimentalWalStore Owner { get; } = owner;
        internal ulong Epoch { get; } = epoch;
        internal long Floor { get; } = floor;
        internal uint PageCount { get; } = count;
        internal Dictionary<uint, long> Pages { get; } = pages;
        internal bool Copied { get; set; }
    }

    internal sealed class ReaderSnapshot(ExperimentalWalStore owner, uint count, Dictionary<uint, long> pages, GenerationSalt salt) : IDisposable
    {
        private bool _closed;
        public byte[] Read(uint id)
        {
            lock (owner._gate)
            {
                ObjectDisposedException.ThrowIf(_closed, this);
                owner.RequireUsable();
                return owner.ReadCore(id, count, pages, salt);
            }
        }
        public void Dispose()
        {
            lock (owner._gate)
            {
                if (_closed) return;
                _closed = true; owner._readers--;
            }
        }
    }
}
