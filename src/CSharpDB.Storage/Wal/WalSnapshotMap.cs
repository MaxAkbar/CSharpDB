using System.Numerics;
using System.Runtime.CompilerServices;

namespace CSharpDB.Storage.Wal;

/// <summary>
/// An immutable directory of independently owned snapshot segments. Only unpublished
/// preparations may be filled; published segments can be shared by later snapshots.
/// </summary>
internal sealed class WalSnapshotMap(int segmentCount = WalSnapshotMap.SegmentCount)
{
    internal const int SegmentCount = 64;
    internal const int CompactPageLimit = 64;
    internal static readonly WalSnapshotMap Empty = new(1);

    internal readonly WalSnapshotSegment?[] Segments = new WalSnapshotSegment?[segmentCount];
    internal int Count { get; private set; }
    internal long MinimumWalOffset { get; private set; } = long.MaxValue;

    internal static int GetSegmentIndex(uint pageId) => (int)(pageId & (SegmentCount - 1));

    internal bool TryGetValue(uint pageId, out long offset)
    {
        var segment = Segments[(int)(pageId & (uint)(Segments.Length - 1))];
        if (segment is not null)
            return segment.TryGetValue(pageId, out offset);
        offset = 0;
        return false;
    }

    // The directory and all newly filled segments are still exclusively owned.
    internal void Complete()
    {
        int count = 0;
        long minimum = long.MaxValue;
        foreach (var segment in Segments)
        {
            if (segment is null)
                continue;
            count += segment.Count;
            minimum = Math.Min(minimum, segment.MinimumWalOffset);
        }
        Count = count;
        MinimumWalOffset = minimum;
    }
}

/// <summary>
/// A fixed-capacity table filled once from unique WAL page IDs. The source map
/// already guarantees uniqueness, so insertion needs no duplicate lookup, resize,
/// free list or stored hash code. All arrays are allocated before admission.
/// </summary>
internal sealed class WalSnapshotSegment
{
    private readonly int[] _buckets;
    private readonly Entry[] _entries;
    private readonly int _bucketShift;

    internal int Capacity => _entries.Length;
    internal int Count { get; private set; }
    internal long MinimumWalOffset { get; private set; } = long.MaxValue;

    internal WalSnapshotSegment(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);
        // At least two buckets avoids a shift by 32 (masked to zero by C#).
        uint bucketCount = BitOperations.RoundUpToPowerOf2((uint)Math.Max(2, capacity));
        _buckets = new int[checked((int)bucketCount)];
        _entries = new Entry[capacity];
        _bucketShift = 32 - BitOperations.Log2(bucketCount);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int GetBucket(uint pageId)
        // Use the high product bits: low page-ID bits are equal within a segment.
        => (int)(unchecked(pageId * 2654435769u) >> _bucketShift);

    // Only an exclusively owned, empty preparation may be filled. The capture
    // checks every table's capacity before inserting any source entries.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void AddUnique(uint pageId, long offset)
    {
        int bucket = GetBucket(pageId);
        int index = Count;
        _entries[index] = new Entry { PageId = pageId, Offset = offset, Next = _buckets[bucket] };
        _buckets[bucket] = index + 1;
        Count = index + 1;
        if (offset < MinimumWalOffset)
            MinimumWalOffset = offset;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool TryGetValue(uint pageId, out long offset)
    {
        // Bucket heads and links are one-based; zero means absent. No page ID or
        // offset is reserved as a sentinel, including page zero and uint.MaxValue.
        for (int link = _buckets[GetBucket(pageId)]; link != 0;)
        {
            ref readonly Entry entry = ref _entries[link - 1];
            if (entry.PageId == pageId)
            {
                offset = entry.Offset;
                return true;
            }
            link = entry.Next;
        }
        offset = 0;
        return false;
    }

    private struct Entry
    {
        internal uint PageId;
        internal int Next;
        internal long Offset;
    }
}

/// <summary>
/// Private scratch storage for one reader. Capacity grows only outside checkpoint
/// and WAL protection. A successful capture consumes it; it must never be reused.
/// </summary>
internal sealed class WalSnapshotPreparation(int segmentCount)
{
    internal readonly WalSnapshotMap Map = new(segmentCount);
    internal readonly WalSnapshotSegment?[] Buffers = new WalSnapshotSegment?[segmentCount];
    internal bool Published;
    internal WalSnapshotKeys? Keys;

    internal void EnsureCapacity(ReadOnlySpan<int> requiredCapacities, int keyCapacity = 0)
    {
        if (Published)
            throw new InvalidOperationException("The snapshot preparation was already published.");

        if (keyCapacity != 0 && (Keys is null || Keys.PageIds.Length < keyCapacity))
            Keys = new WalSnapshotKeys(keyCapacity);

        for (int i = 0; i < Buffers.Length; i++)
        {
            int capacity = requiredCapacities[i];
            if (capacity == 0)
                continue;
            if (Buffers[i] is { Count: > 0 })
                throw new InvalidOperationException("Snapshot buffers must be empty and unpublished.");
            if (Buffers[i] is null || Buffers[i]!.Capacity < capacity)
                Buffers[i] = new WalSnapshotSegment(capacity);
        }
    }
}

/// <summary>
/// A flat directory of keys grouped by segment. Allocated outside admission gates,
/// then initialized under the WAL gate and cached until the set of keys changes.
/// Snapshots own their page maps and never retain this mutable-index helper.
/// </summary>
internal sealed class WalSnapshotKeys(int capacity)
{
    internal readonly uint[] PageIds = new uint[capacity];
    internal readonly int[] Offsets = new int[WalSnapshotMap.SegmentCount + 1];
    internal int Count { get; private set; }

    internal void Initialize(Dictionary<uint, long> pages)
    {
        Array.Clear(Offsets);
        foreach (var entry in pages)
            Offsets[WalSnapshotMap.GetSegmentIndex(entry.Key) + 1]++;
        for (int i = 1; i < Offsets.Length; i++)
            Offsets[i] += Offsets[i - 1];
        Span<int> positions = stackalloc int[WalSnapshotMap.SegmentCount];
        Offsets.AsSpan(0, positions.Length).CopyTo(positions);
        foreach (var entry in pages)
            PageIds[positions[WalSnapshotMap.GetSegmentIndex(entry.Key)]++] = entry.Key;
        Count = pages.Count;
    }
}
