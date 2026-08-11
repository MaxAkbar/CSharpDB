using CSharpDB.Storage.Caching;
using CSharpDB.Storage.Device;

namespace CSharpDB.Storage.Diagnostics;

internal enum StorageRuntimeDetailAvailabilityRaw
{
    Unavailable = 0,
    Available,
    Unsupported,
    NotApplicable,
}

internal readonly record struct LogicalPageReadRuntimeRawSnapshot(
    long CacheHits,
    long CacheMisses);

internal readonly record struct StorageCacheRuntimeRawSnapshot(
    long SharedResidentPages,
    long? SharedCapacityPages,
    long WalResidentPages,
    long WalCapacityPages);

internal readonly record struct StorageDeviceIoRuntimeRawSnapshot(
    long ReadCount,
    long BytesRead,
    long WriteCount,
    long BytesWritten,
    long FlushCount,
    long ResizeCount,
    long SequentialReadCount,
    long SequentialBytesRead,
    long MemoryMappedPageExposureCount,
    long MemoryMappedBytesExposed);

internal readonly record struct StorageIoRuntimeRawSnapshot(
    LogicalPageReadRuntimeRawSnapshot LogicalReads,
    StorageRuntimeDetailAvailabilityRaw CacheAvailability,
    StorageCacheRuntimeRawSnapshot Cache,
    StorageRuntimeDetailAvailabilityRaw PhysicalIoAvailability,
    StorageDeviceIoRuntimeRawSnapshot PhysicalIo);

/// <summary>
/// Internal opt-in implemented only by built-in page caches. The required
/// public <see cref="IPageCache"/> contract deliberately remains unchanged.
/// Callers sample these values while holding the owning page-buffer state gate.
/// </summary>
internal interface IPageCacheRuntimeDiagnosticsProvider
{
    long RuntimeResidentPageCount { get; }

    long? RuntimeCapacityPageCount { get; }
}

/// <summary>
/// Internal opt-in implemented only by the built-in file device. Diagnostics
/// are enabled lazily so the ordinary device path does not allocate counters.
/// </summary>
internal interface IStorageDeviceIoRuntimeDiagnosticsProvider
{
    StorageDeviceIoRuntimeCounters EnableRuntimeDiagnostics();
}

internal static class StorageRuntimeCounterMath
{
    internal static long SaturatingAdd(long left, long right)
    {
        if (left < 0 || right < 0)
            return long.MaxValue;

        return left > long.MaxValue - right
            ? long.MaxValue
            : left + right;
    }

    internal static long SaturatingMultiply(long left, long right)
    {
        if (left < 0 || right < 0)
            return long.MaxValue;
        if (left == 0 || right == 0)
            return 0;

        return left > long.MaxValue / right
            ? long.MaxValue
            : left * right;
    }

    internal static void SaturatingIncrement(ref long location)
        => SaturatingAdd(ref location, 1);

    internal static void SaturatingAdd(ref long location, long value)
    {
        if (value <= 0)
            return;

        while (true)
        {
            long current = Volatile.Read(ref location);
            if (current == long.MaxValue)
                return;

            long updated = SaturatingAdd(current, value);
            if (Interlocked.CompareExchange(ref location, updated, current) == current)
                return;
        }
    }
}

internal sealed class LogicalPageReadRuntimeCounters
{
    private long _cacheHits;
    private long _cacheMisses;

    internal void RecordCacheHit()
        => StorageRuntimeCounterMath.SaturatingIncrement(ref _cacheHits);

    internal void RecordCacheMiss()
        => StorageRuntimeCounterMath.SaturatingIncrement(ref _cacheMisses);

    internal LogicalPageReadRuntimeRawSnapshot Capture()
        => new(
            Volatile.Read(ref _cacheHits),
            Volatile.Read(ref _cacheMisses));
}

/// <summary>
/// Linearizable cumulative physical-I/O counter. Once sealed, later records
/// are ignored and every capture observes the same terminal sample.
/// </summary>
internal sealed class StorageDeviceIoRuntimeCounters
{
    private readonly object _gate = new();
    private long _readCount;
    private long _bytesRead;
    private long _writeCount;
    private long _bytesWritten;
    private long _flushCount;
    private long _resizeCount;
    private long _sequentialReadCount;
    private long _sequentialBytesRead;
    private long _memoryMappedPageExposureCount;
    private long _memoryMappedBytesExposed;
    private bool _sealed;

    internal void RecordRead(long bytesRead, bool sequential)
    {
        lock (_gate)
        {
            if (_sealed)
                return;

            // Publish the total before its sequential subset.
            _readCount = StorageRuntimeCounterMath.SaturatingAdd(_readCount, 1);
            _bytesRead = StorageRuntimeCounterMath.SaturatingAdd(
                _bytesRead,
                Math.Max(0, bytesRead));
            if (!sequential)
                return;

            _sequentialReadCount = StorageRuntimeCounterMath.SaturatingAdd(
                _sequentialReadCount,
                1);
            _sequentialBytesRead = StorageRuntimeCounterMath.SaturatingAdd(
                _sequentialBytesRead,
                Math.Max(0, bytesRead));
        }
    }

    internal void RecordWrite(long bytesWritten)
    {
        lock (_gate)
        {
            if (_sealed)
                return;

            _writeCount = StorageRuntimeCounterMath.SaturatingAdd(_writeCount, 1);
            _bytesWritten = StorageRuntimeCounterMath.SaturatingAdd(
                _bytesWritten,
                Math.Max(0, bytesWritten));
        }
    }

    internal void RecordFlush()
    {
        lock (_gate)
        {
            if (!_sealed)
                _flushCount = StorageRuntimeCounterMath.SaturatingAdd(_flushCount, 1);
        }
    }

    internal void RecordResize()
    {
        lock (_gate)
        {
            if (!_sealed)
                _resizeCount = StorageRuntimeCounterMath.SaturatingAdd(_resizeCount, 1);
        }
    }

    internal void RecordMemoryMappedPageExposure(long bytesExposed)
    {
        lock (_gate)
        {
            if (_sealed)
                return;

            _memoryMappedPageExposureCount = StorageRuntimeCounterMath.SaturatingAdd(
                _memoryMappedPageExposureCount,
                1);
            _memoryMappedBytesExposed = StorageRuntimeCounterMath.SaturatingAdd(
                _memoryMappedBytesExposed,
                Math.Max(0, bytesExposed));
        }
    }

    internal StorageDeviceIoRuntimeRawSnapshot Capture()
    {
        lock (_gate)
            return CaptureLocked();
    }

    internal bool TrySeal(out StorageDeviceIoRuntimeRawSnapshot snapshot)
    {
        lock (_gate)
        {
            if (_sealed)
            {
                snapshot = default;
                return false;
            }

            _sealed = true;
            snapshot = CaptureLocked();
            return true;
        }
    }

    private StorageDeviceIoRuntimeRawSnapshot CaptureLocked()
    {
        // Read the subsets before the totals. RecordRead performs the inverse
        // publication order, so even a future lock-free implementation keeps
        // the subset invariants stable under concurrent capture.
        long sequentialReadCount = _sequentialReadCount;
        long sequentialBytesRead = _sequentialBytesRead;
        long readCount = _readCount;
        long bytesRead = _bytesRead;

        return new StorageDeviceIoRuntimeRawSnapshot(
            readCount,
            bytesRead,
            _writeCount,
            _bytesWritten,
            _flushCount,
            _resizeCount,
            sequentialReadCount,
            sequentialBytesRead,
            _memoryMappedPageExposureCount,
            _memoryMappedBytesExposed);
    }
}

/// <summary>
/// Constant-time aggregate of the live page-buffer cache gauges in a pager
/// family. Each manager publishes its current O(1) sample after a mutation.
/// </summary>
internal sealed class StorageCacheRuntimeDiagnostics
{
    private readonly object _gate = new();
    private long _registrationCount;
    private long _unsupportedRegistrationCount;
    private long _unavailableRegistrationCount;
    private long _unboundedSharedCapacityRegistrationCount;
    private long _sharedResidentPages;
    private long _sharedCapacityPages;
    private long _walResidentPages;
    private long _walCapacityPages;

    internal Lease? TryRegister(
        IPageCache cache,
        int walResidentPages,
        int walCapacityPages)
    {
        ArgumentNullException.ThrowIfNull(cache);

        try
        {
            if (cache is not IPageCacheRuntimeDiagnosticsProvider provider)
                return RegisterUnsupported();

            return RegisterSupported(
                provider.RuntimeResidentPageCount,
                provider.RuntimeCapacityPageCount,
                walResidentPages,
                walCapacityPages);
        }
        catch
        {
            return RegisterUnavailable();
        }
    }

    internal StorageRuntimeDetailAvailabilityRaw Capture(
        out StorageCacheRuntimeRawSnapshot snapshot)
    {
        lock (_gate)
        {
            if (_registrationCount == 0)
            {
                snapshot = default;
                return StorageRuntimeDetailAvailabilityRaw.Unavailable;
            }

            if (_unsupportedRegistrationCount != 0)
            {
                snapshot = default;
                return StorageRuntimeDetailAvailabilityRaw.Unsupported;
            }

            if (_unavailableRegistrationCount != 0)
            {
                snapshot = default;
                return StorageRuntimeDetailAvailabilityRaw.Unavailable;
            }

            snapshot = new StorageCacheRuntimeRawSnapshot(
                _sharedResidentPages,
                _unboundedSharedCapacityRegistrationCount == 0
                    ? _sharedCapacityPages
                    : null,
                _walResidentPages,
                _walCapacityPages);
            return StorageRuntimeDetailAvailabilityRaw.Available;
        }
    }

    private Lease RegisterUnsupported()
    {
        lock (_gate)
        {
            _registrationCount = StorageRuntimeCounterMath.SaturatingAdd(
                _registrationCount,
                1);
            _unsupportedRegistrationCount = StorageRuntimeCounterMath.SaturatingAdd(
                _unsupportedRegistrationCount,
                1);
            return new Lease(this, supported: false, unavailable: false, default);
        }
    }

    private Lease RegisterUnavailable()
    {
        lock (_gate)
        {
            _registrationCount = StorageRuntimeCounterMath.SaturatingAdd(
                _registrationCount,
                1);
            _unavailableRegistrationCount = StorageRuntimeCounterMath.SaturatingAdd(
                _unavailableRegistrationCount,
                1);
            return new Lease(this, supported: true, unavailable: true, default);
        }
    }

    private Lease RegisterSupported(
        long sharedResidentPages,
        long? sharedCapacityPages,
        long walResidentPages,
        long walCapacityPages)
    {
        var sample = new StorageCacheRuntimeRawSnapshot(
            sharedResidentPages,
            sharedCapacityPages,
            walResidentPages,
            walCapacityPages);

        lock (_gate)
        {
            _registrationCount = StorageRuntimeCounterMath.SaturatingAdd(
                _registrationCount,
                1);
            if (!IsValid(sample))
            {
                _unavailableRegistrationCount =
                    StorageRuntimeCounterMath.SaturatingAdd(
                        _unavailableRegistrationCount,
                        1);
                return new Lease(
                    this,
                    supported: true,
                    unavailable: true,
                    sample);
            }

            var lease = new Lease(this, supported: true, unavailable: false, sample);
            AddSampleLocked(sample);
            return lease;
        }
    }

    private static bool IsValid(in StorageCacheRuntimeRawSnapshot sample)
        => sample.SharedResidentPages >= 0 &&
            sample.SharedCapacityPages is not < 0 &&
            (sample.SharedCapacityPages is not { } sharedCapacity ||
                sample.SharedResidentPages <= sharedCapacity) &&
            sample.WalResidentPages >= 0 &&
            sample.WalCapacityPages >= 0 &&
            sample.WalResidentPages <= sample.WalCapacityPages;

    private void AddSampleLocked(in StorageCacheRuntimeRawSnapshot sample)
    {
        _sharedResidentPages = StorageRuntimeCounterMath.SaturatingAdd(
            _sharedResidentPages,
            sample.SharedResidentPages);
        if (sample.SharedCapacityPages is { } sharedCapacityPages)
        {
            _sharedCapacityPages = StorageRuntimeCounterMath.SaturatingAdd(
                _sharedCapacityPages,
                sharedCapacityPages);
        }
        else
        {
            _unboundedSharedCapacityRegistrationCount =
                StorageRuntimeCounterMath.SaturatingAdd(
                    _unboundedSharedCapacityRegistrationCount,
                    1);
        }

        _walResidentPages = StorageRuntimeCounterMath.SaturatingAdd(
            _walResidentPages,
            sample.WalResidentPages);
        _walCapacityPages = StorageRuntimeCounterMath.SaturatingAdd(
            _walCapacityPages,
            sample.WalCapacityPages);
    }

    private void RemoveSampleLocked(in StorageCacheRuntimeRawSnapshot sample)
    {
        _sharedResidentPages = Math.Max(
            0,
            _sharedResidentPages - sample.SharedResidentPages);
        if (sample.SharedCapacityPages is { } sharedCapacityPages)
        {
            _sharedCapacityPages = Math.Max(
                0,
                _sharedCapacityPages - sharedCapacityPages);
        }
        else
        {
            _unboundedSharedCapacityRegistrationCount = Math.Max(
                0,
                _unboundedSharedCapacityRegistrationCount - 1);
        }

        _walResidentPages = Math.Max(
            0,
            _walResidentPages - sample.WalResidentPages);
        _walCapacityPages = Math.Max(
            0,
            _walCapacityPages - sample.WalCapacityPages);
    }

    internal sealed class Lease : IDisposable
    {
        private StorageCacheRuntimeDiagnostics? _owner;
        private readonly bool _supported;
        private bool _unavailable;
        private StorageCacheRuntimeRawSnapshot _sample;

        internal Lease(
            StorageCacheRuntimeDiagnostics owner,
            bool supported,
            bool unavailable,
            StorageCacheRuntimeRawSnapshot sample)
        {
            _owner = owner;
            _supported = supported;
            _unavailable = unavailable;
            _sample = sample;
        }

        internal void TryPublish(
            long sharedResidentPages,
            long? sharedCapacityPages,
            long walResidentPages,
            long walCapacityPages)
        {
            StorageCacheRuntimeDiagnostics? owner = Volatile.Read(ref _owner);
            if (owner is null || !_supported)
                return;

            var next = new StorageCacheRuntimeRawSnapshot(
                sharedResidentPages,
                sharedCapacityPages,
                walResidentPages,
                walCapacityPages);

            lock (owner._gate)
            {
                if (!ReferenceEquals(_owner, owner))
                    return;

                if (!IsValid(next))
                {
                    SetUnavailableLocked();
                    return;
                }

                if (_unavailable)
                {
                    _unavailable = false;
                    owner._unavailableRegistrationCount = Math.Max(
                        0,
                        owner._unavailableRegistrationCount - 1);
                }
                else
                {
                    owner.RemoveSampleLocked(_sample);
                }

                _sample = next;
                owner.AddSampleLocked(next);
            }
        }

        internal void TryMarkUnavailable()
        {
            StorageCacheRuntimeDiagnostics? owner = Volatile.Read(ref _owner);
            if (owner is null || !_supported)
                return;

            lock (owner._gate)
            {
                if (ReferenceEquals(_owner, owner))
                    SetUnavailableLocked();
            }
        }

        internal void SetUnavailableLocked()
        {
            StorageCacheRuntimeDiagnostics owner = _owner!;
            if (_unavailable)
                return;

            owner.RemoveSampleLocked(_sample);
            _unavailable = true;
            owner._unavailableRegistrationCount =
                StorageRuntimeCounterMath.SaturatingAdd(
                    owner._unavailableRegistrationCount,
                    1);
        }

        public void Dispose()
        {
            StorageCacheRuntimeDiagnostics? owner = Volatile.Read(ref _owner);
            if (owner is null)
                return;

            lock (owner._gate)
            {
                if (!ReferenceEquals(_owner, owner))
                    return;

                _owner = null;
                owner._registrationCount = Math.Max(0, owner._registrationCount - 1);
                if (!_supported)
                {
                    owner._unsupportedRegistrationCount = Math.Max(
                        0,
                        owner._unsupportedRegistrationCount - 1);
                }
                else if (_unavailable)
                {
                    owner._unavailableRegistrationCount = Math.Max(
                        0,
                        owner._unavailableRegistrationCount - 1);
                }
                else
                {
                    owner.RemoveSampleLocked(_sample);
                }
            }
        }
    }
}

/// <summary>
/// One enabled-only counter/gauge family shared by the writer pager and all of
/// its snapshot pagers.
/// </summary>
internal sealed class StorageIoRuntimeDiagnostics
{
    private readonly StorageRuntimeDetailAvailabilityRaw _physicalAvailability;
    private readonly StorageDeviceIoRuntimeCounters? _physicalCounters;

    private StorageIoRuntimeDiagnostics(
        StorageRuntimeDetailAvailabilityRaw physicalAvailability,
        StorageDeviceIoRuntimeCounters? physicalCounters)
    {
        _physicalAvailability = physicalAvailability;
        _physicalCounters = physicalCounters;
    }

    internal LogicalPageReadRuntimeCounters LogicalReads { get; } = new();

    internal StorageCacheRuntimeDiagnostics Cache { get; } = new();

    internal static StorageIoRuntimeDiagnostics Create(IStorageDevice device)
    {
        ArgumentNullException.ThrowIfNull(device);

        if (device is MemoryStorageDevice)
        {
            return new StorageIoRuntimeDiagnostics(
                StorageRuntimeDetailAvailabilityRaw.NotApplicable,
                physicalCounters: null);
        }

        if (device is not IStorageDeviceIoRuntimeDiagnosticsProvider provider)
        {
            return new StorageIoRuntimeDiagnostics(
                StorageRuntimeDetailAvailabilityRaw.Unsupported,
                physicalCounters: null);
        }

        try
        {
            StorageDeviceIoRuntimeCounters counters = provider.EnableRuntimeDiagnostics();
            return new StorageIoRuntimeDiagnostics(
                StorageRuntimeDetailAvailabilityRaw.Available,
                counters);
        }
        catch
        {
            return new StorageIoRuntimeDiagnostics(
                StorageRuntimeDetailAvailabilityRaw.Unavailable,
                physicalCounters: null);
        }
    }

    internal StorageIoRuntimeRawSnapshot Capture()
    {
        LogicalPageReadRuntimeRawSnapshot logicalReads = LogicalReads.Capture();
        StorageRuntimeDetailAvailabilityRaw cacheAvailability =
            Cache.Capture(out StorageCacheRuntimeRawSnapshot cache);

        StorageRuntimeDetailAvailabilityRaw physicalAvailability =
            _physicalAvailability;
        StorageDeviceIoRuntimeRawSnapshot physicalIo = default;
        if (physicalAvailability == StorageRuntimeDetailAvailabilityRaw.Available)
        {
            if (_physicalCounters is null)
            {
                physicalAvailability = StorageRuntimeDetailAvailabilityRaw.Unavailable;
            }
            else
            {
                try
                {
                    physicalIo = _physicalCounters.Capture();
                }
                catch
                {
                    physicalAvailability = StorageRuntimeDetailAvailabilityRaw.Unavailable;
                }
            }
        }

        return new StorageIoRuntimeRawSnapshot(
            logicalReads,
            cacheAvailability,
            cache,
            physicalAvailability,
            physicalIo);
    }

    internal bool TrySealPhysicalIo(
        out StorageDeviceIoRuntimeRawSnapshot snapshot)
    {
        if (_physicalAvailability == StorageRuntimeDetailAvailabilityRaw.Available &&
            _physicalCounters is not null)
        {
            return _physicalCounters.TrySeal(out snapshot);
        }

        snapshot = default;
        return false;
    }
}
