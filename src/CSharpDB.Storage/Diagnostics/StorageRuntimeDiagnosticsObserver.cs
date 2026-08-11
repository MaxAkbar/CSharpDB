using CSharpDB.Primitives;

namespace CSharpDB.Storage.Diagnostics;

/// <summary>
/// Internal, path-free bridge from built-in storage work to the owning runtime
/// diagnostics state. Implementations are best-effort observers: storage never
/// permits an observer failure to change database behavior.
/// </summary>
internal interface IStorageRuntimeDiagnosticsObserver
{
    object? CaptureCheckpointCorrelation(StorageCheckpointOriginRaw origin);

    object? CaptureCheckpointCompletionCorrelation();

    void OnRecoveryStarted();

    void OnRecoveryChanged(in StorageRecoveryRuntimeRawSnapshot snapshot);

    void OnRecoveryCompleted(in StorageRecoveryRuntimeRawSnapshot snapshot);

    void OnCheckpointStarted(
        in StorageCheckpointRuntimeRawSnapshot snapshot,
        object? correlation);

    void OnCheckpointChanged(in StorageCheckpointRuntimeRawSnapshot snapshot);

    void OnCheckpointCompleted(
        in StorageCheckpointRuntimeRawSnapshot snapshot,
        object? correlation);

    void OnWalFlushCompleted()
    {
    }

    void OnWalFlushCompleted(int logicalCommitCount)
        => OnWalFlushCompleted();

    void OnWalDurableFlushCompleted(long durableFlushCount)
    {
    }

    void OnStorageDeviceIoSealed(
        in StorageDeviceIoRuntimeRawSnapshot snapshot)
    {
    }
}

internal static class StorageRuntimeDiagnosticsObserverExtensions
{
    internal static object? TryCaptureCheckpointCorrelation(
        this IStorageRuntimeDiagnosticsObserver? observer,
        StorageCheckpointOriginRaw origin)
    {
        if (observer is null)
            return null;

        try
        {
            return observer.CaptureCheckpointCorrelation(origin);
        }
        catch
        {
            return null;
        }
    }

    internal static object? TryCaptureCheckpointCompletionCorrelation(
        this IStorageRuntimeDiagnosticsObserver? observer)
    {
        if (observer is null)
            return null;

        try
        {
            return observer.CaptureCheckpointCompletionCorrelation();
        }
        catch
        {
            return null;
        }
    }

    internal static void TryRecoveryStarted(
        this IStorageRuntimeDiagnosticsObserver? observer)
    {
        if (observer is null)
            return;

        try
        {
            observer.OnRecoveryStarted();
        }
        catch
        {
        }
    }

    internal static void TryRecoveryChanged(
        this IStorageRuntimeDiagnosticsObserver? observer,
        in StorageRecoveryRuntimeRawSnapshot snapshot)
    {
        if (observer is null)
            return;

        try
        {
            observer.OnRecoveryChanged(in snapshot);
        }
        catch
        {
        }
    }

    internal static void TryRecoveryCompleted(
        this IStorageRuntimeDiagnosticsObserver? observer,
        in StorageRecoveryRuntimeRawSnapshot snapshot)
    {
        if (observer is null)
            return;

        try
        {
            observer.OnRecoveryCompleted(in snapshot);
        }
        catch
        {
        }
    }

    internal static void TryCheckpointStarted(
        this IStorageRuntimeDiagnosticsObserver? observer,
        in StorageCheckpointRuntimeRawSnapshot snapshot,
        object? correlation)
    {
        if (observer is null)
            return;

        try
        {
            observer.OnCheckpointStarted(in snapshot, correlation);
        }
        catch
        {
        }
    }

    internal static void TryCheckpointChanged(
        this IStorageRuntimeDiagnosticsObserver? observer,
        in StorageCheckpointRuntimeRawSnapshot snapshot)
    {
        if (observer is null)
            return;

        try
        {
            observer.OnCheckpointChanged(in snapshot);
        }
        catch
        {
        }
    }

    internal static void TryCheckpointCompleted(
        this IStorageRuntimeDiagnosticsObserver? observer,
        in StorageCheckpointRuntimeRawSnapshot snapshot,
        object? correlation)
    {
        if (observer is null)
            return;

        try
        {
            observer.OnCheckpointCompleted(in snapshot, correlation);
        }
        catch
        {
        }
    }

    internal static void TryWalFlushCompleted(
        this IStorageRuntimeDiagnosticsObserver? observer,
        int logicalCommitCount)
    {
        if (observer is null || logicalCommitCount <= 0)
            return;

        try
        {
            observer.OnWalFlushCompleted(logicalCommitCount);
        }
        catch
        {
        }
    }

    internal static void TryWalDurableFlushCompleted(
        this IStorageRuntimeDiagnosticsObserver? observer,
        long durableFlushCount)
    {
        if (observer is null || durableFlushCount <= 0)
            return;

        try
        {
            observer.OnWalDurableFlushCompleted(durableFlushCount);
        }
        catch
        {
        }
    }

    internal static void TryStorageDeviceIoSealed(
        this IStorageRuntimeDiagnosticsObserver? observer,
        in StorageDeviceIoRuntimeRawSnapshot snapshot)
    {
        if (observer is null)
            return;

        try
        {
            observer.OnStorageDeviceIoSealed(in snapshot);
        }
        catch
        {
        }
    }

    internal static StorageRuntimeFailureKindRaw ClassifyRuntimeFailure(
        Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        if (exception is OperationCanceledException)
            return StorageRuntimeFailureKindRaw.OperationCanceled;
        if (exception is TimeoutException)
            return StorageRuntimeFailureKindRaw.TimedOut;
        if (exception is UnauthorizedAccessException)
            return StorageRuntimeFailureKindRaw.AccessDenied;

        for (Exception? current = exception; current is not null; current = current.InnerException)
        {
            if (current is FileNotFoundException or DirectoryNotFoundException)
                return StorageRuntimeFailureKindRaw.NotFound;
            if (current is UnauthorizedAccessException)
                return StorageRuntimeFailureKindRaw.AccessDenied;
            if (current is IOException)
                return StorageRuntimeFailureKindRaw.Io;
        }

        if (exception is CSharpDbException databaseException)
        {
            return databaseException.Code switch
            {
                ErrorCode.CorruptDatabase or ErrorCode.WalError =>
                    StorageRuntimeFailureKindRaw.Corrupt,
                ErrorCode.Busy => StorageRuntimeFailureKindRaw.Busy,
                ErrorCode.ResourceLimitExceeded =>
                    StorageRuntimeFailureKindRaw.ResourceLimit,
                ErrorCode.IoError or ErrorCode.JournalError =>
                    StorageRuntimeFailureKindRaw.Io,
                _ => StorageRuntimeFailureKindRaw.Operation,
            };
        }

        return StorageRuntimeFailureKindRaw.Unknown;
    }
}
