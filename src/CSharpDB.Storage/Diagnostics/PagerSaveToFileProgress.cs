using System.Runtime.CompilerServices;

namespace CSharpDB.Storage.Diagnostics;

internal enum PagerSaveToFilePhase
{
    Checkpointing,
    Copying,
    Staging,
}

internal interface IPagerSaveToFileProgressObserver
{
    void OnPhase(PagerSaveToFilePhase phase);
}

internal static class PagerSaveToFileProgressObserverExtensions
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void TryReportPhase(
        this IPagerSaveToFileProgressObserver? observer,
        PagerSaveToFilePhase phase)
    {
        if (observer is null)
            return;

        try
        {
            observer.OnPhase(phase);
        }
        catch (Exception)
        {
            // Backup progress is best-effort and must never alter snapshot output.
        }
    }
}
