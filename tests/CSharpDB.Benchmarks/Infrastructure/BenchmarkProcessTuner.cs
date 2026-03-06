using System.Diagnostics;

namespace CSharpDB.Benchmarks.Infrastructure;

internal static class BenchmarkProcessTuner
{
    public static void ConfigureIfRequested(bool enableRepro, int? requestedCpuThreads)
    {
        if (!enableRepro)
            return;

        var notes = new List<string>();
        var process = Process.GetCurrentProcess();

        TryApplyHighPriority(process, notes);
        TryApplyCpuAffinity(process, requestedCpuThreads, notes);

        Console.WriteLine($"[bench-env] reproducible mode enabled ({string.Join(", ", notes)})");
    }

    private static void TryApplyHighPriority(Process process, List<string> notes)
    {
        try
        {
            process.PriorityClass = ProcessPriorityClass.High;
            notes.Add("priority=High");
        }
        catch (Exception ex)
        {
            notes.Add($"priority=unchanged ({ex.GetType().Name})");
        }
    }

    private static void TryApplyCpuAffinity(Process process, int? requestedCpuThreads, List<string> notes)
    {
        if (!OperatingSystem.IsWindows())
        {
            notes.Add("affinity=unsupported-os");
            return;
        }

        try
        {
            ulong availableMask = unchecked((ulong)process.ProcessorAffinity.ToInt64());
            int availableThreads = CountSetBits(availableMask);
            if (availableThreads <= 0)
            {
                notes.Add("affinity=unavailable");
                return;
            }

            int desiredThreads = requestedCpuThreads ?? Math.Min(availableThreads, 8);
            desiredThreads = Math.Clamp(desiredThreads, 1, availableThreads);

            ulong selectedMask = SelectLowestSetBits(availableMask, desiredThreads);
            if (selectedMask == 0)
            {
                notes.Add("affinity=unchanged");
                return;
            }

            process.ProcessorAffinity = (IntPtr)unchecked((long)selectedMask);
            notes.Add($"affinity=0x{selectedMask:X} ({desiredThreads}/{availableThreads} threads)");
        }
        catch (Exception ex)
        {
            notes.Add($"affinity=unchanged ({ex.GetType().Name})");
        }
    }

    private static ulong SelectLowestSetBits(ulong mask, int count)
    {
        ulong selected = 0;
        int selectedCount = 0;
        for (int bit = 0; bit < 64 && selectedCount < count; bit++)
        {
            ulong bitMask = 1UL << bit;
            if ((mask & bitMask) == 0)
                continue;

            selected |= bitMask;
            selectedCount++;
        }

        return selected;
    }

    private static int CountSetBits(ulong value)
    {
        int count = 0;
        while (value != 0)
        {
            count += (int)(value & 1UL);
            value >>= 1;
        }

        return count;
    }
}
