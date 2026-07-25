using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace CSharpDB.EntityFrameworkCore.Tools;

/// <summary>
/// Owns the operating-system containment applied to one tool child process.
/// On Windows, closing the job terminates the complete child tree and every
/// process in the job is capped at 512 MiB. The worker host also configures a
/// cross-platform managed-heap ceiling; non-Windows process trees rely on
/// explicit termination by the caller.
/// </summary>
internal sealed class EfCoreProcessContainment : IDisposable
{
    private const uint KillOnJobClose = 0x00002000;
    private const uint ProcessMemoryLimit = 0x00000100;
    private const ulong ProcessMemoryLimitBytes =
        512UL * 1024UL * 1024UL;

    private SafeFileHandle? job;

    private EfCoreProcessContainment(SafeFileHandle? job)
    {
        this.job = job;
    }

    internal static EfCoreProcessContainment Attach(Process process)
    {
        ArgumentNullException.ThrowIfNull(process);

        if (!OperatingSystem.IsWindows())
            return new EfCoreProcessContainment(job: null);

        SafeFileHandle job = CreateJobObject(
            IntPtr.Zero,
            lpName: null);
        if (job.IsInvalid)
            throw new Win32Exception(Marshal.GetLastPInvokeError());

        try
        {
            var limits = new JobObjectExtendedLimitInformation
            {
                BasicLimitInformation =
                    new JobObjectBasicLimitInformation
                    {
                        LimitFlags =
                            KillOnJobClose | ProcessMemoryLimit,
                    },
                ProcessMemoryLimit =
                    new UIntPtr(ProcessMemoryLimitBytes),
            };
            if (!SetInformationJobObject(
                    job,
                    JobObjectInformationClass.ExtendedLimitInformation,
                    ref limits,
                    (uint)Marshal.SizeOf<
                        JobObjectExtendedLimitInformation>()))
            {
                throw new Win32Exception(
                    Marshal.GetLastPInvokeError());
            }
            if (!AssignProcessToJobObject(job, process.Handle))
            {
                throw new Win32Exception(
                    Marshal.GetLastPInvokeError());
            }

            return new EfCoreProcessContainment(job);
        }
        catch
        {
            job.Dispose();
            TryKillProcessTree(process);
            throw;
        }
    }

    internal void Terminate()
    {
        SafeFileHandle? handle =
            Interlocked.Exchange(ref job, null);
        handle?.Dispose();
    }

    public void Dispose() => Terminate();

    private static void TryKillProcessTree(Process process)
    {
        try
        {
            if (!process.HasExited)
                process.Kill(entireProcessTree: true);
        }
        catch (Exception exception) when (
            exception is Win32Exception or InvalidOperationException or
                NotSupportedException)
        {
        }
    }

    [DllImport(
        "kernel32.dll",
        EntryPoint = "CreateJobObjectW",
        CharSet = CharSet.Unicode,
        SetLastError = true)]
    private static extern SafeFileHandle CreateJobObject(
        IntPtr lpJobAttributes,
        string? lpName);

    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool SetInformationJobObject(
        SafeFileHandle hJob,
        JobObjectInformationClass jobObjectInformationClass,
        ref JobObjectExtendedLimitInformation lpJobObjectInformation,
        uint cbJobObjectInformationLength);

    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool AssignProcessToJobObject(
        SafeFileHandle hJob,
        IntPtr hProcess);

    private enum JobObjectInformationClass
    {
        ExtendedLimitInformation = 9,
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct JobObjectBasicLimitInformation
    {
        internal long PerProcessUserTimeLimit;
        internal long PerJobUserTimeLimit;
        internal uint LimitFlags;
        internal UIntPtr MinimumWorkingSetSize;
        internal UIntPtr MaximumWorkingSetSize;
        internal uint ActiveProcessLimit;
        internal UIntPtr Affinity;
        internal uint PriorityClass;
        internal uint SchedulingClass;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct IoCounters
    {
        internal ulong ReadOperationCount;
        internal ulong WriteOperationCount;
        internal ulong OtherOperationCount;
        internal ulong ReadTransferCount;
        internal ulong WriteTransferCount;
        internal ulong OtherTransferCount;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct JobObjectExtendedLimitInformation
    {
        internal JobObjectBasicLimitInformation BasicLimitInformation;
        internal IoCounters IoInfo;
        internal UIntPtr ProcessMemoryLimit;
        internal UIntPtr JobMemoryLimit;
        internal UIntPtr PeakProcessMemoryUsed;
        internal UIntPtr PeakJobMemoryUsed;
    }
}
