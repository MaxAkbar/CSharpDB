using System.Buffers;
using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CSharpDB.Migration;
using Microsoft.Win32.SafeHandles;

namespace CSharpDB.Cli;

internal enum MySqlWorkerStatus
{
    Success,
    Missing,
    Incompatible,
    ConnectionUnavailable,
    InspectionFailed,
}
internal sealed record MySqlWorkerResult
{
    internal required MySqlWorkerStatus Status { get; init; }

    internal MigrationCatalog? Catalog { get; init; }

    internal static MySqlWorkerResult Success(MigrationCatalog catalog)
    {
        ArgumentNullException.ThrowIfNull(catalog);
        return new MySqlWorkerResult
        {
            Status = MySqlWorkerStatus.Success,
            Catalog = catalog,
        };
    }

    internal static MySqlWorkerResult Failure(MySqlWorkerStatus status)
    {
        if (status == MySqlWorkerStatus.Success)
            throw new ArgumentOutOfRangeException(nameof(status));

        return new MySqlWorkerResult { Status = status };
    }
}

internal static class MySqlWorkerClient
{
    internal const string ProtocolV1 = "csharpdb-mysql-worker/v1";
    internal const long MaxCatalogBytes = 64L * 1024 * 1024;
    internal const long MaxStderrBytes = 64L * 1024;

    private const int ExitIncompatible = 10;
    private const int ExitConnectionUnavailable = 11;
    private const int ExitInspectionFailed = 12;
    private const int ExitInternalFailure = 13;
    private static readonly byte[] HeaderBytes =
        Encoding.ASCII.GetBytes(ProtocolV1 + "\n");
    private static readonly UTF8Encoding StrictUtf8 =
        new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    internal static async ValueTask<MySqlWorkerResult> InspectAsync(
        string connectionEnvironmentVariableName,
        string targetCSharpDbVersion,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            connectionEnvironmentVariableName);
        ArgumentException.ThrowIfNullOrWhiteSpace(targetCSharpDbVersion);
        cancellationToken.ThrowIfCancellationRequested();

        string workerDirectory = Path.Combine(
            AppContext.BaseDirectory,
            "adapters",
            "mysql");
        string workerPath = Path.Combine(
            workerDirectory,
            OperatingSystem.IsWindows()
                ? "csharpdb-migration-mysql-worker.exe"
                : "csharpdb-migration-mysql-worker");
        if (!File.Exists(workerPath))
        {
            return MySqlWorkerResult.Failure(
                MySqlWorkerStatus.Missing);
        }

        var startInfo = new ProcessStartInfo
        {
            FileName = workerPath,
            WorkingDirectory = workerDirectory,
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true,
        };
        startInfo.ArgumentList.Add("--protocol");
        startInfo.ArgumentList.Add(ProtocolV1);
        startInfo.ArgumentList.Add("--connection-env");
        startInfo.ArgumentList.Add(connectionEnvironmentVariableName);
        startInfo.ArgumentList.Add("--target-version");
        startInfo.ArgumentList.Add(targetCSharpDbVersion);

        using var process = new Process { StartInfo = startInfo };
        try
        {
            if (!process.Start())
            {
                return MySqlWorkerResult.Failure(
                    MySqlWorkerStatus.Missing);
            }
        }
        catch (Exception exception) when (
            exception is Win32Exception or IOException or
                InvalidOperationException or UnauthorizedAccessException)
        {
            return MySqlWorkerResult.Failure(
                MySqlWorkerStatus.Missing);
        }

        WorkerProcessContainment containment;
        try
        {
            containment = WorkerProcessContainment.Attach(process);
        }
        catch (Exception exception) when (
            exception is Win32Exception or IOException or
                InvalidOperationException or NotSupportedException)
        {
            if (!await KillAndWaitAsync(process).ConfigureAwait(false))
                throw new WorkerTerminationException();
            return MySqlWorkerResult.Failure(
                MySqlWorkerStatus.Incompatible);
        }
        using WorkerProcessContainment containmentScope = containment;

        byte[] stdout;
        using var processCancellation =
            CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken);
        Task<byte[]> stdoutTask = ReadBoundedAsync(
            process.StandardOutput.BaseStream,
            checked(MaxCatalogBytes + HeaderBytes.LongLength),
            processCancellation.Token);
        Task stderrTask = DrainBoundedAsync(
            process.StandardError.BaseStream,
            MaxStderrBytes,
            processCancellation.Token);
        Task exitTask = process.WaitForExitAsync(processCancellation.Token);

        try
        {
            await ObserveAllAsync(
                    [stdoutTask, stderrTask, exitTask])
                .ConfigureAwait(false);
            stdout = await stdoutTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (
            cancellationToken.IsCancellationRequested)
        {
            try
            {
                await TerminateAsync(
                        process,
                        containmentScope,
                        processCancellation,
                        stdoutTask,
                        stderrTask,
                        exitTask)
                    .ConfigureAwait(false);
            }
            finally
            {
                ClearCompletedOutput(stdoutTask);
            }
            throw;
        }
        catch (Exception exception) when (
            exception is WorkerOutputLimitException or IOException or
                InvalidOperationException)
        {
            try
            {
                await TerminateAsync(
                        process,
                        containmentScope,
                        processCancellation,
                        stdoutTask,
                        stderrTask,
                        exitTask)
                    .ConfigureAwait(false);
            }
            finally
            {
                ClearCompletedOutput(stdoutTask);
            }
            return MySqlWorkerResult.Failure(
                MySqlWorkerStatus.Incompatible);
        }

        try
        {
            return process.ExitCode switch
            {
                0 => ParseSuccess(stdout, targetCSharpDbVersion),
                ExitConnectionUnavailable =>
                    MySqlWorkerResult.Failure(
                        MySqlWorkerStatus.ConnectionUnavailable),
                ExitInspectionFailed =>
                    MySqlWorkerResult.Failure(
                        MySqlWorkerStatus.InspectionFailed),
                ExitIncompatible or ExitInternalFailure =>
                    MySqlWorkerResult.Failure(
                        MySqlWorkerStatus.Incompatible),
                _ => MySqlWorkerResult.Failure(
                    MySqlWorkerStatus.Incompatible),
            };
        }
        finally
        {
            CryptographicOperations.ZeroMemory(stdout);
        }
    }

    private static MySqlWorkerResult ParseSuccess(
        byte[] stdout,
        string targetCSharpDbVersion)
    {
        if (stdout.Length <= HeaderBytes.Length ||
            !stdout.AsSpan(0, HeaderBytes.Length).SequenceEqual(HeaderBytes))
        {
            return MySqlWorkerResult.Failure(
                MySqlWorkerStatus.Incompatible);
        }

        try
        {
            string json = StrictUtf8.GetString(
                stdout,
                HeaderBytes.Length,
                stdout.Length - HeaderBytes.Length);
            MigrationCatalog catalog =
                MigrationArtifactSerializer.DeserializeCatalog(json);
            if (catalog.Source.Kind != MigrationSourceKind.MySql ||
                !string.Equals(
                    catalog.TargetCSharpDbVersion,
                    targetCSharpDbVersion,
                    StringComparison.Ordinal))
            {
                return MySqlWorkerResult.Failure(
                    MySqlWorkerStatus.Incompatible);
            }

            return MySqlWorkerResult.Success(catalog);
        }
        catch (Exception exception) when (
            exception is DecoderFallbackException or InvalidDataException or
                JsonException or ArgumentException or FormatException or
                OverflowException)
        {
            return MySqlWorkerResult.Failure(
                MySqlWorkerStatus.Incompatible);
        }
    }

    private static async Task ObserveAllAsync(IReadOnlyList<Task> tasks)
    {
        var remaining = new List<Task>(tasks);
        while (remaining.Count > 0)
        {
            Task completed = await Task.WhenAny(remaining)
                .ConfigureAwait(false);
            await completed.ConfigureAwait(false);
            remaining.Remove(completed);
        }
    }

    private static async Task<byte[]> ReadBoundedAsync(
        Stream stream,
        long maxBytes,
        CancellationToken cancellationToken)
    {
        byte[] buffer = ArrayPool<byte>.Shared.Rent(64 * 1024);
        try
        {
            using var output = new MemoryStream();
            try
            {
                while (true)
                {
                    int read = await stream.ReadAsync(
                            buffer.AsMemory(0, buffer.Length),
                            cancellationToken)
                        .ConfigureAwait(false);
                    if (read == 0)
                        return output.ToArray();
                    if (output.Length > maxBytes - read)
                        throw new WorkerOutputLimitException();
                    output.Write(buffer, 0, read);
                }
            }
            finally
            {
                if (output.TryGetBuffer(out ArraySegment<byte> written))
                    CryptographicOperations.ZeroMemory(written.AsSpan());
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
        }
    }

    private static async Task DrainBoundedAsync(
        Stream stream,
        long maxBytes,
        CancellationToken cancellationToken)
    {
        byte[] buffer = ArrayPool<byte>.Shared.Rent(8 * 1024);
        try
        {
            long total = 0;
            while (true)
            {
                int read = await stream.ReadAsync(
                        buffer.AsMemory(0, buffer.Length),
                        cancellationToken)
                    .ConfigureAwait(false);
                if (read == 0)
                    return;
                if (total > maxBytes - read)
                    throw new WorkerOutputLimitException();
                total += read;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
        }
    }

    private static async Task TerminateAsync(
        Process process,
        WorkerProcessContainment containment,
        CancellationTokenSource processCancellation,
        params Task[] tasks)
    {
        containment.Terminate();
        bool processStopped =
            await KillAndWaitAsync(process).ConfigureAwait(false);

        processCancellation.Cancel();
        try
        {
            using var timeout = new CancellationTokenSource(
                TimeSpan.FromSeconds(5));
            await Task.WhenAll(tasks)
                .WaitAsync(timeout.Token)
                .ConfigureAwait(false);
        }
        catch (Exception exception) when (
            exception is OperationCanceledException or IOException or
                InvalidOperationException or WorkerOutputLimitException)
        {
        }

        if (!processStopped || tasks.Any(static task => !task.IsCompleted))
            throw new WorkerTerminationException();
    }

    private static void ClearCompletedOutput(Task<byte[]> stdoutTask)
    {
        if (stdoutTask.Status == TaskStatus.RanToCompletion)
            CryptographicOperations.ZeroMemory(stdoutTask.Result);
    }

    private static async Task<bool> KillAndWaitAsync(Process process)
    {
        for (int attempt = 0; attempt < 3; attempt++)
        {
            try
            {
                if (process.HasExited)
                    return true;
                process.Kill(entireProcessTree: true);
            }
            catch (Exception exception) when (
                exception is Win32Exception or InvalidOperationException or
                    NotSupportedException)
            {
            }

            try
            {
                using var exitTimeout = new CancellationTokenSource(
                    TimeSpan.FromSeconds(2));
                await process.WaitForExitAsync(exitTimeout.Token)
                    .ConfigureAwait(false);
                return true;
            }
            catch (Exception exception) when (
                exception is OperationCanceledException or
                    InvalidOperationException)
            {
            }
        }

        try
        {
            return process.HasExited;
        }
        catch (InvalidOperationException)
        {
            return false;
        }
    }

    private sealed class WorkerOutputLimitException : IOException;

    private sealed class WorkerTerminationException : IOException;

    private sealed class WorkerProcessContainment : IDisposable
    {
        private const uint KillOnJobClose = 0x00002000;
        private SafeFileHandle? job;

        private WorkerProcessContainment(SafeFileHandle? job)
        {
            this.job = job;
        }

        internal static WorkerProcessContainment Attach(Process process)
        {
            if (!OperatingSystem.IsWindows())
                return new WorkerProcessContainment(job: null);

            SafeFileHandle job = CreateJobObject(
                IntPtr.Zero,
                lpName: null);
            if (job.IsInvalid)
                throw new Win32Exception(Marshal.GetLastPInvokeError());

            try
            {
                var limits = new JobObjectExtendedLimitInformation
                {
                    BasicLimitInformation = new JobObjectBasicLimitInformation
                    {
                        LimitFlags = KillOnJobClose,
                    },
                };
                if (!SetInformationJobObject(
                        job,
                        JobObjectInformationClass.ExtendedLimitInformation,
                        ref limits,
                        (uint)Marshal.SizeOf<JobObjectExtendedLimitInformation>()))
                {
                    throw new Win32Exception(Marshal.GetLastPInvokeError());
                }
                if (!AssignProcessToJobObject(job, process.Handle))
                    throw new Win32Exception(Marshal.GetLastPInvokeError());

                return new WorkerProcessContainment(job);
            }
            catch
            {
                job.Dispose();
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

                throw;
            }
        }

        internal void Terminate()
        {
            SafeFileHandle? handle = Interlocked.Exchange(ref job, null);
            handle?.Dispose();
        }

        public void Dispose() => Terminate();

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
}
