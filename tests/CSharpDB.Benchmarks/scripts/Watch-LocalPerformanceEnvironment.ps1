#requires -Version 7.0

[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string] $OutputPath,

    [Parameter(Mandatory)]
    [string] $StopSignalPath,

    [Parameter(Mandatory)]
    [string] $ReadySignalPath,

    [Parameter(Mandatory)]
    [ValidateRange(1, [int]::MaxValue)]
    [int] $AllowedRootProcessId,

    [Parameter(Mandatory)]
    [string] $AllowedRootStartTimeUtc,

    [ValidateRange(250, 10000)]
    [int] $SampleIntervalMilliseconds = 1000,

    [ValidateRange(0, 100)]
    [double] $MaxExternalCpuPercent = 8,

    [ValidateRange(0, 64)]
    [double] $MaxExternalCpuCoreEquivalent = 0.5,

    [ValidateRange(0, [long]::MaxValue)]
    [long] $MaxExternalIoBytesPerSecond = 4194304,

    [ValidateRange(1, 60)]
    [int] $RequiredConsecutiveBusySamples = 5,

    [ValidateNotNullOrEmpty()]
    [string] $ProhibitedExternalProcessNames =
        'devenv;msbuild;vbcscompiler;testhost;vstest.console;msiexec;' +
        'trustedinstaller;tiworker;mousocoreworker;usoclient;winget;nuget'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

if (-not $IsWindows) {
    throw 'The local performance environment monitor requires Windows.'
}

$nativeTypeName = 'CSharpDbLocalPerformanceNativeMethods'
if ($null -eq ($nativeTypeName -as [type])) {
    Add-Type -TypeDefinition @'
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Runtime.InteropServices;

public static class CSharpDbLocalPerformanceNativeMethods
{
    private const uint TH32CS_SNAPPROCESS = 0x00000002;
    private const uint PROCESS_QUERY_LIMITED_INFORMATION = 0x1000;
    private static readonly IntPtr InvalidHandleValue = new IntPtr(-1);

    [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
    private struct PROCESSENTRY32
    {
        public uint dwSize;
        public uint cntUsage;
        public uint th32ProcessID;
        public IntPtr th32DefaultHeapID;
        public uint th32ModuleID;
        public uint cntThreads;
        public uint th32ParentProcessID;
        public int pcPriClassBase;
        public uint dwFlags;
        [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 260)]
        public string szExeFile;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct IO_COUNTERS
    {
        public ulong ReadOperationCount;
        public ulong WriteOperationCount;
        public ulong OtherOperationCount;
        public ulong ReadTransferCount;
        public ulong WriteTransferCount;
        public ulong OtherTransferCount;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct FILETIME
    {
        public uint LowDateTime;
        public uint HighDateTime;

        public long ToTicks()
        {
            return unchecked(((long)HighDateTime << 32) | LowDateTime);
        }
    }

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern IntPtr CreateToolhelp32Snapshot(uint flags, uint processId);

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern bool Process32First(IntPtr snapshot, ref PROCESSENTRY32 entry);

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern bool Process32Next(IntPtr snapshot, ref PROCESSENTRY32 entry);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern IntPtr OpenProcess(uint desiredAccess, bool inheritHandle, uint processId);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool GetProcessIoCounters(IntPtr process, out IO_COUNTERS counters);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool GetProcessTimes(
        IntPtr process,
        out FILETIME creationTime,
        out FILETIME exitTime,
        out FILETIME kernelTime,
        out FILETIME userTime);

    [DllImport("kernel32.dll")]
    private static extern bool CloseHandle(IntPtr handle);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool GetSystemTimes(
        out FILETIME idleTime,
        out FILETIME kernelTime,
        out FILETIME userTime);

    public static Dictionary<int, int> GetParentProcessIds()
    {
        IntPtr snapshot = CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0);
        if (snapshot == InvalidHandleValue)
            throw new Win32Exception(Marshal.GetLastWin32Error());

        try
        {
            var result = new Dictionary<int, int>();
            var entry = new PROCESSENTRY32();
            entry.dwSize = (uint)Marshal.SizeOf<PROCESSENTRY32>();
            if (!Process32First(snapshot, ref entry))
                throw new Win32Exception(Marshal.GetLastWin32Error());

            do
            {
                result[checked((int)entry.th32ProcessID)] =
                    checked((int)entry.th32ParentProcessID);
                entry.dwSize = (uint)Marshal.SizeOf<PROCESSENTRY32>();
            }
            while (Process32Next(snapshot, ref entry));

            int error = Marshal.GetLastWin32Error();
            if (error != 0 && error != 18)
                throw new Win32Exception(error);
            return result;
        }
        finally
        {
            CloseHandle(snapshot);
        }
    }

    public static bool TryGetIoBytes(int processId, out ulong readBytes, out ulong writeBytes)
    {
        readBytes = 0;
        writeBytes = 0;
        IntPtr process = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, false, checked((uint)processId));
        if (process == IntPtr.Zero)
            return false;

        try
        {
            if (!GetProcessIoCounters(process, out IO_COUNTERS counters))
                return false;
            readBytes = counters.ReadTransferCount;
            writeBytes = counters.WriteTransferCount;
            return true;
        }
        finally
        {
            CloseHandle(process);
        }
    }

    public static bool TryGetProcessTimes(
        int processId,
        out long creationTicks,
        out long cpuTicks)
    {
        creationTicks = 0;
        cpuTicks = 0;
        IntPtr process = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, false, checked((uint)processId));
        if (process == IntPtr.Zero)
            return false;

        try
        {
            if (!GetProcessTimes(
                process,
                out FILETIME creation,
                out FILETIME exit,
                out FILETIME kernel,
                out FILETIME user))
            {
                return false;
            }
            creationTicks = creation.ToTicks();
            cpuTicks = checked(kernel.ToTicks() + user.ToTicks());
            return true;
        }
        finally
        {
            CloseHandle(process);
        }
    }

    public static long[] GetSystemCpuTimes()
    {
        if (!GetSystemTimes(out FILETIME idle, out FILETIME kernel, out FILETIME user))
            throw new Win32Exception(Marshal.GetLastWin32Error());
        return new[] { idle.ToTicks(), kernel.ToTicks(), user.ToTicks() };
    }
}
'@
}

function Convert-ToCsvCell {
    param([AllowNull()] $Value)

    $text = if ($null -eq $Value) { '' } else { [string] $Value }
    return '"' + $text.Replace('"', '""') + '"'
}

function Test-IsAllowedProcess {
    param(
        [Parameter(Mandatory)]
        [int] $ProcessId,

        [Parameter(Mandatory)]
        [Collections.Generic.Dictionary[int, int]] $ParentProcessIds
    )

    if ($ProcessId -eq $PID) {
        return $true
    }

    $current = $ProcessId
    $visited = [Collections.Generic.HashSet[int]]::new()
    for ($depth = 0; $depth -lt 64; $depth++) {
        if ($current -eq $AllowedRootProcessId) {
            return $true
        }
        if (-not $visited.Add($current) -or
            -not $ParentProcessIds.TryGetValue($current, [ref] $current)) {
            return $false
        }
    }

    return $false
}

function Get-ProcessSnapshot {
    $parentProcessIds =
        [CSharpDbLocalPerformanceNativeMethods]::GetParentProcessIds()
    $snapshot = @{}
    foreach ($process in [Diagnostics.Process]::GetProcesses()) {
        try {
            $processId = $process.Id
            $processName = $process.ProcessName
            $startTicks = [long] 0
            $cpuTicks = [long] 0
            $hasCpu = [CSharpDbLocalPerformanceNativeMethods]::TryGetProcessTimes(
                $processId,
                [ref] $startTicks,
                [ref] $cpuTicks)
            $hasStableIdentity = $hasCpu
            $readBytes = [UInt64] 0
            $writeBytes = [UInt64] 0
            $hasIo = [CSharpDbLocalPerformanceNativeMethods]::TryGetIoBytes(
                $processId,
                [ref] $readBytes,
                [ref] $writeBytes)
            $snapshot["$processId/$startTicks"] = [pscustomobject]@{
                Id = $processId
                Name = $processName
                CpuTicks = $cpuTicks
                HasCpu = $hasCpu
                HasStableIdentity = $hasStableIdentity
                ReadBytes = $readBytes
                WriteBytes = $writeBytes
                HasIo = $hasIo
                Allowed = Test-IsAllowedProcess `
                    -ProcessId $processId `
                    -ParentProcessIds $parentProcessIds
            }
        }
        catch {
            # Processes can exit or deny access while a snapshot is collected.
        }
        finally {
            $process.Dispose()
        }
    }

    return $snapshot
}

function Get-EnvironmentMonitorGateState {
    param(
        [Parameter(Mandatory)]
        [double] $ExternalCpuPercent,

        [Parameter(Mandatory)]
        [double] $ExternalCpuCoreEquivalent,

        [Parameter(Mandatory)]
        [double] $ExternalReadBytesPerSecond,

        [Parameter(Mandatory)]
        [double] $ExternalWriteBytesPerSecond,

        [Parameter(Mandatory)]
        [bool] $HasProhibitedExternalProcess,

        [Parameter(Mandatory)]
        [int] $UnobservableAllowedCpuProcessCount,

        [Parameter(Mandatory)]
        [int] $PreviousConsecutiveBusySamples,

        [Parameter(Mandatory)]
        [bool] $PreviouslyContaminated,

        [Parameter(Mandatory)]
        [double] $MaximumExternalCpuPercent,

        [Parameter(Mandatory)]
        [double] $MaximumExternalCpuCoreEquivalent,

        [Parameter(Mandatory)]
        [long] $MaximumExternalIoBytesPerSecond,

        [Parameter(Mandatory)]
        [int] $ConsecutiveBusySamplesRequired
    )

    $busyReasons = [Collections.Generic.List[string]]::new()
    if ($ExternalCpuPercent -gt $MaximumExternalCpuPercent -or
        $ExternalCpuCoreEquivalent -gt $MaximumExternalCpuCoreEquivalent) {
        $busyReasons.Add('external-cpu')
    }
    if (($ExternalReadBytesPerSecond + $ExternalWriteBytesPerSecond) -gt
        $MaximumExternalIoBytesPerSecond) {
        $busyReasons.Add('external-io')
    }
    if ($HasProhibitedExternalProcess) {
        $busyReasons.Add('prohibited-process')
    }
    if ($UnobservableAllowedCpuProcessCount -gt 0) {
        $busyReasons.Add('unobservable-allowed-cpu')
    }

    $consecutiveBusySamples = if ($busyReasons.Count -gt 0) {
        $PreviousConsecutiveBusySamples + 1
    }
    else {
        0
    }
    $contaminated =
        $PreviouslyContaminated -or
        $HasProhibitedExternalProcess -or
        $UnobservableAllowedCpuProcessCount -gt 0 -or
        $consecutiveBusySamples -ge $ConsecutiveBusySamplesRequired

    return [pscustomobject]@{
        BusyReason = $busyReasons -join ';'
        ConsecutiveBusySamples = $consecutiveBusySamples
        Contaminated = $contaminated
    }
}

function Test-AllowedRootProcessAlive {
    param([Parameter(Mandatory)][DateTimeOffset] $ExpectedStartUtc)

    try {
        $rootProcess = [Diagnostics.Process]::GetProcessById($AllowedRootProcessId)
        try {
            return $rootProcess.StartTime.ToUniversalTime() -eq $ExpectedStartUtc.UtcDateTime
        }
        finally {
            $rootProcess.Dispose()
        }
    }
    catch {
        return $false
    }
}

$prohibitedExternalNames = [Collections.Generic.HashSet[string]]::new(
    [StringComparer]::OrdinalIgnoreCase)
foreach ($name in $ProhibitedExternalProcessNames.Split(
        ';',
        [StringSplitOptions]::RemoveEmptyEntries -bor [StringSplitOptions]::TrimEntries)) {
    [void] $prohibitedExternalNames.Add($name)
}
if ($prohibitedExternalNames.Count -eq 0) {
    throw 'At least one prohibited external process name is required.'
}

$resolvedOutputPath = [IO.Path]::GetFullPath($OutputPath)
$resolvedStopSignalPath = [IO.Path]::GetFullPath($StopSignalPath)
$resolvedReadySignalPath = [IO.Path]::GetFullPath($ReadySignalPath)
$allowedRootStartedUtc = [DateTimeOffset]::MinValue
if (-not [DateTimeOffset]::TryParseExact(
        $AllowedRootStartTimeUtc,
        'O',
        [Globalization.CultureInfo]::InvariantCulture,
        [Globalization.DateTimeStyles]::RoundtripKind,
        [ref] $allowedRootStartedUtc) -or
    $allowedRootStartedUtc.Offset -ne [TimeSpan]::Zero -or
    -not (Test-AllowedRootProcessAlive -ExpectedStartUtc $allowedRootStartedUtc)) {
    throw 'The allowed root process identity is invalid or is no longer running.'
}
$outputDirectory = Split-Path -Parent $resolvedOutputPath
New-Item -ItemType Directory -Path $outputDirectory -Force | Out-Null
foreach ($reservedPath in $resolvedOutputPath, $resolvedStopSignalPath, $resolvedReadySignalPath) {
    if (Test-Path -LiteralPath $reservedPath) {
        throw "Local performance monitor path must not already exist: $reservedPath"
    }
}

$header = @(
    'TimestampUtc',
    'IntervalMilliseconds',
    'ExternalCpuPercent',
    'ExternalCpuCoreEquivalent',
    'SystemResidualCpuCoreEquivalent',
    'ExternalReadBytesPerSecond',
    'ExternalWriteBytesPerSecond',
    'ExternalProcessCount',
    'AllowedProcessCount',
    'UnobservableAllowedCpuProcessCount',
    'UnobservableExternalCpuProcessCount',
    'UnobservableExternalIoProcessCount',
    'ProhibitedExternalProcesses',
    'BusyReason',
    'ConsecutiveBusySamples',
    'Contaminated') -join ','
[IO.File]::WriteAllLines($resolvedOutputPath, @($header))

$logicalProcessorCount = [Environment]::ProcessorCount
$previousTimestamp = [DateTimeOffset]::UtcNow
$previousSnapshot = Get-ProcessSnapshot
$previousSystemCpuTimes =
    [CSharpDbLocalPerformanceNativeMethods]::GetSystemCpuTimes()
$consecutiveBusySamples = 0
$contaminated = $false
[IO.File]::WriteAllText(
    $resolvedReadySignalPath,
    $previousTimestamp.ToString('O'))

while ($true) {
    Start-Sleep -Milliseconds $SampleIntervalMilliseconds
    $stopRequested = Test-Path -LiteralPath $resolvedStopSignalPath -PathType Leaf
    if (-not $stopRequested -and
        -not (Test-AllowedRootProcessAlive -ExpectedStartUtc $allowedRootStartedUtc)) {
        throw 'The allowed root process exited before the monitor received its stop signal.'
    }
    $timestamp = [DateTimeOffset]::UtcNow
    $snapshot = Get-ProcessSnapshot
    $systemCpuTimes = [CSharpDbLocalPerformanceNativeMethods]::GetSystemCpuTimes()
    $intervalSeconds = [Math]::Max(
        ($timestamp - $previousTimestamp).TotalSeconds,
        0.001)
    $observableExternalCpuTicks = [long] 0
    $allowedCpuTicks = [long] 0
    $externalReadBytes = [UInt64] 0
    $externalWriteBytes = [UInt64] 0
    $externalProcessCount = 0
    $allowedProcessCount = 0
    $unobservableAllowedCpuProcessCount = 0
    $unobservableExternalCpuProcessCount = 0
    $unobservableExternalIoProcessCount = 0
    $prohibited = [Collections.Generic.SortedSet[string]]::new(
        [StringComparer]::OrdinalIgnoreCase)

    foreach ($entry in $snapshot.GetEnumerator()) {
        $current = $entry.Value
        if ($current.Allowed) {
            $allowedProcessCount++
            if (-not $current.HasCpu -or -not $current.HasStableIdentity) {
                $unobservableAllowedCpuProcessCount++
            }
            elseif ($previousSnapshot.ContainsKey($entry.Key)) {
                $previous = $previousSnapshot[$entry.Key]
                if ($previous.HasCpu -and $previous.HasStableIdentity) {
                    $allowedCpuDelta =
                        [long] $current.CpuTicks - [long] $previous.CpuTicks
                    if ($allowedCpuDelta -gt 0) {
                        $allowedCpuTicks += $allowedCpuDelta
                    }
                }
            }
            continue
        }
        if ($current.Id -le 4) {
            continue
        }

        $externalProcessCount++
        if (-not $current.HasCpu -or -not $current.HasStableIdentity) {
            $unobservableExternalCpuProcessCount++
        }
        if (-not $current.HasIo -or -not $current.HasStableIdentity) {
            $unobservableExternalIoProcessCount++
        }
        if ($prohibitedExternalNames.Contains([string] $current.Name)) {
            [void] $prohibited.Add("$($current.Name)#$($current.Id)")
        }
        if (-not $previousSnapshot.ContainsKey($entry.Key)) {
            continue
        }

        $previous = $previousSnapshot[$entry.Key]
        $cpuDelta = [long] $current.CpuTicks - [long] $previous.CpuTicks
        if ($current.HasCpu -and $current.HasStableIdentity -and
            $previous.HasCpu -and $previous.HasStableIdentity -and
            $cpuDelta -gt 0) {
            $observableExternalCpuTicks += $cpuDelta
        }
        if ($current.HasIo -and $current.HasStableIdentity -and
            $previous.HasIo -and $previous.HasStableIdentity) {
            if ([UInt64] $current.ReadBytes -ge [UInt64] $previous.ReadBytes) {
                $externalReadBytes +=
                    [UInt64] $current.ReadBytes - [UInt64] $previous.ReadBytes
            }
            if ([UInt64] $current.WriteBytes -ge [UInt64] $previous.WriteBytes) {
                $externalWriteBytes +=
                    [UInt64] $current.WriteBytes - [UInt64] $previous.WriteBytes
            }
        }
    }

    $idleDelta = [long] $systemCpuTimes[0] - [long] $previousSystemCpuTimes[0]
    $kernelDelta = [long] $systemCpuTimes[1] - [long] $previousSystemCpuTimes[1]
    $userDelta = [long] $systemCpuTimes[2] - [long] $previousSystemCpuTimes[2]
    $systemBusyCpuTicks = [Math]::Max(
        $kernelDelta + $userDelta - $idleDelta,
        [long] 0)
    $systemResidualCpuTicks = [Math]::Max(
        $systemBusyCpuTicks - $allowedCpuTicks,
        [long] 0)
    $externalCpuCoreEquivalent =
        ($observableExternalCpuTicks / [TimeSpan]::TicksPerSecond) / $intervalSeconds
    $systemResidualCpuCoreEquivalent =
        ($systemResidualCpuTicks / [TimeSpan]::TicksPerSecond) / $intervalSeconds
    $externalCpuPercent =
        $externalCpuCoreEquivalent / $logicalProcessorCount * 100.0
    $externalReadBytesPerSecond = $externalReadBytes / $intervalSeconds
    $externalWriteBytesPerSecond = $externalWriteBytes / $intervalSeconds
    $gateState = Get-EnvironmentMonitorGateState `
        -ExternalCpuPercent $externalCpuPercent `
        -ExternalCpuCoreEquivalent $externalCpuCoreEquivalent `
        -ExternalReadBytesPerSecond $externalReadBytesPerSecond `
        -ExternalWriteBytesPerSecond $externalWriteBytesPerSecond `
        -HasProhibitedExternalProcess ($prohibited.Count -gt 0) `
        -UnobservableAllowedCpuProcessCount $unobservableAllowedCpuProcessCount `
        -PreviousConsecutiveBusySamples $consecutiveBusySamples `
        -PreviouslyContaminated $contaminated `
        -MaximumExternalCpuPercent $MaxExternalCpuPercent `
        -MaximumExternalCpuCoreEquivalent $MaxExternalCpuCoreEquivalent `
        -MaximumExternalIoBytesPerSecond $MaxExternalIoBytesPerSecond `
        -ConsecutiveBusySamplesRequired $RequiredConsecutiveBusySamples
    $consecutiveBusySamples = $gateState.ConsecutiveBusySamples
    $contaminated = $gateState.Contaminated

    $row = @(
        $timestamp.ToString('O'),
        [Math]::Round($intervalSeconds * 1000.0, 3).ToString(
            [Globalization.CultureInfo]::InvariantCulture),
        [Math]::Round($externalCpuPercent, 4).ToString(
            [Globalization.CultureInfo]::InvariantCulture),
        [Math]::Round($externalCpuCoreEquivalent, 4).ToString(
            [Globalization.CultureInfo]::InvariantCulture),
        [Math]::Round($systemResidualCpuCoreEquivalent, 4).ToString(
            [Globalization.CultureInfo]::InvariantCulture),
        [Math]::Round($externalReadBytesPerSecond, 0).ToString(
            [Globalization.CultureInfo]::InvariantCulture),
        [Math]::Round($externalWriteBytesPerSecond, 0).ToString(
            [Globalization.CultureInfo]::InvariantCulture),
        $externalProcessCount,
        $allowedProcessCount,
        $unobservableAllowedCpuProcessCount,
        $unobservableExternalCpuProcessCount,
        $unobservableExternalIoProcessCount,
        ($prohibited -join ';'),
        $gateState.BusyReason,
        $consecutiveBusySamples,
        $contaminated) |
        ForEach-Object { Convert-ToCsvCell $_ }
    [IO.File]::AppendAllText(
        $resolvedOutputPath,
        ($row -join ',') + [Environment]::NewLine)

    $previousTimestamp = $timestamp
    $previousSnapshot = $snapshot
    $previousSystemCpuTimes = $systemCpuTimes

    if ($stopRequested) {
        break
    }
}
