function Remove-CSharpDbDirectoryWithRetry {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$LiteralPath,

        [ValidateRange(0, 60000)]
        [int]$TimeoutMilliseconds = 10000,

        [ValidateRange(1, 10000)]
        [int]$RetryDelayMilliseconds = 250
    )

    if (-not (Test-Path -LiteralPath $LiteralPath)) {
        return
    }

    if (-not $IsWindows) {
        Remove-Item -LiteralPath $LiteralPath -Recurse -Force
        return
    }

    $timer = [Diagnostics.Stopwatch]::StartNew()
    while ($true) {
        try {
            Remove-Item `
                -LiteralPath $LiteralPath `
                -Recurse `
                -Force `
                -ErrorAction Stop
            return
        }
        catch {
            if (-not (Test-Path -LiteralPath $LiteralPath)) {
                return
            }
            if ($timer.ElapsedMilliseconds -ge $TimeoutMilliseconds) {
                throw
            }

            $remainingMilliseconds =
                $TimeoutMilliseconds - [int]$timer.ElapsedMilliseconds
            $delayMilliseconds = [Math]::Min(
                $RetryDelayMilliseconds,
                [Math]::Max(1, $remainingMilliseconds))
            Start-Sleep -Milliseconds $delayMilliseconds
        }
    }
}
