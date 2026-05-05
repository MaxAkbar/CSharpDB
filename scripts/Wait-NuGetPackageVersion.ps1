param(
    [Parameter(Mandatory = $true)]
    [string] $Version,

    [Parameter(Mandatory = $true)]
    [string[]] $PackageId,

    [int] $TimeoutSeconds = 600,

    [int] $PollSeconds = 20,

    [string] $SourceBaseUrl = "https://api.nuget.org/v3-flatcontainer"
)

$ErrorActionPreference = 'Stop'

if ([string]::IsNullOrWhiteSpace($Version)) {
    throw "Version is required."
}

if ($PackageId.Count -eq 0) {
    throw "At least one package ID is required."
}

if ($TimeoutSeconds -le 0) {
    throw "TimeoutSeconds must be greater than zero."
}

if ($PollSeconds -le 0) {
    throw "PollSeconds must be greater than zero."
}

$normalizedVersion = $Version.Trim().ToLowerInvariant()
$pending = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
foreach ($id in $PackageId) {
    if (-not [string]::IsNullOrWhiteSpace($id)) {
        [void] $pending.Add($id.Trim())
    }
}

if ($pending.Count -eq 0) {
    throw "At least one non-empty package ID is required."
}

$baseUrl = $SourceBaseUrl.TrimEnd('/')
$deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)

while ($pending.Count -gt 0 -and [DateTimeOffset]::UtcNow -lt $deadline) {
    foreach ($id in @($pending)) {
        $lowerId = $id.ToLowerInvariant()
        $packageUrl = "$baseUrl/$lowerId/$normalizedVersion/$lowerId.$normalizedVersion.nupkg"

        try {
            $response = Invoke-WebRequest -Uri $packageUrl -Method Head -UseBasicParsing -TimeoutSec 15
            if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 300) {
                Write-Host "NuGet package is visible: $id $Version"
                [void] $pending.Remove($id)
            }
        }
        catch {
            $statusCode = $null
            if ($_.Exception.Response -and $_.Exception.Response.StatusCode) {
                $statusCode = [int]$_.Exception.Response.StatusCode
            }

            if ($statusCode -eq 404) {
                Write-Host "NuGet package is not visible yet: $id $Version"
            }
            else {
                Write-Warning "NuGet verification for $id $Version failed transiently: $($_.Exception.Message)"
            }
        }
    }

    if ($pending.Count -eq 0) {
        break
    }

    Start-Sleep -Seconds ([Math]::Min($PollSeconds, [Math]::Max(1, [int]($deadline - [DateTimeOffset]::UtcNow).TotalSeconds)))
}

if ($pending.Count -gt 0) {
    $missing = ($pending | Sort-Object) -join ', '
    throw "Timed out waiting for NuGet package version $Version to become visible for: $missing"
}
