[CmdletBinding()]
param(
    [string]$ManifestPath = 'artifacts/nuget/CSharpDB-PACKAGE-MANIFEST.json',

    [string]$SourceBaseUrl = 'https://api.nuget.org/v3-flatcontainer'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$versionResolver = Join-Path $PSScriptRoot 'Get-NuGetPackageIdentityVersion.ps1'
$resolvedManifest = if ([IO.Path]::IsPathRooted($ManifestPath)) {
    [IO.Path]::GetFullPath($ManifestPath)
}
else {
    [IO.Path]::GetFullPath((Join-Path $repoRoot $ManifestPath))
}

if (-not (Test-Path -LiteralPath $resolvedManifest -PathType Leaf)) {
    throw "The candidate package manifest was not found: $resolvedManifest"
}
if (-not (Test-Path -LiteralPath $versionResolver -PathType Leaf)) {
    throw "The NuGet package-version resolver was not found: $versionResolver"
}

$manifest = Get-Content -Raw -LiteralPath $resolvedManifest | ConvertFrom-Json
if ([string]::IsNullOrWhiteSpace([string]$manifest.candidateVersion) -or
    $null -eq $manifest.packages -or
    @($manifest.packages).Count -eq 0) {
    throw 'The candidate package manifest is incomplete.'
}

$identityVersion = (& $versionResolver `
    -Version ([string]$manifest.candidateVersion) |
        Select-Object -Last 1).Trim().ToLowerInvariant()
$baseUrl = $SourceBaseUrl.TrimEnd('/')

foreach ($package in $manifest.packages) {
    $packageId = [string]$package.id
    if ([string]::IsNullOrWhiteSpace($packageId)) {
        throw 'The candidate package manifest contains an empty package id.'
    }

    $lowerId = $packageId.ToLowerInvariant()
    $packageUrl = "$baseUrl/$lowerId/$identityVersion/$lowerId.$identityVersion.nupkg"
    try {
        $response = Invoke-WebRequest `
            -Uri $packageUrl `
            -Method Head `
            -UseBasicParsing `
            -TimeoutSec 20
        if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 300) {
            throw "$packageId $($manifest.candidateVersion) is already published; the immutable candidate release must stop before pushing any package."
        }
        throw "NuGet returned unexpected HTTP $($response.StatusCode) while checking $packageId."
    }
    catch {
        $statusCode = $null
        $responseProperty = $_.Exception.PSObject.Properties['Response']
        $errorResponse = if ($null -eq $responseProperty) {
            $null
        }
        else {
            $responseProperty.Value
        }
        if ($null -ne $errorResponse -and
            $null -ne $errorResponse.StatusCode) {
            $statusCode = [int]$errorResponse.StatusCode
        }
        if ($statusCode -eq 404) {
            continue
        }
        throw
    }
}

Write-Host "NuGet candidate version $($manifest.candidateVersion) is absent for all $(@($manifest.packages).Count) release packages."
