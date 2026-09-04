#requires -Version 7.0
[CmdletBinding()]
param(
    [string] $RepositoryRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..')),
    [switch] $VerifyPublishedDownloads
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$html = [IO.File]::ReadAllText((Join-Path $RepositoryRoot 'www/downloads.html'))
$record = Get-Content -Raw -LiteralPath (Join-Path $RepositoryRoot 'docs/releases/downloads-publication.json') | ConvertFrom-Json
$history = Get-Content -Raw -LiteralPath (Join-Path $RepositoryRoot 'docs/releases/changelog-publication.json') | ConvertFrom-Json
$version = $record.version
if ($version -cnotmatch '^\d+\.\d+\.\d+$' -or $version -cne $history.releases[0].version -or
    $record.publishedDate -cne $history.releases[0].publishedDate) {
    throw 'Downloads: version and date must match the latest verified changelog publication, not the working-tree package version.'
}
$releaseUrl = "https://github.com/MaxAkbar/CSharpDB/releases/tag/v$version"
$assetPrefix = "https://github.com/MaxAkbar/CSharpDB/releases/download/v$version/"
foreach ($required in @(
    "data-download-version=`"$version`"",
    "href=`"$releaseUrl`">Latest stable: v$version</a>",
    "datetime=`"$($record.publishedDate)`"",
    "dotnet add package CSharpDB --version $version</code>",
    "git clone --branch v$version --depth 1 ",
    "href=`"changelog.html#v$version`""
)) {
    if (-not $html.Contains($required)) { throw "Downloads: missing or stale release display/command: $required" }
}
foreach ($section in @('admin', 'nuget', 'server', 'cli', 'native', 'nodejs', 'verify', 'all-releases')) {
    if ($html -notmatch ('\bid="' + $section + '"')) { throw "Downloads: missing section '$section'." }
}
if ($html -match '\bnpm\s+(?:install|i)\s+csharpdb(?:@[^\s<]+)?(?:\s|<|$)') {
    throw 'Downloads: do not advertise the unpublished npm package. Use the local source-build instructions.'
}

$assets = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
$packages = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
$packagePattern = '^(.+)\.' + [regex]::Escape($version) + '\.nupkg$'
foreach ($asset in $record.assets) {
    if ($asset -cnotmatch '^[A-Za-z0-9][A-Za-z0-9._-]+$' -or -not $assets.Add($asset)) {
        throw "Downloads: invalid or duplicate asset '$asset'."
    }
    if ($asset -cmatch $packagePattern) { [void] $packages.Add($Matches[1]) }
    elseif ($asset.EndsWith('.nupkg')) { throw "Downloads: package asset '$asset' belongs to a different version." }
}
if ($packages.Count -eq 0) { throw 'Downloads: publication inventory must include NuGet packages.' }
$linkedAssets = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
$linkedPackages = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
foreach ($link in [regex]::Matches($html, '<a\b[^>]*\bhref="([^"]+)"')) {
    $url = $link.Groups[1].Value
    if ($url.StartsWith('https://github.com/MaxAkbar/CSharpDB/releases/download/') -or
        $url.StartsWith('https://github.com/MaxAkbar/CSharpDB/releases/latest/download/')) {
        if (-not $url.StartsWith($assetPrefix, [StringComparison]::Ordinal)) {
            throw "Downloads: asset URL must be pinned to v${version}: $url"
        }
        $name = $url.Substring($assetPrefix.Length)
        if (-not $assets.Contains($name)) { throw "Downloads: unknown release asset '$name'." }
        [void] $linkedAssets.Add($name)
    }
    elseif ($url.StartsWith('https://www.nuget.org/packages/')) {
        $match = [regex]::Match($url, '^https://www\.nuget\.org/packages/([^/]+)/' + [regex]::Escape($version) + '$')
        if (-not $match.Success -or -not $packages.Contains($match.Groups[1].Value)) {
            throw "Downloads: unknown or unpinned NuGet package: $url"
        }
        [void] $linkedPackages.Add($match.Groups[1].Value)
    }
}
foreach ($asset in $assets) {
    if (-not $asset.EndsWith('.nupkg') -and -not $linkedAssets.Contains($asset)) {
        throw "Downloads: missing direct link for release asset '$asset'."
    }
}
if (-not $linkedPackages.SetEquals($packages)) { throw 'Downloads: package list does not cover the published NuGet inventory.' }

# Check platform labels against their actual targets, not just existence in the inventory.
foreach ($card in [regex]::Matches($html, '(?s)<div class="dl-card">\s*<h3>([^<]+)</h3>(.*?)</div>')) {
    $heading = $card.Groups[1].Value
    $platformPattern = switch -CaseSensitive ($heading) {
        'Windows x64' { '(?:-win-x64\.zip|CSharpDB\.Native\.dll)' }
        'Linux x64' { '(?:-linux-x64\.tar\.gz|CSharpDB\.Native\.so)' }
        'macOS Apple silicon' { '(?:-osx-arm64\.tar\.gz|CSharpDB\.Native\.dylib)' }
        default { $null }
    }
    if ($platformPattern -and $card.Groups[2].Value -cnotmatch ('href="' + [regex]::Escape($assetPrefix) + '[^"/]*' + $platformPattern + '"')) {
        throw "Downloads: platform card '$heading' links to a different platform."
    }
}

# Optional, read-only reconciliation. Normal documentation CI uses the reviewed local inventory.
if ($VerifyPublishedDownloads) {
    $live = Invoke-RestMethod -Uri 'https://api.github.com/repos/MaxAkbar/CSharpDB/releases/latest' -TimeoutSec 30 -Headers @{ 'User-Agent'='CSharpDB-Downloads-Check'; Accept='application/vnd.github+json' }
    if ($live.draft -or $live.prerelease -or $live.tag_name -cne "v$version" -or
        ([datetimeoffset]$live.published_at).UtcDateTime.ToString('yyyy-MM-dd') -cne $record.publishedDate) {
        throw 'Downloads: latest published release changed. Review the changelog and download inventory before updating the page.'
    }
    $liveAssets = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    foreach ($asset in $live.assets) {
        if ($asset.state -ne 'uploaded' -or $asset.size -le 0 -or $asset.browser_download_url -cne ($assetPrefix + $asset.name)) {
            throw "Downloads: live asset '$($asset.name)' is incomplete or has an unexpected URL."
        }
        [void] $liveAssets.Add($asset.name)
    }
    if (-not $assets.SetEquals($liveAssets)) { throw 'Downloads: release asset inventory differs from GitHub.' }
    foreach ($package in $packages) {
        $nuget = Invoke-RestMethod -Uri "https://api.nuget.org/v3-flatcontainer/$($package.ToLowerInvariant())/index.json" -TimeoutSec 30
        if ($version -cnotin $nuget.versions) { throw "Downloads: $package v$version is not published on NuGet." }
    }
}
Write-Host "Downloads validation passed: v$version, $($linkedAssets.Count) direct asset links, $($packages.Count) NuGet packages."
