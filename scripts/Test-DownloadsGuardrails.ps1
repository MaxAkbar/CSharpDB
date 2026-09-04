#requires -Version 7.0
[CmdletBinding()]
param()
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$validator = Join-Path $PSScriptRoot 'Test-Downloads.ps1'
$fixture = Join-Path ([IO.Path]::GetTempPath()) ('csharpdb-downloads-tests-' + [guid]::NewGuid().ToString('N'))
$prefix = 'https://github.com/MaxAkbar/CSharpDB/releases/download/v4.6.2/'
$inventory = @('csharpdb-daemon-v4.6.2-win-x64.zip', 'csharpdb-daemon-v4.6.2-linux-x64.tar.gz', 'SHA256SUMS.txt', 'CSharpDB.4.6.2.nupkg', 'CSharpDB.Client.4.6.2.nupkg')
$record = @{ version='4.6.2'; publishedDate='2026-08-14'; assets=$inventory }
$fixtureHtml = @'
<main data-download-version="4.6.2">
<a href="https://github.com/MaxAkbar/CSharpDB/releases/tag/v4.6.2">Latest stable: v4.6.2</a>
<time datetime="2026-08-14">August 14</time><a href="changelog.html#v4.6.2">Notes</a>
<code>dotnet add package CSharpDB --version 4.6.2</code>
<section id="admin"></section><section id="nuget">
<a href="https://www.nuget.org/packages/CSharpDB/4.6.2">CSharpDB</a>
<a href="https://www.nuget.org/packages/CSharpDB.Client/4.6.2">Client</a>
</section><section id="server">
<div class="dl-card"><h3>Windows x64</h3><a href="https://github.com/MaxAkbar/CSharpDB/releases/download/v4.6.2/csharpdb-daemon-v4.6.2-win-x64.zip">Windows</a></div>
<div class="dl-card"><h3>Linux x64</h3><a href="https://github.com/MaxAkbar/CSharpDB/releases/download/v4.6.2/csharpdb-daemon-v4.6.2-linux-x64.tar.gz">Linux</a></div>
<a href="https://github.com/MaxAkbar/CSharpDB/releases/download/v4.6.2/SHA256SUMS.txt">Checksums</a>
</section><section id="cli"></section><section id="native"></section>
<details id="nodejs"><code>git clone --branch v4.6.2 --depth 1 https://github.com/MaxAkbar/CSharpDB.git</code><code>npm install /path/to/csharpdb-1.0.0.tgz</code></details>
<section id="verify"></section><section id="all-releases"></section></main>
'@
$originals = @{
    'www/downloads.html' = $fixtureHtml
    'docs/releases/downloads-publication.json' = ($record | ConvertTo-Json -Depth 5)
    'docs/releases/changelog-publication.json' = (@{ releases=@(@{version='4.6.2'; publishedDate='2026-08-14'}) } | ConvertTo-Json -Depth 5)
}
$liveOriginal = @{
    tag_name='v4.6.2'; published_at='2026-08-14T06:41:22Z'; draft=$false; prerelease=$false
    assets=@($inventory | ForEach-Object { @{name=$_; state='uploaded'; size=100; browser_download_url=$prefix + $_} })
} | ConvertTo-Json -Depth 5
$script:downloadCases = 0
$downloadsLiveRelease = $null
$downloadsNugetVersions = @()
$downloadsNetworkFailure = $false

# Called by the nested validator; dynamic scope keeps all fixture requests offline.
function Invoke-RestMethod($Uri, $Headers, $TimeoutSec) {
    if ($downloadsNetworkFailure) { throw 'Simulated network failure.' }
    if ($Uri -eq 'https://api.github.com/repos/MaxAkbar/CSharpDB/releases/latest') { return $downloadsLiveRelease }
    if ($Uri -match '^https://api\.nuget\.org/v3-flatcontainer/csharpdb(?:\.client)?/index\.json$') {
        return @{ versions=$downloadsNugetVersions }
    }
    throw "Unexpected network request in downloads fixture: $Uri"
}
function Set-Fixture([string] $Path, [string] $Text) { [IO.File]::WriteAllText((Join-Path $fixture $Path), $Text) }
function Get-Fixture([string] $Path) { [IO.File]::ReadAllText((Join-Path $fixture $Path)) }
function Replace-Html([string] $From, [string] $To) { Set-Fixture 'www/downloads.html' ((Get-Fixture 'www/downloads.html').Replace($From, $To)) }
function Edit-Record([scriptblock] $Edit) {
    $data = Get-Fixture 'docs/releases/downloads-publication.json' | ConvertFrom-Json
    & $Edit $data
    Set-Fixture 'docs/releases/downloads-publication.json' ($data | ConvertTo-Json -Depth 5)
}
function Check-Case([string] $Name, [scriptblock] $Change, [string] $ExpectedFailure='', [switch] $Online) {
    foreach ($file in $originals.Keys) {
        $path = Join-Path $fixture $file
        [void] [IO.Directory]::CreateDirectory([IO.Path]::GetDirectoryName($path))
        [IO.File]::WriteAllText($path, $originals[$file])
    }
    $downloadsLiveRelease = $liveOriginal | ConvertFrom-Json
    $downloadsNugetVersions = @('4.5.1', '4.6.2')
    $downloadsNetworkFailure = $false
    # Dot-source changes so the mock sees replacements in this check's scope.
    . $Change
    $failure = ''
    try { & $validator -RepositoryRoot $fixture -VerifyPublishedDownloads:$Online 6>$null }
    catch { $failure = $_.Exception.Message }
    if (($ExpectedFailure -eq '' -and $failure) -or ($ExpectedFailure -ne '' -and $failure -notmatch $ExpectedFailure)) {
        throw "Downloads guardrail '$Name' failed: expected '$ExpectedFailure'; got '$failure'."
    }
    $script:downloadCases++
}
try {
    Check-Case 'valid local inventory and local npm tarball' {}
    Check-Case 'missing section' { Replace-Html 'id="admin"' 'id="other"' } 'missing section'
    Check-Case 'stale page version' { Replace-Html 'data-download-version="4.6.2"' 'data-download-version="4.5.1"' } 'release display'
    Check-Case 'incorrect latest label' { Replace-Html '>Latest stable: v4.6.2' '>Latest stable: v4.6.3' } 'release display'
    Check-Case 'incorrect release date' { Replace-Html 'datetime="2026-08-14"' 'datetime="2026-08-13"' } 'release display'
    Check-Case 'unpinned install command' { Replace-Html ' --version 4.6.2</code>' '</code>' } 'release display'
    Check-Case 'stale Node source tag' { Replace-Html 'git clone --branch v4.6.2' 'git clone --branch v4.5.1' } 'release display'
    Check-Case 'wrong release link' { Replace-Html 'releases/tag/v4.6.2' 'releases/tag/v4.5.1' } 'release display'
    Check-Case 'wrong changelog anchor' { Replace-Html 'changelog.html#v4.6.2' 'changelog.html#v4.5.1' } 'release display'
    Check-Case 'inventory disagrees with publication' { Edit-Record { param($r) $r.version='4.6.3' } } 'latest verified changelog'
    Check-Case 'inventory date disagrees with publication' { Edit-Record { param($r) $r.publishedDate='2026-08-13' } } 'latest verified changelog'
    Check-Case 'unknown direct asset' { Replace-Html 'SHA256SUMS.txt' 'UNKNOWN.txt' } 'unknown release asset'
    Check-Case 'moving latest URL' { Replace-Html 'releases/download/v4.6.2/' 'releases/latest/download/' } 'must be pinned'
    Check-Case 'wrong asset version' { Replace-Html 'releases/download/v4.6.2/' 'releases/download/v4.5.1/' } 'must be pinned'
    Check-Case 'missing checksum link' { Replace-Html ('<a href="' + $prefix + 'SHA256SUMS.txt">Checksums</a>') '' } 'missing direct link'
    Check-Case 'unversioned NuGet URL' { Replace-Html 'packages/CSharpDB/4.6.2' 'packages/CSharpDB' } 'unknown or unpinned'
    Check-Case 'unknown package' { Replace-Html 'packages/CSharpDB.Client/' 'packages/CSharpDB.Unknown/' } 'unknown or unpinned'
    Check-Case 'missing package' { Replace-Html '<a href="https://www.nuget.org/packages/CSharpDB.Client/4.6.2">Client</a>' '' } 'does not cover'
    Check-Case 'duplicate inventory asset' { Edit-Record { param($r) $r.assets += $r.assets[0] } } 'duplicate asset'
    Check-Case 'different package version' { Edit-Record { param($r) $r.assets += 'CSharpDB.Engine.4.5.1.nupkg' } } 'different version'
    Check-Case 'unsafe asset name' { Edit-Record { param($r) $r.assets += '../escape.zip' } } 'invalid or duplicate'
    Check-Case 'platform labels swapped' { Replace-Html '<h3>Windows x64</h3>' '<h3>Linux x64</h3>' } 'different platform'
    Check-Case 'unpublished npm install' { Replace-Html 'npm install /path/to/csharpdb-1.0.0.tgz' 'npm install csharpdb' } 'unpublished npm'
    Check-Case 'unpublished npm short install with version' { Replace-Html 'npm install /path/to/csharpdb-1.0.0.tgz' 'npm i csharpdb@1.0.0' } 'unpublished npm'
    Check-Case 'valid live release and NuGet' {} -Online
    Check-Case 'new published release' { $downloadsLiveRelease.tag_name='v4.6.3' } 'latest published release changed' -Online
    Check-Case 'draft release' { $downloadsLiveRelease.draft=$true } 'latest published release changed' -Online
    Check-Case 'prerelease' { $downloadsLiveRelease.prerelease=$true } 'latest published release changed' -Online
    Check-Case 'live date drift' { $downloadsLiveRelease.published_at='2026-08-15T06:41:22Z' } 'latest published release changed' -Online
    Check-Case 'missing live asset' { $downloadsLiveRelease.assets=@($downloadsLiveRelease.assets | Select-Object -Skip 1) } 'inventory differs' -Online
    Check-Case 'new live asset' { $downloadsLiveRelease.assets += [pscustomobject]@{name='new.txt'; state='uploaded'; size=1; browser_download_url=$prefix+'new.txt'} } 'inventory differs' -Online
    Check-Case 'empty live asset' { $downloadsLiveRelease.assets[0].size=0 } 'incomplete or.*unexpected URL' -Online
    Check-Case 'incomplete upload' { $downloadsLiveRelease.assets[0].state='starter' } 'incomplete or.*unexpected URL' -Online
    Check-Case 'wrong live asset URL' { $downloadsLiveRelease.assets[0].browser_download_url='https://example.invalid/file' } 'unexpected URL' -Online
    Check-Case 'NuGet version absent' { $downloadsNugetVersions=@('4.5.1') } 'not published on NuGet' -Online
    Check-Case 'network failure is not success' { $downloadsNetworkFailure=$true } 'Simulated network failure' -Online
    Write-Host "Downloads guardrails passed: $script:downloadCases fixture cases."
}
finally {
    $resolved = [IO.Path]::GetFullPath($fixture)
    $temporaryRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
    if (-not $resolved.StartsWith($temporaryRoot, [StringComparison]::OrdinalIgnoreCase) -or
        [IO.Path]::GetFileName($resolved) -notmatch '^csharpdb-downloads-tests-[0-9a-f]{32}$') { throw 'Unsafe downloads fixture cleanup path.' }
    if (Test-Path -LiteralPath $resolved) { Remove-Item -LiteralPath $resolved -Recurse -Force }
}
