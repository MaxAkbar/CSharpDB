#requires -Version 7.0
[CmdletBinding()]
param()
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$validator = Join-Path $PSScriptRoot 'Test-Changelog.ps1'
$fixture = Join-Path ([IO.Path]::GetTempPath()) ('csharpdb-changelog-tests-' + [guid]::NewGuid().ToString('N'))
$files = @('www/changelog.html', 'docs/releases/changelog-publication.json', 'RELEASE_NOTES.md', 'src/Directory.Build.props')
$notes = "# What's New`n`n## CSharpDB 4.6.2`n`nA tested release.`n"
$record = @{
    coverageFrom = '4.2.0'
    notesReview = @{ version = '4.6.2'; sha256 = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData([Text.Encoding]::UTF8.GetBytes($notes.Trim()))).ToLowerInvariant() }
    releases = @(@{ version='4.6.2'; publishedDate='2026-08-14' }, @{ version='4.5.1'; publishedDate='2026-08-10' }, @{ version='4.2.0'; publishedDate='2026-07-21' })
}
$fixtureHtml = '<a href="#v4.6.2">Latest release</a><article id="unreleased" data-release-status="unreleased"><span class="news-badge-unreleased">Unreleased</span><h2>Data Modeler</h2></article>'
foreach ($release in $record.releases) {
    $fixtureHtml += '<article id="v{0}" data-release-version="{0}" data-release-status="published"><h3>v{0} — Test</h3><time datetime="{1}">{1}</time><a href="https://github.com/MaxAkbar/CSharpDB/releases/tag/v{0}">Notes</a></article>' -f $release.version, $release.publishedDate
}
$originals = @{
    'www/changelog.html' = $fixtureHtml
    'docs/releases/changelog-publication.json' = ($record | ConvertTo-Json -Depth 5)
    'RELEASE_NOTES.md' = $notes
    'src/Directory.Build.props' = '<Project><PropertyGroup><Version>4.6.2</Version></PropertyGroup></Project>'
}
$script:cases = 0
$changelogLivePages = @{}

# Mimic Invoke-RestMethod's single array response, including pagination; never use the network in these tests.
function Invoke-RestMethod($Uri, $Headers) {
    $page = [int]([regex]::Match($Uri, '&page=([0-9]+)').Groups[1].Value)
    if (-not $changelogLivePages.ContainsKey($page)) { throw 'Unexpected network request in fixture test.' }
    return ,$changelogLivePages[$page]
}
function Set-Fixture([string] $Path, [string] $Text) { [IO.File]::WriteAllText((Join-Path $fixture $Path), $Text) }
function Get-Fixture([string] $Path) { return [IO.File]::ReadAllText((Join-Path $fixture $Path)) }
function Replace-Html([string] $From, [string] $To) { Set-Fixture 'www/changelog.html' ((Get-Fixture 'www/changelog.html').Replace($From, $To)) }
function Set-Candidate([string] $Version = '4.6.3') {
    Set-Fixture 'src/Directory.Build.props' ((Get-Fixture 'src/Directory.Build.props').Replace('<Version>4.6.2</Version>', "<Version>$Version</Version>"))
    $notes = (Get-Fixture 'RELEASE_NOTES.md').Replace('## CSharpDB 4.6.2', "## CSharpDB $Version")
    Set-Fixture 'RELEASE_NOTES.md' $notes
    $record = Get-Fixture 'docs/releases/changelog-publication.json' | ConvertFrom-Json
    $record.notesReview.version = $Version
    $record.notesReview.sha256 = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData([Text.Encoding]::UTF8.GetBytes($notes.Replace("`r`n", "`n").Trim()))).ToLowerInvariant()
    Set-Fixture 'docs/releases/changelog-publication.json' ($record | ConvertTo-Json -Depth 5)
    Replace-Html 'id="unreleased" data-release-status="unreleased"' ('id="unreleased" data-release-version="' + $Version + '" data-release-status="unreleased"')
}
function Check-Case([string] $Name, [scriptblock] $Change, [string] $ExpectedFailure = '', [switch] $Online) {
    foreach ($file in $files) {
        $path = Join-Path $fixture $file
        [IO.Directory]::CreateDirectory([IO.Path]::GetDirectoryName($path)) | Out-Null
        [IO.File]::WriteAllText($path, $originals[$file])
    }
    & $Change
    $failure = ''
    try { & $validator -RepositoryRoot $fixture -VerifyPublishedReleases:$Online 6>$null }
    catch { $failure = $_.Exception.Message }
    if (($ExpectedFailure -eq '' -and $failure) -or ($ExpectedFailure -ne '' -and $failure -notmatch $ExpectedFailure)) {
        throw "Changelog guardrail '$Name' failed: expected '$ExpectedFailure'; got '$failure'."
    }
    $script:cases++
}
try {
    Check-Case 'valid publication fixture' {}
    Check-Case 'changed release notes' { Set-Fixture 'RELEASE_NOTES.md' ((Get-Fixture 'RELEASE_NOTES.md') + "`nA new feature.") } 'without a changelog review'
    Check-Case 'LF/CRLF parity' { Set-Fixture 'RELEASE_NOTES.md' ((Get-Fixture 'RELEASE_NOTES.md').Replace("`r`n", "`n").Replace("`n", "`r`n")) }
    Check-Case 'package version drift' { Set-Fixture 'src/Directory.Build.props' ((Get-Fixture 'src/Directory.Build.props').Replace('<Version>4.6.2</Version>', '<Version>4.6.3</Version>')) } 'first RELEASE_NOTES'
    Check-Case 'unreleased before publisher notes' { Set-Fixture 'RELEASE_NOTES.md' ("## Unreleased`n`n" + (Get-Fixture 'RELEASE_NOTES.md')) } 'first RELEASE_NOTES'
    Check-Case 'missing published entry' { Set-Fixture 'www/changelog.html' ([regex]::Replace((Get-Fixture 'www/changelog.html'), '(?s)<article[^>]*id="v4.2.0".*?</article>', '')) } 'missing published release'
    Check-Case 'wrong date' { Replace-Html 'datetime="2026-08-14"' 'datetime="2026-08-13"' } 'verified UTC date'
    Check-Case 'wrong release link' { Replace-Html 'releases/tag/v4.6.2' 'releases/tag/v4.6.0' } 'full GitHub release link'
    Check-Case 'stale latest link' { Replace-Html 'href="#v4.6.2"' 'href="#v4.5.1"' } 'Latest release link'
    Check-Case 'failed tag called published' { Replace-Html 'data-release-version="4.5.1"' 'data-release-version="4.5.0"' } 'no verified publication record'
    Check-Case 'duplicate entry' { Replace-Html 'data-release-version="4.5.1"' 'data-release-version="4.6.2"' } 'newest version first'
    Check-Case 'missing Unreleased label' { Replace-Html '>Unreleased</span>' '>Release</span>' } 'visibly labelled'
    Check-Case 'dated Unreleased' { Replace-Html '<h2>Data Modeler' '<time datetime="2026-09-04">September 4</time><h2>Data Modeler' } 'must not have a publication date'
    Check-Case 'duplicate publication metadata' {
        $record = Get-Fixture 'docs/releases/changelog-publication.json' | ConvertFrom-Json
        $record.releases += $record.releases[0]
        Set-Fixture 'docs/releases/changelog-publication.json' ($record | ConvertTo-Json -Depth 5)
    } 'duplicate published version'
    Check-Case 'release candidate remains unshipped' { Set-Candidate }
    Check-Case 'preview candidate does not become a stable release' { Set-Candidate '4.6.3-preview.1' }
    Check-Case 'candidate missing from website' { Set-Candidate; Replace-Html 'data-release-version="4.6.3" ' '' } 'current release-notes version'
    Check-Case 'candidate prematurely published' { Set-Candidate; Replace-Html 'id="unreleased" data-release-version="4.6.3" data-release-status="unreleased"' 'id="unreleased" data-release-version="4.6.3" data-release-status="published"' } 'no verified publication record'
    $record = $originals['docs/releases/changelog-publication.json'] | ConvertFrom-Json
    $live = @($record.releases | ForEach-Object { [pscustomobject]@{ draft=$false; prerelease=$false; tag_name="v$($_.version)"; published_at="$($_.publishedDate)T06:00:00Z" } })
    $changelogLivePages = @{ 1 = $live }
    Check-Case 'live API array shape' {} -Online
    $changelogLivePages = @{ 1 = @($live[0]) * 100; 2 = $live }
    Check-Case 'live API pagination' {} -Online
    $changelogLivePages = @{ 1 = @($live | Select-Object -Skip 1) }
    Check-Case 'live publication disappears' {} 'publication history changed' -Online
    $changelogLivePages = @{ 1 = @($live) + [pscustomobject]@{ draft=$false; prerelease=$false; tag_name='v4.6.3'; published_at='2026-09-04T06:00:00Z' } }
    Check-Case 'new live release missing locally' {} 'publication history changed' -Online
    $changelogLivePages = @{ 1 = @($live) + [pscustomobject]@{ draft=$true; prerelease=$false; tag_name='v4.6.3'; published_at=$null } + [pscustomobject]@{ draft=$false; prerelease=$true; tag_name='v4.6.3-preview'; published_at='2026-09-04T06:00:00Z' } }
    Check-Case 'draft and preview excluded' {} -Online
    Write-Host "Changelog guardrails passed: $script:cases fixture cases."
}
finally {
    $resolved = [IO.Path]::GetFullPath($fixture)
    $temporaryRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
    if (-not $resolved.StartsWith($temporaryRoot, [StringComparison]::OrdinalIgnoreCase) -or
        [IO.Path]::GetFileName($resolved) -notmatch '^csharpdb-changelog-tests-[0-9a-f]{32}$') { throw 'Unsafe fixture cleanup path.' }
    if (Test-Path -LiteralPath $resolved) { Remove-Item -LiteralPath $resolved -Recurse -Force }
}
