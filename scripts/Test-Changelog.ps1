#requires -Version 7.0
[CmdletBinding()]
param(
    [string] $RepositoryRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..')),
    [switch] $VerifyPublishedReleases
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Get-Attribute([string] $Attributes, [string] $Name) {
    $match = [regex]::Match($Attributes, '\b' + [regex]::Escape($Name) + '\s*=\s*"([^"]*)"')
    return $match.Groups[1].Value
}

$html = [IO.File]::ReadAllText((Join-Path $RepositoryRoot 'www/changelog.html'))
$publication = Get-Content -Raw -LiteralPath (Join-Path $RepositoryRoot 'docs/releases/changelog-publication.json') | ConvertFrom-Json
$notes = [IO.File]::ReadAllText((Join-Path $RepositoryRoot 'RELEASE_NOTES.md')).Replace("`r`n", "`n").Trim()
[xml] $props = Get-Content -Raw -LiteralPath (Join-Path $RepositoryRoot 'src/Directory.Build.props')
$packageVersion = [string] $props.Project.PropertyGroup.Version
$notesVersion = [regex]::Match($notes, '(?m)^## CSharpDB (\S+)\s*$').Groups[1].Value
$firstHeading = [regex]::Match($notes, '(?m)^## .+$').Value.Trim()
if ($firstHeading -cne "## CSharpDB $packageVersion" -or $notesVersion -cne $packageVersion) {
    throw 'Changelog: the first RELEASE_NOTES.md section must name the current package version. Keep Unreleased on the website, not ahead of the release section consumed by the publisher.'
}
$digest = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData([Text.Encoding]::UTF8.GetBytes($notes))).ToLowerInvariant()
if ($publication.notesReview.version -cne $notesVersion -or $publication.notesReview.sha256 -cne $digest) {
    throw "Changelog: RELEASE_NOTES.md changed without a changelog review. Review the website summary, then record notesReview version '$notesVersion' and SHA256 '$digest' in docs/releases/changelog-publication.json."
}

$approved = @{}
$previous = [version]'999999.0.0'
foreach ($release in $publication.releases) {
    if ($release.version -cnotmatch '^[0-9]+\.[0-9]+\.[0-9]+$' -or $approved.ContainsKey($release.version)) {
        throw "Changelog: invalid or duplicate published version '$($release.version)'."
    }
    $date = [datetime]::ParseExact($release.publishedDate, 'yyyy-MM-dd', [Globalization.CultureInfo]::InvariantCulture)
    if ($date -gt [datetime]::UtcNow.Date -or [version]$release.version -ge $previous) {
        throw 'Changelog: published releases must have real past publication dates and be sorted newest version first.'
    }
    $approved[$release.version] = $release.publishedDate
    $previous = [version]$release.version
}
if ($approved.Count -eq 0 -or $previous -ne [version]$publication.coverageFrom) {
    throw 'Changelog: the publication record must cover the declared history boundary.'
}
$latest = $publication.releases[0].version
if ($html -notmatch ('href="#v' + [regex]::Escape($latest) + '">Latest release</a>')) {
    throw 'Changelog: the Latest release link must point to the newest verified publication.'
}

$articles = @([regex]::Matches($html, '(?s)<article\b(?<attributes>[^>]*)>(?<body>.*?)</article>'))
$seen = @{}
$unreleasedCount = 0
$currentFound = $false
$previous = [version]'999999.0.0'
foreach ($article in $articles) {
    $attributes = $article.Groups['attributes'].Value
    $body = $article.Groups['body'].Value
    $status = Get-Attribute $attributes 'data-release-status'
    $version = Get-Attribute $attributes 'data-release-version'
    if ($status -eq 'unreleased') {
        $unreleasedCount++
        if ($article -ne $articles[0] -or $body -notmatch '<span\b[^>]*>\s*Unreleased\s*</span>' -or $body -match '<time\b') {
            throw 'Changelog: Unreleased must be first, visibly labelled, and must not have a publication date.'
        }
        if ($version -and ($approved.ContainsKey($version) -or $version -cne $notesVersion)) {
            throw 'Changelog: a versioned Unreleased entry must be the current unshipped release candidate.'
        }
    }
    elseif ($status -eq 'published') {
        if (-not $approved.ContainsKey($version)) {
            throw "Changelog: v$version has no verified publication record. A Git tag or release candidate is not a published release."
        }
        if ([version]$version -ge $previous) { throw 'Changelog: published entries must be newest version first.' }
        $previous = [version]$version
        $date = $approved[$version]
        if ($body -notmatch ('<time\b[^>]*datetime="' + [regex]::Escape($date) + '"') -or
            $body -notmatch ('href="https://github\.com/MaxAkbar/CSharpDB/releases/tag/v' + [regex]::Escape($version) + '"') -or
            $body -notmatch ('<h3>v' + [regex]::Escape($version) + ' — ')) {
            throw "Changelog: v$version needs its verified UTC date, matching version heading, and full GitHub release link."
        }
    }
    else { throw "Changelog: unknown release status '$status'." }
    if ($version) {
        if ($seen.ContainsKey($version)) { throw "Changelog: duplicate entry v$version." }
        $seen[$version] = $status
        if ($version -ceq $notesVersion) { $currentFound = $true }
    }
}
if ($unreleasedCount -ne 1 -or -not $currentFound) {
    throw 'Changelog: include exactly one Unreleased section and an entry for the current release-notes version.'
}
foreach ($version in $approved.Keys) {
    if (-not $seen.ContainsKey($version) -or $seen[$version] -ne 'published') {
        throw "Changelog: missing published release v$version."
    }
}

# Optional live reconciliation is read-only. Normal CI remains deterministic/offline.
# Never infer publication from tags or rewrite publication records automatically.
if ($VerifyPublishedReleases) {
    $live = @{}
    $page = 1
    do {
        $response = Invoke-RestMethod -Uri "https://api.github.com/repos/MaxAkbar/CSharpDB/releases?per_page=100&page=$page" -Headers @{ 'User-Agent' = 'CSharpDB-Changelog-Check'; Accept = 'application/vnd.github+json' }
        foreach ($release in $response) {
            if ($release.draft -or $release.prerelease -or $release.tag_name -cnotmatch '^v([0-9]+\.[0-9]+\.[0-9]+)$') { continue }
            $version = $Matches[1]
            if ([version]$version -lt [version]$publication.coverageFrom) { continue }
            $live[$version] = ([datetimeoffset]$release.published_at).UtcDateTime.ToString('yyyy-MM-dd')
        }
        $page++
        if ($page -gt 100) { throw 'Changelog: release pagination exceeded the safety bound.' }
    } while ($response.Count -eq 100)
    if ($live.Count -ne $approved.Count) { throw 'Changelog: GitHub publication history changed. Review and update the publication record and website.' }
    foreach ($version in $live.Keys) {
        if (-not $approved.ContainsKey($version) -or $approved[$version] -cne $live[$version]) {
            throw "Changelog: publication record for v$version differs from GitHub."
        }
    }
}
Write-Host "Changelog validation passed: $($approved.Count) verified release entries, separate Unreleased section, and current release-note review."
