[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$wwwRoot = Join-Path $repoRoot 'www'
$errors = [Collections.Generic.List[string]]::new()

function Add-DocumentationError {
    param([Parameter(Mandatory)][string]$Message)

    $script:errors.Add($Message)
}

$internalLinkPattern = '(?i)(?:href|src)\s*=\s*["''](?<url>[^"'']+)["'']'
foreach ($htmlFile in Get-ChildItem -LiteralPath $wwwRoot -Recurse -File -Filter '*.html') {
    $html = [IO.File]::ReadAllText($htmlFile.FullName)
    foreach ($match in [regex]::Matches($html, $internalLinkPattern)) {
        $url = $match.Groups['url'].Value
        if ($url -match '^(?:https?:|mailto:|tel:|javascript:|data:|//|#)' -or $url.Contains('${')) {
            continue
        }

        $relativeUrl = ($url -split '[?#]')[0]
        if ([string]::IsNullOrWhiteSpace($relativeUrl)) {
            continue
        }

        try {
            $decodedUrl = [Uri]::UnescapeDataString($relativeUrl)
            $candidate = if ($decodedUrl.StartsWith('/')) {
                Join-Path $wwwRoot $decodedUrl.TrimStart('/')
            }
            else {
                Join-Path $htmlFile.DirectoryName $decodedUrl
            }
            if ($decodedUrl.EndsWith('/')) {
                $candidate = Join-Path $candidate 'index.html'
            }
            $candidate = [IO.Path]::GetFullPath($candidate)
            if (-not $candidate.StartsWith($wwwRoot, [StringComparison]::OrdinalIgnoreCase)) {
                Add-DocumentationError "Internal link escapes the published www root: $($htmlFile.FullName) -> $url"
            }
            elseif (-not (Test-Path -LiteralPath $candidate)) {
                Add-DocumentationError "Broken internal link: $($htmlFile.FullName) -> $url"
            }
        }
        catch {
            Add-DocumentationError "Invalid internal link: $($htmlFile.FullName) -> $url ($($_.Exception.Message))"
        }
    }
}

try {
    [xml]$sitemap = [IO.File]::ReadAllText((Join-Path $wwwRoot 'sitemap.xml'))
    $locations = @($sitemap.SelectNodes("//*[local-name()='loc']") | ForEach-Object { [string]$_.InnerText })
    foreach ($location in $locations) {
        $uri = [Uri]$location
        if (-not [string]::Equals($uri.Host, 'csharpdb.com', [StringComparison]::OrdinalIgnoreCase)) {
            continue
        }
        $sitePath = [Uri]::UnescapeDataString($uri.AbsolutePath.TrimStart('/'))
        if ([string]::IsNullOrEmpty($sitePath)) {
            $sitePath = 'index.html'
        }
        elseif ($sitePath.EndsWith('/')) {
            $sitePath += 'index.html'
        }
        $publishedPath = [IO.Path]::GetFullPath((Join-Path $wwwRoot $sitePath))
        if (-not (Test-Path -LiteralPath $publishedPath -PathType Leaf)) {
            Add-DocumentationError "sitemap.xml references a missing published page: $location"
        }
    }
}
catch {
    Add-DocumentationError "sitemap.xml validation failed: $($_.Exception.Message)"
}

if ($errors.Count -gt 0) {
    $message = "Documentation validation failed with $($errors.Count) error(s):`n - " + ($errors -join "`n - ")
    throw $message
}

Write-Host 'Documentation validation passed: internal links and sitemap targets are valid.'
