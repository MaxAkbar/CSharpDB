[CmdletBinding()]
param([string]$SiteRoot = (Join-Path $PSScriptRoot '../www'))

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$siteDirectory = [IO.Path]::GetFullPath($SiteRoot)
$boundary = $siteDirectory.TrimEnd([IO.Path]::DirectorySeparatorChar, [IO.Path]::AltDirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar
$pages = @{}
$errors = [Collections.Generic.List[string]]::new()
$sharedLinks = [Collections.Generic.List[string]]::new()
$bundlePath = Join-Path $siteDirectory 'js/csharpdb.bundle.js'
if (Test-Path -LiteralPath $bundlePath -PathType Leaf) {
    $bundle = [IO.File]::ReadAllText($bundlePath)
    foreach ($match in [regex]::Matches($bundle, '(?:href|src)="\$\{prefix\}(?<url>[^"$]+)"|navLink\(''(?<url>[^'']+)''')) {
        $sharedLinks.Add($match.Groups['url'].Value)
    }
}
foreach ($file in Get-ChildItem -LiteralPath $siteDirectory -Recurse -File -Filter '*.html') {
    $html = [IO.File]::ReadAllText($file.FullName)
    $ids = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    foreach ($match in [regex]::Matches($html, '(?i)\bid\s*=\s*["''](?<id>[^"'']+)["'']')) {
        [void]$ids.Add([Net.WebUtility]::HtmlDecode($match.Groups['id'].Value))
    }
    $pages[$file.FullName] = @{ Html = $html; Ids = $ids }
}
if ($pages.Count -eq 0) { throw "No HTML pages found under $siteDirectory" }
foreach ($path in $pages.Keys) {
    $html = $pages[$path].Html
    $relative = [IO.Path]::GetRelativePath($siteDirectory, $path)
    foreach ($rule in @(
        @('description', '<meta\b(?=[^>]*\bname="description")(?=[^>]*\bcontent="[^"\s][^"]*")[^>]*>'),
        @('canonical', '<link\b(?=[^>]*\brel="canonical")(?=[^>]*\bhref="https://csharpdb\.com/[^"]*")[^>]*>'),
        @('shared navigation placeholder', '<div id="site-nav">\s*</div>'),
        @('shared footer placeholder', '<div id="site-footer">\s*</div>'),
        @('shared component script', '<script\b[^>]*src="(?:\.\./)*js/csharpdb\.bundle\.js(?:\?[^"\s]*)?"')
    )) {
        if (-not [regex]::IsMatch($html, $rule[1])) { $errors.Add("Missing $($rule[0]): $relative") }
    }
    $canonical = [regex]::Match($html, '<link\b[^>]*rel="canonical"[^>]*href="(?<url>[^"]+)"').Groups['url'].Value
    $expectedCanonical = 'https://csharpdb.com/' + $relative.Replace('\', '/')
    $canonicalPaths = @($expectedCanonical)
    if ($expectedCanonical.EndsWith('/index.html')) { $canonicalPaths += $expectedCanonical.Substring(0, $expectedCanonical.Length - 10) }
    if ($canonical -and $canonical -cnotin $canonicalPaths) { $errors.Add("Canonical URL does not match page: $relative -> $canonical") }
    if ($html -match '<footer\b[^>]*\bclass="[^"]*\bsite-footer\b|<nav\b[^>]*\bid="navbar"') { $errors.Add("Duplicated shared component markup: $relative") }
    $prefix = '../' * (($relative.Replace('\', '/') -split '/').Count - 1)
    $urls = @([regex]::Matches($html, '(?i)(?:href|src)\s*=\s*["''](?<url>[^"'']+)["'']') | ForEach-Object { $_.Groups['url'].Value })
    $urls += @($sharedLinks | ForEach-Object { $prefix + $_ })
    foreach ($rawUrl in $urls) {
        $url = [Net.WebUtility]::HtmlDecode($rawUrl)
        if ($url -match '^(?:[a-z][a-z0-9+.-]*:|//)' -or $url.Contains('${')) { continue }
        $parts = $url -split '#', 2
        $pathPart = [Uri]::UnescapeDataString(($parts[0] -split '\?', 2)[0])
        $candidate = if (-not $pathPart) { $path }
            elseif ($pathPart.StartsWith('/')) { Join-Path $siteDirectory $pathPart.TrimStart('/') }
            else { Join-Path ([IO.Path]::GetDirectoryName($path)) $pathPart }
        $candidate = [IO.Path]::GetFullPath($candidate)
        if (-not $candidate.StartsWith($boundary, [StringComparison]::OrdinalIgnoreCase) -and
            -not [string]::Equals($candidate, $siteDirectory, [StringComparison]::OrdinalIgnoreCase)) {
            $errors.Add("Internal link escapes website: $relative -> $url")
            continue
        }
        if (Test-Path -LiteralPath $candidate -PathType Container) { $candidate = Join-Path $candidate 'index.html' }
        if (-not (Test-Path -LiteralPath $candidate -PathType Leaf)) {
            $errors.Add("Broken internal link: $relative -> $url")
            continue
        }
        if ($parts.Count -eq 2 -and $parts[1] -and $pages.ContainsKey($candidate)) {
            $fragment = [Uri]::UnescapeDataString($parts[1])
            $target = $pages[$candidate]
            # Existing sidebar routers also accept short #sql -> #doc-sql / #api-sql aliases.
            if (-not $target.Ids.Contains($fragment) -and -not $target.Ids.Contains("doc-$fragment") -and -not $target.Ids.Contains("api-$fragment")) {
                $errors.Add("Broken section link: $relative -> $url")
            }
        }
    }
}
if ($errors.Count) { throw "Website validation failed:`n - $($errors -join "`n - ")" }
Write-Host "Website validation passed: $($pages.Count) pages have metadata, shared components, and valid local links and fragments."
