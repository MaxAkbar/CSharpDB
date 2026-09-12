[CmdletBinding()]
param()
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$fixture = Join-Path ([IO.Path]::GetTempPath()) "csharpdb-website-check-$([Guid]::NewGuid().ToString('N'))"
[void][IO.Directory]::CreateDirectory($fixture)
$page = '<!doctype html><html><head><meta name="description" content="Fixture"><link rel="canonical" href="https://csharpdb.com/index.html"></head><body><div id="site-nav"></div><main id="main"><a href="other.html#target">Section</a></main><div id="site-footer"></div><script src="js/csharpdb.bundle.js" defer></script></body></html>'
$other = $page.Replace('id="main"', 'id="target"').Replace('https://csharpdb.com/index.html', 'https://csharpdb.com/other.html')
try {
    [void][IO.Directory]::CreateDirectory((Join-Path $fixture 'js'))
    [IO.File]::WriteAllText((Join-Path $fixture 'js/csharpdb.bundle.js'), '// Fixture shared components')
    [IO.File]::WriteAllText((Join-Path $fixture 'index.html'), $page)
    [IO.File]::WriteAllText((Join-Path $fixture 'other.html'), $other)
    & (Join-Path $PSScriptRoot 'Test-Website.ps1') -SiteRoot $fixture
    foreach ($test in @(
        @('broken section', $page.Replace('other.html#target', 'other.html#missing'), 'Broken section link'),
        @('missing file', $page.Replace('other.html#target', 'missing.html'), 'Broken internal link'),
        @('missing description', $page.Replace('<meta name="description" content="Fixture">', ''), 'Missing description'),
        @('missing canonical', $page.Replace('<link rel="canonical" href="https://csharpdb.com/index.html">', ''), 'Missing canonical'),
        @('wrong canonical path', $page.Replace('https://csharpdb.com/index.html', 'https://csharpdb.com/missing.html'), 'Canonical URL does not match page'),
        @('missing navigation placeholder', $page.Replace('<div id="site-nav"></div>', ''), 'Missing shared navigation placeholder'),
        @('duplicated footer', $page.Replace('</body>', '<footer class="site-footer">Copied footer</footer></body>'), 'Duplicated shared component markup')
    )) {
        [IO.File]::WriteAllText((Join-Path $fixture 'index.html'), $test[1])
        $failure = $null
        try { & (Join-Path $PSScriptRoot 'Test-Website.ps1') -SiteRoot $fixture } catch { $failure = $_.Exception.Message }
        if (-not $failure -or -not $failure.Contains($test[2])) { throw "Guardrail did not reject $($test[0]): $failure" }
    }
    Write-Host 'Website guardrails passed: valid links accepted; seven regression cases rejected.'
}
finally {
    # The exact UUID-named directory was created by this script beneath the system temp directory.
    $resolved = [IO.Path]::GetFullPath($fixture)
    $tempBoundary = [IO.Path]::GetFullPath([IO.Path]::GetTempPath()).TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar
    if (-not $resolved.StartsWith($tempBoundary, [StringComparison]::OrdinalIgnoreCase) -or [IO.Path]::GetFileName($resolved) -notmatch '^csharpdb-website-check-[a-f0-9]{32}$') { throw 'Unexpected fixture cleanup path.' }
    Remove-Item -LiteralPath $resolved -Recurse -Force
}
