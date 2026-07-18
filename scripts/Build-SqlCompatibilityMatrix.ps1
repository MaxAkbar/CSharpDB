[CmdletBinding()]
param(
    [switch]$Check
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$manifestPath = Join-Path $repoRoot 'www/docs/sql-compatibility.json'
$outputPath = Join-Path $repoRoot 'www/docs/sql-compatibility.html'

if (-not (Test-Path -LiteralPath $manifestPath -PathType Leaf)) {
    throw "SQL compatibility manifest not found: $manifestPath"
}

function ConvertTo-HtmlText {
    param([AllowNull()][object]$Value)

    if ($null -eq $Value) {
        return ''
    }

    return [Net.WebUtility]::HtmlEncode([string]$Value)
}

function ConvertTo-CssToken {
    param([Parameter(Mandatory)][string]$Value)

    return ($Value -replace '[^a-zA-Z0-9_-]', '-').ToLowerInvariant()
}

function Add-HtmlLine {
    param(
        [Parameter(Mandatory)][Text.StringBuilder]$Builder,
        [AllowEmptyString()][string]$Text = ''
    )

    [void]$Builder.Append($Text)
    [void]$Builder.Append("`n")
}

function Add-StringList {
    param(
        [Parameter(Mandatory)][Text.StringBuilder]$Builder,
        [Parameter(Mandatory)][string]$Heading,
        [Parameter(Mandatory)][AllowEmptyCollection()][object[]]$Items
    )

    if ($Items.Count -eq 0) {
        return
    }

    Add-HtmlLine $Builder "                        <h4>$(ConvertTo-HtmlText $Heading)</h4>"
    Add-HtmlLine $Builder '                        <ul class="compat-list">'
    foreach ($item in $Items) {
        Add-HtmlLine $Builder "                            <li>$(ConvertTo-HtmlText $item)</li>"
    }
    Add-HtmlLine $Builder '                        </ul>'
}

$manifestJson = [IO.File]::ReadAllText($manifestPath)
$manifest = $manifestJson | ConvertFrom-Json -Depth 100

$facetOrder = @(
    'parser',
    'execution',
    'persistence',
    'catalog',
    'embedded_ado_net',
    'rest_stateless',
    'rest_session',
    'grpc_unary',
    'grpc_session',
    'ef_query',
    'ef_migration'
)

$facetLabels = @{
    parser = 'Parser'
    execution = 'Execution'
    persistence = 'Persistence'
    catalog = 'Catalog'
    embedded_ado_net = 'Embedded ADO.NET'
    rest_stateless = 'REST stateless'
    rest_session = 'REST session'
    grpc_unary = 'gRPC unary'
    grpc_session = 'gRPC session'
    ef_query = 'EF query'
    ef_migration = 'EF migration'
}

$features = @($manifest.features | Sort-Object category, feature, id)
$categories = @($features | ForEach-Object { $_.category } | Sort-Object -Unique)
$state = [string]$manifest.verified_against.state
$stateLabel = if ($state -eq 'released') { 'Released snapshot' } else { 'Development preview' }
$shortCommit = ([string]$manifest.verified_against.commit).Substring(0, 7)

$builder = [Text.StringBuilder]::new(131072)
Add-HtmlLine $builder '<!DOCTYPE html>'
Add-HtmlLine $builder '<html lang="en" data-theme="dark">'
Add-HtmlLine $builder '<head>'
Add-HtmlLine $builder '    <meta charset="UTF-8">'
Add-HtmlLine $builder '    <meta name="viewport" content="width=device-width, initial-scale=1.0">'
Add-HtmlLine $builder '    <link rel="stylesheet" href="../css/style.css">'
Add-HtmlLine $builder '    <link rel="preload" href="../js/csharpdb.bundle.js" as="script">'
Add-HtmlLine $builder '    <title>CSharpDB SQL Compatibility</title>'
Add-HtmlLine $builder '    <meta name="description" content="Feature-level compatibility matrix for the CSharpDB SQL dialect, including parser, execution, persistence, transport, ADO.NET, and EF Core facets.">'
Add-HtmlLine $builder '    <link rel="canonical" href="https://csharpdb.com/docs/sql-compatibility.html">'
Add-HtmlLine $builder '    <meta property="og:title" content="CSharpDB SQL Compatibility">'
Add-HtmlLine $builder '    <meta property="og:description" content="Evidence-backed feature compatibility for the CSharpDB SQL dialect.">'
Add-HtmlLine $builder '    <meta property="og:url" content="https://csharpdb.com/docs/sql-compatibility.html">'
Add-HtmlLine $builder '    <meta property="og:image" content="https://csharpdb.com/images/og-banner.png">'
Add-HtmlLine $builder '    <link rel="icon" type="image/png" href="../favicon.png">'
Add-HtmlLine $builder '    <style>'
Add-HtmlLine $builder '        .compat-toolbar { display: grid; grid-template-columns: minmax(220px, 2fr) minmax(160px, 1fr) minmax(160px, 1fr); gap: 12px; margin: 24px 0; }'
Add-HtmlLine $builder '        .compat-toolbar input, .compat-toolbar select { width: 100%; padding: 10px 12px; color: var(--text-primary); background: var(--bg-card); border: 1px solid var(--border); border-radius: var(--radius-sm); }'
Add-HtmlLine $builder '        .compat-card { margin: 18px 0; padding: 22px; background: var(--bg-card); border: 1px solid var(--border); border-radius: var(--radius-md); }'
Add-HtmlLine $builder '        .compat-card[hidden] { display: none; }'
Add-HtmlLine $builder '        .compat-card-head { display: flex; align-items: flex-start; justify-content: space-between; gap: 16px; }'
Add-HtmlLine $builder '        .compat-card h3 { margin: 0 0 5px; }'
Add-HtmlLine $builder '        .compat-id { color: var(--text-muted); font-family: var(--font-mono); font-size: .78rem; }'
Add-HtmlLine $builder '        .compat-badges { display: flex; flex-wrap: wrap; justify-content: flex-end; gap: 7px; }'
Add-HtmlLine $builder '        .compat-badge { display: inline-block; padding: 3px 9px; border-radius: 999px; font-size: .72rem; font-weight: 700; text-transform: uppercase; letter-spacing: .04em; }'
Add-HtmlLine $builder '        .compat-badge-supported { color: #22c55e; background: rgba(34, 197, 94, .13); }'
Add-HtmlLine $builder '        .compat-badge-partial { color: #f59e0b; background: rgba(245, 158, 11, .13); }'
Add-HtmlLine $builder '        .compat-badge-unsupported { color: #ef4444; background: rgba(239, 68, 68, .13); }'
Add-HtmlLine $builder '        .compat-badge-roadmap { color: var(--info); background: rgba(59, 130, 246, .13); }'
Add-HtmlLine $builder '        .compat-syntax { margin: 16px 0; overflow-x: auto; }'
Add-HtmlLine $builder '        .compat-syntax code { white-space: pre-wrap; }'
Add-HtmlLine $builder '        .facet-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(135px, 1fr)); gap: 8px; margin: 16px 0; }'
Add-HtmlLine $builder '        .facet { padding: 9px 10px; border: 1px solid var(--border); border-radius: var(--radius-sm); }'
Add-HtmlLine $builder '        .facet-name { display: block; color: var(--text-secondary); font-size: .73rem; }'
Add-HtmlLine $builder '        .facet-status { font-size: .76rem; font-weight: 700; text-transform: uppercase; }'
Add-HtmlLine $builder '        .facet-supported .facet-status { color: #22c55e; }'
Add-HtmlLine $builder '        .facet-partial .facet-status { color: #f59e0b; }'
Add-HtmlLine $builder '        .facet-unsupported .facet-status { color: #ef4444; }'
Add-HtmlLine $builder '        .facet-not_applicable { opacity: .55; }'
Add-HtmlLine $builder '        .compat-list { margin-top: 6px; }'
Add-HtmlLine $builder '        .compat-proof { font-family: var(--font-mono); font-size: .76rem; }'
Add-HtmlLine $builder '        .compat-meta { display: grid; grid-template-columns: repeat(auto-fit, minmax(185px, 1fr)); gap: 10px; margin: 18px 0; }'
Add-HtmlLine $builder '        .compat-meta > div { padding: 10px 12px; border: 1px solid var(--border); border-radius: var(--radius-sm); }'
Add-HtmlLine $builder '        .compat-meta span { display: block; color: var(--text-muted); font-size: .74rem; }'
Add-HtmlLine $builder '        @media (max-width: 720px) { .compat-toolbar { grid-template-columns: 1fr; } .compat-card-head { display: block; } .compat-badges { justify-content: flex-start; margin-top: 12px; } }'
Add-HtmlLine $builder '    </style>'
Add-HtmlLine $builder '</head>'
Add-HtmlLine $builder '<body>'
Add-HtmlLine $builder '    <script>'
Add-HtmlLine $builder "        window.currentPage = 'docs'; window.pagePathPrefix = '../';"
Add-HtmlLine $builder "        window.pageConfig = { title: 'CSharpDB SQL Compatibility', description: 'Feature-level compatibility for the CSharpDB SQL dialect.', keywords: 'CSharpDB SQL compatibility, SQL dialect, feature matrix', canonicalPath: 'docs/sql-compatibility.html', ogType: 'article' };"
Add-HtmlLine $builder '    </script>'
Add-HtmlLine $builder '    <div id="site-nav"></div>'
Add-HtmlLine $builder '    <main class="page">'
Add-HtmlLine $builder '        <div class="container doc-container">'
Add-HtmlLine $builder '            <div class="doc-content full-width">'
Add-HtmlLine $builder "                <h1>$(ConvertTo-HtmlText $manifest.title)</h1>"
Add-HtmlLine $builder "                <p class=`"lead`">$(ConvertTo-HtmlText $manifest.description)</p>"
if ($state -eq 'development') {
    Add-HtmlLine $builder '                <div class="callout callout-warning">'
    Add-HtmlLine $builder '                    <strong>Development preview.</strong> This page describes the audited main-branch snapshot below, not the latest released package. No released compatibility snapshot has been published yet.'
    Add-HtmlLine $builder '                </div>'
}
else {
    Add-HtmlLine $builder '                <div class="callout callout-info"><strong>Released snapshot.</strong> This matrix describes the package version shown below.</div>'
}
Add-HtmlLine $builder '                <p>Availability and roadmap intent are independent. A planned row may still be unavailable today. Partial rows state their exact limitations. No aggregate compatibility percentage is published.</p>'
Add-HtmlLine $builder '                <div class="compat-meta">'
Add-HtmlLine $builder "                    <div><span>Snapshot</span>$(ConvertTo-HtmlText $stateLabel)</div>"
Add-HtmlLine $builder "                    <div><span>Package</span>$(ConvertTo-HtmlText $manifest.verified_against.package_version)</div>"
Add-HtmlLine $builder "                    <div><span>Commit</span><code>$(ConvertTo-HtmlText $shortCommit)</code></div>"
Add-HtmlLine $builder "                    <div><span>Generated</span>$(ConvertTo-HtmlText $manifest.verified_against.generated_at)</div>"
Add-HtmlLine $builder '                </div>'
Add-HtmlLine $builder '                <p>Machine-readable sources: <a href="sql-compatibility.json">manifest JSON</a> and <a href="sql-compatibility.schema.json">JSON Schema</a>. Follow implementation sequencing in the <a href="sql-compatibility-roadmap.html">SQL Compatibility Roadmap</a>.</p>'
Add-HtmlLine $builder '                <div class="compat-toolbar" aria-label="Compatibility filters">'
Add-HtmlLine $builder '                    <label><span class="sr-only">Search features</span><input id="compat-search" type="search" placeholder="Search feature, syntax, or id"></label>'
Add-HtmlLine $builder '                    <label><span class="sr-only">Filter availability</span><select id="compat-status"><option value="">All availability</option><option value="supported">Supported</option><option value="partial">Partial</option><option value="unsupported">Unsupported</option></select></label>'
Add-HtmlLine $builder '                    <label><span class="sr-only">Filter category</span><select id="compat-category"><option value="">All categories</option>'
foreach ($category in $categories) {
    Add-HtmlLine $builder "                        <option value=`"$(ConvertTo-HtmlText (ConvertTo-CssToken $category))`">$(ConvertTo-HtmlText $category)</option>"
}
Add-HtmlLine $builder '                    </select></label>'
Add-HtmlLine $builder '                </div>'
Add-HtmlLine $builder "                <p id=`"compat-visible-count`" aria-live=`"polite`">Showing $($features.Count) feature rows.</p>"

foreach ($feature in $features) {
    $categoryToken = ConvertTo-CssToken ([string]$feature.category)
    $availabilityToken = ConvertTo-CssToken ([string]$feature.availability)
    $searchText = "$($feature.id) $($feature.category) $($feature.feature) $($feature.syntax) $($feature.limitations -join ' ') $($feature.deviations -join ' ')".ToLowerInvariant()
    Add-HtmlLine $builder "                <article class=`"compat-card`" id=`"feature-$(ConvertTo-HtmlText $feature.id)`" data-status=`"$(ConvertTo-HtmlText $feature.availability)`" data-category=`"$(ConvertTo-HtmlText $categoryToken)`" data-search=`"$(ConvertTo-HtmlText $searchText)`">"
    Add-HtmlLine $builder '                    <div class="compat-card-head">'
    Add-HtmlLine $builder '                        <div>'
    Add-HtmlLine $builder "                            <h3>$(ConvertTo-HtmlText $feature.feature)</h3>"
    Add-HtmlLine $builder "                            <div class=`"compat-id`">$(ConvertTo-HtmlText $feature.id) · $(ConvertTo-HtmlText $feature.category)</div>"
    Add-HtmlLine $builder '                        </div>'
    Add-HtmlLine $builder '                        <div class="compat-badges">'
    Add-HtmlLine $builder "                            <span class=`"compat-badge compat-badge-$availabilityToken`">$(ConvertTo-HtmlText $feature.availability)</span>"
    Add-HtmlLine $builder "                            <span class=`"compat-badge compat-badge-roadmap`">roadmap: $(ConvertTo-HtmlText (([string]$feature.roadmap) -replace '_', ' '))</span>"
    Add-HtmlLine $builder '                        </div>'
    Add-HtmlLine $builder '                    </div>'
    Add-HtmlLine $builder "                    <pre class=`"compat-syntax`"><code>$(ConvertTo-HtmlText $feature.syntax)</code></pre>"
    Add-HtmlLine $builder '                    <div class="facet-grid">'
    foreach ($facetName in $facetOrder) {
        $facetStatus = [string]$feature.facets.$facetName
        $facetStatusToken = ConvertTo-CssToken $facetStatus
        Add-HtmlLine $builder "                        <div class=`"facet facet-$facetStatusToken`"><span class=`"facet-name`">$(ConvertTo-HtmlText $facetLabels[$facetName])</span><span class=`"facet-status`">$(ConvertTo-HtmlText ($facetStatus -replace '_', ' '))</span></div>"
    }
    Add-HtmlLine $builder '                    </div>'
    Add-StringList -Builder $builder -Heading 'Limitations' -Items @($feature.limitations)
    Add-StringList -Builder $builder -Heading 'Intentional deviations' -Items @($feature.deviations)
    if (@($feature.positive_proof_ids).Count -gt 0 -or @($feature.negative_proof_ids).Count -gt 0) {
        Add-HtmlLine $builder '                        <details>'
        Add-HtmlLine $builder '                            <summary>Automated proof ids</summary>'
        if (@($feature.positive_proof_ids).Count -gt 0) {
            Add-HtmlLine $builder "                            <p><strong>Positive:</strong> <span class=`"compat-proof`">$(ConvertTo-HtmlText ($feature.positive_proof_ids -join ', '))</span></p>"
        }
        if (@($feature.negative_proof_ids).Count -gt 0) {
            Add-HtmlLine $builder "                            <p><strong>Negative:</strong> <span class=`"compat-proof`">$(ConvertTo-HtmlText ($feature.negative_proof_ids -join ', '))</span></p>"
        }
        Add-HtmlLine $builder '                        </details>'
    }
    $firstSupported = if ($feature.PSObject.Properties.Name -contains 'first_supported_version' -and $null -ne $feature.first_supported_version) { [string]$feature.first_supported_version } else { 'not supported' }
    Add-HtmlLine $builder "                    <p><strong>First supported version:</strong> $(ConvertTo-HtmlText $firstSupported) · <a href=`"$(ConvertTo-HtmlText $feature.documentation_anchor)`">Related documentation</a></p>"
    Add-HtmlLine $builder '                </article>'
}

Add-HtmlLine $builder '            </div>'
Add-HtmlLine $builder '        </div>'
Add-HtmlLine $builder '    </main>'
Add-HtmlLine $builder '    <div id="site-footer"></div>'
Add-HtmlLine $builder '    <script>'
Add-HtmlLine $builder '        (() => {'
Add-HtmlLine $builder "            const cards = [...document.querySelectorAll('.compat-card')];"
Add-HtmlLine $builder "            const search = document.getElementById('compat-search');"
Add-HtmlLine $builder "            const status = document.getElementById('compat-status');"
Add-HtmlLine $builder "            const category = document.getElementById('compat-category');"
Add-HtmlLine $builder "            const count = document.getElementById('compat-visible-count');"
Add-HtmlLine $builder '            const apply = () => {'
Add-HtmlLine $builder '                const query = search.value.trim().toLowerCase();'
Add-HtmlLine $builder '                let visible = 0;'
Add-HtmlLine $builder '                for (const card of cards) {'
Add-HtmlLine $builder "                    const show = (!query || card.dataset.search.includes(query)) && (!status.value || card.dataset.status === status.value) && (!category.value || card.dataset.category === category.value);"
Add-HtmlLine $builder '                    card.hidden = !show;'
Add-HtmlLine $builder '                    if (show) visible++;'
Add-HtmlLine $builder '                }'
Add-HtmlLine $builder '                count.textContent = `Showing ${visible} feature row${visible === 1 ? '''' : ''s''}.`;'
Add-HtmlLine $builder '            };'
Add-HtmlLine $builder "            search.addEventListener('input', apply);"
Add-HtmlLine $builder "            status.addEventListener('change', apply);"
Add-HtmlLine $builder "            category.addEventListener('change', apply);"
Add-HtmlLine $builder '        })();'
Add-HtmlLine $builder '    </script>'
Add-HtmlLine $builder '    <script src="../js/csharpdb.bundle.js" defer></script>'
Add-HtmlLine $builder '</body>'
Add-HtmlLine $builder '</html>'

$generated = $builder.ToString().Replace("`r`n", "`n")

if ($Check) {
    if (-not (Test-Path -LiteralPath $outputPath -PathType Leaf)) {
        throw "Generated SQL compatibility page is missing: $outputPath. Run scripts/Build-SqlCompatibilityMatrix.ps1."
    }

    $existing = [IO.File]::ReadAllText($outputPath).Replace("`r`n", "`n")
    if (-not [string]::Equals($existing, $generated, [StringComparison]::Ordinal)) {
        throw "Generated SQL compatibility page is stale: $outputPath. Run scripts/Build-SqlCompatibilityMatrix.ps1 and commit the result."
    }

    Write-Host "SQL compatibility HTML is current ($($features.Count) feature rows)."
    return
}

[IO.File]::WriteAllText($outputPath, $generated, [Text.UTF8Encoding]::new($false))
Write-Host "Generated $outputPath from $manifestPath ($($features.Count) feature rows)."
