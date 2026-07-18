[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$wwwRoot = Join-Path $repoRoot 'www'
$manifestPath = Join-Path $wwwRoot 'docs/sql-compatibility.json'
$schemaPath = Join-Path $wwwRoot 'docs/sql-compatibility.schema.json'
$generatedPath = Join-Path $wwwRoot 'docs/sql-compatibility.html'
$errors = [Collections.Generic.List[string]]::new()

function Add-DocumentationError {
    param([Parameter(Mandatory)][string]$Message)

    $script:errors.Add($Message)
}

function Resolve-RepositoryPath {
    param([Parameter(Mandatory)][string]$RelativePath)

    return [IO.Path]::GetFullPath((Join-Path $repoRoot ($RelativePath -replace '/', [IO.Path]::DirectorySeparatorChar)))
}

function Test-FragmentExists {
    param(
        [Parameter(Mandatory)][string]$FilePath,
        [Parameter(Mandatory)][string]$Fragment
    )

    $content = [IO.File]::ReadAllText($FilePath)
    $escaped = [regex]::Escape($Fragment)
    return [regex]::IsMatch($content, "(?i)(?:id|name)\s*=\s*[`"']$escaped[`"']")
}

foreach ($requiredFile in @($manifestPath, $schemaPath, $generatedPath)) {
    if (-not (Test-Path -LiteralPath $requiredFile -PathType Leaf)) {
        Add-DocumentationError "Required documentation artifact is missing: $requiredFile"
    }
}

if ($errors.Count -gt 0) {
    throw ($errors -join [Environment]::NewLine)
}

$manifestJson = [IO.File]::ReadAllText($manifestPath)
try {
    $schemaValid = $manifestJson | Test-Json -SchemaFile $schemaPath -ErrorAction Stop
    if (-not $schemaValid) {
        Add-DocumentationError 'sql-compatibility.json does not satisfy sql-compatibility.schema.json.'
    }
}
catch {
    Add-DocumentationError "SQL compatibility JSON Schema validation failed: $($_.Exception.Message)"
}

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

$featureIds = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
$proofIds = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
$referencedProofIds = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
$proofById = [Collections.Generic.Dictionary[string, object]]::new([StringComparer]::Ordinal)

foreach ($proof in @($manifest.proofs)) {
    $proofId = [string]$proof.id
    if (-not $proofIds.Add($proofId)) {
        Add-DocumentationError "Duplicate compatibility proof id: $proofId"
        continue
    }

    $proofById.Add($proofId, $proof)
    if (-not [bool]$proof.ci_executed) {
        Add-DocumentationError "Proof '$proofId' is not marked as CI-executed."
    }

    $projectPath = Resolve-RepositoryPath ([string]$proof.project)
    $sourcePath = Resolve-RepositoryPath ([string]$proof.source)
    if (-not (Test-Path -LiteralPath $projectPath -PathType Leaf)) {
        Add-DocumentationError "Proof '$proofId' references a missing test project: $($proof.project)"
    }
    if (-not (Test-Path -LiteralPath $sourcePath -PathType Leaf)) {
        Add-DocumentationError "Proof '$proofId' references a missing source file: $($proof.source)"
    }
    else {
        $sourceText = [IO.File]::ReadAllText($sourcePath)
        $testPattern = "\b$([regex]::Escape([string]$proof.test))\s*\("
        if (-not [regex]::IsMatch($sourceText, $testPattern)) {
            Add-DocumentationError "Proof '$proofId' references test '$($proof.test)', which was not found in $($proof.source)."
        }
    }
}

foreach ($feature in @($manifest.features)) {
    $featureId = [string]$feature.id
    if (-not $featureIds.Add($featureId)) {
        Add-DocumentationError "Duplicate SQL compatibility feature id: $featureId"
    }

    $expectedAppliesTo = @(
        foreach ($facetName in $facetOrder) {
            if ([string]$feature.facets.$facetName -ne 'not_applicable') {
                $facetName
            }
        }
    )
    $actualAppliesTo = @($feature.applies_to | ForEach-Object { [string]$_ })
    if (($expectedAppliesTo -join '|') -cne ($actualAppliesTo -join '|')) {
        Add-DocumentationError "Feature '$featureId' applies_to must exactly match its non-not_applicable facets in canonical order. Expected [$($expectedAppliesTo -join ', ')], found [$($actualAppliesTo -join ', ')]."
    }

    $applicableStatuses = @($expectedAppliesTo | ForEach-Object { [string]$feature.facets.$_ })
    $allSupported = @($applicableStatuses | Where-Object { $_ -ne 'supported' }).Count -eq 0
    $hasAvailableBehavior = @($applicableStatuses | Where-Object { $_ -in @('supported', 'partial') }).Count -gt 0
    $derivedAvailability = if ($allSupported) {
        'supported'
    }
    elseif ($hasAvailableBehavior) {
        'partial'
    }
    else {
        'unsupported'
    }

    if ([string]$feature.availability -cne $derivedAvailability) {
        Add-DocumentationError "Feature '$featureId' declares availability '$($feature.availability)' but facets derive '$derivedAvailability'."
    }

    $hasFirstSupportedVersion = $feature.PSObject.Properties.Name -contains 'first_supported_version'
    if ($derivedAvailability -eq 'unsupported') {
        if ($hasFirstSupportedVersion) {
            Add-DocumentationError "Unsupported feature '$featureId' must omit first_supported_version."
        }
    }
    elseif (-not $hasFirstSupportedVersion -or [string]::IsNullOrWhiteSpace([string]$feature.first_supported_version)) {
        Add-DocumentationError "Feature '$featureId' must provide a non-empty first_supported_version."
    }

    $positiveIds = @($feature.positive_proof_ids | ForEach-Object { [string]$_ })
    $negativeIds = @($feature.negative_proof_ids | ForEach-Object { [string]$_ })
    if ($derivedAvailability -eq 'supported' -and $positiveIds.Count -eq 0) {
        Add-DocumentationError "Supported feature '$featureId' has no positive proof ids."
    }
    if ($derivedAvailability -eq 'partial') {
        if ($positiveIds.Count -eq 0) {
            Add-DocumentationError "Partial feature '$featureId' has no positive proof ids."
        }
        if ($negativeIds.Count -eq 0) {
            Add-DocumentationError "Partial feature '$featureId' has no negative or rejection proof ids."
        }
        if (@($feature.limitations).Count -eq 0) {
            Add-DocumentationError "Partial feature '$featureId' has no explicit limitation."
        }
    }

    foreach ($proofId in $positiveIds) {
        [void]$referencedProofIds.Add($proofId)
        if (-not $proofById.ContainsKey($proofId)) {
            Add-DocumentationError "Feature '$featureId' references missing positive proof '$proofId'."
        }
        elseif ([string]$proofById[$proofId].kind -cne 'positive') {
            Add-DocumentationError "Feature '$featureId' lists '$proofId' as positive, but the proof registry marks it '$($proofById[$proofId].kind)'."
        }
    }
    foreach ($proofId in $negativeIds) {
        [void]$referencedProofIds.Add($proofId)
        if (-not $proofById.ContainsKey($proofId)) {
            Add-DocumentationError "Feature '$featureId' references missing negative proof '$proofId'."
        }
        elseif ([string]$proofById[$proofId].kind -cne 'negative') {
            Add-DocumentationError "Feature '$featureId' lists '$proofId' as negative, but the proof registry marks it '$($proofById[$proofId].kind)'."
        }
    }

    if ($derivedAvailability -eq 'supported' -and [string]$feature.facets.execution -eq 'supported') {
        $hasExecutionProof = @(
            $positiveIds | Where-Object {
                $proofById.ContainsKey($_) -and [string]$proofById[$_].dimension -eq 'execution'
            }
        ).Count -gt 0
        if (-not $hasExecutionProof) {
            Add-DocumentationError "Supported executable feature '$featureId' has no positive execution proof."
        }
    }

    if ($derivedAvailability -eq 'supported' -and [bool]$feature.persistent_ddl) {
        foreach ($requiredDimension in @('parser', 'execution', 'persistence', 'catalog')) {
            $hasDimension = @(
                $positiveIds | Where-Object {
                    $proofById.ContainsKey($_) -and [string]$proofById[$_].dimension -eq $requiredDimension
                }
            ).Count -gt 0
            if (-not $hasDimension) {
                Add-DocumentationError "Supported persistent DDL feature '$featureId' has no '$requiredDimension' proof."
            }
        }
    }

    $anchor = [string]$feature.documentation_anchor
    $anchorParts = $anchor.Split('#', 2)
    $targetPath = [IO.Path]::GetFullPath((Join-Path (Split-Path $manifestPath -Parent) $anchorParts[0]))
    if (-not (Test-Path -LiteralPath $targetPath -PathType Leaf)) {
        Add-DocumentationError "Feature '$featureId' documentation target does not exist: $anchor"
    }
    elseif ($anchorParts.Count -eq 2 -and -not (Test-FragmentExists -FilePath $targetPath -Fragment $anchorParts[1])) {
        Add-DocumentationError "Feature '$featureId' documentation fragment does not exist: $anchor"
    }
}

foreach ($proofId in $proofIds) {
    if (-not $referencedProofIds.Contains($proofId)) {
        Add-DocumentationError "Orphaned compatibility proof id is not referenced by any feature: $proofId"
    }
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

$requiredLinkChecks = @(
    @{ Path = 'README.md'; Text = 'https://csharpdb.com/docs/sql-compatibility.html' },
    @{ Path = 'www/docs/index.html'; Text = 'href="sql-compatibility.html"' },
    @{ Path = 'www/docs/sql.html'; Text = 'href="sql-compatibility.html"' },
    @{ Path = 'www/docs/sql-reference.html'; Text = 'href="sql-compatibility.html"' },
    @{ Path = 'www/roadmap.html'; Text = 'href="docs/sql-compatibility.html"' },
    @{ Path = 'www/roadmap-reference.html'; Text = 'href="docs/sql-compatibility.html"' },
    @{ Path = 'www/js/csharpdb.bundle.js'; Text = 'docs/sql-compatibility.html' },
    @{ Path = 'www/sitemap.xml'; Text = 'https://csharpdb.com/docs/sql-compatibility.html' }
)
foreach ($check in $requiredLinkChecks) {
    $checkPath = Resolve-RepositoryPath $check.Path
    if (-not (Test-Path -LiteralPath $checkPath -PathType Leaf)) {
        Add-DocumentationError "Required matrix link source is missing: $($check.Path)"
        continue
    }
    if (-not [IO.File]::ReadAllText($checkPath).Contains([string]$check.Text, [StringComparison]::Ordinal)) {
        Add-DocumentationError "Required matrix link is missing from $($check.Path): $($check.Text)"
    }
}

try {
    [xml]$sitemap = [IO.File]::ReadAllText((Join-Path $wwwRoot 'sitemap.xml'))
    $locations = @($sitemap.SelectNodes("//*[local-name()='loc']") | ForEach-Object { [string]$_.InnerText })
    if (@($locations | Where-Object { $_ -ceq 'https://csharpdb.com/docs/sql-compatibility.html' }).Count -ne 1) {
        Add-DocumentationError 'sitemap.xml must contain exactly one SQL compatibility page entry.'
    }

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

[xml]$buildProps = [IO.File]::ReadAllText((Join-Path $repoRoot 'src/Directory.Build.props'))
$packageVersion = [string]($buildProps.Project.PropertyGroup.Version | Select-Object -First 1)
if ([string]$manifest.verified_against.package_version -cne $packageVersion) {
    Add-DocumentationError "Manifest package version '$($manifest.verified_against.package_version)' does not match src/Directory.Build.props '$packageVersion'."
}

$generatedHtml = [IO.File]::ReadAllText($generatedPath)
if ([string]$manifest.verified_against.state -eq 'development' -and -not $generatedHtml.Contains('Development preview.', [StringComparison]::Ordinal)) {
    Add-DocumentationError 'Development compatibility HTML is not visibly labeled as a development preview.'
}
$visibleGeneratedHtml = [regex]::Replace($generatedHtml, '(?is)<(?:style|script)\b.*?</(?:style|script)>', '')
if ([regex]::IsMatch($visibleGeneratedHtml, '\b[0-9]+(?:\.[0-9]+)?%')) {
    Add-DocumentationError 'Compatibility HTML must not publish an aggregate percentage.'
}

try {
    & (Join-Path $PSScriptRoot 'Build-SqlCompatibilityMatrix.ps1') -Check
}
catch {
    Add-DocumentationError $_.Exception.Message
}

if ($errors.Count -gt 0) {
    $message = "Documentation validation failed with $($errors.Count) error(s):`n - " + ($errors -join "`n - ")
    throw $message
}

Write-Host "Documentation validation passed: $($manifest.features.Count) compatibility rows, $($manifest.proofs.Count) proof ids, internal links, navigation, sitemap, schema, and generated HTML."
