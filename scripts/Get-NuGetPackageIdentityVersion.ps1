[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string] $Version
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$requestedVersion = $Version.Trim()
if ($requestedVersion -notmatch '^[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$') {
    throw "The package version is not a supported semantic version: $Version"
}

# NuGet retains SemVer 2 build metadata in the nuspec, but package identity,
# local artifact filenames, and flat-container URLs omit the +metadata suffix.
$metadataSeparator = $requestedVersion.IndexOf('+')
if ($metadataSeparator -ge 0) {
    $requestedVersion = $requestedVersion.Substring(0, $metadataSeparator)
}

Write-Output $requestedVersion
