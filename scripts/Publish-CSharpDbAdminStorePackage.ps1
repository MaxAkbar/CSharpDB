<#
.SYNOPSIS
Publishes the CSharpDB Studio Microsoft Store package.

.DESCRIPTION
Publishes the existing CSharpDB.Admin web host and the WPF/WebView2 desktop
shell, stages them as a full-trust MSIX package, and creates a .msixupload
wrapper for Partner Center submission.

.PARAMETER Version
The package version. Accepts three-part or four-part numeric versions, with an
optional leading "v". Three-part versions are normalized to x.y.z.0.

.PARAMETER Runtime
The runtime identifier for the Windows app package. Defaults to win-x64.

.PARAMETER Configuration
The build configuration passed to dotnet publish. Defaults to Release.

.PARAMETER OutputRoot
The root folder for publish, stage, and package outputs. Defaults to
artifacts/admin-store.

.PARAMETER NoRestore
Passes --no-restore to dotnet publish.

.PARAMETER SigningCertificateThumbprint
Thumbprint of an installed package-signing certificate. The certificate subject
must match the MSIX manifest Publisher value.

.PARAMETER SigningCertificatePath
Path to a PFX package-signing certificate. The certificate subject must match
the MSIX manifest Publisher value.

.PARAMETER SigningCertificatePassword
Password for SigningCertificatePath, when the PFX requires one.

.PARAMETER SkipSigning
Creates the MSIX without signing it. Unsigned MSIX files cannot be installed
directly with App Installer and are useful only for packaging diagnostics.

.PARAMETER TrustLocalTestCertificate
Imports the auto-created local test certificate into LocalMachine\TrustedPeople
after signing. This requires running PowerShell as Administrator.

.EXAMPLE
.\scripts\Publish-CSharpDbAdminStorePackage.ps1 -Version 3.9.0
#>
[CmdletBinding()]
param(
    [string]$Version,
    [ValidateSet('win-x64')]
    [string]$Runtime = 'win-x64',
    [string]$Configuration = 'Release',
    [string]$OutputRoot,
    [string]$SigningCertificateThumbprint,
    [string]$SigningCertificatePath,
    [string]$SigningCertificatePassword,
    [switch]$SkipSigning,
    [switch]$TrustLocalTestCertificate,
    [switch]$NoRestore
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Resolve-RepoRoot {
    return (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
}

function Resolve-ReleaseVersion {
    param([string]$RequestedVersion)

    if (-not [string]::IsNullOrWhiteSpace($RequestedVersion)) {
        return $RequestedVersion.Trim()
    }

    if (-not [string]::IsNullOrWhiteSpace($env:GITHUB_REF_NAME) -and $env:GITHUB_REF_NAME -match '^v?\d+\.\d+\.\d+(\.\d+)?$') {
        return $env:GITHUB_REF_NAME
    }

    return '1.0.0'
}

function Convert-ToAppxVersion {
    param([string]$ReleaseVersion)

    $normalized = $ReleaseVersion.Trim()
    if ($normalized.StartsWith('v', [StringComparison]::OrdinalIgnoreCase)) {
        $normalized = $normalized.Substring(1)
    }

    if ($normalized -notmatch '^\d+\.\d+\.\d+(\.\d+)?$') {
        throw "MSIX package versions must be numeric: major.minor.patch[.revision]. Received '$ReleaseVersion'."
    }

    if (@($normalized.ToCharArray() | Where-Object { $_ -eq '.' }).Count -eq 2) {
        $normalized = "$normalized.0"
    }

    return $normalized
}

function Assert-SafeOutputRoot {
    param(
        [string]$RepoRoot,
        [string]$ResolvedOutputRoot
    )

    $fullRepoRoot = [System.IO.Path]::GetFullPath($RepoRoot)
    $fullOutputRoot = [System.IO.Path]::GetFullPath($ResolvedOutputRoot)
    $expectedArtifactsRoot = [System.IO.Path]::GetFullPath((Join-Path $fullRepoRoot 'artifacts'))

    if (-not $fullOutputRoot.StartsWith($expectedArtifactsRoot, [StringComparison]::OrdinalIgnoreCase)) {
        throw "OutputRoot must stay under the repository artifacts directory. Resolved path: $fullOutputRoot"
    }
}

function Find-WindowsSdkTool {
    param([string]$ToolName)

    $roots = @(
        ${env:ProgramFiles(x86)},
        $env:ProgramFiles
    ) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }

    foreach ($root in $roots) {
        $sdkBin = Join-Path $root 'Windows Kits\10\bin'
        if (-not (Test-Path $sdkBin)) {
            continue
        }

        $matches = @(Get-ChildItem -Path $sdkBin -Recurse -Filter $ToolName -ErrorAction SilentlyContinue |
            Where-Object { $_.FullName -match '\\x64\\' } |
            Sort-Object FullName -Descending)

        if ($matches.Count -gt 0) {
            return $matches[0].FullName
        }
    }

    throw "$ToolName was not found. Install the Windows SDK or Visual Studio MSIX packaging tools."
}

function Test-IsAdministrator {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = [Security.Principal.WindowsPrincipal]::new($identity)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Get-ManifestPublisher {
    param([xml]$Manifest)

    $publisher = $Manifest.Package.Identity.Publisher
    if ([string]::IsNullOrWhiteSpace($publisher)) {
        throw "The package manifest Identity Publisher value is required for MSIX signing."
    }

    return $publisher
}

function Get-OrCreateLocalTestCertificate {
    param([string]$Publisher)

    $friendlyName = 'CSharpDB Studio MSIX Local Test Certificate'
    $certificates = @(Get-ChildItem -Path Cert:\CurrentUser\My -CodeSigningCert |
        Where-Object {
            $_.Subject -eq $Publisher -and
            $_.FriendlyName -eq $friendlyName -and
            $_.NotAfter -gt (Get-Date).AddDays(30)
        } |
        Sort-Object NotAfter -Descending)

    if ($certificates.Count -gt 0) {
        return $certificates[0]
    }

    Write-Host "Creating local test signing certificate for $Publisher..."
    return New-SelfSignedCertificate `
        -Type Custom `
        -KeyUsage DigitalSignature `
        -CertStoreLocation 'Cert:\CurrentUser\My' `
        -TextExtension @('2.5.29.37={text}1.3.6.1.5.5.7.3.3', '2.5.29.19={text}') `
        -Subject $Publisher `
        -FriendlyName $friendlyName
}

function Export-SigningCertificate {
    param(
        [System.Security.Cryptography.X509Certificates.X509Certificate2]$Certificate,
        [string]$CertificatePath
    )

    Export-Certificate -Cert $Certificate -FilePath $CertificatePath -Force | Out-Null
}

function Invoke-SignMsixPackage {
    param(
        [string]$SignToolPath,
        [string]$MsixPath,
        [string]$Publisher,
        [string]$CertificateThumbprint,
        [string]$CertificatePath,
        [string]$CertificatePassword
    )

    if (-not [string]::IsNullOrWhiteSpace($CertificateThumbprint) -and -not [string]::IsNullOrWhiteSpace($CertificatePath)) {
        throw "Use either SigningCertificateThumbprint or SigningCertificatePath, not both."
    }

    $arguments = @('sign', '/fd', 'SHA256')

    if (-not [string]::IsNullOrWhiteSpace($CertificatePath)) {
        if (-not (Test-Path $CertificatePath)) {
            throw "Signing certificate was not found: $CertificatePath"
        }

        $certificate = Get-PfxCertificate -FilePath $CertificatePath
        if ($certificate.Subject -ne $Publisher) {
            throw "Signing certificate subject '$($certificate.Subject)' does not match manifest Publisher '$Publisher'."
        }

        $arguments += @('/f', $CertificatePath)
        if (-not [string]::IsNullOrWhiteSpace($CertificatePassword)) {
            $arguments += @('/p', $CertificatePassword)
        }
    }
    elseif (-not [string]::IsNullOrWhiteSpace($CertificateThumbprint)) {
        $certificate = Get-ChildItem -Path Cert:\CurrentUser\My, Cert:\LocalMachine\My -CodeSigningCert |
            Where-Object { $_.Thumbprint -eq $CertificateThumbprint } |
            Select-Object -First 1

        if ($null -eq $certificate) {
            throw "Signing certificate thumbprint was not found in CurrentUser\My or LocalMachine\My: $CertificateThumbprint"
        }

        if ($certificate.Subject -ne $Publisher) {
            throw "Signing certificate subject '$($certificate.Subject)' does not match manifest Publisher '$Publisher'."
        }

        $arguments += @('/sha1', $CertificateThumbprint)
    }
    else {
        throw "A signing certificate thumbprint or PFX path is required."
    }

    $arguments += $MsixPath

    Write-Host "Signing MSIX with $SignToolPath..."
    & $SignToolPath @arguments
    if ($LASTEXITCODE -ne 0) {
        throw "SignTool failed with exit code $LASTEXITCODE."
    }

    $signature = Get-AuthenticodeSignature -FilePath $MsixPath
    if ($signature.Status -eq 'NotSigned' -or $null -eq $signature.SignerCertificate) {
        throw "MSIX signing did not produce a signature. Status: $($signature.Status). $($signature.StatusMessage)"
    }
}

function Import-LocalTestCertificate {
    param([string]$CertificatePath)

    if (-not (Test-IsAdministrator)) {
        throw "TrustLocalTestCertificate requires an elevated PowerShell session. Re-run as Administrator or import '$CertificatePath' into Cert:\LocalMachine\TrustedPeople manually."
    }

    Write-Host "Trusting local test certificate in LocalMachine\TrustedPeople..."
    Import-Certificate -FilePath $CertificatePath -CertStoreLocation Cert:\LocalMachine\TrustedPeople | Out-Null
}

function New-PackageImage {
    param(
        [string]$SourcePath,
        [string]$DestinationPath,
        [int]$Width,
        [int]$Height
    )

    Add-Type -AssemblyName System.Drawing

    $destinationDirectory = Split-Path -Parent $DestinationPath
    New-Item -ItemType Directory -Path $destinationDirectory -Force | Out-Null

    $source = [System.Drawing.Image]::FromFile($SourcePath)
    try {
        $bitmap = New-Object System.Drawing.Bitmap($Width, $Height, [System.Drawing.Imaging.PixelFormat]::Format32bppArgb)
        try {
            $graphics = [System.Drawing.Graphics]::FromImage($bitmap)
            try {
                $graphics.Clear([System.Drawing.Color]::Transparent)
                $graphics.CompositingQuality = [System.Drawing.Drawing2D.CompositingQuality]::HighQuality
                $graphics.InterpolationMode = [System.Drawing.Drawing2D.InterpolationMode]::HighQualityBicubic
                $graphics.SmoothingMode = [System.Drawing.Drawing2D.SmoothingMode]::HighQuality

                $scale = [Math]::Min($Width / $source.Width, $Height / $source.Height)
                $targetWidth = [Math]::Max(1, [int]($source.Width * $scale))
                $targetHeight = [Math]::Max(1, [int]($source.Height * $scale))
                $targetX = [int](($Width - $targetWidth) / 2)
                $targetY = [int](($Height - $targetHeight) / 2)

                $graphics.DrawImage($source, $targetX, $targetY, $targetWidth, $targetHeight)
                $bitmap.Save($DestinationPath, [System.Drawing.Imaging.ImageFormat]::Png)
            }
            finally {
                $graphics.Dispose()
            }
        }
        finally {
            $bitmap.Dispose()
        }
    }
    finally {
        $source.Dispose()
    }
}

function Invoke-DotNetPublish {
    param(
        [string]$ProjectPath,
        [string]$OutputPath,
        [string]$Runtime,
        [string]$Configuration,
        [switch]$NoRestore
    )

    $arguments = @(
        'publish',
        $ProjectPath,
        '--configuration', $Configuration,
        '--runtime', $Runtime,
        '--self-contained', 'true',
        '--output', $OutputPath,
        '-p:PublishSingleFile=false'
    )

    if ($NoRestore.IsPresent) {
        $arguments += '--no-restore'
    }

    & dotnet @arguments
    if ($LASTEXITCODE -ne 0) {
        throw "dotnet publish failed for $ProjectPath with exit code $LASTEXITCODE."
    }
}

$repoRoot = Resolve-RepoRoot

if ([string]::IsNullOrWhiteSpace($OutputRoot)) {
    $OutputRoot = Join-Path $repoRoot 'artifacts\admin-store'
}
elseif (-not [System.IO.Path]::IsPathRooted($OutputRoot)) {
    $OutputRoot = Join-Path $repoRoot $OutputRoot
}

$OutputRoot = [System.IO.Path]::GetFullPath($OutputRoot)
Assert-SafeOutputRoot -RepoRoot $repoRoot -ResolvedOutputRoot $OutputRoot

$releaseVersion = Resolve-ReleaseVersion -RequestedVersion $Version
$appxVersion = Convert-ToAppxVersion -ReleaseVersion $releaseVersion

if ($SkipSigning.IsPresent -and (
        -not [string]::IsNullOrWhiteSpace($SigningCertificateThumbprint) -or
        -not [string]::IsNullOrWhiteSpace($SigningCertificatePath) -or
        -not [string]::IsNullOrWhiteSpace($SigningCertificatePassword) -or
        $TrustLocalTestCertificate.IsPresent)) {
    throw "SkipSigning cannot be combined with signing certificate parameters or TrustLocalTestCertificate."
}

$adminProject = Join-Path $repoRoot 'src\CSharpDB.Admin\CSharpDB.Admin.csproj'
$desktopProject = Join-Path $repoRoot 'src\CSharpDB.Admin.Desktop\CSharpDB.Admin.Desktop.csproj'
$manifestTemplate = Join-Path $repoRoot 'src\CSharpDB.Admin.StorePackage\Package.appxmanifest'
$iconSource = Join-Path $repoRoot 'src\CSharpDB.Admin\wwwroot\icon2.png'

foreach ($requiredPath in @($adminProject, $desktopProject, $manifestTemplate, $iconSource)) {
    if (-not (Test-Path $requiredPath)) {
        throw "Required path was not found: $requiredPath"
    }
}

$publishRoot = Join-Path $OutputRoot 'publish'
$stageRoot = Join-Path $OutputRoot 'stage'
$packageRoot = Join-Path $OutputRoot 'packages'
$adminPublish = Join-Path $publishRoot 'admin'
$desktopPublish = Join-Path $publishRoot 'desktop'
$packageName = "csharpdb-studio-v$releaseVersion-$Runtime"
$msixPath = Join-Path $packageRoot "$packageName.msix"
$msixUploadPath = Join-Path $packageRoot "$packageName.msixupload"
$localTestCertificatePath = Join-Path $packageRoot "$packageName-local-test.cer"

if (Test-Path $OutputRoot) {
    Remove-Item -LiteralPath $OutputRoot -Recurse -Force
}

New-Item -ItemType Directory -Path $publishRoot, $stageRoot, $packageRoot -Force | Out-Null

Write-Host "Publishing CSharpDB.Admin..."
Invoke-DotNetPublish -ProjectPath $adminProject -OutputPath $adminPublish -Runtime $Runtime -Configuration $Configuration -NoRestore:$NoRestore

$adminHelpIndex = Join-Path $adminPublish 'wwwroot\help\index.html'
if (-not (Test-Path $adminHelpIndex)) {
    throw "Admin help files were not published. Expected: $adminHelpIndex"
}

Write-Host "Publishing CSharpDB.Admin.Desktop..."
Invoke-DotNetPublish -ProjectPath $desktopProject -OutputPath $desktopPublish -Runtime $Runtime -Configuration $Configuration -NoRestore:$NoRestore

Write-Host "Creating runnable desktop publish layout..."
$desktopAdminPublish = Join-Path $desktopPublish 'admin'
Copy-Item -Path $adminPublish -Destination $desktopAdminPublish -Recurse -Force

Write-Host "Staging MSIX package..."
Copy-Item -Path (Join-Path $desktopPublish '*') -Destination $stageRoot -Recurse -Force

$manifestPath = Join-Path $stageRoot 'AppxManifest.xml'
Copy-Item -Path $manifestTemplate -Destination $manifestPath -Force

[xml]$manifest = Get-Content -Path $manifestPath -Raw
$manifest.Package.Identity.Version = $appxVersion
$manifest.Package.Identity.ProcessorArchitecture = 'x64'
$publisher = Get-ManifestPublisher -Manifest $manifest
$manifest.Save($manifestPath)

$imagesRoot = Join-Path $stageRoot 'Images'
New-PackageImage -SourcePath $iconSource -DestinationPath (Join-Path $imagesRoot 'Square44x44Logo.png') -Width 44 -Height 44
New-PackageImage -SourcePath $iconSource -DestinationPath (Join-Path $imagesRoot 'Square150x150Logo.png') -Width 150 -Height 150
New-PackageImage -SourcePath $iconSource -DestinationPath (Join-Path $imagesRoot 'Wide310x150Logo.png') -Width 310 -Height 150
New-PackageImage -SourcePath $iconSource -DestinationPath (Join-Path $imagesRoot 'StoreLogo.png') -Width 50 -Height 50

$makeAppx = Find-WindowsSdkTool -ToolName 'makeappx.exe'
Write-Host "Creating MSIX with $makeAppx..."
& $makeAppx pack /d $stageRoot /p $msixPath /o
if ($LASTEXITCODE -ne 0) {
    throw "MakeAppx failed with exit code $LASTEXITCODE."
}

if ($SkipSigning.IsPresent) {
    Write-Warning "Created an unsigned MSIX. App Installer will not install it until it is signed with a trusted certificate."
}
else {
    $signTool = Find-WindowsSdkTool -ToolName 'signtool.exe'

    if ([string]::IsNullOrWhiteSpace($SigningCertificateThumbprint) -and [string]::IsNullOrWhiteSpace($SigningCertificatePath)) {
        $localTestCertificate = Get-OrCreateLocalTestCertificate -Publisher $publisher
        $SigningCertificateThumbprint = $localTestCertificate.Thumbprint
        Export-SigningCertificate -Certificate $localTestCertificate -CertificatePath $localTestCertificatePath
        Write-Host "Local test certificate: $localTestCertificatePath"

        if ($TrustLocalTestCertificate.IsPresent) {
            Import-LocalTestCertificate -CertificatePath $localTestCertificatePath
        }
        else {
            Write-Host "To install the MSIX locally, trust the certificate from an elevated PowerShell session:"
            Write-Host "  Import-Certificate -FilePath `"$localTestCertificatePath`" -CertStoreLocation Cert:\LocalMachine\TrustedPeople"
        }
    }

    Invoke-SignMsixPackage `
        -SignToolPath $signTool `
        -MsixPath $msixPath `
        -Publisher $publisher `
        -CertificateThumbprint $SigningCertificateThumbprint `
        -CertificatePath $SigningCertificatePath `
        -CertificatePassword $SigningCertificatePassword
}

if (Test-Path $msixUploadPath) {
    Remove-Item -LiteralPath $msixUploadPath -Force
}

Write-Host "Creating Store upload wrapper..."
$zipUploadPath = "$msixUploadPath.zip"
if (Test-Path $zipUploadPath) {
    Remove-Item -LiteralPath $zipUploadPath -Force
}

Compress-Archive -Path $msixPath -DestinationPath $zipUploadPath -Force
Move-Item -LiteralPath $zipUploadPath -Destination $msixUploadPath -Force

Write-Host "CSharpDB Studio Store package complete."
Write-Host "  MSIX:       $msixPath"
Write-Host "  MSIXUPLOAD: $msixUploadPath"
if (Test-Path $localTestCertificatePath) {
    Write-Host "  TEST CERT:  $localTestCertificatePath"
}
