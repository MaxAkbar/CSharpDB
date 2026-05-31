# CSharpDB Admin Store Package

This project contains the MSIX manifest and Visual Studio packaging metadata for
the Microsoft Store build of CSharpDB Studio.

The maintainer path is:

```powershell
.\scripts\Publish-CSharpDbAdminStorePackage.ps1 -Version 3.9.0
```

The script publishes the existing `CSharpDB.Admin` web host and the
`CSharpDB.Admin.Desktop` WebView2 shell, stages them as an MSIX full-trust
desktop app, and writes Store upload artifacts under `artifacts\admin-store`.
The admin web publish includes the offline help bundle from
`src/CSharpDB.Admin/wwwroot/help`; the packaging script verifies that
`wwwroot/help/index.html` is present before creating the MSIX.

The local `.msix` is signed with a test certificate by default so maintainers
can smoke-test the package before Partner Center submission. To install the
local package with App Installer, import the exported `.cer` into
`Cert:\LocalMachine\TrustedPeople` from an elevated PowerShell session, or run
the script elevated with `-TrustLocalTestCertificate`.

Before submitting to Partner Center, associate this package with the Store app
so the package identity and publisher are replaced with the assigned Store
values.
