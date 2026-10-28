# Published native archive SQL smoke

This executable writes a table archive, loads the actual published CSharpDB
native library, registers an external table, and verifies a scan, primary-key
lookup, declared SQL types and close/reopen persistence through the C API.
Publishing the smoke itself with NativeAOT also exercises the archive writer
with reflection-based JSON serialization disabled.

Run it after publishing both projects for the current machine, for example:

```powershell
dotnet publish src/CSharpDB.Native/CSharpDB.Native.csproj -c Release -r win-x64 -p:ILLinkTreatWarningsAsErrors=true -p:IlcTreatWarningsAsErrors=true
dotnet publish tests/CSharpDB.Native.ArchiveSmoke/CSharpDB.Native.ArchiveSmoke.csproj -c Release -r win-x64 --self-contained true -p:PublishAot=true -o artifacts/native-archive-smoke/win-x64
./artifacts/native-archive-smoke/win-x64/CSharpDB.Native.ArchiveSmoke.exe (Resolve-Path src/CSharpDB.Native/bin/Release/net10.0/win-x64/publish/CSharpDB.Native.dll)
```

CI runs the corresponding `linux-x64`, `win-x64` and `osx-arm64` builds. The
macOS job uses a private directory under `/private/tmp` so the archive path has
no temporary-directory symlink. Native compiler prerequisites are required
only for publishing; the ordinary solution build remains managed.

The smoke checks archive-backed SQL through the native C ABI. Remote gRPC and
performance qualification have separate validation requirements.
