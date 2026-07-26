# CSharpDB Microsoft Access migration adapter

This project provides a bounded Windows capture lane for unencrypted Microsoft
Access `.mdb` and `.accdb` files. It inventories the source through ACE OLE DB
and writes supported local-table rows into the provider-neutral retained
migration package. ACE is not required after capture: planning, replay, resume,
and validation read the retained package.

## Current readiness

**This is an evaluation capture lane, not an apply-ready release route.** Every
catalog currently contains the non-overrideable diagnostic
`MIG-ACCESS-LIVE-QUALIFICATION-PENDING-001`. Planning and apply must remain
blocked until the disposable Windows qualification matrix has passed for the
supported file formats, ACE versions, process architectures, and fixtures.

The repository includes deterministic provider-absent, catalog, scalar, and
retained-replay tests. An opt-in ACE smoke test can be run with:

```powershell
$env:CSHARPDB_ACCESS_LIVE_FIXTURE = 'C:\fixtures\trusted.accdb'
dotnet test tests\CSharpDB.Migration.Access.Tests\CSharpDB.Migration.Access.Tests.csproj --filter FullyQualifiedName~AccessLiveQualificationTests
```

A passing smoke fixture is useful evidence, but it does not remove the release
qualification diagnostic.

## Provider policy

- Capture is Windows-only and uses `System.Data.OleDb`.
- ACE 16 (`Microsoft.ACE.OLEDB.16.0`) is the default.
- ACE 12 is used only when explicitly selected, or when explicit fallback is
  enabled and ACE 16 is proven absent.
- The provider registration must match the running process architecture.
- Jet 4.0 is never selected.
- CSharpDB does not redistribute or install the Access Database Engine.
- Raw connection strings are not accepted. The adapter builds a read-only
  connection string from a validated local file path.
- Password-protected/encrypted databases are outside this v1 contract.

Call `AccessProviderProbe.Check` before capture when a user-facing prerequisite
report is needed. A missing provider produces the typed, value-free
`ProviderUnavailable` failure.

## Consistency and source safety

The source must be a regular `.mdb` or `.accdb` file in a trusted path. For the
entire schema-and-row capture, the adapter:

1. opens a file handle that permits reads but denies writes, replacement, and
   deletion;
2. hashes the complete source under that lease;
3. opens ACE in `Share Deny Write` mode;
4. reads schema and rows through the same ACE connection; and
5. releases both handles only after retained package publication or failure.

The caller must prevent untrusted principals from renaming or replacing source
path ancestors while capture is running.

## Bounded v1 source subset

Included for retained row capture:

- local `TABLE` objects;
- tables with a non-empty primary key, used as deterministic `ORDER BY`;
- signed and unsigned integers, Boolean, decimal/currency, finite floating
  point, text/memo, binary/OLE, GUID, and Access date/time scalars;
- local relationships whose child columns and referenced local primary key are
  completely visible; and
- deterministic row, value, table, total-row, catalog, source-file, and package
  size bounds.

Inventoried but not automatically recreated:

- non-primary indexes, because Access collation and NULL semantics still need
  qualification;
- column defaults; and
- saved queries/views, linked tables, and other non-local table objects.

Fail-closed exclusions:

- tables without a primary key;
- tables containing attachment, multivalued, COM/variant, or another
  unsupported scalar shape;
- relationships targeting a linked/unseen table, a non-primary unique index,
  or incomplete column metadata;
- unsupported `ON UPDATE` behavior; and
- any inspection rowset that is missing required identity, ordinal, type, key,
  or referential-action metadata.

## API outline

```csharp
AccessProviderAvailability provider =
    AccessProviderProbe.Check(AccessOleDbProvider.Ace16);

var inspector = new AccessMigrationSourceInspector(
    @"C:\data\legacy.accdb");
MigrationCatalog catalog = await inspector.InspectAsync(
    new MigrationInspectionRequest
    {
        TargetCSharpDbVersion =
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
        IncludeProfile = false,
    });

RetainedMigrationPackageWriteResult capture =
    await AccessRetainedCapture.CaptureAsync(
        @"C:\data\legacy.accdb",
        @"C:\migration\legacy.csdbaccess");
```

Persist the returned package digest next to trusted migration state. Reopen the
package with `RetainedMigrationPackageSession` and the exact expected digest;
never infer or accept the digest from the package being opened.
