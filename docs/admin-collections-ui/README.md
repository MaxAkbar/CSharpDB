# Admin Collections UI

## Summary

The Admin app now treats document collections as first-class objects alongside
tables, views, forms, and reports. The first implementation is a browser and
JSON editor built on the existing collection client APIs:

- list collection names
- browse documents by page
- fetch one document by exact key
- create or update a document
- delete a document

This deliberately avoids new engine, HTTP, gRPC, or client contracts. Collection
path-index management, document-content search, collection rename, and
collection drop remain future work.

## User Experience

- Object Explorer has a `Collections` filter chip and group.
- The collection group menu supports `New Collection...` and `Refresh`.
- Collection item menus support `Open`, `New Document...`, and `Copy Name`.
- The command palette includes `New Collection` plus one entry for each existing
  collection.
- Each collection opens in a dedicated `collection:{name}` tab using the same
  toolbar, pager, grid, and detail-panel language as table data tabs.

## Collection Tab Behavior

- The grid shows row number, document key, JSON kind, and a compact preview.
- The detail panel shows indented JSON for the selected document.
- Existing document keys are read-only.
- New documents require a nonblank key and valid JSON before save is enabled.
- Save writes through `PutDocumentAsync(collectionName, key, document)`.
- Delete writes through `DeleteDocumentAsync(collectionName, key)` after
  confirmation.
- Successful writes notify Admin change listeners and refresh the current page.
- Exact-key lookup uses `GetDocumentAsync(collectionName, key)`.

## Defaults

- Default page size is `25`.
- Supported page sizes are `10`, `25`, `50`, and `100`.
- Collection names use the same simple identifier shape as the direct client:
  `^[A-Za-z_][A-Za-z0-9_]*$`.
- Deleting all documents leaves the collection itself in place because there is
  no drop-collection API today.
- Generated collection model metadata is not surfaced; documents are edited as
  raw JSON.

## Verification

Run the focused Admin checks after collection UI changes:

```powershell
dotnet build src/CSharpDB.Admin/CSharpDB.Admin.csproj
dotnet test tests/CSharpDB.Admin.Forms.Tests/CSharpDB.Admin.Forms.Tests.csproj
```
