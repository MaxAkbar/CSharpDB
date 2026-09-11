# Database Documenter

Database Documenter creates a searchable data dictionary from saved CSharpDB
metadata. It documents tables, columns, primary and unique keys, checks,
defaults, indexes, foreign keys, views, triggers, and procedures.

## Using Studio

1. Open **Database Documenter** from the sidebar or command palette. To start
   with a particular table, right-click the table and choose **Document Table**.
2. Search by name, description, or SQL text. Filter by object type or enable
   **Missing descriptions** to find documentation gaps.
3. Select an object to inspect its columns, constraints, definitions, and
   incoming or outgoing relationships. Relationship links open the relevant
   table, column, or foreign key.
4. Choose **Edit description**, enter up to 4,000 characters of plain text, and
   select **Save description**. **Cancel** discards the current draft.
5. Select **Refresh** to capture changes made in another session.

Descriptions are stored in the database, so other users of that database can
reuse them. A documentation description overrides an existing native
description, such as a procedure description. Saving a blank description
removes the override and restores the native description when one exists.

Tables, columns, and constraints use stable schema identities. Their
descriptions survive supported renames. Other objects use their kind, owner,
and name; after a definition change, Studio offers **Review description**.
Until reviewed and saved, that override is excluded from the generated
dictionary. Descriptions whose objects disappear remain stored and are
reported as unmatched; they are not guessed onto replacement objects.
Objects without persistent identities cannot distinguish an identical
drop/recreation under the same name from the original object.

Saving checks the description revision and the current definition. If another
user changed either, Studio reports the conflict and retains the draft.
Refresh, review the latest object, and save again. Description updates require
write access; a failed save leaves the draft available.

## Exporting

**Export HTML** downloads a single self-contained file with offline search,
object-type filters, a table of contents, and relationship links. Open it
directly in a browser; it requires no server, CDN, or internet access.
Without JavaScript, all entries remain readable and browser Find still works.

**Export Markdown** downloads one file containing the same dictionary, linked
sections, column and parameter tables, and fenced definitions. It is suitable
for repositories and onboarding documentation. Markdown viewers must support
explicit HTML anchors for section links.

Both exports contain the entire captured dictionary, regardless of the current
search filters. Finish or cancel a description edit before exporting. Object
ordering and anchors are deterministic; capture timestamps change on refresh.
The heading uses a database display name rather than a connection string or
filesystem path. User-authored descriptions and saved definitions are included
as supplied, with format-appropriate escaping.

Incomplete metadata and descriptions awaiting review are visibly marked in
Studio and in both exports. SQL reconstructed from current metadata is labeled
as generated; saved SQL bodies are identified separately.

## Data access and compatibility

Generation reads saved metadata only: it does not scan user rows or execute
procedures, triggers, or saved queries. Opening, searching, and exporting do
not initialize feature catalogs.

The first description save creates the internal
`__documentation_annotations` table inside a transaction. The table is
registered as internal storage and hidden from normal client metadata and
system schema listings. Deleted overrides retain their revision to prevent a
stale edit from recreating them unnoticed. This adds no database file-format
revision.

Direct, HTTP, and gRPC connections use the existing definition-catalog routes.
Optional documentation metadata is advertised through
`DefinitionCatalogPage.DocumentationVersion`. Older servers can expose a
partial dictionary, with missing capabilities shown explicitly and description
editing disabled.

Each Studio operation pins its database client and route. Switching databases
cancels pending work; an operation already in progress cannot be redirected to
the newly selected database. Route-specific dictionaries are separate tabs.

## Reusing the services

```csharp
var generator = new DatabaseDocumenterService();
var document = await generator.GenerateAsync(client, "Commerce", ct: cancellationToken);

string html = DatabaseDocumentRenderer.RenderHtml(document);
string markdown = DatabaseDocumentRenderer.RenderMarkdown(document);

var entry = document.Entries.First(e => e.Kind == "Table");
await new DatabaseDocumentationStore().SaveAsync(
    client, entry.Id, entry.Fingerprint, entry.Revision,
    "Customer orders and their fulfillment state.", cancellationToken);
```

The services are in `CSharpDB.DevOps`. Pass a client bound to one database
and route for the duration of each operation. Renderers use the immutable
document returned by generation.

Collections, external archives, forms, reports, pipelines, saved queries,
diagram exports, publishing, and command-line automation are outside this
version. Relationships describe declared foreign keys; they do not infer
business relationships from data.
