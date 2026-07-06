# What's New

## version4.0.2

version4.0.2 improves full-text indexing scalability for large corpora and makes indexed mutations avoid unnecessary full-table scans.

### Changed

- Full-text postings are now stored in bounded posting chunks instead of rewriting one growing postings blob for every hot-token insert. This keeps repeated-token indexing scalable for large corpora.
- Existing full-text posting blobs remain readable. Databases with the previous full-text storage layout get the new chunk store on open, and legacy posting blobs migrate to chunked postings on the next write for that term.
- Full-text deletes and updates rewrite only the affected posting chunk while preserving existing search result ordering and transaction rollback behavior.
- `DELETE` and `UPDATE` statements can now collect target row IDs through available secondary indexes instead of scanning the full table for indexed predicates.

### Validation

- Added focused hot-token before/after benchmark coverage for insert, query, delete, and update paths.
- Added FileSearcher NuGet before/after benchmark automation that validates real package consumption against local baseline and patched CSharpDB packages.
