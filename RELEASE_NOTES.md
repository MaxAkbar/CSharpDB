# What's New

## version4.0.3

version4.0.3 removes the single-page B-tree payload limit, allowing large SQL values and Collection documents to span multiple 4 KiB pages safely.

### Fixed

- B-tree payloads larger than the 4,075-byte inline limit are now stored in linked overflow pages instead of failing when a leaf page cannot be split.
- Overflow-backed values work across point and snapshot reads, cursor scans, inserts, replacements, deletes, rollback, leaf splits, checkpoints, and database reopen.
- Replaced and deleted overflow chains are reclaimed for reuse, while rejected duplicate inserts and missing-key replacements avoid allocating overflow pages.
- Storage diagnostics recognize overflow pages and validate reference metadata, page types, bounds, lengths, and cycles so corrupt chains are reported clearly.
- Failed implicit Collection writes now release the write gate correctly, allowing later writes to continue.

### Compatibility

- Existing format-v1 databases remain readable and are durably upgraded to format v2 before the next write is committed; read-only opens do not rewrite the file.
- Once upgraded to format v2, a database requires a CSharpDB version that understands tagged overflow references. Back up the database before upgrading if rollback to an older binary may be required.
- Explicitly tagged leaf cells keep ordinary inline values unambiguous, including values whose bytes resemble an overflow reference.

### Performance and Validation

- Added coverage for 10–20 KB SQL, Collection, and direct B-tree values across CRUD, rollback, cursor scans, page splits, checkpoint/reopen, page reuse, format migration, and corruption detection.
- BenchmarkDotNet comparisons found no measurable steady-state regression for existing inline workloads.
- A roughly 6 KB overflow value measured 2.365 ms per durable insert and 4.595 microseconds per hot primary-key read on the benchmark machine. The previous version cannot complete this workload.
- Corrected the B-tree cursor benchmark to retain the final root page after splits so seek measurements exercise the intended tree path.
