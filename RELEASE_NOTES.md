# What's New

## version4.0.1

version4.0.1 fixes a full-text index storage failure for hot terms.

### Fixed

- Full-text internal index stores (postings, term stats, doc stats, meta) now spill oversized payloads into overflow-page chains, the same way duplicate-heavy Collection and SQL index buckets already did. Previously a term appearing in a few thousand indexed rows grew its postings blob past a single 4 KB B-tree leaf cell, and because a cell larger than a page can never be split, inserts failed with `Unable to split leaf page N: no byte-balanced redistribution fits within page capacity` and the document was left out of the index. Existing databases are read-compatible: inline payloads keep decoding as-is, and an oversized postings blob spills on its next write.
- Added a full-text regression test that indexes 3,000 documents sharing a token with `StorePositions` enabled, covering search, delete, and reopen round-trips through the overflow chain.

