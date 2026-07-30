# Phase 6 EF migration SQL corpus

`phase6-up.sql` and `phase6-down.sql` are exact, generated SQL snapshots for
the representative three-version migration chain in
`Phase6OrmMigrationCorpusTests`.

The tests compare provider output with these files and then replay the checked-in
SQL through `CSharpDbConnection`, without using EF to apply it. Update the
snapshots only when a reviewed provider SQL change intentionally alters the
generated commands. The corpus is plain SQL; it does not use a JSON coverage
artifact.
