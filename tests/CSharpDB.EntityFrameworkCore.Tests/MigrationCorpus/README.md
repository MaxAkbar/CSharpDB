# EF ORM migration SQL corpus

`orm-migration-corpus-up.sql` and `orm-migration-corpus-down.sql` are exact,
generated SQL snapshots for
the representative three-version migration chain in
`OrmMigrationCorpusTests`.

The tests compare provider output with these files and then replay the checked-in
SQL through `CSharpDbConnection`, without using EF to apply it. Update the
snapshots only when a reviewed provider SQL change intentionally alters the
generated commands. The corpus is plain SQL; it does not use a JSON coverage
artifact.
