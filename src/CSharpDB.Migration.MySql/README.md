# CSharpDB MySQL migration analyzer

This optional, non-packable project is the bounded MySQL schema-readiness
analyzer for the CSharpDB migration tooling. Phase 7B.3 is schema-only: it
opens one selected database, reads fixed server-variable and
`INFORMATION_SCHEMA` projections, retains bounded `SHOW CREATE TABLE`
evidence, and builds the source snapshot used by the migration planner.

The checkpoint inventories tables, columns and column-default evidence,
primary and unique keys, foreign keys, checks, index variants, views and their
output columns, triggers, stored procedures and functions, and routine
parameters and function return rows. Programmable definitions are inventory
evidence only. They are not parsed, bound, lowered, scratch-executed, or
promoted to target SQL. It does not read application rows, execute
caller-supplied SQL, write to the source, create a target, or provide a CLI or
companion worker.

## Qualified scope

The intended qualification target is Oracle MySQL 8.0 and 8.4 with InnoDB.
MySqlConnector can connect to MariaDB, Aurora MySQL, and other compatible
servers, but driver connectivity is not qualification. The analyzer records
server version and version-comment evidence so non-Oracle variants can be
reported as unqualified instead of being treated as equivalent.

Phase 7B.3 has deterministic reader and catalog tests but no live-server
qualification claim. MySQL 8.0 and 8.4 live fixtures, restricted-account
permission coverage, TLS modes, platforms, and published-runtime smoke tests
must be qualified before this adapter becomes packable or shipping software.

## Connection policy

The caller must select exactly one server and one non-empty database in the
connection string. Comma-delimited multi-host configurations are rejected so
one inspection cannot silently move between servers. The reader opens one
connection with pooling, local-infile requests, user variables, automatic
transaction enlistment, and persisted security information disabled.
Connection, command, and cancellation timeouts are bounded.

TCP connections reject `SslMode=None` and `SslMode=Preferred`; they must use
`Required` or a stronger certificate-verifying mode. Local Unix sockets,
named pipes, and shared-memory transports may explicitly opt out of TLS.
Caller-selected certificate and authentication settings are otherwise
preserved, and the adapter never relaxes them.

Only the configured database name is used as a parameter to fixed metadata
queries. The sole dynamic command is one `SHOW CREATE TABLE` per table name
already returned by the bounded catalog scan. Schema and table identifiers are
quoted separately with backticks, and embedded backticks are doubled. No
caller-supplied SQL fragment is executed.

Raw connection strings, credentials, account names, server names, socket
paths, and provider exception messages are not migration artifacts or public
errors. The durable endpoint identity is a domain-separated SHA-256 digest;
the selected database remains visible because it is required schema metadata.

## Metadata scope

The server snapshot records:

- server version and version comment;
- session SQL mode and time zone;
- server character set and collation;
- system time zone and `lower_case_table_names`;
- generated invisible primary-key visibility when the qualified server exposes
  that session setting;
- `sql_quote_show_create` session behavior;
- `explicit_defaults_for_timestamp` session behavior;
- the selected database;
- ordinary `INFORMATION_SCHEMA.TABLES` metadata;
- ordered `INFORMATION_SCHEMA.COLUMNS` metadata, including bounded
  `COLUMN_TYPE` byte-count and digest evidence;
- primary and unique constraints with ordered column membership;
- foreign-key rules and ordered child/referenced column pairs;
- enforced and unenforced check constraints;
- index type, visibility, uniqueness, prefix length, sort direction, and
  functional-key-part evidence;
- one bounded `SHOW CREATE TABLE` result for every inventoried table;
- bounded column-default evidence, including expression-default and automatic
  `ON UPDATE` markers;
- views, ordered view output columns, and bounded view definitions;
- table-owned triggers, their event, timing, action order, creation settings,
  and bounded action statements; and
- stored procedures and functions, ordered parameters and function return
  rows, execution characteristics, creation settings, and bounded routine
  definitions.

Every result set is consumed under fixed object and text-byte ceilings.
Exceeding a ceiling fails the inspection instead of silently publishing an
incomplete catalog.

Raw `COLUMN_TYPE`, check clauses, functional-index expressions, and
`SHOW CREATE TABLE` text are bounded and used only during analysis. The same
rule applies to column defaults and view, trigger, and routine definitions.
Durable catalog facets retain byte counts, lengths, and domain-separated
digests, never the raw SQL text, definer account, comments, or creation and
alteration timestamps. Privilege-filtered view and routine metadata remains
inventoried with explicit unavailable evidence instead of being treated as a
complete definition. MySQL reports `COLUMN_DEFAULT` as SQL `NULL` for both an
explicit `DEFAULT NULL` and no `DEFAULT` clause, so that case remains
explicitly ambiguous rather than being guessed from text. Non-null defaults,
expression defaults, and `ON UPDATE` behavior remain source expressions and
are excluded by planning until a safe MySQL expression pipeline exists.
On affected Oracle versions, `DEFAULT_GENERATED` can coexist with unavailable
`COLUMN_DEFAULT` evidence; the analyzer inventories and excludes that shape
instead of guessing its expression or aborting the inspection.

Views and triggers remain excluded from target planning because their bodies
have not reached parse, bind, scratch, or differential evidence. Routines have
no automatic CSharpDB lowering contract. No `targetSql`, generic
`deterministic`, or generic `rowLocal` facet is inferred from MySQL declarations
or raw text. `VIEW_TABLE_USAGE` and `VIEW_ROUTINE_USAGE` are
privilege-filtered and do not establish a complete dependency graph, so
dependency proof remains deferred. Functional, prefix, descending, invisible,
non-BTREE, and ambiguous foreign-key support indexes remain explicit blockers
instead of being simplified silently.

Detailed partition metadata, Event Scheduler definitions and schedules,
complete programmable dependency proof, application rows and validation, a
data importer, CLI/worker packaging, and live restricted-account qualification
remain follow-on work. Their absence is represented by non-overrideable
readiness diagnostics; this offline catalog is not migration approval.

MySQL `BIT` remains unsupported because its width and bit-string conversion
semantics are not equivalent to a generic binary mapping. MySQL `TIME` also
remains unsupported because it is a signed duration, not the shared
time-of-day semantic type.

## Dependencies

MySqlConnector 2.6.1 is pinned directly and is managed-only. The reviewed
`net10.0` worker-free runtime package closure is recorded in
`THIRD-PARTY-NOTICES.md`. This project deliberately does not reference
Oracle's `MySql.Data` package or optional MariaDB authentication extensions.
