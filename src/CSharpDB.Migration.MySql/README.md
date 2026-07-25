# CSharpDB MySQL migration analyzer

This optional, non-packable project is the bounded MySQL schema-readiness
analyzer for the CSharpDB migration tooling. Phase 7B.1 is schema-only: it
opens one selected database, reads fixed server-variable and
`INFORMATION_SCHEMA.TABLES`/`INFORMATION_SCHEMA.COLUMNS` projections, and
builds the source snapshot used by the migration planner.

The checkpoint does not read application rows, execute caller-supplied SQL,
retain `SHOW CREATE` output, inspect keys or indexes, write to the source,
create a target, or provide a CLI or companion worker. A full data importer
and broad SQL rewriting remain separate follow-on work.

## Qualified scope

The intended qualification target is Oracle MySQL 8.0 and 8.4 with InnoDB.
MySqlConnector can connect to MariaDB, Aurora MySQL, and other compatible
servers, but driver connectivity is not qualification. The analyzer records
server version and version-comment evidence so non-Oracle variants can be
reported as unqualified instead of being treated as equivalent.

Phase 7B.1 has deterministic reader and catalog tests but no live-server
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
queries. No identifier or SQL fragment supplied by a caller is interpolated
into command text.

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
- the selected database;
- ordinary `INFORMATION_SCHEMA.TABLES` metadata; and
- ordered `INFORMATION_SCHEMA.COLUMNS` metadata, including bounded
  `COLUMN_TYPE` byte-count and digest evidence.

Every result set is consumed under fixed object and text-byte ceilings.
Exceeding a ceiling fails the inspection instead of silently publishing an
incomplete catalog.

Raw `COLUMN_TYPE` text is bounded and used only during analysis; durable
catalog facets retain its byte count and domain-separated digest. MySQL `BIT`
remains unsupported because its width and bit-string conversion semantics are
not equivalent to a generic binary mapping. MySQL `TIME` also remains
unsupported because it is a signed duration, not the shared time-of-day
semantic type.

## Dependencies

MySqlConnector 2.6.1 is pinned directly and is managed-only. The reviewed
`net10.0` worker-free runtime package closure is recorded in
`THIRD-PARTY-NOTICES.md`. This project deliberately does not reference
Oracle's `MySql.Data` package or optional MariaDB authentication extensions.
