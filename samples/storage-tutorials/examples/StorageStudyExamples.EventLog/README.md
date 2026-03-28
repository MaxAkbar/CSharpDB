# StorageStudyExamples.EventLog

Append-only event log / audit trail with batch inserts, severity-based filtering, and analytics.

## Load in the REPL

```
> load event-log
```

## Commands

| Command | Usage | Description |
|---------|-------|-------------|
| `tail` | `tail [count]` | Show most recent events (default: 20) |
| `log` | `log <source> <severity> <message>` | Append a new event |
| `filter` | `filter <source\|severity\|category> <value>` | Filter events by field |
| `stats` | `stats` | Severity histogram and error rates by source |
| `reclassify` | `reclassify <id> <new-severity>` | Change event severity |
| `purge` | `purge <count>` | Delete the oldest N events |

## Schema

```sql
CREATE TABLE events (
    id INTEGER PRIMARY KEY,
    timestamp INTEGER,
    source TEXT,
    severity INTEGER,   -- 0=DEBUG, 1=INFO, 2=WARN, 3=ERROR, 4=CRITICAL
    category TEXT,
    message TEXT
)
CREATE INDEX idx_events_time ON events(timestamp)
CREATE INDEX idx_events_severity ON events(severity)
```

## Seed Data

34 events from five services: AuthService, PaymentGateway, Scheduler, Monitor, and ApiGateway.

## CSharpDB Features Used

`PrepareInsertBatch` for bulk writes, `CREATE INDEX` on timestamp and severity, `GROUP BY` with `COUNT(*)`, multiple aggregate queries, `UPDATE` (reclassify severity on indexed column), `DELETE` (range-based log rotation).

See the [root README](../README.md) for full documentation.
