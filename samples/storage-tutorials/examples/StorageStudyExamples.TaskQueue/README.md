# StorageStudyExamples.TaskQueue

Persistent job queue with priority-based dequeue, state machine transitions, and retry logic.

## Load in the REPL

```
> load task-queue
```

## Commands

| Command | Usage | Description |
|---------|-------|-------------|
| `list` | `list [queue]` | List jobs (all or by queue) |
| `enqueue` | `enqueue <queue> <priority> <payload...>` | Add a job |
| `claim` | `claim <queue>` | Claim highest-priority pending job |
| `complete` | `complete <id>` | Mark job as completed |
| `fail` | `fail <id> <error...>` | Mark job as failed |
| `retry` | `retry <id>` | Re-queue a failed job |
| `cancel` | `cancel <id>` | Cancel a pending job |
| `dashboard` | `dashboard` | Status summary per queue |
| `reprioritize` | `reprioritize <id> <priority>` | Change job priority |
| `move` | `move <id> <queue>` | Move job to different queue |
| `purge` | `purge` | Delete all completed jobs |

## Schema

```sql
CREATE TABLE jobs (
    id INTEGER PRIMARY KEY,
    queue TEXT,
    payload TEXT,
    priority INTEGER,
    status INTEGER,        -- 0=Pending, 1=Running, 2=Completed, 3=Failed
    created_at INTEGER,
    started_at INTEGER,
    completed_at INTEGER,
    retry_count INTEGER,
    max_retries INTEGER,
    error_message TEXT
)
CREATE INDEX idx_jobs_status ON jobs(status)
CREATE INDEX idx_jobs_queue ON jobs(queue)
```

## Seed Data

12 jobs across 3 queues: `email` (4 jobs), `reports` (4 jobs), and `notifications` (4 jobs).

## CSharpDB Features Used

Transactions for atomic state transitions, `ORDER BY priority DESC LIMIT 1` for priority dequeue, `GROUP BY queue, status` for dashboard, `UPDATE` with computed values (reprioritize, move between queues), conditional `DELETE` (cancel pending), bulk `DELETE` (purge completed).

See the [root README](../README.md) for full documentation.
