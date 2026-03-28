// ============================================================================
// Task Queue / Job Scheduler Example
// ============================================================================
//
// Demonstrates using CSharpDB as a persistent job queue with priorities,
// state machine transitions (pending -> running -> completed/failed), and
// retry logic. Shows: transactions for atomic claim/release, batch inserts,
// indexed lookups, GROUP BY dashboards, UPDATE (reprioritize, move between
// queues), DELETE (cancel pending, purge completed).
// ============================================================================

using CSharpDB.Primitives;
using StorageStudyExamples.Core;

namespace StorageStudyExamples.TaskQueue;

public sealed class TaskQueueStore : DataStoreBase
{
    // Job statuses
    private const int Pending = 0;
    private const int Running = 1;
    private const int Completed = 2;
    private const int Failed = 3;

    public override string Name => "Task Queue";
    public override string CommandName => "task-queue";
    public override string Description => "Persistent job queue with priorities, state machine, and retry logic.";

    private static string StatusLabel(long status) => status switch
    {
        Pending => "PENDING",
        Running => "RUNNING",
        Completed => "DONE",
        Failed => "FAILED",
        _ => "?"
    };

    // ── Schema ─────────────────────────────────────────────────────────────

    protected override async Task CreateSchemaAsync()
    {
        await Db.ExecuteAsync("""
            CREATE TABLE jobs (
                id INTEGER PRIMARY KEY,
                queue TEXT,
                payload TEXT,
                priority INTEGER,
                status INTEGER,
                created_at INTEGER,
                started_at INTEGER,
                completed_at INTEGER,
                retry_count INTEGER,
                max_retries INTEGER,
                error_message TEXT
            )
            """);
        await Db.ExecuteAsync("CREATE INDEX idx_jobs_status ON jobs(status)");
        await Db.ExecuteAsync("CREATE INDEX idx_jobs_queue ON jobs(queue)");
    }

    // ── Seed data ──────────────────────────────────────────────────────────

    protected override async Task SeedDataAsync()
    {
        long t = 1700000000;

        var batch = Db.PrepareInsertBatch("jobs");

        //                 id  queue             payload                           pri  status  created  started  completed retry maxR error
        void Enqueue(int id, string queue, string payload, int priority, long createdAt)
        {
            batch.AddRow(
                DbValue.FromInteger(id),
                DbValue.FromText(queue),
                DbValue.FromText(payload),
                DbValue.FromInteger(priority),
                DbValue.FromInteger(Pending),
                DbValue.FromInteger(createdAt),
                DbValue.Null,  // started_at
                DbValue.Null,  // completed_at
                DbValue.FromInteger(0),  // retry_count
                DbValue.FromInteger(3),  // max_retries
                DbValue.Null); // error_message
        }

        // Email queue (priority: higher = more urgent)
        Enqueue(1, "email", "Send welcome email to alice@corp.com", 5, t);
        Enqueue(2, "email", "Send invoice #1042 to billing@acme.com", 8, t + 1);
        Enqueue(3, "email", "Send password reset to bob@corp.com", 10, t + 2);
        Enqueue(4, "email", "Send weekly newsletter (batch)", 2, t + 3);

        // Reports queue
        Enqueue(5, "reports", "Generate Q4 financial summary", 7, t + 5);
        Enqueue(6, "reports", "Generate daily active users report", 5, t + 6);
        Enqueue(7, "reports", "Export compliance audit log", 9, t + 7);
        Enqueue(8, "reports", "Generate inventory snapshot", 4, t + 8);

        // Notifications queue
        Enqueue(9, "notifications", "Push alert: server CPU > 90%", 10, t + 10);
        Enqueue(10, "notifications", "Push alert: deployment complete", 6, t + 11);
        Enqueue(11, "notifications", "Slack message: build #847 passed", 3, t + 12);
        Enqueue(12, "notifications", "Push alert: new signup from enterprise lead", 7, t + 13);

        await batch.ExecuteAsync();
    }

    // ── Interactive commands ───────────────────────────────────────────────

    public override IReadOnlyList<CommandInfo> GetCommands() =>
    [
        new("list",         "list [queue]",                     "List jobs (all or filtered by queue)"),
        new("enqueue",      "enqueue <queue> <priority> <payload...>", "Add a job"),
        new("claim",        "claim <queue>",                    "Claim highest-priority pending job"),
        new("complete",     "complete <id>",                    "Mark job as completed"),
        new("fail",         "fail <id> <error...>",             "Mark job as failed"),
        new("retry",        "retry <id>",                       "Re-queue a failed job"),
        new("cancel",       "cancel <id>",                      "Cancel (delete) a pending job"),
        new("dashboard",    "dashboard",                        "Status summary per queue"),
        new("reprioritize", "reprioritize <id> <priority>",     "Change job priority"),
        new("move",         "move <id> <queue>",                "Move job to different queue"),
        new("purge",        "purge",                            "Delete all completed jobs"),
    ];

    public override async Task<bool> ExecuteCommandAsync(string commandName, string args, TextWriter output)
    {
        switch (commandName)
        {
            case "list":
                await ListJobsAsync(args, output);
                return true;

            case "enqueue":
                await EnqueueCommandAsync(args, output);
                return true;

            case "claim":
                await ClaimCommandAsync(args, output);
                return true;

            case "complete":
                await CompleteCommandAsync(args, output);
                return true;

            case "fail":
                await FailCommandAsync(args, output);
                return true;

            case "retry":
                await RetryCommandAsync(args, output);
                return true;

            case "cancel":
                await CancelCommandAsync(args, output);
                return true;

            case "dashboard":
                await DashboardCommandAsync(output);
                return true;

            case "reprioritize":
                await ReprioritizeCommandAsync(args, output);
                return true;

            case "move":
                await MoveCommandAsync(args, output);
                return true;

            case "purge":
                await PurgeCommandAsync(output);
                return true;

            default:
                return false;
        }
    }

    // ── Command implementations ───────────────────────────────────────────

    private async Task ListJobsAsync(string args, TextWriter output)
    {
        var queue = string.IsNullOrWhiteSpace(args) ? null : args.Trim();

        var sql = queue != null
            ? $"SELECT id, queue, priority, status, retry_count, payload FROM jobs WHERE queue = '{Esc(queue)}' ORDER BY queue, priority DESC"
            : "SELECT id, queue, priority, status, retry_count, payload FROM jobs ORDER BY queue, priority DESC";

        output.WriteLine($"  {"ID",3} {"Queue",-14} {"Pri",4} {"Status",-8} {"Retries",8} {"Payload"}");
        output.WriteLine($"  {new string('-', 3)} {new string('-', 14)} {new string('-', 4)} {new string('-', 8)} {new string('-', 8)} {new string('-', 40)}");

        await using var result = await Db.ExecuteAsync(sql);
        await foreach (var row in result.GetRowsAsync())
        {
            var id = row[0].AsInteger;
            var q = row[1].AsText;
            var pri = row[2].AsInteger;
            var status = StatusLabel(row[3].AsInteger);
            var retries = row[4].AsInteger;
            var payload = row[5].AsText;

            output.WriteLine($"  {id,3} {q,-14} {pri,4} {status,-8} {retries,8} {Truncate(payload, 45)}");
        }
    }

    private async Task EnqueueCommandAsync(string args, TextWriter output)
    {
        var parts = args.Split(' ', 3, StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 3 || !int.TryParse(parts[1], out var priority))
        {
            output.WriteLine("Usage: enqueue <queue> <priority> <payload...>");
            return;
        }

        var queue = parts[0];
        var payload = parts[2];
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

        // Get next ID
        long nextId;
        await using (var maxResult = await Db.ExecuteAsync("SELECT MAX(id) FROM jobs"))
        {
            var rows = await maxResult.ToListAsync();
            nextId = (rows.Count > 0 && !rows[0][0].IsNull) ? rows[0][0].AsInteger + 1 : 1;
        }

        var batch = Db.PrepareInsertBatch("jobs");
        batch.AddRow(
            DbValue.FromInteger(nextId),
            DbValue.FromText(queue),
            DbValue.FromText(payload),
            DbValue.FromInteger(priority),
            DbValue.FromInteger(Pending),
            DbValue.FromInteger(now),
            DbValue.Null,              // started_at
            DbValue.Null,              // completed_at
            DbValue.FromInteger(0),    // retry_count
            DbValue.FromInteger(3),    // max_retries
            DbValue.Null);             // error_message
        await batch.ExecuteAsync();

        output.WriteLine($"  Enqueued job #{nextId} in '{queue}' (pri={priority}): {Truncate(payload, 50)}");
    }

    private async Task ClaimCommandAsync(string args, TextWriter output)
    {
        var queue = args.Trim();
        if (string.IsNullOrEmpty(queue))
        {
            output.WriteLine("Usage: claim <queue>");
            return;
        }

        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        await ClaimNextAsync(queue, now, output);
    }

    private async Task CompleteCommandAsync(string args, TextWriter output)
    {
        if (!long.TryParse(args.Trim(), out var id))
        {
            output.WriteLine("Usage: complete <id>");
            return;
        }

        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        await CompleteJobAsync(id, now, output);
    }

    private async Task FailCommandAsync(string args, TextWriter output)
    {
        var parts = args.Split(' ', 2, StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 2 || !long.TryParse(parts[0], out var id))
        {
            output.WriteLine("Usage: fail <id> <error...>");
            return;
        }

        var error = parts[1];
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        await FailJobAsync(id, error, now, output);
    }

    private async Task RetryCommandAsync(string args, TextWriter output)
    {
        if (!long.TryParse(args.Trim(), out var id))
        {
            output.WriteLine("Usage: retry <id>");
            return;
        }

        // Check current status and retry info
        await using var result = await Db.ExecuteAsync(
            $"SELECT status, retry_count, max_retries FROM jobs WHERE id = {id}");
        var rows = await result.ToListAsync();

        if (rows.Count == 0)
        {
            output.WriteLine($"  Job #{id} not found.");
            return;
        }

        var status = rows[0][0].AsInteger;
        var retryCount = rows[0][1].AsInteger;
        var maxRetries = rows[0][2].AsInteger;

        if (status != Failed)
        {
            output.WriteLine($"  Job #{id} is not failed (status: {StatusLabel(status)}).");
            return;
        }

        if (retryCount >= maxRetries)
        {
            output.WriteLine($"  Job #{id} has exhausted retries ({retryCount}/{maxRetries}).");
            return;
        }

        await Db.ExecuteAsync(
            $"UPDATE jobs SET status = {Pending}, started_at = NULL, completed_at = NULL, error_message = NULL WHERE id = {id}");
        output.WriteLine($"  Job #{id} re-queued (attempt {retryCount + 1}/{maxRetries}).");
    }

    private async Task CancelCommandAsync(string args, TextWriter output)
    {
        if (!long.TryParse(args.Trim(), out var id))
        {
            output.WriteLine("Usage: cancel <id>");
            return;
        }

        await using var result = await Db.ExecuteAsync(
            $"DELETE FROM jobs WHERE id = {id} AND status = {Pending}");

        if (result.RowsAffected > 0)
            output.WriteLine($"  Cancelled job #{id}.");
        else
            output.WriteLine($"  Could not cancel job #{id} (not found or not pending).");
    }

    private async Task DashboardCommandAsync(TextWriter output)
    {
        output.WriteLine($"  {"Queue",-16} {"Pending",8} {"Running",8} {"Done",8} {"Failed",8}");
        output.WriteLine($"  {new string('-', 16)} {new string('-', 8)} {new string('-', 8)} {new string('-', 8)} {new string('-', 8)}");

        var dashboard = new Dictionary<string, long[]>();

        await using (var result = await Db.ExecuteAsync(
            "SELECT queue, status, COUNT(*) FROM jobs GROUP BY queue, status ORDER BY queue, status"))
        {
            await foreach (var row in result.GetRowsAsync())
            {
                var queue = row[0].AsText;
                var status = (int)row[1].AsInteger;
                var count = row[2].AsInteger;

                if (!dashboard.ContainsKey(queue))
                    dashboard[queue] = new long[4];
                dashboard[queue][status] = count;
            }
        }

        foreach (var (queue, counts) in dashboard.OrderBy(x => x.Key))
            output.WriteLine($"  {queue,-16} {counts[0],8} {counts[1],8} {counts[2],8} {counts[3],8}");
    }

    private async Task ReprioritizeCommandAsync(string args, TextWriter output)
    {
        var parts = args.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 2 || !long.TryParse(parts[0], out var id) || !int.TryParse(parts[1], out var priority))
        {
            output.WriteLine("Usage: reprioritize <id> <priority>");
            return;
        }

        await Db.ExecuteAsync($"UPDATE jobs SET priority = {priority} WHERE id = {id}");
        output.WriteLine($"  Job #{id} priority set to {priority}.");
    }

    private async Task MoveCommandAsync(string args, TextWriter output)
    {
        var parts = args.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 2 || !long.TryParse(parts[0], out var id))
        {
            output.WriteLine("Usage: move <id> <queue>");
            return;
        }

        var queue = parts[1];
        await Db.ExecuteAsync($"UPDATE jobs SET queue = '{Esc(queue)}' WHERE id = {id}");
        output.WriteLine($"  Job #{id} moved to '{queue}'.");
    }

    private async Task PurgeCommandAsync(TextWriter output)
    {
        long count;
        await using (var countResult = await Db.ExecuteAsync(
            $"SELECT COUNT(*) FROM jobs WHERE status = {Completed}"))
        {
            var rows = await countResult.ToListAsync();
            count = rows[0][0].AsInteger;
        }

        await Db.ExecuteAsync($"DELETE FROM jobs WHERE status = {Completed}");
        output.WriteLine($"  Purged {count} completed job(s).");
    }

    // ── Scripted demo ──────────────────────────────────────────────────────

    public override async Task RunDemoAsync(TextWriter output)
    {
        long t = 1700000000;

        // ── Enqueue summary ──────────────────────────────────────────────
        output.WriteLine("--- Enqueueing jobs ---");
        output.WriteLine("  Enqueued 12 jobs across 3 queues.");
        output.WriteLine();

        // ── Dequeue: claim highest-priority pending job ─────────────────
        output.WriteLine("--- Dequeue: claiming jobs ---");

        // Claim one job from each queue
        var emailJob = await ClaimNextAsync("email", t + 100, output);
        var reportJob = await ClaimNextAsync("reports", t + 100, output);
        var notifJob = await ClaimNextAsync("notifications", t + 100, output);
        output.WriteLine();

        // ── Complete and fail some jobs ─────────────────────────────────
        output.WriteLine("--- Processing jobs ---");

        // Complete the email and notification jobs, fail the report job
        await CompleteJobAsync(emailJob, t + 105, output);
        await FailJobAsync(reportJob, "Connection to reporting DB timed out", t + 110, output);
        await CompleteJobAsync(notifJob, t + 101, output);
        output.WriteLine();

        // ── Retry failed jobs ───────────────────────────────────────────
        output.WriteLine("--- Retrying failed jobs ---");

        await using (var result = await Db.ExecuteAsync(
            $"SELECT id, queue, retry_count, max_retries FROM jobs WHERE status = {Failed}"))
        {
            await foreach (var row in result.GetRowsAsync())
            {
                var jobId = row[0].AsInteger;
                var queue = row[1].AsText;
                var retries = row[2].AsInteger;
                var maxRetries = row[3].AsInteger;

                if (retries < maxRetries)
                {
                    await Db.ExecuteAsync(
                        $"UPDATE jobs SET status = {Pending}, started_at = NULL, completed_at = NULL, error_message = NULL WHERE id = {jobId}");
                    output.WriteLine($"  Job #{jobId} [{queue}] re-queued (attempt {retries + 1}/{maxRetries}).");
                }
                else
                {
                    output.WriteLine($"  Job #{jobId} [{queue}] exhausted retries ({retries}/{maxRetries}), leaving as failed.");
                }
            }
        }
        output.WriteLine();

        // ── Claim and complete a few more ───────────────────────────────
        output.WriteLine("--- Second round of processing ---");
        var emailJob2 = await ClaimNextAsync("email", t + 200, output);
        var reportJob2 = await ClaimNextAsync("reports", t + 200, output);
        if (emailJob2 > 0) await CompleteJobAsync(emailJob2, t + 202, output);
        if (reportJob2 > 0) await CompleteJobAsync(reportJob2, t + 208, output);
        output.WriteLine();

        // ── Job management operations ─────────────────────────────────
        output.WriteLine("--- Job management ---");

        // Cancel a pending job: remove the daily users report (#6, reports queue)
        await Db.ExecuteAsync(
            $"DELETE FROM jobs WHERE id = 6 AND status = {Pending}");
        output.WriteLine("  Cancelled: job #6 (daily users report) removed from reports queue.");

        // Reprioritize: bump Q4 financial summary (#5) from priority 7 to 10
        await Db.ExecuteAsync(
            "UPDATE jobs SET priority = 10 WHERE id = 5");
        output.WriteLine("  Reprioritized: job #5 (Q4 financial summary) 7 -> 10.");

        // Move between queues: move a notification to a new 'urgent' queue
        // deployment-complete (#10) needs urgent attention
        await Db.ExecuteAsync(
            $"UPDATE jobs SET queue = 'urgent', priority = 10 WHERE id = 10");
        output.WriteLine("  Moved: job #10 (deployment complete) notifications -> urgent queue (pri=10).");

        // Purge completed jobs
        long purgedCount;
        await using (var countResult = await Db.ExecuteAsync(
            $"SELECT COUNT(*) FROM jobs WHERE status = {Completed}"))
        {
            var countRows = await countResult.ToListAsync();
            purgedCount = countRows[0][0].AsInteger;
        }
        await Db.ExecuteAsync($"DELETE FROM jobs WHERE status = {Completed}");
        output.WriteLine($"  Purged: {purgedCount} completed jobs removed.");

        output.WriteLine();

        // ── Dashboard ───────────────────────────────────────────────────
        output.WriteLine("--- Queue Dashboard ---");
        output.WriteLine($"  {"Queue",-16} {"Pending",8} {"Running",8} {"Done",8} {"Failed",8}");
        output.WriteLine($"  {new string('-', 16)} {new string('-', 8)} {new string('-', 8)} {new string('-', 8)} {new string('-', 8)}");

        // Get counts per queue per status
        var dashboard = new Dictionary<string, long[]>(); // queue -> [pending, running, done, failed]

        await using (var result = await Db.ExecuteAsync(
            "SELECT queue, status, COUNT(*) FROM jobs GROUP BY queue, status ORDER BY queue, status"))
        {
            await foreach (var row in result.GetRowsAsync())
            {
                var queue = row[0].AsText;
                var status = (int)row[1].AsInteger;
                var count = row[2].AsInteger;

                if (!dashboard.ContainsKey(queue))
                    dashboard[queue] = new long[4];
                dashboard[queue][status] = count;
            }
        }

        foreach (var (queue, counts) in dashboard.OrderBy(x => x.Key))
            output.WriteLine($"  {queue,-16} {counts[0],8} {counts[1],8} {counts[2],8} {counts[3],8}");

        output.WriteLine();

        // ── Full job list ───────────────────────────────────────────────
        output.WriteLine("--- All jobs ---");
        output.WriteLine($"  {"ID",3} {"Queue",-14} {"Pri",4} {"Status",-8} {"Retries",8} {"Payload"}");
        output.WriteLine($"  {new string('-', 3)} {new string('-', 14)} {new string('-', 4)} {new string('-', 8)} {new string('-', 8)} {new string('-', 40)}");

        await using (var result = await Db.ExecuteAsync(
            "SELECT id, queue, priority, status, retry_count, payload FROM jobs ORDER BY queue, priority"))
        {
            await foreach (var row in result.GetRowsAsync())
            {
                var id = row[0].AsInteger;
                var queue = row[1].AsText;
                var pri = row[2].AsInteger;
                var status = StatusLabel(row[3].AsInteger);
                var retries = row[4].AsInteger;
                var payload = row[5].AsText;

                output.WriteLine($"  {id,3} {queue,-14} {pri,4} {status,-8} {retries,8} {Truncate(payload, 45)}");
            }
        }
    }

    // ── Job operation helpers ──────────────────────────────────────────────

    private async Task<long> ClaimNextAsync(string queue, long now, TextWriter output)
    {
        // Find highest-priority pending job in queue
        await using var result = await Db.ExecuteAsync(
            $"SELECT id, payload, priority FROM jobs WHERE queue = '{Esc(queue)}' AND status = {Pending} ORDER BY priority DESC LIMIT 1");
        var rows = await result.ToListAsync();
        if (rows.Count == 0)
        {
            output.WriteLine($"  [{queue}] No pending jobs.");
            return -1;
        }

        var jobId = rows[0][0].AsInteger;
        var payload = rows[0][1].AsText;
        var priority = rows[0][2].AsInteger;

        // Atomically claim it
        await Db.BeginTransactionAsync();
        try
        {
            await Db.ExecuteAsync(
                $"UPDATE jobs SET status = {Running}, started_at = {now} WHERE id = {jobId}");
            await Db.CommitAsync();
        }
        catch
        {
            await Db.RollbackAsync();
            throw;
        }

        output.WriteLine($"  [{queue}] Claimed job #{jobId} (pri={priority}): {Truncate(payload, 45)}");
        return jobId;
    }

    private async Task CompleteJobAsync(long jobId, long now, TextWriter output)
    {
        await Db.ExecuteAsync(
            $"UPDATE jobs SET status = {Completed}, completed_at = {now} WHERE id = {jobId}");
        output.WriteLine($"  Job #{jobId} completed.");
    }

    private async Task FailJobAsync(long jobId, string error, long now, TextWriter output)
    {
        // Read current retry info
        await using var result = await Db.ExecuteAsync(
            $"SELECT retry_count, max_retries FROM jobs WHERE id = {jobId}");
        var rows = await result.ToListAsync();
        var retryCount = rows[0][0].AsInteger;
        var maxRetries = rows[0][1].AsInteger;

        await Db.ExecuteAsync(
            $"UPDATE jobs SET status = {Failed}, completed_at = {now}, retry_count = {retryCount + 1}, error_message = '{Esc(error)}' WHERE id = {jobId}");
        output.WriteLine($"  Job #{jobId} failed (attempt {retryCount + 1}/{maxRetries}): {error}");
    }
}
