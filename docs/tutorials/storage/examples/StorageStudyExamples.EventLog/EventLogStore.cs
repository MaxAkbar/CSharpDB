// ============================================================================
// Event Log / Audit Trail Example
// ============================================================================
//
// Demonstrates using CSharpDB as an append-only event store.
// Events are batch-inserted and queried by time range, severity, and source.
// Shows: PrepareInsertBatch, CREATE INDEX, GROUP BY aggregates, ORDER BY,
// UPDATE (reclassify severity), DELETE (log rotation / retention purge).
// ============================================================================

using System.Text;
using CSharpDB.Core;
using StorageStudyExamples.Core;

namespace StorageStudyExamples.EventLog;

public sealed class EventLogStore : DataStoreBase
{
    // Severity levels
    private const int Debug = 0;
    private const int Info = 1;
    private const int Warn = 2;
    private const int Error = 3;
    private const int Critical = 4;

    private static string SeverityLabel(long level) => level switch
    {
        Debug => "DEBUG",
        Info => "INFO",
        Warn => "WARN",
        Error => "ERROR",
        Critical => "CRIT",
        _ => "?"
    };

    private static int ParseSeverity(string label) => label.ToLowerInvariant() switch
    {
        "debug" => Debug,
        "info" => Info,
        "warn" => Warn,
        "error" => Error,
        "crit" or "critical" => Critical,
        _ => -1
    };

    /// <summary>A fixed epoch-seconds anchor shared by seeding and demo.</summary>
    private readonly long _baseTime = 1700000000;

    public override string Name => "Event Log";
    public override string CommandName => "event-log";
    public override string Description => "Append-only event log with batch inserts and analytics.";

    // ── Schema ─────────────────────────────────────────────────────────────

    protected override async Task CreateSchemaAsync()
    {
        await Db.ExecuteAsync("""
            CREATE TABLE events (
                id INTEGER PRIMARY KEY,
                timestamp INTEGER,
                source TEXT,
                severity INTEGER,
                category TEXT,
                message TEXT
            )
            """);
        await Db.ExecuteAsync("CREATE INDEX idx_events_time ON events(timestamp)");
        await Db.ExecuteAsync("CREATE INDEX idx_events_severity ON events(severity)");
    }

    // ── Seed data ──────────────────────────────────────────────────────────

    protected override async Task SeedDataAsync()
    {
        var batch = Db.PrepareInsertBatch("events");
        var id = 1;

        void Add(int offsetSeconds, string source, int severity, string category, string message)
        {
            batch.AddRow(
                DbValue.FromInteger(id++),
                DbValue.FromInteger(_baseTime + offsetSeconds),
                DbValue.FromText(source),
                DbValue.FromInteger(severity),
                DbValue.FromText(category),
                DbValue.FromText(message));
        }

        // AuthService events
        Add(0, "AuthService", Info, "auth", "User alice@corp.com logged in");
        Add(2, "AuthService", Info, "auth", "User bob@corp.com logged in");
        Add(5, "AuthService", Warn, "auth", "Failed login attempt for charlie@corp.com");
        Add(6, "AuthService", Warn, "auth", "Failed login attempt for charlie@corp.com");
        Add(7, "AuthService", Error, "auth", "Account charlie@corp.com locked after 3 failures");
        Add(60, "AuthService", Info, "auth", "User diana@corp.com logged in");
        Add(120, "AuthService", Info, "auth", "User alice@corp.com logged out");

        // PaymentGateway events
        Add(10, "PaymentGateway", Info, "payment", "Payment $49.99 processed for order #1001");
        Add(15, "PaymentGateway", Info, "payment", "Payment $129.00 processed for order #1002");
        Add(30, "PaymentGateway", Error, "payment", "Payment declined for order #1003: insufficient funds");
        Add(45, "PaymentGateway", Info, "payment", "Refund $49.99 issued for order #1001");
        Add(90, "PaymentGateway", Critical, "payment", "Gateway connection timeout after 30s");
        Add(91, "PaymentGateway", Critical, "payment", "Failover to backup gateway initiated");
        Add(95, "PaymentGateway", Info, "payment", "Backup gateway online, processing resumed");

        // Scheduler events
        Add(20, "Scheduler", Debug, "jobs", "Cron job 'cleanup-temp' started");
        Add(22, "Scheduler", Debug, "jobs", "Cron job 'cleanup-temp' completed in 1.2s");
        Add(40, "Scheduler", Info, "jobs", "Cron job 'daily-report' started");
        Add(55, "Scheduler", Info, "jobs", "Cron job 'daily-report' completed in 14.8s");
        Add(80, "Scheduler", Warn, "jobs", "Cron job 'sync-inventory' running longer than expected");
        Add(100, "Scheduler", Error, "jobs", "Cron job 'sync-inventory' failed: connection reset");
        Add(101, "Scheduler", Info, "jobs", "Retrying 'sync-inventory' (attempt 2/3)");
        Add(110, "Scheduler", Info, "jobs", "Cron job 'sync-inventory' completed on retry");

        // Monitoring events
        Add(25, "Monitor", Debug, "health", "CPU usage: 42%");
        Add(50, "Monitor", Debug, "health", "CPU usage: 58%");
        Add(75, "Monitor", Warn, "health", "CPU usage: 87% — approaching threshold");
        Add(85, "Monitor", Error, "health", "Memory usage exceeded 90%");
        Add(86, "Monitor", Info, "health", "GC triggered, memory recovered to 65%");
        Add(105, "Monitor", Debug, "health", "CPU usage: 34%");
        Add(115, "Monitor", Debug, "health", "Disk I/O: 12 MB/s read, 4 MB/s write");

        // API events
        Add(3, "ApiGateway", Info, "http", "GET /api/users 200 12ms");
        Add(8, "ApiGateway", Info, "http", "POST /api/orders 201 45ms");
        Add(35, "ApiGateway", Warn, "http", "GET /api/reports 408 30001ms (timeout)");
        Add(70, "ApiGateway", Info, "http", "GET /api/products 200 8ms");
        Add(88, "ApiGateway", Error, "http", "POST /api/payments 503 2ms (service unavailable)");

        await batch.ExecuteAsync();
    }

    // ── Interactive commands ───────────────────────────────────────────────

    public override IReadOnlyList<CommandInfo> GetCommands() =>
    [
        new("tail", "tail [count]", "Show most recent events (default: 20)"),
        new("log", "log <source> <severity> <message...>", "Append a new event"),
        new("filter", "filter <source|severity|category> <value>", "Filter events by field"),
        new("stats", "stats", "Severity histogram and error rates by source"),
        new("reclassify", "reclassify <id> <new-severity>", "Change event severity"),
        new("purge", "purge <count>", "Delete the oldest N events"),
    ];

    public override async Task<bool> ExecuteCommandAsync(string commandName, string args, TextWriter output)
    {
        switch (commandName)
        {
            case "tail":
                await TailAsync(args, output);
                return true;

            case "log":
                await LogAsync(args, output);
                return true;

            case "filter":
                await FilterAsync(args, output);
                return true;

            case "stats":
                await StatsAsync(output);
                return true;

            case "reclassify":
                await ReclassifyAsync(args, output);
                return true;

            case "purge":
                await PurgeAsync(args, output);
                return true;

            default:
                return false;
        }
    }

    // ── tail [count] ──────────────────────────────────────────────────────

    private async Task TailAsync(string args, TextWriter output)
    {
        var count = 20;
        if (!string.IsNullOrWhiteSpace(args) && int.TryParse(args.Trim(), out var n) && n > 0)
            count = n;

        var events = new List<(long Id, long Severity, string Source, string Message)>();

        await using (var result = await Db.ExecuteAsync(
            $"SELECT id, timestamp, source, severity, message FROM events ORDER BY timestamp DESC LIMIT {count}"))
        {
            await foreach (var row in result.GetRowsAsync())
            {
                events.Add((
                    row[0].AsInteger,
                    row[3].AsInteger,
                    row[2].AsText,
                    row[4].AsText));
            }
        }

        if (events.Count == 0)
        {
            output.WriteLine("(no events)");
            return;
        }

        // Reverse so oldest is first for readability
        events.Reverse();

        foreach (var (id, severity, source, message) in events)
            output.WriteLine($"  #{id,3}  [{SeverityLabel(severity),-5}] {source,-16} {message}");

        output.WriteLine($"  ({events.Count} events)");
    }

    // ── log <source> <severity> <message...> ──────────────────────────────

    private async Task LogAsync(string args, TextWriter output)
    {
        var parts = args.Split(' ', 3, StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 3)
        {
            output.WriteLine("Usage: log <source> <severity> <message...>");
            return;
        }

        var source = parts[0];
        var severityLabel = parts[1];
        var message = parts[2];

        var severity = ParseSeverity(severityLabel);
        if (severity < 0)
        {
            output.WriteLine($"Unknown severity: {severityLabel}. Use: debug, info, warn, error, crit.");
            return;
        }

        // Get next ID
        long nextId;
        await using (var result = await Db.ExecuteAsync("SELECT MAX(id) FROM events"))
        {
            var rows = await result.ToListAsync();
            nextId = rows[0][0].IsNull ? 1 : rows[0][0].AsInteger + 1;
        }

        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

        await Db.ExecuteAsync(
            $"INSERT INTO events VALUES ({nextId}, {timestamp}, '{Esc(source)}', {severity}, 'user', '{Esc(message)}')");

        output.WriteLine($"  Logged event #{nextId}: [{SeverityLabel(severity)}] {source} {message}");
    }

    // ── filter <source|severity|category> <value> ─────────────────────────

    private async Task FilterAsync(string args, TextWriter output)
    {
        var parts = args.Split(' ', 2, StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 2)
        {
            output.WriteLine("Usage: filter <source|severity|category> <value>");
            return;
        }

        var field = parts[0].ToLowerInvariant();
        var value = parts[1].Trim();

        string sql;
        switch (field)
        {
            case "source":
                sql = $"SELECT id, timestamp, source, severity, message FROM events WHERE source = '{Esc(value)}' ORDER BY id";
                break;

            case "severity":
            {
                var sev = ParseSeverity(value);
                if (sev < 0)
                {
                    output.WriteLine($"Unknown severity: {value}. Use: debug, info, warn, error, crit.");
                    return;
                }
                sql = $"SELECT id, timestamp, source, severity, message FROM events WHERE severity >= {sev} ORDER BY id";
                break;
            }

            case "category":
                sql = $"SELECT id, timestamp, source, severity, message FROM events WHERE category = '{Esc(value)}' ORDER BY id";
                break;

            default:
                output.WriteLine($"Unknown filter field: {field}. Use: source, severity, category.");
                return;
        }

        var count = 0;
        await using (var result = await Db.ExecuteAsync(sql))
        {
            await foreach (var row in result.GetRowsAsync())
            {
                var id = row[0].AsInteger;
                var sev = SeverityLabel(row[3].AsInteger);
                var src = row[2].AsText;
                var msg = row[4].AsText;
                output.WriteLine($"  #{id,3}  [{sev,-5}] {src,-16} {msg}");
                count++;
            }
        }

        if (count == 0)
            output.WriteLine("(no matching events)");
        else
            output.WriteLine($"  ({count} events)");
    }

    // ── stats ─────────────────────────────────────────────────────────────

    private async Task StatsAsync(TextWriter output)
    {
        // Severity histogram
        output.WriteLine("Severity distribution:");
        await using (var result = await Db.ExecuteAsync(
            "SELECT severity, COUNT(*) FROM events GROUP BY severity ORDER BY severity"))
        {
            await foreach (var row in result.GetRowsAsync())
            {
                var label = SeverityLabel(row[0].AsInteger);
                var count = row[1].AsInteger;
                var bar = new string('#', (int)count);
                output.WriteLine($"  {label,-8} {count,3}  {bar}");
            }
        }
        output.WriteLine();

        // Error rate by source
        output.WriteLine("Error+ rate by source:");

        var totals = new Dictionary<string, long>();
        await using (var result = await Db.ExecuteAsync(
            "SELECT source, COUNT(*) FROM events GROUP BY source"))
        {
            await foreach (var row in result.GetRowsAsync())
                totals[row[0].AsText] = row[1].AsInteger;
        }

        var errorCounts = new Dictionary<string, long>();
        await using (var result = await Db.ExecuteAsync(
            $"SELECT source, COUNT(*) FROM events WHERE severity >= {Error} GROUP BY source"))
        {
            await foreach (var row in result.GetRowsAsync())
                errorCounts[row[0].AsText] = row[1].AsInteger;
        }

        foreach (var (source, total) in totals.OrderBy(x => x.Key))
        {
            var errors = errorCounts.GetValueOrDefault(source, 0);
            var rate = total > 0 ? (double)errors / total * 100 : 0;
            output.WriteLine($"  {source,-18} {errors}/{total} ({rate:F0}%)");
        }
    }

    // ── reclassify <id> <new-severity> ────────────────────────────────────

    private async Task ReclassifyAsync(string args, TextWriter output)
    {
        var parts = args.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 2)
        {
            output.WriteLine("Usage: reclassify <id> <new-severity>");
            return;
        }

        if (!long.TryParse(parts[0], out var id))
        {
            output.WriteLine($"Invalid event ID: {parts[0]}");
            return;
        }

        var newSeverity = ParseSeverity(parts[1]);
        if (newSeverity < 0)
        {
            output.WriteLine($"Unknown severity: {parts[1]}. Use: debug, info, warn, error, crit.");
            return;
        }

        // Get current severity
        long oldSeverity;
        await using (var result = await Db.ExecuteAsync($"SELECT severity FROM events WHERE id = {id}"))
        {
            var rows = await result.ToListAsync();
            if (rows.Count == 0)
            {
                output.WriteLine($"Event #{id} not found.");
                return;
            }
            oldSeverity = rows[0][0].AsInteger;
        }

        await Db.ExecuteAsync($"UPDATE events SET severity = {newSeverity} WHERE id = {id}");
        output.WriteLine($"  Reclassified event #{id}: {SeverityLabel(oldSeverity)} -> {SeverityLabel(newSeverity)}.");
    }

    // ── purge <count> ─────────────────────────────────────────────────────

    private async Task PurgeAsync(string args, TextWriter output)
    {
        if (!int.TryParse(args.Trim(), out var count) || count <= 0)
        {
            output.WriteLine("Usage: purge <count>");
            return;
        }

        // Fetch the IDs of the oldest N events
        var ids = new List<long>();
        await using (var result = await Db.ExecuteAsync(
            $"SELECT id FROM events ORDER BY timestamp LIMIT {count}"))
        {
            await foreach (var row in result.GetRowsAsync())
                ids.Add(row[0].AsInteger);
        }

        if (ids.Count == 0)
        {
            output.WriteLine("No events to purge.");
            return;
        }

        foreach (var id in ids)
            await Db.ExecuteAsync($"DELETE FROM events WHERE id = {id}");

        output.WriteLine($"  Purged {ids.Count} oldest event(s).");
    }

    // ── Demo ───────────────────────────────────────────────────────────────

    public override async Task RunDemoAsync(TextWriter output)
    {
        // Report the batch insert that happened during seeding
        output.WriteLine("--- Inserting events (batch) ---");
        await using (var countResult = await Db.ExecuteAsync("SELECT COUNT(*) FROM events"))
        {
            var rows = await countResult.ToListAsync();
            output.WriteLine($"  Inserted {rows[0][0].AsInteger} events.");
        }
        output.WriteLine();

        // ── Query 1: Recent events (time window) ───────────────────────
        var windowStart = _baseTime + 30;
        var windowEnd = _baseTime + 100;
        output.WriteLine($"--- Events between t+30s and t+100s ---");

        await using (var result = await Db.ExecuteAsync(
            $"SELECT timestamp, source, severity, message FROM events WHERE timestamp >= {windowStart} AND timestamp <= {windowEnd} ORDER BY timestamp"))
        {
            await foreach (var row in result.GetRowsAsync())
            {
                var ts = row[0].AsInteger - _baseTime;
                var src = row[1].AsText;
                var sev = SeverityLabel(row[2].AsInteger);
                var msg = row[3].AsText;
                output.WriteLine($"  t+{ts,3}s  [{sev,-5}] {src,-16} {msg}");
            }
        }
        output.WriteLine();

        // ── Query 2: Errors and above ──────────────────────────────────
        output.WriteLine("--- Errors and critical events ---");

        await using (var result = await Db.ExecuteAsync(
            $"SELECT timestamp, source, severity, message FROM events WHERE severity >= {Error} ORDER BY timestamp"))
        {
            await foreach (var row in result.GetRowsAsync())
            {
                var ts = row[0].AsInteger - _baseTime;
                var src = row[1].AsText;
                var sev = SeverityLabel(row[2].AsInteger);
                var msg = row[3].AsText;
                output.WriteLine($"  t+{ts,3}s  [{sev,-5}] {src,-16} {msg}");
            }
        }
        output.WriteLine();

        // ── Query 3: Count by severity ─────────────────────────────────
        output.WriteLine("--- Event count by severity ---");

        await using (var result = await Db.ExecuteAsync(
            "SELECT severity, COUNT(*) FROM events GROUP BY severity ORDER BY severity"))
        {
            await foreach (var row in result.GetRowsAsync())
            {
                var label = SeverityLabel(row[0].AsInteger);
                var count = row[1].AsInteger;
                var bar = new string('#', (int)count);
                output.WriteLine($"  {label,-8} {count,3}  {bar}");
            }
        }
        output.WriteLine();

        // ── Query 4: Count by source ───────────────────────────────────
        output.WriteLine("--- Event count by source ---");

        await using (var result = await Db.ExecuteAsync(
            "SELECT source, COUNT(*) FROM events GROUP BY source ORDER BY source"))
        {
            await foreach (var row in result.GetRowsAsync())
            {
                var source = row[0].AsText;
                var count = row[1].AsInteger;
                output.WriteLine($"  {source,-18} {count,3} events");
            }
        }
        output.WriteLine();

        // ── Query 5: Error rate by source ──────────────────────────────
        output.WriteLine("--- Error+ rate by source ---");

        // Get total per source
        var totals = new Dictionary<string, long>();
        await using (var result = await Db.ExecuteAsync(
            "SELECT source, COUNT(*) FROM events GROUP BY source"))
        {
            await foreach (var row in result.GetRowsAsync())
                totals[row[0].AsText] = row[1].AsInteger;
        }

        // Get error+ per source
        await using (var result = await Db.ExecuteAsync(
            $"SELECT source, COUNT(*) FROM events WHERE severity >= {Error} GROUP BY source"))
        {
            await foreach (var row in result.GetRowsAsync())
            {
                var source = row[0].AsText;
                var errors = row[1].AsInteger;
                var total = totals.GetValueOrDefault(source, 1);
                var rate = (double)errors / total * 100;
                output.WriteLine($"  {source,-18} {errors}/{total} ({rate:F0}%)");
            }
        }
        output.WriteLine();

        // ── Mutation 1: Reclassify severity ───────────────────────────
        // An analyst reviewed the "Account locked" event (id=5) and determined
        // it is expected behavior, not an error. Reclassify from ERROR to WARN.
        output.WriteLine("--- Log maintenance ---");

        await Db.ExecuteAsync($"UPDATE events SET severity = {Warn} WHERE id = 5");
        output.WriteLine("  Reclassified: event #5 (Account locked) ERROR -> WARN.");

        // ── Mutation 2: Log rotation / retention purge ────────────────
        // Delete all events before t+30s to simulate a retention policy.
        var cutoff = _baseTime + 30;

        long purgeCount;
        await using (var countResult = await Db.ExecuteAsync(
            $"SELECT COUNT(*) FROM events WHERE timestamp < {cutoff}"))
        {
            var countRows = await countResult.ToListAsync();
            purgeCount = countRows[0][0].AsInteger;
        }

        await Db.ExecuteAsync($"DELETE FROM events WHERE timestamp < {cutoff}");
        output.WriteLine($"  Purged: {purgeCount} events older than t+30s (retention policy).");
        output.WriteLine();

        // ── Re-print severity counts after mutations ──────────────────
        output.WriteLine("--- Event count by severity (after maintenance) ---");

        await using (var result = await Db.ExecuteAsync(
            "SELECT severity, COUNT(*) FROM events GROUP BY severity ORDER BY severity"))
        {
            await foreach (var row in result.GetRowsAsync())
            {
                var label = SeverityLabel(row[0].AsInteger);
                var count = row[1].AsInteger;
                var bar = new string('#', (int)count);
                output.WriteLine($"  {label,-8} {count,3}  {bar}");
            }
        }

        await using (var totalResult = await Db.ExecuteAsync("SELECT COUNT(*) FROM events"))
        {
            var totalRows = await totalResult.ToListAsync();
            output.WriteLine($"  Total remaining: {totalRows[0][0].AsInteger} events");
        }
    }
}
