// ============================================================================
// Key-Value Configuration Store Example
// ============================================================================
//
// Demonstrates using CSharpDB as a persistent hierarchical config system
// with namespaces, versioning, and change history -- similar to etcd or
// Windows Registry.
// Shows: transactions (SELECT then INSERT/UPDATE), CREATE INDEX, history
// tracking via a second table, batch UPDATE (rename namespace),
// bulk DELETE (drop entire namespace).
// ============================================================================

using CSharpDB.Primitives;
using CSharpDB.Engine;
using StorageStudyExamples.Core;

namespace StorageStudyExamples.ConfigStore;

public sealed class ConfigDataStore : DataStoreBase
{
    private int _nextEntryId = 1;
    private int _nextHistoryId = 1;

    /// <summary>A fixed epoch-seconds anchor shared by seeding and demo.</summary>
    private readonly long _baseTime = 1700000000;

    public override string Name => "Config Store";
    public override string CommandName => "config-store";
    public override string Description => "Hierarchical config store with versioning and change history.";

    // ── Schema ─────────────────────────────────────────────────────────────

    protected override async Task CreateSchemaAsync()
    {
        await Db.ExecuteAsync("""
            CREATE TABLE config_entries (
                id INTEGER PRIMARY KEY,
                namespace TEXT,
                config_key TEXT,
                value TEXT,
                value_type TEXT,
                version INTEGER,
                updated_at INTEGER
            )
            """);

        await Db.ExecuteAsync("""
            CREATE TABLE config_history (
                id INTEGER PRIMARY KEY,
                namespace TEXT,
                config_key TEXT,
                old_value TEXT,
                new_value TEXT,
                changed_at INTEGER
            )
            """);

        await Db.ExecuteAsync("CREATE INDEX idx_config_ns ON config_entries(namespace)");
    }

    // ── Seed data ──────────────────────────────────────────────────────────

    protected override async Task SeedDataAsync()
    {
        long t = _baseTime;

        // app/display
        await SetConfigAsync("app/display", "theme", "light", "string", t);
        await SetConfigAsync("app/display", "font-size", "14", "int", t);
        await SetConfigAsync("app/display", "language", "en-US", "string", t);
        await SetConfigAsync("app/display", "sidebar-visible", "true", "bool", t);

        // app/network
        await SetConfigAsync("app/network", "timeout-ms", "5000", "int", t);
        await SetConfigAsync("app/network", "retries", "3", "int", t);
        await SetConfigAsync("app/network", "base-url", "https://api.example.com/v2", "string", t);
        await SetConfigAsync("app/network", "proxy-enabled", "false", "bool", t);

        // system/logging
        await SetConfigAsync("system/logging", "level", "info", "string", t);
        await SetConfigAsync("system/logging", "file-path", "/var/log/app.log", "string", t);
        await SetConfigAsync("system/logging", "max-size-mb", "100", "int", t);
        await SetConfigAsync("system/logging", "rotate-count", "5", "int", t);
    }

    // ── Commands ───────────────────────────────────────────────────────────

    public override IReadOnlyList<CommandInfo> GetCommands() =>
    [
        new("list",       "list [namespace]",       "List config entries (all or filtered by namespace)"),
        new("get",        "get <namespace> <key>",   "Get a config value"),
        new("set",        "set <namespace> <key> <value> [type]", "Set/update a config value (type defaults to \"string\")"),
        new("delete",     "delete <namespace> <key>", "Delete a config entry"),
        new("history",    "history [namespace]",     "Show change history (all or filtered)"),
        new("namespaces", "namespaces",              "List all namespaces with key counts"),
        new("rename-ns",  "rename-ns <old> <new>",   "Rename a namespace (batch UPDATE in transaction)"),
        new("drop-ns",    "drop-ns <namespace>",     "Delete all entries in namespace (with history, in transaction)"),
    ];

    public override async Task<bool> ExecuteCommandAsync(string commandName, string args, TextWriter output)
    {
        switch (commandName)
        {
            case "list":
                await ListConfigAsync(args.Trim(), output);
                return true;

            case "get":
            {
                var parts = args.Trim().Split(' ', 2, StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length < 2)
                {
                    output.WriteLine("Usage: get <namespace> <key>");
                    return true;
                }
                await GetConfigAsync(parts[0], parts[1], output);
                return true;
            }

            case "set":
            {
                var parts = args.Trim().Split(' ', 4, StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length < 3)
                {
                    output.WriteLine("Usage: set <namespace> <key> <value> [type]");
                    return true;
                }
                var ns = parts[0];
                var key = parts[1];
                var valueAndType = parts.Length >= 4 ? parts[2..] : [parts[2]];
                string value;
                string valueType;
                if (parts.Length >= 4)
                {
                    value = parts[2];
                    valueType = parts[3];
                }
                else
                {
                    value = parts[2];
                    valueType = "string";
                }
                var timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                await SetConfigAsync(ns, key, value, valueType, timestamp);
                output.WriteLine($"  Set {ns}/{key} = {value} [{valueType}]");
                return true;
            }

            case "delete":
            {
                var parts = args.Trim().Split(' ', 2, StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length < 2)
                {
                    output.WriteLine("Usage: delete <namespace> <key>");
                    return true;
                }
                var timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                await DeleteConfigAsync(parts[0], parts[1], timestamp);
                output.WriteLine($"  Deleted {parts[0]}/{parts[1]}");
                return true;
            }

            case "history":
                await ShowHistoryAsync(args.Trim(), output);
                return true;

            case "namespaces":
                await ListNamespacesAsync(output);
                return true;

            case "rename-ns":
            {
                var parts = args.Trim().Split(' ', 2, StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length < 2)
                {
                    output.WriteLine("Usage: rename-ns <old> <new>");
                    return true;
                }
                await RenameNamespaceAsync(parts[0], parts[1], output);
                return true;
            }

            case "drop-ns":
            {
                var ns = args.Trim();
                if (string.IsNullOrEmpty(ns))
                {
                    output.WriteLine("Usage: drop-ns <namespace>");
                    return true;
                }
                await DropNamespaceAsync(ns, output);
                return true;
            }

            default:
                return false;
        }
    }

    // ── Command implementations ───────────────────────────────────────────

    private async Task ListConfigAsync(string namespaceFilter, TextWriter output)
    {
        string sql;
        if (string.IsNullOrEmpty(namespaceFilter))
        {
            sql = "SELECT namespace, config_key, value, value_type, version FROM config_entries ORDER BY namespace, config_key";
        }
        else
        {
            sql = $"SELECT namespace, config_key, value, value_type, version FROM config_entries WHERE namespace = '{Esc(namespaceFilter)}' ORDER BY namespace, config_key";
        }

        string? currentNamespace = null;
        bool any = false;

        await using (var result = await Db.ExecuteAsync(sql))
        {
            await foreach (var row in result.GetRowsAsync())
            {
                any = true;
                var ns = row[0].AsText;
                var key = row[1].AsText;
                var value = row[2].AsText;
                var type = row[3].AsText;
                var version = row[4].AsInteger;

                if (ns != currentNamespace)
                {
                    if (currentNamespace != null) output.WriteLine();
                    output.WriteLine($"  [{ns}]");
                    currentNamespace = ns;
                }

                var versionTag = version > 1 ? $" (v{version})" : "";
                output.WriteLine($"    {key,-20} = {value,-30} [{type}]{versionTag}");
            }
        }

        if (!any)
        {
            if (string.IsNullOrEmpty(namespaceFilter))
                output.WriteLine("  (no config entries)");
            else
                output.WriteLine($"  No entries in namespace '{namespaceFilter}'.");
        }
    }

    private async Task GetConfigAsync(string ns, string key, TextWriter output)
    {
        await using var result = await Db.ExecuteAsync(
            $"SELECT value, value_type, version FROM config_entries WHERE namespace = '{Esc(ns)}' AND config_key = '{Esc(key)}'");
        var rows = await result.ToListAsync();

        if (rows.Count == 0)
        {
            output.WriteLine($"  Key not found: {ns}/{key}");
            return;
        }

        var value = rows[0][0].AsText;
        var type = rows[0][1].AsText;
        var version = rows[0][2].AsInteger;

        output.WriteLine($"  {ns}/{key} = {value} [{type}] (v{version})");
    }

    private async Task ShowHistoryAsync(string namespaceFilter, TextWriter output)
    {
        string sql;
        if (string.IsNullOrEmpty(namespaceFilter))
        {
            sql = "SELECT namespace, config_key, old_value, new_value, changed_at FROM config_history ORDER BY changed_at";
        }
        else
        {
            sql = $"SELECT namespace, config_key, old_value, new_value, changed_at FROM config_history WHERE namespace = '{Esc(namespaceFilter)}' ORDER BY changed_at";
        }

        output.WriteLine($"  {"Timestamp",-14} {"Namespace",-16} {"Key",-20} {"Old",-14} {"New",-14}");
        output.WriteLine($"  {new string('-', 14)} {new string('-', 16)} {new string('-', 20)} {new string('-', 14)} {new string('-', 14)}");

        bool any = false;
        await using (var result = await Db.ExecuteAsync(sql))
        {
            await foreach (var row in result.GetRowsAsync())
            {
                any = true;
                var ns = row[0].AsText;
                var key = row[1].AsText;
                var oldVal = row[2].AsText;
                var newVal = row[3].AsText;
                var ts = row[4].AsInteger;

                output.WriteLine($"  {ts,-14} {ns,-16} {key,-20} {Truncate(oldVal, 14),-14} {Truncate(newVal, 14),-14}");
            }
        }

        if (!any)
            output.WriteLine("  (no history)");
    }

    private async Task ListNamespacesAsync(TextWriter output)
    {
        bool any = false;
        await using (var result = await Db.ExecuteAsync(
            "SELECT namespace, COUNT(*) FROM config_entries GROUP BY namespace ORDER BY namespace"))
        {
            await foreach (var row in result.GetRowsAsync())
            {
                any = true;
                output.WriteLine($"  {row[0].AsText,-24} {row[1].AsInteger} keys");
            }
        }

        if (!any)
            output.WriteLine("  (no namespaces)");
    }

    private async Task RenameNamespaceAsync(string oldNs, string newNs, TextWriter output)
    {
        // Count entries to move
        long count;
        await using (var countResult = await Db.ExecuteAsync(
            $"SELECT COUNT(*) FROM config_entries WHERE namespace = '{Esc(oldNs)}'"))
        {
            var countRows = await countResult.ToListAsync();
            count = countRows[0][0].AsInteger;
        }

        if (count == 0)
        {
            output.WriteLine($"  Namespace '{oldNs}' not found or empty.");
            return;
        }

        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

        await Db.BeginTransactionAsync();
        try
        {
            await Db.ExecuteAsync(
                $"UPDATE config_entries SET namespace = '{Esc(newNs)}' WHERE namespace = '{Esc(oldNs)}'");

            await Db.ExecuteAsync(
                $"INSERT INTO config_history VALUES ({_nextHistoryId++}, '{Esc(oldNs)}', '(namespace)', '{Esc(oldNs)}', '{Esc(newNs)}', {timestamp})");

            await Db.CommitAsync();
        }
        catch
        {
            await Db.RollbackAsync();
            throw;
        }

        output.WriteLine($"  Renamed namespace: {oldNs} -> {newNs} ({count} keys moved).");
    }

    private async Task DropNamespaceAsync(string ns, TextWriter output)
    {
        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

        await Db.BeginTransactionAsync();
        try
        {
            // Record each deletion in history
            await using (var entries = await Db.ExecuteAsync(
                $"SELECT config_key, value FROM config_entries WHERE namespace = '{Esc(ns)}'"))
            {
                await foreach (var row in entries.GetRowsAsync())
                {
                    var key = row[0].AsText;
                    var value = row[1].AsText;
                    await Db.ExecuteAsync(
                        $"INSERT INTO config_history VALUES ({_nextHistoryId++}, '{Esc(ns)}', '{Esc(key)}', '{Esc(value)}', '(deleted)', {timestamp})");
                }
            }

            await using (var deleteResult = await Db.ExecuteAsync(
                $"DELETE FROM config_entries WHERE namespace = '{Esc(ns)}'"))
            {
                output.WriteLine($"  Dropped namespace '{ns}': {deleteResult.RowsAffected} entries deleted.");
            }

            await Db.CommitAsync();
        }
        catch
        {
            await Db.RollbackAsync();
            throw;
        }
    }

    // ── Demo ───────────────────────────────────────────────────────────────

    public override async Task RunDemoAsync(TextWriter output)
    {
        long t = _baseTime;

        output.WriteLine("--- Setting initial configuration ---");
        output.WriteLine("  12 config entries set across 3 namespaces.");
        output.WriteLine();

        // ── Make some changes ───────────────────────────────────────────
        output.WriteLine("--- Applying configuration changes ---");

        await SetConfigAsync("app/display", "theme", "dark", "string", t + 60);
        output.WriteLine("  app/display/theme: light -> dark");

        await SetConfigAsync("app/display", "font-size", "16", "int", t + 60);
        output.WriteLine("  app/display/font-size: 14 -> 16");

        await SetConfigAsync("app/network", "timeout-ms", "10000", "int", t + 120);
        output.WriteLine("  app/network/timeout-ms: 5000 -> 10000");

        await SetConfigAsync("system/logging", "level", "debug", "string", t + 180);
        output.WriteLine("  system/logging/level: info -> debug");

        await DeleteConfigAsync("app/network", "proxy-enabled", t + 200);
        output.WriteLine("  app/network/proxy-enabled: deleted");

        await SetConfigAsync("app/display", "theme", "solarized", "string", t + 300);
        output.WriteLine("  app/display/theme: dark -> solarized");

        output.WriteLine();

        // ── Namespace operations ───────────────────────────────────────
        output.WriteLine("--- Namespace operations ---");

        // Rename namespace: app/display -> app/ui
        // This is a batch UPDATE affecting multiple rows at once.
        long renameCount;
        await using (var countResult = await Db.ExecuteAsync(
            "SELECT COUNT(*) FROM config_entries WHERE namespace = 'app/display'"))
        {
            var countRows = await countResult.ToListAsync();
            renameCount = countRows[0][0].AsInteger;
        }

        await Db.BeginTransactionAsync();
        try
        {
            await Db.ExecuteAsync(
                "UPDATE config_entries SET namespace = 'app/ui' WHERE namespace = 'app/display'");

            // Record the rename in history
            await Db.ExecuteAsync(
                $"INSERT INTO config_history VALUES ({_nextHistoryId++}, 'app/display', '(namespace)', 'app/display', 'app/ui', {t + 400})");

            await Db.CommitAsync();
        }
        catch
        {
            await Db.RollbackAsync();
            throw;
        }
        output.WriteLine($"  Renamed namespace: app/display -> app/ui ({renameCount} keys moved).");

        // Bulk delete: remove entire system/logging namespace
        // First record each key deletion in history, then delete all at once.
        await Db.BeginTransactionAsync();
        try
        {
            await using (var loggingKeys = await Db.ExecuteAsync(
                "SELECT config_key, value FROM config_entries WHERE namespace = 'system/logging'"))
            {
                await foreach (var row in loggingKeys.GetRowsAsync())
                {
                    var key = row[0].AsText;
                    var value = row[1].AsText;
                    await Db.ExecuteAsync(
                        $"INSERT INTO config_history VALUES ({_nextHistoryId++}, 'system/logging', '{Esc(key)}', '{Esc(value)}', '(deleted)', {t + 400})");
                }
            }

            await Db.ExecuteAsync(
                "DELETE FROM config_entries WHERE namespace = 'system/logging'");
            await Db.CommitAsync();
        }
        catch
        {
            await Db.RollbackAsync();
            throw;
        }
        output.WriteLine("  Deleted namespace: system/logging (all keys removed).");

        output.WriteLine();

        // ── Print current config ────────────────────────────────────────
        output.WriteLine("--- Current configuration ---");

        string? currentNamespace = null;
        await using (var result = await Db.ExecuteAsync(
            "SELECT namespace, config_key, value, value_type, version FROM config_entries ORDER BY namespace, config_key"))
        {
            await foreach (var row in result.GetRowsAsync())
            {
                var ns = row[0].AsText;
                var key = row[1].AsText;
                var value = row[2].AsText;
                var type = row[3].AsText;
                var version = row[4].AsInteger;

                if (ns != currentNamespace)
                {
                    if (currentNamespace != null) output.WriteLine();
                    output.WriteLine($"  [{ns}]");
                    currentNamespace = ns;
                }

                var versionTag = version > 1 ? $" (v{version})" : "";
                output.WriteLine($"    {key,-20} = {value,-30} [{type}]{versionTag}");
            }
        }
        output.WriteLine();

        // ── Print change history ────────────────────────────────────────
        output.WriteLine("--- Change history ---");
        output.WriteLine($"  {"Time",-8} {"Namespace",-16} {"Key",-20} {"Old",-14} {"New",-14}");
        output.WriteLine($"  {new string('-', 8)} {new string('-', 16)} {new string('-', 20)} {new string('-', 14)} {new string('-', 14)}");

        await using (var result = await Db.ExecuteAsync(
            "SELECT namespace, config_key, old_value, new_value, changed_at FROM config_history ORDER BY changed_at"))
        {
            await foreach (var row in result.GetRowsAsync())
            {
                var ns = row[0].AsText;
                var key = row[1].AsText;
                var oldVal = row[2].AsText;
                var newVal = row[3].AsText;
                var ts = row[4].AsInteger - t;

                output.WriteLine($"  t+{ts,-5} {ns,-16} {key,-20} {Truncate(oldVal, 14),-14} {Truncate(newVal, 14),-14}");
            }
        }
        output.WriteLine();

        // ── Summary stats ───────────────────────────────────────────────
        output.WriteLine("--- Summary ---");

        await using (var result = await Db.ExecuteAsync(
            "SELECT namespace, COUNT(*) FROM config_entries GROUP BY namespace ORDER BY namespace"))
        {
            await foreach (var row in result.GetRowsAsync())
                output.WriteLine($"  {row[0].AsText,-20} {row[1].AsInteger} keys");
        }

        await using (var histResult = await Db.ExecuteAsync("SELECT COUNT(*) FROM config_history"))
        {
            var rows = await histResult.ToListAsync();
            output.WriteLine($"  Total changes:       {rows[0][0].AsInteger}");
        }
    }

    // ── Private helpers ────────────────────────────────────────────────────

    private async Task SetConfigAsync(string ns, string key, string value, string valueType, long timestamp)
    {
        // Check if key already exists
        string? oldValue = null;
        long existingId = -1;
        long existingVersion = 0;

        await using (var result = await Db.ExecuteAsync(
            $"SELECT id, value, version FROM config_entries WHERE namespace = '{Esc(ns)}' AND config_key = '{Esc(key)}'"))
        {
            var rows = await result.ToListAsync();
            if (rows.Count > 0)
            {
                existingId = rows[0][0].AsInteger;
                oldValue = rows[0][1].AsText;
                existingVersion = rows[0][2].AsInteger;
            }
        }

        await Db.BeginTransactionAsync();
        try
        {
            if (existingId >= 0)
            {
                // Update existing entry
                await Db.ExecuteAsync(
                    $"UPDATE config_entries SET value = '{Esc(value)}', value_type = '{Esc(valueType)}', version = {existingVersion + 1}, updated_at = {timestamp} WHERE id = {existingId}");

                // Record change in history
                await Db.ExecuteAsync(
                    $"INSERT INTO config_history VALUES ({_nextHistoryId++}, '{Esc(ns)}', '{Esc(key)}', '{Esc(oldValue!)}', '{Esc(value)}', {timestamp})");
            }
            else
            {
                // Insert new entry
                await Db.ExecuteAsync(
                    $"INSERT INTO config_entries VALUES ({_nextEntryId++}, '{Esc(ns)}', '{Esc(key)}', '{Esc(value)}', '{Esc(valueType)}', 1, {timestamp})");
            }

            await Db.CommitAsync();
        }
        catch
        {
            await Db.RollbackAsync();
            throw;
        }
    }

    private async Task DeleteConfigAsync(string ns, string key, long timestamp)
    {
        // Get current value for history
        await using var result = await Db.ExecuteAsync(
            $"SELECT value FROM config_entries WHERE namespace = '{Esc(ns)}' AND config_key = '{Esc(key)}'");
        var rows = await result.ToListAsync();
        if (rows.Count == 0) return;

        var oldValue = rows[0][0].AsText;

        await Db.BeginTransactionAsync();
        try
        {
            await Db.ExecuteAsync(
                $"DELETE FROM config_entries WHERE namespace = '{Esc(ns)}' AND config_key = '{Esc(key)}'");
            await Db.ExecuteAsync(
                $"INSERT INTO config_history VALUES ({_nextHistoryId++}, '{Esc(ns)}', '{Esc(key)}', '{Esc(oldValue)}', '(deleted)', {timestamp})");
            await Db.CommitAsync();
        }
        catch
        {
            await Db.RollbackAsync();
            throw;
        }
    }
}
