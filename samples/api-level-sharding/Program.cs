using System.Globalization;
using CSharpDB.Client;
using CSharpDB.Client.Models;

return await ApiLevelShardingSampleProgram.MainAsync();

internal static class ApiLevelShardingSampleProgram
{
    private const string Keyspace = "orders_by_month";
    private const string CustomerId = "customer-1001";
    private const string CurrentMonth = "2026-06";
    private const int VirtualBucketCount = 16;
    private const int PreviewPageSize = 3;
    private const int OrderHistoryPageSize = 10;

    public static async Task<int> MainAsync()
    {
        string sampleDirectory = AppContext.BaseDirectory;
        string dataDirectory = Path.Combine(sampleDirectory, "shards");

        PrepareDirectory(dataDirectory);

        var options = CreateShardingOptions(dataDirectory);
        await using var sharded = await CSharpDbShardedClient.CreateAsync(options);
        ICSharpDbShardAdminClient shardAdmin = sharded;

        await CreateSchemaOnEveryShardAsync(shardAdmin);
        await SeedOrderHistoryAsync(sharded);
        await DemonstrateTransactionRoutingAsync(sharded);

        Console.WriteLine("API-Level Sharding Sample: E-Commerce Order History");
        Console.WriteLine();
        Console.WriteLine($"Keyspace:        {Keyspace}");
        Console.WriteLine($"Route key shape: yyyy-MM order month");
        Console.WriteLine($"Customer:        {CustomerId}");
        Console.WriteLine($"Virtual buckets: {VirtualBucketCount}");
        Console.WriteLine($"Shard files:     {dataDirectory}");
        Console.WriteLine();

        await PrintRouteMapAsync(shardAdmin, ["2026-06", "2026-05", "2026-04", "2025-12"]);
        await PrintShardAdminSnapshotAsync(shardAdmin);
        await PrintRecentOrdersPageAsync(sharded);
        await PrintOlderMonthPageAsync(sharded, "2026-05");
        await PrintFilledHistoryPageAsync(sharded, ["2026-06", "2026-05", "2026-04", "2025-12"]);
        await PrintManualHistorySummaryAsync(sharded, ["2026-06", "2026-05", "2026-04", "2025-12"]);
        await PrintShardCountsAsync(sharded, options.Shards);
        await PrintMissingRouteFailureAsync(sharded);

        Console.WriteLine();
        Console.WriteLine("Inspect a shard directly with the CLI:");
        Console.WriteLine($"  dotnet run --project src/CSharpDB.Cli -- \"{Path.Combine(dataDirectory, "shard-0.db")}\"");
        Console.WriteLine("  csdb> SELECT order_month, customer_id, order_number, amount FROM orders ORDER BY order_date DESC;");

        return 0;
    }

    private static CSharpDbShardingOptions CreateShardingOptions(string dataDirectory)
        => new()
        {
            Enabled = true,
            Keyspace = Keyspace,
            MapVersion = 1,
            VirtualBucketCount = VirtualBucketCount,
            Shards =
            [
                new CSharpDbShardDefinition { ShardId = "shard-0", DataSource = Path.Combine(dataDirectory, "shard-0.db") },
                new CSharpDbShardDefinition { ShardId = "shard-1", DataSource = Path.Combine(dataDirectory, "shard-1.db") },
                new CSharpDbShardDefinition { ShardId = "shard-2", DataSource = Path.Combine(dataDirectory, "shard-2.db") },
                new CSharpDbShardDefinition { ShardId = "shard-3", DataSource = Path.Combine(dataDirectory, "shard-3.db") },
            ],
            BucketRanges =
            [
                new CSharpDbShardBucketRange { StartBucketInclusive = 0, EndBucketExclusive = 4, ShardId = "shard-0" },
                new CSharpDbShardBucketRange { StartBucketInclusive = 4, EndBucketExclusive = 8, ShardId = "shard-1" },
                new CSharpDbShardBucketRange { StartBucketInclusive = 8, EndBucketExclusive = 12, ShardId = "shard-2" },
                new CSharpDbShardBucketRange { StartBucketInclusive = 12, EndBucketExclusive = 16, ShardId = "shard-3" },
            ],
            ExactKeyPins = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                // Operators can pin hot or archival months to chosen shards.
                ["2026-06"] = "shard-0",
                ["2026-05"] = "shard-1",
                ["2026-04"] = "shard-2",
                ["2025-12"] = "shard-3",
            },
        };

    private static async Task CreateSchemaOnEveryShardAsync(ICSharpDbShardAdminClient shardAdmin)
    {
        IReadOnlyList<CSharpDbShardSqlExecutionResult> results = await shardAdmin.ExecuteSqlOnAllShardsAsync("""
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                order_month TEXT NOT NULL,
                customer_id TEXT NOT NULL,
                order_number TEXT NOT NULL,
                order_date TEXT NOT NULL,
                amount REAL NOT NULL,
                status TEXT NOT NULL
            );
            """);

        ThrowIfAnyShardFailed(results);

        results = await shardAdmin.ExecuteSqlOnAllShardsAsync(
            "CREATE INDEX idx_orders_customer_date ON orders (customer_id, order_date);");

        ThrowIfAnyShardFailed(results);
    }

    private static async Task SeedOrderHistoryAsync(CSharpDbShardedClient sharded)
    {
        IReadOnlyList<OrderSeed> orders =
        [
            new(1001, "2026-06", CustomerId, "SO-202606-1001", "2026-06-01T10:15:00Z", 129.95m, "placed"),
            new(1002, "2026-06", CustomerId, "SO-202606-1002", "2026-06-01T09:05:00Z", 42.50m, "placed"),
            new(1003, "2026-06", CustomerId, "SO-202606-1003", "2026-06-01T08:30:00Z", 310.00m, "packed"),
            new(1004, "2026-06", CustomerId, "SO-202606-1004", "2026-06-01T07:55:00Z", 18.75m, "delivered"),
            new(1013, "2026-06", CustomerId, "SO-202606-1013", "2026-06-01T07:15:00Z", 61.00m, "delivered"),
            new(1005, "2026-05", CustomerId, "SO-202605-1005", "2026-05-29T14:20:00Z", 88.40m, "delivered"),
            new(1006, "2026-05", CustomerId, "SO-202605-1006", "2026-05-22T16:45:00Z", 54.10m, "delivered"),
            new(1007, "2026-05", CustomerId, "SO-202605-1007", "2026-05-07T11:10:00Z", 219.99m, "returned"),
            new(1014, "2026-05", CustomerId, "SO-202605-1014", "2026-05-01T09:30:00Z", 34.99m, "delivered"),
            new(1008, "2026-04", CustomerId, "SO-202604-1008", "2026-04-17T12:00:00Z", 73.35m, "delivered"),
            new(1009, "2026-04", CustomerId, "SO-202604-1009", "2026-04-03T18:25:00Z", 19.99m, "delivered"),
            new(1010, "2025-12", CustomerId, "SO-202512-1010", "2025-12-19T20:10:00Z", 540.00m, "delivered"),
            new(1011, "2026-06", "customer-2002", "SO-202606-2002", "2026-06-01T11:40:00Z", 64.25m, "placed"),
            new(1012, "2026-05", "customer-2002", "SO-202605-2002", "2026-05-15T13:05:00Z", 121.00m, "delivered"),
        ];

        foreach (OrderSeed order in orders)
        {
            ICSharpDbClient monthClient = sharded.ForRoute(RouteForMonth(order.OrderMonth));
            await InsertOrderAsync(monthClient, order);
        }
    }

    private static async Task DemonstrateTransactionRoutingAsync(CSharpDbShardedClient sharded)
    {
        CSharpDbRouteContext route = RouteForMonth(CurrentMonth);
        ICSharpDbClient currentMonth = sharded.ForRoute(route);
        TransactionSessionInfo transaction = await currentMonth.BeginTransactionAsync();

        await sharded.ExecuteInTransactionAsync(
            transaction.TransactionId,
            """
            INSERT INTO orders (id, order_month, customer_id, order_number, order_date, amount, status)
            VALUES (2000, '2026-06', 'customer-1001', 'SO-202606-TX', '2026-06-01T12:05:00Z', 27.25, 'placed');
            """);

        await sharded.CommitTransactionAsync(transaction.TransactionId);
    }

    private static async Task InsertOrderAsync(ICSharpDbClient client, OrderSeed order)
    {
        await client.InsertRowAsync("orders", new Dictionary<string, object?>
        {
            ["id"] = order.Id,
            ["order_month"] = order.OrderMonth,
            ["customer_id"] = order.CustomerId,
            ["order_number"] = order.OrderNumber,
            ["order_date"] = order.OrderDate,
            ["amount"] = decimal.ToDouble(order.Amount),
            ["status"] = order.Status,
        });
    }

    private static async Task PrintRouteMapAsync(ICSharpDbShardAdminClient shardAdmin, IReadOnlyList<string> months)
    {
        Console.WriteLine("Month route map");
        Console.WriteLine("---------------");
        foreach (string month in months)
        {
            CSharpDbRouteContext route = RouteForMonth(month);
            CSharpDbShardResolution resolution = await shardAdmin.ResolveRouteAsync(route);
            Console.WriteLine(
                $"{month,-7} bucket={resolution.Bucket,2} shard={resolution.ShardId,-7} token=0x{resolution.Token:X16}");
        }

        Console.WriteLine();
    }

    private static async Task PrintShardAdminSnapshotAsync(ICSharpDbShardAdminClient shardAdmin)
    {
        CSharpDbShardMapSnapshot map = await shardAdmin.GetShardMapAsync();
        IReadOnlyList<CSharpDbShardStatus> statuses = await shardAdmin.GetShardStatusAsync();

        Console.WriteLine("Shard admin snapshot");
        Console.WriteLine("--------------------");
        Console.WriteLine($"Map version:       {map.MapVersion}");
        Console.WriteLine($"Shard definitions: {map.Shards.Count}");
        Console.WriteLine($"Bucket ranges:     {map.BucketRanges.Count}");
        Console.WriteLine($"Exact pins:        {map.ExactKeyPins.Count}");
        Console.WriteLine($"Directory indexes: {map.Directories.Count} (read-only placeholder for future global lookups)");
        foreach (CSharpDbShardStatus status in statuses)
            Console.WriteLine($"{status.ShardId,-7} enabled={status.Enabled,-5} healthy={status.Healthy,-5} source={status.DataSource}");

        Console.WriteLine();
    }

    private static async Task PrintRecentOrdersPageAsync(CSharpDbShardedClient sharded)
    {
        Console.WriteLine("Recent orders page");
        Console.WriteLine("------------------");
        Console.WriteLine($"Route key: {CurrentMonth}");

        ICSharpDbClient currentMonth = sharded.ForRoute(RouteForMonth(CurrentMonth));
        SqlExecutionResult result = await currentMonth.ExecuteSqlAsync($"""
            SELECT order_number, order_date, amount, status
            FROM orders
            WHERE order_month = '{CurrentMonth}' AND customer_id = '{CustomerId}'
            ORDER BY order_date DESC
            LIMIT {PreviewPageSize} OFFSET 0;
            """);

        PrintOrderRows(result);
        Console.WriteLine();
    }

    private static async Task PrintOlderMonthPageAsync(CSharpDbShardedClient sharded, string month)
    {
        Console.WriteLine("Older history page selected by user");
        Console.WriteLine("-----------------------------------");
        Console.WriteLine($"Route key: {month}");

        ICSharpDbClient selectedMonth = sharded.ForRoute(RouteForMonth(month));
        SqlExecutionResult result = await selectedMonth.ExecuteSqlAsync($"""
            SELECT order_number, order_date, amount, status
            FROM orders
            WHERE order_month = '{month}' AND customer_id = '{CustomerId}'
            ORDER BY order_date DESC
            LIMIT {PreviewPageSize} OFFSET 0;
            """);

        PrintOrderRows(result);
        Console.WriteLine();
    }

    private static async Task PrintFilledHistoryPageAsync(CSharpDbShardedClient sharded, IReadOnlyList<string> months)
    {
        Console.WriteLine("Filled cross-month page");
        Console.WriteLine("-----------------------");
        Console.WriteLine($"Requested {OrderHistoryPageSize} orders. Query months newest-to-oldest until the page is full.");

        IReadOnlyList<OrderPageRow> rows = await LoadOrderHistoryPageAsync(
            sharded,
            months,
            CustomerId,
            pageNumber: 1,
            pageSize: OrderHistoryPageSize);

        foreach (var group in rows.GroupBy(row => row.RouteKey))
            Console.WriteLine($"Route {group.Key} supplied {group.Count()} row(s).");

        Console.WriteLine();
        foreach (OrderPageRow row in rows)
        {
            Console.WriteLine(
                $"{row.RouteKey,-7} {row.OrderNumber,-15} {row.OrderDate,-20} {row.Amount.ToString("C", CultureInfo.GetCultureInfo("en-US")),8} {row.Status}");
        }

        Console.WriteLine();
    }

    private static async Task<IReadOnlyList<OrderPageRow>> LoadOrderHistoryPageAsync(
        CSharpDbShardedClient sharded,
        IReadOnlyList<string> months,
        string customerId,
        int pageNumber,
        int pageSize)
    {
        if (pageNumber <= 0)
            throw new ArgumentOutOfRangeException(nameof(pageNumber), "Page number must be greater than 0.");
        if (pageSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(pageSize), "Page size must be greater than 0.");

        int remainingToSkip = (pageNumber - 1) * pageSize;
        int remainingToTake = pageSize;
        var pageRows = new List<OrderPageRow>(pageSize);

        foreach (string month in months)
        {
            int routeCount = await CountOrdersForRouteAsync(sharded, month, customerId);
            if (remainingToSkip >= routeCount)
            {
                remainingToSkip -= routeCount;
                continue;
            }

            int routeOffset = remainingToSkip;
            remainingToSkip = 0;

            int routeTake = Math.Min(remainingToTake, routeCount - routeOffset);
            if (routeTake <= 0)
                continue;

            IReadOnlyList<OrderPageRow> routeRows = await LoadOrdersForRouteAsync(
                sharded,
                month,
                customerId,
                routeTake,
                routeOffset);

            pageRows.AddRange(routeRows);
            remainingToTake -= routeRows.Count;

            if (remainingToTake == 0)
                break;
        }

        return pageRows;
    }

    private static async Task<int> CountOrdersForRouteAsync(
        CSharpDbShardedClient sharded,
        string month,
        string customerId)
    {
        ICSharpDbClient monthClient = sharded.ForRoute(RouteForMonth(month));
        SqlExecutionResult result = await monthClient.ExecuteSqlAsync($"""
            SELECT COUNT(*) AS order_count
            FROM orders
            WHERE order_month = '{month}' AND customer_id = '{customerId}';
            """);

        ThrowIfSqlFailed(result);
        object? value = result.Rows is { Count: > 0 } ? result.Rows[0][0] : 0;
        return Convert.ToInt32(value, CultureInfo.InvariantCulture);
    }

    private static async Task<IReadOnlyList<OrderPageRow>> LoadOrdersForRouteAsync(
        CSharpDbShardedClient sharded,
        string month,
        string customerId,
        int limit,
        int offset)
    {
        ICSharpDbClient monthClient = sharded.ForRoute(RouteForMonth(month));
        SqlExecutionResult result = await monthClient.ExecuteSqlAsync($"""
            SELECT order_number, order_date, amount, status
            FROM orders
            WHERE order_month = '{month}' AND customer_id = '{customerId}'
            ORDER BY order_date DESC
            LIMIT {limit} OFFSET {offset};
            """);

        ThrowIfSqlFailed(result);

        var rows = new List<OrderPageRow>();
        foreach (object?[] row in result.Rows ?? [])
        {
            rows.Add(new OrderPageRow(
                RouteKey: month,
                OrderNumber: Convert.ToString(row[0], CultureInfo.InvariantCulture) ?? string.Empty,
                OrderDate: Convert.ToString(row[1], CultureInfo.InvariantCulture) ?? string.Empty,
                Amount: Convert.ToDouble(row[2], CultureInfo.InvariantCulture),
                Status: Convert.ToString(row[3], CultureInfo.InvariantCulture) ?? string.Empty));
        }

        return rows;
    }

    private static async Task PrintManualHistorySummaryAsync(CSharpDbShardedClient sharded, IReadOnlyList<string> months)
    {
        Console.WriteLine("Manual multi-month history summary");
        Console.WriteLine("----------------------------------");
        Console.WriteLine("V1 callers explicitly iterate route keys when a view spans months.");

        foreach (string month in months)
        {
            ICSharpDbClient monthClient = sharded.ForRoute(RouteForMonth(month));
            SqlExecutionResult result = await monthClient.ExecuteSqlAsync($"""
                SELECT COUNT(*) AS order_count, SUM(amount) AS total_amount
                FROM orders
                WHERE order_month = '{month}' AND customer_id = '{CustomerId}';
                """);

            object?[] row = result.Rows is { Count: > 0 } ? result.Rows[0] : [];
            double total = row[1] is null ? 0 : Convert.ToDouble(row[1], CultureInfo.InvariantCulture);
            Console.WriteLine($"{month,-7} orders={row[0],2} total={total.ToString("F2", CultureInfo.InvariantCulture),8}");
        }

        Console.WriteLine();
    }

    private static async Task PrintShardCountsAsync(
        CSharpDbShardedClient sharded,
        IReadOnlyList<CSharpDbShardDefinition> shards)
    {
        Console.WriteLine("Shard row counts");
        Console.WriteLine("----------------");
        foreach (CSharpDbShardDefinition shard in shards)
        {
            int count = await sharded.ForShardId(shard.ShardId).GetRowCountAsync("orders");
            Console.WriteLine($"{shard.ShardId,-7} {count,2} rows");
        }

        Console.WriteLine();
    }

    private static async Task PrintMissingRouteFailureAsync(CSharpDbShardedClient sharded)
    {
        Console.WriteLine("Missing route behavior");
        Console.WriteLine("----------------------");

        try
        {
            await sharded.ExecuteSqlAsync("SELECT COUNT(*) FROM orders;");
        }
        catch (CSharpDbClientException ex)
        {
            Console.WriteLine(ex.Message);
        }
    }

    private static void PrintOrderRows(SqlExecutionResult result)
    {
        ThrowIfSqlFailed(result);

        foreach (object?[] row in result.Rows ?? [])
        {
            double amount = Convert.ToDouble(row[2], CultureInfo.InvariantCulture);
            Console.WriteLine(
                $"{row[0],-15} {row[1],-20} {amount.ToString("C", CultureInfo.GetCultureInfo("en-US")),8} {row[3]}");
        }
    }

    private static void ThrowIfSqlFailed(SqlExecutionResult result)
    {
        if (!string.IsNullOrWhiteSpace(result.Error))
            throw new InvalidOperationException(result.Error);
    }

    private static CSharpDbRouteContext RouteForMonth(string orderMonth)
        => new()
        {
            Keyspace = Keyspace,
            Key = orderMonth,
        };

    private static void ThrowIfAnyShardFailed(IReadOnlyList<CSharpDbShardSqlExecutionResult> results)
    {
        CSharpDbShardSqlExecutionResult? failed = results.FirstOrDefault(result => !string.IsNullOrWhiteSpace(result.Error));
        if (failed is not null)
            throw new InvalidOperationException($"Shard '{failed.ShardId}' failed: {failed.Error}");
    }

    private static void PrepareDirectory(string dataDirectory)
    {
        if (Directory.Exists(dataDirectory))
            Directory.Delete(dataDirectory, recursive: true);

        Directory.CreateDirectory(dataDirectory);
    }

    private sealed record OrderSeed(
        long Id,
        string OrderMonth,
        string CustomerId,
        string OrderNumber,
        string OrderDate,
        decimal Amount,
        string Status);

    private sealed record OrderPageRow(
        string RouteKey,
        string OrderNumber,
        string OrderDate,
        double Amount,
        string Status);
}
