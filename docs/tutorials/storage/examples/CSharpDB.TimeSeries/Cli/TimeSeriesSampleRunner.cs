using CSharpDB.TimeSeries;

internal sealed class TimeSeriesSampleRunner
{
    private readonly AnsiConsoleWriter _console;
    private readonly TimeSeriesConsolePresenter _presenter;

    public TimeSeriesSampleRunner(AnsiConsoleWriter console, TimeSeriesConsolePresenter presenter)
    {
        _console = console;
        _presenter = presenter;
    }

    public async Task RunAsync(ITimeSeriesApi api, CancellationToken ct)
    {
        await api.ResetAsync(ct);

        await _console.WriteSectionAsync("Sample scenario");
        await _console.WriteInfoAsync("Resetting database and generating demo time-series data.");
        await _console.WriteBlankLineAsync();

        // ── IoT temperature sensor data ──────────────────────
        await _console.WriteSectionAsync("1. Recording IoT temperature readings");
        await _console.WriteInfoAsync("Simulating a temperature sensor (sensor-01) over the last hour...");

        var baseTime = DateTime.UtcNow.AddHours(-1);
        var random = new Random(42); // Fixed seed for reproducibility

        for (var i = 0; i < 60; i++)
        {
            var timestamp = baseTime.AddMinutes(i);
            // Simulate temperature: base 22C with some noise, rising trend
            var temperature = 22.0 + (i * 0.05) + (random.NextDouble() * 3 - 1.5);
            await api.RecordAsync(
                "temperature_c", Math.Round(temperature, 2), "°C",
                new Dictionary<string, string> { ["sensor"] = "sensor-01", ["location"] = "warehouse-A" },
                timestamp, ct);
        }

        await _console.WriteSuccessAsync("60 temperature readings recorded (1 per minute).");
        await _console.WriteBlankLineAsync();

        // ── CPU metrics ──────────────────────────────────────
        await _console.WriteSectionAsync("2. Recording CPU utilization metrics");
        await _console.WriteInfoAsync("Simulating CPU metrics for web-server-01...");

        for (var i = 0; i < 30; i++)
        {
            var timestamp = baseTime.AddMinutes(i * 2);
            var cpu = 35.0 + random.NextDouble() * 40 + (i > 20 ? 15 : 0); // Spike at end
            await api.RecordAsync(
                "cpu_percent", Math.Round(cpu, 1), "%",
                new Dictionary<string, string> { ["host"] = "web-server-01" },
                timestamp, ct);
        }

        await _console.WriteSuccessAsync("30 CPU utilization readings recorded (1 per 2 minutes).");
        await _console.WriteBlankLineAsync();

        // ── Stock price data ─────────────────────────────────
        await _console.WriteSectionAsync("3. Recording stock price history");
        await _console.WriteInfoAsync("Simulating ACME Corp stock prices...");

        var stockPrice = 142.50;
        for (var i = 0; i < 20; i++)
        {
            var timestamp = baseTime.AddMinutes(i * 3);
            stockPrice += (random.NextDouble() - 0.48) * 2; // Slight upward bias
            await api.RecordAsync(
                "stock_price", Math.Round(stockPrice, 2), "USD",
                new Dictionary<string, string> { ["symbol"] = "ACME", ["exchange"] = "NYSE" },
                timestamp, ct);
        }

        await _console.WriteSuccessAsync("20 stock price readings recorded.");
        await _console.WriteBlankLineAsync();

        // ── Show total count ─────────────────────────────────
        await _console.WriteSectionAsync("4. Database statistics");
        var count = await api.CountAsync(ct);
        await _console.WriteInfoAsync($"Total data points stored: {count:N0}");
        await _console.WriteBlankLineAsync();

        // ── Query temperature range ──────────────────────────
        await _console.WriteSectionAsync("5. Querying temperature data (last 30 minutes)");
        var tempResult = await api.QueryAsync(
            baseTime.AddMinutes(30), baseTime.AddMinutes(60),
            "temperature_c", 100, ct);
        await _presenter.ShowQueryResultAsync(tempResult, "temperature_c");
        await _console.WriteBlankLineAsync();

        // ── ASCII chart of temperature ───────────────────────
        await _console.WriteSectionAsync("6. Temperature trend chart (full hour)");
        var fullTempResult = await api.QueryAsync(
            baseTime, baseTime.AddHours(1),
            "temperature_c", 10_000, ct);
        await _presenter.ShowAsciiChartAsync(fullTempResult.Points);
        await _console.WriteBlankLineAsync();

        // ── Query CPU with aggregation ───────────────────────
        await _console.WriteSectionAsync("7. CPU utilization summary");
        var cpuResult = await api.QueryAsync(
            baseTime, baseTime.AddHours(1),
            "cpu_percent", 10_000, ct);
        await _presenter.ShowQueryResultAsync(cpuResult, "cpu_percent");
        await _console.WriteBlankLineAsync();

        // ── Show latest point ────────────────────────────────
        await _console.WriteSectionAsync("8. Latest data point");
        var latest = await api.GetLatestAsync(ct);
        if (latest is not null)
        {
            await _presenter.ShowPointAsync(latest);
        }
        await _console.WriteBlankLineAsync();

        // ── Show a single stock price lookup ─────────────────
        await _console.WriteSectionAsync("9. Stock price range query");
        var stockResult = await api.QueryAsync(
            baseTime, baseTime.AddHours(1),
            "stock_price", 10_000, ct);
        await _presenter.ShowQueryResultAsync(stockResult, "stock_price");
        await _console.WriteBlankLineAsync();

        // ── ASCII chart of stock price ───────────────────────
        await _console.WriteSectionAsync("10. Stock price trend chart");
        await _presenter.ShowAsciiChartAsync(stockResult.Points);
        await _console.WriteBlankLineAsync();

        await _console.WriteSuccessAsync("Sample completed.");
    }
}
