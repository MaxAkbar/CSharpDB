using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Measures correlated subquery execution in the currently supported row-by-row paths.
/// The benchmark keeps result materialization to a single COUNT(*) row so the numbers
/// primarily reflect subquery evaluation and inner-query reuse behavior.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 6)]
public class SubqueryBenchmarks
{
    [Params(1_000, 10_000)]
    public int RowCount { get; set; }

    private BenchmarkDatabase _bench = null!;
    private long _sink;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _bench = BenchmarkDatabase.CreateWithSchemaAsync(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, tenant_id INTEGER, name TEXT)")
            .GetAwaiter().GetResult();

        var db = _bench.Db;
        db.ExecuteAsync("CREATE TABLE tenant_selected (tenant_id INTEGER, selected_user_id INTEGER)")
            .AsTask().GetAwaiter().GetResult();
        db.ExecuteAsync("CREATE TABLE tenant_featured (tenant_id INTEGER, user_id INTEGER)")
            .AsTask().GetAwaiter().GetResult();
        db.ExecuteAsync("CREATE TABLE tenant_flags (tenant_id INTEGER, enabled INTEGER)")
            .AsTask().GetAwaiter().GetResult();

        int tenantCount = GetTenantCount(RowCount);
        SeedUsersAsync(tenantCount).GetAwaiter().GetResult();
        SeedTenantSelectedAsync(tenantCount).GetAwaiter().GetResult();
        SeedTenantFeaturedAsync(tenantCount).GetAwaiter().GetResult();
        SeedTenantFlagsAsync(tenantCount).GetAwaiter().GetResult();

        db.ExecuteAsync("CREATE INDEX idx_tenant_selected_tenant ON tenant_selected(tenant_id)")
            .AsTask().GetAwaiter().GetResult();
        db.ExecuteAsync("CREATE INDEX idx_tenant_featured_tenant ON tenant_featured(tenant_id)")
            .AsTask().GetAwaiter().GetResult();
        db.ExecuteAsync("CREATE INDEX idx_tenant_flags_tenant_enabled ON tenant_flags(tenant_id, enabled)")
            .AsTask().GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _bench.Dispose();
    }

    [Benchmark(Description = "Correlated scalar subquery filter COUNT(*)")]
    public async Task CorrelatedScalarFilterCount()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT COUNT(*) FROM users u " +
            "WHERE id = (SELECT selected_user_id FROM tenant_selected s WHERE s.tenant_id = u.tenant_id)");
        if (await result.MoveNextAsync())
            _sink = result.Current[0].AsInteger;
    }

    [Benchmark(Description = "Correlated IN subquery filter COUNT(*)")]
    public async Task CorrelatedInFilterCount()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT COUNT(*) FROM users u " +
            "WHERE id IN (SELECT user_id FROM tenant_featured f WHERE f.tenant_id = u.tenant_id)");
        if (await result.MoveNextAsync())
            _sink = result.Current[0].AsInteger;
    }

    [Benchmark(Description = "Correlated EXISTS subquery filter COUNT(*)")]
    public async Task CorrelatedExistsFilterCount()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT COUNT(*) FROM users u " +
            "WHERE EXISTS (SELECT tenant_id FROM tenant_flags fl WHERE fl.tenant_id = u.tenant_id AND fl.enabled = 1)");
        if (await result.MoveNextAsync())
            _sink = result.Current[0].AsInteger;
    }

    private async Task SeedUsersAsync(int tenantCount)
    {
        await _bench.SeedAsync("users", RowCount, id =>
        {
            int tenantId = id % tenantCount;
            return $"INSERT INTO users VALUES ({id}, {tenantId}, 'user_{id}')";
        });
    }

    private async Task SeedTenantSelectedAsync(int tenantCount)
    {
        await _bench.SeedAsync("tenant_selected", tenantCount, tenantId =>
        {
            int selectedUserId = tenantId;
            return $"INSERT INTO tenant_selected VALUES ({tenantId}, {selectedUserId})";
        });
    }

    private async Task SeedTenantFeaturedAsync(int tenantCount)
    {
        await _bench.SeedAsync("tenant_featured", tenantCount, tenantId =>
        {
            int firstFeatured = tenantId;
            return $"INSERT INTO tenant_featured VALUES ({tenantId}, {firstFeatured})";
        });

        await _bench.SeedAsync("tenant_featured", tenantCount, tenantId =>
        {
            int secondFeatured = tenantId + tenantCount;
            if (secondFeatured >= RowCount)
                secondFeatured = tenantId;

            return $"INSERT INTO tenant_featured VALUES ({tenantId}, {secondFeatured})";
        });
    }

    private async Task SeedTenantFlagsAsync(int tenantCount)
    {
        await _bench.SeedAsync("tenant_flags", tenantCount, tenantId =>
        {
            int enabled = (tenantId & 1) == 0 ? 1 : 0;
            return $"INSERT INTO tenant_flags VALUES ({tenantId}, {enabled})";
        });
    }

    private static int GetTenantCount(int rowCount)
        => Math.Max(1, rowCount / 8);
}
