using BenchmarkDotNet.Attributes;
using CSharpDB.Sql;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Measures SQL parser throughput for various statement types.
/// Isolates parsing cost from execution cost.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 5, iterationCount: 20)]
public class ParserBenchmarks
{
    private const string SimpleSelect = "SELECT * FROM users WHERE id = 1";

    private const string ComplexSelect =
        "SELECT p.name, o.qty, p.price * o.qty AS total " +
        "FROM products p " +
        "INNER JOIN orders o ON p.id = o.product_id " +
        "LEFT JOIN categories c ON p.category_id = c.id " +
        "WHERE p.price > 10.0 AND o.qty >= 5 " +
        "GROUP BY p.name, o.qty " +
        "HAVING total > 100 " +
        "ORDER BY total DESC " +
        "LIMIT 50 OFFSET 10";

    private const string InsertSql =
        "INSERT INTO products VALUES (1, 'Widget', 9.99, 'Hardware')";

    private const string CreateTableSql =
        "CREATE TABLE IF NOT EXISTS products (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL, category TEXT)";

    private const string UpdateSql =
        "UPDATE products SET price = 19.99, category = 'Premium' WHERE id = 2 AND price < 50.0";

    private const string DeleteSql =
        "DELETE FROM products WHERE category = 'Discontinued' AND price < 5.0";

    private const string WithCte =
        "WITH high_value AS (SELECT * FROM products WHERE price > 100.0) " +
        "SELECT name, price FROM high_value ORDER BY price DESC LIMIT 10";

    [Benchmark(Description = "Parse simple SELECT")]
    public object ParseSimpleSelect() => Parser.Parse(SimpleSelect);

    [Benchmark(Description = "Parse complex SELECT (JOIN + GROUP BY + HAVING + ORDER BY)")]
    public object ParseComplexSelect() => Parser.Parse(ComplexSelect);

    [Benchmark(Description = "Parse INSERT")]
    public object ParseInsert() => Parser.Parse(InsertSql);

    [Benchmark(Description = "Parse CREATE TABLE")]
    public object ParseCreateTable() => Parser.Parse(CreateTableSql);

    [Benchmark(Description = "Parse UPDATE")]
    public object ParseUpdate() => Parser.Parse(UpdateSql);

    [Benchmark(Description = "Parse DELETE")]
    public object ParseDelete() => Parser.Parse(DeleteSql);

    [Benchmark(Description = "Parse CTE (WITH clause)")]
    public object ParseWithCte() => Parser.Parse(WithCte);
}
