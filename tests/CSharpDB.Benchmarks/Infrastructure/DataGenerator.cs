namespace CSharpDB.Benchmarks.Infrastructure;

/// <summary>
/// Deterministic data generation for repeatable benchmarks.
/// Uses seeded Random to ensure consistent data across runs.
/// </summary>
public static class DataGenerator
{
    private const string Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 ";

    /// <summary>
    /// Generate a random string of the given length using the provided RNG.
    /// </summary>
    public static string RandomString(Random rng, int length)
    {
        var chars = new char[length];
        for (int i = 0; i < length; i++)
            chars[i] = Chars[rng.Next(Chars.Length)];
        return new string(chars);
    }

    /// <summary>
    /// Generate a single INSERT statement for a table with text and integer columns.
    /// Schema: (id INTEGER, col1 TEXT, col2 TEXT, ..., int1 INTEGER, int2 INTEGER, ...)
    /// </summary>
    public static string InsertSql(string table, int id, int numTextCols, int numIntCols, Random rng,
        int textLength = 50)
    {
        var parts = new List<string> { id.ToString() };

        for (int i = 0; i < numTextCols; i++)
            parts.Add($"'{EscapeSql(RandomString(rng, textLength))}'");

        for (int i = 0; i < numIntCols; i++)
            parts.Add(rng.Next(0, 1_000_000).ToString());

        return $"INSERT INTO {table} VALUES ({string.Join(", ", parts)})";
    }

    /// <summary>
    /// Generate a CREATE TABLE statement with the given number of text and integer columns.
    /// </summary>
    public static string CreateTableSql(string table, int numTextCols, int numIntCols)
    {
        var cols = new List<string> { "id INTEGER PRIMARY KEY" };

        for (int i = 0; i < numTextCols; i++)
            cols.Add($"text_col{i} TEXT");

        for (int i = 0; i < numIntCols; i++)
            cols.Add($"int_col{i} INTEGER");

        return $"CREATE TABLE {table} ({string.Join(", ", cols)})";
    }

    /// <summary>
    /// Minimal SQL escaping for single-quoted strings.
    /// </summary>
    private static string EscapeSql(string value) => value.Replace("'", "''");
}
