using System.Data.Common;
using System.Globalization;

namespace CSharpDB.Data;

public sealed class CSharpDbConnectionStringBuilder : DbConnectionStringBuilder
{
    private const string DataSourceKey = "Data Source";
    private const string LoadFromKey = "Load From";
    private const string PoolingKey = "Pooling";
    private const string MaxPoolSizeKey = "Max Pool Size";

    internal const bool DefaultPooling = false;
    internal const int DefaultMaxPoolSize = 32;

    public string DataSource
    {
        get => TryGetValue(DataSourceKey, out var v) ? Convert.ToString(v, CultureInfo.InvariantCulture) ?? "" : "";
        set => this[DataSourceKey] = value;
    }

    public bool Pooling
    {
        get => GetBoolean(PoolingKey, DefaultPooling);
        set => this[PoolingKey] = value;
    }

    public string LoadFrom
    {
        get => TryGetValue(LoadFromKey, out var v) ? Convert.ToString(v, CultureInfo.InvariantCulture) ?? "" : "";
        set => this[LoadFromKey] = value;
    }

    public int MaxPoolSize
    {
        get => GetInt32(MaxPoolSizeKey, DefaultMaxPoolSize, minValue: 1);
        set
        {
            if (value < 1)
                throw new ArgumentOutOfRangeException(nameof(value), "Max Pool Size must be greater than 0.");
            this[MaxPoolSizeKey] = value;
        }
    }

    public CSharpDbConnectionStringBuilder() { }

    public CSharpDbConnectionStringBuilder(string connectionString)
    {
        ConnectionString = connectionString;
    }

    private bool GetBoolean(string key, bool defaultValue)
    {
        if (!TryGetValue(key, out var value))
            return defaultValue;

        return value switch
        {
            bool typed => typed,
            string text when bool.TryParse(text, out bool parsed) => parsed,
            int numeric => numeric != 0,
            long numeric => numeric != 0L,
            _ => throw new FormatException($"{key} must be a boolean value.")
        };
    }

    private int GetInt32(string key, int defaultValue, int minValue)
    {
        if (!TryGetValue(key, out var value))
            return defaultValue;

        int parsed = value switch
        {
            int typed => typed,
            long typed when typed >= int.MinValue && typed <= int.MaxValue => (int)typed,
            string text when int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out int result) => result,
            _ => throw new FormatException($"{key} must be an integer value.")
        };

        if (parsed < minValue)
            throw new FormatException($"{key} must be greater than or equal to {minValue}.");

        return parsed;
    }
}
