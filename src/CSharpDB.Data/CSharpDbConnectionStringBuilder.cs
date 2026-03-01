using System.Data.Common;

namespace CSharpDB.Data;

public sealed class CSharpDbConnectionStringBuilder : DbConnectionStringBuilder
{
    private const string DataSourceKey = "Data Source";

    public string DataSource
    {
        get => TryGetValue(DataSourceKey, out var v) ? (string)v : "";
        set => this[DataSourceKey] = value;
    }

    public CSharpDbConnectionStringBuilder() { }

    public CSharpDbConnectionStringBuilder(string connectionString)
    {
        ConnectionString = connectionString;
    }
}
