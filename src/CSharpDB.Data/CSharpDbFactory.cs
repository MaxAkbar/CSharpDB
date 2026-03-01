using System.Data.Common;

namespace CSharpDB.Data;

public sealed class CSharpDbFactory : DbProviderFactory
{
    public static readonly CSharpDbFactory Instance = new();

    private CSharpDbFactory() { }

    public override DbConnection CreateConnection() => new CSharpDbConnection();
    public override DbCommand CreateCommand() => new CSharpDbCommand();
    public override DbParameter CreateParameter() => new CSharpDbParameter();
    public override DbConnectionStringBuilder CreateConnectionStringBuilder()
        => new CSharpDbConnectionStringBuilder();

    public override bool CanCreateDataAdapter => false;
}
