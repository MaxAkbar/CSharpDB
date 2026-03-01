using CSharpDB.Data;

namespace CSharpDB.Data.Tests;

public class FactoryTests
{
    [Fact]
    public void Instance_IsSingleton()
    {
        Assert.Same(CSharpDbFactory.Instance, CSharpDbFactory.Instance);
    }

    [Fact]
    public void CreateConnection_ReturnsCSharpDbConnection()
    {
        var conn = CSharpDbFactory.Instance.CreateConnection();
        Assert.IsType<CSharpDbConnection>(conn);
    }

    [Fact]
    public void CreateCommand_ReturnsCSharpDbCommand()
    {
        var cmd = CSharpDbFactory.Instance.CreateCommand();
        Assert.IsType<CSharpDbCommand>(cmd);
    }

    [Fact]
    public void CreateParameter_ReturnsCSharpDbParameter()
    {
        var param = CSharpDbFactory.Instance.CreateParameter();
        Assert.IsType<CSharpDbParameter>(param);
    }

    [Fact]
    public void CreateConnectionStringBuilder_ReturnsCSharpDbConnectionStringBuilder()
    {
        var csb = CSharpDbFactory.Instance.CreateConnectionStringBuilder();
        Assert.IsType<CSharpDbConnectionStringBuilder>(csb);
    }

    [Fact]
    public void CanCreateDataAdapter_IsFalse()
    {
        Assert.False(CSharpDbFactory.Instance.CanCreateDataAdapter);
    }
}
