using CSharpDB.Admin.Configuration;
using CSharpDB.Client;
using CSharpDB.Engine;
using CSharpDB.Primitives;
using Microsoft.Extensions.Configuration;

namespace CSharpDB.Admin.Forms.Tests.Admin;

public sealed class AdminClientOptionsBuilderTests
{
    [Fact]
    public void Build_LocalDirectDefaultsToHybridIncrementalDurable()
    {
        IConfiguration configuration = CreateConfiguration(new Dictionary<string, string?>
        {
            ["ConnectionStrings:CSharpDB"] = "Data Source=admin.db",
        });

        AdminHostDatabaseOptions hostOptions = AdminClientOptionsBuilder.BindHostDatabaseOptions(configuration);

        CSharpDbClientOptions options = AdminClientOptionsBuilder.Build(
            configuration,
            hostOptions,
            CSharpDbTransport.Direct,
            endpoint: null);

        Assert.Equal(CSharpDbTransport.Direct, options.Transport);
        Assert.Equal("Data Source=admin.db", options.ConnectionString);
        Assert.Null(options.Endpoint);
        Assert.NotNull(options.DirectDatabaseOptions);
        Assert.NotNull(options.HybridDatabaseOptions);
        Assert.Equal(
            ImplicitInsertExecutionMode.ConcurrentWriteTransactions,
            options.DirectDatabaseOptions.ImplicitInsertExecutionMode);
        Assert.Equal(HybridPersistenceMode.IncrementalDurable, options.HybridDatabaseOptions.PersistenceMode);
    }

    [Fact]
    public void Build_DirectOpenModeDisablesHybridOptions()
    {
        IConfiguration configuration = CreateConfiguration(new Dictionary<string, string?>
        {
            ["ConnectionStrings:CSharpDB"] = "Data Source=admin.db",
            ["CSharpDB:HostDatabase:OpenMode"] = "Direct",
        });

        AdminHostDatabaseOptions hostOptions = AdminClientOptionsBuilder.BindHostDatabaseOptions(configuration);

        CSharpDbClientOptions options = AdminClientOptionsBuilder.Build(
            configuration,
            hostOptions,
            CSharpDbTransport.Direct,
            endpoint: null);

        Assert.Equal(AdminHostOpenMode.Direct, hostOptions.OpenMode);
        Assert.NotNull(options.DirectDatabaseOptions);
        Assert.Null(options.HybridDatabaseOptions);
    }

    [Fact]
    public void Build_RemoteEndpointDoesNotAttachDirectOrHybridOptions()
    {
        IConfiguration configuration = CreateConfiguration(new Dictionary<string, string?>
        {
            ["ConnectionStrings:CSharpDB"] = "Data Source=admin.db",
        });
        AdminHostDatabaseOptions hostOptions = AdminClientOptionsBuilder.BindHostDatabaseOptions(configuration);

        CSharpDbClientOptions options = AdminClientOptionsBuilder.Build(
            configuration,
            hostOptions,
            CSharpDbTransport.Grpc,
            "http://127.0.0.1:5820");

        Assert.Equal(CSharpDbTransport.Grpc, options.Transport);
        Assert.Equal("http://127.0.0.1:5820", options.Endpoint);
        Assert.Null(options.ConnectionString);
        Assert.Null(options.DirectDatabaseOptions);
        Assert.Null(options.HybridDatabaseOptions);
    }

    [Fact]
    public void Build_DirectEndpointUsesHybridOptions()
    {
        IConfiguration configuration = CreateConfiguration(new Dictionary<string, string?>());
        AdminHostDatabaseOptions hostOptions = AdminClientOptionsBuilder.BindHostDatabaseOptions(configuration);

        CSharpDbClientOptions options = AdminClientOptionsBuilder.Build(
            configuration,
            hostOptions,
            transport: null,
            endpoint: "endpoint.db");

        Assert.Null(options.Transport);
        Assert.Equal("endpoint.db", options.Endpoint);
        Assert.Null(options.ConnectionString);
        Assert.NotNull(options.DirectDatabaseOptions);
        Assert.NotNull(options.HybridDatabaseOptions);
    }

    [Fact]
    public void Build_DirectConnectionStringAttachesFunctionRegistry()
    {
        IConfiguration configuration = CreateConfiguration(new Dictionary<string, string?>
        {
            ["ConnectionStrings:CSharpDB"] = "Data Source=admin.db",
        });
        AdminHostDatabaseOptions hostOptions = AdminClientOptionsBuilder.BindHostDatabaseOptions(configuration);
        DbFunctionRegistry functions = CreateFunctionRegistry();

        CSharpDbClientOptions options = AdminClientOptionsBuilder.Build(
            configuration,
            hostOptions,
            CSharpDbTransport.Direct,
            endpoint: null,
            functions);

        Assert.Same(functions, options.DirectDatabaseOptions!.Functions);
    }

    [Fact]
    public void BuildDirectDataSource_UsesHybridOptionsForDatabaseSwitches()
    {
        AdminHostDatabaseOptions hostOptions = new();

        CSharpDbClientOptions options = AdminClientOptionsBuilder.BuildDirectDataSource(
            @"C:\data\switched.db",
            hostOptions);

        Assert.Equal(CSharpDbTransport.Direct, options.Transport);
        Assert.Equal(@"C:\data\switched.db", options.DataSource);
        Assert.NotNull(options.DirectDatabaseOptions);
        Assert.NotNull(options.HybridDatabaseOptions);
        Assert.Equal(HybridPersistenceMode.IncrementalDurable, options.HybridDatabaseOptions.PersistenceMode);
    }

    [Fact]
    public void BuildDirectDataSource_AttachesFunctionRegistry()
    {
        AdminHostDatabaseOptions hostOptions = new();
        DbFunctionRegistry functions = CreateFunctionRegistry();

        CSharpDbClientOptions options = AdminClientOptionsBuilder.BuildDirectDataSource(
            @"C:\data\switched.db",
            hostOptions,
            functions);

        Assert.Same(functions, options.DirectDatabaseOptions!.Functions);
    }

    [Fact]
    public void Build_RemoteEndpointIgnoresFunctionRegistry()
    {
        IConfiguration configuration = CreateConfiguration(new Dictionary<string, string?>());
        AdminHostDatabaseOptions hostOptions = AdminClientOptionsBuilder.BindHostDatabaseOptions(configuration);

        CSharpDbClientOptions options = AdminClientOptionsBuilder.Build(
            configuration,
            hostOptions,
            CSharpDbTransport.Grpc,
            "http://127.0.0.1:5820",
            CreateFunctionRegistry());

        Assert.Null(options.DirectDatabaseOptions);
    }

    private static IConfiguration CreateConfiguration(Dictionary<string, string?> values)
        => new ConfigurationBuilder()
            .AddInMemoryCollection(values)
            .Build();

    private static DbFunctionRegistry CreateFunctionRegistry()
        => DbFunctionRegistry.Create(functions =>
            functions.AddScalar(
                "AddOne",
                1,
                new DbScalarFunctionOptions(DbType.Integer),
                static (_, args) => DbValue.FromInteger(args[0].AsInteger + 1)));
}
