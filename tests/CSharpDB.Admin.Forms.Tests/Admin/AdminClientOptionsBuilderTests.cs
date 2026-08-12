using System.Reflection;
using CSharpDB.Admin.Configuration;
using CSharpDB.Admin.Services;
using CSharpDB.Client;
using CSharpDB.Engine;
using CSharpDB.Primitives;
using CSharpDB.Observability;
using Microsoft.Extensions.Configuration;
using CSharpDbTransport = CSharpDB.Client.CSharpDbTransport;

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

    [Fact]
    public async Task InternalDirectBuild_AttachesAndPreservesValidatedObservabilityOptions()
    {
        IConfiguration configuration = CreateConfiguration(new Dictionary<string, string?>
        {
            ["ConnectionStrings:CSharpDB"] = "Data Source=admin.db",
        });
        AdminHostDatabaseOptions hostOptions = new();
        var observability = new CSharpDbObservabilityOptions
        {
            Enabled = true,
            DatabaseAlias = "admin",
            Logging = new CSharpDbLoggingOptions { SqlText = SqlTextCaptureMode.None },
            History = new CSharpDbHistoryOptions
            {
                ActiveQueryCapacity = 100,
                RecentQueryCapacity = 100,
                RecentOperationCapacity = 100,
                Retention = TimeSpan.FromMinutes(15),
            },
        };
        observability.Validate();

        CSharpDbClientOptions initial = AdminClientOptionsBuilder.Build(
            configuration,
            hostOptions,
            CSharpDbTransport.Direct,
            endpoint: null,
            functions: null,
            observability);
        CSharpDbClientOptions switched = AdminClientOptionsBuilder.BuildDirectDataSource(
            @"C:\data\switched.db",
            hostOptions,
            functions: null,
            observability);

        Assert.Same(observability, initial.DirectDatabaseOptions!.ObservabilityOptions);
        Assert.Same(observability, switched.DirectDatabaseOptions!.ObservabilityOptions);
        Assert.True(switched.DirectDatabaseOptions.ObservabilityOptions!.Enabled);
        Assert.Equal(SqlTextCaptureMode.None, switched.DirectDatabaseOptions.ObservabilityOptions.Logging.SqlText);

        ICSharpDbClient client = DispatchProxy.Create<ICSharpDbClient, NoOpClientProxy>();
        await using var holder = new DatabaseClientHolder(
            client,
            shardAdmin: null,
            baseClientOptions: initial,
            hostOptions,
            DbFunctionRegistry.Empty);
        CSharpDbClientOptions holderSwitch = holder.BuildSwitchClientOptions(@"C:\data\holder-switched.db");
        Assert.Same(observability, holderSwitch.DirectDatabaseOptions!.ObservabilityOptions);
    }

    [Fact]
    public void ExistingPublicBuilderAndHolderSignaturesRemainExact()
    {
        Type builder = typeof(AdminClientOptionsBuilder);
        Assert.NotNull(builder.GetMethod(
            nameof(AdminClientOptionsBuilder.Build),
            BindingFlags.Public | BindingFlags.Static,
            binder: null,
            [typeof(IConfiguration), typeof(AdminHostDatabaseOptions), typeof(CSharpDbTransport?), typeof(string), typeof(DbFunctionRegistry)],
            modifiers: null));
        Assert.NotNull(builder.GetMethod(
            nameof(AdminClientOptionsBuilder.BuildDirectDataSource),
            BindingFlags.Public | BindingFlags.Static,
            binder: null,
            [typeof(string), typeof(AdminHostDatabaseOptions), typeof(DbFunctionRegistry)],
            modifiers: null));
        Assert.NotNull(builder.GetMethod(
            nameof(AdminClientOptionsBuilder.BuildDirectDatabaseOptions),
            BindingFlags.Public | BindingFlags.Static,
            binder: null,
            [typeof(AdminHostDatabaseOptions), typeof(DbFunctionRegistry)],
            modifiers: null));
        Assert.NotNull(typeof(DatabaseClientHolder).GetConstructor(
            [typeof(ICSharpDbClient), typeof(ICSharpDbShardAdminClient), typeof(CSharpDbClientOptions), typeof(AdminHostDatabaseOptions), typeof(DbFunctionRegistry)]));
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

    public class NoOpClientProxy : DispatchProxy
    {
        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            ArgumentNullException.ThrowIfNull(targetMethod);
            if (targetMethod.Name == "get_DataSource")
                return "test";
            if (targetMethod.Name == nameof(IAsyncDisposable.DisposeAsync))
                return ValueTask.CompletedTask;
            throw new NotSupportedException(targetMethod.Name);
        }
    }
}
