using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Http.Json;
using CSharpDB.Api.Dtos;
using CSharpDB.Api.Middleware;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using CSharpDB.Observability;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using ObservabilityTransport = CSharpDB.Observability.CSharpDbTransport;

namespace CSharpDB.Api.Tests;

[Collection("ObservabilityDiagnostics")]
public sealed class ObservabilityHostTests
{
    [Fact]
    public async Task RestExtensionsWithoutObservabilityRegistration_ServeRequests()
    {
        WebApplicationBuilder builder = WebApplication.CreateBuilder(
            new WebApplicationOptions
            {
                EnvironmentName = "Testing",
            });
        builder.WebHost.UseTestServer();
        builder.Services.AddCSharpDbRestApi();
        builder.Services.AddSingleton<ICSharpDbClient>(_ =>
            CSharpDbClient.Create(new CSharpDbClientOptions
            {
                Transport = CSharpDB.Client.CSharpDbTransport.Direct,
                ConnectionString = "Data Source=:memory:",
            }));

        await using WebApplication app = builder.Build();
        app.MapCSharpDbRestApi();
        await app.StartAsync(TestContext.Current.CancellationToken);

        using HttpClient client = app.GetTestClient();
        using HttpResponseMessage response = await client.GetAsync(
            "/api/not-a-real-route",
            TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public void DisabledHttpScopeReturnsContinuationTaskWithoutAllocatingScope()
    {
        Task continuationTask = Task.CompletedTask;
        int invocationCount = 0;
        ObservabilityTransport observedTransport = ObservabilityTransport.Unknown;
        var middleware = new CSharpDbOperationScopeMiddleware(
            _ =>
            {
                invocationCount++;
                observedTransport = CSharpDbOperationScope.CurrentTransport;
                return continuationTask;
            },
            new CSharpDbObservabilityOptions());
        var context = new DefaultHttpContext();

        Task warmup = middleware.InvokeAsync(context);
        Assert.Same(continuationTask, warmup);

        long before = GC.GetAllocatedBytesForCurrentThread();
        Task? returnedTask = null;
        for (int index = 0; index < 256; index++)
            returnedTask = middleware.InvokeAsync(context);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.Same(continuationTask, returnedTask);
        Assert.Equal(257, invocationCount);
        Assert.Equal(ObservabilityTransport.Embedded, observedTransport);
        Assert.Equal(0, allocated);
    }

    [Fact]
    public void LegacyNullOptionsConstructor_RemainsSourceCompatible()
    {
        Task continuationTask = Task.CompletedTask;
        RequestDelegate next = _ => continuationTask;

        var middleware = new CSharpDbOperationScopeMiddleware(next, null);

        Assert.Same(
            continuationTask,
            middleware.InvokeAsync(new DefaultHttpContext()));
    }

    [Fact]
    public void InvalidConfigurationFailsBeforeDatabaseWarmup()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>
        {
            ["CSharpDB:Observability:DatabaseAlias"] = @"C:\private\database.db",
        });

        var services = new ServiceCollection();
        services.AddSingleton<ILoggerFactory>(new CapturingLoggerFactory());
        services.AddCSharpDbObservability(configuration);
        using ServiceProvider provider = services.BuildServiceProvider();

        CSharpDbObservabilityOptionsValidationException error = Assert.Throws<
            CSharpDbObservabilityOptionsValidationException>(
            () => provider.StartCSharpDbObservability(ObservabilityTransport.Direct));

        Assert.Contains(error.Errors, message => message.Contains("DatabaseAlias", StringComparison.Ordinal));
    }

    [Fact]
    public async Task HttpScopeTagsTransportPreservesActivityAndRestoresAmbientState()
    {
        using var inboundActivity = new Activity("http-inbound");
        inboundActivity.SetIdFormat(ActivityIdFormat.W3C);
        inboundActivity.Start();
        Activity? expectedActivity = Activity.Current;

        var middleware = new CSharpDbOperationScopeMiddleware(
            _ =>
            {
                Assert.Equal(ObservabilityTransport.Http, CSharpDbOperationScope.CurrentTransport);
                Assert.Null(CSharpDbOperationScope.Current);
                Assert.NotNull(CSharpDbOperationScope.CurrentSessionId);
                Assert.Same(expectedActivity, Activity.Current);
                return Task.CompletedTask;
            },
            CreateEnabledLoggingOptions());

        await middleware.InvokeAsync(new DefaultHttpContext());

        Assert.Equal(ObservabilityTransport.Embedded, CSharpDbOperationScope.CurrentTransport);
        Assert.Null(CSharpDbOperationScope.Current);
        Assert.Null(CSharpDbOperationScope.CurrentSessionId);
        Assert.Same(expectedActivity, Activity.Current);
    }

    [Fact]
    public async Task HttpScopeDoesNotSwallowDownstreamFailures()
    {
        var expected = new InvalidOperationException("downstream");
        var middleware = new CSharpDbOperationScopeMiddleware(
            _ => Task.FromException(expected),
            CreateEnabledLoggingOptions());

        InvalidOperationException actual = await Assert.ThrowsAsync<InvalidOperationException>(
            () => middleware.InvokeAsync(new DefaultHttpContext()));

        Assert.Same(expected, actual);
        Assert.Equal(ObservabilityTransport.Embedded, CSharpDbOperationScope.CurrentTransport);
        Assert.Null(CSharpDbOperationScope.CurrentSessionId);
    }

    [Fact]
    public void DefaultCaptureStartupLogsDoNotExposeConfigurationCanary()
    {
        const string canary = "AllAlphaBearerCapabilitySecret";
        var loggerFactory = new CapturingLoggerFactory();
        using ServiceProvider provider = BuildServices(
            loggerFactory,
            new Dictionary<string, string?>
            {
                ["CSharpDB:Observability:Enabled"] = "true",
                ["ConnectionStrings:CSharpDB"] = $"Data Source={canary}",
            });

        provider.StartCSharpDbObservability(ObservabilityTransport.Direct);

        LogEntry hostStart = Assert.Single(
            loggerFactory.Entries,
            entry => entry.EventId == CSharpDbLogEventIds.HostStarting);
        Assert.DoesNotContain(canary, hostStart.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(
            loggerFactory.Entries,
            entry => entry.Message.Contains(canary, StringComparison.Ordinal));
        Assert.DoesNotContain(
            loggerFactory.Entries,
            entry => entry.EventId == CSharpDbLogEventIds.RawSqlCaptureEnabled);
    }

    [Fact]
    public void RawCapturePublishesExactlyOneTypedWarningAfterBridgeSubscription()
    {
        var loggerFactory = new CapturingLoggerFactory();
        using ServiceProvider provider = BuildServices(
            loggerFactory,
            new Dictionary<string, string?>
            {
                ["CSharpDB:Observability:Enabled"] = "true",
                ["CSharpDB:Observability:DatabaseAlias"] = "raw-test",
                ["CSharpDB:Observability:Logging:SqlText"] = "Raw",
            });

        provider.StartCSharpDbObservability(ObservabilityTransport.Direct);

        LogEntry warning = Assert.Single(
            loggerFactory.Entries,
            entry => entry.EventId == CSharpDbLogEventIds.RawSqlCaptureEnabled);
        Assert.Equal(CSharpDbLogEvents.RawSqlCaptureEnabled.Name, warning.EventName);
        Assert.Contains("raw-test", warning.Message, StringComparison.Ordinal);
        Assert.Contains("Raw", warning.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ThrowingLoggerCannotPreventHostObservabilityStartup()
    {
        using ServiceProvider provider = BuildServices(
            new ThrowingLoggerFactory(),
            new Dictionary<string, string?>
            {
                ["CSharpDB:Observability:Enabled"] = "true",
                ["CSharpDB:Observability:Logging:SqlText"] = "Raw",
            });

        Exception? error = Record.Exception(
            () => provider.StartCSharpDbObservability(ObservabilityTransport.Direct));

        Assert.Null(error);
    }

    [Fact]
    public async Task ApiProgramPassesBoundOptionsToDirectDatabase()
    {
        string databasePath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb-api-observability-{Guid.NewGuid():N}.db");

        try
        {
            await using var factory = new TestApiFactory(databasePath);
            using HttpClient client = factory.CreateClient();

            CSharpDbObservabilityOptions observability =
                factory.Services.GetRequiredService<CSharpDbObservabilityOptions>();
            CSharpDbClientOptions clientOptions =
                factory.Services.GetRequiredService<CSharpDbClientOptions>();

            Assert.True(observability.Enabled);
            Assert.Equal("api-test", observability.DatabaseAlias);
            Assert.Same(observability, clientOptions.DirectDatabaseOptions?.ObservabilityOptions);
        }
        finally
        {
            TryDelete(databasePath);
            TryDelete(databasePath + ".wal");
        }
    }

    [Fact]
    public async Task RestQueryLog_UsesHttpCorrelationAndDoesNotExposeSqlOrLiteral()
    {
        const string secret = "RestQueryAllAlphaCanary";
        string databasePath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb-api-query-observability-{Guid.NewGuid():N}.db");
        var loggerFactory = new CapturingLoggerFactory();
        var observer = new QueryCompletedObserver();
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            observer,
            static name => name == CSharpDbLogEvents.QueryCompleted.Name);

        try
        {
            await using var factory = new TestApiFactory(databasePath, loggerFactory);
            using HttpClient client = factory.CreateClient();
            loggerFactory.Entries.Clear();
            observer.Clear();

            using HttpResponseMessage response = await client.PostAsJsonAsync(
                "/api/sql/execute",
                new ExecuteSqlRequest($"SELECT '{secret}' AS value"),
                TestContext.Current.CancellationToken);
            response.EnsureSuccessStatusCode();

            CSharpDbQueryCompletedEvent completed = Assert.Single(observer.Events);
            Assert.Equal(ObservabilityTransport.Http, completed.Context.Transport);
            Assert.NotNull(completed.Context.TraceId);
            Assert.NotNull(completed.Context.SessionId);
            Assert.Equal(SqlTextCaptureMode.None, completed.SqlTextCaptureMode);
            Assert.Null(completed.CapturedSqlText);

            LogEntry log = Assert.Single(
                loggerFactory.Entries,
                entry => entry.EventId == CSharpDbLogEventIds.QueryCompleted);
            Assert.Equal(CSharpDbLogEvents.QueryCompleted.Name, log.EventName);
            Assert.DoesNotContain(secret, log.Message, StringComparison.Ordinal);
            Assert.DoesNotContain(databasePath, log.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            TryDelete(databasePath);
            TryDelete(databasePath + ".wal");
        }
    }

    private static ServiceProvider BuildServices(
        ILoggerFactory loggerFactory,
        IReadOnlyDictionary<string, string?> values)
    {
        IConfiguration configuration = BuildConfiguration(values);
        var services = new ServiceCollection();
        services.AddSingleton(loggerFactory);
        services.AddCSharpDbObservability(configuration);
        return services.BuildServiceProvider();
    }

    private static CSharpDbObservabilityOptions CreateEnabledLoggingOptions()
        => new()
        {
            Enabled = true,
            Logging = new CSharpDbLoggingOptions { Enabled = true },
        };

    private static IConfiguration BuildConfiguration(
        IReadOnlyDictionary<string, string?> values)
        => new ConfigurationBuilder()
            .AddInMemoryCollection(values)
            .Build();

    private static void TryDelete(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);
        }
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
    }

    private sealed class TestApiFactory(
        string databasePath,
        ILoggerFactory? loggerFactory = null) : WebApplicationFactory<Program>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseEnvironment("Development");
            if (loggerFactory is not null)
            {
                builder.ConfigureServices(services =>
                {
                    services.RemoveAll<ILoggerFactory>();
                    services.AddSingleton(loggerFactory);
                });
            }
            builder.ConfigureAppConfiguration((_, configuration) =>
            {
                configuration.AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["ConnectionStrings:CSharpDB"] = $"Data Source={databasePath}",
                    ["CSharpDB:Observability:Enabled"] = "true",
                    ["CSharpDB:Observability:DatabaseAlias"] = "api-test",
                    ["CSharpDB:Observability:Logging:Queries"] = "true",
                    ["CSharpDB:Observability:Logging:SlowQueries"] = "false",
                });
            });
        }
    }

    private sealed class CapturingLoggerFactory : ILoggerFactory
    {
        public ConcurrentQueue<LogEntry> Entries { get; } = new();

        public void AddProvider(ILoggerProvider provider)
        {
        }

        public ILogger CreateLogger(string categoryName) => new CapturingLogger(Entries);

        public void Dispose()
        {
        }
    }

    private sealed class CapturingLogger(ConcurrentQueue<LogEntry> entries) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            entries.Enqueue(new LogEntry(eventId.Id, eventId.Name, formatter(state, exception)));
        }
    }

    private sealed class ThrowingLoggerFactory : ILoggerFactory
    {
        public void AddProvider(ILoggerProvider provider)
        {
        }

        public ILogger CreateLogger(string categoryName) => new ThrowingLogger();

        public void Dispose()
        {
        }
    }

    private sealed class ThrowingLogger : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull
            => throw new InvalidOperationException("logger scope failure");

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
            => throw new InvalidOperationException("logger write failure");
    }

    private sealed record LogEntry(int EventId, string? EventName, string Message);

    private sealed class QueryCompletedObserver : IObserver<KeyValuePair<string, object?>>
    {
        private readonly ConcurrentQueue<CSharpDbQueryCompletedEvent> _events = new();

        internal IReadOnlyList<CSharpDbQueryCompletedEvent> Events => _events.ToArray();

        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }

        public void OnNext(KeyValuePair<string, object?> value)
        {
            if (value.Value is CSharpDbQueryCompletedEvent completed)
                _events.Enqueue(completed);
        }

        internal void Clear() => _events.Clear();
    }
}
