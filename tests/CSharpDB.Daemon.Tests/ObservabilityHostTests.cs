using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using CSharpDB.Daemon.Grpc;
using CSharpDB.Observability;
using Grpc.Core;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using ObservabilityTransport = CSharpDB.Observability.CSharpDbTransport;

namespace CSharpDB.Daemon.Tests;

[Collection("ObservabilityDiagnostics")]
public sealed class ObservabilityHostTests
{
    [Fact]
    public void DisabledGrpcInterceptorReturnsContinuationTaskWithoutAllocatingScope()
    {
        Task<string> continuationTask = Task.FromResult("response");
        int invocationCount = 0;
        ObservabilityTransport observedTransport = ObservabilityTransport.Unknown;
        var interceptor = new CSharpDbOperationScopeGrpcInterceptor(
            new CSharpDbObservabilityOptions());
        UnaryServerMethod<string, string> continuation = (request, _) =>
        {
            invocationCount++;
            observedTransport = CSharpDbOperationScope.CurrentTransport;
            return continuationTask;
        };

        Task<string> warmup = interceptor.UnaryServerHandler(
            "request",
            null!,
            continuation);
        Assert.Same(continuationTask, warmup);

        long before = GC.GetAllocatedBytesForCurrentThread();
        Task<string>? returnedTask = null;
        for (int index = 0; index < 256; index++)
        {
            returnedTask = interceptor.UnaryServerHandler(
                "request",
                null!,
                continuation);
        }
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.Same(continuationTask, returnedTask);
        Assert.Equal(257, invocationCount);
        Assert.Equal(ObservabilityTransport.Embedded, observedTransport);
        Assert.Equal(0, allocated);
    }

    [Fact]
    public async Task GrpcInterceptorTagsTransportPreservesActivityAndRestoresAmbientState()
    {
        using var inboundActivity = new Activity("grpc-inbound");
        inboundActivity.SetIdFormat(ActivityIdFormat.W3C);
        inboundActivity.Start();
        Activity? expectedActivity = Activity.Current;
        var interceptor = new CSharpDbOperationScopeGrpcInterceptor(
            CreateEnabledLoggingOptions());

        string response = await interceptor.UnaryServerHandler<string, string>(
            "request",
            null!,
            (request, _) =>
            {
                Assert.Equal("request", request);
                Assert.Equal(ObservabilityTransport.Grpc, CSharpDbOperationScope.CurrentTransport);
                Assert.Null(CSharpDbOperationScope.Current);
                Assert.NotNull(CSharpDbOperationScope.CurrentSessionId);
                Assert.Same(expectedActivity, Activity.Current);
                return Task.FromResult("response");
            });

        Assert.Equal("response", response);
        Assert.Equal(ObservabilityTransport.Embedded, CSharpDbOperationScope.CurrentTransport);
        Assert.Null(CSharpDbOperationScope.Current);
        Assert.Null(CSharpDbOperationScope.CurrentSessionId);
        Assert.Same(expectedActivity, Activity.Current);
    }

    [Fact]
    public async Task GrpcInterceptorDoesNotSwallowDownstreamFailures()
    {
        var expected = new InvalidOperationException("downstream");
        var interceptor = new CSharpDbOperationScopeGrpcInterceptor(
            CreateEnabledLoggingOptions());

        InvalidOperationException actual = await Assert.ThrowsAsync<InvalidOperationException>(
            () => interceptor.UnaryServerHandler<string, string>(
                "request",
                null!,
                (_, _) => Task.FromException<string>(expected)));

        Assert.Same(expected, actual);
        Assert.Equal(ObservabilityTransport.Embedded, CSharpDbOperationScope.CurrentTransport);
        Assert.Null(CSharpDbOperationScope.CurrentSessionId);
    }

    [Fact]
    public async Task DaemonProgramPassesBoundOptionsToDirectDatabase()
    {
        string databasePath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb-daemon-observability-{Guid.NewGuid():N}.db");

        try
        {
            await using var factory = new TestDaemonFactory(databasePath);
            using HttpClient client = factory.CreateClient();

            CSharpDbObservabilityOptions observability =
                factory.Services.GetRequiredService<CSharpDbObservabilityOptions>();
            CSharpDbClientOptions clientOptions =
                factory.Services.GetRequiredService<CSharpDbClientOptions>();

            Assert.True(observability.Enabled);
            Assert.Equal("daemon-test", observability.DatabaseAlias);
            Assert.Same(observability, clientOptions.DirectDatabaseOptions?.ObservabilityOptions);
        }
        finally
        {
            TryDelete(databasePath);
            TryDelete(databasePath + ".wal");
        }
    }

    [Fact]
    public async Task GrpcQueryLog_UsesGrpcCorrelationAndDoesNotExposeSqlOrLiteral()
    {
        const string secret = "GrpcQueryAllAlphaCanary";
        string databasePath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb-daemon-query-observability-{Guid.NewGuid():N}.db");
        var loggerFactory = new CapturingLoggerFactory();
        var observer = new QueryCompletedObserver();
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            observer,
            static name => name == CSharpDbLogEvents.QueryCompleted.Name);

        try
        {
            await using var factory = new TestDaemonFactory(databasePath, loggerFactory);
            using var transportClient = new HttpClient(factory.Server.CreateHandler())
            {
                BaseAddress = new Uri("http://localhost"),
                DefaultRequestVersion = HttpVersion.Version20,
                DefaultVersionPolicy = HttpVersionPolicy.RequestVersionExact,
            };
            await using ICSharpDbClient client = CSharpDbClient.Create(
                new CSharpDbClientOptions
                {
                    Transport = CSharpDB.Client.CSharpDbTransport.Grpc,
                    Endpoint = "http://localhost",
                    HttpClient = transportClient,
                });
            loggerFactory.Entries.Clear();
            observer.Clear();

            SqlExecutionResult result = await client.ExecuteSqlAsync(
                $"SELECT '{secret}' AS value",
                TestContext.Current.CancellationToken);
            Assert.Null(result.Error);

            CSharpDbQueryCompletedEvent completed = Assert.Single(observer.Events);
            Assert.Equal(ObservabilityTransport.Grpc, completed.Context.Transport);
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

    private sealed class TestDaemonFactory(
        string databasePath,
        ILoggerFactory? loggerFactory = null) : WebApplicationFactory<Program>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseEnvironment("Development");
            builder.UseSetting("CSharpDB:Observability:Enabled", "true");
            builder.UseSetting(
                "CSharpDB:Observability:DatabaseAlias",
                "daemon-test");
            builder.UseSetting(
                "CSharpDB:Observability:Logging:Queries",
                "true");
            builder.UseSetting(
                "CSharpDB:Observability:Logging:SlowQueries",
                "false");
            builder.ConfigureServices(services =>
            {
                services.AddHostedService<TestDaemonClientShutdown>();
                if (loggerFactory is not null)
                {
                    services.RemoveAll<ILoggerFactory>();
                    services.AddSingleton(loggerFactory);
                }
            });
            builder.ConfigureAppConfiguration((_, configuration) =>
            {
                configuration.AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["ConnectionStrings:CSharpDB"] = $"Data Source={databasePath}",
                    ["CSharpDB:Observability:Enabled"] = "true",
                    ["CSharpDB:Observability:DatabaseAlias"] = "daemon-test",
                    ["CSharpDB:Observability:Logging:Queries"] = "true",
                    ["CSharpDB:Observability:Logging:SlowQueries"] = "false",
                });
            });
        }
    }

    private static CSharpDbObservabilityOptions CreateEnabledLoggingOptions()
        => new()
        {
            Enabled = true,
            Logging = new CSharpDbLoggingOptions { Enabled = true },
        };

    private sealed class TestDaemonClientShutdown(ICSharpDbClient client) : IHostedService
    {
        private int _stopped;

        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            if (Interlocked.Exchange(ref _stopped, 1) == 0)
                await client.DisposeAsync().ConfigureAwait(false);
        }
    }

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

    private sealed class CapturingLoggerFactory : ILoggerFactory
    {
        internal ConcurrentQueue<LogEntry> Entries { get; } = new();

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
            => entries.Enqueue(new LogEntry(
                eventId.Id,
                eventId.Name,
                formatter(state, exception)));
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
