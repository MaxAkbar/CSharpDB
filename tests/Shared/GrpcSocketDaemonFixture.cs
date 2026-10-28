using System.Collections.Concurrent;
using System.Net;
using CSharpDB.Client;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CSharpDB.Testing;

/// <summary>Runs the actual daemon with two real loopback socket listeners.</summary>
internal sealed class GrpcSocketDaemonFixture : IAsyncDisposable
{
    private readonly SocketDaemonFactory _factory;
    private readonly string _dbPath;
    private readonly ProtocolCapture _capture = new();

    public GrpcSocketDaemonFixture(string? apiKey = null, Action<IServiceCollection>? configureServices = null)
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_grpc_socket_{Guid.NewGuid():N}.db");
        _factory = new SocketDaemonFactory(_dbPath, _capture, apiKey, configureServices);
        _factory.UseKestrel(options =>
        {
            options.Listen(IPAddress.Loopback, 0, listener => ConfigureListener(listener, HttpProtocols.Http1));
            options.Listen(IPAddress.Loopback, 0, listener => ConfigureListener(listener, HttpProtocols.Http2));
        });
        try
        {
            string[] addresses = _factory.Services.GetRequiredService<IServer>().Features
                .Get<IServerAddressesFeature>()!.Addresses.ToArray();
            if (addresses.Length != 2)
                throw new InvalidOperationException("Expected exactly two loopback listeners.");
            CompatibilityEndpoint = new Uri(addresses[0]);
            NativeEndpoint = new Uri(addresses[1]);
        }
        catch
        {
            _factory.Dispose();
            DeleteDatabaseFiles();
            throw;
        }
    }

    public Uri CompatibilityEndpoint { get; }
    public Uri NativeEndpoint { get; }
    public IReadOnlyDictionary<RequestShape, long> Snapshot() => _capture.Counts.ToDictionary();
    public IReadOnlyList<Task> ConnectionClosures => _capture.ConnectionClosures.Values.ToArray();

    private void ConfigureListener(ListenOptions listener, HttpProtocols protocol)
    {
        listener.Protocols = protocol;
        listener.Use(next => async connection =>
        {
            var closed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _capture.ConnectionClosures.TryAdd(connection.ConnectionId, closed.Task);
            using var registration = connection.ConnectionClosed.Register(() => closed.TrySetResult());
            try { await next(connection); }
            finally { closed.TrySetResult(); }
        });
    }

    public ICSharpDbClient CreateClient(CSharpDbGrpcMode mode, string? apiKey = null)
        => CSharpDbClient.Create(new CSharpDbClientOptions
        {
            Transport = CSharpDbTransport.Grpc,
            GrpcMode = mode,
            Endpoint = EndpointFor(mode).AbsoluteUri,
            ApiKey = apiKey,
        });

    public Uri EndpointFor(CSharpDbGrpcMode mode)
        => mode == CSharpDbGrpcMode.Native ? NativeEndpoint : CompatibilityEndpoint;

    public async ValueTask DisposeAsync()
    {
        await _factory.DisposeAsync();
        DeleteDatabaseFiles();
    }

    private void DeleteDatabaseFiles()
    {
        foreach (string path in new[] { _dbPath, _dbPath + ".wal" })
        {
            try { File.Delete(path); }
            catch (IOException) { }
        }
    }

    internal readonly record struct RequestShape(string Protocol, string ContentType, string Method);

    private sealed class ProtocolCapture : IStartupFilter
    {
        public ConcurrentDictionary<RequestShape, long> Counts { get; } = new();
        public ConcurrentDictionary<string, Task> ConnectionClosures { get; } = new();

        public Action<IApplicationBuilder> Configure(Action<IApplicationBuilder> next)
            => app =>
            {
                app.Use(async (context, continuation) =>
                {
                    if (context.Request.Path.StartsWithSegments("/csharpdb.rpc.CSharpDbRpc"))
                    {
                        var shape = new RequestShape(context.Request.Protocol,
                            context.Request.ContentType ?? "", context.Request.Path.Value!);
                        Counts.AddOrUpdate(shape, 1, static (_, count) => count + 1);
                    }
                    await continuation();
                });
                next(app);
            };
    }

    private sealed class SocketDaemonFactory(string dbPath, ProtocolCapture capture, string? apiKey, Action<IServiceCollection>? configureServices)
        : WebApplicationFactory<global::Program>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            // Standalone benchmark executables do not receive the test SDK's
            // generated content-root metadata. Resolve the actual source host
            // instead of the factory's solution-root/assembly-name fallback.
            builder.UseContentRoot(FindDaemonContentRoot());
            builder.UseEnvironment("Development");
            var settings = new Dictionary<string, string?>
            {
                ["ConnectionStrings:CSharpDB"] = $"Data Source={dbPath}",
                ["CSharpDB:Observability:Enabled"] = "false",
                ["CSharpDB:Observability:Health:Enabled"] = "false",
                ["CSharpDB:Daemon:Security:Mode"] = apiKey is null ? "None" : "ApiKey",
                ["CSharpDB:Daemon:Security:ApiKey"] = apiKey,
            };
            foreach (var pair in settings)
                if (pair.Value is not null)
                    builder.UseSetting(pair.Key, pair.Value);
            builder.ConfigureAppConfiguration((_, config) => config.AddInMemoryCollection(settings));
            builder.ConfigureLogging(logging => logging.ClearProviders());
            builder.ConfigureServices(services =>
            {
                services.AddSingleton<IStartupFilter>(capture);
                services.AddHostedService<ClientShutdown>();
                configureServices?.Invoke(services);
            });
        }

        private static string FindDaemonContentRoot()
        {
            for (DirectoryInfo? directory = new(AppContext.BaseDirectory);
                 directory is not null; directory = directory.Parent)
            {
                string candidate = Path.Combine(directory.FullName, "src", "CSharpDB.Daemon");
                if (File.Exists(Path.Combine(candidate, "CSharpDB.Daemon.csproj")))
                    return candidate;
            }
            throw new DirectoryNotFoundException("Run the socket fixture from a CSharpDB source checkout.");
        }
    }

    private sealed class ClientShutdown(ICSharpDbClient client) : IHostedService
    {
        public Task StartAsync(CancellationToken ct) => Task.CompletedTask;
        public Task StopAsync(CancellationToken ct) => client.DisposeAsync().AsTask();
    }
}
