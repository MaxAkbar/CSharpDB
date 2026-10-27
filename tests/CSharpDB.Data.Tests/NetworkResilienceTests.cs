using System.Collections.Concurrent;
using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using CSharpDB.Client;
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

namespace CSharpDB.Data.Tests;

[Collection("ConnectionPoolState")]
public sealed class NetworkResilienceTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(CSharpDbGrpcMode.Auto, false)]
    [InlineData(CSharpDbGrpcMode.Auto, true)]
    [InlineData(CSharpDbGrpcMode.Native, false)]
    [InlineData(CSharpDbGrpcMode.Native, true)]
    public async Task Tls_PooledSdkAndAdo_UseHttp2AndPreserveCallerHttpClient(CSharpDbGrpcMode mode, bool ado)
    {
        await using var host = new RecoveryHost(mode, tls: true);
        using var http = host.CreatePinnedHttpClient();
        var options = new CSharpDbClientOptions
        {
            Transport = CSharpDbTransport.Grpc, GrpcMode = mode,
            Endpoint = host.Endpoint.AbsoluteUri, HttpClient = http,
        };
        string cs = ConnectionString(host, mode, 1);
        try
        {
            for (int i = 0; i < 3; i++)
            {
                if (ado)
                {
                    await using var connection = new CSharpDbConnection(cs, http);
                    await connection.OpenAsync(Ct);
                    using var command = connection.CreateCommand(); command.CommandText = "SELECT 42";
                    Assert.Equal(42L, Convert.ToInt64(await command.ExecuteScalarAsync(Ct)));
                }
                else
                {
                    await using var client = await CSharpDbClient.RentAsync(options, 1, Ct);
                    Assert.Equal(42L, (await client.ExecuteSqlAsync("SELECT 42", Ct)).Rows![0][0]);
                }
            }
            Assert.Equal(1, host.Count("GetInfo"));
            Assert.Equal(3, host.Count("ExecuteSql"));
            Assert.All(host.Requests.Keys, shape =>
            {
                Assert.Equal("HTTP/2", shape.Protocol);
                Assert.StartsWith("application/grpc", shape.ContentType, StringComparison.Ordinal);
                Assert.DoesNotContain("grpc-web", shape.ContentType, StringComparison.Ordinal);
            });
            await CSharpDbClient.ClearPoolAsync(host.Endpoint.AbsoluteUri);
            using var request = new HttpRequestMessage(HttpMethod.Get, host.Endpoint)
            { Version = HttpVersion.Version20, VersionPolicy = HttpVersionPolicy.RequestVersionExact };
            using var response = await http.SendAsync(request, Ct);
            Assert.Equal(HttpVersion.Version20, response.Version);
        }
        finally { await CSharpDbClient.ClearPoolAsync(host.Endpoint.AbsoluteUri); }
    }

    [Fact]
    public async Task Tls_UntrustedCertificate_IsRejected()
    {
        await using var host = new RecoveryHost(CSharpDbGrpcMode.Native, tls: true);
        var options = new CSharpDbClientOptions
        {
            Transport = CSharpDbTransport.Grpc, GrpcMode = CSharpDbGrpcMode.Native,
            Endpoint = host.Endpoint.AbsoluteUri,
        };
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        timeout.CancelAfter(TimeSpan.FromSeconds(10));
        try
        {
            await Assert.ThrowsAsync<CSharpDbClientException>(() => CSharpDbClient.RentAsync(options, 1, timeout.Token).AsTask());
            Assert.Equal(0, host.Count("GetInfo"));
        }
        finally { await CSharpDbClient.ClearPoolAsync(host.Endpoint.AbsoluteUri); }
    }

    [Theory]
    [InlineData(CSharpDbGrpcMode.Auto, false)]
    [InlineData(CSharpDbGrpcMode.Auto, true)]
    [InlineData(CSharpDbGrpcMode.Native, false)]
    [InlineData(CSharpDbGrpcMode.Native, true)]
    public async Task ServerRestart_DiscardsFailedRental_AndDoesNotReplayFailedWrite(CSharpDbGrpcMode mode, bool ado)
    {
        await using var host = new RecoveryHost(mode);
        string cs = ConnectionString(host, mode, 1);
        var options = new CSharpDbClientOptions
        { Transport = CSharpDbTransport.Grpc, GrpcMode = mode, Endpoint = host.Endpoint.AbsoluteUri };
        const string setup = "CREATE TABLE recovery (id INTEGER PRIMARY KEY); INSERT INTO recovery VALUES (1)";
        try
        {
            if (ado)
            {
                await using var connection = new CSharpDbConnection(cs);
                await connection.OpenAsync(Ct);
                using var command = connection.CreateCommand(); command.CommandText = setup;
                await command.ExecuteNonQueryAsync(Ct);
            }
            else
            {
                await using var client = await CSharpDbClient.RentAsync(options, 1, Ct);
                Assert.Null((await client.ExecuteSqlAsync(setup, Ct)).Error);
            }
            await host.StopAsync();
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            timeout.CancelAfter(TimeSpan.FromSeconds(5));
            if (ado)
            {
                await using var failed = new CSharpDbConnection(cs);
                await failed.OpenAsync(timeout.Token);
                using var command = failed.CreateCommand(); command.CommandText = "INSERT INTO recovery VALUES (2)";
                await Assert.ThrowsAnyAsync<Exception>(() => command.ExecuteNonQueryAsync(timeout.Token));
            }
            else
            {
                await using var failed = await CSharpDbClient.RentAsync(options, 1, timeout.Token);
                await Assert.ThrowsAnyAsync<Exception>(() => failed.ExecuteSqlAsync("INSERT INTO recovery VALUES (2)", timeout.Token));
            }
            host.Start();
            if (ado)
            {
                await using var recovered = new CSharpDbConnection(cs);
                await recovered.OpenAsync(Ct);
                using var command = recovered.CreateCommand(); command.CommandText = "SELECT COUNT(*) FROM recovery";
                Assert.Equal(1L, await command.ExecuteScalarAsync(Ct));
            }
            else
            {
                await using var recovered = await CSharpDbClient.RentAsync(options, 1, Ct);
                var result = await recovered.ExecuteSqlAsync("SELECT COUNT(*) FROM recovery", Ct);
                Assert.Null(result.Error); Assert.Equal(1L, result.Rows![0][0]);
            }
            Assert.Equal(2, host.Count("GetInfo"));
            Assert.Equal(2, host.Count("ExecuteSql"));
        }
        finally { await CSharpDbClient.ClearPoolAsync(host.Endpoint.AbsoluteUri); }
    }

    [Theory]
    [InlineData(CSharpDbGrpcMode.Auto)]
    [InlineData(CSharpDbGrpcMode.Native)]
    public async Task SharedPool_64SdkAndAdoCallers_SurviveRepeatedRetirement(CSharpDbGrpcMode mode)
    {
        await using var host = new RecoveryHost(mode);
        string cs = ConnectionString(host, mode, 8);
        var options = new CSharpDbClientOptions
        { Transport = CSharpDbTransport.Grpc, GrpcMode = mode, Endpoint = host.Endpoint.AbsoluteUri };
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        deadline.CancelAfter(TimeSpan.FromSeconds(60));
        CancellationToken pressureToken = deadline.Token;
        try
        {
            Task reads = Task.WhenAll(Enumerable.Range(0, 64).Select(async lane =>
            {
                await start.Task;
                for (int i = 0; i < 256; i++)
                {
                    if (lane % 2 == 0)
                    {
                        await using var client = await CSharpDbClient.RentAsync(options, 8, pressureToken);
                        var result = await client.ExecuteSqlAsync("SELECT 42", pressureToken);
                        Assert.Null(result.Error); Assert.Equal(42L, result.Rows![0][0]);
                    }
                    else
                    {
                        await using var connection = new CSharpDbConnection(cs);
                        await connection.OpenAsync(pressureToken);
                        using var command = connection.CreateCommand(); command.CommandText = "SELECT 42";
                        Assert.Equal(42L, Convert.ToInt64(await command.ExecuteScalarAsync(pressureToken)));
                    }
                }
            }));
            start.SetResult();
            int clears = 0;
            while (!reads.IsCompleted)
            {
                await CSharpDbClient.ClearPoolAsync(host.Endpoint.AbsoluteUri);
                clears++;
                await Task.Delay(20, pressureToken);
            }
            await reads.WaitAsync(TimeSpan.FromSeconds(60), Ct);
            Assert.True(clears > 0);
            Assert.Equal(64 * 256, host.Count("ExecuteSql"));
            await using var final = await CSharpDbClient.RentAsync(options, 8, Ct);
            Assert.Equal(42L, (await final.ExecuteSqlAsync("SELECT 42", Ct)).Rows![0][0]);
        }
        finally { await CSharpDbClient.ClearPoolAsync(host.Endpoint.AbsoluteUri); }
    }

    private static string ConnectionString(RecoveryHost host, CSharpDbGrpcMode mode, int capacity)
        => $"Transport=Grpc;Endpoint={host.Endpoint};Pooling=true;Max Pool Size={capacity};Grpc Mode={mode}";

    private sealed class RecoveryHost : IAsyncDisposable
    {
        private readonly string _path = Path.Combine(Path.GetTempPath(), $"csharpdb_recovery_{Guid.NewGuid():N}.db");
        private readonly CSharpDbGrpcMode _mode;
        private readonly X509Certificate2? _certificate;
        private Factory? _factory;
        public Uri Endpoint { get; private set; } = null!;
        public ConcurrentDictionary<RequestShape, long> Requests { get; } = new();

        public RecoveryHost(CSharpDbGrpcMode mode, bool tls = false)
        {
            _mode = mode;
            if (tls)
            {
                using var rsa = RSA.Create(2048);
                var request = new CertificateRequest("CN=localhost", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
                var names = new SubjectAlternativeNameBuilder(); names.AddDnsName("localhost"); names.AddIpAddress(IPAddress.Loopback);
                request.CertificateExtensions.Add(names.Build());
                using var generated = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow.AddMinutes(30));
                _certificate = X509CertificateLoader.LoadPkcs12(generated.Export(X509ContentType.Pfx), null, X509KeyStorageFlags.DefaultKeySet);
            }
            Start();
        }
        public void Start()
        {
            if (_factory is not null) throw new InvalidOperationException("Already running.");
            _factory = new Factory(_path, Requests);
            _factory.UseKestrel(options => options.Listen(IPAddress.Loopback, Endpoint?.Port ?? 0, listener =>
            {
                listener.Protocols = _certificate is not null || _mode == CSharpDbGrpcMode.Native ? HttpProtocols.Http2 : HttpProtocols.Http1;
                if (_certificate is not null) listener.UseHttps(_certificate);
            }));
            Endpoint = new Uri(_factory.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.Single());
        }
        public async Task StopAsync()
        {
            if (_factory is null) return;
            var factory = _factory; _factory = null; await factory.DisposeAsync();
        }
        public HttpClient CreatePinnedHttpClient()
        {
            string fingerprint = _certificate!.GetCertHashString(HashAlgorithmName.SHA256);
            return new HttpClient(new SocketsHttpHandler
            {
                SslOptions = new() { RemoteCertificateValidationCallback = (_, certificate, _, _) =>
                    certificate?.GetCertHashString(HashAlgorithmName.SHA256) == fingerprint },
            });
        }
        public long Count(string method) => Requests.Where(p => p.Key.Method.EndsWith("/" + method, StringComparison.Ordinal)).Sum(p => p.Value);
        public async ValueTask DisposeAsync()
        {
            await StopAsync(); _certificate?.Dispose();
            foreach (string path in new[] { _path, _path + ".wal" })
                try { File.Delete(path); } catch (IOException) { }
        }
        public readonly record struct RequestShape(string Protocol, string ContentType, string Method);

        private sealed class Capture(ConcurrentDictionary<RequestShape, long> requests) : IStartupFilter
        {
            public Action<IApplicationBuilder> Configure(Action<IApplicationBuilder> next) => app =>
            {
                app.Use(async (context, continuation) =>
                {
                    if (context.Request.Path.StartsWithSegments("/csharpdb.rpc.CSharpDbRpc"))
                        requests.AddOrUpdate(new(context.Request.Protocol, context.Request.ContentType ?? "", context.Request.Path.Value!), 1, static (_, n) => n + 1);
                    await continuation();
                });
                next(app);
            };
        }
        private sealed class Factory(string path, ConcurrentDictionary<RequestShape, long> requests) : WebApplicationFactory<global::Program>
        {
            protected override void ConfigureWebHost(IWebHostBuilder builder)
            {
                builder.UseEnvironment("Development");
                var settings = new Dictionary<string, string?>
                {
                    ["ConnectionStrings:CSharpDB"] = $"Data Source={path}",
                    ["CSharpDB:Observability:Enabled"] = "false",
                    ["CSharpDB:Observability:Health:Enabled"] = "false",
                    ["CSharpDB:Daemon:Security:Mode"] = "None",
                };
                builder.ConfigureAppConfiguration((_, configuration) => configuration.AddInMemoryCollection(settings));
                builder.ConfigureLogging(logging => logging.ClearProviders());
                builder.ConfigureServices(services =>
                {
                    services.AddSingleton<IStartupFilter>(new Capture(requests));
                    services.AddHostedService<Shutdown>();
                });
            }
        }
        private sealed class Shutdown(ICSharpDbClient client) : IHostedService
        {
            public Task StartAsync(CancellationToken ct) => Task.CompletedTask;
            public Task StopAsync(CancellationToken ct) => client.DisposeAsync().AsTask();
        }
    }
}
