using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Text.Json;
using System.Text.Json.Nodes;
using CSharpDB.Client;
using CSharpDB.Client.Grpc;
using CSharpDB.Client.Models;
using CSharpDB.Testing;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ModelColumn = CSharpDB.Client.Models.ColumnDefinition;
using ModelType = CSharpDB.Client.Models.DbType;
using ModelSqlKind = CSharpDB.Client.Models.SqlTypeKind;
using ModelSqlType = CSharpDB.Client.Models.SqlTypeDescriptor;

namespace CSharpDB.Daemon.Tests;

public sealed class GrpcResultSchemaReuseTests
{
    private const string Capability = "result-schema-ref-v1";

    [Theory(Timeout = 30000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RawPeer_NegotiatesOnlyWhenOffered_AndReferenceSavesSerializedBytes(bool offer)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var host = new GrpcSocketDaemonFixture();
        await using var session = await RawSession.OpenAsync(host, offer, ct);
        Assert.Equal(offer, session.Ready.Capabilities.Contains(Capability));
        const string sql = "SELECT CAST(42 AS INTEGER) AS result_schema_value";
        var full = await session.SqlAsync(sql, ct);
        var repeat = await session.SqlAsync(sql, ct);
        Assert.NotNull(full.Result.ColumnNames);
        Assert.NotEmpty(full.Result.Columns);
        if (offer)
        {
            AssertFull(full, 1);
            AssertReference(repeat, 1);
            Assert.Equal(full.Result.Rows, repeat.Result.Rows);
            // Normalize elapsed timing and request ID to isolate schema bytes.
            var normalizedFull = full.Clone(); var normalizedReference = repeat.Clone();
            normalizedFull.RequestId = normalizedReference.RequestId = 1;
            normalizedFull.Result.Elapsed = normalizedReference.Result.Elapsed = new Duration();
            int fullBytes = normalizedFull.CalculateSize(), referenceBytes = normalizedReference.CalculateSize();
            Assert.True(referenceBytes < fullBytes, $"Full {fullBytes} bytes; reference {referenceBytes} bytes.");
            TestContext.Current.TestOutputHelper?.WriteLine($"Schema wire bytes: actual full={full.CalculateSize()}, ref={repeat.CalculateSize()}; normalized full={fullBytes}, ref={referenceBytes}, saved={fullBytes - referenceBytes}.");
        }
        else
        {
            Assert.Equal(0UL, full.ResultSchemaId);
            Assert.Equal(0UL, repeat.ResultSchemaId);
            Assert.False(full.ResultSchemaReference);
            Assert.False(repeat.ResultSchemaReference);
            Assert.Equal(full.Result.ColumnNames, repeat.Result.ColumnNames);
            Assert.Equal(full.Result.Columns, repeat.Result.Columns);
        }
    }

    [Fact(Timeout = 30000)]
    public async Task RawSession_InvalidatesOnDmlErrorsInfoTransactionsAndDdl_WithoutReusingIds()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var host = new GrpcSocketDaemonFixture();
        await using var session = await RawSession.OpenAsync(host, true, ct);
        AssertNoSchema(await session.SqlAsync("CREATE TABLE schema_invalidation (id INTEGER PRIMARY KEY, value INTEGER); INSERT INTO schema_invalidation VALUES (1, 42)", ct));
        const string sql = "SELECT value FROM schema_invalidation WHERE id = 1";
        AssertFull(await session.SqlAsync(sql, ct), 1);
        AssertReference(await session.SqlAsync(sql, ct), 1);
        AssertNoSchema(await session.SqlAsync("UPDATE schema_invalidation SET value = 43 WHERE id = 1", ct));
        AssertFull(await session.SqlAsync(sql, ct), 2);
        AssertReference(await session.SqlAsync(sql, ct), 2);
        AssertNoSchema(await session.SendAsync(new() { GetInfo = new Empty() }, ct));
        AssertFull(await session.SqlAsync(sql, ct), 3);
        var error = await session.SqlAsync("SELECT missing FROM schema_invalidation", ct);
        Assert.True(error.Error is not null || !string.IsNullOrEmpty(error.Result?.Error));
        AssertNoSchema(error);
        AssertFull(await session.SqlAsync(sql, ct), 4);
        var begin = await session.SendAsync(new() { BeginTransaction = new Empty() }, ct);
        AssertNoSchema(begin);
        string id = begin.Transaction.TransactionId;
        AssertFull(await session.TransactionSqlAsync(id, sql, ct), 5);
        AssertReference(await session.TransactionSqlAsync(id, sql, ct), 5);
        const string projection = "SELECT CAST(value AS BIGINT) AS value FROM schema_invalidation WHERE id = 1";
        AssertFull(await session.TransactionSqlAsync(id, projection, ct), 6);
        AssertReference(await session.TransactionSqlAsync(id, projection, ct), 6);
        AssertNoSchema(await session.SendAsync(new() { RollbackTransaction = new() { TransactionId = id } }, ct));
        AssertFull(await session.SqlAsync(sql, ct), 7);
        AssertNoSchema(await session.SqlAsync("DROP TABLE schema_invalidation; CREATE TABLE schema_invalidation (id INTEGER PRIMARY KEY, value BIGINT); INSERT INTO schema_invalidation VALUES (1, 43)", ct));
        var changed = await session.SqlAsync(sql, ct);
        AssertFull(changed, 8);
        Assert.Equal(SqlTypeKindEnum.SqlTypeKindBigInt, changed.Result.Columns[0].DeclaredType.Kind);
        begin = await session.SendAsync(new() { BeginTransaction = new Empty() }, ct);
        AssertNoSchema(begin);
        id = begin.Transaction.TransactionId;
        AssertFull(await session.TransactionSqlAsync(id, sql, ct), 9);
        AssertNoSchema(await session.SendAsync(new() { CommitTransaction = new() { TransactionId = id } }, ct));
        AssertFull(await session.SqlAsync(sql, ct), 10);
    }

    [Theory(Timeout = 30000)]
    [InlineData("SchemaId")]
    [InlineData("Name")]
    [InlineData("Type")]
    [InlineData("Nullable")]
    [InlineData("IsPrimaryKey")]
    [InlineData("IsIdentity")]
    [InlineData("IsRowVersion")]
    [InlineData("Collation")]
    [InlineData("DefaultSql")]
    [InlineData("DeclaredKind")]
    [InlineData("DeclaredLength")]
    [InlineData("DeclaredPrecision")]
    [InlineData("DeclaredScale")]
    [InlineData("DeclaredFractionalSeconds")]
    [InlineData("DeclaredPresence")]
    [InlineData("NamesNullEmpty")]
    [InlineData("TypesNullEmpty")]
    [InlineData("NullabilityNullEmpty")]
    public async Task ActualMetadataChange_RequiresNewFullSchema_EvenWhenSqlAndRowsAreIdentical(string change)
    {
        var ct = TestContext.Current.CancellationToken;
        var baseline = Metadata(change, changed: false);
        var changed = Metadata(change, changed: true);
        var control = new ResponseControl { Result = baseline };
        await using var host = new GrpcSocketDaemonFixture(configureServices: services => ReplaceBackend(services, control));
        await using var session = await RawSession.OpenAsync(host, true, ct);
        const string sql = "SELECT controlled_metadata";
        var first = await session.SqlAsync(sql, ct);
        AssertFull(first, 1);
        AssertReference(await session.SqlAsync(sql, ct), 1);
        Volatile.Write(ref control.Result, changed);
        var next = await session.SqlAsync(sql, ct);
        AssertFull(next, 2);
        AssertReference(await session.SqlAsync(sql, ct), 2);
        Assert.Equal(first.Result.Rows, next.Result.Rows);
        Assert.Equal(4, control.SqlCalls);
    }

    [Fact(Timeout = 30000)]
    public async Task PooledSdkResults_AreFreshAfterCallerMutation_AndAcrossAlternatingProjections()
    {
        var ct = TestContext.Current.CancellationToken;
        var capture = new SessionInterceptor();
        await using var host = Host(capture);
        var options = Options(host);
        const string sql = "SELECT CAST(42 AS INTEGER) AS fresh_value";
        try
        {
            SqlExecutionResult first;
            string[] expectedNames, expectedTypes; bool[] expectedNullability;
            await using (var rental = await CSharpDbClient.RentAsync(options, 1, ct))
            {
                first = await rental.ExecuteSqlAsync(sql, ct);
                expectedNames = first.ColumnNames!.ToArray(); expectedTypes = first.ColumnTypes!.ToArray();
                expectedNullability = first.ColumnNullability!.ToArray();
                first.ColumnNames![0] = "poison"; first.ColumnTypes![0] = "BLOB";
                first.ColumnNullability![0] = !first.ColumnNullability[0];
                first.Columns![0] = new ModelColumn { Name = "poison", Type = ModelType.Blob };
                first.Rows![0][0] = "poison";
            }
            await using var next = await CSharpDbClient.RentAsync(options, 1, ct);
            var second = await next.ExecuteSqlAsync(sql, ct);
            Assert.Equal(expectedNames, second.ColumnNames);
            Assert.Equal(expectedTypes, second.ColumnTypes);
            Assert.Equal(expectedNullability, second.ColumnNullability);
            Assert.Equal("fresh_value", second.Columns![0].Name);
            Assert.Equal(ModelSqlKind.Integer, second.Columns[0].DeclaredType!.Kind);
            Assert.Equal(42L, second.Rows![0][0]);
            Assert.NotSame(first.ColumnNames, second.ColumnNames);
            Assert.NotSame(first.ColumnTypes, second.ColumnTypes);
            Assert.NotSame(first.ColumnNullability, second.ColumnNullability);
            Assert.NotSame(first.Columns, second.Columns);
            Assert.NotSame(first.Rows, second.Rows);
            var firstReplies = capture.Responses.Where(r => r.Result is not null).ToArray();
            Assert.Equal(2, firstReplies.Length);
            AssertFull(firstReplies[0], 1); AssertReference(firstReplies[1], 1);
            var work = Enumerable.Range(0, 24).Select(async n =>
            {
                string type = n % 2 == 0 ? "INTEGER" : "BIGINT";
                string alias = n % 2 == 0 ? "small_value" : "large_value";
                var result = await next.ExecuteSqlAsync($"SELECT CAST({n} AS {type}) AS {alias}", ct);
                Assert.Equal(alias, Assert.Single(result.ColumnNames!));
                Assert.Equal(n % 2 == 0 ? ModelSqlKind.Integer : ModelSqlKind.BigInt, Assert.Single(result.Columns!).DeclaredType!.Kind);
                Assert.Equal((long)n, result.Rows![0][0]);
            });
            await Task.WhenAll(work);
            Assert.Equal(1, HttpCount(host, "CommandSession"));
        }
        finally { await CSharpDbClient.ClearPoolAsync(host.NativeEndpoint.AbsoluteUri); }
    }

    [Fact(Timeout = 30000)]
    public async Task NewClient_WithNonNegotiatingPeer_UsesFullMetadataOnEveryResult()
    {
        var ct = TestContext.Current.CancellationToken;
        var capture = new SessionInterceptor("no-echo");
        await using var host = Host(capture);
        await using var client = CSharpDbClient.Create(Options(host));
        for (int i = 0; i < 3; i++)
        {
            var result = await client.ExecuteSqlAsync("SELECT CAST(42 AS INTEGER) AS legacy_value", ct);
            Assert.Equal("legacy_value", Assert.Single(result.ColumnNames!));
            Assert.Equal(ModelSqlKind.Integer, Assert.Single(result.Columns!).DeclaredType!.Kind);
            Assert.Equal(42L, result.Rows![0][0]);
        }
        Assert.DoesNotContain(Capability, capture.Responses.Single(r => r.Ready is not null).Ready.Capabilities);
        Assert.All(capture.Responses.Where(r => r.Result is not null), response =>
        {
            AssertNoSchema(response);
            Assert.NotNull(response.Result.ColumnNames);
            Assert.NotEmpty(response.Result.Columns);
        });
        Assert.Equal(3, capture.SqlCommands);
    }

    [Theory(Timeout = 30000)]
    [InlineData("unknown-id")]
    [InlineData("mixed-reference")]
    [InlineData("unsolicited-reference")]
    [InlineData("full-gap")]
    public async Task MalformedReference_ClosesSession_WithoutReplayingTheSql(string corruption)
    {
        var ct = TestContext.Current.CancellationToken;
        var capture = new SessionInterceptor(corruption);
        await using var host = Host(capture);
        await using var client = CSharpDbClient.Create(Options(host));
        bool warm = corruption is "unknown-id" or "mixed-reference";
        if (warm) Assert.Null((await client.ExecuteSqlAsync("SELECT 42 AS value", ct)).Error);
        var error = await Assert.ThrowsAsync<CSharpDbClientException>(() => client.ExecuteSqlAsync("SELECT 42 AS value", ct));
        Assert.Equal(StatusCode.DataLoss, Assert.IsType<RpcException>(error.InnerException).StatusCode);
        await Assert.ThrowsAsync<CSharpDbClientException>(() => client.ExecuteSqlAsync("SELECT 99 AS value", ct));
        Assert.Equal(warm ? 2 : 1, capture.SqlCommands);
        Assert.Equal(1, HttpCount(host, "CommandSession"));
    }

    private static void AssertFull(CommandSessionResponse response, ulong id)
    {
        Assert.NotNull(response.Result); Assert.True(response.Result.IsQuery); Assert.Null(response.Result.Error);
        Assert.Equal(id, response.ResultSchemaId); Assert.False(response.ResultSchemaReference);
    }
    private static void AssertReference(CommandSessionResponse response, ulong id)
    {
        Assert.Equal(id, response.ResultSchemaId); Assert.True(response.ResultSchemaReference);
        Assert.Null(response.Result.ColumnNames); Assert.Null(response.Result.ColumnTypes);
        Assert.Null(response.Result.ColumnNullability); Assert.Empty(response.Result.Columns);
    }
    private static void AssertNoSchema(CommandSessionResponse response)
    { Assert.Equal(0UL, response.ResultSchemaId); Assert.False(response.ResultSchemaReference); }
    private static CSharpDbClientOptions Options(GrpcSocketDaemonFixture host) => new()
    {
        Transport = CSharpDbTransport.Grpc, GrpcMode = CSharpDbGrpcMode.Native,
        GrpcCallMode = CSharpDbGrpcCallMode.Streaming, Endpoint = host.NativeEndpoint.AbsoluteUri,
    };
    private static long HttpCount(GrpcSocketDaemonFixture host, string method)
        => host.Snapshot().Where(x => x.Key.Method.EndsWith("/" + method, StringComparison.Ordinal)).Sum(x => x.Value);
    private static GrpcSocketDaemonFixture Host(SessionInterceptor capture) => new(configureServices: services =>
    {
        services.AddSingleton(capture);
        services.AddGrpc(options => options.Interceptors.Add<SessionInterceptor>());
    });

    private static SqlExecutionResult Metadata(string change, bool changed)
    {
        var declared = new ModelSqlType { Kind = ModelSqlKind.VarChar, Length = 20 };
        if (change is "DeclaredPrecision" or "DeclaredScale") declared = new() { Kind = ModelSqlKind.Decimal, Precision = 10, Scale = 2 };
        if (change == "DeclaredFractionalSeconds") declared = new() { Kind = ModelSqlKind.Timestamp, FractionalSecondsPrecision = 3 };
        var column = new ModelColumn
        {
            SchemaId = Guid.Parse("10000000-0000-0000-0000-000000000001"), Name = "value", Type = ModelType.Text,
            Nullable = false, DeclaredType = change == "DeclaredPresence" && !changed ? null : declared,
        };
        if (changed && change is not ("NamesNullEmpty" or "TypesNullEmpty" or "NullabilityNullEmpty" or "DeclaredPresence"))
        {
            JsonObject node = JsonSerializer.SerializeToNode(column)!.AsObject();
            switch (change)
            {
                case "SchemaId": node["SchemaId"] = "20000000-0000-0000-0000-000000000002"; break;
                case "Name": node["Name"] = "other"; break;
                case "Type": node["Type"] = (int)ModelType.Blob; break;
                case "Nullable": node["Nullable"] = true; break;
                case "IsPrimaryKey": case "IsIdentity": case "IsRowVersion": node[change] = true; break;
                case "Collation": node["Collation"] = "NOCASE"; break;
                case "DefaultSql": node["DefaultSql"] = "'default'"; break;
                case "DeclaredKind": node["DeclaredType"]!["Kind"] = (int)ModelSqlKind.Text; break;
                case "DeclaredLength": node["DeclaredType"]!["Length"] = 40; break;
                case "DeclaredPrecision": node["DeclaredType"]!["Precision"] = 12; break;
                case "DeclaredScale": node["DeclaredType"]!["Scale"] = 3; break;
                case "DeclaredFractionalSeconds": node["DeclaredType"]!["FractionalSecondsPrecision"] = 4; break;
                default: throw new ArgumentOutOfRangeException(nameof(change));
            }
            column = node.Deserialize<ModelColumn>()!;
        }
        return new()
        {
            IsQuery = true, Rows = [new object?[] { "row" }], Columns = [column],
            ColumnNames = change == "NamesNullEmpty" ? changed ? [] : null : ["value"],
            ColumnTypes = change == "TypesNullEmpty" ? changed ? [] : null : ["VARCHAR(20)"],
            ColumnNullability = change == "NullabilityNullEmpty" ? changed ? [] : null : [false],
        };
    }
    private static void ReplaceBackend(IServiceCollection services, ResponseControl control)
    {
        var original = services.Last(s => s.ServiceType == typeof(ICSharpDbClient));
        services.Replace(ServiceDescriptor.Singleton<ICSharpDbClient>(provider =>
        {
            var proxy = DispatchProxy.Create<ICSharpDbClient, ControlledBackend>();
            ((ControlledBackend)(object)proxy).Inner = (ICSharpDbClient)original.ImplementationFactory!(provider);
            ((ControlledBackend)(object)proxy).Control = control;
            return proxy;
        }));
    }
    public sealed class ResponseControl { public SqlExecutionResult Result = null!; public int SqlCalls; }
    public class ControlledBackend : DispatchProxy
    {
        public ICSharpDbClient Inner = null!;
        public ResponseControl Control = null!;
        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (targetMethod!.Name == nameof(ICSharpDbClient.ExecuteSqlAsync))
            { Interlocked.Increment(ref Control.SqlCalls); return Task.FromResult(Volatile.Read(ref Control.Result)); }
            try { return targetMethod.Invoke(Inner, args); }
            catch (TargetInvocationException error) when (error.InnerException is not null)
            { ExceptionDispatchInfo.Capture(error.InnerException).Throw(); throw; }
        }
    }
    private sealed class RawSession(GrpcChannel channel, AsyncDuplexStreamingCall<CommandSessionRequest, CommandSessionResponse> call,
        CommandSessionReady ready) : IAsyncDisposable
    {
        private ulong _id;
        public CommandSessionReady Ready { get; } = ready;
        public static async Task<RawSession> OpenAsync(GrpcSocketDaemonFixture host, bool offer, CancellationToken ct)
        {
            var channel = GrpcChannel.ForAddress(host.NativeEndpoint);
            var call = new CSharpDbRpc.CSharpDbRpcClient(channel).CommandSession(cancellationToken: ct);
            var hello = new CommandSessionHello { ProtocolVersion = 1 };
            if (offer) hello.Capabilities.Add(Capability);
            try
            {
                await call.RequestStream.WriteAsync(new CommandSessionRequest { Hello = hello }, ct);
                Assert.True(await call.ResponseStream.MoveNext(ct));
                return new(channel, call, call.ResponseStream.Current.Ready);
            }
            catch { call.Dispose(); channel.Dispose(); throw; }
        }
        public Task<CommandSessionResponse> SqlAsync(string sql, CancellationToken ct)
            => SendAsync(new() { ExecuteSql = new() { Sql = sql } }, ct);
        public Task<CommandSessionResponse> TransactionSqlAsync(string id, string sql, CancellationToken ct)
            => SendAsync(new() { ExecuteInTransaction = new() { TransactionId = id, Sql = sql } }, ct);
        public async Task<CommandSessionResponse> SendAsync(CommandSessionRequest request, CancellationToken ct)
        {
            request.RequestId = ++_id;
            await call.RequestStream.WriteAsync(request, ct);
            Assert.True(await call.ResponseStream.MoveNext(ct));
            Assert.Equal(_id, call.ResponseStream.Current.RequestId);
            return call.ResponseStream.Current;
        }
        public async ValueTask DisposeAsync()
        {
            try
            {
                await call.RequestStream.CompleteAsync().WaitAsync(TimeSpan.FromSeconds(5));
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                Assert.False(await call.ResponseStream.MoveNext(timeout.Token));
            }
            finally { call.Dispose(); channel.Dispose(); }
        }
    }
    public sealed class SessionInterceptor(string? mode = null) : Interceptor
    {
        private readonly string? _mode = mode;
        public ConcurrentQueue<CommandSessionResponse> Responses { get; } = new();
        public int SqlCommands;
        private int _results;
        private CommandSessionResponse? _first;
        public override Task DuplexStreamingServerHandler<TRequest, TResponse>(IAsyncStreamReader<TRequest> reader,
            IServerStreamWriter<TResponse> writer, ServerCallContext context, DuplexStreamingServerMethod<TRequest, TResponse> continuation)
            => continuation(new Reader<TRequest>(reader, this), new Writer<TResponse>(writer, this), context);
        private sealed class Reader<T>(IAsyncStreamReader<T> inner, SessionInterceptor owner) : IAsyncStreamReader<T>
        {
            public T Current { get; private set; } = default!;
            public async Task<bool> MoveNext(CancellationToken ct)
            {
                if (!await inner.MoveNext(ct)) return false;
                Current = inner.Current;
                if (Current is CommandSessionRequest request)
                {
                    if (request.ExecuteSql is not null) Interlocked.Increment(ref owner.SqlCommands);
                    if (request.Hello is not null && owner._mode is "no-echo" or "unsolicited-reference")
                    {
                        request = request.Clone(); request.Hello.Capabilities.Clear(); Current = (T)(object)request;
                    }
                }
                return true;
            }
        }
        private sealed class Writer<T>(IServerStreamWriter<T> inner, SessionInterceptor owner) : IServerStreamWriter<T>
        {
            public WriteOptions? WriteOptions { get => inner.WriteOptions; set => inner.WriteOptions = value; }
            public Task WriteAsync(T message)
            {
                if (message is not CommandSessionResponse response) return inner.WriteAsync(message);
                response = response.Clone();
                if (response.Result is not null)
                {
                    int number = Interlocked.Increment(ref owner._results);
                    if (number == 1) owner._first = response.Clone();
                    if (owner._mode == "unknown-id" && number == 2) response.ResultSchemaId = 999;
                    if (owner._mode == "mixed-reference" && number == 2) response.Result.ColumnNames = owner._first!.Result.ColumnNames.Clone();
                    if (owner._mode == "full-gap" && number == 1) response.ResultSchemaId = 7;
                    if (owner._mode == "unsolicited-reference" && number == 1)
                    {
                        response.ResultSchemaId = 1; response.ResultSchemaReference = true;
                        response.Result.ColumnNames = null; response.Result.ColumnTypes = null;
                        response.Result.ColumnNullability = null; response.Result.Columns.Clear();
                    }
                }
                owner.Responses.Enqueue(response.Clone());
                return inner.WriteAsync((T)(object)response);
            }
        }
    }
}
