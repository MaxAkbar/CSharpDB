using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.ExceptionServices;
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

public sealed class GrpcCompactScalarTests
{
    private const string ScalarCapability = "single-int64-result-v1";
    private const string SchemaCapability = "result-schema-ref-v1";

    [Theory(Timeout = 30000)]
    [InlineData(0L)]
    [InlineData(1L)]
    [InlineData(-1L)]
    [InlineData(42L)]
    [InlineData(long.MinValue)]
    [InlineData(long.MaxValue)]
    public async Task SignedLongs_PreservePresenceAndValue_AcrossActualWireAndProtobufRoundtrip(long value)
    {
        var ct = TestContext.Current.CancellationToken;
        var control = new Control { Result = One(value) };
        await using var host = Host(control);
        await using var raw = await RawSession.OpenAsync(host, schema: false, scalar: true, ct);
        var response = await raw.QueryAsync(ct);
        Assert.True(response.Result.HasScalarInt64);
        Assert.Equal(value, response.Result.ScalarInt64);
        Assert.Null(response.Result.Rows);
        var roundtrip = CommandSessionResponse.Parser.ParseFrom(response.ToByteArray());
        Assert.True(roundtrip.Result.HasScalarInt64);
        Assert.Equal(value, roundtrip.Result.ScalarInt64);
        Assert.Null(roundtrip.Result.Rows);
        await using var sdk = CSharpDbClient.Create(Options(host));
        var model = await sdk.ExecuteSqlAsync("SELECT controlled_scalar", ct);
        Assert.Equal(value, Assert.IsType<long>(Assert.Single(Assert.Single(model.Rows!))));
        Assert.Equal(2, control.SqlCalls);
    }

    [Theory(Timeout = 30000)]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task ScalarAndSchemaCapabilities_NegotiateIndependently(bool schema, bool scalar)
    {
        var ct = TestContext.Current.CancellationToken;
        var control = new Control { Result = One(42L) };
        await using var host = Host(control);
        await using var raw = await RawSession.OpenAsync(host, schema, scalar, ct);
        Assert.Equal(schema, raw.Ready.Capabilities.Contains(SchemaCapability));
        Assert.Equal(scalar, raw.Ready.Capabilities.Contains(ScalarCapability));
        var first = await raw.QueryAsync(ct);
        var repeat = await raw.QueryAsync(ct);
        foreach (var response in new[] { first, repeat })
        {
            Assert.Equal(scalar, response.Result.HasScalarInt64);
            if (scalar) { Assert.Null(response.Result.Rows); Assert.Equal(42L, response.Result.ScalarInt64); }
            else Assert.NotNull(response.Result.Rows);
        }
        Assert.False(first.ResultSchemaReference);
        Assert.Equal(schema ? 1UL : 0UL, first.ResultSchemaId);
        Assert.Equal(schema ? 1UL : 0UL, repeat.ResultSchemaId);
        Assert.Equal(schema, repeat.ResultSchemaReference);
        Assert.Equal(schema, repeat.Result.ColumnNames is null);
    }

    [Theory(Timeout = 30000)]
    [InlineData("bool")]
    [InlineData("null")]
    [InlineData("decimal")]
    [InlineData("int")]
    [InlineData("double")]
    [InlineData("empty")]
    [InlineData("multirow")]
    [InlineData("multicol")]
    [InlineData("missing-authority")]
    [InlineData("bad-names-width")]
    [InlineData("bad-types-width")]
    [InlineData("bad-nullability-width")]
    [InlineData("bad-columns-width")]
    [InlineData("nonquery")]
    [InlineData("error")]
    public async Task IneligibleResults_KeepTheCompleteGenericRowRepresentation(string shape)
    {
        var ct = TestContext.Current.CancellationToken;
        var result = Fallback(shape);
        var control = new Control { Result = result };
        await using var host = Host(control);
        await using var raw = await RawSession.OpenAsync(host, schema: false, scalar: true, ct);
        var response = await raw.QueryAsync(ct);
        Assert.False(response.Result.HasScalarInt64);
        var expected = GrpcModelMapper.ToMessage(result);
        Assert.Equal(expected, response.Result);
        Assert.Equal(1, control.SqlCalls);
    }

    [Fact(Timeout = 30000)]
    public async Task CompactResults_UseFreshPublicRowsAndCurrentMetadata_AcrossPoolReuse()
    {
        var ct = TestContext.Current.CancellationToken;
        var control = new Control { Result = One(42L, ModelSqlKind.Integer) };
        await using var host = Host(control);
        var options = Options(host);
        try
        {
            SqlExecutionResult first;
            await using (var rental = await CSharpDbClient.RentAsync(options, 1, ct))
            {
                first = await rental.ExecuteSqlAsync("SELECT controlled_scalar", ct);
                first.Rows![0][0] = "poison";
                first.Rows!.Add(new object?[] { "extra row" });
                first.ColumnNames![0] = "poison";
                first.Columns![0] = new() { Name = "poison", Type = ModelType.Blob };
            }
            await using var next = await CSharpDbClient.RentAsync(options, 1, ct);
            var repeat = await next.ExecuteSqlAsync("SELECT controlled_scalar", ct);
            Assert.Equal(42L, Assert.IsType<long>(Assert.Single(Assert.Single(repeat.Rows!))));
            Assert.Equal("value", Assert.Single(repeat.ColumnNames!));
            Assert.Equal(ModelSqlKind.Integer, Assert.Single(repeat.Columns!).DeclaredType!.Kind);
            Assert.NotSame(first.Rows, repeat.Rows);
            Assert.NotSame(first.Rows![0], repeat.Rows![0]);
            Assert.NotSame(first.ColumnNames, repeat.ColumnNames);
            Assert.NotSame(first.Columns, repeat.Columns);
            Volatile.Write(ref control.Result, One(long.MaxValue, ModelSqlKind.BigInt));
            var changed = await next.ExecuteSqlAsync("SELECT controlled_scalar", ct);
            Assert.Equal(long.MaxValue, Assert.IsType<long>(Assert.Single(Assert.Single(changed.Rows!))));
            Assert.Equal(ModelSqlKind.BigInt, Assert.Single(changed.Columns!).DeclaredType!.Kind);
            var frames = control.Responses.Where(r => r.Result is not null).ToArray();
            Assert.Equal(3, frames.Length);
            Assert.All(frames, frame => Assert.True(frame.Result.HasScalarInt64));
            Assert.Equal(1UL, frames[0].ResultSchemaId);
            Assert.True(frames[1].ResultSchemaReference);
            Assert.Equal(2UL, frames[2].ResultSchemaId);
            Assert.False(frames[2].ResultSchemaReference);
            Assert.Equal(1, HttpCount(host, "CommandSession"));
        }
        finally { await CSharpDbClient.ClearPoolAsync(host.NativeEndpoint.AbsoluteUri); }
    }

    [Fact(Timeout = 30000)]
    public async Task NonNegotiatingPeer_LeavesNewClientOnLegacyRows_AndUnaryStaysLegacy()
    {
        var ct = TestContext.Current.CancellationToken;
        var control = new Control { Result = One(42L), Mode = "no-echo" };
        await using var host = Host(control);
        await using var stream = CSharpDbClient.Create(Options(host));
        for (int n = 0; n < 2; n++)
            Assert.Equal(42L, (await stream.ExecuteSqlAsync("SELECT controlled_scalar", ct)).Rows![0][0]);
        Assert.DoesNotContain(ScalarCapability, control.Responses.Single(r => r.Ready is not null).Ready.Capabilities);
        Assert.All(control.Responses.Where(r => r.Result is not null), r =>
        { Assert.False(r.Result.HasScalarInt64); Assert.NotNull(r.Result.Rows); });
        await using var unary = host.CreateClient(CSharpDbGrpcMode.Native);
        Assert.Equal(42L, (await unary.ExecuteSqlAsync("SELECT controlled_scalar", ct)).Rows![0][0]);
        Assert.Equal(1, HttpCount(host, "ExecuteSql"));
    }

    [Fact(Timeout = 30000)]
    public async Task Compact42_RemovesSixProtobufBytes_WithoutChangingThePublicRows()
    {
        var ct = TestContext.Current.CancellationToken;
        var control = new Control { Result = One(42L) };
        await using var host = Host(control);
        await using var ordinary = await RawSession.OpenAsync(host, schema: true, scalar: false, ct);
        await using var compact = await RawSession.OpenAsync(host, schema: true, scalar: true, ct);
        var legacy = await ordinary.QueryAsync(ct);
        var encoded = await compact.QueryAsync(ct);
        Assert.NotNull(legacy.Result.Rows); Assert.False(legacy.Result.HasScalarInt64);
        Assert.Null(encoded.Result.Rows); Assert.True(encoded.Result.HasScalarInt64);
        int legacyBytes = legacy.CalculateSize(), compactBytes = encoded.CalculateSize();
        Assert.Equal(6, legacyBytes - compactBytes);
        TestContext.Current.TestOutputHelper?.WriteLine($"Int64=42 normalized protobuf bytes: ordinary={legacyBytes}, compact={compactBytes}, saved={legacyBytes - compactBytes}; excludes gRPC/HTTP2/TCP framing.");
    }

    [Theory(Timeout = 30000)]
    [InlineData("mixed-empty")]
    [InlineData("mixed-filled")]
    [InlineData("unnegotiated")]
    [InlineData("bad-width")]
    [InlineData("reference-width")]
    [InlineData("nonquery")]
    [InlineData("error")]
    [InlineData("missing-metadata")]
    public async Task MalformedCompactFrame_ClosesSession_WithoutReplayingTheCommand(string mode)
    {
        var ct = TestContext.Current.CancellationToken;
        var control = new Control { Result = mode == "reference-width" ? Fallback("multicol") : One(42L), Mode = mode };
        await using var host = Host(control);
        await using var client = CSharpDbClient.Create(Options(host));
        bool warm = mode == "reference-width";
        if (warm) Assert.Equal(2, (await client.ExecuteSqlAsync("SELECT controlled_scalar", ct)).ColumnNames!.Length);
        var error = await Assert.ThrowsAsync<CSharpDbClientException>(() => client.ExecuteSqlAsync("SELECT controlled_scalar", ct));
        Assert.Equal(StatusCode.DataLoss, Assert.IsType<RpcException>(error.InnerException).StatusCode);
        await Assert.ThrowsAsync<CSharpDbClientException>(() => client.ExecuteSqlAsync("SELECT no_replay", ct));
        Assert.Equal(warm ? 2 : 1, control.SqlCalls);
        Assert.Equal(1, HttpCount(host, "CommandSession"));
    }

    [Fact]
    public void PublicUnaryMapper_RejectsCompactPayloadInsteadOfSilentlyDroppingIt()
    {
        var message = new SqlExecutionResultMessage
        {
            IsQuery = true, ScalarInt64 = 0, Elapsed = new Duration(),
            ColumnNames = new StringList { Values = { "value" } },
        };
        Assert.True(message.HasScalarInt64);
        Assert.Throws<CSharpDbClientException>(() => GrpcModelMapper.ToModel(message));
    }

    private static SqlExecutionResult One(object? value, ModelSqlKind kind = ModelSqlKind.BigInt)
        => new()
        {
            IsQuery = true, ColumnNames = ["value"], ColumnTypes = [kind.ToString().ToUpperInvariant()],
            ColumnNullability = [value is null], Columns = [Column("value", kind, value is null)],
            Rows = [new object?[] { value }], RowsAffected = 1,
        };
    private static ModelColumn Column(string name, ModelSqlKind kind, bool nullable = false) => new()
    {
        Name = name, Type = kind == ModelSqlKind.Decimal ? ModelType.Decimal : kind == ModelSqlKind.Double ? ModelType.Real : ModelType.Integer,
        Nullable = nullable, DeclaredType = new ModelSqlType { Kind = kind },
    };
    private static SqlExecutionResult Fallback(string shape)
    {
        object? value = shape switch { "bool" => true, "null" => null, "decimal" => 12.34m, "int" => 42, "double" => 42d, _ => 42L };
        ModelSqlKind kind = shape switch { "bool" => ModelSqlKind.Boolean, "decimal" => ModelSqlKind.Decimal, "double" => ModelSqlKind.Double, _ => ModelSqlKind.BigInt };
        string[]? names = ["value"], types = [kind.ToString().ToUpperInvariant()];
        bool[]? nullability = [value is null]; ModelColumn[]? columns = [Column("value", kind, value is null)];
        List<object?[]>? rows = [new object?[] { value }];
        bool query = true; string? error = null;
        switch (shape)
        {
            case "empty": rows = []; break;
            case "multirow": rows!.Add(new object?[] { 43L }); break;
            case "multicol":
                names = ["value", "other"]; types = ["BIGINT", "BIGINT"]; nullability = [false, false];
                columns = [Column("value", kind), Column("other", kind)]; rows = [new object?[] { 42L, 43L }]; break;
            case "missing-authority": names = null; columns = null; break;
            case "bad-names-width": names = []; break;
            case "bad-types-width": types = []; break;
            case "bad-nullability-width": nullability = []; break;
            case "bad-columns-width": columns = []; break;
            case "nonquery": query = false; names = null; types = null; nullability = null; columns = null; rows = null; break;
            case "error": error = "controlled SQL error"; break;
        }
        return new() { IsQuery = query, Error = error, ColumnNames = names, ColumnTypes = types, ColumnNullability = nullability,
            Columns = columns, Rows = rows, RowsAffected = rows?.Count ?? 1 };
    }

    private static CSharpDbClientOptions Options(GrpcSocketDaemonFixture host) => new()
    { Transport = CSharpDbTransport.Grpc, GrpcMode = CSharpDbGrpcMode.Native, GrpcCallMode = CSharpDbGrpcCallMode.Streaming, Endpoint = host.NativeEndpoint.AbsoluteUri };
    private static long HttpCount(GrpcSocketDaemonFixture host, string method)
        => host.Snapshot().Where(x => x.Key.Method.EndsWith("/" + method, StringComparison.Ordinal)).Sum(x => x.Value);
    private static GrpcSocketDaemonFixture Host(Control control) => new(configureServices: services =>
    {
        var original = services.Last(s => s.ServiceType == typeof(ICSharpDbClient));
        services.Replace(ServiceDescriptor.Singleton<ICSharpDbClient>(provider =>
        {
            var proxy = DispatchProxy.Create<ICSharpDbClient, Backend>();
            ((Backend)(object)proxy).Inner = (ICSharpDbClient)original.ImplementationFactory!(provider);
            ((Backend)(object)proxy).Control = control;
            return proxy;
        }));
        services.AddSingleton(new Capture(control));
        services.AddGrpc(options => options.Interceptors.Add<Capture>());
    });
    public sealed class Control
    {
        public SqlExecutionResult Result = null!;
        public string? Mode;
        public int SqlCalls, Results;
        public ConcurrentQueue<CommandSessionResponse> Responses { get; } = new();
    }
    public class Backend : DispatchProxy
    {
        public ICSharpDbClient Inner = null!;
        public Control Control = null!;
        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (targetMethod!.Name == nameof(ICSharpDbClient.ExecuteSqlAsync))
            { Interlocked.Increment(ref Control.SqlCalls); return Task.FromResult(Volatile.Read(ref Control.Result)); }
            try { return targetMethod.Invoke(Inner, args); }
            catch (TargetInvocationException error) when (error.InnerException is not null)
            { ExceptionDispatchInfo.Capture(error.InnerException).Throw(); throw; }
        }
    }
    public sealed class Capture(Control control) : Interceptor
    {
        public override Task DuplexStreamingServerHandler<TRequest, TResponse>(IAsyncStreamReader<TRequest> reader,
            IServerStreamWriter<TResponse> writer, ServerCallContext context, DuplexStreamingServerMethod<TRequest, TResponse> continuation)
            => continuation(new Reader<TRequest>(reader, control), new Writer<TResponse>(writer, control), context);
        private sealed class Reader<T>(IAsyncStreamReader<T> inner, Control state) : IAsyncStreamReader<T>
        {
            public T Current { get; private set; } = default!;
            public async Task<bool> MoveNext(CancellationToken ct)
            {
                if (!await inner.MoveNext(ct)) return false;
                Current = inner.Current;
                if (Current is CommandSessionRequest { Hello: not null } request && state.Mode is "no-echo" or "unnegotiated")
                {
                    request = request.Clone(); request.Hello.Capabilities.Remove(ScalarCapability); Current = (T)(object)request;
                }
                return true;
            }
        }
        private sealed class Writer<T>(IServerStreamWriter<T> inner, Control state) : IServerStreamWriter<T>
        {
            public WriteOptions? WriteOptions { get => inner.WriteOptions; set => inner.WriteOptions = value; }
            public Task WriteAsync(T message)
            {
                if (message is not CommandSessionResponse response) return inner.WriteAsync(message);
                response = response.Clone();
                if (response.Result is not null)
                {
                    int number = Interlocked.Increment(ref state.Results);
                    if (state.Mode != "no-echo" && (number == 1 && state.Mode != "reference-width" || number == 2 && state.Mode == "reference-width"))
                    {
                        switch (state.Mode)
                        {
                            case "mixed-empty": response.Result.Rows = new VariantArrayList(); break;
                            case "mixed-filled":
                                response.Result.Rows = new VariantArrayList();
                                response.Result.Rows.Items.Add(new VariantArray { Items = { new VariantValue { Int64Value = 42 } } }); break;
                            case "unnegotiated": response.Result.ScalarInt64 = 42; response.Result.Rows = null; break;
                            case "bad-width": response.Result.ColumnNames.Values.Add("other"); break;
                            case "reference-width": response.Result.ScalarInt64 = 42; response.Result.Rows = null; break;
                            case "nonquery": response.Result.IsQuery = false; response.ResultSchemaId = 0; response.ResultSchemaReference = false; break;
                            case "error": response.Result.Error = "malicious scalar error"; response.ResultSchemaId = 0; response.ResultSchemaReference = false; break;
                            case "missing-metadata":
                                response.Result.ColumnNames = null; response.Result.ColumnTypes = null;
                                response.Result.ColumnNullability = null; response.Result.Columns.Clear(); break;
                        }
                    }
                }
                state.Responses.Enqueue(response.Clone());
                return inner.WriteAsync((T)(object)response);
            }
        }
    }
    private sealed class RawSession(GrpcChannel channel, AsyncDuplexStreamingCall<CommandSessionRequest, CommandSessionResponse> call,
        CommandSessionReady ready) : IAsyncDisposable
    {
        private ulong _id;
        public CommandSessionReady Ready { get; } = ready;
        public static async Task<RawSession> OpenAsync(GrpcSocketDaemonFixture host, bool schema, bool scalar, CancellationToken ct)
        {
            var channel = GrpcChannel.ForAddress(host.NativeEndpoint);
            var call = new CSharpDbRpc.CSharpDbRpcClient(channel).CommandSession(cancellationToken: ct);
            var hello = new CommandSessionHello { ProtocolVersion = 1 };
            if (schema) hello.Capabilities.Add(SchemaCapability);
            if (scalar) hello.Capabilities.Add(ScalarCapability);
            try
            {
                await call.RequestStream.WriteAsync(new() { Hello = hello }, ct);
                Assert.True(await call.ResponseStream.MoveNext(ct));
                return new(channel, call, call.ResponseStream.Current.Ready);
            }
            catch { call.Dispose(); channel.Dispose(); throw; }
        }
        public async Task<CommandSessionResponse> QueryAsync(CancellationToken ct)
        {
            await call.RequestStream.WriteAsync(new() { RequestId = ++_id, ExecuteSql = new() { Sql = "SELECT controlled_scalar" } }, ct);
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
}
