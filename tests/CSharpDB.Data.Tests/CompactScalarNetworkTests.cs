using System.Collections.Concurrent;
using System.Globalization;
using System.Reflection;
using System.Runtime.ExceptionServices;
using CSharpDB.Client;
using CSharpDB.Client.Grpc;
using CSharpDB.Client.Models;
using CSharpDB.Testing;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace CSharpDB.Data.Tests;

[Collection("ConnectionPoolState")]
public sealed class CompactScalarNetworkTests
{
    [Theory(Timeout = 45000)]
    [InlineData(CSharpDbGrpcMode.Auto, CSharpDbGrpcCallMode.Unary)]
    [InlineData(CSharpDbGrpcMode.Native, CSharpDbGrpcCallMode.Unary)]
    [InlineData(CSharpDbGrpcMode.Native, CSharpDbGrpcCallMode.Streaming)]
    public async Task ScalarsAndReaders_PreserveSignedLimitsAndDeclaredTypes(
        CSharpDbGrpcMode mode, CSharpDbGrpcCallMode calls)
    {
        var ct = TestContext.Current.CancellationToken;
        var capture = new CompactCapture();
        await using var host = Host(capture);
        string endpoint = host.EndpointFor(mode).AbsoluteUri;
        try
        {
            await using var connection = new CSharpDbConnection(ConnectionString(endpoint, mode, calls));
            await connection.OpenAsync(ct);
            using var command = connection.CreateCommand();
            foreach (long value in new[] { 0L, -1L, 1L, long.MinValue, long.MaxValue, 2147483648L })
            {
                command.CommandText = $"SELECT CAST({value.ToString(CultureInfo.InvariantCulture)} AS BIGINT) AS value";
                Assert.Equal(value, Assert.IsType<long>(await command.ExecuteScalarAsync(ct)));
                using var reader = await command.ExecuteReaderAsync(ct);
                Assert.Equal(typeof(long), reader.GetFieldType(0));
                Assert.True(await reader.ReadAsync(ct));
                Assert.Equal(value, reader.GetInt64(0));
                Assert.False(await reader.ReadAsync(ct));
            }
            foreach (int value in new[] { int.MinValue, 0, int.MaxValue })
            {
                command.CommandText = $"SELECT CAST({value.ToString(CultureInfo.InvariantCulture)} AS INTEGER) AS value";
                Assert.Equal(value, Assert.IsType<int>(await command.ExecuteScalarAsync(ct)));
                using var reader = await command.ExecuteReaderAsync(ct);
                Assert.Equal(typeof(int), reader.GetFieldType(0));
                Assert.True(await reader.ReadAsync(ct));
                Assert.Equal(value, reader.GetInt32(0));
            }
            var integerReplies = capture.Results.ToArray();
            Assert.Equal(18, integerReplies.Length);
            Assert.All(integerReplies, result => Assert.Equal(calls == CSharpDbGrpcCallMode.Streaming, result.HasScalarInt64));
            if (calls == CSharpDbGrpcCallMode.Streaming)
                Assert.All(integerReplies, result => Assert.Null(result.Rows));

            (string Sql, object Expected)[] fallback =
            [
                ("SELECT CAST(NULL AS INTEGER) AS value", DBNull.Value),
                ("SELECT CAST(1 AS BOOLEAN) AS value", true),
                ("SELECT CAST(0 AS BOOLEAN) AS value", false),
                ("SELECT CAST(12.34 AS DECIMAL(10,2)) AS value", 12.34m),
            ];
            foreach (var test in fallback)
            {
                command.CommandText = test.Sql;
                object? actual = await command.ExecuteScalarAsync(ct);
                Assert.Equal(test.Expected.GetType(), actual?.GetType());
                Assert.Equal(test.Expected, actual);
                Assert.False(capture.Results.Last().HasScalarInt64);
                Assert.NotNull(capture.Results.Last().Rows);
            }
        }
        finally { await CSharpDbClient.ClearPoolAsync(endpoint); }
    }

    [Theory(Timeout = 30000)]
    [InlineData(2147483648L, false)]
    [InlineData(0L, true)]
    [InlineData(1L, true)]
    public async Task CompactCarrier_PreservesCheckedIntegerAndBooleanConversion(long value, bool boolean)
    {
        var ct = TestContext.Current.CancellationToken;
        var capture = new CompactCapture();
        var response = new SqlExecutionResult
        {
            IsQuery = true, ColumnNames = ["value"], ColumnTypes = [boolean ? "BOOLEAN" : "INTEGER"],
            ColumnNullability = [false], Rows = [[value]],
            Columns = [new ColumnDefinition
            {
                Name = "value", Type = Client.Models.DbType.Integer, Nullable = false,
                DeclaredType = new() { Kind = boolean ? SqlTypeKind.Boolean : SqlTypeKind.Integer },
            }],
        };
        await using var host = Host(capture, services =>
        {
            var original = services.Last(service => service.ServiceType == typeof(ICSharpDbClient));
            services.Replace(ServiceDescriptor.Singleton<ICSharpDbClient>(provider =>
            {
                var proxy = DispatchProxy.Create<ICSharpDbClient, ControlledBackend>();
                var state = (ControlledBackend)(object)proxy;
                state.Inner = (ICSharpDbClient)original.ImplementationFactory!(provider);
                state.Result = response;
                return proxy;
            }));
        });
        string endpoint = host.NativeEndpoint.AbsoluteUri;
        try
        {
            await using var connection = new CSharpDbConnection(ConnectionString(endpoint, CSharpDbGrpcMode.Native, CSharpDbGrpcCallMode.Streaming));
            await connection.OpenAsync(ct);
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT controlled_carrier";
            if (boolean)
                Assert.Equal(value != 0, Assert.IsType<bool>(await command.ExecuteScalarAsync(ct)));
            else
                await Assert.ThrowsAsync<OverflowException>(() => command.ExecuteScalarAsync(ct));
            Assert.True(Assert.Single(capture.Results).HasScalarInt64);
        }
        finally { await CSharpDbClient.ClearPoolAsync(endpoint); }
    }

    private static string ConnectionString(string endpoint, CSharpDbGrpcMode mode, CSharpDbGrpcCallMode calls)
        => $"Transport=Grpc;Endpoint={endpoint};Grpc Mode={mode};Grpc Call Mode={calls};Pooling=true;Max Pool Size=1";

    private static GrpcSocketDaemonFixture Host(CompactCapture capture, Action<IServiceCollection>? configure = null)
        => new(configureServices: services =>
        {
            services.AddSingleton(capture);
            services.AddGrpc(options => options.Interceptors.Add<CompactCapture>());
            configure?.Invoke(services);
        });

    public class ControlledBackend : DispatchProxy
    {
        public ICSharpDbClient Inner = null!;
        public SqlExecutionResult Result = null!;
        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (targetMethod!.Name == nameof(ICSharpDbClient.ExecuteSqlAsync)) return Task.FromResult(Result);
            try { return targetMethod.Invoke(Inner, args); }
            catch (TargetInvocationException error) when (error.InnerException is not null)
            { ExceptionDispatchInfo.Capture(error.InnerException).Throw(); throw; }
        }
    }

    public sealed class CompactCapture : Interceptor
    {
        public ConcurrentQueue<SqlExecutionResultMessage> Results { get; } = new();
        public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(TRequest request,
            ServerCallContext context, UnaryServerMethod<TRequest, TResponse> continuation)
        {
            TResponse response = await continuation(request, context);
            if (response is SqlExecutionResultMessage result) Results.Enqueue(result.Clone());
            return response;
        }
        public override Task DuplexStreamingServerHandler<TRequest, TResponse>(IAsyncStreamReader<TRequest> reader,
            IServerStreamWriter<TResponse> writer, ServerCallContext context,
            DuplexStreamingServerMethod<TRequest, TResponse> continuation)
            => continuation(reader, new Writer<TResponse>(writer, this), context);

        private sealed class Writer<T>(IServerStreamWriter<T> inner, CompactCapture owner) : IServerStreamWriter<T>
        {
            public WriteOptions? WriteOptions { get => inner.WriteOptions; set => inner.WriteOptions = value; }
            public Task WriteAsync(T response)
            {
                if (response is CommandSessionResponse { Result: not null } message) owner.Results.Enqueue(message.Result.Clone());
                return inner.WriteAsync(response);
            }
        }
    }
}
