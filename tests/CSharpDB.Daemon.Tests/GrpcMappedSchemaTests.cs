using System.Collections.Concurrent;
using System.Reflection;
using CSharpDB.Client;
using CSharpDB.Client.Grpc;
using CSharpDB.Client.Internal;
using CSharpDB.Client.Models;
using CSharpDB.Testing;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using ModelColumn = CSharpDB.Client.Models.ColumnDefinition;
using ModelType = CSharpDB.Client.Models.DbType;
using ModelSqlKind = CSharpDB.Client.Models.SqlTypeKind;

namespace CSharpDB.Daemon.Tests;

public sealed class GrpcMappedSchemaTests
{
    [Theory(Timeout = 30000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DelayedReferenceMapping_UsesItsCapturedSchema_AfterNewProjectionAndCacheClear(bool compact)
    {
        var ct = TestContext.Current.CancellationToken;
        var oldDefinition = Definition("old_value");
        var newDefinition = Definition("new_value");
        newDefinition.Columns[0].DeclaredType.Kind = SqlTypeKindEnum.SqlTypeKindVarChar;
        newDefinition.Columns[0].DeclaredType.Length = 73;
        var delayedFrame = Reference(1, 42);
        if (compact) { delayedFrame.Result.Rows = null; delayedFrame.Result.ScalarInt64 = 42; }
        var peer = new ScriptedPeer(
            Full(1, oldDefinition), delayedFrame, Full(2, newDefinition),
            new CommandSessionResponse { Info = new DatabaseInfoMessage { DataSource = "cleared" } });
        await using var session = new GrpcCommandSession(new ScriptedInvoker(peer));
        _ = await session.ExecuteSqlAsync(Query(), ct);
        var delayed = await session.ExecuteSqlAsync(Query(), ct);
        AssertRawMetadataAbsent(delayed.Response);
        Assert.NotNull(delayed.Metadata);
        var newer = await session.ExecuteSqlAsync(Query(), ct);
        Assert.Equal("new_value", Assert.Single(GrpcModelMapper.ToSessionModel(newer.Response, newer.Metadata).ColumnNames!));
        _ = await session.ExecuteAsync<DatabaseInfoMessage>(new() { GetInfo = new Empty() }, ct);

        // Mapping is deliberately delayed until both the replacement and clear have
        // completed. It must not consult the session's current schema at this point.
        var mapped = GrpcModelMapper.ToSessionModel(delayed.Response, delayed.Metadata);
        AssertMetadataEqual(GrpcModelMapper.ToModel(oldDefinition), mapped);
        Assert.Equal(42L, Assert.Single(Assert.Single(mapped.Rows!)));
        AssertRawMetadataAbsent(delayed.Response);
        Assert.Equal(3, peer.SqlWrites);
    }

    [Fact(Timeout = 30000)]
    public async Task CanonicalSchema_IsPrivate_FromWireMutationAndEveryPublicMetadataObject()
    {
        var ct = TestContext.Current.CancellationToken;
        var definition = Definition("safe_value");
        var expected = GrpcModelMapper.ToModel(definition.Clone());
        var peer = new ScriptedPeer(Full(1, definition), Reference(1, 2), Reference(1, 3));
        await using var session = new GrpcCommandSession(new ScriptedInvoker(peer));
        var firstReply = await session.ExecuteSqlAsync(Query(), ct);
        var first = GrpcModelMapper.ToSessionModel(firstReply.Response, firstReply.Metadata);
        var secondReply = await session.ExecuteSqlAsync(Query(), ct);
        var second = GrpcModelMapper.ToSessionModel(secondReply.Response, secondReply.Metadata);
        AssertFreshMetadata(first, second);

        // The received full protobuf and public init-only DTOs are both mutable
        // through ordinary reflection. Neither is the canonical cache storage.
        firstReply.Response.ColumnNames.Values[0] = "wire_poison";
        firstReply.Response.Columns[0].Name = "wire_poison";
        firstReply.Response.Columns[0].DeclaredType.Length = 999;
        Poison(first);
        AssertMetadataEqual(expected, second);
        var thirdReply = await session.ExecuteSqlAsync(Query(), ct);
        var third = GrpcModelMapper.ToSessionModel(thirdReply.Response, thirdReply.Metadata);
        AssertMetadataEqual(expected, third);
        AssertFreshMetadata(second, third);
        AssertRawMetadataAbsent(thirdReply.Response);
        Assert.Equal(3, peer.SqlWrites);
    }

    [Theory(Timeout = 30000)]
    [InlineData("absent")]
    [InlineData("empty")]
    [InlineData("mixed")]
    [InlineData("legacy-column")]
    [InlineData("all-facets")]
    public async Task FullAndReferencedMetadata_PreserveCanonicalMappingExactly(string shape)
    {
        var ct = TestContext.Current.CancellationToken;
        var definition = Definition("value");
        switch (shape)
        {
            case "absent":
                definition.ColumnNames = null; definition.ColumnTypes = null;
                definition.ColumnNullability = null; definition.Columns.Clear(); break;
            case "empty":
                definition.ColumnNames = new StringList(); definition.ColumnTypes = new StringList();
                definition.ColumnNullability = new BoolList(); definition.Columns.Clear(); break;
            case "mixed":
                definition.ColumnNames = new StringList(); definition.ColumnTypes = null;
                definition.ColumnNullability = new BoolList(); definition.Columns.Clear(); break;
            case "legacy-column": definition.Columns[0].DeclaredType = null; break;
            case "all-facets":
                // Existing mapping copies facets without applying additional SQL
                // validation. The cache must preserve that same wire contract.
                definition.Columns[0].DeclaredType = new()
                {
                    Kind = SqlTypeKindEnum.SqlTypeKindDecimal, Length = -7,
                    Precision = 31, Scale = 12, FractionalSecondsPrecision = 8,
                };
                break;
        }
        var expected = GrpcModelMapper.ToModel(definition.Clone());
        var peer = new ScriptedPeer(Full(1, definition), Reference(1, 7));
        await using var session = new GrpcCommandSession(new ScriptedInvoker(peer));
        var first = await session.ExecuteSqlAsync(Query(), ct);
        var repeat = await session.ExecuteSqlAsync(Query(), ct);
        var firstModel = GrpcModelMapper.ToSessionModel(first.Response, first.Metadata);
        var repeatedModel = GrpcModelMapper.ToSessionModel(repeat.Response, repeat.Metadata);
        AssertMetadataEqual(expected, firstModel);
        AssertMetadataEqual(expected, repeatedModel);
        AssertRawMetadataAbsent(repeat.Response);
        if (shape == "legacy-column")
        {
            Assert.Null(Assert.Single(repeatedModel.Columns!).DeclaredType);
            Assert.Equal(ModelSqlKind.BigInt, repeatedModel.Columns![0].EffectiveType.Kind);
        }
        if (firstModel.ColumnNames is not null) Assert.NotSame(firstModel.ColumnNames, repeatedModel.ColumnNames);
        if (firstModel.ColumnTypes is not null) Assert.NotSame(firstModel.ColumnTypes, repeatedModel.ColumnTypes);
        if (firstModel.ColumnNullability is not null) Assert.NotSame(firstModel.ColumnNullability, repeatedModel.ColumnNullability);
    }

    [Fact(Timeout = 30000)]
    public async Task GenericWireApi_RestoresMetadata_WithoutExposingCanonicalMappedObjects()
    {
        var ct = TestContext.Current.CancellationToken;
        var definition = Definition("generic_value");
        var expected = GrpcModelMapper.ToModel(definition.Clone());
        var peer = new ScriptedPeer(Full(1, definition), Reference(1, 2), Reference(1, 3));
        await using var session = new GrpcCommandSession(new ScriptedInvoker(peer));
        // Exercise lazy canonical construction too: both initial commands use
        // the generic wire API before any direct SQL mapping is requested.
        _ = await session.ExecuteAsync<SqlExecutionResultMessage>(Query(), ct);
        var generic = await session.ExecuteAsync<SqlExecutionResultMessage>(Query(), ct);
        Assert.NotNull(generic.ColumnNames);
        Assert.Single(generic.Columns);
        AssertMetadataEqual(expected, GrpcModelMapper.ToModel(generic));
        generic.ColumnNames!.Values[0] = "generic_poison";
        generic.Columns[0].Name = "generic_poison";
        generic.Columns[0].DeclaredType.Length = 999;
        var direct = await session.ExecuteSqlAsync(Query(), ct);
        AssertRawMetadataAbsent(direct.Response);
        AssertMetadataEqual(expected, GrpcModelMapper.ToSessionModel(direct.Response, direct.Metadata));
        Assert.Equal(3, peer.SqlWrites);
    }

    [Theory(Timeout = 30000)]
    [InlineData("guid")]
    [InlineData("physical-enum")]
    [InlineData("declared-enum")]
    public async Task InvalidNewFullMetadata_PreservesCanonicalException_ClosesSessionWithoutReplay(string invalid)
    {
        var ct = TestContext.Current.CancellationToken;
        var malformed = Definition("bad_value");
        switch (invalid)
        {
            case "guid": malformed.Columns[0].SchemaId = "not-a-guid"; break;
            case "physical-enum": malformed.Columns[0].Type = (DbTypeEnum)999; break;
            case "declared-enum": malformed.Columns[0].DeclaredType.Kind = (SqlTypeKindEnum)999; break;
        }
        var expected = Record.Exception(() => GrpcModelMapper.ToModel(malformed.Clone()));
        Assert.NotNull(expected);
        Assert.Equal(invalid == "guid" ? typeof(CSharpDbClientException) : typeof(ArgumentOutOfRangeException), expected.GetType());
        var peer = new ScriptedPeer(Full(1, Definition("good_value")), Reference(1, 2), Full(2, malformed));
        await using var session = new GrpcCommandSession(new ScriptedInvoker(peer));
        _ = await session.ExecuteSqlAsync(Query(), ct);
        _ = await session.ExecuteSqlAsync(Query(), ct);
        var actual = await Record.ExceptionAsync(async () => { _ = await session.ExecuteSqlAsync(Query(), ct); });
        Assert.NotNull(actual);
        Assert.Equal(expected.GetType(), actual.GetType());
        Assert.Equal(expected.Message, actual.Message);
        var closed = await Assert.ThrowsAsync<RpcException>(async () => { _ = await session.ExecuteSqlAsync(Query(), ct); });
        Assert.Equal(StatusCode.Unavailable, closed.StatusCode);
        Assert.Equal(3, peer.SqlWrites);
        Assert.Equal(1, peer.CallStarts);
        Assert.True(peer.Disposals > 0);
    }

    [Fact(Timeout = 30000)]
    public async Task CacheClear_RequiresNewDefinition_AndNewSessionStartsWithItsOwnSchema()
    {
        var ct = TestContext.Current.CancellationToken;
        var peer = new ScriptedPeer(Full(1, Definition("before_clear")),
            new CommandSessionResponse { Info = new DatabaseInfoMessage() }, Reference(1, 9));
        await using (var session = new GrpcCommandSession(new ScriptedInvoker(peer)))
        {
            _ = await session.ExecuteSqlAsync(Query(), ct);
            _ = await session.ExecuteAsync<DatabaseInfoMessage>(new() { GetInfo = new Empty() }, ct);
            var error = await Assert.ThrowsAsync<RpcException>(async () => { _ = await session.ExecuteSqlAsync(Query(), ct); });
            Assert.Equal(StatusCode.DataLoss, error.StatusCode);
        }
        var reconnected = new ScriptedPeer(Full(1, Definition("after_reconnect")), Reference(1, 4));
        await using var fresh = new GrpcCommandSession(new ScriptedInvoker(reconnected));
        _ = await fresh.ExecuteSqlAsync(Query(), ct);
        var response = await fresh.ExecuteSqlAsync(Query(), ct);
        Assert.Equal("after_reconnect", Assert.Single(GrpcModelMapper.ToSessionModel(response.Response, response.Metadata).ColumnNames!));
        Assert.Equal(1, peer.CallStarts);
        Assert.Equal(1, reconnected.CallStarts);
    }

    [Fact(Timeout = 30000)]
    public async Task SocketPoolReuseAndReconnect_DoNotRetainCallerReflectionMutations()
    {
        var ct = TestContext.Current.CancellationToken;
        var capture = new GrpcResultSchemaReuseTests.SessionInterceptor();
        await using var host = new GrpcSocketDaemonFixture(configureServices: services =>
        {
            services.AddSingleton(capture);
            services.AddGrpc(options => options.Interceptors.Add<GrpcResultSchemaReuseTests.SessionInterceptor>());
        });
        var options = new CSharpDbClientOptions
        {
            Transport = CSharpDbTransport.Grpc, GrpcMode = CSharpDbGrpcMode.Native,
            GrpcCallMode = CSharpDbGrpcCallMode.Streaming, Endpoint = host.NativeEndpoint.AbsoluteUri,
        };
        const string sql = "SELECT CAST(42 AS INTEGER) AS mapped_value";
        try
        {
            SqlExecutionResult first, expected;
            await using (var rental = await CSharpDbClient.RentAsync(options, 1, ct))
            {
                first = await rental.ExecuteSqlAsync(sql, ct);
                expected = await rental.ExecuteSqlAsync(sql, ct);
                AssertFreshMetadata(first, expected);
                Poison(first);
            }
            await using (var rental = await CSharpDbClient.RentAsync(options, 1, ct))
            {
                var reused = await rental.ExecuteSqlAsync(sql, ct);
                AssertMetadataEqual(expected, reused);
                AssertFreshMetadata(expected, reused);
                Assert.Equal(42L, Assert.Single(Assert.Single(reused.Rows!)));
            }
            var beforeReconnect = capture.Responses.Where(r => r.Result is not null).ToArray();
            Assert.Equal(3, beforeReconnect.Length);
            Assert.False(beforeReconnect[0].ResultSchemaReference);
            Assert.All(beforeReconnect.Skip(1), response => Assert.True(response.ResultSchemaReference));
            await CSharpDbClient.ClearPoolAsync(options.Endpoint!);
            await using var reconnected = await CSharpDbClient.RentAsync(options, 1, ct);
            var fresh = await reconnected.ExecuteSqlAsync(sql, ct);
            AssertMetadataEqual(expected, fresh);
            AssertFreshMetadata(expected, fresh);
            var last = capture.Responses.Last(r => r.Result is not null);
            Assert.False(last.ResultSchemaReference);
            Assert.Equal(1UL, last.ResultSchemaId);
        }
        finally { await CSharpDbClient.ClearPoolAsync(options.Endpoint!); }
    }

    private static void Poison(SqlExecutionResult result)
    {
        var column = result.Columns![0];
        typeof(ModelColumn).GetProperty(nameof(ModelColumn.Name))!.SetValue(column, "reflection_poison");
        typeof(ModelColumn).GetProperty(nameof(ModelColumn.SchemaId))!.SetValue(column, Guid.NewGuid());
        typeof(SqlTypeDescriptor).GetProperty(nameof(SqlTypeDescriptor.Kind))!.SetValue(column.DeclaredType, ModelSqlKind.Blob);
        typeof(SqlTypeDescriptor).GetProperty(nameof(SqlTypeDescriptor.Length))!.SetValue(column.DeclaredType, 999);
        result.ColumnNames![0] = "array_poison";
        result.ColumnTypes![0] = "BLOB";
        result.ColumnNullability![0] = !result.ColumnNullability[0];
        result.Columns[0] = new ModelColumn { Name = "replacement", Type = ModelType.Blob };
    }

    private static void AssertFreshMetadata(SqlExecutionResult first, SqlExecutionResult second)
    {
        Assert.NotSame(first.ColumnNames, second.ColumnNames);
        Assert.NotSame(first.ColumnTypes, second.ColumnTypes);
        Assert.NotSame(first.ColumnNullability, second.ColumnNullability);
        Assert.NotSame(first.Columns, second.Columns);
        Assert.NotSame(first.Columns![0], second.Columns![0]);
        Assert.NotSame(first.Columns[0].DeclaredType, second.Columns[0].DeclaredType);
    }

    private static void AssertMetadataEqual(SqlExecutionResult expected, SqlExecutionResult actual)
    {
        Assert.Equal(expected.ColumnNames, actual.ColumnNames);
        Assert.Equal(expected.ColumnTypes, actual.ColumnTypes);
        Assert.Equal(expected.ColumnNullability, actual.ColumnNullability);
        if (expected.Columns is null) { Assert.Null(actual.Columns); return; }
        Assert.NotNull(actual.Columns);
        Assert.Equal(expected.Columns.Length, actual.Columns.Length);
        for (int i = 0; i < expected.Columns.Length; i++)
        {
            var wanted = expected.Columns[i]; var got = actual.Columns[i];
            Assert.Equal(wanted.SchemaId, got.SchemaId); Assert.Equal(wanted.Name, got.Name);
            Assert.Equal(wanted.Type, got.Type); Assert.Equal(wanted.Nullable, got.Nullable);
            Assert.Equal(wanted.IsPrimaryKey, got.IsPrimaryKey); Assert.Equal(wanted.IsIdentity, got.IsIdentity);
            Assert.Equal(wanted.IsRowVersion, got.IsRowVersion); Assert.Equal(wanted.Collation, got.Collation);
            Assert.Equal(wanted.DefaultSql, got.DefaultSql);
            if (wanted.DeclaredType is null) { Assert.Null(got.DeclaredType); continue; }
            Assert.NotNull(got.DeclaredType);
            Assert.Equal(wanted.DeclaredType.Kind, got.DeclaredType.Kind);
            Assert.Equal(wanted.DeclaredType.Length, got.DeclaredType.Length);
            Assert.Equal(wanted.DeclaredType.Precision, got.DeclaredType.Precision);
            Assert.Equal(wanted.DeclaredType.Scale, got.DeclaredType.Scale);
            Assert.Equal(wanted.DeclaredType.FractionalSecondsPrecision, got.DeclaredType.FractionalSecondsPrecision);
        }
    }

    private static void AssertRawMetadataAbsent(SqlExecutionResultMessage result)
    {
        Assert.Null(result.ColumnNames); Assert.Null(result.ColumnTypes);
        Assert.Null(result.ColumnNullability); Assert.Empty(result.Columns);
    }

    private static CommandSessionRequest Query() => new() { ExecuteSql = new() { Sql = "SELECT scripted_metadata" } };
    private static CommandSessionResponse Full(ulong id, SqlExecutionResultMessage result) => new() { ResultSchemaId = id, Result = result };
    private static CommandSessionResponse Reference(ulong id, long value) => new()
    {
        ResultSchemaId = id, ResultSchemaReference = true,
        Result = new SqlExecutionResultMessage
        {
            IsQuery = true, Elapsed = new Duration(), Rows = GrpcModelMapper.ToVariantArrayList([new object?[] { value }]),
        },
    };

    private static SqlExecutionResultMessage Definition(string name)
    {
        var result = new SqlExecutionResultMessage
        {
            IsQuery = true, Elapsed = new Duration(), ColumnNames = GrpcModelMapper.ToStringList([name]),
            ColumnTypes = GrpcModelMapper.ToStringList(["BIGINT"]), ColumnNullability = GrpcModelMapper.ToBoolList([true]),
            Rows = GrpcModelMapper.ToVariantArrayList([new object?[] { 1L }]),
        };
        result.Columns.Add(new ColumnDefinitionMessage
        {
            SchemaId = "12345678-1234-5678-9012-123456789012", Name = name, Type = DbTypeEnum.DbTypeInteger,
            Nullable = true, IsPrimaryKey = true, IsIdentity = true, IsRowVersion = true,
            Collation = "NOCASE", DefaultSql = "42",
            DeclaredType = new() { Kind = SqlTypeKindEnum.SqlTypeKindBigInt, Length = 23 },
        });
        return result;
    }

    private sealed class ScriptedPeer(params CommandSessionResponse[] responses)
    {
        private readonly Queue<CommandSessionResponse> _script = new(responses);
        private readonly ConcurrentQueue<CommandSessionResponse> _received = new();
        public int SqlWrites, CallStarts, Disposals;
        public bool Completed;
        public void Write(CommandSessionRequest request)
        {
            if (request.Hello is not null)
            {
                _received.Enqueue(new() { Ready = new() { ProtocolVersion = 1, MaxInFlight = 1,
                    Capabilities = { "sql", "transactions", "result-schema-ref-v1", "single-int64-result-v1" } } });
                return;
            }
            if (request.ExecuteSql is not null) SqlWrites++;
            var response = _script.Dequeue();
            response.RequestId = request.RequestId;
            _received.Enqueue(response);
        }
        public bool Read(out CommandSessionResponse response) => _received.TryDequeue(out response!);
    }

    private sealed class ScriptedInvoker(ScriptedPeer peer) : CallInvoker
    {
        public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options)
        {
            peer.CallStarts++;
            return new((IClientStreamWriter<TRequest>)(object)new Writer(peer),
                (IAsyncStreamReader<TResponse>)(object)new Reader(peer), Task.FromResult(new Metadata()),
                () => Status.DefaultSuccess, () => new Metadata(), () => peer.Disposals++);
        }
        public override TResponse BlockingUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) => throw new NotSupportedException();
        public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) => throw new NotSupportedException();
        public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) => throw new NotSupportedException();
        public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options) => throw new NotSupportedException();
        private sealed class Writer(ScriptedPeer state) : IClientStreamWriter<CommandSessionRequest>
        {
            public WriteOptions? WriteOptions { get; set; }
            public Task WriteAsync(CommandSessionRequest message) { state.Write(message); return Task.CompletedTask; }
            public Task WriteAsync(CommandSessionRequest message, CancellationToken ct)
            {
                ct.ThrowIfCancellationRequested();
                return WriteAsync(message);
            }
            public Task CompleteAsync() { state.Completed = true; return Task.CompletedTask; }
        }
        private sealed class Reader(ScriptedPeer state) : IAsyncStreamReader<CommandSessionResponse>
        {
            public CommandSessionResponse Current { get; private set; } = null!;
            public Task<bool> MoveNext(CancellationToken ct)
            {
                ct.ThrowIfCancellationRequested();
                if (state.Read(out var next)) { Current = next; return Task.FromResult(true); }
                if (!state.Completed) throw new InvalidOperationException("The scripted response was not queued by its request.");
                return Task.FromResult(false);
            }
        }
    }
}
