using System.Collections.Concurrent;
using System.Reflection;
using CSharpDB.Client;
using CSharpDB.Client.Grpc;
using CSharpDB.Client.Internal;
using CSharpDB.Client.Models;
using CSharpDB.Testing;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.DependencyInjection;
using ModelColumn = CSharpDB.Client.Models.ColumnDefinition;
using ModelKind = CSharpDB.Client.Models.SqlTypeKind;
using ModelType = CSharpDB.Client.Models.DbType;

namespace CSharpDB.Daemon.Tests;

public sealed class GrpcPrivateScalarProjectionTests
{
    [Theory]
    [InlineData(0L, "integer")]
    [InlineData(-1L, "bigint")]
    [InlineData(long.MinValue, "none")]
    [InlineData(long.MaxValue, "facets")]
    public void CompactProjection_PreservesCarrierAndEveryDeclaredFacet(long value, string declared)
    {
        var definition = Definition("value");
        if (declared == "none") definition.Columns[0].DeclaredType = null;
        else if (declared == "bigint")
            definition.Columns[0].DeclaredType.Kind = SqlTypeKindEnum.SqlTypeKindBigInt;
        else if (declared == "facets")
        {
            // SDK metadata mapping copies these values; ADO performs its existing
            // facet validation later. Projection must neither normalize nor drop them.
            definition.Columns[0].DeclaredType = new()
            {
                Kind = SqlTypeKindEnum.SqlTypeKindDecimal, Length = -7,
                Precision = 31, Scale = 12, FractionalSecondsPrecision = 8,
            };
        }
        var metadata = new GrpcSessionResultMetadata(definition);
        var response = Compact(value);
        var projected = GrpcTransportClient.MapScalarProjectionReply(new(response, metadata));
        var sdk = GrpcModelMapper.ToSessionModel(response, metadata);

        Assert.True(projected.HasCompactInt64);
        Assert.Equal(value, projected.Int64);
        Assert.Null(projected.Fallback);
        Assert.Equal(value, Assert.IsType<long>(Assert.Single(Assert.Single(sdk.Rows!))));
        var expectedType = sdk.Columns![0].DeclaredType;
        if (expectedType is null) Assert.Null(projected.DeclaredType);
        else
        {
            ScalarTypeSnapshot actualType = Assert.IsType<ScalarTypeSnapshot>(projected.DeclaredType);
            Assert.Equal(new ScalarTypeSnapshot(expectedType.Kind, expectedType.Length,
                expectedType.Precision, expectedType.Scale, expectedType.FractionalSecondsPrecision), actualType);
        }
    }

    [Theory]
    [InlineData("ordinary-long")]
    [InlineData("null")]
    [InlineData("no-rows")]
    [InlineData("wide")]
    [InlineData("nonquery")]
    [InlineData("error")]
    [InlineData("uncached-full")]
    [InlineData("names-only")]
    [InlineData("columns-only")]
    public void UnsupportedProjection_MapsTheAlreadyReceivedReply_WithSdkSemantics(string shape)
    {
        var definition = Definition("value");
        var response = Compact(42);
        bool capture = true;
        switch (shape)
        {
            case "ordinary-long": response.ClearScalarInt64(); response.Rows = Rows(42L); break;
            case "null": response.ClearScalarInt64(); response.Rows = Rows(null); break;
            case "no-rows": response.ClearScalarInt64(); response.Rows = new VariantArrayList(); break;
            case "wide":
                definition.ColumnNames = GrpcModelMapper.ToStringList(["value", "ignored"]);
                definition.ColumnTypes = GrpcModelMapper.ToStringList(["INTEGER", "TEXT"]);
                definition.ColumnNullability = GrpcModelMapper.ToBoolList([true, true]);
                definition.Columns.Add(new ColumnDefinitionMessage
                {
                    Name = "ignored", Type = DbTypeEnum.DbTypeText,
                    DeclaredType = new() { Kind = SqlTypeKindEnum.SqlTypeKindText },
                });
                response.ClearScalarInt64();
                response.Rows = GrpcModelMapper.ToVariantArrayList([new object?[] { 42L, "second" }]);
                break;
            case "nonquery":
                response = new() { IsQuery = false, RowsAffected = 3, Elapsed = new Duration() };
                capture = false;
                break;
            case "error":
                response = new() { IsQuery = false, Error = "SQL failed", Elapsed = new Duration() };
                capture = false;
                break;
            case "uncached-full":
                response = definition.Clone(); response.Rows = null; response.ScalarInt64 = 42;
                capture = false;
                break;
            case "names-only": definition.Columns.Clear(); break;
            case "columns-only": definition.ColumnNames = null; break;
        }
        GrpcSessionResultMetadata? metadata = capture ? new(definition) : null;
        var expected = GrpcModelMapper.ToSessionModel(response, metadata);
        var projected = GrpcTransportClient.MapScalarProjectionReply(new(response, metadata));

        Assert.False(projected.HasCompactInt64);
        Assert.Null(projected.DeclaredType);
        Assert.NotNull(projected.Fallback);
        AssertResultEqual(expected, projected.Fallback);
    }

    [Theory]
    [InlineData("compact-with-rows")]
    [InlineData("compact-nonquery")]
    [InlineData("compact-error")]
    [InlineData("compact-width")]
    [InlineData("compact-duration")]
    [InlineData("ordinary-duration")]
    [InlineData("missing-duration")]
    public void MalformedReply_PreservesSdkException_IncludingDurationValidation(string malformed)
    {
        var definition = Definition("value");
        var response = Compact(42);
        switch (malformed)
        {
            case "compact-with-rows": response.Rows = new VariantArrayList(); break;
            case "compact-nonquery": response.IsQuery = false; break;
            case "compact-error": response.Error = ""; break;
            case "compact-width":
                definition.ColumnNames = GrpcModelMapper.ToStringList(["value", "other"]); break;
            case "compact-duration": response.Elapsed = new Duration { Seconds = 1, Nanos = -1 }; break;
            case "ordinary-duration":
                response.ClearScalarInt64(); response.Rows = Rows(42L);
                response.Elapsed = new Duration { Seconds = 1, Nanos = -1 }; break;
            case "missing-duration": response.Elapsed = null; break;
        }
        var metadata = new GrpcSessionResultMetadata(definition);
        Exception? expected = Record.Exception(() => GrpcModelMapper.ToSessionModel(response, metadata));
        Exception? actual = Record.Exception(() => GrpcTransportClient.MapScalarProjectionReply(new(response, metadata)));
        Assert.NotNull(expected);
        Assert.NotNull(actual);
        Assert.Equal(expected.GetType(), actual.GetType());
        Assert.Equal(expected.Message, actual.Message);
    }

    [Fact(Timeout = 30000)]
    public async Task DelayedPrivateProjection_UsesCapturedType_AfterPublicMutationNewSchemaAndClear()
    {
        var ct = TestContext.Current.CancellationToken;
        var oldDefinition = Definition("old_value");
        var newDefinition = Definition("new_value");
        newDefinition.Columns[0].DeclaredType.Kind = SqlTypeKindEnum.SqlTypeKindBigInt;
        var peer = new ScriptedPeer(Full(1, oldDefinition), Reference(1, 42), Reference(1, 43),
            Full(2, newDefinition), new() { Info = new DatabaseInfoMessage() });
        await using var session = new GrpcCommandSession(new ScriptedInvoker(peer));
        var first = await session.ExecuteSqlAsync(Query(), ct);
        SqlExecutionResult publicResult = GrpcModelMapper.ToSessionModel(first.Response, first.Metadata);
        var delayed = await session.ExecuteSqlAsync(Query(), ct);
        var repeated = await session.ExecuteSqlAsync(Query(), ct);
        var newer = await session.ExecuteSqlAsync(Query(), ct);
        _ = await session.ExecuteAsync<DatabaseInfoMessage>(new() { GetInfo = new Empty() }, ct);

        publicResult.ColumnNames![0] = "caller_array";
        typeof(ModelColumn).GetProperty(nameof(ModelColumn.Name))!.SetValue(publicResult.Columns![0], "caller_column");
        SqlTypeDescriptor publicType = publicResult.Columns[0].DeclaredType!;
        typeof(SqlTypeDescriptor).GetProperty(nameof(SqlTypeDescriptor.Kind))!.SetValue(publicType, ModelKind.Blob);
        typeof(SqlTypeDescriptor).GetProperty(nameof(SqlTypeDescriptor.Length))!.SetValue(publicType, 99);
        typeof(SqlTypeDescriptor).GetProperty(nameof(SqlTypeDescriptor.Precision))!.SetValue(publicType, 17);
        typeof(SqlTypeDescriptor).GetProperty(nameof(SqlTypeDescriptor.Scale))!.SetValue(publicType, 4);
        typeof(SqlTypeDescriptor).GetProperty(nameof(SqlTypeDescriptor.FractionalSecondsPrecision))!.SetValue(publicType, 6);
        publicResult.Columns[0] = new() { Name = "replacement", Type = ModelType.Blob };
        first.Response.Columns[0].DeclaredType.Kind = SqlTypeKindEnum.SqlTypeKindText;

        var oldProjection = GrpcTransportClient.MapScalarProjectionReply(delayed);
        var repeatedProjection = GrpcTransportClient.MapScalarProjectionReply(repeated);
        var newProjection = GrpcTransportClient.MapScalarProjectionReply(newer);
        Assert.True(oldProjection.HasCompactInt64);
        Assert.Equal(42L, oldProjection.Int64);
        Assert.Equal(new ScalarTypeSnapshot(ModelKind.Integer, null, null, null, null), oldProjection.DeclaredType);
        Assert.True(repeatedProjection.HasCompactInt64);
        Assert.Equal(43L, repeatedProjection.Int64);
        Assert.Equal(new ScalarTypeSnapshot(ModelKind.Integer, null, null, null, null), repeatedProjection.DeclaredType);
        Assert.True(newProjection.HasCompactInt64);
        Assert.Equal(1L, newProjection.Int64);
        Assert.Equal(new ScalarTypeSnapshot(ModelKind.BigInt, null, null, null, null), newProjection.DeclaredType);
        Assert.Null(delayed.Response.ColumnNames);
        Assert.Empty(delayed.Response.Columns);
        Assert.Null(repeated.Response.ColumnNames);
        Assert.Empty(repeated.Response.Columns);

        // Reusing immutable schema data must not cache per-response validation.
        var invalidDuration = Compact(44);
        invalidDuration.Elapsed = new Duration { Seconds = 1, Nanos = -1 };
        Exception? expected = Record.Exception(() => GrpcModelMapper.ToSessionModel(invalidDuration, delayed.Metadata));
        Exception? actual = Record.Exception(() => GrpcTransportClient.MapScalarProjectionReply(new(invalidDuration, delayed.Metadata)));
        Assert.NotNull(expected);
        Assert.NotNull(actual);
        Assert.Equal(expected.GetType(), actual.GetType());
        Assert.Equal(expected.Message, actual.Message);
        var validAgain = GrpcTransportClient.MapScalarProjectionReply(new(Compact(45), delayed.Metadata));
        Assert.True(validAgain.HasCompactInt64);
        Assert.Equal(45L, validAgain.Int64);
        Assert.Equal(new ScalarTypeSnapshot(ModelKind.Integer, null, null, null, null), validAgain.DeclaredType);
        Assert.Equal(4, peer.SqlWrites);
    }

    [Fact(Timeout = 30000)]
    public async Task RealRental_UsesTransactionOwnershipAndRollbackCleanup_AndRejectsStaleRental()
    {
        var ct = TestContext.Current.CancellationToken;
        var capture = new ProjectionInterceptor();
        await using var host = Host(capture);
        var options = Options(host);
        try
        {
            var rental = await CSharpDbClient.RentAsync(options, 1, ct);
            try
            {
                await rental.ExecuteSqlAsync("CREATE TABLE scalar_private (id INTEGER PRIMARY KEY, value INTEGER)", ct);
                var scalar = Assert.IsAssignableFrom<IScalarProjectionClient>(rental);
                Assert.True(scalar.SupportsScalarProjection);
                var transaction = await rental.BeginTransactionAsync(ct);
                await rental.ExecuteInTransactionAsync(transaction.TransactionId,
                    "INSERT INTO scalar_private VALUES (1, 42)", ct);
                var result = await scalar.ExecuteScalarProjectionAsync(
                    "SELECT value FROM scalar_private WHERE id = 1", transaction.TransactionId, ct);
                Assert.True(result.HasCompactInt64);
                Assert.Equal(42L, result.Int64);
                Assert.Equal(ModelKind.Integer, Assert.IsType<ScalarTypeSnapshot>(result.DeclaredType).Kind);
                int commands = capture.SqlCommandCount;
                await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    await scalar.ExecuteScalarProjectionAsync("SELECT 99", "not-owned", ct));
                Assert.Equal(commands, capture.SqlCommandCount);
                await rental.DisposeAsync(); // The active transaction must be rolled back before owner reuse.
                await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
                    await scalar.ExecuteScalarProjectionAsync("SELECT 99", null, ct));
                Assert.Equal(commands, capture.SqlCommandCount);
            }
            finally { await rental.DisposeAsync(); }

            await using var next = await CSharpDbClient.RentAsync(options, 1, ct);
            var sdk = await next.ExecuteSqlAsync("SELECT value FROM scalar_private WHERE id = 1", ct);
            Assert.Empty(sdk.Rows!);
            var projected = await Assert.IsAssignableFrom<IScalarProjectionClient>(next)
                .ExecuteScalarProjectionAsync("SELECT CAST(7 AS BIGINT) AS value", null, ct);
            Assert.True(projected.HasCompactInt64);
            Assert.Equal(7L, projected.Int64);
            Assert.Equal(ModelKind.BigInt, Assert.IsType<ScalarTypeSnapshot>(projected.DeclaredType).Kind);
            Assert.Single(capture.Requests, r => r.RollbackTransaction is not null);
            Assert.Single(capture.Requests, r => r.ExecuteInTransaction?.Sql.StartsWith("SELECT value", StringComparison.Ordinal) == true);
        }
        finally { await CSharpDbClient.ClearPoolAsync(options.Endpoint!); }
    }

    [Fact(Timeout = 30000)]
    public async Task RealRental_QueuedCancellationSendsNothing_AndDisposeDrainsActiveProjection()
    {
        var ct = TestContext.Current.CancellationToken;
        var capture = new ProjectionInterceptor(blockFirstResult: true);
        await using var host = Host(capture);
        var options = Options(host);
        try
        {
            var rental = await CSharpDbClient.RentAsync(options, 1, ct);
            try
            {
                var scalar = Assert.IsAssignableFrom<IScalarProjectionClient>(rental);
                Task<ScalarProjectionResult> active = scalar.ExecuteScalarProjectionAsync(
                    "SELECT CAST(41 AS INTEGER) AS value", null, ct).AsTask();
                await capture.FirstResultReached.Task.WaitAsync(TimeSpan.FromSeconds(15), ct);
                using var queuedCancellation = CancellationTokenSource.CreateLinkedTokenSource(ct);
                Task<ScalarProjectionResult> queued = scalar.ExecuteScalarProjectionAsync(
                    "SELECT CAST(99 AS INTEGER) AS value", null, queuedCancellation.Token).AsTask();
                Assert.False(queued.IsCompleted);
                queuedCancellation.Cancel();
                var canceled = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await queued);
                Assert.Equal(queuedCancellation.Token, canceled.CancellationToken);
                Assert.False(active.IsCompleted);
                Assert.Equal(1, capture.SqlCommandCount);

                Task disposing = rental.DisposeAsync().AsTask();
                Assert.False(disposing.IsCompleted);
                capture.ReleaseFirstResult.TrySetResult();
                Assert.Equal(41L, (await active.WaitAsync(TimeSpan.FromSeconds(15), ct)).Int64);
                await disposing;
                await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
                    await scalar.ExecuteScalarProjectionAsync("SELECT 100", null, ct));
                Assert.Equal(1, capture.SqlCommandCount);
                Assert.DoesNotContain(capture.Requests, r => r.ExecuteSql?.Sql.Contains("99", StringComparison.Ordinal) == true);
            }
            finally
            {
                capture.ReleaseFirstResult.TrySetResult();
                await rental.DisposeAsync();
            }
        }
        finally { await CSharpDbClient.ClearPoolAsync(options.Endpoint!); }
    }

    [Fact(Timeout = 30000)]
    public async Task RealRental_InvalidCompactResponse_FaultsBeforeProjection_WithoutReplay()
    {
        var ct = TestContext.Current.CancellationToken;
        var capture = new ProjectionInterceptor(corruptFirstCompact: true);
        await using var host = Host(capture);
        var options = Options(host);
        try
        {
            await using var rental = await CSharpDbClient.RentAsync(options, 1, ct);
            var scalar = Assert.IsAssignableFrom<IScalarProjectionClient>(rental);
            var error = await Record.ExceptionAsync(async () =>
                await scalar.ExecuteScalarProjectionAsync("SELECT CAST(42 AS INTEGER) AS value", null, ct));
            Assert.NotNull(error);
            Assert.IsType<CSharpDbClientException>(error);
            Assert.Contains("compact integer", error.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(1, capture.SqlCommandCount);
            await Assert.ThrowsAsync<CSharpDbClientException>(async () =>
                await scalar.ExecuteScalarProjectionAsync("SELECT CAST(99 AS INTEGER) AS value", null, ct));
            Assert.Equal(1, capture.SqlCommandCount);
            Assert.Single(capture.Requests, r => r.Hello is not null);
        }
        finally { await CSharpDbClient.ClearPoolAsync(options.Endpoint!); }
    }

    [Fact(Timeout = 30000)]
    public async Task PooledMapper_RepeatedMixedCompletions_DoNotRetainPriorResultErrorOrToken()
    {
        var retained = new List<RetainedProjection>();
        for (int index = 0; index < 72; index++)
        {
            using var work = NewPooledWork(index, index % 6, TestContext.Current.CancellationToken);
            bool completedAtCall = index / 6 % 2 == 0;
            if (completedAtCall) CompletePooledWork(work);
            ValueTask<ScalarProjectionResult> operation = InvokePooledMapper(work);
            Assert.Equal(completedAtCall, operation.IsCompleted);
            if (!completedAtCall) CompletePooledWork(work); // Exercise suspended and synchronous completion for every mode.
            RetainedProjection? result = await CheckPooledWorkAsync(operation, work);
            if (result is not null) retained.Add(result);
            if (index % 6 == 5) AssertRetainedProjections(retained);
        }
        Assert.Equal(24, retained.Count);
        AssertRetainedProjections(retained);
    }

    [Fact(Timeout = 30000)]
    public async Task PooledMapper_ConcurrentMixedCompletions_KeepIndependentPayloadsFailuresAndTokens()
    {
        var retained = new List<RetainedProjection>();
        for (int batch = 0; batch < 8; batch++)
        {
            PooledWork[] work = Enumerable.Range(0, 12)
                .Select(lane => NewPooledWork(1000 + batch * 12 + lane, lane % 6, TestContext.Current.CancellationToken))
                .ToArray();
            try
            {
                Task<RetainedProjection?>[] observations = work.Select(item =>
                {
                    ValueTask<ScalarProjectionResult> operation = InvokePooledMapper(item);
                    Assert.False(operation.IsCompleted);
                    return CheckPooledWorkAsync(operation, item); // Consume each ValueTask exactly once.
                }).ToArray();
                Assert.All(observations, observation => Assert.False(observation.IsCompleted));
                // Every operation is pending before completion; alternate completion order.
                foreach (int lane in batch % 2 == 0 ? Enumerable.Range(0, 12).Reverse() : Enumerable.Range(0, 12))
                    CompletePooledWork(work[lane]);
                RetainedProjection?[] completed = await Task.WhenAll(observations)
                    .WaitAsync(TimeSpan.FromSeconds(15), TestContext.Current.CancellationToken);
                retained.AddRange(completed.OfType<RetainedProjection>());
                AssertRetainedProjections(retained);
            }
            finally
            {
                foreach (PooledWork item in work)
                {
                    item.Completion.TrySetCanceled(CancellationToken.None);
                    item.Dispose();
                }
            }
        }
        Assert.Equal(32, retained.Count);
        AssertRetainedProjections(retained);
    }

    private static ValueTask<ScalarProjectionResult> InvokePooledMapper(PooledWork work)
    {
        // Target the actual pooled mapper without adding a production API for tests.
        MethodInfo method = typeof(GrpcTransportClient).GetMethod("MapScalarProjectionAsync",
            BindingFlags.NonPublic | BindingFlags.Static, binder: null,
            types: [typeof(Task<GrpcCommandSession.SqlReply>), typeof(CancellationToken)], modifiers: null)
            ?? throw new InvalidOperationException("The private scalar response mapper was not found.");
        return Assert.IsType<ValueTask<ScalarProjectionResult>>(
            method.Invoke(null, [work.Completion.Task, work.Cancellation.Token]));
    }

    private static PooledWork NewPooledWork(int seed, int mode, CancellationToken token)
    {
        ModelKind kind = seed / 6 % 2 == 0 ? ModelKind.Integer : ModelKind.BigInt;
        string name = $"pooled_value_{seed}";
        long value = 100000L + seed;
        var definition = Definition(name);
        definition.Columns[0].DeclaredType.Kind = kind == ModelKind.Integer
            ? SqlTypeKindEnum.SqlTypeKindInteger : SqlTypeKindEnum.SqlTypeKindBigInt;
        var metadata = new GrpcSessionResultMetadata(definition);
        var response = Compact(value);
        if (mode == 1)
        {
            response.ClearScalarInt64();
            response.Rows = Rows(-value);
        }
        if (mode == 5) response.Elapsed = new Duration { Seconds = 1, Nanos = -1 };
        Exception? fault = mode switch
        {
            2 => new RpcException(new Status(StatusCode.Unavailable, $"pooled_rpc_{seed}")),
            3 => new RpcException(new Status(StatusCode.Cancelled, $"pooled_cancel_{seed}")),
            4 => new InvalidOperationException($"pooled_untranslated_{seed}"),
            _ => null,
        };
        return new PooledWork
        {
            Seed = seed, Mode = mode, Kind = kind, Name = name, Value = value,
            Cancellation = CancellationTokenSource.CreateLinkedTokenSource(token),
            Reply = new(response, metadata), Fault = fault,
            ExpectedMappingFailure = mode == 5
                ? Record.Exception(() => GrpcModelMapper.ToSessionModel(response, metadata)) : null,
        };
    }

    private static void CompletePooledWork(PooledWork work)
    {
        if (work.Mode == 3) work.Cancellation.Cancel();
        if (work.Fault is { } fault) work.Completion.SetException(fault);
        else work.Completion.SetResult(work.Reply);
    }

    private static async Task<RetainedProjection?> CheckPooledWorkAsync(
        ValueTask<ScalarProjectionResult> operation, PooledWork work)
    {
        ScalarProjectionResult result = default;
        Exception? error = null;
        try { result = await operation; }
        catch (Exception actual) { error = actual; }
        switch (work.Mode)
        {
            case 0:
            case 1:
                Assert.Null(error);
                var retained = new RetainedProjection(result, work.Mode == 0, work.Value, work.Kind, work.Name);
                AssertRetainedProjections([retained]);
                return retained;
            case 2:
                Assert.Contains($"pooled_rpc_{work.Seed}", Assert.IsAssignableFrom<CSharpDbClientException>(error).Message);
                break;
            case 3:
                Assert.Equal(work.Cancellation.Token,
                    Assert.IsAssignableFrom<OperationCanceledException>(error).CancellationToken);
                break;
            case 4:
                Assert.Same(work.Fault, error);
                break;
            default:
                Assert.NotNull(work.ExpectedMappingFailure);
                Assert.NotNull(error);
                Assert.Equal(work.ExpectedMappingFailure.GetType(), error.GetType());
                Assert.Equal(work.ExpectedMappingFailure.Message, error.Message);
                break;
        }
        return null;
    }

    private static void AssertRetainedProjections(IEnumerable<RetainedProjection> retained)
    {
        foreach (RetainedProjection item in retained)
        {
            Assert.Equal(item.Compact, item.Result.HasCompactInt64);
            if (item.Compact)
            {
                Assert.Equal(item.Value, item.Result.Int64);
                Assert.Null(item.Result.Fallback);
                Assert.Equal(item.Kind, Assert.IsType<ScalarTypeSnapshot>(item.Result.DeclaredType).Kind);
            }
            else
            {
                Assert.Null(item.Result.DeclaredType);
                SqlExecutionResult fallback = Assert.IsType<SqlExecutionResult>(item.Result.Fallback);
                Assert.Equal(item.Name, Assert.Single(fallback.ColumnNames!));
                Assert.Equal(-item.Value, Assert.IsType<long>(Assert.Single(Assert.Single(fallback.Rows!))));
                Assert.Equal(item.Kind, Assert.Single(fallback.Columns!).DeclaredType!.Kind);
            }
        }
    }

    private sealed record RetainedProjection(ScalarProjectionResult Result, bool Compact, long Value, ModelKind Kind, string Name);
    private sealed class PooledWork : IDisposable
    {
        public required int Seed { get; init; }
        public required int Mode { get; init; }
        public required ModelKind Kind { get; init; }
        public required string Name { get; init; }
        public required long Value { get; init; }
        public required CancellationTokenSource Cancellation { get; init; }
        public required GrpcCommandSession.SqlReply Reply { get; init; }
        public Exception? Fault { get; init; }
        public Exception? ExpectedMappingFailure { get; init; }
        public TaskCompletionSource<GrpcCommandSession.SqlReply> Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public void Dispose() => Cancellation.Dispose();
    }

    private static void AssertResultEqual(SqlExecutionResult expected, SqlExecutionResult actual)
    {
        Assert.Equal(expected.IsQuery, actual.IsQuery);
        Assert.Equal(expected.RowsAffected, actual.RowsAffected);
        Assert.Equal(expected.Error, actual.Error);
        Assert.Equal(expected.Elapsed, actual.Elapsed);
        Assert.Equal(expected.ColumnNames, actual.ColumnNames);
        Assert.Equal(expected.ColumnTypes, actual.ColumnTypes);
        Assert.Equal(expected.ColumnNullability, actual.ColumnNullability);
        if (expected.Columns is null) Assert.Null(actual.Columns);
        else
        {
            Assert.NotNull(actual.Columns);
            Assert.Equal(expected.Columns.Length, actual.Columns.Length);
            for (int i = 0; i < expected.Columns.Length; i++)
            {
                Assert.Equal(expected.Columns[i].Name, actual.Columns[i].Name);
                Assert.Equal(expected.Columns[i].Type, actual.Columns[i].Type);
                Assert.Equal(expected.Columns[i].DeclaredType?.Kind, actual.Columns[i].DeclaredType?.Kind);
            }
        }
        if (expected.Rows is null) Assert.Null(actual.Rows);
        else
        {
            Assert.NotNull(actual.Rows);
            Assert.Equal(expected.Rows.Count, actual.Rows.Count);
            for (int i = 0; i < expected.Rows.Count; i++)
                Assert.Equal(expected.Rows[i], actual.Rows[i]);
        }
    }

    private static CSharpDbClientOptions Options(GrpcSocketDaemonFixture host) => new()
    {
        Transport = CSharpDbTransport.Grpc, GrpcMode = CSharpDbGrpcMode.Native,
        GrpcCallMode = CSharpDbGrpcCallMode.Streaming, Endpoint = host.NativeEndpoint.AbsoluteUri,
    };

    private static GrpcSocketDaemonFixture Host(ProjectionInterceptor capture) => new(configureServices: services =>
    {
        services.AddSingleton(capture);
        services.AddGrpc(options => options.Interceptors.Add<ProjectionInterceptor>());
    });

    private static VariantArrayList Rows(object? value)
        => GrpcModelMapper.ToVariantArrayList([new object?[] { value }]);
    private static SqlExecutionResultMessage Compact(long value)
        => new() { IsQuery = true, ScalarInt64 = value, Elapsed = new Duration() };
    private static CommandSessionRequest Query() => new() { ExecuteSql = new() { Sql = "SELECT scripted_private_scalar" } };
    private static CommandSessionResponse Full(ulong id, SqlExecutionResultMessage definition)
    {
        definition.Rows = null; definition.ScalarInt64 = 1;
        return new() { ResultSchemaId = id, Result = definition };
    }
    private static CommandSessionResponse Reference(ulong id, long value)
        => new() { ResultSchemaId = id, ResultSchemaReference = true, Result = Compact(value) };

    private static SqlExecutionResultMessage Definition(string name)
    {
        var result = new SqlExecutionResultMessage
        {
            IsQuery = true, Elapsed = new Duration(),
            ColumnNames = GrpcModelMapper.ToStringList([name]),
            ColumnTypes = GrpcModelMapper.ToStringList(["INTEGER"]),
            ColumnNullability = GrpcModelMapper.ToBoolList([true]),
        };
        result.Columns.Add(new ColumnDefinitionMessage
        {
            SchemaId = "12345678-1234-5678-9012-123456789012", Name = name,
            Type = DbTypeEnum.DbTypeInteger, Nullable = true,
            DeclaredType = new() { Kind = SqlTypeKindEnum.SqlTypeKindInteger },
        });
        return result;
    }

    public sealed class ProjectionInterceptor(bool blockFirstResult = false, bool corruptFirstCompact = false) : Interceptor
    {
        private readonly bool _blockFirstResult = blockFirstResult;
        private readonly bool _corruptFirstCompact = corruptFirstCompact;
        private int _results;
        public ConcurrentQueue<CommandSessionRequest> Requests { get; } = new();
        public TaskCompletionSource FirstResultReached { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource ReleaseFirstResult { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int SqlCommandCount => Requests.Count(r => r.ExecuteSql is not null || r.ExecuteInTransaction is not null);
        public override Task DuplexStreamingServerHandler<TRequest, TResponse>(IAsyncStreamReader<TRequest> reader,
            IServerStreamWriter<TResponse> writer, ServerCallContext context,
            DuplexStreamingServerMethod<TRequest, TResponse> continuation)
            => continuation(new CapturingReader<TRequest>(reader, this),
                new ControlledWriter<TResponse>(writer, this, context.CancellationToken), context);

        private sealed class CapturingReader<T>(IAsyncStreamReader<T> inner, ProjectionInterceptor owner) : IAsyncStreamReader<T>
        {
            public T Current => inner.Current;
            public async Task<bool> MoveNext(CancellationToken ct)
            {
                if (!await inner.MoveNext(ct)) return false;
                if (Current is CommandSessionRequest request) owner.Requests.Enqueue(request.Clone());
                return true;
            }
        }
        private sealed class ControlledWriter<T>(IServerStreamWriter<T> inner, ProjectionInterceptor owner,
            CancellationToken ct) : IServerStreamWriter<T>
        {
            public WriteOptions? WriteOptions { get => inner.WriteOptions; set => inner.WriteOptions = value; }
            public async Task WriteAsync(T message)
            {
                if (message is CommandSessionResponse { Result: not null } response &&
                    Interlocked.Increment(ref owner._results) == 1)
                {
                    owner.FirstResultReached.TrySetResult();
                    if (owner._blockFirstResult) await owner.ReleaseFirstResult.Task.WaitAsync(ct);
                    if (owner._corruptFirstCompact)
                    {
                        Assert.True(response.Result.HasScalarInt64);
                        response = response.Clone();
                        response.Result.Rows = new VariantArrayList();
                        message = (T)(object)response;
                    }
                }
                await inner.WriteAsync(message);
            }
        }
    }

    private sealed class ScriptedPeer(params CommandSessionResponse[] responses)
    {
        private readonly Queue<CommandSessionResponse> _script = new(responses);
        private readonly ConcurrentQueue<CommandSessionResponse> _received = new();
        public int SqlWrites;
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
            => new((IClientStreamWriter<TRequest>)(object)new Writer(peer),
                (IAsyncStreamReader<TResponse>)(object)new Reader(peer), Task.FromResult(new Metadata()),
                () => Status.DefaultSuccess, () => new Metadata(), () => { });
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
                if (!state.Completed) throw new InvalidOperationException("No response queued by the scripted command.");
                return Task.FromResult(false);
            }
        }
    }
}
