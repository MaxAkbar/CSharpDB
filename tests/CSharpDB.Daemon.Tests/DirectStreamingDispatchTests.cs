using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.ExceptionServices;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using CSharpDB.Testing;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using CoreException = CSharpDB.Primitives.CSharpDbException;
using CoreError = CSharpDB.Primitives.ErrorCode;

namespace CSharpDB.Daemon.Tests;

public sealed class DirectStreamingDispatchTests
{
    [Fact(Timeout = 30000)]
    public async Task Disposal_DrainsStreamAndUnaryCalls_BeforeRollbackAndReuse()
    {
        var ct = TestContext.Current.CancellationToken;
        var control = new Control { HoldSql = "INSERT INTO dispatch_drain VALUES (1)", HoldMetadata = true };
        await using var host = Host(control);
        var options = Options(host);
        ICSharpDbClient? first = null;
        try
        {
            first = await CSharpDbClient.RentAsync(options, 1, ct);
            Assert.Null((await first.ExecuteSqlAsync("CREATE TABLE dispatch_drain (id INTEGER PRIMARY KEY)", ct)).Error);
            string id = (await first.BeginTransactionAsync(ct)).TransactionId;
            var direct = first.ExecuteInTransactionAsync(id, control.HoldSql!, ct);
            await control.SqlEntered.Task.WaitAsync(TimeSpan.FromSeconds(5), ct);
            var unary = first.GetTableNamesAsync(ct);
            await control.MetadataEntered.Task.WaitAsync(TimeSpan.FromSeconds(5), ct);
            Task disposed = first.DisposeAsync().AsTask();
            Assert.Same(disposed, first.DisposeAsync().AsTask());
            Task<ICSharpDbClient> waiting = CSharpDbClient.RentAsync(options, 1, ct).AsTask();
            Assert.False(disposed.IsCompleted);
            Assert.False(waiting.IsCompleted);
            control.SqlRelease.TrySetResult();
            Assert.Null((await direct.WaitAsync(TimeSpan.FromSeconds(5), ct)).Error);
            Assert.False(disposed.IsCompleted);
            Assert.False(waiting.IsCompleted);
            Assert.Equal(0, control.Count(nameof(ICSharpDbClient.RollbackTransactionAsync)));
            control.MetadataRelease.TrySetResult();
            Assert.Contains("dispatch_drain", await unary.WaitAsync(TimeSpan.FromSeconds(5), ct));
            await disposed.WaitAsync(TimeSpan.FromSeconds(5), ct);
            await using var next = await waiting.WaitAsync(TimeSpan.FromSeconds(5), ct);
            Assert.Equal(1, control.Count(nameof(ICSharpDbClient.RollbackTransactionAsync)));
            Assert.Equal(0L, (await next.ExecuteSqlAsync("SELECT COUNT(*) FROM dispatch_drain", ct)).Rows![0][0]);
            Assert.Equal(1, HttpCount(host, "CommandSession"));

            // Both the direct stream methods and unary admin methods must still
            // pass the same disposed-rental guard after its owner is reused.
            var before = host.Snapshot().ToDictionary();
            await Assert.ThrowsAsync<ObjectDisposedException>(() => first.ExecuteSqlAsync("SELECT 42", ct));
            await Assert.ThrowsAsync<ObjectDisposedException>(() => first.GetInfoAsync(ct));
            await Assert.ThrowsAsync<ObjectDisposedException>(() => first.CommitTransactionAsync(id, ct));
            await Assert.ThrowsAsync<ObjectDisposedException>(() => first.GetMaintenanceReportAsync(ct));
            Assert.Equal(before.OrderBy(x => x.Key.Method), host.Snapshot().OrderBy(x => x.Key.Method));
            Assert.Equal(42L, (await next.ExecuteSqlAsync("SELECT 42", ct)).Rows![0][0]);
        }
        finally
        {
            control.ReleaseAll();
            if (first is not null) await first.DisposeAsync();
            await CSharpDbClient.ClearPoolAsync(host.NativeEndpoint.AbsoluteUri);
        }
    }

    [Fact(Timeout = 30000)]
    public async Task ForeignTransactionChecks_RemainLocal_WhileUnaryCallIsActive()
    {
        var ct = TestContext.Current.CancellationToken;
        var control = new Control { HoldMetadata = true };
        await using var host = Host(control);
        try
        {
            await using var owner = await CSharpDbClient.RentAsync(Options(host), 2, ct);
            await using var other = await CSharpDbClient.RentAsync(Options(host), 2, ct);
            string id = (await owner.BeginTransactionAsync(ct)).TransactionId;
            var unary = owner.GetTableNamesAsync(ct);
            await control.MetadataEntered.Task.WaitAsync(TimeSpan.FromSeconds(5), ct);
            try
            {
                await Assert.ThrowsAsync<InvalidOperationException>(() => other.ExecuteInTransactionAsync(id, "SELECT 42", ct));
                await Assert.ThrowsAsync<InvalidOperationException>(() => other.CommitTransactionAsync(id, ct));
                await Assert.ThrowsAsync<InvalidOperationException>(() => other.RollbackTransactionAsync(id, ct));
                Assert.Equal(0, control.Count(nameof(ICSharpDbClient.ExecuteInTransactionAsync)));
                Assert.Equal(0, control.Count(nameof(ICSharpDbClient.CommitTransactionAsync)));
                Assert.Equal(0, control.Count(nameof(ICSharpDbClient.RollbackTransactionAsync)));
                Assert.False(unary.IsCompleted);
                Assert.Equal(42L, (await owner.ExecuteInTransactionAsync(id, "SELECT 42", ct)).Rows![0][0]);
                await owner.CommitTransactionAsync(id, ct);
                Assert.Equal(1, control.Count(nameof(ICSharpDbClient.ExecuteInTransactionAsync)));
                Assert.Equal(1, control.Count(nameof(ICSharpDbClient.CommitTransactionAsync)));
            }
            finally { control.MetadataRelease.TrySetResult(); }
            await unary.WaitAsync(TimeSpan.FromSeconds(5), ct);
            Assert.Equal(42L, (await other.ExecuteSqlAsync("SELECT 42", ct)).Rows![0][0]);
        }
        finally { control.ReleaseAll(); await CSharpDbClient.ClearPoolAsync(host.NativeEndpoint.AbsoluteUri); }
    }

    [Theory(Timeout = 30000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task StreamOrUnaryRpcFailure_UsesSharedPoisonAndRollback_WithoutReplay(bool unaryFailure)
    {
        var ct = TestContext.Current.CancellationToken;
        var control = new Control();
        await using var host = Host(control);
        var options = Options(host);
        try
        {
            await using (var first = await CSharpDbClient.RentAsync(options, 1, ct))
            {
                Assert.Null((await first.ExecuteSqlAsync("CREATE TABLE dispatch_failure (id INTEGER PRIMARY KEY)", ct)).Error);
                string id = (await first.BeginTransactionAsync(ct)).TransactionId;
                Assert.Null((await first.ExecuteInTransactionAsync(id, "INSERT INTO dispatch_failure VALUES (1)", ct)).Error);
                control.FailMetadata = unaryFailure;
                control.FailSql = unaryFailure ? null : "SELECT direct_failure";
                var error = await Assert.ThrowsAsync<CoreException>(() => unaryFailure
                    ? first.GetTableNamesAsync(ct) : (Task)first.ExecuteSqlAsync("SELECT direct_failure", ct));
                Assert.Equal(CoreError.Busy, error.Code);
                var rpc = Assert.IsType<RpcException>(error.InnerException);
                Assert.Equal(StatusCode.Aborted, rpc.StatusCode);
                Assert.Equal("Busy", rpc.Trailers.GetValue("csharpdb-error-code"));
                control.FailMetadata = false; control.FailSql = null;
            }
            Assert.Equal(1, control.FailureAttempts);
            Assert.Equal(1, control.Count(nameof(ICSharpDbClient.RollbackTransactionAsync)));
            await using var next = await CSharpDbClient.RentAsync(options, 1, ct);
            Assert.Equal(0L, (await next.ExecuteSqlAsync("SELECT COUNT(*) FROM dispatch_failure", ct)).Rows![0][0]);
            Assert.Equal(2, HttpCount(host, "CommandSession"));
            Assert.Equal(1, control.FailureAttempts);
        }
        finally { await CSharpDbClient.ClearPoolAsync(host.NativeEndpoint.AbsoluteUri); }
    }

    [Fact(Timeout = 30000)]
    public async Task SqlErrorResult_IsReturnedOnce_AndDoesNotPoisonTheRental()
    {
        var ct = TestContext.Current.CancellationToken;
        var control = new Control();
        await using var host = Host(control);
        var options = Options(host);
        try
        {
            await using (var first = await CSharpDbClient.RentAsync(options, 1, ct))
            {
                int before = control.Count(nameof(ICSharpDbClient.ExecuteSqlAsync));
                var result = await first.ExecuteSqlAsync("SELECT sql_result_error", ct);
                Assert.Equal("controlled SQL error result", result.Error);
                Assert.Equal(before + 1, control.Count(nameof(ICSharpDbClient.ExecuteSqlAsync)));
                Assert.NotNull(await first.GetInfoAsync(ct));
                Assert.Empty(await first.GetTableNamesAsync(ct));
            }
            await using var next = await CSharpDbClient.RentAsync(options, 1, ct);
            Assert.Equal(42L, (await next.ExecuteSqlAsync("SELECT 42", ct)).Rows![0][0]);
            Assert.Equal(1, HttpCount(host, "CommandSession"));
        }
        finally { await CSharpDbClient.ClearPoolAsync(host.NativeEndpoint.AbsoluteUri); }
    }

    private static CSharpDbClientOptions Options(GrpcSocketDaemonFixture host) => new()
    {
        Transport = CSharpDbTransport.Grpc, GrpcMode = CSharpDbGrpcMode.Native,
        GrpcCallMode = CSharpDbGrpcCallMode.Streaming, Endpoint = host.NativeEndpoint.AbsoluteUri,
    };
    private static long HttpCount(GrpcSocketDaemonFixture host, string method)
        => host.Snapshot().Where(x => x.Key.Method.EndsWith("/" + method, StringComparison.Ordinal)).Sum(x => x.Value);
    private static GrpcSocketDaemonFixture Host(Control control) => new(configureServices: services =>
    {
        var original = services.Last(x => x.ServiceType == typeof(ICSharpDbClient));
        services.Replace(ServiceDescriptor.Singleton<ICSharpDbClient>(provider =>
        {
            var real = (ICSharpDbClient)original.ImplementationFactory!(provider);
            var proxy = DispatchProxy.Create<ICSharpDbClient, Backend>();
            ((Backend)(object)proxy).Inner = real;
            ((Backend)(object)proxy).Control = control;
            return proxy;
        }));
    });
    public sealed class Control
    {
        public string? HoldSql, FailSql;
        public bool HoldMetadata, FailMetadata;
        public int FailureAttempts;
        public ConcurrentDictionary<string, int> Calls { get; } = new();
        public TaskCompletionSource SqlEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource SqlRelease { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource MetadataEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource MetadataRelease { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int Count(string method) => Calls.GetValueOrDefault(method);
        public void ReleaseAll() { SqlRelease.TrySetResult(); MetadataRelease.TrySetResult(); }
    }
    public class Backend : DispatchProxy
    {
        public ICSharpDbClient Inner = null!;
        public Control Control = null!;
        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            MethodInfo method = targetMethod!;
            Control.Calls.AddOrUpdate(method.Name, 1, static (_, count) => count + 1);
            if ((Control.FailMetadata && method.Name == nameof(ICSharpDbClient.GetTableNamesAsync))
                || (Control.FailSql is { } failure && method.Name == nameof(ICSharpDbClient.ExecuteSqlAsync) && Equals(args![0], failure)))
            { Interlocked.Increment(ref Control.FailureAttempts); throw new CoreException(CoreError.Busy, "controlled RPC failure"); }
            if (method.Name == nameof(ICSharpDbClient.ExecuteSqlAsync) && Equals(args![0], "SELECT sql_result_error"))
                return Task.FromResult(new SqlExecutionResult { Error = "controlled SQL error result" });
            if (Control.HoldMetadata && method.Name == nameof(ICSharpDbClient.GetTableNamesAsync))
                return HoldMetadataAsync((CancellationToken)args![0]!);
            object? result;
            try { result = method.Invoke(Inner, args); }
            catch (TargetInvocationException error) when (error.InnerException is not null)
            { ExceptionDispatchInfo.Capture(error.InnerException).Throw(); throw; }
            if (Control.HoldSql is { } held && method.Name == nameof(ICSharpDbClient.ExecuteInTransactionAsync) && Equals(args![1], held))
                return HoldSqlAsync((Task<SqlExecutionResult>)result!, (CancellationToken)args[2]!);
            return result;
        }
        private async Task<SqlExecutionResult> HoldSqlAsync(Task<SqlExecutionResult> execution, CancellationToken ct)
        {
            var result = await execution;
            Control.SqlEntered.TrySetResult();
            await Control.SqlRelease.Task.WaitAsync(ct);
            return result;
        }
        private async Task<IReadOnlyList<string>> HoldMetadataAsync(CancellationToken ct)
        {
            Control.MetadataEntered.TrySetResult();
            await Control.MetadataRelease.Task.WaitAsync(ct);
            return await Inner.GetTableNamesAsync(ct);
        }
    }
}
