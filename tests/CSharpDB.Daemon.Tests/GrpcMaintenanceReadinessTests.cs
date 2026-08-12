extern alias CSharpDbApi;

using System.Reflection;
using CSharpDB.Client;
using CSharpDB.Client.Grpc;
using CSharpDB.Client.Models;
using CSharpDB.Daemon.Grpc;
using CSharpDB.Observability;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using CSharpDbApiSecurityOptions =
    CSharpDbApi::CSharpDB.Api.Security.CSharpDbApiSecurityOptions;
using CSharpDbHostReadinessCoordinator =
    CSharpDbApi::CSharpDB.Api.CSharpDbHostReadinessCoordinator;

namespace CSharpDB.Daemon.Tests;

public sealed class GrpcMaintenanceReadinessTests
{
    private static CancellationToken Ct =>
        TestContext.Current.CancellationToken;

    [Theory]
    [InlineData("restore-validate", true, CSharpDbReadinessReason.None)]
    [InlineData("restore", false, CSharpDbReadinessReason.RestoreInProgress)]
    [InlineData("foreign-key-validate", true, CSharpDbReadinessReason.None)]
    [InlineData("foreign-key-apply", false, CSharpDbReadinessReason.ExclusiveMaintenance)]
    [InlineData("reindex", false, CSharpDbReadinessReason.ExclusiveMaintenance)]
    [InlineData("vacuum", false, CSharpDbReadinessReason.ExclusiveMaintenance)]
    public async Task MaintenanceRpc_UsesExpectedReadinessLease(
        string operation,
        bool expectedReady,
        CSharpDbReadinessReason expectedReason)
    {
        using TestHarness harness = CreateHarness();
        bool requiresVerification = expectedReason !=
                                    CSharpDbReadinessReason.None;
        harness.Control.BlockVerification = requiresVerification;

        Task call = operation switch
        {
            "restore-validate" => harness.Service.Restore(
                new RestoreRequestMessage
                {
                    SourcePath = "backup.db",
                    ValidateOnly = true,
                },
                TestServerCallContext.Create("/csharpdb.rpc.CSharpDbRpc/Restore")),
            "restore" => harness.Service.Restore(
                new RestoreRequestMessage { SourcePath = "backup.db" },
                TestServerCallContext.Create("/csharpdb.rpc.CSharpDbRpc/Restore")),
            "foreign-key-validate" => harness.Service.MigrateForeignKeys(
                new ForeignKeyMigrationRequestMessage
                {
                    ValidateOnly = true,
                    ViolationSampleLimit = 10,
                },
                TestServerCallContext.Create(
                    "/csharpdb.rpc.CSharpDbRpc/MigrateForeignKeys")),
            "foreign-key-apply" => harness.Service.MigrateForeignKeys(
                new ForeignKeyMigrationRequestMessage
                {
                    ViolationSampleLimit = 10,
                },
                TestServerCallContext.Create(
                    "/csharpdb.rpc.CSharpDbRpc/MigrateForeignKeys")),
            "reindex" => harness.Service.Reindex(
                new ReindexRequestMessage
                {
                    Scope = ReindexScopeEnum.ReindexScopeAll,
                },
                TestServerCallContext.Create("/csharpdb.rpc.CSharpDbRpc/Reindex")),
            "vacuum" => harness.Service.Vacuum(
                new Empty(),
                TestServerCallContext.Create("/csharpdb.rpc.CSharpDbRpc/Vacuum")),
            _ => throw new ArgumentOutOfRangeException(nameof(operation)),
        };

        Assert.Equal(operation, await harness.Control.Started.Task.WaitAsync(Ct));
        Assert.Equal(expectedReady, harness.Readiness.IsReady);
        Assert.Equal(expectedReason, harness.Readiness.Snapshot.ReadinessReason);

        harness.Control.Release.TrySetResult();
        if (requiresVerification)
        {
            await harness.Control.VerificationStarted.Task.WaitAsync(Ct);
            Assert.False(harness.Readiness.IsReady);
            Assert.Equal(
                expectedReason,
                harness.Readiness.Snapshot.ReadinessReason);
            harness.Control.VerificationRelease.TrySetResult();
        }

        await call;

        Assert.Equal(requiresVerification ? 1 : 0, harness.Control.VerificationCount);
        Assert.True(harness.Readiness.IsReady);
        Assert.Equal(
            CSharpDbReadinessReason.None,
            harness.Readiness.Snapshot.ReadinessReason);
    }

    [Fact]
    public async Task FullRestoreFailure_RequestsPersistentReopenRecovery()
    {
        using TestHarness harness = CreateHarness();
        harness.Control.Failure = new InvalidOperationException(
            "restore failed after replacement");

        Task call = harness.Service.Restore(
            new RestoreRequestMessage { SourcePath = "backup.db" },
            TestServerCallContext.Create("/csharpdb.rpc.CSharpDbRpc/Restore"));

        Assert.Equal(
            "restore",
            await harness.Control.Started.Task.WaitAsync(Ct));
        Assert.Equal(
            CSharpDbReadinessReason.RestoreInProgress,
            harness.Readiness.Snapshot.ReadinessReason);

        harness.Control.Release.TrySetResult();
        RpcException exception = await Assert.ThrowsAsync<RpcException>(
            () => call);

        Assert.Equal(StatusCode.Internal, exception.StatusCode);
        Assert.Equal(0, harness.Control.VerificationCount);
        Assert.False(harness.Readiness.IsReady);
        Assert.Equal(
            CSharpDbReadinessReason.ReopenPending,
            harness.Readiness.Snapshot.ReadinessReason);
    }

    [Theory]
    [InlineData("restore", CSharpDbReadinessReason.ReopenPending)]
    [InlineData("reindex", CSharpDbReadinessReason.Unavailable)]
    public async Task SuccessfulExclusiveOperation_VerificationFailureRequestsRecovery(
        string operation,
        CSharpDbReadinessReason expectedReason)
    {
        using TestHarness harness = CreateHarness();
        harness.Control.VerificationFailure = new InvalidOperationException(
            "verification failed");

        Task call = operation == "restore"
            ? harness.Service.Restore(
                new RestoreRequestMessage { SourcePath = "backup.db" },
                TestServerCallContext.Create("/csharpdb.rpc.CSharpDbRpc/Restore"))
            : harness.Service.Reindex(
                new ReindexRequestMessage
                {
                    Scope = ReindexScopeEnum.ReindexScopeAll,
                },
                TestServerCallContext.Create("/csharpdb.rpc.CSharpDbRpc/Reindex"));

        Assert.Equal(operation, await harness.Control.Started.Task.WaitAsync(Ct));
        harness.Control.Release.TrySetResult();
        await harness.Control.VerificationStarted.Task.WaitAsync(Ct);
        RpcException exception = await Assert.ThrowsAsync<RpcException>(
            () => call);

        Assert.Equal(StatusCode.Internal, exception.StatusCode);
        Assert.Equal("verification failed", exception.Status.Detail);
        Assert.Equal(1, harness.Control.VerificationCount);
        Assert.False(harness.Readiness.IsReady);
        Assert.Equal(expectedReason, harness.Readiness.Snapshot.ReadinessReason);
    }

    [Fact]
    public async Task ExclusiveOperationFailure_PreservesOriginalErrorWhenRecoveryProbeFails()
    {
        using TestHarness harness = CreateHarness();
        harness.Control.Failure = new InvalidOperationException(
            "maintenance operation failed");
        harness.Control.VerificationFailure = new InvalidOperationException(
            "recovery probe failed");

        Task call = harness.Service.Reindex(
            new ReindexRequestMessage
            {
                Scope = ReindexScopeEnum.ReindexScopeAll,
            },
            TestServerCallContext.Create("/csharpdb.rpc.CSharpDbRpc/Reindex"));

        Assert.Equal("reindex", await harness.Control.Started.Task.WaitAsync(Ct));
        harness.Control.Release.TrySetResult();
        RpcException exception = await Assert.ThrowsAsync<RpcException>(
            () => call);

        Assert.Equal(StatusCode.Internal, exception.StatusCode);
        Assert.Equal("maintenance operation failed", exception.Status.Detail);
        Assert.Equal(1, harness.Control.VerificationCount);
        Assert.False(harness.Readiness.IsReady);
        Assert.Equal(
            CSharpDbReadinessReason.Unavailable,
            harness.Readiness.Snapshot.ReadinessReason);
    }

    [Fact]
    public async Task ExclusiveOperationFailure_StaysReadyWhenRecoveryProbeSucceeds()
    {
        using TestHarness harness = CreateHarness();
        harness.Control.Failure = new InvalidOperationException(
            "maintenance operation failed");

        Task call = harness.Service.Reindex(
            new ReindexRequestMessage
            {
                Scope = ReindexScopeEnum.ReindexScopeAll,
            },
            TestServerCallContext.Create("/csharpdb.rpc.CSharpDbRpc/Reindex"));

        Assert.Equal("reindex", await harness.Control.Started.Task.WaitAsync(Ct));
        harness.Control.Release.TrySetResult();
        RpcException exception = await Assert.ThrowsAsync<RpcException>(
            () => call);

        Assert.Equal("maintenance operation failed", exception.Status.Detail);
        Assert.Equal(1, harness.Control.VerificationCount);
        Assert.True(harness.Readiness.IsReady);
        Assert.Equal(
            CSharpDbReadinessReason.None,
            harness.Readiness.Snapshot.ReadinessReason);
    }

    [Fact]
    public async Task VerificationTimeout_IsBoundedAndRequestsRecovery()
    {
        using TestHarness harness = CreateHarness(
            TimeSpan.FromMilliseconds(50));
        harness.Control.BlockVerification = true;

        Task call = harness.Service.Reindex(
            new ReindexRequestMessage
            {
                Scope = ReindexScopeEnum.ReindexScopeAll,
            },
            TestServerCallContext.Create("/csharpdb.rpc.CSharpDbRpc/Reindex"));

        Assert.Equal("reindex", await harness.Control.Started.Task.WaitAsync(Ct));
        harness.Control.Release.TrySetResult();
        await harness.Control.VerificationStarted.Task.WaitAsync(Ct);
        RpcException exception = await Assert.ThrowsAsync<RpcException>(
            () => call);

        Assert.Equal(StatusCode.Internal, exception.StatusCode);
        Assert.Contains("configured timeout", exception.Status.Detail);
        Assert.False(harness.Readiness.IsReady);
        Assert.Equal(
            CSharpDbReadinessReason.Unavailable,
            harness.Readiness.Snapshot.ReadinessReason);
    }

    [Fact]
    public async Task RpcCancellationDuringVerification_RemainsCancellation()
    {
        using TestHarness harness = CreateHarness();
        harness.Control.BlockVerification = true;
        using var cancellation = new CancellationTokenSource();
#pragma warning disable xUnit1051 // The RPC-specific token is the subject of this test.
        ServerCallContext context = TestServerCallContext.CreateWithCancellation(
            "/csharpdb.rpc.CSharpDbRpc/Reindex",
            requestHeaders: null,
            cancellationToken: cancellation.Token);
#pragma warning restore xUnit1051

        Task call = harness.Service.Reindex(
            new ReindexRequestMessage
            {
                Scope = ReindexScopeEnum.ReindexScopeAll,
            },
            context);

        Assert.Equal("reindex", await harness.Control.Started.Task.WaitAsync(Ct));
        harness.Control.Release.TrySetResult();
        await harness.Control.VerificationStarted.Task.WaitAsync(Ct);
        cancellation.Cancel();

        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => call);
        Assert.Equal(1, harness.Control.VerificationCount);
        Assert.False(harness.Readiness.IsReady);
        Assert.Equal(
            CSharpDbReadinessReason.Unavailable,
            harness.Readiness.Snapshot.ReadinessReason);
    }

    private static TestHarness CreateHarness(TimeSpan? readinessTimeout = null)
    {
        var options = new CSharpDbObservabilityOptions();
        if (readinessTimeout is TimeSpan timeout)
            options.Health.ReadinessTimeout = timeout;
        options.Validate();
        var state = new CSharpDbHostState();
        state.MarkReady();
        var readiness = new CSharpDbHostReadinessCoordinator(
            state,
            options);
        var services = new ServiceCollection()
            .AddSingleton(readiness)
            .AddSingleton(options)
            .BuildServiceProvider();
        ICSharpDbClient client = DispatchProxy.Create<
            ICSharpDbClient,
            MaintenanceClientProxy>();
        var control = (MaintenanceClientProxy)(object)client;
        var service = new CSharpDbRpcService(
            client,
            Options.Create(new CSharpDbApiSecurityOptions()),
            services);
        return new TestHarness(service, control, readiness, services);
    }

    private sealed class TestHarness(
        CSharpDbRpcService service,
        MaintenanceClientProxy control,
        CSharpDbHostReadinessCoordinator readiness,
        ServiceProvider services) : IDisposable
    {
        internal CSharpDbRpcService Service { get; } = service;
        internal MaintenanceClientProxy Control { get; } = control;
        internal CSharpDbHostReadinessCoordinator Readiness { get; } = readiness;

        public void Dispose()
        {
            services.Dispose();
            Readiness.Dispose();
        }
    }

    public class MaintenanceClientProxy : DispatchProxy
    {
        internal TaskCompletionSource<string> Started { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal TaskCompletionSource Release { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal Exception? Failure { get; set; }
        internal TaskCompletionSource VerificationStarted { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal TaskCompletionSource VerificationRelease { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal bool BlockVerification { get; set; }
        internal Exception? VerificationFailure { get; set; }
        internal int VerificationCount => Volatile.Read(ref _verificationCount);
        private int _verificationCount;

        protected override object? Invoke(
            MethodInfo? targetMethod,
            object?[]? args)
            => targetMethod?.Name switch
            {
                nameof(ICSharpDbClient.RestoreAsync) => RestoreAsync(
                    (RestoreRequest)args![0]!),
                nameof(ICSharpDbClient.MigrateForeignKeysAsync) =>
                    MigrateForeignKeysAsync(
                        (ForeignKeyMigrationRequest)args![0]!),
                nameof(ICSharpDbClient.ReindexAsync) => ReindexAsync(
                    (ReindexRequest)args![0]!),
                nameof(ICSharpDbClient.VacuumAsync) => VacuumAsync(),
                nameof(ICSharpDbClient.GetInfoAsync) => GetInfoAsync(
                    (CancellationToken)args![0]!),
                _ => throw new NotSupportedException(targetMethod?.Name),
            };

        private async Task<RestoreResult> RestoreAsync(RestoreRequest request)
        {
            Started.TrySetResult(
                request.ValidateOnly ? "restore-validate" : "restore");
            await Release.Task.ConfigureAwait(false);
            ThrowIfRequested();
            return new RestoreResult
            {
                SourcePath = request.SourcePath,
                ValidateOnly = request.ValidateOnly,
            };
        }

        private async Task<ForeignKeyMigrationResult> MigrateForeignKeysAsync(
            ForeignKeyMigrationRequest request)
        {
            Started.TrySetResult(
                request.ValidateOnly
                    ? "foreign-key-validate"
                    : "foreign-key-apply");
            await Release.Task.ConfigureAwait(false);
            ThrowIfRequested();
            return new ForeignKeyMigrationResult
            {
                ValidateOnly = request.ValidateOnly,
                Succeeded = true,
            };
        }

        private async Task<ReindexResult> ReindexAsync(ReindexRequest request)
        {
            Started.TrySetResult("reindex");
            await Release.Task.ConfigureAwait(false);
            ThrowIfRequested();
            return new ReindexResult { Scope = request.Scope };
        }

        private async Task<VacuumResult> VacuumAsync()
        {
            Started.TrySetResult("vacuum");
            await Release.Task.ConfigureAwait(false);
            ThrowIfRequested();
            return new VacuumResult();
        }

        private async Task<DatabaseInfo> GetInfoAsync(
            CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _verificationCount);
            VerificationStarted.TrySetResult();
            if (BlockVerification)
            {
                await VerificationRelease.Task.WaitAsync(cancellationToken)
                    .ConfigureAwait(false);
            }

            if (VerificationFailure is not null)
                throw VerificationFailure;

            return new DatabaseInfo { DataSource = "maintenance-test" };
        }

        private void ThrowIfRequested()
        {
            if (Failure is not null)
                throw Failure;
        }
    }
}
