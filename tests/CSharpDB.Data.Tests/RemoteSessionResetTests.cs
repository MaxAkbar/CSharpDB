using System.Reflection;
using CSharpDB.Client;
using CSharpDB.Client.Models;

namespace CSharpDB.Data.Tests;

public sealed class RemoteSessionResetTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task FailedRollback_StillDisposesClient_AndIsAttemptedOnlyOnce(bool explicitRollback, bool synchronousFailure)
    {
        var client = DispatchProxy.Create<ICSharpDbClient, TransactionClientProxy>();
        var proxy = (TransactionClientProxy)client;
        proxy.FailRollback = true;
        proxy.SynchronousFailure = synchronousFailure;
        var session = new RemoteDatabaseSession(client);
        await session.BeginTransactionAsync(TestContext.Current.CancellationToken);
        if (explicitRollback)
            await Assert.ThrowsAsync<HttpRequestException>(() => session.RollbackAsync(TestContext.Current.CancellationToken).AsTask());
        await session.DisposeAsync();
        await session.DisposeAsync();
        Assert.Equal(1, proxy.Disposals);
        Assert.Equal(1, proxy.Rollbacks);
        Assert.Equal("transaction-id", proxy.LastTransactionId);
        if (!explicitRollback) Assert.True(proxy.LastRollbackToken.CanBeCanceled);
    }

    [Fact]
    public async Task FailedCommit_IsNotReplayed_AndCleanupDisposesClient()
    {
        var client = DispatchProxy.Create<ICSharpDbClient, TransactionClientProxy>();
        var proxy = (TransactionClientProxy)client;
        var session = new RemoteDatabaseSession(client);
        await session.BeginTransactionAsync(TestContext.Current.CancellationToken);
        await Assert.ThrowsAsync<HttpRequestException>(() => session.CommitAsync(TestContext.Current.CancellationToken).AsTask());
        await session.DisposeAsync();
        Assert.Equal(1, proxy.Disposals);
        Assert.Equal(1, proxy.Commits);
        Assert.Equal(1, proxy.Rollbacks);
    }

    public class TransactionClientProxy : DispatchProxy
    {
        public bool FailRollback { get; set; }
        public bool SynchronousFailure { get; set; }
        public int Disposals { get; private set; }
        public int Rollbacks { get; private set; }
        public int Commits { get; private set; }
        public string? LastTransactionId { get; private set; }
        public CancellationToken LastRollbackToken { get; private set; }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            switch (targetMethod!.Name)
            {
                case nameof(ICSharpDbClient.DisposeAsync):
                    Disposals++;
                    return ValueTask.CompletedTask;
                case nameof(ICSharpDbClient.BeginTransactionAsync):
                    return Task.FromResult(new TransactionSessionInfo { TransactionId = "transaction-id", ExpiresAtUtc = DateTime.UtcNow.AddMinutes(1) });
                case nameof(ICSharpDbClient.RollbackTransactionAsync):
                    Rollbacks++;
                    LastTransactionId = (string)args![0]!;
                    LastRollbackToken = (CancellationToken)args[1]!;
                    if (!FailRollback) return Task.CompletedTask;
                    if (SynchronousFailure) throw new HttpRequestException("Injected rollback failure");
                    return Task.FromException(new HttpRequestException("Injected rollback failure"));
                case nameof(ICSharpDbClient.CommitTransactionAsync):
                    Commits++;
                    return Task.FromException(new HttpRequestException("Injected commit failure"));
                default: throw new NotSupportedException(targetMethod.Name);
            }
        }
    }
}
