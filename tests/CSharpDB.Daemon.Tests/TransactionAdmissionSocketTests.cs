using System.Reflection;
using System.Runtime.ExceptionServices;
using CSharpDB.Client;
using CSharpDB.Client.Internal;
using CSharpDB.Testing;
using Microsoft.Extensions.DependencyInjection;

namespace CSharpDB.Daemon.Tests;

public sealed class TransactionAdmissionSocketTests
{
    [Theory]
    [InlineData(CSharpDbGrpcMode.Auto, CSharpDbGrpcCallMode.Unary, false)]
    [InlineData(CSharpDbGrpcMode.Auto, CSharpDbGrpcCallMode.Unary, true)]
    [InlineData(CSharpDbGrpcMode.Native, CSharpDbGrpcCallMode.Unary, false)]
    [InlineData(CSharpDbGrpcMode.Native, CSharpDbGrpcCallMode.Unary, true)]
    [InlineData(CSharpDbGrpcMode.Native, CSharpDbGrpcCallMode.Streaming, false)]
    [InlineData(CSharpDbGrpcMode.Native, CSharpDbGrpcCallMode.Streaming, true)]
    public async Task SchemaOwnerCanFinishWhileAnotherConnectionWaitsForBegin(
        CSharpDbGrpcMode mode, CSharpDbGrpcCallMode callMode, bool commit)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        deadline.CancelAfter(TimeSpan.FromSeconds(30));
        CancellationToken ct = deadline.Token;
        await using var engine = EngineTransportClient.CreatePrivateMemory("socket-admission", null, null);
        ICSharpDbClient observed = DispatchProxy.Create<ICSharpDbClient, BeginObserver>();
        var observer = (BeginObserver)observed;
        observer.Inner = engine;
        await using var host = new GrpcSocketDaemonFixture(configureServices: services => services.AddSingleton(observed));
        var options = new CSharpDbClientOptions
        {
            Transport = CSharpDbTransport.Grpc, GrpcMode = mode, GrpcCallMode = callMode,
            Endpoint = host.EndpointFor(mode).AbsoluteUri,
        };
        await using var firstClient = CSharpDbClient.Create(options);
        await using var secondClient = CSharpDbClient.Create(options);
        using var cancelBegin = CancellationTokenSource.CreateLinkedTokenSource(ct);
        Task<CSharpDB.Client.Models.TransactionSessionInfo>? pending = null;
        Task completion = Task.CompletedTask;
        try
        {
            string first = (await firstClient.BeginTransactionAsync(ct)).TransactionId;
            Assert.Null((await firstClient.ExecuteInTransactionAsync(first, "CREATE TABLE socket_admission (id INTEGER);", ct)).Error);
            pending = secondClient.BeginTransactionAsync(cancelBegin.Token);
            // Signal only after the real shared engine has returned its
            // incomplete schema-admission task; request arrival alone races.
            Assert.True(await observer.SecondBegin.WaitAsync(ct));
            completion = commit ? firstClient.CommitTransactionAsync(first, ct) : firstClient.RollbackTransactionAsync(first, ct);
            await completion.WaitAsync(TimeSpan.FromSeconds(10), ct);
            string second = (await pending.WaitAsync(ct)).TransactionId;
            await secondClient.RollbackTransactionAsync(second, ct);
            Assert.Equal(commit, (await firstClient.GetTableNamesAsync(ct)).Contains("socket_admission"));
        }
        finally
        {
            cancelBegin.Cancel();
            if (pending is not null)
            {
                try { await pending.WaitAsync(TimeSpan.FromSeconds(10), CancellationToken.None); }
                catch (OperationCanceledException) { }
                catch (CSharpDbClientException) when (cancelBegin.IsCancellationRequested) { }
            }
            await completion.WaitAsync(TimeSpan.FromSeconds(10), CancellationToken.None);
        }
    }

    public class BeginObserver : DispatchProxy
    {
        public ICSharpDbClient Inner { get; set; } = null!;
        private int _begins;
        private readonly TaskCompletionSource<bool> _secondBegin = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public Task<bool> SecondBegin => _secondBegin.Task;

        protected override object? Invoke(MethodInfo? method, object?[]? arguments)
        {
            try
            {
                object? result = method!.Invoke(Inner, arguments);
                if (method.Name == nameof(ICSharpDbClient.BeginTransactionAsync) && Interlocked.Increment(ref _begins) == 2)
                    _secondBegin.TrySetResult(result is Task { IsCompleted: false });
                return result;
            }
            catch (TargetInvocationException error) when (error.InnerException is not null)
            {
                ExceptionDispatchInfo.Capture(error.InnerException).Throw();
                throw;
            }
        }
    }
}
