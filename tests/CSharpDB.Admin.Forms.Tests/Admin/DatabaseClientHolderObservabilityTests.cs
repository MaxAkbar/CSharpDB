using System.Reflection;
using CSharpDB.Admin.Configuration;
using CSharpDB.Admin.Services;
using CSharpDB.Client;
using CSharpDB.Observability;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Forms.Tests.Admin;

public sealed class DatabaseClientHolderObservabilityTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(0, 1)]
    [InlineData(int.MaxValue - 1, int.MaxValue)]
    [InlineData(int.MaxValue, int.MaxValue)]
    public void ObservabilityLeaseCount_SaturatesWithoutOverflow(
        int current,
        int expected)
        => Assert.Equal(expected, DatabaseClientHolder.SaturatingIncrementLeaseCount(current));

    [Fact]
    public async Task PlainClientContract_RemainsOptional_AndUnsupportedInnerFailsClearly()
    {
        (ICSharpDbClient plainClient, RecordingClientProxy recording) =
            CreateProxy<ICSharpDbClient>();
        await using DatabaseClientHolder holder = CreateHolder(plainClient);

        Assert.False(typeof(ICSharpDbObservabilityClient).IsAssignableFrom(typeof(ICSharpDbClient)));
        Assert.IsAssignableFrom<ICSharpDbClient>(holder);
        ICSharpDbObservabilityClient diagnostics =
            Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(holder);

        await Assert.ThrowsAsync<CSharpDbObservabilityNotSupportedException>(
            () => diagnostics.GetRuntimeDiagnosticsAsync(Ct));
        Assert.Empty(recording.Invocations);
    }

    [Fact]
    public async Task Delegation_PreservesSynchronousInnerValidation()
    {
        (IObservabilityTestClient inner, RecordingClientProxy recording) =
            CreateProxy<IObservabilityTestClient>();
        var expected = new ArgumentOutOfRangeException("maximumRecords");
        recording.SetSynchronousException(
            nameof(ICSharpDbObservabilityClient.GetActiveQueriesAsync),
            expected);
        await using DatabaseClientHolder holder = CreateHolder(inner);
        var diagnostics = (ICSharpDbObservabilityClient)holder;

        ArgumentOutOfRangeException actual = Assert.Throws<ArgumentOutOfRangeException>(
            () =>
            {
                _ = diagnostics.GetActiveQueriesAsync(-1, Ct);
            });

        Assert.Same(expected, actual);
        AssertInvocation(
            recording,
            nameof(ICSharpDbObservabilityClient.GetActiveQueriesAsync),
            -1,
            Ct);
    }

    [Fact]
    public async Task AllObservabilityMethods_ForwardArgumentsAndCancellationExactly()
    {
        (IObservabilityTestClient inner, RecordingClientProxy recording) =
            CreateProxy<IObservabilityTestClient>();
        ConfigureCompletedObservabilityResults(recording);
        await using DatabaseClientHolder holder = CreateHolder(inner);
        var diagnostics = (ICSharpDbObservabilityClient)holder;

        using var runtimeCancellation = new CancellationTokenSource();
        using var activeCancellation = new CancellationTokenSource();
        using var recentCancellation = new CancellationTokenSource();
        using var planCancellation = new CancellationTokenSource();
        using var sessionsCancellation = new CancellationTokenSource();
        using var detailCancellation = new CancellationTokenSource();
        var planId = new OpaqueDiagnosticsId("11111111111111111111111111111111");
        var detailId = new OpaqueDiagnosticsId("22222222222222222222222222222222");

        await diagnostics.GetRuntimeDiagnosticsAsync(runtimeCancellation.Token);
        await diagnostics.GetActiveQueriesAsync(7, activeCancellation.Token);
        await diagnostics.GetRecentQueriesAsync(11, recentCancellation.Token);
        await diagnostics.GetQueryPlanDiagnosticsAsync(planId, planCancellation.Token);
        await diagnostics.GetSessionsAsync(13, sessionsCancellation.Token);
        await diagnostics.GetQueryDetailAsync(detailId, detailCancellation.Token);

        Assert.Equal(6, recording.Invocations.Count);
        AssertInvocation(
            recording,
            nameof(ICSharpDbObservabilityClient.GetRuntimeDiagnosticsAsync),
            runtimeCancellation.Token);
        AssertInvocation(
            recording,
            nameof(ICSharpDbObservabilityClient.GetActiveQueriesAsync),
            7,
            activeCancellation.Token);
        AssertInvocation(
            recording,
            nameof(ICSharpDbObservabilityClient.GetRecentQueriesAsync),
            11,
            recentCancellation.Token);
        AssertInvocation(
            recording,
            nameof(ICSharpDbObservabilityClient.GetQueryPlanDiagnosticsAsync),
            planId,
            planCancellation.Token);
        AssertInvocation(
            recording,
            nameof(ICSharpDbObservabilityClient.GetSessionsAsync),
            13,
            sessionsCancellation.Token);
        AssertInvocation(
            recording,
            nameof(ICSharpDbObservabilityClient.GetQueryDetailAsync),
            detailId,
            detailCancellation.Token);
    }

    [Fact]
    public async Task ReplacementCycle_ReflectsEachCurrentClientsCapabilityImmediately()
    {
        (IObservabilityTestClient first, RecordingClientProxy firstRecording) =
            CreateProxy<IObservabilityTestClient>();
        (ICSharpDbClient unsupported, RecordingClientProxy unsupportedRecording) =
            CreateProxy<ICSharpDbClient>();
        (IObservabilityTestClient second, RecordingClientProxy secondRecording) =
            CreateProxy<IObservabilityTestClient>();
        ConfigureCompletedObservabilityResults(firstRecording);
        ConfigureCompletedObservabilityResults(secondRecording);
        await using DatabaseClientHolder holder = CreateHolder(first);
        var diagnostics = (ICSharpDbObservabilityClient)holder;

        await diagnostics.GetRuntimeDiagnosticsAsync(Ct);
        await holder.ReplaceClientAsync(unsupported, newShardAdmin: null, newBaseClientOptions: null);

        Assert.Equal(1, firstRecording.DisposeCount);
        await Assert.ThrowsAsync<CSharpDbObservabilityNotSupportedException>(
            () => diagnostics.GetRuntimeDiagnosticsAsync(Ct));

        await holder.ReplaceClientAsync(second, newShardAdmin: null, newBaseClientOptions: null);
        Assert.Equal(1, unsupportedRecording.DisposeCount);
        await diagnostics.GetRuntimeDiagnosticsAsync(Ct);

        Assert.Single(firstRecording.Invocations);
        Assert.Single(secondRecording.Invocations);
    }

    [Fact]
    public async Task Replacement_DoesNotDisposeCapturedClientOrHoldHolderLockDuringInFlightCall()
    {
        (IObservabilityTestClient first, RecordingClientProxy firstRecording) =
            CreateProxy<IObservabilityTestClient>();
        (IObservabilityTestClient second, RecordingClientProxy secondRecording) =
            CreateProxy<IObservabilityTestClient>();
        var firstResult = new TaskCompletionSource<
            DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>>(
                TaskCreationOptions.RunContinuationsAsynchronously);
        firstRecording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetRuntimeDiagnosticsAsync),
            firstResult.Task);
        ConfigureCompletedObservabilityResults(secondRecording);
        await using DatabaseClientHolder holder = CreateHolder(first);
        var diagnostics = (ICSharpDbObservabilityClient)holder;

        Task<DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>> capturedCall =
            diagnostics.GetRuntimeDiagnosticsAsync(Ct);
        Task replacement = holder.ReplaceClientAsync(
            second,
            newShardAdmin: null,
            newBaseClientOptions: null);

        Assert.False(replacement.IsCompleted);
        Assert.Equal(0, firstRecording.DisposeCount);

        // The replacement is already visible even though disposal of the old
        // client is waiting for its captured call to finish.
        await diagnostics.GetRuntimeDiagnosticsAsync(Ct);
        Assert.Single(secondRecording.Invocations);

        firstResult.SetResult(null!);
        await capturedCall;
        await replacement;

        Assert.Equal(1, firstRecording.DisposeCount);
    }

    [Fact]
    public async Task DisposeAsync_WaitsForCapturedObservabilityCall_AndDisposesOnce()
    {
        (IObservabilityTestClient inner, RecordingClientProxy recording) =
            CreateProxy<IObservabilityTestClient>();
        var result = new TaskCompletionSource<
            DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>>(
                TaskCreationOptions.RunContinuationsAsynchronously);
        recording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetRuntimeDiagnosticsAsync),
            result.Task);
        DatabaseClientHolder holder = CreateHolder(inner);
        var diagnostics = (ICSharpDbObservabilityClient)holder;

        Task<DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>> capturedCall =
            diagnostics.GetRuntimeDiagnosticsAsync(Ct);
        Task disposal = holder.DisposeAsync().AsTask();

        Assert.False(disposal.IsCompleted);
        Assert.Equal(0, recording.DisposeCount);
        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => diagnostics.GetRuntimeDiagnosticsAsync(Ct));

        result.SetResult(null!);
        await capturedCall;
        await disposal;
        await holder.DisposeAsync();

        Assert.Equal(1, recording.DisposeCount);
    }

    private static DatabaseClientHolder CreateHolder(ICSharpDbClient inner)
        => new(
            inner,
            shardAdmin: null,
            baseClientOptions: null,
            hostDatabaseOptions: new AdminHostDatabaseOptions(),
            functions: DbFunctionRegistry.Create(_ => { }));

    private static void ConfigureCompletedObservabilityResults(RecordingClientProxy recording)
    {
        recording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetRuntimeDiagnosticsAsync),
            Task.FromResult<DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>>(null!));
        recording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetActiveQueriesAsync),
            Task.FromResult<DiagnosticsTopologySnapshot<
                DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>>(null!));
        recording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetRecentQueriesAsync),
            Task.FromResult<DiagnosticsTopologySnapshot<
                DiagnosticsCollectionSnapshot<RecentQuerySnapshot>>>(null!));
        recording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetQueryPlanDiagnosticsAsync),
            Task.FromResult<DiagnosticsTopologySnapshot<
                DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>>>(null!));
        recording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetSessionsAsync),
            Task.FromResult<DiagnosticsTopologySnapshot<
                DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>>(null!));
        recording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetQueryDetailAsync),
            Task.FromResult<DiagnosticsTopologySnapshot<
                DiagnosticsValueSnapshot<QueryDetailSnapshot>>>(null!));
    }

    private static void AssertInvocation(
        RecordingClientProxy recording,
        string methodName,
        params object[] expectedArguments)
    {
        Invocation invocation = Assert.Single(
            recording.Invocations,
            item => string.Equals(item.MethodName, methodName, StringComparison.Ordinal));
        Assert.Equal(expectedArguments, invocation.Arguments);
    }

    private static (T Client, RecordingClientProxy Recording) CreateProxy<T>()
        where T : class
    {
        T client = DispatchProxy.Create<T, RecordingClientProxy>();
        return (client, (RecordingClientProxy)(object)client);
    }

    public interface IObservabilityTestClient : ICSharpDbClient, ICSharpDbObservabilityClient;

    public sealed record Invocation(string MethodName, object?[] Arguments);

    public class RecordingClientProxy : DispatchProxy
    {
        private readonly Dictionary<string, object> _results = new(StringComparer.Ordinal);
        private readonly Dictionary<string, Exception> _synchronousExceptions = new(StringComparer.Ordinal);
        private readonly List<Invocation> _invocations = [];
        private int _disposeCount;

        public IReadOnlyList<Invocation> Invocations
        {
            get
            {
                lock (_invocations)
                    return _invocations.ToArray();
            }
        }

        public int DisposeCount => Volatile.Read(ref _disposeCount);

        public void SetResult(string methodName, object result)
            => _results[methodName] = result;

        public void SetSynchronousException(string methodName, Exception exception)
            => _synchronousExceptions[methodName] = exception;

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            ArgumentNullException.ThrowIfNull(targetMethod);

            if (targetMethod.Name == "get_DataSource")
                return "test";

            if (targetMethod.Name == nameof(IAsyncDisposable.DisposeAsync))
            {
                Interlocked.Increment(ref _disposeCount);
                return ValueTask.CompletedTask;
            }

            lock (_invocations)
                _invocations.Add(new Invocation(targetMethod.Name, args?.ToArray() ?? []));

            if (_synchronousExceptions.TryGetValue(targetMethod.Name, out Exception? exception))
                throw exception;

            if (!_results.TryGetValue(targetMethod.Name, out object? result))
                throw new InvalidOperationException($"No result was configured for {targetMethod.Name}.");

            return result;
        }
    }
}
