using CSharpDB.Execution;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class QueryResultLifecycleTests
{
    [Fact]
    public async Task DisposeAsync_WhenOperatorDisposeThrows_StillInvokesCallback()
    {
        var result = new QueryResult(new ThrowingDisposeOperator());
        bool callbackInvoked = false;
        result.SetDisposeCallback(
            () =>
            {
                callbackInvoked = true;
                return ValueTask.CompletedTask;
            });

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => result.DisposeAsync().AsTask());

        Assert.True(callbackInvoked);
    }

    [Fact]
    public async Task AppendDisposeCallback_WhenExistingCallbackThrows_StillInvokesAppendedCallback()
    {
        var result = new QueryResult(new NoOpOperator());
        bool appendedCallbackInvoked = false;
        result.SetDisposeCallback(
            () => ValueTask.FromException(
                new InvalidOperationException("Existing callback failed.")));
        result.AppendDisposeCallback(
            () =>
            {
                appendedCallbackInvoked = true;
                return ValueTask.CompletedTask;
            });

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => result.DisposeAsync().AsTask());

        Assert.True(appendedCallbackInvoked);
    }

    private class NoOpOperator : IOperator
    {
        public ColumnDefinition[] OutputSchema { get; } = [];
        public bool ReusesCurrentRowBuffer => false;
        public DbValue[] Current => [];
        public ValueTask OpenAsync(CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask<bool> MoveNextAsync(CancellationToken ct = default) =>
            ValueTask.FromResult(false);
        public virtual ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class ThrowingDisposeOperator : NoOpOperator
    {
        public override ValueTask DisposeAsync() =>
            ValueTask.FromException(
                new InvalidOperationException("Operator disposal failed."));
    }
}
