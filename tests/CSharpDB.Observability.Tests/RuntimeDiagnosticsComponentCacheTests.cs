using System.Collections.Concurrent;
using CSharpDB.Observability;

namespace CSharpDB.Observability.Tests;

public sealed class RuntimeDiagnosticsComponentCacheTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ConcurrentFactories_RunOutsideLockAndRetainOneDisposableWinner()
    {
        const int contenderCount = 8;
        var state = new CSharpDbRuntimeDiagnosticsState(
            new CSharpDbObservabilityOptions { Enabled = true });
        using var factoriesEntered = new CountdownEvent(contenderCount);
        using var releaseFactories = new ManualResetEventSlim();
        var components = new ConcurrentBag<TrackingComponent>();

        Task<TrackingComponent>[] contenders = Enumerable.Range(0, contenderCount)
            .Select(_ => Task.Run(() => state.GetOrCreateComponent(() =>
            {
                var component = new TrackingComponent();
                components.Add(component);
                factoriesEntered.Signal();
                releaseFactories.Wait(TimeSpan.FromSeconds(10), Ct);
                return component;
            }), Ct))
            .ToArray();

        Assert.True(factoriesEntered.Wait(TimeSpan.FromSeconds(10), Ct));
        releaseFactories.Set();
        TrackingComponent[] winners = await Task.WhenAll(contenders);

        TrackingComponent winner = Assert.IsType<TrackingComponent>(winners[0]);
        Assert.All(winners, candidate => Assert.Same(winner, candidate));
        Assert.Equal(contenderCount, components.Count);
        Assert.Equal(
            contenderCount - 1,
            components.Count(static component => component.DisposeCount == 1));
        Assert.Equal(0, winner.DisposeCount);

        state.Dispose();

        Assert.Equal(1, winner.DisposeCount);
        Assert.All(components, static component => Assert.Equal(1, component.DisposeCount));
        state.Dispose();
        Assert.All(components, static component => Assert.Equal(1, component.DisposeCount));
    }

    [Fact]
    public void ReplacementState_HasFreshComponentsAndSharedIdentityOnly()
    {
        var primary = new CSharpDbRuntimeDiagnosticsState(
            new CSharpDbObservabilityOptions
            {
                Enabled = true,
                DatabaseAlias = "primary",
            });
        TrackingComponent primaryComponent =
            primary.GetOrCreateComponent(static () => new TrackingComponent());
        CSharpDbRuntimeDiagnosticsState replacement = primary.CreateForOptions(
            new CSharpDbObservabilityOptions
            {
                Enabled = true,
                DatabaseAlias = "replacement",
            });
        TrackingComponent replacementComponent =
            replacement.GetOrCreateComponent(static () => new TrackingComponent());

        Assert.NotSame(primaryComponent, replacementComponent);
        Assert.Equal(primary.ServerInstanceId, replacement.ServerInstanceId);
        Assert.Equal("primary", primary.DatabaseAlias);
        Assert.Equal("replacement", replacement.DatabaseAlias);

        primary.Dispose();

        Assert.Equal(1, primaryComponent.DisposeCount);
        Assert.Equal(0, replacementComponent.DisposeCount);
        Assert.Same(
            replacementComponent,
            replacement.GetOrCreateComponent(static () => new TrackingComponent()));

        replacement.Dispose();
        Assert.Equal(1, replacementComponent.DisposeCount);
    }

    [Fact]
    public async Task DisposeRacingFactory_DiscardsAndDisposesItsCandidate()
    {
        var state = new CSharpDbRuntimeDiagnosticsState(
            new CSharpDbObservabilityOptions { Enabled = true });
        var candidate = new TrackingComponent();
        using var factoryEntered = new ManualResetEventSlim();
        using var releaseFactory = new ManualResetEventSlim();
        Task<TrackingComponent> getComponent = Task.Run(() =>
            state.GetOrCreateComponent(() =>
            {
                factoryEntered.Set();
                releaseFactory.Wait(TimeSpan.FromSeconds(10), Ct);
                return candidate;
            }), Ct);

        Assert.True(factoryEntered.Wait(TimeSpan.FromSeconds(10), Ct));
        state.Dispose();
        releaseFactory.Set();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => getComponent);
        Assert.Equal(1, candidate.DisposeCount);
    }

    private sealed class TrackingComponent : IDisposable
    {
        private int _disposeCount;

        internal int DisposeCount => Volatile.Read(ref _disposeCount);

        public void Dispose()
            => Interlocked.Increment(ref _disposeCount);
    }
}
