using CSharpDB.Observability;

namespace CSharpDB.Observability.Tests;

public sealed class OperationScopeTests
{
    [Fact]
    public async Task Scope_NestsFlowsAndRestoresOperationAndTransport()
    {
        Assert.Null(CSharpDbOperationScope.Current);
        Assert.Equal(CSharpDbTransport.Embedded, CSharpDbOperationScope.CurrentTransport);

        using (CSharpDbOperationScope.EnterTransport(CSharpDbTransport.Http))
        {
            Assert.Null(CSharpDbOperationScope.Current);
            Assert.Equal(CSharpDbTransport.Http, CSharpDbOperationScope.CurrentTransport);

            CSharpDbOperationContext request = CSharpDbOperationContext.CreateRequest(
                CSharpDbOperationClass.Script,
                CSharpDbTransport.Http,
                "primary");
            using (CSharpDbOperationScope.Enter(request))
            {
                Assert.Same(request, CSharpDbOperationScope.Current);
                Assert.Equal(CSharpDbTransport.Http, CSharpDbOperationScope.CurrentTransport);

                await Task.Yield();

                Assert.Same(request, CSharpDbOperationScope.Current);
                using (CSharpDbOperationScope.EnterTransport(CSharpDbTransport.Grpc))
                {
                    Assert.Same(request, CSharpDbOperationScope.Current);
                    Assert.Equal(CSharpDbTransport.Grpc, CSharpDbOperationScope.CurrentTransport);
                }

                Assert.Equal(CSharpDbTransport.Http, CSharpDbOperationScope.CurrentTransport);
            }

            Assert.Null(CSharpDbOperationScope.Current);
            Assert.Equal(CSharpDbTransport.Http, CSharpDbOperationScope.CurrentTransport);
        }

        Assert.Null(CSharpDbOperationScope.Current);
        Assert.Equal(CSharpDbTransport.Embedded, CSharpDbOperationScope.CurrentTransport);
    }

    [Fact]
    public void InternalRuntimeOperation_FlowsWithExactContextAndRestores()
    {
        CSharpDbOperationContext context = CSharpDbOperationContext.CreateRoot(
            CSharpDbOperationClass.Query,
            CSharpDbTransport.Direct,
            "primary");
        var runtimeOperation = new object();

        Assert.Null(CSharpDbOperationScope.CurrentQueryRuntimeOperation);
        using (CSharpDbOperationScope.Enter(context, runtimeOperation))
        {
            Assert.Same(context, CSharpDbOperationScope.Current);
            Assert.Same(
                runtimeOperation,
                CSharpDbOperationScope.CurrentQueryRuntimeOperation);

            CSharpDbOperationContext child =
                CSharpDbOperationContext.CreateStatement(context);
            using (CSharpDbOperationScope.Enter(child))
            {
                Assert.Same(child, CSharpDbOperationScope.Current);
                Assert.Null(CSharpDbOperationScope.CurrentQueryRuntimeOperation);
            }

            Assert.Same(
                runtimeOperation,
                CSharpDbOperationScope.CurrentQueryRuntimeOperation);
        }

        Assert.Null(CSharpDbOperationScope.Current);
        Assert.Null(CSharpDbOperationScope.CurrentQueryRuntimeOperation);
    }

    [Fact]
    public void Scope_OutOfOrderAndRepeatedDisposalCannotLeakAmbientState()
    {
        IDisposable outer = CSharpDbOperationScope.EnterTransport(CSharpDbTransport.Http);
        CSharpDbOperationContext operation = CSharpDbOperationContext.CreateRoot(
            CSharpDbOperationClass.Query,
            CSharpDbTransport.Http,
            "primary");
        IDisposable inner = CSharpDbOperationScope.Enter(operation);

        outer.Dispose();
        Assert.Same(operation, CSharpDbOperationScope.Current);

        inner.Dispose();
        inner.Dispose();
        outer.Dispose();

        Assert.Null(CSharpDbOperationScope.Current);
        Assert.Equal(CSharpDbTransport.Embedded, CSharpDbOperationScope.CurrentTransport);
    }

    [Fact]
    public async Task Scope_CapturedExecutionContextRetainsItsImmutableAmbientValue()
    {
        var childEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseChild = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Task child;

        using (CSharpDbOperationScope.EnterTransport(CSharpDbTransport.Grpc))
        {
            child = Task.Run(
                async () =>
                {
                    childEntered.SetResult();
                    await releaseChild.Task;
                    Assert.Equal(CSharpDbTransport.Grpc, CSharpDbOperationScope.CurrentTransport);
                },
                TestContext.Current.CancellationToken);

            await childEntered.Task;
        }

        Assert.Equal(CSharpDbTransport.Embedded, CSharpDbOperationScope.CurrentTransport);
        releaseChild.SetResult();
        await child;
    }

    [Fact]
    public void StatementWithoutFingerprint_InheritsParentCorrelation()
    {
        CSharpDbOperationContext parent = CSharpDbOperationContext.CreateRequest(
            CSharpDbOperationClass.Procedure,
            CSharpDbTransport.Grpc,
            "primary");

        CSharpDbOperationContext statement = CSharpDbOperationContext.CreateStatement(parent);

        Assert.Equal(parent.OperationId, statement.ParentOperationId);
        Assert.Equal(parent.TraceId, statement.TraceId);
        Assert.Equal(parent.Transport, statement.Transport);
        Assert.Null(statement.QueryFingerprint);
        Assert.True(statement.CountsAsStatement);
    }

    [Fact]
    public void BoundaryScope_CarriesSafeSessionAndRestoresAllAmbientValues()
    {
        OpaqueDiagnosticsId sessionId = OpaqueDiagnosticsId.Create();

        Assert.Null(CSharpDbOperationScope.CurrentSessionId);
        using (CSharpDbOperationScope.EnterBoundary(CSharpDbTransport.Direct, sessionId))
        {
            Assert.Equal(CSharpDbTransport.Direct, CSharpDbOperationScope.CurrentTransport);
            Assert.Equal(sessionId, CSharpDbOperationScope.CurrentSessionId);

            CSharpDbOperationContext root = CSharpDbOperationContext.CreateRoot(
                CSharpDbOperationClass.Query,
                CSharpDbOperationScope.CurrentTransport,
                "primary",
                CSharpDbOperationScope.CurrentSessionId);
            using (CSharpDbOperationScope.Enter(root))
            {
                Assert.Equal(sessionId, CSharpDbOperationScope.CurrentSessionId);
            }
        }

        Assert.Equal(CSharpDbTransport.Embedded, CSharpDbOperationScope.CurrentTransport);
        Assert.Null(CSharpDbOperationScope.CurrentSessionId);
    }

    [Fact]
    public void ChildRequest_InheritsParentCorrelationClockAndBoundary()
    {
        var timeProvider = new FixedTimeProvider();
        OpaqueDiagnosticsId sessionId = OpaqueDiagnosticsId.Create();
        CSharpDbOperationContext parent = CSharpDbOperationContext.CreateRequest(
            CSharpDbOperationClass.Script,
            CSharpDbTransport.Grpc,
            "primary",
            sessionId,
            timeProvider);

        CSharpDbOperationContext child = CSharpDbOperationContext.CreateRequest(
            parent,
            CSharpDbOperationClass.Procedure);

        Assert.Equal(parent.OperationId, child.ParentOperationId);
        Assert.Equal(parent.TraceId, child.TraceId);
        Assert.Equal(parent.Transport, child.Transport);
        Assert.Equal(parent.DatabaseAlias, child.DatabaseAlias);
        Assert.Equal(parent.SessionId, child.SessionId);
        Assert.True(child.CountsAsRequest);
        Assert.Equal(TimeSpan.Zero, child.GetElapsedTime());
    }

    [Fact]
    public void TransportScope_RejectsUnknownOrUndefinedValues()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            CSharpDbOperationScope.EnterTransport(CSharpDbTransport.Unknown));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            CSharpDbOperationScope.EnterTransport((CSharpDbTransport)999));
    }

    [Fact]
    public void TransportScope_CarriesSessionAndRestoresOuterCorrelation()
    {
        OpaqueDiagnosticsId outerSessionId = OpaqueDiagnosticsId.Create();
        OpaqueDiagnosticsId innerSessionId = OpaqueDiagnosticsId.Create();

        using (CSharpDbOperationScope.EnterTransport(
                   CSharpDbTransport.Direct,
                   outerSessionId))
        {
            Assert.Equal(CSharpDbTransport.Direct, CSharpDbOperationScope.CurrentTransport);
            Assert.Equal(outerSessionId, CSharpDbOperationScope.CurrentSessionId);

            using (CSharpDbOperationScope.EnterTransport(
                       CSharpDbTransport.Http,
                       innerSessionId))
            {
                Assert.Equal(CSharpDbTransport.Http, CSharpDbOperationScope.CurrentTransport);
                Assert.Equal(innerSessionId, CSharpDbOperationScope.CurrentSessionId);
            }

            Assert.Equal(CSharpDbTransport.Direct, CSharpDbOperationScope.CurrentTransport);
            Assert.Equal(outerSessionId, CSharpDbOperationScope.CurrentSessionId);
        }

        Assert.Equal(CSharpDbTransport.Embedded, CSharpDbOperationScope.CurrentTransport);
        Assert.Null(CSharpDbOperationScope.CurrentSessionId);
    }

    [Fact]
    public void CorrelationScope_DoesNotOwnInnerBoundaryBuffer()
    {
        var received = new List<KeyValuePair<string, object?>>();
        int filterCalls = 0;
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            new CapturingObserver(received),
            (name, _, _) =>
            {
                filterCalls++;
                return name == CSharpDbLogEvents.RawSqlCaptureEnabled.Name;
            });

        int filterCallsBeforeTransportEntry = filterCalls;
        IDisposable outer = CSharpDbOperationScope.EnterTransport(CSharpDbTransport.Http);
        int filterCallsAfterTransportEntry = filterCalls;
        Assert.Equal(filterCallsBeforeTransportEntry, filterCallsAfterTransportEntry);

        using (CSharpDbOperationScope.EnterBoundary(CSharpDbTransport.Direct))
        {
            int filterCallsAfterBoundaryEntry = filterCalls;
            Assert.True(filterCallsAfterBoundaryEntry > filterCallsAfterTransportEntry);
            Assert.True(CSharpDbDiagnostics.EventPublisher.IsEnabled(
                CSharpDbLogEvents.RawSqlCaptureEnabled));
            CSharpDbDiagnostics.EventPublisher.Publish(
                CSharpDbLogEvents.RawSqlCaptureEnabled,
                static () => new CSharpDbRawSqlCaptureEnabledEvent(
                    "primary",
                    SqlTextCaptureMode.Raw));

            Assert.Equal(filterCallsAfterBoundaryEntry, filterCalls);
            Assert.Empty(received);
        }

        KeyValuePair<string, object?> item = Assert.Single(received);
        Assert.Equal(CSharpDbLogEvents.RawSqlCaptureEnabled.Name, item.Key);
        Assert.IsType<CSharpDbRawSqlCaptureEnabledEvent>(item.Value);

        outer.Dispose();
        Assert.Single(received);
    }

    [Fact]
    public void CorrelationScope_MoreThanBoundaryCapacity_FlushesEveryInnerTerminal()
    {
        const int eventCount = 4_096 + 1 + 128 + 1;
        var received = new List<KeyValuePair<string, object?>>(eventCount);
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            new CapturingObserver(received),
            static name => name == CSharpDbLogEvents.QueryCompleted.Name);
        OpaqueDiagnosticsId sessionId = OpaqueDiagnosticsId.Create();

        using (CSharpDbOperationScope.EnterTransport(
                   CSharpDbTransport.Http,
                   sessionId))
        {
            for (int index = 0; index < eventCount; index++)
            {
                using (CSharpDbOperationScope.EnterBoundary(
                           CSharpDbOperationScope.CurrentTransport,
                           CSharpDbOperationScope.CurrentSessionId))
                {
                    CSharpDbOperationContext context =
                        CSharpDbOperationContext.CreateRoot(
                            CSharpDbOperationClass.Query,
                            CSharpDbOperationScope.CurrentTransport,
                            "primary",
                            CSharpDbOperationScope.CurrentSessionId);
                    PublishCompleted(context);
                    Assert.Equal(index, received.Count);
                }

                Assert.Equal(index + 1, received.Count);
            }
        }

        Assert.Equal(eventCount, received.Count);
        Assert.All(
            received.Select(static item => Assert.IsType<CSharpDbQueryCompletedEvent>(item.Value)),
            item =>
            {
                Assert.Equal(CSharpDbTransport.Http, item.Context.Transport);
                Assert.Equal(sessionId, item.Context.SessionId);
        });
    }

    [Fact]
    public async Task DeferredBoundary_LifetimeDisposeRace_NeverFlushesBeforeLeaseRelease()
    {
        const int iterations = 2_048;
        OpaqueDiagnosticsId sessionId = OpaqueDiagnosticsId.Create();
        var observer = new SessionCountingObserver(sessionId);
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            observer,
            static name => name == CSharpDbLogEvents.QueryCompleted.Name);

        for (int iteration = 0; iteration < iterations; iteration++)
        {
            CSharpDbDeferredDiagnosticBoundary boundary =
                CSharpDbOperationScope.CreateDeferredBoundary(
                    CSharpDbTransport.Direct,
                    sessionId);
            CSharpDbOperationContext context = CSharpDbOperationContext.CreateRoot(
                CSharpDbOperationClass.Query,
                CSharpDbTransport.Direct,
                "primary",
                sessionId);
            using (boundary.Enter())
                PublishCompleted(context);

            int countBeforeRace = observer.Count;
            using var start = new Barrier(participantCount: 2);
            CancellationToken ct = TestContext.Current.CancellationToken;
            Task<IDisposable?> lifetimeTask = Task.Run(() =>
            {
                start.SignalAndWait(ct);
                return boundary.TryAcquireLifetime();
            }, ct);
            Task disposeTask = Task.Run(() =>
            {
                start.SignalAndWait(ct);
                boundary.Dispose();
            }, ct);

            await Task.WhenAll(lifetimeTask, disposeTask);
            IDisposable? lifetime = await lifetimeTask;
            if (lifetime is not null)
            {
                Assert.Equal(countBeforeRace, observer.Count);
                lifetime.Dispose();
            }

            await boundary.FlushCompletion.WaitAsync(
                TimeSpan.FromSeconds(5),
                TestContext.Current.CancellationToken);
            Assert.Equal(countBeforeRace + 1, observer.Count);

            boundary.Dispose();
            Assert.Equal(countBeforeRace + 1, observer.Count);
        }

        Assert.Equal(iterations, observer.Count);
    }

    [Fact]
    public void BoundaryBuffer_RetainsMaximumCompositeOutcomesSlowEventsAndHeadroom()
    {
        const int statementCount = 4_096;
        const int boundaryHeadroom = 128;
        var received = new List<KeyValuePair<string, object?>>();
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            new CapturingObserver(received),
            static name =>
                name == CSharpDbLogEvents.QueryCompleted.Name ||
                name == CSharpDbLogEvents.QueryFailed.Name ||
                name == CSharpDbLogEvents.QueryCanceled.Name ||
                name == CSharpDbLogEvents.SlowQuery.Name ||
                name == CSharpDbLogEvents.LongRunningQuery.Name ||
                name == CSharpDbLogEvents.HostStarting.Name ||
                name == CSharpDbLogEvents.TransactionCompleted.Name);

        CSharpDbOperationContext parent = CSharpDbOperationContext.CreateRequest(
            CSharpDbOperationClass.Script,
            CSharpDbTransport.Direct,
            "primary");
        OpaqueDiagnosticsId? firstStatementId = null;

        using (CSharpDbOperationScope.EnterBoundary(CSharpDbTransport.Direct))
        {
            for (int index = 0; index < statementCount; index++)
            {
                CSharpDbOperationContext statement =
                    CSharpDbOperationContext.CreateStatement(parent);
                if (index == 0)
                    firstStatementId = statement.OperationId;
                switch (index % 3)
                {
                    case 0:
                        PublishCompleted(statement);
                        break;
                    case 1:
                        PublishFailed(statement);
                        break;
                    default:
                        PublishCanceled(statement);
                        break;
                }
                PublishLongRunning(statement);
                PublishSlow(statement);
            }

            PublishCompleted(parent);
            PublishLongRunning(parent);
            PublishSlow(parent);

            // Model enclosing logical work that shares the same outer boundary.
            for (int index = 0; index < boundaryHeadroom; index++)
            {
                CSharpDbOperationContext outer = CSharpDbOperationContext.CreateRoot(
                    CSharpDbOperationClass.Query,
                    CSharpDbTransport.Direct,
                    "primary");
                PublishCompleted(outer);
                PublishLongRunning(outer);
                PublishSlow(outer);
            }

            // Operational events have an independent reserved budget.
            for (int index = 0; index < boundaryHeadroom / 2; index++)
            {
                CSharpDbOperationContext host = CSharpDbOperationContext.CreateRoot(
                    CSharpDbOperationClass.Database,
                    CSharpDbTransport.Direct,
                    "primary");
                CSharpDbDiagnostics.EventPublisher.Publish(
                    CSharpDbLogEvents.HostStarting,
                    () => new CSharpDbHostStartingEvent(host));

                CSharpDbOperationContext transaction =
                    CSharpDbOperationContext.CreateRoot(
                        CSharpDbOperationClass.Transaction,
                        CSharpDbTransport.Direct,
                        "primary");
                CSharpDbDiagnostics.EventPublisher.Publish(
                    CSharpDbLogEvents.TransactionCompleted,
                    () => new CSharpDbLifecycleCompletedEvent(
                        transaction,
                        transaction.GetUtcNow(),
                        TimeSpan.Zero,
                        CSharpDbOperationOutcome.Succeeded,
                        error: null));
            }

            Assert.Empty(received);
        }

        CSharpDbQueryTerminalEvent[] terminals = received
            .Where(static item => item.Key != CSharpDbLogEvents.SlowQuery.Name)
            .Select(static item => item.Value)
            .OfType<CSharpDbQueryTerminalEvent>()
            .ToArray();
        CSharpDbSlowQueryEvent[] slow = received
            .Select(static item => item.Value)
            .OfType<CSharpDbSlowQueryEvent>()
            .ToArray();
        CSharpDbLongRunningQueryEvent[] longRunning = received
            .Select(static item => item.Value)
            .OfType<CSharpDbLongRunningQueryEvent>()
            .ToArray();
        Assert.Equal(statementCount + 1 + boundaryHeadroom, terminals.Length);
        Assert.Equal(statementCount + 1 + boundaryHeadroom, slow.Length);
        Assert.Equal(statementCount + 1 + boundaryHeadroom, longRunning.Length);
        Assert.Equal(
            boundaryHeadroom / 2,
            received.Count(static item =>
                item.Key == CSharpDbLogEvents.HostStarting.Name));
        Assert.Equal(
            boundaryHeadroom / 2,
            received.Count(static item =>
                item.Key == CSharpDbLogEvents.TransactionCompleted.Name));
        Assert.Contains(
            terminals,
            item => item.Context.OperationId == firstStatementId);
        Assert.Contains(
            terminals,
            item => item.Context.OperationId == parent.OperationId);
        Assert.Contains(
            slow,
            item => item.Context.OperationId == firstStatementId);
        Assert.Contains(
            slow,
            item => item.Context.OperationId == parent.OperationId);
        Assert.Contains(
            longRunning,
            item => item.Context.OperationId == firstStatementId);
        Assert.Contains(
            longRunning,
            item => item.Context.OperationId == parent.OperationId);
    }

    [Fact]
    public void BoundaryBuffer_OptionalOverflowDropsOldestSlowOnly()
    {
        const int optionalCapacity = 4_096 + 1 + 128;
        var received = new List<KeyValuePair<string, object?>>();
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            new CapturingObserver(received),
            static name =>
                name == CSharpDbLogEvents.QueryCompleted.Name ||
                name == CSharpDbLogEvents.SlowQuery.Name);

        CSharpDbOperationContext before = CSharpDbOperationContext.CreateRoot(
            CSharpDbOperationClass.Query,
            CSharpDbTransport.Direct,
            "primary");
        CSharpDbOperationContext after = CSharpDbOperationContext.CreateRoot(
            CSharpDbOperationClass.Query,
            CSharpDbTransport.Direct,
            "primary");
        OpaqueDiagnosticsId? firstSlowId = null;
        OpaqueDiagnosticsId? lastSlowId = null;

        using (CSharpDbOperationScope.EnterBoundary(CSharpDbTransport.Direct))
        {
            PublishCompleted(before);
            for (int index = 0; index <= optionalCapacity; index++)
            {
                CSharpDbOperationContext context = CSharpDbOperationContext.CreateRoot(
                    CSharpDbOperationClass.Query,
                    CSharpDbTransport.Direct,
                    "primary");
                if (index == 0)
                    firstSlowId = context.OperationId;
                if (index == optionalCapacity)
                    lastSlowId = context.OperationId;
                PublishSlow(context);
            }

            PublishCompleted(after);
        }

        CSharpDbQueryCompletedEvent[] completed = received
            .Select(static item => item.Value)
            .OfType<CSharpDbQueryCompletedEvent>()
            .ToArray();
        CSharpDbSlowQueryEvent[] slow = received
            .Select(static item => item.Value)
            .OfType<CSharpDbSlowQueryEvent>()
            .ToArray();
        Assert.Equal(2, completed.Length);
        Assert.Contains(completed, item => item.Context.OperationId == before.OperationId);
        Assert.Contains(completed, item => item.Context.OperationId == after.OperationId);
        Assert.Equal(optionalCapacity, slow.Length);
        Assert.DoesNotContain(slow, item => item.Context.OperationId == firstSlowId);
        Assert.Contains(slow, item => item.Context.OperationId == lastSlowId);
        Assert.Equal(CSharpDbLogEvents.QueryCompleted.Name, received[0].Key);
        Assert.Equal(CSharpDbLogEvents.QueryCompleted.Name, received[^1].Key);
    }

    private static void PublishCompleted(CSharpDbOperationContext context)
        => CSharpDbDiagnostics.EventPublisher.Publish(
            CSharpDbLogEvents.QueryCompleted,
            () => new CSharpDbQueryCompletedEvent(
                context,
                context.GetUtcNow(),
                TimeSpan.Zero,
                timeToFirstResult: null,
                TimeSpan.Zero,
                TimeSpan.Zero,
                rowsProduced: 0,
                rowsAffected: 0));

    private static void PublishLongRunning(CSharpDbOperationContext context)
        => CSharpDbDiagnostics.EventPublisher.Publish(
            CSharpDbLogEvents.LongRunningQuery,
            () => new CSharpDbLongRunningQueryEvent(
                context,
                context.GetUtcNow(),
                TimeSpan.FromSeconds(1),
                TimeSpan.FromSeconds(1),
                QueryExecutionPhase.Executing));

    private static void PublishFailed(CSharpDbOperationContext context)
        => CSharpDbDiagnostics.EventPublisher.Publish(
            CSharpDbLogEvents.QueryFailed,
            () => new CSharpDbQueryFailedEvent(
                context,
                context.GetUtcNow(),
                TimeSpan.Zero,
                timeToFirstResult: null,
                TimeSpan.Zero,
                TimeSpan.Zero,
                rowsProduced: 0,
                rowsAffected: 0,
                SafeErrorProjector.Project(SafeErrorKind.DatabaseOperation)));

    private static void PublishCanceled(CSharpDbOperationContext context)
        => CSharpDbDiagnostics.EventPublisher.Publish(
            CSharpDbLogEvents.QueryCanceled,
            () => new CSharpDbQueryCanceledEvent(
                context,
                context.GetUtcNow(),
                TimeSpan.Zero,
                timeToFirstResult: null,
                TimeSpan.Zero,
                TimeSpan.Zero,
                rowsProduced: 0,
                rowsAffected: 0,
                SafeErrorProjector.Project(SafeErrorKind.OperationCanceled)));

    private static void PublishSlow(CSharpDbOperationContext context)
    {
        TimeSpan duration = TimeSpan.FromMilliseconds(1);
        CSharpDbDiagnostics.EventPublisher.Publish(
            CSharpDbLogEvents.SlowQuery,
            () => new CSharpDbSlowQueryEvent(
                context,
                context.GetUtcNow(),
                duration,
                timeToFirstResult: null,
                TimeSpan.Zero,
                duration,
                rowsProduced: 0,
                rowsAffected: 0,
                CSharpDbOperationOutcome.Succeeded,
                error: null,
                duration));
    }


    private sealed class FixedTimeProvider : TimeProvider
    {
        private static readonly DateTimeOffset UtcNow =
            new(2026, 8, 10, 0, 0, 0, TimeSpan.Zero);

        public override DateTimeOffset GetUtcNow() => UtcNow;
        public override long GetTimestamp() => 42;
        public override long TimestampFrequency => 1_000;
    }

    private sealed class CapturingObserver(
        List<KeyValuePair<string, object?>> received) :
        IObserver<KeyValuePair<string, object?>>
    {
        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }

        public void OnNext(KeyValuePair<string, object?> value)
            => received.Add(value);
    }

    private sealed class SessionCountingObserver(OpaqueDiagnosticsId sessionId) :
        IObserver<KeyValuePair<string, object?>>
    {
        private int _count;

        internal int Count => Volatile.Read(ref _count);

        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }

        public void OnNext(KeyValuePair<string, object?> value)
        {
            if (value.Value is CSharpDbQueryCompletedEvent completed &&
                Equals(sessionId, completed.Context.SessionId))
            {
                Interlocked.Increment(ref _count);
            }
        }
    }
}
