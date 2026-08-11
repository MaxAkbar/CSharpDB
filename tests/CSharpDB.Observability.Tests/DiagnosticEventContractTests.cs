using System.Diagnostics;
using System.Text.Json;
using CSharpDB.Observability;

namespace CSharpDB.Observability.Tests;

public sealed class DiagnosticEventContractTests
{
    [Fact]
    public void EventCatalog_HasStableUniqueIdsNamesCategoriesAndSafeTemplates()
    {
        Assert.Equal(18, CSharpDbLogEvents.All.Count);
        Assert.Equal(
            CSharpDbLogEvents.All.Count,
            CSharpDbLogEvents.All.Select(static definition => definition.EventId).Distinct().Count());
        Assert.Equal(
            CSharpDbLogEvents.All.Count,
            CSharpDbLogEvents.All.Select(static definition => definition.Name).Distinct(StringComparer.Ordinal).Count());

        Assert.Equal(CSharpDbLogEventIds.HostStarting, CSharpDbLogEvents.HostStarting.EventId);
        Assert.Equal(CSharpDbLogEventIds.RawSqlCaptureEnabled, CSharpDbLogEvents.RawSqlCaptureEnabled.EventId);
        Assert.Equal(CSharpDbLogEventIds.QueryCompleted, CSharpDbLogEvents.QueryCompleted.EventId);
        Assert.Equal(CSharpDbLogEventIds.SlowQuery, CSharpDbLogEvents.SlowQuery.EventId);
        Assert.Equal(CSharpDbLogEventIds.QueryFailed, CSharpDbLogEvents.QueryFailed.EventId);
        Assert.Equal(CSharpDbLogEventIds.QueryCanceled, CSharpDbLogEvents.QueryCanceled.EventId);
        Assert.Equal(CSharpDbLogEventIds.LongRunningQuery, CSharpDbLogEvents.LongRunningQuery.EventId);
        Assert.Equal(CSharpDbLogEventIds.TransactionCompleted, CSharpDbLogEvents.TransactionCompleted.EventId);
        Assert.Equal(CSharpDbLogEventIds.CheckpointCompleted, CSharpDbLogEvents.CheckpointCompleted.EventId);
        Assert.Equal(CSharpDbLogEventIds.MaintenanceCompleted, CSharpDbLogEvents.MaintenanceCompleted.EventId);

        foreach (CSharpDbLogEventDefinition definition in CSharpDbLogEvents.All)
        {
            Assert.StartsWith("CSharpDB.", definition.Name, StringComparison.Ordinal);
            Assert.NotEqual(CSharpDbLogEventCategory.Unknown, definition.Category);
            Assert.False(string.IsNullOrWhiteSpace(definition.MessageTemplate));
            Assert.DoesNotContain("{CapturedSqlText", definition.MessageTemplate, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("{RawSql", definition.MessageTemplate, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("{Exception", definition.MessageTemplate, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("{Message", definition.MessageTemplate, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("{Path", definition.MessageTemplate, StringComparison.OrdinalIgnoreCase);
        }
    }

    [Fact]
    public void QueryTerminalEvents_AreImmutableAndEnforceCaptureAndOutcomePolicy()
    {
        CSharpDbOperationContext context = CreateQueryContext();
        DateTimeOffset completedAtUtc = new(2026, 8, 10, 12, 0, 2, TimeSpan.Zero);
        SafeErrorProjection failure = SafeErrorProjector.Project(SafeErrorKind.DatabaseOperation);
        SafeErrorProjection cancellation = SafeErrorProjector.Project(SafeErrorKind.OperationCanceled);

        var completed = new CSharpDbQueryCompletedEvent(
            context,
            completedAtUtc,
            totalDuration: TimeSpan.FromSeconds(2),
            timeToFirstResult: TimeSpan.FromMilliseconds(300),
            queueDuration: TimeSpan.FromMilliseconds(100),
            executionAndConsumptionDuration: TimeSpan.FromMilliseconds(1_900),
            rowsProduced: 4,
            rowsAffected: 0);
        var failed = new CSharpDbQueryFailedEvent(
            context,
            completedAtUtc,
            TimeSpan.FromSeconds(2),
            TimeSpan.FromMilliseconds(300),
            TimeSpan.FromMilliseconds(100),
            TimeSpan.FromMilliseconds(1_900),
            rowsProduced: 1,
            rowsAffected: 0,
            failure);
        var canceled = new CSharpDbQueryCanceledEvent(
            context,
            completedAtUtc,
            TimeSpan.FromSeconds(2),
            timeToFirstResult: null,
            TimeSpan.FromMilliseconds(100),
            TimeSpan.FromMilliseconds(500),
            rowsProduced: 0,
            rowsAffected: 0,
            cancellation);
        var slow = new CSharpDbSlowQueryEvent(
            context,
            completedAtUtc,
            TimeSpan.FromSeconds(2),
            TimeSpan.FromMilliseconds(300),
            TimeSpan.FromMilliseconds(100),
            TimeSpan.FromMilliseconds(1_900),
            rowsProduced: 4,
            rowsAffected: 0,
            CSharpDbOperationOutcome.Succeeded,
            error: null,
            slowQueryThreshold: TimeSpan.FromMilliseconds(500));

        Assert.Equal(CSharpDbOperationOutcome.Succeeded, completed.Outcome);
        Assert.Equal(CSharpDbOperationOutcome.Failed, failed.Outcome);
        Assert.Same(failure, failed.Error);
        Assert.Equal(CSharpDbOperationOutcome.Canceled, canceled.Outcome);
        Assert.Same(cancellation, canceled.Error);
        Assert.Equal(TimeSpan.FromMilliseconds(500), slow.SlowQueryThreshold);
        Assert.All(
            typeof(CSharpDbQueryTerminalEvent).GetProperties(),
            static property => Assert.Null(property.SetMethod));

        const string secret = "BearerCapabilitySecret";
        Assert.Throws<ArgumentException>(() => new CSharpDbQueryCompletedEvent(
            context,
            completedAtUtc,
            TimeSpan.FromSeconds(2),
            timeToFirstResult: null,
            queueDuration: TimeSpan.Zero,
            executionAndConsumptionDuration: TimeSpan.Zero,
            rowsProduced: 0,
            rowsAffected: 0,
            SqlTextCaptureMode.None,
            capturedSqlText: secret));
        Assert.Throws<ArgumentException>(() => new CSharpDbSlowQueryEvent(
            context,
            completedAtUtc,
            TimeSpan.FromMilliseconds(499),
            timeToFirstResult: null,
            queueDuration: TimeSpan.Zero,
            executionAndConsumptionDuration: TimeSpan.Zero,
            rowsProduced: 0,
            rowsAffected: 0,
            CSharpDbOperationOutcome.Succeeded,
            error: null,
            slowQueryThreshold: TimeSpan.FromMilliseconds(500)));
    }

    [Fact]
    public void LongRunningQueryEvent_IsImmutableThresholdBoundAndContainsNoSensitivePayload()
    {
        CSharpDbOperationContext context = CreateQueryContext();
        var longRunning = new CSharpDbLongRunningQueryEvent(
            context,
            new DateTimeOffset(2026, 8, 10, 12, 0, 2, TimeSpan.Zero),
            elapsed: TimeSpan.FromSeconds(2),
            longRunningQueryThreshold: TimeSpan.FromSeconds(1),
            QueryExecutionPhase.Executing);

        Assert.Same(context, longRunning.Context);
        Assert.Equal(TimeSpan.FromSeconds(2), longRunning.Elapsed);
        Assert.Equal(TimeSpan.FromSeconds(1), longRunning.LongRunningQueryThreshold);
        Assert.Equal(QueryExecutionPhase.Executing, longRunning.Phase);
        Assert.All(
            typeof(CSharpDbLongRunningQueryEvent).GetProperties(),
            static property => Assert.Null(property.SetMethod));

        Assert.Throws<ArgumentException>(() => new CSharpDbLongRunningQueryEvent(
            context,
            longRunning.ObservedAtUtc,
            elapsed: TimeSpan.FromMilliseconds(999),
            longRunningQueryThreshold: TimeSpan.FromSeconds(1),
            QueryExecutionPhase.Executing));
        Assert.Throws<ArgumentOutOfRangeException>(() => new CSharpDbLongRunningQueryEvent(
            context,
            longRunning.ObservedAtUtc,
            elapsed: TimeSpan.FromSeconds(2),
            longRunningQueryThreshold: TimeSpan.FromSeconds(1),
            QueryExecutionPhase.Completed));

        string json = JsonSerializer.Serialize(
            longRunning,
            CSharpDbObservabilityJsonContext.Default.CSharpDbLongRunningQueryEvent);
        Assert.DoesNotContain("sql", json, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("parameter", json, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("error", json, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("path", json, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void DefaultQueryEventSerialization_DropsSecretExceptionSqlPathAndValues()
    {
        const string secret = "BearerCapabilitySecret";
        var exception = new InvalidOperationException(
            $"SELECT '{secret}' FROM C:\\private\\database.db");
        SafeErrorProjection safeError = SafeErrorProjector.Project(exception);
        var failed = new CSharpDbQueryFailedEvent(
            CreateQueryContext(),
            new DateTimeOffset(2026, 8, 10, 12, 0, 2, TimeSpan.Zero),
            TimeSpan.FromSeconds(2),
            timeToFirstResult: null,
            queueDuration: TimeSpan.FromMilliseconds(5),
            executionAndConsumptionDuration: TimeSpan.FromMilliseconds(10),
            rowsProduced: 0,
            rowsAffected: 0,
            safeError);

        string json = JsonSerializer.Serialize(
            failed,
            CSharpDbObservabilityJsonContext.Default.CSharpDbQueryFailedEvent);

        Assert.DoesNotContain(secret, json, StringComparison.Ordinal);
        Assert.DoesNotContain("SELECT", json, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("private", json, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(nameof(InvalidOperationException), json, StringComparison.Ordinal);
        Assert.Contains("unexpected_error", json, StringComparison.Ordinal);
        Assert.Contains("\"sqlTextCaptureMode\":\"None\"", json, StringComparison.Ordinal);
        Assert.Contains("\"capturedSqlText\":null", json, StringComparison.Ordinal);
    }

    [Fact]
    public void RawSqlWarningPayload_AcceptsOnlyRawModeAndSafeAlias()
    {
        var warning = new CSharpDbRawSqlCaptureEnabledEvent("primary", SqlTextCaptureMode.Raw);

        Assert.Equal("primary", warning.DatabaseAlias);
        Assert.Equal(SqlTextCaptureMode.Raw, warning.SqlTextCaptureMode);
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new CSharpDbRawSqlCaptureEnabledEvent("primary", SqlTextCaptureMode.Normalized));
        Assert.Throws<ArgumentException>(() =>
            new CSharpDbRawSqlCaptureEnabledEvent("C:\\private\\database.db", SqlTextCaptureMode.Raw));
    }

    [Fact]
    public void Publisher_DoesNotAllocatePayloadWhenDisabled()
    {
        using var listener = new DiagnosticListener("CSharpDB.Tests.Disabled");
        var publisher = new CSharpDbDiagnosticEventPublisher(listener);
        bool factoryInvoked = false;

        publisher.Publish(
            CSharpDbLogEvents.QueryCompleted,
            () =>
            {
                factoryInvoked = true;
                return CreateCompletedEvent();
            });

        Assert.False(factoryInvoked);
        Assert.False(publisher.IsEnabled(CSharpDbLogEvents.QueryCompleted));
    }

    [Fact]
    public void Publisher_WritesTypedPayloadAndContainsSubscriberAndFactoryFailures()
    {
        using var listener = new DiagnosticListener("CSharpDB.Tests.Enabled");
        var publisher = new CSharpDbDiagnosticEventPublisher(listener);
        var received = new List<KeyValuePair<string, object?>>();
        using IDisposable subscription = listener.Subscribe(new CapturingObserver(received));

        publisher.Publish(CSharpDbLogEvents.QueryCompleted, CreateCompletedEvent);

        KeyValuePair<string, object?> item = Assert.Single(received);
        Assert.Equal(CSharpDbLogEvents.QueryCompleted.Name, item.Key);
        Assert.IsType<CSharpDbQueryCompletedEvent>(item.Value);
        Assert.True(publisher.IsEnabled(CSharpDbLogEvents.QueryCompleted));

        Exception? factoryException = Record.Exception(() => publisher.Publish(
            CSharpDbLogEvents.QueryCompleted,
            static () => throw new InvalidOperationException("subscriber-secret")));
        Assert.Null(factoryException);

        using var throwingListener = new DiagnosticListener("CSharpDB.Tests.Throwing");
        var throwingPublisher = new CSharpDbDiagnosticEventPublisher(throwingListener);
        using IDisposable throwingSubscription = throwingListener.Subscribe(new ThrowingObserver());

        Exception? subscriberException = Record.Exception(() =>
            throwingPublisher.Publish(CSharpDbLogEvents.QueryCompleted, CreateCompletedEvent));
        Assert.Null(subscriberException);
    }

    [Fact]
    public void CustomPublisher_IsNotRedirectedByTheGlobalBoundaryBuffer()
    {
        using var listener = new DiagnosticListener("CSharpDB.Tests.CustomBoundary");
        var publisher = new CSharpDbDiagnosticEventPublisher(listener);
        var received = new List<KeyValuePair<string, object?>>();
        using IDisposable subscription = listener.Subscribe(new CapturingObserver(received));

        using (CSharpDbOperationScope.EnterTransport(CSharpDbTransport.Direct))
        {
            publisher.Publish(CSharpDbLogEvents.QueryCompleted, CreateCompletedEvent);
            Assert.Single(received);
        }

        Assert.Single(received);
    }

    [Fact]
    public void SourceGeneratedContext_CoversAllPublicEventPayloads()
    {
        Type[] eventTypes =
        [
            typeof(CSharpDbQueryCompletedEvent),
            typeof(CSharpDbSlowQueryEvent),
            typeof(CSharpDbQueryFailedEvent),
            typeof(CSharpDbQueryCanceledEvent),
            typeof(CSharpDbLongRunningQueryEvent),
            typeof(CSharpDbHostStartingEvent),
            typeof(CSharpDbRawSqlCaptureEnabledEvent),
            typeof(CSharpDbLifecycleCompletedEvent),
            typeof(CSharpDbHealthTransitionEvent),
            typeof(CSharpDbApiErrorEvent),
        ];

        Assert.All(
            eventTypes,
            eventType => Assert.NotNull(CSharpDbObservabilityJsonContext.Default.GetTypeInfo(eventType)));
    }

    private static CSharpDbOperationContext CreateQueryContext()
        => CSharpDbOperationContext.CreateRoot(
            CSharpDbOperationClass.Query,
            CSharpDbTransport.Direct,
            "primary");

    private static CSharpDbQueryCompletedEvent CreateCompletedEvent()
        => new(
            CreateQueryContext(),
            new DateTimeOffset(2026, 8, 10, 12, 0, 2, TimeSpan.Zero),
            totalDuration: TimeSpan.FromSeconds(2),
            timeToFirstResult: TimeSpan.FromMilliseconds(100),
            queueDuration: TimeSpan.FromMilliseconds(5),
            executionAndConsumptionDuration: TimeSpan.FromMilliseconds(1_900),
            rowsProduced: 1,
            rowsAffected: 0);

    private sealed class CapturingObserver(List<KeyValuePair<string, object?>> received)
        : IObserver<KeyValuePair<string, object?>>
    {
        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }

        public void OnNext(KeyValuePair<string, object?> value) => received.Add(value);
    }

    private sealed class ThrowingObserver : IObserver<KeyValuePair<string, object?>>
    {
        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }

        public void OnNext(KeyValuePair<string, object?> value)
            => throw new InvalidOperationException("listener-secret");
    }
}
