using System.Collections.Concurrent;
using System.Data.Common;
using System.Diagnostics;
using CSharpDB.Data;
using CSharpDB.Engine;
using CSharpDB.Observability;
using CSharpDB.Sql;

namespace CSharpDB.Data.Tests;

[Collection(ObservabilityDiagnosticsCollection.Name)]
public sealed class AdoActivityTracingTests : IAsyncLifetime
{
    private const string DatabaseAlias = "ado-activity-tests";
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
        => await CSharpDbConnection.ClearAllPoolsAsync();

    public async ValueTask DisposeAsync()
        => await CSharpDbConnection.ClearAllPoolsAsync();

    [Fact]
    public async Task DirectCommand_AdoptsOneBoundaryActivityAndCorrelatesHistory()
    {
        using var activities = new ActivityRecorder();
        await using var connection = new CSharpDbConnection(
            "Data Source=:memory:;Pooling=false",
            CreateOptions());
        await connection.OpenAsync(Ct);
        activities.Clear();

        using var parent = new Activity("ado-caller-parent").Start();
        await using CSharpDbCommand command =
            (CSharpDbCommand)connection.CreateCommand();
        command.CommandText = "SELECT 42";

        Assert.Equal(42L, Convert.ToInt64(
            await command.ExecuteScalarAsync(Ct)));
        Assert.Same(parent, Activity.Current);

        Activity query = Assert.Single(activities.Stopped("csharpdb.query"));
        Assert.Equal(parent.SpanId, query.ParentSpanId);
        Assert.Equal("direct", Tag(query, "csharpdb.transport"));
        Assert.Equal("root", Tag(query, "csharpdb.operation.role"));

        DirectDatabaseSession session = Assert.IsType<DirectDatabaseSession>(
            connection.GetSession());
        object state = Assert.IsAssignableFrom<object>(
            AdoCommandObservation.GetRuntimeDiagnosticsStateForTest(session));
        QueryFingerprint fingerprint =
            SqlQueryFingerprintProvider.Instance.CreateFingerprint(
                command.CommandText,
                Ct);
        IReadOnlyList<RecentQuerySnapshot> records =
            Assert.IsAssignableFrom<IReadOnlyList<RecentQuerySnapshot>>(
                AdoCommandObservation.CaptureRecentQueriesForTest(state).Records);
        RecentQuerySnapshot recent = Assert.Single(
            records,
            record => record.Fingerprint == fingerprint);
        Assert.Equal(query.TraceId.ToHexString(), recent.TraceId?.Value);
        Assert.Equal(
            recent.OperationId.Value,
            Tag(query, "csharpdb.operation.id"));
    }

    [Fact]
    public async Task MissingParameter_StopsOneSafeErrorActivityBeforeDispatch()
    {
        const string canary = "missing_parameter_secret_71bd";
        using var activities = new ActivityRecorder();
        await using var connection = new CSharpDbConnection(
            "Data Source=:memory:;Pooling=false",
            CreateOptions());
        await connection.OpenAsync(Ct);
        activities.Clear();

        await using CSharpDbCommand command =
            (CSharpDbCommand)connection.CreateCommand();
        command.CommandText = $"SELECT @{canary}";

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => command.ExecuteScalarAsync(Ct));

        Assert.Null(Activity.Current);
        Activity failed = Assert.Single(
            activities.Stopped("csharpdb.query"));
        Assert.Equal(ActivityStatusCode.Error, failed.Status);
        Assert.Equal("failed", Tag(failed, "csharpdb.operation.outcome"));
        Assert.Equal("invalid_argument", Tag(failed, "error.type"));
        Assert.Equal("invalid_argument", Tag(failed, "csharpdb.error.code"));
        Assert.DoesNotContain(
            failed.TagObjects,
            tag => tag.Key.Contains("exception", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(
            canary,
            string.Join(
                "|",
                failed.TagObjects.Select(
                    static tag => $"{tag.Key}={tag.Value}")),
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void DisposeBeforeDispatch_StopsActivityWithoutInventingFailure()
    {
        CSharpDbObservabilityOptions options =
            CreateOptions().ObservabilityOptions!;
        using IDisposable state =
            AdoCommandObservation.CreateRuntimeDiagnosticsStateForTest(options);
        using var activities = new ActivityRecorder();
        Activity? previous = Activity.Current;
        using AdoCommandObservation observation =
            Assert.IsType<AdoCommandObservation>(
                AdoCommandObservation.TryStartForTest(
                    options,
                    state,
                    "SELECT 1",
                    OpaqueDiagnosticsId.Create()));

        Assert.NotSame(previous, Activity.Current);
        observation.Dispose();

        Assert.Same(previous, Activity.Current);
        Activity abandoned = Assert.Single(
            activities.Stopped("csharpdb.query"));
        Assert.Equal(ActivityStatusCode.Unset, abandoned.Status);
        Assert.Null(Tag(abandoned, "csharpdb.operation.outcome"));
    }

    private static DatabaseOptions CreateOptions()
        => new()
        {
            ObservabilityOptions = new CSharpDbObservabilityOptions
            {
                Enabled = true,
                DatabaseAlias = DatabaseAlias,
                Logging = new CSharpDbLoggingOptions
                {
                    Enabled = false,
                    Queries = false,
                    SlowQueries = false,
                    SqlText = SqlTextCaptureMode.Raw,
                },
                OpenTelemetry = new CSharpDbOpenTelemetryOptions
                {
                    Enabled = true,
                },
            },
        };

    private static string? Tag(Activity activity, string name)
        => activity.TagObjects.FirstOrDefault(
            item => string.Equals(item.Key, name, StringComparison.Ordinal)).Value
            ?.ToString();

    private sealed class ActivityRecorder : IDisposable
    {
        private readonly ConcurrentQueue<Activity> _stopped = new();
        private readonly ActivityListener _listener;

        internal ActivityRecorder()
        {
            _listener = new ActivityListener
            {
                ShouldListenTo = static source =>
                    source.Name == CSharpDbDiagnostics.ActivitySourceName,
                Sample = static (ref ActivityCreationOptions<ActivityContext> _) =>
                    ActivitySamplingResult.AllDataAndRecorded,
                SampleUsingParentId = static (
                    ref ActivityCreationOptions<string> _) =>
                    ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = _stopped.Enqueue,
            };
            ActivitySource.AddActivityListener(_listener);
        }

        internal Activity[] Stopped(string name)
            => _stopped
                .Where(activity => activity.OperationName == name)
                .ToArray();

        internal void Clear() => _stopped.Clear();

        public void Dispose() => _listener.Dispose();
    }
}
