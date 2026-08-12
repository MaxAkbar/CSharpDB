using System.Diagnostics;
using CSharpDB.Client.Internal;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Observability;
using CSharpDB.Sql;

namespace CSharpDB.Tests;

[Collection(ObservabilityDiagnosticsCollection.Name)]
public sealed class ActivityTracingTests
{
    private const string DatabaseAlias = "activity-tests";
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task LazyQuery_RemainsRunningDetached_AndStopsOnEarlyDispose()
    {
        const string canary = "trace-literal-secret-92bf";
        using var activities = new ActivityRecorder();
        await using Database database = await Database.OpenInMemoryAsync(
            CreateOptions(),
            Ct);
        await ExecuteNonQueryAsync(
            database,
            "CREATE TABLE traced_items (id INTEGER PRIMARY KEY, value TEXT)");
        await ExecuteNonQueryAsync(
            database,
            $"INSERT INTO traced_items VALUES (1, '{canary}')");
        activities.Clear();

        QueryResult result = await database.ExecuteAsync(
            $"SELECT value FROM traced_items WHERE value = '{canary}'",
            Ct);

        Assert.Null(Activity.Current);
        Activity started = Assert.Single(
            activities.Started(CSharpDbActivityNames.Query));
        Assert.Empty(activities.Stopped(CSharpDbActivityNames.Query));
        Assert.Equal(ActivityKind.Internal, started.Kind);
        Assert.Equal(default, started.ParentSpanId);

        using var unrelated = new Activity("unrelated-caller-work").Start();
        await result.DisposeAsync();
        Assert.Same(unrelated, Activity.Current);

        Activity stopped = Assert.Single(
            activities.Stopped(CSharpDbActivityNames.Query));
        Assert.Equal(ActivityStatusCode.Unset, stopped.Status);
        Assert.Equal("succeeded", Tag(stopped, "csharpdb.operation.outcome"));
        Assert.Equal("csharpdb", Tag(stopped, "db.system.name"));
        Assert.Equal(DatabaseAlias, Tag(stopped, "db.namespace"));
        Assert.Equal("QUERY", Tag(stopped, "db.operation.name"));
        Assert.StartsWith(
            QueryFingerprint.Algorithm + ":",
            Tag(stopped, "csharpdb.query.fingerprint"),
            StringComparison.Ordinal);
        Assert.Null(Tag(stopped, "db.query.text"));
        AssertCanaryAbsent(stopped, canary);
    }

    [Fact]
    public async Task LazyQueryExhaustion_RestoresTheReentryParentInsteadOfStoppedCreationParent()
    {
        using var activities = new ActivityRecorder();
        await using Database database = await Database.OpenInMemoryAsync(
            CreateOptions(),
            Ct);
        activities.Clear();
        Activity? previous = Activity.Current;
        var creationParent = new Activity("creation-parent").Start();
        QueryResult result = await database.ExecuteAsync("SELECT 1", Ct);
        creationParent.Stop();
        var reentryParent = new Activity("reentry-parent").Start();

        try
        {
            Assert.True(await result.MoveNextAsync(Ct));
            Assert.False(await result.MoveNextAsync(Ct));

            Assert.Same(reentryParent, Activity.Current);
            Assert.Single(activities.Stopped(CSharpDbActivityNames.Query));
        }
        finally
        {
            await result.DisposeAsync();
            if (ReferenceEquals(Activity.Current, reentryParent))
                reentryParent.Stop();
            else
                Activity.Current = previous;
            creationParent.Dispose();
            reentryParent.Dispose();
        }
    }

    [Fact]
    public async Task FailedQuery_IsChildOfCaller_AndUsesOnlySafeErrorTags()
    {
        const string canary = "missing_trace_secret_81ce";
        using var activities = new ActivityRecorder();
        await using Database database = await Database.OpenInMemoryAsync(
            CreateOptions(),
            Ct);
        activities.Clear();

        using var parent = new Activity("caller-parent").Start();
        ActivitySpanId expectedParentSpanId = parent.SpanId;
        await Assert.ThrowsAnyAsync<Exception>(
            () => database.ExecuteAsync(
                $"SELECT * FROM {canary}",
                Ct).AsTask());

        Assert.Same(parent, Activity.Current);
        Activity failed = Assert.Single(
            activities.Stopped(CSharpDbActivityNames.Query));
        Assert.Equal(expectedParentSpanId, failed.ParentSpanId);
        Assert.Equal(ActivityStatusCode.Error, failed.Status);
        Assert.Equal("failed", Tag(failed, "csharpdb.operation.outcome"));
        Assert.Equal("database_not_found", Tag(failed, "error.type"));
        Assert.Equal("csharpdb.not_found", Tag(failed, "csharpdb.error.code"));
        AssertCanaryAbsent(failed, canary);
    }

    [Fact]
    public async Task DirectClientAndEngine_AdoptOneLogicalQueryActivity()
    {
        string databasePath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_activity_{Guid.NewGuid():N}.db");
        using var activities = new ActivityRecorder();

        try
        {
            await using var client = new EngineTransportClient(
                databasePath,
                CreateOptions());
            CSharpDB.Client.Models.SqlExecutionResult result =
                await client.ExecuteSqlAsync("SELECT 1", Ct);

            Assert.Null(result.Error);
            Activity query = Assert.Single(
                activities.Stopped(CSharpDbActivityNames.Query));
            Assert.Equal("root", Tag(query, "csharpdb.operation.role"));
            Assert.Equal("direct", Tag(query, "csharpdb.transport"));
        }
        finally
        {
            foreach (string path in new[]
                     {
                         databasePath,
                         databasePath + ".wal",
                         databasePath + ".manifest.json",
                     })
            {
                if (File.Exists(path))
                    File.Delete(path);
            }
        }
    }

    [Fact]
    public async Task TransactionAndCheckpoint_TraceWithoutLoggingListeners()
    {
        using var activities = new ActivityRecorder();
        await using Database database = await Database.OpenInMemoryAsync(
            CreateOptions(),
            Ct);
        activities.Clear();

        await database.BeginTransactionAsync(Ct);
        Assert.Null(Activity.Current);
        Assert.Single(activities.Started(CSharpDbActivityNames.Transaction));
        Assert.Empty(activities.Stopped(CSharpDbActivityNames.Transaction));

        await database.CommitAsync(Ct);
        Activity transaction = Assert.Single(
            activities.Stopped(CSharpDbActivityNames.Transaction));
        Assert.Equal(ActivityStatusCode.Unset, transaction.Status);
        Assert.Equal(
            "succeeded",
            Tag(transaction, "csharpdb.operation.outcome"));

        activities.Clear();
        await database.CheckpointAsync(Ct);
        Activity checkpoint = Assert.Single(
            activities.Stopped(CSharpDbActivityNames.Checkpoint));
        Assert.Equal(ActivityKind.Internal, checkpoint.Kind);
        Assert.Equal(ActivityStatusCode.Unset, checkpoint.Status);
        Assert.Equal("checkpoint", Tag(checkpoint, "csharpdb.maintenance.kind"));
        Assert.Equal(
            "succeeded",
            Tag(checkpoint, "csharpdb.operation.outcome"));
    }

    [Fact]
    public void ShardedExactContext_RebindsTraceAndParentsExplicitAttempt()
    {
        const string canary = "sharded-cancel-secret-a5d1";
        const string sql = "SELECT * FROM shard_items WHERE id = 42";
        QueryFingerprint fingerprint =
            SqlQueryNormalizer.CreateFingerprint(sql, Ct);
        CSharpDbOperationContext original = CSharpDbOperationContext.CreateRoot(
            CSharpDbOperationClass.Query,
            CSharpDbTransport.Sharded,
            DatabaseAlias,
            queryFingerprint: fingerprint);
        Assert.Null(original.TraceId);

        using var activities = new ActivityRecorder();
        ClientOperationObservation coordinator = Assert.IsType<ClientOperationObservation>(
            ClientOperationObservation.StartQueryCoordinator(
                CreateObservabilityOptions(),
                sql,
                original));

        Assert.NotSame(original, coordinator.Context);
        Assert.Equal(original.OperationId, coordinator.Context.OperationId);
        Assert.Equal(original.ParentOperationId, coordinator.Context.ParentOperationId);
        Assert.Equal(original.StartedAtUtc, coordinator.Context.StartedAtUtc);
        Assert.Equal(original.StartingTimestamp, coordinator.Context.StartingTimestamp);
        Assert.Equal(original.QueryFingerprint, coordinator.Context.QueryFingerprint);
        Assert.Null(original.TraceId);
        Assert.NotNull(coordinator.Context.TraceId);

        using (coordinator.EnterScope())
        {
            ClientOperationObservation attempt = Assert.IsType<ClientOperationObservation>(
                coordinator.StartInternalAttempt(
                    CSharpDbTransport.Http,
                    "shard-a"));
            using (attempt.EnterScope())
                attempt.Succeed(rowsProduced: 1);

            coordinator.Fail(new OperationCanceledException(canary));
        }

        Activity[] stopped = activities.Stopped(CSharpDbActivityNames.Query);
        Assert.Equal(2, stopped.Length);
        Activity coordinatorActivity = Assert.Single(
            stopped,
            activity => Tag(activity, "csharpdb.operation.role") == "root");
        Activity attemptActivity = Assert.Single(
            stopped,
            activity => Tag(activity, "csharpdb.operation.role") == "internal");
        Assert.Equal(coordinatorActivity.SpanId, attemptActivity.ParentSpanId);
        Assert.Equal(
            coordinator.Context.OperationId.Value,
            Tag(attemptActivity, "csharpdb.operation.parent_id"));
        Assert.Null(Tag(attemptActivity, "csharpdb.parent_operation.id"));
        Assert.Equal(
            coordinatorActivity.TraceId.ToHexString(),
            coordinator.Context.TraceId!.Value);
        Assert.Equal(ActivityStatusCode.Error, coordinatorActivity.Status);
        Assert.Equal(
            "canceled",
            Tag(coordinatorActivity, "csharpdb.operation.outcome"));
        Assert.Equal("operation_canceled", Tag(coordinatorActivity, "error.type"));
        Assert.Equal(ActivityStatusCode.Unset, attemptActivity.Status);
        Assert.Equal("shard-a", Tag(attemptActivity, "csharpdb.database.alias"));
        AssertCanaryAbsent(coordinatorActivity, canary);
    }

    [Fact]
    public async Task ShardedDirectFanOut_AdoptsAttemptsWithoutEngineDuplicates()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_activity_shards_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        using var activities = new ActivityRecorder();

        try
        {
            var options = new CSharpDB.Client.CSharpDbShardingOptions
            {
                Keyspace = "activity",
                MapVersion = 1,
                VirtualBucketCount = 2,
                Shards =
                [
                    new CSharpDB.Client.CSharpDbShardDefinition
                    {
                        ShardId = "shard-0",
                        DataSource = Path.Combine(directory, "shard-0.db"),
                    },
                    new CSharpDB.Client.CSharpDbShardDefinition
                    {
                        ShardId = "shard-1",
                        DataSource = Path.Combine(directory, "shard-1.db"),
                    },
                ],
                BucketRanges =
                [
                    new CSharpDB.Client.CSharpDbShardBucketRange
                    {
                        StartBucketInclusive = 0,
                        EndBucketExclusive = 1,
                        ShardId = "shard-0",
                    },
                    new CSharpDB.Client.CSharpDbShardBucketRange
                    {
                        StartBucketInclusive = 1,
                        EndBucketExclusive = 2,
                        ShardId = "shard-1",
                    },
                ],
                DirectDatabaseOptions = CreateOptions(),
            };
            await using CSharpDB.Client.CSharpDbShardedClient client =
                await CSharpDB.Client.CSharpDbShardedClient.CreateAsync(
                    options,
                    ct: Ct);
            activities.Clear();

            IReadOnlyList<CSharpDB.Client.CSharpDbShardSqlExecutionResult> results =
                await client.ExecuteSqlOnAllShardsAsync("SELECT 1", Ct);

            Assert.Equal(2, results.Count);
            Assert.All(results, static result => Assert.Null(result.Error));
            Activity[] queries = activities.Stopped(CSharpDbActivityNames.Query);
            Assert.Equal(3, queries.Length);
            Activity coordinator = Assert.Single(
                queries,
                activity => Tag(activity, "csharpdb.operation.role") == "root");
            Activity[] attempts = queries
                .Where(activity =>
                    Tag(activity, "csharpdb.operation.role") == "internal")
                .ToArray();
            Assert.Equal(2, attempts.Length);
            Assert.DoesNotContain(
                queries,
                activity =>
                    Tag(activity, "csharpdb.operation.role") == "statement");
            Assert.All(attempts, attempt =>
            {
                Assert.Equal(coordinator.TraceId, attempt.TraceId);
                Assert.Equal(coordinator.SpanId, attempt.ParentSpanId);
                Assert.Equal("direct", Tag(attempt, "csharpdb.transport"));
            });
            Assert.Equal(
                ["shard-0", "shard-1"],
                attempts
                    .Select(activity =>
                        Tag(activity, "csharpdb.database.alias"))
                    .Order(StringComparer.Ordinal));
        }
        finally
        {
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public void CompletingOwner_DeambientsActiveDescendantWithoutRevivingStoppedOwner()
    {
        using var activities = new ActivityRecorder();
        Activity? previous = Activity.Current;
        ClientOperationObservation coordinator = Assert.IsType<ClientOperationObservation>(
            ClientOperationObservation.StartQueryCoordinator(
                CreateObservabilityOptions(),
                "SELECT 1"));
        IDisposable ownerScope = coordinator.EnterScope();
        var child = new Activity("caller-child").Start();
        bool childStopped = false;

        try
        {
            coordinator.Succeed(rowsProduced: 1);
            ownerScope.Dispose();

            Assert.Same(previous, Activity.Current);
            Assert.Equal(TimeSpan.Zero, child.Duration);
            Assert.Single(activities.Stopped(CSharpDbActivityNames.Query));

            child.Stop();
            childStopped = true;
            Assert.Same(previous, Activity.Current);
        }
        finally
        {
            if (!childStopped)
                child.Stop();
            Activity.Current = previous;
            ownerScope.Dispose();
        }
    }

    [Fact]
    public void CompletingOwner_NestedDescendantsDoNotDisplaceUnrelatedAmbientActivity()
    {
        using var activities = new ActivityRecorder();
        Activity? previous = Activity.Current;
        var unrelated = new Activity("unrelated-caller").Start();
        ClientOperationObservation coordinator = Assert.IsType<ClientOperationObservation>(
            ClientOperationObservation.StartQueryCoordinator(
                CreateObservabilityOptions(),
                "SELECT 1"));
        IDisposable ownerScope = coordinator.EnterScope();
        var child = new Activity("caller-child").Start();
        var grandchild = new Activity("caller-grandchild").Start();
        bool childStopped = false;
        bool grandchildStopped = false;

        try
        {
            coordinator.Succeed(rowsProduced: 1);
            ownerScope.Dispose();

            Assert.Same(unrelated, Activity.Current);
            Assert.Equal(TimeSpan.Zero, child.Duration);
            Assert.Equal(TimeSpan.Zero, grandchild.Duration);

            grandchild.Stop();
            grandchildStopped = true;
            Assert.Same(unrelated, Activity.Current);

            child.Stop();
            childStopped = true;
            Assert.Same(unrelated, Activity.Current);
            Assert.Single(activities.Stopped(CSharpDbActivityNames.Query));
        }
        finally
        {
            if (!grandchildStopped)
                grandchild.Stop();
            if (!childStopped)
                child.Stop();
            unrelated.Stop();
            Activity.Current = previous;
            ownerScope.Dispose();
        }
    }

    [Fact]
    public void MaintenanceSeam_UsesStableBackupRestoreAndMaintenanceTerminals()
    {
        using var activities = new ActivityRecorder();

        MaintenanceObservation backup = StartMaintenanceObservation(
            CSharpDbOperationClass.Backup);
        backup.Succeed(
            completedUnits: 16,
            totalUnits: 16,
            warningCount: 1);

        MaintenanceObservation restore = StartMaintenanceObservation(
            CSharpDbOperationClass.Restore);
        restore.Reject(SafeErrorKind.InvalidArgument);

        MaintenanceObservation maintenance = StartMaintenanceObservation(
            CSharpDbOperationClass.Maintenance);
        maintenance.Fail(new OperationCanceledException("maintenance-secret"));

        Activity backupActivity = Assert.Single(
            activities.Stopped("csharpdb.backup"));
        Assert.Equal(ActivityStatusCode.Unset, backupActivity.Status);
        Assert.Equal("succeeded", Tag(backupActivity, "csharpdb.operation.outcome"));
        Assert.Equal("16", Tag(backupActivity, "csharpdb.maintenance.completed_units"));
        Assert.Equal("1", Tag(backupActivity, "csharpdb.maintenance.warning_count"));

        Activity restoreActivity = Assert.Single(
            activities.Stopped("csharpdb.restore"));
        Assert.Equal(ActivityStatusCode.Error, restoreActivity.Status);
        Assert.Equal("rejected", Tag(restoreActivity, "csharpdb.operation.outcome"));
        Assert.Equal("invalid_argument", Tag(restoreActivity, "error.type"));

        Activity maintenanceActivity = Assert.Single(
            activities.Stopped("csharpdb.maintenance"));
        Assert.Equal(ActivityStatusCode.Error, maintenanceActivity.Status);
        Assert.Equal("canceled", Tag(maintenanceActivity, "csharpdb.operation.outcome"));
        Assert.Equal("operation_canceled", Tag(maintenanceActivity, "error.type"));
        AssertCanaryAbsent(maintenanceActivity, "maintenance-secret");
    }

    [Fact]
    public void NamedPipeTransport_UsesTheFrozenMetricCompatibleLiteral()
    {
        using var activities = new ActivityRecorder();
        CSharpDbOperationContext context = CSharpDbOperationContext.CreateRoot(
            CSharpDbOperationClass.Query,
            CSharpDbTransport.NamedPipe,
            DatabaseAlias);

        CSharpDbActivityOperation operation = Assert.IsType<CSharpDbActivityOperation>(
            CSharpDbActivityOperation.Start(tracingEnabled: true, context));
        operation.Complete(CSharpDbOperationOutcome.Succeeded);

        Activity activity = Assert.Single(
            activities.Stopped(CSharpDbActivityNames.Query));
        Assert.Equal("namedpipe", Tag(activity, "csharpdb.transport"));
        Assert.True(CSharpDbMetricTagNames.IsAllowedValue(
            CSharpDbMetricTagNames.Transport,
            Tag(activity, "csharpdb.transport")));
    }

    private static DatabaseOptions CreateOptions()
        => new() { ObservabilityOptions = CreateObservabilityOptions() };

    private static CSharpDbObservabilityOptions CreateObservabilityOptions()
        => new()
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
        };

    private static async ValueTask ExecuteNonQueryAsync(
        Database database,
        string sql)
    {
        await using QueryResult result = await database.ExecuteAsync(sql, Ct);
    }

    private static MaintenanceObservation StartMaintenanceObservation(
        CSharpDbOperationClass operationClass)
    {
        CSharpDbActivityOperation activityOperation =
            Assert.IsType<CSharpDbActivityOperation>(
                CSharpDbActivityOperation.Start(
                    operationClass,
                    operationClass,
                    static currentClass => CSharpDbOperationContext.CreateRoot(
                        currentClass,
                        CSharpDbTransport.Direct,
                        DatabaseAlias),
                    out CSharpDbOperationContext context));
        Assert.Null(Activity.Current);
        return new MaintenanceObservation(
            context,
            runtimeOperation: null,
            lifecycleOperation: null,
            activityOperation);
    }

    private static string? Tag(Activity activity, string name)
        => activity.TagObjects.FirstOrDefault(
            item => string.Equals(item.Key, name, StringComparison.Ordinal)).Value
            ?.ToString();

    private static void AssertCanaryAbsent(
        Activity activity,
        params string[] canaries)
    {
        string projection = string.Join(
            "|",
            activity.TagObjects.Select(
                static item => $"{item.Key}={item.Value}"));
        foreach (string canary in canaries)
        {
            Assert.DoesNotContain(
                canary,
                projection,
                StringComparison.OrdinalIgnoreCase);
        }
    }

    private static class CSharpDbActivityNames
    {
        internal const string Query = "csharpdb.query";
        internal const string Transaction = "csharpdb.transaction";
        internal const string Checkpoint = "csharpdb.checkpoint";
    }

    private sealed class ActivityRecorder : IDisposable
    {
        private readonly object _gate = new();
        private readonly List<Activity> _started = [];
        private readonly List<Activity> _stopped = [];
        private readonly ActivityListener _listener;

        internal ActivityRecorder()
        {
            _listener = new ActivityListener
            {
                ShouldListenTo = static source =>
                    source.Name == CSharpDbDiagnostics.ActivitySourceName,
                Sample = Sample,
                SampleUsingParentId = SampleUsingParentId,
                ActivityStarted = OnStarted,
                ActivityStopped = OnStopped,
            };
            ActivitySource.AddActivityListener(_listener);
        }

        internal Activity[] Started(string name)
        {
            lock (_gate)
            {
                return _started
                    .Where(activity => activity.OperationName == name)
                    .ToArray();
            }
        }

        internal Activity[] Stopped(string name)
        {
            lock (_gate)
            {
                return _stopped
                    .Where(activity => activity.OperationName == name)
                    .ToArray();
            }
        }

        internal void Clear()
        {
            lock (_gate)
            {
                _started.Clear();
                _stopped.Clear();
            }
        }

        public void Dispose() => _listener.Dispose();

        private static ActivitySamplingResult Sample(
            ref ActivityCreationOptions<ActivityContext> options)
            => ActivitySamplingResult.AllDataAndRecorded;

        private static ActivitySamplingResult SampleUsingParentId(
            ref ActivityCreationOptions<string> options)
            => ActivitySamplingResult.AllDataAndRecorded;

        private void OnStarted(Activity activity)
        {
            lock (_gate)
                _started.Add(activity);
        }

        private void OnStopped(Activity activity)
        {
            lock (_gate)
                _stopped.Add(activity);
        }
    }
}
