using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace CSharpDB.Observability;

/// <summary>
/// Owns one logical CSharpDB activity. The activity is deliberately detached
/// after creation so long-lived transactions and lazy query results never
/// become ambient during unrelated caller work.
/// </summary>
internal sealed class CSharpDbActivityOperation
{
    private readonly Activity _activity;
    private readonly bool _preserveAmbientOnStop;
    private int _completed;

    private CSharpDbActivityOperation(
        Activity activity,
        CSharpDbOperationContext context,
        bool preserveAmbientOnStop = false)
    {
        _activity = activity;
        _preserveAmbientOnStop = preserveAmbientOnStop;
        SetStartTags(activity, context);
    }

    internal Activity Activity => _activity;

    internal bool IsCompleted => Volatile.Read(ref _completed) != 0;

    internal static bool ShouldStart(bool tracingEnabled)
        => tracingEnabled && CSharpDbDiagnostics.ActivitySource.HasListeners();

    /// <summary>
    /// Starts an activity, creates its correlation context while that activity
    /// is current, and then restores the caller's previous ambient activity.
    /// Callers invoke this only after <see cref="ShouldStart"/> succeeds so the
    /// disabled path does not allocate a captured context factory.
    /// </summary>
    internal static CSharpDbActivityOperation? Start<TState>(
        CSharpDbOperationClass operationClass,
        TState state,
        Func<TState, CSharpDbOperationContext> createContext,
        out CSharpDbOperationContext context)
    {
        ArgumentNullException.ThrowIfNull(createContext);

        Activity? previous = Activity.Current;
        Activity? activity = null;
        try
        {
            activity = CSharpDbDiagnostics.ActivitySource.StartActivity(
                GetActivityName(operationClass),
                ActivityKind.Internal);
            context = createContext(state);
            return activity is null
                ? null
                : new CSharpDbActivityOperation(activity, context);
        }
        catch
        {
            try
            {
                activity?.Stop();
            }
            catch
            {
                // Listener failures are diagnostic work and must not strand
                // the ambient activity while context creation unwinds.
            }

            throw;
        }
        finally
        {
            Activity.Current = previous;
        }
    }

    /// <summary>
    /// Starts a root activity at an already-captured physical-operation time.
    /// The explicit empty parent prevents a storage callback thread's ambient
    /// activity from becoming an accidental parent.
    /// </summary>
    internal static CSharpDbActivityOperation? StartCapturedRoot<TState>(
        CSharpDbOperationClass operationClass,
        DateTimeOffset startedAtUtc,
        TState state,
        Func<TState, CSharpDbOperationContext> createContext,
        out CSharpDbOperationContext context)
    {
        ArgumentNullException.ThrowIfNull(createContext);

        Activity? previous = Activity.Current;
        Activity? activity = null;
        try
        {
            Activity.Current = null;
            activity = CSharpDbDiagnostics.ActivitySource.StartActivity(
                GetActivityName(operationClass),
                ActivityKind.Internal,
                parentContext: default,
                tags: null,
                links: null,
                startTime: startedAtUtc.ToUniversalTime());
            context = createContext(state);
            return activity is null
                ? null
                : new CSharpDbActivityOperation(
                    activity,
                    context,
                    preserveAmbientOnStop: true);
        }
        catch
        {
            try
            {
                activity?.Stop();
            }
            catch
            {
                // Listener failures are diagnostic work only.
            }

            throw;
        }
        finally
        {
            Activity.Current = previous;
        }
    }

    /// <summary>
    /// Starts an activity for an already-created exact context. Exact-context
    /// callers should prefer carrying an existing operation binding; this is
    /// the safe fallback for lifecycle-only seams.
    /// </summary>
    internal static CSharpDbActivityOperation? Start(
        bool tracingEnabled,
        CSharpDbOperationContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        if (!ShouldStart(tracingEnabled))
            return null;

        Activity? previous = Activity.Current;
        Activity? activity = null;
        try
        {
            activity = CSharpDbDiagnostics.ActivitySource.StartActivity(
                GetActivityName(context.OperationClass),
                ActivityKind.Internal);
            return activity is null
                ? null
                : new CSharpDbActivityOperation(activity, context);
        }
        catch
        {
            try
            {
                activity?.Stop();
            }
            catch
            {
                // Tracing is best effort.
            }

            return null;
        }
        finally
        {
            Activity.Current = previous;
        }
    }

    internal IDisposable WrapScope(IDisposable operationScope)
    {
        ArgumentNullException.ThrowIfNull(operationScope);
        if (IsCompleted || ReferenceEquals(Activity.Current, _activity))
            return operationScope;

        Activity? previous = Activity.Current;
        Activity.Current = _activity;
        return new ActivityScope(this, operationScope, previous);
    }

    internal void Complete(
        CSharpDbOperationOutcome outcome,
        SafeErrorProjection? error = null)
        => CompleteCore(outcome, error, beforeStop: null, completedAtUtc: null);

    internal void CompleteAt(
        CSharpDbOperationOutcome outcome,
        SafeErrorProjection? error,
        DateTimeOffset completedAtUtc)
        => CompleteCore(outcome, error, beforeStop: null, completedAtUtc);

    /// <summary>
    /// Stops an activity whose diagnostic setup could not be retained without
    /// projecting that setup failure as a database-operation failure.
    /// </summary>
    internal void Abandon()
    {
        if (Interlocked.Exchange(ref _completed, 1) != 0)
            return;

        try
        {
            RegisterOutlivingAmbientDescendant();
            StopActivity();
        }
        catch
        {
            // Tracing teardown is best effort only.
        }
    }

    internal void CompleteQuery(
        CSharpDbOperationOutcome outcome,
        SafeErrorProjection? error,
        long rowsProduced,
        long rowsAffected,
        TimeSpan queueDuration,
        TimeSpan? timeToFirstResult,
        bool isSlow)
        => CompleteCore(
            outcome,
            error,
            activity =>
            {
                activity.SetTag("csharpdb.query.rows_produced", rowsProduced);
                activity.SetTag("csharpdb.query.rows_affected", rowsAffected);
                activity.SetTag(
                    "csharpdb.query.queue_duration_ms",
                    queueDuration.TotalMilliseconds);
                if (timeToFirstResult is TimeSpan firstResult)
                {
                    activity.SetTag(
                        "csharpdb.query.time_to_first_result_ms",
                        firstResult.TotalMilliseconds);
                }

                activity.SetTag("csharpdb.query.slow", isSlow);
            },
            completedAtUtc: null);

    internal void CompleteMaintenance(
        CSharpDbOperationOutcome outcome,
        SafeErrorProjection? error,
        long? completedUnits,
        long? totalUnits,
        int warningCount,
        int errorCount)
        => CompleteCore(
            outcome,
            error,
            activity =>
            {
                if (completedUnits is long completed)
                    activity.SetTag("csharpdb.maintenance.completed_units", completed);
                if (totalUnits is long total)
                    activity.SetTag("csharpdb.maintenance.total_units", total);
                activity.SetTag("csharpdb.maintenance.warning_count", warningCount);
                activity.SetTag("csharpdb.maintenance.error_count", errorCount);
            },
            completedAtUtc: null);

    private void CompleteCore(
        CSharpDbOperationOutcome outcome,
        SafeErrorProjection? error,
        Action<Activity>? beforeStop,
        DateTimeOffset? completedAtUtc)
    {
        if (Interlocked.Exchange(ref _completed, 1) != 0)
            return;

        try
        {
            beforeStop?.Invoke(_activity);
            _activity.SetTag(
                "csharpdb.operation.outcome",
                GetOutcomeValue(outcome));
            if (outcome != CSharpDbOperationOutcome.Succeeded)
            {
                SafeErrorProjection safeError = error ??
                    SafeErrorProjector.Project(
                        outcome == CSharpDbOperationOutcome.Canceled
                            ? SafeErrorKind.OperationCanceled
                            : SafeErrorKind.DatabaseOperation);
                _activity.SetTag("error.type", safeError.ErrorType);
                _activity.SetTag("csharpdb.error.code", safeError.Code);
                _activity.SetStatus(ActivityStatusCode.Error);
            }

            if (completedAtUtc is DateTimeOffset capturedCompletion)
            {
                DateTimeOffset safeCompletion = capturedCompletion.ToUniversalTime();
                if (safeCompletion < _activity.StartTimeUtc)
                    safeCompletion = _activity.StartTimeUtc;
                _activity.SetEndTime(safeCompletion.UtcDateTime);
            }

            RegisterOutlivingAmbientDescendant();
            StopActivity();
        }
        catch
        {
            // Activity listeners and exporters must not alter the observed
            // database operation's terminal result.
            try
            {
                RegisterOutlivingAmbientDescendant();
                StopActivity();
            }
            catch
            {
                // Best effort only.
            }
        }
    }

    private void RegisterOutlivingAmbientDescendant()
    {
        if (IsDescendantOf(Activity.Current, _activity))
            CompletedOwnerAmbientGuard.Register(_activity);
    }

    private void StopActivity()
    {
        if (!_preserveAmbientOnStop)
        {
            _activity.Stop();
            return;
        }

        Activity? ambient = Activity.Current;
        try
        {
            _activity.Stop();
        }
        finally
        {
            if (!ReferenceEquals(Activity.Current, ambient))
                Activity.Current = ambient;
        }
    }

    private static void SetStartTags(
        Activity activity,
        CSharpDbOperationContext context)
    {
        activity.SetTag("db.system.name", "csharpdb");
        activity.SetTag("db.namespace", context.DatabaseAlias);
        activity.SetTag(
            "db.operation.name",
            GetDatabaseOperationName(context.OperationClass));
        activity.SetTag("csharpdb.schema.version", CSharpDbDiagnostics.SchemaVersion);
        activity.SetTag("csharpdb.operation.id", context.OperationId.Value);
        if (context.ParentOperationId is OpaqueDiagnosticsId parentOperationId)
        {
            activity.SetTag(
                "csharpdb.operation.parent_id",
                parentOperationId.Value);
        }

        activity.SetTag(
            "csharpdb.operation.class",
            GetOperationClassValue(context.OperationClass));
        activity.SetTag(
            "csharpdb.operation.role",
            GetOperationRoleValue(context.Role));
        activity.SetTag(
            "csharpdb.transport",
            GetTransportValue(context.Transport));
        activity.SetTag("csharpdb.database.alias", context.DatabaseAlias);
        if (context.SessionId is OpaqueDiagnosticsId sessionId)
            activity.SetTag("csharpdb.session.id", sessionId.Value);
        if (context.QueryFingerprint is QueryFingerprint fingerprint)
            activity.SetTag("csharpdb.query.fingerprint", fingerprint.Value);
        if (context.OperationClass is
            CSharpDbOperationClass.Checkpoint or
            CSharpDbOperationClass.Backup or
            CSharpDbOperationClass.Restore or
            CSharpDbOperationClass.Reindex or
            CSharpDbOperationClass.Vacuum or
            CSharpDbOperationClass.Maintenance)
        {
            activity.SetTag(
                "csharpdb.maintenance.kind",
                GetOperationClassValue(context.OperationClass));
        }
    }

    private static string GetActivityName(CSharpDbOperationClass operationClass)
        => operationClass switch
        {
            CSharpDbOperationClass.Query => "csharpdb.query",
            CSharpDbOperationClass.Script => "csharpdb.script",
            CSharpDbOperationClass.Procedure => "csharpdb.procedure",
            CSharpDbOperationClass.Transaction => "csharpdb.transaction",
            CSharpDbOperationClass.Database => "csharpdb.database",
            CSharpDbOperationClass.Recovery => "csharpdb.recovery",
            CSharpDbOperationClass.Checkpoint => "csharpdb.checkpoint",
            CSharpDbOperationClass.Backup => "csharpdb.backup",
            CSharpDbOperationClass.Restore => "csharpdb.restore",
            CSharpDbOperationClass.Reindex => "csharpdb.reindex",
            CSharpDbOperationClass.Vacuum => "csharpdb.vacuum",
            CSharpDbOperationClass.Maintenance => "csharpdb.maintenance",
            CSharpDbOperationClass.Pipeline => "csharpdb.pipeline",
            _ => "csharpdb.operation",
        };

    private static string GetDatabaseOperationName(
        CSharpDbOperationClass operationClass)
        => operationClass switch
        {
            CSharpDbOperationClass.Query => "QUERY",
            CSharpDbOperationClass.Script => "SCRIPT",
            CSharpDbOperationClass.Procedure => "CALL",
            CSharpDbOperationClass.Transaction => "TRANSACTION",
            CSharpDbOperationClass.Database => "DATABASE",
            CSharpDbOperationClass.Recovery => "RECOVERY",
            CSharpDbOperationClass.Checkpoint => "CHECKPOINT",
            CSharpDbOperationClass.Backup => "BACKUP",
            CSharpDbOperationClass.Restore => "RESTORE",
            CSharpDbOperationClass.Reindex => "REINDEX",
            CSharpDbOperationClass.Vacuum => "VACUUM",
            CSharpDbOperationClass.Maintenance => "MAINTENANCE",
            CSharpDbOperationClass.Pipeline => "PIPELINE",
            _ => "OPERATION",
        };

    private static string GetOperationClassValue(
        CSharpDbOperationClass operationClass)
        => operationClass switch
        {
            CSharpDbOperationClass.Query => "query",
            CSharpDbOperationClass.Script => "script",
            CSharpDbOperationClass.Procedure => "procedure",
            CSharpDbOperationClass.Transaction => "transaction",
            CSharpDbOperationClass.Database => "database",
            CSharpDbOperationClass.Recovery => "recovery",
            CSharpDbOperationClass.Checkpoint => "checkpoint",
            CSharpDbOperationClass.Backup => "backup",
            CSharpDbOperationClass.Restore => "restore",
            CSharpDbOperationClass.Reindex => "reindex",
            CSharpDbOperationClass.Vacuum => "vacuum",
            CSharpDbOperationClass.Maintenance => "maintenance",
            CSharpDbOperationClass.Pipeline => "pipeline",
            _ => "unknown",
        };

    private static string GetOperationRoleValue(CSharpDbOperationRole role)
        => role switch
        {
            CSharpDbOperationRole.Root => "root",
            CSharpDbOperationRole.Request => "request",
            CSharpDbOperationRole.Statement => "statement",
            CSharpDbOperationRole.Internal => "internal",
            _ => "unknown",
        };

    private static string GetTransportValue(CSharpDbTransport transport)
        => transport switch
        {
            CSharpDbTransport.Embedded => "embedded",
            CSharpDbTransport.Direct => "direct",
            CSharpDbTransport.Http => "http",
            CSharpDbTransport.Grpc => "grpc",
            CSharpDbTransport.Tcp => "tcp",
            CSharpDbTransport.NamedPipe => "namedpipe",
            CSharpDbTransport.Sharded => "sharded",
            _ => "unknown",
        };

    private static string GetOutcomeValue(CSharpDbOperationOutcome outcome)
        => outcome switch
        {
            CSharpDbOperationOutcome.Succeeded => "succeeded",
            CSharpDbOperationOutcome.Failed => "failed",
            CSharpDbOperationOutcome.Canceled => "canceled",
            CSharpDbOperationOutcome.Rejected => "rejected",
            _ => "unknown",
        };

    private sealed class ActivityScope : IDisposable
    {
        private CSharpDbActivityOperation? _owner;
        private IDisposable? _operationScope;
        private readonly Activity? _previous;

        internal ActivityScope(
            CSharpDbActivityOperation owner,
            IDisposable operationScope,
            Activity? previous)
        {
            _owner = owner;
            _operationScope = operationScope;
            _previous = previous;
        }

        public void Dispose()
        {
            CSharpDbActivityOperation? owner = Interlocked.Exchange(
                ref _owner,
                null);
            if (owner is null)
                return;

            try
            {
                Activity? current = Activity.Current;
                bool completedDescendant = owner.IsCompleted &&
                    IsDescendantOf(current, owner._activity);
                if (completedDescendant)
                {
                    CompletedOwnerAmbientGuard.Register(owner._activity);
                }

                if (ReferenceEquals(current, owner._activity) ||
                    (owner.IsCompleted &&
                     (current is null ||
                      ReferenceEquals(current, owner._activity.Parent) ||
                       completedDescendant)))
                {
                    Activity.Current = _previous;
                }
            }
            finally
            {
                Interlocked.Exchange(ref _operationScope, null)?.Dispose();
            }
        }

    }

    private static bool IsDescendantOf(
        Activity? candidate,
        Activity ancestor)
    {
        for (Activity? current = candidate?.Parent;
             current is not null;
             current = current.Parent)
        {
            if (ReferenceEquals(current, ancestor))
                return true;
        }

        return false;
    }

    /// <summary>
    /// A caller-owned Activity can outlive its stopped CSharpDB parent. The BCL
    /// remembers that private parent when the child starts and restores it when
    /// the child later stops, even if the child was no longer ambient. Register
    /// only that rare completed-parent case and redirect the attempted revival
    /// to the ambient Activity that existed immediately before it.
    /// </summary>
    private static class CompletedOwnerAmbientGuard
    {
        private static readonly ConditionalWeakTable<Activity, Marker>
            s_completedOwners = new();

        [ThreadStatic]
        private static bool t_restoring;

        static CompletedOwnerAmbientGuard()
            => Activity.CurrentChanged += OnActivityCurrentChanged;

        internal static void Register(Activity owner)
            => s_completedOwners.GetValue(
                owner,
                static _ => new Marker());

        private static void OnActivityCurrentChanged(
            object? sender,
            ActivityChangedEventArgs args)
        {
            if (t_restoring || args.Current is null)
                return;

            for (Activity? current = args.Current;
                 current is not null;
                 current = current.Parent)
            {
                if (!s_completedOwners.TryGetValue(current, out _))
                    continue;

                t_restoring = true;
                try
                {
                    Activity.Current = args.Previous;
                }
                finally
                {
                    t_restoring = false;
                }

                return;
            }
        }

        private sealed class Marker
        {
        }
    }
}
