using System.Collections.Concurrent;
using CSharpDB.Client;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Observability;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Data;

internal static class CSharpDbConnectionPoolRegistry
{
    private const int RetiredDiagnosticsCapacity =
        CSharpDbObservabilityOptions.MaximumHistoryCapacity;
    private static readonly ConcurrentDictionary<PoolKey, CSharpDbConnectionPool> s_pools = new();
    private static readonly ConcurrentDictionary<CSharpDbConnectionPool, byte>
        s_diagnosticPools = new();
    private static readonly ConcurrentDictionary<DirectDatabaseSession, byte>
        s_directDiagnosticSessions = new();
    private static readonly SemaphoreSlim s_gate = new(1, 1);
    private static readonly StringComparer s_pathComparer =
        OperatingSystem.IsWindows() ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal;
    private static readonly Dictionary<string, Task> s_retiringPools = new(s_pathComparer);
    private static readonly Dictionary<string, int> s_directLeaseCounts = new(s_pathComparer);
    private static readonly Dictionary<string, FileDeletionReservation> s_fileDeletionReservations =
        new(s_pathComparer);
    private static readonly object s_retiredDiagnosticsGate = new();
    private static readonly Queue<DataConnectionDiagnosticsRawSnapshot>
        s_retiredDiagnostics = new();
    private static readonly HashSet<CSharpDbConnectionPool>
        s_observedRetirements = new(ReferenceEqualityComparer.Instance);
    private static long s_retiredDiagnosticsDroppedCount;

    internal static async ValueTask<PooledDatabaseSession> OpenPooledSessionAsync(
        PoolKey key,
        Func<CancellationToken, ValueTask<Database>> openDatabaseAsync,
        CSharpDbObservabilityOptions? observabilityOptionsSnapshot,
        CancellationToken cancellationToken,
        TimeProvider? diagnosticsTimeProvider = null,
        DataRuntimeDiagnosticsStateOwner? runtimeDiagnosticsStateOwner = null)
    {
        while (true)
        {
            if (runtimeDiagnosticsStateOwner is { IsDisposed: true })
                throw new CSharpDbConnectionPoolRetiredException();

            // An existing pool arbitrates checkout against disable/retirement with
            // its own gate. This avoids the registry-wide gate on the steady-state
            // path without weakening direct-lease exclusion: a direct reservation
            // must disable this pool while idle, or fail while a checkout is active.
            if (s_pools.TryGetValue(key, out CSharpDbConnectionPool? existingPool))
            {
                try
                {
                    PooledDatabaseSession session =
                        await existingPool.OpenSessionAsync(cancellationToken);
                    if (runtimeDiagnosticsStateOwner is not null)
                    {
                        DisposeUnusedRuntimeDiagnosticsStateOwner(
                            runtimeDiagnosticsStateOwner,
                            existingPool.RuntimeDiagnosticsStateOwner);
                    }
                    return session;
                }
                catch (CSharpDbConnectionPoolRetiredException)
                {
                    await EvictDisabledPoolAsync(existingPool);
                    if (runtimeDiagnosticsStateOwner is not null &&
                        ReferenceEquals(
                            runtimeDiagnosticsStateOwner,
                            existingPool.RuntimeDiagnosticsStateOwner))
                    {
                        // This plan belonged to the family that just retired.
                        // Its state cannot truthfully be reused by the replacement;
                        // finish retirement so the cached plan is marked stale and
                        // let the caller re-resolve on its next open attempt.
                        await existingPool.Retirement;
                        throw;
                    }

                    // An unadopted resolver-created candidate can still be
                    // adopted by the replacement family created on this retry.
                    continue;
                }
                catch
                {
                    DisposeUnusedRuntimeDiagnosticsStateOwner(
                        runtimeDiagnosticsStateOwner,
                        existingPool.RuntimeDiagnosticsStateOwner);
                    throw;
                }
            }

            CSharpDbConnectionPool pool;
            try
            {
                pool = await GetOrCreateAsync(
                    key,
                    openDatabaseAsync,
                    observabilityOptionsSnapshot,
                    cancellationToken,
                    diagnosticsTimeProvider,
                    runtimeDiagnosticsStateOwner);
            }
            catch
            {
                runtimeDiagnosticsStateOwner?.Dispose();
                throw;
            }

            try
            {
                PooledDatabaseSession session =
                    await pool.OpenSessionAsync(cancellationToken);
                if (runtimeDiagnosticsStateOwner is not null)
                {
                    DisposeUnusedRuntimeDiagnosticsStateOwner(
                        runtimeDiagnosticsStateOwner,
                        pool.RuntimeDiagnosticsStateOwner);
                }
                return session;
            }
            catch (CSharpDbConnectionPoolRetiredException)
            {
                await EvictDisabledPoolAsync(pool);
                if (runtimeDiagnosticsStateOwner is not null &&
                    ReferenceEquals(
                        runtimeDiagnosticsStateOwner,
                        pool.RuntimeDiagnosticsStateOwner))
                {
                    await pool.Retirement;
                    throw;
                }

                // An unadopted candidate remains available for the replacement
                // family created on the next iteration.
            }
            catch
            {
                DisposeUnusedRuntimeDiagnosticsStateOwner(
                    runtimeDiagnosticsStateOwner,
                    pool.RuntimeDiagnosticsStateOwner);
                throw;
            }
        }
    }

    private static async ValueTask<CSharpDbConnectionPool> GetOrCreateAsync(
        PoolKey key,
        Func<CancellationToken, ValueTask<Database>> openDatabaseAsync,
        CSharpDbObservabilityOptions? observabilityOptionsSnapshot,
        CancellationToken cancellationToken,
        TimeProvider? diagnosticsTimeProvider,
        DataRuntimeDiagnosticsStateOwner? runtimeDiagnosticsStateOwner)
    {
        ArgumentNullException.ThrowIfNull(openDatabaseAsync);

        while (true)
        {
            Task? retirementTask = null;
            IDisposable? lifecycleBoundary = null;
            bool registryGateHeld = false;
            try
            {
                while (!await TryAcquireRegistryGateForCloseAsync(
                           cancellationToken,
                           lifecycleBoundary is not null,
                           dataSource: key.DataSource))
                {
                    lifecycleBoundary ??=
                        DataLifecycleDiagnosticBoundary.EnterEnabledBoundary(
                            OpaqueDiagnosticsId.Create());
                }
                registryGateHeld = true;

                ThrowIfFileDeletionReserved(key.DataSource);

                if (s_directLeaseCounts.TryGetValue(key.DataSource, out int directLeaseCount) &&
                    directLeaseCount > 0)
                {
                    throw new InvalidOperationException(
                        "Cannot open a pooled embedded connection while non-pooled connections for the same data source are open.");
                }

                if (s_retiringPools.TryGetValue(key.DataSource, out Task? retiring))
                {
                    if (retiring.IsCompletedSuccessfully)
                        s_retiringPools.Remove(key.DataSource);
                    else
                        retirementTask = retiring;
                }

                if (retirementTask is null)
                {
                    if (s_pools.TryGetValue(key, out CSharpDbConnectionPool? existing))
                    {
                        return existing;
                    }

                    KeyValuePair<PoolKey, CSharpDbConnectionPool>[] incompatiblePools = s_pools
                        .Where(pair =>
                            s_pathComparer.Equals(pair.Key.DataSource, key.DataSource) &&
                            !pair.Key.Equals(key))
                        .ToArray();

                    foreach ((PoolKey incompatibleKey, CSharpDbConnectionPool incompatiblePool) in incompatiblePools)
                    {
                        bool disabled;
                        try
                        {
                            disabled = await incompatiblePool.TryDisableIfIdleAsync();
                        }
                        catch
                        {
                            s_pools.TryRemove(new KeyValuePair<PoolKey, CSharpDbConnectionPool>(
                                incompatibleKey,
                                incompatiblePool));
                            RegisterRetirement(incompatibleKey.DataSource, incompatiblePool);
                            throw;
                        }

                        if (!disabled)
                        {
                            throw new InvalidOperationException(
                                "Cannot change pooled embedded database configuration while connections for the same data source are open.");
                        }

                        s_pools.TryRemove(new KeyValuePair<PoolKey, CSharpDbConnectionPool>(
                            incompatibleKey,
                            incompatiblePool));
                    }

                    var created = new CSharpDbConnectionPool(
                        key,
                        key.MaxPoolSize,
                        openDatabaseAsync,
                        observabilityOptionsSnapshot,
                        diagnosticsTimeProvider,
                        runtimeDiagnosticsStateOwner);
                    if (!s_pools.TryAdd(key, created))
                        return s_pools[key];

                    TryRegisterDiagnosticPool(created);

                    return created;
                }
            }
            finally
            {
                if (registryGateHeld)
                    s_gate.Release();
                lifecycleBoundary?.Dispose();
            }

            await retirementTask.WaitAsync(cancellationToken);
        }
    }

    internal static async ValueTask<DirectDatabaseSession> OpenDirectSessionAsync(
        string dataSource,
        Func<CancellationToken, ValueTask<Database>> openDatabaseAsync,
        CSharpDbObservabilityOptions? observabilityOptionsSnapshot,
        CancellationToken cancellationToken,
        TimeProvider? diagnosticsTimeProvider = null,
        DataRuntimeDiagnosticsStateOwner? runtimeDiagnosticsStateOwner = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(dataSource);
        ArgumentNullException.ThrowIfNull(openDatabaseAsync);

        await ReserveDirectLeaseAsync(dataSource, cancellationToken);
        try
        {
            Database database = await openDatabaseAsync(cancellationToken);
            var session = new DirectDatabaseSession(
                database,
                directDatabase => DisposeDirectDatabaseAsync(dataSource, directDatabase),
                observabilityOptionsSnapshot,
                diagnosticsTimeProvider,
                runtimeDiagnosticsStateOwner,
                UnregisterDirectDiagnosticsSession);
            TryRegisterDirectDiagnosticsSession(session);
            return session;
        }
        catch
        {
            try
            {
                await ReleaseDirectLeaseAsync(dataSource);
            }
            finally
            {
                runtimeDiagnosticsStateOwner?.Dispose();
            }
            throw;
        }
    }

    private static void DisposeUnusedRuntimeDiagnosticsStateOwner(
        DataRuntimeDiagnosticsStateOwner? candidate,
        DataRuntimeDiagnosticsStateOwner? adopted)
    {
        if (candidate is not null && !ReferenceEquals(candidate, adopted))
            candidate.Dispose();
    }

    internal static async ValueTask ClearPoolAsync(PoolKey key)
    {
        IDisposable? lifecycleBoundary = null;
        bool registryGateHeld = false;
        try
        {
            while (!await TryAcquireRegistryGateForCloseAsync(
                       CancellationToken.None,
                       lifecycleBoundary is not null,
                       exactKey: key))
            {
                lifecycleBoundary ??=
                    DataLifecycleDiagnosticBoundary.EnterEnabledBoundary(
                        OpaqueDiagnosticsId.Create());
            }
            registryGateHeld = true;

            if (s_pools.TryRemove(key, out CSharpDbConnectionPool? pool))
            {
                try
                {
                    await pool.DisableAsync();
                }
                finally
                {
                    RegisterRetirement(key.DataSource, pool);
                }
            }
        }
        finally
        {
            if (registryGateHeld)
                s_gate.Release();
            lifecycleBoundary?.Dispose();
        }
    }

    internal static async ValueTask ClearPoolsAsync(Func<PoolKey, bool> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);

        IDisposable? lifecycleBoundary = null;
        bool registryGateHeld = false;
        List<Exception>? errors = null;
        try
        {
            while (!await TryAcquireRegistryGateForCloseAsync(
                       CancellationToken.None,
                       lifecycleBoundary is not null))
            {
                lifecycleBoundary ??=
                    DataLifecycleDiagnosticBoundary.EnterEnabledBoundary(
                        OpaqueDiagnosticsId.Create());
            }
            registryGateHeld = true;

            KeyValuePair<PoolKey, CSharpDbConnectionPool>[] matches = s_pools
                .Where(pair => predicate(pair.Key))
                .ToArray();

            foreach ((PoolKey key, CSharpDbConnectionPool pool) in matches)
            {
                if (s_pools.TryRemove(new KeyValuePair<PoolKey, CSharpDbConnectionPool>(key, pool)))
                {
                    try
                    {
                        await pool.DisableAsync();
                    }
                    catch (Exception exception)
                    {
                        (errors ??= []).Add(exception);
                    }
                    finally
                    {
                        RegisterRetirement(key.DataSource, pool);
                    }
                }
            }
        }
        finally
        {
            if (registryGateHeld)
                s_gate.Release();
            lifecycleBoundary?.Dispose();
        }

        ThrowDisableErrors(errors);
    }

    internal static async ValueTask ClearAllAsync()
    {
        IDisposable? lifecycleBoundary = null;
        bool registryGateHeld = false;
        List<Exception>? errors = null;
        try
        {
            while (!await TryAcquireRegistryGateForCloseAsync(
                       CancellationToken.None,
                       lifecycleBoundary is not null))
            {
                lifecycleBoundary ??=
                    DataLifecycleDiagnosticBoundary.EnterEnabledBoundary(
                        OpaqueDiagnosticsId.Create());
            }
            registryGateHeld = true;

            KeyValuePair<PoolKey, CSharpDbConnectionPool>[] entries = s_pools.ToArray();
            s_pools.Clear();

            foreach ((PoolKey key, CSharpDbConnectionPool pool) in entries)
            {
                try
                {
                    await pool.DisableAsync();
                }
                catch (Exception exception)
                {
                    (errors ??= []).Add(exception);
                }
                finally
                {
                    RegisterRetirement(key.DataSource, pool);
                }
            }
        }
        finally
        {
            if (registryGateHeld)
                s_gate.Release();
            lifecycleBoundary?.Dispose();
        }

        ThrowDisableErrors(errors);
    }

    internal static async ValueTask<IAsyncDisposable> AcquireFileDeletionReservationAsync(
        string dataSource,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(dataSource);
        var reservation = new FileDeletionReservation(dataSource);

        IDisposable? lifecycleBoundary = null;
        bool registryGateHeld = false;
        try
        {
            while (!await TryAcquireRegistryGateForCloseAsync(
                       cancellationToken,
                       lifecycleBoundary is not null,
                       dataSource: dataSource))
            {
                lifecycleBoundary ??=
                    DataLifecycleDiagnosticBoundary.EnterEnabledBoundary(
                        OpaqueDiagnosticsId.Create());
            }
            registryGateHeld = true;

            if (s_fileDeletionReservations.ContainsKey(dataSource))
            {
                throw new InvalidOperationException(
                    "The embedded database for the same data source is already being deleted.");
            }

            if (s_directLeaseCounts.TryGetValue(dataSource, out int directLeaseCount) &&
                directLeaseCount > 0)
            {
                throw CreateActiveConnectionsException();
            }

            if (s_retiringPools.TryGetValue(dataSource, out Task? retiringPool))
            {
                if (!retiringPool.IsCompleted)
                    throw CreateActiveConnectionsException();

                await retiringPool;
                s_retiringPools.Remove(dataSource);
            }

            KeyValuePair<PoolKey, CSharpDbConnectionPool>[] matches = s_pools
                .Where(pair => s_pathComparer.Equals(pair.Key.DataSource, dataSource))
                .ToArray();

            // Avoid retiring idle pool variants when another matching variant is
            // already known to have a checked-out session.
            if (matches.Any(pair => pair.Value.ActiveSessionCount > 0))
                throw CreateActiveConnectionsException();

            foreach (KeyValuePair<PoolKey, CSharpDbConnectionPool> pair in matches)
            {
                bool retired;
                try
                {
                    retired = await pair.Value.TryDisableIfIdleImmediatelyAsync();
                }
                catch
                {
                    s_pools.TryRemove(pair);
                    RegisterRetirement(pair.Key.DataSource, pair.Value);
                    throw;
                }

                // A checkout or physical open won the pool gate after the preflight.
                if (!retired)
                    throw CreateActiveConnectionsException();

                s_pools.TryRemove(pair);
            }

            s_fileDeletionReservations.Add(dataSource, reservation);
            return reservation;
        }
        finally
        {
            if (registryGateHeld)
                s_gate.Release();
            lifecycleBoundary?.Dispose();
        }
    }

    internal static int GetPoolCountForTest() => s_pools.Count;

    internal static async ValueTask<DataRuntimeDiagnosticsRegistrySnapshot>
        CaptureRuntimeDiagnosticsAsync(
            int maximumContributorRecords,
            int maximumSessionRecordsPerContributor,
            CancellationToken cancellationToken = default)
    {
        DataRuntimeDiagnosticsRegistry.ValidateCapacity(
            maximumContributorRecords,
            nameof(maximumContributorRecords));
        DataRuntimeDiagnosticsRegistry.ValidateCapacity(
            maximumSessionRecordsPerContributor,
            nameof(maximumSessionRecordsPerContributor));

        CSharpDbConnectionPool[] pools = s_diagnosticPools.Keys
            .Take(maximumContributorRecords)
            .ToArray();
        DirectDatabaseSession[] directSessions = s_directDiagnosticSessions.Keys
            .Take(maximumContributorRecords)
            .ToArray();
        DataConnectionDiagnosticsRawSnapshot[] retired;
        long retiredHistoryDrops;
        int retiredCount;
        lock (s_retiredDiagnosticsGate)
        {
            retiredCount = s_retiredDiagnostics.Count;
            retired = s_retiredDiagnostics
                .Take(maximumContributorRecords)
                .ToArray();
            retiredHistoryDrops = s_retiredDiagnosticsDroppedCount;
        }

        long sourceCount =
            (long)s_diagnosticPools.Count +
            s_directDiagnosticSessions.Count +
            retiredCount;
        var captured = new List<DataConnectionDiagnosticsRawSnapshot>(
            Math.Min(maximumContributorRecords, (int)Math.Min(int.MaxValue, sourceCount)));
        foreach (IDataRuntimeDiagnosticsContributor contributor in
                 pools.Cast<IDataRuntimeDiagnosticsContributor>()
                     .Concat(directSessions))
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                DataConnectionDiagnosticsRawSnapshot? snapshot =
                    await contributor.CaptureRuntimeDiagnosticsAsync(
                        maximumSessionRecordsPerContributor,
                        cancellationToken);
                if (snapshot is null)
                {
                    // The source remains part of sourceCount and is reflected
                    // by capture truncation.
                }
                else
                    captured.Add(snapshot);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch
            {
                // A racing/unavailable source is reflected in sourceCount.
            }
        }
        captured.AddRange(retired.Select(snapshot => snapshot with
        {
            SessionCapacity = maximumSessionRecordsPerContributor,
        }));

        DataConnectionDiagnosticsRawSnapshot[] ordered = captured
            .OrderBy(static snapshot => snapshot.ContributorId.Value, StringComparer.Ordinal)
            .Take(maximumContributorRecords)
            .ToArray();
        long dropped = Math.Max(0, retiredHistoryDrops);
        bool truncated = sourceCount > ordered.Length || dropped > 0;
        return new DataRuntimeDiagnosticsRegistrySnapshot(
            Array.AsReadOnly(ordered),
            maximumContributorRecords,
            dropped,
            truncated);
    }

    internal static int GetIdleCountForTest(PoolKey key)
    {
        return s_pools.TryGetValue(key, out CSharpDbConnectionPool? pool)
            ? pool.IdleCount
            : 0;
    }

    private static async ValueTask ReserveDirectLeaseAsync(
        string dataSource,
        CancellationToken cancellationToken)
    {
        IDisposable? lifecycleBoundary = null;
        bool registryGateHeld = false;
        try
        {
            while (!await TryAcquireRegistryGateForCloseAsync(
                       cancellationToken,
                       lifecycleBoundary is not null,
                       dataSource: dataSource))
            {
                lifecycleBoundary ??=
                    DataLifecycleDiagnosticBoundary.EnterEnabledBoundary(
                        OpaqueDiagnosticsId.Create());
            }
            registryGateHeld = true;

            ThrowIfFileDeletionReserved(dataSource);

            if (s_retiringPools.TryGetValue(dataSource, out Task? retiring))
            {
                if (!retiring.IsCompleted)
                {
                    throw new InvalidOperationException(
                        "Cannot open a non-pooled embedded connection while pooled connections for the same data source are still active.");
                }

                await retiring.WaitAsync(cancellationToken);
                s_retiringPools.Remove(dataSource);
            }

            KeyValuePair<PoolKey, CSharpDbConnectionPool>[] matchingPools = s_pools
                .Where(pair => s_pathComparer.Equals(pair.Key.DataSource, dataSource))
                .ToArray();

            foreach ((PoolKey key, CSharpDbConnectionPool pool) in matchingPools)
            {
                bool disabled;
                try
                {
                    disabled = await pool.TryDisableIfIdleAsync();
                }
                catch
                {
                    s_pools.TryRemove(new KeyValuePair<PoolKey, CSharpDbConnectionPool>(key, pool));
                    RegisterRetirement(key.DataSource, pool);
                    throw;
                }

                if (!disabled)
                {
                    throw new InvalidOperationException(
                        "Cannot open a non-pooled embedded connection while pooled connections for the same data source are open.");
                }

                s_pools.TryRemove(new KeyValuePair<PoolKey, CSharpDbConnectionPool>(key, pool));
            }

            s_directLeaseCounts.TryGetValue(dataSource, out int leaseCount);
            s_directLeaseCounts[dataSource] = checked(leaseCount + 1);
        }
        finally
        {
            if (registryGateHeld)
                s_gate.Release();
            lifecycleBoundary?.Dispose();
        }
    }

    private static async ValueTask DisposeDirectDatabaseAsync(
        string dataSource,
        Database database)
    {
        // Keep the path leased if physical disposal fails: the database may
        // still own file/WAL handles, so allowing a pooled engine to open would
        // reintroduce the mixed-ownership corruption risk this coordinator prevents.
        await database.DisposeAsync();
        await ReleaseDirectLeaseAsync(dataSource);
    }

    private static async ValueTask ReleaseDirectLeaseAsync(string dataSource)
    {
        await s_gate.WaitAsync();
        try
        {
            if (!s_directLeaseCounts.TryGetValue(dataSource, out int leaseCount))
                return;

            if (leaseCount <= 1)
                s_directLeaseCounts.Remove(dataSource);
            else
                s_directLeaseCounts[dataSource] = leaseCount - 1;
        }
        finally
        {
            s_gate.Release();
        }
    }

    internal static async ValueTask EvictDisabledPoolAsync(CSharpDbConnectionPool pool)
    {
        ArgumentNullException.ThrowIfNull(pool);

        await s_gate.WaitAsync();
        try
        {
            PoolKey key = pool.Key;
            if (s_pools.TryRemove(new KeyValuePair<PoolKey, CSharpDbConnectionPool>(key, pool)))
                RegisterRetirement(key.DataSource, pool);
        }
        finally
        {
            s_gate.Release();
        }
    }

    private static void ThrowDisableErrors(List<Exception>? errors)
    {
        if (errors is { Count: > 0 })
        {
            throw new AggregateException(
                "One or more embedded connection pools failed to close.",
                errors);
        }
    }

    private static InvalidOperationException CreateActiveConnectionsException()
        => new(
            "Cannot delete the embedded database while connections for the same data source are open.");

    private static void ThrowIfFileDeletionReserved(string dataSource)
    {
        if (s_fileDeletionReservations.ContainsKey(dataSource))
        {
            throw new InvalidOperationException(
                "Cannot open an embedded connection while the database for the same data source is being deleted.");
        }
    }

    private static async ValueTask<bool> TryAcquireRegistryGateForCloseAsync(
            CancellationToken cancellationToken,
            bool boundaryEstablished,
            string? dataSource = null,
            PoolKey? exactKey = null)
    {
        await s_gate.WaitAsync(cancellationToken);
        try
        {
            if (!boundaryEstablished &&
                HasLifecycleLoggingPool(dataSource, exactKey))
            {
                s_gate.Release();
                return false;
            }

            // The caller now owns the acquired registry gate. If a close boundary
            // was required, it was entered by the caller before this retry so its
            // AsyncLocal frame is visible to every nested database disposal.
            return true;
        }
        catch
        {
            s_gate.Release();
            throw;
        }
    }

    private static bool HasLifecycleLoggingPool(
        string? dataSource,
        PoolKey? exactKey)
    {
        foreach (CSharpDbConnectionPool pool in s_pools.Values)
        {
            PoolKey key = pool.Key;
            if (dataSource is not null &&
                !s_pathComparer.Equals(key.DataSource, dataSource))
            {
                continue;
            }

            if (exactKey is PoolKey expected && !key.Equals(expected))
                continue;
            if (pool.LifecycleLoggingEnabled)
                return true;
        }

        return false;
    }

    private static async ValueTask ReleaseFileDeletionReservationAsync(
        string dataSource,
        FileDeletionReservation reservation)
    {
        await s_gate.WaitAsync();
        try
        {
            if (s_fileDeletionReservations.TryGetValue(dataSource, out FileDeletionReservation? current) &&
                ReferenceEquals(current, reservation))
            {
                s_fileDeletionReservations.Remove(dataSource);
            }
        }
        finally
        {
            s_gate.Release();
        }
    }

    private static void RegisterRetirement(
        string dataSource,
        CSharpDbConnectionPool pool)
    {
        Task retirement = pool.Retirement;
        if (retirement.IsCompletedSuccessfully)
        {
            ObserveRetirement(pool);
            return;
        }

        if (s_retiringPools.TryGetValue(dataSource, out Task? existing))
            s_retiringPools[dataSource] = Task.WhenAll(existing, retirement);
        else
            s_retiringPools.Add(dataSource, retirement);

        ObserveRetirement(pool);
    }

    private static void ObserveRetirement(CSharpDbConnectionPool pool)
    {
        if (!pool.IsRuntimeDiagnosticsEnabled)
            return;

        try
        {
            lock (s_retiredDiagnosticsGate)
            {
                if (!s_observedRetirements.Add(pool))
                    return;
            }

            AsyncFlowControl flowControl = default;
            bool flowSuppressed = false;
            try
            {
                if (!ExecutionContext.IsFlowSuppressed())
                {
                    flowControl = ExecutionContext.SuppressFlow();
                    flowSuppressed = true;
                }
                _ = ObserveRetirementAsync(pool);
            }
            finally
            {
                if (flowSuppressed)
                    flowControl.Undo();
            }
        }
        catch
        {
            // Diagnostics retention cannot replace pool retirement behavior.
            try
            {
                lock (s_retiredDiagnosticsGate)
                    s_observedRetirements.Remove(pool);
            }
            catch
            {
                // Best-effort cleanup after a diagnostic-only failure.
            }
        }
    }

    private static async Task ObserveRetirementAsync(CSharpDbConnectionPool pool)
    {
        bool succeeded = false;
        try
        {
            await pool.Retirement.ConfigureAwait(false);
            succeeded = true;
        }
        catch
        {
            // A failed physical close is represented as a poisoned tombstone.
        }

        try
        {
            DataConnectionDiagnosticsRawSnapshot? tombstone =
                pool.CreateRetiredDiagnosticsTombstone(succeeded);
            s_diagnosticPools.TryRemove(pool, out _);
            lock (s_retiredDiagnosticsGate)
            {
                s_observedRetirements.Remove(pool);
                if (tombstone is null)
                    return;

                if (s_retiredDiagnostics.Count == RetiredDiagnosticsCapacity)
                {
                    s_retiredDiagnostics.Dequeue();
                    s_retiredDiagnosticsDroppedCount =
                        DataRuntimeDiagnosticsRegistry.SaturatingAddNonNegative(
                            s_retiredDiagnosticsDroppedCount,
                            1);
                }
                s_retiredDiagnostics.Enqueue(tombstone);
            }
        }
        catch
        {
            // Retired diagnostics are best effort and never affect close.
            try
            {
                s_diagnosticPools.TryRemove(pool, out _);
                lock (s_retiredDiagnosticsGate)
                    s_observedRetirements.Remove(pool);
            }
            catch
            {
                // Best-effort cleanup after a diagnostic-only failure.
            }
        }
    }

    private static void TryRegisterDiagnosticPool(CSharpDbConnectionPool pool)
    {
        if (!pool.IsRuntimeDiagnosticsEnabled)
            return;

        try
        {
            s_diagnosticPools.TryAdd(pool, 0);
        }
        catch
        {
            // Diagnostics registration cannot replace pool creation.
        }
    }

    internal static void TryRegisterDirectDiagnosticsSession(
        DirectDatabaseSession session)
    {
        if (!session.IsRuntimeDiagnosticsEnabled)
            return;

        try
        {
            s_directDiagnosticSessions.TryAdd(session, 0);
        }
        catch
        {
            // Diagnostics registration cannot replace session open.
        }
    }

    internal static void UnregisterDirectDiagnosticsSession(
        DirectDatabaseSession session)
        => s_directDiagnosticSessions.TryRemove(session, out _);

    private sealed class FileDeletionReservation(string dataSource) : IAsyncDisposable
    {
        private string? _dataSource = dataSource;

        public ValueTask DisposeAsync()
        {
            string? releasedDataSource = Interlocked.Exchange(ref _dataSource, null);
            return releasedDataSource is null
                ? ValueTask.CompletedTask
                : ReleaseFileDeletionReservationAsync(releasedDataSource, this);
        }
    }
}

internal readonly record struct PoolKey(
    string DataSource,
    int MaxPoolSize,
    CSharpDbEmbeddedOpenMode EffectiveOpenMode,
    CSharpDbStoragePreset? EffectiveStoragePreset,
    bool EffectiveAdaptiveQueryReoptimization,
    object? ExplicitDirectDatabaseOptions,
    object? ExplicitHybridDatabaseOptions);

/// <summary>
/// Owns one warm embedded engine for a pool key and multiplexes logical ADO.NET
/// sessions over it. A logical close resets only session-scoped state; disabling
/// the pool performs the physical database close.
/// </summary>
internal sealed class CSharpDbConnectionPool :
    IDataRuntimeDiagnosticsContributor,
    ICSharpDbDataMetricsProvider
{
    private const string BusyMessage = "Database is busy with an active transaction.";
    private const string SchemaBusyMessage =
        "Database schema is busy with an active transaction or snapshot reader.";
    private const string PoisonedMessage =
        "The pooled database is unavailable because a prior session could not be reset safely.";

    private readonly Func<CancellationToken, ValueTask<Database>> _openDatabaseAsync;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly SemaphoreSlim _sessionSlots;
    private readonly int _maxPoolSize;
    private readonly TaskCompletionSource _retirement =
        new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly Dictionary<long, HashSet<Database.ReaderSession>> _readerSessions = new();
    private readonly HashSet<long> _sessionsWithTemporaryState = [];

    private readonly PoolKey _key;
    private readonly CSharpDbObservabilityOptions? _observabilityOptionsSnapshot;
    private readonly bool _lifecycleLoggingEnabled;
    private readonly DataSessionRuntimeDiagnostics? _runtimeDiagnostics;
    private readonly DataRuntimeDiagnosticsStateOwner? _runtimeDiagnosticsStateOwner;
    private readonly CSharpDbRuntimeMetrics? _runtimeMetrics;
    private IDisposable? _metricsRegistration;
    private Database? _database;
    private bool _disabled;
    private bool _poisoned;
    private bool _retirementStarted;
    private int _activeSessionCount;
    private long _nextSessionId;
    private long? _transactionOwnerSessionId;
    private long? _transactionStartedTimestamp;
    private IReadOnlyDictionary<string, long>? _transactionSnapshotRowCounts;
    private bool _transactionSchemaMutated;
    private int _temporaryCleanupCountForTest;
    private int _waiterCount;

    internal static Action? BeforeFirstPhysicalOpenForTest { get; set; }
    internal static Action? BeforeOpenSessionGateForTest { get; set; }

    internal CSharpDbConnectionPool(
        PoolKey key,
        int maxPoolSize,
        Func<CancellationToken, ValueTask<Database>> openDatabaseAsync,
        CSharpDbObservabilityOptions? observabilityOptionsSnapshot = null,
        TimeProvider? diagnosticsTimeProvider = null,
        DataRuntimeDiagnosticsStateOwner? runtimeDiagnosticsStateOwner = null)
    {
        _key = key;
        _observabilityOptionsSnapshot = observabilityOptionsSnapshot;
        _lifecycleLoggingEnabled =
            DataLifecycleDiagnosticBoundary.IsLifecycleLoggingEnabled(
                _observabilityOptionsSnapshot);
        _runtimeDiagnostics = DataSessionRuntimeDiagnostics.Create(
            observabilityOptionsSnapshot,
            DataConnectionOwnerKind.Pooled,
            diagnosticsTimeProvider);
        _runtimeDiagnosticsStateOwner = runtimeDiagnosticsStateOwner;
        _maxPoolSize = maxPoolSize;
        _sessionSlots = new SemaphoreSlim(maxPoolSize, maxPoolSize);
        _openDatabaseAsync = openDatabaseAsync;
        try
        {
            _runtimeMetrics = runtimeDiagnosticsStateOwner?.State.RuntimeMetrics;
            _metricsRegistration = _runtimeMetrics?.RegisterDataProvider(
                this,
                CSharpDB.Observability.CSharpDbTransport.Direct);
        }
        catch
        {
            // Metrics registration is best effort and cannot prevent pool use.
        }
    }

    internal PoolKey Key => _key;
    internal CSharpDbObservabilityOptions? ObservabilityOptionsSnapshot =>
        _observabilityOptionsSnapshot;
    internal bool LifecycleLoggingEnabled => _lifecycleLoggingEnabled;
    internal bool IsRuntimeDiagnosticsEnabled => _runtimeDiagnostics is not null;
    internal DataRuntimeDiagnosticsStateOwner? RuntimeDiagnosticsStateOwner =>
        _runtimeDiagnosticsStateOwner;
    internal CSharpDbRuntimeDiagnosticsState? RuntimeDiagnosticsState =>
        _runtimeDiagnosticsStateOwner is not null
            ? _runtimeDiagnosticsStateOwner.State
            : _database?.RuntimeDiagnosticsState;
    internal int ActiveSessionCount => Volatile.Read(ref _activeSessionCount);
    internal Task Retirement => _retirement.Task;
    internal int ActiveSnapshotReaderCountForTest => _database?.ActiveReaderCount ?? 0;
    internal int TemporaryCleanupCountForTest => Volatile.Read(ref _temporaryCleanupCountForTest);

    bool ICSharpDbDataMetricsProvider.TryCaptureMetrics(
        out CSharpDbDataMetricSnapshot snapshot)
    {
        try
        {
            snapshot = new CSharpDbDataMetricSnapshot(
                Math.Max(0, Volatile.Read(ref _activeSessionCount)),
                _runtimeDiagnostics?.ActiveReaderCount,
                Math.Max(0, Volatile.Read(ref _waiterCount)),
                Math.Clamp(_sessionSlots.CurrentCount, 0, _maxPoolSize));
            return true;
        }
        catch
        {
            snapshot = default;
            return false;
        }
    }

    internal int IdleCount =>
        !_disabled && _database is not null && ActiveSessionCount == 0 ? 1 : 0;

    public async ValueTask<DataConnectionDiagnosticsRawSnapshot?> CaptureRuntimeDiagnosticsAsync(
        int maximumSessionRecords,
        CancellationToken cancellationToken = default)
    {
        DataRuntimeDiagnosticsRegistry.ValidateCapacity(
            maximumSessionRecords,
            nameof(maximumSessionRecords));
        DataSessionRuntimeDiagnostics? diagnostics = _runtimeDiagnostics;
        if (diagnostics is null)
            return null;

        const int MaximumConsistencyAttempts = 8;
        for (int attempt = 0; attempt < MaximumConsistencyAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!diagnostics.Consistency.TryStartRead(out long version))
            {
                await Task.Yield();
                continue;
            }

            PoolDiagnosticsState state;
            await _gate.WaitAsync(cancellationToken);
            try
            {
                bool retirementSucceeded = Retirement.IsCompletedSuccessfully;
                bool retirementFailed = Retirement.IsFaulted;
                bool poisoned = _poisoned || retirementFailed;
                ConnectionPoolLifecycleState poolState = poisoned
                    ? ConnectionPoolLifecycleState.Poisoned
                    : _retirementStarted && retirementSucceeded
                        ? ConnectionPoolLifecycleState.Retired
                        : _retirementStarted
                            ? ConnectionPoolLifecycleState.Retiring
                            : _disabled
                                ? ConnectionPoolLifecycleState.Disabled
                                : ConnectionPoolLifecycleState.Enabled;
                state = new PoolDiagnosticsState(
                    _sessionSlots.CurrentCount,
                    Volatile.Read(ref _waiterCount),
                    ActiveSessionCount,
                    _database is not null && ActiveSessionCount == 0 && !_disabled ? 1 : 0,
                    _transactionOwnerSessionId,
                    _transactionStartedTimestamp,
                    poolState,
                    _disabled,
                    poisoned,
                    _retirementStarted,
                    retirementSucceeded,
                    retirementFailed);
            }
            finally
            {
                _gate.Release();
            }

            DataSessionRuntimeDiagnostics.SessionStateBatch sessionBatch =
                diagnostics.CopySessions(
                    maximumSessionRecords,
                    state.TransactionOwnerSessionKey);
            DataSessionRuntimeDiagnostics.SessionStateCopy[] sessionCopies =
                sessionBatch.Records;
            DateTimeOffset snapshotAtUtc = diagnostics.GetUtcNowOrLast();
            long? snapshotTimestamp = state.TransactionOwnerSessionKey.HasValue
                ? diagnostics.GetTimestampOrNull()
                : null;
            if (!diagnostics.Consistency.IsReadValid(version))
            {
                await Task.Yield();
                continue;
            }

            // Registration/removal is bracketed by the same consistency stamp.
            // A mismatch here means diagnostics registration itself failed and
            // this source is safer to omit than to publish contradictory gauges.
            if (sessionBatch.TotalCount != state.ActiveLogicalSessions)
                return null;

            DataSessionDiagnosticsRawSnapshot[] sessions =
                DataSessionRuntimeDiagnostics.ProjectSessions(
                    sessionCopies,
                    state.TransactionOwnerSessionKey);
            bool sessionsTruncated =
                sessionBatch.TotalCount > sessionCopies.Length;
            OpaqueDiagnosticsId? transactionOwnerSessionId =
                state.TransactionOwnerSessionKey is long transactionOwnerKey
                    ? sessionCopies.FirstOrDefault(
                        copy => copy.SessionKey == transactionOwnerKey).SessionId
                    : null;
            TimeSpan? oldestTransactionAge =
                state.TransactionOwnerSessionKey.HasValue
                    ? diagnostics.GetElapsedTimeOrNull(
                        state.TransactionStartedTimestamp,
                        snapshotTimestamp)
                    : null;

            return new DataConnectionDiagnosticsRawSnapshot(
                diagnostics.ContributorId,
                diagnostics.DatabaseAlias,
                snapshotAtUtc,
                diagnostics.OwnerKind,
                _maxPoolSize,
                Math.Clamp(state.AvailableSlots, 0, _maxPoolSize),
                Math.Max(0, state.WaiterCount),
                state.ActiveLogicalSessions,
                sessionBatch.ActiveReaderCount,
                state.TransactionOwnerSessionKey.HasValue ? 1 : 0,
                oldestTransactionAge,
                state.WarmEngineIdleCount,
                state.RetirementSucceeded ? 1 : 0,
                state.Poisoned ? 1 : 0,
                state.Disabled ? 1 : 0,
                state.RetirementStarted &&
                    !state.RetirementSucceeded &&
                    !state.RetirementFailed ? 1 : 0,
                transactionOwnerSessionId,
                state.PoolState,
                Array.AsReadOnly(sessions),
                maximumSessionRecords,
                0,
                sessionsTruncated);
        }

        return null;
    }

    internal DataConnectionDiagnosticsRawSnapshot? CreateRetiredDiagnosticsTombstone(
        bool retirementSucceeded)
    {
        DataSessionRuntimeDiagnostics? diagnostics = _runtimeDiagnostics;
        if (diagnostics is null)
            return null;

        DateTimeOffset snapshotAtUtc = diagnostics.GetUtcNowOrLast();
        bool poisoned = Volatile.Read(ref _poisoned) || !retirementSucceeded;
        return new DataConnectionDiagnosticsRawSnapshot(
            diagnostics.ContributorId,
            diagnostics.DatabaseAlias,
            snapshotAtUtc,
            diagnostics.OwnerKind,
            _maxPoolSize,
            _maxPoolSize,
            0,
            0,
            0,
            0,
            null,
            0,
            retirementSucceeded ? 1 : 0,
            poisoned ? 1 : 0,
            1,
            0,
            null,
            poisoned
                ? ConnectionPoolLifecycleState.Poisoned
                : ConnectionPoolLifecycleState.Retired,
            Array.Empty<DataSessionDiagnosticsRawSnapshot>(),
            CSharpDbObservabilityOptions.MaximumActiveOperationCapacity,
            0,
            false);
    }

    private readonly record struct PoolDiagnosticsState(
        int AvailableSlots,
        int WaiterCount,
        int ActiveLogicalSessions,
        int WarmEngineIdleCount,
        long? TransactionOwnerSessionKey,
        long? TransactionStartedTimestamp,
        ConnectionPoolLifecycleState PoolState,
        bool Disabled,
        bool Poisoned,
        bool RetirementStarted,
        bool RetirementSucceeded,
        bool RetirementFailed);

    internal ValueTask<PooledDatabaseSession> OpenSessionAsync(
        CancellationToken cancellationToken)
        => _runtimeDiagnostics is null
            ? OpenSessionWithoutRuntimeDiagnosticsAsync(cancellationToken)
            : OpenObservedSessionAsync(cancellationToken);

    private async ValueTask<PooledDatabaseSession>
        OpenSessionWithoutRuntimeDiagnosticsAsync(
            CancellationToken cancellationToken)
    {
        await _sessionSlots.WaitAsync(cancellationToken);
        bool sessionCreated = false;
        using IDisposable? lifecycleBoundary = EnterDatabaseOpenBoundary();
        try
        {
            BeforeOpenSessionGateForTest?.Invoke();
            await _gate.WaitAsync(cancellationToken);
            try
            {
                if (_disabled)
                    throw new CSharpDbConnectionPoolRetiredException();

                if (_database is null)
                {
                    BeforeFirstPhysicalOpenForTest?.Invoke();
                    _database = await _openDatabaseAsync(cancellationToken);
                }

                long sessionId = ++_nextSessionId;
                Interlocked.Increment(ref _activeSessionCount);
                sessionCreated = true;
                return new PooledDatabaseSession(this, sessionId);
            }
            finally
            {
                _gate.Release();
            }
        }
        finally
        {
            if (!sessionCreated)
                _sessionSlots.Release();
        }
    }

    private async ValueTask<PooledDatabaseSession> OpenObservedSessionAsync(
        CancellationToken cancellationToken)
    {
        await WaitForSessionSlotAsync(cancellationToken);
        bool sessionCreated = false;
        bool diagnosticsMutationStarted = false;
        long sessionId = 0;
        DateTimeOffset diagnosticsCreatedAtUtc = default;
        OpaqueDiagnosticsId? preferredSessionId = null;
        if (_runtimeDiagnostics is not null)
        {
            diagnosticsCreatedAtUtc = _runtimeDiagnostics.GetUtcNowOrLast();
            // Capture the logical connection identity before a first-physical-
            // open lifecycle boundary temporarily establishes its own scope.
            preferredSessionId = CSharpDbOperationScope.CurrentSessionId;
        }

        using IDisposable? lifecycleBoundary = EnterDatabaseOpenBoundary();
        try
        {
            BeforeOpenSessionGateForTest?.Invoke();
            await _gate.WaitAsync(cancellationToken);
            try
            {
                if (_disabled)
                    throw new CSharpDbConnectionPoolRetiredException();

                if (_database is null)
                {
                    BeforeFirstPhysicalOpenForTest?.Invoke();
                    _database = await _openDatabaseAsync(cancellationToken);
                }
                sessionId = ++_nextSessionId;
                if (_runtimeDiagnostics is not null)
                {
                    _runtimeDiagnostics.Consistency.BeginMutation();
                    diagnosticsMutationStarted = true;
                }
                Interlocked.Increment(ref _activeSessionCount);
            }
            finally
            {
                _gate.Release();
            }

            if (_runtimeDiagnostics is not null)
            {
                _runtimeDiagnostics.RegisterSession(
                    sessionId,
                    preferredSessionId,
                    diagnosticsCreatedAtUtc);
            }

            sessionCreated = true;
            return new PooledDatabaseSession(this, sessionId);
        }
        finally
        {
            if (diagnosticsMutationStarted)
                _runtimeDiagnostics!.Consistency.EndMutation();
            if (!sessionCreated)
                ReleaseSessionSlot();
        }
    }

    private async ValueTask WaitForSessionSlotAsync(CancellationToken cancellationToken)
    {
        DataDiagnosticsConsistencyStamp? consistency = _runtimeDiagnostics?.Consistency;
        consistency?.BeginMutation();
        bool acquiredImmediately;
        try
        {
            acquiredImmediately = _sessionSlots.Wait(0, cancellationToken);
        }
        finally
        {
            consistency?.EndMutation();
        }

        if (acquiredImmediately)
            return;

        long startingTimestamp = 0;
        bool measureWait = _runtimeMetrics?.TryStartPoolWait(
            out startingTimestamp) == true;
        CSharpDbOperationOutcome outcome = CSharpDbOperationOutcome.Succeeded;
        MutateWaiterCount(1);
        try
        {
            await _sessionSlots.WaitAsync(cancellationToken);
        }
        catch (OperationCanceledException)
        {
            outcome = CSharpDbOperationOutcome.Canceled;
            throw;
        }
        catch
        {
            outcome = CSharpDbOperationOutcome.Failed;
            throw;
        }
        finally
        {
            MutateWaiterCount(-1);
            if (measureWait)
                _runtimeMetrics?.RecordPoolWait(startingTimestamp, outcome);
        }
    }

    private void MutateWaiterCount(int delta)
    {
        DataDiagnosticsConsistencyStamp? consistency = _runtimeDiagnostics?.Consistency;
        consistency?.BeginMutation();
        try
        {
            if (delta > 0)
                Interlocked.Increment(ref _waiterCount);
            else
                Interlocked.Decrement(ref _waiterCount);
        }
        finally
        {
            consistency?.EndMutation();
        }
    }

    private void ReleaseSessionSlot()
    {
        DataDiagnosticsConsistencyStamp? consistency = _runtimeDiagnostics?.Consistency;
        consistency?.BeginMutation();
        try
        {
            _sessionSlots.Release();
        }
        finally
        {
            consistency?.EndMutation();
        }
    }

    private IDisposable? EnterDatabaseOpenBoundary()
    {
        if (Volatile.Read(ref _database) is not null ||
            !DataLifecycleDiagnosticBoundary.IsLifecycleLoggingEnabled(
                _observabilityOptionsSnapshot))
        {
            return null;
        }

        return DataLifecycleDiagnosticBoundary.EnterEnabledBoundary(
            OpaqueDiagnosticsId.Create());
    }

    internal ValueTask<QueryResult> ExecuteAsync(
        long sessionId,
        string sql,
        CancellationToken cancellationToken)
        => ExecuteAsync(sessionId, sql, sql, cancellationToken);

    internal ValueTask<QueryResult> ExecuteAsync(
        long sessionId,
        string executionSql,
        string? observabilitySql,
        CancellationToken cancellationToken)
        => ExecuteAsync(
            sessionId,
            executionSql,
            observabilitySql,
            observation: null,
            cancellationToken);

    internal ValueTask<QueryResult> ExecuteAsync(
        long sessionId,
        string executionSql,
        string? observabilitySql,
        AdoCommandObservation? observation,
        CancellationToken cancellationToken)
    {
        SqlStatementClassification classification = SqlStatementClassifier.Classify(executionSql);
        return classification.IsReadOnly
            ? ExecuteReadAsync(
                sessionId,
                classification.Statement,
                observabilitySql,
                observation,
                cancellationToken)
            : ExecuteWriteAsync(
                sessionId,
                classification.Statement,
                database => database.ExecuteAsync(
                    classification.Statement,
                    QueryObservabilitySource.CreateFingerprint(database, observabilitySql),
                    cancellationToken),
                observation,
                cancellationToken);
    }

    internal ValueTask<QueryResult> ExecuteAsync(
        long sessionId,
        Statement statement,
        CancellationToken cancellationToken)
        => ExecuteAsync(sessionId, statement, observabilitySql: null, cancellationToken);

    internal ValueTask<QueryResult> ExecuteAsync(
        long sessionId,
        Statement statement,
        string? observabilitySql,
        CancellationToken cancellationToken)
        => ExecuteAsync(
            sessionId,
            statement,
            observabilitySql,
            observation: null,
            cancellationToken);

    internal ValueTask<QueryResult> ExecuteAsync(
        long sessionId,
        Statement statement,
        string? observabilitySql,
        AdoCommandObservation? observation,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(statement);
        return SqlStatementClassifier.IsReadOnly(statement)
            ? ExecuteReadAsync(
                sessionId,
                statement,
                observabilitySql,
                observation,
                cancellationToken)
            : ExecuteWriteAsync(
                sessionId,
                statement,
                database => database.ExecuteAsync(
                    statement,
                    QueryObservabilitySource.CreateFingerprint(database, observabilitySql),
                    cancellationToken),
                observation,
                cancellationToken);
    }

    internal ValueTask<QueryResult> ExecuteAsync(
        long sessionId,
        SimpleInsertSql insert,
        CancellationToken cancellationToken)
        => ExecuteAsync(sessionId, insert, observabilitySql: null, cancellationToken);

    internal ValueTask<QueryResult> ExecuteAsync(
        long sessionId,
        SimpleInsertSql insert,
        string? observabilitySql,
        CancellationToken cancellationToken)
        => ExecuteAsync(
            sessionId,
            insert,
            observabilitySql,
            observation: null,
            cancellationToken);

    internal ValueTask<QueryResult> ExecuteAsync(
        long sessionId,
        SimpleInsertSql insert,
        string? observabilitySql,
        AdoCommandObservation? observation,
        CancellationToken cancellationToken)
        => ExecuteWriteAsync(
            sessionId,
            statement: null,
            database => database.ExecuteAsync(
                insert,
                QueryObservabilitySource.CreateFingerprint(database, observabilitySql),
                cancellationToken),
            observation,
            cancellationToken);

    internal async ValueTask BeginTransactionAsync(
        long sessionId,
        CancellationToken cancellationToken)
    {
        DateTimeOffset? transactionStartedAtUtc = null;
        long? transactionStartedTimestamp = null;
        bool diagnosticsMutationStarted = false;
        await _gate.WaitAsync(cancellationToken);
        try
        {
            ThrowIfUnavailable();
            if (_transactionOwnerSessionId == sessionId)
                throw new InvalidOperationException("A transaction is already active.");
            if (_transactionOwnerSessionId.HasValue)
                throw new InvalidOperationException(BusyMessage);

            Database database = GetDatabase();
            using var temporaryScope = database.EnterTemporaryTableSessionScope(sessionId);
            IReadOnlyDictionary<string, long> snapshotRowCounts =
                database.CaptureReaderSnapshotRowCounts();
            await database.BeginTransactionAsync(cancellationToken);
            if (_runtimeDiagnostics is not null)
            {
                transactionStartedAtUtc = _runtimeDiagnostics.GetUtcNowOrLast();
                transactionStartedTimestamp =
                    _runtimeDiagnostics.GetTimestampOrNull();
                _runtimeDiagnostics.Consistency.BeginMutation();
                diagnosticsMutationStarted = true;
            }
            _transactionOwnerSessionId = sessionId;
            _transactionStartedTimestamp = transactionStartedTimestamp;
            _transactionSnapshotRowCounts = snapshotRowCounts;
            _transactionSchemaMutated = false;
        }
        finally
        {
            _gate.Release();
            if (diagnosticsMutationStarted)
            {
                try
                {
                    if (transactionStartedAtUtc is DateTimeOffset started)
                        _runtimeDiagnostics!.TouchSession(sessionId, started);
                }
                catch
                {
                    // Diagnostics cannot replace the transaction result.
                }
                finally
                {
                    _runtimeDiagnostics!.Consistency.EndMutation();
                }
            }
        }
    }

    internal ValueTask CommitAsync(long sessionId, CancellationToken cancellationToken)
        => CompleteTransactionAsync(sessionId, commit: true, cancellationToken);

    internal ValueTask RollbackAsync(long sessionId, CancellationToken cancellationToken)
        => CompleteTransactionAsync(sessionId, commit: false, cancellationToken);

    internal async ValueTask SaveToFileAsync(
        long sessionId,
        string filePath,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);

        await _gate.WaitAsync(cancellationToken);
        try
        {
            ThrowIfUnavailable();
            ThrowIfOwnedByOtherSession(sessionId);
            await GetDatabase().SaveToFileAsync(filePath, cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    internal IReadOnlyCollection<string> GetTableNames(long sessionId)
        => ExecuteIntrospection(sessionId, static database => database.GetTableNames().ToArray());

    internal TableSchema? GetTableSchema(long sessionId, string tableName)
        => ExecuteIntrospection(sessionId, database => database.GetTableSchema(tableName));

    internal IReadOnlyCollection<IndexSchema> GetIndexes(long sessionId)
        => ExecuteIntrospection(sessionId, static database => database.GetIndexes().ToArray());

    internal IReadOnlyCollection<string> GetViewNames(long sessionId)
        => ExecuteIntrospection(sessionId, static database => database.GetViewNames().ToArray());

    internal string? GetViewSql(long sessionId, string viewName)
        => ExecuteIntrospection(sessionId, database => database.GetViewSql(viewName));

    internal IReadOnlyCollection<TriggerSchema> GetTriggers(long sessionId)
        => ExecuteIntrospection(sessionId, static database => database.GetTriggers().ToArray());

    internal ValueTask ReleaseSessionAsync(long sessionId)
        => _runtimeDiagnostics is null
            ? ReleaseSessionWithoutRuntimeDiagnosticsAsync(sessionId)
            : ReleaseObservedSessionAsync(sessionId);

    private async ValueTask ReleaseSessionWithoutRuntimeDiagnosticsAsync(
        long sessionId)
    {
        Database? databaseToDispose = null;
        Exception? resetException = null;
        bool startRetirement = false;
        Task? retirementToAwait = null;
        bool evictPool;

        await _gate.WaitAsync();
        try
        {
            if (_transactionOwnerSessionId == sessionId)
            {
                try
                {
                    Database database = GetDatabase();
                    using var temporaryScope =
                        database.EnterTemporaryTableSessionScope(sessionId);
                    await database.RollbackAsync();
                }
                catch (Exception exception)
                {
                    // Never reuse an engine whose transaction state could not be reset.
                    _disabled = true;
                    _poisoned = true;
                    resetException = exception;
                }

                _transactionOwnerSessionId = null;
                _transactionSnapshotRowCounts = null;
                _transactionSchemaMutated = false;
            }

            bool hasOutstandingReaders = _readerSessions.ContainsKey(sessionId);
            bool hasTemporaryState = _sessionsWithTemporaryState.Remove(sessionId);
            if (hasOutstandingReaders || hasTemporaryState)
            {
                try
                {
                    if (hasOutstandingReaders)
                        DisposeReaderSessions(sessionId);

                    if (hasTemporaryState)
                    {
                        Database database = GetDatabase();
                        using var temporaryScope =
                            database.EnterTemporaryTableSessionScope(sessionId);
                        Interlocked.Increment(ref _temporaryCleanupCountForTest);
                        await database.ClearTemporaryTablesAsync();
                    }
                }
                catch (Exception exception)
                {
                    _disabled = true;
                    _poisoned = true;
                    resetException = exception;
                }
            }

            if (ActiveSessionCount > 0)
                Interlocked.Decrement(ref _activeSessionCount);

            if (_disabled && ActiveSessionCount == 0)
            {
                startRetirement = TryStartRetirement(out databaseToDispose);
                if (!startRetirement)
                    retirementToAwait = Retirement;
            }

            evictPool = _disabled;
        }
        finally
        {
            _gate.Release();
        }

        try
        {
            if (startRetirement)
                await DisposeRetiredDatabaseAsync(databaseToDispose);
            else if (retirementToAwait is not null)
                await retirementToAwait;
        }
        finally
        {
            _sessionSlots.Release();
            if (evictPool)
                await CSharpDbConnectionPoolRegistry.EvictDisabledPoolAsync(this);
        }

        if (resetException is not null)
        {
            throw new InvalidOperationException(
                "Failed to reset the pooled database session.",
                resetException);
        }
    }

    private async ValueTask ReleaseObservedSessionAsync(long sessionId)
    {
        DateTimeOffset? releasedAtUtc = TryGetDiagnosticsUtcNow();
        bool diagnosticsMutationStarted = false;
        Database? databaseToDispose = null;
        Exception? resetException = null;
        bool startRetirement = false;
        Task? retirementToAwait = null;
        bool evictPool;

        await _gate.WaitAsync();
        try
        {
            BeginDiagnosticsMutation(ref diagnosticsMutationStarted);
            if (_transactionOwnerSessionId == sessionId)
            {
                try
                {
                    Database database = GetDatabase();
                    using var temporaryScope = database.EnterTemporaryTableSessionScope(sessionId);
                    await database.RollbackAsync();
                }
                catch (Exception exception)
                {
                    // Never reuse an engine whose transaction state could not be reset.
                    _disabled = true;
                    _poisoned = true;
                    resetException = exception;
                }

                _transactionOwnerSessionId = null;
                _transactionStartedTimestamp = null;
                _transactionSnapshotRowCounts = null;
                _transactionSchemaMutated = false;
            }

            bool hasOutstandingReaders = _readerSessions.ContainsKey(sessionId);
            bool hasTemporaryState = _sessionsWithTemporaryState.Remove(sessionId);
            if (hasOutstandingReaders || hasTemporaryState)
            {
                try
                {
                    if (hasOutstandingReaders)
                        DisposeReaderSessions(sessionId);

                    if (hasTemporaryState)
                    {
                        Database database = GetDatabase();
                        using var temporaryScope = database.EnterTemporaryTableSessionScope(sessionId);
                        Interlocked.Increment(ref _temporaryCleanupCountForTest);
                        await database.ClearTemporaryTablesAsync();
                    }
                }
                catch (Exception exception)
                {
                    _disabled = true;
                    _poisoned = true;
                    resetException = exception;
                }
            }

            if (ActiveSessionCount > 0)
                Interlocked.Decrement(ref _activeSessionCount);

            if (_disabled && ActiveSessionCount == 0)
            {
                startRetirement = TryStartRetirement(out databaseToDispose);
                if (!startRetirement)
                    retirementToAwait = Retirement;
            }

            evictPool = _disabled;
        }
        finally
        {
            _gate.Release();
            if (diagnosticsMutationStarted)
            {
                try
                {
                    _runtimeDiagnostics!.RemoveSession(
                        sessionId,
                        releasedAtUtc);
                }
                catch
                {
                    // Diagnostics cannot replace connection close behavior.
                }
                finally
                {
                    _runtimeDiagnostics!.Consistency.EndMutation();
                }
            }
        }

        try
        {
            if (startRetirement)
                await DisposeRetiredDatabaseAsync(databaseToDispose);
            else if (retirementToAwait is not null)
                await retirementToAwait;
        }
        finally
        {
            ReleaseSessionSlot();
            if (evictPool)
                await CSharpDbConnectionPoolRegistry.EvictDisabledPoolAsync(this);
        }

        if (resetException is not null)
            throw new InvalidOperationException(
                "Failed to reset the pooled database session.",
                resetException);
    }

    internal async ValueTask DisableAsync()
    {
        bool diagnosticsMutationStarted = false;
        Database? databaseToDispose = null;
        bool startRetirement = false;
        Task? retirementToAwait = null;

        await _gate.WaitAsync();
        try
        {
            BeginDiagnosticsMutation(ref diagnosticsMutationStarted);
            _disabled = true;
            if (ActiveSessionCount == 0)
            {
                startRetirement = TryStartRetirement(out databaseToDispose);
                if (!startRetirement)
                    retirementToAwait = Retirement;
            }
        }
        finally
        {
            _gate.Release();
            if (diagnosticsMutationStarted)
                _runtimeDiagnostics!.Consistency.EndMutation();
        }

        if (startRetirement)
            await DisposeRetiredDatabaseAsync(databaseToDispose);
        else if (retirementToAwait is not null)
            await retirementToAwait;
    }

    internal async ValueTask<bool> TryDisableIfIdleAsync()
    {
        bool diagnosticsMutationStarted = false;
        Database? databaseToDispose = null;
        bool startRetirement;
        Task retirementToAwait;

        await _gate.WaitAsync();
        try
        {
            if (ActiveSessionCount > 0)
                return false;

            BeginDiagnosticsMutation(ref diagnosticsMutationStarted);
            _disabled = true;
            startRetirement = TryStartRetirement(out databaseToDispose);
            retirementToAwait = Retirement;
        }
        finally
        {
            _gate.Release();
            if (diagnosticsMutationStarted)
                _runtimeDiagnostics!.Consistency.EndMutation();
        }

        if (startRetirement)
            await DisposeRetiredDatabaseAsync(databaseToDispose);
        else
            await retirementToAwait;

        return true;
    }

    internal async ValueTask<bool> TryDisableIfIdleImmediatelyAsync()
    {
        bool diagnosticsMutationStarted = false;
        Database? databaseToDispose = null;
        bool startRetirement = false;
        Task? retirementToAwait = null;

        // Deletion must not wait behind a checkout or a physical open. Whichever
        // operation obtains the pool gate first owns the path for this attempt.
        if (!_gate.Wait(0))
            return false;

        try
        {
            if (ActiveSessionCount > 0 || (_disabled && !Retirement.IsCompleted))
                return false;

            BeginDiagnosticsMutation(ref diagnosticsMutationStarted);
            _disabled = true;
            startRetirement = TryStartRetirement(out databaseToDispose);
            if (!startRetirement)
                retirementToAwait = Retirement;
        }
        finally
        {
            _gate.Release();
            if (diagnosticsMutationStarted)
                _runtimeDiagnostics!.Consistency.EndMutation();
        }

        if (startRetirement)
            await DisposeRetiredDatabaseAsync(databaseToDispose);
        else if (retirementToAwait is not null)
            await retirementToAwait;

        return true;
    }

    private bool TryStartRetirement(out Database? database)
    {
        if (_retirementStarted)
        {
            database = null;
            return false;
        }

        _retirementStarted = true;
        database = _database;
        _database = null;
        return true;
    }

    private async ValueTask DisposeRetiredDatabaseAsync(Database? database)
    {
        try
        {
            if (database is not null)
                await database.DisposeAsync();

            _runtimeDiagnostics?.Consistency.BeginMutation();
            try
            {
                _retirement.TrySetResult();
            }
            finally
            {
                _runtimeDiagnostics?.Consistency.EndMutation();
            }
        }
        catch (Exception exception)
        {
            _runtimeDiagnostics?.Consistency.BeginMutation();
            try
            {
                _retirement.TrySetException(exception);
            }
            finally
            {
                _runtimeDiagnostics?.Consistency.EndMutation();
            }
            throw;
        }
        finally
        {
            Interlocked.Exchange(ref _metricsRegistration, null)?.Dispose();
            _runtimeDiagnosticsStateOwner?.Dispose();
        }
    }

    private async ValueTask<QueryResult> ExecuteReadAsync(
        long sessionId,
        Statement statement,
        string? observabilitySql,
        AdoCommandObservation? observation,
        CancellationToken cancellationToken)
    {
        DataSessionOperationLease? diagnosticsOperation =
            _runtimeDiagnostics?.TryBeginOperation(sessionId);
        bool gateHeld = false;
        try
        {
            using (observation?.MeasureQueueWait())
                await _gate.WaitAsync(cancellationToken);
            gateHeld = true;
            using IDisposable? queueDurationScope = observation?.EnterQueueDurationScope();
            ThrowIfUnavailable();

            Database database = GetDatabase();
            QueryFingerprint? fingerprint =
                QueryObservabilitySource.CreateFingerprint(database, observabilitySql);
            using var temporaryScope = database.EnterTemporaryTableSessionScope(sessionId);
            if (_transactionOwnerSessionId.HasValue &&
                _transactionOwnerSessionId.Value != sessionId &&
                _transactionSchemaMutated)
            {
                throw new InvalidOperationException(SchemaBusyMessage);
            }

            if (_transactionOwnerSessionId == sessionId)
            {
                observation?.MarkDispatchHandoff(database);
                QueryResult liveResult = await database.ExecuteAsync(
                    statement,
                    fingerprint,
                    cancellationToken);
                QueryResult detached = await DetachQueryResultAsync(liveResult, cancellationToken);
                return CompleteObservedResult(
                    ref gateHeld,
                    diagnosticsOperation,
                    detached);
            }

            if (database.HasTemporaryTablesForCurrentSession)
            {
                if (_transactionOwnerSessionId.HasValue)
                    throw new InvalidOperationException(BusyMessage);

                observation?.MarkDispatchHandoff(database);
                QueryResult temporaryResult = await database.ExecuteAsync(
                    statement,
                    fingerprint,
                    cancellationToken);
                QueryResult detached = await DetachQueryResultAsync(temporaryResult, cancellationToken);
                return CompleteObservedResult(
                    ref gateHeld,
                    diagnosticsOperation,
                    detached);
            }

            Database.ReaderSession readerSession = _transactionSnapshotRowCounts is null
                ? database.CreateReaderSession()
                : database.CreateReaderSession(
                    _transactionSnapshotRowCounts,
                    allowCurrentCatalogRowCounts: false);
            TrackReaderSession(sessionId, readerSession);
            try
            {
                observation?.MarkDispatchHandoff(database);
                QueryResult result = await readerSession.ExecuteReadAsync(
                    statement,
                    fingerprint,
                    cancellationToken);
                result.AppendDisposeCallback(
                    () => ReleaseReaderSessionAsync(sessionId, readerSession));
                return CompleteObservedResult(
                    ref gateHeld,
                    diagnosticsOperation,
                    result);
            }
            catch
            {
                UntrackReaderSession(sessionId, readerSession);
                readerSession.Dispose();
                throw;
            }
        }
        catch
        {
            ReleaseGateIfHeld(ref gateHeld);
            diagnosticsOperation?.Complete();
            throw;
        }
        finally
        {
            if (gateHeld)
                _gate.Release();
        }
    }

    private async ValueTask<QueryResult> ExecuteWriteAsync(
        long sessionId,
        Statement? statement,
        Func<Database, ValueTask<QueryResult>> executeAsync,
        AdoCommandObservation? observation,
        CancellationToken cancellationToken)
    {
        DataSessionOperationLease? diagnosticsOperation =
            _runtimeDiagnostics?.TryBeginOperation(sessionId);
        bool gateHeld = false;
        try
        {
            using (observation?.MeasureQueueWait())
                await _gate.WaitAsync(cancellationToken);
            gateHeld = true;
            using IDisposable? queueDurationScope = observation?.EnterQueueDurationScope();
            ThrowIfUnavailable();
            ThrowIfOwnedByOtherSession(sessionId);

            Database database = GetDatabase();
            using var temporaryScope = database.EnterTemporaryTableSessionScope(sessionId);
            try
            {
                bool persistentSchemaMutation = statement is not null &&
                    IsPersistentSchemaMutation(statement);
                if (persistentSchemaMutation)
                {
                    if (_readerSessions.Count > 0)
                        throw new InvalidOperationException(SchemaBusyMessage);

                    // Set this before execution. A failed DDL statement can leave the
                    // live catalog changed until the explicit transaction is rolled back.
                    if (_transactionOwnerSessionId == sessionId)
                        _transactionSchemaMutated = true;
                }

                observation?.MarkDispatchHandoff(database);
                QueryResult result = await executeAsync(database);
                QueryResult detached = await DetachQueryResultAsync(result, cancellationToken);
                return CompleteObservedResult(
                    ref gateHeld,
                    diagnosticsOperation,
                    detached);
            }
            finally
            {
                // The engine is authoritative here. In particular, a trigger or
                // failed statement may create an otherwise invisible empty temp
                // context, so AST classification alone is not sufficient.
                if (database.HasTemporaryTableContextForCurrentSession)
                    _sessionsWithTemporaryState.Add(sessionId);
            }
        }
        catch
        {
            ReleaseGateIfHeld(ref gateHeld);
            diagnosticsOperation?.Complete();
            throw;
        }
        finally
        {
            if (gateHeld)
                _gate.Release();
        }
    }

    private QueryResult CompleteObservedResult(
        ref bool gateHeld,
        DataSessionOperationLease? diagnosticsOperation,
        QueryResult result)
    {
        ReleaseGateIfHeld(ref gateHeld);
        return diagnosticsOperation?.ObserveResult(result) ?? result;
    }

    private void ReleaseGateIfHeld(ref bool gateHeld)
    {
        if (!gateHeld)
            return;

        _gate.Release();
        gateHeld = false;
    }

    private async ValueTask CompleteTransactionAsync(
        long sessionId,
        bool commit,
        CancellationToken cancellationToken)
    {
        DateTimeOffset? completedAtUtc = TryGetDiagnosticsUtcNow();
        bool diagnosticsMutationStarted = false;
        await _gate.WaitAsync(cancellationToken);
        try
        {
            ThrowIfUnavailable();
            if (_transactionOwnerSessionId != sessionId)
            {
                if (_transactionOwnerSessionId.HasValue)
                    throw new InvalidOperationException(BusyMessage);
                throw new InvalidOperationException("No active transaction.");
            }

            Database database = GetDatabase();
            using var temporaryScope = database.EnterTemporaryTableSessionScope(sessionId);
            try
            {
                if (commit)
                    await database.CommitAsync(cancellationToken);
                else
                    await database.RollbackAsync(cancellationToken);
            }
            catch
            {
                // The engine may have failed before or after changing its own
                // transaction state. Stop every logical session from using it
                // until the owner closes and retirement performs final cleanup.
                BeginDiagnosticsMutation(ref diagnosticsMutationStarted);
                _disabled = true;
                _poisoned = true;
                throw;
            }

            BeginDiagnosticsMutation(ref diagnosticsMutationStarted);
            _transactionOwnerSessionId = null;
            _transactionStartedTimestamp = null;
            _transactionSnapshotRowCounts = null;
            _transactionSchemaMutated = false;
        }
        finally
        {
            _gate.Release();
            if (diagnosticsMutationStarted)
            {
                try
                {
                    if (completedAtUtc is DateTimeOffset completed)
                        _runtimeDiagnostics!.TouchSession(sessionId, completed);
                }
                catch
                {
                    // Diagnostics cannot replace the transaction result.
                }
                finally
                {
                    _runtimeDiagnostics!.Consistency.EndMutation();
                }
            }
        }
    }

    private DateTimeOffset? TryGetDiagnosticsUtcNow()
    {
        if (_runtimeDiagnostics is null)
            return null;

        return _runtimeDiagnostics.GetUtcNowOrLast();
    }

    private void BeginDiagnosticsMutation(ref bool mutationStarted)
    {
        if (!mutationStarted && _runtimeDiagnostics is not null)
        {
            _runtimeDiagnostics.Consistency.BeginMutation();
            mutationStarted = true;
        }
    }

    private TResult ExecuteIntrospection<TResult>(
        long sessionId,
        Func<Database, TResult> action)
    {
        _gate.Wait();
        try
        {
            ThrowIfUnavailable();
            ThrowIfOwnedByOtherSession(sessionId);
            return action(GetDatabase());
        }
        finally
        {
            _gate.Release();
        }
    }

    private void ThrowIfUnavailable()
    {
        if (_poisoned)
            throw new InvalidOperationException(PoisonedMessage);

        if (_database is null)
            throw new InvalidOperationException("The pooled database is not available.");
    }

    private static bool IsPersistentSchemaMutation(Statement statement)
    {
        return statement switch
        {
            CreateTableStatement { IsTemporary: false } => true,
            CreateExternalTableStatement => true,
            DropTableStatement { IsTemporary: false } => true,
            PersistTempTableStatement => true,
            DropExternalTableStatement => true,
            AlterTableStatement => true,
            CreateIndexStatement => true,
            DropIndexStatement => true,
            CreateViewStatement => true,
            DropViewStatement => true,
            CreateTriggerStatement => true,
            DropTriggerStatement => true,
            CreateValidationRuleStatement => true,
            ConditionalStatement conditional =>
                conditional.Body.Any(IsPersistentSchemaMutation),
            _ => false,
        };
    }

    private void ThrowIfOwnedByOtherSession(long sessionId)
    {
        if (_transactionOwnerSessionId.HasValue && _transactionOwnerSessionId.Value != sessionId)
            throw new InvalidOperationException(BusyMessage);
    }

    private Database GetDatabase()
        => _database ?? throw new InvalidOperationException("The pooled database is not available.");

    private void TrackReaderSession(long sessionId, Database.ReaderSession readerSession)
    {
        if (!_readerSessions.TryGetValue(sessionId, out HashSet<Database.ReaderSession>? readers))
        {
            readers = new HashSet<Database.ReaderSession>();
            _readerSessions.Add(sessionId, readers);
        }

        readers.Add(readerSession);
    }

    private void UntrackReaderSession(long sessionId, Database.ReaderSession readerSession)
    {
        if (!_readerSessions.TryGetValue(sessionId, out HashSet<Database.ReaderSession>? readers))
            return;

        readers.Remove(readerSession);
        if (readers.Count == 0)
            _readerSessions.Remove(sessionId);
    }

    private void DisposeReaderSessions(long sessionId)
    {
        if (!_readerSessions.Remove(
                sessionId,
                out HashSet<Database.ReaderSession>? readers))
        {
            return;
        }

        foreach (Database.ReaderSession reader in readers)
            reader.Dispose();
    }

    private async ValueTask ReleaseReaderSessionAsync(
        long sessionId,
        Database.ReaderSession readerSession)
    {
        await _gate.WaitAsync();
        try
        {
            if (!_readerSessions.TryGetValue(
                    sessionId,
                    out HashSet<Database.ReaderSession>? readers) ||
                !readers.Remove(readerSession))
            {
                return;
            }

            if (readers.Count == 0)
                _readerSessions.Remove(sessionId);

            readerSession.Dispose();
        }
        finally
        {
            _gate.Release();
        }
    }

    private static async ValueTask<QueryResult> DetachQueryResultAsync(
        QueryResult result,
        CancellationToken cancellationToken)
    {
        if (!result.IsQuery)
            return result;

        await using (result)
        {
            List<DbValue[]> rows = await result.ToListAsync(cancellationToken);
            return QueryResult.FromMaterializedRows(result.Schema, rows);
        }
    }
}

internal sealed class CSharpDbConnectionPoolRetiredException : InvalidOperationException
{
    internal CSharpDbConnectionPoolRetiredException()
        : base("The connection pool is no longer accepting new sessions.")
    {
    }
}

internal sealed class PooledDatabaseSession : ICSharpDbSession
{
    private CSharpDbConnectionPool? _pool;
    private readonly CSharpDbConnectionPool _ownerPool;
    private readonly long _sessionId;

    internal PooledDatabaseSession(CSharpDbConnectionPool pool, long sessionId)
    {
        _pool = pool;
        _ownerPool = pool;
        _sessionId = sessionId;
    }

    public bool SupportsStructuredExecution => true;
    public CSharpDbObservabilityOptions? ObservabilityOptionsSnapshot =>
        _ownerPool.ObservabilityOptionsSnapshot;
    public CSharpDbRuntimeDiagnosticsState? RuntimeDiagnosticsState =>
        _ownerPool.RuntimeDiagnosticsState;
    public object RuntimeDiagnosticsIdentityKey => _ownerPool;
    public IDataRuntimeDiagnosticsContributor RuntimeDiagnosticsContributor => _ownerPool;
    public ICSharpDbObservabilityClient? RemoteObservabilityClient => null;
    internal int ActiveSnapshotReaderCountForTest =>
        _ownerPool.ActiveSnapshotReaderCountForTest;
    internal int TemporaryCleanupCountForTest =>
        _ownerPool.TemporaryCleanupCountForTest;

    public ValueTask<QueryResult> ExecuteAsync(
        string sql,
        CancellationToken cancellationToken = default)
        => GetPool().ExecuteAsync(_sessionId, sql, cancellationToken);

    public ValueTask<QueryResult> ExecuteAsync(
        string executionSql,
        string? observabilitySql,
        CancellationToken cancellationToken = default)
        => GetPool().ExecuteAsync(
            _sessionId,
            executionSql,
            observabilitySql,
            cancellationToken);

    public ValueTask<QueryResult> ExecuteAsync(
        string executionSql,
        string? observabilitySql,
        AdoCommandObservation? observation,
        CancellationToken cancellationToken = default)
        => GetPool().ExecuteAsync(
            _sessionId,
            executionSql,
            observabilitySql,
            observation,
            cancellationToken);

    public ValueTask<QueryResult> ExecuteAsync(
        Statement statement,
        CancellationToken cancellationToken = default)
        => GetPool().ExecuteAsync(_sessionId, statement, cancellationToken);

    public ValueTask<QueryResult> ExecuteAsync(
        Statement statement,
        string? observabilitySql,
        CancellationToken cancellationToken = default)
        => GetPool().ExecuteAsync(
            _sessionId,
            statement,
            observabilitySql,
            cancellationToken);

    public ValueTask<QueryResult> ExecuteAsync(
        Statement statement,
        string? observabilitySql,
        AdoCommandObservation? observation,
        CancellationToken cancellationToken = default)
        => GetPool().ExecuteAsync(
            _sessionId,
            statement,
            observabilitySql,
            observation,
            cancellationToken);

    public ValueTask<QueryResult> ExecuteAsync(
        SimpleInsertSql insert,
        CancellationToken cancellationToken = default)
        => GetPool().ExecuteAsync(_sessionId, insert, cancellationToken);

    public ValueTask<QueryResult> ExecuteAsync(
        SimpleInsertSql insert,
        string? observabilitySql,
        CancellationToken cancellationToken = default)
        => GetPool().ExecuteAsync(
            _sessionId,
            insert,
            observabilitySql,
            cancellationToken);

    public ValueTask<QueryResult> ExecuteAsync(
        SimpleInsertSql insert,
        string? observabilitySql,
        AdoCommandObservation? observation,
        CancellationToken cancellationToken = default)
        => GetPool().ExecuteAsync(
            _sessionId,
            insert,
            observabilitySql,
            observation,
            cancellationToken);

    public ValueTask BeginTransactionAsync(CancellationToken cancellationToken = default)
        => GetPool().BeginTransactionAsync(_sessionId, cancellationToken);

    public ValueTask CommitAsync(CancellationToken cancellationToken = default)
        => GetPool().CommitAsync(_sessionId, cancellationToken);

    public ValueTask RollbackAsync(CancellationToken cancellationToken = default)
        => GetPool().RollbackAsync(_sessionId, cancellationToken);

    public ValueTask SaveToFileAsync(
        string filePath,
        CancellationToken cancellationToken = default)
        => GetPool().SaveToFileAsync(_sessionId, filePath, cancellationToken);

    public IReadOnlyCollection<string> GetTableNames() => GetPool().GetTableNames(_sessionId);
    public TableSchema? GetTableSchema(string tableName) => GetPool().GetTableSchema(_sessionId, tableName);
    public IReadOnlyCollection<IndexSchema> GetIndexes() => GetPool().GetIndexes(_sessionId);
    public IReadOnlyCollection<string> GetViewNames() => GetPool().GetViewNames(_sessionId);
    public string? GetViewSql(string viewName) => GetPool().GetViewSql(_sessionId, viewName);
    public IReadOnlyCollection<TriggerSchema> GetTriggers() => GetPool().GetTriggers(_sessionId);

    public async ValueTask DisposeAsync()
    {
        CSharpDbConnectionPool? pool = _pool;
        _pool = null;

        if (pool is not null)
            await pool.ReleaseSessionAsync(_sessionId);
    }

    private CSharpDbConnectionPool GetPool()
        => _pool ?? throw new InvalidOperationException("Session is closed.");
}
