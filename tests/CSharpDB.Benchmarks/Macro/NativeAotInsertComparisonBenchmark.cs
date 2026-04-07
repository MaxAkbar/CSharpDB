using System.Data.Common;
using System.Diagnostics;
using System.Reflection;
using System.Runtime.InteropServices;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Data;
using Microsoft.Data.Sqlite;

namespace CSharpDB.Benchmarks.Macro;

public static class NativeAotInsertComparisonBenchmark
{
    private const int BatchSize = 100;
    private static readonly TimeSpan WarmupDuration = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan MeasuredDuration = TimeSpan.FromSeconds(5);

    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        var results = new List<BenchmarkResult>(capacity: 12);

        foreach (ProviderKind provider in Enum.GetValues<ProviderKind>())
        {
            results.Add(await RunSingleInsertRawAsync(provider));
            results.Add(await RunSingleInsertPreparedAsync(provider));
            results.Add(await RunBatchInsertRawAsync(provider));
            results.Add(await RunBatchInsertPreparedAsync(provider));
        }

        return results;
    }

    private static async Task<BenchmarkResult> RunSingleInsertRawAsync(ProviderKind provider)
    {
        await using var context = await NativeCompareContext.CreateAsync(provider, "native-aot-compare-single");
        int nextId = 1_000_000;

        BenchmarkResult result = await MacroBenchmarkRunner.RunForDurationAsync(
            $"{context.NamePrefix}_Raw_SingleInsert_5s",
            WarmupDuration,
            MeasuredDuration,
            async () =>
            {
                int id = nextId++;
                string sql = $"INSERT INTO bench VALUES ({id}, {id * 10L}, '{GetCategory(id)}');";
                int rowsAffected = await context.ExecuteNonQueryAsync(sql);
                if (rowsAffected != 1)
                    throw new InvalidOperationException($"Expected one inserted row for id={id}, observed {rowsAffected}.");
            });

        return CloneResult(result, extraInfo: context.WithNotes("mode=raw-sql", "workload=single-row auto-commit"));
    }

    private static async Task<BenchmarkResult> RunSingleInsertPreparedAsync(ProviderKind provider)
    {
        await using var context = await NativeCompareContext.CreateAsync(provider, "native-aot-compare-prepared-single");
        int nextId = 2_000_000;

        BenchmarkResult result;
        if (provider == ProviderKind.CSharpDbNativeAot)
        {
            using var command = context.NativeDatabase.PrepareNonQuery("INSERT INTO bench VALUES (@id, @value, @category);");
            result = await MacroBenchmarkRunner.RunForDurationAsync(
                $"{context.NamePrefix}_Prepared_SingleInsert_5s",
                WarmupDuration,
                MeasuredDuration,
                async () =>
                {
                    int id = nextId++;
                    await command.ExecuteAsync(
                        ("@id", (object)id),
                        ("@value", id * 10L),
                        ("@category", GetCategory(id)));
                });
        }
        else
        {
            using DbCommand command = context.Connection.CreateCommand();
            command.CommandText = "INSERT INTO bench VALUES (@id, @value, @category);";
            DbParameter idParam = AddParameter(command, "@id", 0);
            DbParameter valueParam = AddParameter(command, "@value", 0L);
            DbParameter categoryParam = AddParameter(command, "@category", "");
            command.Prepare();

            result = await MacroBenchmarkRunner.RunForDurationAsync(
                $"{context.NamePrefix}_Prepared_SingleInsert_5s",
                WarmupDuration,
                MeasuredDuration,
                async () =>
                {
                    int id = nextId++;
                    idParam.Value = id;
                    valueParam.Value = id * 10L;
                    categoryParam.Value = GetCategory(id);
                    int rowsAffected = await command.ExecuteNonQueryAsync();
                    if (rowsAffected != 1)
                        throw new InvalidOperationException($"Expected one inserted row for id={id}, observed {rowsAffected}.");
                });
        }

        return CloneResult(result, extraInfo: context.WithNotes("mode=prepared", "workload=single-row auto-commit"));
    }

    private static async Task<BenchmarkResult> RunBatchInsertRawAsync(ProviderKind provider)
    {
        await using var context = await NativeCompareContext.CreateAsync(provider, "native-aot-compare-batch");
        int nextId = 2_000_000;

        BenchmarkResult transactionResult = await MacroBenchmarkRunner.RunForDurationAsync(
            $"{context.NamePrefix}_Raw_Batch100_5s",
            WarmupDuration,
            MeasuredDuration,
            async () =>
            {
                await context.BeginTransactionAsync();
                try
                {
                    for (int i = 0; i < BatchSize; i++)
                    {
                        int id = nextId++;
                        string sql = $"INSERT INTO bench VALUES ({id}, {id * 10L}, '{GetCategory(id)}');";
                        int rowsAffected = await context.ExecuteNonQueryAsync(sql);
                        if (rowsAffected != 1)
                            throw new InvalidOperationException($"Expected one inserted row for id={id}, observed {rowsAffected}.");
                    }

                    await context.CommitAsync();
                }
                catch
                {
                    try
                    {
                        await context.RollbackAsync();
                    }
                    catch
                    {
                        // Preserve the original benchmark failure.
                    }

                    throw;
                }
            });

        return CloneResult(
            transactionResult,
            totalOps: transactionResult.TotalOps * BatchSize,
            extraInfo: context.WithNotes(
                "mode=raw-sql",
                $"batch-size={BatchSize}",
                "throughput-unit=rows/sec from 100-row transactions",
                "workload=explicit transaction batch"));
    }

    private static async Task<BenchmarkResult> RunBatchInsertPreparedAsync(ProviderKind provider)
    {
        await using var context = await NativeCompareContext.CreateAsync(provider, "native-aot-compare-prepared-batch");
        int nextId = 4_000_000;

        BenchmarkResult transactionResult;
        if (provider == ProviderKind.CSharpDbNativeAot)
        {
            using var command = context.NativeDatabase.PrepareNonQuery("INSERT INTO bench VALUES (@id, @value, @category);");
            transactionResult = await MacroBenchmarkRunner.RunForDurationAsync(
                $"{context.NamePrefix}_Prepared_Batch100_5s",
                WarmupDuration,
                MeasuredDuration,
                async () =>
                {
                    await context.BeginTransactionAsync();
                    try
                    {
                        for (int i = 0; i < BatchSize; i++)
                        {
                            int id = nextId++;
                            await command.ExecuteAsync(
                                ("@id", (object)id),
                                ("@value", id * 10L),
                                ("@category", GetCategory(id)));
                        }

                        await context.CommitAsync();
                    }
                    catch
                    {
                        try
                        {
                            await context.RollbackAsync();
                        }
                        catch
                        {
                            // Preserve the original benchmark failure.
                        }

                        throw;
                    }
                });
        }
        else
        {
            using DbCommand command = context.Connection.CreateCommand();
            command.CommandText = "INSERT INTO bench VALUES (@id, @value, @category);";
            DbParameter idParam = AddParameter(command, "@id", 0);
            DbParameter valueParam = AddParameter(command, "@value", 0L);
            DbParameter categoryParam = AddParameter(command, "@category", "");
            command.Prepare();

            transactionResult = await MacroBenchmarkRunner.RunForDurationAsync(
                $"{context.NamePrefix}_Prepared_Batch100_5s",
                WarmupDuration,
                MeasuredDuration,
                async () =>
                {
                    await using DbTransaction transaction = await context.Connection.BeginTransactionAsync();
                    command.Transaction = transaction;
                    try
                    {
                        for (int i = 0; i < BatchSize; i++)
                        {
                            int id = nextId++;
                            idParam.Value = id;
                            valueParam.Value = id * 10L;
                            categoryParam.Value = GetCategory(id);
                            int rowsAffected = await command.ExecuteNonQueryAsync();
                            if (rowsAffected != 1)
                                throw new InvalidOperationException($"Expected one inserted row for id={id}, observed {rowsAffected}.");
                        }

                        await transaction.CommitAsync();
                    }
                    catch
                    {
                        try
                        {
                            await transaction.RollbackAsync();
                        }
                        catch
                        {
                            // Preserve the original benchmark failure.
                        }

                        throw;
                    }
                    finally
                    {
                        command.Transaction = null;
                    }
                });
        }

        return CloneResult(
            transactionResult,
            totalOps: transactionResult.TotalOps * BatchSize,
            extraInfo: context.WithNotes(
                "mode=prepared",
                $"batch-size={BatchSize}",
                "throughput-unit=rows/sec from 100-row transactions",
                "workload=explicit transaction batch"));
    }

    private static DbParameter AddParameter(DbCommand command, string name, object? value)
    {
        DbParameter parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value ?? DBNull.Value;
        command.Parameters.Add(parameter);
        return parameter;
    }

    private static BenchmarkResult CloneResult(
        BenchmarkResult source,
        int? totalOps = null,
        string? extraInfo = null)
    {
        return new BenchmarkResult
        {
            Name = source.Name,
            TotalOps = totalOps ?? source.TotalOps,
            ElapsedMs = source.ElapsedMs,
            P50Ms = source.P50Ms,
            P90Ms = source.P90Ms,
            P95Ms = source.P95Ms,
            P99Ms = source.P99Ms,
            P999Ms = source.P999Ms,
            MinMs = source.MinMs,
            MaxMs = source.MaxMs,
            MeanMs = source.MeanMs,
            StdDevMs = source.StdDevMs,
            ExtraInfo = extraInfo ?? source.ExtraInfo
        };
    }

    private static string GetCategory(int id)
        => (id % 4) switch
        {
            0 => "Alpha",
            1 => "Beta",
            2 => "Gamma",
            _ => "Delta",
        };

    private static string GetSqliteProviderVersion()
    {
        Assembly assembly = typeof(SqliteConnection).Assembly;
        string? informationalVersion = assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?
            .InformationalVersion;

        if (!string.IsNullOrWhiteSpace(informationalVersion))
            return informationalVersion.Split('+', 2)[0];

        return assembly.GetName().Version?.ToString() ?? "unknown";
    }

    private static string GetCSharpDbProviderVersion()
    {
        Assembly assembly = typeof(CSharpDbConnection).Assembly;
        string? informationalVersion = assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?
            .InformationalVersion;

        if (!string.IsNullOrWhiteSpace(informationalVersion))
            return informationalVersion.Split('+', 2)[0];

        return assembly.GetName().Version?.ToString() ?? "unknown";
    }

    private enum ProviderKind
    {
        CSharpDbAdoNet,
        CSharpDbNativeAot,
        SqliteAdoNet,
    }

    private sealed class NativeCompareContext : IAsyncDisposable
    {
        private readonly ProviderKind _provider;
        private readonly string _filePath;
        private readonly DbConnection? _connection;
        private readonly DbCommand? _command;
        private readonly NativeAotDatabase? _nativeDatabase;

        private NativeCompareContext(
            ProviderKind provider,
            string filePath,
            string namePrefix,
            string baseExtraInfo,
            DbConnection? connection,
            DbCommand? command,
            NativeAotDatabase? nativeDatabase)
        {
            _provider = provider;
            _filePath = filePath;
            NamePrefix = namePrefix;
            BaseExtraInfo = baseExtraInfo;
            _connection = connection;
            _command = command;
            _nativeDatabase = nativeDatabase;
        }

        internal string NamePrefix { get; }
        internal string BaseExtraInfo { get; }
        internal DbConnection Connection => _connection ?? throw new InvalidOperationException("ADO.NET connection is not available.");
        internal NativeAotDatabase NativeDatabase => _nativeDatabase ?? throw new InvalidOperationException("NativeAOT database is not available.");

        internal static async Task<NativeCompareContext> CreateAsync(ProviderKind provider, string prefix)
        {
            string filePath = Path.Combine(Path.GetTempPath(), $"{prefix}_{Guid.NewGuid():N}.db");

            switch (provider)
            {
                case ProviderKind.CSharpDbAdoNet:
                {
                    var connection = new CSharpDbConnection($"Data Source={filePath};Pooling=false");
                    await connection.OpenAsync();
                    DbCommand command = connection.CreateCommand();
                    command.CommandText = "CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER, category TEXT);";
                    await command.ExecuteNonQueryAsync();
                    return new NativeCompareContext(
                        provider,
                        filePath,
                        "NativeMix_CSharpDB_AdoNet_DefaultDurable",
                        $"provider=CSharpDB.Data/{GetCSharpDbProviderVersion()}, pooling=false, durability=default-durable, surface=adonet",
                        connection,
                        command,
                        null);
                }
                case ProviderKind.CSharpDbNativeAot:
                {
                    var nativeDatabase = NativeAotDatabase.Open(filePath);
                    int rowsAffected = nativeDatabase.ExecuteNonQuery("CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER, category TEXT);");
                    if (rowsAffected != 0)
                        throw new InvalidOperationException($"Expected CREATE TABLE rowsAffected=0, observed {rowsAffected}.");

                    return new NativeCompareContext(
                        provider,
                        filePath,
                        "NativeMix_CSharpDB_NativeAot_DefaultDurable",
                        $"provider=CSharpDB.Native/nativeaot, library={Path.GetFileName(NativeAotApi.Instance.LibraryPath)}, rid={NativeAotApi.Instance.RuntimeIdentifier}, durability=default-durable, surface=ffi",
                        null,
                        null,
                        nativeDatabase);
                }
                case ProviderKind.SqliteAdoNet:
                {
                    var connection = new SqliteConnection(new SqliteConnectionStringBuilder
                    {
                        DataSource = filePath,
                        Mode = SqliteOpenMode.ReadWriteCreate,
                        Cache = SqliteCacheMode.Private,
                        Pooling = false,
                        DefaultTimeout = 30,
                    }.ToString());
                    await connection.OpenAsync();
                    await ApplyAndVerifySqlitePragmasAsync(connection);
                    DbCommand command = connection.CreateCommand();
                    command.CommandText = "CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER, category TEXT);";
                    await command.ExecuteNonQueryAsync();
                    return new NativeCompareContext(
                        provider,
                        filePath,
                        "NativeMix_SQLite_AdoNet_WalFull",
                        $"provider=Microsoft.Data.Sqlite/{GetSqliteProviderVersion()}, cache=private, pooling=false, journal_mode=wal, synchronous=full, surface=adonet",
                        connection,
                        command,
                        null);
                }
                default:
                    throw new ArgumentOutOfRangeException(nameof(provider), provider, null);
            }
        }

        internal async Task<int> ExecuteNonQueryAsync(string sql)
        {
            if (_nativeDatabase != null)
                return await _nativeDatabase.ExecuteNonQueryAsync(sql);

            DbCommand command = _command ?? throw new InvalidOperationException("ADO.NET command is not available.");
            command.CommandText = sql;
            return await command.ExecuteNonQueryAsync();
        }

        internal async Task BeginTransactionAsync()
        {
            if (_nativeDatabase != null)
            {
                await _nativeDatabase.BeginAsync();
                return;
            }

            if (_command?.Transaction != null)
                throw new InvalidOperationException("A transaction is already active.");

            DbConnection connection = _connection ?? throw new InvalidOperationException("ADO.NET connection is not available.");
            _command!.Transaction = await connection.BeginTransactionAsync();
        }

        internal async Task CommitAsync()
        {
            if (_nativeDatabase != null)
            {
                await _nativeDatabase.CommitAsync();
                return;
            }

            DbTransaction transaction = _command?.Transaction ?? throw new InvalidOperationException("No active transaction.");
            await transaction.CommitAsync();
            await transaction.DisposeAsync();
            _command!.Transaction = null;
        }

        internal async Task RollbackAsync()
        {
            if (_nativeDatabase != null)
            {
                await _nativeDatabase.RollbackAsync();
                return;
            }

            DbTransaction? transaction = _command?.Transaction;
            if (transaction == null)
                return;

            await transaction.RollbackAsync();
            await transaction.DisposeAsync();
            _command!.Transaction = null;
        }

        internal string WithNotes(params string[] notes)
        {
            if (notes.Length == 0)
                return BaseExtraInfo;

            return $"{BaseExtraInfo}, {string.Join(", ", notes)}";
        }

        public async ValueTask DisposeAsync()
        {
            if (_command?.Transaction != null)
            {
                try
                {
                    await _command.Transaction.DisposeAsync();
                }
                catch
                {
                    // Best effort only.
                }
            }

            if (_command != null)
                await _command.DisposeAsync();

            if (_connection != null)
                await _connection.DisposeAsync();

            _nativeDatabase?.Dispose();

            DeleteFiles(_provider, _filePath);
        }

        private static async Task ApplyAndVerifySqlitePragmasAsync(SqliteConnection connection)
        {
            using var journalCommand = connection.CreateCommand();
            journalCommand.CommandText = "PRAGMA journal_mode=WAL;";
            string journalMode = (Convert.ToString(await journalCommand.ExecuteScalarAsync()) ?? string.Empty).Trim();
            if (!journalMode.Equals("wal", StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException($"Expected journal_mode=wal, observed '{journalMode}'.");

            using var syncSetCommand = connection.CreateCommand();
            syncSetCommand.CommandText = "PRAGMA synchronous=FULL;";
            await syncSetCommand.ExecuteNonQueryAsync();

            using var syncVerifyCommand = connection.CreateCommand();
            syncVerifyCommand.CommandText = "PRAGMA synchronous;";
            string verifiedSynchronous = (Convert.ToString(await syncVerifyCommand.ExecuteScalarAsync()) ?? string.Empty).Trim();
            if (!verifiedSynchronous.Equals("full", StringComparison.OrdinalIgnoreCase) &&
                !verifiedSynchronous.Equals("2", StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException($"Expected synchronous=FULL, observed '{verifiedSynchronous}'.");
            }
        }

        private static void DeleteFiles(ProviderKind provider, string filePath)
        {
            try { if (File.Exists(filePath)) File.Delete(filePath); } catch { }

            switch (provider)
            {
                case ProviderKind.CSharpDbAdoNet:
                case ProviderKind.CSharpDbNativeAot:
                    try { if (File.Exists(filePath + ".wal")) File.Delete(filePath + ".wal"); } catch { }
                    break;
                case ProviderKind.SqliteAdoNet:
                    try { if (File.Exists(filePath + "-wal")) File.Delete(filePath + "-wal"); } catch { }
                    try { if (File.Exists(filePath + "-shm")) File.Delete(filePath + "-shm"); } catch { }
                    break;
            }
        }
    }

    private sealed class NativeAotDatabase : IDisposable
    {
        private readonly IntPtr _handle;

        private NativeAotDatabase(IntPtr handle)
        {
            _handle = handle;
        }

        internal static NativeAotDatabase Open(string path)
        {
            IntPtr pathPtr = Marshal.StringToCoTaskMemUTF8(path);
            try
            {
                IntPtr handle = NativeAotApi.Instance.Open(pathPtr);
                if (handle == IntPtr.Zero)
                    throw new InvalidOperationException($"Failed to open NativeAOT database '{path}': {NativeAotApi.Instance.GetLastErrorMessage()}");

                return new NativeAotDatabase(handle);
            }
            finally
            {
                Marshal.FreeCoTaskMem(pathPtr);
            }
        }

        internal Task<int> ExecuteNonQueryAsync(string sql)
            => Task.FromResult(ExecuteNonQuery(sql));

        internal int ExecuteNonQuery(string sql)
        {
            IntPtr sqlPtr = Marshal.StringToCoTaskMemUTF8(sql);
            try
            {
                IntPtr resultHandle = NativeAotApi.Instance.Execute(_handle, sqlPtr);
                if (resultHandle == IntPtr.Zero)
                    throw new InvalidOperationException($"NativeAOT execute failed for SQL '{sql}': {NativeAotApi.Instance.GetLastErrorMessage()}");

                try
                {
                    return NativeAotApi.Instance.ResultRowsAffected(resultHandle);
                }
                finally
                {
                    NativeAotApi.Instance.ResultFree(resultHandle);
                }
            }
            finally
            {
                Marshal.FreeCoTaskMem(sqlPtr);
            }
        }

        internal Task BeginAsync()
        {
            NativeAotApi.Instance.Begin(_handle);
            return Task.CompletedTask;
        }

        internal Task CommitAsync()
        {
            NativeAotApi.Instance.Commit(_handle);
            return Task.CompletedTask;
        }

        internal Task RollbackAsync()
        {
            NativeAotApi.Instance.Rollback(_handle);
            return Task.CompletedTask;
        }

        internal NativeAotPreparedCommand PrepareNonQuery(string sql)
        {
            IntPtr sqlPtr = Marshal.StringToCoTaskMemUTF8(sql);
            try
            {
                IntPtr statementHandle = NativeAotApi.Instance.Prepare(_handle, sqlPtr);
                if (statementHandle == IntPtr.Zero)
                    throw new InvalidOperationException($"NativeAOT prepare failed for SQL '{sql}': {NativeAotApi.Instance.GetLastErrorMessage()}");

                return new NativeAotPreparedCommand(statementHandle);
            }
            finally
            {
                Marshal.FreeCoTaskMem(sqlPtr);
            }
        }

        public void Dispose()
        {
            NativeAotApi.Instance.Close(_handle);
        }
    }

    private sealed class NativeAotPreparedCommand : IDisposable
    {
        private readonly IntPtr _statementHandle;

        internal NativeAotPreparedCommand(IntPtr statementHandle)
        {
            _statementHandle = statementHandle;
        }

        internal Task<int> ExecuteAsync(params (string Name, object? Value)[] parameters)
            => Task.FromResult(Execute(parameters));

        internal int Execute(params (string Name, object? Value)[] parameters)
        {
            for (int i = 0; i < parameters.Length; i++)
            {
                var parameter = parameters[i];
                Bind(parameter.Name, parameter.Value);
            }

            IntPtr resultHandle = NativeAotApi.Instance.StatementExecute(_statementHandle);
            if (resultHandle == IntPtr.Zero)
                throw new InvalidOperationException($"NativeAOT prepared execution failed: {NativeAotApi.Instance.GetLastErrorMessage()}");

            try
            {
                return NativeAotApi.Instance.ResultRowsAffected(resultHandle);
            }
            finally
            {
                NativeAotApi.Instance.ResultFree(resultHandle);
            }
        }

        private void Bind(string name, object? value)
        {
            IntPtr namePtr = Marshal.StringToCoTaskMemUTF8(name);
            try
            {
                switch (value)
                {
                    case null:
                    case DBNull:
                        NativeAotApi.Instance.StatementBindNull(_statementHandle, namePtr);
                        break;
                    case string text:
                    {
                        IntPtr valuePtr = Marshal.StringToCoTaskMemUTF8(text);
                        try
                        {
                            NativeAotApi.Instance.StatementBindText(_statementHandle, namePtr, valuePtr);
                        }
                        finally
                        {
                            Marshal.FreeCoTaskMem(valuePtr);
                        }

                        break;
                    }
                    case long int64:
                        NativeAotApi.Instance.StatementBindInt64(_statementHandle, namePtr, int64);
                        break;
                    case int int32:
                        NativeAotApi.Instance.StatementBindInt64(_statementHandle, namePtr, int32);
                        break;
                    case short int16:
                        NativeAotApi.Instance.StatementBindInt64(_statementHandle, namePtr, int16);
                        break;
                    case byte uint8:
                        NativeAotApi.Instance.StatementBindInt64(_statementHandle, namePtr, uint8);
                        break;
                    case bool boolean:
                        NativeAotApi.Instance.StatementBindInt64(_statementHandle, namePtr, boolean ? 1 : 0);
                        break;
                    case double doubleValue:
                        NativeAotApi.Instance.StatementBindDouble(_statementHandle, namePtr, doubleValue);
                        break;
                    case float floatValue:
                        NativeAotApi.Instance.StatementBindDouble(_statementHandle, namePtr, floatValue);
                        break;
                    default:
                        throw new NotSupportedException($"Unsupported NativeAOT benchmark parameter type '{value.GetType().Name}'.");
                }
            }
            finally
            {
                Marshal.FreeCoTaskMem(namePtr);
            }
        }

        public void Dispose()
        {
            NativeAotApi.Instance.StatementFree(_statementHandle);
        }
    }

    private sealed class NativeAotApi
    {
        private readonly OpenDelegate _open;
        private readonly CloseDelegate _close;
        private readonly ExecuteDelegate _execute;
        private readonly PrepareDelegate _prepare;
        private readonly StatementBindInt64Delegate _statementBindInt64;
        private readonly StatementBindDoubleDelegate _statementBindDouble;
        private readonly StatementBindTextDelegate _statementBindText;
        private readonly StatementBindNullDelegate _statementBindNull;
        private readonly StatementExecuteDelegate _statementExecute;
        private readonly StatementFreeDelegate _statementFree;
        private readonly ResultRowsAffectedDelegate _resultRowsAffected;
        private readonly ResultFreeDelegate _resultFree;
        private readonly BeginDelegate _begin;
        private readonly CommitDelegate _commit;
        private readonly RollbackDelegate _rollback;
        private readonly LastErrorDelegate _lastError;
        private readonly ClearErrorDelegate _clearError;

        private NativeAotApi(string libraryPath)
        {
            LibraryPath = libraryPath;
            RuntimeIdentifier = DetectCurrentRid();
            IntPtr libraryHandle = NativeLibrary.Load(libraryPath);

            _open = GetExport<OpenDelegate>(libraryHandle, "csharpdb_open");
            _close = GetExport<CloseDelegate>(libraryHandle, "csharpdb_close");
            _execute = GetExport<ExecuteDelegate>(libraryHandle, "csharpdb_execute");
            _prepare = GetExport<PrepareDelegate>(libraryHandle, "csharpdb_prepare");
            _statementBindInt64 = GetExport<StatementBindInt64Delegate>(libraryHandle, "csharpdb_stmt_bind_int64");
            _statementBindDouble = GetExport<StatementBindDoubleDelegate>(libraryHandle, "csharpdb_stmt_bind_double");
            _statementBindText = GetExport<StatementBindTextDelegate>(libraryHandle, "csharpdb_stmt_bind_text");
            _statementBindNull = GetExport<StatementBindNullDelegate>(libraryHandle, "csharpdb_stmt_bind_null");
            _statementExecute = GetExport<StatementExecuteDelegate>(libraryHandle, "csharpdb_stmt_execute");
            _statementFree = GetExport<StatementFreeDelegate>(libraryHandle, "csharpdb_stmt_free");
            _resultRowsAffected = GetExport<ResultRowsAffectedDelegate>(libraryHandle, "csharpdb_result_rows_affected");
            _resultFree = GetExport<ResultFreeDelegate>(libraryHandle, "csharpdb_result_free");
            _begin = GetExport<BeginDelegate>(libraryHandle, "csharpdb_begin");
            _commit = GetExport<CommitDelegate>(libraryHandle, "csharpdb_commit");
            _rollback = GetExport<RollbackDelegate>(libraryHandle, "csharpdb_rollback");
            _lastError = GetExport<LastErrorDelegate>(libraryHandle, "csharpdb_last_error");
            _clearError = GetExport<ClearErrorDelegate>(libraryHandle, "csharpdb_clear_error");
        }

        internal static NativeAotApi Instance { get; } = new(ResolveLibraryPath());

        internal string LibraryPath { get; }
        internal string RuntimeIdentifier { get; }

        internal IntPtr Open(IntPtr pathPtr) => _open(pathPtr);

        internal void Close(IntPtr handle) => _close(handle);

        internal IntPtr Execute(IntPtr dbHandle, IntPtr sqlPtr) => _execute(dbHandle, sqlPtr);

        internal IntPtr Prepare(IntPtr dbHandle, IntPtr sqlPtr) => _prepare(dbHandle, sqlPtr);

        internal int ResultRowsAffected(IntPtr resultHandle) => _resultRowsAffected(resultHandle);

        internal void ResultFree(IntPtr resultHandle) => _resultFree(resultHandle);

        internal void StatementBindInt64(IntPtr statementHandle, IntPtr namePtr, long value)
        {
            if (_statementBindInt64(statementHandle, namePtr, value) != 0)
                throw new InvalidOperationException($"NativeAOT bind int64 failed: {GetLastErrorMessage()}");
        }

        internal void StatementBindDouble(IntPtr statementHandle, IntPtr namePtr, double value)
        {
            if (_statementBindDouble(statementHandle, namePtr, value) != 0)
                throw new InvalidOperationException($"NativeAOT bind double failed: {GetLastErrorMessage()}");
        }

        internal void StatementBindText(IntPtr statementHandle, IntPtr namePtr, IntPtr valuePtr)
        {
            if (_statementBindText(statementHandle, namePtr, valuePtr) != 0)
                throw new InvalidOperationException($"NativeAOT bind text failed: {GetLastErrorMessage()}");
        }

        internal void StatementBindNull(IntPtr statementHandle, IntPtr namePtr)
        {
            if (_statementBindNull(statementHandle, namePtr) != 0)
                throw new InvalidOperationException($"NativeAOT bind null failed: {GetLastErrorMessage()}");
        }

        internal IntPtr StatementExecute(IntPtr statementHandle) => _statementExecute(statementHandle);

        internal void StatementFree(IntPtr statementHandle) => _statementFree(statementHandle);

        internal void Begin(IntPtr dbHandle)
        {
            if (_begin(dbHandle) != 0)
                throw new InvalidOperationException($"NativeAOT begin failed: {GetLastErrorMessage()}");
        }

        internal void Commit(IntPtr dbHandle)
        {
            if (_commit(dbHandle) != 0)
                throw new InvalidOperationException($"NativeAOT commit failed: {GetLastErrorMessage()}");
        }

        internal void Rollback(IntPtr dbHandle)
        {
            if (_rollback(dbHandle) != 0)
                throw new InvalidOperationException($"NativeAOT rollback failed: {GetLastErrorMessage()}");
        }

        internal string GetLastErrorMessage()
        {
            IntPtr errorPtr = _lastError();
            string message = errorPtr == IntPtr.Zero
                ? "Unknown native error."
                : (Marshal.PtrToStringUTF8(errorPtr) ?? "Unknown native error.");
            _clearError();
            return message;
        }

        private static TDelegate GetExport<TDelegate>(IntPtr libraryHandle, string name)
            where TDelegate : Delegate
        {
            IntPtr export = NativeLibrary.GetExport(libraryHandle, name);
            return Marshal.GetDelegateForFunctionPointer<TDelegate>(export);
        }

        private static string ResolveLibraryPath()
        {
            string rid = DetectCurrentRid();
            string extension = RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
                ? ".dll"
                : RuntimeInformation.IsOSPlatform(OSPlatform.OSX)
                    ? ".dylib"
                    : ".so";

            DirectoryInfo? current = new(AppContext.BaseDirectory);
            while (current != null)
            {
                string repoCandidate = Path.Combine(current.FullName, "src", "CSharpDB.Native", "CSharpDB.Native.csproj");
                if (File.Exists(repoCandidate))
                {
                    string libraryPath = Path.Combine(
                        current.FullName,
                        "src",
                        "CSharpDB.Native",
                        "bin",
                        "Release",
                        "net10.0",
                        rid,
                        "publish",
                        "CSharpDB.Native" + extension);

                    if (File.Exists(libraryPath))
                        return libraryPath;

                    throw new FileNotFoundException(
                        $"NativeAOT library not found at '{libraryPath}'. Publish it first with: dotnet publish .\\src\\CSharpDB.Native\\CSharpDB.Native.csproj -c Release -r {rid}",
                        libraryPath);
                }

                current = current.Parent;
            }

            throw new FileNotFoundException("Could not locate repo root to resolve CSharpDB.Native publish output.");
        }

        private static string DetectCurrentRid()
        {
            string arch = RuntimeInformation.ProcessArchitecture switch
            {
                Architecture.X64 => "x64",
                Architecture.X86 => "x86",
                Architecture.Arm64 => "arm64",
                Architecture.Arm => "arm",
                _ => throw new PlatformNotSupportedException($"Unsupported architecture '{RuntimeInformation.ProcessArchitecture}'.")
            };

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                return $"win-{arch}";
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                return $"linux-{arch}";
            if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
                return $"osx-{arch}";

            throw new PlatformNotSupportedException("Unsupported operating system for NativeAOT benchmark.");
        }

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate IntPtr OpenDelegate(IntPtr pathPtr);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate void CloseDelegate(IntPtr dbHandle);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate IntPtr ExecuteDelegate(IntPtr dbHandle, IntPtr sqlPtr);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate IntPtr PrepareDelegate(IntPtr dbHandle, IntPtr sqlPtr);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate int StatementBindInt64Delegate(IntPtr statementHandle, IntPtr namePtr, long value);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate int StatementBindDoubleDelegate(IntPtr statementHandle, IntPtr namePtr, double value);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate int StatementBindTextDelegate(IntPtr statementHandle, IntPtr namePtr, IntPtr valuePtr);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate int StatementBindNullDelegate(IntPtr statementHandle, IntPtr namePtr);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate IntPtr StatementExecuteDelegate(IntPtr statementHandle);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate void StatementFreeDelegate(IntPtr statementHandle);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate int ResultRowsAffectedDelegate(IntPtr resultHandle);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate void ResultFreeDelegate(IntPtr resultHandle);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate int BeginDelegate(IntPtr dbHandle);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate int CommitDelegate(IntPtr dbHandle);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate int RollbackDelegate(IntPtr dbHandle);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate IntPtr LastErrorDelegate();

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate void ClearErrorDelegate();
    }
}
