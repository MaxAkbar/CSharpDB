using System.Globalization;
using System.Runtime.InteropServices;

namespace CSharpDB.Benchmarks.Infrastructure;

internal sealed class SqliteCApiDatabase : IDisposable
{
    private const string NativeLibraryName = "e_sqlite3";

    internal const int Ok = 0;
    internal const int Busy = 5;
    internal const int Row = 100;
    internal const int Done = 101;

    private const int OpenReadWrite = 0x00000002;
    private const int OpenCreate = 0x00000004;
    private const int OpenNoMutex = 0x00008000;
    private const int OpenPrivateCache = 0x00040000;

    internal static readonly IntPtr TransientDestructor = new(-1);

    private IntPtr _handle;

    private SqliteCApiDatabase(string filePath, IntPtr handle)
    {
        FilePath = filePath;
        _handle = handle;
    }

    internal string FilePath { get; }

    internal static string LibraryVersion
        => Marshal.PtrToStringUTF8(sqlite3_libversion()) ?? "unknown";

    internal static SqliteCApiDatabase Open(string filePath)
    {
        int flags = OpenReadWrite | OpenCreate | OpenNoMutex | OpenPrivateCache;
        int rc = sqlite3_open_v2(filePath, out IntPtr handle, flags, IntPtr.Zero);
        if (rc != Ok || handle == IntPtr.Zero)
        {
            string message = handle != IntPtr.Zero
                ? GetErrorMessage(handle)
                : $"open rc={rc.ToString(CultureInfo.InvariantCulture)}";

            if (handle != IntPtr.Zero)
                sqlite3_close_v2(handle);

            throw new InvalidOperationException($"SQLite open failed for '{filePath}': {message}");
        }

        return new SqliteCApiDatabase(filePath, handle);
    }

    internal void ExecuteNonQuery(string sql)
    {
        ThrowIfDisposed();

        int rc = sqlite3_exec(_handle, sql, IntPtr.Zero, IntPtr.Zero, out IntPtr errorMessagePtr);
        if (rc == Ok)
            return;

        string? extraMessage = errorMessagePtr == IntPtr.Zero ? null : Marshal.PtrToStringUTF8(errorMessagePtr);
        if (errorMessagePtr != IntPtr.Zero)
            sqlite3_free(errorMessagePtr);

        throw CreateException("exec", rc, extraMessage ?? sql);
    }

    internal string ExecuteScalarText(string sql)
    {
        using var statement = Prepare(sql);
        int rc = statement.Step();
        if (rc != Row)
            throw statement.CreateException("step", rc);

        string? value = statement.GetColumnText(0);

        statement.Reset(allowBusy: false);
        return value ?? string.Empty;
    }

    internal int ExecuteScalarInt32(string sql)
    {
        using var statement = Prepare(sql);
        int rc = statement.Step();
        if (rc != Row)
            throw statement.CreateException("step", rc);

        int value = statement.GetColumnInt32(0);

        statement.Reset(allowBusy: false);
        return value;
    }

    internal void SetBusyTimeout(int timeoutMs)
    {
        ThrowIfDisposed();

        int rc = sqlite3_busy_timeout(_handle, timeoutMs);
        if (rc != Ok)
            throw CreateException("busy_timeout", rc, timeoutMs.ToString(CultureInfo.InvariantCulture));
    }

    internal SqliteCApiStatement Prepare(string sql)
    {
        ThrowIfDisposed();

        int rc = sqlite3_prepare_v2(_handle, sql, -1, out IntPtr statement, out _);
        if (rc != Ok || statement == IntPtr.Zero)
            throw CreateException("prepare", rc, sql);

        return new SqliteCApiStatement(this, statement, sql);
    }

    internal InvalidOperationException CreateException(string operation, int resultCode, string? detail = null)
    {
        string resultCodeText = resultCode.ToString(CultureInfo.InvariantCulture);
        string message = GetErrorMessage(_handle);

        if (!string.IsNullOrWhiteSpace(detail))
            return new InvalidOperationException($"SQLite {operation} failed rc={resultCodeText}: {message}. Detail: {detail}");

        return new InvalidOperationException($"SQLite {operation} failed rc={resultCodeText}: {message}");
    }

    public void Dispose()
    {
        if (_handle == IntPtr.Zero)
            return;

        sqlite3_close_v2(_handle);
        _handle = IntPtr.Zero;
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_handle == IntPtr.Zero, this);
    }

    private static string GetErrorMessage(IntPtr handle)
        => Marshal.PtrToStringUTF8(sqlite3_errmsg(handle)) ?? "unknown";

    [DllImport(NativeLibraryName, CallingConvention = CallingConvention.Cdecl)]
    private static extern int sqlite3_open_v2(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string filename,
        out IntPtr db,
        int flags,
        IntPtr zVfs);

    [DllImport(NativeLibraryName, CallingConvention = CallingConvention.Cdecl)]
    private static extern int sqlite3_close_v2(IntPtr db);

    [DllImport(NativeLibraryName, CallingConvention = CallingConvention.Cdecl)]
    private static extern int sqlite3_busy_timeout(IntPtr db, int milliseconds);

    [DllImport(NativeLibraryName, CallingConvention = CallingConvention.Cdecl)]
    private static extern int sqlite3_exec(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string sql,
        IntPtr callback,
        IntPtr callbackArgument,
        out IntPtr errorMessage);

    [DllImport(NativeLibraryName, CallingConvention = CallingConvention.Cdecl)]
    private static extern void sqlite3_free(IntPtr value);

    [DllImport(NativeLibraryName, CallingConvention = CallingConvention.Cdecl)]
    private static extern IntPtr sqlite3_errmsg(IntPtr db);

    [DllImport(NativeLibraryName, CallingConvention = CallingConvention.Cdecl)]
    private static extern IntPtr sqlite3_libversion();

    [DllImport(NativeLibraryName, CallingConvention = CallingConvention.Cdecl)]
    private static extern int sqlite3_prepare_v2(
        IntPtr db,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string sql,
        int bytes,
        out IntPtr statement,
        out IntPtr trailingSql);

    internal sealed class SqliteCApiStatement : IDisposable
    {
        private IntPtr _handle;
        private readonly SqliteCApiDatabase _database;
        private readonly string _sql;

        internal SqliteCApiStatement(SqliteCApiDatabase database, IntPtr handle, string sql)
        {
            _database = database;
            _handle = handle;
            _sql = sql;
        }

        internal void BindInt64(int parameterIndex, long value)
        {
            ThrowIfDisposed();

            int rc = sqlite3_bind_int64(_handle, parameterIndex, value);
            if (rc != Ok)
                throw CreateException("bind_int64", rc);
        }

        internal void BindText(int parameterIndex, byte[] utf8Value)
        {
            ThrowIfDisposed();
            ArgumentNullException.ThrowIfNull(utf8Value);

            int rc = sqlite3_bind_text(_handle, parameterIndex, utf8Value, utf8Value.Length, TransientDestructor);
            if (rc != Ok)
                throw CreateException("bind_text", rc);
        }

        internal int Step()
        {
            ThrowIfDisposed();
            return sqlite3_step(_handle);
        }

        internal void Reset(bool allowBusy)
        {
            ThrowIfDisposed();

            int rc = sqlite3_reset(_handle);
            if (rc == Ok)
                return;

            if (allowBusy && rc == Busy)
                return;

            throw CreateException("reset", rc);
        }

        internal void ClearBindings()
        {
            ThrowIfDisposed();

            int rc = sqlite3_clear_bindings(_handle);
            if (rc != Ok)
                throw CreateException("clear_bindings", rc);
        }

        internal string? GetColumnText(int columnIndex)
        {
            ThrowIfDisposed();
            IntPtr ptr = sqlite3_column_text(_handle, columnIndex);
            return ptr == IntPtr.Zero ? null : Marshal.PtrToStringUTF8(ptr);
        }

        internal int GetColumnInt32(int columnIndex)
        {
            ThrowIfDisposed();
            return sqlite3_column_int(_handle, columnIndex);
        }

        internal InvalidOperationException CreateException(string operation, int resultCode)
            => _database.CreateException(operation, resultCode, _sql);

        public void Dispose()
        {
            if (_handle == IntPtr.Zero)
                return;

            sqlite3_finalize(_handle);
            _handle = IntPtr.Zero;
        }

        private void ThrowIfDisposed()
        {
            ObjectDisposedException.ThrowIf(_handle == IntPtr.Zero, this);
        }

        [DllImport(NativeLibraryName, CallingConvention = CallingConvention.Cdecl)]
        private static extern int sqlite3_bind_int64(IntPtr statement, int parameterIndex, long value);

        [DllImport(NativeLibraryName, CallingConvention = CallingConvention.Cdecl)]
        private static extern int sqlite3_bind_text(
            IntPtr statement,
            int parameterIndex,
            byte[] value,
            int valueLength,
            IntPtr destructor);

        [DllImport(NativeLibraryName, CallingConvention = CallingConvention.Cdecl)]
        private static extern int sqlite3_step(IntPtr statement);

        [DllImport(NativeLibraryName, CallingConvention = CallingConvention.Cdecl)]
        private static extern int sqlite3_reset(IntPtr statement);

        [DllImport(NativeLibraryName, CallingConvention = CallingConvention.Cdecl)]
        private static extern int sqlite3_clear_bindings(IntPtr statement);

        [DllImport(NativeLibraryName, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr sqlite3_column_text(IntPtr statement, int columnIndex);

        [DllImport(NativeLibraryName, CallingConvention = CallingConvention.Cdecl)]
        private static extern int sqlite3_column_int(IntPtr statement, int columnIndex);

        [DllImport(NativeLibraryName, CallingConvention = CallingConvention.Cdecl)]
        private static extern int sqlite3_finalize(IntPtr statement);
    }
}
