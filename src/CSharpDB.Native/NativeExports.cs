using System.Runtime.InteropServices;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using CSharpDB.Core;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Storage.Diagnostics;

namespace CSharpDB.Native;

/// <summary>
/// C-compatible entry points exported from the NativeAOT shared library.
/// Every public method marked [UnmanagedCallersOnly] becomes a symbol in
/// the resulting .dll / .so / .dylib that any language can call via FFI.
///
/// Handles are opaque IntPtr wrappers around GCHandle, preventing the GC
/// from collecting managed objects while foreign code holds a reference.
/// </summary>
public static class NativeExports
{
    // ================================================================
    //  Database lifecycle
    // ================================================================

    /// <summary>
    /// Open or create a database file.
    /// Returns an opaque database handle, or IntPtr.Zero on error
    /// (call csharpdb_last_error for details).
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "csharpdb_open")]
    public static IntPtr Open(IntPtr pathPtr)
    {
        try
        {
            string path = Marshal.PtrToStringUTF8(pathPtr)!;
            var db = Database.OpenAsync(path).AsTask().GetAwaiter().GetResult();
            return HandleTable.Alloc(db);
        }
        catch (Exception ex)
        {
            ErrorState.Set(ex);
            return IntPtr.Zero;
        }
    }

    /// <summary>
    /// Close a database and free its handle. Safe to call with IntPtr.Zero.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "csharpdb_close")]
    public static void Close(IntPtr dbHandle)
    {
        if (dbHandle == IntPtr.Zero) return;

        try
        {
            var db = HandleTable.Get<Database>(dbHandle);
            db.DisposeAsync().AsTask().GetAwaiter().GetResult();
            HandleTable.Free(dbHandle);
        }
        catch (Exception ex)
        {
            ErrorState.Set(ex);
        }
    }

    // ================================================================
    //  SQL execution
    // ================================================================

    /// <summary>
    /// Execute a SQL statement. Returns a result handle.
    /// For DML/DDL the result contains rows-affected count.
    /// For SELECT the result contains rows that must be iterated.
    /// Returns IntPtr.Zero on error.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "csharpdb_execute")]
    public static IntPtr Execute(IntPtr dbHandle, IntPtr sqlPtr)
    {
        try
        {
            var db = HandleTable.Get<Database>(dbHandle);
            string sql = Marshal.PtrToStringUTF8(sqlPtr)!;
            var result = db.ExecuteAsync(sql).AsTask().GetAwaiter().GetResult();
            return HandleTable.Alloc(result);
        }
        catch (Exception ex)
        {
            ErrorState.Set(ex);
            return IntPtr.Zero;
        }
    }

    // ================================================================
    //  Result navigation
    // ================================================================

    /// <summary>
    /// Returns 1 if the result is a query (SELECT), 0 otherwise.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "csharpdb_result_is_query")]
    public static int ResultIsQuery(IntPtr resultHandle)
    {
        try
        {
            var result = HandleTable.Get<QueryResult>(resultHandle);
            return result.IsQuery ? 1 : 0;
        }
        catch (Exception ex)
        {
            ErrorState.Set(ex);
            return 0;
        }
    }

    /// <summary>
    /// Returns the number of rows affected by a DML/DDL statement.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "csharpdb_result_rows_affected")]
    public static int ResultRowsAffected(IntPtr resultHandle)
    {
        try
        {
            var result = HandleTable.Get<QueryResult>(resultHandle);
            return result.RowsAffected;
        }
        catch (Exception ex)
        {
            ErrorState.Set(ex);
            return -1;
        }
    }

    /// <summary>
    /// Returns the number of columns in the result schema.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "csharpdb_result_column_count")]
    public static int ResultColumnCount(IntPtr resultHandle)
    {
        try
        {
            var result = HandleTable.Get<QueryResult>(resultHandle);
            return result.Schema.Length;
        }
        catch (Exception ex)
        {
            ErrorState.Set(ex);
            return -1;
        }
    }

    /// <summary>
    /// Returns the name of a column by index. Caller must NOT free the pointer.
    /// The string is valid until the result is freed.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "csharpdb_result_column_name")]
    public static IntPtr ResultColumnName(IntPtr resultHandle, int columnIndex)
    {
        try
        {
            var result = HandleTable.Get<QueryResult>(resultHandle);
            if (columnIndex < 0 || columnIndex >= result.Schema.Length)
                return IntPtr.Zero;

            string name = result.Schema[columnIndex].Name;
            return StringCache.GetOrAdd(resultHandle, columnIndex, name);
        }
        catch (Exception ex)
        {
            ErrorState.Set(ex);
            return IntPtr.Zero;
        }
    }

    /// <summary>
    /// Advance to the next row. Returns 1 if a row is available, 0 at end, -1 on error.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "csharpdb_result_next")]
    public static int ResultNext(IntPtr resultHandle)
    {
        try
        {
            var result = HandleTable.Get<QueryResult>(resultHandle);
            bool hasRow = result.MoveNextAsync().AsTask().GetAwaiter().GetResult();
            return hasRow ? 1 : 0;
        }
        catch (Exception ex)
        {
            ErrorState.Set(ex);
            return -1;
        }
    }

    /// <summary>
    /// Returns the DbType of the value at the given column in the current row.
    /// 0=Null, 1=Integer, 2=Real, 3=Text, 4=Blob. Returns -1 on error.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "csharpdb_result_column_type")]
    public static int ResultColumnType(IntPtr resultHandle, int columnIndex)
    {
        try
        {
            var result = HandleTable.Get<QueryResult>(resultHandle);
            var row = result.Current;
            if (columnIndex < 0 || columnIndex >= row.Length)
                return -1;
            return (int)row[columnIndex].Type;
        }
        catch (Exception ex)
        {
            ErrorState.Set(ex);
            return -1;
        }
    }

    /// <summary>
    /// Returns 1 if the column value is NULL, 0 otherwise.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "csharpdb_result_is_null")]
    public static int ResultIsNull(IntPtr resultHandle, int columnIndex)
    {
        try
        {
            var result = HandleTable.Get<QueryResult>(resultHandle);
            var row = result.Current;
            if (columnIndex < 0 || columnIndex >= row.Length)
                return 1;
            return row[columnIndex].IsNull ? 1 : 0;
        }
        catch (Exception ex)
        {
            ErrorState.Set(ex);
            return 1;
        }
    }

    /// <summary>
    /// Read an integer value from the current row at the given column index.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "csharpdb_result_get_int64")]
    public static long ResultGetInt64(IntPtr resultHandle, int columnIndex)
    {
        try
        {
            var result = HandleTable.Get<QueryResult>(resultHandle);
            return result.Current[columnIndex].AsInteger;
        }
        catch (Exception ex)
        {
            ErrorState.Set(ex);
            return 0;
        }
    }

    /// <summary>
    /// Read a real (double) value from the current row at the given column index.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "csharpdb_result_get_double")]
    public static double ResultGetDouble(IntPtr resultHandle, int columnIndex)
    {
        try
        {
            var result = HandleTable.Get<QueryResult>(resultHandle);
            return result.Current[columnIndex].AsReal;
        }
        catch (Exception ex)
        {
            ErrorState.Set(ex);
            return 0.0;
        }
    }

    /// <summary>
    /// Read a text value from the current row. Returns a UTF-8 string pointer.
    /// The pointer is valid until the next call to csharpdb_result_next or csharpdb_result_free.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "csharpdb_result_get_text")]
    public static IntPtr ResultGetText(IntPtr resultHandle, int columnIndex)
    {
        try
        {
            var result = HandleTable.Get<QueryResult>(resultHandle);
            string text = result.Current[columnIndex].AsText;
            return StringCache.SetCurrentRowText(resultHandle, columnIndex, text);
        }
        catch (Exception ex)
        {
            ErrorState.Set(ex);
            return IntPtr.Zero;
        }
    }

    /// <summary>
    /// Read a blob value from the current row. Writes the blob size to outSize.
    /// Returns a pointer to the blob data. Valid until next row or result free.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "csharpdb_result_get_blob")]
    public static IntPtr ResultGetBlob(IntPtr resultHandle, int columnIndex, IntPtr outSize)
    {
        try
        {
            var result = HandleTable.Get<QueryResult>(resultHandle);
            byte[] blob = result.Current[columnIndex].AsBlob;
            if (outSize != IntPtr.Zero)
                Marshal.WriteInt32(outSize, blob.Length);
            return BlobCache.SetCurrentRowBlob(resultHandle, columnIndex, blob);
        }
        catch (Exception ex)
        {
            ErrorState.Set(ex);
            if (outSize != IntPtr.Zero)
                Marshal.WriteInt32(outSize, 0);
            return IntPtr.Zero;
        }
    }

    /// <summary>
    /// Free a result handle. Safe to call with IntPtr.Zero.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "csharpdb_result_free")]
    public static void ResultFree(IntPtr resultHandle)
    {
        if (resultHandle == IntPtr.Zero) return;

        try
        {
            var result = HandleTable.Get<QueryResult>(resultHandle);
            result.DisposeAsync().AsTask().GetAwaiter().GetResult();
            StringCache.Remove(resultHandle);
            BlobCache.Remove(resultHandle);
            HandleTable.Free(resultHandle);
        }
        catch (Exception ex)
        {
            ErrorState.Set(ex);
        }
    }

    // ================================================================
    //  Transactions
    // ================================================================

    /// <summary>
    /// Begin an explicit transaction. Returns 0 on success, -1 on error.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "csharpdb_begin")]
    public static int Begin(IntPtr dbHandle)
    {
        try
        {
            var db = HandleTable.Get<Database>(dbHandle);
            db.BeginTransactionAsync().AsTask().GetAwaiter().GetResult();
            return 0;
        }
        catch (Exception ex)
        {
            ErrorState.Set(ex);
            return -1;
        }
    }

    /// <summary>
    /// Commit the current transaction. Returns 0 on success, -1 on error.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "csharpdb_commit")]
    public static int Commit(IntPtr dbHandle)
    {
        try
        {
            var db = HandleTable.Get<Database>(dbHandle);
            db.CommitAsync().AsTask().GetAwaiter().GetResult();
            return 0;
        }
        catch (Exception ex)
        {
            ErrorState.Set(ex);
            return -1;
        }
    }

    /// <summary>
    /// Rollback the current transaction. Returns 0 on success, -1 on error.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "csharpdb_rollback")]
    public static int Rollback(IntPtr dbHandle)
    {
        try
        {
            var db = HandleTable.Get<Database>(dbHandle);
            db.RollbackAsync().AsTask().GetAwaiter().GetResult();
            return 0;
        }
        catch (Exception ex)
        {
            ErrorState.Set(ex);
            return -1;
        }
    }

    // ================================================================
    //  Diagnostics and maintenance
    // ================================================================

    /// <summary>
    /// Inspect a database file and return the report as JSON.
    /// Caller must free the returned string with csharpdb_string_free.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "csharpdb_inspect_storage_json")]
    public static IntPtr InspectStorageJson(IntPtr pathPtr, int includePages)
    {
        try
        {
            string path = ReadRequiredUtf8String(pathPtr, nameof(pathPtr));
            var report = DatabaseInspector.InspectAsync(
                path,
                new DatabaseInspectOptions { IncludePages = includePages != 0 },
                CancellationToken.None).AsTask().GetAwaiter().GetResult();
            return SerializeJson(report, NativeJsonContext.Default.DatabaseInspectReport);
        }
        catch (Exception ex)
        {
            ErrorState.Set(ex);
            return IntPtr.Zero;
        }
    }

    /// <summary>
    /// Inspect the companion WAL file and return the report as JSON.
    /// Caller must free the returned string with csharpdb_string_free.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "csharpdb_inspect_wal_json")]
    public static IntPtr InspectWalJson(IntPtr pathPtr)
    {
        try
        {
            string path = ReadRequiredUtf8String(pathPtr, nameof(pathPtr));
            var report = WalInspector.InspectAsync(path, options: null, CancellationToken.None).AsTask().GetAwaiter().GetResult();
            return SerializeJson(report, NativeJsonContext.Default.WalInspectReport);
        }
        catch (Exception ex)
        {
            ErrorState.Set(ex);
            return IntPtr.Zero;
        }
    }

    /// <summary>
    /// Inspect a single database page and return the report as JSON.
    /// Caller must free the returned string with csharpdb_string_free.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "csharpdb_inspect_page_json")]
    public static IntPtr InspectPageJson(IntPtr pathPtr, uint pageId, int includeHex)
    {
        try
        {
            string path = ReadRequiredUtf8String(pathPtr, nameof(pathPtr));
            var report = DatabaseInspector.InspectPageAsync(path, pageId, includeHex != 0, CancellationToken.None).AsTask().GetAwaiter().GetResult();
            return SerializeJson(report, NativeJsonContext.Default.PageInspectReport);
        }
        catch (Exception ex)
        {
            ErrorState.Set(ex);
            return IntPtr.Zero;
        }
    }

    /// <summary>
    /// Inspect indexes and return the report as JSON.
    /// Caller must free the returned string with csharpdb_string_free.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "csharpdb_check_indexes_json")]
    public static IntPtr CheckIndexesJson(IntPtr pathPtr, IntPtr indexNamePtr, int sampleSize)
    {
        try
        {
            string path = ReadRequiredUtf8String(pathPtr, nameof(pathPtr));
            string? indexName = ReadOptionalUtf8String(indexNamePtr);
            int? normalizedSampleSize = sampleSize > 0 ? sampleSize : null;
            var report = IndexInspector.CheckAsync(path, indexName, normalizedSampleSize, CancellationToken.None).AsTask().GetAwaiter().GetResult();
            return SerializeJson(report, NativeJsonContext.Default.IndexInspectReport);
        }
        catch (Exception ex)
        {
            ErrorState.Set(ex);
            return IntPtr.Zero;
        }
    }

    /// <summary>
    /// Compute maintenance metrics and return the report as JSON.
    /// Caller must free the returned string with csharpdb_string_free.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "csharpdb_get_maintenance_report_json")]
    public static IntPtr GetMaintenanceReportJson(IntPtr pathPtr)
    {
        try
        {
            string path = ReadRequiredUtf8String(pathPtr, nameof(pathPtr));
            var report = DatabaseMaintenanceCoordinator.GetMaintenanceReportAsync(path, CancellationToken.None).AsTask().GetAwaiter().GetResult();
            return SerializeJson(report, NativeJsonContext.Default.DatabaseMaintenanceReport);
        }
        catch (Exception ex)
        {
            ErrorState.Set(ex);
            return IntPtr.Zero;
        }
    }

    /// <summary>
    /// Rebuild indexes and return the result as JSON.
    /// Caller must free the returned string with csharpdb_string_free.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "csharpdb_reindex_json")]
    public static IntPtr ReindexJson(IntPtr pathPtr, int scope, IntPtr namePtr)
    {
        try
        {
            string path = ReadRequiredUtf8String(pathPtr, nameof(pathPtr));
            var request = new DatabaseReindexRequest
            {
                Scope = scope switch
                {
                    0 => DatabaseReindexScope.All,
                    1 => DatabaseReindexScope.Table,
                    2 => DatabaseReindexScope.Index,
                    _ => throw new ArgumentOutOfRangeException(nameof(scope), scope, "Scope must be 0 (all), 1 (table), or 2 (index)."),
                },
                Name = ReadOptionalUtf8String(namePtr),
            };

            var result = DatabaseMaintenanceCoordinator.ReindexAsync(path, request, CancellationToken.None).AsTask().GetAwaiter().GetResult();
            return SerializeJson(result, NativeJsonContext.Default.DatabaseReindexResult);
        }
        catch (Exception ex)
        {
            ErrorState.Set(ex);
            return IntPtr.Zero;
        }
    }

    /// <summary>
    /// Vacuum a database file and return the result as JSON.
    /// Caller must free the returned string with csharpdb_string_free.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "csharpdb_vacuum_json")]
    public static IntPtr VacuumJson(IntPtr pathPtr)
    {
        try
        {
            string path = ReadRequiredUtf8String(pathPtr, nameof(pathPtr));
            var result = DatabaseMaintenanceCoordinator.VacuumAsync(path, CancellationToken.None).AsTask().GetAwaiter().GetResult();
            return SerializeJson(result, NativeJsonContext.Default.DatabaseVacuumResult);
        }
        catch (Exception ex)
        {
            ErrorState.Set(ex);
            return IntPtr.Zero;
        }
    }

    /// <summary>
    /// Free an unmanaged UTF-8 string returned by a JSON export.
    /// Safe to call with IntPtr.Zero.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "csharpdb_string_free")]
    public static void StringFree(IntPtr valuePtr)
    {
        Utf8StringMemory.Free(valuePtr);
    }

    /// <summary>
    /// Return the length, in bytes, of a UTF-8 string returned by a JSON export.
    /// Safe to call with IntPtr.Zero.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "csharpdb_string_length")]
    public static int StringLength(IntPtr valuePtr)
    {
        if (valuePtr == IntPtr.Zero)
            return 0;

        int length = 0;
        while (Marshal.ReadByte(valuePtr, length) != 0)
            length++;

        return length;
    }

    // ================================================================
    //  Error reporting
    // ================================================================

    /// <summary>
    /// Returns the last error message as a UTF-8 string, or IntPtr.Zero if no error.
    /// The pointer is valid until the next API call on the same thread.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "csharpdb_last_error")]
    public static IntPtr LastError()
    {
        return ErrorState.GetMessagePtr();
    }

    /// <summary>
    /// Returns the last error code. 0 = no error.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "csharpdb_last_error_code")]
    public static int LastErrorCode()
    {
        return ErrorState.GetCode();
    }

    /// <summary>
    /// Clear the last error state.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "csharpdb_clear_error")]
    public static void ClearError()
    {
        ErrorState.Clear();
    }

    private static string ReadRequiredUtf8String(IntPtr valuePtr, string paramName)
    {
        if (valuePtr == IntPtr.Zero)
            throw new ArgumentNullException(paramName);

        return Marshal.PtrToStringUTF8(valuePtr)
            ?? throw new ArgumentException("Expected a UTF-8 string pointer.", paramName);
    }

    private static string? ReadOptionalUtf8String(IntPtr valuePtr)
    {
        if (valuePtr == IntPtr.Zero)
            return null;

        string? value = Marshal.PtrToStringUTF8(valuePtr);
        return string.IsNullOrWhiteSpace(value) ? null : value.Trim();
    }

    private static IntPtr SerializeJson<TValue>(TValue value, JsonTypeInfo<TValue> typeInfo)
    {
        string json = JsonSerializer.Serialize(value, typeInfo);
        return Utf8StringMemory.Allocate(json);
    }
}
