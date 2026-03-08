using System.Runtime.InteropServices;
using CSharpDB.Core;
using CSharpDB.Engine;
using CSharpDB.Execution;

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
}
