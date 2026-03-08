/*
 * csharpdb.h — C API for the CSharpDB embedded database engine.
 *
 * This header describes the public interface of the NativeAOT shared library
 * (csharpdb.dll / libcsharpdb.so / libcsharpdb.dylib).
 *
 * All functions are thread-safe with respect to different database handles.
 * A single database handle must NOT be used concurrently from multiple threads.
 *
 * Error handling follows the errno pattern:
 *   - Functions returning a pointer return NULL on error.
 *   - Functions returning int return -1 on error (unless documented otherwise).
 *   - Call csharpdb_last_error() to get the error message after a failure.
 *
 * Usage example:
 *
 *   csharpdb_t db = csharpdb_open("mydata.db");
 *   if (!db) { fprintf(stderr, "%s\n", csharpdb_last_error()); exit(1); }
 *
 *   csharpdb_result_t r = csharpdb_execute(db, "SELECT id, name FROM users");
 *   int cols = csharpdb_result_column_count(r);
 *   while (csharpdb_result_next(r) == 1) {
 *       long long id   = csharpdb_result_get_int64(r, 0);
 *       const char* nm = csharpdb_result_get_text(r, 1);
 *       printf("%lld %s\n", id, nm);
 *   }
 *   csharpdb_result_free(r);
 *   csharpdb_close(db);
 */

#ifndef CSHARPDB_H
#define CSHARPDB_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------------------------------------------------ */
/*  Opaque handle types                                                */
/* ------------------------------------------------------------------ */

/** Opaque database handle. */
typedef void* csharpdb_t;

/** Opaque query result handle. */
typedef void* csharpdb_result_t;

/* ------------------------------------------------------------------ */
/*  Column / value type codes (matches CSharpDB.Core.DbType)           */
/* ------------------------------------------------------------------ */

#define CSHARPDB_NULL    0
#define CSHARPDB_INTEGER 1
#define CSHARPDB_REAL    2
#define CSHARPDB_TEXT    3
#define CSHARPDB_BLOB    4

/* ------------------------------------------------------------------ */
/*  Database lifecycle                                                 */
/* ------------------------------------------------------------------ */

/**
 * Open or create a database file.
 * @param path  UTF-8 file path.
 * @return Database handle, or NULL on error.
 */
csharpdb_t csharpdb_open(const char* path);

/**
 * Close a database and release all resources.
 * Safe to call with NULL.
 */
void csharpdb_close(csharpdb_t db);

/* ------------------------------------------------------------------ */
/*  SQL execution                                                      */
/* ------------------------------------------------------------------ */

/**
 * Execute a SQL statement.
 * @param db   Database handle.
 * @param sql  UTF-8 SQL string.
 * @return Result handle, or NULL on error.
 */
csharpdb_result_t csharpdb_execute(csharpdb_t db, const char* sql);

/* ------------------------------------------------------------------ */
/*  Result metadata                                                    */
/* ------------------------------------------------------------------ */

/**
 * Returns 1 if the result is from a SELECT query, 0 for DML/DDL.
 */
int csharpdb_result_is_query(csharpdb_result_t result);

/**
 * Returns the number of rows affected by INSERT/UPDATE/DELETE.
 */
int csharpdb_result_rows_affected(csharpdb_result_t result);

/**
 * Returns the number of columns in the result set.
 */
int csharpdb_result_column_count(csharpdb_result_t result);

/**
 * Returns the name of a column (UTF-8).
 * The pointer is valid until csharpdb_result_free().
 */
const char* csharpdb_result_column_name(csharpdb_result_t result, int column_index);

/* ------------------------------------------------------------------ */
/*  Row iteration                                                      */
/* ------------------------------------------------------------------ */

/**
 * Advance to the next row.
 * @return 1 if a row is available, 0 at end of results, -1 on error.
 */
int csharpdb_result_next(csharpdb_result_t result);

/**
 * Returns the type code of the value at the given column in the current row.
 * See CSHARPDB_NULL, CSHARPDB_INTEGER, etc.
 */
int csharpdb_result_column_type(csharpdb_result_t result, int column_index);

/**
 * Returns 1 if the column value is NULL, 0 otherwise.
 */
int csharpdb_result_is_null(csharpdb_result_t result, int column_index);

/**
 * Read a 64-bit integer from the current row.
 */
int64_t csharpdb_result_get_int64(csharpdb_result_t result, int column_index);

/**
 * Read a double-precision float from the current row.
 */
double csharpdb_result_get_double(csharpdb_result_t result, int column_index);

/**
 * Read a UTF-8 text value from the current row.
 * The pointer is valid until the next csharpdb_result_next() or csharpdb_result_free().
 */
const char* csharpdb_result_get_text(csharpdb_result_t result, int column_index);

/**
 * Read a blob from the current row.
 * @param out_size  Receives the blob size in bytes (may be NULL).
 * @return Pointer to blob data, valid until next row or result free.
 */
const void* csharpdb_result_get_blob(csharpdb_result_t result, int column_index, int* out_size);

/**
 * Free a result handle and all associated resources.
 * Safe to call with NULL.
 */
void csharpdb_result_free(csharpdb_result_t result);

/* ------------------------------------------------------------------ */
/*  Transactions                                                       */
/* ------------------------------------------------------------------ */

/**
 * Begin an explicit transaction.
 * @return 0 on success, -1 on error.
 */
int csharpdb_begin(csharpdb_t db);

/**
 * Commit the current transaction.
 * @return 0 on success, -1 on error.
 */
int csharpdb_commit(csharpdb_t db);

/**
 * Rollback the current transaction.
 * @return 0 on success, -1 on error.
 */
int csharpdb_rollback(csharpdb_t db);

/* ------------------------------------------------------------------ */
/*  Error handling                                                     */
/* ------------------------------------------------------------------ */

/**
 * Returns the last error message (UTF-8), or NULL if no error.
 * The pointer is valid until the next API call on the same thread.
 */
const char* csharpdb_last_error(void);

/**
 * Returns the last error code. 0 = no error, -1 = generic error.
 * Positive values correspond to CSharpDB.Core.ErrorCode values.
 */
int csharpdb_last_error_code(void);

/**
 * Clear the error state for the current thread.
 */
void csharpdb_clear_error(void);

#ifdef __cplusplus
}
#endif

#endif /* CSHARPDB_H */
