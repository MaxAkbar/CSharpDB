"""csharpdb.py — Python wrapper for the CSharpDB NativeAOT shared library.

Usage:
    from csharpdb import CSharpDB

    with CSharpDB("path/to/CSharpDB.Native.dll") as db:
        db.open("mydata.db")
        db.execute("CREATE TABLE t (id INTEGER, name TEXT)")
        db.execute("INSERT INTO t VALUES (1, 'Alice')")
        rows = db.query("SELECT * FROM t")
        print(rows)  # [{'id': 1, 'name': 'Alice'}]
"""

import ctypes
import sys


class CSharpDBError(Exception):
    """Raised when a CSharpDB native call fails."""

    def __init__(self, message: str, code: int = -1):
        super().__init__(message)
        self.code = code


class CSharpDB:
    """Thin Python wrapper around the CSharpDB NativeAOT shared library."""

    # Type codes matching CSharpDB.Core.DbType
    NULL, INTEGER, REAL, TEXT, BLOB = 0, 1, 2, 3, 4

    def __init__(self, lib_path: str | None = None):
        """Load the native library.

        Args:
            lib_path: Path to CSharpDB.Native.dll/.so/.dylib.
                      Auto-detects by platform if not provided.
        """
        if lib_path is None:
            if sys.platform == "win32":
                lib_path = "./CSharpDB.Native.dll"
            elif sys.platform == "darwin":
                lib_path = "./CSharpDB.Native.dylib"
            else:
                lib_path = "./CSharpDB.Native.so"

        self._lib = ctypes.CDLL(lib_path)
        self._setup_signatures()
        self._db = None

    def _setup_signatures(self):
        L = self._lib
        vp = ctypes.c_void_p

        # Database lifecycle
        L.csharpdb_open.restype = vp
        L.csharpdb_open.argtypes = [ctypes.c_char_p]
        L.csharpdb_close.restype = None
        L.csharpdb_close.argtypes = [vp]

        # SQL execution
        L.csharpdb_execute.restype = vp
        L.csharpdb_execute.argtypes = [vp, ctypes.c_char_p]

        # Result metadata
        L.csharpdb_result_is_query.restype = ctypes.c_int
        L.csharpdb_result_is_query.argtypes = [vp]
        L.csharpdb_result_next.restype = ctypes.c_int
        L.csharpdb_result_next.argtypes = [vp]
        L.csharpdb_result_rows_affected.restype = ctypes.c_int
        L.csharpdb_result_rows_affected.argtypes = [vp]
        L.csharpdb_result_column_count.restype = ctypes.c_int
        L.csharpdb_result_column_count.argtypes = [vp]
        L.csharpdb_result_column_name.restype = ctypes.c_char_p
        L.csharpdb_result_column_name.argtypes = [vp, ctypes.c_int]

        # Value access
        L.csharpdb_result_column_type.restype = ctypes.c_int
        L.csharpdb_result_column_type.argtypes = [vp, ctypes.c_int]
        L.csharpdb_result_is_null.restype = ctypes.c_int
        L.csharpdb_result_is_null.argtypes = [vp, ctypes.c_int]
        L.csharpdb_result_get_int64.restype = ctypes.c_int64
        L.csharpdb_result_get_int64.argtypes = [vp, ctypes.c_int]
        L.csharpdb_result_get_double.restype = ctypes.c_double
        L.csharpdb_result_get_double.argtypes = [vp, ctypes.c_int]
        L.csharpdb_result_get_text.restype = ctypes.c_char_p
        L.csharpdb_result_get_text.argtypes = [vp, ctypes.c_int]
        L.csharpdb_result_get_blob.restype = vp
        L.csharpdb_result_get_blob.argtypes = [vp, ctypes.c_int, ctypes.POINTER(ctypes.c_int)]
        L.csharpdb_result_free.restype = None
        L.csharpdb_result_free.argtypes = [vp]

        # Transactions
        L.csharpdb_begin.restype = ctypes.c_int
        L.csharpdb_begin.argtypes = [vp]
        L.csharpdb_commit.restype = ctypes.c_int
        L.csharpdb_commit.argtypes = [vp]
        L.csharpdb_rollback.restype = ctypes.c_int
        L.csharpdb_rollback.argtypes = [vp]

        # Error handling
        L.csharpdb_last_error.restype = ctypes.c_char_p
        L.csharpdb_last_error_code.restype = ctypes.c_int
        L.csharpdb_clear_error.restype = None

    def _check_error(self, context: str = ""):
        err = self._lib.csharpdb_last_error()
        if err:
            code = self._lib.csharpdb_last_error_code()
            raise CSharpDBError(
                f"CSharpDB error{' (' + context + ')' if context else ''}: {err.decode()}",
                code,
            )

    def open(self, path: str):
        """Open or create a database file."""
        self._db = self._lib.csharpdb_open(path.encode("utf-8"))
        if not self._db:
            self._check_error("open")

    def close(self):
        """Close the database. Safe to call multiple times."""
        if self._db:
            self._lib.csharpdb_close(self._db)
            self._db = None

    def execute(self, sql: str) -> int:
        """Execute a non-query statement (INSERT, UPDATE, DELETE, DDL).

        Returns:
            Number of rows affected.
        """
        r = self._lib.csharpdb_execute(self._db, sql.encode("utf-8"))
        if not r:
            self._check_error("execute")
        affected = self._lib.csharpdb_result_rows_affected(r)
        self._lib.csharpdb_result_free(r)
        return affected

    def query(self, sql: str) -> list[dict]:
        """Execute a SELECT and return rows as a list of dicts.

        Value types:
            INTEGER -> int
            REAL    -> float
            TEXT    -> str
            BLOB    -> bytes
            NULL    -> None
        """
        r = self._lib.csharpdb_execute(self._db, sql.encode("utf-8"))
        if not r:
            self._check_error("query")

        col_count = self._lib.csharpdb_result_column_count(r)
        columns = []
        for i in range(col_count):
            name = self._lib.csharpdb_result_column_name(r, i)
            columns.append(name.decode() if name else f"col_{i}")

        rows = []
        while self._lib.csharpdb_result_next(r) == 1:
            row = {}
            for i, col in enumerate(columns):
                if self._lib.csharpdb_result_is_null(r, i) == 1:
                    row[col] = None
                else:
                    col_type = self._lib.csharpdb_result_column_type(r, i)
                    if col_type == self.INTEGER:
                        row[col] = self._lib.csharpdb_result_get_int64(r, i)
                    elif col_type == self.REAL:
                        row[col] = self._lib.csharpdb_result_get_double(r, i)
                    elif col_type == self.TEXT:
                        val = self._lib.csharpdb_result_get_text(r, i)
                        row[col] = val.decode() if val else ""
                    elif col_type == self.BLOB:
                        size = ctypes.c_int(0)
                        ptr = self._lib.csharpdb_result_get_blob(
                            r, i, ctypes.byref(size)
                        )
                        if ptr and size.value > 0:
                            row[col] = ctypes.string_at(ptr, size.value)
                        else:
                            row[col] = b""
                    else:
                        row[col] = None
            rows.append(row)

        self._lib.csharpdb_result_free(r)
        return rows

    def query_one(self, sql: str) -> dict | None:
        """Execute a SELECT and return the first row, or None."""
        rows = self.query(sql)
        return rows[0] if rows else None

    def begin(self):
        """Begin an explicit transaction."""
        if self._lib.csharpdb_begin(self._db) != 0:
            self._check_error("begin")

    def commit(self):
        """Commit the current transaction."""
        if self._lib.csharpdb_commit(self._db) != 0:
            self._check_error("commit")

    def rollback(self):
        """Rollback the current transaction."""
        if self._lib.csharpdb_rollback(self._db) != 0:
            self._check_error("rollback")

    def transaction(self, fn):
        """Run a function inside a transaction. Auto-commits on success, rolls back on error."""
        self.begin()
        try:
            result = fn()
            self.commit()
            return result
        except Exception:
            self.rollback()
            raise

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
