using System.Runtime.InteropServices;
using CSharpDB.ImportExport.TableArchives;
using CSharpDB.Primitives;

if (args.Length != 1)
    throw new ArgumentException("Pass the absolute path to the published CSharpDB.Native library.");
string libraryPath = Path.GetFullPath(args[0]);
if (!File.Exists(libraryPath)) throw new FileNotFoundException("Published native library not found.", libraryPath);
nint library = NativeLibrary.Load(libraryPath);
NativeLibrary.SetDllImportResolver(typeof(NativeMethods).Assembly,
    (name, _, _) => name == NativeMethods.LibraryName ? library : 0);

string temporaryRoot = Path.TrimEndingDirectorySeparator(Path.GetFullPath(Path.GetTempPath()));
string directory = Path.Combine(temporaryRoot, $"csharpdb-native-archive-{Guid.NewGuid():N}");
Directory.CreateDirectory(directory);
string databasePath = Path.Combine(directory, "archive.db");
try
{
    using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(1));
    var schema = new TableSchema
    {
        TableName = "source_items",
        Columns =
        [
            new ColumnDefinition { Name = "id", Type = DbType.Integer, DeclaredType = SqlTypeDescriptor.Create(SqlTypeKind.BigInt), IsPrimaryKey = true, Nullable = false },
            new ColumnDefinition { Name = "label", Type = DbType.Text, DeclaredType = SqlTypeDescriptor.Create(SqlTypeKind.VarChar, length: 32), Nullable = false },
        ],
    };
    DbValue[][] rows =
    [
        [DbValue.FromInteger(1), DbValue.FromText("Alpha")],
        [DbValue.FromInteger(2), DbValue.FromText("Beta")],
    ];
    // When this executable is published with NativeAOT, fixture writing also
    // proves that archive JSON serialization works with reflection disabled.
    string archivePath = Path.Combine(directory, "items.csdbtable");
    await TableArchiveWriter.WriteAsync(archivePath, schema,
        TableArchiveWriter.ToAsyncRows(rows, deadline.Token), deadline.Token);
    if (!await TableArchiveReader.HasIntegerPrimaryKeyIndexAsync(archivePath, deadline.Token))
        throw new InvalidOperationException("Archive fixture has no primary-key lookup index.");
    nint db = Open(databasePath);
    try
    {
        Execute(db, "CREATE EXTERNAL TABLE archived_items FROM 'items.csdbtable'");
        CheckRows(db, "SELECT id, label FROM archived_items ORDER BY id", [(1, "Alpha"), (2, "Beta")]);
        CheckRows(db, "SELECT id, label FROM archived_items WHERE id = 2", [(2, "Beta")]);
    }
    finally { NativeMethods.Close(db); }
    db = Open(databasePath);
    try { CheckRows(db, "SELECT id, label FROM archived_items WHERE id = 1", [(1, "Alpha")]); }
    finally { NativeMethods.Close(db); }
    Console.WriteLine("Published native archive SQL smoke passed: generated fixture, scan, indexed lookup, declared types and reopen.");
}
finally
{
    if (!string.Equals(Path.GetDirectoryName(Path.GetFullPath(directory)), temporaryRoot, StringComparison.Ordinal))
        throw new InvalidOperationException("Archive smoke cleanup escaped its temporary directory.");
    // Every result and database handle has been released before cleanup.
    Directory.Delete(directory, recursive: true);
}

static nint Open(string path)
{
    nint db = NativeMethods.Open(path);
    return db != 0 ? db : throw Failure("Open");
}

static void Execute(nint db, string sql)
{
    nint result = NativeMethods.Execute(db, sql);
    if (result == 0) throw Failure(sql);
    NativeMethods.Free(result);
}

static void CheckRows(nint db, string sql, (long Id, string Label)[] expected)
{
    nint result = NativeMethods.Execute(db, sql);
    if (result == 0) throw Failure(sql);
    try
    {
        if (Marshal.PtrToStringUTF8(NativeMethods.DeclaredType(result, 0)) != "BIGINT" ||
            Marshal.PtrToStringUTF8(NativeMethods.DeclaredType(result, 1)) != "VARCHAR(32)")
            throw new InvalidOperationException("Archive declared types changed.");
        foreach (var row in expected)
        {
            if (NativeMethods.Next(result) != 1 || NativeMethods.Int64(result, 0) != row.Id ||
                Marshal.PtrToStringUTF8(NativeMethods.Text(result, 1)) != row.Label)
                throw Failure("Archive row differs");
        }
        if (NativeMethods.Next(result) != 0) throw Failure("Unexpected trailing archive row");
    }
    finally { NativeMethods.Free(result); }
}

static Exception Failure(string operation) =>
    new InvalidOperationException($"{operation}: {Marshal.PtrToStringUTF8(NativeMethods.LastError())}");

internal static partial class NativeMethods
{
    internal const string LibraryName = "csharpdb_archive_smoke";
    [LibraryImport(LibraryName, EntryPoint = "csharpdb_open", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint Open(string path);
    [LibraryImport(LibraryName, EntryPoint = "csharpdb_close")]
    internal static partial void Close(nint db);
    [LibraryImport(LibraryName, EntryPoint = "csharpdb_execute", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint Execute(nint db, string sql);
    [LibraryImport(LibraryName, EntryPoint = "csharpdb_result_next")]
    internal static partial int Next(nint result);
    [LibraryImport(LibraryName, EntryPoint = "csharpdb_result_get_int64")]
    internal static partial long Int64(nint result, int column);
    [LibraryImport(LibraryName, EntryPoint = "csharpdb_result_get_text")]
    internal static partial nint Text(nint result, int column);
    [LibraryImport(LibraryName, EntryPoint = "csharpdb_result_column_declared_type")]
    internal static partial nint DeclaredType(nint result, int column);
    [LibraryImport(LibraryName, EntryPoint = "csharpdb_result_free")]
    internal static partial void Free(nint result);
    [LibraryImport(LibraryName, EntryPoint = "csharpdb_last_error")]
    internal static partial nint LastError();
}
