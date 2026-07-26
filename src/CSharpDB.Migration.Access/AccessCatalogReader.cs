using System.Collections.ObjectModel;
using System.Data;
using System.Data.OleDb;
using System.Globalization;
using System.Runtime.Versioning;
using System.Text;

namespace CSharpDB.Migration.Access;

[SupportedOSPlatform("windows")]
internal static class AccessCatalogReader
{
    internal static ValueTask<AccessCatalogSnapshot>
        ReadAsync(
        AccessSourceSession session,
        AccessInspectionLimits limits,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(limits);
        limits.Validate();
        cancellationToken.ThrowIfCancellationRequested();
        try
        {
            AccessCatalogSnapshot snapshot =
                Read(
                    session,
                    limits,
                    cancellationToken);
            return ValueTask.FromResult(snapshot);
        }
        catch (AccessMigrationException)
        {
            throw;
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception) when (
            exception is OleDbException or
                InvalidOperationException or
                InvalidCastException or
                FormatException or
                OverflowException)
        {
            throw new AccessMigrationException(
                AccessMigrationErrorCode.CatalogReadFailed,
                "The Microsoft Access catalog could not be read safely.");
        }
    }

    private static AccessCatalogSnapshot Read(
        AccessSourceSession session,
        AccessInspectionLimits limits,
        CancellationToken cancellationToken)
    {
        using DataTable tableRows =
            session.Connection.GetOleDbSchemaTable(
                OleDbSchemaGuid.Tables,
                restrictions: null) ??
            throw new AccessMigrationException(
                AccessMigrationErrorCode.CatalogReadFailed,
                "The ACE provider returned no table catalog.");
        var tableNames = new List<string>();
        var unsupported =
            new List<AccessSchemaObjectMetadata>();
        var textBudget =
            new CatalogTextBudget(
                limits.MaxCatalogTextBytes);

        foreach (DataRow row in tableRows.Rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            string name =
                RequiredText(row, "TABLE_NAME");
            string type =
                RequiredText(row, "TABLE_TYPE");
            textBudget.Add(name);
            textBudget.Add(type);
            if (IsSystemObject(name, type))
                continue;
            if (string.Equals(
                    type,
                    "TABLE",
                    StringComparison.OrdinalIgnoreCase))
            {
                tableNames.Add(name);
            }
            else
            {
                unsupported.Add(
                    new AccessSchemaObjectMetadata(
                        name,
                        type));
            }
        }

        string[] distinctTableNames =
            tableNames.Distinct(
                    StringComparer.OrdinalIgnoreCase)
                .OrderBy(
                    static name => name,
                    StringComparer.Ordinal)
                .ToArray();
        if (distinctTableNames.Length >
            limits.MaxTables)
        {
            throw Limit(
                "table-count");
        }

        var tables =
            new List<AccessTableMetadata>(
                distinctTableNames.Length);
        foreach (string tableName in distinctTableNames)
        {
            cancellationToken.ThrowIfCancellationRequested();
            IReadOnlyList<AccessColumnMetadata> columns =
                ReadColumns(
                    session.Connection,
                    tableName,
                    limits,
                    textBudget);
            IReadOnlyList<string> primaryKey =
                ReadPrimaryKey(
                    session.Connection,
                    tableName,
                    columns,
                    limits,
                    textBudget);
            IReadOnlyList<AccessIndexMetadata> indexes =
                ReadIndexes(
                    session.Connection,
                    tableName,
                    columns,
                    limits,
                    textBudget);
            tables.Add(new AccessTableMetadata
            {
                Name = tableName,
                Columns = columns,
                PrimaryKeyColumns = primaryKey,
                Indexes = indexes,
            });
        }

        IReadOnlyList<AccessForeignKeyMetadata>
            foreignKeys =
                ReadForeignKeys(
                    session.Connection,
                    tables,
                    limits,
                    textBudget);

        int schemaObjects = checked(
            tables.Count +
            unsupported.Count +
            tables.Sum(static table =>
                table.Columns.Count +
                table.Indexes.Count +
                (table.PrimaryKeyColumns.Count > 0
                    ? 1
                    : 0)) +
            foreignKeys.Count +
            foreignKeys.Sum(static foreignKey =>
                foreignKey.Columns.Count) +
            2);
        if (schemaObjects > limits.MaxSchemaObjects)
            throw Limit("schema-object");

        string sourceVersion;
        try
        {
            sourceVersion =
                session.Connection.ServerVersion;
        }
        catch (InvalidOperationException)
        {
            sourceVersion = "unknown";
        }

        return new AccessCatalogSnapshot
        {
            SourceContentDigest =
                session.SourceContentDigest,
            ProviderId = session.ProviderId,
            ProviderVersion =
                session.ProviderVersion,
            SourceVersion = sourceVersion,
            SourceName = Path.GetFileNameWithoutExtension(
                session.SourcePath),
            SourceExtension = Path.GetExtension(
                    session.SourcePath)
                .ToLowerInvariant(),
            Tables =
                new ReadOnlyCollection<
                    AccessTableMetadata>(
                    tables),
            UnsupportedObjects =
                new ReadOnlyCollection<
                    AccessSchemaObjectMetadata>(
                    unsupported
                        .OrderBy(
                            static item => item.Type,
                            StringComparer.Ordinal)
                        .ThenBy(
                            static item => item.Name,
                            StringComparer.Ordinal)
                        .ToArray()),
            ForeignKeys = foreignKeys,
        };
    }

    private static IReadOnlyList<AccessColumnMetadata>
        ReadColumns(
        OleDbConnection connection,
        string tableName,
        AccessInspectionLimits limits,
        CatalogTextBudget textBudget)
    {
        using DataTable schema =
            connection.GetOleDbSchemaTable(
                OleDbSchemaGuid.Columns,
                [null, null, tableName, null]) ??
            throw new AccessMigrationException(
                AccessMigrationErrorCode.CatalogReadFailed,
                "The ACE provider returned no column catalog.");
        AccessColumnMetadata[] columns =
            schema.Rows.Cast<DataRow>()
                .Select(row =>
                    ReadColumn(
                        row,
                        textBudget))
                .OrderBy(static column =>
                    column.Ordinal)
                .ThenBy(static column =>
                    column.Name,
                    StringComparer.Ordinal)
                .ToArray();
        if (columns.Length == 0 ||
            columns.Length >
                limits.MaxColumnsPerTable)
        {
            throw Limit("per-table column");
        }
        if (columns.Select(static item =>
                item.Name)
            .Distinct(
                StringComparer.OrdinalIgnoreCase)
            .Count() != columns.Length)
        {
            throw new AccessMigrationException(
                AccessMigrationErrorCode.CatalogReadFailed,
                "An Access table contains duplicate case-insensitive column names.");
        }
        return Array.AsReadOnly(columns);
    }

    private static AccessColumnMetadata ReadColumn(
        DataRow row,
        CatalogTextBudget textBudget)
    {
        string name =
            RequiredText(row, "COLUMN_NAME");
        textBudget.Add(name);
        int ordinal =
            RequiredInt32(
                row,
                "ORDINAL_POSITION");
        if (ordinal <= 0)
        {
            throw new AccessMigrationException(
                AccessMigrationErrorCode.CatalogReadFailed,
                "The ACE provider returned a non-positive column ordinal.");
        }
        int providerTypeValue =
            RequiredInt32(row, "DATA_TYPE");
        if (!Enum.IsDefined(
                typeof(OleDbType),
                providerTypeValue))
        {
            throw new AccessMigrationException(
                AccessMigrationErrorCode.CatalogReadFailed,
                "The ACE provider returned an unknown OLE DB column type.");
        }

        string? defaultText =
            OptionalText(
                row,
                "COLUMN_DEFAULT");
        if (defaultText is not null)
            textBudget.Add(defaultText);
        return new AccessColumnMetadata
        {
            Name = name,
            Ordinal = ordinal,
            ProviderType =
                (OleDbType)providerTypeValue,
            Nullable =
                OptionalBoolean(
                    row,
                    "IS_NULLABLE") ??
                true,
            MaximumLength =
                OptionalInt64(
                    row,
                    "CHARACTER_MAXIMUM_LENGTH"),
            Precision =
                OptionalInt32(
                    row,
                    "NUMERIC_PRECISION"),
            Scale =
                OptionalInt32(
                    row,
                    "NUMERIC_SCALE"),
            HasDefault =
                defaultText is not null,
            DefaultDigest =
                defaultText is null
                    ? null
                    : AccessStableDigest.Text(
                        "csharpdb-access-default/v1",
                        defaultText),
        };
    }

    private static IReadOnlyList<string> ReadPrimaryKey(
        OleDbConnection connection,
        string tableName,
        IReadOnlyList<AccessColumnMetadata> columns,
        AccessInspectionLimits limits,
        CatalogTextBudget textBudget)
    {
        using DataTable? schema =
            connection.GetOleDbSchemaTable(
                OleDbSchemaGuid.Primary_Keys,
                [null, null, tableName]);
        if (schema is null || schema.Rows.Count == 0)
            return [];
        string[] names = schema.Rows
            .Cast<DataRow>()
            .Select(row => new
            {
                Name =
                    RequiredText(
                        row,
                        "COLUMN_NAME"),
                Ordinal =
                    OptionalInt32(
                        row,
                        "ORDINAL") ??
                    OptionalInt32(
                        row,
                        "ORDINAL_POSITION") ??
                    0,
            })
            .OrderBy(static item => item.Ordinal)
            .ThenBy(
                static item => item.Name,
                StringComparer.Ordinal)
            .Select(static item => item.Name)
            .ToArray();
        if (names.Length == 0 ||
            names.Length > limits.MaxKeyColumns ||
            names.Distinct(
                    StringComparer.OrdinalIgnoreCase)
                .Count() != names.Length)
        {
            throw Limit("primary-key column");
        }
        foreach (string name in names)
        {
            textBudget.Add(name);
            if (!columns.Any(column =>
                    string.Equals(
                        column.Name,
                        name,
                        StringComparison.OrdinalIgnoreCase)))
            {
                throw new AccessMigrationException(
                    AccessMigrationErrorCode.CatalogReadFailed,
                    "An Access primary key references an unknown column.");
            }
        }
        return Array.AsReadOnly(names);
    }

    private static IReadOnlyList<AccessIndexMetadata>
        ReadIndexes(
        OleDbConnection connection,
        string tableName,
        IReadOnlyList<AccessColumnMetadata> columns,
        AccessInspectionLimits limits,
        CatalogTextBudget textBudget)
    {
        using DataTable? schema =
            connection.GetOleDbSchemaTable(
                OleDbSchemaGuid.Indexes,
                [null, null, null, null, tableName]);
        if (schema is null || schema.Rows.Count == 0)
            return [];
        var groups = schema.Rows
            .Cast<DataRow>()
            .Where(static row =>
                OptionalText(
                    row,
                    "INDEX_NAME") is not null &&
                OptionalText(
                    row,
                    "COLUMN_NAME") is not null)
            .GroupBy(
                static row =>
                    RequiredText(
                        row,
                        "INDEX_NAME"),
                StringComparer.OrdinalIgnoreCase);
        var indexes =
            new List<AccessIndexMetadata>();
        foreach (IGrouping<string, DataRow> group in groups)
        {
            DataRow first = group.First();
            string[] members = group
                .Select(row => new
                {
                    Name =
                        RequiredText(
                            row,
                            "COLUMN_NAME"),
                    Ordinal =
                        OptionalInt32(
                            row,
                            "ORDINAL_POSITION") ??
                        0,
                })
                .OrderBy(static item =>
                    item.Ordinal)
                .ThenBy(
                    static item => item.Name,
                    StringComparer.Ordinal)
                .Select(static item =>
                    item.Name)
                .ToArray();
            if (members.Length == 0 ||
                members.Length > limits.MaxKeyColumns)
            {
                throw Limit("index column");
            }
            foreach (string member in members)
            {
                textBudget.Add(member);
                if (!columns.Any(column =>
                        string.Equals(
                            column.Name,
                            member,
                            StringComparison.OrdinalIgnoreCase)))
                {
                    throw new AccessMigrationException(
                        AccessMigrationErrorCode.CatalogReadFailed,
                        "An Access index references an unknown column.");
                }
            }
            textBudget.Add(group.Key);
            indexes.Add(new AccessIndexMetadata
            {
                Name = group.Key,
                Unique =
                    OptionalBoolean(
                        first,
                        "UNIQUE") ??
                    false,
                Primary =
                    OptionalBoolean(
                        first,
                        "PRIMARY_KEY") ??
                    false,
                Columns =
                    Array.AsReadOnly(members),
            });
        }
        if (indexes.Count >
            limits.MaxIndexesPerTable)
        {
            throw Limit("per-table index");
        }
        return new ReadOnlyCollection<
            AccessIndexMetadata>(
            indexes.OrderBy(
                    static item => item.Name,
                    StringComparer.Ordinal)
                .ToArray());
    }

    private static IReadOnlyList<AccessForeignKeyMetadata>
        ReadForeignKeys(
        OleDbConnection connection,
        IReadOnlyList<AccessTableMetadata> tables,
        AccessInspectionLimits limits,
        CatalogTextBudget textBudget)
    {
        using DataTable? schema =
            connection.GetOleDbSchemaTable(
                OleDbSchemaGuid.Foreign_Keys,
                restrictions: null);
        if (schema is null ||
            schema.Rows.Count == 0)
        {
            return [];
        }

        var foreignKeys =
            new List<AccessForeignKeyMetadata>();
        IEnumerable<IGrouping<string, DataRow>>
            groups =
                schema.Rows.Cast<DataRow>()
                    .GroupBy(
                        static row =>
                            string.Concat(
                                RequiredText(
                                    row,
                                    "FK_TABLE_NAME"),
                                "\0",
                                RequiredText(
                                    row,
                                    "FK_NAME")),
                        StringComparer
                            .OrdinalIgnoreCase);
        foreach (IGrouping<string, DataRow> group in
                 groups)
        {
            DataRow first = group.First();
            string name =
                RequiredText(first, "FK_NAME");
            string sourceTable =
                RequiredText(
                    first,
                    "FK_TABLE_NAME");
            string referencedTable =
                RequiredText(
                    first,
                    "PK_TABLE_NAME");
            string updateRule =
                RequiredText(
                    first,
                    "UPDATE_RULE");
            string deleteRule =
                RequiredText(
                    first,
                    "DELETE_RULE");
            string? referencedKeyName =
                OptionalText(
                    first,
                    "PK_NAME");
            foreach (string value in
                     new[]
                     {
                         name,
                         sourceTable,
                         referencedTable,
                         updateRule,
                         deleteRule,
                     })
            {
                textBudget.Add(value);
            }
            if (referencedKeyName is not null)
                textBudget.Add(referencedKeyName);

            AccessForeignKeyColumnMetadata[] columns =
                group.Select(row =>
                        new AccessForeignKeyColumnMetadata(
                            RequiredText(
                                row,
                                "FK_COLUMN_NAME"),
                            RequiredText(
                                row,
                                "PK_COLUMN_NAME"),
                            RequiredInt32(
                                row,
                                "ORDINAL")))
                    .OrderBy(static column =>
                        column.Ordinal)
                    .ToArray();
            if (columns.Length == 0 ||
                columns.Length >
                    limits.MaxKeyColumns ||
                !columns.Select(static column =>
                        column.Ordinal)
                    .SequenceEqual(
                        Enumerable.Range(
                            1,
                            columns.Length)))
            {
                throw Limit(
                    "foreign-key column");
            }
            foreach (
                AccessForeignKeyColumnMetadata column
                in columns)
            {
                textBudget.Add(column.SourceColumn);
                textBudget.Add(
                    column.ReferencedColumn);
            }

            AccessTableMetadata? source =
                tables.SingleOrDefault(table =>
                    string.Equals(
                        table.Name,
                        sourceTable,
                        StringComparison
                            .OrdinalIgnoreCase));
            AccessTableMetadata? target =
                tables.SingleOrDefault(table =>
                    string.Equals(
                        table.Name,
                        referencedTable,
                        StringComparison
                            .OrdinalIgnoreCase));
            if (source is not null &&
                columns.Any(column =>
                    !source.Columns.Any(candidate =>
                        string.Equals(
                            candidate.Name,
                            column.SourceColumn,
                            StringComparison
                                .OrdinalIgnoreCase))))
            {
                throw new AccessMigrationException(
                    AccessMigrationErrorCode
                        .CatalogReadFailed,
                    "An Access foreign key references an unknown source column.");
            }
            if (target is not null &&
                columns.Any(column =>
                    !target.Columns.Any(candidate =>
                        string.Equals(
                            candidate.Name,
                            column.ReferencedColumn,
                            StringComparison
                                .OrdinalIgnoreCase))))
            {
                throw new AccessMigrationException(
                    AccessMigrationErrorCode
                        .CatalogReadFailed,
                    "An Access foreign key references an unknown target column.");
            }

            foreignKeys.Add(
                new AccessForeignKeyMetadata
                {
                    Name = name,
                    SourceTable = sourceTable,
                    ReferencedTable =
                        referencedTable,
                    ReferencedKeyName =
                        referencedKeyName,
                    UpdateRule = updateRule,
                    DeleteRule = deleteRule,
                    Columns =
                        Array.AsReadOnly(columns),
                });
        }
        if (foreignKeys.Count >
                limits.MaxForeignKeys ||
            foreignKeys.Sum(static foreignKey =>
                foreignKey.Columns.Count) >
                limits.MaxForeignKeyColumns)
        {
            throw Limit("foreign-key");
        }
        return new ReadOnlyCollection<
            AccessForeignKeyMetadata>(
            foreignKeys.OrderBy(
                    static item =>
                        item.SourceTable,
                    StringComparer.Ordinal)
                .ThenBy(
                    static item => item.Name,
                    StringComparer.Ordinal)
                .ToArray());
    }

    private static bool IsSystemObject(
        string name,
        string type) =>
        name.StartsWith(
            "MSys",
            StringComparison.OrdinalIgnoreCase) ||
        name.StartsWith(
            "~",
            StringComparison.Ordinal) ||
        type.Contains(
            "SYSTEM",
            StringComparison.OrdinalIgnoreCase);

    private static string RequiredText(
        DataRow row,
        string name) =>
        OptionalText(row, name) ??
        throw new AccessMigrationException(
            AccessMigrationErrorCode.CatalogReadFailed,
            $"The ACE catalog is missing required '{name}' metadata.");

    private static string? OptionalText(
        DataRow row,
        string name)
    {
        if (!row.Table.Columns.Contains(name) ||
            row.IsNull(name))
        {
            return null;
        }
        string? value = Convert.ToString(
            row[name],
            CultureInfo.InvariantCulture);
        return string.IsNullOrWhiteSpace(value)
            ? null
            : value;
    }

    private static int RequiredInt32(
        DataRow row,
        string name) =>
        OptionalInt32(row, name) ??
        throw new AccessMigrationException(
            AccessMigrationErrorCode.CatalogReadFailed,
            $"The ACE catalog is missing required '{name}' metadata.");

    private static int? OptionalInt32(
        DataRow row,
        string name)
    {
        if (!row.Table.Columns.Contains(name) ||
            row.IsNull(name))
        {
            return null;
        }
        return Convert.ToInt32(
            row[name],
            CultureInfo.InvariantCulture);
    }

    private static long? OptionalInt64(
        DataRow row,
        string name)
    {
        if (!row.Table.Columns.Contains(name) ||
            row.IsNull(name))
        {
            return null;
        }
        return Convert.ToInt64(
            row[name],
            CultureInfo.InvariantCulture);
    }

    private static bool? OptionalBoolean(
        DataRow row,
        string name)
    {
        if (!row.Table.Columns.Contains(name) ||
            row.IsNull(name))
        {
            return null;
        }
        return Convert.ToBoolean(
            row[name],
            CultureInfo.InvariantCulture);
    }

    private static AccessRetainedCaptureLimitException
        Limit(string category) =>
        new(
            $"Microsoft Access inspection exceeded the fixed {category} bound.");

    private sealed class CatalogTextBudget
    {
        private readonly long maximum;
        private long used;

        internal CatalogTextBudget(long maximum)
        {
            this.maximum = maximum <= 0
                ? throw new ArgumentOutOfRangeException(
                    nameof(maximum))
                : maximum;
        }

        internal void Add(string value)
        {
            long bytes;
            try
            {
                bytes =
                    Encoding.UTF8.GetByteCount(value);
                used = checked(used + bytes);
            }
            catch (OverflowException)
            {
                throw new AccessRetainedCaptureLimitException(
                    "Microsoft Access inspection exceeded its catalog text bound.");
            }
            if (used > maximum)
            {
                throw new AccessRetainedCaptureLimitException(
                    "Microsoft Access inspection exceeded its catalog text bound.");
            }
        }
    }
}
