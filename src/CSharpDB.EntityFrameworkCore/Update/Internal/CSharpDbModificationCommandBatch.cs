using System.Data;
using System.Text;
using CSharpDB.Primitives;
using CSharpDB.Data;
using CSharpDB.EntityFrameworkCore.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;

namespace CSharpDB.EntityFrameworkCore.Update.Internal;

public sealed class CSharpDbModificationCommandBatch : ModificationCommandBatch
{
    private readonly List<IReadOnlyModificationCommand> _commands = [];
    private bool _areMoreBatchesExpected;

    public CSharpDbModificationCommandBatch(ModificationCommandBatchFactoryDependencies dependencies)
        => _ = dependencies;

    public override IReadOnlyList<IReadOnlyModificationCommand> ModificationCommands => _commands;

    public override bool RequiresTransaction => _commands.Count > 0;

    public override bool AreMoreBatchesExpected => _areMoreBatchesExpected;

    public override bool TryAddCommand(IReadOnlyModificationCommand modificationCommand)
    {
        ArgumentNullException.ThrowIfNull(modificationCommand);
        _commands.Add(modificationCommand);
        return true;
    }

    public override void Complete(bool moreBatchesExpected)
        => _areMoreBatchesExpected = moreBatchesExpected;

    public override void Execute(IRelationalConnection connection)
        => ExecuteAsync(connection, CancellationToken.None).GetAwaiter().GetResult();

    public override async Task ExecuteAsync(IRelationalConnection connection, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        if (connection.DbConnection is not CSharpDbConnection dbConnection)
            throw new InvalidOperationException("The CSharpDB EF Core provider requires an active CSharpDbConnection.");

        foreach (IReadOnlyModificationCommand command in _commands)
        {
            try
            {
                await ExecuteCommandAsync(dbConnection, command, cancellationToken);
            }
            catch (CSharpDbDataException exception)
            {
                throw new DbUpdateException(
                    $"An error occurred while saving changes for table '{command.TableName}'.",
                    exception,
                    command.Entries);
            }
        }
    }

    private static async Task ExecuteCommandAsync(
        CSharpDbConnection connection,
        IReadOnlyModificationCommand command,
        CancellationToken cancellationToken)
    {
        await using var dbCommand = connection.CreateCommand();
        var sql = new StringBuilder();
        var parameters = new List<CSharpDbParameter>();
        int parameterIndex = 0;

        switch (command.EntityState)
        {
            case EntityState.Added:
                BuildInsertCommand(dbCommand, command, sql, parameters, ref parameterIndex);
                break;
            case EntityState.Modified:
                BuildUpdateCommand(dbCommand, command, sql, parameters, ref parameterIndex);
                break;
            case EntityState.Deleted:
                BuildDeleteCommand(dbCommand, command, sql, parameters, ref parameterIndex);
                break;
            default:
                throw new NotSupportedException($"Entity state '{command.EntityState}' is not supported by the CSharpDB EF Core provider.");
        }

        dbCommand.CommandText = sql.ToString();
        foreach (CSharpDbParameter parameter in parameters)
            dbCommand.Parameters.Add(parameter);

        CSharpDbCommandExecutionResult executionResult = await dbCommand.ExecuteCommandAsync(cancellationToken);

        if (command.EntityState is EntityState.Modified or EntityState.Deleted
            && executionResult.Result.RowsAffected != 1)
        {
            throw new DbUpdateConcurrencyException(
                $"The database operation expected to affect 1 row, but actually affected {executionResult.Result.RowsAffected} row(s).",
                command.Entries);
        }

        if (command.EntityState == EntityState.Added && executionResult.GeneratedIntegerKey is long generatedIntegerKey)
        {
            foreach (IColumnModification readColumn in command.ColumnModifications.Where(static column => column.IsRead))
            {
                if (!readColumn.IsKey || !IsIntegerProperty(readColumn.Property))
                    continue;

                readColumn.Value = ConvertGeneratedValue(readColumn.Property!.ClrType, generatedIntegerKey);
            }
        }

        IColumnModification[] rowVersionColumns =
            command.ColumnModifications
                .Where(IsRowVersionColumn)
                .ToArray();
        if (rowVersionColumns.Length > 1)
        {
            throw new InvalidOperationException(
                $"Modification command for table '{command.TableName}' contains more than one rowversion column.");
        }

        if (command.EntityState is EntityState.Added or EntityState.Modified &&
            rowVersionColumns is [IColumnModification rowVersionColumn])
        {
            byte[] generatedRowVersion =
                executionResult.GeneratedRowVersion ??
                throw new DbUpdateException(
                    $"The database did not return a generated rowversion for table '{command.TableName}'. Verify that the column was created as BLOB ROWVERSION NOT NULL.",
                    command.Entries);

            if (generatedRowVersion.Length != sizeof(long))
            {
                throw new DbUpdateException(
                    $"The database returned a {generatedRowVersion.Length}-byte rowversion for table '{command.TableName}', but CSharpDB rowversion values must contain exactly {sizeof(long)} bytes.",
                    command.Entries);
            }

            rowVersionColumn.Value = generatedRowVersion;
        }
    }

    private static void BuildInsertCommand(
        CSharpDbCommand dbCommand,
        IReadOnlyModificationCommand command,
        StringBuilder sql,
        List<CSharpDbParameter> parameters,
        ref int parameterIndex)
    {
        ValidateTarget(command);

        var writeColumns = command.ColumnModifications.Where(static column => column.IsWrite).ToList();
        if (writeColumns.Count == 0)
        {
            sql.Append("INSERT INTO ")
                .Append(SqlIdentifierRules.Quote(command.TableName))
                .Append(" DEFAULT VALUES");
            return;
        }

        sql.Append("INSERT INTO ")
            .Append(SqlIdentifierRules.Quote(command.TableName))
            .Append(" (");

        for (int i = 0; i < writeColumns.Count; i++)
        {
            if (i > 0)
                sql.Append(", ");

            sql.Append(SqlIdentifierRules.Quote(writeColumns[i].ColumnName));
        }

        sql.Append(") VALUES (");

        for (int i = 0; i < writeColumns.Count; i++)
        {
            if (i > 0)
                sql.Append(", ");

            IColumnModification column = writeColumns[i];
            string parameterName = $"p{parameterIndex++}";
            sql.Append('@').Append(parameterName);
            parameters.Add(CreateParameter(dbCommand, column, parameterName, column.IsWrite ? column.Value : null));
        }

        sql.Append(')');
    }

    private static void BuildUpdateCommand(
        CSharpDbCommand dbCommand,
        IReadOnlyModificationCommand command,
        StringBuilder sql,
        List<CSharpDbParameter> parameters,
        ref int parameterIndex)
    {
        ValidateTarget(command);

        var writeColumns = command.ColumnModifications.Where(static column => column.IsWrite).ToList();
        if (writeColumns.Count == 0)
            throw new NotSupportedException($"Update for '{command.TableName}' does not contain writable columns and is not supported by the CSharpDB EF Core provider.");

        var conditionColumns = command.ColumnModifications.Where(static column => column.IsCondition).ToList();
        if (conditionColumns.Count == 0)
            throw new NotSupportedException($"Update for '{command.TableName}' does not contain a key/concurrency predicate and is not supported by the CSharpDB EF Core provider.");

        sql.Append("UPDATE ")
            .Append(SqlIdentifierRules.Quote(command.TableName))
            .Append(" SET ");

        for (int i = 0; i < writeColumns.Count; i++)
        {
            if (i > 0)
                sql.Append(", ");

            IColumnModification column = writeColumns[i];
            string parameterName = $"p{parameterIndex++}";

            sql.Append(SqlIdentifierRules.Quote(column.ColumnName))
                .Append(" = @")
                .Append(parameterName);

            parameters.Add(CreateParameter(dbCommand, column, parameterName, column.Value));
        }

        AppendWhereClause(dbCommand, conditionColumns, sql, parameters, ref parameterIndex);
    }

    private static void BuildDeleteCommand(
        CSharpDbCommand dbCommand,
        IReadOnlyModificationCommand command,
        StringBuilder sql,
        List<CSharpDbParameter> parameters,
        ref int parameterIndex)
    {
        ValidateTarget(command);

        var conditionColumns = command.ColumnModifications.Where(static column => column.IsCondition).ToList();
        if (conditionColumns.Count == 0)
            throw new NotSupportedException($"Delete for '{command.TableName}' does not contain a key/concurrency predicate and is not supported by the CSharpDB EF Core provider.");

        sql.Append("DELETE FROM ")
            .Append(SqlIdentifierRules.Quote(command.TableName));

        AppendWhereClause(dbCommand, conditionColumns, sql, parameters, ref parameterIndex);
    }

    private static void AppendWhereClause(
        CSharpDbCommand dbCommand,
        IReadOnlyList<IColumnModification> conditionColumns,
        StringBuilder sql,
        List<CSharpDbParameter> parameters,
        ref int parameterIndex)
    {
        sql.Append(" WHERE ");

        for (int i = 0; i < conditionColumns.Count; i++)
        {
            if (i > 0)
                sql.Append(" AND ");

            IColumnModification column = conditionColumns[i];
            object? value = column.UseOriginalValue ? column.OriginalValue : column.Value;

            if (value is null or DBNull)
            {
                sql.Append(SqlIdentifierRules.Quote(column.ColumnName)).Append(" IS NULL");
                continue;
            }

            string parameterName = $"p{parameterIndex++}";
            sql.Append(SqlIdentifierRules.Quote(column.ColumnName))
                .Append(" = @")
                .Append(parameterName);

            parameters.Add(CreateParameter(dbCommand, column, parameterName, value));
        }
    }

    private static CSharpDbParameter CreateParameter(
        CSharpDbCommand dbCommand,
        IColumnModification column,
        string parameterName,
        object? value)
    {
        RelationalTypeMapping typeMapping = column.TypeMapping
            ?? throw new InvalidOperationException(
                $"Column '{column.ColumnName}' is missing a relational type mapping for the CSharpDB EF Core provider.");

        return (CSharpDbParameter)typeMapping.CreateParameter(
            dbCommand,
            parameterName,
            value,
            column.IsNullable,
            ParameterDirection.Input);
    }

    private static void ValidateTarget(IReadOnlyModificationCommand command)
    {
        CSharpDbProviderValidation.ValidateSimpleIdentifier(command.TableName, "Table name");
        if (!string.IsNullOrWhiteSpace(command.Schema))
            throw new NotSupportedException("Schemas are not supported by the CSharpDB EF Core provider.");

        foreach (IColumnModification column in command.ColumnModifications)
            CSharpDbProviderValidation.ValidateSimpleIdentifier(column.ColumnName, "Column name");
    }

    private static bool IsIntegerProperty(IProperty? property)
        => property?.ClrType switch
        {
            var type when type == typeof(byte) => true,
            var type when type == typeof(sbyte) => true,
            var type when type == typeof(short) => true,
            var type when type == typeof(ushort) => true,
            var type when type == typeof(int) => true,
            var type when type == typeof(uint) => true,
            var type when type == typeof(long) => true,
            var type when type == typeof(ulong) => true,
            _ => false,
        };

    private static bool IsRowVersionColumn(
        IColumnModification column) =>
        column.IsRead &&
        column.Property is
        {
            IsConcurrencyToken: true,
            ValueGenerated:
                ValueGenerated.OnAddOrUpdate,
            ClrType: not null,
        } property &&
        property.ClrType == typeof(byte[]);

    private static object ConvertGeneratedValue(Type targetType, long generatedIntegerKey)
        => targetType == typeof(byte) ? checked((byte)generatedIntegerKey)
            : targetType == typeof(sbyte) ? checked((sbyte)generatedIntegerKey)
            : targetType == typeof(short) ? checked((short)generatedIntegerKey)
            : targetType == typeof(ushort) ? checked((ushort)generatedIntegerKey)
            : targetType == typeof(int) ? checked((int)generatedIntegerKey)
            : targetType == typeof(uint) ? checked((uint)generatedIntegerKey)
            : targetType == typeof(long) ? generatedIntegerKey
            : targetType == typeof(ulong) ? checked((ulong)generatedIntegerKey)
            : generatedIntegerKey;
}
