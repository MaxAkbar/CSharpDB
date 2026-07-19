using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;

namespace CSharpDB.EntityFrameworkCore.Storage.Internal;

internal sealed class CSharpDbRelationalTransactionFactory(
    RelationalTransactionFactoryDependencies dependencies)
    : IRelationalTransactionFactory
{
    public RelationalTransaction Create(
        IRelationalConnection connection,
        DbTransaction transaction,
        Guid transactionId,
        IDiagnosticsLogger<DbLoggerCategory.Database.Transaction> logger,
        bool transactionOwned) =>
        new CSharpDbRelationalTransaction(
            connection,
            transaction,
            transactionId,
            logger,
            transactionOwned,
            dependencies.SqlGenerationHelper);
}

internal sealed class CSharpDbRelationalTransaction(
    IRelationalConnection connection,
    DbTransaction transaction,
    Guid transactionId,
    IDiagnosticsLogger<DbLoggerCategory.Database.Transaction> logger,
    bool transactionOwned,
    ISqlGenerationHelper sqlGenerationHelper)
    : RelationalTransaction(
        connection,
        transaction,
        transactionId,
        logger,
        transactionOwned,
        sqlGenerationHelper)
{
    private const string SavepointMessage =
        "CSharpDB does not support transaction savepoints.";

    public override bool SupportsSavepoints => false;

    public override void CreateSavepoint(string name) =>
        throw new NotSupportedException(SavepointMessage);

    public override Task CreateSavepointAsync(
        string name,
        CancellationToken cancellationToken = default) =>
        Task.FromException(new NotSupportedException(SavepointMessage));

    public override void RollbackToSavepoint(string name) =>
        throw new NotSupportedException(SavepointMessage);

    public override Task RollbackToSavepointAsync(
        string name,
        CancellationToken cancellationToken = default) =>
        Task.FromException(new NotSupportedException(SavepointMessage));

    public override void ReleaseSavepoint(string name) =>
        throw new NotSupportedException(SavepointMessage);

    public override Task ReleaseSavepointAsync(
        string name,
        CancellationToken cancellationToken = default) =>
        Task.FromException(new NotSupportedException(SavepointMessage));
}
