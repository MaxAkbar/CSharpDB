namespace CSharpDB.Migration.CSharpDb;

public enum CSharpDbMigrationFaultPoint
{
    BeforeRows,
    AfterRowsBeforeReceipt,
    AfterReceiptBeforeCommit,
    AfterCommit,
}

public interface ICSharpDbMigrationFaultInjector
{
    ValueTask InjectAsync(
        CSharpDbMigrationFaultPoint point,
        MigrationTargetBatch batch,
        CancellationToken cancellationToken = default);
}

internal sealed class NoOpMigrationFaultInjector : ICSharpDbMigrationFaultInjector
{
    internal static readonly NoOpMigrationFaultInjector Instance = new();

    public ValueTask InjectAsync(
        CSharpDbMigrationFaultPoint point,
        MigrationTargetBatch batch,
        CancellationToken cancellationToken = default) => ValueTask.CompletedTask;
}
