using CSharpDB.Storage.Transactions;

namespace CSharpDB.Tests;

public sealed class TransactionCoordinatorTests
{
    [Fact]
    public void PublishPendingCommit_PrunesConflictState_WhenNoExplicitTransactionsRemain()
    {
        using var coordinator = new TransactionCoordinator();

        TransactionCoordinator.PendingCommitReservation reservation = coordinator.ReservePendingCommit(
            new uint[] { 1u, 2u },
            new[] { new LogicalConflictKey("table:bench", 1) });

        coordinator.PublishPendingCommit(reservation);

        var counts = coordinator.GetTrackedConflictVersionCounts();
        Assert.Equal(0, counts.PageVersionCount);
        Assert.Equal(0, counts.LogicalVersionCount);
    }

    [Fact]
    public void UnregisterExplicitTransaction_PrunesVersionsOlderThanOldestActiveSnapshot()
    {
        using var coordinator = new TransactionCoordinator();
        LogicalConflictKey oldKey = new("table:bench", 1);
        LogicalConflictKey newKey = new("table:bench", 2);

        coordinator.RegisterExplicitTransaction(transactionId: 1, startVersion: 0);
        coordinator.RegisterExplicitTransaction(transactionId: 2, startVersion: 1);

        TransactionCoordinator.PendingCommitReservation firstReservation = coordinator.ReservePendingCommit(
            new uint[] { 1u },
            new[] { oldKey });
        coordinator.PublishPendingCommit(firstReservation);

        TransactionCoordinator.PendingCommitReservation secondReservation = coordinator.ReservePendingCommit(
            new uint[] { 2u },
            new[] { newKey });
        coordinator.PublishPendingCommit(secondReservation);

        var countsBeforePrune = coordinator.GetTrackedConflictVersionCounts();
        Assert.Equal(2, countsBeforePrune.PageVersionCount);
        Assert.Equal(2, countsBeforePrune.LogicalVersionCount);

        coordinator.UnregisterExplicitTransaction(1);

        var countsAfterPrune = coordinator.GetTrackedConflictVersionCounts();
        Assert.Equal(1, countsAfterPrune.PageVersionCount);
        Assert.Equal(1, countsAfterPrune.LogicalVersionCount);
        Assert.False(coordinator.TryGetPageLastWriteVersion(1u, out _));
        Assert.True(coordinator.TryGetPageLastWriteVersion(2u, out long retainedPageVersion));
        Assert.Equal(2L, retainedPageVersion);
        Assert.False(coordinator.HasLogicalConflict(new[] { oldKey }, startVersion: 0, out _));
        Assert.True(coordinator.HasLogicalConflict(new[] { newKey }, startVersion: 1, out LogicalConflictKey retainedConflictKey));
        Assert.Equal(newKey, retainedConflictKey);

        coordinator.UnregisterExplicitTransaction(2);

        var countsAfterAllTransactionsComplete = coordinator.GetTrackedConflictVersionCounts();
        Assert.Equal(0, countsAfterAllTransactionsComplete.PageVersionCount);
        Assert.Equal(0, countsAfterAllTransactionsComplete.LogicalVersionCount);
    }
}
