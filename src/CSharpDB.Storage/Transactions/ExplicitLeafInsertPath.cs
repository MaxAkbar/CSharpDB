namespace CSharpDB.Storage.Transactions;

internal readonly record struct ExplicitLeafInsertPath(
    uint RootPageId,
    uint[] PageIds);
