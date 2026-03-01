using CSharpDB.Core;

namespace CSharpDB.Execution;

public interface IOperator : IAsyncDisposable
{
    ColumnDefinition[] OutputSchema { get; }

    /// <summary>
    /// True when the operator may reuse/mutate the same Current row buffer across MoveNext calls.
    /// Consumers that need row ownership (e.g. sort materialization) should clone when this is true.
    /// </summary>
    bool ReusesCurrentRowBuffer => true;

    ValueTask OpenAsync(CancellationToken ct = default);
    ValueTask<bool> MoveNextAsync(CancellationToken ct = default);
    DbValue[] Current { get; }
}

internal interface IRowBufferReuseController
{
    void SetReuseCurrentRowBuffer(bool reuse);
}

internal interface IPreDecodeFilterSupport
{
    void SetPreDecodeFilter(int columnIndex, CSharpDB.Sql.BinaryOp op, DbValue literal);
}
