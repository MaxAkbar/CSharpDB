namespace CSharpDB.Execution;

internal sealed class OptimisticInsertSequenceContext
{
    private bool _enabled;
    private bool _allowOptimisticInsert;
    private bool _hasLastKey;
    private long _lastKey;

    internal void Reset(bool enabled)
    {
        _enabled = enabled;
        _allowOptimisticInsert = enabled;
        _hasLastKey = false;
        _lastKey = 0;
    }

    internal bool TryBeginInsert(long key)
    {
        if (!_enabled || !_allowOptimisticInsert)
            return false;

        if (_hasLastKey && key <= _lastKey)
        {
            _allowOptimisticInsert = false;
            return false;
        }

        return true;
    }

    internal void RecordInsertSuccess(long key)
    {
        _hasLastKey = true;
        _lastKey = key;
    }

    internal void RecordInsertFallback(long key)
    {
        _hasLastKey = true;
        _lastKey = key;
        _allowOptimisticInsert = false;
    }
}
