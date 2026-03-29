internal sealed class PrefixedReplCommand : IReplCommand
{
    private readonly IReplCommand _innerCommand;
    private readonly IReadOnlyList<string> _prefixArguments;

    public PrefixedReplCommand(
        string name,
        string description,
        string usage,
        IReplCommand innerCommand,
        params string[] prefixArguments)
    {
        Name = name;
        Description = description;
        Usage = usage;
        _innerCommand = innerCommand;
        _prefixArguments = prefixArguments;
    }

    public string Name { get; }
    public string Description { get; }
    public string Usage { get; }

    public Task ExecuteAsync(ReplCommandContext context, IReadOnlyList<string> arguments, CancellationToken ct)
    {
        var mergedArguments = _prefixArguments.Concat(arguments).ToArray();
        return _innerCommand.ExecuteAsync(context, mergedArguments, ct);
    }
}
