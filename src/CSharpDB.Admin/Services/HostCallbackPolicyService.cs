using CSharpDB.Primitives;

namespace CSharpDB.Admin.Services;

public sealed class HostCallbackPolicyService
{
    private readonly DbExtensionPolicy _policy;

    public HostCallbackPolicyService(DbExtensionPolicy policy)
    {
        _policy = policy;
    }

    public DbExtensionPolicy Policy => _policy;

    public DbExtensionPolicyDecision Evaluate(DbHostCallbackDescriptor callback)
    {
        ArgumentNullException.ThrowIfNull(callback);

        return DbExtensionPolicyEvaluator.Evaluate(
            callback,
            _policy,
            DbExtensionHostMode.Embedded);
    }
}
