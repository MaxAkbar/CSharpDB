using Microsoft.EntityFrameworkCore.Update;

namespace CSharpDB.EntityFrameworkCore.Update.Internal;

public sealed class CSharpDbModificationCommandBatchFactory : IModificationCommandBatchFactory
{
    private readonly ModificationCommandBatchFactoryDependencies _dependencies;

    public CSharpDbModificationCommandBatchFactory(ModificationCommandBatchFactoryDependencies dependencies)
        => _dependencies = dependencies;

    public ModificationCommandBatch Create()
        => new CSharpDbModificationCommandBatch(_dependencies);
}
