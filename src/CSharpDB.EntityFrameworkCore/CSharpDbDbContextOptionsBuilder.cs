using Microsoft.EntityFrameworkCore;

namespace CSharpDB.EntityFrameworkCore;

public sealed class CSharpDbDbContextOptionsBuilder
{
    public CSharpDbDbContextOptionsBuilder(DbContextOptionsBuilder optionsBuilder)
        => OptionsBuilder = optionsBuilder ?? throw new ArgumentNullException(nameof(optionsBuilder));

    public DbContextOptionsBuilder OptionsBuilder { get; }
}
