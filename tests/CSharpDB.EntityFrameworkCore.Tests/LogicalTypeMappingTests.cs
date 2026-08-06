using CSharpDB.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace CSharpDB.EntityFrameworkCore.Tests;

public sealed class LogicalTypeMappingTests
{
    [Fact]
    public void ConventionalClrMappings_UseLogicalSqlDeclarations()
    {
        using var context = new LogicalTypeContext();
        IEntityType entity = context.Model.FindEntityType(typeof(LogicalTypeEntity))!;

        AssertColumn(entity, nameof(LogicalTypeEntity.Active), "BOOLEAN");
        AssertColumn(entity, nameof(LogicalTypeEntity.Tiny), "TINYINT");
        AssertColumn(entity, nameof(LogicalTypeEntity.Small), "SMALLINT");
        AssertColumn(entity, nameof(LogicalTypeEntity.Id), "INTEGER");
        AssertColumn(entity, nameof(LogicalTypeEntity.Large), "BIGINT");
        AssertColumn(entity, nameof(LogicalTypeEntity.Single), "REAL");
        AssertColumn(entity, nameof(LogicalTypeEntity.Double), "DOUBLE PRECISION");
        AssertColumn(entity, nameof(LogicalTypeEntity.Amount), "DECIMAL(12,3)");
        AssertColumn(entity, nameof(LogicalTypeEntity.Code), "VARCHAR(20)");
        AssertColumn(entity, nameof(LogicalTypeEntity.FixedCode), "CHAR(4)");
        AssertColumn(entity, nameof(LogicalTypeEntity.Identifier), "UUID");
        AssertColumn(entity, nameof(LogicalTypeEntity.Date), "DATE");
        AssertColumn(entity, nameof(LogicalTypeEntity.Time), "TIME(3)");
        AssertColumn(entity, nameof(LogicalTypeEntity.Timestamp), "TIMESTAMP(3)");
        AssertColumn(entity, nameof(LogicalTypeEntity.TimestampWithZone), "TIMESTAMP(3) WITH TIME ZONE");
        AssertColumn(entity, nameof(LogicalTypeEntity.Duration), "INTERVAL DAY TO SECOND");
        AssertColumn(entity, nameof(LogicalTypeEntity.PreciseDuration), "INTERVAL DAY TO SECOND(3)");
        AssertColumn(entity, nameof(LogicalTypeEntity.Payload), "VARBINARY(16)");

        IProperty amount = entity.FindProperty(nameof(LogicalTypeEntity.Amount))!;
        Assert.Null(amount.GetRelationalTypeMapping().Converter);
        Assert.Equal(typeof(decimal), amount.GetRelationalTypeMapping().ClrType);

        IProperty duration = entity.FindProperty(nameof(LogicalTypeEntity.Duration))!;
        AssertIntervalConverter(duration, new TimeSpan(1, 2, 3, 4, 5).Add(TimeSpan.FromTicks(6)));
    }

    [Fact]
    public void ExplicitIntegerDecimalMapping_RetainsLegacyScaledConverter()
    {
        using var context = new LegacyDecimalContext();
        IProperty amount = context.Model
            .FindEntityType(typeof(LegacyDecimalEntity))!
            .FindProperty(nameof(LegacyDecimalEntity.Amount))!;

        Assert.Equal("INTEGER", amount.GetColumnType());
        Assert.NotNull(amount.GetRelationalTypeMapping().Converter);
        Assert.Equal(
            typeof(long),
            amount.GetRelationalTypeMapping().Converter!.ProviderClrType);
    }

    [Fact]
    public void ExplicitLogicalStoreTypes_PreserveDeclarationsAndFacets()
    {
        using var context = new ExplicitLogicalTypeContext();
        IEntityType entity = context.Model.FindEntityType(typeof(ExplicitLogicalTypeEntity))!;

        AssertColumn(entity, nameof(ExplicitLogicalTypeEntity.Amount), "DECIMAL(10,4)");
        AssertColumn(entity, nameof(ExplicitLogicalTypeEntity.Json), "JSON");
        AssertColumn(entity, nameof(ExplicitLogicalTypeEntity.Xml), "XML");
        AssertColumn(entity, nameof(ExplicitLogicalTypeEntity.FixedPayload), "BINARY(8)");
        AssertColumn(entity, nameof(ExplicitLogicalTypeEntity.Time), "TIME(6)");
        AssertColumn(entity, nameof(ExplicitLogicalTypeEntity.Timestamp), "TIMESTAMP(6)");
        AssertColumn(
            entity,
            nameof(ExplicitLogicalTypeEntity.TimestampWithZone),
            "TIMESTAMP(6) WITH TIME ZONE");
        AssertColumn(entity, nameof(ExplicitLogicalTypeEntity.YearMonth), "INTERVAL YEAR TO MONTH");
        AssertColumn(entity, nameof(ExplicitLogicalTypeEntity.Duration), "INTERVAL DAY TO SECOND");
        AssertColumn(
            entity,
            nameof(ExplicitLogicalTypeEntity.PreciseDuration),
            "INTERVAL DAY TO SECOND(6)");
        AssertColumn(entity, nameof(ExplicitLogicalTypeEntity.Bits), "BIT");
        AssertColumn(entity, nameof(ExplicitLogicalTypeEntity.FixedBits), "BIT(8)");
        AssertColumn(entity, nameof(ExplicitLogicalTypeEntity.VariableBits), "BIT VARYING(16)");
        AssertColumn(entity, nameof(ExplicitLogicalTypeEntity.VarBitAlias), "VARBIT(24)");

        AssertIntervalConverter(
            entity.FindProperty(nameof(ExplicitLogicalTypeEntity.PreciseDuration))!,
            new TimeSpan(2, 3, 4, 5, 6).Add(TimeSpan.FromTicks(7)));
    }

    [Fact]
    public async Task ExplicitIntervalsAndBitStrings_RoundTripThroughEfCore()
    {
        await using var connection = new CSharpDbConnection("Data Source=:memory:");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await using var context = new ExplicitLogicalTypeContext(connection);
        await context.Database.EnsureCreatedAsync(TestContext.Current.CancellationToken);

        var expectedDuration = new TimeSpan(1, 2, 3, 4, 5).Add(TimeSpan.FromTicks(6));
        var expectedPreciseDuration = new TimeSpan(2, 3, 4, 5, 6);
        var entity = new ExplicitLogicalTypeEntity
        {
            Amount = 12.3456m,
            Json = "{\"ok\":true}",
            Xml = "<root />",
            FixedPayload = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08],
            Time = new TimeOnly(3, 4, 5),
            Timestamp = new DateTime(2026, 8, 5, 3, 4, 5),
            TimestampWithZone = new DateTimeOffset(2026, 8, 5, 3, 4, 5, TimeSpan.Zero),
            YearMonth = "2-03",
            Duration = expectedDuration,
            PreciseDuration = expectedPreciseDuration,
            Bits = [0b1010_0101],
            FixedBits = [0b0101_1010],
            VariableBits = [0x12, 0x34],
            VarBitAlias = [0x12, 0x34, 0x56],
        };

        context.Add(entity);
        await context.SaveChangesAsync(TestContext.Current.CancellationToken);
        context.ChangeTracker.Clear();

        ExplicitLogicalTypeEntity loaded = await context.Set<ExplicitLogicalTypeEntity>()
            .AsNoTracking()
            .SingleAsync(TestContext.Current.CancellationToken);

        Assert.Equal("2-03", loaded.YearMonth);
        Assert.Equal(expectedDuration, loaded.Duration);
        Assert.Equal(expectedPreciseDuration, loaded.PreciseDuration);
        Assert.Equal(entity.Bits, loaded.Bits);
        Assert.Equal(entity.FixedBits, loaded.FixedBits);
        Assert.Equal(entity.VariableBits, loaded.VariableBits);
        Assert.Equal(entity.VarBitAlias, loaded.VarBitAlias);
    }

    private static void AssertIntervalConverter(IProperty property, TimeSpan value)
    {
        var converter = Assert.IsAssignableFrom<Microsoft.EntityFrameworkCore.Storage.ValueConversion.ValueConverter>(
            property.GetRelationalTypeMapping().Converter);
        Assert.Equal(typeof(string), converter.ProviderClrType);
        string canonical = Assert.IsType<string>(converter.ConvertToProvider(value));
        Assert.Equal(value.ToString("c", System.Globalization.CultureInfo.InvariantCulture), canonical);
        Assert.Equal(value, converter.ConvertFromProvider(canonical));
    }

    private static void AssertColumn(
        IEntityType entity,
        string propertyName,
        string expectedStoreType) =>
        Assert.Equal(
            expectedStoreType,
            entity.FindProperty(propertyName)!.GetColumnType());

    private sealed class LogicalTypeContext : DbContext
    {
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder) =>
            optionsBuilder.UseCSharpDb("Data Source=:memory:");

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<LogicalTypeEntity>()
                .Property(entity => entity.Amount)
                .HasPrecision(12, 3);
            modelBuilder.Entity<LogicalTypeEntity>()
                .Property(entity => entity.Code)
                .HasMaxLength(20);
            modelBuilder.Entity<LogicalTypeEntity>()
                .Property(entity => entity.FixedCode)
                .HasMaxLength(4)
                .IsFixedLength();
            modelBuilder.Entity<LogicalTypeEntity>()
                .Property(entity => entity.Payload)
                .HasMaxLength(16);
            modelBuilder.Entity<LogicalTypeEntity>()
                .Property(entity => entity.Time)
                .HasPrecision(3);
            modelBuilder.Entity<LogicalTypeEntity>()
                .Property(entity => entity.Timestamp)
                .HasPrecision(3);
            modelBuilder.Entity<LogicalTypeEntity>()
                .Property(entity => entity.TimestampWithZone)
                .HasPrecision(3);
            modelBuilder.Entity<LogicalTypeEntity>()
                .Property(entity => entity.PreciseDuration)
                .HasPrecision(3);
        }
    }

    private sealed class LegacyDecimalContext : DbContext
    {
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder) =>
            optionsBuilder.UseCSharpDb("Data Source=:memory:");

        protected override void OnModelCreating(ModelBuilder modelBuilder) =>
            modelBuilder.Entity<LegacyDecimalEntity>()
                .Property(entity => entity.Amount)
                .HasPrecision(18, 4)
                .HasColumnType("INTEGER");
    }

    private sealed class ExplicitLogicalTypeContext : DbContext
    {
        private readonly CSharpDbConnection? _connection;

        public ExplicitLogicalTypeContext()
        {
        }

        public ExplicitLogicalTypeContext(CSharpDbConnection connection) =>
            _connection = connection;

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (_connection is null)
                optionsBuilder.UseCSharpDb("Data Source=:memory:");
            else
                optionsBuilder.UseCSharpDb(_connection);
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<ExplicitLogicalTypeEntity>()
                .Property(entity => entity.Amount)
                .HasColumnType("NUMERIC(10,4)");
            modelBuilder.Entity<ExplicitLogicalTypeEntity>()
                .Property(entity => entity.Json)
                .HasColumnType("JSON");
            modelBuilder.Entity<ExplicitLogicalTypeEntity>()
                .Property(entity => entity.Xml)
                .HasColumnType("XML");
            modelBuilder.Entity<ExplicitLogicalTypeEntity>()
                .Property(entity => entity.FixedPayload)
                .HasColumnType("BINARY(8)");
            modelBuilder.Entity<ExplicitLogicalTypeEntity>()
                .Property(entity => entity.Time)
                .HasColumnType("TIME(6)");
            modelBuilder.Entity<ExplicitLogicalTypeEntity>()
                .Property(entity => entity.Timestamp)
                .HasColumnType("TIMESTAMP(6)");
            modelBuilder.Entity<ExplicitLogicalTypeEntity>()
                .Property(entity => entity.TimestampWithZone)
                .HasColumnType("TIMESTAMP(6) WITH TIME ZONE");
            modelBuilder.Entity<ExplicitLogicalTypeEntity>()
                .Property(entity => entity.YearMonth)
                .HasColumnType("INTERVAL YEAR TO MONTH");
            modelBuilder.Entity<ExplicitLogicalTypeEntity>()
                .Property(entity => entity.Duration)
                .HasColumnType("INTERVAL DAY TO SECOND");
            modelBuilder.Entity<ExplicitLogicalTypeEntity>()
                .Property(entity => entity.PreciseDuration)
                .HasColumnType("INTERVAL DAY TO SECOND(6)");
            modelBuilder.Entity<ExplicitLogicalTypeEntity>()
                .Property(entity => entity.Bits)
                .HasColumnType("BIT");
            modelBuilder.Entity<ExplicitLogicalTypeEntity>()
                .Property(entity => entity.FixedBits)
                .HasColumnType("BIT(8)");
            modelBuilder.Entity<ExplicitLogicalTypeEntity>()
                .Property(entity => entity.VariableBits)
                .HasColumnType("BIT VARYING(16)");
            modelBuilder.Entity<ExplicitLogicalTypeEntity>()
                .Property(entity => entity.VarBitAlias)
                .HasColumnType("VARBIT(24)");
        }
    }

    private sealed class LogicalTypeEntity
    {
        public int Id { get; set; }
        public bool Active { get; set; }
        public byte Tiny { get; set; }
        public short Small { get; set; }
        public long Large { get; set; }
        public float Single { get; set; }
        public double Double { get; set; }
        public decimal Amount { get; set; }
        public string Code { get; set; } = string.Empty;
        public string FixedCode { get; set; } = string.Empty;
        public Guid Identifier { get; set; }
        public DateOnly Date { get; set; }
        public TimeOnly Time { get; set; }
        public DateTime Timestamp { get; set; }
        public DateTimeOffset TimestampWithZone { get; set; }
        public TimeSpan Duration { get; set; }
        public TimeSpan PreciseDuration { get; set; }
        public byte[] Payload { get; set; } = [];
    }

    private sealed class LegacyDecimalEntity
    {
        public int Id { get; set; }
        public decimal Amount { get; set; }
    }

    private sealed class ExplicitLogicalTypeEntity
    {
        public int Id { get; set; }
        public decimal Amount { get; set; }
        public string Json { get; set; } = string.Empty;
        public string Xml { get; set; } = string.Empty;
        public byte[] FixedPayload { get; set; } = [];
        public TimeOnly Time { get; set; }
        public DateTime Timestamp { get; set; }
        public DateTimeOffset TimestampWithZone { get; set; }
        public string YearMonth { get; set; } = string.Empty;
        public TimeSpan Duration { get; set; }
        public TimeSpan PreciseDuration { get; set; }
        public byte[] Bits { get; set; } = [];
        public byte[] FixedBits { get; set; } = [];
        public byte[] VariableBits { get; set; } = [];
        public byte[] VarBitAlias { get; set; } = [];
    }
}
