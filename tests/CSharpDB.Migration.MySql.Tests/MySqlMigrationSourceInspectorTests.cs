using System.Reflection;
using CSharpDB.Migration;
using CSharpDB.Migration.MySql;
using MySqlConnector;

namespace CSharpDB.Migration.MySql.Tests;

public sealed class MySqlMigrationSourceInspectorTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task InspectUsesImmutableReaderSeam()
    {
        var reader = new RecordingReader(MySqlTestSnapshot.Create());
        var inspector = new MySqlMigrationSourceInspector(
            reader,
            MySqlInspectionLimits.Default);

        MigrationCatalog catalog = await inspector.InspectAsync(Request(), Ct);

        Assert.True(reader.Called);
        Assert.Equal(MigrationSourceKind.MySql, inspector.SourceKind);
        Assert.Equal(MigrationSourceKind.MySql, catalog.Source.Kind);
    }

    [Fact]
    public async Task ProfileRequestPerformsNoSourceAccess()
    {
        var reader = new RecordingReader(MySqlTestSnapshot.Create());
        var inspector = new MySqlMigrationSourceInspector(
            reader,
            MySqlInspectionLimits.Default);

        await Assert.ThrowsAsync<NotSupportedException>(
            async () => await inspector.InspectAsync(
                Request() with { IncludeProfile = true },
                Ct));

        Assert.False(reader.Called);
    }

    [Fact]
    public async Task InvalidProfileSizePerformsNoSourceAccess()
    {
        var reader = new RecordingReader(MySqlTestSnapshot.Create());
        var inspector = new MySqlMigrationSourceInspector(
            reader,
            MySqlInspectionLimits.Default);

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            async () => await inspector.InspectAsync(
                Request() with { ProfileSampleSize = 0 },
                Ct));

        Assert.False(reader.Called);
    }

    [Fact]
    public async Task UnsupportedTargetPerformsNoSourceAccess()
    {
        var reader = new RecordingReader(MySqlTestSnapshot.Create());
        var inspector = new MySqlMigrationSourceInspector(
            reader,
            MySqlInspectionLimits.Default);

        await Assert.ThrowsAsync<NotSupportedException>(
            async () => await inspector.InspectAsync(
                Request() with { TargetCSharpDbVersion = "999.0.0" },
                Ct));

        Assert.False(reader.Called);
    }

    [Fact]
    public async Task ProviderShapeFailuresHaveGenericSecretFreePublicMessage()
    {
        const string secret = "Password=DoNotEchoMySqlInspectorSecret";
        var inspector = new MySqlMigrationSourceInspector(
            new ThrowingReader(new FormatException(secret)),
            MySqlInspectionLimits.Default);

        MySqlMigrationException error =
            await Assert.ThrowsAsync<MySqlMigrationException>(
                async () => await inspector.InspectAsync(Request(), Ct));

        Assert.Equal(
            "The MySQL schema could not be inspected safely.",
            error.Message);
        Assert.Null(error.InnerException);
        Assert.DoesNotContain(secret, error.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task ProviderExceptionsHaveGenericSecretFreePublicMessage()
    {
        const string secret = "Password=DoNotEchoMySqlProviderSecret";
        ConstructorInfo? constructor = typeof(MySqlException).GetConstructor(
            BindingFlags.Instance | BindingFlags.NonPublic,
            binder: null,
            [typeof(string)],
            modifiers: null);
        Assert.NotNull(constructor);
        var providerFailure = Assert.IsType<MySqlException>(
            constructor.Invoke([secret]));
        var inspector = new MySqlMigrationSourceInspector(
            new ThrowingReader(providerFailure),
            MySqlInspectionLimits.Default);

        MySqlMigrationException error =
            await Assert.ThrowsAsync<MySqlMigrationException>(
                async () => await inspector.InspectAsync(Request(), Ct));

        Assert.Equal(
            "The MySQL schema could not be inspected safely.",
            error.Message);
        Assert.Null(error.InnerException);
        Assert.DoesNotContain(secret, error.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task PreCanceledInspectionPerformsNoSourceAccess()
    {
        var reader = new RecordingReader(MySqlTestSnapshot.Create());
        var inspector = new MySqlMigrationSourceInspector(
            reader,
            MySqlInspectionLimits.Default);
        using var canceled = new CancellationTokenSource();
        canceled.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await inspector.InspectAsync(Request(), canceled.Token));

        Assert.False(reader.Called);
    }

    private static MigrationInspectionRequest Request() =>
        new()
        {
            TargetCSharpDbVersion =
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
        };

    private sealed class RecordingReader : IMySqlCatalogReader
    {
        private readonly MySqlCatalogSnapshot snapshot;

        public RecordingReader(MySqlCatalogSnapshot snapshot)
        {
            this.snapshot = snapshot;
        }

        public bool Called { get; private set; }

        public ValueTask<MySqlCatalogSnapshot> ReadAsync(
            MySqlInspectionLimits limits,
            CancellationToken cancellationToken)
        {
            Called = true;
            return ValueTask.FromResult(snapshot);
        }
    }

    private sealed class ThrowingReader : IMySqlCatalogReader
    {
        private readonly Exception exception;

        public ThrowingReader(Exception exception)
        {
            this.exception = exception;
        }

        public ValueTask<MySqlCatalogSnapshot> ReadAsync(
            MySqlInspectionLimits limits,
            CancellationToken cancellationToken) =>
            ValueTask.FromException<MySqlCatalogSnapshot>(exception);
    }
}
