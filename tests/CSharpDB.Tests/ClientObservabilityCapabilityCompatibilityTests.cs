using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Client;
using CSharpDB.Engine;
using CSharpDB.Observability;

namespace CSharpDB.Tests;

public sealed class ClientObservabilityCapabilityCompatibilityTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public void PublicAccessDeniedException_HasOneSafeNonRetainingShape()
    {
        const string remoteCanary =
            "api-key SELECT secret FROM C:/private/diagnostics.db";
        Type exceptionType = typeof(CSharpDbObservabilityAccessDeniedException);

        var exception = new CSharpDbObservabilityAccessDeniedException();

        Assert.True(exceptionType.IsPublic);
        Assert.True(exceptionType.IsSealed);
        Assert.Equal(
            CSharpDbObservabilityAccessDeniedException.SafeMessage,
            exception.Message);
        Assert.DoesNotContain(remoteCanary, exception.Message, StringComparison.Ordinal);
        Assert.Null(exception.InnerException);
        ConstructorInfo constructor = Assert.Single(
            exceptionType.GetConstructors(BindingFlags.Public | BindingFlags.Instance));
        Assert.Empty(constructor.GetParameters());
    }

    [Fact]
    public void ExistingClientContract_HasFrozenSourceAndBinaryShape()
    {
        Type contract = typeof(ICSharpDbClient);

        Assert.Equal([typeof(IAsyncDisposable)], contract.GetInterfaces());
        Assert.DoesNotContain(
            typeof(ICSharpDbObservabilityClient),
            contract.GetInterfaces());
        Assert.DoesNotContain(
            contract.GetMethods(BindingFlags.Public | BindingFlags.Instance),
            static method => method.Name is
                nameof(ICSharpDbObservabilityClient.GetRuntimeDiagnosticsAsync) or
                nameof(ICSharpDbObservabilityClient.GetStorageDiagnosticsAsync) or
                nameof(ICSharpDbObservabilityClient.GetWalDiagnosticsAsync) or
                nameof(ICSharpDbObservabilityClient.GetActiveQueriesAsync) or
                nameof(ICSharpDbObservabilityClient.GetRecentQueriesAsync) or
                nameof(ICSharpDbObservabilityClient.GetQueryPlanDiagnosticsAsync) or
                nameof(ICSharpDbObservabilityClient.GetSessionsAsync) or
                nameof(ICSharpDbObservabilityClient.GetActiveMaintenanceOperationsAsync) or
                nameof(ICSharpDbObservabilityClient.GetRecentMaintenanceOperationsAsync) or
                nameof(ICSharpDbObservabilityClient.GetQueryDetailAsync));

        string binaryAndSourceShape = string.Join(
            '\n',
            contract
                .GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
                .OrderBy(static method => method.Name, StringComparer.Ordinal)
                .ThenBy(FormatMethod, StringComparer.Ordinal)
                .Select(FormatMethod));
        string fingerprint = Convert.ToHexString(
            SHA256.HashData(Encoding.UTF8.GetBytes(binaryAndSourceShape)));

        // Frozen from the v4.5 ICSharpDbClient contract. Parameter names and
        // optional defaults are included because they are source-compatibility
        // surface; CLR types and method names cover the binary contract.
        Assert.Equal(
            "A21F122F31DB29F1986D21BCA64BAB93A3873099F2CFF2D9741B1B00399509B1",
            fingerprint);
    }

    [Fact]
    public async Task PublicWrapper_DelegatesTheOptionalDirectCapability()
    {
        await using ICSharpDbClient client = CSharpDbClient.Create(
            new CSharpDbClientOptions
            {
                DataSource = ":memory:",
                DirectDatabaseOptions = CreateOptions("public-wrapper"),
            });
        var diagnostics = Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(client);

        Assert.Null((await client.ExecuteSqlAsync("SELECT 1", Ct)).Error);
        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>> recent =
            await diagnostics.GetRecentQueriesAsync(10, Ct);

        Assert.Equal(DiagnosticsAvailability.Available, recent.Metadata.Availability);
        Assert.Single(recent.Aggregate.Records!);
    }

    [Fact]
    public async Task PublicWrapper_MapsAMissingCustomCapabilityToOneSafeException()
    {
        const string sensitiveDataSource = "https://secret.example.test/private-database";
        ICSharpDbClient legacy =
            DispatchProxy.Create<ICSharpDbClient, LegacyClientProxy>();
        ((LegacyClientProxy)(object)legacy).DataSource = sensitiveDataSource;
        ConstructorInfo constructor = Assert.IsAssignableFrom<ConstructorInfo>(
            typeof(CSharpDbClient).GetConstructor(
                BindingFlags.Instance | BindingFlags.NonPublic,
                binder: null,
                types: [typeof(ICSharpDbClient)],
                modifiers: null));
        await using var wrapper = Assert.IsType<CSharpDbClient>(
            constructor.Invoke([legacy]));
        var diagnostics = Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(wrapper);

        CSharpDbObservabilityNotSupportedException exception =
            await Assert.ThrowsAsync<CSharpDbObservabilityNotSupportedException>(
                () => diagnostics.GetRuntimeDiagnosticsAsync(Ct));

        Assert.Equal(CSharpDbObservabilityNotSupportedException.SafeMessage, exception.Message);
        Assert.DoesNotContain(sensitiveDataSource, exception.Message, StringComparison.Ordinal);
        Assert.Null(exception.InnerException);
    }

    private static string FormatMethod(MethodInfo method)
        => $"{FormatType(method.ReturnType)} {method.Name}(" +
           string.Join(
               ",",
               method.GetParameters().Select(FormatParameter)) +
           ")";

    private static string FormatParameter(ParameterInfo parameter)
    {
        string defaultValue = parameter.HasDefaultValue
            ? $"={FormatDefaultValue(parameter.DefaultValue)}"
            : string.Empty;
        return $"{FormatType(parameter.ParameterType)} {parameter.Name}{defaultValue}";
    }

    private static string FormatDefaultValue(object? value)
        => value switch
        {
            null => "null",
            string text => $"\"{text}\"",
            bool boolean => boolean ? "true" : "false",
            _ => Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture)
                 ?? "null",
        };

    private static string FormatType(Type type)
    {
        if (type.IsByRef)
            return $"{FormatType(type.GetElementType()!)}&";
        if (type.IsArray)
            return $"{FormatType(type.GetElementType()!)}[]";
        if (!type.IsGenericType)
            return type.FullName ?? type.Name;

        string name = type.GetGenericTypeDefinition().FullName!;
        int arityMarker = name.IndexOf('`');
        if (arityMarker >= 0)
            name = name[..arityMarker];
        return $"{name}<{string.Join(',', type.GetGenericArguments().Select(FormatType))}>";
    }

    private static DatabaseOptions CreateOptions(string alias)
        => new()
        {
            ObservabilityOptions = new CSharpDbObservabilityOptions
            {
                Enabled = true,
                DatabaseAlias = alias,
                Logging = new CSharpDbLoggingOptions
                {
                    Enabled = false,
                    Queries = false,
                    SlowQueries = false,
                },
            },
        };

    public class LegacyClientProxy : DispatchProxy
    {
        public string DataSource { get; set; } = "legacy";

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
            => targetMethod?.Name switch
            {
                "get_DataSource" => DataSource,
                nameof(IAsyncDisposable.DisposeAsync) => ValueTask.CompletedTask,
                _ => throw new NotSupportedException(),
            };
    }
}
